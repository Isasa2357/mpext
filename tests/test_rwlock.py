import multiprocessing as mp
import time
import unittest
from contextlib import ExitStack

import mpext


def hold(lock, write, started, entered, release):
    started.set()
    with (lock.write_lock() if write else lock.read_lock()):
        entered.set()
        if not release.wait(15):
            raise TimeoutError("parent did not release worker")


def check_after_exception(lock, done):
    for get_lock in (lock.read_lock, lock.write_lock):
        try:
            with get_lock():
                raise ValueError("leave the protected section")
        except ValueError:
            pass
    with lock.write_lock():
        pass
    with lock.read_lock():
        done.set()


def stop_process(process):
    if process.is_alive():
        process.terminate()
    process.join(5)


def wait_for_writer(lock):
    # Wait for actual registration, independent of process scheduling.
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        with lock._changed:
            if lock._waiting_writers.value:
                return
        time.sleep(0.005)
    else:
        raise AssertionError("writer did not register")


class RWLockTests(unittest.TestCase):
    def test_writer_preference(self):
        for method in ("spawn", "fork"):
            if method not in mp.get_all_start_methods():
                continue
            with self.subTest(method=method), ExitStack() as cleanup:
                ctx = mp.get_context(method)
                lock = mpext.RWLock(ctx=ctx)
                processes = []

                def start(write):
                    started, entered, release = (ctx.Event() for _ in range(3))
                    p = ctx.Process(target=hold, args=(lock, write, started, entered, release))
                    p.start()
                    cleanup.callback(stop_process, p)
                    processes.append(p)
                    self.assertTrue(started.wait(5))
                    return entered, release

                first, release_first = start(False)
                second, release_second = start(False)
                self.assertTrue(first.wait(5))
                self.assertTrue(second.wait(5))
                writer, release_writer = start(True)
                wait_for_writer(lock)

                late, release_late = start(False)
                self.assertFalse(late.wait(0.1))
                release_first.set()
                processes[0].join(5)
                self.assertEqual(processes[0].exitcode, 0)
                self.assertFalse(writer.is_set())
                release_second.set()
                self.assertTrue(writer.wait(5))
                other_writer, release_other = start(True)
                wait_for_writer(lock)
                self.assertFalse(other_writer.wait(0.1))
                self.assertFalse(late.is_set())
                release_writer.set()
                self.assertTrue(other_writer.wait(5))
                self.assertFalse(late.is_set())
                release_other.set()
                self.assertTrue(late.wait(5))
                release_late.set()
                for p in processes:
                    p.join(5)
                    self.assertEqual(p.exitcode, 0)

    def test_exception_releases_lock(self):
        ctx = mp.get_context("spawn")
        done = ctx.Event()
        lock = mpext.RWLock(ctx=ctx)
        p = ctx.Process(target=check_after_exception, args=(lock, done))
        p.start()
        self.addCleanup(stop_process, p)
        self.assertTrue(done.wait(5))
        p.join(5)
        self.assertEqual(p.exitcode, 0)
