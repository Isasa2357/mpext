import multiprocessing as mp
from contextlib import contextmanager


class RWLock:
    """Non-reentrant, writer-preferring interprocess read/write lock.

    Pass the same instance to each process at startup. Create it with the
    multiprocessing context used to start those processes. Waiting writers
    can delay readers indefinitely. Writers have no FIFO ordering guarantee.
    """

    def __init__(self, *, ctx=None):
        if ctx is None:
            ctx = mp.get_context()
        self._changed = ctx.Condition(ctx.Lock())
        # Every access to these shared values is guarded by the condition.
        self._readers = ctx.RawValue("q", 0)
        self._waiting_writers = ctx.RawValue("q", 0)
        self._writing = ctx.RawValue("b", False)

    @contextmanager
    def read_lock(self):
        """Admit a reader when no writer is active or waiting."""
        with self._changed:
            while self._writing.value or self._waiting_writers.value:
                self._changed.wait()
            self._readers.value += 1
        try:
            yield
        finally:
            with self._changed:
                self._readers.value -= 1
                if self._readers.value == 0 and self._waiting_writers.value:
                    self._changed.notify_all()

    @contextmanager
    def write_lock(self):
        """Block new readers and wait for exclusive access."""
        with self._changed:
            self._waiting_writers.value += 1
            try:
                while self._readers.value or self._writing.value:
                    self._changed.wait()
                self._writing.value = True
            finally:
                self._waiting_writers.value -= 1
                if not self._writing.value and not self._waiting_writers.value:
                    # A cancelled wait may have been the last queued writer.
                    self._changed.notify_all()
        try:
            yield
        finally:
            with self._changed:
                self._writing.value = False
                self._changed.notify_all()
