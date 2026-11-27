# mpext

Process synchronization tools for Python's multiprocessing module.

Install from this checkout with `python -m pip install -e .`.

## RWLock

Readers can run concurrently. Once a writer is waiting, new readers wait
until active readers finish and all queued writers have had a chance to write.
Each writer has exclusive access.

Create the lock in the parent and pass it to workers at startup. Use the same
multiprocessing context for the lock and the processes.

```python
import multiprocessing as mp

import mpext


def increment(lock, counter):
    with lock.write_lock():
        counter.value += 1


if __name__ == "__main__":
    ctx = mp.get_context("spawn")
    lock = mpext.RWLock(ctx=ctx)
    counter = ctx.RawValue("q", 0)
    worker = ctx.Process(target=increment, args=(lock, counter))
    worker.start()
    worker.join()
    with lock.read_lock():
        print(counter.value)
```

Use each lock once per critical section. Nesting acquisitions or upgrading
from read to write can deadlock. Writers have no FIFO guarantee, and a
continuous stream of writers can delay readers indefinitely.

Run the tests with `python -m unittest discover -s tests -v` after installation.
