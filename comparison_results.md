# Producer-consumer with a bounded buffer: threads vs processes

Files: `Jamil_Umer_4_threads.py`, `Jamil_Umer_4_multiprocess.py`
Run either one with e.g. `python3 file_name.py -p 3 -c 3 -n 2 -i 8`; `--help` lists the knobs (producers, consumers, capacity, jitter, start method).

## What changed between versions

| | Threading | Multiprocessing |
|---|---|---|
| Buffer storage | `deque` in the one shared heap | Ring buffer in shared memory (`RawArray` + head/tail/count) |
| Item type | Any Python object (`(producer, seq)` tuple) | Must fit a fixed C slot, so an int: `pid * 1_000_000 + seq` |
| Lock / semaphores | `threading.Lock`, `threading.Semaphore` | `multiprocessing.Lock`, `multiprocessing.Semaphore` (same API, but they must be created up front and handed to each child) |
| Returning results | Each thread writes its own list slot | Children mail results through a `Queue`; the parent must **drain it before joining**, or a child blocked flushing the pipe never exits |
| Start-up | `Thread(...).start()` | Depends on start method. `spawn` re-imports the file in every child, so workers must be top-level functions and `main()` sits behind `if __name__ == "__main__"` |
| Random sleeps | Seeded per worker | Must be seeded per worker, since `fork` clones the parent's RNG state and all children would otherwise sleep in lock-step |
| Log lines | `print` inside the mutex | Same, plus `flush=True`: each process buffers stdout on its own |
| Hang safety | Threads cannot be killed: daemon threads plus a `join` timeout detect a hang and exit non-zero | A `finally` block `terminate()`s anything alive, then the exit codes and `active_children()` are checked |

## What the GIL cost

Median of 3 runs, 200 items, buffer of 8, 2 producers with the GIL enabled, **2 CPU cores**).

Consumer work is a pure-Python loop (about 10 ms per item):

| consumers | threads | processes (fork) |
|---|---|---|
| 1 | 1.80 s | 1.83 s |
| 2 | 1.90 s (0.95x) | 0.91 s (2.03x) |
| 4 | 1.93 s (0.93x) | 0.95 s (1.94x) |

Consumer work is a 10 ms sleep (stands in for blocking I/O):

| consumers | threads | processes (fork) |
|---|---|---|
| 1 | 2.04 s | 2.04 s |
| 2 | 1.02 s (2.00x) | 1.02 s (2.00x) |
| 4 | 0.51 s (3.99x) | 0.52 s (3.94x) |

- **CPU-bound: the GIL cost all of the parallelism.** Extra consumer threads bought no speedup, and were about 5-7% slower than one thread (contention for the interpreter lock). Processes got the full 2x the two cores allow; going to 4 consumers adds nothing because the machine has only 2 cores, and that is a hardware limit, not a GIL effect.
- **I/O-bound: the GIL cost nothing.** `sleep` (like blocking reads and writes) releases the lock, so threads scaled exactly as well as processes.
- **The GIL did not make the buffer safe.** A single `deque.append` is atomic, but "wait until there is room, then insert, then wake a consumer" spans many steps. The mutex and the two semaphores were needed in the threaded version too.
- **What processes cost instead.** Start-up (with no real work, 1000 items took about 0.02 s on threads, 0.03 s on forked processes and 0.14 s on `spawn`; the fork/thread gap is within noise at this size), plus the shared-memory restrictions in the table above.

## Note

Only 2 cores, so the best possible process speedup is 2x. Medians of 3 runs, one machine.
