#!/usr/bin/env python3

# Bounded-buffer producer/consumer -- THREADING version.
#
#   1. What is the buffer, and who may touch it?
#      BoundedBuffer: a deque plus one mutex. Only put() and get() touch the
#      deque, and each does so while holding the mutex.
#   2. What makes a producer wait, and what wakes it?
#      The counting semaphore `empty_slots` (starts at `capacity`). put() takes
#      one permit and blocks when none are left. Every get() hands a permit back,
#      which wakes exactly one waiting producer.
#   3. What makes a consumer wait, and what wakes it?
#      The counting semaphore `filled_slots` (starts at 0). get() takes a permit
#      and blocks at zero. Every put() hands one back.
#   4. How does a consumer know that no more items are coming?
#      Poison pill. main() joins every producer, then enqueues one STOP per
#      consumer. The buffer is FIFO, so every real item sits ahead of every STOP,
#      and a consumer exits after its FIRST STOP, so N consumers eat exactly N.
#
# Every log line is printed while holding the buffer mutex, so the "buffer size"
# shown on a line is the size at that exact moment, and lines appear in the
# order the operations really happened.

# Run Instructions:
# python3 pc_multiprocessing.py

# 1 producer, 1 consumer first
# python3 Jamil_Umer_4_threads.py -p 1 -c 1

# then 3 and 3
# python3 Jamil_Umer_4_threads.py -p 3 -c 3

# tiny buffer to force the full/empty cases (capacity 1 or 2)
# python3 Jamil_Umer_4_threads.py -p 3 -c 3 -n 1
# python3 Jamil_Umer_4_threads.py -p 3 -c 3 -n 2

# bigger random sleeps to shake out races (seconds, max per item)
# python3 Jamil_Umer_4_threads.py -p 3 -c 3 -n 2 --jitter 0.05

# run it 20 times in a row and stop at the first failure
# for i in $(seq 1 20); do python3 Jamil_Umer_4_threads.py -p 3 -c 3 -n 2 --seed $i --quiet || break; done

import argparse
import random
import sys
import threading
import time
from collections import deque

STOP = None  # poison pill; real items are (producer_id, seq) tuples


def burn(n):
    """Pure-Python CPU work. It holds the GIL for the whole loop."""
    x = 0
    for i in range(n):
        x += i * i % 7
    return x


class BoundedBuffer:
    def __init__(self, capacity, quiet=False):
        self.capacity = capacity
        self.quiet = quiet
        self._items = deque()
        self._mutex = threading.Lock()
        self._empty_slots = threading.Semaphore(capacity)
        self._filled_slots = threading.Semaphore(0)

    def _say(self, text):
        """Print one line. Caller must hold self._mutex."""
        if not self.quiet:
            print(text, flush=True)

    def _note(self, who, text):
        """Print a line that does not change the buffer (waiting / resumed)."""
        with self._mutex:
            self._say(f"[{who}] {text}")

    def put(self, item, who):
        # Fast path: a free slot exists. Slow path: say we are about to wait.
        if not self._empty_slots.acquire(blocking=False):
            self._note(who, "buffer full - waiting")
            self._empty_slots.acquire()
            self._note(who, "resumed")
        with self._mutex:
            self._items.append(item)
            size = len(self._items)
            if item is STOP:
                self._say(f"[{who}] {'sent stop signal':<22} buffer size: {size}")
            else:
                what = f"produced item {item[1]}"
                self._say(f"[{who}] {what:<22} buffer size: {size}")
        self._filled_slots.release()

    def get(self, who):
        if not self._filled_slots.acquire(blocking=False):
            self._note(who, "buffer empty - waiting")
            self._filled_slots.acquire()
            self._note(who, "resumed")
        with self._mutex:
            item = self._items.popleft()
            if item is STOP:
                self._say(f"[{who}] received stop signal - exiting")
            else:
                what = f"consumed item {item[1]}"
                self._say(f"[{who}] {what:<22} buffer size: {len(self._items)}")
        self._empty_slots.release()
        return item

    def size(self):
        with self._mutex:
            return len(self._items)


def producer(pid, buf, n_items, jitter, seed):
    rng = random.Random(seed)
    for seq in range(n_items):
        if jitter:
            time.sleep(rng.uniform(0, jitter))  # "make an item"
        buf.put((pid, seq), f"producer {pid}")


def consumer(cid, buf, jitter, work, io, seed, results):
    rng = random.Random(seed)
    mine = []
    while True:
        item = buf.get(f"consumer {cid}")
        if item is STOP:
            break
        if jitter:
            time.sleep(rng.uniform(0, jitter))
        if io:
            time.sleep(io)  # stands in for a blocking I/O call
        if work:
            burn(work)
        mine.append(item)
    results[cid - 1] = mine  # each thread writes its own slot: no race


def join_or_die(threads, deadline):
    for t in threads:
        t.join(timeout=max(0.0, deadline - time.monotonic()))
    hung = [t.name for t in threads if t.is_alive()]
    if hung:
        print(f"HUNG: {hung} still alive at the deadline", flush=True)
        sys.exit(2)  # threads are daemons, so this really exits


def check(results, n_producers, n_items):
    """Return a list of problems (empty list == everything is right)."""
    problems = []
    if any(r is None for r in results):
        return ["a consumer never reported its results"]
    got = sorted(it for r in results for it in r)
    want = [(p, s) for p in range(1, n_producers + 1) for s in range(n_items)]
    if got != want:
        missing = sorted(set(want) - set(got))
        extra = [it for it in got if got.count(it) > 1]
        problems.append(f"items differ: {len(missing)} missing, {len(set(extra))} duplicated")
    for cid, r in enumerate(results, 1):  # per-producer FIFO order, per consumer
        last = {}
        for p, s in r:
            if s <= last.get(p, -1):
                problems.append(f"consumer {cid} saw producer {p} out of order")
                break
            last[p] = s
    return problems


def parse_args(argv):
    ap = argparse.ArgumentParser(description="Bounded-buffer producer/consumer (threads)")
    ap.add_argument("-p", "--producers", type=int, default=2)
    ap.add_argument("-c", "--consumers", type=int, default=2)
    ap.add_argument("-n", "--capacity", type=int, default=3, help="buffer slots")
    ap.add_argument("-i", "--items", type=int, default=8, help="items made by EACH producer")
    ap.add_argument("--jitter", type=float, default=0.01,
                    help="max random sleep (s) per item, producers and consumers")
    ap.add_argument("--work", type=int, default=0, help="CPU loop iterations per item")
    ap.add_argument("--io", type=float, default=0.0, help="fixed sleep (s) per item")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--timeout", type=float, default=60.0, help="hang detector (s)")
    ap.add_argument("--quiet", action="store_true", help="no per-item log lines")
    a = ap.parse_args(argv)
    if min(a.producers, a.consumers, a.capacity, a.items) < 1:
        ap.error("producers, consumers, capacity and items must all be >= 1")
    return a


def main(argv=None):
    a = parse_args(argv)
    buf = BoundedBuffer(a.capacity, a.quiet)
    results = [None] * a.consumers

    producers = [
        threading.Thread(target=producer, name=f"producer {p}", daemon=True,
                         args=(p, buf, a.items, a.jitter, a.seed * 1000 + p))
        for p in range(1, a.producers + 1)
    ]
    consumers = [
        threading.Thread(target=consumer, name=f"consumer {c}", daemon=True,
                         args=(c, buf, a.jitter, a.work, a.io, a.seed * 1000 + 500 + c, results))
        for c in range(1, a.consumers + 1)
    ]

    deadline = time.monotonic() + a.timeout
    t0 = time.perf_counter()
    for t in consumers + producers:
        t.start()

    join_or_die(producers, deadline)          # 1. all real items are now enqueued
    for _ in consumers:                       # 2. one poison pill per consumer
        buf.put(STOP, "main")
    join_or_die(consumers, deadline)          # 3. every consumer has exited
    elapsed = time.perf_counter() - t0

    left = buf.size()
    print(f"all producers finished, all consumers finished, {left} items left", flush=True)
    problems = check(results, a.producers, a.items)
    if left:
        problems.append(f"{left} items left in the buffer")
    total = a.producers * a.items
    consumed = sum(len(r) for r in results if r)
    print(f"produced={total} consumed={consumed} left={left} "
          f"per-consumer={[len(r) for r in results if r is not None]}")
    print(f"elapsed: {elapsed:.3f}s")
    if problems:
        print("FAILED: " + "; ".join(problems))
        return 1
    print("verified: every item consumed exactly once, per-producer order preserved")
    return 0


if __name__ == "__main__":
    sys.exit(main())