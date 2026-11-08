#!/usr/bin/env python3
# Bounded-buffer producer/consumer -- MULTIPROCESSING version.

# Same algorithm as Jamil_Umer_4_threads.py (two counting semaphores + one mutex), but
# every piece of shared state now has to live somewhere all processes can see.
#
# The four design questions, answered before writing any code
#   1. What is the buffer, and who may touch it?
#      SharedBoundedBuffer: a ring buffer in shared memory (a RawArray of slots
#      plus head / tail / count counters) guarded by one cross-process Lock.
#      Only put() and get() touch it, and only while holding that lock.
#      Items must fit a fixed-size C slot, so an item is one int:
#      pid * ITEM_BASE + seq (a thread version can pass any Python object).
#   2. What makes a producer wait, and what wakes it?
#      `empty_slots` (a multiprocessing.Semaphore, starts at `capacity`). put()
#      takes a permit and blocks at zero; every get() gives one back.
#   3. What makes a consumer wait, and what wakes it?
#      `filled_slots` (starts at 0). get() takes a permit and blocks at zero;
#      every put() gives one back.
#   4. How does a consumer know that no more items are coming?
#      Poison pill, exactly as before: join all producers, then put one STOP per
#      consumer. FIFO order puts every real item ahead of every STOP; a consumer
#      exits after its first STOP, so N consumers eat exactly N.
#
# Run Instructions:
# python3 Jamil_Umer_4_multiprocess.py
# Without args, the defaults are 2 producers, 2 consumers, buffer of 3, 8 items per producer

# 1 producer, 1 consumer first, then 3 and 3
# python3 Jamil_Umer_4_multiprocess.py -p 1 -c 1
# python3 Jamil_Umer_4_multiprocess.py -p 3 -c 3
#
# # tiny buffer to force the full or empty cases
# python3 Jamil_Umer_4_multiprocess.py -p 3 -c 3 -n 1
# python3 Jamil_Umer_4_multiprocess.py -p 3 -c 3 -n 2
#
# # bigger random sleeps to shake out races
# python3 Jamil_Umer_4_multiprocess.py -p 3 -c 3 -n 2 --jitter 0.05
#
# # run it 20 times in a row and stop at the first failure
# for i in $(seq 1 20); do python3 Jamil_Umer_4_multiprocess.py -p 3 -c 3 -n 2 --seed $i --quiet || break; done

import argparse
import multiprocessing as mp
import queue
import random
import sys
import time

STOP = -1                 # poison pill; real items are encoded >= ITEM_BASE
ITEM_BASE = 1_000_000     # item value = producer_id * ITEM_BASE + seq


def burn(n):
    """Pure-Python CPU work (identical to the threading version)."""
    x = 0
    for i in range(n):
        x += i * i % 7
    return x


class SharedBoundedBuffer:
    def __init__(self, ctx, capacity, quiet=False):
        self.capacity = capacity
        self.quiet = quiet
        # Raw* = plain shared memory, no hidden lock; our own mutex guards it.
        self._slots = ctx.RawArray("q", capacity)
        self._head = ctx.RawValue("q", 0)    # next slot to read
        self._tail = ctx.RawValue("q", 0)    # next slot to write
        self._count = ctx.RawValue("q", 0)   # items currently stored
        self._mutex = ctx.Lock()
        self._empty_slots = ctx.Semaphore(capacity)
        self._filled_slots = ctx.Semaphore(0)

    def _say(self, text):
        """Print one line. Caller must hold self._mutex."""
        if not self.quiet:
            print(text, flush=True)  # flush: stdout is block-buffered per process

    def _note(self, who, text):
        with self._mutex:
            self._say(f"[{who}] {text}")

    def put(self, value, who):
        if not self._empty_slots.acquire(False):
            self._note(who, "buffer full - waiting")
            self._empty_slots.acquire()
            self._note(who, "resumed")
        with self._mutex:
            self._slots[self._tail.value] = value
            self._tail.value = (self._tail.value + 1) % self.capacity
            self._count.value += 1
            size = self._count.value
            if value == STOP:
                self._say(f"[{who}] {'sent stop signal':<22} buffer size: {size}")
            else:
                what = f"produced item {value % ITEM_BASE}"
                self._say(f"[{who}] {what:<22} buffer size: {size}")
        self._filled_slots.release()

    def get(self, who):
        if not self._filled_slots.acquire(False):
            self._note(who, "buffer empty - waiting")
            self._filled_slots.acquire()
            self._note(who, "resumed")
        with self._mutex:
            value = self._slots[self._head.value]
            self._head.value = (self._head.value + 1) % self.capacity
            self._count.value -= 1
            if value == STOP:
                self._say(f"[{who}] received stop signal - exiting")
            else:
                what = f"consumed item {value % ITEM_BASE}"
                self._say(f"[{who}] {what:<22} buffer size: {self._count.value}")
        self._empty_slots.release()
        return value

    def size(self):
        with self._mutex:
            return self._count.value


def producer(pid, buf, n_items, jitter, seed):
    rng = random.Random(seed)
    for seq in range(n_items):
        if jitter:
            time.sleep(rng.uniform(0, jitter))  # "make an item"
        buf.put(pid * ITEM_BASE + seq, f"producer {pid}")


def consumer(cid, buf, jitter, work, io, seed, results_q):
    rng = random.Random(seed)
    mine = []
    while True:
        value = buf.get(f"consumer {cid}")
        if value == STOP:
            break
        if jitter:
            time.sleep(rng.uniform(0, jitter))
        if io:
            time.sleep(io)  # stands in for a blocking I/O call
        if work:
            burn(work)
        mine.append(value)
    results_q.put((cid, mine))  # processes share nothing, so mail the answer home


def check(results, n_producers, n_items):
    """results: {consumer_id: [encoded items]}. Returns a list of problems."""
    problems = []
    got = sorted(v for r in results.values() for v in r)
    want = sorted(p * ITEM_BASE + s for p in range(1, n_producers + 1) for s in range(n_items))
    if got != want:
        missing = set(want) - set(got)
        dupes = {v for v in got if got.count(v) > 1}
        problems.append(f"items differ: {len(missing)} missing, {len(dupes)} duplicated")
    for cid, r in sorted(results.items()):  # per-producer FIFO order, per consumer
        last = {}
        for v in r:
            p, s = divmod(v, ITEM_BASE)
            if s <= last.get(p, -1):
                problems.append(f"consumer {cid} saw producer {p} out of order")
                break
            last[p] = s
    return problems


def parse_args(argv):
    ap = argparse.ArgumentParser(description="Bounded-buffer producer/consumer (processes)")
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
    ap.add_argument("--start-method", choices=["fork", "spawn", "forkserver"], default=None,
                    help="default: the platform default")
    a = ap.parse_args(argv)
    if min(a.producers, a.consumers, a.capacity, a.items) < 1:
        ap.error("producers, consumers, capacity and items must all be >= 1")
    if a.items >= ITEM_BASE:
        ap.error(f"items must be < {ITEM_BASE}")
    return a


def main(argv=None):
    a = parse_args(argv)
    ctx = mp.get_context(a.start_method)
    buf = SharedBoundedBuffer(ctx, a.capacity, a.quiet)
    results_q = ctx.Queue()

    producers = [
        ctx.Process(target=producer, name=f"producer {p}",
                    args=(p, buf, a.items, a.jitter, a.seed * 1000 + p))
        for p in range(1, a.producers + 1)
    ]
    consumers = [
        ctx.Process(target=consumer, name=f"consumer {c}",
                    args=(c, buf, a.jitter, a.work, a.io, a.seed * 1000 + 500 + c, results_q))
        for c in range(1, a.consumers + 1)
    ]
    everyone = consumers + producers
    deadline = time.monotonic() + a.timeout

    def remaining():
        return max(0.0, deadline - time.monotonic())

    def join_or_die(procs):
        for p in procs:
            p.join(timeout=remaining())
        hung = [p.name for p in procs if p.is_alive()]
        if hung:
            print(f"HUNG: {hung} still alive at the deadline", flush=True)
            sys.exit(2)

    results = {}
    t0 = time.perf_counter()
    try:
        for p in everyone:
            p.start()

        join_or_die(producers)                    # 1. all real items are enqueued
        for _ in consumers:                       # 2. one poison pill per consumer
            buf.put(STOP, "main")
        try:                                      # 3. drain results BEFORE joining
            for _ in consumers:
                cid, items = results_q.get(timeout=remaining())
                results[cid] = items
        except queue.Empty:
            print("HUNG: a consumer never reported back", flush=True)
            sys.exit(2)
        join_or_die(consumers)                    # 4. every consumer has exited
        elapsed = time.perf_counter() - t0
    finally:
        for p in everyone:                        # safety net, not the shutdown path
            if p.is_alive():
                p.terminate()
            p.join()

    left = buf.size()
    stray = mp.active_children()
    print(f"all producers finished, all consumers finished, {left} items left", flush=True)
    problems = check(results, a.producers, a.items)
    if left:
        problems.append(f"{left} items left in the buffer")
    bad_exit = [p.name for p in everyone if p.exitcode != 0]
    if bad_exit:
        problems.append(f"non-zero exit: {bad_exit}")
    if stray:
        problems.append(f"stray processes: {stray}")
    total = a.producers * a.items
    consumed = sum(len(r) for r in results.values())
    print(f"produced={total} consumed={consumed} left={left} "
          f"per-consumer={[len(results[c]) for c in sorted(results)]} "
          f"start-method={ctx.get_start_method()}")
    print(f"elapsed: {elapsed:.3f}s")
    if problems:
        print("FAILED: " + "; ".join(problems))
        return 1
    print("verified: every item consumed exactly once, per-producer order preserved, "
          "all processes exited 0, no strays")
    return 0


if __name__ == "__main__":
    sys.exit(main())