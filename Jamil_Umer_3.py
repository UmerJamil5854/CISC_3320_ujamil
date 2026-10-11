#!/usr/bin/env python3

# To run the program:
# python3 Jamil_Umer_3.py data.csv

import argparse
import copy
import csv
from collections import deque

QUANTUM = 4  # default time slice for Round Robin / MLFQ


class SimProcess:
    def __init__(self, pid, arrival_time, burst_time, priority=0):
        self.pid = pid
        self.arrival_time = arrival_time
        self.burst_time = burst_time
        self.remaining_time = burst_time   # shrinks under RR/MLFQ
        self.priority = priority
        self.start_time = None             # first time it got the CPU
        self.completion_time = None


def _non_preemptive(processes, key):
    """
    Shared loop for FCFS, SJF and Priority: whenever the CPU is free,
    run the ready process with the smallest `key` to completion.
    """
    segments = []
    current_time = 0
    remaining = list(processes)

    while remaining:
        ready = [p for p in remaining if p.arrival_time <= current_time]
        if not ready:
            # CPU idle: jump to the next arrival
            current_time = min(p.arrival_time for p in remaining)
            continue

        p = min(ready, key=key)

        p.start_time = current_time
        current_time += p.burst_time
        p.completion_time = current_time
        p.remaining_time = 0
        segments.append((p.pid, p.start_time, p.completion_time))
        remaining.remove(p)

    return segments


def _multilevel_queue(processes, quanta):
    """
    Shared for Round Robin and MLFQ. `quanta` has one time slice per
    queue level.
    """
    queues = [deque() for _ in quanta]
    last = len(quanta) - 1
    pending = deque(sorted(processes, key=lambda p: p.arrival_time))
    segments = []
    current_time = 0

    def admit(now):
        while pending and pending[0].arrival_time <= now:
            queues[0].append(pending.popleft())

    while pending or any(queues):
        if not any(queues):
            # CPU idle: jump to the next arrival
            current_time = max(current_time, pending[0].arrival_time)
        admit(current_time)

        # run from the highest-priority non-empty queue
        level = next(l for l, q in enumerate(queues) if q)
        p = queues[level].popleft()
        if p.start_time is None:
            p.start_time = current_time

        run = min(quanta[level], p.remaining_time)

        # a new arrival (enters level 0) preempts anything running below level 0
        if level > 0 and pending and pending[0].arrival_time < current_time + run:
            run = pending[0].arrival_time - current_time

        segments.append((p.pid, current_time, current_time + run))
        current_time += run
        p.remaining_time -= run

        # arrivals during this slice are queued BEFORE the preempted process
        admit(current_time)

        if p.remaining_time == 0:
            p.completion_time = current_time
        elif run == quanta[level]:
            queues[min(level + 1, last)].append(p)   # used full slice: demote
        else:
            queues[level].append(p)                  # preempted: same level, back of queue

    return segments


def fcfs_schedule(processes, quantum=None):
    return _non_preemptive(processes, key=lambda p: p.arrival_time)


def sjf_schedule(processes, quantum=None):
    # shortest burst wins; ties go to earlier arrival
    return _non_preemptive(processes, key=lambda p: (p.burst_time, p.arrival_time))


def priority_schedule(processes, quantum=None):
    # Non-preemptive. Lower number = higher priority; ties go to earlier arrival
    return _non_preemptive(processes, key=lambda p: (p.priority, p.arrival_time))


def round_robin_schedule(processes, quantum=QUANTUM):
    return _multilevel_queue(processes, [quantum or QUANTUM])


def mlfq_schedule(processes, quantum=QUANTUM):
    # 3 queues; quantum doubles at each level (e.g. 4, 8, 16)
    base = quantum or QUANTUM
    return _multilevel_queue(processes, [base, base * 2, base * 4])


ALGORITHMS = {
    "FCFS": fcfs_schedule,
    "SJF": sjf_schedule,
    "Round Robin": round_robin_schedule,
    "Priority": priority_schedule,
    "MLFQ": mlfq_schedule,
}


def schedule(processes, algorithm, quantum=None):
    fresh = copy.deepcopy(processes)   # each algorithm gets its own clean copy
    segments = ALGORITHMS[algorithm](fresh, quantum)
    return segments, fresh


def load_workload(path):
    with open(path, mode="r", encoding="utf-8") as file:
        return [
            SimProcess(r[0], int(r[1]), int(r[2]), int(r[3]))
            for r in csv.reader(file) if r
        ]


def print_results(finished):
    print(f"{'PID':<6}{'Arrival':<9}{'Burst':<7}{'Start':<7}"
          f"{'Completion':<12}{'Waiting':<9}{'Turnaround'}")

    total_wait = total_tat = 0
    for p in sorted(finished, key=lambda p: p.start_time):
        tat = p.completion_time - p.arrival_time
        wait = tat - p.burst_time
        total_wait += wait
        total_tat += tat
        print(f"{p.pid:<6}{p.arrival_time:<9}{p.burst_time:<7}{p.start_time:<7}"
              f"{p.completion_time:<12}{wait:<9}{tat}")

    n = len(finished)
    print(f"Average Waiting Time:    {total_wait / n:.2f}")
    print(f"Average Turnaround Time: {total_tat / n:.2f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OS scheduler simulator")
    parser.add_argument("workload", help="path to the CSV workload file")
    args = parser.parse_args()

    processes = load_workload(args.workload)

    for i, name in enumerate(ALGORITHMS):
        if i > 0:
            print()  # blank line between algorithms
        print(name)
        _, finished = schedule(processes, name, QUANTUM)
        print_results(finished)