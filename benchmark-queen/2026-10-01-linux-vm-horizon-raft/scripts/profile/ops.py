#!/usr/bin/env python3
"""Inclusive broker CPU per operation: each sample goes to the first rule whose
frame it contains. ops.py STACKS"""

from __future__ import annotations

import sys
from collections import Counter

from stacks import read_samples

RULES = [
    ("HTTP: pop handler", ("handle_pop", "::pop_run", "answer_pop", "render_part_infos")),
    ("HTTP: ack handler", ("handle_ack", "dispatch_ack", "ack_impl", "resolve_acks", "ack_group")),
    ("HTTP: push handler", ("handle_push", "push_impl", "dispatch_push")),
    ("HTTP: other routes / metrics", ("handle_metrics", "prometheus", "handlers::")),
    ("HTTP: connection I/O (hyper)", ("hyper::",)),
    ("raft: planner / lanes", ("plan_cycle_lanes", "batcher::", "queen-planner")),
    ("raft: log write + fsync", ("replicator::local", "write_group_nosync", "QlogWrite", "handoff_group")),
    ("raft: qlog (queue log)", ("rsm::qlog",)),
    ("raft: apply", ("rsm::apply",)),
    ("consume engine (claims/checkpoint)", ("rsm::consume::Engine", "consume::checkpoint", "consume::pop")),
    ("consume: long-poll wake/serve", ("consume::wait",)),
    ("timers / retention / sweeper", ("timers", "retention", "sweeper", "maintenance", "syscollect")),
    ("tokio scheduler (idle/park/steal)", ("park_internal", "worker::run", "steal")),
]


def label(comm: str, stack: list[str]) -> str:
    for name, keys in RULES:
        if any(key in frame for frame in stack for key in keys):
            return name
    return f"other ({comm})"


def main() -> None:
    samples = read_samples(sys.argv[1])
    counts = Counter(label(comm, stack) for comm, stack in samples)
    for name, count in counts.most_common(25):
        print(f"{100 * count / len(samples):5.1f}%  {name}")


if __name__ == "__main__":
    main()
