#!/usr/bin/env python3
"""Inclusive share of broker samples per system call, per thread, and the Rust
frames above each one: syscalls.py STACKS [SYSCALL [DEPTH]]"""

from __future__ import annotations

import sys
from collections import Counter

from stacks import read_samples, rust_frames


def main() -> None:
    samples = read_samples(sys.argv[1])
    wanted = sys.argv[2] if len(sys.argv) > 2 else None
    depth = int(sys.argv[3]) if len(sys.argv) > 3 else 3
    total = len(samples)
    by_syscall: Counter[str] = Counter()
    by_thread: Counter[tuple[str, str]] = Counter()
    callers: Counter[tuple[str, str]] = Counter()
    for comm, stack in samples:
        syscall = next((frame for frame in stack if frame.startswith("__arm64_sys_")), None)
        if syscall is None:
            continue
        by_syscall[syscall] += 1
        by_thread[(comm, syscall)] += 1
        if syscall == wanted:
            above = stack[stack.index(syscall) + 1:]
            callers[(comm, " <- ".join(list(rust_frames(above))[:depth]))] += 1
    print(f"samples {total}")
    for syscall, count in by_syscall.most_common(15):
        print(f"{100 * count / total:5.1f}%  {syscall}")
    print()
    for (comm, syscall), count in by_thread.most_common(15):
        print(f"{100 * count / total:5.1f}%  {comm:16s} {syscall}")
    if wanted:
        print()
        for (comm, chain), count in callers.most_common(30):
            print(f"{100 * count / total:5.1f}%  {comm[:15]:15s} {chain[:230]}")


if __name__ == "__main__":
    main()
