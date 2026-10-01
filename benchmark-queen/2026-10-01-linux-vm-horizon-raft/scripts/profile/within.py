#!/usr/bin/env python3
"""Leaf frames, with their first broker caller, of the samples that contain a
frame: within.py STACKS SUBSTRING [TOP]"""

from __future__ import annotations

import sys
from collections import Counter

from stacks import read_samples, short


def main() -> None:
    samples = read_samples(sys.argv[1])
    target = sys.argv[2]
    top = int(sys.argv[3]) if len(sys.argv) > 3 else 20
    leaves: Counter[tuple[str, str]] = Counter()
    hits = 0
    for _, stack in samples:
        if not any(target in frame for frame in stack):
            continue
        hits += 1
        leaf = short(stack[0]) if stack else "?"
        caller = next((short(frame) for frame in stack if "queen::" in frame), "?")
        leaves[(leaf, caller)] += 1
    total = len(samples)
    print(f"{target}: {100 * hits / total:.1f}% of broker samples")
    for (leaf, caller), count in leaves.most_common(top):
        print(f"{100 * count / total:5.1f}%  {leaf[-45:]:45s} in {caller[-70:]}")


if __name__ == "__main__":
    main()
