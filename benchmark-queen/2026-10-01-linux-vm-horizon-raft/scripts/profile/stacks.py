"""Parse `perf script -F comm,ip,sym --no-inline` output, demangled with rustfilt.

Each sample is a thread name followed by its frames, leaf first.
"""

from __future__ import annotations

import re
from collections.abc import Iterator


def read_samples(path: str) -> list[tuple[str, list[str]]]:
    samples: list[tuple[str, list[str]]] = []
    comm: str | None = None
    stack: list[str] = []
    with open(path, errors="replace") as lines:
        for line in lines:
            if line.startswith("\t"):
                parts = line.strip().split(" ", 1)
                stack.append(parts[1] if len(parts) > 1 else parts[0])
            elif line.strip():
                if comm is not None:
                    samples.append((comm, stack))
                comm, stack = line.strip(), []
    if comm is not None:
        samples.append((comm, stack))
    return samples


def short(frame: str) -> str:
    """A demangled Rust frame without closures, shims and generic arguments."""
    frame = re.sub(r"::\{closure#\d+\}|::\{shim[^}]*\}", "", frame)
    for _ in range(4):
        frame = re.sub(r"<[^<>]*>", "", frame)
    return frame.split(" as ")[0].replace("queen::", "")


def rust_frames(stack: list[str]) -> Iterator[str]:
    return (short(frame) for frame in stack if "::" in frame)
