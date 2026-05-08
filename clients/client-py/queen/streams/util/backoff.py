"""
Exponential backoff helper for retrying transient failures (HTTP 5xx,
lease conflicts, etc.). Mirror of the JS makeBackoff helper.
"""

from __future__ import annotations

import asyncio
import random
from typing import Callable


def make_backoff(initial_ms: float = 50, max_ms: float = 5000, multiplier: float = 2):
    """
    Returns a stateful backoff helper with .next() and .reset().

    Usage:
        bo = make_backoff()
        for attempt in range(N):
            try:
                ...
                bo.reset()
                break
            except TransientError:
                await asyncio.sleep(bo.next() / 1000.0)
    """

    state = {"attempt": 0}

    def _next() -> float:
        ms = min(max_ms, int(initial_ms * (multiplier ** state["attempt"])))
        state["attempt"] += 1
        # Add jitter up to 30%.
        return ms + int(random.random() * ms * 0.3)

    def _reset() -> None:
        state["attempt"] = 0

    class _Backoff:
        next = staticmethod(_next)
        reset = staticmethod(_reset)

    return _Backoff()


async def sleep(ms: float) -> None:
    await asyncio.sleep(ms / 1000.0)
