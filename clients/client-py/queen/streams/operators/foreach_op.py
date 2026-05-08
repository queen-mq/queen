"""
ForeachOperator — terminal at-least-once side-effect.

Invoked per emitted record before the cycle commits the source ack. If
``fn`` raises, the cycle is aborted and messages will be redelivered.
For exactly-once external effects, use ``.to(queue)``.
"""

from __future__ import annotations

import inspect
from typing import Any, Callable


class ForeachOperator:
    kind = "foreach"

    def __init__(self, fn: Callable[..., Any]):
        self.fn = fn
        self.config: dict = {"kind": "foreach"}

    async def run_effects(self, entries: list) -> None:
        for entry in entries:
            try:
                out = self.fn(entry.get("value"), entry.get("ctx"))
            except TypeError:
                out = self.fn(entry.get("value"))
            if inspect.isawaitable(out):
                await out
