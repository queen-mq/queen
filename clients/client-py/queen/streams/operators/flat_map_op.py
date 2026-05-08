"""
FlatMapOperator — emit zero or more values per input record.

The user fn returns a list of values; the operator wraps each in an envelope
sharing the source message's metadata.
"""

from __future__ import annotations

import inspect
from typing import Any, Callable


class FlatMapOperator:
    kind = "flatMap"

    def __init__(self, fn: Callable[..., Any]):
        self.fn = fn
        self.config: dict = {}

    async def apply(self, envelope: dict) -> list[dict]:
        try:
            out = self.fn(envelope.get("msg"), envelope.get("ctx"))
        except TypeError:
            out = self.fn(envelope.get("msg"))
        if inspect.isawaitable(out):
            out = await out
        if not isinstance(out, list):
            raise TypeError(
                f"flat_map fn must return a list (got {type(out).__name__})"
            )
        return [{**envelope, "value": v} for v in out]
