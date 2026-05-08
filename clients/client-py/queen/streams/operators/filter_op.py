"""
FilterOperator — drop envelopes where predicate returns False.

The predicate sees the original Queen ``msg`` to keep filtering decisions
tied to the source record, not to a possibly already-mapped value.
"""

from __future__ import annotations

import inspect
from typing import Any, Callable


class FilterOperator:
    kind = "filter"

    def __init__(self, predicate: Callable[..., Any]):
        self.fn = predicate
        self.config: dict = {}

    async def apply(self, envelope: dict) -> list[dict]:
        try:
            keep = self.fn(envelope.get("msg"), envelope.get("ctx"))
        except TypeError:
            keep = self.fn(envelope.get("msg"))
        if inspect.isawaitable(keep):
            keep = await keep
        return [envelope] if keep else []
