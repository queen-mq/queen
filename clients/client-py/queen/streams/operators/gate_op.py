"""
GateOperator — per-message ALLOW/DENY decision with persistent per-key state.

See [GateOperator.js](clients/client-js/client-v2/streams/operators/GateOperator.js)
for the full semantic spec — this is a 1:1 Python port.

Used to build rate limiters, throttlers, fairness gates, circuit breakers.

Returns of the user fn:
  - ``True`` / ``{"allow": True}``  → state persisted, downstream sees the message
  - ``False`` / ``{"allow": False}`` → halt batch, partial ack, no lease release
"""

from __future__ import annotations

import inspect
from typing import Any, Callable


class GateOperator:
    kind = "gate"

    def __init__(self, fn: Callable[..., Any]):
        if not callable(fn):
            raise TypeError(".gate(fn) requires a callable")
        self.fn = fn
        self.config: dict = {}

    async def evaluate(self, envelope: dict, ctx: Any) -> dict:
        out = self.fn(envelope.get("value"), ctx)
        if inspect.isawaitable(out):
            out = await out
        if out is True or out is False:
            return {"allow": bool(out)}
        if isinstance(out, dict) and isinstance(out.get("allow"), bool):
            return {"allow": out["allow"]}
        raise TypeError(".gate(fn) must return bool or {'allow': bool}")
