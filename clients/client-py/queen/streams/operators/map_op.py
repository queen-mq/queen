"""
MapOperator — transform each input record. Sync or async user fn.

User-fn signature: ``(msg, ctx=None) -> new_value``

Pre-reducer:
  msg = the original Queen message (a dict with ``data``, ``partitionId``, ...)
  ctx = None (Queen msg already carries the metadata)

Post-reducer (after .window_tumbling + .reduce/.aggregate):
  msg = the aggregated value (e.g. {count, sum, min, max})
  ctx = {partition, partitionId, key, windowKey, windowStart, windowEnd}
"""

from __future__ import annotations

import inspect
from typing import Any, Callable


class MapOperator:
    kind = "map"

    def __init__(self, fn: Callable[..., Any]):
        self.fn = fn
        self.config: dict = {}

    async def apply(self, envelope: dict) -> list[dict]:
        try:
            value = self.fn(envelope.get("msg"), envelope.get("ctx"))
        except TypeError:
            # Function may take a single positional arg (no ctx).
            value = self.fn(envelope.get("msg"))
        if inspect.isawaitable(value):
            value = await value
        return [{**envelope, "value": value}]
