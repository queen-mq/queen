"""
KeyByOperator — override the implicit partition-key with a user-derived
key for downstream stateful operators (window/reduce/aggregate).

Default key is the source ``partition_id``, giving zero cross-worker
contention. ``.key_by(...)`` overrides to a different value per record.
"""

from __future__ import annotations

import inspect
from typing import Any, Callable


class KeyByOperator:
    kind = "keyBy"

    def __init__(self, fn: Callable[[Any], Any]):
        self.fn = fn
        self.config: dict = {}

    async def apply(self, envelope: dict) -> list[dict]:
        key = self.fn(envelope.get("msg"))
        if inspect.isawaitable(key):
            key = await key
        return [{**envelope, "key": str(key)}]
