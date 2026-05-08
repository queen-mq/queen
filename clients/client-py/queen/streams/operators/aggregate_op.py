"""
AggregateOperator — sugar over reduce. 1:1 port of AggregateOperator.js.
"""

from __future__ import annotations

import inspect
from typing import Any, Callable

from .reduce_op import ReduceOperator


_RESERVED = {"count", "sum", "min", "max", "avg"}


class AggregateOperator(ReduceOperator):
    def __init__(self, extractors: dict):
        if not isinstance(extractors, dict):
            raise TypeError("aggregate requires a dict of named extractors")
        fields = []
        for name, fn in extractors.items():
            if not callable(fn):
                raise TypeError(f"aggregate.{name} must be callable")
            fields.append((name, fn))
        if not fields:
            raise ValueError("aggregate requires at least one extractor")

        initial: dict = {}
        for name, _ in fields:
            if name == "min":
                initial[name] = None
            elif name == "max":
                initial[name] = None
            elif name == "avg":
                initial["__avg_sum"] = 0
                initial["__avg_count"] = 0
                initial["avg"] = 0
            else:
                initial[name] = 0

        async def fn(acc: Any, msg: Any) -> dict:
            nxt = dict(acc) if isinstance(acc, dict) else {}
            for name, ex in fields:
                v = ex(msg)
                if inspect.isawaitable(v):
                    v = await v
                if name == "count":
                    nxt["count"] = nxt.get("count", 0) + (v if isinstance(v, (int, float)) and not isinstance(v, bool) else 1)
                elif name == "sum":
                    nxt["sum"] = nxt.get("sum", 0) + (v if isinstance(v, (int, float)) and not isinstance(v, bool) else 0)
                elif name == "min":
                    if isinstance(v, (int, float)) and not isinstance(v, bool):
                        cur = nxt.get("min")
                        nxt["min"] = v if cur is None else min(cur, v)
                elif name == "max":
                    if isinstance(v, (int, float)) and not isinstance(v, bool):
                        cur = nxt.get("max")
                        nxt["max"] = v if cur is None else max(cur, v)
                elif name == "avg":
                    if isinstance(v, (int, float)) and not isinstance(v, bool):
                        nxt["__avg_sum"] = nxt.get("__avg_sum", 0) + v
                        nxt["__avg_count"] = nxt.get("__avg_count", 0) + 1
                        nxt["avg"] = nxt["__avg_sum"] / nxt["__avg_count"] if nxt["__avg_count"] > 0 else 0
                else:
                    nxt[name] = nxt.get(name, 0) + (v if isinstance(v, (int, float)) and not isinstance(v, bool) else 0)
            return nxt

        super().__init__(fn, initial)
        self.kind = "aggregate"
        self.fields = [name for name, _ in fields]
        self.config = {"fields": list(self.fields)}


RESERVED_AGGREGATE_FIELDS = list(_RESERVED)
