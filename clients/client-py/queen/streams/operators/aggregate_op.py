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
        self.feed_config = {"fields": list(self.fields)}
        self._extractors = fields

    async def feed_fold(self, acc: Any, msg: Any) -> Any:
        """The aggregate's fold as the JS SDK does it (AggregateOperator.js), for a feed:
        ``(x || 0) + (typeof v === 'number' ? v : 1)`` and so on, with JS truthiness."""
        from ..util.js import UNDEFINED, call_user, is_number, js_truthy

        if isinstance(acc, dict):
            nxt = dict(acc)
        elif isinstance(acc, (list, str)):
            nxt = {str(i): x for i, x in enumerate(acc)}
        else:
            nxt = {}

        def or0(x: Any) -> Any:
            return x if js_truthy(x) else 0

        for name, ex in self._extractors:
            v = await call_user(ex, msg)
            num = is_number(v)
            if name == "count":
                nxt["count"] = _js_add(or0(nxt.get("count", UNDEFINED)), v if num else 1)
            elif name == "sum":
                nxt["sum"] = _js_add(or0(nxt.get("sum", UNDEFINED)), v if num else 0)
            elif name == "min":
                if num:
                    cur = nxt.get("min", UNDEFINED)
                    nxt["min"] = v if cur is None or cur is UNDEFINED else _js_min(cur, v)
            elif name == "max":
                if num:
                    cur = nxt.get("max", UNDEFINED)
                    nxt["max"] = v if cur is None or cur is UNDEFINED else _js_max(cur, v)
            elif name == "avg":
                if num:
                    nxt["__avg_sum"] = _js_add(or0(nxt.get("__avg_sum", UNDEFINED)), v)
                    nxt["__avg_count"] = _js_add(or0(nxt.get("__avg_count", UNDEFINED)), 1)
                    cnt = nxt["__avg_count"]
                    nxt["avg"] = (nxt["__avg_sum"] / cnt) if is_number(cnt) and cnt > 0 else 0
            else:
                nxt[name] = _js_add(or0(nxt.get(name, UNDEFINED)), v if num else 0)
        return nxt


def _to_number(x: Any) -> Any:
    from ..util.js import UNDEFINED, is_number

    if is_number(x):
        return x
    if x is True:
        return 1
    if x is False or x is None:
        return 0
    if x is UNDEFINED:
        return float("nan")
    if isinstance(x, str):
        t = x.strip()
        if t == "":
            return 0
        try:
            return float(t) if any(c in t for c in ".eE") or t in ("Infinity", "-Infinity") else int(t)
        except ValueError:
            return float("nan")
    return float("nan")


def _js_add(a: Any, b: Any) -> Any:
    """``a + b`` in JS for the values a fold meets: numbers add, a string concatenates."""
    from ..util.js import is_number, js_str

    if is_number(a) and is_number(b):
        return a + b
    if isinstance(a, str) or isinstance(b, str) or isinstance(a, (dict, list)) or isinstance(b, (dict, list)):
        return js_str(a) + js_str(b)
    return _to_number(a) + _to_number(b)


def _js_min(a: Any, b: Any) -> Any:
    x, y = _to_number(a), _to_number(b)
    if x != x or y != y:
        return float("nan")
    return x if x <= y else y


def _js_max(a: Any, b: Any) -> Any:
    x, y = _to_number(a), _to_number(b)
    if x != x or y != y:
        return float("nan")
    return x if x >= y else y


RESERVED_AGGREGATE_FIELDS = list(_RESERVED)
