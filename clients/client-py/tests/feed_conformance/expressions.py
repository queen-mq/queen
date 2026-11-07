"""
The expression language of the feed conformance kit (clients/conformance/feed
README, section 3), evaluated in Python. Port of the evaluator in
clients/conformance/feed/runners/js.mjs: MISSING is the SDK's UNDEFINED.
"""

from __future__ import annotations

import math
from typing import Any, Callable, Dict, List

from queen.streams.util.js import (
    UNDEFINED,
    is_number,
    js_sort_key,
    js_str,
    js_truthy,
    utf16_len,
)


class Error(Exception):
    """An expression's own error (``$throw``, ``$flaky``): its JS name is 'Error'."""

    js_name = "Error"


_flaky: Dict[str, int] = {}


def _deep_eq(a: Any, b: Any) -> bool:
    if a is UNDEFINED or b is UNDEFINED:
        return a is b
    if a is None or b is None or not isinstance(a, (dict, list)) or not isinstance(b, (dict, list)):
        return _strict_eq(a, b)
    if isinstance(a, list) != isinstance(b, list):
        return False
    if isinstance(a, list):
        return len(a) == len(b) and all(_deep_eq(x, y) for x, y in zip(a, b))
    ka = [k for k, v in a.items() if v is not UNDEFINED]
    kb = [k for k, v in b.items() if v is not UNDEFINED]
    return len(ka) == len(kb) and all(k in b and _deep_eq(a[k], b[k]) for k in ka)


def _strict_eq(a: Any, b: Any) -> bool:
    # `a === b` on primitives (objects only equal themselves).
    if isinstance(a, bool) or isinstance(b, bool):
        return a is b
    if is_number(a) and is_number(b):
        return a == b
    if isinstance(a, str) and isinstance(b, str):
        return a == b
    if a is None and b is None:
        return True
    return a is b


def _js_number(seg: str) -> Any:
    s = seg.strip()
    if s == "":
        return 0
    try:
        f = float(s)
    except ValueError:
        return math.nan
    return int(f) if f.is_integer() else f


def _get_path(args: List[Any], path: Any) -> Any:
    segs = js_str(path).split(".")
    i0 = _js_number(segs[0])
    cur = args[i0] if isinstance(i0, int) and 0 <= i0 < len(args) else UNDEFINED
    for seg in segs[1:]:
        if cur is None or cur is UNDEFINED or not isinstance(cur, (dict, list)):
            return UNDEFINED
        if isinstance(cur, list):
            n = _js_number(seg)
            cur = cur[n] if isinstance(n, int) and 0 <= n < len(cur) else UNDEFINED
        else:
            cur = cur.get(seg, UNDEFINED)
    return cur


def _cmp(a: Any, b: Any) -> Any:
    if is_number(a) and is_number(b):
        return -1 if a < b else 1 if a > b else 0
    if isinstance(a, str) and isinstance(b, str):
        ka, kb = js_sort_key(a), js_sort_key(b)
        return -1 if ka < kb else 1 if ka > kb else 0
    return None


def _to_number(v: Any) -> Any:
    if is_number(v):
        return v
    if v is True:
        return 1
    if v is False or v is None:
        return 0
    if isinstance(v, str):
        return _js_number(v)
    return math.nan


def _js_mod(a: Any, b: Any) -> Any:
    x, y = _to_number(a), _to_number(b)
    if isinstance(x, int) and isinstance(y, int):
        if y == 0:
            return math.nan
        r = abs(x) % abs(y)
        return -r if x < 0 else r
    try:
        return math.fmod(x, y)
    except (ValueError, ZeroDivisionError):
        return math.nan


def evaluate(expr: Any, args: List[Any]) -> Any:
    if expr is None or not isinstance(expr, (dict, list)):
        return expr
    if isinstance(expr, list):
        out = []
        for e in expr:
            v = evaluate(e, args)
            out.append(None if v is UNDEFINED else v)
        return out
    keys = list(expr.keys())
    op = next((k for k in keys if isinstance(k, str) and k.startswith("$")), None)
    if op is None:
        res = {}
        for k in keys:
            v = evaluate(expr[k], args)
            if v is not UNDEFINED:
                res[k] = v
        return res
    if len(keys) != 1:
        raise Error(f"expression: {op} stands alone in its object")
    a = expr[op]

    def ev(e: Any) -> Any:
        return evaluate(e, args)

    if op == "$get":
        return _get_path(args, a)
    if op == "$eq":
        return _deep_eq(ev(a[0]), ev(a[1]))
    if op == "$ne":
        return not _deep_eq(ev(a[0]), ev(a[1]))
    if op in ("$lt", "$lte", "$gt", "$gte"):
        c = _cmp(ev(a[0]), ev(a[1]))
        if c is None:
            return False
        return {"$lt": c < 0, "$lte": c <= 0, "$gt": c > 0, "$gte": c >= 0}[op]
    if op == "$and":
        for e in a:
            if not js_truthy(ev(e)):
                return False
        return True
    if op == "$or":
        for e in a:
            if js_truthy(ev(e)):
                return True
        return False
    if op == "$not":
        return not js_truthy(ev(a))
    if op == "$if":
        return ev(a[1]) if js_truthy(ev(a[0])) else ev(a[2])
    if op == "$add":
        s: Any = 0
        for e in a:
            v = ev(e)
            if is_number(v):
                s = s + v
        return s
    if op == "$mod":
        return _js_mod(ev(a[0]), ev(a[1]))
    if op == "$str":
        return js_str(ev(a))
    if op == "$concat":
        parts = []
        for e in a:
            v = ev(e)
            parts.append("" if v is UNDEFINED else js_str(v))
        return "".join(parts)
    if op == "$len":
        v = ev(a)
        if isinstance(v, list):
            return len(v)
        if isinstance(v, str):
            return utf16_len(v)
        return 0
    if op == "$keys":
        v = ev(a)
        if isinstance(v, dict):
            return sorted((k for k, x in v.items() if x is not UNDEFINED), key=js_sort_key)
        return []
    if op == "$missing":
        return UNDEFINED
    if op == "$throw":
        raise Error(js_str(a))
    if op == "$flaky":
        key = js_str(ev(a.get("key", UNDEFINED)))
        n = _flaky.get(key, 0)
        if n < a.get("times", 0):
            _flaky[key] = n + 1
            raise Error(f"flaky {key} {n + 1}")
        return ev(a.get("then", UNDEFINED))
    raise Error(f"expression: unknown operator {op}")


def fn(expr: Any) -> Callable[..., Any]:
    """A function of any number of arguments that answers the expression."""

    def run(*args: Any) -> Any:
        return evaluate(expr, list(args))

    return run
