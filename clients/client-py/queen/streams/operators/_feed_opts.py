"""
Option helpers of the feed operators (pattern, join, gather, joinTable): the
options arrive as a mapping with the JS names (``onMatch``, ``onTimeout``,
``eventTime`` ...) or as Python keywords (``on_match``, ``on_timeout``,
``event_time`` ...); both mean the same.
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

from ..util.js import UNDEFINED, js_json_dumps

_SNAKE_TO_CAMEL = {
    "on_match": "onMatch",
    "on_timeout": "onTimeout",
    "on_repeat": "onRepeat",
    "on_complete": "onComplete",
    "event_time": "eventTime",
    "allowed_lateness": "allowedLateness",
    "on_late": "onLate",
}


def camel_opts(opts: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> Dict[str, Any]:
    """One mapping with the JS names, from a mapping and/or keywords."""
    out: Dict[str, Any] = {}
    for src in (opts or {}), kwargs:
        for k, v in src.items():
            out[_SNAKE_TO_CAMEL.get(k, k)] = v
    return out


def given(opts: Mapping[str, Any], name: str) -> Any:
    """The option, or UNDEFINED when absent (None counts as absent, as JS `?? ` reads null)."""
    v = opts.get(name, UNDEFINED)
    return UNDEFINED if v is None else v


def jstr(v: Any) -> str:
    out = js_json_dumps(v) if not callable(v) else UNDEFINED
    return "undefined" if out is UNDEFINED else out


def require_fn(fn: Any, name: str, what: str) -> Any:
    if not callable(fn):
        raise ValueError(f"{what}: {name} must be a function")
    return fn


def optional_fn(fn: Any, name: str, what: str) -> Any:
    if fn is None or fn is UNDEFINED:
        return None
    return require_fn(fn, name, what)


def step_name_of(opts: Mapping[str, Any]) -> Optional[str]:
    name = opts.get("name")
    return name if isinstance(name, str) and len(name) > 0 else None
