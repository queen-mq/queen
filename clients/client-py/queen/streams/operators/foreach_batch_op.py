"""
ForeachBatchOperator — terminal at-least-once side effect, in batches
(``Stream.from_feed`` only). Port of the JS SDK's ForeachBatchOperator.js.

The runner keeps the results that reach the end of the chain and calls
``fn(items)`` with up to ``max_items`` of them: at the end of each read (by
default), or, with ``max_wait_millis``, once the batch is full or its first
item has waited that long. The position moves only after ``fn`` returns, in
the commit right after it: a crash before that delivers the batch again,
with the same ids.

User-fn signature: ``(items) -> None`` (or a coroutine), each item a dict
``{"value", "ctx", "id"}``: the value and context ``.foreach()`` would get,
and an id that is the same on every delivery of that result (a key for an
upsert).

The options come as keywords or a mapping, in the Python names
(``max_items``, ``max_wait_millis``) or the JS ones (``maxItems``,
``maxWaitMillis``); None counts as not given.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Mapping, Optional, Tuple

from ..util.js import js_is_integer
from ._feed_opts import jstr

DEFAULT_MAX_ITEMS = 1000

# Each spelling of an option -> its Python name.
_NAMES = {
    "max_items": "max_items",
    "maxItems": "max_items",
    "max_wait_millis": "max_wait_millis",
    "maxWaitMillis": "max_wait_millis",
}

_WHAT = ".foreach_batch(fn, options)"


class ForeachBatchOperator:
    kind = "foreachBatch"

    def __init__(self, fn: Callable[..., Any], opts: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> None:
        if not callable(fn):
            raise ValueError(f"{_WHAT}: fn is a function (items) => ..., each item {{ value, ctx, id }}")
        if opts is not None and not isinstance(opts, Mapping):
            raise ValueError(f"{_WHAT}: options is a mapping {{ max_items, max_wait_millis }}")
        # The Python name -> (the spelling the caller used, the value).
        given: Dict[str, Tuple[str, Any]] = {}
        for src in (opts or {}), kwargs:
            for k, v in src.items():
                name = _NAMES.get(k) if isinstance(k, str) else None
                if name is None:
                    raise ValueError(f"{_WHAT}: unknown option {jstr(k)} (max_items, max_wait_millis)")
                given[name] = (k, v)

        def option(name: str, default: int) -> Tuple[str, Any, Any]:
            spelled, raw = given.get(name, (name, None))
            return spelled, default if raw is None else raw, raw

        spelled, max_items, raw = option("max_items", DEFAULT_MAX_ITEMS)
        if not js_is_integer(max_items) or max_items < 1:
            raise ValueError(f"{_WHAT}: {spelled} is a whole number from 1, got {jstr(raw)}")
        spelled, max_wait_millis, raw = option("max_wait_millis", 0)
        if not js_is_integer(max_wait_millis) or max_wait_millis < 0:
            raise ValueError(f"{_WHAT}: {spelled} is a whole number from 0, got {jstr(raw)}")
        self.fn = fn
        self.max_items = int(max_items)
        self.max_wait_millis = int(max_wait_millis)
        self.config: dict = {"kind": "foreachBatch", "maxItems": self.max_items, "maxWaitMillis": self.max_wait_millis}
