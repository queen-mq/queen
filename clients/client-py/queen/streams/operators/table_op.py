"""
TableOperator — ``.join_table()``: the latest row per key of a table queue,
joined into every message. Feed sources only. See runtime/feed/tables.py.
Port of TableOperator.js.

    Stream.from_feed(client, queues=["bookings"]).join_table(
        queue="property-settings",                 # one message per change of a property
        on=lambda m: m["data"]["propertyId"],      # the row a booking looks up
        select=lambda booking, settings: {**booking, "currency": (settings or {}).get("currency")},
    )

key(row) is a row's key (default: its partition), value(row) what the table
keeps (default: its payload; None deletes the row). missing: 'keep' (the
default: select gets UNDEFINED) or 'drop'. initial: 'all' (a new stream first
reads the table queue from its beginning) or 'now'.
"""

from __future__ import annotations

import re
from typing import Any, Mapping

from ..util.js import UNDEFINED
from ._feed_opts import given, jstr, step_name_of

_PATTERN = re.compile(r"[*?]")


def _require_fn(fn: Any, name: str) -> Any:
    if not callable(fn):
        raise ValueError(f"joinTable: {name} must be a function")
    return fn


def _optional_fn(fn: Any, name: str) -> Any:
    if fn is None or fn is UNDEFINED:
        return None
    return _require_fn(fn, name)


class TableOperator:
    kind = "table"

    def __init__(self, opts: Mapping[str, Any]):
        if not isinstance(opts, Mapping):
            raise ValueError("joinTable: pass an options object")
        q = opts.get("queue", UNDEFINED)
        queues = list(q) if isinstance(q, (list, tuple)) else [q]
        if not queues or any(not isinstance(x, str) or len(x) == 0 or _PATTERN.search(x) for x in queues):
            raise ValueError("joinTable: queue is the table queue's name (or a list of names, no patterns)")
        seen = []
        for x in queues:
            if x not in seen:
                seen.append(x)
        self.queues = seen
        self.on_fn = _require_fn(opts.get("on"), "on")
        self.key_fn = _optional_fn(given(opts, "key"), "key")
        self.value_fn = _optional_fn(given(opts, "value"), "value")
        self.select_fn = _optional_fn(given(opts, "select"), "select")
        missing = given(opts, "missing")
        self.missing = "keep" if missing is UNDEFINED else missing
        if self.missing not in ("keep", "drop"):
            raise ValueError(f"joinTable: missing is 'keep' or 'drop', got {jstr(opts.get('missing'))}")
        initial = given(opts, "initial")
        self.initial = "all" if initial is UNDEFINED else initial
        if self.initial not in ("all", "now"):
            raise ValueError(
                f"joinTable: initial is 'all' (read the table queue from its beginning) or 'now', got {jstr(opts.get('initial'))}"
            )
        self.step_name = step_name_of(opts)
        self.config = {"kind": "table", "queues": self.queues, "key": bool(self.key_fn), "value": bool(self.value_fn)}
