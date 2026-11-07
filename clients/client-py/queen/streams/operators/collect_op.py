"""
CollectOperator — ``.join(sides=...)`` and ``.gather()``: the parts of one
entity, collected until all are in or the deadline passes. Feed sources only.
See runtime/feed/collect.py for the rules. Port of CollectOperator.js.

    .join(sides={"order": lambda m: m["queue"] == "orders", "paid": "payments", "sent": "shipments"},
          within="2d", select=lambda got, key: {"order": key})

    .gather(steps=["payments", "stock", "shipping"], key=lambda m: m["data"]["orderId"],
            within="5m", on_complete=lambda got, key: {...}, on_timeout=lambda got, missing, key: {...})

    .gather(count=lambda m: m["data"]["parts"], key=..., within="10m", ...)

Sides and steps are names with a test each (a queue name stands for
``m["queue"] == name``); a message is the part of the first test it passes.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping

from ..util.js import UNDEFINED, js_number_str, parse_duration_ms
from ._feed_opts import given, jstr, optional_fn, require_fn, step_name_of
from ._window_common import step_time_options


def _parts_of(spec: Any, name: str, what: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    def add(part_name: Any, test: Any) -> None:
        if not isinstance(part_name, str) or len(part_name) == 0:
            raise ValueError(f"{what}: every {name} has a name")
        if any(p["name"] == part_name for p in out):
            raise ValueError(f"{what}: {name} {jstr(part_name)} is named twice")
        if isinstance(test, str):
            out.append({"name": part_name, "queue": test, "pred": None})
        else:
            out.append({"name": part_name, "queue": None, "pred": require_fn(test, f"{name} {part_name}", what)})

    if isinstance(spec, (list, tuple)):
        for q in spec:
            add(q, q)
    elif isinstance(spec, Mapping):
        for k, v in spec.items():
            add(k, v)
    else:
        raise ValueError(f"{what}: {name} is a list of queue names or {{ name: test }}")
    return out


class CollectOperator:
    kind = "collect"

    def __init__(self, collect_kind: str, opts: Mapping[str, Any]):
        what = collect_kind
        if not isinstance(opts, Mapping):
            raise ValueError(f"{what}: pass an options object")
        within = opts.get("within")
        if not isinstance(within, str):
            raise ValueError(f"{what}: within is a duration string, e.g. within: '15m'")
        self.collect_kind = collect_kind
        self.within_ms = parse_duration_ms(within)
        self.key_fn = optional_fn(given(opts, "key"), "key", what)
        self.keep_fn = optional_fn(given(opts, "keep"), "keep", what)
        self.on_timeout = optional_fn(given(opts, "onTimeout"), "onTimeout", what)
        repeat = given(opts, "onRepeat")
        self.on_repeat = "keep" if repeat is UNDEFINED else repeat
        if self.on_repeat not in ("keep", "latest"):
            raise ValueError(
                f"{what}: onRepeat is 'keep' (the first message of a part stays) or 'latest', got {jstr(opts.get('onRepeat'))}"
            )
        self.step_name = step_name_of(opts)
        if collect_kind == "join":
            self.parts = _parts_of(opts.get("sides"), "side", what)
            if len(self.parts) < 2:
                raise ValueError("join: sides names at least two")
            self.count_fn = None
            self.on_complete = optional_fn(given(opts, "select"), "select", what)
            self.remember = False
        else:
            has_steps = given(opts, "steps") is not UNDEFINED
            has_count = given(opts, "count") is not UNDEFINED
            if has_steps == has_count:
                raise ValueError("gather: give steps (the parts by name) or count (how many parts a run has), one of the two")
            self.parts = _parts_of(opts.get("steps"), "step", what) if has_steps else None
            if self.parts is not None and len(self.parts) == 0:
                raise ValueError("gather: steps names at least one")
            self.count_fn = require_fn(opts.get("count"), "count", what) if has_count else None
            self.on_complete = optional_fn(given(opts, "onComplete"), "onComplete", what)
            self.remember = True
        self.operator_tag = f"{what}:{js_number_str(self.within_ms)}"
        self.config = {
            "kind": what,
            "within": self.within_ms,
            "parts": [p["name"] for p in self.parts] if self.parts else None,
            "count": bool(self.count_fn),
            "onRepeat": self.on_repeat,
            "timeout": bool(self.on_timeout),
        }
        step_time_options(self, dict(opts), what)
