"""
PatternOperator — ``.pattern()`` and ``.join()``: two kinds of message for one
entity, in time. Feed sources only (``Stream.from_feed``), where every queue
shares one clock: the time the log committed each message. With
``eventTime`` (or after a step on event time) the deadlines run on the
messages' own times instead (step_time_options in _window_common.py).
Port of PatternOperator.js.

pattern: a ``start`` message, then a ``then`` message within ``within``.
  A ``then`` in time sends ``onMatch(start, then)``; no ``then`` in time sends
  ``onTimeout(start)`` once the clock passes the deadline ("A and no B").

join: a ``left`` and a ``right`` message, in either order, within ``within``.
  The second one sends ``select(left, right)``; one left alone sends
  ``onTimeout(message, side)`` at its deadline.

The entity is ``key(msg, value)``, else ``.key_by()``, else the partition name.
One message waits per entity (per side, for a join): a second ``start`` while
one waits is ignored (``onRepeat: 'keep'``, the default) or takes its place
(``'restart'``). ``keep(msg, value)`` keeps only what it answers as the waiting
message's ``data``. Every function may be async; ``onMatch``, ``select`` and
``onTimeout`` answer what to send on (None or UNDEFINED sends nothing).
"""

from __future__ import annotations

from typing import Any, Mapping

from ..util.js import UNDEFINED, js_number_str, parse_duration_ms
from ._feed_opts import given, jstr, optional_fn, require_fn, step_name_of
from ._window_common import step_time_options

_REPEAT = ("keep", "restart")


class PatternOperator:
    kind = "pattern"

    def __init__(self, pattern_kind: str, opts: Mapping[str, Any]):
        what = "join" if pattern_kind == "join" else "pattern"
        if not isinstance(opts, Mapping):
            raise ValueError(f"{what}: pass an options object")
        within = opts.get("within")
        if not isinstance(within, str):
            raise ValueError(f"{what}: within is a duration string, e.g. within: '15m'")
        self.pattern_kind = what
        self.within_ms = parse_duration_ms(within)
        self.key_fn = optional_fn(given(opts, "key"), "key", what)
        self.keep_fn = optional_fn(given(opts, "keep"), "keep", what)
        self.step_name = step_name_of(opts)
        self.on_timeout = optional_fn(given(opts, "onTimeout"), "onTimeout", what)
        repeat = given(opts, "onRepeat")
        self.on_repeat = "keep" if repeat is UNDEFINED else repeat
        if self.on_repeat not in _REPEAT:
            raise ValueError(f"{what}: onRepeat is 'keep' or 'restart', got {jstr(opts.get('onRepeat'))}")
        if what == "pattern":
            self.start = require_fn(given(opts, "start"), "start", what)
            self.then = require_fn(given(opts, "then"), "then", what)
            self.on_match = optional_fn(given(opts, "onMatch"), "onMatch", what)
        else:
            self.left = require_fn(given(opts, "left"), "left", what)
            self.right = require_fn(given(opts, "right"), "right", what)
            self.select = optional_fn(given(opts, "select"), "select", what)
        self.operator_tag = f"{what}:{js_number_str(self.within_ms)}"
        self.config = {
            "kind": what,
            "within": self.within_ms,
            "onRepeat": self.on_repeat,
            "timeout": bool(self.on_timeout),
        }
        step_time_options(self, dict(opts), what)
