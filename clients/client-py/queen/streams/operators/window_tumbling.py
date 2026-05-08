"""
WindowTumblingOperator — bucket envelopes by floor(time, N).

1:1 port of WindowTumblingOperator.js.
"""

from __future__ import annotations

from typing import Any, Callable, Optional

from ._window_common import (
    MS_PER_SEC,
    extract_event_time,
    normalise_late_opts,
    to_iso,
)


class WindowTumblingOperator:
    kind = "window"
    window_kind = "tumbling"

    def __init__(
        self,
        seconds: float,
        grace_period: float = 0,
        idle_flush_ms: int = 5000,
        event_time: Optional[Callable[[Any], Any]] = None,
        allowed_lateness: float = 0,
        on_late: Optional[str] = None,
    ):
        if not isinstance(seconds, (int, float)) or seconds <= 0:
            raise ValueError("window_tumbling requires seconds=<positive number>")
        self.window_ms = int(seconds * MS_PER_SEC)
        self.grace_period_ms = max(0, int(grace_period * MS_PER_SEC))
        self.idle_flush_ms = max(0, int(idle_flush_ms))
        self.event_time_fn = event_time if callable(event_time) else None
        self.allowed_lateness_ms = max(0, int(allowed_lateness * MS_PER_SEC))
        self.on_late_policy = normalise_late_opts(on_late)
        self.operator_tag = f"tumb:{seconds}"
        self.config = {
            "kind": "window-tumbling",
            "seconds": seconds,
            "gracePeriod": grace_period,
            "idleFlushMs": self.idle_flush_ms,
            "eventTime": bool(self.event_time_fn),
            "allowedLateness": allowed_lateness,
            "onLate": self.on_late_policy,
        }

    async def apply(self, envelope: dict) -> list[dict]:
        ts = await extract_event_time(envelope.get("msg"), self.event_time_fn)
        if ts is None:
            raise RuntimeError("window_tumbling could not determine timestamp for message")
        window_start = (ts // self.window_ms) * self.window_ms
        window_end = window_start + self.window_ms
        return [{
            **envelope,
            "eventTimeMs": ts,
            "windowStart": window_start,
            "windowEnd": window_end,
            "windowKey": to_iso(window_start),
            "operatorTag": self.operator_tag,
            "gracePeriodMs": self.grace_period_ms,
        }]
