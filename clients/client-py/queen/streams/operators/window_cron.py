"""
WindowCronOperator — wall-clock-aligned tumbling windows.

1:1 port of WindowCronOperator.js. Currently supports the ``every:``
shorthand only.
"""

from __future__ import annotations

from typing import Any, Callable, Optional

from ._window_common import (
    MS_PER_SEC,
    every_to_ms,
    extract_event_time,
    normalise_late_opts,
    to_iso,
)


# Jan 1 1970 UTC was a Thursday; shift so weeks align to Monday 00:00 UTC.
_WEEK_EPOCH_OFFSET_MS = 4 * 86_400 * 1000


class WindowCronOperator:
    kind = "window"
    window_kind = "cron"

    def __init__(
        self,
        every: str,
        grace_period: float = 0,
        idle_flush_ms: int = 30_000,
        event_time: Optional[Callable[[Any], Any]] = None,
        allowed_lateness: float = 0,
        on_late: Optional[str] = None,
    ):
        if not isinstance(every, str):
            raise ValueError(
                "window_cron requires every='second'|'minute'|'hour'|'day'|'week'"
            )
        self.every = every
        self.window_ms = every_to_ms(every)
        self.grace_period_ms = max(0, int(grace_period * MS_PER_SEC))
        self.idle_flush_ms = max(0, int(idle_flush_ms))
        self.event_time_fn = event_time if callable(event_time) else None
        self.allowed_lateness_ms = max(0, int(allowed_lateness * MS_PER_SEC))
        self.on_late_policy = normalise_late_opts(on_late)
        self.operator_tag = f"cron:{every}"
        self.config = {
            "kind": "window-cron",
            "every": every,
            "gracePeriod": grace_period,
            "idleFlushMs": self.idle_flush_ms,
            "eventTime": bool(self.event_time_fn),
            "allowedLateness": allowed_lateness,
            "onLate": self.on_late_policy,
        }

    async def apply(self, envelope: dict) -> list[dict]:
        ts = await extract_event_time(envelope.get("msg"), self.event_time_fn)
        if ts is None:
            raise RuntimeError("window_cron could not determine timestamp for message")
        window_start = self._floor_to_boundary(ts)
        return [{
            **envelope,
            "eventTimeMs": ts,
            "windowStart": window_start,
            "windowEnd": window_start + self.window_ms,
            "windowKey": to_iso(window_start),
            "operatorTag": self.operator_tag,
            "gracePeriodMs": self.grace_period_ms,
        }]

    def _floor_to_boundary(self, ts: int) -> int:
        if self.every == "week":
            shifted = ts - _WEEK_EPOCH_OFFSET_MS
            week = shifted // self.window_ms
            return week * self.window_ms + _WEEK_EPOCH_OFFSET_MS
        return (ts // self.window_ms) * self.window_ms
