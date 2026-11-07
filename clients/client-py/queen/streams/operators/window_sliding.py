"""
WindowSlidingOperator — overlapping fixed-size windows that slide every
``slide`` seconds. 1:1 port of WindowSlidingOperator.js.
"""

from __future__ import annotations

import math
from typing import Any, Callable, Optional

from ._window_common import (
    MS_PER_SEC,
    extract_event_time,
    feed_time_options,
    ms_value,
    normalise_late_opts,
    to_iso,
    window_lateness_ms,
)


class WindowSlidingOperator:
    kind = "window"
    window_kind = "sliding"

    def __init__(
        self,
        size: float,
        slide: float,
        grace_period: float = 0,
        idle_flush_ms: int = 5000,
        event_time: Optional[Callable[[Any], Any]] = None,
        allowed_lateness: Any = 0,
        on_late: Optional[str] = None,
        idle: Any = None,
    ):
        if not isinstance(size, (int, float)) or isinstance(size, bool) or size <= 0:
            raise ValueError("window_sliding requires size=<positive number>")
        if not isinstance(slide, (int, float)) or isinstance(slide, bool) or slide <= 0:
            raise ValueError("window_sliding requires slide=<positive number>")
        if size % slide != 0:
            raise ValueError(
                f"window_sliding: size ({size}) must be an integer multiple of slide ({slide}) "
                "to keep the per-event window count finite"
            )
        self.size_ms = ms_value(size * MS_PER_SEC)
        self.slide_ms = ms_value(slide * MS_PER_SEC)
        self.windows_per_event = int(size / slide)
        self.grace_period_ms = max(0, math.floor(grace_period * MS_PER_SEC))
        self.idle_flush_ms = max(0, math.floor(idle_flush_ms))
        self.event_time_fn = event_time if callable(event_time) else None
        self.allowed_lateness_ms = window_lateness_ms(allowed_lateness, "windowSliding")
        self.on_late_policy = normalise_late_opts(on_late)
        self.operator_tag = f"slide:{size}:{slide}"
        self.config = {
            "kind": "window-sliding",
            "size": size,
            "slide": slide,
            "gracePeriod": grace_period,
            "idleFlushMs": self.idle_flush_ms,
            "eventTime": bool(self.event_time_fn),
            "allowedLateness": allowed_lateness,
            "onLate": self.on_late_policy,
        }
        feed_time_options(self, event_time, idle, "windowSliding")

    async def apply(self, envelope: dict) -> list[dict]:
        ts = await extract_event_time(envelope.get("msg"), self.event_time_fn)
        if ts is None:
            raise RuntimeError("window_sliding could not determine timestamp for message")
        latest_start = (ts // self.slide_ms) * self.slide_ms
        out = []
        for i in range(self.windows_per_event):
            window_start = latest_start - i * self.slide_ms
            if ts < window_start or ts >= window_start + self.size_ms:
                continue
            out.append({
                **envelope,
                "eventTimeMs": ts,
                "windowStart": window_start,
                "windowEnd": window_start + self.size_ms,
                "windowKey": to_iso(window_start),
                "operatorTag": self.operator_tag,
                "gracePeriodMs": self.grace_period_ms,
            })
        return out
