"""
WindowSessionOperator — per-key activity-based windows. 1:1 port.
"""

from __future__ import annotations

import inspect
import time as _time
from typing import Any, Callable, Optional

from ._window_common import (
    MS_PER_SEC,
    extract_event_time,
    normalise_late_opts,
    to_iso,
)


_OPEN_SEPARATOR = "\u001f"


class WindowSessionOperator:
    kind = "window"
    window_kind = "session"

    def __init__(
        self,
        gap: float,
        grace_period: float = 0,
        idle_flush_ms: int = 1000,
        event_time: Optional[Callable[[Any], Any]] = None,
        allowed_lateness: float = 0,
        on_late: Optional[str] = None,
    ):
        if not isinstance(gap, (int, float)) or gap <= 0:
            raise ValueError("window_session requires gap=<positive number>")
        self.gap_ms = int(gap * MS_PER_SEC)
        self.grace_period_ms = max(0, int(grace_period * MS_PER_SEC))
        self.idle_flush_ms = max(0, int(idle_flush_ms))
        self.event_time_fn = event_time if callable(event_time) else None
        self.allowed_lateness_ms = max(0, int(allowed_lateness * MS_PER_SEC))
        self.on_late_policy = normalise_late_opts(on_late)
        self.operator_tag = f"sess:{gap}"
        self.config = {
            "kind": "window-session",
            "gap": gap,
            "gracePeriod": grace_period,
            "idleFlushMs": self.idle_flush_ms,
            "eventTime": bool(self.event_time_fn),
            "allowedLateness": allowed_lateness,
            "onLate": self.on_late_policy,
        }

    def open_session_state_key(self, user_key: str) -> str:
        return f"{self.operator_tag}{_OPEN_SEPARATOR}open{_OPEN_SEPARATOR}{user_key}"

    async def apply(self, envelope: dict) -> list[dict]:
        ts = await extract_event_time(envelope.get("msg"), self.event_time_fn)
        if ts is None:
            raise RuntimeError("window_session could not determine timestamp for message")
        return [{
            **envelope,
            "eventTimeMs": ts,
            "operatorTag": self.operator_tag,
            "gracePeriodMs": self.grace_period_ms,
        }]

    async def run_session(
        self,
        envelopes: list,
        loaded_state: dict,
        reducer_fn: Callable,
        reducer_initial: Callable[[], Any],
        now_ms: Optional[int] = None,
    ) -> dict:
        sessions: dict = {}

        # Seed from PG state.
        for state_key, value in loaded_state.items():
            parts = state_key.split(_OPEN_SEPARATOR)
            if len(parts) != 3 or parts[0] != self.operator_tag or parts[1] != "open":
                continue
            user_key = parts[2]
            sessions[user_key] = {
                "acc": value.get("acc") if isinstance(value, dict) and value.get("acc") is not None else reducer_initial(),
                "sessionStart": value.get("sessionStart") if isinstance(value.get("sessionStart"), (int, float)) else None,
                "lastEventTime": value.get("lastEventTime") if isinstance(value.get("lastEventTime"), (int, float)) else None,
                "dirty": False,
                "seeded": True,
                "closed": False,
            }

        closed_emits: list = []

        for env in envelopes:
            user_key = env.get("key")
            ts = env.get("eventTimeMs")
            s = sessions.get(user_key)

            if s is None or s.get("lastEventTime") is None:
                s = {
                    "acc": reducer_initial(),
                    "sessionStart": ts,
                    "lastEventTime": ts,
                    "dirty": True,
                    "seeded": s.get("seeded", False) if s else False,
                    "closed": False,
                }
                sessions[user_key] = s
            elif s["lastEventTime"] + self.gap_ms >= ts:
                # Extends prior session.
                if ts >= s["lastEventTime"]:
                    s["lastEventTime"] = ts
                s["dirty"] = True
            else:
                # Gap exceeded: close prior, start new.
                closed_emits.append(self._build_emit(user_key, s))
                s = {
                    "acc": reducer_initial(),
                    "sessionStart": ts,
                    "lastEventTime": ts,
                    "dirty": True,
                    "seeded": False,
                    "closed": False,
                }
                sessions[user_key] = s

            value_arg = env.get("value") if env.get("value") is not None else env.get("msg")
            new_acc = reducer_fn(s["acc"], value_arg)
            if inspect.isawaitable(new_acc):
                new_acc = await new_acc
            s["acc"] = new_acc

        # Idle-flush sweep.
        flush_reference = now_ms if isinstance(now_ms, (int, float)) else int(_time.time() * 1000)
        for user_key, s in sessions.items():
            if s.get("lastEventTime") is None:
                continue
            session_end = s["lastEventTime"] + self.gap_ms
            if session_end + self.grace_period_ms <= flush_reference:
                closed_emits.append(self._build_emit(user_key, s))
                s["closed"] = True

        # Build state ops.
        state_ops: list = []
        for user_key, s in sessions.items():
            state_key = self.open_session_state_key(user_key)
            if s.get("closed"):
                if s.get("seeded"):
                    state_ops.append({"type": "delete", "key": state_key})
            elif s.get("dirty"):
                state_ops.append({
                    "type": "upsert",
                    "key": state_key,
                    "value": {
                        "acc": s["acc"],
                        "sessionStart": s["sessionStart"],
                        "lastEventTime": s["lastEventTime"],
                    },
                })

        return {"stateOps": state_ops, "emits": closed_emits}

    def _build_emit(self, user_key: str, s: dict) -> dict:
        return {
            "key": user_key,
            "windowStart": s["sessionStart"],
            "windowEnd": s["lastEventTime"] + self.gap_ms,
            "windowKey": to_iso(s["sessionStart"]),
            "value": s["acc"],
        }
