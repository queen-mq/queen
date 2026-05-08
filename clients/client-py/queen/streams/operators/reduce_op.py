"""
ReduceOperator — fold envelopes within a window into a single value.

1:1 port of ReduceOperator.js.
"""

from __future__ import annotations

import copy
import inspect
from datetime import datetime, timezone
from typing import Any, Callable, Optional


class ReduceOperator:
    kind = "reduce"

    def __init__(self, fn: Callable, initial: Any = None):
        self.fn = fn
        self.initial = initial
        self.config: dict = {"hasInitial": initial is not None}

    async def run(
        self,
        envelopes: list,
        loaded_state: dict,
        stream_time_ms: Optional[int] = None,
        operator_tag: str = "",
        grace_period_ms: int = 0,
        ignore_unowned: bool = True,
    ) -> dict:
        accumulators: dict = {}

        # Seed accumulators with every loaded state row owned by THIS operator.
        for state_key, value in loaded_state.items():
            parts = parse_state_key(state_key)
            if parts is None:
                continue
            if parts["operatorTag"].startswith("__"):
                continue
            if ignore_unowned and operator_tag and parts["operatorTag"] != operator_tag:
                continue

            ws = (
                value.get("windowStart")
                if isinstance(value, dict) and isinstance(value.get("windowStart"), (int, float))
                else _parse_iso_ms(parts["windowKey"]) or 0
            )
            we = (
                value.get("windowEnd")
                if isinstance(value, dict) and isinstance(value.get("windowEnd"), (int, float))
                else ws
            )
            acc = (
                value.get("acc")
                if isinstance(value, dict) and value.get("acc") is not None
                else value
            )
            accumulators[state_key] = {
                "key": parts["userKey"],
                "windowStart": ws,
                "windowEnd": we,
                "windowKey": parts["windowKey"],
                "acc": acc,
                "touched": False,
                "seeded": True,
            }

        # Apply this batch's envelopes.
        batch_max_window_start = float("-inf")
        for env in envelopes:
            tag = env.get("operatorTag") or operator_tag or ""
            sk = state_key_for(tag, env.get("windowKey"), env.get("key"))
            entry = accumulators.get(sk)
            if entry is None:
                entry = {
                    "key": env.get("key"),
                    "windowStart": env.get("windowStart"),
                    "windowEnd": env.get("windowEnd"),
                    "windowKey": env.get("windowKey"),
                    "acc": self._initial(),
                    "touched": False,
                    "seeded": False,
                }
                accumulators[sk] = entry
            value_arg = env.get("value") if env.get("value") is not None else env.get("msg")
            new_acc = self.fn(entry["acc"], value_arg)
            if inspect.isawaitable(new_acc):
                new_acc = await new_acc
            entry["acc"] = new_acc
            entry["touched"] = True
            if env.get("windowStart", float("-inf")) > batch_max_window_start:
                batch_max_window_start = env["windowStart"]

        effective_stream_time = (
            stream_time_ms
            if isinstance(stream_time_ms, (int, float)) and stream_time_ms > float("-inf")
            else batch_max_window_start
        )

        state_ops: list = []
        emits: list = []
        for sk, entry in accumulators.items():
            closed = (entry["windowEnd"] + grace_period_ms) <= effective_stream_time
            if closed:
                emits.append({
                    "key": entry["key"],
                    "windowStart": entry["windowStart"],
                    "windowEnd": entry["windowEnd"],
                    "windowKey": entry["windowKey"],
                    "value": entry["acc"],
                })
                if entry["seeded"]:
                    state_ops.append({"type": "delete", "key": sk})
            elif entry["touched"]:
                state_ops.append({
                    "type": "upsert",
                    "key": sk,
                    "value": {
                        "acc": entry["acc"],
                        "windowStart": entry["windowStart"],
                        "windowEnd": entry["windowEnd"],
                    },
                })
        return {"stateOps": state_ops, "emits": emits}

    def _initial(self) -> Any:
        if self.initial is None:
            return None
        if isinstance(self.initial, (int, float, str, bool)):
            return self.initial
        # Deep clone so each window starts fresh.
        return copy.deepcopy(self.initial)


_SEP = "\u001f"


def state_key_for(operator_tag: str, window_key: str, user_key: Optional[str] = None) -> str:
    """
    Build a state key for a windowed reducer.

    Shape (v0.2+): ``{operatorTag}\\u001f{windowKey}\\u001f{userKey}``

    Reserved key prefix: state keys starting with "__" are internal.
    """
    if user_key is None:
        return f"{operator_tag}{_SEP}{window_key}"
    return f"{operator_tag}{_SEP}{window_key}{_SEP}{user_key}"


def parse_state_key(state_key: Any) -> Optional[dict]:
    if not isinstance(state_key, str):
        return None
    parts = state_key.split(_SEP)
    if len(parts) == 3:
        return {"operatorTag": parts[0], "windowKey": parts[1], "userKey": parts[2]}
    if len(parts) == 2:
        # Legacy v0.1 shape.
        return {"operatorTag": "", "windowKey": parts[0], "userKey": parts[1]}
    return None


def _parse_iso_ms(s: str) -> Optional[int]:
    try:
        s2 = s.strip()
        if s2.endswith("Z"):
            s2 = s2[:-1] + "+00:00"
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None
