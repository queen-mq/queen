"""
State-read helper. Calls /streams/v1/state/get. 1:1 port of state.js.
"""

from __future__ import annotations

from typing import Iterable, Optional


class StreamsStateGetFailed(Exception):
    code = "STREAMS_STATE_GET_FAILED"


async def get_state(http, query_id: str, partition_id: str, keys: Optional[Iterable[str]] = None) -> dict:
    body = {
        "query_id": query_id,
        "partition_id": partition_id,
        "keys": list(keys) if keys else [],
    }
    res = await http.post("/streams/v1/state/get", body)
    if not res or res.get("success") is False:
        msg = res.get("error") if isinstance(res, dict) and res.get("error") else "state.get failed"
        raise StreamsStateGetFailed(msg)

    out: dict = {}
    for row in res.get("rows", []) or []:
        if isinstance(row, dict) and isinstance(row.get("key"), str):
            out[row["key"]] = row.get("value")
    return out
