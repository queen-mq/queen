"""
Build and submit a /streams/v1/cycle commit. 1:1 port of cycle.js.
"""

from __future__ import annotations

from typing import Any, Optional


class StreamsCycleFailed(Exception):
    code = "STREAMS_CYCLE_FAILED"

    def __init__(self, message: str, body: Any = None):
        super().__init__(message)
        self.body = body


async def commit_cycle(
    http,
    query_id: str,
    partition_id: str,
    consumer_group: str,
    state_ops: Optional[list] = None,
    push_items: Optional[list] = None,
    ack: Any = None,
    release_lease: bool = True,
) -> dict:
    body = {
        "query_id": query_id,
        "partition_id": partition_id,
        "consumer_group": consumer_group,
        "state_ops": state_ops or [],
        "push_items": push_items or [],
        "ack": ack,
        # Default true preserves the historical "atomic full-batch" cycle.
        # Pass False from gate operators that want to keep the source lease
        # alive so the un-acked tail of the batch is redelivered in order.
        "release_lease": False if release_lease is False else True,
    }
    res = await http.post("/streams/v1/cycle", body)
    if not res or res.get("success") is False:
        msg = res.get("error") if isinstance(res, dict) and res.get("error") else "cycle commit failed"
        raise StreamsCycleFailed(msg, body=res)
    return res
