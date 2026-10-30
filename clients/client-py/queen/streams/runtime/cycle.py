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


async def claim_partitions(
    http,
    query_id: str,
    partition_ids: list,
    consumer_group: str,
    lease_seconds: int = 30,
) -> Any:
    """
    Lease idle partitions of the query's source queue for the group, with an
    empty batch (``POST /streams/v1/claim``, broker 2.7). Each partition
    answers ``claimed`` (under the answer's ``lease_id``), ``busy``,
    ``backlog`` or ``missing``. A cycle whose ack names the lease with
    ``count: 0`` closes the partition's windows; the broker refuses it whole
    if the lease is gone. An older broker answers 404 (StreamsHttpError).
    """
    body = {
        "query_id": query_id,
        "partition_ids": [str(p) for p in partition_ids],
        "consumer_group": consumer_group,
        "lease_seconds": lease_seconds,
    }
    return await http.post("/streams/v1/claim", body)
