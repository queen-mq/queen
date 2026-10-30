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


async def list_state_partitions(
    http,
    query_id: str,
    untouched_for_ms: Optional[int] = None,
    after: Optional[str] = None,
    limit: Optional[int] = None,
) -> dict:
    """
    One page of the partitions holding the query's state
    (``POST /streams/v1/state/partitions``, broker 2.7). A runner remembers
    only the partitions it processed, so after a restart this is how it finds
    the windows nobody is closing. Pass the answer's ``next`` as ``after`` for
    the next page. An older broker answers 404 (StreamsHttpError).
    """
    body: dict = {"query_id": query_id}
    if untouched_for_ms:
        body["untouched_for_ms"] = int(untouched_for_ms)
    if after:
        body["after"] = after
    if limit:
        body["limit"] = int(limit)
    res = await http.post("/streams/v1/state/partitions", body)
    if not isinstance(res, dict) or res.get("success") is False:
        msg = res.get("error") if isinstance(res, dict) and res.get("error") else "state.partitions failed"
        raise StreamsStateGetFailed(msg)
    return res
