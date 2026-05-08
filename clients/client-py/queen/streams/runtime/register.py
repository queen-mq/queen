"""
Query registration helper. 1:1 port of register.js.
"""

from __future__ import annotations

from typing import Optional

from .streams_http_client import StreamsHttpError


class StreamsConfigHashMismatch(Exception):
    code = "STREAMS_CONFIG_HASH_MISMATCH"


class StreamsRegisterFailed(Exception):
    code = "STREAMS_REGISTER_FAILED"


async def register_query(
    http,
    name: str,
    source_queue: str,
    config_hash: str,
    sink_queue: Optional[str] = None,
    reset: bool = False,
) -> dict:
    body = {
        "name": name,
        "source_queue": source_queue,
        "config_hash": config_hash,
        "reset": bool(reset),
    }
    if sink_queue:
        body["sink_queue"] = sink_queue

    try:
        res = await http.post("/streams/v1/queries", body)
    except StreamsHttpError as ex:
        if ex.status == 409 and ex.body:
            inner = (
                ex.body.get("error") if isinstance(ex.body, dict) and ex.body.get("error") else "config_hash mismatch"
            )
            hint = (
                "\n\nHint: pass reset=True to Stream.run(...) to wipe the existing "
                "queen_streams.state for this query_id, or pick a new query_id."
            )
            raise StreamsConfigHashMismatch(inner + hint) from ex
        raise

    if not res or res.get("success") is False:
        msg = res.get("error") if isinstance(res, dict) and res.get("error") else "register failed"
        raise StreamsRegisterFailed(msg)

    return {
        "queryId": res.get("query_id"),
        "name": res.get("name"),
        "fresh": bool(res.get("fresh")),
        "didReset": bool(res.get("reset")),
    }
