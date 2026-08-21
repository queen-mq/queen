"""
The ephemeral vocabulary these tests answer with, plus the client factory.

The scripted server itself is kv_unit's -- an ``httpx`` transport driven by a
canned list of descriptors, recording method, path, query, raw bytes and parsed
body for every request. It is IMPORTED rather than copied for the reason the JS
suite gives for the same choice: two plan servers drift, and the day one of them
stops recording ``raw`` is the day a byte-identity pin silently becomes a shape
check.

What is local here is the vocabulary. The ephemeral routes have their own
envelopes (§3.1), and a test that spells ``{"queue": …, "messages": […]}`` out by
hand at every call site hides the wire behind its own noise.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from queen import Queen

from ..kv_unit.plan_server import PlanServer, Recorded

__all__ = [
    "PlanServer",
    "Recorded",
    "make",
    "pushed",
    "popped",
    "frame",
    "acked",
    "OLD_BROKER",
    "OLD_PROXY",
    "QUEUE_NOT_FOUND",
    "until",
]


def make(plan: Optional[List[Any]] = None, **client_options: Any):
    """A real Queen client wired to a scripted transport. Close it in the test."""
    server = PlanServer(*(plan or []))
    client = Queen(
        url="http://plan.local", transport=server, retry_attempts=1, **client_options
    )
    return client, server


def pushed(count: int) -> Dict[str, Any]:
    """``POST /api/v1/ephemeral/push`` -> 201 ``{pushed}`` (all-or-nothing)."""
    return {"status": 201, "json": {"pushed": count}}


def popped(queue: str, messages: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """``GET /api/v1/ephemeral/pop`` -> 200 ``{queue, messages}``; empty on timeout."""
    return {"status": 200, "json": {"queue": queue, "messages": messages or []}}


def frame(n: int = 1, **extra: Any) -> Dict[str, Any]:
    """One delivered frame, shaped like a real ephemeral pop element (§3.1)."""
    return {
        "id": f"e:beef:Default:{n}",
        "partition": "Default",
        "payload": {"n": n},
        "attempts": 0,
        **extra,
    }


def acked(*results: Dict[str, Any]) -> Dict[str, Any]:
    """``POST /api/v1/ephemeral/ack`` -> 200 ``{results:[{id, outcome}]}``."""
    return {"status": 200, "json": {"results": list(results)}}


#: What a broker older than 1.1 answers on every route of this family: the
#: routes were never registered.
OLD_BROKER: Dict[str, Any] = {"status": 404, "json": {"error": "not_found"}}

#: And what an old PROXY answers, which is the same verdict wearing the proxy's
#: fail-closed vocabulary: an unknown API path is `route_blocked`.
OLD_PROXY: Dict[str, Any] = {
    "status": 404,
    "json": {"error": "route_blocked", "code": "route_blocked"},
}

#: The OTHER 404, and the reason the mapping has to read the body: a broker that
#: fully supports the family, answering `depth` about a queue that is not there.
#: Byte-identical to what handlers/ephemeral.rs writes.
QUEUE_NOT_FOUND: Dict[str, Any] = {
    "status": 404,
    "json": {
        "error": "no ephemeral queue by that name exists on this broker",
        "code": "ephemeral_queue_not_found",
    },
}


async def until(predicate, what: str, timeout_s: float = 2.0) -> None:
    """Wait for a background drain to land.

    A buffered push returns when the message is IN the buffer, so every
    assertion about what reached the wire has to wait for the drain task the
    add scheduled. Bounded, and failing with the caller's own words, because an
    unbounded wait on a regression is a hung suite instead of a red test.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.002)
    raise AssertionError(f"timed out waiting for: {what}")
