"""
Scripted plan server for the kv/timers wire tests -- no broker, no sockets.

Same seam as tests/http_unit/test_retry_429.py (``HttpClient(transport=...)``,
which ``Queen(transport=...)`` forwards), so these tests exercise the REAL
client all the way down to the bytes it would put on the wire, and stop one
layer above the socket.

Why the wire body is what gets asserted, and not the return value: the JSON
object the client builds IS the contract towards the broker
(PLAN_KV_TIMERS.md §6.3, §8.2, §10.4). A wrong shape does not raise anywhere --
the broker either answers a named 400, or, in the case the plan calls out for
Go, commits a transaction whose gate silently never existed. The only thing
that catches that before production is an assertion on the exact body.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Union
from urllib.parse import parse_qs

import httpx


@dataclass
class Recorded:
    """One request as the server would have received it."""

    method: str
    path: str
    query: Dict[str, List[str]] = field(default_factory=dict)
    body: Any = None
    headers: Dict[str, str] = field(default_factory=dict)

    @property
    def route(self) -> str:
        """``"POST /api/v1/kv"`` -- method and path in one assertable string."""
        return f"{self.method} {self.path}"


# A descriptor is {"status": int, "json": Any} or a callable taking the parsed
# request body and returning one.
Descriptor = Union[Dict[str, Any], Callable[[Recorded], Dict[str, Any]]]


def _echo_kv_results(rec: Recorded) -> Dict[str, Any]:
    """Index-aligned stand-in for kv_apply_v1's return (§6.4)."""
    ops = _ops_of(rec.body)
    return {
        "status": 200,
        "json": {
            "results": [
                {
                    "index": i,
                    "op": o.get("op"),
                    "key": o.get("key"),
                    "applied": True,
                    "value": o.get("value"),
                    "version": 100 + i,
                }
                for i, o in enumerate(ops)
            ]
        },
    }


def _echo_timer_results(rec: Recorded) -> Dict[str, Any]:
    ops = _ops_of(rec.body)
    return {
        "status": 200,
        "json": {
            "results": [
                {
                    "ok": True,
                    "status": "scheduled",
                    "queue": o.get("queue"),
                    "timerKey": o.get("timerKey"),
                    "txn": o.get("txn"),
                    "messageId": f"mid-{i}",
                    "deliverAt": "2026-08-17T00:00:00.000000Z",
                }
                for i, o in enumerate(ops)
            ]
        },
    }


def _ops_of(body: Any) -> List[Dict[str, Any]]:
    if isinstance(body, list):
        return body
    if isinstance(body, dict) and isinstance(body.get("operations"), list):
        return body["operations"]
    return []


def _default_for(rec: Recorded) -> Dict[str, Any]:
    if rec.path == "/api/v1/kv" and rec.method == "POST":
        return _echo_kv_results(rec)
    if rec.path == "/api/v1/timers" and rec.method == "POST":
        return _echo_timer_results(rec)
    if rec.path.startswith("/api/v1/timers/") and rec.method == "DELETE":
        return {"status": 200, "json": {"ok": True, "status": "cancelled", "txn": None}}
    if rec.path.startswith("/api/v1/timers/") and rec.method == "GET":
        return {"status": 200, "json": {"found": False}}
    if rec.path == "/api/v1/transaction":
        return {"status": 200, "json": {"transactionId": "txn-1", "success": True, "results": []}}
    return {"status": 200, "json": {"ok": True}}


class PlanServer(httpx.AsyncBaseTransport):
    """Serves `plan` descriptors in request order, then falls back to a
    shape-appropriate default. Records every request verbatim."""

    def __init__(self, *plan: Descriptor, default: Optional[Descriptor] = None):
        self.plan: List[Descriptor] = list(plan)
        self.default = default
        self.requests: List[Recorded] = []
        self._index = 0

    # -- assertions helpers --------------------------------------------------

    @property
    def last(self) -> Recorded:
        assert self.requests, "no request was made"
        return self.requests[-1]

    @property
    def only(self) -> Recorded:
        assert len(self.requests) == 1, f"expected exactly 1 request, got {len(self.requests)}"
        return self.requests[0]

    # -- transport -----------------------------------------------------------

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        raw = request.content
        try:
            body = json.loads(raw) if raw else None
        except Exception:
            body = raw.decode("utf-8", "replace")

        rec = Recorded(
            method=request.method,
            path=request.url.path,
            query=parse_qs(request.url.query.decode() if isinstance(request.url.query, bytes) else request.url.query),
            body=body,
            headers={k.lower(): v for k, v in request.headers.items()},
        )
        self.requests.append(rec)

        desc: Descriptor
        if self._index < len(self.plan):
            desc = self.plan[self._index]
        elif self.default is not None:
            desc = self.default
        else:
            desc = _default_for(rec)
        self._index += 1

        if callable(desc):
            desc = desc(rec)

        headers = dict(desc.get("headers") or {})
        if desc.get("retry_after") is not None:
            headers["Retry-After"] = str(desc["retry_after"])
        return httpx.Response(desc.get("status", 200), headers=headers, json=desc.get("json", {"ok": True}))


def kv_results(*results: Dict[str, Any]) -> Dict[str, Any]:
    """A canned ``POST /api/v1/kv`` answer."""
    return {"status": 200, "json": {"results": list(results)}}


def error_body(status: int, error: str, reason: Optional[str] = None, detail: Optional[str] = None) -> Dict[str, Any]:
    """The broker's kv/timer error envelope: the stable identifier lives in
    ``error``, never in the prose (§13.5)."""
    body: Dict[str, Any] = {"error": error}
    if reason is not None:
        body["reason"] = reason
    if detail is not None:
        body["detail"] = detail
    return {"status": status, "json": body}
