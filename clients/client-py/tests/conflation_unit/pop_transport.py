"""
A scripted pop transport -- no broker, no sockets.

Same seam as tests/kv_unit/plan_server.py and tests/http_unit/test_retry_429.py
(``HttpClient(transport=...)``, which ``Queen(transport=...)`` forwards), so
these tests drive the REAL client all the way down to the bytes it would put on
the wire and stop one layer above the socket.

Why the QUERY STRING is what gets asserted: PLAN_CONFLATION §3.1 puts
`conflation` on the query string of the pop routes, never in a body, exactly
like `subscriptionMode`. An option that never reaches the query string raises
nothing anywhere -- the broker ignores unknown query params and the consumer
silently drains the whole backlog message by message, which is precisely the
failure §4 forbids. The only thing that catches that before production is an
assertion on the emitted query.

Pop and non-pop requests are scripted separately because a consume round trip is
two calls (pop, then ack) and only the first one is the contract under test.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs

import httpx


class PopBudgetExhausted(RuntimeError):
    """The consume loop kept polling when the test expected it to stop.

    A scripted transport answers instantly and never touches a socket, so a
    consume loop that refuses to stop never yields to the event loop either --
    it starves the very timeout that was supposed to bound it, and the test
    HANGS instead of failing. A pop budget turns that into a fast, named
    failure that says exactly what went wrong.

    Deliberately worded to avoid the substrings the consumer's error triage
    matches on ("timeout", "connection", "fetch failed"): those would be
    classified as retryable and swallowed by the very loop under test.
    """



@dataclass
class Recorded:
    """One request as the broker would have received it."""

    method: str
    path: str
    query: Dict[str, List[str]] = field(default_factory=dict)
    body: Any = None
    # The query string exactly as it left the client, order included. parse_qs
    # above answers "was this parameter sent, and with what value"; this answers
    # "is this the same request the pre-feature SDK sent", which is a stronger
    # claim and the one the autopilot tests make.
    raw_query: str = ""

    @property
    def route(self) -> str:
        return f"{self.method} {self.path}"

    def param(self, name: str) -> Optional[str]:
        """First value of a query parameter, or None when it was not sent.

        None is a real assertion, not a convenience: "the parameter is absent"
        is the compatibility contract for every non-conflating consumer.
        """
        values = self.query.get(name)
        return values[0] if values else None


def message(
    transaction_id: str = "txn-1",
    partition_id: str = "part-1",
    lease_id: str = "lease-1",
    payload: Any = None,
) -> Dict[str, Any]:
    """A pop frame with the three fields the ack path treats as mandatory."""
    return {
        "transactionId": transaction_id,
        "partitionId": partition_id,
        "partition": "Default",
        "leaseId": lease_id,
        "data": payload if payload is not None else {"n": 1},
    }


def pop_body(
    messages: Optional[List[Dict[str, Any]]] = None,
    *,
    conflation: bool = False,
    conflict: bool = False,
    queue: str = "orders",
    group: str = "workers",
) -> Dict[str, Any]:
    """A pop response shaped like ``render_pop_parts`` (handlers/data.rs).

    The two conflation keys are emitted only when true, mirroring the broker: an
    old broker simply has no such keys, which is exactly the case §4's
    degrade-loudly rule has to detect.
    """
    msgs = messages or []
    body: Dict[str, Any] = {
        "success": True,
        "queue": queue,
        "partition": "Default",
        "partitionId": "part-1",
        "leaseId": "lease-1" if msgs else "",
        "consumerGroup": group,
        "messages": msgs,
        "partitionsClaimed": 1 if msgs else 0,
    }
    if conflation:
        body["conflation"] = True
    if conflict:
        body["conflationConflict"] = True
    return body


class PopTransport(httpx.AsyncBaseTransport):
    """Serves `pop_plan` bodies to pop routes in order, then `default_pop`.

    Everything that is not a pop (ack, ack/batch) gets a benign success so a
    consume loop can complete a full cycle without a broker.
    """

    def __init__(
        self,
        *pop_plan: Dict[str, Any],
        default_pop: Optional[Dict[str, Any]] = None,
        max_pops: int = 20,
    ) -> None:
        self.pop_plan: List[Dict[str, Any]] = list(pop_plan)
        self.default_pop = default_pop if default_pop is not None else pop_body()
        self.max_pops = max_pops
        self.requests: List[Recorded] = []
        self._pop_index = 0

    @property
    def pops(self) -> List[Recorded]:
        return [r for r in self.requests if "/pop" in r.path]

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        # A real transport always suspends. This one does not, so without an
        # explicit yield a consume loop runs forever inside a single task step:
        # the event loop never regains control, no timeout callback ever fires,
        # and nothing can cancel it. One sleep(0) per request restores the
        # scheduling point every other bound in these tests depends on.
        await asyncio.sleep(0)

        raw = request.content
        try:
            body = json.loads(raw) if raw else None
        except Exception:
            body = raw.decode("utf-8", "replace")

        query = request.url.query
        raw_query = query.decode() if isinstance(query, bytes) else query
        rec = Recorded(
            method=request.method,
            path=request.url.path,
            query=parse_qs(raw_query),
            body=body,
            raw_query=raw_query,
        )
        self.requests.append(rec)

        if "/pop" in rec.path:
            if self._pop_index >= self.max_pops:
                raise PopBudgetExhausted(
                    f"the consume loop issued more than {self.max_pops} pops; "
                    "it was expected to stop"
                )
            if self._pop_index < len(self.pop_plan):
                payload = self.pop_plan[self._pop_index]
            else:
                payload = self.default_pop
            self._pop_index += 1
            return httpx.Response(200, json=payload)

        return httpx.Response(200, json={"success": True})
