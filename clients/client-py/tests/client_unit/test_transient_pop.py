"""
consume() -- a pop the broker could not answer for a while is polled again.

The rule: a consumer stops only for a fault that waiting cannot fix (a 4xx the
broker answers the same way next time, a conflation it cannot apply, a stop by
its caller). What the broker, or the way to it, answers while it cannot serve
(the 503s of a leader election, a 500 from a node shutting down, a gateway's
502 or 504, a timeout, a dropped connection) is waited out, paced by
Retry-After, and the consumer goes on.

Before: only a 429 and errors whose text said "timeout" or "connection" were
retried. On 2026-10-29 the prod cluster lost its leader twice and pops were
answered 503 "the request deadline elapsed" for a few minutes: a consumer
re-raised that and stopped, and nothing restarted a process that stayed up.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any, List, Optional

import httpx
import pytest

from queen import Queen
from queen.consumer.consumer_manager import is_transient_pop_error, transient_delay_seconds

from ..conflation_unit.pop_transport import message, pop_body

CONSUME_TIMEOUT_S = 5.0


def broker_busy(code: str, error: str, retry_after: Optional[str] = "0") -> httpx.Response:
    """The broker's own answer while it has no leader (server/src/handlers/raft.rs)."""
    headers = {"Retry-After": retry_after} if retry_after is not None else {}
    return httpx.Response(503, headers=headers, json={"error": error, "code": code})


def deadline_elapsed(retry_after: str = "0") -> httpx.Response:
    return broker_busy("timeout", "the request deadline elapsed", retry_after)


def no_leader(retry_after: str = "0") -> httpx.Response:
    return broker_busy("no_leader", "no leader is known; retry after", retry_after)


def serves(*transaction_ids: str) -> httpx.Response:
    return httpx.Response(200, json=pop_body([message(t) for t in transaction_ids]))


@dataclass
class Seen:
    method: str
    path: str
    at: float
    body: Any


class ScriptedBroker(httpx.AsyncBaseTransport):
    """Answers each pop with the next step of the script: a response, or an
    exception to raise as the transport would. Once the script runs out it
    calls ``when_done`` (a test sets the consumer's stop signal there) and
    answers an empty pop. Acks are accepted.
    """

    def __init__(self, *script: Any, when_done: Any = None) -> None:
        self.script: List[Any] = list(script)
        self.when_done = when_done
        self.requests: List[Seen] = []

    @property
    def pops(self) -> List[Seen]:
        return [r for r in self.requests if "/pop" in r.path]

    @property
    def acks(self) -> List[Seen]:
        return [r for r in self.requests if r.path.startswith("/api/v1/ack")]

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0)  # a real transport suspends; see pop_transport.py
        raw = request.content
        seen = Seen(request.method, request.url.path, time.monotonic(), json.loads(raw) if raw else None)
        self.requests.append(seen)
        if "/pop" in seen.path:
            if self.script:
                step = self.script.pop(0)
                if isinstance(step, BaseException):
                    raise step
                return step
            if self.when_done is not None:
                self.when_done()
            return httpx.Response(200, json=pop_body())
        acks = seen.body.get("acknowledgments") or [seen.body]
        return httpx.Response(
            200,
            json=[
                {"index": i, "transactionId": a.get("transactionId"), "success": True, "error": None, "leaseReleased": True}
                for i, a in enumerate(acks)
            ],
        )


def settled(broker: ScriptedBroker) -> List[str]:
    out = []
    for ack in broker.acks:
        for a in ack.body.get("acknowledgments") or [ack.body]:
            out.append(f"{a['transactionId']}:{a['status']}")
    return out


async def consume_until_script_ends(*script: Any, wait: bool = True, timeout_millis: Optional[int] = None):
    """Run a consumer against a scripted broker until the script runs out."""
    stop = asyncio.Event()
    broker = ScriptedBroker(*script, when_done=stop.set)
    client = Queen(url="http://plan.local", transport=broker, retry_attempts=1)
    handled: List[str] = []

    async def handler(msg: Any) -> None:
        handled.extend(m["transactionId"] for m in (msg if isinstance(msg, list) else [msg]))

    builder = client.queue("orders").group("workers").batch(1).wait(wait)
    if timeout_millis is not None:
        builder = builder.timeout_millis(timeout_millis)
    try:
        await asyncio.wait_for(builder.consume(handler, signal=stop), CONSUME_TIMEOUT_S)
    finally:
        await client.close()
    return broker, handled


@pytest.mark.asyncio
async def test_a_leader_election_is_waited_out_and_the_next_message_handled():
    broker, handled = await consume_until_script_ends(
        deadline_elapsed(), deadline_elapsed(), deadline_elapsed(), serves("txn-1")
    )

    assert handled == ["txn-1"]
    assert settled(broker) == ["txn-1:completed"]
    # 3 refused pops, the one that brings the message, the one that ends the script.
    assert len(broker.pops) == 5


@pytest.mark.asyncio
async def test_the_next_pop_waits_the_retry_after_the_broker_sent():
    broker, handled = await consume_until_script_ends(no_leader("1"), serves("txn-1"))

    refused, served = broker.pops[0], broker.pops[1]
    gap = served.at - refused.at
    assert 0.75 <= gap < 2.0, f"polled again after {gap:.3f} s, the Retry-After was 1 s"
    assert handled == ["txn-1"]


@pytest.mark.asyncio
async def test_a_500_from_a_node_shutting_down_and_a_gateway_502_that_is_not_json():
    broker, handled = await consume_until_script_ends(
        httpx.Response(500, headers={"Retry-After": "0"}, json={"error": "planner channel closed", "code": "internal"}),
        httpx.Response(502, headers={"Retry-After": "0", "Content-Type": "text/html"}, content=b"<html>502 Bad Gateway</html>"),
        serves("txn-1"),
    )

    assert handled == ["txn-1"]


@pytest.mark.asyncio
async def test_a_connection_the_server_dropped_without_answering():
    # A node killed mid-request. Its text says neither "timeout" nor
    # "connection", so the consumer used to stop on it.
    broker, handled = await consume_until_script_ends(
        httpx.RemoteProtocolError("Server disconnected without sending a response."), serves("txn-1")
    )

    assert handled == ["txn-1"]


@pytest.mark.asyncio
async def test_wait_false_a_pop_the_client_timed_out_with_no_text():
    broker, handled = await consume_until_script_ends(
        httpx.ReadTimeout(""), serves("txn-1"), wait=False, timeout_millis=200
    )

    assert handled == ["txn-1"]


@pytest.mark.asyncio
async def test_a_stop_during_the_wait_ends_consume_at_once():
    stop = asyncio.Event()
    broker = ScriptedBroker(no_leader("30"))
    client = Queen(url="http://plan.local", transport=broker, retry_attempts=1)

    async def handler(msg: Any) -> None:
        pass

    running = asyncio.ensure_future(client.queue("orders").group("workers").consume(handler, signal=stop))
    try:
        while not broker.pops:
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.1)  # the answer is read and the consumer is waiting
        started = time.monotonic()
        stop.set()
        await asyncio.wait_for(running, 2.0)
        elapsed = time.monotonic() - started
    finally:
        running.cancel()
        await client.close()

    assert elapsed < 0.25, f"stopping took {elapsed:.3f} s, not the 30 s wait"
    assert len(broker.pops) == 1, "no pop after the stop"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status, body",
    [
        (400, {"error": "subscriptionFrom is not a timestamp", "code": "rejected"}),
        (503, {"error": "raft phase 1: the message path is routed but not yet wired", "code": "raft_phase1_unsupported"}),
    ],
    ids=["400-rejected", "503-raft_phase1_unsupported"],
)
async def test_still_stops_on_what_waiting_cannot_fix(status, body):
    broker = ScriptedBroker(httpx.Response(status, headers={"Retry-After": "1"}, json=body))
    client = Queen(url="http://plan.local", transport=broker, retry_attempts=1)

    async def handler(msg: Any) -> None:
        pass

    try:
        with pytest.raises(httpx.HTTPStatusError) as raised:
            await asyncio.wait_for(client.queue("orders").group("workers").consume(handler), CONSUME_TIMEOUT_S)
    finally:
        await client.close()

    assert raised.value.response.status_code == status
    assert raised.value.code == body["code"]
    assert len(broker.pops) == 1, "not polled again"


def _status_error(status: int, code: Optional[str] = None) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "http://plan.local/api/v1/pop/queue/orders")
    error = httpx.HTTPStatusError(f"HTTP {status}", request=request, response=httpx.Response(status, request=request))
    error.code = code  # type: ignore[attr-defined]
    return error


@pytest.mark.parametrize(
    "error",
    [
        _status_error(503, "timeout"),
        _status_error(503, "no_leader"),
        _status_error(503, "retry"),
        _status_error(503, "outcome_unknown"),
        _status_error(503, "standby"),
        _status_error(500, "internal"),
        _status_error(502),
        _status_error(504),
        _status_error(408),
        _status_error(429, "overloaded"),
        httpx.ReadTimeout(""),
        httpx.ConnectError("[Errno 61] Connection refused"),
        httpx.ReadError(""),
        httpx.RemoteProtocolError("Server disconnected without sending a response."),
    ],
    ids=lambda e: f"{type(e).__name__}-{getattr(getattr(e, 'response', None), 'status_code', '')}-{getattr(e, 'code', '')}",
)
def test_is_transient_for_what_waiting_can_fix(error):
    assert is_transient_pop_error(error) is True


@pytest.mark.parametrize(
    "error",
    [
        _status_error(400, "rejected"),
        _status_error(401),
        _status_error(403, "cluster_suspended"),
        _status_error(404),
        _status_error(413, "name_too_long"),
        _status_error(503, "raft_phase1_unsupported"),
        httpx.UnsupportedProtocol("Request URL is missing an 'http://' or 'https://' protocol."),
        RuntimeError("a bug that says timeout and connection"),
    ],
    ids=lambda e: f"{type(e).__name__}-{getattr(getattr(e, 'response', None), 'status_code', '')}-{getattr(e, 'code', '')}",
)
def test_is_not_transient_for_what_waiting_cannot_fix(error):
    assert is_transient_pop_error(error) is False


def test_the_delay_is_the_retry_after_plus_or_minus_20_percent():
    assert transient_delay_seconds(1, 1.0, lambda: 0.5) == pytest.approx(1.0)
    assert transient_delay_seconds(9, 1.0, lambda: 0.5) == pytest.approx(1.0)
    assert transient_delay_seconds(1, 1.0, lambda: 0.0) == pytest.approx(0.8)
    assert transient_delay_seconds(1, 1.0, lambda: 0.999999) == pytest.approx(1.2, abs=1e-5)


def test_without_a_retry_after_the_delay_doubles_from_1_s_up_to_30_s():
    delays = [transient_delay_seconds(n, None, lambda: 0.5) for n in (1, 2, 3, 4, 5, 6, 7, 5000)]
    assert delays == pytest.approx([1, 2, 4, 8, 16, 30, 30, 30])


def test_the_delay_is_never_more_than_30_s():
    assert transient_delay_seconds(1, 3600.0, lambda: 0.999999) == pytest.approx(30.0)
    assert transient_delay_seconds(1, 3600.0, lambda: 0.0) == pytest.approx(24.0)
