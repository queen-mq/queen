"""
Client-side 429/403 handling tests (PLAN_QUEEN_PROXY_CLOUD.md §4/§9,
blocker B4 -- "client 429/backoff work ... mandatory pre-enforcement").

The proxy error contract under test:
  429  Retry-After: <seconds>  {"error": "...", "code": "rate_limited" | "quota_exceeded"}
  403                          {"error": "...", "code": "cluster_suspended" | "storage_quota_exceeded"
                                                        | "feature_gated" | "forbidden"}

These run against httpx.MockTransport (no broker, no real sockets, no port
allocation), mirroring tests/streams_unit/fake_server.py's approach for
StreamsHttpClient -- but injected via HttpClient's own `transport=` seam
instead of monkeypatching __init__.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional

import httpx
import pytest

from queen import Queen
from queen.http.http_client import HttpClient


class PlanTransport(httpx.AsyncBaseTransport):
    """Serves a canned response `plan` (list of descriptor dicts) in request
    order; once exhausted, `default` is served for any further requests.
    Records each hit's arrival time (relative to construction) so tests can
    sanity-check backoff pacing.

    Descriptor keys: status (int), json (Any), retry_after (str, optional).
    """

    def __init__(self, plan: Optional[List[Dict[str, Any]]] = None, default: Optional[Dict[str, Any]] = None):
        self.plan = list(plan or [])
        self.default = default or {"status": 200, "json": {"ok": True}}
        self.hits: List[Dict[str, Any]] = []
        self._start = time.monotonic()
        self._index = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.hits.append(
            {
                "method": request.method,
                "path": request.url.path,
                "at_ms": (time.monotonic() - self._start) * 1000,
            }
        )
        desc = self.plan[self._index] if self._index < len(self.plan) else self.default
        self._index += 1

        headers = dict(desc.get("headers") or {})
        if desc.get("retry_after") is not None:
            headers["Retry-After"] = str(desc["retry_after"])

        return httpx.Response(desc.get("status", 200), headers=headers, json=desc.get("json", {"ok": True}))


def rate_limited(retry_after: Optional[str] = None) -> Dict[str, Any]:
    desc: Dict[str, Any] = {"status": 429, "json": {"error": "slow down", "code": "rate_limited"}}
    if retry_after is not None:
        desc["retry_after"] = retry_after
    return desc


def repeat(desc: Dict[str, Any], n: int) -> List[Dict[str, Any]]:
    return [dict(desc) for _ in range(n)]


# ---------------------------------------------------------------------------
# HttpClient (direct) -- the centralized retry429 mechanism itself.
# ---------------------------------------------------------------------------


class TestHttpClientRetry429:
    @pytest.mark.asyncio
    async def test_honors_retry_after_header(self):
        transport = PlanTransport(plan=[rate_limited(retry_after="0")])
        client = HttpClient(base_url="http://fake.local", transport=transport, retry_429={"base_ms": 5, "cap_ms": 50})
        try:
            result = await client.get("/x")
            assert result == {"ok": True}
            assert len(transport.hits) == 2, "one 429 then one success"
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_exponential_backoff_when_no_retry_after(self):
        transport = PlanTransport(plan=repeat(rate_limited(), 2))
        client = HttpClient(base_url="http://fake.local", transport=transport, retry_429={"base_ms": 20, "cap_ms": 2000})
        try:
            result = await client.get("/x")
            assert result == {"ok": True}
            assert len(transport.hits) == 3
            gap1 = transport.hits[1]["at_ms"] - transport.hits[0]["at_ms"]
            gap2 = transport.hits[2]["at_ms"] - transport.hits[1]["at_ms"]
            assert gap2 > gap1 * 1.2, f"expected gap2 ({gap2}ms) to exceed gap1 ({gap1}ms) by backoff growth"
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_gives_up_after_max_attempts_for_default_kind(self):
        transport = PlanTransport(default=rate_limited())
        client = HttpClient(
            base_url="http://fake.local", transport=transport, retry_429={"max_attempts": 3, "base_ms": 1, "cap_ms": 5}
        )
        try:
            with pytest.raises(httpx.HTTPStatusError) as exc_info:
                await client.post("/api/v1/push", {"items": []})
            err = exc_info.value
            assert err.response.status_code == 429
            assert err.code == "rate_limited"  # type: ignore[attr-defined]
            assert len(transport.hits) == 3, "exactly maxAttempts tries, no more"
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_defaults_to_ten_attempts_when_not_configured(self):
        transport = PlanTransport(default=rate_limited())
        client = HttpClient(base_url="http://fake.local", transport=transport, retry_429={"base_ms": 1, "cap_ms": 2})
        try:
            with pytest.raises(httpx.HTTPStatusError):
                await client.post("/api/v1/push", {"items": []})
            assert len(transport.hits) == 10
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_pop_retries_past_push_default_of_ten(self):
        transport = PlanTransport(
            plan=repeat(rate_limited(retry_after="0"), 14),
            default={"status": 200, "json": {"messages": [{"id": "m1"}]}},
        )
        client = HttpClient(base_url="http://fake.local", transport=transport, retry_429={"base_ms": 1, "cap_ms": 5})
        try:
            result = await client.get("/api/v1/pop", retry_kind="pop")
            assert result == {"messages": [{"id": "m1"}]}
            assert len(transport.hits) == 15, "pop must not give up at the push default of 10 attempts"
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_explicit_max_attempts_bounds_pop_too(self):
        transport = PlanTransport(default=rate_limited())
        client = HttpClient(
            base_url="http://fake.local", transport=transport, retry_429={"max_attempts": 2, "base_ms": 1, "cap_ms": 5}
        )
        try:
            with pytest.raises(httpx.HTTPStatusError):
                await client.get("/api/v1/pop", retry_kind="pop")
            assert len(transport.hits) == 2, "explicit max_attempts bounds pop as well as push"
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_403_never_retried_and_preserves_code(self):
        transport = PlanTransport(default={"status": 403, "json": {"error": "cluster suspended", "code": "cluster_suspended"}})
        client = HttpClient(base_url="http://fake.local", transport=transport)
        try:
            with pytest.raises(httpx.HTTPStatusError) as exc_info:
                await client.post("/api/v1/push", {"items": []})
            err = exc_info.value
            assert err.response.status_code == 403
            assert err.code == "cluster_suspended"  # type: ignore[attr-defined]
            assert len(transport.hits) == 1, "403 must not be retried"
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_ordinary_400_still_not_retried(self):
        transport = PlanTransport(default={"status": 400, "json": {"error": "bad request"}})
        client = HttpClient(base_url="http://fake.local", transport=transport)
        try:
            with pytest.raises(httpx.HTTPStatusError):
                await client.get("/x")
            assert len(transport.hits) == 1
        finally:
            await client.close()


# ---------------------------------------------------------------------------
# Queen public API wiring: config plumbing (retry_429) + call-site marking
# (push vs. wait=True pop) actually reach HttpClient.
# ---------------------------------------------------------------------------


class TestQueenWiring:
    @pytest.mark.asyncio
    async def test_push_retries_429_then_succeeds(self):
        transport = PlanTransport(
            plan=[rate_limited(retry_after="0")],
            default={"status": 200, "json": [{"status": "queued", "transactionId": "tx-1"}]},
        )
        queen = Queen({"url": "http://fake.local"}, transport=transport, retry_429={"base_ms": 5, "cap_ms": 50})
        try:
            result = await queen.queue("q1").push({"hello": "world"})
            assert len(transport.hits) == 2
            assert result[0]["status"] == "queued"
        finally:
            await queen.close()

    @pytest.mark.asyncio
    async def test_push_surfaces_terminal_403(self):
        transport = PlanTransport(default={"status": 403, "json": {"error": "over quota", "code": "storage_quota_exceeded"}})
        queen = Queen({"url": "http://fake.local"}, transport=transport)
        try:
            with pytest.raises(Exception) as exc_info:
                await queen.queue("q1").push({"hello": "world"})
            assert getattr(exc_info.value, "code", None) == "storage_quota_exceeded"
            assert len(transport.hits) == 1
        finally:
            await queen.close()

    @pytest.mark.asyncio
    async def test_long_poll_pop_rides_out_more_429s_than_push_default(self):
        transport = PlanTransport(
            plan=repeat(rate_limited(retry_after="0"), 12),
            default={"status": 200, "json": {"messages": [{"transactionId": "tx-1", "data": {"x": 1}}]}},
        )
        queen = Queen({"url": "http://fake.local"}, transport=transport, retry_429={"base_ms": 1, "cap_ms": 5})
        try:
            messages = await queen.queue("q1").wait(True).pop()
            assert len(messages) == 1
            assert len(transport.hits) == 13
        finally:
            await queen.close()

    @pytest.mark.asyncio
    async def test_non_waiting_pop_swallows_exhausted_429_to_empty_list(self):
        transport = PlanTransport(default=rate_limited())
        queen = Queen(
            {"url": "http://fake.local"}, transport=transport, retry_429={"max_attempts": 2, "base_ms": 1, "cap_ms": 5}
        )
        try:
            messages = await queen.queue("q1").wait(False).pop()
            assert messages == [], "pop() keeps its swallow-errors-to-[] contract"
            assert len(transport.hits) == 2
        finally:
            await queen.close()


# ---------------------------------------------------------------------------
# ConsumerManager worker loop: the actual hot-loop/die bug (B4) this task
# fixes -- consume() must back off through 429s and stop cleanly on a
# terminal 403 instead of spinning or crashing uncontrolled.
# ---------------------------------------------------------------------------


class TestConsumeLoop:
    @pytest.mark.asyncio
    async def test_backs_off_through_429_and_delivers_message(self):
        transport = PlanTransport(
            plan=repeat(rate_limited(retry_after="0"), 3),
            default={
                "status": 200,
                "json": {"messages": [{"transactionId": "tx-1", "partitionId": "p-1", "data": {"x": 1}}]},
            },
        )
        queen = Queen({"url": "http://fake.local"}, transport=transport, retry_429={"base_ms": 1, "cap_ms": 5})
        try:
            received: List[Any] = []

            async def handler(msg: Any) -> None:
                received.append(msg)

            # .each() delivers one message per handler call (default
            # batches the whole list) -- simplest shape to assert on here.
            await asyncio.wait_for(
                queen.queue("q1").wait(True).limit(1).auto_ack(False).each().consume(handler),
                timeout=10,
            )
            assert len(received) == 1
            assert received[0]["transactionId"] == "tx-1"
            assert len(transport.hits) >= 4, "the 3 rate-limited attempts plus the final success must all have hit the server"
        finally:
            await queen.close()

    @pytest.mark.asyncio
    async def test_stops_on_terminal_403(self):
        transport = PlanTransport(default={"status": 403, "json": {"error": "cluster suspended", "code": "cluster_suspended"}})
        queen = Queen({"url": "http://fake.local"}, transport=transport)
        try:
            async def handler(msg: Any) -> None:
                pass

            with pytest.raises(Exception) as exc_info:
                await asyncio.wait_for(queen.queue("q1").wait(True).consume(handler), timeout=10)
            assert getattr(exc_info.value, "code", None) == "cluster_suspended"
            assert len(transport.hits) == 1, "must stop after the first 403, not hot-loop"
        finally:
            await queen.close()
