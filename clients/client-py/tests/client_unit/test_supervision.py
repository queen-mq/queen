import asyncio
import base64
import json
import time

import httpx
import pytest

from queen import Queen
from queen.consumer.supervision import Supervision
from ..conflation_unit.pop_transport import message, pop_body


class Transport(httpx.AsyncBaseTransport):
    def __init__(self, failure=False):
        self.docs, self.requests = [], []
        self.failure = failure

    async def handle_async_request(self, request):
        self.requests.append(request)
        if request.url.path == "/api/v1/kv":
            ops = json.loads(request.content)["operations"]
            assert len(ops) == 2
            head, chunk = ops
            assert head["ns"] == "queen-supervisor"
            assert head["ttlSeconds"] == chunk["ttlSeconds"] == 60
            assert head["value"]["write"] == chunk["value"]["write"]
            raw = base64.b64decode(chunk["value"]["data"])
            assert len(raw) == head["value"]["bytes"]
            self.docs.append(json.loads(raw))
            assert request.headers["authorization"] == "Bearer test-token"
            return httpx.Response(403 if self.failure else 200, json={"results": [{"applied": True}, {"applied": True}]})
        return httpx.Response(200, json=pop_body([message("txn-1")]))


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_default_off_and_opt_in_lifecycle(enabled):
    transport = Transport()
    client = Queen("http://plan.local", transport=transport, bearer_token="test-token")
    async def handler(_):
        await asyncio.sleep(0)
    try:
        qb = client.queue("orders").concurrency(2).each().limit(1).auto_ack(False).wait(False)
        if enabled:
            qb.supervision({"group": "billing-production"})
        await qb.consume(handler)
    finally:
        await client.close()
    if not enabled:
        assert not transport.docs
        return
    last = transport.docs[-1]
    assert last["state"] == "stopped"
    pool = last["pool_status"][0]
    assert (pool["running"], pool["busy"], pool["completed"], pool["failed"]) == (0, 0, 2, 0)
    assert pool["last_completed_at_epoch"] is not None
    assert pool["oldest_inflight_seconds"] is None
    assert not [r for r in transport.requests if r.url.path.startswith("/api/v1/ack")]


@pytest.mark.asyncio
async def test_publication_refusal_preserves_handler_error():
    transport = Transport(failure=True)
    client = Queen("http://plan.local", transport=transport, bearer_token="test-token")
    async def handler(_):
        raise ValueError("private failure")
    try:
        with pytest.raises(ValueError, match="private failure"):
            await client.queue("orders").supervision({"group": "billing"}).auto_ack(False).wait(False).consume(handler)
    finally:
        await client.close()
    assert transport.docs[-1]["state"] == "stopped"
    assert transport.docs[-1]["pool_status"][0]["failed"] == 1
    assert "private failure" not in json.dumps(transport.docs)


@pytest.mark.asyncio
async def test_progress_cancellation_unique_identity_and_validation():
    options = {"queue": "orders", "concurrency": 2}
    reporter = Supervision(None, {"group": "billing"}, options)
    assert reporter.id != Supervision(None, {"group": "billing"}, options).id
    entered = asyncio.Event()
    async def handler():
        entered.set()
        await asyncio.Event().wait()
    task = asyncio.create_task(reporter.wrap(handler)())
    await entered.wait()
    assert reporter.document("running")["pool_status"][0]["busy"] == 1
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    pool = reporter.document("running")["pool_status"][0]
    assert pool["busy"] == 0 and pool["failed"] == 1
    for group in ["", "coordination", "a/b", "a\n", None]:
        with pytest.raises(ValueError):
            Supervision(None, {"group": group}, options)


@pytest.mark.asyncio
async def test_publication_has_a_total_deadline():
    cancelled = asyncio.Event()
    class Slow:
        async def post(self, *args):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
    reporter = Supervision(Slow(), {"group": "billing"}, {"concurrency": 1})
    start = time.monotonic()
    await reporter.publish("running")
    assert cancelled.is_set()
    assert time.monotonic() - start < 3


@pytest.mark.asyncio
async def test_cancelling_consume_stops_the_publisher_and_reports_exits():
    transport = Transport()
    client = Queen("http://plan.local", transport=transport, bearer_token="test-token")
    entered = asyncio.Event()
    async def handler(_):
        entered.set()
        await asyncio.Event().wait()
    async def consume():
        await client.queue("orders").supervision({"group": "billing"}).auto_ack(False).wait(False).consume(handler)
    task = asyncio.create_task(consume())
    try:
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        # gather cancellation waits for its worker finally blocks.
        assert transport.docs[-1]["state"] == "stopped"
        assert transport.docs[-1]["pool_status"][0]["running"] == 0
        assert not [t for t in asyncio.all_tasks() if "Supervision.run" in repr(t.get_coro())]
    finally:
        await client.close()
