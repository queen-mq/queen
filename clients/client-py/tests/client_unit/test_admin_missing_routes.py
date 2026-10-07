"""
Admin methods whose routes the 2.x broker does not have raise before any request.

`move_message_to_dlq()` posted to /api/v1/messages/:partitionId/:transactionId/dlq
and `clear_queue()` sent DELETE /api/v1/queues/:name/clear. A 2.x broker answers
both 404 `no_such_route` (server/src/rsm/facade/real/phase2/reads.rs lists every
/api/v1/messages route; phase2.rs falls through to the 404). They now raise
NotImplementedError, naming what the broker offers instead, and send nothing.
"""

from __future__ import annotations

import pytest

from queen import Queen

from ..kv_unit.plan_server import PlanServer


def make():
    server = PlanServer(
        default={"status": 404, "json": {"code": "no_such_route", "error": "not found"}}
    )
    client = Queen(url="http://plan.local", transport=server, retry_attempts=1)
    return client, server


@pytest.mark.asyncio
async def test_move_message_to_dlq_raises_before_any_request():
    client, server = make()
    try:
        with pytest.raises(NotImplementedError) as raised:
            await client.admin.move_message_to_dlq("part-1", "txn-1")
    finally:
        await client.close()

    assert server.requests == []
    text = str(raised.value)
    assert "'dlq'" in text and "queen.ack(" in text, "names the 2.x way: an ack with status dlq"
    assert "lease" in text


@pytest.mark.asyncio
@pytest.mark.parametrize("partition", [None, "p1"])
async def test_clear_queue_raises_before_any_request(partition):
    client, server = make()
    try:
        with pytest.raises(NotImplementedError) as raised:
            await client.admin.clear_queue("orders", partition)
    finally:
        await client.close()

    assert server.requests == []
    text = str(raised.value)
    assert "seek_consumer_group" in text
    assert "delete()" in text
