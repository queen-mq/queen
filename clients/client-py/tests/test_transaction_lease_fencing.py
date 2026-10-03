"""
A bundle that acknowledges messages of two leases, against a live broker.

A Queen 2 broker fences an ACK with the lease the operation carries, and lends
requiredLeases to an ACK without one only when the bundle names a single
lease. So an ACK that does not carry its own lease is applied in a bundle of
two leases even after its lease expired and another consumer took the message.
The wire half is tests/kv_unit/test_transaction_ack_leases.py.
"""

import asyncio
import os
import time
import uuid

import httpx
import pytest


SERVER_URL = os.environ.get("QUEEN_SERVER_URL", "http://localhost:6632")


def uniq(name: str) -> str:
    return f"test-txn-fence-{name}-{uuid.uuid4().hex[:8]}"


async def pop_leased(queue: str, lease_seconds: int) -> list:
    """One pop that does not wait, leased for ``lease_seconds``.

    The SDK's pop has no per-call lease, so this is the query of
    ``batch(1).wait(False).subscription_mode("all").pop()`` plus the broker's
    own ``leaseSeconds`` parameter.
    """
    async with httpx.AsyncClient(timeout=10.0) as http:
        res = await http.get(
            f"{SERVER_URL}/api/v1/pop/queue/{queue}",
            params={
                "batch": "1",
                "wait": "false",
                "subscriptionMode": "all",
                "leaseSeconds": str(lease_seconds),
            },
        )
    if res.status_code == 204:
        return []
    res.raise_for_status()
    return [m for m in res.json().get("messages") or [] if m]


async def wait_for(pop, timeout_s: float = 15.0) -> list:
    deadline = time.monotonic() + timeout_s
    while True:
        messages = await pop()
        if messages or time.monotonic() >= deadline:
            return messages
        await asyncio.sleep(0.2)


async def pop_one(queue: str, lease_seconds: int) -> dict:
    """The one delivery of ``queue``, leased for ``lease_seconds``."""
    messages = await wait_for(lambda: pop_leased(queue, lease_seconds), 5.0)
    assert len(messages) == 1, f"expected one message from {queue}, got {len(messages)}"
    assert messages[0].get("leaseId"), "a leased pop hands out a leaseId"
    return messages[0]


async def pop_now(client, queue: str) -> list:
    return await client.queue(queue).subscription_mode("all").batch(1).wait(False).pop()


async def delete_queues(client, *queues: str) -> None:
    for queue in queues:
        try:
            await client.queue(queue).delete()
        except Exception:
            pass  # never created


@pytest.mark.asyncio
async def test_a_bundle_with_an_ack_under_an_expired_lease_is_refused_whole(client):
    queue_a, queue_b = uniq("a"), uniq("b")
    try:
        await client.queue(queue_a).push([{"data": {"name": "a"}}])
        await client.queue(queue_b).push([{"data": {"name": "b"}}])
        a = await pop_one(queue_a, 1)
        b = await pop_one(queue_b, 60)
        assert a["leaseId"] != b["leaseId"]

        # Lease A expires, and another consumer takes the message.
        taken = await wait_for(lambda: pop_leased(queue_a, 60))
        assert taken, "the message of the expired lease is delivered again"
        assert taken[0]["transactionId"] == a["transactionId"]
        assert taken[0]["leaseId"] != a["leaseId"]

        refused = None
        try:
            await client.transaction().ack(a).ack(b).commit()
        except Exception as error:
            refused = error
        assert refused is not None, "an ACK under an expired lease must refuse the whole bundle"
        assert "rolled back" in str(refused)

        # Nothing of the bundle happened: each holder still settles its own message.
        assert (await client.transaction().ack(taken[0]).commit())["success"] is True
        assert (await client.transaction().ack(b).commit())["success"] is True
        assert await pop_now(client, queue_a) == []
        assert await pop_now(client, queue_b) == []
    finally:
        await delete_queues(client, queue_a, queue_b)


@pytest.mark.asyncio
async def test_a_bundle_of_two_live_leases_completes_both(client):
    queue_a, queue_b = uniq("a"), uniq("b")
    try:
        await client.queue(queue_a).push([{"data": {"name": "a"}}])
        await client.queue(queue_b).push([{"data": {"name": "b"}}])
        a = await pop_one(queue_a, 60)
        b = await pop_one(queue_b, 60)

        result = await client.transaction().ack(a).ack(b).commit()

        assert result["success"] is True
        assert await pop_now(client, queue_a) == []
        assert await pop_now(client, queue_b) == []
    finally:
        await delete_queues(client, queue_a, queue_b)
