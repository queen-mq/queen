"""
A nack in each() mode skips only its own partition.

A multi-partition pop holds several partitions under one lease, and a nack
releases only the failed message's partition and clamps that partition's cursor
at it. The later messages of that partition come back; the other partitions are
still leased to this worker. Mirrors the JS client's fix (ConsumerManager.js).
"""

from __future__ import annotations

import asyncio

import pytest

from queen import Queen

from ..conflation_unit.pop_transport import PopTransport, message, pop_body

CONSUME_TIMEOUT_S = 5.0


def make(*pop_plan):
    transport = PopTransport(*pop_plan)
    client = Queen(url="http://plan.local", transport=transport, retry_attempts=1)
    return client, transport


def acks(transport):
    return [r for r in transport.requests if r.path.startswith("/api/v1/ack")]


async def run(builder, handler, **kwargs):
    return await asyncio.wait_for(builder.consume(handler, **kwargs), CONSUME_TIMEOUT_S)


@pytest.mark.asyncio
async def test_each_skips_only_the_nacked_partition_of_a_multi_partition_pop():
    """A multi-partition pop holds several partitions under one lease, and a
    nack releases only the failed message's partition. The later messages of
    that partition come back; the other partitions are still leased to this
    worker, so their messages are handled now, not after the lease expires."""
    client, transport = make(
        pop_body(
            [
                message("txn-1", partition_id="part-a"),
                message("txn-2", partition_id="part-a"),
                message("txn-3", partition_id="part-b"),
            ]
        ),
        pop_body([message("txn-4", lease_id="lease-4")]),
    )
    stop = asyncio.Event()
    seen = []

    async def handler(msg):
        seen.append(msg["transactionId"])
        if msg["transactionId"] == "txn-1":
            raise ValueError("first one fails")
        if msg["transactionId"] == "txn-4":
            stop.set()

    try:
        await run(
            client.queue("orders").group("workers").batch(3).each().wait(False),
            handler,
            signal=stop,
        )
    finally:
        await client.close()

    assert seen == ["txn-1", "txn-3", "txn-4"]
    sent = acks(transport)
    assert [(r.body["transactionId"], r.body["status"]) for r in sent] == [
        ("txn-1", "failed"),
        ("txn-3", "completed"),
        ("txn-4", "completed"),
    ]
