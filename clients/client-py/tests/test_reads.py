"""
Reads by offset (queue.fetch) and partition discovery (queue.changed_since)
against a live broker.
"""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

QUEUE_FETCH = "test-queue-v2-reads-fetch"
QUEUE_CHANGED = "test-queue-v2-reads-changed"


async def push_each(client, queue, items):
    for i in range(0, len(items), 200):
        await asyncio.gather(
            *(
                client.queue(queue).partition(p).push([{"transactionId": txn, "data": data}])
                for p, txn, data in items[i : i + 200]
            )
        )


@pytest.mark.asyncio
async def test_fetch_reads_every_record_by_offset(client):
    # 1,200 partitions: more than one fetch call (1000 entries each).
    partitions = [f"entity-{i}" for i in range(1200)]
    per_partition = 3
    for n in range(per_partition):
        await push_each(client, QUEUE_FETCH, [(p, f"{p}-{n}", {"p": p, "n": n}) for p in partitions])

    read = await client.queue(QUEUE_FETCH).fetch(partitions)
    assert list(read) == partitions
    for p in partitions:
        part = read[p]
        got = [(r["offset"], r["payload"], r["transactionId"]) for r in part["records"]]
        assert got == [(n, {"p": p, "n": n}, f"{p}-{n}") for n in range(per_partition)], p
        assert (part["highWatermark"], part["logStartOffset"], part["nextOffset"]) == (per_partition, 0, per_partition)

    # A later read from nextOffset answers only what came after.
    await client.queue(QUEUE_FETCH).partition("entity-7").push([{"transactionId": "entity-7-late", "data": {"late": True}}])
    later = await client.queue(QUEUE_FETCH).fetch(
        ["entity-7", "entity-8"],
        offsets={"entity-7": read["entity-7"]["nextOffset"], "entity-8": read["entity-8"]["nextOffset"]},
    )
    assert [(r["offset"], r["payload"]) for r in later["entity-7"]["records"]] == [(per_partition, {"late": True})]
    assert later["entity-8"]["records"] == []

    # The reads took no lease and moved no cursor: a pop still gets the messages.
    popped = await client.queue(QUEUE_FETCH).partition("entity-9").batch(10).wait(False).pop()
    assert len(popped) == per_partition

    missing = await client.queue("test-queue-v2-reads-does-not-exist").fetch(["x"])
    assert missing["x"]["records"] == []


@pytest.mark.asyncio
async def test_changed_since_lists_the_written_partitions(client):
    before = datetime.now(timezone.utc) - timedelta(minutes=1)
    names = [f"c-{i}" for i in range(1100)]
    await push_each(client, QUEUE_CHANGED, [(p, f"{p}-0", {"p": p}) for p in names])

    # 1,100 partitions: two pages.
    first = await client.queue(QUEUE_CHANGED).changed_since(before)
    assert sorted(p["name"] for p in first["partitions"]) == sorted(names)
    assert isinstance(first["safeTime"], str)
    one = next(p for p in first["partitions"] if p["name"] == "c-5")
    assert (one["lastOffset"], one["logStart"]) == (0, 0)
    assert isinstance(one["lastWriteAt"], str) and isinstance(one["id"], str)

    # From the safeTime of that pass, a write to one partition lists it, and
    # nothing written before that safeTime comes back.
    await client.queue(QUEUE_CHANGED).partition("c-42").push([{"data": {"again": True}}])
    after = await client.queue(QUEUE_CHANGED).changed_since(first["safeTime"])
    assert any(p["name"] == "c-42" and p["lastOffset"] == 1 for p in after["partitions"])
    assert all(p["lastWriteAt"] >= first["safeTime"] for p in after["partitions"])

    everything = await client.queue(QUEUE_CHANGED).changed_since()
    assert len(everything["partitions"]) == len(names)
    none = await client.queue("test-queue-v2-reads-does-not-exist").changed_since(before)
    assert none["partitions"] == []
