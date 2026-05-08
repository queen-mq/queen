"""High-throughput tests. 1:1 port of test-v2/stream/throughput.js (2 scenarios)."""

from __future__ import annotations

import time

import pytest

from queen.streams import Stream

from ._helpers import (
    STREAMS_URL,
    drain_until,
    expect,
    mk_name,
    summarise,
)


@pytest.mark.asyncio
async def test_throughput5kAcross20Partitions(client):
    src = mk_name("throughput5kAcross20Partitions", "src")
    sink = mk_name("throughput5kAcross20Partitions", "sink")
    query_id = mk_name("throughput5kAcross20Partitions", "q")
    await client.queue(src).create()
    await client.queue(sink).create()

    NUM_PARTITIONS = 20
    PER_PARTITION = 250
    BATCH = 100

    push_start = time.monotonic()
    for p in range(NUM_PARTITIONS):
        partition = f"p-{p}"
        for off in range(0, PER_PARTITION, BATCH):
            items = [{"data": {"v": i + 1}} for i in range(off, min(off + BATCH, PER_PARTITION))]
            await client.queue(src).partition(partition).push(items)
    push_ms = (time.monotonic() - push_start) * 1000

    handle = await (
        Stream.from_(client.queue(src))
        .window_tumbling(seconds=5, idle_flush_ms=1000)
        .aggregate({"count": lambda m: 1, "sum": lambda m: m["v"]})
        .map(lambda agg, ctx: {"partition": ctx["partition"], **agg})
        .to(client.queue(sink))
        .run(query_id=query_id, url=STREAMS_URL, batch_size=200, max_partitions=8, reset=True)
    )

    def predicate(out):
        seen = {m.get("data", {}).get("partition") for m in out}
        return (
            len(seen) >= NUM_PARTITIONS
            and sum(m.get("data", {}).get("count", 0) for m in out) >= NUM_PARTITIONS * PER_PARTITION
        )

    emits = await drain_until(client, sink, until=predicate, timeout_ms=60000)
    await handle.stop()

    expected_sum = (PER_PARTITION * (PER_PARTITION + 1)) // 2
    totals: dict = {}
    for m in emits:
        p = m.get("data", {}).get("partition")
        if p not in totals:
            totals[p] = {"count": 0, "sum": 0}
        totals[p]["count"] += m.get("data", {}).get("count", 0)
        totals[p]["sum"] += m.get("data", {}).get("sum", 0)

    missing = mismatched = 0
    for p in range(NUM_PARTITIONS):
        t = totals.get(f"p-{p}")
        if t is None:
            missing += 1
            continue
        if t["count"] != PER_PARTITION or t["sum"] != expected_sum:
            mismatched += 1

    res = summarise("throughput5kAcross20Partitions", [
        expect(missing, "==", 0, "no partitions missed"),
        expect(mismatched, "==", 0, "all partitions have exact count + sum"),
        expect(handle.metrics()["errorsTotal"], "==", 0, "no runner errors"),
        expect(push_ms, "<", 15000, "push completed reasonably fast"),
    ])
    assert res["success"], res["message"]


@pytest.mark.asyncio
async def test_throughputManyWindowsSinglePartition(client):
    src = mk_name("throughputManyWindowsSinglePartition", "src")
    sink = mk_name("throughputManyWindowsSinglePartition", "sink")
    query_id = mk_name("throughputManyWindowsSinglePartition", "q")
    await client.queue(src).create()
    await client.queue(sink).create()

    N = 200
    for off in range(0, N, 50):
        items = [{"data": {"v": i + 1}} for i in range(off, min(off + 50, N))]
        await client.queue(src).partition("p").push(items)

    handle = await (
        Stream.from_(client.queue(src))
        .window_tumbling(seconds=1, idle_flush_ms=600)
        .aggregate({"count": lambda m: 1, "sum": lambda m: m["v"]})
        .to(client.queue(sink))
        .run(query_id=query_id, url=STREAMS_URL, batch_size=50, reset=True)
    )

    emits = await drain_until(
        client, sink,
        until=lambda out: sum(m.get("data", {}).get("count", 0) for m in out) >= N,
        timeout_ms=30000,
    )
    await handle.stop()

    total_count = sum(m.get("data", {}).get("count", 0) for m in emits)
    total_sum = sum(m.get("data", {}).get("sum", 0) for m in emits)
    expected_sum = (N * (N + 1)) // 2
    res = summarise("throughputManyWindowsSinglePartition", [
        expect(total_count, "==", N, "all messages counted"),
        expect(total_sum, "==", expected_sum, "sum matches 1..N"),
        expect(handle.metrics()["errorsTotal"], "==", 0, "no errors"),
    ])
    assert res["success"], res["message"]
