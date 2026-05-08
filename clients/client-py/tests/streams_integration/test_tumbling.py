"""
Tumbling window scenarios. 1:1 port of test-v2/stream/tumbling.js (5 tests).
"""

from __future__ import annotations

import asyncio

import pytest

from queen.streams import Stream

from ._helpers import (
    STREAMS_URL,
    drain_sink,
    drain_until,
    expect,
    mk_name,
    push_spread,
    sleep,
    summarise,
)


@pytest.mark.asyncio
async def test_tumblingBasicWindowSum(client):
    src = mk_name("tumblingBasicWindowSum", "src")
    sink = mk_name("tumblingBasicWindowSum", "sink")
    query_id = mk_name("tumblingBasicWindowSum", "q")
    await client.queue(src).create()
    await client.queue(sink).create()

    async def push_loop():
        for i in range(8):
            await client.queue(src).partition("one").push([{"data": {"amount": i + 1}}])
            await sleep(1100)

    push_task = asyncio.create_task(push_loop())
    handle = await (
        Stream.from_(client.queue(src))
        .window_tumbling(seconds=2, idle_flush_ms=800)
        .aggregate({"count": lambda m: 1, "sum": lambda m: m["amount"]})
        .to(client.queue(sink))
        .run(query_id=query_id, url=STREAMS_URL, batch_size=50, reset=True)
    )
    await push_task
    emits = await drain_until(
        client, sink,
        until=lambda out: sum(m.get("data", {}).get("count", 0) for m in out) >= 8,
        timeout_ms=15000,
    )
    await handle.stop()

    total_sum = sum(m.get("data", {}).get("sum", 0) for m in emits)
    total_count = sum(m.get("data", {}).get("count", 0) for m in emits)
    res = summarise("tumblingBasicWindowSum", [
        expect(len(emits), ">=", 3, "at least 3 windows closed"),
        expect(total_sum, "==", 36, "sum across all windows = 1..8"),
        expect(total_count, "==", 8, "count across all windows = 8"),
        expect(handle.metrics()["errorsTotal"], "==", 0, "no runner errors"),
    ])
    assert res["success"], res["message"]


@pytest.mark.asyncio
async def test_tumblingPerPartitionIsolation(client):
    src = mk_name("tumblingPerPartitionIsolation", "src")
    sink = mk_name("tumblingPerPartitionIsolation", "sink")
    query_id = mk_name("tumblingPerPartitionIsolation", "q")
    await client.queue(src).create()
    await client.queue(sink).create()
    customers = ["cust-A", "cust-B", "cust-C"]

    async def push_loop():
        for i in range(6):
            for c_idx, cust in enumerate(customers):
                amount = (c_idx + 1) * 100 + i
                await client.queue(src).partition(cust).push(
                    [{"data": {"customer": cust, "amount": amount}}]
                )
            await sleep(1200)

    task = asyncio.create_task(push_loop())
    handle = await (
        Stream.from_(client.queue(src))
        .window_tumbling(seconds=2, idle_flush_ms=1000)
        .aggregate({"count": lambda m: 1, "sum": lambda m: m["amount"]})
        .map(lambda agg, ctx: {"partition": ctx["partition"], **agg})
        .to(client.queue(sink))
        .run(query_id=query_id, url=STREAMS_URL, batch_size=50, max_partitions=4, reset=True)
    )
    await task
    emits = await drain_until(
        client, sink,
        until=lambda out: sum(m.get("data", {}).get("count", 0) for m in out) >= 18,
        timeout_ms=18000,
    )
    await handle.stop()

    cross_bleed = 0
    per_sum = {c: 0 for c in customers}
    per_cnt = {c: 0 for c in customers}
    for m in emits:
        p = m.get("data", {}).get("partition")
        if p not in per_sum:
            cross_bleed += 1
            continue
        per_sum[p] += m.get("data", {}).get("sum", 0)
        per_cnt[p] += m.get("data", {}).get("count", 0)

    res = summarise("tumblingPerPartitionIsolation", [
        expect(cross_bleed, "==", 0, "no cross-bleed"),
        expect(per_cnt["cust-A"], "==", 6, "cust-A count"),
        expect(per_cnt["cust-B"], "==", 6, "cust-B count"),
        expect(per_cnt["cust-C"], "==", 6, "cust-C count"),
        expect(per_sum["cust-A"], "==", 615, "cust-A sum"),
        expect(per_sum["cust-B"], "==", 1215, "cust-B sum"),
        expect(per_sum["cust-C"], "==", 1815, "cust-C sum"),
    ])
    assert res["success"], res["message"]


@pytest.mark.asyncio
async def test_tumblingAggregateAllStats(client):
    src = mk_name("tumblingAggregateAllStats", "src")
    sink = mk_name("tumblingAggregateAllStats", "sink")
    query_id = mk_name("tumblingAggregateAllStats", "q")
    await client.queue(src).create()
    await client.queue(sink).create()

    for v in (10, 5, 30, 2, 3):
        await client.queue(src).partition("p").push([{"data": {"v": v}}])

    handle = await (
        Stream.from_(client.queue(src))
        .window_tumbling(seconds=3, idle_flush_ms=500)
        .aggregate({
            "count": lambda m: 1,
            "sum":   lambda m: m["v"],
            "min":   lambda m: m["v"],
            "max":   lambda m: m["v"],
            "avg":   lambda m: m["v"],
        })
        .to(client.queue(sink))
        .run(query_id=query_id, url=STREAMS_URL, batch_size=50, reset=True)
    )
    emits = await drain_until(client, sink, until=lambda out: len(out) >= 1, timeout_ms=10000)
    await handle.stop()

    assert len(emits) > 0, "no emit observed"
    v = emits[0].get("data", {})
    res = summarise("tumblingAggregateAllStats", [
        expect(v.get("count"), "==", 5, "count"),
        expect(v.get("sum"),   "==", 50, "sum"),
        expect(v.get("min"),   "==", 2, "min"),
        expect(v.get("max"),   "==", 30, "max"),
        expect(v.get("avg"),   "==", 10, "avg"),
    ])
    assert res["success"], res["message"]


@pytest.mark.asyncio
async def test_tumblingGracePeriodDelaysClose(client):
    src = mk_name("tumblingGracePeriodDelaysClose", "src")
    sink = mk_name("tumblingGracePeriodDelaysClose", "sink")
    query_id = mk_name("tumblingGracePeriodDelaysClose", "q")
    await client.queue(src).create()
    await client.queue(sink).create()

    await client.queue(src).partition("p").push([
        {"data": {"v": 1}}, {"data": {"v": 2}}, {"data": {"v": 3}},
    ])

    handle = await (
        Stream.from_(client.queue(src))
        .window_tumbling(seconds=2, grace_period=4, idle_flush_ms=500)
        .aggregate({"count": lambda m: 1, "sum": lambda m: m["v"]})
        .to(client.queue(sink))
        .run(query_id=query_id, url=STREAMS_URL, batch_size=50, reset=True)
    )
    await sleep(3000)
    early = await drain_sink(client, sink, timeout_ms=500)
    await sleep(4500)
    late = await drain_sink(client, sink, timeout_ms=500)
    await handle.stop()

    res = summarise("tumblingGracePeriodDelaysClose", [
        expect(len(early), "==", 0, "no emit during grace period"),
        expect(len(late), ">=", 1, "window closed after grace expired"),
        expect(late[0].get("data", {}).get("sum"), "==", 6, "sum"),
        expect(late[0].get("data", {}).get("count"), "==", 3, "count"),
    ])
    assert res["success"], res["message"]


@pytest.mark.asyncio
async def test_tumblingIdleFlushClosesQuietPartitions(client):
    src = mk_name("tumblingIdleFlushClosesQuietPartitions", "src")
    sink = mk_name("tumblingIdleFlushClosesQuietPartitions", "sink")
    query_id = mk_name("tumblingIdleFlushClosesQuietPartitions", "q")
    await client.queue(src).create()
    await client.queue(sink).create()

    for _ in range(3):
        await client.queue(src).partition("quiet").push([{"data": {"v": 7}}])

    handle = await (
        Stream.from_(client.queue(src))
        .window_tumbling(seconds=1, idle_flush_ms=700)
        .aggregate({"count": lambda m: 1, "sum": lambda m: m["v"]})
        .to(client.queue(sink))
        .run(query_id=query_id, url=STREAMS_URL, batch_size=50, reset=True)
    )
    emits = await drain_until(client, sink, until=lambda out: len(out) >= 1, timeout_ms=8000)
    await handle.stop()

    res = summarise("tumblingIdleFlushClosesQuietPartitions", [
        expect(len(emits), ">=", 1, "idle flush emitted closed window"),
        expect(emits[0].get("data", {}).get("count"), "==", 3, "count"),
        expect(emits[0].get("data", {}).get("sum"), "==", 21, "sum"),
        expect(handle.metrics()["flushCyclesTotal"], ">=", 1, "flushCyclesTotal advanced"),
    ])
    assert res["success"], res["message"]
