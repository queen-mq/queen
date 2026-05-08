"""Combined / multi-stage scenarios. 1:1 port of test-v2/stream/combined.js (2 scenarios)."""

from __future__ import annotations

import asyncio

import pytest

from queen.streams import Stream

from ._helpers import (
    STREAMS_URL,
    drain_until,
    expect,
    mk_name,
    sleep,
    summarise,
)


@pytest.mark.asyncio
async def test_combinedFullPipeline(client):
    src = mk_name("combinedFullPipeline", "src")
    query_id = mk_name("combinedFullPipeline", "q")
    await client.queue(src).create()

    for u in range(3):
        items = []
        for i in range(3):
            items.append({"data": {"type": "order", "user": u, "amount": 100 + u * 100 + i}})
            items.append({"data": {"type": "heartbeat"}})
        await client.queue(src).partition(f"user-{u}").push(items)

    captured: list = []
    handle = await (
        Stream.from_(client.queue(src))
        .filter(lambda m: m["data"]["type"] == "order")
        .map(lambda m: {"user": m["data"]["user"], "amount": m["data"]["amount"]})
        .window_tumbling(seconds=5, idle_flush_ms=800)
        .aggregate({"count": lambda m: 1, "sum": lambda m: m["amount"]})
        .map(lambda agg, ctx: {"user": ctx["partition"], "windowKey": ctx["windowKey"], **agg})
        .filter(lambda v: v["count"] > 0)
        .foreach(lambda value: captured.append(value))
        .run(query_id=query_id, url=STREAMS_URL, batch_size=100, max_partitions=4, reset=True)
    )

    start = asyncio.get_event_loop().time()
    while len(captured) < 3 and (asyncio.get_event_loop().time() - start) < 15:
        await sleep(100)
    await handle.stop()

    by_user = {c["user"]: c for c in captured}
    res = summarise("combinedFullPipeline", [
        expect(len(by_user), "==", 3, "three users emitted"),
        expect(by_user.get("user-0", {}).get("count"), "==", 3, "user-0 count"),
        expect(by_user.get("user-0", {}).get("sum"), "==", 303, "user-0 sum"),
        expect(by_user.get("user-1", {}).get("count"), "==", 3, "user-1 count"),
        expect(by_user.get("user-1", {}).get("sum"), "==", 603, "user-1 sum"),
        expect(by_user.get("user-2", {}).get("count"), "==", 3, "user-2 count"),
        expect(by_user.get("user-2", {}).get("sum"), "==", 903, "user-2 sum"),
    ])
    assert res["success"], res["message"]


@pytest.mark.asyncio
async def test_combinedTwoConcurrentStreams(client):
    src = mk_name("combinedTwoConcurrentStreams", "src")
    sink_a = mk_name("combinedTwoConcurrentStreams-a", "sink")
    sink_b = mk_name("combinedTwoConcurrentStreams-b", "sink")
    q_a = mk_name("combinedTwoConcurrentStreams-a", "q")
    q_b = mk_name("combinedTwoConcurrentStreams-b", "q")
    await client.queue(src).create()
    await client.queue(sink_a).create()
    await client.queue(sink_b).create()

    items = [{"data": {"v": i + 1}} for i in range(12)]
    await client.queue(src).partition("p").push(items)

    stream_a = (
        Stream.from_(client.queue(src))
        .window_tumbling(seconds=3, idle_flush_ms=600)
        .aggregate({"sum": lambda m: m["v"]})
        .to(client.queue(sink_a))
    )
    stream_b = (
        Stream.from_(client.queue(src))
        .window_tumbling(seconds=3, idle_flush_ms=600)
        .aggregate({"count": lambda m: 1})
        .to(client.queue(sink_b))
    )

    handle_a, handle_b = await asyncio.gather(
        stream_a.run(query_id=q_a, url=STREAMS_URL, batch_size=50, reset=True),
        stream_b.run(query_id=q_b, url=STREAMS_URL, batch_size=50, reset=True),
    )

    emits_a, emits_b = await asyncio.gather(
        drain_until(client, sink_a,
                    until=lambda out: sum(m.get("data", {}).get("sum", 0) for m in out) >= 78,
                    timeout_ms=12000),
        drain_until(client, sink_b,
                    until=lambda out: sum(m.get("data", {}).get("count", 0) for m in out) >= 12,
                    timeout_ms=12000),
    )
    await handle_a.stop()
    await handle_b.stop()

    sum_a = sum(m.get("data", {}).get("sum", 0) for m in emits_a)
    count_b = sum(m.get("data", {}).get("count", 0) for m in emits_b)
    res = summarise("combinedTwoConcurrentStreams", [
        expect(len(emits_a), ">=", 1, "stream A emitted"),
        expect(len(emits_b), ">=", 1, "stream B emitted"),
        expect(sum_a, "==", 78, "stream A sum 1..12"),
        expect(count_b, "==", 12, "stream B count"),
    ])
    assert res["success"], res["message"]
