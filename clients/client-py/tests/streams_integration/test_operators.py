"""
Stateless operators on streams: map, filter, flat_map, key_by, foreach,
.to(sink). 1:1 port of test-v2/stream/operators.js (5 scenarios).
"""

from __future__ import annotations

import pytest

from queen.streams import Stream

from ._helpers import (
    STREAMS_URL,
    drain_until,
    expect,
    mk_name,
    push_spread,
    sleep,
    summarise,
)


@pytest.mark.asyncio
async def test_streamMapFilterSink(client):
    src = mk_name("streamMapFilterSink", "src")
    sink = mk_name("streamMapFilterSink", "sink")
    query_id = mk_name("streamMapFilterSink", "q")
    await client.queue(src).create()
    await client.queue(sink).create()

    items = []
    for i in range(30):
        kind = ["order", "heartbeat", "log"][i % 3]
        items.append({"partition": "Default", "data": {"type": kind, "n": i}})
    await push_spread(client, src, items)

    handle = await (
        Stream.from_(client.queue(src))
        .filter(lambda m: m["data"]["type"] != "heartbeat")
        .map(lambda m: {**m["data"], "marked": True})
        .to(client.queue(sink))
        .run(query_id=query_id, url=STREAMS_URL, batch_size=100, reset=True)
    )
    drained = await drain_until(client, sink, until=lambda out: len(out) >= 20, timeout_ms=8000)
    await handle.stop()

    checks = [
        expect(len(drained), ">=", 20, "sink message count"),
        expect(all(m.get("data", {}).get("marked") is True for m in drained), "==", True, "every msg has marked=true"),
        expect(all(m.get("data", {}).get("type") != "heartbeat" for m in drained), "==", True, "no heartbeats survived filter"),
        expect(handle.metrics()["errorsTotal"], "==", 0, "no errors"),
    ]
    res = summarise("streamMapFilterSink", checks)
    assert res["success"], res["message"]


@pytest.mark.asyncio
async def test_streamFlatMapFanout(client):
    src = mk_name("streamFlatMapFanout", "src")
    sink = mk_name("streamFlatMapFanout", "sink")
    query_id = mk_name("streamFlatMapFanout", "q")
    await client.queue(src).create()
    await client.queue(sink).create()

    items = [{"partition": "Default", "data": {"id": i}} for i in range(5)]
    await push_spread(client, src, items)

    handle = await (
        Stream.from_(client.queue(src))
        .flat_map(lambda m: [{"id": m["data"]["id"], "copy": c} for c in (1, 2, 3, 4)])
        .to(client.queue(sink))
        .run(query_id=query_id, url=STREAMS_URL, batch_size=100, reset=True)
    )
    drained = await drain_until(client, sink, until=lambda out: len(out) >= 20, timeout_ms=8000)
    await handle.stop()
    res = summarise("streamFlatMapFanout", [expect(len(drained), "==", 20, "fan-out count")])
    assert res["success"], res["message"]


@pytest.mark.asyncio
async def test_streamForeachCapturesAll(client):
    src = mk_name("streamForeachCapturesAll", "src")
    query_id = mk_name("streamForeachCapturesAll", "q")
    await client.queue(src).create()

    items = [{"partition": "Default", "data": {"i": i}} for i in range(12)]
    await push_spread(client, src, items)

    captured: list = []
    handle = await (
        Stream.from_(client.queue(src))
        .foreach(lambda v: captured.append(v))
        .run(query_id=query_id, url=STREAMS_URL, batch_size=50, reset=True)
    )
    import asyncio as _aio
    start = _aio.get_event_loop().time()
    while len(captured) < 12 and (_aio.get_event_loop().time() - start) < 5:
        await sleep(50)
    await handle.stop()

    res = summarise("streamForeachCapturesAll", [
        expect(len(captured), ">=", 12, "foreach captured all"),
        expect(all(isinstance(v.get("i"), int) for v in captured), "==", True, "each has i"),
    ])
    assert res["success"], res["message"]


@pytest.mark.asyncio
async def test_streamKeyByDoesNotCrash(client):
    src = mk_name("streamKeyByDoesNotCrash", "src")
    sink = mk_name("streamKeyByDoesNotCrash", "sink")
    query_id = mk_name("streamKeyByDoesNotCrash", "q")
    await client.queue(src).create()
    await client.queue(sink).create()

    items = [{"partition": "Default", "data": {"region": "EU" if i % 2 == 0 else "US", "n": i}} for i in range(6)]
    await push_spread(client, src, items)

    handle = await (
        Stream.from_(client.queue(src))
        .key_by(lambda m: m["data"]["region"])
        .map(lambda m: m["data"])
        .to(client.queue(sink))
        .run(query_id=query_id, url=STREAMS_URL, batch_size=100, reset=True)
    )
    drained = await drain_until(client, sink, until=lambda out: len(out) >= 6, timeout_ms=8000)
    await handle.stop()
    res = summarise("streamKeyByDoesNotCrash", [
        expect(len(drained), "==", 6, "all messages reach sink"),
        expect(handle.metrics()["errorsTotal"], "==", 0, "no errors"),
    ])
    assert res["success"], res["message"]


@pytest.mark.asyncio
async def test_streamAsyncMap(client):
    src = mk_name("streamAsyncMap", "src")
    sink = mk_name("streamAsyncMap", "sink")
    query_id = mk_name("streamAsyncMap", "q")
    await client.queue(src).create()
    await client.queue(sink).create()

    items = [{"partition": "Default", "data": {"i": i}} for i in range(5)]
    await push_spread(client, src, items)

    async def async_map(m):
        await sleep(20)
        return {**m["data"], "asyncEnriched": True}

    handle = await (
        Stream.from_(client.queue(src))
        .map(async_map)
        .to(client.queue(sink))
        .run(query_id=query_id, url=STREAMS_URL, batch_size=100, reset=True)
    )
    drained = await drain_until(client, sink, until=lambda out: len(out) >= 5, timeout_ms=8000)
    await handle.stop()
    res = summarise("streamAsyncMap", [
        expect(len(drained), "==", 5, "all enriched events reach sink"),
        expect(all(m.get("data", {}).get("asyncEnriched") is True for m in drained), "==", True, "every msg async-enriched"),
    ])
    assert res["success"], res["message"]
