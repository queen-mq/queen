"""Session windows. 1:1 port of test-v2/stream/session.js (3 scenarios)."""

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
    sleep,
    summarise,
)


@pytest.mark.asyncio
async def test_sessionGapClosesAndStartsNew(client):
    src = mk_name("sessionGapClosesAndStartsNew", "src")
    sink = mk_name("sessionGapClosesAndStartsNew", "sink")
    query_id = mk_name("sessionGapClosesAndStartsNew", "q")
    await client.queue(src).create()
    await client.queue(sink).create()

    async def push_seq():
        await client.queue(src).partition("user-1").push([{"data": {"v": 1}}])
        await sleep(500)
        await client.queue(src).partition("user-1").push([{"data": {"v": 2}}])
        await sleep(4000)
        await client.queue(src).partition("user-1").push([{"data": {"v": 3}}])
        await sleep(500)
        await client.queue(src).partition("user-1").push([{"data": {"v": 4}}])

    task = asyncio.create_task(push_seq())
    handle = await (
        Stream.from_(client.queue(src))
        .window_session(gap=2, idle_flush_ms=500)
        .aggregate({"count": lambda m: 1, "sum": lambda m: m["v"]})
        .to(client.queue(sink))
        .run(query_id=query_id, url=STREAMS_URL, batch_size=50, reset=True)
    )
    await task
    await sleep(5000)
    emits = await drain_sink(client, sink, timeout_ms=2000)
    await handle.stop()

    counts = sorted(m.get("data", {}).get("count", 0) for m in emits)
    sums = sorted(m.get("data", {}).get("sum", 0) for m in emits)
    res = summarise("sessionGapClosesAndStartsNew", [
        expect(len(emits), "==", 2, "two sessions emitted"),
        expect(counts[0], "==", 2, "session 1 count"),
        expect(counts[1], "==", 2, "session 2 count"),
        expect(sums[0], "==", 3, "session 1 sum"),
        expect(sums[1], "==", 7, "session 2 sum"),
    ])
    assert res["success"], res["message"]


@pytest.mark.asyncio
async def test_sessionIdleFlushClosesQuietSession(client):
    src = mk_name("sessionIdleFlushClosesQuietSession", "src")
    sink = mk_name("sessionIdleFlushClosesQuietSession", "sink")
    query_id = mk_name("sessionIdleFlushClosesQuietSession", "q")
    await client.queue(src).create()
    await client.queue(sink).create()

    for i in range(3):
        await client.queue(src).partition("user-quiet").push([{"data": {"v": i + 1}}])
        await sleep(200)

    handle = await (
        Stream.from_(client.queue(src))
        .window_session(gap=1, idle_flush_ms=500)
        .aggregate({"count": lambda m: 1, "sum": lambda m: m["v"]})
        .to(client.queue(sink))
        .run(query_id=query_id, url=STREAMS_URL, batch_size=50, reset=True)
    )
    emits = await drain_until(client, sink, until=lambda out: len(out) >= 1, timeout_ms=8000)
    await handle.stop()

    res = summarise("sessionIdleFlushClosesQuietSession", [
        expect(len(emits), "==", 1, "one session emitted"),
        expect(emits[0].get("data", {}).get("count"), "==", 3, "session count"),
        expect(emits[0].get("data", {}).get("sum"), "==", 6, "session sum"),
        expect(handle.metrics()["flushCyclesTotal"], ">=", 1, "flush fired"),
    ])
    assert res["success"], res["message"]


@pytest.mark.asyncio
async def test_sessionMultipleKeysIndependent(client):
    src = mk_name("sessionMultipleKeysIndependent", "src")
    sink = mk_name("sessionMultipleKeysIndependent", "sink")
    query_id = mk_name("sessionMultipleKeysIndependent", "q")
    await client.queue(src).create()
    await client.queue(sink).create()

    for i in range(3):
        await client.queue(src).partition("shared").push([
            {"data": {"user": "alice", "v": i + 1}},
            {"data": {"user": "bob", "v": (i + 1) * 10}},
        ])
        await sleep(200)

    handle = await (
        Stream.from_(client.queue(src))
        .key_by(lambda m: m["data"]["user"])
        .window_session(gap=1, idle_flush_ms=500)
        .aggregate({"count": lambda m: 1, "sum": lambda m: m["v"]})
        .map(lambda v, ctx: {"user": ctx["key"], **v})
        .to(client.queue(sink))
        .run(query_id=query_id, url=STREAMS_URL, batch_size=50, reset=True)
    )
    emits = await drain_until(client, sink, until=lambda out: len(out) >= 2, timeout_ms=8000)
    await handle.stop()

    by_user: dict = {m.get("data", {})["user"]: m.get("data", {}) for m in emits if m.get("data", {}).get("user")}
    res = summarise("sessionMultipleKeysIndependent", [
        expect(len(by_user), "==", 2, "two distinct users emitted"),
        expect(by_user.get("alice", {}).get("count"), "==", 3, "alice count"),
        expect(by_user.get("alice", {}).get("sum"), "==", 6, "alice sum"),
        expect(by_user.get("bob", {}).get("count"), "==", 3, "bob count"),
        expect(by_user.get("bob", {}).get("sum"), "==", 60, "bob sum"),
    ])
    assert res["success"], res["message"]
