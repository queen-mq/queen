"""Crash recovery + config_hash safety. 1:1 port of test-v2/stream/recovery.js (3 scenarios)."""

from __future__ import annotations

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


class _FakeQueueRef:
    def __init__(self, name: str):
        self._queue_name = name


@pytest.mark.asyncio
async def test_recoveryConfigHashMismatchRejects(client):
    src = mk_name("recoveryConfigHashMismatchRejects", "src")
    query_id = mk_name("recoveryConfigHashMismatchRejects", "q")
    await client.queue(src).create()

    handle1 = await (
        Stream.from_(client.queue(src))
        .map(lambda m: m["data"])
        .to(_FakeQueueRef("never"))
        .run(query_id=query_id, url=STREAMS_URL, reset=True)
    )
    await handle1.stop()

    threw = False
    err_msg = ""
    try:
        handle2 = await (
            Stream.from_(client.queue(src))
            .map(lambda m: m["data"])
            .filter(lambda m: True)
            .to(_FakeQueueRef("never"))
            .run(query_id=query_id, url=STREAMS_URL)
        )
        await handle2.stop()
    except Exception as e:
        threw = True
        err_msg = str(e)
    res = summarise("recoveryConfigHashMismatchRejects", [
        expect(threw, "==", True, "mismatch threw"),
        expect("config_hash mismatch" in err_msg.lower(), "==", True, "error mentions config_hash mismatch"),
    ])
    assert res["success"], res["message"]


@pytest.mark.asyncio
async def test_recoveryResetTrueAcceptsNewShape(client):
    src = mk_name("recoveryResetTrueAcceptsNewShape", "src")
    query_id = mk_name("recoveryResetTrueAcceptsNewShape", "q")
    await client.queue(src).create()

    handle1 = await (
        Stream.from_(client.queue(src))
        .map(lambda m: m["data"])
        .to(_FakeQueueRef("never"))
        .run(query_id=query_id, url=STREAMS_URL, reset=True)
    )
    await handle1.stop()

    threw = False
    try:
        handle2 = await (
            Stream.from_(client.queue(src))
            .map(lambda m: m["data"])
            .filter(lambda m: True)
            .to(_FakeQueueRef("never"))
            .run(query_id=query_id, url=STREAMS_URL, reset=True)
        )
        await handle2.stop()
    except Exception:
        threw = True

    res = summarise("recoveryResetTrueAcceptsNewShape", [
        expect(threw, "==", False, "reset=True accepted"),
    ])
    assert res["success"], res["message"]


@pytest.mark.asyncio
async def test_recoveryMidStreamResume(client):
    src = mk_name("recoveryMidStreamResume", "src")
    sink = mk_name("recoveryMidStreamResume", "sink")
    query_id = mk_name("recoveryMidStreamResume", "q")
    await client.queue(src).create()
    await client.queue(sink).create()

    def build():
        return (
            Stream.from_(client.queue(src))
            .window_tumbling(seconds=1, idle_flush_ms=500)
            .aggregate({"count": lambda m: 1, "sum": lambda m: m["v"]})
            .to(client.queue(sink))
        )

    for _ in range(5):
        await client.queue(src).partition("p").push([{"data": {"v": 1}}])
    handle1 = await build().run(query_id=query_id, url=STREAMS_URL, batch_size=50, reset=True)
    await drain_until(client, sink, until=lambda out: len(out) >= 1, timeout_ms=5000)
    await handle1.stop()
    m1 = handle1.metrics()

    for _ in range(5):
        await client.queue(src).partition("p").push([{"data": {"v": 1}}])
    handle2 = await build().run(query_id=query_id, url=STREAMS_URL, batch_size=50)
    await sleep(3000)
    await handle2.stop()
    m2 = handle2.metrics()

    res = summarise("recoveryMidStreamResume", [
        expect(m1["messagesTotal"], ">=", 5, "run 1 saw events"),
        expect(m2["messagesTotal"], ">=", 5, "run 2 picked up new events"),
        expect(m2["messagesTotal"], "<=", 8, "run 2 did NOT re-process all of run 1"),
    ])
    assert res["success"], res["message"]
