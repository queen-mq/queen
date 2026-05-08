"""Cron windows. 1:1 port of test-v2/stream/cron.js (3 scenarios)."""

from __future__ import annotations

import asyncio

import pytest

from queen.streams import Stream
from queen.streams.operators import WindowCronOperator

from ._helpers import (
    STREAMS_URL,
    drain_until,
    expect,
    mk_name,
    sleep,
    summarise,
)


def test_cronRejectsInvalidEvery():
    """Invalid 'every' value rejected (pure validation)."""
    with pytest.raises(ValueError, match=r"every must be"):
        WindowCronOperator(every="fortnight")


@pytest.mark.asyncio
async def test_cronBoundariesAlignToUTC():
    """Pure annotation test - no broker needed."""
    cases = [
        ("minute", "2026-01-01T10:00:35.123Z", "2026-01-01T10:00:00.000Z"),
        ("hour",   "2026-01-01T10:30:35.000Z", "2026-01-01T10:00:00.000Z"),
        ("day",    "2026-01-01T22:30:35.000Z", "2026-01-01T00:00:00.000Z"),
        ("week",   "2026-01-07T12:00:00.000Z", "2026-01-05T00:00:00.000Z"),
    ]
    checks = []
    for every, created, expected in cases:
        op = WindowCronOperator(every=every)
        out = await op.apply({"msg": {"createdAt": created, "data": {}}, "key": "p1", "value": {}})
        checks.append(expect(out[0]["windowKey"], "==", expected, f"every={every}"))
    res = summarise("cronBoundariesAlignToUTC", checks)
    assert res["success"], res["message"]


@pytest.mark.asyncio
async def test_cronEverySecondLive(client):
    src = mk_name("cronEverySecondLive", "src")
    sink = mk_name("cronEverySecondLive", "sink")
    query_id = mk_name("cronEverySecondLive", "q")
    await client.queue(src).create()
    await client.queue(sink).create()

    async def push_loop():
        for _ in range(7):
            await client.queue(src).partition("p").push([{"data": {"v": 1}}])
            await sleep(700)

    task = asyncio.create_task(push_loop())
    handle = await (
        Stream.from_(client.queue(src))
        .window_cron(every="second", idle_flush_ms=500)
        .aggregate({"count": lambda m: 1})
        .to(client.queue(sink))
        .run(query_id=query_id, url=STREAMS_URL, batch_size=50, reset=True)
    )
    await task
    emits = await drain_until(client, sink, until=lambda out: len(out) >= 4, timeout_ms=10000)
    await handle.stop()

    res = summarise("cronEverySecondLive", [
        expect(len(emits), ">=", 4, "multiple 1-second windows closed"),
        expect(all(isinstance(m.get("data", {}).get("count"), int) for m in emits), "==", True, "every emit has count"),
    ])
    assert res["success"], res["message"]
