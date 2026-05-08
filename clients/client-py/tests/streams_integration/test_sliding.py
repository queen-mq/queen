"""Sliding/hopping windows. 1:1 port of test-v2/stream/sliding.js."""

from __future__ import annotations

import asyncio

import pytest

from queen.streams import Stream
from queen.streams.operators import WindowSlidingOperator

from ._helpers import (
    STREAMS_URL,
    drain_sink,
    expect,
    mk_name,
    sleep,
    summarise,
)


def test_slidingRejectsInvalidConfig():
    """size must be a multiple of slide (pure validation, no broker needed)."""
    with pytest.raises(ValueError, match=r"must be an integer multiple"):
        WindowSlidingOperator(size=60, slide=7)


@pytest.mark.asyncio
async def test_slidingEventsAppearInOverlappingWindows(client):
    src = mk_name("slidingEventsAppearInOverlappingWindows", "src")
    sink = mk_name("slidingEventsAppearInOverlappingWindows", "sink")
    query_id = mk_name("slidingEventsAppearInOverlappingWindows", "q")
    await client.queue(src).create()
    await client.queue(sink).create()

    async def push_loop():
        for _ in range(6):
            await client.queue(src).partition("p").push([{"data": {"v": 1}}])
            await sleep(1000)

    task = asyncio.create_task(push_loop())
    handle = await (
        Stream.from_(client.queue(src))
        .window_sliding(size=4, slide=2, idle_flush_ms=1000)
        .aggregate({"count": lambda m: 1, "sum": lambda m: m["v"]})
        .to(client.queue(sink))
        .run(query_id=query_id, url=STREAMS_URL, batch_size=50, reset=True)
    )
    await task
    await sleep(6000)
    emits = await drain_sink(client, sink, timeout_ms=2000)
    await handle.stop()

    total_count = sum(m.get("data", {}).get("count", 0) for m in emits)
    res = summarise("slidingEventsAppearInOverlappingWindows", [
        expect(len(emits), ">=", 4, "multiple closed windows observed"),
        expect(total_count, ">=", 8, "overlap counted"),
        expect(total_count, "<=", 12, "overlap bounded"),
    ])
    assert res["success"], res["message"]
