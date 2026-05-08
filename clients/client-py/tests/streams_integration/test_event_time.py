"""Event-time / watermark scenarios. 1:1 port of test-v2/stream/eventTime.js (4 scenarios)."""

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


@pytest.mark.asyncio
async def test_eventTimeBucketsByExtractor(client):
    src = mk_name("eventTimeBucketsByExtractor", "src")
    sink = mk_name("eventTimeBucketsByExtractor", "sink")
    query_id = mk_name("eventTimeBucketsByExtractor", "q")
    await client.queue(src).create()
    await client.queue(sink).create()

    await client.queue(src).partition("p").push([
        {"data": {"v": 1, "eventTs": "2026-01-01T10:00:01Z"}},
        {"data": {"v": 2, "eventTs": "2026-01-01T10:00:30Z"}},
        {"data": {"v": 5, "eventTs": "2026-01-01T10:01:05Z"}},
    ])

    handle = await (
        Stream.from_(client.queue(src))
        .window_tumbling(seconds=60, event_time=lambda m: m["data"]["eventTs"], idle_flush_ms=700)
        .aggregate({"count": lambda m: 1, "sum": lambda m: m["v"]})
        .map(lambda agg, ctx: {"windowKey": ctx["windowKey"], **agg})
        .to(client.queue(sink))
        .run(query_id=query_id, url=STREAMS_URL, batch_size=50, reset=True)
    )
    emits = await drain_until(client, sink, until=lambda out: len(out) >= 1, timeout_ms=10000)
    await handle.stop()

    by_win = {m.get("data", {}).get("windowKey"): m.get("data", {}) for m in emits}
    target = by_win.get("2026-01-01T10:00:00.000Z", {})
    res = summarise("eventTimeBucketsByExtractor", [
        expect(len(emits), ">=", 1, "at least one window emitted"),
        expect(target.get("sum"), "==", 3, "10:00 window sum"),
        expect(target.get("count"), "==", 2, "10:00 window count"),
    ])
    assert res["success"], res["message"]


def _build_event_time_stream(client, src, sink, **opts):
    return (
        Stream.from_(client.queue(src))
        .window_tumbling(**opts)
        .aggregate({"count": lambda m: 1, "sum": lambda m: m["v"]})
        .to(client.queue(sink))
    )


@pytest.mark.asyncio
async def test_eventTimeLateDropExcludesEvent(client):
    src = mk_name("eventTimeLateDropExcludesEvent", "src")
    sink = mk_name("eventTimeLateDropExcludesEvent", "sink")
    query_id = mk_name("eventTimeLateDropExcludesEvent", "q")
    await client.queue(src).create()
    await client.queue(sink).create()

    common = dict(
        seconds=60,
        event_time=lambda m: m["data"]["eventTs"],
        allowed_lateness=30,
        on_late="drop",
        idle_flush_ms=700,
    )

    await client.queue(src).partition("p").push([
        {"data": {"v": 100, "eventTs": "2026-01-01T11:00:00Z"}}
    ])
    handle1 = await _build_event_time_stream(client, src, sink, **common).run(
        query_id=query_id, url=STREAMS_URL, batch_size=50, reset=True
    )
    await sleep(2000)
    await handle1.stop()

    await client.queue(src).partition("p").push([
        {"data": {"v": 999, "eventTs": "2026-01-01T05:00:00Z"}}
    ])
    handle2 = await _build_event_time_stream(client, src, sink, **common).run(
        query_id=query_id, url=STREAMS_URL, batch_size=50
    )
    await sleep(2000)
    await handle2.stop()

    res = summarise("eventTimeLateDropExcludesEvent", [
        expect(handle2.metrics().get("lateEventsTotal", 0), ">=", 1, "late event recorded"),
    ])
    assert res["success"], res["message"]


@pytest.mark.asyncio
async def test_eventTimeAllowedLatenessIncluded(client):
    src = mk_name("eventTimeAllowedLatenessIncluded", "src")
    sink = mk_name("eventTimeAllowedLatenessIncluded", "sink")
    query_id = mk_name("eventTimeAllowedLatenessIncluded", "q")
    await client.queue(src).create()
    await client.queue(sink).create()

    common = dict(
        seconds=60,
        event_time=lambda m: m["data"]["eventTs"],
        allowed_lateness=60,
        on_late="drop",
        idle_flush_ms=700,
    )

    await client.queue(src).partition("p").push([
        {"data": {"v": 1, "eventTs": "2026-01-01T11:00:00Z"}}
    ])
    handle1 = await _build_event_time_stream(client, src, sink, **common).run(
        query_id=query_id, url=STREAMS_URL, batch_size=50, reset=True
    )
    await sleep(2000)
    await handle1.stop()

    await client.queue(src).partition("p").push([
        {"data": {"v": 7, "eventTs": "2026-01-01T10:59:30Z"}}
    ])
    handle2 = await _build_event_time_stream(client, src, sink, **common).run(
        query_id=query_id, url=STREAMS_URL, batch_size=50
    )
    await sleep(2000)
    await handle2.stop()

    res = summarise("eventTimeAllowedLatenessIncluded", [
        expect(handle2.metrics().get("lateEventsTotal", 0), "==", 0, "no late events"),
    ])
    assert res["success"], res["message"]


@pytest.mark.asyncio
async def test_eventTimeLateIncludePolicy(client):
    src = mk_name("eventTimeLateIncludePolicy", "src")
    sink = mk_name("eventTimeLateIncludePolicy", "sink")
    query_id = mk_name("eventTimeLateIncludePolicy", "q")
    await client.queue(src).create()
    await client.queue(sink).create()

    common = dict(
        seconds=60,
        event_time=lambda m: m["data"]["eventTs"],
        allowed_lateness=30,
        on_late="include",
        idle_flush_ms=700,
    )

    await client.queue(src).partition("p").push([
        {"data": {"v": 1, "eventTs": "2026-01-01T11:00:00Z"}}
    ])
    handle1 = await _build_event_time_stream(client, src, sink, **common).run(
        query_id=query_id, url=STREAMS_URL, batch_size=50, reset=True
    )
    await sleep(2000)
    await handle1.stop()

    await client.queue(src).partition("p").push([
        {"data": {"v": 999, "eventTs": "2026-01-01T05:00:00Z"}}
    ])
    handle2 = await _build_event_time_stream(client, src, sink, **common).run(
        query_id=query_id, url=STREAMS_URL, batch_size=50
    )
    await sleep(2000)
    await handle2.stop()

    res = summarise("eventTimeLateIncludePolicy", [
        expect(handle2.metrics().get("lateEventsTotal", 0), "==", 0, "include policy did not increment dropped count"),
    ])
    assert res["success"], res["message"]
