"""
Closing windows under a claim (broker 2.7, issue #153). Ports of the Rust
streams tests a_session_open_at_a_restart_closes_without_a_new_event and
two_runners_close_each_session_once.
"""

from __future__ import annotations

import random
import time

import pytest

from queen.streams import Stream

from ._helpers import STREAMS_URL, drain_until, mk_name, sleep


def _counts(messages: list, partition: str) -> list:
    return [m.get("data", {}).get("count") for m in messages if m.get("partition") == partition]


def _group() -> str:
    return f"drain-{int(time.time() * 1000)}-{random.randint(0, 999_999)}"


@pytest.mark.asyncio
async def test_aSessionOpenAtARestartClosesWithoutANewEvent(client):
    """A session open when its runner stops closes on the next runner with no
    new event on its partition. Before claims, the idle flush only knew the
    partitions its own process had popped, so this window stayed open for
    good (and kept its partition from being cleaned up)."""
    src = mk_name("aSessionOpenAtARestart", "src")
    sink = mk_name("aSessionOpenAtARestart", "sink")
    query_id = mk_name("aSessionOpenAtARestart", "q")
    await client.queue(src).create()
    await client.queue(sink).create()

    def build():
        return (
            Stream.from_(client.queue(src))
            .window_session(gap=3, idle_flush_ms=250)
            .aggregate({"count": lambda m: 1})
            .to(client.queue(sink))
        )

    opts = {"url": STREAMS_URL, "subscription_mode": "all", "max_wait_millis": 200}
    try:
        # Two events of trace-a, then a stop while its 3 s session is open.
        first = await build().run(query_id=query_id, reset=True, **opts)
        for _ in range(2):
            await client.queue(src).partition("trace-a").push([{"data": {"n": 1}}])
            await sleep(100)
        await sleep(1_200)
        folded = first.metrics()["messagesTotal"]
        await first.stop()
        assert folded == 2, "runner 1 should have folded both events"

        # Runner 2, same query. trace-b is the control: it gets an event now.
        second = await build().run(query_id=query_id, **opts)
        try:
            await client.queue(src).partition("trace-b").push([{"data": {"n": 1}}])
            group = _group()
            out = await drain_until(
                client,
                sink,
                until=lambda ms: bool(_counts(ms, "trace-a")) and bool(_counts(ms, "trace-b")),
                timeout_ms=15_000,
                group=group,
            )
            # Nothing more comes for either partition.
            await sleep(2_000)
            late = await drain_until(
                client, sink, until=lambda _: False, timeout_ms=500, group=group
            )
        finally:
            await second.stop()
    finally:
        await client.queue(src).delete()
        await client.queue(sink).delete()

    assert _counts(out + late, "trace-a") == [2], "trace-a's session closes once, with both events"
    assert _counts(out + late, "trace-b") == [1]


@pytest.mark.asyncio
async def test_twoRunnersCloseEachSessionOnce(client):
    """Two runners on one query close every session exactly once: each close
    runs under its partition's lease, and the broker refuses a close whose
    lease is gone."""
    src = mk_name("twoRunnersCloseEachSessionOnce", "src")
    sink = mk_name("twoRunnersCloseEachSessionOnce", "sink")
    query_id = mk_name("twoRunnersCloseEachSessionOnce", "q")
    await client.queue(src).create()
    await client.queue(sink).create()

    def build():
        return (
            Stream.from_(client.queue(src))
            .window_session(gap=2, idle_flush_ms=100)
            .aggregate({"count": lambda m: 1})
            .to(client.queue(sink))
        )

    opts = {
        "url": STREAMS_URL,
        "subscription_mode": "all",
        "max_wait_millis": 100,
        "max_partitions": 2,
    }
    partitions = 20
    try:
        a = await build().run(query_id=query_id, reset=True, **opts)
        b = await build().run(query_id=query_id, **opts)
        try:
            for round_ in range(2):
                for p in range(partitions):
                    await client.queue(src).partition(f"s-{p}").push([{"data": {"round": round_}}])
                await sleep(300)

            group = _group()
            out = await drain_until(
                client, sink, until=lambda ms: len(ms) >= partitions, timeout_ms=20_000, group=group
            )
            # Anything emitted twice would arrive within a few flush ticks.
            await sleep(2_000)
            late = await drain_until(
                client, sink, until=lambda _: False, timeout_ms=500, group=group
            )
        finally:
            await a.stop()
            await b.stop()
    finally:
        await client.queue(src).delete()
        await client.queue(sink).delete()

    by_partition: dict = {}
    for m in out + late:
        by_partition.setdefault(m.get("partition"), []).append(m.get("data", {}).get("count"))
    assert len(by_partition) == partitions, by_partition
    for p, counts in by_partition.items():
        assert counts == [2], f"partition {p}: one session of two events, got {counts}"
