"""
Cycle/Runner tests against the fake streams server. 1:1 port of
cycle.test.js — 4 test cases.
"""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from queen.streams.stream import Stream

from .fake_server import (
    create_fake_streams_server,
    create_fake_source,
    fake_message,
)


async def run_until_drained(handle, server, delta_cycles: int, timeout_ms: float = 3000) -> None:
    baseline = len(server.recorded["cycles"])
    start = asyncio.get_event_loop().time()
    while (
        len(server.recorded["cycles"]) - baseline < delta_cycles
        and (asyncio.get_event_loop().time() - start) * 1000 < timeout_ms
    ):
        await asyncio.sleep(0.025)
    await handle.stop()


class FakeQueueRef:
    """Minimal `_queue_name` holder used in `.to(...)` targets."""

    def __init__(self, name: str):
        self._queue_name = name
        self.queue_name = name


# ---------------------------------------------------------------------------
# Runner — stateless pipeline
# ---------------------------------------------------------------------------


class TestRunnerStatelessPipeline:
    @pytest.mark.asyncio
    async def test_routes_filter_map_to_sink_through_cycles_one_per_partition(self):
        server = await create_fake_streams_server()
        try:
            part_a = "00000000-0000-0000-0000-000000000aaa"
            part_b = "00000000-0000-0000-0000-000000000bbb"
            source = create_fake_source("orders", [
                [
                    fake_message(part_a, "cust-1", {"type": "order", "amount": 50}, "2026-01-01T10:00:01Z"),
                    fake_message(part_a, "cust-1", {"type": "heartbeat"}, "2026-01-01T10:00:02Z"),
                    fake_message(part_b, "cust-2", {"type": "order", "amount": 10}, "2026-01-01T10:00:03Z"),
                ]
            ])

            handle = await (
                Stream.from_(source)
                .filter(lambda m: m.get("data", {}).get("type") != "heartbeat")
                .map(lambda m: {**m["data"], "marked": True})
                .to(FakeQueueRef("orders.enriched"))
                .run(query_id="streams.test.stateless", url=server.url)
            )

            await run_until_drained(handle, server, 2)

            assert len(server.recorded["registers"]) == 1
            cycles = server.recorded["cycles"]
            assert len(cycles) == 2

            partitions = sorted(c["partition_id"] for c in cycles)
            assert partitions == sorted([part_a, part_b])

            cyc_a = next(c for c in cycles if c["partition_id"] == part_a)
            assert len(cyc_a["state_ops"]) == 0
            assert len(cyc_a["push_items"]) == 1
            assert cyc_a["push_items"][0]["payload"] == {"type": "order", "amount": 50, "marked": True}
            assert cyc_a["ack"]["status"] == "completed"
            assert cyc_a["ack"]["transactionId"].startswith("tx-")

            cyc_b = next(c for c in cycles if c["partition_id"] == part_b)
            assert len(cyc_b["push_items"]) == 1
        finally:
            await server.close()


# ---------------------------------------------------------------------------
# Runner — windowed aggregation
# ---------------------------------------------------------------------------


class TestRunnerWindowedAggregation:
    @pytest.mark.asyncio
    async def test_emits_closed_windows_upserts_open_ones_and_seeds_from_prior_state(self):
        server = await create_fake_streams_server()
        try:
            part_a = "00000000-0000-0000-0000-000000000111"
            source = create_fake_source("orders", [
                [
                    fake_message(part_a, "cust-1", {"amount": 10}, "2026-01-01T10:00:05Z"),
                    fake_message(part_a, "cust-1", {"amount": 20}, "2026-01-01T10:00:30Z"),
                    fake_message(part_a, "cust-1", {"amount": 99}, "2026-01-01T10:01:05Z"),
                ]
            ])

            handle = await (
                Stream.from_(source)
                .window_tumbling(seconds=60)
                .aggregate({"count": lambda m: 1, "sum": lambda m: m["amount"]})
                .to(FakeQueueRef("orders.totals"))
                .run(query_id="streams.test.windowed", url=server.url)
            )

            await run_until_drained(handle, server, 1)

            cycles = server.recorded["cycles"]
            assert len(cycles) == 1
            c = cycles[0]
            assert c["partition_id"] == part_a
            assert len(c["push_items"]) == 1
            assert c["push_items"][0]["payload"]["count"] == 2
            assert c["push_items"][0]["payload"]["sum"] == 30

            upserts = [o for o in c["state_ops"] if o["type"] == "upsert"]
            deletes = [o for o in c["state_ops"] if o["type"] == "delete"]
            assert len(upserts) == 1
            assert len(deletes) == 0
            assert upserts[0]["value"]["acc"]["count"] == 1
            assert upserts[0]["value"]["acc"]["sum"] == 99
        finally:
            await server.close()


# ---------------------------------------------------------------------------
# Runner — config_hash mismatch
# ---------------------------------------------------------------------------


class TestRunnerConfigHashMismatch:
    @pytest.mark.asyncio
    async def test_throws_a_clear_error_pointing_at_reset_true(self):
        server = await create_fake_streams_server()
        try:
            source = create_fake_source("q", [[]])
            a = await (
                Stream.from_(source)
                .map(lambda m: m.get("data"))
                .to(FakeQueueRef("sink"))
                .run(query_id="streams.test.mismatch", url=server.url)
            )
            await a.stop()

            source_b = create_fake_source("q", [[]])
            stream_b = (
                Stream.from_(source_b)
                .map(lambda m: m.get("data"))
                .filter(lambda m: True)
                .to(FakeQueueRef("sink"))
            )
            with pytest.raises(Exception, match=r"config_hash mismatch"):
                await stream_b.run(query_id="streams.test.mismatch", url=server.url)
        finally:
            await server.close()


# ---------------------------------------------------------------------------
# Runner — state seeded from server
# ---------------------------------------------------------------------------


class TestRunnerStateSeededFromServer:
    @pytest.mark.asyncio
    async def test_uses_prior_state_for_ongoing_windows(self):
        server = await create_fake_streams_server()
        try:
            part_a = "00000000-0000-0000-0000-000000000222"

            source = create_fake_source("q", [
                [fake_message(part_a, "p1", {"amount": 50}, "2026-01-01T10:00:30Z")]
            ])

            handle = await (
                Stream.from_(source)
                .window_tumbling(seconds=60)
                .reduce(lambda a, m: a + m["amount"], 0)
                .to(FakeQueueRef("sink"))
                .run(query_id="streams.test.seed.runner", url=server.url)
            )

            await run_until_drained(handle, server, 1)
            assert len(server.recorded["stateGets"]) >= 1
        finally:
            await server.close()
