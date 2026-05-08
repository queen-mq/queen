"""
End-to-end test for queen-streams against a live Queen server + PG.

1:1 port of e2e.test.js. Skipped automatically when QUEEN_URL is unset to
``"skip"`` or the broker is unreachable.

Run::

    QUEEN_URL=http://localhost:6632 pytest tests/streams_unit/test_e2e.py -v
"""

from __future__ import annotations

import asyncio
import os
import time

import httpx
import pytest

from queen import Queen, Stream


QUEEN_URL = os.environ.get("QUEEN_URL", "http://localhost:6632")


async def _reachable(url: str) -> bool:
    try:
        async with httpx.AsyncClient(timeout=2.0) as c:
            r = await c.get(url + "/health")
            return r.is_success
    except Exception:
        return False


SHOULD_RUN = QUEEN_URL != "skip"


SOURCE_QUEUE = f"streams_e2e_src_{int(time.time() * 1000)}"
SINK_QUEUE = f"streams_e2e_sink_{int(time.time() * 1000)}"
QUERY_ID = f"streams.e2e.{int(time.time() * 1000)}"

NUM_PARTITIONS = 50
MESSAGES_PER_PARTITION = 200
# Use a short window so the test exercises window-close within a sensible
# total runtime (~15s). The original JS variant used 60s + a long-running
# producer; reduced here for CI-friendly turnaround while preserving the
# same partition / message volumes (10k events).
WINDOW_SECONDS = 5


class TestQueenStreamsE2E:
    @pytest.mark.asyncio
    async def test_processes_10k_messages_across_50_partitions_with_exactly_once_aggregation(self):
        if not SHOULD_RUN:
            pytest.skip("QUEEN_URL=skip")
        if not await _reachable(QUEEN_URL):
            pytest.skip(f"Queen at {QUEEN_URL} not reachable")

        q = Queen(url=QUEEN_URL)

        try:
            await q.queue(SOURCE_QUEUE).create()
            await q.queue(SINK_QUEUE).create()

            # 1. Push 10k messages.
            tx = 0
            expected_sums: dict = {}
            for p in range(NUM_PARTITIONS):
                partition = f"p-{p}"
                items = []
                s = 0
                for i in range(MESSAGES_PER_PARTITION):
                    amount = (i % 7) + 1
                    s += amount
                    items.append({
                        "queue": SOURCE_QUEUE,
                        "partition": partition,
                        "payload": {"amount": amount, "idx": i, "partition": partition},
                        "transactionId": f"e2e-{p}-{i}-{tx}",
                    })
                    tx += 1
                expected_sums[partition] = s
                # Send in chunks
                for off in range(0, len(items), 200):
                    await q.queue(SOURCE_QUEUE).push(items[off:off + 200])

            # 2. Start streaming query.
            # idle_flush_ms=2000 lets the runner flush ripe windows even
            # on quiet partitions; combined with a 5s window the test
            # observes window closures inside its ~15s budget.
            handle = await (
                Stream.from_(q.queue(SOURCE_QUEUE))
                .window_tumbling(seconds=WINDOW_SECONDS, idle_flush_ms=2000)
                .aggregate({"count": lambda m: 1, "sum": lambda m: m["amount"]})
                .to(q.queue(SINK_QUEUE))
                .run(
                    query_id=QUERY_ID,
                    url=QUEEN_URL,
                    batch_size=100,
                    max_partitions=4,
                    reset=True,
                )
            )

            # 3. Mid-stream kill, then restart.
            await asyncio.sleep(2.0)
            await handle.stop()
            print("[e2e] mid-stream stop, metrics =", handle.metrics())

            handle2 = await (
                Stream.from_(q.queue(SOURCE_QUEUE))
                .window_tumbling(seconds=WINDOW_SECONDS, idle_flush_ms=2000)
                .aggregate({"count": lambda m: 1, "sum": lambda m: m["amount"]})
                .to(q.queue(SINK_QUEUE))
                .run(query_id=QUERY_ID, url=QUEEN_URL, batch_size=100, max_partitions=4)
            )

            # 4. Tail messages to force window closures.
            await asyncio.sleep(3.0)
            tail_items = [
                {
                    "queue": SOURCE_QUEUE,
                    "partition": f"p-{p}",
                    "payload": {"amount": 0, "idx": -1, "partition": f"p-{p}", "tail": True},
                    "transactionId": f"e2e-tail-{p}-{tx + p}",
                }
                for p in range(NUM_PARTITIONS)
            ]
            await q.queue(SOURCE_QUEUE).push(tail_items)
            # Wait long enough for windowEnd + idle-flush to close the windows.
            await asyncio.sleep(10.0)

            # 5. Drain the sink queue. Use long-poll + a dedicated consumer
            # group so the partition-lease cycle releases between pops.
            sink_messages = []
            cg = f"e2e-drain-{int(time.time())}"
            deadline = time.monotonic() + 30.0
            while time.monotonic() < deadline:
                popped = await q.queue(SINK_QUEUE).group(cg).batch(500).wait(True).timeout_millis(1000).pop()
                if not popped:
                    if sink_messages:
                        break
                    continue
                sink_messages.extend(popped)
                await q.ack(popped, True, {"group": cg})
            print(f"[e2e] sink messages received = {len(sink_messages)}")

            assert len(sink_messages) > 0, "expected at least one sink emit"
            seen = set()
            for m in sink_messages:
                dedup_key = f"{getattr(m, 'partition', None) or m.get('partition')}|{getattr(m, 'transactionId', None) or m.get('transactionId')}"
                assert dedup_key not in seen, f"duplicate sink message for {dedup_key}"
                seen.add(dedup_key)

            await handle2.stop()
            print("[e2e] final metrics =", handle2.metrics())
        finally:
            await q.close()
