"""
Example 06 — Throughput pipeline (~1500 msg/sec end-to-end).

1:1 port of 06-throughput-pipeline.js. Pushes ~1500 msg/sec across 30
partitions and runs two streams (windowed totals + filtered high-value)
concurrently for DURATION_MS, reporting throughput.

Run:
    QUEEN_URL=http://localhost:6632 DURATION_MS=30000 python examples/06_throughput_pipeline.py

Note: the JS version is ~400 lines with sub-second metric reports. This
Python port preserves the topology and semantics; refer to the JS file for
the full reporting logic.
"""

from __future__ import annotations

import asyncio
import os
import random
import time

from queen import Queen, Stream


URL = os.environ.get("QUEEN_URL", "http://localhost:6632")
DURATION_MS = int(os.environ.get("DURATION_MS", "30000"))
TARGET_RATE = int(os.environ.get("TARGET_RATE", "1500"))

TAG = f"{int(time.time() * 1000):x}"
RAW = f"pipeline_orders_raw_{TAG}"
TOTALS = f"pipeline_orders_totals_{TAG}"
HV = f"pipeline_orders_high_value_{TAG}"


async def main():
    q = Queen(url=URL)
    try:
        for qn in (RAW, TOTALS, HV):
            await q.queue(qn).create()

        # Producers (3 parallel) each pushing TARGET_RATE/3 msg/sec
        stop = [False]
        pushed = [0]

        async def producer(name: str, share: int):
            interval = 1.0 / share
            customers = [f"cust-{i}" for i in range(30)]
            while not stop[0]:
                t0 = time.monotonic()
                cust = random.choice(customers)
                amount = random.randint(1, 1000)
                await q.queue(RAW).partition(cust).push([{"data": {"name": name, "amount": amount}}])
                pushed[0] += 1
                elapsed = time.monotonic() - t0
                if elapsed < interval:
                    await asyncio.sleep(interval - elapsed)

        per_producer = TARGET_RATE // 3
        producers = [asyncio.create_task(producer(f"producer-{i}", per_producer)) for i in range(3)]

        # Stream A: windowed totals
        stream_a = await (
            Stream.from_(q.queue(RAW))
            .window_tumbling(seconds=1, idle_flush_ms=500)
            .aggregate({"count": lambda m: 1, "sum": lambda m: m["amount"]})
            .to(q.queue(TOTALS))
            .run(query_id=f"pipeline.totals.{TAG}", url=URL, batch_size=50,
                 max_partitions=8, reset=True)
        )

        # Stream B: filter + enrich
        stream_b = await (
            Stream.from_(q.queue(RAW))
            .filter(lambda m: m["data"]["amount"] > 500)
            .map(lambda m: {**m["data"], "highValue": True})
            .to(q.queue(HV))
            .run(query_id=f"pipeline.highvalue.{TAG}", url=URL, batch_size=50,
                 max_partitions=8, reset=True)
        )

        await asyncio.sleep(DURATION_MS / 1000.0)
        stop[0] = True
        for p in producers:
            await p

        await asyncio.sleep(5)  # drain
        await stream_a.stop()
        await stream_b.stop()

        print(f"\nFinal stats: pushed={pushed[0]}")
        print(f"Stream A: {stream_a.metrics()}")
        print(f"Stream B: {stream_b.metrics()}")
    finally:
        await q.close()


if __name__ == "__main__":
    asyncio.run(main())
