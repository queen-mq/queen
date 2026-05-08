"""
Example 06 — Patient health-rate stream. 1:1 port of 06-health.js.

Pushes simulated heart-rate readings across N patients and runs a tumbling
windowed aggregation. Equivalent in shape to 00_demo.py.

Run:
    QUEEN_URL=http://localhost:6632 python examples/06_health.py
"""

from __future__ import annotations

import asyncio
import os
import random
import time

from queen import Queen, Stream


URL = os.environ.get("QUEEN_URL", "http://localhost:6632")
TAG = f"{int(time.time() * 1000):x}"
SOURCE_QUEUE = f"streams_health_hr_{TAG}"
QUERY_ID = f"streams.health.{TAG}"
PATIENTS = [f"patient-{i}" for i in range(10)]


async def main():
    q = Queen(url=URL)
    try:
        await q.queue(SOURCE_QUEUE).create()

        async def producer():
            print("Starting patient heart rate producer")
            for _ in range(60):
                await asyncio.gather(*[
                    q.queue(SOURCE_QUEUE).partition(p).push([
                        {"data": {"patient": p, "heartRate": random.randint(40, 100)}}
                    ])
                    for p in PATIENTS
                ])
                await asyncio.sleep(1)

        prod_task = asyncio.create_task(producer())

        def on_window(agg, ctx):
            print("closed window:", agg, ctx)

        handle = await (
            Stream.from_(q.queue(SOURCE_QUEUE))
            .window_tumbling(seconds=10, idle_flush_ms=2000)
            .aggregate({
                "count": lambda m: 1,
                "sum":   lambda m: m["heartRate"],
                "min":   lambda m: m["heartRate"],
                "max":   lambda m: m["heartRate"],
                "avg":   lambda m: m["heartRate"],
            })
            .map(lambda agg, ctx: ({**agg, "kind": "window-emit"}, on_window(agg, ctx))[0])
            .run(query_id=QUERY_ID, url=URL, batch_size=50, max_partitions=10, reset=True)
        )

        await prod_task
        await asyncio.sleep(15)
        await handle.stop()
        print("Stats:", handle.metrics())
    finally:
        await q.close()


if __name__ == "__main__":
    asyncio.run(main())
