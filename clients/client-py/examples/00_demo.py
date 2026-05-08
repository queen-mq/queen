"""
Demo — push some data, run a streaming pipeline with v0.2 features,
watch the results. 1:1 port of streams/examples/00-demo.js.

Showcases:
  - Tumbling windows with grace_period and idle_flush_ms
  - Per-partition aggregation across 3 customers
  - .map(agg, ctx) post-reducer with window/partition metadata
  - .foreach(window, ctx) terminal side effect
  - The idle flush closing windows on quiet partitions

Run:
    QUEEN_URL=http://localhost:6632 python examples/00_demo.py
"""

from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timezone

from queen import Queen, Stream


URL = os.environ.get("QUEEN_URL", "http://localhost:6632")
TAG = f"{int(time.time() * 1000):x}"
SOURCE_QUEUE = f"streams_demo_orders_{TAG}"
SINK_QUEUE = f"streams_demo_totals_{TAG}"
QUERY_ID = f"streams.demo.{TAG}"

CUSTOMERS = ["cust-A", "cust-B", "cust-C"]
MESSAGES_PER_CUSTOMER = 10
WINDOW_SECONDS = 3
GRACE_SECONDS = 1


def iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")


async def main():
    print(f"[demo] Queen at {URL}")
    print(f"[demo] source = {SOURCE_QUEUE}")
    print(f"[demo] sink   = {SINK_QUEUE}")
    print(f"[demo] query  = {QUERY_ID}")

    q = Queen(url=URL)
    try:
        print("\n[demo] creating queues...")
        await q.queue(SOURCE_QUEUE).create()
        await q.queue(SINK_QUEUE).create()

        async def push_loop():
            print("\n[demo] pushing messages over ~12 seconds...")
            for i in range(MESSAGES_PER_CUSTOMER):
                await asyncio.gather(*[
                    q.queue(SOURCE_QUEUE).partition(cust).push([{
                        "data": {
                            "customerId": cust,
                            "amount": 10 + idx * 5 + i,
                            "idx": i,
                            "wave": "demo",
                        }
                    }])
                    for idx, cust in enumerate(CUSTOMERS)
                ])
                print(".", end="", flush=True)
                await asyncio.sleep(1.2)
            print("\n[demo] all messages pushed")

        push_task = asyncio.create_task(push_loop())

        print("\n[demo] starting stream...")

        def post_map(agg, ctx):
            clean = {k: v for k, v in agg.items() if not k.startswith("__avg_")}
            return {
                "patient": ctx["partition"],
                "windowStart": iso(ctx["windowStart"]),
                "windowEnd": iso(ctx["windowEnd"]),
                **clean,
            }

        def on_window(window, ctx):
            print(
                f"[{window['windowStart']}] {window['patient']}: "
                f"count={window['count']} sum={window['sum']} "
                f"avg={window['avg']:.1f} min={window['min']} max={window['max']}"
            )

        handle = await (
            Stream.from_(q.queue(SOURCE_QUEUE))
            .window_tumbling(seconds=WINDOW_SECONDS, grace_period=GRACE_SECONDS, idle_flush_ms=1500)
            .aggregate({
                "count": lambda m: 1,
                "sum":   lambda m: m["amount"],
                "min":   lambda m: m["amount"],
                "max":   lambda m: m["amount"],
                "avg":   lambda m: m["amount"],
            })
            .map(post_map)
            .foreach(on_window)
            .run(query_id=QUERY_ID, url=URL, batch_size=50, max_partitions=4, reset=True)
        )

        await push_task
        print("\n[demo] waiting 5s for idle flush to close trailing windows...")
        await asyncio.sleep(5)
        print("\n[demo] stopping stream...")
        await handle.stop()

        m = handle.metrics()
        print("\n[demo] final stream metrics:")
        print(f"  cycles:        {m['cyclesTotal']}")
        print(f"  flush cycles:  {m['flushCyclesTotal']}")
        print(f"  messages:      {m['messagesTotal']}")
        print(f"  push items:    {m['pushItemsTotal']}")
        print(f"  state ops:     {m['stateOpsTotal']}")
        print(f"  late events:   {m['lateEventsTotal']}")
        print(f"  errors:        {m['errorsTotal']}")
    finally:
        await q.close()


if __name__ == "__main__":
    asyncio.run(main())
