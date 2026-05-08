"""
Example 02 — Per-customer windowed sum. 1:1 port of 02-per-customer-windowed-sum.js.

Source 'orders' is partitioned by customerId. Stream computes a 1-minute
tumbling sum of `amount` per customer.

Run:
    QUEEN_URL=http://localhost:6632 python examples/02_per_customer_windowed_sum.py
"""

from __future__ import annotations

import asyncio
import os

from queen import Queen, Stream


URL = os.environ.get("QUEEN_URL", "http://localhost:6632")


async def main():
    q = Queen(url=URL)
    try:
        handle = await (
            Stream.from_(q.queue("orders"))
            .window_tumbling(seconds=60)
            .aggregate({
                "count": lambda m: 1,
                "sum":   lambda m: m["data"]["amount"],
                "avg":   lambda m: m["data"]["amount"],
                "max":   lambda m: m["data"]["amount"],
            })
            .to(q.queue("orders.totals_per_customer_per_min"))
            .run(query_id="examples.orders.per_customer_per_min",
                 url=URL, batch_size=200, max_partitions=4)
        )
        await asyncio.sleep(5 * 60)
        print("Stats:", handle.metrics())
        await handle.stop()
    finally:
        await q.close()


if __name__ == "__main__":
    asyncio.run(main())
