"""
Example 01 — Stateless enrichment. 1:1 port of 01-stateless-enrichment.js.

Read events from ``events.raw``, enrich each one with derived fields, drop
heartbeats, and emit to ``events.enriched``. No state, no windowing.

Run:
    QUEEN_URL=http://localhost:6632 python examples/01_stateless_enrichment.py
"""

from __future__ import annotations

import asyncio
import os

from queen import Queen, Stream


URL = os.environ.get("QUEEN_URL", "http://localhost:6632")


def upper_type(t):
    return t.upper() if isinstance(t, str) else None


async def main():
    q = Queen(url=URL)
    try:
        print("Starting stateless enrichment stream...")
        handle = await (
            Stream.from_(q.queue("events.raw"))
            .filter(lambda m: m.get("data") and m["data"].get("type") != "heartbeat")
            .map(lambda m: {
                **m["data"],
                "receivedAt": m.get("createdAt"),
                "upperType": upper_type(m["data"].get("type")),
            })
            .to(q.queue("events.enriched"))
            .run(query_id="examples.events.enrich", url=URL, batch_size=200, max_partitions=4)
        )

        await asyncio.sleep(60)
        print("Stopping...", handle.metrics())
        await handle.stop()
    finally:
        await q.close()


if __name__ == "__main__":
    asyncio.run(main())
