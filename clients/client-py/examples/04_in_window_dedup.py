"""
Example 04 — In-window dedup. 1:1 port of 04-in-window-dedup.js.

Dedupes webhook eventIds within a 5-minute tumbling window per partition.

Run:
    QUEEN_URL=http://localhost:6632 python examples/04_in_window_dedup.py
"""

from __future__ import annotations

import asyncio
import os

from queen import Queen, Stream


URL = os.environ.get("QUEEN_URL", "http://localhost:6632")


def dedup(seen, m):
    eid = m.get("data", {}).get("eventId") if isinstance(m, dict) else None
    if not eid:
        return seen
    if eid not in seen:
        seen.append(eid)
    return seen


async def main():
    q = Queen(url=URL)
    try:
        handle = await (
            Stream.from_(q.queue("webhooks.raw"))
            .window_tumbling(seconds=300)
            .reduce(dedup, [])
            .flat_map(lambda seen, ctx=None: [{"eventId": eid} for eid in seen])
            .to(q.queue("webhooks.unique"))
            .run(query_id="examples.webhooks.dedup_5min", url=URL)
        )
        await asyncio.sleep(10 * 60)
        print("Stats:", handle.metrics())
        await handle.stop()
    finally:
        await q.close()


if __name__ == "__main__":
    asyncio.run(main())
