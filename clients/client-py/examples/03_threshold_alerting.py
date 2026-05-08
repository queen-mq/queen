"""
Example 03 — Threshold alerting. 1:1 port of 03-threshold-alerting.js.

Computes a 30-second average CPU per host; emits an alert when avg > 90.

Run:
    QUEEN_URL=http://localhost:6632 python examples/03_threshold_alerting.py
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone

from queen import Queen, Stream


URL = os.environ.get("QUEEN_URL", "http://localhost:6632")


def reducer(acc, m):
    v = m["data"].get("value", 0) if isinstance(m, dict) else 0
    return {"sum": acc["sum"] + v, "count": acc["count"] + 1}


def severity(agg):
    avg = agg["sum"] / agg["count"] if agg["count"] > 0 else 0
    if avg <= 90:
        return None
    return {
        "severity": "high",
        "avg": avg,
        "windowSamples": agg["count"],
        "detectedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


async def main():
    q = Queen(url=URL)
    try:
        handle = await (
            Stream.from_(q.queue("metrics.cpu"))
            .window_tumbling(seconds=30)
            .reduce(reducer, {"sum": 0, "count": 0})
            .map(lambda agg, ctx=None: severity(agg))
            .filter(lambda v: v is not None)
            .to(q.queue("alerts.cpu"))
            .run(query_id="examples.alerts.cpu_high", url=URL)
        )
        await asyncio.sleep(5 * 60)
        print("Stats:", handle.metrics())
        await handle.stop()
    finally:
        await q.close()


if __name__ == "__main__":
    asyncio.run(main())
