"""
Example 05 — Per-entity hourly profile build. 1:1 port of 05-per-entity-hourly-profile.js.

Run:
    QUEEN_URL=http://localhost:6632 python examples/05_per_entity_hourly_profile.py
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone

from queen import Queen, Stream


URL = os.environ.get("QUEEN_URL", "http://localhost:6632")


async def write_profile(profile: dict) -> None:
    print(f"[example05] writing profile for {profile.get('customerId')} => {json.dumps(profile)}")


def merge_profile(acc, m):
    data = m.get("data", {}) if isinstance(m, dict) else {}
    acc["customerId"] = data.get("customerId")
    acc["events"] = acc.get("events", 0) + 1
    acc["lastSeen"] = m.get("createdAt") or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    if data.get("amount"):
        acc["totalAmount"] = acc.get("totalAmount", 0) + data["amount"]
    if isinstance(data.get("tags"), list):
        acc["tags"] = list(set([*(acc.get("tags") or []), *data["tags"]]))
    return acc


async def main():
    q = Queen(url=URL)
    try:
        handle = await (
            Stream.from_(q.queue("account.events"))
            .window_tumbling(seconds=3600)
            .reduce(merge_profile, {"customerId": None, "events": 0, "totalAmount": 0, "tags": []})
            .foreach(lambda profile: write_profile(profile))
            .run(query_id="examples.customer.hourly_profile", url=URL, batch_size=100, max_partitions=4)
        )
        await asyncio.sleep(60 * 60)
        print("Stats:", handle.metrics())
        await handle.stop()
    finally:
        await q.close()


if __name__ == "__main__":
    asyncio.run(main())
