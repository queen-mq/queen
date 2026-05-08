"""
Example 08 — Rate-limiter stress test (many tenants, large bursts).

1:1 port of 08-rate-limiter-stress.js. Validates the .gate() pattern at
channel-manager scale: 100 tenants, 10k msgs each.

Run:
    QUEEN_URL=http://localhost:6632 python examples/08_rate_limiter_stress.py
"""

from __future__ import annotations

import asyncio
import os
import time

from queen import Queen, Stream, token_bucket_gate


URL = os.environ.get("QUEEN_URL", "http://localhost:6632")
TENANTS = int(os.environ.get("TENANTS", "100"))
MSGS_PER_TENANT = int(os.environ.get("MSGS_PER_TENANT", "10000"))
RUNNERS = int(os.environ.get("RUNNERS", "4"))
LEASE_SEC = int(os.environ.get("LEASE_SEC", "2"))
REFILL_PER_SEC = float(os.environ.get("REFILL_PER_SEC", "100"))
CAPACITY = int(os.environ.get("CAPACITY", str(int(REFILL_PER_SEC * LEASE_SEC))))
BATCH = int(os.environ.get("BATCH", "200"))
TIMEOUT_MS = int(os.environ.get("TIMEOUT_MS", "180000"))

TAG = f"{int(time.time() * 1000):x}"
Q_REQUESTS = f"rl_stress_req_{TAG}"
Q_APPROVED = f"rl_stress_apx_{TAG}"
TOTAL = TENANTS * MSGS_PER_TENANT


async def main():
    print(f"[stress] Queen={URL}, tenants={TENANTS}, msgs/tenant={MSGS_PER_TENANT} (total={TOTAL:,})")
    print(f"[stress] runners={RUNNERS} capacity={CAPACITY} refill={REFILL_PER_SEC}/sec lease={LEASE_SEC}s")

    q = Queen(url=URL)
    try:
        await q.queue(Q_REQUESTS).config(
            lease_time=LEASE_SEC, retry_limit=100_000,
            retention_enabled=True, retention_seconds=3600,
            completed_retention_seconds=3600,
        ).create()
        await q.queue(Q_APPROVED).config(
            lease_time=30, retention_enabled=True,
            retention_seconds=3600, completed_retention_seconds=3600,
        ).create()

        tenants = [f"tenant-{i:03d}" for i in range(TENANTS)]

        print(f"[stress] burst-pushing {TOTAL:,} msgs across {TENANTS} tenants...")
        push_start = time.monotonic()
        for i, tenant in enumerate(tenants):
            for off in range(0, MSGS_PER_TENANT, 500):
                items = [
                    {"data": {"tenantId": tenant, "seq": seq}}
                    for seq in range(off, min(off + 500, MSGS_PER_TENANT))
                ]
                await q.queue(Q_REQUESTS).partition(tenant).push(items)
            if (i + 1) % 10 == 0:
                print(f"  pushed tenants {i + 1}/{TENANTS}")
        print(f"[stress] push done in {time.monotonic() - push_start:.1f}s")

        gate_fn = token_bucket_gate(capacity=CAPACITY, refill_per_sec=REFILL_PER_SEC)

        # Spin up RUNNERS parallel streams sharing the same query_id (they
        # cooperate via the broker's partition leases).
        streams = []
        for r in range(RUNNERS):
            handle = await (
                Stream.from_(q.queue(Q_REQUESTS))
                .gate(gate_fn)
                .to(q.queue(Q_APPROVED))
                .run(
                    query_id=f"rate-limiter-stress-{TAG}",
                    url=URL,
                    batch_size=BATCH,
                    max_partitions=TENANTS // RUNNERS + 1,
                    max_wait_millis=500,
                    reset=(r == 0),
                )
            )
            streams.append(handle)
        print(f"[stress] {RUNNERS} runners up")

        drained = [0]
        stop_drain = [False]

        async def drain():
            cg = f"rl-stress-{TAG}"
            while not stop_drain[0]:
                try:
                    batch = await q.queue(Q_APPROVED).group(cg).batch(500).wait(True).timeout_millis(500).pop()
                    if not batch:
                        continue
                    drained[0] += len(batch)
                    await q.ack(batch, True, {"group": cg})
                except Exception:
                    if not stop_drain[0]:
                        raise

        drain_task = asyncio.create_task(drain())
        start = time.monotonic()
        last_report = start
        last_drained = 0
        while drained[0] < TOTAL and (time.monotonic() - start) * 1000 < TIMEOUT_MS:
            await asyncio.sleep(2)
            now = time.monotonic()
            rate = (drained[0] - last_drained) / (now - last_report) if (now - last_report) > 0 else 0
            print(f"[T+{(now - start):.0f}s] drained={drained[0]:,}/{TOTAL:,} rate={rate:.0f} msg/sec")
            last_report = now
            last_drained = drained[0]

        stop_drain[0] = True
        for s in streams:
            await s.stop()
        await drain_task

        print(f"\n[stress] DONE: {drained[0]:,}/{TOTAL:,} drained in {(time.monotonic() - start):.1f}s")
        print(f"[stress] aggregate rate ≈ {drained[0] / max(1, time.monotonic() - start):.0f} msg/sec")
    finally:
        await q.close()


if __name__ == "__main__":
    asyncio.run(main())
