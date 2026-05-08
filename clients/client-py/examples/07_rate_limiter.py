"""
Example 07 — Rate limiter via .gate() + per-partition lease back-pressure.
1:1 port of 07-rate-limiter.js.

Token-bucket rate limiter built entirely on streams primitives, preserving
FIFO order per partition WITHOUT a deferred queue. Back-pressure is the
broker's per-partition lease.

Run:
    QUEEN_URL=http://localhost:6632 python examples/07_rate_limiter.py
"""

from __future__ import annotations

import asyncio
import os
import time

from queen import Queen, Stream, token_bucket_gate


URL = os.environ.get("QUEEN_URL", "http://localhost:6632")
MSGS_PER_TENANT = int(os.environ.get("MSGS_PER_TENANT", "50"))
TENANTS = int(os.environ.get("TENANTS", "3"))
CAPACITY = int(os.environ.get("CAPACITY", "10"))
REFILL_PER_SEC = float(os.environ.get("REFILL_PER_SEC", "5"))
LEASE_SEC = int(os.environ.get("LEASE_SEC", "2"))
BATCH = int(os.environ.get("BATCH", "10"))
TIMEOUT_MS = int(os.environ.get("TIMEOUT_MS", "60000"))

TAG = f"{int(time.time() * 1000):x}"
Q_REQUESTS = f"rl_requests_{TAG}"
Q_APPROVED = f"rl_approved_{TAG}"
TOTAL_MSGS = MSGS_PER_TENANT * TENANTS


async def main():
    print(f"[rate-limiter] Queen     = {URL}")
    print(f"[rate-limiter] tag       = {TAG}")
    print(f"[rate-limiter] tenants={TENANTS}, msgs/tenant={MSGS_PER_TENANT} (total {TOTAL_MSGS})")
    print(f"[rate-limiter] bucket: capacity={CAPACITY}, refill={REFILL_PER_SEC}/sec, leaseTime={LEASE_SEC}s")

    q = Queen(url=URL)
    try:
        print("[rate-limiter] creating queues...")
        await q.queue(Q_REQUESTS).config(
            lease_time=LEASE_SEC,
            retry_limit=100,
            retention_enabled=True,
            retention_seconds=3600,
            completed_retention_seconds=3600,
        ).create()
        await q.queue(Q_APPROVED).config(
            lease_time=30,
            retention_enabled=True,
            retention_seconds=3600,
            completed_retention_seconds=3600,
        ).create()

        tenants = [f"tenant-{i}" for i in range(TENANTS)]

        print(f"[rate-limiter] burst-pushing {TOTAL_MSGS} requests across {TENANTS} partitions...")
        push_start = time.monotonic()
        for tenant_id in tenants:
            items = [
                {"data": {"tenantId": tenant_id, "seq": seq, "pushedAt": int(time.time() * 1000)}}
                for seq in range(MSGS_PER_TENANT)
            ]
            await q.queue(Q_REQUESTS).partition(tenant_id).push(items)
        print(f"[rate-limiter] push done in {(time.monotonic() - push_start) * 1000:.0f}ms")

        # Build a token-bucket gate via the helper.
        gate_fn = token_bucket_gate(capacity=CAPACITY, refill_per_sec=REFILL_PER_SEC)

        stream = await (
            Stream.from_(q.queue(Q_REQUESTS))
            .gate(gate_fn)
            .to(q.queue(Q_APPROVED))
            .run(
                query_id=f"rate-limiter-{TAG}",
                url=URL,
                batch_size=BATCH,
                max_partitions=TENANTS,
                max_wait_millis=500,
                reset=True,
            )
        )
        print("[rate-limiter] stream up")

        arrivals: dict = {t: [] for t in tenants}
        drained = [0]
        drain_stop = [False]

        async def drain():
            cg = f"rl-consumer-{TAG}"
            while not drain_stop[0]:
                try:
                    batch = await q.queue(Q_APPROVED).group(cg).batch(50).wait(True).timeout_millis(500).pop()
                    if not batch:
                        continue
                    arrived_at = int(time.time() * 1000)
                    for m in batch:
                        d = m.data
                        arrivals[d["tenantId"]].append({"seq": d["seq"], "arrivedAt": arrived_at})
                        drained[0] += 1
                    await q.ack(batch, True, {"group": cg})
                except Exception as ex:
                    if not drain_stop[0]:
                        print(f"[consumer] error: {ex}")

        drain_task = asyncio.create_task(drain())

        # Reporter
        last_report_at = time.monotonic()
        last_drained = 0
        start = time.monotonic()
        while drained[0] < TOTAL_MSGS and (time.monotonic() - start) * 1000 < TIMEOUT_MS:
            await asyncio.sleep(0.2)
            now = time.monotonic()
            if now - last_report_at >= 2:
                dt = now - last_report_at
                rate = (drained[0] - last_drained) / dt if dt > 0 else 0
                m = stream.metrics()
                print(
                    f"[T+{(now - start):.1f}s] drained={drained[0]}/{TOTAL_MSGS} "
                    f"rate={rate:.1f} msg/sec  "
                    f"gate allows={m.get('gateAllowsTotal', 0)} denies={m.get('gateDenialsTotal', 0)}"
                )
                last_report_at = now
                last_drained = drained[0]

        drain_stop[0] = True
        await stream.stop()
        await drain_task

        print("\n" + "=" * 80)
        print("RATE LIMITER VERIFICATION")
        print("=" * 80)

        all_passed = True
        if drained[0] == TOTAL_MSGS:
            print(f"  ✅ {drained[0]}/{TOTAL_MSGS} messages drained")
        else:
            print(f"  ❌ {drained[0]}/{TOTAL_MSGS} drained")
            all_passed = False

        # FIFO per tenant
        for t in tenants:
            arr = arrivals[t]
            in_order = all(arr[i]["seq"] > arr[i - 1]["seq"] for i in range(1, len(arr)))
            if not arr:
                print(f"  ❌ {t}: no arrivals")
                all_passed = False
            elif not in_order:
                seqs = [a["seq"] for a in arr][:20]
                print(f"  ❌ {t}: out-of-order; first seqs = {seqs}")
                all_passed = False
            elif len(arr) != MSGS_PER_TENANT:
                print(f"  ❌ {t}: {len(arr)}/{MSGS_PER_TENANT} arrived")
                all_passed = False
            else:
                print(f"  ✅ {t}: all {MSGS_PER_TENANT} arrived in order")

        m = stream.metrics()
        print(f"\n     gate ALLOWS:  {m.get('gateAllowsTotal', 0)}")
        print(f"     gate DENIES:  {m.get('gateDenialsTotal', 0)}")
        print(f"     cycles total: {m['cyclesTotal']}")

        print("\n" + "=" * 80)
        if all_passed:
            print("✅ ALL CHECKS PASSED — rate limiter works as designed")
        else:
            print("❌ ONE OR MORE CHECKS FAILED")
        print("=" * 80)
    finally:
        await q.close()


if __name__ == "__main__":
    asyncio.run(main())
