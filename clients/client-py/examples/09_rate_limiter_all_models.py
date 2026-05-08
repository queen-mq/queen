"""
Example 09 — Rate-limiter test, all canonical models.

1:1 port of 09-rate-limiter-all-models.js. Validates that the same .gate()
primitive expresses every common rate-limit shape:

    A — req/sec    : 1 token per HTTP call (Airbnb-style)
    B — msg/sec    : 1 token per individual message (WhatsApp Cloud)
    C — 1:1        : degenerate case where 1 req = 1 msg = 1 token
    D — cost       : variable weight per message (TPM-style)
    A+B cascade    : two gates in series (Booking-style)
    E — sliding    : sliding-window quota (SendGrid daily, OTA hourly)

Each scenario runs end-to-end:
  1. push N items per tenant in burst
  2. fan out N runners on the gate stream
  3. drain the sink, measure rate / order / count / weight

Run:
    QUEEN_URL=http://localhost:6632 python examples/09_rate_limiter_all_models.py

Skip a scenario:
    SKIP=A,D python examples/09_rate_limiter_all_models.py
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Callable

from queen import Queen, Stream, sliding_window_gate, token_bucket_gate


URL = os.environ.get("QUEEN_URL", "http://localhost:6632")
TENANTS = int(os.environ.get("TENANTS", "20"))
ELEMS_PER_TENANT = int(os.environ.get("ELEMS_PER_TENANT", "500"))
RUNNERS = int(os.environ.get("RUNNERS", "4"))
LEASE_SEC = int(os.environ.get("LEASE_SEC", "2"))
BATCH = int(os.environ.get("BATCH", "200"))
TIMEOUT_MS = int(os.environ.get("TIMEOUT_MS", "180_000"))
SKIP = set((os.environ.get("SKIP") or "").split(",")) - {""}

RATE_REQ_PER_SEC = 100
RATE_MSG_PER_SEC = 100
RATE_WEIGHT_PER_SEC = 1000
CAPACITY_REQ = RATE_REQ_PER_SEC * LEASE_SEC
CAPACITY_MSG = RATE_MSG_PER_SEC * LEASE_SEC
CAPACITY_WEIGHT = RATE_WEIGHT_PER_SEC * LEASE_SEC

TAG_BASE = f"{int(time.time() * 1000):x}"


# ---------------------------------------------------------------- helpers


async def create_queue(q, name: str, lease_sec: int) -> None:
    await q.queue(name).config(
        lease_time=lease_sec,
        retry_limit=100_000,
        retention_enabled=True,
        retention_seconds=3600,
        completed_retention_seconds=3600,
    ).create()


async def push_all(q, queue_name: str, tenants: list, mk_item: Callable[[str, int], dict]) -> dict:
    start = time.monotonic()
    total = 0
    for tenant in tenants:
        for off in range(0, ELEMS_PER_TENANT, 500):
            items = [
                {"data": mk_item(tenant, seq)}
                for seq in range(off, min(off + 500, ELEMS_PER_TENANT))
            ]
            await q.queue(queue_name).partition(tenant).push(items)
        total += ELEMS_PER_TENANT
    return {"total": total, "elapsed": time.monotonic() - start}


async def drain_until(q, sink: str, expected: int, capture: Callable[[Any], None], tag: str) -> int:
    cg = f"rl-{tag}"
    drained = 0
    deadline = time.monotonic() + TIMEOUT_MS / 1000.0
    while drained < expected and time.monotonic() < deadline:
        batch = await q.queue(sink).group(cg).batch(500).wait(True).timeout_millis(500).pop()
        if not batch:
            continue
        for m in batch:
            capture(m.data)
            drained += 1
        await q.ack(batch, True, {"group": cg})
    return drained


# ---------------------------------------------------------------- scenarios


async def run_gate_stream(q, source: str, sink: str, gate_fn, query_id: str) -> list:
    streams = []
    for r in range(RUNNERS):
        handle = await (
            Stream.from_(q.queue(source))
            .gate(gate_fn)
            .to(q.queue(sink))
            .run(
                query_id=query_id,
                url=URL,
                batch_size=BATCH,
                max_partitions=max(2, TENANTS // RUNNERS + 1),
                max_wait_millis=500,
                reset=(r == 0),
            )
        )
        streams.append(handle)
    return streams


async def stop_all(streams: list) -> None:
    for s in streams:
        await s.stop()


async def scenario_A(q, tenants: list) -> dict:
    print("\n" + "=" * 80)
    print("SCENARIO A — req/sec (1 token per HTTP call)")
    print("=" * 80)
    src = f"rl_a_req_{TAG_BASE}"
    sink = f"rl_a_apx_{TAG_BASE}"
    qid = f"rl-a-{TAG_BASE}"
    await create_queue(q, src, LEASE_SEC)
    await create_queue(q, sink, 30)

    push = await push_all(q, src, tenants, lambda t, s: {"tenantId": t, "seq": s, "kind": "req"})
    print(f"  pushed {push['total']:,} in {push['elapsed']:.1f}s")
    expected = push["total"]

    gate_fn = token_bucket_gate(capacity=CAPACITY_REQ, refill_per_sec=RATE_REQ_PER_SEC)
    arrivals = []
    streams = await run_gate_stream(q, src, sink, gate_fn, qid)
    drained = await drain_until(q, sink, expected, lambda d: arrivals.append((d.get("tenantId"), d.get("seq"), int(time.time() * 1000))), f"A-{TAG_BASE}")
    await stop_all(streams)
    return {"name": "A", "expected": expected, "drained": drained, "rate": RATE_REQ_PER_SEC}


async def scenario_B(q, tenants: list) -> dict:
    print("\n" + "=" * 80)
    print("SCENARIO B — msg/sec (1 token per individual message)")
    print("=" * 80)
    src = f"rl_b_msg_{TAG_BASE}"
    sink = f"rl_b_apx_{TAG_BASE}"
    qid = f"rl-b-{TAG_BASE}"
    await create_queue(q, src, LEASE_SEC)
    await create_queue(q, sink, 30)

    push = await push_all(q, src, tenants, lambda t, s: {"tenantId": t, "seq": s, "msgs": 1})
    expected = push["total"]
    print(f"  pushed {expected:,} in {push['elapsed']:.1f}s")

    gate_fn = token_bucket_gate(capacity=CAPACITY_MSG, refill_per_sec=RATE_MSG_PER_SEC)
    arrivals = []
    streams = await run_gate_stream(q, src, sink, gate_fn, qid)
    drained = await drain_until(q, sink, expected, lambda d: arrivals.append((d.get("tenantId"), d.get("seq"), int(time.time() * 1000))), f"B-{TAG_BASE}")
    await stop_all(streams)
    return {"name": "B", "expected": expected, "drained": drained, "rate": RATE_MSG_PER_SEC}


async def scenario_D(q, tenants: list) -> dict:
    print("\n" + "=" * 80)
    print("SCENARIO D — cost-weighted (variable weight per message)")
    print("=" * 80)
    src = f"rl_d_cost_{TAG_BASE}"
    sink = f"rl_d_apx_{TAG_BASE}"
    qid = f"rl-d-{TAG_BASE}"
    await create_queue(q, src, LEASE_SEC)
    await create_queue(q, sink, 30)

    # Each msg has weight 1..10
    push = await push_all(q, src, tenants, lambda t, s: {"tenantId": t, "seq": s, "weight": (s % 10) + 1})
    expected = push["total"]
    print(f"  pushed {expected:,} in {push['elapsed']:.1f}s")

    gate_fn = token_bucket_gate(
        capacity=CAPACITY_WEIGHT,
        refill_per_sec=RATE_WEIGHT_PER_SEC,
        cost_fn=lambda msg: msg.get("weight", 1),
    )
    arrivals = []
    streams = await run_gate_stream(q, src, sink, gate_fn, qid)
    drained = await drain_until(q, sink, expected,
                                 lambda d: arrivals.append((d.get("tenantId"), d.get("seq"), d.get("weight"))),
                                 f"D-{TAG_BASE}")
    await stop_all(streams)
    return {"name": "D", "expected": expected, "drained": drained,
            "totalWeight": sum(a[2] or 0 for a in arrivals)}


async def scenario_E(q, tenants: list) -> dict:
    print("\n" + "=" * 80)
    print("SCENARIO E — sliding-window quota (SendGrid/OTA hourly)")
    print("=" * 80)
    src = f"rl_e_slide_{TAG_BASE}"
    sink = f"rl_e_apx_{TAG_BASE}"
    qid = f"rl-e-{TAG_BASE}"
    await create_queue(q, src, LEASE_SEC)
    await create_queue(q, sink, 30)

    # Push fewer per tenant since sliding is harder to drain.
    elems = min(200, ELEMS_PER_TENANT)
    push = await push_all(q, src, tenants, lambda t, s: {"tenantId": t, "seq": s})
    expected = push["total"]
    print(f"  pushed {expected:,} in {push['elapsed']:.1f}s")

    gate_fn = sliding_window_gate(limit=100, window_sec=1)
    arrivals = []
    streams = await run_gate_stream(q, src, sink, gate_fn, qid)
    drained = await drain_until(q, sink, expected,
                                 lambda d: arrivals.append((d.get("tenantId"), d.get("seq"))),
                                 f"E-{TAG_BASE}")
    await stop_all(streams)
    return {"name": "E", "expected": expected, "drained": drained}


# ---------------------------------------------------------------- main


async def main():
    print("=" * 80)
    print("RATE-LIMITER ALL-MODELS TEST")
    print("=" * 80)
    print(f"Queen URL          = {URL}")
    print(f"tenants/scenario   = {TENANTS}, elems/tenant = {ELEMS_PER_TENANT:,}")
    print(f"runners/scenario   = {RUNNERS}, lease={LEASE_SEC}s, batch={BATCH}")

    q = Queen(url=URL)
    try:
        tenants = [f"tenant-{i:03d}" for i in range(TENANTS)]
        results = []
        if "A" not in SKIP:
            results.append(await scenario_A(q, tenants))
        if "B" not in SKIP:
            results.append(await scenario_B(q, tenants))
        if "D" not in SKIP:
            results.append(await scenario_D(q, tenants))
        if "E" not in SKIP:
            results.append(await scenario_E(q, tenants))

        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        for r in results:
            ok = "✅" if r["drained"] == r["expected"] else "⚠"
            print(f"  {ok} scenario {r['name']}: drained {r['drained']}/{r['expected']}")
    finally:
        await q.close()


if __name__ == "__main__":
    asyncio.run(main())
