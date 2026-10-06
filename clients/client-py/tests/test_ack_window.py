"""
Ack-window honesty (2026-07-30, parity with client-js test-v2/ackwindow.js).

log_ack_by_hash_v1 resolves txn hashes through the queen.log_txns sidecar.
A hash that cannot be resolved — purged sidecar row, or a transactionId that
never existed — is correctly NOT acked (cursor stops, frames redeliver). The
bug pinned here: the broker reported those items success=True (they appear in
neither noopHashes nor staleHashes), so the client believed the ack landed
while the cursor never moved; a `failed` nack in that state vanished without
retry charge or DLQ hand-off.

Expected contract (post-fix): unresolvable items answer success=False with an
explicit "unresolvable" error; the real leased batch stays ackable.
"""

import asyncio
import time

import pytest


def uniq(prefix: str) -> str:
    return f"test-ackwindow-{prefix}-{int(time.time() * 1000)}"


async def pop_retry(client, queue, batch=3, tries=40):
    for _ in range(tries):
        msgs = await client.queue(queue).batch(batch).wait(False).pop()
        if msgs and len(msgs) >= batch:
            return msgs
        await asyncio.sleep(0.15)
    return msgs or []


@pytest.mark.asyncio
async def test_ack_unknown_txn_must_fail(client):
    queue = uniq("unknown")
    await client.queue(queue).config({"lease_time": 30}).create()

    await client.queue(queue).partition("Default").push(
        [{"data": {"n": n}, "transactionId": f"{queue}-tx-{n}"} for n in (1, 2, 3)]
    )

    msgs = await pop_retry(client, queue, batch=3)
    assert len(msgs) == 3, f"Expected 3 messages, got {len(msgs)}"

    ghost = {
        "transactionId": f"{queue}-ghost-never-pushed",
        "partitionId": msgs[0]["partitionId"],
        "leaseId": msgs[0].get("leaseId"),
    }

    # Completed ack of a nonexistent txn: must NOT be reported as success.
    r1 = await client.ack(dict(ghost))
    assert not r1["success"], (
        "BUG: completed ack of a never-pushed transactionId reported success"
    )
    assert "unresolv" in (r1.get("error") or "").lower(), (
        f"unknown-txn ack rejected with wrong error: {r1.get('error')}"
    )

    # Failed nack of a nonexistent txn: pre-fix this was silently swallowed —
    # no retry charge, no DLQ — while the client was told ok.
    r2 = await client.ack(dict(ghost), False, {"error": "ghost nack"})
    assert not r2["success"], (
        "BUG: failed nack of a never-pushed transactionId reported success"
    )
    assert "unresolv" in (r2.get("error") or "").lower(), (
        f"unknown-txn nack rejected with wrong error: {r2.get('error')}"
    )

    # The rejected calls must not have burned the lease: real batch still acks.
    r3 = await client.ack(msgs)
    assert r3["success"], f"real batch ack failed after ghost rejections: {r3.get('error')}"


@pytest.mark.asyncio
async def test_batch_ack_under_a_stale_lease_reports_failure(client):
    """The broker answers 200 with success=False per item when it refuses an
    ack; a batch ack must report that, not the HTTP status."""
    queue = uniq("stale-lease")
    await client.queue(queue).config({"lease_time": 30}).create()
    await client.queue(queue).partition("Default").push(
        [{"data": {"n": n}, "transactionId": f"{queue}-tx-{n}"} for n in (1, 2)]
    )

    msgs = await pop_retry(client, queue, batch=2)
    assert len(msgs) == 2, f"Expected 2 messages, got {len(msgs)}"

    stale = [{**m, "leaseId": "01a00000-0000-7000-8000-000000000000"} for m in msgs]
    refused = await client.ack(stale)
    assert refused["success"] is False, f"refused batch ack reported success: {refused}"
    assert "lease" in (refused.get("error") or "").lower(), refused.get("error")
    assert [r["success"] for r in refused["results"]] == [False, False]

    accepted = await client.ack(msgs)
    assert accepted["success"] is True, f"real batch ack failed: {accepted.get('error')}"


@pytest.mark.asyncio
async def test_renew_of_a_released_lease_reports_failure(client):
    """The broker answers 200 with success=False, renewed=0 when it extended
    nothing; renew() must report that."""
    queue = uniq("renew")
    await client.queue(queue).config({"lease_time": 30}).create()
    await client.queue(queue).partition("Default").push(
        [{"data": {"n": 1}, "transactionId": f"{queue}-tx-1"}]
    )

    msgs = await pop_retry(client, queue, batch=1)
    assert len(msgs) == 1

    live = await client.renew(msgs[0])
    assert live["success"] is True and live.get("renewed") == 1, live
    assert live["newExpiresAt"]

    acked = await client.ack(msgs[0])
    assert acked["success"] is True, acked.get("error")

    gone = await client.renew(msgs[0])
    assert gone["success"] is False, f"renew of a released lease reported success: {gone}"
    assert gone.get("renewed") == 0
    assert gone["newExpiresAt"] is None
