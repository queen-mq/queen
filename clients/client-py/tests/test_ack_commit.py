"""
Ack-as-commit contract tests (parity with client-js test-v2/semantics.js
items 14-18).

Queen's ack is an offset commit: acking message N implicitly completes every
silent gap before it. These tests pin the honesty guarantees layered on top:

1. An explicit `failed` nack in the same ack call as later completed acks
   clamps the cursor — the nacked message and everything after it redelivers
   (a nack is never silently swallowed by a later success).
2. Same clamp for `retry`, without charging the retry budget.
3. A nack that resolves BELOW the committed cursor is rejected with an
   'already committed' error instead of a silent no-op.
4. A completed ack below the cursor succeeds but is flagged noop=True.
5. `.each()` consumers abandon the rest of the popped batch after a nack
   (the lease is dead: continuing would only produce duplicates).
"""

import asyncio
import time

import pytest


def uniq(prefix: str) -> str:
    return f"test-ackcommit-{prefix}-{int(time.time() * 1000)}"


async def pop_retry(client, queue, group=None, batch=1, tries=20):
    """Pop retrying briefly — rides out push->visibility latency."""
    for _ in range(tries):
        builder = client.queue(queue).batch(batch).wait(False)
        if group:
            builder = builder.group(group)
        msgs = await builder.pop()
        if msgs:
            return msgs
        await asyncio.sleep(0.15)
    return []


async def dlq_count(client, queue):
    res = await client.queue(queue).dlq().limit(50).get()
    return len(res.get("messages") or []) if res else 0


@pytest.mark.asyncio
async def test_nack_not_skipped_by_later_ack_same_call(client):
    queue = uniq("nack-clamp")
    await client.queue(queue).config({"lease_time": 30}).create()

    await client.queue(queue).partition("Default").push(
        [{"data": {"n": n}, "transactionId": f"{queue}-tx-{n}"} for n in range(1, 6)]
    )

    msgs = await pop_retry(client, queue, batch=5)
    assert len(msgs) == 5, f"Expected 5 messages, got {len(msgs)}"

    # One batch ack call: #2 failed, everything else completed (the
    # parallel-processing pattern: per-message outcome, single commit).
    for msg in msgs:
        msg["_status"] = "completed"
    msgs[1]["_status"] = "failed"
    msgs[1]["_error"] = "ack-commit-test poison"
    await client.ack(msgs)

    # The cursor must clamp just before #2: the nack releases the lease and
    # #2..#5 redeliver (later completed acks above a nack are redelivered —
    # at-least-once duplicates, never a lost nack).
    again = await pop_retry(client, queue, batch=10)
    txs = sorted(m["transactionId"] for m in again)
    want = sorted(f"{queue}-tx-{n}" for n in range(2, 6))
    assert txs == want, f"Nacked message skipped by later acks: redelivered {txs}, want {want}"


@pytest.mark.asyncio
async def test_retry_not_skipped_by_later_ack_same_call(client):
    queue = uniq("retry-clamp")
    await client.queue(queue).config({"lease_time": 30}).create()

    await client.queue(queue).partition("Default").push(
        [{"data": {"n": n}, "transactionId": f"{queue}-tx-{n}"} for n in range(1, 4)]
    )

    msgs = await pop_retry(client, queue, batch=3)
    assert len(msgs) == 3, f"Expected 3 messages, got {len(msgs)}"

    msgs[0]["_status"] = "completed"
    msgs[1]["_status"] = "retry"
    msgs[2]["_status"] = "completed"
    await client.ack(msgs)

    again = await pop_retry(client, queue, batch=10)
    txs = sorted(m["transactionId"] for m in again)
    want = sorted(f"{queue}-tx-{n}" for n in (2, 3))
    assert txs == want, f"'retry' skipped by a later ack: redelivered {txs}, want {want}"
    assert await dlq_count(client, queue) == 0, "'retry' clamp leaked into the DLQ"


@pytest.mark.asyncio
async def test_nack_below_cursor_is_rejected(client):
    queue = uniq("late-nack")
    await client.queue(queue).config({"lease_time": 30}).create()

    await client.queue(queue).partition("Default").push(
        [{"data": {"n": n}, "transactionId": f"{queue}-tx-{n}"} for n in range(1, 4)]
    )

    msgs = await pop_retry(client, queue, batch=3)
    assert len(msgs) == 3, f"Expected 3 messages, got {len(msgs)}"

    # Ack the MIDDLE message: cursor commits past #1 and #2, lease stays live.
    mid = await client.ack(msgs[1])
    assert mid["success"], f"Ack of middle message failed: {mid.get('error')}"

    # Nack #1, now below the cursor: the server cannot honor it anymore and
    # must SAY so instead of answering ok.
    late = await client.ack(msgs[0], False, {"error": "too late"})
    assert not late["success"], "Nack below the committed cursor was silently accepted"
    assert "committed" in (late.get("error") or "").lower(), (
        f"Nack below cursor rejected with wrong error: {late.get('error')}"
    )


@pytest.mark.asyncio
async def test_ack_below_cursor_is_noop_flagged(client):
    queue = uniq("late-ack")
    await client.queue(queue).config({"lease_time": 30}).create()

    await client.queue(queue).partition("Default").push(
        [{"data": {"n": n}, "transactionId": f"{queue}-tx-{n}"} for n in range(1, 4)]
    )

    msgs = await pop_retry(client, queue, batch=3)
    assert len(msgs) == 3, f"Expected 3 messages, got {len(msgs)}"

    mid = await client.ack(msgs[1])
    assert mid["success"], f"Ack of middle message failed: {mid.get('error')}"

    # Completed ack of #1, already below the cursor: fine, but flagged noop.
    late = await client.ack(msgs[0])
    assert late["success"], f"Completed ack below cursor failed: {late.get('error')}"
    assert late.get("noop") is True, f"Below-cursor ack not flagged (noop={late.get('noop')})"

    # A normal in-range ack must NOT carry the flag.
    fresh = await client.ack(msgs[2])
    assert fresh["success"] and fresh.get("noop") is not True, (
        f"In-range ack wrongly flagged noop (success={fresh['success']}, noop={fresh.get('noop')})"
    )


@pytest.mark.asyncio
async def test_each_stops_batch_after_nack(client):
    queue = uniq("each-stop")
    await client.queue(queue).config({"lease_time": 30, "retry_limit": 1}).create()

    await client.queue(queue).partition("Default").push(
        [{"data": {"n": n}, "transactionId": f"{queue}-tx-{n}"} for n in range(1, 6)]
    )

    seen = {}  # txn -> handler invocations

    async def handler(msg):
        tx = msg["transactionId"]
        seen[tx] = seen.get(tx, 0) + 1
        if msg["data"]["n"] == 2:
            raise Exception("ack-commit-test poison")

    await (
        client.queue(queue)
        .batch(5)
        .wait(False)
        .idle_millis(3000)
        .each()
        .consume(handler)
    )

    # #2 is poison (retry_limit=1: delivered twice, then DLQ'd). Every other
    # message must be handled EXACTLY once: after the nack the client must
    # abandon the rest of the popped batch (dead lease) instead of processing
    # messages that are guaranteed to redeliver.
    dupes = {
        f"{queue}-tx-{n}": seen.get(f"{queue}-tx-{n}", 0)
        for n in (1, 3, 4, 5)
        if seen.get(f"{queue}-tx-{n}", 0) > 1
    }
    assert not dupes, f"Messages processed more than once after a mid-batch nack: {dupes}"

    missing = [f"{queue}-tx-{n}" for n in (1, 3, 4, 5) if f"{queue}-tx-{n}" not in seen]
    assert not missing, f"Messages never processed: {missing}"

    assert await dlq_count(client, queue) == 1, "Poison message did not end up in the DLQ"
