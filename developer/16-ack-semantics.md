# ACK Semantics — ack as commit

> **TL;DR** — In Queen, an ack is an **offset commit**, not a per-message
> checkmark. Acking message N as `completed` commits everything up to and
> including N: silently-unacked messages before N are completed implicitly and
> never redelivered. On top of that, two honesty guarantees hold: **an explicit
> nack is never skipped** by a later completed ack in the same call, and an ack
> that arrives **after** the cursor already committed past its position is
> reported back (`noop:true` for completed, rejection for nacks) instead of
> being silently swallowed.

## 1. Storage model: one cursor, no per-message state

The segment engine keeps **no per-message consumption state**. Each push
appends an immutable segment (a zstd blob of K message "frames") to
`queen.seg_segments`. Consumption is a single cursor per
`(partition, consumer group)` in `queen.partition_consumers`:

```
(next_seq, next_off)  =  "every frame before this position is done"
```

Everything ack does is move that cursor forward. It follows that:

- You cannot express "message 4 failed but message 10 succeeded" as durable
state. The cursor is one number.
- Ack throughput is O(1) per call regardless of batch size — this is the
price/performance trade-off the engine was built around (same family as
Kafka's consumer offsets, unlike SQS's per-message receipt handles).

A non-auto-ack pop takes a **lease** on the cursor: `worker_id` (the `leaseId`
returned by pop), `lease_expires_at`, and `batch_end_seq/off` (the far end of
the delivered range — acks cannot commit past it). The cursor itself does NOT
move at pop time; it moves at ack time.

## 2. The core rule: max-acked-ok wins, clamped at the first explicit signal

One ack call (single `POST /api/v1/ack` or batch `POST /api/v1/ack/batch`)
carries one or more `(transactionId, status)` pairs. Statuses:


| Status                                             | Meaning                                                    |
| -------------------------------------------------- | ---------------------------------------------------------- |
| `completed` (also `success`/`acked`/`ok`, default) | Commit up to here                                          |
| `failed`                                           | Processing failed: retry, charge the retry budget          |
| `retry`                                            | Redeliver without charging the retry budget                |
| `dlq`                                              | Force dead-letter immediately, bypassing remaining retries |


The server (`queen.seg_ack_by_txn_v1`, in
`server/sql/procedures/024_storage_v2_pop_ext.sql`) resolves each txn to its
`(seq, frame_idx)` position via the dedup window and computes, within the
leased range:

1. **Head signal** — the LOWEST position with an explicit `failed`/`dlq`/
  `retry`. The cursor may never advance past it.
2. **Max-ok** — the HIGHEST position acked `completed`, **clamped below the
  head signal**.
3. **New cursor** = just past max-ok. Every silent gap (a message you never
  mentioned) below the new cursor is **implicitly completed** — this is the
   commit semantics, and it is intentional: "ack the last message of the
   batch" commits the whole batch.

Then the head signal decides the action:

- `**failed`** with retry budget left (`batch_retry_count < retry_limit`,
default 3): cursor stays at the completed prefix, lease is released,
counter +1 → the failed message *and everything after it* redeliver.
Completed acks above the failure are redelivered too — **at-least-once
duplicates, never a lost nack**. Budget exhausted → the poison frame is
dead-lettered (`queen.seg_dlq`) or dropped if the queue has DLQ disabled.
- `**retry*`*: same redelivery, no budget charge.
- `**dlq**`: the frame at the head signal is dead-lettered immediately.
- **No signal, cursor reached `batch_end`**: lease released, retry counter
reset — batch complete.
- **No signal, cursor short of `batch_end`** (partial ack): cursor advances,
**lease is kept** so you can keep acking the rest of the batch.

Lease expiry never charges the retry budget: an expired lease simply makes the
range poppable again from the committed cursor.

## 3. Honesty guarantees (the sharp edges, made visible)

Because the API *looks* per-message but the store is an offset, two situations
used to be silently mis-handled. They are now explicit:

### a. Nack + later ack in the SAME call → the nack wins

```
ack batch: {1: ok, 2: failed, 3: ok, 4: ok, 5: ok}
→ cursor commits past 1 only; 2,3,4,5 redeliver; 2 is the retry head
```

Before this guarantee, max-acked-wins would commit past 5 and the `failed` on
2 was ignored: no retry, no DLQ, no error — silent loss. This is the pattern
produced by parallel batch processing (`Promise.allSettled` + per-message
`_status`), so it had to be correct.

### b. Ack arriving BELOW the committed cursor → reported, not swallowed

Once the cursor commits past a position (e.g. you acked message 7, then try
to ack/nack message 3), the store can no longer do anything with it. The
per-item response now says so:

- `completed` below the cursor → `success: true, noop: true` — a harmless
duplicate commit; nothing changed.
- `failed`/`retry`/`dlq` below the cursor → `success: false, error: "already committed: the cursor moved past this message before this ack"` — the
explicit signal cannot be honored, and you must know that. If you see this
error, you acked out of order (see §5).

Wire shape (both `/ack` and `/ack/batch` return a top-level array, one item
per acknowledgment, in request order):

```json
[{"index": 0, "transactionId": "...", "success": true,
  "error": null, "leaseReleased": true, "dlq": false, "noop": false}]
```

## 4. What the clients do (JS / Python / Go)

All three consumer managers behave identically:

- `**.each()**` processes one message at a time and acks in order. On a
handler error it nacks (`failed` with the error message) and **abandons the
rest of the popped batch**: the nack released the lease and clamped the
cursor at the failed message, so the tail is guaranteed to redeliver —
processing it would only produce duplicates and rejected acks.
- `**.batch(N)`** (no `.each()`) hands the whole array to the handler: one
ack for all on success, one nack for all on a throw (all-or-nothing).
- Rejected/noop ack results are logged loudly (`ack-rejected` /
`nack-rejected`); they are no longer invisible.

Contract tests (all three languages, same five cases):
`clients/client-js/test-v2/semantics.js` (items 14-18),
`clients/client-py/tests/test_ack_commit.py`,
`clients/client-go/tests/ack_commit_test.go`.

## 5. Rules of thumb for users

1. **Sequential consumption** (`.each()` or manual in-order acking): just ack
  as you go; on failure nack and stop — the clients do this for you.
2. **Parallel batch processing**: collect every message's outcome and send
  **ONE batch ack** at the end (per-message `_status`). Within a single call
   the clamp rule protects your nacks. Do NOT ack single messages out of
   order while others are still in flight: anything below your highest
   completed ack gets committed, and a later nack on it will be rejected as
   already committed.
3. **Offset-commit style**: acking only the last message of a batch commits
  the whole batch. Legitimate and supported — but it is a statement that
   everything before it is done.
4. **Idempotent handlers are mandatory.** Redelivery happens on lease expiry,
  on any nack (including of *other* messages below yours in the batch), and
   on broker failover. This is standard at-least-once.
5. `retry` = "not now" (no budget charge); `failed` = "broken" (charges one of
  the `retry_limit` attempts, default 3, then DLQ); `dlq` = "poison, get it
   out of my way now".

## 6. History, for the archaeologists

- The old row engine (`003_ack.sql`, C++ broker era) had the same
max-acked-wins implicit ack — including silently swallowing nacks below a
later completed ack.
- The segment engine's first design tracked a durable per-position map
(`batch_positions`, "Trap 2") with contiguous-prefix advancement — true
per-message acks. It was retired by RUSTFIX item 10 to restore the v0.16.0
contract: gaps redelivering (including already-acked positions above a gap)
broke the "ack the last message" pattern and risked livelocks for
offset-commit users.
- The current semantics is the synthesis: implicit ack for silent gaps
(v0.16.0 contract preserved), explicit signals never skipped, below-cursor
acks reported honestly. If a genuine need for per-message guarantees with
out-of-order acking ever materializes, the `batch_positions` machinery is
the starting point for an opt-in strict mode — do not make it the default.

