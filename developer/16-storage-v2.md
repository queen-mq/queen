# 16 — Storage v2: the segments engine

Storage v2 is Queen's second, **per-queue opt-in** message engine. Instead of one row per message (`queen.messages`, the "rows" engine), it stores **one row per segment of K messages**: the broker packs the messages of a push into length-prefixed frames, compresses them together with zstd, and inserts a single row. The wire format is unchanged — clients cannot tell which engine served them.

Why it exists: per-tuple and per-index overhead in the rows engine is paid once per *message*; the segments engine pays it once per *segment* and compresses similar payloads together. Measured head-to-head numbers are [below](#measured-numbers).

This page is the developer map: where the engine lives, how it works, how a queue opts in, and — important — what it does **not** do yet.

---

## Where it all lives

**SQL** (schema `q2`, applied idempotently at boot like every procedures file — the whole schema+procedures apply is serialized across concurrently booting brokers by a `pg_advisory_lock(hashtext('queen_schema_apply'))` session lock in `initialize_schema`, so parallel `CREATE OR REPLACE FUNCTION` can't trip PostgreSQL's "tuple concurrently updated" catalog race):

```
lib/schema/procedures/
├── 023_storage_v2.sql               ← q2 schema + push/pop/ack/renew core
├── 024_storage_v2_pop_ext.sql       ← wildcard pop, ack-by-txn fallback, pending probe
├── 025_storage_v2_dlq.sql           ← retry tracking + poison-message DLQ
├── 026_storage_v2_maintenance.sql   ← retention sweep, eviction, transactions
└── 027_storage_v2_observability.sql ← stats, dashboard + prometheus procedures
                                       (redefines queen.* observability functions
                                        to cover both engines)
```

The opt-in flag itself is on the **v1** side: `queen.queues.storage VARCHAR(16) NOT NULL DEFAULT 'rows'` (added in `lib/schema/schema.sql`). Queue *configuration* (lease time, retry limit, retention, max size, …) stays in `queen.queues` for both engines; `q2` only stores what the engine needs (`q2.queues.dedup_window_seconds` plus the mechanical state below), correlated by queue name.

**C++**:

| What | Where |
|---|---|
| Frame codec, zstd, base64, in-process lease registry | `server/include/queen/storage_v2.hpp` |
| Route handlers: `handle_push_v2`, `handle_pop_v2`, `handle_pop_wildcard_v2`, `try_handle_ack_v2`, cross-request fusion | `server/src/routes/storage_v2_routes.cpp` (+ `server/include/queen/routes/storage_v2_routes.hpp`) |
| Engine routing (`is_segment_queue`) at the endpoints | `server/src/routes/push.cpp`, `pop.cpp`, `ack.cpp`, `transactions.cpp`, `leases.cpp` |
| Opt-in flag + flip guard + `dedupWindowSeconds` knob | `server/src/routes/configure.cpp` |
| `storage` flag in the queue-config cache | `server/src/services/shared_state_manager.cpp`, `server/include/queen/caches/queue_config_cache.hpp` |
| Background sweeps (`q2.retention_sweep_v1`, `q2.evict_v1`) | `server/src/services/retention_service.cpp`, `server/src/services/eviction_service.cpp` |
| File-buffer replay routed by storage flag | `server/src/managers/async_queue_manager.cpp` |
| `queen_queue_depth_*` metrics | `server/src/routes/prometheus.cpp` |
| Streams v1-only guards | `server/src/routes/streams/register_query.cpp`, `streams/cycle.cpp` |

Build note: the segments engine links **zstd** (`-lzstd`; on macOS the Makefile picks up the Homebrew keg `$(BREW_PREFIX)/opt/zstd`).

---

## Architecture

### Segments and frames

A segment row (`q2.segments`) is `(partition_id, seq, created_at, msg_count, blob)` — and the **primary key `(partition_id, seq)` is the only index**. The `blob` is `zstd(concatenated frames)`, stored `EXTERNAL` so TOAST doesn't waste CPU re-compressing already-compressed bytes. Blobs travel base64-encoded through the text-mode libpq layer.

Frame layout (little-endian, packed/unpacked by `queen::storage_v2::pack_frames` / `unpack_frames`):

```
u32  frame_len            length of everything after this field
u8   flags                bit0: trace_id present, bit1: producer_sub present,
                          bit2: payload encrypted (envelope JSON)
u8[16] message_id         UUID bytes
[u8[16] trace_id]         iff flags & 1
u16  txn_len, txn bytes
[u16 psub_len, psub]      iff flags & 2
payload bytes             rest of the frame
```

### seq: the per-partition serializer

Segment identity is `(partition_id, seq BIGINT)`. `seq` is allocated by `UPDATE q2.partitions SET last_seq = last_seq + 1` — the row lock serializes concurrent pushes to the same partition (replacing the v1 PUSHSER advisory lock) and guarantees **seq order == commit order**: a pop can never see seq N+1 without N. `created_at` is stamped with `clock_timestamp()` *after* the serializer, so it is monotonic per partition in commit order (the invariant time-based retention needs — the same invariant [05 — Database schema §7](05-database-schema.md) documents for v1).

`q2.partitions` also carries the retention watermark (below) and has `fillfactor = 70` so the one-UPDATE-per-segment traffic stays HOT.

Queues and partitions are created lazily on first push, exactly like v1 (`q2.push_segment_v1` does the `INSERT ... ON CONFLICT DO NOTHING` dance itself).

### Cursor consumption — no per-message state

Consumption state is one row per (partition, consumer group) in `q2.consumers`: a cursor `(next_seq, next_off)` meaning "frames `[0..next_off)` of segment `next_seq` are consumed; every segment below `next_seq` is fully consumed". There is no per-message bookkeeping anywhere.

The cursor has an explicit guard for the segment-deleted-by-retention case: the offset applies only if the first row read has exactly `seq = next_seq`; otherwise consumption restarts at the head of the first surviving segment.

The pop procedures return raw segment rows plus slicing bounds (`seq`, `startOff`, `take`); the **broker** decompresses and slices — SQL never looks inside a blob. A non-auto-ack pop takes the (partition, group) lease: `worker_id` on the consumer row, which doubles as the wire `leaseId`, plus `batch_end_seq/off` so an ack can never advance past the leased batch.

### Ack — absolute position, contiguous prefix

The v1 wire ack carries `(transactionId, partitionId, leaseId)`; for segment queues the broker resolves txn → position through the in-process `LeaseRegistry` (`server/include/queen/storage_v2.hpp`), populated at pop time. When a batch completes (every message acked, or any nack closes it early), the broker calls `q2.ack_segments_v1` with the **highest contiguous acked-OK prefix** — a nack never advances the cursor past the failed message, so redelivery restarts exactly there (which is also what drives the retry counter, below). Partial ack mid-segment = redelivery from the exact frame.

The registry is per-process. On a registry miss where the partition belongs to `q2`, the broker falls back to `q2.ack_by_txn_v1` (024): each txn is resolved to its position through the dedup window and the same contiguous-prefix rule is applied in SQL. Txns that can't be resolved (dedup disabled, or entry already purged) count as **not acked** — redelivery over data loss, by design. Transactions use the same two-tier resolution: `/api/v1/transaction` acks resolve through the local registry when the pop was served by this broker, and are otherwise shipped inside the `q2.transaction_wire_v1` payload in the txns form (`{"partitionId","group","worker","txns":[{"txn","ok"},...]}`), resolved SQL-side by the same `ack_by_txn` machinery **atomically with the transaction's pushes** — cross-broker and post-restart transactions work. (Engine routing still needs the request to be v2-shaped: a push to a segments queue, or at least one ack the local registry resolves. A transaction consisting *solely* of acks for leases held elsewhere is indistinguishable from a rows transaction on the wire and runs the v1 path — pure-ack batches belong on `/api/v1/ack`, which resolves the engine per partition.)

An explicit `status: "dlq"` ack (v1 parity) disposes the position like an acked-OK one for cursor contiguity and additionally files the frame's snapshot into `q2.dlq`.

Lease renewal (`POST /api/v1/lease/:leaseId/extend`) can't tell a v1 lease from a v2 one, so `leases.cpp` renews both engines (`queen.renew_lease_v2` + `q2.renew_lease_v1`) and merges: success if either renewed.

### Dedup — a window, not an index

Dedup lives in a side table `q2.dedup (partition_id, hashtextextended(txn), …)` bounded by `q2.queues.dedup_window_seconds` (default **3600**, `0` = off). Probe+insert is a single `INSERT ... ON CONFLICT` statement under the partition serializer, so it's race-free within a partition. A duplicate aborts the whole call (`QDUP` exception → rollback, so no seq gap and no partial state); the broker resolves the dup list with `q2.find_dups_v1`, repacks the segment without the dup frames, answers `duplicate` for them, and retries — a rare path by design.

When the broker generated the `transactionId` itself (the default), the dedup probe is **skipped entirely** — work v1 cannot avoid, because its unique index is unconditional.

The dedup window doubles as the txn → position resolver for `ack_by_txn`, DLQ and traces. Its steady-state size is O(rate × window), independent of retention/backlog; expired entries are purged by the retention sweep.

### Retry and DLQ

Attempt state lives on the (partition, group) consumer row (025), not per message: a non-auto-ack delivery whose batch **starts at the same position** as the previous delivery is a redelivery (nack or lease expiry left the cursor in place) → `attempt_count` increments; a batch starting anywhere else is fresh work → reset to 1. Acks need no extra writes: advancing the cursor moves the start position, which resets the counter naturally. The position match is retention-aware, so a redelivery whose start was shifted by a sweep still counts against the same attempt series (v1 retry-accounting parity).

When a delivery's attempt count exceeds the queue's `retry_limit` (and the queue has DLQ enabled), the broker — the only party that can decompress frames — extracts the poisoned **head** frame and calls `q2.dlq_head_v1` with a payload *snapshot*. That files the `q2.dlq` row, advances the cursor past that single frame, resets the attempt state, releases the lease, and the broker re-pops once so the client gets a fresh batch. Storing a snapshot (no FK into `q2.segments`) keeps the DLQ decoupled from segment retention — v1's FK-cascade coupling silently drops DLQ rows when the source row is deleted.

### Retention — watermark, not head-scan

`q2.partitions.retention_seq` is a low-watermark: every segment with `seq < retention_seq` is deleted. The boundary walk (`q2.retention_boundary_v1`) restarts from where the previous sweep stopped, never from the dead head of an index — the classic queue-table head-scan poison — and the segments table needs zero secondary indexes to support it.

`q2.retention_sweep_v1` (026) runs inside the existing `RetentionService` cycle (same `RETENTION_INTERVAL`, same advisory lock `737001`, so replicas never double-sweep) and mirrors the v1 semantics, reading configuration from `queen.queues`:

- **`retention_seconds`** ("all"): segments older than the cutoff are deleted regardless of consumption. Gated on `retention_enabled AND retention_seconds > 0`.
- **`completed_retention_seconds`** ("consumed"): v2 has no per-message consumed flag, so a segment counts as *consumed* when its `seq` is below **every** consumer group's `next_seq` for the partition. Consumed segments older than the cutoff are deleted regardless of `retention_seconds`. ⚠ A partition with **no consumer groups has nothing consumed** — completed-retention alone never deletes from a partition nobody has ever popped (v1 likewise requires a consumer row).

The sweep also purges expired `q2.dedup` entries in LIMIT-batched deletes.

`q2.evict_v1` (max_queue_size enforcement) runs analogously inside the `EvictionService` cycle.

### Observability

027 redefines the shared observability procedures (`queen.get_prometheus_metrics_v1`, `queen.get_consumer_groups_v4`, `queen.has_pending_messages`, `queen.get_queue_messages_v1`, `queen.list_messages_v1`) to cover both engines, and adds `q2.stats_v1`. `/metrics` gains two per-queue gauges labeled with the engine: `queen_queue_depth_total_messages` and `queen_queue_depth_pending_messages` (`storage="rows"|"segments"`).

---

## Opting in

The engine is selected per queue at configure time:

```json
POST /api/v1/configure
{ "queue": "orders", "options": { "storage": "segments" } }
```

- `options.storage` must be `'rows'` or `'segments'` (400 otherwise). Omitting it preserves the queue's current engine; new queues default to `'rows'`.
- **Flip guard (fail-closed)**: switching engines is only allowed while the engine being abandoned holds **no data** for that queue. Whenever a storage value is requested and the queue isn't known to already run on it, configure probes the opposite engine and rejects the flip if messages exist — including when the config lookup itself fails. New/empty queues pass at the cost of one cheap probe.
- `encryptionEnabled: true` works on segments queues exactly like on rows queues: the push path encrypts each frame's payload into an `{"encrypted","iv","authTag"}` envelope (frame flag bit 2) and the pop path decrypts it. (The slice-1 reject is gone.)
- `options.dedupWindowSeconds` (segments queues only) sets the dedup window, persisted to `q2.queues.dedup_window_seconds` via `q2.set_queue_options_v1` after a successful configure (`0` disables dedup). It must be a non-negative int32; sending it for a rows queue is rejected (400) instead of silently ignored — the rows engine dedups through its unconditional unique index, not a window.
- `configure_queue_v1` predates the flag, so the broker persists `storage` with a follow-up `UPDATE queen.queues` after a successful configure, then refreshes the queue-config cache (`SharedStateManager`) that every routing decision reads. The cache (and the reply's echoed `options.storage` / `options.dedupWindowSeconds`) update only when **every** persisted step succeeded; a half-applied configure answers 500 and drops the cache entry so the next access refetches DB truth.

After that, everything is transparent: `push.cpp` / `pop.cpp` / `ack.cpp` / `transactions.cpp` route by `routes_v2::is_segment_queue(queue)` (config cache; unknown queues default to v1) and emit the exact v1 wire responses. File-buffer failover also works: buffered events are storage-agnostic JSON and replay routes each event by the queue's *current* flag (`async_queue_manager.cpp`).

---

## Cross-request fusion

One HTTP push == one segment per (queue, partition) would degenerate to 1-frame segments for single-message producers — exactly the shape the engine is worst at. So `handle_push_v2` parks frames in a **per-worker-thread accumulator**, one pending segment per (queue, partition), and flushes ONE fused segment when it reaches a frame budget or its oldest frame ages out — the same self-clocked group-commit philosophy as the v1 push engine. Each request parks until the flush carrying *its* frames commits, then gets its own per-item results (a duplicate `transactionId` inside the parked window answers `duplicate` with the first occurrence's `message_id`).

| Variable | Default | Meaning |
|---|---|---|
| `QUEEN_V2_FUSION_HOLD_MS` | 15 | Max age of the oldest parked frame before flush. **`0` bypasses the accumulator entirely** (every request flushes inline — exact pre-fusion behavior, the zero-risk switch). |
| `QUEEN_V2_FUSION_FRAMES` | 100 | Frame count that triggers an immediate flush (clamped to ≥ 1). |

Read once at first use (restart to change). Fusion is per uWS worker: worst case the same (queue, partition) yields one segment per worker instead of one. Parked frames are deliberately **not** flushed on shutdown — their HTTP requests die with the process (no 201 was ever sent) and producers retry idempotently; the file buffer covers DB-down, not process death.

---

## Wire compatibility

**The v1 wire format is preserved byte-for-byte.** Push, pop (specific and wildcard, long-poll included), ack (single and batch), lease renewal and transactions on a segments queue return the same JSON shapes, the same status codes and the same failover behavior (`buffered` 201) as the rows engine. Clients, SDKs and the proxy need zero changes; a queue can opt in without anyone downstream noticing — except for the [limits](#limits-read-before-relying-on-v2) below.

One server-wide wire fix landed with this work (applies to **both** engines identically, so v1/v2 parity holds): empty responses — e.g. an empty pop — now answer **204 with no body and no `Content-Length`**, per RFC 9110. Historically they were sent as `204` *with* a JSON body, which strict HTTP clients (Go `net/http`, node's llhttp, raw curl pipelines) reject at the wire level. `ResponseRegistry::deliver_string` suppresses the payload whenever the status is 204; clients must treat 204 as "no messages" without parsing a body (undici/fetch and every Queen SDK already did).

---

## Measured numbers

From the spike that validated the design (`benchmark-queen/storage-v2-spike/README.md`, run-20260710-112620-K50): 600k messages, ~256 B JSON payloads, K=50, 8 partitions, 4 workers, PG 17 pinned to 2 cores / `shared_buffers` 512MB / `synchronous_commit=on`.

| metric | v1 (rows) | v2 (segments) | gain |
|---|---|---|---|
| ingest msg/s | 36.2k | 50.7k | **1.4x** (at −27% PG CPU) |
| ingest WAL/msg | 970 B | 392 B (119 B without dedup) | **2.5x / 8.1x** |
| heap B/msg | 512 | 120 | **4.3x** |
| index B/msg | 242 | 1.4 | **173x** |
| total B/msg (incl. dedup window) | 754 | 267 | **2.8x** |
| segments table only B/msg | 754 | 122 | **6.2x** |
| consume msg/s | 22.4k | 116k | **5.2x** (at 1/4 the CPU) |
| consume WAL/msg | 754 B | 4 B | **~190x** |
| retention sweep of 600k | 5.4 s | 0.34 s | **16x** |
| retention WAL/msg | 566 B | 137 B | **4.1x** |
| group zstd ratio | — | 3.32x | |

Caveats (the spike README's "honesty of the comparison" section): the numbers are conservative for v2 in some ways (the driver did zstd in single-threaded JS; v1 wasn't charged for `update_partition_lookup_v1`), and the 3.32x compression ratio comes from synthetic payloads with repeated keys — revalidate on real production payloads.

---

## Limits (read before relying on v2)

These are the current, deliberate gaps of the integration. Every one is enforced in code — the references are the source of truth.

1. **Streams are v1-only.** Registering a stream whose source *or* sink is a segments queue is rejected with 501 (`streams/register_query.cpp`); `streams/cycle.cpp` guards its sink pushes the same way. Rationale: streams read and write through v1 SQL only — a segments source/sink would strand messages.

2. **Wildcard pops skip poison eviction.** The retry counter and DLQ head-eviction run on *specific-partition* pops only; the wildcard wire (`q2.pop_wildcard_wire_v1`, 024) does not carry `attempt` yet, so a poison message popped through the wildcard path keeps being redelivered instead of being dead-lettered. A consumer that pops the partition directly will evict it. See the `PopWaitJob` comment in `storage_v2_routes.cpp`.

3. **Completed-retention needs at least one consumer group.** A segment counts as consumed only when its seq is below every group's cursor; on a partition with no consumer rows, `completed_retention_seconds` deletes nothing (use `retention_seconds` for unconsumed data). Deletion granularity is the segment: a segment becomes reclaimable when *fully* consumed/expired.

4. **Dedup is a window, not forever.** v1's unique index dedups a retried `transactionId` against any message still in the table — effectively for the message's whole retention lifetime. v2 dedups only inside `dedup_window_seconds` (default 3600, `0` = off, settable per queue via `options.dedupWindowSeconds`): a duplicate arriving *after* the window is accepted as a new message. The window also bounds `ack_by_txn` resolution (an unresolvable txn is redelivered, not lost) — and therefore cross-broker transaction acks and post-restart acks too. Size the window to your producers' maximum retry horizon.

5. **Transactions support push and ack operations only** (400 otherwise), and mixed rows/segments transactions are rejected: every queue in the transaction must be on the same engine (400 on a rows *push*; 409 on an ack whose `partitionId` cannot be a q2 partition). Rejections and rollbacks answer the v1 failure body (`{transactionId, success:false, error, results}`). See `server/src/routes/transactions.cpp`.

Two limits documented here previously are **gone**: transaction acks no longer need to reach the broker that served the pop (the old 409 — cross-broker/post-restart acks now resolve SQL-side in the txns form, atomically with the pushes), and `encryptionEnabled` is fully supported on segments queues (the old configure-time 400).

---

## See also

- `benchmark-queen/storage-v2-spike/README.md` — design rationale, benchmark methodology, correctness suite
- [05 — Database schema](05-database-schema.md) — the v1 schema this engine sits beside
- [09 — Retention](09-retention.md) — the v1 retention service the v2 sweep plugs into
- `server/ENV_VARIABLES.md` — fusion knobs and everything else
