# 18 — Log engine: offset re-addressing + seg-level dedup + R1 retention (design & implementation spec)

Status: IMPLEMENTED (2026-07-22, same day as spec). Greenfield replacement of
the seg_* family on branch `rustserverandstorage`. No data migration (dev
version). Acceptance: JS suite 117/117, Go suite + streams green (uncached),
Py 141 passed/2 skipped, window=0 explicit-ack landmine verified dead,
duplicate pushes return original mids, 8-min R1 soak clean. Deviations from
this spec are documented in the implementing agents' notes; the load-bearing
ones are annotated inline below.
Coordination semantics are UNCHANGED (per-batch claim, lease pop→ack, no
assignment). The HTTP wire contract is UNCHANGED: same endpoints, same JSON
keys; values that used to carry `(seq, frame_idx)` now carry a single
per-partition message offset as an opaque token. Acks stay txn-addressed on the
wire. The JS/Go/Py client suites are the acceptance gate.

## 1. Naming

Tables: `queen.log_queues`, `queen.log_partitions`, `queen.log_segments`,
`queen.log_txns`, `queen.log_consumers`. Shared tables `queen.consumer_watermarks`
and `queen.consumer_groups_metadata` stay as-is. The rows engine (001-018 SQL)
is untouched. All new SQL functions are `queen.log_*_v1`.

Old seg_* SQL files (023-034, patch_multi_v2.sql, 099) are deleted from the
repo and from `schema.rs`; a new `022_log_drop_legacy.sql` idempotently drops
every legacy seg_* function and table (IF EXISTS) so existing dev DBs boot
clean. `028_storage_v2_migrate.sql` (rows→seg migration) is retired; a
rows→log migration can be written later if ever needed.

New SQL files (applied in order after the 019-022 streams files, all
idempotent; the old seg files 023-034 + 099 + patch_multi_v2.sql are deleted
from the repo and from the schema.rs roster):

- `040_log_drop_legacy.sql` — drop seg_* leftovers (dynamic DO block over pg_proc/pg_class for seg_% names in schema queen; also DROP COLUMN IF EXISTS for the seg-fold columns on queen.partition_consumers: next_seq, next_off, batch_end_seq, batch_end_off, batch_positions, attempt_seq, attempt_off, attempt_count, total_consumed)
- `041_log_schema.sql` — tables + helpers (§2)
- `042_log_push.sql` — push (§4): log_push_one_v1 + log_push_multi_v1 + log_segment_at_v1
- `043_log_pop.sql` — pop (§6): specific, wildcard wire, wildcard bin, discover, has_pending
- `044_log_ack.sql` — ack (§7): log_ack_v1, log_ack_by_hash_v1, renew, DLQ, transaction_wire
- `045_log_maintenance.sql` — retention/txns/evict step functions (§8)
- `046_log_streams.sql` — streams port (former 029_seg_streams)
- `047_log_admin.sql` — consumer-group admin, messages browsing/observability, traces glue (former seg parts of 027/030/031)
- `048_log_stats.sql` — stats refresh + analytics/status/prometheus support (former 034 + seg parts of 007/013/015/018 used by handlers)

Push internals refinement (supersedes §4's savepoint note): **no savepoints at
all**. `log_push_one_v1(p_queue, p_partition, p_msg_count, p_hashes,
p_verified, p_blob, p_pid UUID DEFAULT NULL, p_window INT DEFAULT NULL)`
resolves pid/window only when not supplied; when the queue has dedup ON it
takes the partition row lock FIRST via `SELECT last_offset ... FOR UPDATE`,
probes the span `(GREATEST(p_verified, txns_start-1), last_offset]` (time-capped
by the window) BEFORE allocating, and on a duplicate returns
`{"status":"duplicate","dups":[{"i":k,"off":O}...]}` having written NOTHING
(no rollback needed); only a clean probe proceeds to the allocator UPDATE +
inserts. Dedup-off queues skip the SELECT and use the single
`UPDATE ... RETURNING` fast path directly. `log_push_multi_v1` does the §4
set-based provisioning skip + ascending-id pre-lock, then calls
log_push_one_v1 per segment with resolved pid/window; per-segment duplicate
results coexist with other segments' commits without subtransactions.
`log_transaction_wire_v1` treats a duplicate return as an error and RAISEs
(ERRCODE unique_violation, 'QDUP ...') so the whole transaction rolls back —
its all-or-nothing contract is preserved.

Post-condition (hard check): `grep -rn "queen\.seg_" server/src server/sql` →
zero hits outside `040_log_drop_legacy.sql`; `grep -rn "seg_push\|seg_pop\|seg_ack" server/src` → zero.

## 2. Schema (023_log_schema.sql)

```sql
CREATE TABLE IF NOT EXISTS queen.log_queues (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,
    lease_time INTEGER NOT NULL DEFAULT 60,
    retention_seconds INTEGER NOT NULL DEFAULT 3600,
    dedup_window_seconds INTEGER NOT NULL DEFAULT 3600,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS queen.log_partitions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_id UUID NOT NULL REFERENCES queen.log_queues(id) ON DELETE CASCADE,
    name TEXT NOT NULL DEFAULT 'Default',
    last_offset  BIGINT NOT NULL DEFAULT -1,  -- allocator; next base = last_offset+1
    log_start    BIGINT NOT NULL DEFAULT 0,   -- segment retention watermark
    txns_start   BIGINT NOT NULL DEFAULT 0,   -- log_txns purge watermark
    last_write_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),  -- QUANTIZED (see push)
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (queue_id, name)
);
ALTER TABLE queen.log_partitions SET (fillfactor = 70);
CREATE INDEX IF NOT EXISTS idx_log_partitions_queue_write
    ON queen.log_partitions (queue_id, last_write_at);

CREATE TABLE IF NOT EXISTS queen.log_segments (
    partition_id UUID NOT NULL REFERENCES queen.log_partitions(id) ON DELETE CASCADE,
    base_offset BIGINT NOT NULL,
    end_offset  BIGINT NOT NULL,   -- inclusive; msg_count = end_offset-base_offset+1
    created_at  TIMESTAMPTZ NOT NULL,
    blob BYTEA NOT NULL,           -- frame packing + zstd, UNCHANGED from seg engine
    PRIMARY KEY (partition_id, base_offset)
);
ALTER TABLE queen.log_segments ALTER COLUMN blob SET STORAGE EXTERNAL;
ALTER TABLE queen.log_segments SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_insert_scale_factor = 0.05,
    autovacuum_vacuum_cost_limit = 4000,
    autovacuum_vacuum_cost_delay = 0,
    toast.autovacuum_vacuum_scale_factor = 0.02,
    toast.autovacuum_vacuum_cost_limit = 4000,
    toast.autovacuum_vacuum_cost_delay = 0
);

-- One row per segment; 16 bytes per frame, frame order. ALWAYS written (dedup
-- on or off): ack-by-hash resolution depends on it. Purged by its own
-- watermark (txns_start), window = GREATEST(dedup_window, completed_retention, 900s).
CREATE TABLE IF NOT EXISTS queen.log_txns (
    partition_id UUID NOT NULL,
    base_offset BIGINT NOT NULL,
    end_offset  BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL,
    hashes BYTEA NOT NULL,         -- 16*msg_count bytes, xxh3_128 big-endian
    PRIMARY KEY (partition_id, base_offset)
);
ALTER TABLE queen.log_txns SET (
    autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_cost_delay = 0);

CREATE TABLE IF NOT EXISTS queen.log_consumers (
    partition_id UUID NOT NULL REFERENCES queen.log_partitions(id) ON DELETE CASCADE,
    consumer_group TEXT NOT NULL DEFAULT '__QUEUE_MODE__',
    committed BIGINT NOT NULL DEFAULT -1,  -- last acked offset; next wanted = committed+1
    batch_end BIGINT,                      -- inclusive end of leased batch; NULL = no lease
    worker_id TEXT,
    lease_expires_at TIMESTAMPTZ,
    lease_acquired_at TIMESTAMPTZ,
    batch_retry_count INTEGER NOT NULL DEFAULT 0,
    attempt_offset BIGINT,                 -- redelivery detection (former attempt_seq/off)
    attempt_count INTEGER NOT NULL DEFAULT 0,
    total_consumed BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (partition_id, consumer_group)
);
ALTER TABLE queen.log_consumers SET (fillfactor = 50);

-- Helper: explode a 16B-stride hash blob into (idx, h) rows. idx is 0-based.
CREATE OR REPLACE FUNCTION queen.log_unnest_hashes(p BYTEA)
RETURNS TABLE (idx INT, h BYTEA) LANGUAGE sql IMMUTABLE AS $$
    SELECT g, substring(p FROM g*16 + 1 FOR 16)
    FROM generate_series(0, octet_length(p)/16 - 1) g
$$;
```

Invariants: per partition, `[base_offset, end_offset]` ranges are disjoint and
allocated monotonically under the `log_partitions` row lock (the same
serializer as the old `last_seq`). Rollback of a push rolls back the allocator
bump → no gaps in steady state; gaps from retention are tolerated by the pop
scan exactly like the old engine tolerated seq gaps. `created_at` is monotone
per partition in commit order (stamped after the row lock — PUSHSER invariant,
needed by time-based retention and timestamp subscriptions).

## 3. Hashing (broker-side only)

The broker computes `xxh3_128(txn utf8 bytes)` (crate `xxhash-rust`, feature
`xxh3`), serialized big-endian 16B. SQL never hashes — it only stores and
compares bytea. Ack-by-txn on the wire → broker maps txn→hash → SQL resolves
hash→offset via `log_txns`. 128-bit kills the 64-bit collision concern.

## 4. Push (024_log_push.sql)

```
queen.log_push_multi_v1(
    p_queues     TEXT[],
    p_partitions TEXT[],
    p_msg_counts INT[],
    p_hashes     BYTEA[],   -- aligned; 16*count bytes each, frame order
    p_verified   INT8[],    -- aligned; broker dedup watermark per segment (see protocol)
    p_blobs      BYTEA[]
) RETURNS JSONB  -- input-order array:
                 --   {"status":"queued","baseOffset":B,"createdAt":"..."}
                 -- | {"status":"duplicate","dups":[{"i":k,"off":O}, ...]}
```

Structure follows the old `seg_push_segments_multi_v2` (typed arrays, no jsonb
on the hot path):
1. Steady-state provisioning skip: one existence count over
   `unnest(p_queues, p_partitions)`; only when something is missing run the
   three `INSERT .. ON CONFLICT DO NOTHING` provisioning statements
   (log_queues, queen.queues config row with storage='segments' — the public
   configure contract keeps the value 'segments'! — and log_partitions).
2. Deadlock-safe pre-lock: ONE set-based
   `PERFORM ... ORDER BY p.id FOR UPDATE OF p` over the bundle's partitions
   (ascending id — the global lock order shared with acks/retention).
3. `v_now := clock_timestamp()` once, after the locks.
4. Per segment, in ascending partition id order:
   - allocate: `UPDATE queen.log_partitions SET last_offset = last_offset + cnt,
     last_write_at = CASE WHEN clock_timestamp() - last_write_at > interval '1 second'
     THEN clock_timestamp() ELSE last_write_at END WHERE id = pid
     RETURNING last_offset` → `base := ret - cnt + 1`, `prev_last := base - 1`.
     (The CASE is the HOT fix: last_write_at is indexed; bumping it at most
     1/s per partition keeps ~all allocator updates HOT. The candidate scan
     already tolerates 2 minutes of slack, so 1s staleness is absorbed.)
   - dedup probe — ONLY if the queue's dedup_window_seconds > 0 AND
     `prev_last > p_verified[i]` (the broker already vouches for
     `(-∞, p_verified[i]]`). Probe span rows:
       `FROM queen.log_txns t WHERE t.partition_id = pid
        AND t.end_offset > GREATEST(p_verified[i], txns_start-1)
        AND t.created_at >= v_now - window`
     joined `queen.log_unnest_hashes(t.hashes) th` × incoming
     `queen.log_unnest_hashes(p_hashes[i]) ih ON th.h = ih.h`
     → collect `(ih.idx, t.base_offset + th.idx)` dup pairs.
     `p_verified[i] = -1` means "no broker cache — probe the whole window".
   - Dedup-enabled segments run inside a savepoint (BEGIN/EXCEPTION) like v2;
     a duplicate raises → only that segment rolls back (allocator bump
     included), result = duplicate + dups list. Dedup-off segments run with NO
     savepoint (v2 fast path preserved).
   - `INSERT INTO queen.log_segments (partition_id, base_offset, end_offset,
      created_at, blob) VALUES (pid, base, base+cnt-1, v_now, p_blobs[i])`
   - `INSERT INTO queen.log_txns (...) VALUES (pid, base, base+cnt-1, v_now, p_hashes[i])`
     — ALWAYS (dedup on or off).

The duplicate result carries the ORIGINAL occurrence's offset `off`. The
broker resolves the original message id (wire contract: dup responses return
the canonical mid) by fetching the covering segment's blob
(`log_segment_at_v1(pid, off)` helper: base_offset <= off ORDER BY base DESC
LIMIT 1) and unpacking frame `off - base` in Rust. Rare path. If retention
already deleted that segment, the mid is reported as the zero uuid.

## 5. Dedup protocol (broker cache, exact, HA-safe)

Rust module `server/src/dedup.rs`:

- Per (partition_id) cache entry: `hydrated_from: i64` (offsets > this are
  fully known), `verified_upto: i64` (== the partition's last_offset as of our
  last successful push/hydration), `set: HashSet<u128>`, and a ring of
  `(base_offset, created_at_ms, count)` per segment for window expiry.
- Validity rule: the cache may claim `p_verified = verified_upto` on a push
  ONLY if it knows every hash in `(hydrated_from, verified_upto]` AND
  `hydrated_from` is at or below the window start (or txns_start). Otherwise
  send `p_verified = -1` (SQL probes the whole window — always correct).
- Hydration (first use of a partition, or after eviction): one query
  `SELECT base_offset, end_offset, created_at, hashes FROM queen.log_txns
   WHERE partition_id=$1 AND created_at >= now() - window` → build set, set
  `hydrated_from = MIN(base_offset)-1` (or -1 when no rows), `verified_upto = MAX(end_offset)` (or the partition's
  current last_offset fetched in the same round trip; if unknown use -1 and
  let the first push's returned base_offset advance it).
- After a successful push returning base B for count K: if `B == verified_upto + 1`
  (no interleave) insert the bundle's hashes, `verified_upto = B+K-1`.
  If `B > verified_upto + 1` (another broker interleaved), the SQL probe
  already covered `(p_verified, B-1]`, so the push is correct — but the cache
  is now incomplete: either re-hydrate the missing span
  (`WHERE base_offset > verified_upto AND base_offset < B`) or degrade to
  `p_verified = -1` until the next hydration. Both are correct; implement the
  span top-up, fall back to -1 on any error.
- Window expiry: drop ring prefix older than window; raise `hydrated_from`
  accordingly. Eviction: global LRU by memory (`QUEEN_DEDUP_CACHE_MB`,
  default 512); evicted partitions simply re-hydrate on next push.
- Layer-1 (intra-request) and layer-2 (intra-flush) broker dedup stay as
  today, now comparing 16B hashes instead of composed strings.
- Kill switch: `QUEEN_DEDUP_CACHE=0` → always send -1. Correctness never
  depends on the cache.

## 6. Pop (025_log_pop.sql)

Same claim algorithm, offsets. `log_pop_v1(queue, partition, group, budget,
lease_seconds, worker, auto_ack, sub_mode, sub_from)` RETURNS
`(r_base BIGINT, r_start_idx INT, r_take INT, r_msg_count INT,
r_created_at TIMESTAMPTZ, r_blob BYTEA)`:

- Claim: `SELECT committed FROM queen.log_consumers ... FOR UPDATE SKIP LOCKED`
  with the same lease-free predicate and the same claim-first row-creation
  dance as `seg_pop_segments_v1` (023:344-372) — NEVER an unconditional
  INSERT..ON CONFLICT per pop (the deadlock lesson).
- Subscription seeding on first contact: port 025's sub_mode/sub_from
  seeding with `committed = last_offset` (mode 'new') or the boundary walk by
  created_at → `first qualifying base_offset - 1` (timestamp mode).
- Scan: `wanted := committed + 1`. Head segment:
  `SELECT ... WHERE partition_id=pid AND base_offset <= wanted
   ORDER BY base_offset DESC LIMIT 1`; if found AND end_offset >= wanted →
  start there with `start_idx = wanted - base_offset`; else (retention gap or
  nothing yet) forward: `WHERE base_offset > wanted ORDER BY base_offset LIMIT 1`,
  start_idx 0. Continue forward until budget. Track `last_delivered` offset.
- autoAck: `UPDATE log_consumers SET committed = last_delivered,
  worker_id=NULL, lease_expires_at=NULL, batch_end=NULL,
  total_consumed = total_consumed + taken`.
- Leased: `SET worker_id=$worker, lease_expires_at=now()+lease,
  lease_acquired_at=..., batch_end = last_delivered`.

Wildcard wire (`log_pop_wildcard_wire_v1`) and binary
(`log_pop_wildcard_bin_v1`, RETURNS (meta JSONB, blobs BYTEA[])) port the
024 candidate scan verbatim with: hot filter `p.last_write_at >= watermark - 2min`,
`(c.partition_id IS NULL OR p.last_offset > c.committed)`, lease-free check,
`ORDER BY random() LIMIT cand_cap`, empty-scan watermark maintenance with the
30s lease-ignoring re-check (RUSTFIX 12), and the consumer_groups_metadata
bulk-seed bootstrap (RUSTFIX 13). JSON keys in meta stay EXACTLY:
`seq` (carries base_offset), `startOff` (carries start_idx), `take`,
`msgCount`, `createdAt`, plus `partition`, `partitionId`.
`log_pop_discover_*` (033) and `log_has_pending_v1` (trivial with offsets:
`p.last_offset > COALESCE(c.committed, -1)`) port the same way.

## 7. Ack (026_log_ack.sql)

- `log_ack_v1(queue, partition, group, worker, p_upto BIGINT, p_ok BOOLEAN,
  p_acked_count INT)`: port of seg_ack_segments_v1 — lease validation
  identical (worker check only when non-empty; RUSTFIX 11), clamp
  `p_upto <= batch_end`, then `committed = p_upto`, release lease, counters.
  NO seg_segments reads (no normalization needed — this deletes the
  msg_count lookups).
- `log_ack_by_hash_v1(p_partition_id UUID, p_group TEXT, p_worker TEXT,
  p_hashes BYTEA[], p_statuses TEXT[]) RETURNS JSONB`: exact port of the
  seg_ack_by_txn_v1 decision procedure (024:474-773) with scalar positions:
  - resolve each hash → offset via log_txns rows covering
    `[GREATEST(committed+1, txns_start), batch_end]` (lease) or `> committed`
    (lease-less); unresolvable hashes count as NOT acked (redelivery over
    loss, unchanged).
  - below-cursor honesty (noop/stale), head-signal clamp (dlq>failed>retry at
    equal offset), implicit-ack max-ok, retry budget `batch_retry_count`,
    forced DLQ keep-lease handoff, exhausted-budget drop/DLQ, reached-end
    release+reset — ALL identical, tuples→scalars. delta =
    `new_committed - old_committed` (pure arithmetic).
  - returns `noopHashes`/`staleHashes` as hex arrays; the BROKER maps back to
    txn strings for the wire (it has the request's txn list).
- `log_renew_lease_v1(worker, seconds)` — port (GREATEST rule, MIN expiry).
- DLQ: port 025's seg_dlq table → `queen.log_dlq` with `offset BIGINT`
  replacing (seq, frame_idx); `log_dlq_head_v1` etc. Same quarantine
  semantics (RUSTFIX poison quarantine).
- `log_transaction_wire_v1(p JSONB)` — port of seg_transaction_wire_v1:
  same mixed-engine guard (queen.queues.storage <> 'segments' rejected), same
  ascending-id pre-lock, pushes via the log_push body (share code via an
  internal helper or duplicate the per-segment body — keep ONE allocator code
  path: extract `log_push_one_v1(...)` used by both), acks via
  log_ack_v1/log_ack_by_hash_v1 in ascending (partition, group) order.

## 8. Retention R1 + maintenance (027_log_maintenance.sql + retention.rs)

SQL steps (each call = ONE transaction, called from Rust in a loop):

- `log_retention_step_v1(p_pid UUID, p_all_cutoff TIMESTAMPTZ,
  p_completed_cutoff TIMESTAMPTZ, p_max_rows INT) RETURNS JSONB
  {"deleted":N,"new_log_start":X,"done":bool}`:
  compute boundary = GREATEST(rule1 boundary via created_at walk from
  log_start, capped rule2 boundary via MIN(committed)+1 across groups) exactly
  as 026 does today; delete AT MOST p_max_rows segment rows
  (`SELECT base_offset ... ORDER BY base_offset LIMIT p_max_rows` then ranged
  DELETE); advance log_start to last deleted end_offset+1 (or boundary when
  done); done = boundary reached. The log_partitions row lock is held only
  for this one bounded transaction.
- `log_txns_purge_step_v1(p_pid UUID, p_cutoff TIMESTAMPTZ, p_max_rows INT)`
  — same pattern over log_txns/txns_start.
- `log_evict_max_wait_step_v1(...)` — port db::seg_evict_max_wait semantics
  (max_wait_time_seconds age eviction) per partition, bounded.

retention.rs rewrite:
- Interval default 5000 ms (config change: `RETENTION_INTERVAL` default
  300000 → 5000).
- Cycle: acquire SESSION advisory lock 737001 (`pg_try_advisory_lock`) on the
  dedicated connection (release at cycle end; skip cycle if not acquired).
  Then: list retention-enabled log queues + partitions + cutoffs (one query);
  per partition: loop log_retention_step_v1 until done; then per partition
  with txns to purge: loop log_txns_purge_step_v1
  (cutoff = now() - GREATEST(dedup_window_seconds, completed_retention_seconds, 900));
  then max_wait eviction; then metrics purge — each phase in its OWN
  transactions (autocommit function calls), never one giant transaction.
  RETENTION_BATCH_SIZE (default 50000) = p_max_rows per step call.

## 9. Stats/observability (030_log_stats.sql + stats.rs/handlers)

Port `seg_refresh_all_stats_v1` (034) to O(partitions): pending per
(queue,group) = `SUM(GREATEST(p.last_offset - GREATEST(c.committed, p.log_start-1), 0))`
— NO log_segments scan. Lag/analytics/status/prometheus queries in handlers
(analytics.rs, status.rs, queues.rs, messages.rs, migration.rs) and
webapp-facing SPs port from seg tables to log tables mechanically. Messages
browsing (027 observability) ports offsets into the same JSON keys.
Traces (030) and consumer-group admin (031: group delete/seek — seek by
timestamp = boundary walk → committed) port mechanically.
Streams (029_seg_streams → 028_log_streams): same cycle logic reading
log_segments by offset ranges.

## 10. Rust changes (server/src)

- `db.rs`: every SQL wrapper → new log_* signatures; ALL hot statements via
  `client.prepare_cached(...)` (deadpool-postgres statement cache), including
  pop/push/ack/renew/has_pending. Position types: `(i64, i32)` pairs → `i64`.
- `fusion.rs`: build per-segment `hashes: Vec<u8>` (16×K) instead of metas
  JSON; call `log_push_multi_v1` with (queues, partitions, counts, hashes,
  verified, blobs); demux `baseOffset`; duplicate path resolves original mids
  via the covering-blob fetch; verified values from dedup.rs cache.
  Delete the QUEEN_V2_PUSH_MULTI flag and the v1 path.
- `dedup.rs` (new): the cache of §5.
- `handlers/data.rs`: offsets throughout; LeaseRegistry values → offsets;
  ack path computes hashes; pop response builder unchanged JSON keys; parked
  long-poll: `drop(client)` (and permit) BEFORE `notifier.wait_queue` — the
  connection must NOT be held during the park; re-acquire on wake (next loop
  iteration already does).
- `retention.rs`: §8.
- `vegas.rs`: windowed-min rtt_base — 60-slot ring of per-second minima;
  `rtt_base = min(ring)`; a new minimum updates the current slot; slots expire
  as time advances (so base can RISE when the cheap ops disappear). Keep
  alpha/beta logic unchanged.
- `main.rs`: enable TCP_NODELAY on accepted connections (axum::serve builder
  if available in axum 0.7.9, else set it in a small accept loop /
  make_service wrapper — verify at implementation).
- `util.rs`: `txn_hash128(txn: &str) -> [u8; 16]` via xxhash-rust xxh3_128,
  big-endian. `Cargo.toml`: add `xxhash-rust = { version = "0.8", features = ["xxh3"] }`.
- `config.rs`: RETENTION_INTERVAL default 5000; add QUEEN_DEDUP_CACHE_MB
  (512), QUEEN_DEDUP_CACHE (1). Remove QUEEN_V2_PUSH_MULTI.
- `schema.rs`: new file list (§1).
- `stats.rs`, `syscollect.rs`, handlers analytics/status/queues/messages/
  migration/streams/traces/consumer_groups: port table/SP names + offsets.
- `file_buffer.rs` / `internal.rs` / `reconcile.rs`: port any seg_* SQL they
  embed (grep!).

## 11. Wire-compat invariants (what the suites will catch)

- Endpoint paths, JSON keys, and status codes: UNCHANGED.
- Pop response `seq`/`startOff` keys now carry base_offset/start_idx values —
  clients treat them as opaque; leaseId semantics unchanged (worker uuid, "" on autoAck).
- Ack by txn strings on the wire; duplicate push responses return the original
  (first-occurrence) message id, zero-uuid if its segment was already retained away.
- `/configure` contract unchanged (storage:'segments' value kept for compat).
- Same at-least-once semantics: implicit-ack, explicit-signal-never-skipped,
  below-cursor honesty (noop/stale), retry budget, DLQ handoff, eviction gaps.

## 12. Acceptance

1. `cargo build --release` clean.
2. Fresh PG16 (:5457 container) → broker boots, schema applies, smoke
   push/pop/ack round-trip.
3. JS parity suite ≥ baseline (100/112; the ~12 known failures are
   pre-existing — no NEW failures).
4. Go client tests green; Py client tests green (same baseline rule).
5. A 10-min local soak (E1b shape) holds steady state with retention R1
   (no sweep stalls: no Lock:transactionid pileups on push during sweeps).
6. Dedup smoke: window>0 queue, duplicate txn batch → duplicate response with
   original mids; dedup-off queue + explicit acks → cursor advances (the
   window=0 landmine is dead).
