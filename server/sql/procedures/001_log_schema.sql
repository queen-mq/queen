-- ============================================================================
-- Log engine (18-log-engine.md §2) — schema.
--
-- Greenfield replacement of the retired seg_* engine: positions are a
-- single per-partition BIGINT message offset instead of (seq, frame_idx)
-- pairs. A segment covers the inclusive offset range [base_offset,
-- end_offset]; per partition those ranges are DISJOINT and allocated
-- monotonically under the queen.log_partitions row lock (the same serializer
-- role the old last_seq played). Rollback of a push rolls back the allocator
-- bump, so offsets have no gaps in steady state; gaps introduced by retention
-- are tolerated by the pop scan exactly like the old engine tolerated seq
-- gaps. created_at is monotone per partition in commit order (stamped after
-- the row lock — the PUSHSER invariant; time-based retention and timestamp
-- subscriptions depend on it).
--
-- Shared tables queen.consumer_watermarks / queen.consumer_groups_metadata
-- stay as-is (the retired rows engine is gone entirely). Applied idempotently
-- at boot like every procedures file.
-- ============================================================================

-- queen.log_queues is GONE (2026-07-31, queue-identity merge): the log engine
-- anchors its partitions directly on queen.queues(id). One queue identity, one
-- id space — the (tenant_id, name) bridge joins between config and data are
-- deleted with it, and with them the cross-tenant-join hazard class the
-- retention work list had to pin with a unit test. There is no upgrade path:
-- the schema model is always-virgin (the one-off merge migration is retired).

CREATE TABLE IF NOT EXISTS queen.log_partitions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_id UUID NOT NULL REFERENCES queen.queues(id) ON DELETE CASCADE,
    name TEXT NOT NULL DEFAULT 'Default',
    last_offset  BIGINT NOT NULL DEFAULT -1,  -- allocator; next base = last_offset+1
    log_start    BIGINT NOT NULL DEFAULT 0,   -- segment retention watermark
    txns_start   BIGINT NOT NULL DEFAULT 0,   -- log_txns purge watermark
    last_write_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),  -- QUANTIZED (see push)
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (queue_id, name)
);
-- One allocator UPDATE per push lands on this row. last_write_at is INDEXED
-- (below), so a bump of it makes the update non-HOT — which is why push
-- quantizes it to at most one real change per second per partition
-- (003_log_push); the
-- steady-state allocator update then only touches non-indexed columns and
-- stays HOT. fillfactor headroom keeps those HOT chains on-page so the two
-- unique indexes stay O(#partitions) instead of O(#updates).
ALTER TABLE queen.log_partitions SET (fillfactor = 70);
-- Churn control (2026-07-23): every push UPDATEs this row, so at high single-
-- push rates the dead-tuple count between autovacuum passes dwarfs the live
-- row count (measured: wildcard pop candidate scans grew to ~12ms on a
-- 1000-row queue purely from scan bloat). Threshold-based triggering (scale
-- factor 0) keeps vacuum re-firing every naptime under churn — a pass over a
-- few thousand live rows costs ms.
--
-- vacuum_truncate = off (2026-07-24, caught live during the 24h soak with
-- pg_stat_progress_vacuum in phase "truncating heap"): the heap-truncation
-- step of vacuum takes an ACCESS EXCLUSIVE lock, and on these tiny fixed-
-- population tables (~one row per partition/consumer-group, never shrinking)
-- it reclaims nothing while freezing EVERY push and pop behind the lock for
-- seconds. This was the root cause of the whole "wobble" class — the periodic
-- lag/latency spikes seen across every high-rate run (soak waves at t≈15/55/
-- 158min, the 6M-lag excursion of the 800k explicit-ack test, the ackErr
-- bursts). Disabling truncation removes the exclusive lock; the fixed row
-- count means we never wanted the truncation anyway.
ALTER TABLE queen.log_partitions SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 500,
    autovacuum_vacuum_cost_delay = 0,
    vacuum_truncate = off);
-- Candidate-selection index: wildcard pop range-scans only recently-written
-- partitions of a queue instead of the full partition set (decisive at
-- 10-20k partitions/queue). The scan already tolerates 2 minutes of slack, so
-- the 1s quantization staleness of last_write_at is absorbed.
CREATE INDEX IF NOT EXISTS idx_log_partitions_queue_write
    ON queen.log_partitions (queue_id, last_write_at);

-- ----------------------------------------------------------------------------
-- RETENTION WORK-LIST WATERMARKS (2026-08-24). Two per-partition scalar FACTS:
-- the created_at of the row sitting AT each purge watermark —
--   oldest_live_at = created_at of the segment at log_start  (NULL = no live
--                    segments: retention can never have anything to do here)
--   oldest_txn_at  = created_at of the log_txns row at txns_start (NULL = the
--                    sidecar is fully purged)
--
-- They exist because retention's work list was Θ(#partitions), not Θ(deletable
-- work): the cycle materialized ONE ROW PER PARTITION (queues JOIN
-- log_partitions, 827k rows on the 2026-08-24 soak cell — the exact query that
-- died on a statement timeout there) and then made a per-partition step call
-- for each, at 2-5 ms a call, 99.9% of them finding nothing eligible. A serial
-- pass was 30-70 minutes against newly-eligible segments arriving at ~500/s,
-- so the measured delete rate (~20 segments/s) could never catch up. With these
-- columns the cycle instead range-scans, per queue, exactly the partitions that
-- CAN yield something.
--
-- Why a scalar suffices: created_at is monotone in base_offset within a
-- partition (the PUSHSER invariant, stamped under the allocator row lock — see
-- this file's header and log_retention_boundary_v1 in 006), so "this partition
-- has something deletable under cutoff C" is EXACTLY "the row at its watermark
-- is older than C". No aggregate, no scan.
--
-- FACT, never POLICY: the column stores the AGE OF THE OLDEST LIVE DATA, not
-- eligibility. Cutoffs stay computed from queue config at query time, so an
-- edit to retention_seconds / dedup_window_seconds applies on the NEXT cycle
-- with nothing to invalidate and no backfill.
--
-- TWO columns, not one: the segment watermark cannot stand in for the sidecar's.
-- The txns cutoff is now() - GREATEST(dedup_window_seconds,
-- completed_retention_seconds, 900) — 3600 s by default — while retention
-- windows are typically far longer, so a segment-derived due test ("oldest
-- segment older than the txns cutoff") is true for essentially EVERY partition
-- holding more than an hour of data: 827k of them on the measured cell, i.e.
-- the very cost class this pair exists to remove. Two facts, two probes, both
-- Θ(work).
--
-- WRITERS (each maintains them in the SAME transaction and under the SAME
-- log_partitions row lock that moves the watermark itself — if you add a third,
-- it must too, and the daily safety walk in retention.rs will catch you if you
-- forget):
--   * queen.log_retention_step_v1  (006) — the ONLY writer of log_start
--   * queen.log_txns_purge_step_v1 (006) — the ONLY writer of txns_start
--   * the push allocator (003) — COALESCE(col, clock_timestamp()), so ONLY the
--     empty -> non-empty transition writes; for a partition that already holds
--     data the value is byte-identical and the allocator UPDATE stays HOT,
--     exactly like the last_write_at quantization it sits next to.
--   * queue/partition DROP deletes the row — nothing to maintain.
--
-- PARTIAL indexes (the retention mirror of idx_log_partitions_queue_write): the
-- work list only ever probes NON-NULL values, so NULL rows — partitions with
-- nothing live, of which a large partition population has many — are kept out
-- of the index entirely. `col < $cutoff` implies `col IS NOT NULL` for a strict
-- operator, which is what lets the planner match the predicate and still take
-- the range scan.
--
-- COST accepted here: log_start / txns_start are NOT indexed, so the two step
-- functions' partition UPDATE used to be HOT; with these columns indexed it is
-- not. At the measured ~500 eligible segments/s that is ~500 extra index tuples
-- per second on an 827k-entry index, against a table already carrying ~1000
-- pushes/s of HOT churn and already tuned for it (fillfactor 70, scale_factor 0
-- / threshold 500 autovacuum, vacuum_truncate off, above).
--
-- CONCURRENTLY (2026-08-24 boot-deadlock incident, schema.rs module doc): these
-- two indexes are the first in this corpus to arrive on an ALREADY-POPULATED
-- hot table (827k partitions, ~1000 pushes/s on the cell that hit it), so a
-- plain CREATE INDEX would hold ShareLock — blocking every push — for the whole
-- build. The applier runs CONCURRENTLY statements outside any transaction with
-- lock_timeout disabled; on a virgin deployment the table is empty and the
-- concurrent build costs nothing extra. Indexes born WITH their table stay
-- plain CREATE INDEX (see the authoring rules in schema.rs).
-- ----------------------------------------------------------------------------
ALTER TABLE queen.log_partitions
    ADD COLUMN IF NOT EXISTS oldest_live_at TIMESTAMPTZ;
ALTER TABLE queen.log_partitions
    ADD COLUMN IF NOT EXISTS oldest_txn_at TIMESTAMPTZ;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_log_partitions_queue_oldest
    ON queen.log_partitions (queue_id, oldest_live_at)
    WHERE oldest_live_at IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_log_partitions_queue_oldest_txn
    ON queen.log_partitions (queue_id, oldest_txn_at)
    WHERE oldest_txn_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS queen.log_segments (
    partition_id UUID NOT NULL REFERENCES queen.log_partitions(id) ON DELETE CASCADE,
    base_offset BIGINT NOT NULL,
    end_offset  BIGINT NOT NULL,   -- inclusive; msg_count = end_offset-base_offset+1
    created_at  TIMESTAMPTZ NOT NULL,
    blob BYTEA NOT NULL,           -- frame packing + zstd, UNCHANGED from seg engine
    PRIMARY KEY (partition_id, base_offset)
);
-- EXTERNAL: TOAST must not burn CPU re-compressing the broker's already-zstd'd
-- (incompressible) bytes. ZERO secondary indexes: the PK is also the pop path.
ALTER TABLE queen.log_segments ALTER COLUMN blob SET STORAGE EXTERNAL;
-- Autovacuum, tuned by measurement (2026-07-23 VM campaign): the original
-- 0.02/0.05 factors re-fired vacuum every ~13s at 1M msg/s — each pass over
-- the growing heap+toast competed for I/O exactly when the system had no
-- headroom (measured: absorbed hiccup at 800k/s, stall at 900k, collapse
-- contributor at 1M). 0.1/0.3 keeps vacuum on pace for retention churn (the
-- heap still plateaus via slot reuse — verified over 300s at 1M/s with
-- retention sweeping every 5s) without the re-trigger storm. cost_limit stays
-- high / delay 0 so each pass finishes fast. Storage params only — no table
-- rewrite, idempotent.
ALTER TABLE queen.log_segments SET (
    autovacuum_vacuum_scale_factor = 0.1,
    autovacuum_vacuum_insert_scale_factor = 0.3,
    autovacuum_vacuum_cost_limit = 4000,
    autovacuum_vacuum_cost_delay = 0,
    toast.autovacuum_vacuum_scale_factor = 0.1,
    toast.autovacuum_vacuum_insert_scale_factor = 0.3,
    toast.autovacuum_vacuum_cost_limit = 4000,
    toast.autovacuum_vacuum_cost_delay = 0
);

-- One row per segment; 16 bytes per frame, frame order. ALWAYS written (dedup
-- on or off): ack-by-hash resolution depends on it. Purged by its own
-- watermark (txns_start), window = GREATEST(dedup_window, completed_retention, 900s).
-- No FK on partition_id: the purge path must never pay FK-trigger cost, and
-- the ranges are only ever reached through a live log_partitions row.
CREATE TABLE IF NOT EXISTS queen.log_txns (
    partition_id UUID NOT NULL,
    base_offset BIGINT NOT NULL,
    end_offset  BIGINT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL,
    hashes BYTEA NOT NULL,         -- 16*msg_count bytes, xxh3_128 big-endian
    PRIMARY KEY (partition_id, base_offset)
);
-- Same measured rationale as log_segments: append-heavy at segment rate, the
-- 0.02 factor re-triggered continuously during growth.
ALTER TABLE queen.log_txns SET (
    autovacuum_vacuum_scale_factor = 0.1,
    autovacuum_vacuum_insert_scale_factor = 0.3,
    autovacuum_vacuum_cost_delay = 0);

-- Coordination: cursor / lease / retry state per (partition, group). committed
-- is the single source of truth for consumption ("everything <= committed is
-- done"); a leased batch is the span (committed, batch_end]. Replaces the
-- retired seg-era fold onto the rows engine's coordination table (engine and
-- table both gone) — the log engine keys on log_partitions ids, an
-- independent UUID space.
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
    -- The lease this row currently holds was a CONFLATING one (PLAN_CONFLATION
    -- §2.2). See the ALTER below for the rationale; this copy only serves a
    -- database created from scratch.
    lease_conflated BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (partition_id, consumer_group)
);
-- Every pop/ack UPDATEs its row (no indexed columns touched → HOT); heavy
-- headroom so the churn stays on-page.
ALTER TABLE queen.log_consumers SET (fillfactor = 50);
-- Same churn rationale as log_partitions above: pop/ack update rates dwarf
-- the live row count, and the wildcard candidate LEFT JOIN scans this table.
-- vacuum_truncate = off for the same reason as log_partitions: the ACCESS
-- EXCLUSIVE heap-truncation lock stalls every pop/ack, and this fixed-
-- population table (one row per partition+group) never shrinks (2026-07-24).
ALTER TABLE queen.log_consumers SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 500,
    autovacuum_vacuum_cost_delay = 0,
    vacuum_truncate = off);

-- The lease this row currently holds was a CONFLATING one (PLAN_CONFLATION §2.2).
-- Written by the pop that took the lease, read by the ack that closes it. It
-- exists because ack must be able to (a) report the skipped count and (b) apply
-- the completion clamp WITHOUT a second lookup: the ack path already does
-- SELECT * INTO v_c on this row, so this column costs nothing to read, and it is
-- non-indexed so writing it keeps the lease UPDATE HOT.
-- CREATE TABLE above is IF NOT EXISTS, so this idempotent ALTER is what upgrades
-- an existing database (precedent: 019_worker_metrics.sql, 024_kv.sql).
ALTER TABLE queen.log_consumers
    ADD COLUMN IF NOT EXISTS lease_conflated BOOLEAN NOT NULL DEFAULT FALSE;

-- A3 (PLAN_HOTLIST_FOLLOWUP.md): the durable floor under the mesh hot-list hint.
--
-- A hot-list ring only learns that OLD partitions became pending again — a backward
-- seek, a consumer-group delete, anything that moves a cursor without writing a row —
-- from the mesh, and the mesh is best-effort by design: a frame is dropped when a
-- peer's queue is full or the peer is down (server/src/mesh.rs). That was survivable
-- while every broker walked every partition every 30s; with the windowed floor the
-- dropped frame costs the replay a full-walk interval. So the cursor move ALSO
-- publishes a row here, inside the same transaction that performs it, and every broker
-- polls this table on its reconcile cadence (~60s) and repairs the rings it holds.
--
-- Keyed by NAMES, not ids, because that is what a ring is keyed by: the broker has no
-- queue_id in hand at reseed time. Keyed WITHOUT the partition so the table cannot
-- grow with traffic — it holds one row per (tenant, queue, group) ever repaired inside
-- the prune window, i.e. tens of rows in production. `partition_name` is the SCOPE of
-- the repair, exactly like the group on a mesh hint: a per-partition seek names its
-- partition and the reader marks only that one, while NULL means "walk the queue".
-- Two repairs of the same (tenant, queue, group) naming DIFFERENT partitions therefore
-- widen to NULL rather than losing one of them (queen.hotlist_repair_publish_v1).
--
-- This is a publication channel for DELIBERATE cursor moves, not a general lost-
-- notification repair: an entry wrongly cleared, an INFLIGHT stranded by a dropped pop
-- future, a stale WHEEL park or a dropped hint for a PUSH are still the full walk's
-- job. Say so here, or the next reader will assume more.
CREATE TABLE IF NOT EXISTS queen.hotlist_repairs (
    tenant_id      UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    queue_name     TEXT NOT NULL,
    consumer_group TEXT NOT NULL,
    -- NULL = the whole queue (a queue-wide seek, a group delete).
    partition_name TEXT,
    repair_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reason         TEXT NOT NULL,
    PRIMARY KEY (tenant_id, queue_name, consumer_group)
);
-- One user, and it is NOT the brokers: the hourly prune inside
-- queen.hotlist_repair_publish_v1. The poll deliberately has no predicate here —
-- db::hotlist_repairs_all reads the whole table (bounded by the primary key and by that
-- prune) and acts on any CHANGE in (repair_at, partition_name) — because a watermark is
-- unsound on this data: now() is transaction_timestamp, so a slow group delete commits a
-- row dated BEFORE one a faster seek already carried the watermark past, and that repair
-- would be skipped for ever. Pinned by
-- reconcile.rs::a_repair_that_commits_out_of_timestamp_order_is_still_acted_on.
CREATE INDEX IF NOT EXISTS idx_hotlist_repairs_at ON queen.hotlist_repairs (repair_at);

-- Helper: explode a 16B-stride hash blob into (idx, h) rows. idx is 0-based.
-- Used by the push dedup probe (003_log_push) and ack-by-hash resolution
-- (005_log_ack); pure
-- byte slicing, so IMMUTABLE (inlinable into the calling queries).
CREATE OR REPLACE FUNCTION queen.log_unnest_hashes(p BYTEA)
RETURNS TABLE (idx INT, h BYTEA) LANGUAGE sql IMMUTABLE AS $$
    SELECT g, substring(p FROM g*16 + 1 FOR 16)
    FROM generate_series(0, octet_length(p)/16 - 1) g
$$;
