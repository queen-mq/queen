-- ============================================================================
-- Log engine (18-log-engine.md §2) — schema.
--
-- Greenfield replacement of the seg_* engine (dropped by 040): positions are a
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
-- stay as-is; the rows engine (001-018) is untouched. Applied idempotently at
-- boot like every procedures file.
-- ============================================================================

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
-- One allocator UPDATE per push lands on this row. last_write_at is INDEXED
-- (below), so a bump of it makes the update non-HOT — which is why push
-- quantizes it to at most one real change per second per partition (042); the
-- steady-state allocator update then only touches non-indexed columns and
-- stays HOT. fillfactor headroom keeps those HOT chains on-page so the two
-- unique indexes stay O(#partitions) instead of O(#updates).
ALTER TABLE queen.log_partitions SET (fillfactor = 70);
-- Candidate-selection index: wildcard pop range-scans only recently-written
-- partitions of a queue instead of the full partition set (decisive at
-- 10-20k partitions/queue). The scan already tolerates 2 minutes of slack, so
-- the 1s quantization staleness of last_write_at is absorbed.
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
-- seg-era fold onto queen.partition_consumers (unfolded by 040) — the log
-- engine keys on log_partitions ids, an independent UUID space.
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
-- Every pop/ack UPDATEs its row (no indexed columns touched → HOT); heavy
-- headroom so the churn stays on-page.
ALTER TABLE queen.log_consumers SET (fillfactor = 50);

-- Helper: explode a 16B-stride hash blob into (idx, h) rows. idx is 0-based.
-- Used by the push dedup probe (042) and ack-by-hash resolution (044); pure
-- byte slicing, so IMMUTABLE (inlinable into the calling queries).
CREATE OR REPLACE FUNCTION queen.log_unnest_hashes(p BYTEA)
RETURNS TABLE (idx INT, h BYTEA) LANGUAGE sql IMMUTABLE AS $$
    SELECT g, substring(p FROM g*16 + 1 FOR 16)
    FROM generate_series(0, octet_length(p)/16 - 1) g
$$;
