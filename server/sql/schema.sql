-- ============================================================================
-- Queen Message Queue - Consolidated Database Schema
-- Version: 0 (base schema)
-- ============================================================================
--
-- This file contains the complete current schema for Queen MQ.
-- It is designed to be idempotent (safe to run multiple times).
--
-- Usage:
-- - Fresh install: Run this file once to create all objects
-- - Upgrades: Apply migrations from server/migrations/ with version > 0
--
-- ============================================================================

-- Create schema
CREATE SCHEMA IF NOT EXISTS queen;

-- ============================================================================
-- Core Tables
-- ============================================================================

CREATE TABLE IF NOT EXISTS queen.queues (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    -- Track B (PLAN_QUEEN_PROXY_CLOUD.md §5): opaque tenant scoping key on queue
    -- identity. NOT NULL DEFAULT the fixed default tenant = a catalog-only change
    -- on existing tables (no backfill/rewrite). Uniqueness is (tenant_id, name),
    -- established by the migration block below (idempotent for existing DBs).
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    namespace VARCHAR(255),
    task VARCHAR(255),
    priority INTEGER DEFAULT 0,
    lease_time INTEGER DEFAULT 300,
    retry_limit INTEGER DEFAULT 3,
    retry_delay INTEGER DEFAULT 1000,
    ttl INTEGER DEFAULT 3600,
    -- RUSTFIX item 2: default TRUE so unconfigured queues keep the v0.16.0
    -- "always dead-letter" behaviour (the old ack ignored these flags entirely).
    dead_letter_queue BOOLEAN DEFAULT TRUE,
    dlq_after_max_retries BOOLEAN DEFAULT TRUE,
    delayed_processing INTEGER DEFAULT 0,
    window_buffer INTEGER DEFAULT 0,
    retention_seconds INTEGER DEFAULT 0,
    completed_retention_seconds INTEGER DEFAULT 0,
    retention_enabled BOOLEAN DEFAULT FALSE,
    encryption_enabled BOOLEAN DEFAULT FALSE,
    max_wait_time_seconds INTEGER DEFAULT 0,
    max_queue_size INTEGER DEFAULT 0,
    -- MINIMUM POP WAIT (2026-07-29, TASK M). Milliseconds a pop may hold its
    -- claim back while the batch is UNDER-FULL, so one PG commit carries more
    -- messages ("pop magro" — the ~4-commits-per-delivered-message ceiling).
    -- Distinct from long-poll wait (which waits on an EMPTY queue) and from
    -- window_buffer (seconds, message VISIBILITY deferral for every consumer):
    -- this delays only THIS pop's claim, only when the queue is NON-empty, and
    -- ends early the instant the ring holds the requested batch size.
    -- 0 = OFF = every existing deployment byte-identical.
    min_pop_wait_time INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- The rows engine's tables (queen.partitions, queen.messages,
-- queen.partition_consumers, queen.messages_consumed, queen.dead_letter_queue,
-- queen.partition_lookup) were REMOVED on 2026-07-30. This is a log-engine-only
-- broker: messages live in queen.log_segments, partitions in
-- queen.log_partitions, cursors in queen.log_consumers and dead letters in
-- queen.log_dlq (041/044). 050_drop_rows_tables.sql removes the tables from
-- databases that already have them.

-- Storage engine selector. Storage v2 "segments" is the engine this (Rust) broker
-- serves; the engine itself lives in procedures/023..028_storage_v2*.sql. This is a
-- segments-only broker, so new queues default to 'segments' (the rows engine and its
-- hot-path procedures are retired). The column is kept (rather than dropped) so the
-- dual-engine observability guards in 026/027 and the rows→segments migration can key
-- off it. Catalog-only change on PG >= 11: constant default, no table rewrite.
ALTER TABLE queen.queues ADD COLUMN IF NOT EXISTS storage VARCHAR(16) NOT NULL DEFAULT 'segments';

-- MINIMUM POP WAIT (TASK M): idempotent upgrade for existing installations.
-- Nullable-with-constant-default ⇒ catalog-only on PG >= 11 (no table rewrite),
-- and DEFAULT 0 means an upgraded deployment behaves exactly as before.
ALTER TABLE queen.queues ADD COLUMN IF NOT EXISTS min_pop_wait_time INTEGER DEFAULT 0;

-- RUSTFIX item 2: flip the DLQ column defaults to TRUE on existing tables too
-- (CREATE TABLE IF NOT EXISTS above does not alter an existing table's defaults).
-- This only changes the default for FUTURE inserts — existing rows keep whatever
-- explicit value they were configured with, and the load-bearing parity fix is the
-- COALESCE(...,true) in 024 which covers NULL columns and absent queen.queues rows.
ALTER TABLE queen.queues ALTER COLUMN dead_letter_queue SET DEFAULT TRUE;
ALTER TABLE queen.queues ALTER COLUMN dlq_after_max_retries SET DEFAULT TRUE;

CREATE TABLE IF NOT EXISTS queen.consumer_groups_metadata (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Track B: subscription metadata is per-queue (queue_name), so it collides
    -- across tenants sharing a name — scope it. Uniqueness becomes
    -- (tenant_id, consumer_group, queue_name, partition_name, namespace, task),
    -- established by the migration block below (the old 5-col UNIQUE is dropped).
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    consumer_group TEXT NOT NULL,
    queue_name TEXT NOT NULL DEFAULT '',
    partition_name TEXT NOT NULL DEFAULT '',
    namespace TEXT NOT NULL DEFAULT '',
    task TEXT NOT NULL DEFAULT '',
    subscription_mode TEXT NOT NULL,
    subscription_timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_consumer_groups_metadata_lookup 
ON queen.consumer_groups_metadata(consumer_group, queue_name, namespace, task);

-- Watermark tracking for efficient wildcard POP discovery
-- Tracks the timestamp of the last "empty scan" per (queue, consumer_group)
-- When a consumer finds no pending data, we update their watermark to NOW()
-- On next POP, we only scan partitions updated since the watermark
-- This avoids expensive full scans for up-to-date consumers
CREATE TABLE IF NOT EXISTS queen.consumer_watermarks (
    -- Track B: the empty-scan floor is per (queue_name, group) — colliding across
    -- tenants sharing a name would let one tenant's "queue is empty since T" floor
    -- hide another tenant's fresh partitions. Scope by tenant_id (part of the PK).
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    queue_name VARCHAR(255) NOT NULL,
    consumer_group VARCHAR(255) NOT NULL DEFAULT '__QUEUE_MODE__',
    last_empty_scan_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, queue_name, consumer_group)
);

CREATE TABLE IF NOT EXISTS queen.retention_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- A queen.log_partitions id. No FK: the audit row must outlive the partition
    -- (the cleanup phase writes one __partition_deleted__ row as it deletes).
    partition_id UUID,
    messages_deleted INTEGER DEFAULT 0,
    retention_type VARCHAR(50),
    executed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS queen.system_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    timestamp TIMESTAMPTZ NOT NULL,
    hostname TEXT NOT NULL,
    port INTEGER NOT NULL,
    worker_id TEXT NOT NULL,
    sample_count INTEGER NOT NULL DEFAULT 60,
    metrics JSONB NOT NULL,
    CONSTRAINT unique_metric_per_replica 
        UNIQUE (timestamp, hostname, port, worker_id)
);

CREATE TABLE IF NOT EXISTS queen.message_traces (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- No FKs: these carry log-engine ids (queen.log_partitions), and a trace must
    -- outlive the segment it describes. 047 drops the constraints on existing DBs.
    message_id UUID,
    partition_id UUID,
    transaction_id VARCHAR(255) NOT NULL,
    consumer_group VARCHAR(255),
    event_type VARCHAR(100),
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    worker_id VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS queen.message_trace_names (
    trace_id UUID REFERENCES queen.message_traces(id) ON DELETE CASCADE,
    trace_name TEXT NOT NULL,
    PRIMARY KEY (trace_id, trace_name)
);

-- System state table for shared configuration across instances
CREATE TABLE IF NOT EXISTS queen.system_state (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- Track B (PLAN_QUEEN_PROXY_CLOUD.md §5) — native tenant scoping migration.
-- ============================================================================
-- Brings EXISTING databases to the (tenant_id, name) queue-identity shape the
-- CREATE TABLEs above already have on fresh installs. Every step is catalog-only
-- (NOT NULL DEFAULT constant column, index/constraint swap) — no table rewrite,
-- so it is safe on tables with millions of rows and idempotent on re-apply.
-- log_queues is migrated in 041 (its own file); the two shared coordination
-- tables + queen.queues are handled here.

-- queen.queues: add the column, drop the rows-engine name FK that pinned name
-- unique (partition_lookup no longer references it), drop the old UNIQUE(name),
-- establish UNIQUE(tenant_id, name) so two tenants can hold the same queue name.
ALTER TABLE queen.queues ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';
ALTER TABLE queen.queues DROP CONSTRAINT IF EXISTS queues_name_key;
CREATE UNIQUE INDEX IF NOT EXISTS queues_tenant_name_uk ON queen.queues(tenant_id, name);

-- queen.consumer_watermarks: add tenant_id, re-key the PK to include it (a shared
-- empty-scan floor across tenants would strand a tenant's fresh partitions).
ALTER TABLE queen.consumer_watermarks ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint
               WHERE conrelid = 'queen.consumer_watermarks'::regclass AND contype = 'p'
                 AND array_length(conkey, 1) = 2) THEN
        ALTER TABLE queen.consumer_watermarks DROP CONSTRAINT consumer_watermarks_pkey;
        ALTER TABLE queen.consumer_watermarks ADD PRIMARY KEY (tenant_id, queue_name, consumer_group);
    END IF;
END $$;

-- queen.consumer_groups_metadata: add tenant_id, swap the 5-col UNIQUE for the
-- 6-col one. The old constraint is auto-named, so drop every unique constraint
-- on the table (there is only ever the one) and re-establish the scoped index.
ALTER TABLE queen.consumer_groups_metadata ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';
DO $$
DECLARE r record;
BEGIN
    FOR r IN SELECT conname FROM pg_constraint
             WHERE conrelid = 'queen.consumer_groups_metadata'::regclass AND contype = 'u'
    LOOP
        EXECUTE format('ALTER TABLE queen.consumer_groups_metadata DROP CONSTRAINT %I', r.conname);
    END LOOP;
END $$;
CREATE UNIQUE INDEX IF NOT EXISTS cgm_tenant_uk
    ON queen.consumer_groups_metadata(tenant_id, consumer_group, queue_name, partition_name, namespace, task);


-- ============================================================================
-- TABLES
-- ============================================================================

-- Unified stats table for all hierarchy levels
CREATE TABLE IF NOT EXISTS queen.stats (
    -- Primary key: type + flexible key
    stat_type VARCHAR(20) NOT NULL,      -- 'system', 'namespace', 'task', 'queue', 'partition'
    stat_key VARCHAR(512) NOT NULL,      -- Identifier varies by type
    
    -- Foreign key references (nullable based on type, for cascades)
    queue_id UUID REFERENCES queen.queues(id) ON DELETE CASCADE,
    -- A queen.log_partitions id when stat_type='partition'. No FK: log partitions
    -- have no row in a rows table, and that table no longer exists.
    partition_id UUID,
    consumer_group VARCHAR(255) DEFAULT '__QUEUE_MODE__',
    
    -- Universal counters
    child_count INTEGER NOT NULL DEFAULT 0,       -- partitions for queue, queues for namespace, etc.
    total_messages BIGINT NOT NULL DEFAULT 0,
    pending_messages BIGINT NOT NULL DEFAULT 0,
    processing_messages BIGINT NOT NULL DEFAULT 0,
    completed_messages BIGINT NOT NULL DEFAULT 0,
    dead_letter_messages BIGINT NOT NULL DEFAULT 0,

    -- Retained payload bytes for this stat row (segments engine, 'queue' rows).
    -- Track B / storage quota (PLAN_QUEEN_PROXY_CLOUD.md §6.1): the sum of
    -- octet_length(log_segments.blob) over the queue's LIVE (retained) segments,
    -- i.e. the compressed (zstd) on-the-wire payload bytes as stored — TOAST
    -- bookkeeping, indexes, WAL and the log_txns hash sidecar are NOT counted.
    -- Populated at the stats-refresh cadence by log_refresh_all_stats_v1 (048),
    -- so it lags one refresh interval (the proxy storage quota is hysteretic).
    retained_bytes BIGINT NOT NULL DEFAULT 0,

    -- Time bounds (for lag calculations)
    oldest_pending_at TIMESTAMPTZ,
    newest_message_at TIMESTAMPTZ,
    
    -- Lag metrics (pre-computed during roll-up)
    avg_lag_seconds INTEGER DEFAULT 0,
    max_lag_seconds INTEGER DEFAULT 0,
    median_lag_seconds INTEGER DEFAULT 0,
    avg_offset_lag INTEGER DEFAULT 0,
    max_offset_lag INTEGER DEFAULT 0,
    
    -- Throughput metrics (rate per second, computed from deltas)
    ingested_per_second NUMERIC(10,2) DEFAULT 0,
    processed_per_second NUMERIC(10,2) DEFAULT 0,
    
    -- Delta tracking for rate calculation
    prev_total_messages BIGINT DEFAULT 0,
    prev_completed_messages BIGINT DEFAULT 0,
    prev_snapshot_at TIMESTAMPTZ,
    
    -- Incremental scan tracking (for partition stats)
    last_scanned_at TIMESTAMPTZ,              -- Last time we scanned messages for this partition
    last_scanned_message_id UUID,             -- Last message ID we counted
    
    -- Metadata
    last_computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (stat_type, stat_key)
);

-- Indexes for efficient lookups
CREATE INDEX IF NOT EXISTS idx_stats_type ON queen.stats(stat_type);
CREATE INDEX IF NOT EXISTS idx_stats_queue_id ON queen.stats(queue_id) WHERE queue_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_stats_partition_id ON queen.stats(partition_id) WHERE partition_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_stats_computed ON queen.stats(last_computed_at);
-- Track B / storage quota (§6.1): retained payload bytes per stat row. Catalog-
-- only migration for existing DBs (no backfill; the next refresh cycle populates it).
ALTER TABLE queen.stats ADD COLUMN IF NOT EXISTS retained_bytes BIGINT NOT NULL DEFAULT 0;

-- Time-series history for throughput charts
CREATE TABLE IF NOT EXISTS queen.stats_history (
    stat_type VARCHAR(20) NOT NULL,
    stat_key VARCHAR(512) NOT NULL,
    bucket_time TIMESTAMPTZ NOT NULL,
    
    -- Snapshot of counters at this point
    total_messages BIGINT DEFAULT 0,
    pending_messages BIGINT DEFAULT 0,
    processing_messages BIGINT DEFAULT 0,
    completed_messages BIGINT DEFAULT 0,
    dead_letter_messages BIGINT DEFAULT 0,
    
    -- Delta since last bucket (for throughput)
    messages_ingested INTEGER DEFAULT 0,
    messages_processed INTEGER DEFAULT 0,
    
    -- Lag at this point
    max_lag_seconds INTEGER DEFAULT 0,
    
    PRIMARY KEY (stat_type, stat_key, bucket_time)
);

CREATE INDEX IF NOT EXISTS idx_stats_history_time ON queen.stats_history(bucket_time DESC);
CREATE INDEX IF NOT EXISTS idx_stats_history_lookup ON queen.stats_history(stat_type, stat_key, bucket_time DESC);
-- ============================================================================
-- Legacy index cleanup (safe to re-run; see DISCOVERY.md finding #3)
-- ============================================================================
-- These two indexes were discovered to be redundant with
-- messages_partition_transaction_unique (partition_id, transaction_id):
--   - idx_messages_transaction_id       (transaction_id)           → prefix
--   - idx_messages_txn_partition        (transaction_id, partition_id) → reversed columns
--
-- Neither is referenced by any hot query path (push's ON CONFLICT uses the
-- UNIQUE, pop's cursor scan uses idx_messages_partition_created). On
-- long-running instances these grew to 3–4 GB each, pushing the total
-- messages index footprint above shared_buffers and causing cache thrashing.
--
-- The DROPs below are idempotent (IF EXISTS). On fresh databases they are
-- no-ops; on existing databases they reclaim the space at server startup.
-- Schema apply takes an AccessExclusive lock per DROP — acceptable because
-- it runs at Queen boot before workload resumes.
DROP INDEX IF EXISTS queen.idx_messages_transaction_id;
DROP INDEX IF EXISTS queen.idx_messages_txn_partition;

-- ============================================================================
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_queues_name ON queen.queues(name);
-- NOTE: idx_messages_partition_created_id removed - duplicate of idx_messages_partition_created
-- NOTE: idx_messages_transaction_id removed - see "Legacy index cleanup" above.


CREATE INDEX IF NOT EXISTS idx_retention_history_partition ON queen.retention_history(partition_id);
CREATE INDEX IF NOT EXISTS idx_retention_history_executed ON queen.retention_history(executed_at);
CREATE INDEX IF NOT EXISTS idx_system_metrics_timestamp ON queen.system_metrics(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_system_metrics_replica ON queen.system_metrics(hostname, port);
CREATE INDEX IF NOT EXISTS idx_system_metrics_worker ON queen.system_metrics(worker_id);
CREATE INDEX IF NOT EXISTS idx_system_metrics_metrics ON queen.system_metrics USING GIN (metrics);
CREATE INDEX IF NOT EXISTS idx_message_traces_message_id ON queen.message_traces(message_id);
CREATE INDEX IF NOT EXISTS idx_message_traces_transaction_partition ON queen.message_traces(transaction_id, partition_id);
CREATE INDEX IF NOT EXISTS idx_message_traces_created_at ON queen.message_traces(created_at DESC);
-- Supports the trace readers' partition_id lookups. The FK this index was
-- originally provisioned for is gone (queen.message_traces now carries log ids
-- and no constraint), but the reader joins remain.
CREATE INDEX IF NOT EXISTS idx_message_traces_partition_id ON queen.message_traces(partition_id);
CREATE INDEX IF NOT EXISTS idx_message_trace_names_name ON queen.message_trace_names(trace_name);
CREATE INDEX IF NOT EXISTS idx_message_trace_names_trace_id ON queen.message_trace_names(trace_id);
CREATE INDEX IF NOT EXISTS idx_system_state_key ON queen.system_state(key);

-- ============================================================================
-- Storage parameters
-- ============================================================================

ALTER TABLE queen.stats SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 50,
    autovacuum_vacuum_cost_delay = 0,
    fillfactor = 50
);

-- No triggers are defined here. The rows engine's partition-lookup trigger and
-- its function went with the rows tables on 2026-07-30; the log engine's own
-- partition-lifecycle counters live on queen.log_partitions (014).
