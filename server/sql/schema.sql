-- ============================================================================
-- Queen Message Queue - Consolidated Database Schema
-- Version: 0 (base schema)
-- ============================================================================
--
-- This file contains the complete current schema for Queen MQ.
-- It is designed to be idempotent (safe to run multiple times).
--
-- Usage: applied at every boot, together with procedures/001..023.
-- Deployments are always-virgin — there is no upgrade/migration path.
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
    -- identity. Uniqueness is (tenant_id, name), enforced by the
    -- queues_tenant_name_uk index below.
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    namespace VARCHAR(255),
    task VARCHAR(255),
    priority INTEGER DEFAULT 0,
    -- 60, not 300 (2026-07-31, queue-identity merge): 60 is what an implicitly
    -- push-created queue has ALWAYS leased at — the pop path read it from
    -- log_queues (default 60) while this column's old 300 was only ever shown
    -- by the config display, which therefore lied. /configure still writes an
    -- explicit 300 when leaseTime is omitted, so configured queues are
    -- unchanged. One column, one truth.
    lease_time INTEGER DEFAULT 60,
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
    -- Absorbed from queen.log_queues (2026-07-31 queue-identity merge): the
    -- push dedup probe window. 0 disables the probe.
    dedup_window_seconds INTEGER NOT NULL DEFAULT 3600,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- This is a log-engine-only broker: messages live in queen.log_segments,
-- partitions in queen.log_partitions, cursors in queen.log_consumers and dead
-- letters in queen.log_dlq (001/005). The rows engine's tables and the
-- migration files that used to clean them up are gone: deployments start from
-- an EMPTY database, so this file plus procedures/001..023 IS the schema —
-- there is no upgrade path to carry.

-- There is no `storage` engine-selector column: one engine, nothing to
-- select. The wire contract still echoes storage:"segments" as a literal
-- (012/010/011).

CREATE TABLE IF NOT EXISTS queen.consumer_groups_metadata (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Queue identity is queen.queues(id) — 2026-07-31 merge. queue_id is NULL
    -- only for namespace/task DISCOVERY subscriptions, which genuinely name no
    -- queue; a queue-scoped registration upserts the queues row first (a group
    -- may subscribe before any push creates the queue), so the FK always
    -- resolves. tenant_id survives for the namespace/task rows, which have no
    -- queue row to inherit scoping from.
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    consumer_group TEXT NOT NULL,
    queue_id UUID REFERENCES queen.queues(id) ON DELETE CASCADE,
    partition_name TEXT NOT NULL DEFAULT '',
    namespace TEXT NOT NULL DEFAULT '',
    task TEXT NOT NULL DEFAULT '',
    subscription_mode TEXT NOT NULL,
    subscription_timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- NULLS NOT DISTINCT (PG >= 15): two discovery rows with queue_id NULL and the
-- same (group, namespace, task) must collide, or re-registration duplicates.
CREATE UNIQUE INDEX IF NOT EXISTS cgm_identity_uk
    ON queen.consumer_groups_metadata(tenant_id, consumer_group, queue_id, partition_name, namespace, task)
    NULLS NOT DISTINCT;
CREATE INDEX IF NOT EXISTS idx_cgm_queue ON queen.consumer_groups_metadata(queue_id);

-- Watermark tracking for efficient wildcard POP discovery
-- Tracks the timestamp of the last "empty scan" per (queue, consumer_group)
-- When a consumer finds no pending data, we update their watermark to NOW()
-- On next POP, we only scan partitions updated since the watermark
-- This avoids expensive full scans for up-to-date consumers
CREATE TABLE IF NOT EXISTS queen.consumer_watermarks (
    -- Keyed by queue ID, not name (2026-07-31 merge): the tenant scoping the
    -- old (tenant_id, queue_name) pair provided is inherited from the queues
    -- row itself, and the cascade clears the floor when the queue goes.
    queue_id UUID NOT NULL REFERENCES queen.queues(id) ON DELETE CASCADE,
    consumer_group VARCHAR(255) NOT NULL DEFAULT '__QUEUE_MODE__',
    last_empty_scan_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (queue_id, consumer_group)
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
    -- outlive the segment it describes.
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

-- Queue identity is (tenant_id, name), enforced here — the composite unique
-- index every by-name resolve relies on. (Formerly established by a Track B
-- migration block; deployments are always-virgin now, so the CREATE TABLE
-- above plus this index ARE the shape.)
CREATE UNIQUE INDEX IF NOT EXISTS queues_tenant_name_uk ON queen.queues(tenant_id, name);

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
    -- Populated at the stats-refresh cadence by log_refresh_all_stats_v1
    -- (011_log_stats), so it lags one refresh interval (the proxy storage
    -- quota is hysteretic).
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
-- Indexes
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_queues_name ON queen.queues(name);

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
-- partition-lifecycle counters live on queen.log_partitions
-- (019_worker_metrics; attachment in 020_log_partition_counters).
