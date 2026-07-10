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
    name VARCHAR(255) UNIQUE NOT NULL,
    namespace VARCHAR(255),
    task VARCHAR(255),
    priority INTEGER DEFAULT 0,
    lease_time INTEGER DEFAULT 300,
    retry_limit INTEGER DEFAULT 3,
    retry_delay INTEGER DEFAULT 1000,
    ttl INTEGER DEFAULT 3600,
    dead_letter_queue BOOLEAN DEFAULT FALSE,
    dlq_after_max_retries BOOLEAN DEFAULT FALSE,
    delayed_processing INTEGER DEFAULT 0,
    window_buffer INTEGER DEFAULT 0,
    retention_seconds INTEGER DEFAULT 0,
    completed_retention_seconds INTEGER DEFAULT 0,
    retention_enabled BOOLEAN DEFAULT FALSE,
    encryption_enabled BOOLEAN DEFAULT FALSE,
    max_wait_time_seconds INTEGER DEFAULT 0,
    max_queue_size INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS queen.partitions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_id UUID REFERENCES queen.queues(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL DEFAULT 'Default',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_activity TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(queue_id, name)
);

CREATE TABLE IF NOT EXISTS queen.messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id VARCHAR(255) NOT NULL,
    trace_id UUID DEFAULT gen_random_uuid(),
    partition_id UUID REFERENCES queen.partitions(id) ON DELETE CASCADE,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    is_encrypted BOOLEAN DEFAULT FALSE,
    -- Server-stamped authenticated producer identity (JWT 'sub' claim) when JWT auth is enabled.
    -- Clients CANNOT set this field; it is always set by the server from the validated JWT.
    -- NULL means: (a) auth disabled, (b) pre-feature message, or (c) message produced by an
    -- internal path (e.g. failover replay where the original sub was not preserved).
    producer_sub TEXT
);

-- Idempotent upgrade for existing installations (safe on tables with millions of rows:
-- nullable column with no default is a catalog-only change in Postgres >= 11 - no table rewrite).
ALTER TABLE queen.messages ADD COLUMN IF NOT EXISTS producer_sub TEXT;

-- Storage engine selector (storage v2 "segments" is per-queue opt-in; the
-- engine itself lives in procedures/023_storage_v2.sql under schema q2).
-- Catalog-only change on PG >= 11: constant default, no table rewrite.
ALTER TABLE queen.queues ADD COLUMN IF NOT EXISTS storage VARCHAR(16) NOT NULL DEFAULT 'rows';

-- Unique constraint scoped to partition (not global)
CREATE UNIQUE INDEX IF NOT EXISTS messages_partition_transaction_unique 
    ON queen.messages(partition_id, transaction_id);

-- Aggressive per-table autovacuum for queen.messages. Under high-throughput
-- retention (insert-new / delete-old churn) the default scale-factor lets dead
-- tuples accumulate faster than they're reclaimed, the heap grows past RAM, and
-- both the dedup-unique probe (on insert) and retention DELETEs start hitting
-- cold pages -> throughput collapses. These settings keep vacuum on pace with
-- parallel retention so the heap plateaus via slot reuse. Idempotent; storage
-- params only (no table rewrite, safe on existing installations).
ALTER TABLE queen.messages SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_insert_scale_factor = 0.05,
    autovacuum_vacuum_cost_limit = 4000,
    autovacuum_vacuum_cost_delay = 0
);

CREATE TABLE IF NOT EXISTS queen.partition_consumers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    partition_id UUID REFERENCES queen.partitions(id) ON DELETE CASCADE,
    consumer_group VARCHAR(255) DEFAULT '__QUEUE_MODE__',
    last_consumed_id UUID DEFAULT '00000000-0000-0000-0000-000000000000',
    last_consumed_created_at TIMESTAMPTZ,
    total_messages_consumed BIGINT DEFAULT 0,
    total_batches_consumed BIGINT DEFAULT 0,
    last_consumed_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    lease_acquired_at TIMESTAMPTZ,
    message_batch JSONB,
    batch_size INTEGER DEFAULT 0,
    acked_count INTEGER DEFAULT 0,
    worker_id VARCHAR(255),
    pending_estimate BIGINT DEFAULT 0,
    last_stats_update TIMESTAMPTZ,
    batch_retry_count INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(partition_id, consumer_group),
    CHECK (
        (last_consumed_id = '00000000-0000-0000-0000-000000000000' 
         AND last_consumed_created_at IS NULL)
        OR 
        (last_consumed_id != '00000000-0000-0000-0000-000000000000' 
         AND last_consumed_created_at IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS queen.consumer_groups_metadata (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    consumer_group TEXT NOT NULL,
    queue_name TEXT NOT NULL DEFAULT '',
    partition_name TEXT NOT NULL DEFAULT '',
    namespace TEXT NOT NULL DEFAULT '',
    task TEXT NOT NULL DEFAULT '',
    subscription_mode TEXT NOT NULL,
    subscription_timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (consumer_group, queue_name, partition_name, namespace, task)
);

CREATE INDEX IF NOT EXISTS idx_consumer_groups_metadata_lookup 
ON queen.consumer_groups_metadata(consumer_group, queue_name, namespace, task);

-- Watermark tracking for efficient wildcard POP discovery
-- Tracks the timestamp of the last "empty scan" per (queue, consumer_group)
-- When a consumer finds no pending data, we update their watermark to NOW()
-- On next POP, we only scan partitions updated since the watermark
-- This avoids expensive full scans for up-to-date consumers
CREATE TABLE IF NOT EXISTS queen.consumer_watermarks (
    queue_name VARCHAR(255) NOT NULL,
    consumer_group VARCHAR(255) NOT NULL DEFAULT '__QUEUE_MODE__',
    last_empty_scan_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (queue_name, consumer_group)
);

CREATE TABLE IF NOT EXISTS queen.messages_consumed (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    partition_id UUID REFERENCES queen.partitions(id) ON DELETE CASCADE,
    consumer_group VARCHAR(255) NOT NULL,
    messages_completed INTEGER DEFAULT 0,
    messages_failed INTEGER DEFAULT 0,
    acked_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS queen.dead_letter_queue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id UUID REFERENCES queen.messages(id) ON DELETE CASCADE,
    partition_id UUID REFERENCES queen.partitions(id) ON DELETE CASCADE,
    consumer_group VARCHAR(255),
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    original_created_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS queen.retention_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    partition_id UUID REFERENCES queen.partitions(id) ON DELETE CASCADE,
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
    message_id UUID REFERENCES queen.messages(id) ON DELETE CASCADE,
    partition_id UUID REFERENCES queen.partitions(id) ON DELETE CASCADE,
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

CREATE TABLE IF NOT EXISTS queen.partition_lookup (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_name VARCHAR(255) NOT NULL REFERENCES queen.queues(name) ON DELETE CASCADE,
    partition_id UUID NOT NULL REFERENCES queen.partitions(id) ON DELETE CASCADE,
    last_message_id UUID NOT NULL,
    last_message_created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(queue_name, partition_id)
);


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
    partition_id UUID REFERENCES queen.partitions(id) ON DELETE CASCADE,
    consumer_group VARCHAR(255) DEFAULT '__QUEUE_MODE__',
    
    -- Universal counters
    child_count INTEGER NOT NULL DEFAULT 0,       -- partitions for queue, queues for namespace, etc.
    total_messages BIGINT NOT NULL DEFAULT 0,
    pending_messages BIGINT NOT NULL DEFAULT 0,
    processing_messages BIGINT NOT NULL DEFAULT 0,
    completed_messages BIGINT NOT NULL DEFAULT 0,
    dead_letter_messages BIGINT NOT NULL DEFAULT 0,
    
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


CREATE INDEX IF NOT EXISTS idx_messages_consumed_acked_at ON queen.messages_consumed(acked_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_consumed_partition_acked ON queen.messages_consumed(partition_id, acked_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_consumed_consumer_acked ON queen.messages_consumed(consumer_group, acked_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_consumed_partition_id ON queen.messages_consumed(partition_id);
CREATE INDEX IF NOT EXISTS idx_dlq_partition ON queen.dead_letter_queue(partition_id);
CREATE INDEX IF NOT EXISTS idx_dlq_consumer_group ON queen.dead_letter_queue(consumer_group);
CREATE INDEX IF NOT EXISTS idx_dlq_failed_at ON queen.dead_letter_queue(failed_at DESC);
CREATE INDEX IF NOT EXISTS idx_dlq_message_consumer ON queen.dead_letter_queue(message_id, consumer_group);
CREATE INDEX IF NOT EXISTS idx_retention_history_partition ON queen.retention_history(partition_id);
CREATE INDEX IF NOT EXISTS idx_retention_history_executed ON queen.retention_history(executed_at);
CREATE INDEX IF NOT EXISTS idx_system_metrics_timestamp ON queen.system_metrics(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_system_metrics_replica ON queen.system_metrics(hostname, port);
CREATE INDEX IF NOT EXISTS idx_system_metrics_worker ON queen.system_metrics(worker_id);
CREATE INDEX IF NOT EXISTS idx_system_metrics_metrics ON queen.system_metrics USING GIN (metrics);
CREATE INDEX IF NOT EXISTS idx_message_traces_message_id ON queen.message_traces(message_id);
CREATE INDEX IF NOT EXISTS idx_message_traces_transaction_partition ON queen.message_traces(transaction_id, partition_id);
CREATE INDEX IF NOT EXISTS idx_message_traces_created_at ON queen.message_traces(created_at DESC);
-- Required FK supporting index for queen.message_traces.partition_id (ON DELETE CASCADE).
-- Without this, every parent row deleted from queen.partitions triggers a full seq scan of
-- queen.message_traces during the cascade, making partition cleanup take tens of seconds.
CREATE INDEX IF NOT EXISTS idx_message_traces_partition_id ON queen.message_traces(partition_id);
CREATE INDEX IF NOT EXISTS idx_message_trace_names_name ON queen.message_trace_names(trace_name);
CREATE INDEX IF NOT EXISTS idx_message_trace_names_trace_id ON queen.message_trace_names(trace_id);
CREATE INDEX IF NOT EXISTS idx_system_state_key ON queen.system_state(key);

-- Stored procedure indexes
-- NOTE: idx_messages_txn_partition removed - see "Legacy index cleanup" at top.
CREATE INDEX IF NOT EXISTS idx_messages_partition_created ON queen.messages (partition_id, created_at, id);
-- NOTE: idx_partition_lookup_queue_message_ts removed - it prevented HOT updates
-- causing severe bloat on this high-update table (600x bloat in 10 hours)
--
-- The index below is HOT-safe: partition_id is set on insert and never modified by
-- the upsert path (which only changes last_message_id, last_message_created_at,
-- updated_at). It is also already in the HOT-breaking column set via the existing
-- UNIQUE(queue_name, partition_id) constraint, so adding it does not enlarge the
-- set of indexed columns. Required as the FK supporting index for the
-- ON DELETE CASCADE from queen.partitions; without it, partition cleanup
-- triggers a seq scan of partition_lookup per deleted parent row.
CREATE INDEX IF NOT EXISTS idx_partition_lookup_partition_id ON queen.partition_lookup(partition_id);

-- Supports queen.renew_lease_v2 (filters partition_consumers by worker_id alone)
-- and the lease-validation EXISTS subqueries in queen.ack_messages_v2 and
-- queen.execute_transaction_v2. Without this, every renew_lease_v2 inner-loop
-- iteration sequentially scans partition_consumers, pushing avg latency to
-- ~90ms per call on prod-class deployments (5k-10k+ rows). With the index, the
-- UPDATE/EXISTS lookups become O(log N) B-tree descents.
--
-- Partial on (worker_id IS NOT NULL): the table holds one row per
-- (partition_id, consumer_group), most of which sit at worker_id IS NULL
-- (no active lease). Indexing only the active-lease rows keeps the index in
-- the kilobyte range and bounds churn to lease-acquire/release transitions
-- (one entry insert per pop, one delete per ack/transaction-completion).
--
-- HOT trade-off: pop_unified_batch_v4 / ack_messages_v2 mutate worker_id at
-- lease boundaries and so lose HOT eligibility for those specific UPDATEs,
-- paying one extra index-page write per lease cycle. renew_lease_v2 does NOT
-- mutate worker_id (only lease_expires_at), so it stays HOT-safe even with
-- this index in place. The fillfactor=50 setting on partition_consumers was
-- already provisioned to absorb non-HOT in-page updates.
--
-- Production rollout: deploy via CREATE INDEX CONCURRENTLY first (cannot run
-- inside the transactional schema apply), then this IF NOT EXISTS form is a
-- no-op on existing prod databases and creates the index on fresh deploys.
CREATE INDEX IF NOT EXISTS idx_partition_consumers_worker_id_active
    ON queen.partition_consumers (worker_id)
    WHERE worker_id IS NOT NULL;

-- ============================================================================
-- Trigger Functions
-- ============================================================================

-- Partition lookup trigger (statement-level, batch-efficient)
-- NOTE: ORDER BY partition_id ensures consistent lock ordering to prevent deadlocks
CREATE OR REPLACE FUNCTION queen.update_partition_lookup_trigger()
RETURNS TRIGGER AS $$
BEGIN
    WITH batch_max AS (
        SELECT DISTINCT ON (partition_id)
            partition_id, 
            created_at as max_created_at,
            id as max_id
        FROM new_messages
        ORDER BY partition_id, created_at DESC, id DESC
    )
    INSERT INTO queen.partition_lookup (
        queue_name, partition_id, last_message_id, last_message_created_at, updated_at
    )
    SELECT 
        q.name, 
        bm.partition_id, 
        bm.max_id, 
        bm.max_created_at, 
        NOW()
    FROM batch_max bm
    JOIN queen.partitions p ON p.id = bm.partition_id
    JOIN queen.queues q ON q.id = p.queue_id
    ORDER BY bm.partition_id  -- Consistent lock ordering prevents deadlocks
    ON CONFLICT (queue_name, partition_id)
    DO UPDATE SET
        last_message_id = EXCLUDED.last_message_id,
        last_message_created_at = EXCLUDED.last_message_created_at,
        updated_at = NOW()
    WHERE 
        EXCLUDED.last_message_created_at > queen.partition_lookup.last_message_created_at
        OR (EXCLUDED.last_message_created_at = queen.partition_lookup.last_message_created_at 
            AND EXCLUDED.last_message_id > queen.partition_lookup.last_message_id);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;




ALTER TABLE queen.partition_lookup SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 50,
    autovacuum_vacuum_cost_delay = 0,
    fillfactor = 50
);

ALTER TABLE queen.partition_consumers SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 50,
    autovacuum_vacuum_cost_delay = 0,
    fillfactor = 50
);

ALTER TABLE queen.stats SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 50,
    autovacuum_vacuum_cost_delay = 0,
    fillfactor = 50
);

-- ============================================================================
-- Triggers
-- ============================================================================

-- PUSHPOPLOOKUPSOL: partition_lookup is now maintained by libqueen after each
-- successful push commit via queen.update_partition_lookup_v1(), plus a
-- periodic queen.reconcile_partition_lookup_v1() safety-net every 5s. The
-- synchronous trigger is unconditionally dropped on schema apply to remove
-- the push-vs-push row-lock contention it caused.
--
-- Rollback: remove this DROP and re-add the CREATE OR REPLACE TRIGGER block
-- below. The trigger function body (queen.update_partition_lookup_trigger)
-- is still defined above for this purpose.
DROP TRIGGER IF EXISTS trg_update_partition_lookup ON queen.messages;

-- pop_unified_batch_v2_noorder was renamed to pop_unified_batch_v3 (see
-- lib/schema/procedures/002c_pop_unified_v3.sql). This DROP makes the
-- schema apply idempotent: fresh databases never see the old name, and
-- upgraded databases have the orphaned old function removed on first
-- re-apply. Safe to remove this line once all deployments have
-- re-applied the schema at least once.
DROP FUNCTION IF EXISTS queen.pop_unified_batch_v2_noorder(JSONB);

-- pop_unified_batch_v4 (lib/schema/procedures/002d_pop_unified_v4.sql) is
-- the multi-partition successor to v3. With max_partitions=1 (default) the
-- response shape is byte-equivalent to v3 plus per-message partition info.
-- v3 is intentionally kept around as an emergency-revert target — flipping
-- the dispatched SQL string in lib/queen/pending_job.hpp back to
-- pop_unified_batch_v3 fully reverts the multi-partition feature.
-- Procedure files are auto-discovered by libqueen (alphabetical order), so
-- no explicit include is required here.
