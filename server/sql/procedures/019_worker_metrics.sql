-- ============================================================================
-- Worker Metrics Tables
-- ============================================================================
-- Real-time metrics collected from event loop workers.
-- Provides accurate throughput and lag measurements without expensive table scans.
-- ============================================================================

-- Worker-level metrics (per worker, per minute)
CREATE TABLE IF NOT EXISTS queen.worker_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Worker identity
    hostname VARCHAR(255) NOT NULL,
    worker_id INTEGER NOT NULL,
    pid INTEGER NOT NULL,
    
    -- Time bucket (minute granularity)
    bucket_time TIMESTAMPTZ NOT NULL,
    
    -- Event loop health
    avg_event_loop_lag_ms INTEGER DEFAULT 0,
    max_event_loop_lag_ms INTEGER DEFAULT 0,
    
    -- Connection pool
    avg_free_slots INTEGER DEFAULT 0,
    min_free_slots INTEGER DEFAULT 0,
    db_connections INTEGER DEFAULT 0,
    
    -- Job queue pressure
    avg_job_queue_size INTEGER DEFAULT 0,
    max_job_queue_size INTEGER DEFAULT 0,
    backoff_size INTEGER DEFAULT 0,
    
    -- Throughput: Request counts (number of API calls)
    jobs_done BIGINT DEFAULT 0,
    push_request_count BIGINT DEFAULT 0,
    pop_request_count BIGINT DEFAULT 0,
    ack_request_count BIGINT DEFAULT 0,
    transaction_count BIGINT DEFAULT 0,
    
    -- Throughput: Message counts (actual messages processed)
    push_message_count BIGINT DEFAULT 0,   -- Messages pushed
    pop_message_count BIGINT DEFAULT 0,    -- Messages popped (can exceed push if multiple consumer groups)
    ack_message_count BIGINT DEFAULT 0,    -- Ack attempts (success + failed)
    ack_success_count BIGINT DEFAULT 0,    -- Successful acks
    ack_failed_count BIGINT DEFAULT 0,     -- Failed acks (retries, errors)
    
    -- Lag metrics (aggregated across all queues for this worker)
    -- Note: lag is per pop, so a message consumed by 2 groups = 2 lag measurements
    lag_count BIGINT DEFAULT 0,       -- Number of pops measured (for weighted avg)
    avg_lag_ms BIGINT DEFAULT 0,
    max_lag_ms BIGINT DEFAULT 0,
    
    -- Error metrics
    db_error_count BIGINT DEFAULT 0,  -- DB query failures
    dlq_count BIGINT DEFAULT 0,       -- Messages moved to DLQ
    
    -- Timestamp
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(hostname, worker_id, pid, bucket_time)
);

-- Indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_worker_metrics_bucket ON queen.worker_metrics(bucket_time DESC);
CREATE INDEX IF NOT EXISTS idx_worker_metrics_worker ON queen.worker_metrics(hostname, worker_id);
CREATE INDEX IF NOT EXISTS idx_worker_metrics_created ON queen.worker_metrics(created_at);

-- Per-queue ops metrics (aggregated across all workers).
-- Historically this table only tracked pop_count + lag; it now also tracks
-- per-queue push / ack / transaction counts (flushed from libqueen's
-- in-memory WorkerMetrics at minute boundaries) and partition lifecycle
-- events (bumped by statement-level triggers on queen.log_partitions).
-- The idempotent ALTERs below add the newer columns on first boot (no-ops on
-- every boot after).
-- Queue identity is the queen.queues id (2026-07-31 queue-identity merge):
-- queue_id replaces the old (queue_name[, tenant_id]) key, the FK cascade
-- cleans up on queue delete, and tenant scoping is inherited from the queues
-- row. This is the only shape — deployments are always-virgin (the retired
-- queue-identity merge migration used to carry old-shape DBs here).
CREATE TABLE IF NOT EXISTS queen.queue_lag_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bucket_time TIMESTAMPTZ NOT NULL,
    queue_id UUID NOT NULL REFERENCES queen.queues(id) ON DELETE CASCADE,

    -- Aggregated across all workers for this queue
    pop_count BIGINT DEFAULT 0,
    avg_lag_ms BIGINT DEFAULT 0,
    max_lag_ms BIGINT DEFAULT 0,

    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Per-queue ops counters (PR 3). Flushed once/minute per worker from libqueen.
ALTER TABLE queen.queue_lag_metrics ADD COLUMN IF NOT EXISTS push_request_count BIGINT DEFAULT 0;
ALTER TABLE queen.queue_lag_metrics ADD COLUMN IF NOT EXISTS push_message_count BIGINT DEFAULT 0;
ALTER TABLE queen.queue_lag_metrics ADD COLUMN IF NOT EXISTS pop_empty_count    BIGINT DEFAULT 0;
ALTER TABLE queen.queue_lag_metrics ADD COLUMN IF NOT EXISTS ack_request_count  BIGINT DEFAULT 0;
ALTER TABLE queen.queue_lag_metrics ADD COLUMN IF NOT EXISTS ack_success_count  BIGINT DEFAULT 0;
ALTER TABLE queen.queue_lag_metrics ADD COLUMN IF NOT EXISTS ack_failed_count   BIGINT DEFAULT 0;
ALTER TABLE queen.queue_lag_metrics ADD COLUMN IF NOT EXISTS transaction_count  BIGINT DEFAULT 0;
-- Partition lifecycle counters (PR 3b). Bumped by queen.log_partitions INSERT /
-- DELETE statement-level triggers. partition_count is a snapshot written once
-- per minute by StatsService::run_fast_aggregation.
ALTER TABLE queen.queue_lag_metrics ADD COLUMN IF NOT EXISTS partitions_created INTEGER DEFAULT 0;
ALTER TABLE queen.queue_lag_metrics ADD COLUMN IF NOT EXISTS partitions_deleted INTEGER DEFAULT 0;
ALTER TABLE queen.queue_lag_metrics ADD COLUMN IF NOT EXISTS partition_count    INTEGER;
-- Currently-parked long-poll POP requests per queue. Sampled ~1Hz by
-- libqueen's stats timer; what's persisted at each minute-boundary flush is
-- the AVERAGE across the ~60 within-minute samples (not the last value).
-- This is a GAUGE, so:
--   - Across workers in the same minute we SUM (cluster-wide total parked).
--   - Across time buckets we AVG (typical gauge value over the window).
-- See get_queue_ops_v1 below.
ALTER TABLE queen.queue_lag_metrics ADD COLUMN IF NOT EXISTS parked_count       INTEGER DEFAULT 0;
-- Number of messages the avg_lag_ms average covers in this bucket. Lets the
-- cross-replica upsert merge avg_lag_ms as a weighted average (the same
-- SUM(avg*count)/SUM(count) identity the worker_metrics readers use).
ALTER TABLE queen.queue_lag_metrics ADD COLUMN IF NOT EXISTS lag_count          BIGINT DEFAULT 0;
-- (The Track-B tenant_id column + (bucket, name, tenant) unique key are gone:
-- queue_id carries both halves of the old key, and two tenants sharing a queue
-- NAME are distinct queue ids by construction.)

CREATE INDEX IF NOT EXISTS idx_queue_lag_bucket ON queen.queue_lag_metrics(bucket_time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS qlm_bucket_queue_uk
    ON queen.queue_lag_metrics(bucket_time, queue_id);
CREATE INDEX IF NOT EXISTS idx_queue_lag_queue
    ON queen.queue_lag_metrics(queue_id);

-- ============================================================================
-- Per-replica parked-count breakdown
-- ============================================================================
--
-- queen.queue_lag_metrics already stores parked_count *cluster-aggregated*
-- (SUMmed across workers via the upsert), which is what the default Parked
-- chart on the System view consumes. A parked long-poll, however, lives on
-- exactly ONE worker (the one that owns the HTTP connection), so it's
-- operationally useful to break the gauge down per replica — e.g. to detect
-- load-balancer skew that pins all consumers to one host.
--
-- This table holds the per-replica view. Each worker writes its own
-- minute-averaged parked_count into its own row. The aggregate value in
-- queue_lag_metrics.parked_count remains authoritative for the cluster-wide
-- chart (one INSERT per worker per minute either way; tiny extra cost).
--
-- We deliberately keep the breakdown narrow (parked only) instead of
-- promoting all queue_lag_metrics columns to per-replica: load skew across
-- replicas is already visible per-worker in queen.worker_metrics; only
-- parked needs per-(replica × queue) attribution.
CREATE TABLE IF NOT EXISTS queen.queue_parked_replica (
    bucket_time TIMESTAMPTZ NOT NULL,
    queue_name  VARCHAR(512) NOT NULL,
    hostname    VARCHAR(255) NOT NULL,
    worker_id   INTEGER      NOT NULL,
    parked_count INTEGER DEFAULT 0,
    PRIMARY KEY (bucket_time, queue_name, hostname, worker_id)
);
-- Track B (§5): tenant scoping key (same rationale as queue_lag_metrics). The PK
-- gains tenant_id so a shared queue name on one replica keeps per-tenant rows.
-- Guarded so the PK is rebuilt exactly once (first boot), never churned on
-- every boot.
ALTER TABLE queen.queue_parked_replica ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';
DO $$
DECLARE v_has_tenant_pk BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = 'queen.queue_parked_replica'::regclass AND i.indisprimary
          AND a.attname = 'tenant_id'
    ) INTO v_has_tenant_pk;
    IF NOT v_has_tenant_pk THEN
        ALTER TABLE queen.queue_parked_replica DROP CONSTRAINT IF EXISTS queue_parked_replica_pkey;
        ALTER TABLE queen.queue_parked_replica
            ADD PRIMARY KEY (bucket_time, queue_name, hostname, worker_id, tenant_id);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_queue_parked_replica_bucket
    ON queen.queue_parked_replica(bucket_time DESC);
CREATE INDEX IF NOT EXISTS idx_queue_parked_replica_queue
    ON queen.queue_parked_replica(queue_name);

-- ============================================================================
-- Query Procedures
-- ============================================================================

-- Get aggregated throughput metrics across all workers
CREATE OR REPLACE FUNCTION queen.get_worker_throughput_v1(
    p_from TIMESTAMPTZ DEFAULT NOW() - INTERVAL '1 hour',
    p_to TIMESTAMPTZ DEFAULT NOW(),
    p_bucket_minutes INTEGER DEFAULT 1
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN (
        SELECT COALESCE(jsonb_agg(
            jsonb_build_object(
                'timestamp', to_char(bucket, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
                -- Request rates
                'pushRequestsPerSecond', ROUND(push_request_count::numeric / 60, 2),
                'popRequestsPerSecond', ROUND(pop_request_count::numeric / 60, 2),
                'ackRequestsPerSecond', ROUND(ack_request_count::numeric / 60, 2),
                'jobsPerSecond', ROUND(jobs_done::numeric / 60, 2),
                -- Message rates (true throughput)
                'pushMessagesPerSecond', ROUND(push_message_count::numeric / 60, 2),
                'popMessagesPerSecond', ROUND(pop_message_count::numeric / 60, 2),
                'ackMessagesPerSecond', ROUND(ack_message_count::numeric / 60, 2),
                'ackSuccessPerSecond', ROUND(ack_success_count::numeric / 60, 2),
                'ackFailedPerSecond', ROUND(ack_failed_count::numeric / 60, 2),
                -- Lag
                'avgLagMs', avg_lag_ms,
                'maxLagMs', max_lag_ms,
                -- Errors
                'dbErrors', db_error_count,
                'dlqCount', dlq_count
            ) ORDER BY bucket DESC
        ), '[]'::jsonb)
        FROM (
            SELECT 
                date_trunc('minute', bucket_time) as bucket,
                SUM(push_request_count) as push_request_count,
                SUM(pop_request_count) as pop_request_count,
                SUM(ack_request_count) as ack_request_count,
                SUM(jobs_done) as jobs_done,
                SUM(push_message_count) as push_message_count,
                SUM(pop_message_count) as pop_message_count,
                SUM(ack_message_count) as ack_message_count,
                SUM(ack_success_count) as ack_success_count,
                SUM(ack_failed_count) as ack_failed_count,
                -- Weighted average for lag
                CASE 
                    WHEN SUM(lag_count) > 0 
                    THEN SUM(avg_lag_ms * lag_count) / SUM(lag_count)
                    ELSE 0 
                END as avg_lag_ms,
                MAX(max_lag_ms) as max_lag_ms,
                SUM(db_error_count) as db_error_count,
                SUM(dlq_count) as dlq_count
            FROM queen.worker_metrics
            WHERE bucket_time >= p_from AND bucket_time <= p_to
            GROUP BY 1
        ) t
    );
END;
$$;

-- Get per-queue lag metrics
-- Track B (§5): p_tenant scopes the lag series to one tenant's rows.
CREATE OR REPLACE FUNCTION queen.get_queue_lag_v1(
    p_from TIMESTAMPTZ DEFAULT NOW() - INTERVAL '1 hour',
    p_to TIMESTAMPTZ DEFAULT NOW(),
    p_queue_name TEXT DEFAULT NULL,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001'
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_bucket_minutes INTEGER;
BEGIN
    -- Roll the raw per-minute rows up to the SAME duration-derived bucket width
    -- get_queue_ops_v1 uses. Unrolled, a wide custom range (the QueueOperations
    -- picker accepts any from/to) returned one object per queue per MINUTE —
    -- a 7-day window over 200 queues is ~2M objects in a single response.
    v_bucket_minutes := CASE
        WHEN EXTRACT(EPOCH FROM (p_to - p_from)) / 60 <= 60 THEN 1
        WHEN EXTRACT(EPOCH FROM (p_to - p_from)) / 60 <= 360 THEN 5
        WHEN EXTRACT(EPOCH FROM (p_to - p_from)) / 60 <= 1440 THEN 15
        WHEN EXTRACT(EPOCH FROM (p_to - p_from)) / 60 <= 10080 THEN 60
        ELSE 360
    END;

    RETURN (
        SELECT COALESCE(jsonb_agg(
            jsonb_build_object(
                'queueName', queue_name,
                'popCount', pop_count,
                'avgLagMs', avg_lag_ms,
                'maxLagMs', max_lag_ms,
                -- Bucket width travels WITH each point (the endpoint's contract
                -- is a bare array, so there is no envelope to carry it in) —
                -- a client must not assume 1-minute points.
                'bucketMinutes', v_bucket_minutes,
                'bucketTime', to_char(bucket_time, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
            ) ORDER BY bucket_time DESC, queue_name
        ), '[]'::jsonb)
        FROM (
            -- Queue identity is the queen.queues id: the metrics row carries
            -- queue_id, the name (and the tenant filter) come from the joined
            -- queues row.
            SELECT q.name AS queue_name,
                   date_trunc('minute', qlm.bucket_time) -
                       (EXTRACT(minute FROM qlm.bucket_time)::integer % v_bucket_minutes)
                       * INTERVAL '1 minute' AS bucket_time,
                   SUM(qlm.pop_count) AS pop_count,
                   -- pop-weighted average; NULL (not 0) when the window held no
                   -- pop sample at all, so "unmeasured" stays distinct from "zero".
                   CASE WHEN SUM(qlm.pop_count) > 0
                        THEN SUM(qlm.avg_lag_ms * qlm.pop_count) / SUM(qlm.pop_count)
                        END AS avg_lag_ms,
                   CASE WHEN SUM(qlm.pop_count) > 0
                        THEN MAX(qlm.max_lag_ms)
                        END AS max_lag_ms
            FROM queen.queue_lag_metrics qlm
            JOIN queen.queues q ON q.id = qlm.queue_id
            WHERE qlm.bucket_time >= p_from
              AND qlm.bucket_time <= p_to
              AND q.tenant_id = p_tenant
              AND (p_queue_name IS NULL OR q.name = p_queue_name)
            GROUP BY 1, 2
        ) rolled
    );
END;
$$;

-- Get worker health metrics
CREATE OR REPLACE FUNCTION queen.get_worker_health_v1(
    p_from TIMESTAMPTZ DEFAULT NOW() - INTERVAL '5 minutes',
    p_to TIMESTAMPTZ DEFAULT NOW()
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN (
        SELECT COALESCE(jsonb_agg(
            jsonb_build_object(
                'hostname', hostname,
                'workerId', worker_id,
                'pid', pid,
                'avgEventLoopLagMs', avg_evl,
                'maxEventLoopLagMs', max_evl,
                'avgFreeSlots', avg_free,
                'minFreeSlots', min_free,
                'dbConnections', db_conns,
                'avgJobQueueSize', avg_jq,
                'maxJobQueueSize', max_jq,
                'dbErrors', db_errors,
                'lastSeen', to_char(last_seen, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
            ) ORDER BY hostname, worker_id
        ), '[]'::jsonb)
        FROM (
            SELECT 
                hostname,
                worker_id,
                pid,
                AVG(avg_event_loop_lag_ms)::integer as avg_evl,
                MAX(max_event_loop_lag_ms) as max_evl,
                AVG(avg_free_slots)::integer as avg_free,
                MIN(min_free_slots) as min_free,
                MAX(db_connections) as db_conns,
                NULLIF(AVG(avg_job_queue_size)::integer, 0) as avg_jq,
                NULLIF(MAX(max_job_queue_size), 0) as max_jq,
                SUM(db_error_count) as db_errors,
                MAX(bucket_time) as last_seen
            FROM queen.worker_metrics
            WHERE bucket_time >= p_from AND bucket_time <= p_to
            GROUP BY hostname, worker_id, pid
        ) t
    );
END;
$$;

-- System-wide summary (running totals, O(1) lookup)
-- Updated by trigger on worker_metrics inserts
CREATE TABLE IF NOT EXISTS queen.worker_metrics_summary (
    id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),  -- Singleton row
    
    -- Running totals (all time)
    total_push_requests BIGINT DEFAULT 0,
    total_pop_requests BIGINT DEFAULT 0,
    total_ack_requests BIGINT DEFAULT 0,
    total_transactions BIGINT DEFAULT 0,
    total_push_messages BIGINT DEFAULT 0,
    total_pop_messages BIGINT DEFAULT 0,
    total_ack_messages BIGINT DEFAULT 0,
    total_ack_success BIGINT DEFAULT 0,
    total_ack_failed BIGINT DEFAULT 0,
    total_db_errors BIGINT DEFAULT 0,
    total_dlq BIGINT DEFAULT 0,
    
    -- Last update
    last_updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Initialize singleton row if not exists
INSERT INTO queen.worker_metrics_summary (id) VALUES (1) ON CONFLICT DO NOTHING;

-- Trigger function to update summary on new metrics
CREATE OR REPLACE FUNCTION queen.update_worker_metrics_summary()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE queen.worker_metrics_summary SET
        total_push_requests = total_push_requests + NEW.push_request_count,
        total_pop_requests = total_pop_requests + NEW.pop_request_count,
        total_ack_requests = total_ack_requests + NEW.ack_request_count,
        total_transactions = total_transactions + NEW.transaction_count,
        total_push_messages = total_push_messages + NEW.push_message_count,
        total_pop_messages = total_pop_messages + NEW.pop_message_count,
        total_ack_messages = total_ack_messages + NEW.ack_message_count,
        total_ack_success = total_ack_success + NEW.ack_success_count,
        total_ack_failed = total_ack_failed + NEW.ack_failed_count,
        total_db_errors = total_db_errors + NEW.db_error_count,
        total_dlq = total_dlq + NEW.dlq_count,
        last_updated_at = NOW()
    WHERE id = 1;
    
    RETURN NEW;
END;
$$;

-- Create trigger (only on INSERT, not UPDATE to avoid double counting)
DROP TRIGGER IF EXISTS trg_update_worker_metrics_summary ON queen.worker_metrics;
CREATE TRIGGER trg_update_worker_metrics_summary
    AFTER INSERT ON queen.worker_metrics
    FOR EACH ROW
    EXECUTE FUNCTION queen.update_worker_metrics_summary();

-- O(1) function to get system totals
CREATE OR REPLACE FUNCTION queen.get_system_totals_v1()
RETURNS JSONB
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN (
        SELECT jsonb_build_object(
            'pushRequests', total_push_requests,
            'popRequests', total_pop_requests,
            'ackRequests', total_ack_requests,
            'transactions', total_transactions,
            'pushMessages', total_push_messages,
            'popMessages', total_pop_messages,
            'ackMessages', total_ack_messages,
            'ackSuccess', total_ack_success,
            'ackFailed', total_ack_failed,
            'dbErrors', total_db_errors,
            'dlqCount', total_dlq,
            'pendingMessages', total_push_messages - total_pop_messages,
            'lastUpdatedAt', to_char(last_updated_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
        )
        FROM queen.worker_metrics_summary
        WHERE id = 1
    );
END;
$$;

-- Cleanup old worker metrics (call periodically)
CREATE OR REPLACE FUNCTION queen.cleanup_worker_metrics_v1(
    p_retention_days INTEGER DEFAULT 7
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted_metrics INTEGER := 0;
    v_deleted_lag INTEGER := 0;
    v_deleted_parked_replica INTEGER := 0;
    v_deleted_retention INTEGER := 0;
BEGIN
    DELETE FROM queen.worker_metrics
    WHERE bucket_time < NOW() - (p_retention_days || ' days')::INTERVAL;
    GET DIAGNOSTICS v_deleted_metrics = ROW_COUNT;

    DELETE FROM queen.queue_lag_metrics
    WHERE bucket_time < NOW() - (p_retention_days || ' days')::INTERVAL;
    GET DIAGNOSTICS v_deleted_lag = ROW_COUNT;

    DELETE FROM queen.queue_parked_replica
    WHERE bucket_time < NOW() - (p_retention_days || ' days')::INTERVAL;
    GET DIAGNOSTICS v_deleted_parked_replica = ROW_COUNT;

    -- queen.retention_history is written per retention/eviction step
    -- (006_log_maintenance) and has no FK cascade to clean it up, so it ages
    -- out on the same window as the other metrics tables.
    DELETE FROM queen.retention_history
    WHERE executed_at < NOW() - (p_retention_days || ' days')::INTERVAL;
    GET DIAGNOSTICS v_deleted_retention = ROW_COUNT;

    RETURN jsonb_build_object(
        'deletedMetrics', v_deleted_metrics,
        'deletedLagMetrics', v_deleted_lag,
        'deletedParkedReplica', v_deleted_parked_replica,
        'deletedRetentionHistory', v_deleted_retention
    );
END;
$$;

-- ============================================================================
-- UPDATED APIs (same format as v2, but using worker_metrics for throughput/lag)
-- ============================================================================

-- queen.get_system_overview_v3: Same format as v2, but with real-time throughput from worker_metrics
-- Track B (§5): p_tenant scopes the overview. The DEFAULT-tenant path is the
-- pre-Track-B body VERBATIM (byte-identical when the flag is OFF — every request
-- is the default tenant), because it blends cluster-wide worker_metrics_summary
-- lifetime counters that are NOT per-tenant. A SPECIFIC tenant gets a view
-- re-derived purely from that tenant's queen.stats 'queue' rows (per-queue →
-- per-tenant after the 011_log_stats refresh fix) + tenant-scoped queue/
-- partition/namespace/task counts, so those global counters never leak across
-- tenants.
CREATE OR REPLACE FUNCTION queen.get_system_overview_v3(
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001')
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_stats queen.stats;
    v_summary queen.worker_metrics_summary;
    v_queue_count INTEGER;
    v_namespace_count INTEGER;
    v_task_count INTEGER;
    v_recent_throughput RECORD;
    v_recent_lag RECORD;
    v_default UUID := '00000000-0000-0000-0000-000000000001';
    v_agg RECORD;
    v_part_count INTEGER;
    v_push_5m BIGINT;
    v_ack_5m BIGINT;
BEGIN
  IF p_tenant = v_default THEN
    -- Get existing system stats (for pending/processing counts)
    SELECT * INTO v_stats
    FROM queen.stats
    WHERE stat_type = 'system' AND stat_key = 'global';
    
    -- Get worker metrics summary (for totals)
    SELECT * INTO v_summary FROM queen.worker_metrics_summary WHERE id = 1;
    
    -- Get counts
    SELECT COUNT(*) INTO v_queue_count FROM queen.queues;
    SELECT COUNT(*) INTO v_namespace_count FROM queen.stats WHERE stat_type = 'namespace';
    SELECT COUNT(*) INTO v_task_count FROM queen.stats WHERE stat_type = 'task';
    
    -- Get recent throughput (last 5 minutes) from worker_metrics
    SELECT 
        COALESCE(SUM(push_message_count), 0) as push_per_min,
        COALESCE(SUM(ack_message_count), 0) as ack_per_min
    INTO v_recent_throughput
    FROM queen.worker_metrics
    WHERE bucket_time >= NOW() - INTERVAL '5 minutes';
    
    -- Get recent lag from worker_metrics (last 5 minutes for current state).
    -- worker_metrics lag is only sampled AT POP, so with consumers stopped there
    -- is no sample at all. Both expressions must yield NULL (never 0) in that
    -- case, otherwise the COALESCE chain below can never reach the queen.stats
    -- fallback (oldest-pending age, 011_log_stats) and a stalled backlog reads
    -- as zero lag.
    SELECT
        CASE WHEN SUM(lag_count) > 0
             THEN (SUM(avg_lag_ms * lag_count) / SUM(lag_count) / 1000)::integer
             END as avg_lag_seconds,
        CASE WHEN SUM(lag_count) > 0
             THEN (MAX(max_lag_ms) / 1000)::integer
             END as max_lag_seconds
    INTO v_recent_lag
    FROM queen.worker_metrics
    WHERE bucket_time >= NOW() - INTERVAL '5 minutes';
    
    -- Return same format as get_system_overview_v2
    RETURN jsonb_build_object(
        'queues', v_queue_count,
        'partitions', COALESCE(v_stats.child_count, 0),
        'namespaces', v_namespace_count,
        'tasks', v_task_count,
        'messages', jsonb_build_object(
            'total', COALESCE(v_summary.total_push_messages, v_stats.total_messages, 0),
            -- Prefer queen.stats (refreshed ground-truth from messages table) over
            -- worker_metrics_summary running counters, which can drift negative in queue
            -- mode and are structurally negative in bus mode (pop fanout > push count).
            -- GREATEST(0, ...) guards against transient skew during stats refresh.
            'pending', GREATEST(0, COALESCE(
                v_stats.pending_messages - v_stats.processing_messages,
                v_summary.total_push_messages - v_summary.total_pop_messages,
                0
            )),
            'processing', COALESCE(v_stats.processing_messages, 0),
            'completed', COALESCE(v_summary.total_ack_success, v_stats.completed_messages, 0),
            'failed', COALESCE(v_summary.total_ack_failed, 0),
            'deadLetter', COALESCE(v_summary.total_dlq, v_stats.dead_letter_messages, 0)
        ),
        'lag', jsonb_build_object(
            'time', jsonb_build_object(
                'avg', COALESCE(v_recent_lag.avg_lag_seconds, v_stats.avg_lag_seconds, 0),
                'median', COALESCE(v_stats.median_lag_seconds, 0),
                'min', 0,
                'max', COALESCE(v_recent_lag.max_lag_seconds, v_stats.max_lag_seconds, 0)
            ),
            'offset', jsonb_build_object(
                'avg', COALESCE(v_stats.avg_offset_lag, 0),
                'median', 0,
                'min', 0,
                'max', COALESCE(v_stats.max_offset_lag, 0)
            )
        ),
        'throughput', jsonb_build_object(
            'ingestedPerSecond', ROUND(COALESCE(v_recent_throughput.push_per_min, 0)::numeric / 300, 2),
            'processedPerSecond', ROUND(COALESCE(v_recent_throughput.ack_per_min, 0)::numeric / 300, 2)
        ),
        'timestamp', to_char(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'statsAge', CASE
            WHEN v_summary.last_updated_at IS NOT NULL
            THEN EXTRACT(EPOCH FROM (NOW() - v_summary.last_updated_at))::integer
            ELSE -1
        END
    );
  END IF;

    -- ------------------------------------------------------- per-tenant view
    -- Counts scoped to the tenant's queues; the partition count is the live
    -- log-partition count for this tenant, counted off queen.log_partitions
    -- exactly like the 011_log_stats refresh does.
    -- (No storage predicate: the engine-selector column is gone, every queue
    -- is the log engine.)
    SELECT COUNT(*) INTO v_queue_count
    FROM queen.queues WHERE tenant_id = p_tenant;
    SELECT COUNT(DISTINCT namespace) INTO v_namespace_count
    FROM queen.queues WHERE tenant_id = p_tenant AND namespace IS NOT NULL AND namespace != '';
    SELECT COUNT(DISTINCT task) INTO v_task_count
    FROM queen.queues WHERE tenant_id = p_tenant AND task IS NOT NULL AND task != '';
    -- Partition -> queue is one hop: lp.queue_id IS the queen.queues id.
    SELECT COUNT(*) INTO v_part_count
    FROM queen.log_partitions lp
    JOIN queen.queues q ON q.id = lp.queue_id
    WHERE q.tenant_id = p_tenant;

    -- Throughput over the SAME 5-minute window the default-tenant branch uses,
    -- from the tenant-scoped per-queue counters. queen.stats.ingested_per_second
    -- cannot serve here: it is a delta of RETAINED frames, so it collapses to 0
    -- for a full stats cycle after every retention sweep and reads as "ingest
    -- stopped" — see the semantics note in 011_log_stats.
    SELECT COALESCE(SUM(qlm.push_message_count), 0),
           COALESCE(SUM(qlm.ack_success_count + qlm.ack_failed_count), 0)
    INTO v_push_5m, v_ack_5m
    FROM queen.queue_lag_metrics qlm
    JOIN queen.queues q ON q.id = qlm.queue_id
    WHERE q.tenant_id = p_tenant
      AND qlm.bucket_time >= NOW() - INTERVAL '5 minutes';

    -- Message / lag / throughput totals re-derived from the tenant's 'queue'
    -- stat rows (same axes aggregate_system_stats_v2 uses, scoped by tenant).
    SELECT
        COALESCE(SUM(s.total_messages), 0)              AS total_m,
        COALESCE(SUM(s.pending_messages), 0)            AS pending_m,
        COALESCE(SUM(s.processing_messages), 0)         AS processing_m,
        COALESCE(SUM(s.completed_messages), 0)          AS completed_m,
        COALESCE(SUM(s.dead_letter_messages), 0)        AS dlq_m,
        COALESCE(AVG(s.avg_lag_seconds)::integer, 0)    AS avg_lag,
        COALESCE(MAX(s.max_lag_seconds), 0)             AS max_lag,
        COALESCE(MAX(s.median_lag_seconds), 0)          AS median_lag,
        COALESCE(MAX(s.avg_offset_lag), 0)              AS avg_off,
        COALESCE(MAX(s.max_offset_lag), 0)              AS max_off,
        COALESCE(SUM(s.ingested_per_second), 0)         AS ips,
        COALESCE(SUM(s.processed_per_second), 0)        AS pps,
        MAX(s.last_computed_at)                         AS last_at
    INTO v_agg
    FROM queen.stats s
    JOIN queen.queues q ON q.id = s.queue_id
    WHERE s.stat_type = 'queue' AND q.tenant_id = p_tenant;

    RETURN jsonb_build_object(
        'queues', COALESCE(v_queue_count, 0),
        'partitions', COALESCE(v_part_count, 0),
        'namespaces', COALESCE(v_namespace_count, 0),
        'tasks', COALESCE(v_task_count, 0),
        'messages', jsonb_build_object(
            'total', v_agg.total_m,
            'pending', GREATEST(0, v_agg.pending_m - v_agg.processing_m),
            'processing', v_agg.processing_m,
            'completed', v_agg.completed_m,
            'failed', 0,
            'deadLetter', v_agg.dlq_m
        ),
        'lag', jsonb_build_object(
            'time', jsonb_build_object(
                'avg', v_agg.avg_lag, 'median', v_agg.median_lag, 'min', 0, 'max', v_agg.max_lag
            ),
            'offset', jsonb_build_object(
                'avg', v_agg.avg_off, 'median', 0, 'min', 0, 'max', v_agg.max_off
            )
        ),
        'throughput', jsonb_build_object(
            'ingestedPerSecond', ROUND(v_push_5m::numeric / 300, 2),
            'processedPerSecond', ROUND(v_ack_5m::numeric / 300, 2)
        ),
        'timestamp', to_char(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'statsAge', CASE
            WHEN v_agg.last_at IS NOT NULL
            THEN EXTRACT(EPOCH FROM (NOW() - v_agg.last_at))::integer
            ELSE -1
        END
    );
END;
$$;

GRANT EXECUTE ON FUNCTION queen.get_system_overview_v3(UUID) TO PUBLIC;

-- queen.get_status_v3: Same format as v2, but throughput from worker_metrics
-- Enhanced with worker health and error tracking
CREATE OR REPLACE FUNCTION queen.get_status_v3(p_filters JSONB DEFAULT '{}'::jsonb)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_from_ts TIMESTAMPTZ;
    v_to_ts TIMESTAMPTZ;
    v_queue TEXT;
    v_namespace TEXT;
    v_task TEXT;
    v_duration_minutes INTEGER;
    v_bucket_minutes INTEGER;
    v_throughput JSONB;
    v_queues JSONB;
    v_messages JSONB;
    v_leases JSONB;
    v_dlq JSONB;
    v_workers JSONB;
    v_errors JSONB;
    v_point_count INTEGER;
    v_summary queen.worker_metrics_summary;
    v_stats_pending BIGINT;
    v_stats_processing BIGINT;
    v_dlq_parts BIGINT;
    v_dlq_errors JSONB;
BEGIN
    -- Parse filters
    v_from_ts := COALESCE((p_filters->>'from')::timestamptz, NOW() - INTERVAL '1 hour');
    v_to_ts := COALESCE((p_filters->>'to')::timestamptz, NOW());
    v_queue := p_filters->>'queue';
    v_namespace := p_filters->>'namespace';
    v_task := p_filters->>'task';
    
    -- Calculate bucket size
    v_duration_minutes := EXTRACT(EPOCH FROM (v_to_ts - v_from_ts)) / 60;
    v_bucket_minutes := CASE
        WHEN v_duration_minutes <= 60 THEN 1
        WHEN v_duration_minutes <= 360 THEN 5
        WHEN v_duration_minutes <= 1440 THEN 15
        WHEN v_duration_minutes <= 10080 THEN 60
        ELSE 360
    END;
    
    -- Get throughput from worker_metrics (system-wide or per-queue).
    -- When queue / namespace / task filter is set we aggregate queue_lag_metrics
    -- (now carries per-queue push/pop/ack counts thanks to PR 3c). System-health
    -- signals that aren't scoped to a queue (event-loop lag, DB errors, CPU,
    -- DB pool) still come from worker_metrics / system_metrics so the Dashboard
    -- can show "this queue's throughput vs the system's health" side by side.
    IF v_queue IS NOT NULL OR v_namespace IS NOT NULL OR v_task IS NOT NULL THEN
        WITH qlm AS (
            SELECT
                date_trunc('minute', qlm.bucket_time) -
                    (EXTRACT(minute FROM qlm.bucket_time)::integer % v_bucket_minutes) * INTERVAL '1 minute' AS bucket,
                SUM(qlm.pop_count)           AS pop_msg,
                SUM(qlm.push_message_count)  AS push_msg,
                SUM(qlm.ack_success_count + qlm.ack_failed_count) AS ack_msg,
                SUM(qlm.ack_failed_count)    AS ack_fail,
                CASE WHEN SUM(qlm.pop_count) > 0
                     THEN SUM(qlm.avg_lag_ms * qlm.pop_count) / SUM(qlm.pop_count)
                     ELSE 0 END AS avg_lag_ms,
                MAX(qlm.max_lag_ms)          AS max_lag_ms
            FROM queen.queue_lag_metrics qlm
            -- Queue identity is the queen.queues id: one id-join supplies the
            -- name/namespace/task filter columns, and each bucket is counted
            -- exactly once by construction. This function stays deliberately
            -- operator/global (it aggregates across tenants).
            JOIN queen.queues q ON q.id = qlm.queue_id
            WHERE qlm.bucket_time >= v_from_ts
              AND qlm.bucket_time <= v_to_ts
              AND (v_queue IS NULL OR q.name = v_queue)
              AND (v_namespace IS NULL OR q.namespace = v_namespace)
              AND (v_task IS NULL OR q.task = v_task)
            GROUP BY 1
        ),
        -- System-wide signals (not scoped to queue). These still matter when
        -- drilling into a queue because they tell the operator if the apparent
        -- slowdown is queue-specific or a global resource issue.
        wm AS (
            SELECT
                date_trunc('minute', bucket_time) -
                    (EXTRACT(minute FROM bucket_time)::integer % v_bucket_minutes) * INTERVAL '1 minute' AS bucket,
                ROUND(AVG(avg_event_loop_lag_ms)) AS avg_el_lag,
                MAX(max_event_loop_lag_ms)        AS max_el_lag,
                MIN(min_free_slots)               AS min_slots,
                SUM(db_error_count)               AS db_errs,
                SUM(dlq_count)                    AS dlq_cnt
            FROM queen.worker_metrics
            WHERE bucket_time >= v_from_ts AND bucket_time <= v_to_ts
            GROUP BY 1
        ),
        sm AS (
            SELECT
                date_trunc('minute', timestamp) -
                    (EXTRACT(minute FROM timestamp)::integer % v_bucket_minutes) * INTERVAL '1 minute' AS bucket,
                AVG(((metrics->'cpu'->'user_us'->>'avg')::numeric) / 100.0)          AS cpu_user_pct,
                AVG(((metrics->'cpu'->'system_us'->>'avg')::numeric) / 100.0)        AS cpu_sys_pct,
                AVG(((metrics->'memory'->'rss_bytes'->>'avg')::numeric) / 1048576.0) AS rss_mb,
                AVG((metrics->'database'->'pool_active'->>'avg')::numeric)           AS db_pool_active,
                AVG((metrics->'database'->'pool_idle'->>'avg')::numeric)             AS db_pool_idle,
                AVG((metrics->'database'->'pool_size'->>'avg')::numeric)             AS db_pool_size
            FROM queen.system_metrics
            WHERE timestamp >= v_from_ts AND timestamp <= v_to_ts
            GROUP BY 1
        )
        SELECT
            COALESCE(jsonb_agg(
                jsonb_build_object(
                    'timestamp', to_char(qlm.bucket, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
                    'ingested', COALESCE(qlm.push_msg, 0),
                    'processed', COALESCE(qlm.ack_msg, qlm.pop_msg, 0),
                    'popMessages', COALESCE(qlm.pop_msg, 0),
                    'ingestedPerSecond', ROUND(COALESCE(qlm.push_msg, 0)::numeric / (v_bucket_minutes * 60), 2),
                    'processedPerSecond', ROUND(COALESCE(qlm.ack_msg, 0)::numeric / (v_bucket_minutes * 60), 2),
                    'popPerSecond', ROUND(COALESCE(qlm.pop_msg, 0)::numeric / (v_bucket_minutes * 60), 2),
                    'avgLagMs', qlm.avg_lag_ms,
                    'maxLagMs', qlm.max_lag_ms,
                    'avgEventLoopLagMs', wm.avg_el_lag,
                    'maxEventLoopLagMs', wm.max_el_lag,
                    'minFreeSlots', wm.min_slots,
                    'dbErrors', wm.db_errs,
                    'ackFailed', qlm.ack_fail,
                    'dlqCount', wm.dlq_cnt,
                    'queenCpuUserPct', ROUND(sm.cpu_user_pct, 2),
                    'queenCpuSysPct', ROUND(sm.cpu_sys_pct, 2),
                    'queenRssMb', ROUND(sm.rss_mb, 1),
                    'dbPoolActive', ROUND(sm.db_pool_active, 1),
                    'dbPoolIdle', ROUND(sm.db_pool_idle, 1),
                    'dbPoolSize', ROUND(sm.db_pool_size, 1)
                ) ORDER BY qlm.bucket DESC
            ), '[]'::jsonb),
            COUNT(*)
        INTO v_throughput, v_point_count
        FROM qlm
        LEFT JOIN wm ON wm.bucket = qlm.bucket
        LEFT JOIN sm ON sm.bucket = qlm.bucket;
    ELSE
        -- System-wide throughput from worker_metrics (aggregated with worker health).
        -- LEFT JOIN queen.system_metrics (bucket-aligned) to attach per-bucket
        -- CPU %, RSS MB and DB pool numbers. Values from system_metrics.metrics
        -- are stored as: cpu.user_us.avg / cpu.system_us.avg (0..10000 = %*100),
        -- memory.rss_bytes.avg (bytes), database.pool_active.avg / pool_idle.avg.
        -- We aggregate across replicas/workers with AVG for CPU / memory / pool.
        WITH wm AS (
            SELECT
                date_trunc('minute', bucket_time) -
                    (EXTRACT(minute FROM bucket_time)::integer % v_bucket_minutes) * INTERVAL '1 minute' AS bucket,
                SUM(push_message_count) as push_msg,
                SUM(pop_message_count)  as pop_msg,
                SUM(ack_message_count)  as ack_msg,
                CASE WHEN SUM(lag_count) > 0
                     THEN SUM(avg_lag_ms * lag_count) / SUM(lag_count)
                     ELSE 0 END as avg_lag,
                MAX(max_lag_ms) as max_lag,
                ROUND(AVG(avg_event_loop_lag_ms)) as avg_el_lag,
                MAX(max_event_loop_lag_ms) as max_el_lag,
                MIN(min_free_slots) as min_slots,
                SUM(db_error_count) as db_errs,
                SUM(dlq_count) as dlq_cnt,
                SUM(ack_failed_count) as ack_fail
            FROM queen.worker_metrics
            WHERE bucket_time >= v_from_ts AND bucket_time <= v_to_ts
            GROUP BY 1
        ),
        sm AS (
            SELECT
                date_trunc('minute', timestamp) -
                    (EXTRACT(minute FROM timestamp)::integer % v_bucket_minutes) * INTERVAL '1 minute' AS bucket,
                -- CPU percentages (user + system) averaged across replicas/workers.
                -- metrics.cpu.*_us.avg is percent * 100 (0..10000+), divide by 100.
                AVG(((metrics->'cpu'->'user_us'->>'avg')::numeric) / 100.0) as cpu_user_pct,
                AVG(((metrics->'cpu'->'system_us'->>'avg')::numeric) / 100.0) as cpu_sys_pct,
                AVG(((metrics->'memory'->'rss_bytes'->>'avg')::numeric) / 1024.0 / 1024.0) as rss_mb,
                AVG((metrics->'database'->'pool_active'->>'avg')::numeric) as db_pool_active,
                AVG((metrics->'database'->'pool_idle'->>'avg')::numeric) as db_pool_idle,
                AVG((metrics->'database'->'pool_size'->>'avg')::numeric) as db_pool_size
            FROM queen.system_metrics
            WHERE timestamp >= v_from_ts AND timestamp <= v_to_ts
            GROUP BY 1
        )
        SELECT
            COALESCE(jsonb_agg(
                jsonb_build_object(
                    'timestamp', to_char(wm.bucket, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
                    'ingested', wm.push_msg,
                    'processed', wm.ack_msg,
                    'popMessages', wm.pop_msg,
                    'ingestedPerSecond', ROUND(wm.push_msg::numeric / (v_bucket_minutes * 60), 2),
                    'processedPerSecond', ROUND(wm.ack_msg::numeric / (v_bucket_minutes * 60), 2),
                    'popPerSecond', ROUND(wm.pop_msg::numeric / (v_bucket_minutes * 60), 2),
                    'avgLagMs', wm.avg_lag,
                    'maxLagMs', wm.max_lag,
                    'avgEventLoopLagMs', wm.avg_el_lag,
                    'maxEventLoopLagMs', wm.max_el_lag,
                    'minFreeSlots', wm.min_slots,
                    'dbErrors', wm.db_errs,
                    'ackFailed', wm.ack_fail,
                    'dlqCount', wm.dlq_cnt,
                    -- System metrics per bucket (null when no sample yet)
                    'queenCpuUserPct', ROUND(sm.cpu_user_pct, 2),
                    'queenCpuSysPct', ROUND(sm.cpu_sys_pct, 2),
                    'queenRssMb', ROUND(sm.rss_mb, 1),
                    'dbPoolActive', ROUND(sm.db_pool_active, 1),
                    'dbPoolIdle', ROUND(sm.db_pool_idle, 1),
                    'dbPoolSize', ROUND(sm.db_pool_size, 1)
                ) ORDER BY wm.bucket DESC
            ), '[]'::jsonb),
            COUNT(*)
        INTO v_throughput, v_point_count
        FROM wm
        LEFT JOIN sm ON sm.bucket = wm.bucket;
    END IF;
    
    -- NEW: Get current worker health (last 2 minutes)
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'hostname', hostname,
            'workerId', worker_id,
            'avgEventLoopLagMs', avg_el,
            'maxEventLoopLagMs', max_el,
            'freeSlots', free_slots,
            'dbConnections', db_conns,
            'jobQueueSize', job_queue,
            'messagesProcessed', msgs
        ) ORDER BY hostname, worker_id
    ), '[]'::jsonb) INTO v_workers
    FROM (
        SELECT 
            hostname, worker_id,
            ROUND(AVG(avg_event_loop_lag_ms)) as avg_el,
            MAX(max_event_loop_lag_ms) as max_el,
            MIN(min_free_slots) as free_slots,
            MAX(db_connections) as db_conns,
            -- This broker has no job queue: nothing writes the column, so it
            -- must read as NULL ("not measured"), never as a confident 0.
            NULLIF(MAX(max_job_queue_size), 0) as job_queue,
            SUM(push_message_count + ack_message_count) as msgs
        FROM queen.worker_metrics
        WHERE bucket_time >= NOW() - INTERVAL '2 minutes'
        GROUP BY hostname, worker_id
    ) t;
    
    -- Get active queues from stats (unchanged)
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'id', q.id,
            'name', q.name,
            'namespace', q.namespace,
            'task', q.task,
            'partitions', COALESCE(s.child_count, 0),
            'totalConsumed', COALESCE(s.completed_messages, 0)
        )
    ), '[]'::jsonb) INTO v_queues
    FROM queen.queues q
    LEFT JOIN queen.stats s ON s.stat_type = 'queue' AND s.queue_id = q.id
    WHERE (v_queue IS NULL OR q.name = v_queue)
      AND (v_namespace IS NULL OR q.namespace = v_namespace)
      AND (v_task IS NULL OR q.task = v_task)
      AND COALESCE(s.total_messages, 0) > 0;
    
    -- Get message counts from summary
    SELECT * INTO v_summary FROM queen.worker_metrics_summary WHERE id = 1;

    -- Live watermarks from queen.stats (the same source get_status_queues_v2 and
    -- the overview use). NOT from the summary's lifetime push/pop counters:
    -- pop_message_count can exceed push_message_count whenever a queue has more
    -- than one consumer group, so (push - pop) goes negative and clamps to 0 —
    -- reporting "no backlog" on a queue that has one.
    SELECT COALESCE(SUM(s.pending_messages), 0),
           COALESCE(SUM(s.processing_messages), 0)
    INTO v_stats_pending, v_stats_processing
    FROM queen.stats s
    WHERE s.stat_type = 'queue';

    -- Enhanced messages with request counts
    v_messages := jsonb_build_object(
        'total', COALESCE(v_summary.total_push_messages, 0),
        'pending', GREATEST(0, v_stats_pending - v_stats_processing),
        'processing', v_stats_processing,
        'completed', COALESCE(v_summary.total_ack_success, 0),
        'failed', COALESCE(v_summary.total_ack_failed, 0),
        'deadLetter', COALESCE(v_summary.total_dlq, 0),
        -- NEW: Request vs message breakdown
        'requests', jsonb_build_object(
            'push', COALESCE(v_summary.total_push_requests, 0),
            'pop', COALESCE(v_summary.total_pop_requests, 0),
            'ack', COALESCE(v_summary.total_ack_requests, 0)
        ),
        'batchEfficiency', jsonb_build_object(
            'push', CASE WHEN v_summary.total_push_requests > 0 
                        THEN ROUND(v_summary.total_push_messages::numeric / v_summary.total_push_requests, 2)
                        ELSE 0 END,
            'pop', CASE WHEN v_summary.total_pop_requests > 0 
                       THEN ROUND(v_summary.total_pop_messages::numeric / v_summary.total_pop_requests, 2)
                       ELSE 0 END,
            'ack', CASE WHEN v_summary.total_ack_requests > 0 
                       THEN ROUND(v_summary.total_ack_messages::numeric / v_summary.total_ack_requests, 2)
                       ELSE 0 END
        )
    );
    
    -- NEW: Error summary
    v_errors := jsonb_build_object(
        'dbErrors', COALESCE(v_summary.total_db_errors, 0),
        'ackFailed', COALESCE(v_summary.total_ack_failed, 0),
        'dlqMessages', COALESCE(v_summary.total_dlq, 0)
    );
    
    -- Active leases: queen.log_consumers rows with a future lease_expires_at.
    -- The second leg of this expression used to sum in the ROWS-engine lease
    -- table; it went with that table, and it contributed a structural 0 to every
    -- field here anyway. The leased span is (committed, batch_end] and
    -- `committed` is the last acked offset, so there is no per-lease acked
    -- counter — 'totalAcked' keeps emitting 0, the same value the summed
    -- expression produced.
    SELECT jsonb_build_object(
        'active', COALESCE(l.active, 0),
        'partitionsWithLeases', COALESCE(l.parts, 0),
        'totalBatchSize', COALESCE(l.batch, 0),
        'totalAcked', COALESCE(l.acked, 0)
    ) INTO v_leases
    FROM (
        SELECT COUNT(*) AS active, COUNT(DISTINCT partition_id) AS parts,
               COALESCE(SUM(GREATEST(COALESCE(batch_end, committed) - committed, 0)), 0) AS batch,
               0::bigint AS acked
        FROM queen.log_consumers
        WHERE lease_expires_at IS NOT NULL AND lease_expires_at > NOW()
    ) l;

    -- DLQ stats. totalMessages stays the lifetime counter (that is what the
    -- summary holds); affectedPartitions and topErrors are computed from the
    -- LIVE dead-letter contents instead of the 0 / [] literals that made two
    -- panels permanently dead on every deployment.
    SELECT COUNT(DISTINCT d.partition_id),
           COALESCE(
               (SELECT jsonb_agg(t.e)
                FROM (SELECT jsonb_build_object(
                                 'error', COALESCE(d2.error, 'unknown'),
                                 'count', COUNT(*)) AS e
                      FROM queen.log_dlq d2
                      GROUP BY COALESCE(d2.error, 'unknown')
                      ORDER BY COUNT(*) DESC
                      LIMIT 5) t),
               '[]'::jsonb)
    INTO v_dlq_parts, v_dlq_errors
    FROM queen.log_dlq d;

    v_dlq := jsonb_build_object(
        'totalMessages', COALESCE(v_summary.total_dlq, 0),
        'currentMessages', (SELECT COUNT(*) FROM queen.log_dlq),
        'affectedPartitions', COALESCE(v_dlq_parts, 0),
        'topErrors', COALESCE(v_dlq_errors, '[]'::jsonb)
    );

    RETURN jsonb_build_object(
        'timeRange', jsonb_build_object(
            'from', to_char(v_from_ts, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'to', to_char(v_to_ts, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
        ),
        'bucketMinutes', v_bucket_minutes,
        'pointCount', COALESCE(v_point_count, 0),
        'throughput', COALESCE(v_throughput, '[]'::jsonb),
        'queues', v_queues,
        'messages', v_messages,
        'leases', v_leases,
        'deadLetterQueue', v_dlq,
        -- NEW: Worker health and errors
        'workers', COALESCE(v_workers, '[]'::jsonb),
        'errors', v_errors,
        'statsAge', CASE 
            WHEN v_summary.last_updated_at IS NOT NULL 
            THEN EXTRACT(EPOCH FROM (NOW() - v_summary.last_updated_at))::integer 
            ELSE -1 
        END
    );
END;
$$;

-- ============================================================================
-- WORKER METRICS TIME SERIES API (for System Metrics view)
-- ============================================================================

-- queen.get_worker_metrics_timeseries_v1: Returns time series data for charts
CREATE OR REPLACE FUNCTION queen.get_worker_metrics_timeseries_v1(p_filters JSONB DEFAULT '{}'::jsonb)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_from_ts TIMESTAMPTZ;
    v_to_ts TIMESTAMPTZ;
    v_hostname TEXT;
    v_worker_id INTEGER;
    v_duration_minutes INTEGER;
    v_bucket_minutes INTEGER;
    v_timeseries JSONB;
    v_workers JSONB;
    v_queues JSONB;
    v_summary JSONB;
    v_point_count INTEGER;
BEGIN
    -- Parse filters
    v_from_ts := COALESCE((p_filters->>'from')::timestamptz, NOW() - INTERVAL '1 hour');
    v_to_ts := COALESCE((p_filters->>'to')::timestamptz, NOW());
    v_hostname := p_filters->>'hostname';
    v_worker_id := (p_filters->>'workerId')::integer;
    
    -- Calculate bucket size based on duration
    v_duration_minutes := EXTRACT(EPOCH FROM (v_to_ts - v_from_ts)) / 60;
    v_bucket_minutes := CASE
        WHEN v_duration_minutes <= 60 THEN 1
        WHEN v_duration_minutes <= 360 THEN 5
        WHEN v_duration_minutes <= 1440 THEN 15
        WHEN v_duration_minutes <= 10080 THEN 60
        ELSE 360
    END;
    
    -- Get aggregated time series data (with proper bucketing based on time range)
    SELECT 
        COALESCE(jsonb_agg(
            jsonb_build_object(
                'timestamp', to_char(bucket, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
                -- Throughput
                'pushMessages', push_msg,
                'popMessages', pop_msg,
                'ackMessages', ack_msg,
                'pushRequests', push_req,
                'popRequests', pop_req,
                'ackRequests', ack_req,
                'jobsDone', jobs,
                -- Throughput per second
                'pushPerSecond', ROUND(push_msg::numeric / (v_bucket_minutes * 60), 2),
                'popPerSecond', ROUND(pop_msg::numeric / (v_bucket_minutes * 60), 2),
                'ackPerSecond', ROUND(ack_msg::numeric / (v_bucket_minutes * 60), 2),
                -- Worker health
                'avgEventLoopLagMs', avg_el,
                'maxEventLoopLagMs', max_el,
                'avgFreeSlots', avg_slots,
                'minFreeSlots', min_slots,
                'dbConnections', db_conns,
                'avgJobQueueSize', avg_queue,
                'maxJobQueueSize', max_queue,
                'backoffSize', backoff,
                -- Lag
                'avgLagMs', avg_lag,
                'maxLagMs', max_lag,
                'lagCount', lag_cnt,
                -- Errors
                'dbErrors', db_errs,
                'ackSuccess', ack_ok,
                'ackFailed', ack_fail,
                'dlqCount', dlq
            ) ORDER BY bucket DESC
        ), '[]'::jsonb),
        COUNT(*)
    INTO v_timeseries, v_point_count
    FROM (
        SELECT 
            date_trunc('minute', bucket_time) - 
                (EXTRACT(minute FROM bucket_time)::integer % v_bucket_minutes) * INTERVAL '1 minute' AS bucket,
            -- Throughput sums
            SUM(push_message_count) as push_msg,
            SUM(pop_message_count) as pop_msg,
            SUM(ack_message_count) as ack_msg,
            SUM(push_request_count) as push_req,
            SUM(pop_request_count) as pop_req,
            SUM(ack_request_count) as ack_req,
            SUM(jobs_done) as jobs,
            -- Worker health averages
            ROUND(AVG(avg_event_loop_lag_ms)) as avg_el,
            MAX(max_event_loop_lag_ms) as max_el,
            ROUND(AVG(avg_free_slots)) as avg_slots,
            MIN(min_free_slots) as min_slots,
            MAX(db_connections) as db_conns,
            -- No job queue / backoff ring in this broker: NULL, not 0.
            NULLIF(ROUND(AVG(avg_job_queue_size)), 0) as avg_queue,
            NULLIF(MAX(max_job_queue_size), 0) as max_queue,
            NULLIF(MAX(backoff_size), 0) as backoff,
            -- Lag (weighted average)
            CASE WHEN SUM(lag_count) > 0 
                 THEN ROUND(SUM(avg_lag_ms::numeric * lag_count) / SUM(lag_count))
                 ELSE 0 END as avg_lag,
            MAX(max_lag_ms) as max_lag,
            SUM(lag_count) as lag_cnt,
            -- Errors
            SUM(db_error_count) as db_errs,
            SUM(ack_success_count) as ack_ok,
            SUM(ack_failed_count) as ack_fail,
            SUM(dlq_count) as dlq
        FROM queen.worker_metrics
        WHERE bucket_time >= v_from_ts 
          AND bucket_time <= v_to_ts
          AND (v_hostname IS NULL OR hostname = v_hostname)
          AND (v_worker_id IS NULL OR worker_id = v_worker_id)
        GROUP BY 1
    ) t;
    
    -- Get per-worker current status
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'hostname', hostname,
            'workerId', worker_id,
            'avgEventLoopLagMs', avg_el,
            'maxEventLoopLagMs', max_el,
            'freeSlots', free_slots,
            'dbConnections', db_conns,
            'jobQueueSize', job_queue,
            'backoffSize', backoff,
            'messagesProcessed', msgs,
            'lastSeen', last_seen
        ) ORDER BY hostname, worker_id
    ), '[]'::jsonb) INTO v_workers
    FROM (
        SELECT 
            hostname, worker_id,
            ROUND(AVG(avg_event_loop_lag_ms)) as avg_el,
            MAX(max_event_loop_lag_ms) as max_el,
            MIN(min_free_slots) as free_slots,
            MAX(db_connections) as db_conns,
            -- No job queue / backoff ring in this broker: NULL, not 0.
            NULLIF(MAX(max_job_queue_size), 0) as job_queue,
            NULLIF(MAX(backoff_size), 0) as backoff,
            SUM(push_message_count + ack_message_count) as msgs,
            to_char(MAX(bucket_time), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as last_seen
        FROM queen.worker_metrics
        WHERE bucket_time >= NOW() - INTERVAL '5 minutes'
        GROUP BY hostname, worker_id
    ) t;
    
    -- Get per-queue lag metrics
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'queueName', queue_name,
            'popCount', pop_cnt,
            'avgLagMs', avg_lag,
            'maxLagMs', max_lag
        ) ORDER BY pop_cnt DESC
    ), '[]'::jsonb) INTO v_queues
    FROM (
        -- Operator/global view: queue identity is the queen.queues id, joined
        -- for the display name. Grouping by name keeps the historical
        -- one-series-per-name shape (same-named queues fold together).
        SELECT
            q.name AS queue_name,
            SUM(qlm.pop_count) as pop_cnt,
            CASE WHEN SUM(qlm.pop_count) > 0
                 THEN ROUND(SUM(qlm.avg_lag_ms::numeric * qlm.pop_count) / SUM(qlm.pop_count))
                 ELSE 0 END as avg_lag,
            MAX(qlm.max_lag_ms) as max_lag
        FROM queen.queue_lag_metrics qlm
        JOIN queen.queues q ON q.id = qlm.queue_id
        WHERE qlm.bucket_time >= v_from_ts AND qlm.bucket_time <= v_to_ts
        GROUP BY q.name
    ) t;
    
    -- Get summary totals
    SELECT jsonb_build_object(
        'totalPushMessages', total_push_messages,
        'totalPopMessages', total_pop_messages,
        'totalAckMessages', total_ack_messages,
        'totalPushRequests', total_push_requests,
        'totalPopRequests', total_pop_requests,
        'totalAckRequests', total_ack_requests,
        'totalDbErrors', total_db_errors,
        'totalAckFailed', total_ack_failed,
        'totalDlq', total_dlq,
        'pendingMessages', total_push_messages - total_pop_messages
    ) INTO v_summary
    FROM queen.worker_metrics_summary
    WHERE id = 1;
    
    RETURN jsonb_build_object(
        'timeRange', jsonb_build_object(
            'from', to_char(v_from_ts, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'to', to_char(v_to_ts, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
        ),
        'bucketMinutes', v_bucket_minutes,
        'pointCount', COALESCE(v_point_count, 0),
        'timeSeries', COALESCE(v_timeseries, '[]'::jsonb),
        'workers', COALESCE(v_workers, '[]'::jsonb),
        'queues', COALESCE(v_queues, '[]'::jsonb),
        'summary', COALESCE(v_summary, '{}'::jsonb)
    );
END;
$$;

-- ============================================================================
-- Partition lifecycle triggers (PR 3b)
-- ============================================================================
-- Bump per-queue partitions_created / partitions_deleted counters on
-- queen.queue_lag_metrics at the current minute bucket. Both triggers are
-- AFTER ... FOR EACH STATEMENT with a transition table, so they run exactly
-- once per INSERT / DELETE statement regardless of how many rows were
-- affected. They aggregate by queue before upserting, so one UPSERT is
-- issued per distinct queue touched per statement.
--
-- 2026-07-30: re-attached from the rows engine's partition table (dropped with
-- the rest of that message plane) to queen.log_partitions, the table the live
-- engine actually creates and deletes partitions in. On the old table these
-- counters had stopped measuring partitions at all: the only writer left was
-- /configure's phantom row, so partitions_created counted /configure CALLS —
-- 1 where the truth was 2 real partitions. Real writers now: push provisioning
-- (003_log_push) on the INSERT side, queen.log_partition_cleanup_step_v1
-- (006_log_maintenance) plus queue-delete CASCADE on the DELETE side.
--
-- Hot-path analysis:
--   - INSERT side: 003_log_push probes for missing partitions first and only runs the
--     provisioning INSERT when v_missing > 0, so in steady state the statement
--     never executes and the trigger never fires. When it does fire after an
--     ON CONFLICT DO NOTHING that inserted nothing, the transition table is
--     empty, the body's INSERT..SELECT yields no rows and performs zero writes.
--   - DELETE side: runs from the retention service, off the hot path.
-- ============================================================================

CREATE OR REPLACE FUNCTION queen.bump_partitions_created_trigger()
RETURNS TRIGGER AS $$
BEGIN
    -- queue_lag_metrics keys on the queen.queues id, and
    -- log_partitions.queue_id IS that id (queue-identity merge): no join at
    -- all, the transition table already carries the metric key.
    -- ORDER BY is LOAD-BEARING, not cosmetic. This fires from inside
    -- 003_log_push's partition-provisioning INSERT, which sorts its own rows
    -- (ORDER BY 1, 2)
    -- precisely so concurrent bundles touching overlapping queue sets take their
    -- row locks in one canonical order. An unordered multi-row upsert here would
    -- reintroduce exactly that deadlock one level down: hash aggregation emits
    -- groups in an order that depends on the group SET, so two pushes covering
    -- {q1,q2,q3} and {q1,q3} can lock q1/q3 in opposite orders and deadlock the
    -- push. Sorting the aggregate on the conflict key restores the total order.
    INSERT INTO queen.queue_lag_metrics (bucket_time, queue_id, partitions_created)
    SELECT
        date_trunc('minute', NOW()),
        np.queue_id,
        COUNT(*)
    FROM new_partitions np
    GROUP BY np.queue_id
    ORDER BY np.queue_id
    ON CONFLICT (bucket_time, queue_id) DO UPDATE
    SET partitions_created = queen.queue_lag_metrics.partitions_created + EXCLUDED.partitions_created;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION queen.bump_partitions_deleted_trigger()
RETURNS TRIGGER AS $$
BEGIN
    -- On DELETE the queue row may already be gone: log_partitions.queue_id is
    -- ON DELETE CASCADE from queen.queues (001_log_schema), so deleting a queue runs this
    -- statement with the parent removed. Those rows are deliberately NOT
    -- counted — the EXISTS guard drops them. A partition that disappeared
    -- because its whole queue was deleted is not a partition-lifecycle event,
    -- and inserting a metric row for it would be an FK violation anyway (the
    -- parent queues row is mid-delete, and the cascade has already swept this
    -- queue's queue_lag_metrics rows). What this counter measures is the
    -- retention cleanup (queen.log_partition_cleanup_step_v1,
    -- 006_log_maintenance), which keeps its queue and so always passes the
    -- guard.
    INSERT INTO queen.queue_lag_metrics (bucket_time, queue_id, partitions_deleted)
    SELECT
        date_trunc('minute', NOW()),
        op.queue_id,
        COUNT(*)
    FROM old_partitions op
    WHERE EXISTS (SELECT 1 FROM queen.queues q WHERE q.id = op.queue_id)
    GROUP BY op.queue_id
    ORDER BY op.queue_id   -- same lock-order discipline as the INSERT side
    ON CONFLICT (bucket_time, queue_id) DO UPDATE
    SET partitions_deleted = queen.queue_lag_metrics.partitions_deleted + EXCLUDED.partitions_deleted;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- The ATTACHMENT lives in 020_log_partition_counters.sql, not here. Two
-- reasons, both learned the hard way: under the old apply order this file ran
-- before the log schema created queen.log_partitions, so an attachment guarded
-- on the table existing silently skipped the whole first boot of a fresh
-- database; and re-running DROP/CREATE TRIGGER on every boot takes an ACCESS
-- EXCLUSIVE lock on the engine's hottest table, stalling every push and pop
-- behind a rolling restart.

-- ============================================================================
-- queen.get_queue_ops_v1: Per-queue ops time series for the System view
-- ============================================================================
-- Returns pop / push / ack / trx throughput + lag + partition lifecycle per
-- queue bucketed on the range's natural bucket (same rule as get_status_v3).
-- Uses the extended queue_lag_metrics columns populated by libqueen and the
-- partition-lifecycle triggers.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.get_queue_ops_v1(
    p_filters JSONB DEFAULT '{}'::jsonb
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_from_ts TIMESTAMPTZ;
    v_to_ts TIMESTAMPTZ;
    v_queue TEXT;
    v_duration_minutes INTEGER;
    v_bucket_minutes INTEGER;
    v_series JSONB;
    v_queues JSONB;
    -- Track B (§5): tenant travels in the filter JSON (`_tenant`); signature unchanged.
    v_tenant UUID := COALESCE((p_filters->>'_tenant')::uuid, '00000000-0000-0000-0000-000000000001');
BEGIN
    v_from_ts := COALESCE((p_filters->>'from')::timestamptz, NOW() - INTERVAL '1 hour');
    v_to_ts := COALESCE((p_filters->>'to')::timestamptz, NOW());
    v_queue := p_filters->>'queue';

    v_duration_minutes := EXTRACT(EPOCH FROM (v_to_ts - v_from_ts)) / 60;
    v_bucket_minutes := CASE
        WHEN v_duration_minutes <= 60 THEN 1
        WHEN v_duration_minutes <= 360 THEN 5
        WHEN v_duration_minutes <= 1440 THEN 15
        WHEN v_duration_minutes <= 10080 THEN 60
        ELSE 360
    END;

    WITH rolled AS (
        -- Queue identity is the queen.queues id: name and tenant filter come
        -- from the joined queues row.
        SELECT
            date_trunc('minute', qlm.bucket_time) -
                (EXTRACT(minute FROM qlm.bucket_time)::integer % v_bucket_minutes) * INTERVAL '1 minute' AS bucket,
            q.name                  AS queue_name,
            SUM(qlm.push_request_count) AS push_req,
            SUM(qlm.push_message_count) AS push_msg,
            SUM(qlm.pop_count)          AS pop_msg,
            SUM(qlm.pop_empty_count)    AS pop_empty,
            SUM(qlm.ack_request_count)  AS ack_req,
            SUM(qlm.ack_success_count)  AS ack_ok,
            SUM(qlm.ack_failed_count)   AS ack_fail,
            SUM(qlm.transaction_count)  AS trx_cnt,
            SUM(qlm.partitions_created) AS parts_created,
            SUM(qlm.partitions_deleted) AS parts_deleted,
            MAX(qlm.partition_count)    AS parts_snapshot,
            -- Lag is sampled AT POP. A bucket with pushes but no pops has NO
            -- lag sample, which is not the same fact as "lag was zero" — emit
            -- NULL so the client renders "—" instead of a reassuring 0 for a
            -- backlogged queue whose consumers are stopped.
            CASE WHEN SUM(qlm.pop_count) > 0
                 THEN SUM(qlm.avg_lag_ms * qlm.pop_count) / SUM(qlm.pop_count)
                 END AS avg_lag_ms,
            CASE WHEN SUM(qlm.pop_count) > 0
                 THEN MAX(qlm.max_lag_ms)
                 END AS max_lag_ms,
            -- parked_count is a GAUGE (already SUM-aggregated across workers
            -- at insert time). Across time-buckets we AVG to get the typical
            -- in-flight long-poll count for the window.
            AVG(COALESCE(qlm.parked_count, 0))::numeric AS parked_avg
        FROM queen.queue_lag_metrics qlm
        JOIN queen.queues q ON q.id = qlm.queue_id
        WHERE qlm.bucket_time >= v_from_ts
          AND qlm.bucket_time <= v_to_ts
          AND q.tenant_id = v_tenant
          AND (v_queue IS NULL OR q.name = v_queue)
        GROUP BY 1, 2
    )
    SELECT
        COALESCE(jsonb_agg(
            jsonb_build_object(
                'bucket', to_char(bucket, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
                'queueName', queue_name,
                'pushRequests',       push_req,
                'pushMessages',       push_msg,
                'popMessages',        pop_msg,
                'popEmpty',           pop_empty,
                'ackRequests',        ack_req,
                'ackSuccess',         ack_ok,
                'ackFailed',          ack_fail,
                'transactions',       trx_cnt,
                'partitionsCreated',  parts_created,
                'partitionsDeleted',  parts_deleted,
                'partitionCount',     parts_snapshot,
                'avgLagMs',           avg_lag_ms,
                'maxLagMs',           max_lag_ms,
                -- Rate-normalized helpers so the UI doesn't have to divide:
                'pushPerSecond',      ROUND(push_msg::numeric / (v_bucket_minutes * 60), 2),
                'popPerSecond',       ROUND(pop_msg::numeric  / (v_bucket_minutes * 60), 2),
                'ackPerSecond',       ROUND((ack_ok + ack_fail)::numeric / (v_bucket_minutes * 60), 2),
                'emptyPerSecond',     ROUND(pop_empty::numeric / (v_bucket_minutes * 60), 2),
                -- Parked is a gauge (typical in-flight long-poll count for
                -- the window) — no /sec normalization. Rounded to 2 dp so
                -- multi-minute buckets show fractional values cleanly.
                'parkedCount',        ROUND(parked_avg, 2)
            ) ORDER BY bucket ASC, queue_name
        ), '[]'::jsonb)
    INTO v_series
    FROM rolled;

    -- Distinct queue list (for UI filters)
    SELECT COALESCE(jsonb_agg(qn ORDER BY qn), '[]'::jsonb) INTO v_queues
    FROM (
        SELECT DISTINCT q.name AS qn
        FROM queen.queue_lag_metrics qlm
        JOIN queen.queues q ON q.id = qlm.queue_id
        WHERE qlm.bucket_time >= v_from_ts AND qlm.bucket_time <= v_to_ts
          AND q.tenant_id = v_tenant
          AND (v_queue IS NULL OR q.name = v_queue)
    ) q;

    RETURN jsonb_build_object(
        'timeRange', jsonb_build_object(
            'from', to_char(v_from_ts, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'to',   to_char(v_to_ts,   'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
        ),
        'bucketMinutes', v_bucket_minutes,
        'series',  v_series,
        'queues',  v_queues
    );
END;
$$;

-- ============================================================================
-- queen.get_queue_parked_per_replica_v1: Parked-by-(queue, replica) time series
-- ============================================================================
-- Returns the same minute-averaged parked gauge as get_queue_ops_v1, but
-- broken out by replica (hostname × worker_id) instead of cluster-aggregated.
-- The chart's "individual replicas" toggle on the Parked tab uses this.
--
-- Bucketing rules match get_queue_ops_v1 so the time axis lines up exactly.
-- Across time-buckets we AVG (gauge); within a bucket we just AVG across the
-- per-minute rows belonging to (queue, replica).
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.get_queue_parked_per_replica_v1(
    p_filters JSONB DEFAULT '{}'::jsonb
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_from_ts TIMESTAMPTZ;
    v_to_ts TIMESTAMPTZ;
    v_queue TEXT;
    v_duration_minutes INTEGER;
    v_bucket_minutes INTEGER;
    v_series JSONB;
    v_replicas JSONB;
    -- Track B (§5): tenant travels in the filter JSON (`_tenant`); signature unchanged.
    v_tenant UUID := COALESCE((p_filters->>'_tenant')::uuid, '00000000-0000-0000-0000-000000000001');
BEGIN
    v_from_ts := COALESCE((p_filters->>'from')::timestamptz, NOW() - INTERVAL '1 hour');
    v_to_ts   := COALESCE((p_filters->>'to')::timestamptz,   NOW());
    v_queue   := p_filters->>'queue';

    v_duration_minutes := EXTRACT(EPOCH FROM (v_to_ts - v_from_ts)) / 60;
    v_bucket_minutes := CASE
        WHEN v_duration_minutes <= 60 THEN 1
        WHEN v_duration_minutes <= 360 THEN 5
        WHEN v_duration_minutes <= 1440 THEN 15
        WHEN v_duration_minutes <= 10080 THEN 60
        ELSE 360
    END;

    WITH rolled AS (
        SELECT
            date_trunc('minute', bucket_time) -
                (EXTRACT(minute FROM bucket_time)::integer % v_bucket_minutes) * INTERVAL '1 minute' AS bucket,
            queue_name,
            hostname,
            worker_id,
            AVG(COALESCE(parked_count, 0))::numeric AS parked_avg
        FROM queen.queue_parked_replica
        WHERE bucket_time >= v_from_ts
          AND bucket_time <= v_to_ts
          AND tenant_id = v_tenant
          AND (v_queue IS NULL OR queue_name = v_queue)
        GROUP BY 1, 2, 3, 4
    )
    SELECT
        COALESCE(jsonb_agg(
            jsonb_build_object(
                'bucket',      to_char(bucket, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
                'queueName',   queue_name,
                'hostname',    hostname,
                'workerId',    worker_id,
                'parkedCount', ROUND(parked_avg, 2)
            ) ORDER BY bucket ASC, queue_name, hostname, worker_id
        ), '[]'::jsonb)
    INTO v_series
    FROM rolled;

    -- Distinct (hostname, worker_id) pairs in the window — useful for the UI
    -- to know which replicas are represented even before any datapoint.
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'hostname', hostname,
        'workerId', worker_id
    ) ORDER BY hostname, worker_id), '[]'::jsonb)
    INTO v_replicas
    FROM (
        SELECT DISTINCT hostname, worker_id
        FROM queen.queue_parked_replica
        WHERE bucket_time >= v_from_ts
          AND bucket_time <= v_to_ts
          AND tenant_id = v_tenant
          AND (v_queue IS NULL OR queue_name = v_queue)
    ) q;

    RETURN jsonb_build_object(
        'timeRange', jsonb_build_object(
            'from', to_char(v_from_ts, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'to',   to_char(v_to_ts,   'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
        ),
        'bucketMinutes', v_bucket_minutes,
        'series',   v_series,
        'replicas', v_replicas
    );
END;
$$;

-- Grant permissions
GRANT EXECUTE ON FUNCTION queen.get_worker_throughput_v1(TIMESTAMPTZ, TIMESTAMPTZ, INTEGER) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_queue_lag_v1(TIMESTAMPTZ, TIMESTAMPTZ, TEXT, UUID) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_worker_health_v1(TIMESTAMPTZ, TIMESTAMPTZ) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_system_totals_v1() TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_system_overview_v3(UUID) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_status_v3(JSONB) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_worker_metrics_timeseries_v1(JSONB) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.cleanup_worker_metrics_v1(INTEGER) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_queue_ops_v1(JSONB) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_queue_parked_per_replica_v1(JSONB) TO PUBLIC;

