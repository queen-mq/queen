-- ============================================================================
-- Stats Tables and Optimized Analytics Procedures
-- ============================================================================
-- Pre-computed statistics for O(1) analytics queries
-- Replaces expensive message table scans with cached counters
-- ============================================================================

-- ============================================================================
-- Stats Tables and Optimized Analytics Procedures
-- ============================================================================
-- Pre-computed statistics for O(1) analytics queries
-- Replaces expensive message table scans with cached counters
-- ============================================================================

-- ============================================================================
-- ROWS-ENGINE RECONCILER: REMOVED
-- ============================================================================
-- Deleted here: increment_message_counts_v1, compute_partition_stats_v1 / v2 /
-- v3, compute_partition_stats_chunk_v1, aggregate_queue_stats_v1 / v2.
-- They built the 'partition' stat rows by walking the rows message plane
-- (the dropped messages / partitions / partition_consumers /
-- dead_letter_queue / partition_lookup tables), which this broker no longer
-- ships (every queue is storage='segments'). Their last caller,
-- refresh_all_stats_v1, is gone too:
-- POST /api/v1/stats/refresh now runs queen.log_refresh_all_stats_v1
-- (011_log_stats.sql), which writes the 'queue' rows straight from the log
-- tables and then calls the three aggregators kept below.

-- Aggregate namespace stats from queue stats
CREATE OR REPLACE FUNCTION queen.aggregate_namespace_stats_v1()
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_updated INTEGER := 0;
BEGIN
    INSERT INTO queen.stats (
        stat_type, stat_key,
        child_count, total_messages, pending_messages, processing_messages,
        completed_messages, dead_letter_messages,
        avg_lag_seconds, max_lag_seconds,
        ingested_per_second, processed_per_second,
        last_computed_at
    )
    SELECT 
        'namespace',
        q.namespace,
        COUNT(DISTINCT q.id),
        COALESCE(SUM(s.total_messages), 0),
        COALESCE(SUM(s.pending_messages), 0),
        COALESCE(SUM(s.processing_messages), 0),
        COALESCE(SUM(s.completed_messages), 0),
        COALESCE(SUM(s.dead_letter_messages), 0),
        COALESCE(AVG(s.avg_lag_seconds)::integer, 0),
        COALESCE(MAX(s.max_lag_seconds), 0),
        COALESCE(SUM(s.ingested_per_second), 0),
        COALESCE(SUM(s.processed_per_second), 0),
        NOW()
    FROM queen.queues q
    LEFT JOIN queen.stats s ON s.stat_type = 'queue' AND s.queue_id = q.id
    WHERE q.namespace IS NOT NULL AND q.namespace != ''
    GROUP BY q.namespace
    ON CONFLICT (stat_type, stat_key) DO UPDATE SET
        child_count = EXCLUDED.child_count,
        total_messages = EXCLUDED.total_messages,
        pending_messages = EXCLUDED.pending_messages,
        processing_messages = EXCLUDED.processing_messages,
        completed_messages = EXCLUDED.completed_messages,
        dead_letter_messages = EXCLUDED.dead_letter_messages,
        avg_lag_seconds = EXCLUDED.avg_lag_seconds,
        max_lag_seconds = EXCLUDED.max_lag_seconds,
        ingested_per_second = EXCLUDED.ingested_per_second,
        processed_per_second = EXCLUDED.processed_per_second,
        last_computed_at = NOW();
    
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    
    RETURN jsonb_build_object('namespacesUpdated', v_updated);
END;
$$;

-- Aggregate task stats from queue stats
CREATE OR REPLACE FUNCTION queen.aggregate_task_stats_v1()
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_updated INTEGER := 0;
BEGIN
    INSERT INTO queen.stats (
        stat_type, stat_key,
        child_count, total_messages, pending_messages, processing_messages,
        completed_messages, dead_letter_messages,
        avg_lag_seconds, max_lag_seconds,
        ingested_per_second, processed_per_second,
        last_computed_at
    )
    SELECT 
        'task',
        q.task,
        COUNT(DISTINCT q.id),
        COALESCE(SUM(s.total_messages), 0),
        COALESCE(SUM(s.pending_messages), 0),
        COALESCE(SUM(s.processing_messages), 0),
        COALESCE(SUM(s.completed_messages), 0),
        COALESCE(SUM(s.dead_letter_messages), 0),
        COALESCE(AVG(s.avg_lag_seconds)::integer, 0),
        COALESCE(MAX(s.max_lag_seconds), 0),
        COALESCE(SUM(s.ingested_per_second), 0),
        COALESCE(SUM(s.processed_per_second), 0),
        NOW()
    FROM queen.queues q
    LEFT JOIN queen.stats s ON s.stat_type = 'queue' AND s.queue_id = q.id
    WHERE q.task IS NOT NULL AND q.task != ''
    GROUP BY q.task
    ON CONFLICT (stat_type, stat_key) DO UPDATE SET
        child_count = EXCLUDED.child_count,
        total_messages = EXCLUDED.total_messages,
        pending_messages = EXCLUDED.pending_messages,
        processing_messages = EXCLUDED.processing_messages,
        completed_messages = EXCLUDED.completed_messages,
        dead_letter_messages = EXCLUDED.dead_letter_messages,
        avg_lag_seconds = EXCLUDED.avg_lag_seconds,
        max_lag_seconds = EXCLUDED.max_lag_seconds,
        ingested_per_second = EXCLUDED.ingested_per_second,
        processed_per_second = EXCLUDED.processed_per_second,
        last_computed_at = NOW();
    
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    
    RETURN jsonb_build_object('tasksUpdated', v_updated);
END;
$$;

-- aggregate_system_stats_v1 REMOVED: superseded by v2 below (the only one
-- log_refresh_all_stats_v1 calls) and its child_count counted rows-engine
-- partitions.

-- Optimized system stats aggregation
-- Eliminates correlated subqueries for prev_* values (same fix as aggregate_queue_stats_v2)
CREATE OR REPLACE FUNCTION queen.aggregate_system_stats_v2()
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_updated INTEGER := 0;
BEGIN
    INSERT INTO queen.stats (
        stat_type, stat_key,
        child_count, total_messages, pending_messages, processing_messages,
        completed_messages, dead_letter_messages,
        avg_lag_seconds, max_lag_seconds, median_lag_seconds,
        avg_offset_lag, max_offset_lag,
        ingested_per_second, processed_per_second,
        last_computed_at
    )
    SELECT
        'system',
        'global',
        -- child_count = live partition count. Repointed from the dropped
        -- rows-engine partitions table to the log partition table.
        (SELECT COUNT(*) FROM queen.log_partitions),
        COALESCE(SUM(s.total_messages), 0),
        COALESCE(SUM(s.pending_messages), 0),
        COALESCE(SUM(s.processing_messages), 0),
        COALESCE(SUM(s.completed_messages), 0),
        COALESCE(SUM(s.dead_letter_messages), 0),
        COALESCE(AVG(s.avg_lag_seconds)::integer, 0),
        COALESCE(MAX(s.max_lag_seconds), 0),
        COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.max_lag_seconds)::integer, 0),
        COALESCE(AVG(s.pending_messages)::integer, 0),
        COALESCE(MAX(s.pending_messages)::integer, 0),
        COALESCE(SUM(s.ingested_per_second), 0),
        COALESCE(SUM(s.processed_per_second), 0),
        NOW()
    FROM queen.stats s
    WHERE s.stat_type = 'queue'
    ON CONFLICT (stat_type, stat_key) DO UPDATE SET
        child_count = EXCLUDED.child_count,
        total_messages = EXCLUDED.total_messages,
        pending_messages = EXCLUDED.pending_messages,
        processing_messages = EXCLUDED.processing_messages,
        completed_messages = EXCLUDED.completed_messages,
        dead_letter_messages = EXCLUDED.dead_letter_messages,
        avg_lag_seconds = EXCLUDED.avg_lag_seconds,
        max_lag_seconds = EXCLUDED.max_lag_seconds,
        median_lag_seconds = EXCLUDED.median_lag_seconds,
        avg_offset_lag = EXCLUDED.avg_offset_lag,
        max_offset_lag = EXCLUDED.max_offset_lag,
        -- >= 3 s elapsed floor on the rate window, matching 011's queue-row
        -- arms (staggered replica writers can land near-simultaneously; a
        -- sub-second denominator makes a garbage spike). ELSE arm untouched:
        -- this writer's historical semantics keep the previous value.
        ingested_per_second = CASE
            WHEN queen.stats.prev_snapshot_at IS NOT NULL
                AND NOW() > queen.stats.prev_snapshot_at
                AND EXTRACT(EPOCH FROM (NOW() - queen.stats.prev_snapshot_at)) >= 3
            THEN ((EXCLUDED.total_messages - queen.stats.prev_total_messages)::numeric /
                  EXTRACT(EPOCH FROM (NOW() - queen.stats.prev_snapshot_at)))
            ELSE queen.stats.ingested_per_second
        END,
        processed_per_second = CASE
            WHEN queen.stats.prev_snapshot_at IS NOT NULL
                AND NOW() > queen.stats.prev_snapshot_at
                AND EXTRACT(EPOCH FROM (NOW() - queen.stats.prev_snapshot_at)) >= 3
            THEN ((EXCLUDED.completed_messages - queen.stats.prev_completed_messages)::numeric /
                  EXTRACT(EPOCH FROM (NOW() - queen.stats.prev_snapshot_at)))
            ELSE queen.stats.processed_per_second
        END,
        prev_total_messages = queen.stats.total_messages,
        prev_completed_messages = queen.stats.completed_messages,
        prev_snapshot_at = queen.stats.last_computed_at,
        last_computed_at = NOW();

    GET DIAGNOSTICS v_updated = ROW_COUNT;

    RETURN jsonb_build_object('systemUpdated', v_updated);
END;
$$;

-- NOTE: write_stats_history_v1 and cleanup_stats_history_v1 REMOVED
-- Throughput history is now handled by worker_metrics (see 019_worker_metrics.sql)

-- cleanup_orphaned_stats_v1 / v2 REMOVED: both pruned 'partition' stat rows by
-- probing the rows-engine partitions table, which this engine no longer has,
-- and their only caller was refresh_all_stats_v1 (also removed). No 'partition'
-- rows are written any more, so there is nothing left to orphan.

-- ============================================================================
-- OPTIMIZED ANALYTICS PROCEDURES (V2)
-- These return the SAME FORMAT as V1 but use pre-computed stats
-- ============================================================================

-- NOTE: get_system_overview_v2 REMOVED - replaced by get_system_overview_v3 in 019_worker_metrics.sql

-- queen.get_queues_v2: O(queues) queue list with stats
-- Track B (§5): p_tenant scopes the queue list (resources/queues).
CREATE OR REPLACE FUNCTION queen.get_queues_v2(
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001')
RETURNS JSONB
LANGUAGE plpgsql
AS $$
BEGIN
    -- Return same format as get_queues_v1
    RETURN (
        SELECT jsonb_build_object(
            'queues', COALESCE(jsonb_agg(
                jsonb_build_object(
                    'id', q.id,
                    'name', q.name,
                    'namespace', q.namespace,
                    'task', q.task,
                    'createdAt', to_char(q.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                    'partitions', COALESCE(s.child_count, 0),
                    -- Storage quota (§6.1): retained payload bytes for this queue,
                    -- refreshed at the stats cadence (log_refresh_all_stats_v1).
                    'retainedBytes', COALESCE(s.retained_bytes, 0),
                    'messages', jsonb_build_object(
                        'total', COALESCE(s.total_messages, 0),
                        'pending', GREATEST(0, COALESCE(s.pending_messages, 0) - COALESCE(s.processing_messages, 0)),
                        'processing', COALESCE(s.processing_messages, 0)
                    )
                ) ORDER BY q.created_at DESC
            ), '[]'::jsonb)
        )
        FROM queen.queues q
        LEFT JOIN queen.stats s ON s.stat_type = 'queue' AND s.queue_id = q.id
        WHERE q.tenant_id = p_tenant
    );
END;
$$;

-- queen.get_queue_v2 REMOVED from this file: the body here drilled down through
-- the rows-engine partitions table. 011_log_stats.sql defines the live
-- (TEXT, UUID) form over the log tables.

-- queen.get_namespaces_v2: O(namespaces) namespace list.
-- Track B (§5): p_tenant scopes the rollup. Re-derived from THIS tenant's
-- 'queue' stat rows (each queue belongs to exactly one tenant) rather than the
-- shared pre-aggregated 'namespace' rows (which merge namespaces across
-- tenants). The math is aggregate_namespace_stats_v1's formula filtered by
-- tenant, so flag OFF (every queue = default tenant) reproduces the previous
-- output.
CREATE OR REPLACE FUNCTION queen.get_namespaces_v2(
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001')
RETURNS JSONB
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN (
        SELECT jsonb_build_object(
            'namespaces', COALESCE(jsonb_agg(
                jsonb_build_object(
                    'namespace', t.ns,
                    'queues', t.qcount,
                    -- Real log-partition count for the namespace. This used to
                    -- count the rows-engine partitions table: a phantom number,
                    -- 0 for push-created queues, 1 per /configure'd queue --
                    -- while the queue actually had N log partitions. Queue
                    -- identity is the queen.queues id (lp.queue_id points at it
                    -- directly), so it agrees with the per-queue 'partitions'
                    -- get_queues_v2 reports.
                    'partitions', (
                        SELECT COUNT(*)
                        FROM queen.log_partitions lp
                        JOIN queen.queues q2 ON q2.id = lp.queue_id
                        WHERE q2.namespace = t.ns AND q2.tenant_id = p_tenant
                    ),
                    'messages', jsonb_build_object(
                        'total', t.total_m,
                        'pending', GREATEST(0, t.pending_m - t.processing_m)
                    )
                ) ORDER BY t.ns
            ), '[]'::jsonb)
        )
        FROM (
            SELECT q.namespace AS ns,
                   COUNT(DISTINCT q.id) AS qcount,
                   COALESCE(SUM(s.total_messages), 0) AS total_m,
                   COALESCE(SUM(s.pending_messages), 0) AS pending_m,
                   COALESCE(SUM(s.processing_messages), 0) AS processing_m
            FROM queen.queues q
            LEFT JOIN queen.stats s ON s.stat_type = 'queue' AND s.queue_id = q.id
            WHERE q.namespace IS NOT NULL AND q.namespace != ''
              AND q.tenant_id = p_tenant
            GROUP BY q.namespace
        ) t
    );
END;
$$;

GRANT EXECUTE ON FUNCTION queen.get_namespaces_v2(UUID) TO PUBLIC;

-- queen.get_tasks_v2: O(tasks) task list. Track B (§5): p_tenant scopes the
-- rollup, re-derived per tenant exactly like get_namespaces_v2 above.
CREATE OR REPLACE FUNCTION queen.get_tasks_v2(
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001')
RETURNS JSONB
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN (
        SELECT jsonb_build_object(
            'tasks', COALESCE(jsonb_agg(
                jsonb_build_object(
                    'task', t.tk,
                    'queues', t.qcount,
                    -- Real log-partition count for the task (same repoint as
                    -- get_namespaces_v2 above: the rows-engine partitions table
                    -- gave a phantom 0/1 per queue, not the live count; queue
                    -- identity is the queen.queues id).
                    'partitions', (
                        SELECT COUNT(*)
                        FROM queen.log_partitions lp
                        JOIN queen.queues q2 ON q2.id = lp.queue_id
                        WHERE q2.task = t.tk AND q2.tenant_id = p_tenant
                    ),
                    'messages', jsonb_build_object(
                        'total', t.total_m,
                        'pending', GREATEST(0, t.pending_m - t.processing_m)
                    )
                ) ORDER BY t.tk
            ), '[]'::jsonb)
        )
        FROM (
            SELECT q.task AS tk,
                   COUNT(DISTINCT q.id) AS qcount,
                   COALESCE(SUM(s.total_messages), 0) AS total_m,
                   COALESCE(SUM(s.pending_messages), 0) AS pending_m,
                   COALESCE(SUM(s.processing_messages), 0) AS processing_m
            FROM queen.queues q
            LEFT JOIN queen.stats s ON s.stat_type = 'queue' AND s.queue_id = q.id
            WHERE q.task IS NOT NULL AND q.task != ''
              AND q.tenant_id = p_tenant
            GROUP BY q.task
        ) t
    );
END;
$$;

GRANT EXECUTE ON FUNCTION queen.get_tasks_v2(UUID) TO PUBLIC;

-- NOTE: get_status_v2 REMOVED - replaced by get_status_v3 in 019_worker_metrics.sql

-- queen.get_queue_detail_v2 REMOVED from this file: same story as get_queue_v2
-- above -- the body here read the rows-engine partitions and partition_consumers
-- tables; 011_log_stats.sql defines the live (TEXT, UUID) form over the log
-- tables.

-- queen.get_status_queues_v2: Queues list using pre-computed stats
CREATE OR REPLACE FUNCTION queen.get_status_queues_v2(
    p_filters JSONB DEFAULT '{}'::jsonb,
    p_limit INTEGER DEFAULT 100,
    p_offset INTEGER DEFAULT 0
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_namespace TEXT;
    v_task TEXT;
    v_queue TEXT;
    -- Track B (§5): tenant travels in the filter JSON (`_tenant`); signature unchanged.
    v_tenant UUID := COALESCE((p_filters->>'_tenant')::uuid, '00000000-0000-0000-0000-000000000001');
BEGIN
    v_namespace := p_filters->>'namespace';
    v_task := p_filters->>'task';
    v_queue := p_filters->>'queue';

    RETURN (
        SELECT jsonb_build_object(
            'queues', COALESCE(jsonb_agg(queue_data), '[]'::jsonb),
            'pagination', jsonb_build_object(
                'limit', p_limit,
                'offset', p_offset
            )
        )
        FROM (
            SELECT jsonb_build_object(
                'id', q.id,
                'name', q.name,
                'namespace', q.namespace,
                'task', q.task,
                'createdAt', to_char(q.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                'partitions', COALESCE(s.child_count, 0),
                'messages', jsonb_build_object(
                    'total', COALESCE(s.total_messages, 0),
                    'pending', COALESCE(s.pending_messages, 0),
                    'processing', COALESCE(s.processing_messages, 0),
                    -- Exposed so the console's per-queue volume chart stops
                    -- stacking a "Completed" series off a key that was never
                    -- in the payload (silently 0 for every queue).
                    'completed', COALESCE(s.completed_messages, 0),
                    'deadLetter', COALESCE(s.dead_letter_messages, 0)
                )
            ) as queue_data
            FROM queen.queues q
            LEFT JOIN queen.stats s ON s.stat_type = 'queue' AND s.queue_id = q.id
            WHERE q.tenant_id = v_tenant
              AND (v_namespace IS NULL OR q.namespace = v_namespace)
              AND (v_task IS NULL OR q.task = v_task)
              -- `queue` is forwarded by the handler; filtering here stops the
              -- client from having to fetch the whole tenant list to show one.
              AND (v_queue IS NULL OR q.name = v_queue)
            ORDER BY q.created_at DESC
            LIMIT p_limit OFFSET p_offset
        ) subq
    );
END;
$$;

-- ============================================================================
-- FULL STATS REFRESH / STATS LEADER ELECTION: REMOVED
-- ============================================================================
-- refresh_all_stats_v1 was the rows reconciler's entry point (it chained
-- compute_partition_stats_v3 -> aggregate_queue_stats_v2 -> ... ->
-- cleanup_orphaned_stats_v2, all deleted above); queen.log_refresh_all_stats_v1
-- in 011_log_stats.sql replaces it. is_stats_leader went with it: the Rust
-- stats task (src/stats.rs) elects its own leader through a transaction-level
-- advisory lock, so nothing called this any more.

-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================

-- NOTE: the GRANTs of the deleted rows-engine functions went with them
-- (is_stats_leader, increment_message_counts_v1, compute_partition_stats_v1/v2/
-- v3, compute_partition_stats_chunk_v1, aggregate_queue_stats_v1/v2,
-- aggregate_system_stats_v1, cleanup_orphaned_stats_v1/v2, refresh_all_stats_v1).
GRANT EXECUTE ON FUNCTION queen.aggregate_namespace_stats_v1() TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.aggregate_task_stats_v1() TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.aggregate_system_stats_v2() TO PUBLIC;
-- NOTE: write_stats_history_v1 and cleanup_stats_history_v1 REMOVED (handled by worker_metrics)

-- NOTE: get_system_overview_v2 and get_status_v2 REMOVED (replaced by v3 in 019_worker_metrics.sql)
-- NOTE: get_queue_v2 and get_queue_detail_v2 are defined (and granted) in
-- 011_log_stats.sql, not here.
GRANT EXECUTE ON FUNCTION queen.get_queues_v2(UUID) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_namespaces_v2(UUID) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_tasks_v2(UUID) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_status_queues_v2(JSONB, INTEGER, INTEGER) TO PUBLIC;
