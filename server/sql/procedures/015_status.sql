-- ============================================================================
-- Status/Dashboard Stored Procedures
-- ============================================================================
-- Async stored procedures for status/dashboard operations
-- Note: get_status_v1, get_status_queues_v1, get_queue_detail_v1 have been
--       replaced by v2 versions in 018_stats.sql
-- Note: get_queue_messages_v1 lived here and was deleted with the rows message
--       plane — it read the retired rows tables, was never ported to the log
--       engine (see 010_log_admin.sql) and had no caller.
-- Note: get_analytics_v1 lived here too; its copy was dead (shadowed at every
--       boot by the log-native body). The only definition is the one in
--       011_log_stats.sql.
-- ============================================================================

-- ============================================================================
-- queen.get_system_metrics_v1: System metrics time series
-- Returns data grouped by replica with timeSeries arrays, matching frontend expectations
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.get_system_metrics_v1(p_filters JSONB DEFAULT '{}'::jsonb)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_from_ts TIMESTAMPTZ;
    v_to_ts TIMESTAMPTZ;
    v_hostname TEXT;
    v_worker_id TEXT;
    v_duration_minutes INTEGER;
    v_bucket_minutes INTEGER;
    v_bucket_interval INTERVAL;
    v_point_count INTEGER;
BEGIN
    v_from_ts := COALESCE((p_filters->>'from')::timestamptz, NOW() - INTERVAL '1 hour');
    v_to_ts := COALESCE((p_filters->>'to')::timestamptz, NOW());
    v_hostname := p_filters->>'hostname';
    v_worker_id := p_filters->>'workerId';
    
    -- Calculate duration and bucket size (matching C++ logic)
    v_duration_minutes := EXTRACT(EPOCH FROM (v_to_ts - v_from_ts)) / 60;
    
    -- Calculate bucket size: aim for ~50 points maximum
    v_bucket_minutes := CASE
        WHEN v_duration_minutes <= 60 THEN 1           -- Up to 1 hour: 1 minute buckets
        WHEN v_duration_minutes <= 360 THEN 5          -- Up to 6 hours: 5 minute buckets
        WHEN v_duration_minutes <= 1440 THEN 15        -- Up to 24 hours: 15 minute buckets
        WHEN v_duration_minutes <= 10080 THEN 60       -- Up to 7 days: 1 hour buckets
        ELSE 360                                        -- Longer: 6 hour buckets
    END;
    
    v_bucket_interval := (v_bucket_minutes || ' minutes')::INTERVAL;
    
    -- Build result with replica grouping and aggregated metrics
    RETURN (
        WITH bucketed_data AS (
            SELECT
                date_trunc('minute', sm.timestamp) - 
                    (EXTRACT(minute FROM sm.timestamp)::integer % v_bucket_minutes) * INTERVAL '1 minute' AS bucket_timestamp,
                sm.hostname,
                sm.port,
                sm.worker_id,
                sm.timestamp,
                sm.sample_count,
                sm.metrics
            FROM queen.system_metrics sm
            WHERE sm.timestamp >= v_from_ts AND sm.timestamp <= v_to_ts
              AND (v_hostname IS NULL OR sm.hostname = v_hostname)
              AND (v_worker_id IS NULL OR sm.worker_id = v_worker_id)
        ),
        aggregated AS (
            SELECT
                bucket_timestamp,
                hostname,
                port,
                worker_id,
                SUM(sample_count) as sample_count,
                jsonb_build_object(
                    'cpu', jsonb_build_object(
                        'user_us', jsonb_build_object(
                            'avg', SUM((metrics->'cpu'->'user_us'->>'avg')::numeric * sample_count) / NULLIF(SUM(sample_count), 0),
                            'min', MIN((metrics->'cpu'->'user_us'->>'min')::numeric),
                            'max', MAX((metrics->'cpu'->'user_us'->>'max')::numeric),
                            'last', (array_agg((metrics->'cpu'->'user_us'->>'last')::numeric ORDER BY timestamp DESC))[1]
                        ),
                        'system_us', jsonb_build_object(
                            'avg', SUM((metrics->'cpu'->'system_us'->>'avg')::numeric * sample_count) / NULLIF(SUM(sample_count), 0),
                            'min', MIN((metrics->'cpu'->'system_us'->>'min')::numeric),
                            'max', MAX((metrics->'cpu'->'system_us'->>'max')::numeric),
                            'last', (array_agg((metrics->'cpu'->'system_us'->>'last')::numeric ORDER BY timestamp DESC))[1]
                        )
                    ),
                    'memory', jsonb_build_object(
                        'rss_bytes', jsonb_build_object(
                            'avg', SUM((metrics->'memory'->'rss_bytes'->>'avg')::numeric * sample_count) / NULLIF(SUM(sample_count), 0),
                            'min', MIN((metrics->'memory'->'rss_bytes'->>'min')::numeric),
                            'max', MAX((metrics->'memory'->'rss_bytes'->>'max')::numeric),
                            'last', (array_agg((metrics->'memory'->'rss_bytes'->>'last')::numeric ORDER BY timestamp DESC))[1]
                        ),
                        'virtual_bytes', jsonb_build_object(
                            'avg', SUM((metrics->'memory'->'virtual_bytes'->>'avg')::numeric * sample_count) / NULLIF(SUM(sample_count), 0),
                            'min', MIN((metrics->'memory'->'virtual_bytes'->>'min')::numeric),
                            'max', MAX((metrics->'memory'->'virtual_bytes'->>'max')::numeric),
                            'last', (array_agg((metrics->'memory'->'virtual_bytes'->>'last')::numeric ORDER BY timestamp DESC))[1]
                        )
                    ),
                    'database', jsonb_build_object(
                        'pool_size', jsonb_build_object(
                            'avg', SUM((metrics->'database'->'pool_size'->>'avg')::numeric * sample_count) / NULLIF(SUM(sample_count), 0),
                            'min', MIN((metrics->'database'->'pool_size'->>'min')::numeric),
                            'max', MAX((metrics->'database'->'pool_size'->>'max')::numeric),
                            'last', (array_agg((metrics->'database'->'pool_size'->>'last')::numeric ORDER BY timestamp DESC))[1]
                        ),
                        'pool_idle', jsonb_build_object(
                            'avg', SUM((metrics->'database'->'pool_idle'->>'avg')::numeric * sample_count) / NULLIF(SUM(sample_count), 0),
                            'min', MIN((metrics->'database'->'pool_idle'->>'min')::numeric),
                            'max', MAX((metrics->'database'->'pool_idle'->>'max')::numeric),
                            'last', (array_agg((metrics->'database'->'pool_idle'->>'last')::numeric ORDER BY timestamp DESC))[1]
                        ),
                        'pool_active', jsonb_build_object(
                            'avg', SUM((metrics->'database'->'pool_active'->>'avg')::numeric * sample_count) / NULLIF(SUM(sample_count), 0),
                            'min', MIN((metrics->'database'->'pool_active'->>'min')::numeric),
                            'max', MAX((metrics->'database'->'pool_active'->>'max')::numeric),
                            'last', (array_agg((metrics->'database'->'pool_active'->>'last')::numeric ORDER BY timestamp DESC))[1]
                        )
                    ),
                    'threadpool', jsonb_build_object(
                        'db', jsonb_build_object(
                            'pool_size', jsonb_build_object(
                                'avg', SUM((metrics->'threadpool'->'db'->'pool_size'->>'avg')::numeric * sample_count) / NULLIF(SUM(sample_count), 0),
                                'min', MIN((metrics->'threadpool'->'db'->'pool_size'->>'min')::numeric),
                                'max', MAX((metrics->'threadpool'->'db'->'pool_size'->>'max')::numeric),
                                'last', (array_agg((metrics->'threadpool'->'db'->'pool_size'->>'last')::numeric ORDER BY timestamp DESC))[1]
                            ),
                            'queue_size', jsonb_build_object(
                                'avg', SUM((metrics->'threadpool'->'db'->'queue_size'->>'avg')::numeric * sample_count) / NULLIF(SUM(sample_count), 0),
                                'min', MIN((metrics->'threadpool'->'db'->'queue_size'->>'min')::numeric),
                                'max', MAX((metrics->'threadpool'->'db'->'queue_size'->>'max')::numeric),
                                'last', (array_agg((metrics->'threadpool'->'db'->'queue_size'->>'last')::numeric ORDER BY timestamp DESC))[1]
                            )
                        ),
                        'system', jsonb_build_object(
                            'pool_size', jsonb_build_object(
                                'avg', SUM((metrics->'threadpool'->'system'->'pool_size'->>'avg')::numeric * sample_count) / NULLIF(SUM(sample_count), 0),
                                'min', MIN((metrics->'threadpool'->'system'->'pool_size'->>'min')::numeric),
                                'max', MAX((metrics->'threadpool'->'system'->'pool_size'->>'max')::numeric),
                                'last', (array_agg((metrics->'threadpool'->'system'->'pool_size'->>'last')::numeric ORDER BY timestamp DESC))[1]
                            ),
                            'queue_size', jsonb_build_object(
                                'avg', SUM((metrics->'threadpool'->'system'->'queue_size'->>'avg')::numeric * sample_count) / NULLIF(SUM(sample_count), 0),
                                'min', MIN((metrics->'threadpool'->'system'->'queue_size'->>'min')::numeric),
                                'max', MAX((metrics->'threadpool'->'system'->'queue_size'->>'max')::numeric),
                                'last', (array_agg((metrics->'threadpool'->'system'->'queue_size'->>'last')::numeric ORDER BY timestamp DESC))[1]
                            )
                        )
                    ),
                    'registries', jsonb_build_object(
                        'response', jsonb_build_object(
                            'avg', SUM((metrics->'registries'->'response'->>'avg')::numeric * sample_count) / NULLIF(SUM(sample_count), 0),
                            'min', MIN((metrics->'registries'->'response'->>'min')::numeric),
                            'max', MAX((metrics->'registries'->'response'->>'max')::numeric),
                            'last', (array_agg((metrics->'registries'->'response'->>'last')::numeric ORDER BY timestamp DESC))[1]
                        )
                    ),
                    'shared_state', jsonb_build_object(
                        'enabled', bool_or((metrics->'shared_state'->>'enabled')::boolean),
                        'sidecar_ops', jsonb_build_object(
                            'push', jsonb_build_object(
                                'count', (array_agg((metrics->'shared_state'->'sidecar_ops'->'push'->'count'->>'last')::numeric ORDER BY timestamp DESC))[1],
                                'latency_us', (array_agg((metrics->'shared_state'->'sidecar_ops'->'push'->'latency_us'->>'last')::numeric ORDER BY timestamp DESC))[1],
                                'items', (array_agg((metrics->'shared_state'->'sidecar_ops'->'push'->'items'->>'last')::numeric ORDER BY timestamp DESC))[1]
                            ),
                            'pop', jsonb_build_object(
                                'count', (array_agg((metrics->'shared_state'->'sidecar_ops'->'pop'->'count'->>'last')::numeric ORDER BY timestamp DESC))[1],
                                'latency_us', (array_agg((metrics->'shared_state'->'sidecar_ops'->'pop'->'latency_us'->>'last')::numeric ORDER BY timestamp DESC))[1],
                                'items', (array_agg((metrics->'shared_state'->'sidecar_ops'->'pop'->'items'->>'last')::numeric ORDER BY timestamp DESC))[1]
                            ),
                            'ack', jsonb_build_object(
                                'count', (array_agg((metrics->'shared_state'->'sidecar_ops'->'ack'->'count'->>'last')::numeric ORDER BY timestamp DESC))[1],
                                'latency_us', (array_agg((metrics->'shared_state'->'sidecar_ops'->'ack'->'latency_us'->>'last')::numeric ORDER BY timestamp DESC))[1],
                                'items', (array_agg((metrics->'shared_state'->'sidecar_ops'->'ack'->'items'->>'last')::numeric ORDER BY timestamp DESC))[1]
                            )
                        ),
                        'queue_backoff', jsonb_build_object(
                            'queues_with_backoff', (array_agg((metrics->'shared_state'->'queue_backoff'->'queues_with_backoff'->>'last')::numeric ORDER BY timestamp DESC))[1],
                            'total_backed_off_groups', (array_agg((metrics->'shared_state'->'queue_backoff'->'total_backed_off_groups'->>'last')::numeric ORDER BY timestamp DESC))[1],
                            'avg_interval_ms', (array_agg((metrics->'shared_state'->'queue_backoff'->'avg_interval_ms'->>'last')::numeric ORDER BY timestamp DESC))[1]
                        ),
                        'queue_backoff_summary', (array_agg(metrics->'shared_state'->'queue_backoff_summary' ORDER BY timestamp DESC) FILTER (WHERE metrics->'shared_state'->'queue_backoff_summary' IS NOT NULL))[1],
                        'queue_config_cache', jsonb_build_object(
                            'size', (array_agg((metrics->'shared_state'->'queue_config_cache'->'size'->>'last')::numeric ORDER BY timestamp DESC))[1],
                            'hits', (array_agg((metrics->'shared_state'->'queue_config_cache'->'hits'->>'last')::numeric ORDER BY timestamp DESC))[1],
                            'misses', (array_agg((metrics->'shared_state'->'queue_config_cache'->'misses'->>'last')::numeric ORDER BY timestamp DESC))[1]
                        ),
                        'consumer_presence', jsonb_build_object(
                            'queues_tracked', (array_agg((metrics->'shared_state'->'consumer_presence'->'queues_tracked'->>'last')::numeric ORDER BY timestamp DESC))[1],
                            'servers_tracked', (array_agg((metrics->'shared_state'->'consumer_presence'->'servers_tracked'->>'last')::numeric ORDER BY timestamp DESC))[1],
                            'total_registrations', (array_agg((metrics->'shared_state'->'consumer_presence'->'total_registrations'->>'last')::numeric ORDER BY timestamp DESC))[1]
                        ),
                        'server_health', jsonb_build_object(
                            'alive', (array_agg((metrics->'shared_state'->'server_health'->'alive'->>'last')::numeric ORDER BY timestamp DESC))[1],
                            'dead', (array_agg((metrics->'shared_state'->'server_health'->'dead'->>'last')::numeric ORDER BY timestamp DESC))[1]
                        ),
                        'transport', jsonb_build_object(
                            'sent', (array_agg((metrics->'shared_state'->'transport'->'sent'->>'last')::numeric ORDER BY timestamp DESC))[1],
                            'received', (array_agg((metrics->'shared_state'->'transport'->'received'->>'last')::numeric ORDER BY timestamp DESC))[1],
                            'dropped', (array_agg((metrics->'shared_state'->'transport'->'dropped'->>'last')::numeric ORDER BY timestamp DESC))[1]
                        )
                    )
                ) as metrics
            FROM bucketed_data
            GROUP BY bucket_timestamp, hostname, port, worker_id
        ),
        -- Group data by replica (hostname:port:worker_id)
        replica_grouped AS (
            SELECT
                hostname,
                port,
                worker_id,
                jsonb_agg(
                    jsonb_build_object(
                        'timestamp', to_char(bucket_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                        'sampleCount', sample_count,
                        'metrics', metrics
                    ) ORDER BY bucket_timestamp ASC
                ) as time_series
            FROM aggregated
            GROUP BY hostname, port, worker_id
        ),
        replicas_array AS (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'hostname', hostname,
                    'port', port,
                    'workerId', worker_id,
                    'timeSeries', time_series
                )
            ) as replicas
            FROM replica_grouped
        )
        SELECT jsonb_build_object(
            'timeRange', jsonb_build_object(
                'from', to_char(v_from_ts, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                'to', to_char(v_to_ts, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
            ),
            'replicas', COALESCE((SELECT replicas FROM replicas_array), '[]'::jsonb),
            'replicaCount', COALESCE((SELECT jsonb_array_length(replicas) FROM replicas_array), 0),
            'bucketMinutes', v_bucket_minutes,
            'pointCount', (SELECT COUNT(*) FROM aggregated)
        )
    );
END;
$$;

-- Grant execute permissions
-- (the grants for get_queue_messages_v1 and get_analytics_v1 went with their
--  definitions; get_analytics_v1 is granted by 011_log_stats.sql)
GRANT EXECUTE ON FUNCTION queen.get_system_metrics_v1(JSONB) TO PUBLIC;
