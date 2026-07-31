-- ============================================================================
-- Retention & Eviction Analytics
-- ============================================================================
-- Aggregates queen.retention_history into a time-series grouped by minute
-- bucket (adjusted by range, same rule as get_status_v3) and split by
-- retention_type.
--
-- retention_type values written by the services today (see
-- server/src/services/retention_service.cpp and eviction_service.cpp):
--   - 'retention'              — age-based cleanup of expired messages
--   - 'completed_retention'    — cleanup of fully-consumed messages
--   - 'max_wait_time_eviction' — pop-side eviction of waiting messages
--
-- New rows written by PR 3 (partition counter triggers) will use:
--   - '__partition_created__' / '__partition_deleted__' — partition events
-- These are filtered OUT of this procedure; they are surfaced through
-- queue_lag_metrics.partitions_created / partitions_deleted instead.
-- ============================================================================

CREATE OR REPLACE FUNCTION queen.get_retention_timeseries_v1(
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
    v_totals JSONB;
    -- Track B (§5): tenant travels in the filter JSON (`_tenant`). The log
    -- engine's retention/eviction steps (006_log_maintenance) write
    -- queen.retention_history, and
    -- their partition_id is a queen.log_partitions id, so the queue resolves
    -- through lp.queue_id (which IS the queen.queues id) or the row lands in
    -- the "unresolvable" bucket and escapes the queue/tenant filters.
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

    WITH bucketed AS (
        SELECT
            date_trunc('minute', rh.executed_at) -
                (EXTRACT(minute FROM rh.executed_at)::integer % v_bucket_minutes) * INTERVAL '1 minute' AS bucket,
            rh.retention_type,
            rh.messages_deleted,
            -- Queue identity is the queen.queues id: lp.queue_id IS it, no
            -- name-join. (The retired rows engine's partition leg went with
            -- its table: no row here ever pointed at it, and the FK went with
            -- it — retention_history carries none.)
            lp.queue_id AS queue_id
        FROM queen.retention_history rh
        LEFT JOIN queen.log_partitions lp ON lp.id = rh.partition_id
        WHERE rh.executed_at >= v_from_ts
          AND rh.executed_at <= v_to_ts
          AND rh.retention_type NOT LIKE '\_\_%\_\_' ESCAPE '\'
    ),
    filtered AS (
        SELECT b.*
        FROM bucketed b
        LEFT JOIN queen.queues q ON q.id = b.queue_id
        WHERE (v_queue IS NULL OR q.name = v_queue)
          -- Track B (§5): a resolvable queue must belong to the tenant; rows with
          -- no resolvable queue (orphaned partition) stay in, as before OFF.
          AND (q.id IS NULL OR q.tenant_id = v_tenant)
    ),
    grouped AS (
        SELECT
            bucket,
            SUM(CASE WHEN retention_type = 'retention'
                     THEN messages_deleted ELSE 0 END) AS retention_msgs,
            SUM(CASE WHEN retention_type = 'completed_retention'
                     THEN messages_deleted ELSE 0 END) AS completed_retention_msgs,
            SUM(CASE WHEN retention_type = 'max_wait_time_eviction'
                     THEN messages_deleted ELSE 0 END) AS eviction_msgs,
            SUM(messages_deleted) AS total_msgs,
            COUNT(*) AS event_count
        FROM filtered
        GROUP BY bucket
    )
    SELECT
        COALESCE(jsonb_agg(
            jsonb_build_object(
                'bucket', to_char(bucket, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
                'retentionMsgs', retention_msgs,
                'completedRetentionMsgs', completed_retention_msgs,
                'evictionMsgs', eviction_msgs,
                'totalMsgs', total_msgs,
                'eventCount', event_count
            ) ORDER BY bucket ASC
        ), '[]'::jsonb),
        jsonb_build_object(
            'retentionMsgs', COALESCE(SUM(retention_msgs), 0),
            'completedRetentionMsgs', COALESCE(SUM(completed_retention_msgs), 0),
            'evictionMsgs', COALESCE(SUM(eviction_msgs), 0),
            'totalMsgs', COALESCE(SUM(total_msgs), 0),
            'eventCount', COALESCE(SUM(event_count), 0)
        )
    INTO v_series, v_totals
    FROM grouped;

    RETURN jsonb_build_object(
        'timeRange', jsonb_build_object(
            'from', to_char(v_from_ts, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'to',   to_char(v_to_ts,   'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
        ),
        'bucketMinutes', v_bucket_minutes,
        'series', COALESCE(v_series, '[]'::jsonb),
        'totals', COALESCE(v_totals, '{}'::jsonb)
    );
END;
$$;

GRANT EXECUTE ON FUNCTION queen.get_retention_timeseries_v1(JSONB) TO PUBLIC;
