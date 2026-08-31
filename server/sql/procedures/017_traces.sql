-- ============================================================================
-- Traces Stored Procedures
-- ============================================================================
-- Async stored procedures for message tracing operations
-- ============================================================================

-- queen.record_trace_v1 used to live here. It resolved the trace's message_id
-- from the rows engine's message table and BAILED when no such row existed —
-- which is every message on this broker. 010_log_admin.sql owns the only
-- definition now (message_id NULL, trace still keyed by
-- (partition_id, transaction_id)); the shadowed original is gone rather than
-- being applied and immediately overwritten.

-- ============================================================================
-- queen.get_message_traces_v1: Get all traces for a message
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.get_message_traces_v1(p_partition_id UUID, p_transaction_id TEXT)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_traces JSONB;
BEGIN
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'id', mt.id,
            'event_type', mt.event_type,
            'data', mt.data,
            'consumer_group', mt.consumer_group,
            'worker_id', mt.worker_id,
            'created_at', to_char(mt.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'trace_names', COALESCE(
                (SELECT jsonb_agg(mtn.trace_name ORDER BY mtn.trace_name) 
                 FROM queen.message_trace_names mtn WHERE mtn.trace_id = mt.id),
                '[]'::jsonb
            )
        ) ORDER BY mt.created_at ASC
    ), '[]'::jsonb) INTO v_traces
    FROM queen.message_traces mt
    WHERE mt.partition_id = p_partition_id AND mt.transaction_id = p_transaction_id;
    
    RETURN jsonb_build_object('traces', v_traces);
END;
$$;

-- ============================================================================
-- queen.get_traces_by_name_v1: Get traces by trace name with message info
-- ============================================================================
-- Track B (§5): p_tenant scopes the by-name listing — this is a NAME-addressed
-- read (no pid in the request), so without it any tenant reads every other
-- tenant's transaction ids and trace payloads. queen.message_traces carries no
-- tenant column; ownership resolves through the trace's partition:
-- queen.log_partitions.queue_id IS the queen.queues id now (the only engine
-- this broker ships; the rows-engine ownership branch went with the rows
-- tables, and the log_queues hop went with the queue-identity merge).
-- A trace whose partition row is gone (queen.message_traces carries no
-- partition FK, so a queue delete leaves the traces behind) is unattributable
-- and belongs to nobody — it is filtered out rather than shown to everybody.
-- p_tenant is LAST with a DEFAULT so old positional call sites keep working.
CREATE OR REPLACE FUNCTION queen.get_traces_by_name_v1(
    p_trace_name TEXT,
    p_limit INTEGER DEFAULT 100,
    p_offset INTEGER DEFAULT 0,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001'
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_traces JSONB;
    v_total INTEGER;
BEGIN
    -- Get total count first (joined through message_traces so the count sees the
    -- same tenant-scoped set as the page below; trace_names cascade-delete with
    -- their trace, so the join drops nothing else).
    SELECT COUNT(*) INTO v_total
    FROM queen.message_trace_names mtn
    JOIN queen.message_traces mt ON mtn.trace_id = mt.id
    WHERE mtn.trace_name = p_trace_name
      AND EXISTS (SELECT 1 FROM queen.log_partitions lp
                  JOIN queen.queues q ON q.id = lp.queue_id
                  WHERE lp.id = mt.partition_id AND q.tenant_id = p_tenant);

    -- Get traces with pagination.
    -- LIMIT/OFFSET must be applied to the ROWS, inside the subquery, BEFORE
    -- jsonb_agg — an aggregate without GROUP BY yields exactly one row, so a
    -- trailing LIMIT/OFFSET pages that single row: offset 0 ignored the limit
    -- and aggregated everything, offset>0 returned no row at all (NULL body).
    -- Same shape get_available_trace_names_v1 below already uses.
    -- queue_name/partition_name resolve through queen.log_partitions ->
    -- queen.queues directly: partition.queue_id IS the queues id (queue
    -- identity merge), so the display join is one hop.
    -- message_payload is NULL BY CONSTRUCTION and the key is kept so the JSON
    -- shape is unchanged: log payloads live inside segment blobs, opaque to SQL,
    -- exactly as this column already read under the log engine.
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'id', s.id,
            'transaction_id', s.transaction_id,
            'partition_id', s.partition_id,
            'event_type', s.event_type,
            'data', s.data,
            'consumer_group', s.consumer_group,
            'worker_id', s.worker_id,
            'created_at', to_char(s.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'queue_name', s.queue_name,
            'partition_name', s.partition_name,
            'message_payload', s.message_payload,
            'trace_names', COALESCE(
                (SELECT jsonb_agg(mtn2.trace_name ORDER BY mtn2.trace_name)
                 FROM queen.message_trace_names mtn2 WHERE mtn2.trace_id = s.id),
                '[]'::jsonb
            )
        ) ORDER BY s.created_at ASC
    ), '[]'::jsonb) INTO v_traces
    FROM (
        SELECT mt.id, mt.transaction_id, mt.partition_id, mt.event_type, mt.data,
               mt.consumer_group, mt.worker_id, mt.created_at,
               q.name        AS queue_name,
               lp.name       AS partition_name,
               NULL::JSONB   AS message_payload
        FROM queen.message_trace_names mtn
        JOIN queen.message_traces mt ON mtn.trace_id = mt.id
        LEFT JOIN queen.log_partitions lp ON lp.id = mt.partition_id
        LEFT JOIN queen.queues q ON q.id = lp.queue_id
        WHERE mtn.trace_name = p_trace_name
          AND EXISTS (SELECT 1 FROM queen.log_partitions lp2
                      JOIN queen.queues q2 ON q2.id = lp2.queue_id
                      WHERE lp2.id = mt.partition_id AND q2.tenant_id = p_tenant)
        ORDER BY mt.created_at ASC
        LIMIT p_limit OFFSET p_offset
    ) s;

    RETURN jsonb_build_object(
        'traces', v_traces,
        'total', v_total,
        'pagination', jsonb_build_object(
            'limit', p_limit,
            'offset', p_offset
        )
    );
END;
$$;

-- ============================================================================
-- queen.get_available_trace_names_v1: Get distinct trace names with counts
-- ============================================================================
-- Track B (§5): p_tenant scopes the enumeration — a name is only counted, ranked
-- and returned when at least ONE of its traces belongs to p_tenant, and its
-- counts/last_seen are derived from that tenant's traces alone (the name set is
-- caller-chosen text, so a global list leaks both the names and the activity
-- times of every other tenant). Same log_partitions -> queues ownership
-- resolution as get_traces_by_name_v1 above.
CREATE OR REPLACE FUNCTION queen.get_available_trace_names_v1(
    p_limit INTEGER DEFAULT 50,
    p_offset INTEGER DEFAULT 0,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001'
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_names JSONB;
    v_total INTEGER;
BEGIN
    -- Get total count
    SELECT COUNT(DISTINCT mtn.trace_name) INTO v_total
    FROM queen.message_trace_names mtn
    JOIN queen.message_traces mt ON mtn.trace_id = mt.id
    WHERE EXISTS (SELECT 1 FROM queen.log_partitions lp
                  JOIN queen.queues q ON q.id = lp.queue_id
                  WHERE lp.id = mt.partition_id AND q.tenant_id = p_tenant);

    -- Get trace names with stats
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'trace_name', trace_name,
            'trace_count', trace_count,
            'message_count', message_count,
            'last_seen', last_seen
        ) ORDER BY last_seen DESC
    ), '[]'::jsonb) INTO v_names
    FROM (
        SELECT 
            mtn.trace_name,
            COUNT(DISTINCT mtn.trace_id) as trace_count,
            COUNT(DISTINCT mt.transaction_id) as message_count,
            to_char(MAX(mt.created_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') as last_seen
        FROM queen.message_trace_names mtn
        JOIN queen.message_traces mt ON mtn.trace_id = mt.id
        WHERE EXISTS (SELECT 1 FROM queen.log_partitions lp
                      JOIN queen.queues q ON q.id = lp.queue_id
                      WHERE lp.id = mt.partition_id AND q.tenant_id = p_tenant)
        GROUP BY mtn.trace_name
        ORDER BY MAX(mt.created_at) DESC
        LIMIT p_limit OFFSET p_offset
    ) subq;
    
    RETURN jsonb_build_object(
        'trace_names', v_names,
        'total', v_total,
        'pagination', jsonb_build_object(
            'limit', p_limit,
            'offset', p_offset
        )
    );
END;
$$;

-- Grant execute permissions (record_trace_v1 is defined AND granted in
-- 010_log_admin).
GRANT EXECUTE ON FUNCTION queen.get_message_traces_v1(UUID, TEXT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_traces_by_name_v1(TEXT, INTEGER, INTEGER, UUID) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_available_trace_names_v1(INTEGER, INTEGER, UUID) TO PUBLIC;

