-- ============================================================================
-- Traces Stored Procedures
-- ============================================================================
-- Async stored procedures for message tracing operations
-- ============================================================================

-- ============================================================================
-- queen.record_trace_v1: Record a trace event for a message
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.record_trace_v1(p_data JSONB)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_message_id UUID;
    v_trace_id UUID;
    v_transaction_id TEXT;
    v_partition_id UUID;
    v_consumer_group TEXT;
    v_event_type TEXT;
    v_trace_data JSONB;
    v_worker_id TEXT;
    v_trace_names TEXT[];
    v_name TEXT;
BEGIN
    -- Extract fields from input
    v_transaction_id := p_data->>'transactionId';
    v_partition_id := (p_data->>'partitionId')::uuid;
    v_consumer_group := COALESCE(p_data->>'consumerGroup', '__QUEUE_MODE__');
    v_event_type := COALESCE(p_data->>'eventType', 'info');
    v_trace_data := COALESCE(p_data->'data', '{}'::jsonb);
    v_worker_id := p_data->>'workerId';
    
    -- Parse traceNames array
    IF p_data->'traceNames' IS NOT NULL AND jsonb_typeof(p_data->'traceNames') = 'array' THEN
        SELECT array_agg(elem::text) INTO v_trace_names
        FROM jsonb_array_elements_text(p_data->'traceNames') elem;
    END IF;
    
    -- Get message_id from transaction_id + partition_id
    SELECT id INTO v_message_id
    FROM queen.messages 
    WHERE transaction_id = v_transaction_id AND partition_id = v_partition_id
    LIMIT 1;
    
    IF v_message_id IS NULL THEN
        RETURN jsonb_build_object(
            'success', false,
            'error', 'Message not found'
        );
    END IF;
    
    -- Insert main trace record
    INSERT INTO queen.message_traces 
        (message_id, partition_id, transaction_id, consumer_group, event_type, data, worker_id)
    VALUES 
        (v_message_id, v_partition_id, v_transaction_id, v_consumer_group, v_event_type, v_trace_data, v_worker_id)
    RETURNING id INTO v_trace_id;
    
    -- Insert trace names if any
    IF v_trace_names IS NOT NULL AND array_length(v_trace_names, 1) > 0 THEN
        FOREACH v_name IN ARRAY v_trace_names LOOP
            INSERT INTO queen.message_trace_names (trace_id, trace_name)
            VALUES (v_trace_id, v_name)
            ON CONFLICT (trace_id, trace_name) DO NOTHING;
        END LOOP;
    END IF;
    
    RETURN jsonb_build_object(
        'success', true,
        'traceId', v_trace_id
    );
END;
$$;

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
            'created_at', to_char(mt.created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
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
-- tenant column; ownership resolves through the trace's partition, which lives
-- in queen.log_partitions (log engine) OR queen.partitions (rows engine), so the
-- predicate must cover BOTH to stay correct on either storage. A trace whose
-- partition row is gone (the FKs are dropped in 047, so a queue delete leaves
-- the traces behind) is unattributable and belongs to nobody — it is filtered
-- out rather than shown to everybody. Added LAST with a DEFAULT so old
-- positional call sites keep working; DROP the 3-arg form so the scoped one is
-- unambiguous on re-apply.
DROP FUNCTION IF EXISTS queen.get_traces_by_name_v1(TEXT, INTEGER, INTEGER);
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
      AND (EXISTS (SELECT 1 FROM queen.log_partitions lp
                   JOIN queen.log_queues lq ON lq.id = lp.queue_id
                   WHERE lp.id = mt.partition_id AND lq.tenant_id = p_tenant)
        OR EXISTS (SELECT 1 FROM queen.partitions rp
                   JOIN queen.queues rq ON rq.id = rp.queue_id
                   WHERE rp.id = mt.partition_id AND rq.tenant_id = p_tenant));

    -- Get traces with pagination
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'id', mt.id,
            'transaction_id', mt.transaction_id,
            'partition_id', mt.partition_id,
            'event_type', mt.event_type,
            'data', mt.data,
            'consumer_group', mt.consumer_group,
            'worker_id', mt.worker_id,
            'created_at', to_char(mt.created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'queue_name', q.name,
            'partition_name', p.name,
            'message_payload', m.payload,
            'trace_names', COALESCE(
                (SELECT jsonb_agg(mtn2.trace_name ORDER BY mtn2.trace_name) 
                 FROM queen.message_trace_names mtn2 WHERE mtn2.trace_id = mt.id),
                '[]'::jsonb
            )
        ) ORDER BY mt.created_at ASC
    ), '[]'::jsonb) INTO v_traces
    FROM queen.message_trace_names mtn
    JOIN queen.message_traces mt ON mtn.trace_id = mt.id
    LEFT JOIN queen.messages m ON mt.message_id = m.id
    LEFT JOIN queen.partitions p ON mt.partition_id = p.id
    LEFT JOIN queen.queues q ON p.queue_id = q.id
    WHERE mtn.trace_name = p_trace_name
      AND (EXISTS (SELECT 1 FROM queen.log_partitions lp
                   JOIN queen.log_queues lq ON lq.id = lp.queue_id
                   WHERE lp.id = mt.partition_id AND lq.tenant_id = p_tenant)
        OR EXISTS (SELECT 1 FROM queen.partitions rp
                   JOIN queen.queues rq ON rq.id = rp.queue_id
                   WHERE rp.id = mt.partition_id AND rq.tenant_id = p_tenant))
    LIMIT p_limit OFFSET p_offset;
    
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
-- times of every other tenant). Same dual-engine ownership resolution as
-- get_traces_by_name_v1 above. DROP the 2-arg form so the scoped one is
-- unambiguous on re-apply.
DROP FUNCTION IF EXISTS queen.get_available_trace_names_v1(INTEGER, INTEGER);
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
                  JOIN queen.log_queues lq ON lq.id = lp.queue_id
                  WHERE lp.id = mt.partition_id AND lq.tenant_id = p_tenant)
       OR EXISTS (SELECT 1 FROM queen.partitions rp
                  JOIN queen.queues rq ON rq.id = rp.queue_id
                  WHERE rp.id = mt.partition_id AND rq.tenant_id = p_tenant);

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
            to_char(MAX(mt.created_at), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') as last_seen
        FROM queen.message_trace_names mtn
        JOIN queen.message_traces mt ON mtn.trace_id = mt.id
        WHERE EXISTS (SELECT 1 FROM queen.log_partitions lp
                      JOIN queen.log_queues lq ON lq.id = lp.queue_id
                      WHERE lp.id = mt.partition_id AND lq.tenant_id = p_tenant)
           OR EXISTS (SELECT 1 FROM queen.partitions rp
                      JOIN queen.queues rq ON rq.id = rp.queue_id
                      WHERE rp.id = mt.partition_id AND rq.tenant_id = p_tenant)
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

-- Grant execute permissions
GRANT EXECUTE ON FUNCTION queen.record_trace_v1(JSONB) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_message_traces_v1(UUID, TEXT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_traces_by_name_v1(TEXT, INTEGER, INTEGER, UUID) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_available_trace_names_v1(INTEGER, INTEGER, UUID) TO PUBLIC;

