-- ============================================================================
-- Storage v2 (segments) — trace recording made engine-agnostic.
-- ============================================================================
-- 011's queen.record_trace_v1 resolves the trace's message_id from
-- queen.messages (rows engine) and BAILS with {"success":false,"error":
-- "Message not found"} when no such row exists. Segment queues never write
-- queen.messages, so tracing a segment message would always fail.
--
-- This file (applied AFTER 011 in lexical order, the same versioning pattern
-- 027 uses) redefines queen.record_trace_v1 to be engine-agnostic:
--   * rows engine: unchanged — message_id still resolves from queen.messages,
--     so a traced rows message keeps its FK-linked trace;
--   * segments engine: record the trace with message_id = NULL. A segment
--     message has NO queen.messages row, and message_traces.message_id is a FK
--     into queen.messages — so the seg_dedup message_id is NOT a valid FK
--     target and must not be used here (it violates message_traces_message_id_fkey).
--     message_id is a NULLABLE FK, so NULL is allowed.
-- queen.message_traces is keyed by (partition_id, transaction_id), so
-- queen.get_message_traces_v1 reads back the trace regardless of message_id.
--
-- Decoupling from the rows tables: queen.message_traces FKs message_id ->
-- queen.messages and partition_id -> queen.partitions. Segment queues use
-- queen.seg_partitions UUIDs (independent of queen.partitions) and have no
-- queen.messages rows, so both FKs would reject a segment trace. We drop them
-- here — the SAME treatment the plan gives queen.dead_letter_queue's FK
-- (preserve the coordination table, decouple it from the going-away rows
-- message store). Traces stay keyed by (partition_id, transaction_id).
-- ============================================================================
ALTER TABLE queen.message_traces DROP CONSTRAINT IF EXISTS message_traces_partition_id_fkey;
ALTER TABLE queen.message_traces DROP CONSTRAINT IF EXISTS message_traces_message_id_fkey;

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

    -- Resolve message_id from queen.messages (rows engine). For a segment
    -- message this returns NULL — and it MUST stay NULL: seg messages have no
    -- queen.messages row, and message_traces.message_id FKs into queen.messages,
    -- so any non-NULL value that isn't a real messages.id would be rejected.
    SELECT id INTO v_message_id
    FROM queen.messages
    WHERE transaction_id = v_transaction_id AND partition_id = v_partition_id
    LIMIT 1;

    -- Insert main trace record. message_id is NULL for segment messages — the
    -- trace is still keyed by (partition_id, transaction_id) and readable via
    -- queen.get_message_traces_v1.
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

GRANT EXECUTE ON FUNCTION queen.record_trace_v1(JSONB) TO PUBLIC;
