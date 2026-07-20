-- ============================================================================
-- Segments-native stats refresh.
-- ============================================================================
-- The v1 stats reconciler (013_stats.sql) computes queen.stats 'partition' rows
-- from queen.messages and rolls them up with aggregate_queue_stats_v2 (which
-- GROUPs BY partition_id). Under the segments engine queen.messages is empty and
-- segment partitions live in queen.seg_partitions (NOT queen.partitions), so that
-- path leaves queen.stats empty -> every stats-backed dashboard reader
-- (get_system_overview_v3, get_status_queues_v2, get_status_v3, aggregate_*)
-- returns zeros.
--
-- This file adds queen.seg_refresh_all_stats_v1(): it writes queen.stats 'queue'
-- rows DIRECTLY from the segments tables (the same worst-cursor pending math as
-- queen.seg_stats_v1), then reuses the EXISTING queue->namespace/task/system
-- rollups (which read 'queue' rows and never touch partition_id). The background
-- stats task (server/src/stats.rs) calls this once per cadence under an advisory
-- lock, exactly like the C++ StatsService.
--
-- Queue identity: queen.stats.queue_id/stat_key key off queen.queues.id (the
-- rows-engine queue row that every segments queue also has, joined by name), so
-- all existing readers find the rows unchanged. partition_id stays NULL (seg
-- partition uuids are not in queen.partitions, and the queue rollup here does not
-- need them).
-- ============================================================================

CREATE OR REPLACE FUNCTION queen.seg_refresh_all_stats_v1()
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_now      TIMESTAMPTZ := NOW();
    v_queues   INTEGER := 0;
    v_ns       JSONB;
    v_task     JSONB;
    v_sys      JSONB;
    v_seg_parts BIGINT;
BEGIN
    -- ------------------------------------------------------------------ queues
    -- One pre-aggregated pass: worst (most-behind) cursor per partition, then
    -- per-partition segment totals, then rolled up to the queue. Mirrors the
    -- pending math of queen.seg_stats_v1 (027) so the numbers match the engine-
    -- native view the queue LIST/DETAIL already show.
    WITH worst AS (
        SELECT DISTINCT ON (c.partition_id)
               c.partition_id, c.next_seq, c.next_off
        FROM queen.partition_consumers c
        ORDER BY c.partition_id, c.next_seq, c.next_off
    ),
    seg_agg AS (
        SELECT s.partition_id,
               COALESCE(SUM(s.msg_count), 0)::bigint AS total_frames,
               COALESCE(SUM(CASE WHEN s.seq >= COALESCE(w.next_seq, 0)
                        THEN s.msg_count
                             - CASE WHEN s.seq = w.next_seq THEN w.next_off ELSE 0 END
                        ELSE 0 END), 0)::bigint AS pending,
               MIN(s.created_at) FILTER (WHERE s.seq >= COALESCE(w.next_seq, 0)
                    AND s.msg_count > CASE WHEN s.seq = w.next_seq THEN w.next_off ELSE 0 END)
                    AS oldest_pending_at,
               MAX(s.created_at) AS newest_at
        FROM queen.seg_segments s
        LEFT JOIN worst w ON w.partition_id = s.partition_id
        GROUP BY s.partition_id
    ),
    -- in-flight (leased-but-unacked) frames per partition, worst-cursor group.
    lease_agg AS (
        SELECT c.partition_id,
               SUM(GREATEST(0, COALESCE(c.batch_size, 0) - COALESCE(c.acked_count, 0)))::bigint AS processing
        FROM queen.partition_consumers c
        WHERE c.lease_expires_at IS NOT NULL AND c.lease_expires_at > v_now
        GROUP BY c.partition_id
    ),
    -- dead-letter frames per queue (segments DLQ).
    dlq_agg AS (
        SELECT q.name AS queue_name, COUNT(*)::bigint AS dlq
        FROM queen.seg_dlq d
        JOIN queen.seg_partitions p ON p.id = d.partition_id
        JOIN queen.seg_queues q     ON q.id = p.queue_id
        GROUP BY q.name
    ),
    per_queue AS (
        SELECT sq.name AS queue_name,
               COUNT(DISTINCT sp.id)::bigint                    AS child_count,
               COALESCE(SUM(sa.total_frames), 0)::bigint        AS total_messages,
               COALESCE(SUM(sa.pending), 0)::bigint             AS pending_messages,
               COALESCE(SUM(la.processing), 0)::bigint          AS processing_messages,
               MIN(sa.oldest_pending_at)                        AS oldest_pending_at,
               MAX(sa.newest_at)                                AS newest_message_at
        FROM queen.seg_queues sq
        JOIN queen.seg_partitions sp ON sp.queue_id = sq.id
        LEFT JOIN seg_agg   sa ON sa.partition_id = sp.id
        LEFT JOIN lease_agg la ON la.partition_id = sp.id
        GROUP BY sq.name
    )
    INSERT INTO queen.stats (
        stat_type, stat_key, queue_id, partition_id, consumer_group,
        child_count, total_messages, pending_messages, processing_messages,
        completed_messages, dead_letter_messages,
        oldest_pending_at, newest_message_at,
        avg_lag_seconds, max_lag_seconds, avg_offset_lag, max_offset_lag,
        ingested_per_second, processed_per_second,
        last_computed_at
    )
    SELECT
        'queue',
        q.id::text,
        q.id,
        NULL,
        '__QUEUE_MODE__',
        COALESCE(pq.child_count, 0),
        COALESCE(pq.total_messages, 0),
        COALESCE(pq.pending_messages, 0),
        LEAST(COALESCE(pq.processing_messages, 0), COALESCE(pq.pending_messages, 0)),
        GREATEST(0, COALESCE(pq.total_messages, 0)
                    - COALESCE(pq.pending_messages, 0)
                    - COALESCE(da.dlq, 0)),
        COALESCE(da.dlq, 0),
        pq.oldest_pending_at,
        pq.newest_message_at,
        COALESCE(EXTRACT(EPOCH FROM (v_now - pq.oldest_pending_at))::integer, 0),
        COALESCE(EXTRACT(EPOCH FROM (v_now - pq.oldest_pending_at))::integer, 0),
        COALESCE(pq.pending_messages, 0)::integer,
        COALESCE(pq.pending_messages, 0)::integer,
        0, 0,
        v_now
    FROM queen.queues q
    LEFT JOIN per_queue pq ON pq.queue_name = q.name
    LEFT JOIN dlq_agg   da ON da.queue_name = q.name
    WHERE q.storage = 'segments'
    ON CONFLICT (stat_type, stat_key) DO UPDATE SET
        queue_id             = EXCLUDED.queue_id,
        child_count          = EXCLUDED.child_count,
        total_messages       = EXCLUDED.total_messages,
        pending_messages     = EXCLUDED.pending_messages,
        processing_messages  = EXCLUDED.processing_messages,
        completed_messages   = EXCLUDED.completed_messages,
        dead_letter_messages = EXCLUDED.dead_letter_messages,
        oldest_pending_at    = EXCLUDED.oldest_pending_at,
        newest_message_at    = EXCLUDED.newest_message_at,
        avg_lag_seconds      = EXCLUDED.avg_lag_seconds,
        max_lag_seconds      = EXCLUDED.max_lag_seconds,
        avg_offset_lag       = EXCLUDED.avg_offset_lag,
        max_offset_lag       = EXCLUDED.max_offset_lag,
        -- throughput from delta vs the previous snapshot (per-second rates).
        ingested_per_second = CASE
            WHEN queen.stats.prev_snapshot_at IS NOT NULL
                AND v_now > queen.stats.prev_snapshot_at
                AND EXTRACT(EPOCH FROM (v_now - queen.stats.prev_snapshot_at)) > 0
            THEN GREATEST(0, ((EXCLUDED.total_messages - queen.stats.prev_total_messages)::numeric /
                  EXTRACT(EPOCH FROM (v_now - queen.stats.prev_snapshot_at))))
            ELSE 0
        END,
        processed_per_second = CASE
            WHEN queen.stats.prev_snapshot_at IS NOT NULL
                AND v_now > queen.stats.prev_snapshot_at
                AND EXTRACT(EPOCH FROM (v_now - queen.stats.prev_snapshot_at)) > 0
            THEN GREATEST(0, ((EXCLUDED.completed_messages - queen.stats.prev_completed_messages)::numeric /
                  EXTRACT(EPOCH FROM (v_now - queen.stats.prev_snapshot_at))))
            ELSE 0
        END,
        prev_total_messages     = queen.stats.total_messages,
        prev_completed_messages = queen.stats.completed_messages,
        prev_snapshot_at        = queen.stats.last_computed_at,
        last_computed_at        = v_now;

    GET DIAGNOSTICS v_queues = ROW_COUNT;

    -- ---------------------------------------------- namespace / task / system
    -- These read queen.stats 'queue' rows (queue_id join) and never touch
    -- partition_id, so they work unchanged for the segments queue rows above.
    v_ns   := queen.aggregate_namespace_stats_v1();
    v_task := queen.aggregate_task_stats_v1();
    v_sys  := queen.aggregate_system_stats_v2();

    -- aggregate_system_stats_v2 sets child_count = COUNT(queen.partitions) which
    -- is the rows-engine partition count (near-zero for segments). Overwrite it
    -- with the real live segment-partition count so the overview 'partitions'
    -- tile is correct.
    SELECT COUNT(*) INTO v_seg_parts FROM queen.seg_partitions;
    UPDATE queen.stats SET child_count = v_seg_parts
    WHERE stat_type = 'system' AND stat_key = 'global';

    RETURN jsonb_build_object(
        'engine', 'segments',
        'queuesUpdated', v_queues,
        'segPartitions', v_seg_parts,
        'namespaces', v_ns,
        'tasks', v_task,
        'system', v_sys,
        'refreshedAt', to_char(v_now, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
    );
END;
$$;

GRANT EXECUTE ON FUNCTION queen.seg_refresh_all_stats_v1() TO PUBLIC;

-- ============================================================================
-- queen.get_queue_detail_v2 — dual-mode redefinition (segments branch).
-- ============================================================================
-- The 013 body reads queen.stats 'partition' rows via queen.partitions, which
-- are empty for a segments queue -> the QueueDetail page shows an empty
-- partitions[] and zero totals. This file loads AFTER 013 in lexical order and
-- supersedes it: segments queues take an early-return path that computes the
-- SAME output shape (queue.config, partitions[].{messages,stats,cursor,...},
-- totals.messages.*) directly from seg_segments / partition_consumers / seg_dlq,
-- using the worst-cursor pending math of queen.seg_stats_v1. Rows queues fall
-- through to the original 013 body (byte-identical for them).
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.get_queue_detail_v2(p_queue_name TEXT)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_queue_id UUID;
    v_queue_info JSONB;
    v_partitions JSONB;
    v_totals JSONB;
    v_is_seg BOOLEAN;
BEGIN
    SELECT (q.storage = 'segments') INTO v_is_seg
    FROM queen.queues q WHERE q.name = p_queue_name;

    IF v_is_seg IS NULL THEN
        RETURN jsonb_build_object('error', 'Queue not found');
    END IF;

    IF NOT v_is_seg THEN
        -- Non-segments queue: reproduce the 013 rows-engine body verbatim.
        SELECT q.id, jsonb_build_object(
            'queue', jsonb_build_object(
                'id', q.id, 'name', q.name, 'namespace', q.namespace, 'task', q.task,
                'priority', q.priority,
                'createdAt', to_char(q.created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                'config', jsonb_build_object(
                    'leaseTime', q.lease_time, 'retryLimit', q.retry_limit,
                    'retryDelay', q.retry_delay, 'ttl', q.ttl,
                    'maxQueueSize', q.max_queue_size, 'deadLetterQueue', q.dead_letter_queue)))
        INTO v_queue_id, v_queue_info
        FROM queen.queues q WHERE q.name = p_queue_name;

        WITH partition_stats AS (
            SELECT p.id, p.name, p.created_at,
                   COALESCE(MAX(s.total_messages), 0) AS total,
                   COALESCE(MAX(s.pending_messages), 0) AS pending,
                   COALESCE(MAX(s.processing_messages), 0) AS processing,
                   COALESCE(MAX(s.completed_messages), 0) AS completed,
                   0 AS failed,
                   COALESCE(MAX(s.dead_letter_messages), 0) AS dead_letter,
                   MAX(s.oldest_pending_at) AS oldest_message,
                   MAX(s.newest_message_at) AS newest_message,
                   COALESCE(SUM(pc.total_messages_consumed), 0) AS total_consumed,
                   COALESCE(SUM(pc.total_batches_consumed), 0) AS batches_consumed,
                   MAX(pc.last_consumed_at) AS last_activity
            FROM queen.partitions p
            LEFT JOIN queen.stats s ON s.stat_type = 'partition' AND s.partition_id = p.id
            LEFT JOIN queen.partition_consumers pc ON pc.partition_id = p.id
            WHERE p.queue_id = v_queue_id
            GROUP BY p.id, p.name, p.created_at
        )
        SELECT COALESCE(jsonb_agg(jsonb_build_object(
                    'id', id, 'name', name,
                    'createdAt', to_char(created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                    'messages', jsonb_build_object('total', total, 'pending', pending,
                        'processing', processing, 'completed', completed, 'failed', failed, 'deadLetter', dead_letter),
                    'stats', jsonb_build_object('total', total, 'pending', pending,
                        'processing', processing, 'completed', completed, 'failed', failed, 'deadLetter', dead_letter),
                    'cursor', jsonb_build_object('totalConsumed', total_consumed, 'batchesConsumed', batches_consumed),
                    'lastActivity', CASE WHEN last_activity IS NOT NULL THEN to_char(last_activity, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
                    'oldestMessage', CASE WHEN oldest_message IS NOT NULL THEN to_char(oldest_message, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
                    'newestMessage', CASE WHEN newest_message IS NOT NULL THEN to_char(newest_message, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END
                ) ORDER BY name), '[]'::jsonb),
               jsonb_build_object('messages', jsonb_build_object(
                    'total', COALESCE(SUM(total),0), 'pending', COALESCE(SUM(pending),0),
                    'processing', COALESCE(SUM(processing),0), 'completed', COALESCE(SUM(completed),0),
                    'failed', COALESCE(SUM(failed),0), 'deadLetter', COALESCE(SUM(dead_letter),0)))
        INTO v_partitions, v_totals FROM partition_stats;

        RETURN v_queue_info || jsonb_build_object('partitions', v_partitions, 'totals', v_totals);
    END IF;

    -- ---------------------------------------------------------- segments queue
    SELECT jsonb_build_object(
        'queue', jsonb_build_object(
            'id', q.id, 'name', q.name, 'namespace', q.namespace, 'task', q.task,
            'priority', q.priority,
            'createdAt', to_char(q.created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'config', jsonb_build_object(
                'leaseTime', q.lease_time, 'retryLimit', q.retry_limit,
                'retryDelay', q.retry_delay, 'ttl', q.ttl,
                'maxQueueSize', q.max_queue_size, 'deadLetterQueue', q.dead_letter_queue)))
    INTO v_queue_info
    FROM queen.queues q WHERE q.name = p_queue_name;

    WITH parts AS (
        SELECT sp.id, sp.name, sp.created_at
        FROM queen.seg_partitions sp
        JOIN queen.seg_queues sq ON sq.id = sp.queue_id
        WHERE sq.name = p_queue_name
    ),
    worst AS (
        SELECT DISTINCT ON (c.partition_id) c.partition_id, c.next_seq, c.next_off
        FROM queen.partition_consumers c
        JOIN parts pa ON pa.id = c.partition_id
        ORDER BY c.partition_id, c.next_seq, c.next_off
    ),
    seg_agg AS (
        SELECT s.partition_id,
               COALESCE(SUM(s.msg_count),0)::bigint AS total,
               COALESCE(SUM(CASE WHEN s.seq >= COALESCE(w.next_seq,0)
                    THEN s.msg_count - CASE WHEN s.seq = w.next_seq THEN w.next_off ELSE 0 END
                    ELSE 0 END),0)::bigint AS pending,
               MIN(s.created_at) FILTER (WHERE s.seq >= COALESCE(w.next_seq,0)
                    AND s.msg_count > CASE WHEN s.seq = w.next_seq THEN w.next_off ELSE 0 END) AS oldest_message,
               MAX(s.created_at) AS newest_message
        FROM queen.seg_segments s
        JOIN parts pa ON pa.id = s.partition_id
        LEFT JOIN worst w ON w.partition_id = s.partition_id
        GROUP BY s.partition_id
    ),
    cur AS (
        SELECT c.partition_id,
               SUM(GREATEST(0, COALESCE(c.batch_size,0)-COALESCE(c.acked_count,0)))
                   FILTER (WHERE c.lease_expires_at IS NOT NULL AND c.lease_expires_at > NOW())::bigint AS processing,
               COALESCE(SUM(c.total_consumed),0) AS total_consumed,
               COALESCE(SUM(c.total_batches_consumed),0) AS batches_consumed,
               MAX(c.last_consumed_at) AS last_activity
        FROM queen.partition_consumers c
        JOIN parts pa ON pa.id = c.partition_id
        GROUP BY c.partition_id
    ),
    dlq AS (
        SELECT d.partition_id, COUNT(*)::bigint AS dead_letter
        FROM queen.seg_dlq d JOIN parts pa ON pa.id = d.partition_id
        GROUP BY d.partition_id
    ),
    per_part AS (
        SELECT pa.id, pa.name, pa.created_at,
               COALESCE(sa.total,0) AS total,
               COALESCE(sa.pending,0) AS pending,
               LEAST(COALESCE(cu.processing,0), COALESCE(sa.pending,0)) AS processing,
               GREATEST(0, COALESCE(sa.total,0) - COALESCE(sa.pending,0) - COALESCE(dl.dead_letter,0)) AS completed,
               COALESCE(dl.dead_letter,0) AS dead_letter,
               sa.oldest_message, sa.newest_message,
               COALESCE(cu.total_consumed,0) AS total_consumed,
               COALESCE(cu.batches_consumed,0) AS batches_consumed,
               cu.last_activity
        FROM parts pa
        LEFT JOIN seg_agg sa ON sa.partition_id = pa.id
        LEFT JOIN cur cu     ON cu.partition_id = pa.id
        LEFT JOIN dlq dl     ON dl.partition_id = pa.id
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'id', id, 'name', name,
                'createdAt', to_char(created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                'messages', jsonb_build_object('total', total, 'pending', pending,
                    'processing', processing, 'completed', completed, 'failed', 0, 'deadLetter', dead_letter),
                'stats', jsonb_build_object('total', total, 'pending', pending,
                    'processing', processing, 'completed', completed, 'failed', 0, 'deadLetter', dead_letter),
                'cursor', jsonb_build_object('totalConsumed', total_consumed, 'batchesConsumed', batches_consumed),
                'lastActivity', CASE WHEN last_activity IS NOT NULL THEN to_char(last_activity, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
                'oldestMessage', CASE WHEN oldest_message IS NOT NULL THEN to_char(oldest_message, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
                'newestMessage', CASE WHEN newest_message IS NOT NULL THEN to_char(newest_message, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END
            ) ORDER BY name), '[]'::jsonb),
           jsonb_build_object('messages', jsonb_build_object(
                'total', COALESCE(SUM(total),0), 'pending', COALESCE(SUM(pending),0),
                'processing', COALESCE(SUM(processing),0), 'completed', COALESCE(SUM(completed),0),
                'failed', 0, 'deadLetter', COALESCE(SUM(dead_letter),0)))
    INTO v_partitions, v_totals FROM per_part;

    RETURN v_queue_info || jsonb_build_object('partitions', v_partitions, 'totals', v_totals);
END;
$$;

GRANT EXECUTE ON FUNCTION queen.get_queue_detail_v2(TEXT) TO PUBLIC;

-- ============================================================================
-- queen.get_analytics_v1 — segments-native redefinition.
-- ============================================================================
-- The 009 body buckets queen.messages by created_at; that table is dropped once
-- the rows engine is retired (099), so /api/v1/status/analytics 500s. Segment
-- blobs carry a created_at + msg_count, so the message-volume time-series is
-- reproduced by bucketing queen.seg_segments (per-message granularity within a
-- segment is not recoverable, so a segment's frames are attributed to its
-- creation bucket — the same shape/interval logic as the original).
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.get_analytics_v1(p_filters JSONB DEFAULT '{}'::jsonb)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_from TIMESTAMPTZ;
    v_to TIMESTAMPTZ;
    v_interval TEXT;
    v_queue TEXT;
    v_namespace TEXT;
    v_task TEXT;
    v_bw INTERVAL;
    v_result JSONB;
BEGIN
    v_from := COALESCE((p_filters->>'from')::timestamptz, NOW() - INTERVAL '24 hours');
    v_to := COALESCE((p_filters->>'to')::timestamptz, NOW());
    v_interval := COALESCE(p_filters->>'interval', 'hour');
    v_queue := p_filters->>'queue';
    v_namespace := p_filters->>'namespace';
    v_task := p_filters->>'task';
    v_bw := CASE v_interval
        WHEN 'minute' THEN INTERVAL '1 minute'
        WHEN 'hour'   THEN INTERVAL '1 hour'
        WHEN 'day'    THEN INTERVAL '1 day'
        ELSE INTERVAL '1 hour'
    END;

    WITH time_buckets AS (
        SELECT generate_series(date_trunc(v_interval, v_from),
                               date_trunc(v_interval, v_to), v_bw) AS bucket
    ),
    message_buckets AS (
        SELECT date_trunc(v_interval, s.created_at) AS bucket,
               SUM(s.msg_count) AS message_count
        FROM queen.seg_segments s
        JOIN queen.seg_partitions p ON p.id = s.partition_id
        JOIN queen.seg_queues q     ON q.id = p.queue_id
        JOIN queen.queues qq        ON qq.name = q.name
        WHERE s.created_at >= v_from AND s.created_at <= v_to
          AND (v_queue IS NULL OR q.name = v_queue)
          AND (v_namespace IS NULL OR qq.namespace = v_namespace)
          AND (v_task IS NULL OR qq.task = v_task)
        GROUP BY date_trunc(v_interval, s.created_at)
    )
    SELECT jsonb_build_object(
        'dataPoints', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                       'timestamp', to_char(tb.bucket, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                       'messages', COALESCE(mb.message_count, 0)
                   ) ORDER BY tb.bucket)
            FROM time_buckets tb
            LEFT JOIN message_buckets mb ON mb.bucket = tb.bucket
        ), '[]'::jsonb),
        'interval', v_interval,
        'from', to_char(v_from, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'to', to_char(v_to, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
    ) INTO v_result;

    RETURN v_result;
END;
$$;

GRANT EXECUTE ON FUNCTION queen.get_analytics_v1(JSONB) TO PUBLIC;
