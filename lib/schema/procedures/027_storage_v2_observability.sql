-- ============================================================================
-- Storage v2 (segments) observability.
-- ============================================================================
-- The v1 observability surface (006 has_pending, 008 consumer-group lag,
-- 009/010 message listing, 013 stats reconciler, 018 prometheus) reads
-- queen.messages / queen.partition_consumers directly, so storage='segments'
-- queues would show ZEROS everywhere. This file:
--   * adds queen.seg_stats_v1 — the v2-native per-queue stats entry point;
--   * redefines (versioning pattern: this file applies AFTER the originals
--     in lexical order and supersedes them at boot) the v1 procedures that
--     routes/webapp dispatch, making them dual-mode:
--       - queen.get_consumer_groups_v4   (routes/consumer_groups.cpp)
--       - queen.get_prometheus_metrics_v1 (routes/prometheus.cpp)
--       - queen.has_pending_messages      (pg_qpubsub wrappers)
--       - queen.get_queue_messages_v1     (routes/status.cpp)
--       - queen.list_messages_v1          (routes/messages.cpp)
--       - queen.get_dlq_messages_v1       (routes/dlq.cpp)
-- Guarantee: for storage='rows' queues every redefined function returns
-- results byte-identical to the original — the v2 branches are guarded on
-- queen.queues.storage = 'segments' and only APPEND rows / take a separate
-- early-return path.
--
-- v2 semantics used throughout:
--   * pending frames for (partition, group) = SUM over queen.seg_segments s with
--     s.seq >= cursor.next_seq of (s.msg_count - next_off if s.seq=next_seq).
--   * "worst group" per partition = the minimum (next_seq, next_off) cursor;
--     a partition with no consumer rows has its whole backlog pending.
--   * time lag = age of the oldest segment that still has unconsumed frames.
--   * blobs are OPAQUE to SQL: payloads (and transaction-id text — queen.seg_dedup
--     stores only hashtextextended(txn)) can only be recovered by the broker
--     decompressing frames. Message-level listings therefore carry
--     "payloadAvailable": false and "transactionId": null; the route layer
--     must fetch/decode the segment blob if it wants either.
-- ============================================================================

-- ============================================================================
-- queen.seg_stats_v1: per-queue stats for the segments engine.
-- Returns a JSONB array, one element per q2 queue (optionally filtered):
--   { queue, segments, totalFrames, pendingFrames, totalBytes,
--     oldestPendingAt, partitions: [ { partition, segments, totalFrames,
--       pendingFrames, totalBytes, lastSeq, retentionSeq, cursorSeq,
--       cursorOff, oldestPendingAt } ] }
-- pendingFrames is computed against the WORST (most behind) consumer group.
-- Reports whatever lives in q2 regardless of the queen.queues.storage flag
-- (it is the engine-native view; the flag only routes traffic).
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.seg_stats_v1(p_queue TEXT DEFAULT NULL)
RETURNS JSONB
LANGUAGE sql STABLE
AS $$
WITH parts AS (
    SELECT q.name AS queue, p.id AS partition_id, p.name AS partition,
           p.last_seq, p.retention_seq
    FROM queen.seg_partitions p
    JOIN queen.seg_queues q ON q.id = p.queue_id
    WHERE p_queue IS NULL OR q.name = p_queue
),
-- Worst (most behind) group cursor per partition; NULL when never popped ->
-- whole backlog pending (COALESCE(next_seq, 0) below includes every segment).
worst AS (
    SELECT DISTINCT ON (c.partition_id)
           c.partition_id, c.next_seq, c.next_off
    FROM queen.partition_consumers c
    JOIN parts pa ON pa.partition_id = c.partition_id
    ORDER BY c.partition_id, c.next_seq, c.next_off
),
-- ONE pre-aggregated pass over queen.seg_segments (GROUP BY partition_id) instead
-- of per-partition LATERAL SUMs: under partition-count/size skew the planner
-- turned the LATERALs into re-scans measured at 20-45s; a single hash join
-- + aggregate stays flat.
seg_agg AS (
    SELECT s.partition_id,
           COUNT(*)::bigint                               AS segments,
           COALESCE(SUM(s.msg_count), 0)::bigint          AS total_frames,
           COALESCE(SUM(octet_length(s.blob)), 0)::bigint AS total_bytes,
           COALESCE(SUM(CASE WHEN s.seq >= COALESCE(w.next_seq, 0)
                    THEN s.msg_count
                         - CASE WHEN s.seq = w.next_seq THEN w.next_off ELSE 0 END
                    ELSE 0 END), 0)::bigint               AS pending,
           MIN(s.created_at) FILTER (WHERE s.seq >= COALESCE(w.next_seq, 0)
                AND s.msg_count >
                    CASE WHEN s.seq = w.next_seq THEN w.next_off ELSE 0 END)
                                                          AS oldest_pending_at
    FROM queen.seg_segments s
    JOIN parts pa ON pa.partition_id = s.partition_id
    LEFT JOIN worst w ON w.partition_id = s.partition_id
    GROUP BY s.partition_id
),
per_part AS (
    SELECT pa.queue, pa.partition, pa.partition_id, pa.last_seq, pa.retention_seq,
           w.next_seq AS cursor_seq, w.next_off AS cursor_off,
           COALESCE(sa.segments, 0)      AS segments,
           COALESCE(sa.total_frames, 0)  AS total_frames,
           COALESCE(sa.total_bytes, 0)   AS total_bytes,
           COALESCE(sa.pending, 0)       AS pending_frames,
           sa.oldest_pending_at
    FROM parts pa
    LEFT JOIN worst w ON w.partition_id = pa.partition_id
    LEFT JOIN seg_agg sa ON sa.partition_id = pa.partition_id
)
SELECT COALESCE(jsonb_agg(jsonb_build_object(
    'queue',          queue,
    'segments',       segments,
    'totalFrames',    total_frames,
    'pendingFrames',  pending_frames,
    'totalBytes',     total_bytes,
    'oldestPendingAt', CASE WHEN oldest IS NOT NULL
        THEN to_char(oldest, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') ELSE NULL END,
    'partitions',     partitions
) ORDER BY queue), '[]'::jsonb)
FROM (
    SELECT queue,
           SUM(segments)        AS segments,
           SUM(total_frames)    AS total_frames,
           SUM(pending_frames)  AS pending_frames,
           SUM(total_bytes)     AS total_bytes,
           MIN(oldest_pending_at) AS oldest,
           jsonb_agg(jsonb_build_object(
               'partition',      partition,
               'segments',       segments,
               'totalFrames',    total_frames,
               'pendingFrames',  pending_frames,
               'totalBytes',     total_bytes,
               'lastSeq',        last_seq,
               'retentionSeq',   retention_seq,
               'cursorSeq',      cursor_seq,
               'cursorOff',      cursor_off,
               'oldestPendingAt', CASE WHEN oldest_pending_at IS NOT NULL
                   THEN to_char(oldest_pending_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') ELSE NULL END
           ) ORDER BY partition) AS partitions
    FROM per_part
    GROUP BY queue
) x;
$$;

GRANT EXECUTE ON FUNCTION queen.seg_stats_v1(TEXT) TO PUBLIC;

-- ============================================================================
-- queen.get_consumer_groups_v4 — dual-mode redefinition.
-- The rows-engine block is copied verbatim from 008 (with an added
-- storage <> 'segments' guard that is a no-op for rows queues, since v2
-- queues never get queen.partition_consumers rows); segments-engine groups
-- are computed from queen.partition_consumers/queen.seg_segments and APPENDED to the array.
-- v2 entries carry the same keys plus "storage":"segments"; unlike the rows
-- branch, totalLag IS populated (pending frames beyond the cursor are a
-- cheap PK-range SUM in v2). maxTimeLag = age of the oldest unconsumed
-- segment. Subscription metadata does not exist in v2 -> nulls.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.get_consumer_groups_v4()
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_result JSONB;
    v_v2 JSONB;
BEGIN
    -- ------------------------------------------------------------- rows (v1)
    WITH cg_metadata AS (
        -- Pre-resolve metadata per (consumer_group, queue_name) pair
        -- This runs the complex OR join over ~26 distinct pairs instead of 128k+ rows
        SELECT
            sub.consumer_group,
            sub.queue_name,
            MAX(cgm.subscription_mode) as subscription_mode,
            MAX(cgm.subscription_timestamp) as subscription_timestamp,
            MAX(cgm.created_at) as subscription_created_at
        FROM (
            SELECT DISTINCT pc.consumer_group, q.name as queue_name, q.namespace, q.task
            FROM queen.partition_consumers pc
            JOIN queen.partitions p ON p.id = pc.partition_id
            JOIN queen.queues q ON q.id = p.queue_id
            WHERE q.storage IS DISTINCT FROM 'segments'
        ) sub
        LEFT JOIN queen.consumer_groups_metadata cgm
            ON cgm.consumer_group = sub.consumer_group
            AND (
                cgm.queue_name = sub.queue_name
                OR (cgm.queue_name = '' AND cgm.namespace = sub.namespace)
                OR (cgm.queue_name = '' AND cgm.task = sub.task)
            )
        GROUP BY sub.consumer_group, sub.queue_name
    ),
    consumer_data AS (
        SELECT
            pc.consumer_group,
            q.name as queue_name,
            pc.last_consumed_at,
            -- Detect if partition has unconsumed messages (fast comparison)
            CASE
                WHEN pl.last_message_id IS NULL THEN FALSE  -- Empty partition
                WHEN pc.last_consumed_created_at IS NOT NULL THEN
                    (pl.last_message_created_at, pl.last_message_id) >
                    (pc.last_consumed_created_at, pc.last_consumed_id)
                WHEN cgm.subscription_timestamp IS NOT NULL THEN
                    -- "new" mode: only has lag if latest message is after subscription time
                    (pl.last_message_created_at, pl.last_message_id) >
                    (cgm.subscription_timestamp, '00000000-0000-0000-0000-000000000000'::uuid)
                ELSE TRUE  -- Never consumed, no subscription = all messages
            END as has_lag,
            -- Approximate time lag: age of newest unconsumed message (lower bound)
            CASE
                WHEN pl.last_message_id IS NULL THEN NULL  -- Empty partition
                WHEN pc.last_consumed_created_at IS NOT NULL THEN
                    CASE WHEN (pl.last_message_created_at, pl.last_message_id) >
                              (pc.last_consumed_created_at, pc.last_consumed_id)
                         THEN EXTRACT(EPOCH FROM (NOW() - pl.last_message_created_at))::integer
                         ELSE 0 END
                WHEN cgm.subscription_timestamp IS NOT NULL THEN
                    CASE WHEN (pl.last_message_created_at, pl.last_message_id) >
                              (cgm.subscription_timestamp, '00000000-0000-0000-0000-000000000000'::uuid)
                         THEN EXTRACT(EPOCH FROM (NOW() - pl.last_message_created_at))::integer
                         ELSE 0 END
                ELSE
                    EXTRACT(EPOCH FROM (NOW() - pl.last_message_created_at))::integer
            END as time_lag_seconds,
            cgm.subscription_mode,
            cgm.subscription_timestamp,
            cgm.subscription_created_at
        FROM queen.partition_consumers pc
        JOIN queen.partitions p ON p.id = pc.partition_id
        JOIN queen.queues q ON q.id = p.queue_id
        LEFT JOIN queen.partition_lookup pl ON pl.partition_id = pc.partition_id
        -- Simple equality join to pre-resolved metadata (~26 rows)
        LEFT JOIN cg_metadata cgm
            ON cgm.consumer_group = pc.consumer_group
            AND cgm.queue_name = q.name
        WHERE q.storage IS DISTINCT FROM 'segments'
    ),
    aggregated AS (
        SELECT
            consumer_group,
            queue_name,
            COUNT(*) as member_count,
            SUM(CASE WHEN has_lag THEN 1 ELSE 0 END) as partitions_with_lag,
            COALESCE(MAX(time_lag_seconds), 0) as max_time_lag,
            MAX(subscription_mode) as subscription_mode,
            MAX(subscription_timestamp) as subscription_timestamp,
            MAX(subscription_created_at) as subscription_created_at,
            CASE
                WHEN MAX(time_lag_seconds) > 300 THEN 'Lagging'
                WHEN MAX(CASE WHEN last_consumed_at IS NOT NULL THEN 1 ELSE 0 END) = 1 THEN 'Stable'
                ELSE 'Dead'
            END as state
        FROM consumer_data
        GROUP BY consumer_group, queue_name
    )
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'name', consumer_group,
            'topics', jsonb_build_array(queue_name),
            'queueName', queue_name,
            'members', member_count,
            'partitionsWithLag', partitions_with_lag,
            'totalLag', NULL,  -- Cannot compute without scanning messages
            'maxTimeLag', max_time_lag,
            'state', state,
            'subscriptionMode', subscription_mode,
            'subscriptionTimestamp', CASE WHEN subscription_timestamp IS NOT NULL
                THEN to_char(subscription_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
            'subscriptionCreatedAt', CASE WHEN subscription_created_at IS NOT NULL
                THEN to_char(subscription_created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END
        ) ORDER BY consumer_group, queue_name
    ), '[]'::jsonb) INTO v_result
    FROM aggregated;

    -- ------------------------------------------------------- segments (v2)
    -- Pending frames per (partition, group) via ONE pre-aggregated
    -- segments x consumers join (GROUP BY partition_id, consumer_group)
    -- instead of a per-consumer-row LATERAL SUM — the LATERAL plan
    -- degenerates under skew (see queen.seg_stats_v1).
    WITH pend AS (
        SELECT c.partition_id, c.consumer_group,
               COALESCE(SUM(CASE WHEN s.seq >= c.next_seq
                        THEN s.msg_count
                             - CASE WHEN s.seq = c.next_seq THEN c.next_off ELSE 0 END
                        ELSE 0 END), 0)::bigint AS pending,
               MIN(s.created_at) FILTER (WHERE s.seq >= c.next_seq
                    AND s.msg_count >
                        CASE WHEN s.seq = c.next_seq THEN c.next_off ELSE 0 END)
                    AS oldest_unconsumed_at
        FROM queen.partition_consumers c
        LEFT JOIN queen.seg_segments s ON s.partition_id = c.partition_id
        GROUP BY c.partition_id, c.consumer_group
    ),
    v2_data AS (
        SELECT c.consumer_group,
               q.name AS queue_name,
               c.total_consumed,
               COALESCE(l.pending, 0) AS pending,
               l.oldest_unconsumed_at
        FROM queen.partition_consumers c
        JOIN queen.seg_partitions p ON p.id = c.partition_id
        JOIN queen.seg_queues q ON q.id = p.queue_id
        -- Strict guard: only queues currently routed to the segments engine.
        JOIN queen.queues quv ON quv.name = q.name AND quv.storage = 'segments'
        LEFT JOIN pend l ON l.partition_id = c.partition_id
                        AND l.consumer_group = c.consumer_group
    ),
    v2_aggregated AS (
        SELECT consumer_group,
               queue_name,
               COUNT(*) AS member_count,
               SUM(CASE WHEN pending > 0 THEN 1 ELSE 0 END) AS partitions_with_lag,
               SUM(pending) AS total_lag,
               COALESCE(MAX(EXTRACT(EPOCH FROM (NOW() - oldest_unconsumed_at))::integer), 0) AS max_time_lag,
               CASE
                   WHEN COALESCE(MAX(EXTRACT(EPOCH FROM (NOW() - oldest_unconsumed_at))::integer), 0) > 300 THEN 'Lagging'
                   WHEN MAX(total_consumed) > 0 THEN 'Stable'
                   ELSE 'Dead'
               END AS state
        FROM v2_data
        GROUP BY consumer_group, queue_name
    )
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'name', consumer_group,
            'topics', jsonb_build_array(queue_name),
            'queueName', queue_name,
            'members', member_count,
            'partitionsWithLag', partitions_with_lag,
            'totalLag', total_lag,
            'maxTimeLag', max_time_lag,
            'state', state,
            'storage', 'segments',
            'subscriptionMode', NULL,
            'subscriptionTimestamp', NULL,
            'subscriptionCreatedAt', NULL
        ) ORDER BY consumer_group, queue_name
    ), '[]'::jsonb) INTO v_v2
    FROM v2_aggregated;

    RETURN v_result || v_v2;
END;
$$;

GRANT EXECUTE ON FUNCTION queen.get_consumer_groups_v4() TO PUBLIC;

-- ============================================================================
-- queen.get_prometheus_metrics_v1 — dual-mode redefinition of 018.
-- Changes vs the original (rows-only deployments render byte-identical
-- Prometheus text):
--   * 'dlq' totals now include queen.seg_dlq, and 'per_queue' unions the v2 DLQ
--     counts into the SAME metric names (queen_dlq_depth[_by_queue]) — for
--     a rows queue the numbers are unchanged, v2 queues simply appear.
--   * new 'queue_depth' section with per-queue message counts for BOTH
--     engines under the same field names:
--       [{queue, storage, total_messages, pending_messages}]
--     rows engine  -> reconciler-maintained queen.stats (stat_type='queue');
--     segments     -> SUM(msg_count) of live segments, pending = frames
--                     beyond the worst group cursor.
--     NOTE for the route layer: prometheus.cpp's format_db_metrics ignores
--     unknown keys, so this section is invisible until a formatter is added
--     (suggested family: queen_queue_depth_by_queue{queue,storage}).
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.get_prometheus_metrics_v1()
RETURNS JSONB
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_cutoff TIMESTAMPTZ := NOW() - INTERVAL '5 minutes';
BEGIN
    RETURN jsonb_build_object(
        -- Cluster lifetime totals (singleton, identical across replicas).
        'system_totals', (
            SELECT jsonb_build_object(
                'pushRequests',  total_push_requests,
                'popRequests',   total_pop_requests,
                'ackRequests',   total_ack_requests,
                'transactions',  total_transactions,
                'pushMessages',  total_push_messages,
                'popMessages',   total_pop_messages,
                'ackMessages',   total_ack_messages,
                'ackSuccess',    total_ack_success,
                'ackFailed',     total_ack_failed,
                'dbErrors',      total_db_errors,
                'dlqCount',      total_dlq
            )
            FROM queen.worker_metrics_summary
            WHERE id = 1
        ),

        -- Per-queue: most recent bucket within the last 5 minutes.
        -- Cluster-wide aggregate (already SUM-ed across workers in
        -- queue_lag_metrics on insert). Engine-agnostic: v2 queues appear
        -- here too because the broker records buckets at the HTTP layer.
        'per_queue_lag', COALESCE((
            SELECT jsonb_agg(row_to_json(q))
            FROM (
                SELECT DISTINCT ON (queue_name)
                    queue_name           AS queue,
                    pop_count,
                    avg_lag_ms,
                    max_lag_ms,
                    push_request_count,
                    push_message_count,
                    pop_empty_count,
                    ack_request_count,
                    ack_success_count,
                    ack_failed_count,
                    transaction_count,
                    COALESCE(parked_count, 0) AS parked_count,
                    EXTRACT(EPOCH FROM (NOW() - bucket_time))::bigint
                                              AS bucket_age_seconds
                FROM queen.queue_lag_metrics
                WHERE bucket_time >= v_cutoff
                ORDER BY queue_name, bucket_time DESC
            ) q
        ), '[]'::jsonb),

        -- Per-worker: most recent bucket within the last 5 minutes, keyed
        -- by (hostname, worker_id, pid). Includes both event-loop health
        -- gauges and last-minute throughput counters.
        'per_worker', COALESCE((
            SELECT jsonb_agg(row_to_json(w))
            FROM (
                SELECT DISTINCT ON (hostname, worker_id, pid)
                    hostname,
                    worker_id,
                    pid,
                    EXTRACT(EPOCH FROM (NOW() - bucket_time))::bigint
                                                AS bucket_age_seconds,
                    avg_event_loop_lag_ms,
                    max_event_loop_lag_ms,
                    avg_free_slots,
                    min_free_slots,
                    db_connections,
                    avg_job_queue_size,
                    max_job_queue_size,
                    backoff_size,
                    jobs_done,
                    push_request_count,
                    pop_request_count,
                    ack_request_count,
                    transaction_count,
                    push_message_count,
                    pop_message_count,
                    ack_message_count,
                    ack_success_count,
                    ack_failed_count,
                    lag_count,
                    avg_lag_ms,
                    max_lag_ms,
                    db_error_count,
                    dlq_count
                FROM queen.worker_metrics
                WHERE bucket_time >= v_cutoff
                ORDER BY hostname, worker_id, pid, bucket_time DESC
            ) w
        ), '[]'::jsonb),

        -- DLQ depth: cluster total + per-queue breakdown, BOTH engines.
        'dlq', jsonb_build_object(
            'total', (SELECT COUNT(*) FROM queen.dead_letter_queue)
                   + (SELECT COUNT(*) FROM queen.seg_dlq),
            'per_queue', COALESCE((
                SELECT jsonb_agg(row_to_json(d))
                FROM (
                    SELECT queue, SUM(count)::bigint AS count
                    FROM (
                        SELECT q.name AS queue, COUNT(*)::bigint AS count
                        FROM queen.dead_letter_queue dlq
                        JOIN queen.partitions p ON p.id = dlq.partition_id
                        JOIN queen.queues q ON q.id = p.queue_id
                        GROUP BY q.name
                        UNION ALL
                        SELECT q2q.name, COUNT(*)::bigint
                        FROM queen.seg_dlq d2
                        JOIN queen.seg_partitions p2 ON p2.id = d2.partition_id
                        JOIN queen.seg_queues q2q ON q2q.id = p2.queue_id
                        GROUP BY q2q.name
                    ) u
                    GROUP BY queue
                    ORDER BY queue
                ) d
            ), '[]'::jsonb)
        ),

        -- Queue depth (message counts), both engines, same field names.
        'queue_depth', COALESCE((
            SELECT jsonb_agg(row_to_json(qd))
            FROM (
                -- rows engine: reconciler-maintained queue-level stats.
                SELECT q.name::text AS queue,
                       'rows'::text AS storage,
                       SUM(s.total_messages)::bigint   AS total_messages,
                       SUM(s.pending_messages)::bigint AS pending_messages
                FROM queen.stats s
                JOIN queen.queues q ON q.id = s.queue_id
                WHERE s.stat_type = 'queue'
                  AND q.storage IS DISTINCT FROM 'segments'
                GROUP BY q.name
                UNION ALL
                -- segments engine: SUM(msg_count) over live segments;
                -- pending = frames beyond the worst group cursor. ONE
                -- pre-aggregated pass over queen.seg_segments (GROUP BY
                -- partition_id) instead of per-partition LATERAL SUMs — the
                -- LATERAL plan degenerates under skew (see queen.seg_stats_v1).
                SELECT q2q.name::text,
                       'segments'::text,
                       SUM(COALESCE(sa.total_frames, 0))::bigint,
                       SUM(COALESCE(sa.pending, 0))::bigint
                FROM queen.seg_partitions p2
                JOIN queen.seg_queues q2q ON q2q.id = p2.queue_id
                JOIN queen.queues quv ON quv.name = q2q.name
                                     AND quv.storage = 'segments'
                LEFT JOIN (
                    SELECT s2.partition_id,
                           SUM(s2.msg_count) AS total_frames,
                           SUM(CASE WHEN s2.seq >= COALESCE(w.next_seq, 0)
                                THEN s2.msg_count
                                     - CASE WHEN s2.seq = w.next_seq
                                            THEN w.next_off ELSE 0 END
                                ELSE 0 END) AS pending
                    FROM queen.seg_segments s2
                    LEFT JOIN (
                        SELECT DISTINCT ON (c.partition_id)
                               c.partition_id, c.next_seq, c.next_off
                        FROM queen.partition_consumers c
                        ORDER BY c.partition_id, c.next_seq, c.next_off
                    ) w ON w.partition_id = s2.partition_id
                    GROUP BY s2.partition_id
                ) sa ON sa.partition_id = p2.id
                GROUP BY q2q.name
                ORDER BY queue
            ) qd
        ), '[]'::jsonb)
    );
END;
$$;

GRANT EXECUTE ON FUNCTION queen.get_prometheus_metrics_v1() TO PUBLIC;

-- ============================================================================
-- queen.has_pending_messages — dual-mode redefinition of 006.
-- segments queues answer via the q2 cursor check: coarse
-- last_seq >= next_seq (may report true when retention already ate the
-- backlog or the head frame offset is exhausted mid-normalization — the
-- subsequent pop simply returns empty). Mirrors the v1 lease semantics:
-- a live lease on (partition, group) means "someone is on it" -> not pending.
-- The rows branch is the 006 body verbatim.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.has_pending_messages(
    p_queue_name TEXT,
    p_partition_name TEXT DEFAULT NULL,
    p_consumer_group TEXT DEFAULT '__QUEUE_MODE__'
) RETURNS BOOLEAN
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_has_messages BOOLEAN;
    v_specific_partition_id UUID;
    v_storage TEXT;
BEGIN
    IF p_partition_name = '' THEN
        p_partition_name := NULL;
    END IF;

    SELECT storage INTO v_storage FROM queen.queues WHERE name = p_queue_name;

    IF v_storage = 'segments' THEN
        RETURN EXISTS (
            SELECT 1
            FROM queen.seg_partitions p
            JOIN queen.seg_queues q ON q.id = p.queue_id AND q.name = p_queue_name
            LEFT JOIN queen.partition_consumers c
                ON c.partition_id = p.id
               AND c.consumer_group = p_consumer_group
            WHERE (p_partition_name IS NULL OR p.name = p_partition_name)
              AND (c.lease_expires_at IS NULL OR c.lease_expires_at <= NOW())
              AND ((c.partition_id IS NULL AND p.last_seq >= 1)
                   OR p.last_seq >= c.next_seq)
        );
    END IF;

    IF p_partition_name IS NOT NULL THEN
        SELECT p.id INTO v_specific_partition_id
        FROM queen.partitions p
        JOIN queen.queues q ON p.queue_id = q.id
        WHERE q.name = p_queue_name AND p.name = p_partition_name;

        IF v_specific_partition_id IS NULL THEN
            RETURN FALSE;
        END IF;
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM queen.partition_lookup pl
        LEFT JOIN queen.partition_consumers pc
            ON pc.partition_id = pl.partition_id
            AND pc.consumer_group = p_consumer_group
        WHERE pl.queue_name = p_queue_name
          AND (v_specific_partition_id IS NULL OR pl.partition_id = v_specific_partition_id)
          AND (pc.lease_expires_at IS NULL OR pc.lease_expires_at <= NOW())
          AND (pc.last_consumed_created_at IS NULL
               OR pl.last_message_created_at > pc.last_consumed_created_at
               OR (pl.last_message_created_at = pc.last_consumed_created_at
                   AND pl.last_message_id > pc.last_consumed_id))
        LIMIT 1
    ) INTO v_has_messages;

    RETURN v_has_messages;
END;
$$;

GRANT EXECUTE ON FUNCTION queen.has_pending_messages(text, text, text) TO PUBLIC;

-- ============================================================================
-- queen.get_queue_messages_v1 — dual-mode redefinition of 009.
-- segments queues take an early-return path listing SEGMENT-level rows
-- (newest first, LIMIT/OFFSET applied to segments):
--   { type:'segment', seq, msgCount, sizeBytes, partitionId, queuePath,
--     queue, partition, status, createdAt, payloadAvailable:false,
--     messages:[{id, frameIdx, transactionId:null, txnHash,
--                payloadAvailable:false}] }
-- ROUTE-LAYER CONTRACT: frames are opaque to SQL (zstd lives in the C++
-- broker). The per-message list comes from queen.seg_dedup, so it exists only
-- within the dedup window and only for queues with dedup enabled; the
-- transaction-id TEXT is not recoverable (dedup stores its hash). To show
-- payloads or transaction ids the route must fetch the segment blob
-- (queen.seg_segments PK lookup) and decode frames broker-side.
-- Status vs the __QUEUE_MODE__ cursor: seq < next_seq -> completed;
-- otherwise processing when the lease is live, else pending (a partially
-- consumed head segment reports 'pending' for its remainder).
-- The rows branch is the 009 body verbatim.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.get_queue_messages_v1(
    p_queue_name TEXT,
    p_limit INTEGER DEFAULT 50,
    p_offset INTEGER DEFAULT 0
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM queen.queues
               WHERE name = p_queue_name AND storage = 'segments') THEN
        RETURN (
            SELECT COALESCE(jsonb_agg(seg.obj ORDER BY seg.created_at DESC, seg.seq DESC), '[]'::jsonb)
            FROM (
                SELECT s.created_at, s.seq,
                    jsonb_build_object(
                        'type', 'segment',
                        'seq', s.seq,
                        'msgCount', s.msg_count,
                        'sizeBytes', octet_length(s.blob),
                        'partitionId', p.id,
                        'queuePath', q.name || '/' || p.name,
                        'queue', q.name,
                        'partition', p.name,
                        'status', CASE
                            WHEN c.next_seq IS NOT NULL AND s.seq < c.next_seq THEN 'completed'
                            WHEN c.lease_expires_at IS NOT NULL AND c.lease_expires_at > NOW() THEN 'processing'
                            ELSE 'pending'
                        END,
                        'createdAt', to_char(s.created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                        'payloadAvailable', false,
                        'messages', COALESCE((
                            SELECT jsonb_agg(jsonb_build_object(
                                'id', d.message_id,
                                'frameIdx', d.frame_idx,
                                'transactionId', NULL,
                                'txnHash', d.txn_hash::text,
                                'payloadAvailable', false
                            ) ORDER BY d.frame_idx)
                            -- PK (partition_id, txn_hash) bounds this to one
                            -- partition; fine for dashboard-sized listings.
                            FROM queen.seg_dedup d
                            WHERE d.partition_id = s.partition_id AND d.seq = s.seq
                        ), '[]'::jsonb)
                    ) AS obj
                FROM queen.seg_segments s
                JOIN queen.seg_partitions p ON p.id = s.partition_id
                JOIN queen.seg_queues q ON q.id = p.queue_id
                LEFT JOIN queen.partition_consumers c
                    ON c.partition_id = p.id AND c.consumer_group = '__QUEUE_MODE__'
                WHERE q.name = p_queue_name
                ORDER BY s.created_at DESC, s.seq DESC
                LIMIT p_limit OFFSET p_offset
            ) seg
        );
    END IF;

    RETURN (
        SELECT COALESCE(jsonb_agg(
            jsonb_build_object(
                'id', m.id,
                'transactionId', m.transaction_id,
                'partitionId', m.partition_id,
                'queuePath', q.name || '/' || p.name,
                'queue', q.name,
                'partition', p.name,
                -- NOTE: Must use exact timestamp comparison (not DATE_TRUNC) to preserve microsecond precision
                'status', CASE
                    WHEN dlq.message_id IS NOT NULL THEN 'dead_letter'
                    WHEN pc.last_consumed_created_at IS NOT NULL AND
                        (m.created_at, m.id) <= (pc.last_consumed_created_at, pc.last_consumed_id)
                    THEN 'completed'
                    WHEN pc.lease_expires_at IS NOT NULL AND pc.lease_expires_at > NOW() THEN 'processing'
                    ELSE 'pending'
                END,
                'createdAt', to_char(m.created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
            ) ORDER BY m.created_at DESC, m.id DESC
        ), '[]'::jsonb)
        FROM queen.messages m
        JOIN queen.partitions p ON p.id = m.partition_id
        JOIN queen.queues q ON q.id = p.queue_id
        LEFT JOIN queen.partition_consumers pc ON pc.partition_id = p.id AND pc.consumer_group = '__QUEUE_MODE__'
        LEFT JOIN queen.dead_letter_queue dlq ON dlq.message_id = m.id
        WHERE q.name = p_queue_name
        LIMIT p_limit OFFSET p_offset
    );
END;
$$;

GRANT EXECUTE ON FUNCTION queen.get_queue_messages_v1(TEXT, INTEGER, INTEGER) TO PUBLIC;

-- ============================================================================
-- queen.list_messages_v1 — dual-mode redefinition of 010.
-- The rows pipeline is the 010 body verbatim; message-shaped entries for
-- segments queues (from queen.seg_dedup, the only per-message metadata SQL has)
-- are then APPENDED to result.messages:
--   { id, transactionId:null, txnHash, partitionId, queuePath, queue,
--     partition, namespace, task, status, queueStatus, busStatus,
--     payloadAvailable:false, segment:{seq, frameIdx}, queuePriority,
--     traceId:null, producerSub:null, createdAt, leaseExpiresAt }
-- ROUTE-LAYER CONTRACT: same as get_queue_messages_v1 — entries exist only
-- within the dedup window / for dedup-enabled queues; payload and
-- transaction-id text require broker-side frame decode. The v2 entries are
-- appended AFTER the rows page and independently capped at the same limit
-- (a combined page can therefore hold up to 2x limit when both engines
-- match the filter — segment-queue filters normally isolate one engine).
-- 'mode' detection stays rows-only ('none' for a pure segments queue).
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.list_messages_v1(p_filters JSONB DEFAULT '{}'::jsonb)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_from_ts TIMESTAMPTZ;
    v_to_ts TIMESTAMPTZ;
    v_queue TEXT;
    v_partition TEXT;
    v_namespace TEXT;
    v_task TEXT;
    v_status TEXT;
    v_limit INTEGER;
    v_offset INTEGER;
    v_result JSONB;
    v_has_queue_mode BOOLEAN;
    v_total_bus_groups INTEGER;
    v_v2 JSONB;
BEGIN
    -- Parse filters
    -- from: inclusive of the exact timestamp
    v_from_ts := COALESCE((p_filters->>'from')::timestamptz, NOW() - INTERVAL '1 hour');
    -- to: inclusive of the entire minute (truncate to minute, add 1 minute, use < comparison)
    v_to_ts := date_trunc('minute', COALESCE((p_filters->>'to')::timestamptz, NOW())) + INTERVAL '1 minute';
    v_queue := p_filters->>'queue';
    v_partition := p_filters->>'partition';
    v_namespace := p_filters->>'namespace';
    v_task := p_filters->>'task';
    v_status := p_filters->>'status';
    v_limit := COALESCE((p_filters->>'limit')::integer, 200);
    v_offset := COALESCE((p_filters->>'offset')::integer, 0);

    -- Detect mode for the filtered messages
    SELECT
        bool_or(pc.consumer_group = '__QUEUE_MODE__'),
        COUNT(DISTINCT pc.consumer_group) FILTER (WHERE pc.consumer_group != '__QUEUE_MODE__')
    INTO v_has_queue_mode, v_total_bus_groups
    FROM queen.messages m
    JOIN queen.partitions p ON p.id = m.partition_id
    JOIN queen.queues q ON q.id = p.queue_id
    LEFT JOIN queen.partition_consumers pc ON pc.partition_id = p.id
    WHERE m.created_at >= v_from_ts AND m.created_at < v_to_ts
      AND (v_queue IS NULL OR q.name = v_queue);

    -- Get messages with computed status, then filter by status if specified
    WITH message_data AS (
        SELECT
            m.id,
            m.transaction_id,
            m.partition_id,
            m.created_at,
            m.trace_id,
            m.producer_sub,
            q.name as queue_name,
            p.name as partition_name,
            q.namespace,
            q.task,
            q.priority as queue_priority,
            pc_queue.lease_expires_at,
            -- Queue mode status
            -- NOTE: Must use exact timestamp comparison (not DATE_TRUNC) to preserve microsecond precision
            CASE
                WHEN dlq.message_id IS NOT NULL THEN 'dead_letter'
                WHEN pc_queue.last_consumed_created_at IS NOT NULL AND
                    (m.created_at, m.id) <= (pc_queue.last_consumed_created_at, pc_queue.last_consumed_id)
                THEN 'completed'
                WHEN pc_queue.lease_expires_at IS NOT NULL AND pc_queue.lease_expires_at > NOW() THEN 'processing'
                ELSE 'pending'
            END as queue_status,
            -- Bus mode: count of consumer groups that consumed this message
            -- NOTE: Must use exact timestamp comparison (not DATE_TRUNC) to preserve microsecond precision
            (
                SELECT COUNT(*)::integer
                FROM queen.partition_consumers pc
                WHERE pc.partition_id = m.partition_id
                  AND pc.consumer_group != '__QUEUE_MODE__'
                  AND pc.last_consumed_created_at IS NOT NULL
                  AND (m.created_at, m.id) <= (pc.last_consumed_created_at, pc.last_consumed_id)
            ) as consumed_by_groups_count,
            -- Per-partition mode detection
            (pc_queue.consumer_group IS NOT NULL) as partition_has_queue_mode,
            (
                SELECT COUNT(*)::integer
                FROM queen.partition_consumers pc
                WHERE pc.partition_id = m.partition_id
                  AND pc.consumer_group != '__QUEUE_MODE__'
            ) as partition_bus_groups_count
        FROM queen.messages m
        JOIN queen.partitions p ON p.id = m.partition_id
        JOIN queen.queues q ON q.id = p.queue_id
        LEFT JOIN queen.partition_consumers pc_queue ON pc_queue.partition_id = p.id AND pc_queue.consumer_group = '__QUEUE_MODE__'
        LEFT JOIN queen.dead_letter_queue dlq ON dlq.message_id = m.id
        WHERE m.created_at >= v_from_ts AND m.created_at < v_to_ts
          AND (v_queue IS NULL OR q.name = v_queue)
          AND (v_partition IS NULL OR p.name = v_partition)
          AND (v_namespace IS NULL OR q.namespace = v_namespace)
          AND (v_task IS NULL OR q.task = v_task)
        ORDER BY m.created_at DESC, m.id DESC
    ),
    -- Compute final status and filter
    filtered_messages AS (
        SELECT
            *,
            CASE
                WHEN queue_status = 'dead_letter' THEN 'dead_letter'
                WHEN NOT partition_has_queue_mode AND partition_bus_groups_count > 0 THEN
                    CASE WHEN consumed_by_groups_count = partition_bus_groups_count THEN 'completed' ELSE 'pending' END
                ELSE queue_status
            END as final_status
        FROM message_data
    )
    SELECT jsonb_build_object(
        'messages', COALESCE(jsonb_agg(
            jsonb_build_object(
                'id', id,
                'transactionId', transaction_id,
                'partitionId', partition_id,
                'queuePath', queue_name || '/' || partition_name,
                'queue', queue_name,
                'partition', partition_name,
                'namespace', namespace,
                'task', task,
                'status', final_status,
                'queueStatus', queue_status,
                'busStatus', jsonb_build_object(
                    'consumedBy', consumed_by_groups_count,
                    'totalGroups', partition_bus_groups_count
                ),
                'traceId', trace_id,
                'producerSub', producer_sub,
                'queuePriority', queue_priority,
                'createdAt', to_char(created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                'leaseExpiresAt', CASE WHEN lease_expires_at IS NOT NULL
                    THEN to_char(lease_expires_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END
            )
        ), '[]'::jsonb),
        'mode', jsonb_build_object(
            'hasQueueMode', COALESCE(v_has_queue_mode, false),
            'busGroupsCount', COALESCE(v_total_bus_groups, 0),
            'type', CASE
                WHEN NOT COALESCE(v_has_queue_mode, false) AND COALESCE(v_total_bus_groups, 0) > 0 THEN 'bus'
                WHEN COALESCE(v_has_queue_mode, false) AND COALESCE(v_total_bus_groups, 0) = 0 THEN 'queue'
                WHEN COALESCE(v_has_queue_mode, false) AND COALESCE(v_total_bus_groups, 0) > 0 THEN 'hybrid'
                ELSE 'none'
            END
        )
    ) INTO v_result
    FROM (
        SELECT * FROM filtered_messages
        WHERE (v_status IS NULL OR final_status = v_status)
        LIMIT v_limit OFFSET v_offset
    ) sub;

    -- --------------------------------------------- segments (v2) entries
    -- No index on queen.seg_dedup.created_at: the scan is bounded by the dedup
    -- window's steady-state size (O(rate x window)) — acceptable for a
    -- dashboard listing.
    SELECT COALESCE(jsonb_agg(v2.obj ORDER BY v2.created_at DESC, v2.message_id DESC), '[]'::jsonb)
    INTO v_v2
    FROM (
        SELECT vd.created_at, vd.message_id,
            jsonb_build_object(
                'id', vd.message_id,
                'transactionId', NULL,
                'txnHash', vd.txn_hash::text,
                'partitionId', vd.partition_id,
                'queuePath', vd.queue_name || '/' || vd.partition_name,
                'queue', vd.queue_name,
                'partition', vd.partition_name,
                'namespace', vd.namespace,
                'task', vd.task,
                'status', vd.final_status,
                'queueStatus', vd.final_status,
                'busStatus', jsonb_build_object(
                    'consumedBy', vd.consumed_by_groups,
                    'totalGroups', vd.total_bus_groups
                ),
                'traceId', NULL,
                'producerSub', NULL,
                'queuePriority', vd.queue_priority,
                'payloadAvailable', false,
                'segment', jsonb_build_object('seq', vd.seq, 'frameIdx', vd.frame_idx),
                'createdAt', to_char(vd.created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                'leaseExpiresAt', CASE WHEN vd.lease_expires_at IS NOT NULL
                    THEN to_char(vd.lease_expires_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END
            ) AS obj
        FROM (
            SELECT d.message_id, d.txn_hash, d.seq, d.frame_idx, d.created_at,
                   p.id AS partition_id, p.name AS partition_name,
                   q.name AS queue_name,
                   quv.namespace, quv.task, quv.priority AS queue_priority,
                   c.lease_expires_at,
                   -- Frame consumed iff (seq, frame_idx) < cursor (next_seq, next_off).
                   CASE
                       WHEN EXISTS (SELECT 1 FROM queen.seg_dlq dl
                                    WHERE dl.partition_id = d.partition_id
                                      AND dl.seq = d.seq AND dl.frame_idx = d.frame_idx)
                           THEN 'dead_letter'
                       WHEN c.next_seq IS NOT NULL
                            AND (d.seq, d.frame_idx) < (c.next_seq, c.next_off)
                           THEN 'completed'
                       WHEN c.lease_expires_at IS NOT NULL AND c.lease_expires_at > NOW()
                           THEN 'processing'
                       ELSE 'pending'
                   END AS final_status,
                   (SELECT COUNT(*)::integer FROM queen.partition_consumers c2
                    WHERE c2.partition_id = d.partition_id
                      AND c2.consumer_group <> '__QUEUE_MODE__'
                      AND (d.seq, d.frame_idx) < (c2.next_seq, c2.next_off)) AS consumed_by_groups,
                   (SELECT COUNT(*)::integer FROM queen.partition_consumers c2
                    WHERE c2.partition_id = d.partition_id
                      AND c2.consumer_group <> '__QUEUE_MODE__') AS total_bus_groups
            FROM queen.seg_dedup d
            JOIN queen.seg_partitions p ON p.id = d.partition_id
            JOIN queen.seg_queues q ON q.id = p.queue_id
            -- Strict guard: only queues currently routed to the segments engine.
            JOIN queen.queues quv ON quv.name = q.name AND quv.storage = 'segments'
            LEFT JOIN queen.partition_consumers c
                ON c.partition_id = p.id AND c.consumer_group = '__QUEUE_MODE__'
            WHERE d.created_at >= v_from_ts AND d.created_at < v_to_ts
              AND (v_queue IS NULL OR q.name = v_queue)
              AND (v_partition IS NULL OR p.name = v_partition)
              AND (v_namespace IS NULL OR quv.namespace = v_namespace)
              AND (v_task IS NULL OR quv.task = v_task)
        ) vd
        WHERE (v_status IS NULL OR vd.final_status = v_status)
        ORDER BY vd.created_at DESC, vd.message_id DESC
        LIMIT v_limit
    ) v2;

    -- Rows-only deployments: v_v2 = '[]' and v_result is returned untouched.
    IF jsonb_array_length(v_v2) > 0 THEN
        v_result := jsonb_set(v_result, '{messages}',
                              (v_result->'messages') || v_v2);
    END IF;

    RETURN v_result;
END;
$$;

GRANT EXECUTE ON FUNCTION queen.list_messages_v1(JSONB) TO PUBLIC;

-- ============================================================================
-- queen.get_dlq_messages_v1 — dual-mode redefinition of 010.
-- The rows branch is the 010 body verbatim (same joins, filters, keys and
-- outer LIMIT/OFFSET placement — byte-identical output for rows queues);
-- queen.seg_dlq rows are UNIONed in with the SAME keys so /api/v1/dlq serves both
-- engines:
--   * payload snapshots live in queen.seg_dlq itself (data), no messages join;
--   * retryCount: v2 tracks attempts on the consumer row, not per DLQ entry —
--     a frame is dead-lettered exactly when attempts exceed the queue's
--     retry_limit, so the queue's retry_limit IS the count it failed with;
--   * partitionId: the q2 partition uuid (echoed by pop/ack wire calls);
--   * createdAt: v2 blobs are opaque to SQL, the original enqueue time is
--     not recoverable per frame -> failed_at (the DLQ event time);
--   * producerSub: not tracked in v2 -> null.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.get_dlq_messages_v1(p_filters JSONB DEFAULT '{}'::jsonb)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_queue TEXT;
    v_consumer_group TEXT;
    v_limit INTEGER;
    v_offset INTEGER;
BEGIN
    v_queue := p_filters->>'queue';
    v_consumer_group := p_filters->>'consumerGroup';
    v_limit := COALESCE((p_filters->>'limit')::integer, 100);
    v_offset := COALESCE((p_filters->>'offset')::integer, 0);

    RETURN (
        SELECT jsonb_build_object(
            'messages', COALESCE(jsonb_agg(t.msg ORDER BY t.failed_at DESC), '[]'::jsonb),
            'pagination', jsonb_build_object(
                'limit', v_limit,
                'offset', v_offset
            )
        )
        FROM (
            -- rows engine (010 verbatim keys)
            SELECT dlq.failed_at,
                   jsonb_build_object(
                       'id', m.id,
                       'transactionId', m.transaction_id,
                       'partitionId', m.partition_id,
                       'queue', q.name,
                       'partition', p.name,
                       'consumerGroup', dlq.consumer_group,
                       'errorMessage', dlq.error_message,
                       'retryCount', dlq.retry_count,
                       'data', m.payload,
                       'producerSub', m.producer_sub,
                       'createdAt', to_char(m.created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                       'failedAt', to_char(dlq.failed_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                   ) AS msg
            FROM queen.dead_letter_queue dlq
            JOIN queen.messages m ON m.id = dlq.message_id
            JOIN queen.partitions p ON p.id = dlq.partition_id
            JOIN queen.queues q ON q.id = p.queue_id
            WHERE (v_queue IS NULL OR q.name = v_queue)
              AND (v_consumer_group IS NULL OR dlq.consumer_group = v_consumer_group)
            UNION ALL
            -- segments engine (queen.seg_dlq payload snapshots), same keys
            SELECT d2.failed_at,
                   jsonb_build_object(
                       'id', d2.id,
                       'transactionId', d2.transaction_id,
                       'partitionId', d2.partition_id,
                       'queue', q2q.name,
                       'partition', p2.name,
                       'consumerGroup', d2.consumer_group,
                       'errorMessage', d2.error,
                       'retryCount', COALESCE(qq.retry_limit, 3),
                       'data', d2.payload,
                       'producerSub', NULL,
                       'createdAt', to_char(d2.failed_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                       'failedAt', to_char(d2.failed_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                   ) AS msg
            FROM queen.seg_dlq d2
            JOIN queen.seg_partitions p2 ON p2.id = d2.partition_id
            JOIN queen.seg_queues q2q ON q2q.id = p2.queue_id
            LEFT JOIN queen.queues qq ON qq.name = q2q.name
            WHERE (v_queue IS NULL OR q2q.name = v_queue)
              AND (v_consumer_group IS NULL OR d2.consumer_group = v_consumer_group)
        ) t
        LIMIT v_limit OFFSET v_offset
    );
END;
$$;

GRANT EXECUTE ON FUNCTION queen.get_dlq_messages_v1(JSONB) TO PUBLIC;
