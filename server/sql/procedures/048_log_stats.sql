-- ============================================================================
-- Log engine (18-log-engine.md §9) — stats refresh + analytics/status support.
--
--   queen.log_oldest_pending_at_v1   two-probe helper: created_at of the
--                                    segment covering (or next after) an offset
--   queen.log_refresh_all_stats_v1   queen.stats reconciler (former 034
--                                    seg_refresh_all_stats_v1; stats.rs calls
--                                    it once per cadence under advisory lock)
--   queen.get_queue_detail_v2        dual-mode redefinition (log branch)
--   queen.get_analytics_v1           log-native redefinition (time-series)
--   queen.log_queue_message_stats_v1 per-queue (segments, messages) counters
--                                    (former db.rs::seg_queue_message_stats)
--   queen.log_queue_stats_all_v1     broker-wide per-queue counters (former
--                                    db.rs::seg_queue_stats_all)
--
-- THE O(partitions) CONTRACT (§9): pending per (queue, group) is pure
-- watermark arithmetic —
--     pending = SUM over partitions of
--               GREATEST(last_offset - GREATEST(committed, log_start-1), 0)
-- — NO queen.log_segments scans. The old 034 refresh aggregated
-- SUM(msg_count) over every live segment each cycle, i.e. O(segments) per
-- refresh; offsets make every count a subtraction:
--     retained (live) frames = last_offset - log_start + 1
-- which is EXACT, not an estimate, because deletes are prefix-contiguous
-- (041 header: no mid-log gaps, ever). The only log_segments touches left in
-- the refresh are per-partition O(log n) PK probes for two timestamps
-- (oldest pending / newest) that offsets cannot carry.
--
-- Queue identity is unchanged from 034: queen.stats rows key off
-- queen.queues.id (joined by name; storage stays 'segments' — the public
-- contract keeps that value, §11), so every existing queen.stats reader
-- (get_status_v3 / get_status_queues_v2 / get_system_overview_v3 /
-- aggregate_*) finds the rows with no change. partition_id stays NULL (log
-- partition uuids are not in queen.partitions).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- log_oldest_pending_at_v1: created_at of the segment holding offset
-- p_wanted, or of the next existing segment when p_wanted fell into a
-- retention gap. Two O(log n) PK probes — the pop scan's head-probe pattern
-- (§6), NOT a filtered backward scan (a "base<=wanted AND end>=wanted" LIMIT 1
-- would walk the whole dead prefix when no covering segment exists). NULL when
-- nothing exists at/after p_wanted (fully drained partition).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.log_oldest_pending_at_v1(
    p_pid    UUID,
    p_wanted BIGINT
) RETURNS TIMESTAMPTZ
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    v_created TIMESTAMPTZ;
    v_end     BIGINT;
BEGIN
    SELECT s.created_at, s.end_offset INTO v_created, v_end
    FROM queen.log_segments s
    WHERE s.partition_id = p_pid AND s.base_offset <= p_wanted
    ORDER BY s.base_offset DESC LIMIT 1;

    IF FOUND AND v_end >= p_wanted THEN
        RETURN v_created;   -- head segment covers the wanted offset
    END IF;

    -- Retention gap (or nothing yet): first segment past the wanted offset.
    SELECT s.created_at INTO v_created
    FROM queen.log_segments s
    WHERE s.partition_id = p_pid AND s.base_offset > p_wanted
    ORDER BY s.base_offset LIMIT 1;

    RETURN v_created;
END;
$$;

GRANT EXECUTE ON FUNCTION queen.log_oldest_pending_at_v1(UUID, BIGINT) TO PUBLIC;

-- ----------------------------------------------------------------------------
-- log_refresh_all_stats_v1: write queen.stats 'queue' rows directly from the
-- log tables, then reuse the EXISTING queue->namespace/task/system rollups
-- (013 — they read 'queue' rows and never touch partition_id). Same return
-- keys as the 034 function it replaces ('engine' stays 'segments': it is a
-- telemetry label aligned with the queen.queues.storage compat value, and
-- stats.rs prints the summary verbatim).
--
-- Per-partition math (worst = most-behind cursor, MIN(committed) across the
-- partition's groups; no consumer rows => committed -1 => the whole retained
-- range is pending, matching 034's COALESCE(next_seq, 0)):
--   total    = last_offset - log_start + 1          (retained frames, exact)
--   pending  = last_offset - GREATEST(worst, log_start-1)
--   processing = SUM over live leases of batch_end - committed (the leased
--                span (committed, batch_end]; 034 read batch_size-acked_count)
--   completed  = total - pending - dlq (floored at 0, as before)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.log_refresh_all_stats_v1()
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_now       TIMESTAMPTZ := NOW();
    v_queues    INTEGER := 0;
    v_ns        JSONB;
    v_task      JSONB;
    v_sys       JSONB;
    v_log_parts BIGINT;
BEGIN
    -- ------------------------------------------------------------------ queues
    WITH worst AS (
        SELECT c.partition_id, MIN(c.committed) AS committed
        FROM queen.log_consumers c
        GROUP BY c.partition_id
    ),
    part_agg AS (
        SELECT p.id AS partition_id,
               p.queue_id,
               GREATEST(p.last_offset - p.log_start + 1, 0)::bigint AS total_frames,
               GREATEST(p.last_offset
                        - GREATEST(COALESCE(w.committed, -1), p.log_start - 1),
                        0)::bigint AS pending,
               -- Timestamps: two bounded PK probes per partition (see header);
               -- the oldest-pending probe only runs when something IS pending.
               CASE WHEN p.last_offset
                         > GREATEST(COALESCE(w.committed, -1), p.log_start - 1)
                    THEN queen.log_oldest_pending_at_v1(
                             p.id,
                             GREATEST(COALESCE(w.committed, -1), p.log_start - 1) + 1)
               END AS oldest_pending_at,
               tail.created_at AS newest_at
        FROM queen.log_partitions p
        LEFT JOIN worst w ON w.partition_id = p.id
        LEFT JOIN LATERAL (
            SELECT s.created_at
            FROM queen.log_segments s
            WHERE s.partition_id = p.id
            ORDER BY s.base_offset DESC LIMIT 1
        ) tail ON true
    ),
    -- in-flight (leased-but-unacked) frames per partition. batch_end NULL
    -- means no lease; committed can only move up to batch_end, so the span
    -- is never negative — GREATEST guards a torn read anyway.
    lease_agg AS (
        SELECT c.partition_id,
               SUM(GREATEST(0, COALESCE(c.batch_end, c.committed) - c.committed))::bigint
                   AS processing
        FROM queen.log_consumers c
        WHERE c.lease_expires_at IS NOT NULL AND c.lease_expires_at > v_now
        GROUP BY c.partition_id
    ),
    -- dead-letter frames per queue (log DLQ, 044). Track B (§5): keyed by
    -- (name, tenant) so two tenants sharing a queue name don't merge.
    dlq_agg AS (
        SELECT q.name AS queue_name, q.tenant_id, COUNT(*)::bigint AS dlq
        FROM queen.log_dlq d
        JOIN queen.log_partitions p ON p.id = d.partition_id
        JOIN queen.log_queues q     ON q.id = p.queue_id
        GROUP BY q.name, q.tenant_id
    ),
    -- Storage quota (§6.1): retained payload bytes per (queue, tenant) =
    -- SUM(octet_length(blob)) over live segments. octet_length on STORAGE
    -- EXTERNAL blobs reads the length from the TOAST pointer (no chunk
    -- detoast), so this is a bounded heap scan of the LIVE segments at refresh
    -- cadence — the one O(segments) touch this reader makes, for the bytes
    -- gauge only (the §9 no-scan contract governs the hot watermark counters).
    -- Measures the compressed (zstd) on-the-wire bytes AS STORED; TOAST/index/
    -- WAL overhead and the log_txns hash sidecar are excluded.
    seg_bytes AS (
        SELECT lq.name AS queue_name, lq.tenant_id,
               COALESCE(SUM(octet_length(s.blob)), 0)::bigint AS retained_bytes
        FROM queen.log_segments s
        JOIN queen.log_partitions lp ON lp.id = s.partition_id
        JOIN queen.log_queues lq     ON lq.id = lp.queue_id
        GROUP BY lq.name, lq.tenant_id
    ),
    per_queue AS (
        SELECT lq.name AS queue_name, lq.tenant_id,
               COUNT(DISTINCT lp.id)::bigint                    AS child_count,
               COALESCE(SUM(pa.total_frames), 0)::bigint        AS total_messages,
               COALESCE(SUM(pa.pending), 0)::bigint             AS pending_messages,
               COALESCE(SUM(la.processing), 0)::bigint          AS processing_messages,
               MIN(pa.oldest_pending_at)                        AS oldest_pending_at,
               MAX(pa.newest_at)                                AS newest_message_at
        FROM queen.log_queues lq
        JOIN queen.log_partitions lp ON lp.queue_id = lq.id
        LEFT JOIN part_agg  pa ON pa.partition_id = lp.id
        LEFT JOIN lease_agg la ON la.partition_id = lp.id
        GROUP BY lq.name, lq.tenant_id
    )
    INSERT INTO queen.stats (
        stat_type, stat_key, queue_id, partition_id, consumer_group,
        child_count, total_messages, pending_messages, processing_messages,
        completed_messages, dead_letter_messages, retained_bytes,
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
        COALESCE(sb.retained_bytes, 0),
        pq.oldest_pending_at,
        pq.newest_message_at,
        COALESCE(EXTRACT(EPOCH FROM (v_now - pq.oldest_pending_at))::integer, 0),
        COALESCE(EXTRACT(EPOCH FROM (v_now - pq.oldest_pending_at))::integer, 0),
        COALESCE(pq.pending_messages, 0)::integer,
        COALESCE(pq.pending_messages, 0)::integer,
        0, 0,
        v_now
    FROM queen.queues q
    -- Track B (§5): join the log-derived aggregates by (name, tenant) — the
    -- (tenant_id, name) queue identity — so two tenants that share a queue name
    -- get SEPARATE stat rows (retained bytes, totals) instead of merging.
    LEFT JOIN per_queue pq ON pq.queue_name = q.name AND pq.tenant_id = q.tenant_id
    LEFT JOIN dlq_agg   da ON da.queue_name = q.name AND da.tenant_id = q.tenant_id
    LEFT JOIN seg_bytes sb ON sb.queue_name = q.name AND sb.tenant_id = q.tenant_id
    WHERE q.storage = 'segments'
    ON CONFLICT (stat_type, stat_key) DO UPDATE SET
        queue_id             = EXCLUDED.queue_id,
        child_count          = EXCLUDED.child_count,
        total_messages       = EXCLUDED.total_messages,
        pending_messages     = EXCLUDED.pending_messages,
        processing_messages  = EXCLUDED.processing_messages,
        completed_messages   = EXCLUDED.completed_messages,
        dead_letter_messages = EXCLUDED.dead_letter_messages,
        retained_bytes       = EXCLUDED.retained_bytes,
        oldest_pending_at    = EXCLUDED.oldest_pending_at,
        newest_message_at    = EXCLUDED.newest_message_at,
        avg_lag_seconds      = EXCLUDED.avg_lag_seconds,
        max_lag_seconds      = EXCLUDED.max_lag_seconds,
        avg_offset_lag       = EXCLUDED.avg_offset_lag,
        max_offset_lag       = EXCLUDED.max_offset_lag,
        -- throughput from delta vs the previous snapshot (per-second rates).
        -- NOTE (semantics shift vs 034, inherent to R1): total_messages is the
        -- RETAINED count, so ingest/processed rates dip transiently right
        -- after a retention sweep shrinks the retained window — same class of
        -- artifact the 034 numbers had (its SUM(msg_count) shrank on sweep
        -- too), not a regression.
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
    -- partition_id, so they work unchanged for the log queue rows above.
    v_ns   := queen.aggregate_namespace_stats_v1();
    v_task := queen.aggregate_task_stats_v1();
    v_sys  := queen.aggregate_system_stats_v2();

    -- aggregate_system_stats_v2 sets child_count = COUNT(queen.partitions),
    -- the rows-engine partition count (near-zero here). Overwrite with the
    -- live log-partition count so the overview 'partitions' tile is correct
    -- (same fix 034 carried).
    SELECT COUNT(*) INTO v_log_parts FROM queen.log_partitions;
    UPDATE queen.stats SET child_count = v_log_parts
    WHERE stat_type = 'system' AND stat_key = 'global';

    RETURN jsonb_build_object(
        'engine', 'segments',
        'queuesUpdated', v_queues,
        'segPartitions', v_log_parts,
        'namespaces', v_ns,
        'tasks', v_task,
        'system', v_sys,
        'refreshedAt', to_char(v_now, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
    );
END;
$$;

GRANT EXECUTE ON FUNCTION queen.log_refresh_all_stats_v1() TO PUBLIC;

-- ============================================================================
-- queen.get_queue_detail_v2 — dual-mode redefinition (log branch).
-- ============================================================================
-- Loads after 013 in the boot roster and supersedes it (exactly as 034 did):
-- rows queues keep the 013 body verbatim; storage='segments' queues take an
-- early-return path computing the SAME output shape (queue.config,
-- partitions[].{messages,stats,cursor,...}, totals.messages.*) from the log
-- tables with the watermark math above. Two log-schema honesty notes, keys
-- preserved for the webapp:
--   * cursor.batchesConsumed is 0 — queen.log_consumers does not carry the
--     old total_batches_consumed counter (deliberate schema diet, 041).
--   * lastActivity = MAX(lease_acquired_at) — the closest activity signal
--     the log schema keeps (former last_consumed_at is gone).
-- ============================================================================
-- Track B (§5): p_tenant scopes the single-queue detail. DROP the pre-tenant
-- (TEXT) form (013 also defines it) so the scoped (TEXT, UUID) is what callers hit.
DROP FUNCTION IF EXISTS queen.get_queue_detail_v2(TEXT);
CREATE OR REPLACE FUNCTION queen.get_queue_detail_v2(
    p_queue_name TEXT,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001')
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
    FROM queen.queues q WHERE q.name = p_queue_name AND q.tenant_id = p_tenant;

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
        FROM queen.queues q WHERE q.name = p_queue_name AND q.tenant_id = p_tenant;

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

    -- --------------------------------------------------------------- log queue
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
    FROM queen.queues q WHERE q.name = p_queue_name AND q.tenant_id = p_tenant;

    WITH parts AS (
        SELECT lp.id, lp.name, lp.created_at, lp.last_offset, lp.log_start
        FROM queen.log_partitions lp
        JOIN queen.log_queues lq ON lq.id = lp.queue_id
        WHERE lq.name = p_queue_name AND lq.tenant_id = p_tenant
    ),
    worst AS (
        SELECT c.partition_id, MIN(c.committed) AS committed
        FROM queen.log_consumers c
        JOIN parts pa ON pa.id = c.partition_id
        GROUP BY c.partition_id
    ),
    -- Watermark arithmetic (see log_refresh_all_stats_v1 header) + the two
    -- per-partition timestamp probes.
    base AS (
        SELECT pa.id, pa.name, pa.created_at,
               GREATEST(pa.last_offset - pa.log_start + 1, 0)::bigint AS total,
               GREATEST(pa.last_offset
                        - GREATEST(COALESCE(w.committed, -1), pa.log_start - 1),
                        0)::bigint AS pending,
               GREATEST(COALESCE(w.committed, -1), pa.log_start - 1) AS floor_off
        FROM parts pa
        LEFT JOIN worst w ON w.partition_id = pa.id
    ),
    enrich AS (
        SELECT b.id, b.name, b.created_at, b.total, b.pending,
               CASE WHEN b.pending > 0
                    THEN queen.log_oldest_pending_at_v1(b.id, b.floor_off + 1)
               END AS oldest_message,
               tail.created_at AS newest_message
        FROM base b
        LEFT JOIN LATERAL (
            SELECT s.created_at FROM queen.log_segments s
            WHERE s.partition_id = b.id
            ORDER BY s.base_offset DESC LIMIT 1
        ) tail ON true
    ),
    cur AS (
        SELECT c.partition_id,
               SUM(GREATEST(0, COALESCE(c.batch_end, c.committed) - c.committed))
                   FILTER (WHERE c.lease_expires_at IS NOT NULL AND c.lease_expires_at > NOW())::bigint AS processing,
               COALESCE(SUM(c.total_consumed), 0) AS total_consumed,
               MAX(c.lease_acquired_at) AS last_activity
        FROM queen.log_consumers c
        JOIN parts pa ON pa.id = c.partition_id
        GROUP BY c.partition_id
    ),
    dlq AS (
        SELECT d.partition_id, COUNT(*)::bigint AS dead_letter
        FROM queen.log_dlq d JOIN parts pa ON pa.id = d.partition_id
        GROUP BY d.partition_id
    ),
    per_part AS (
        SELECT en.id, en.name, en.created_at,
               en.total,
               en.pending,
               LEAST(COALESCE(cu.processing, 0), en.pending) AS processing,
               GREATEST(0, en.total - en.pending - COALESCE(dl.dead_letter, 0)) AS completed,
               COALESCE(dl.dead_letter, 0) AS dead_letter,
               en.oldest_message, en.newest_message,
               COALESCE(cu.total_consumed, 0) AS total_consumed,
               cu.last_activity
        FROM enrich en
        LEFT JOIN cur cu ON cu.partition_id = en.id
        LEFT JOIN dlq dl ON dl.partition_id = en.id
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'id', id, 'name', name,
                'createdAt', to_char(created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                -- 'failed' is NULL, not 0: the log engine keeps no per-partition
                -- failure counter (ack failures are per (queue, minute) in
                -- queue_lag_metrics), and a literal 0 next to a live ack-failure
                -- chart for the same queue reads as "no failures".
                'messages', jsonb_build_object('total', total, 'pending', pending,
                    'processing', processing, 'completed', completed, 'failed', NULL, 'deadLetter', dead_letter),
                'stats', jsonb_build_object('total', total, 'pending', pending,
                    'processing', processing, 'completed', completed, 'failed', NULL, 'deadLetter', dead_letter),
                'cursor', jsonb_build_object('totalConsumed', total_consumed, 'batchesConsumed', 0),
                'lastActivity', CASE WHEN last_activity IS NOT NULL THEN to_char(last_activity, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
                'oldestMessage', CASE WHEN oldest_message IS NOT NULL THEN to_char(oldest_message, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
                'newestMessage', CASE WHEN newest_message IS NOT NULL THEN to_char(newest_message, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END
            ) ORDER BY name), '[]'::jsonb),
           jsonb_build_object('messages', jsonb_build_object(
                'total', COALESCE(SUM(total),0), 'pending', COALESCE(SUM(pending),0),
                'processing', COALESCE(SUM(processing),0), 'completed', COALESCE(SUM(completed),0),
                'failed', NULL, 'deadLetter', COALESCE(SUM(dead_letter),0)))
    INTO v_partitions, v_totals FROM per_part;

    RETURN v_queue_info || jsonb_build_object('partitions', v_partitions, 'totals', v_totals);
END;
$$;

GRANT EXECUTE ON FUNCTION queen.get_queue_detail_v2(TEXT, UUID) TO PUBLIC;

-- ============================================================================
-- queen.get_queue_v2 — dual-mode redefinition (rows + LOG).
-- ============================================================================
-- 013's body reads queen.partitions + the 'partition' stat rows, both of which
-- are EMPTY under the log engine (048 writes only 'queue'-type stat rows, and
-- log partitions live in queen.log_partitions). It therefore described every
-- healthy log queue as "0 partitions / 0 messages" at HTTP 200 — the shape a
-- caller cannot distinguish from a real empty queue. This redefinition keeps
-- 013's body verbatim for rows queues and computes the SAME output shape from
-- the log tables (same watermark arithmetic as get_queue_detail_v2 above) for
-- storage='segments'. `failed` is NULL, not 0: the log engine keeps no
-- per-partition failure counter.
-- ============================================================================
-- 013's rows-engine body, verbatim, under its own name so the dual-mode
-- entry point below can delegate to it without duplicating it here.
CREATE OR REPLACE FUNCTION queen.get_queue_v2_rows(
    p_queue_name TEXT,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001')
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_queue_id UUID;
    v_queue_info JSONB;
    v_partitions JSONB;
    v_totals JSONB;
BEGIN
    -- Get queue info
    SELECT q.id, jsonb_build_object(
        'id', q.id,
        'name', q.name,
        'namespace', q.namespace,
        'task', q.task,
        'createdAt', to_char(q.created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
    )
    INTO v_queue_id, v_queue_info
    FROM queen.queues q
    WHERE q.name = p_queue_name AND q.tenant_id = p_tenant;
    
    IF v_queue_id IS NULL THEN
        RETURN jsonb_build_object('error', 'Queue not found');
    END IF;
    
    -- Get partitions with pre-computed stats
    -- Aggregate across ALL consumer groups per partition (MAX for totals since they're the same)
    WITH partition_stats AS (
        SELECT 
            p.id,
            p.name,
            p.created_at,
            -- Use MAX to get the message counts (same for all consumer groups)
            COALESCE(MAX(s.total_messages), 0) as total,
            -- Pending/processing may differ per consumer group - use MAX for queue view
            COALESCE(MAX(s.pending_messages), 0) as pending,
            COALESCE(MAX(s.processing_messages), 0) as processing,
            COALESCE(MAX(s.completed_messages), 0) as completed,
            0 as failed,
            COALESCE(MAX(s.dead_letter_messages), 0) as dead_letter,
            MAX(s.oldest_pending_at) as oldest_message,
            MAX(s.newest_message_at) as newest_message
        FROM queen.partitions p
        LEFT JOIN queen.stats s ON s.stat_type = 'partition' 
            AND s.partition_id = p.id
        WHERE p.queue_id = v_queue_id
        GROUP BY p.id, p.name, p.created_at
    )
    SELECT 
        COALESCE(jsonb_agg(
            jsonb_build_object(
                'id', id,
                'name', name,
                'createdAt', to_char(created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                'stats', jsonb_build_object(
                    'total', total,
                    'pending', pending,
                    'processing', processing,
                    'completed', completed,
                    'failed', failed,
                    'deadLetter', dead_letter
                ),
                'oldestMessage', CASE WHEN oldest_message IS NOT NULL 
                    THEN to_char(oldest_message, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
                'newestMessage', CASE WHEN newest_message IS NOT NULL 
                    THEN to_char(newest_message, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END
            ) ORDER BY name
        ), '[]'::jsonb),
        jsonb_build_object(
            'total', COALESCE(SUM(total), 0),
            'pending', COALESCE(SUM(pending), 0),
            'processing', COALESCE(SUM(processing), 0),
            'completed', COALESCE(SUM(completed), 0),
            'failed', COALESCE(SUM(failed), 0),
            'deadLetter', COALESCE(SUM(dead_letter), 0)
        )
    INTO v_partitions, v_totals
    FROM partition_stats;
    
    RETURN v_queue_info || jsonb_build_object(
        'partitions', v_partitions,
        'totals', v_totals,
        -- Storage quota (§6.1): retained payload bytes for this queue, from the
        -- queue's stat row (refreshed at the stats cadence). Scoped implicitly:
        -- v_queue_id was resolved WHERE tenant_id = p_tenant above.
        'retainedBytes', COALESCE((SELECT retained_bytes FROM queen.stats
                                   WHERE stat_type = 'queue' AND queue_id = v_queue_id), 0)
    );
END;
$$;

GRANT EXECUTE ON FUNCTION queen.get_queue_v2_rows(TEXT, UUID) TO PUBLIC;

DROP FUNCTION IF EXISTS queen.get_queue_v2(TEXT);
CREATE OR REPLACE FUNCTION queen.get_queue_v2(
    p_queue_name TEXT,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001')
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_queue_id UUID;
    v_is_seg BOOLEAN;
    v_queue_info JSONB;
    v_partitions JSONB;
    v_totals JSONB;
BEGIN
    SELECT q.id, (q.storage = 'segments') INTO v_queue_id, v_is_seg
    FROM queen.queues q WHERE q.name = p_queue_name AND q.tenant_id = p_tenant;

    IF v_queue_id IS NULL THEN
        RETURN jsonb_build_object('error', 'Queue not found');
    END IF;

    IF NOT v_is_seg THEN
        RETURN queen.get_queue_v2_rows(p_queue_name, p_tenant);
    END IF;

    SELECT jsonb_build_object(
        'id', q.id, 'name', q.name, 'namespace', q.namespace, 'task', q.task,
        'createdAt', to_char(q.created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'))
    INTO v_queue_info
    FROM queen.queues q WHERE q.id = v_queue_id;

    WITH parts AS (
        SELECT lp.id, lp.name, lp.created_at, lp.last_offset, lp.log_start
        FROM queen.log_partitions lp
        JOIN queen.log_queues lq ON lq.id = lp.queue_id
        WHERE lq.name = p_queue_name AND lq.tenant_id = p_tenant
    ),
    worst AS (
        SELECT c.partition_id, MIN(c.committed) AS committed
        FROM queen.log_consumers c
        JOIN parts pa ON pa.id = c.partition_id
        GROUP BY c.partition_id
    ),
    base AS (
        SELECT pa.id, pa.name, pa.created_at,
               GREATEST(pa.last_offset - pa.log_start + 1, 0)::bigint AS total,
               GREATEST(pa.last_offset
                        - GREATEST(COALESCE(w.committed, -1), pa.log_start - 1),
                        0)::bigint AS pending,
               GREATEST(COALESCE(w.committed, -1), pa.log_start - 1) AS floor_off
        FROM parts pa
        LEFT JOIN worst w ON w.partition_id = pa.id
    ),
    enrich AS (
        SELECT b.id, b.name, b.created_at, b.total, b.pending,
               CASE WHEN b.pending > 0
                    THEN queen.log_oldest_pending_at_v1(b.id, b.floor_off + 1)
               END AS oldest_message,
               tail.created_at AS newest_message
        FROM base b
        LEFT JOIN LATERAL (
            SELECT s.created_at FROM queen.log_segments s
            WHERE s.partition_id = b.id
            ORDER BY s.base_offset DESC LIMIT 1
        ) tail ON true
    ),
    cur AS (
        SELECT c.partition_id,
               SUM(GREATEST(0, COALESCE(c.batch_end, c.committed) - c.committed))
                   FILTER (WHERE c.lease_expires_at IS NOT NULL AND c.lease_expires_at > NOW())::bigint AS processing
        FROM queen.log_consumers c
        JOIN parts pa ON pa.id = c.partition_id
        GROUP BY c.partition_id
    ),
    dlq AS (
        SELECT d.partition_id, COUNT(*)::bigint AS dead_letter
        FROM queen.log_dlq d JOIN parts pa ON pa.id = d.partition_id
        GROUP BY d.partition_id
    ),
    per_part AS (
        SELECT en.id, en.name, en.created_at, en.total, en.pending,
               LEAST(COALESCE(cu.processing, 0), en.pending) AS processing,
               GREATEST(0, en.total - en.pending - COALESCE(dl.dead_letter, 0)) AS completed,
               COALESCE(dl.dead_letter, 0) AS dead_letter,
               en.oldest_message, en.newest_message
        FROM enrich en
        LEFT JOIN cur cu ON cu.partition_id = en.id
        LEFT JOIN dlq dl ON dl.partition_id = en.id
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'id', id, 'name', name,
                'createdAt', to_char(created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                'stats', jsonb_build_object(
                    'total', total, 'pending', pending, 'processing', processing,
                    'completed', completed, 'failed', NULL, 'deadLetter', dead_letter),
                'oldestMessage', CASE WHEN oldest_message IS NOT NULL
                    THEN to_char(oldest_message, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
                'newestMessage', CASE WHEN newest_message IS NOT NULL
                    THEN to_char(newest_message, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END
            ) ORDER BY name), '[]'::jsonb),
           jsonb_build_object(
                'total', COALESCE(SUM(total), 0), 'pending', COALESCE(SUM(pending), 0),
                'processing', COALESCE(SUM(processing), 0), 'completed', COALESCE(SUM(completed), 0),
                'failed', NULL, 'deadLetter', COALESCE(SUM(dead_letter), 0))
    INTO v_partitions, v_totals FROM per_part;

    RETURN v_queue_info || jsonb_build_object(
        'partitions', v_partitions,
        'totals', v_totals,
        'retainedBytes', COALESCE((SELECT retained_bytes FROM queen.stats
                                   WHERE stat_type = 'queue' AND queue_id = v_queue_id), 0)
    );
END;
$$;

GRANT EXECUTE ON FUNCTION queen.get_queue_v2(TEXT, UUID) TO PUBLIC;

-- ============================================================================
-- queen.get_analytics_v1 — log-native redefinition.
-- ============================================================================
-- Same shape/interval logic as the 034 version: the message-volume series
-- buckets queen.log_segments by created_at, attributing a segment's frames
-- (end_offset - base_offset + 1 — exact, replaces msg_count) to its creation
-- bucket; per-message granularity inside a segment is not recoverable, as
-- before. This is the one intentionally O(segments-in-range) reader: it is a
-- paid-per-request dashboard endpoint with a time filter, not the refresh
-- loop, so the §9 no-scan rule does not apply.
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
    -- Track B (§5): tenant travels in the filter JSON (`_tenant`); signature unchanged.
    v_tenant UUID := COALESCE((p_filters->>'_tenant')::uuid, '00000000-0000-0000-0000-000000000001');
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
               SUM(s.end_offset - s.base_offset + 1) AS message_count
        FROM queen.log_segments s
        JOIN queen.log_partitions p ON p.id = s.partition_id
        JOIN queen.log_queues q     ON q.id = p.queue_id
        JOIN queen.queues qq        ON qq.name = q.name AND qq.tenant_id = q.tenant_id
        WHERE s.created_at >= v_from AND s.created_at <= v_to
          AND q.tenant_id = v_tenant
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

-- ----------------------------------------------------------------------------
-- log_queue_message_stats_v1: per-queue (segments, messages) counters — the
-- SQL home for db.rs::seg_queue_message_stats (queue DETAIL enrichment).
-- messages = watermark arithmetic (exact retained frames, O(partitions));
-- segments = COUNT over live queen.log_segments rows via the PK (index-only,
-- bounded by the queue's live segment count — a per-request admin read, not
-- the refresh loop). NOT pg_relation_size: the row count is what the wire
-- field reports and dev partitions are small; size estimates would change
-- the meaning of an existing field.
-- ----------------------------------------------------------------------------
-- Track B (§5): p_tenant scopes the per-queue counters. DROP the (TEXT) form.
DROP FUNCTION IF EXISTS queen.log_queue_message_stats_v1(TEXT);
CREATE OR REPLACE FUNCTION queen.log_queue_message_stats_v1(
    p_queue TEXT,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001')
RETURNS TABLE (segments BIGINT, messages BIGINT)
LANGUAGE sql STABLE
AS $$
    SELECT COALESCE((
               SELECT count(*)
               FROM queen.log_segments s
               JOIN queen.log_partitions p ON p.id = s.partition_id
               JOIN queen.log_queues q     ON q.id = p.queue_id
               WHERE q.name = p_queue AND q.tenant_id = p_tenant
           ), 0)::bigint,
           COALESCE((
               SELECT SUM(GREATEST(p.last_offset - p.log_start + 1, 0))
               FROM queen.log_partitions p
               JOIN queen.log_queues q ON q.id = p.queue_id
               WHERE q.name = p_queue AND q.tenant_id = p_tenant
           ), 0)::bigint
$$;

GRANT EXECUTE ON FUNCTION queen.log_queue_message_stats_v1(TEXT, UUID) TO PUBLIC;

-- ----------------------------------------------------------------------------
-- log_queue_stats_all_v1: broker-wide per-queue counters — the SQL home for
-- db.rs::seg_queue_stats_all (queue LIST enrichment; queen.stats can lag a
-- refresh cadence, this is the live view). Same accounting as above.
-- ----------------------------------------------------------------------------
-- Track B (§5): p_tenant scopes the broker-wide per-queue counters to one tenant.
DROP FUNCTION IF EXISTS queen.log_queue_stats_all_v1();
CREATE OR REPLACE FUNCTION queen.log_queue_stats_all_v1(
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001')
RETURNS TABLE (queue_name TEXT, partitions BIGINT, segments BIGINT, messages BIGINT)
LANGUAGE sql STABLE
AS $$
    -- Segment counts pre-aggregated per partition BEFORE the queue join: a
    -- flat 3-way join would fan a partition's watermark row out once per
    -- segment and over-count messages by that factor.
    WITH seg_counts AS (
        SELECT s.partition_id, count(*)::bigint AS segs
        FROM queen.log_segments s
        GROUP BY s.partition_id
    )
    SELECT q.name,
           count(p.id)::bigint,
           COALESCE(SUM(sc.segs), 0)::bigint,
           COALESCE(SUM(GREATEST(p.last_offset - p.log_start + 1, 0)), 0)::bigint
    FROM queen.log_queues q
    LEFT JOIN queen.log_partitions p ON p.queue_id = q.id
    LEFT JOIN seg_counts sc ON sc.partition_id = p.id
    WHERE q.tenant_id = p_tenant
    GROUP BY q.name
$$;

GRANT EXECUTE ON FUNCTION queen.log_queue_stats_all_v1(UUID) TO PUBLIC;
