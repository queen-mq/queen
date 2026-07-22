-- ============================================================================
-- Log engine (18-log-engine.md §9) — admin surface.
-- ============================================================================
-- Port of the seg parts of three retired files (dropped by 040):
--   * 031_seg_consumer_groups.sql — the MUTATING consumer-group management ops
--     (delete a group, seek its cursor per-queue and per-partition) plus the
--     shared one-cursor seek helper;
--   * 027_storage_v2_observability.sql — the v2 branches of the dual-engine
--     redefinitions the Rust broker actually calls: get_consumer_groups_v4
--     (group listing), list_messages_v1 (messages browsing) and
--     get_dlq_messages_v1 (DLQ browsing). NOT ported: get_queue_messages_v1 /
--     has_pending_messages / get_prometheus_metrics_v1 — the first two have no
--     Rust call site (C++-era routes; the pop path's pending check is 043's
--     log_has_pending_v1), prometheus support belongs to 048;
--   * 030_seg_traces.sql — the engine-agnostic record_trace_v1 redefinition +
--     the message_traces FK decoupling. 030 itself referenced no seg tables,
--     but its redefinition must survive 030's deletion or tracing a log
--     message would regress to 011's "Message not found" bail.
--
-- Positions are re-addressed to single per-partition BIGINT offsets. Every
-- JSON key is UNCHANGED (§11): keys that used to carry a segment seq now carry
-- base_offset, and frameIdx carries (offset - base_offset) — clients treat
-- both as opaque.
--
-- Rows-engine guarantee (same as 027): for storage='rows' queues every
-- redefined function returns results byte-identical to the original — the
-- rows branches below are verbatim copies, and the log branches are guarded
-- on queen.queues.storage = 'segments' (the /configure contract keeps that
-- storage VALUE for the log engine, §11).
--
-- Applied idempotently at boot after 041-046 (lexical order): every log_*
-- table it references exists by 044 (log_dlq).
-- ============================================================================

-- ============================================================================
-- queen.log_delete_consumer_group_v1: drop a group's LOG cursor state.
-- Removes every queen.log_consumers row for the group (across all partitions
-- of every queue) and its per-(queue, group) empty-scan watermarks. The
-- rows-side coordination tables (queen.partition_consumers /
-- consumer_groups_metadata / consumer_watermarks) are handled by the sibling
-- queen.delete_consumer_group_v1 the broker also calls — p_delete_metadata is
-- echoed for a consistent response shape. deletedPartitions = number of
-- log_consumers cursors removed.
--
-- Unlike the seg era there is NO engine-scoping join: the seg fold had put
-- both engines' cursors in queen.partition_consumers (unfolded by 040), while
-- queen.log_consumers holds log-engine cursors ONLY, so the plain group
-- predicate is already exact and the two engines' deletes stay disjoint.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.log_delete_consumer_group_v1(
    p_group TEXT,
    p_delete_metadata BOOLEAN DEFAULT TRUE
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted INTEGER;
BEGIN
    DELETE FROM queen.log_consumers WHERE consumer_group = p_group;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    -- Drop the empty-scan watermark so a later group of the same name does not
    -- inherit a stale "queue is empty since T" floor and silently skip cold
    -- partitions (symmetric with the watermark cleanup in log_seek_* below).
    DELETE FROM queen.consumer_watermarks WHERE consumer_group = p_group;

    RETURN jsonb_build_object(
        'success', true,
        'consumerGroup', p_group,
        'deletedPartitions', v_deleted,
        'metadataDeleted', p_delete_metadata
    );
END;
$$;

-- ============================================================================
-- Cursor seek — shared helper: position ONE (partition, group) log cursor.
--   p_to_end     -> committed = last_offset (skip everything: next wanted =
--                   last_offset + 1, exactly the seg version's last_seq+1/0).
--   p_timestamp  -> committed = first base_offset with created_at >=
--                   p_timestamp, minus 1 (created_at is monotone in
--                   base_offset — the PUSHSER invariant — so a forward walk
--                   from the retention watermark log_start finds the boundary;
--                   the same walk 043 uses for timestamp subscriptions and
--                   045 uses for the retention boundary). committed =
--                   last_offset when nothing that recent exists yet
--                   (future-only cursor). Timestamp resolution is segment-
--                   granular, exactly like the seg version's (seq, off=0).
-- Any live lease is released and the retry/redelivery state reset; the row is
-- created when the group has never popped this partition (INSERT ... ON
-- CONFLICT DO UPDATE).
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.log_seek_one_v1(
    p_partition_id UUID,
    p_last_offset BIGINT,
    p_log_start BIGINT,
    p_group TEXT,
    p_to_end BOOLEAN,
    p_timestamp TIMESTAMPTZ
) RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_committed BIGINT;
BEGIN
    IF p_to_end THEN
        v_committed := p_last_offset;
    ELSE
        SELECT s.base_offset - 1 INTO v_committed
        FROM queen.log_segments s
        WHERE s.partition_id = p_partition_id
          AND s.base_offset >= p_log_start
          AND s.created_at >= p_timestamp
        ORDER BY s.base_offset LIMIT 1;
        IF v_committed IS NULL THEN
            v_committed := p_last_offset;
        END IF;
    END IF;

    INSERT INTO queen.log_consumers (
        partition_id, consumer_group, committed, batch_end,
        worker_id, lease_expires_at, lease_acquired_at,
        batch_retry_count, attempt_offset, attempt_count)
    VALUES (p_partition_id, p_group, v_committed, NULL,
            NULL, NULL, NULL, 0, NULL, 0)
    ON CONFLICT (partition_id, consumer_group) DO UPDATE SET
        committed = EXCLUDED.committed,
        batch_end = NULL,
        worker_id = NULL, lease_expires_at = NULL, lease_acquired_at = NULL,
        batch_retry_count = 0,
        attempt_offset = NULL, attempt_count = 0;
END;
$$;

-- ============================================================================
-- queen.log_seek_consumer_group_v1: seek the log cursor for EVERY partition
-- of a queue. Body of the POST .../seek route (per-queue variant).
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.log_seek_consumer_group_v1(
    p_group TEXT,
    p_queue TEXT,
    p_to_end BOOLEAN DEFAULT FALSE,
    p_timestamp TIMESTAMPTZ DEFAULT NULL
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_p RECORD;
    v_updated INTEGER := 0;
BEGIN
    IF NOT p_to_end AND p_timestamp IS NULL THEN
        RETURN jsonb_build_object(
            'success', false,
            'error', 'Must specify either toEnd=true or a timestamp');
    END IF;

    FOR v_p IN
        SELECT p.id, p.last_offset, p.log_start
        FROM queen.log_partitions p
        JOIN queen.log_queues q ON q.id = p.queue_id
        WHERE q.name = p_queue
    LOOP
        PERFORM queen.log_seek_one_v1(v_p.id, v_p.last_offset, v_p.log_start,
                                      p_group, p_to_end, p_timestamp);
        v_updated := v_updated + 1;
    END LOOP;

    -- Clear the empty-scan watermark so a backward seek re-exposes cold
    -- partitions to the wildcard candidate filter on the next pop.
    DELETE FROM queen.consumer_watermarks
    WHERE queue_name = p_queue AND consumer_group = p_group;

    RETURN jsonb_build_object(
        'success', true,
        'consumerGroup', p_group,
        'queueName', p_queue,
        'seekToEnd', p_to_end,
        'targetTimestamp', CASE WHEN p_timestamp IS NOT NULL
            THEN to_char(p_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
        'partitionsUpdated', v_updated
    );
END;
$$;

-- ============================================================================
-- queen.log_seek_partition_v1: seek the log cursor for ONE named partition.
-- Body of the POST .../partitions/:partition/seek route.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.log_seek_partition_v1(
    p_group TEXT,
    p_queue TEXT,
    p_partition TEXT,
    p_to_end BOOLEAN DEFAULT FALSE,
    p_timestamp TIMESTAMPTZ DEFAULT NULL
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_id UUID;
    v_last_offset BIGINT;
    v_log_start BIGINT;
BEGIN
    IF NOT p_to_end AND p_timestamp IS NULL THEN
        RETURN jsonb_build_object(
            'success', false,
            'error', 'Must specify either toEnd=true or a timestamp');
    END IF;

    SELECT p.id, p.last_offset, p.log_start
    INTO v_id, v_last_offset, v_log_start
    FROM queen.log_partitions p
    JOIN queen.log_queues q ON q.id = p.queue_id
    WHERE q.name = p_queue AND p.name = p_partition;

    IF v_id IS NULL THEN
        RETURN jsonb_build_object('success', false, 'error', 'Partition not found');
    END IF;

    PERFORM queen.log_seek_one_v1(v_id, v_last_offset, v_log_start,
                                  p_group, p_to_end, p_timestamp);

    DELETE FROM queen.consumer_watermarks
    WHERE queue_name = p_queue AND consumer_group = p_group;

    RETURN jsonb_build_object(
        'success', true,
        'consumerGroup', p_group,
        'queueName', p_queue,
        'partitionName', p_partition,
        'seekToEnd', p_to_end,
        'targetTimestamp', CASE WHEN p_timestamp IS NOT NULL
            THEN to_char(p_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
        'updated', true
    );
END;
$$;

-- ============================================================================
-- queen.get_consumer_groups_v4 — dual-mode redefinition (rows + LOG).
-- The rows-engine block is copied verbatim from 027/008 (the storage <>
-- 'segments' guard is a no-op for rows queues, since log queues never get
-- queen.partition_consumers rows); log-engine groups are computed from
-- queen.log_consumers / log_partitions and APPENDED to the array. Log entries
-- carry the same keys plus "storage":"segments" (the wire storage value, §11);
-- unlike the rows branch, totalLag IS populated — with offsets it is pure
-- arithmetic (§9): pending per (partition, group) =
--   GREATEST(last_offset - GREATEST(committed, log_start - 1), 0)
-- — NO log_segments scan for the count. maxTimeLag = age of the oldest
-- unconsumed segment (that one is a per-partition PK-range MIN over
-- log_segments, the only segment touch here). Subscription metadata does not
-- exist per-partition in the log engine -> nulls, as before.
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

    -- -------------------------------------------------------------- log (v2)
    -- Pending frames are pure allocator/cursor arithmetic (§9) — the seg
    -- version's pre-aggregated segments×consumers SUM is gone. Only the time
    -- lag still touches log_segments: MIN(created_at) over the not-yet-
    -- consumed PK range per (partition, group).
    WITH lag AS (
        SELECT c.partition_id, c.consumer_group,
               MIN(s.created_at) AS oldest_unconsumed_at
        FROM queen.log_consumers c
        JOIN queen.log_segments s
          ON s.partition_id = c.partition_id
         AND s.end_offset > c.committed
        GROUP BY c.partition_id, c.consumer_group
    ),
    v2_data AS (
        SELECT c.consumer_group,
               q.name AS queue_name,
               c.total_consumed,
               -- committed=-1 (never consumed) counts the whole live log;
               -- log_start-1 dominates once retention has eaten past the
               -- cursor (evicted frames are not lag).
               GREATEST(p.last_offset - GREATEST(c.committed, p.log_start - 1), 0) AS pending,
               l.oldest_unconsumed_at
        FROM queen.log_consumers c
        JOIN queen.log_partitions p ON p.id = c.partition_id
        JOIN queen.log_queues q ON q.id = p.queue_id
        -- Strict guard: only queues currently routed to the log engine
        -- (storage value 'segments' on the wire, §11).
        JOIN queen.queues quv ON quv.name = q.name AND quv.storage = 'segments'
        LEFT JOIN lag l ON l.partition_id = c.partition_id
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

-- ============================================================================
-- queen.list_messages_v1 — dual-mode redefinition (rows + LOG).
-- The rows pipeline is the 010/027 body verbatim; message-shaped entries for
-- log queues are then APPENDED to result.messages. Per-message metadata for
-- the log engine comes from queen.log_txns exploded to frames (the ONLY
-- per-message data SQL has — 16B xxh3_128 per frame; mids and txn text live
-- inside the blob):
--   { id:null, transactionId:null, txnHash:"<32 hex>", partitionId, queuePath,
--     queue, partition, namespace, task, status, queueStatus, busStatus,
--     payloadAvailable:false, segment:{seq, frameIdx}, queuePriority,
--     traceId:null, producerSub:null, createdAt, leaseExpiresAt }
-- KEY SEMANTICS (§11): segment.seq carries base_offset and segment.frameIdx
-- carries (offset - base_offset) — exactly the address the route layer's
-- enrichment pass needs to fetch the covering blob (log_segments PK) and
-- decode the frame, which fills data/payload/transactionId (and can fill id:
-- the frame carries the mid — Rust phase). txnHash switches from the seg
-- engine's hashtextextended bigint-text to 16B hex; both are opaque tokens.
-- ROUTE-LAYER CONTRACT: entries exist only within the log_txns purge window
-- (GREATEST(dedup_window, completed_retention, 900s) — a wider net than the
-- seg engine's dedup-only window, and no longer dedup-gated). The entries are
-- appended AFTER the rows page and independently capped at the same limit (a
-- combined page can therefore hold up to 2x limit when both engines match the
-- filter — log-queue filters normally isolate one engine). 'mode' detection
-- stays rows-only ('none' for a pure log queue), as in 027.
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

    -- ------------------------------------------------- log-engine entries
    -- No index on queen.log_txns.created_at: the scan is bounded by the txns
    -- window's steady-state size (O(rate x window)) — acceptable for a
    -- dashboard listing, same trade the seg version made over seg_dedup.
    -- A frame is consumed iff its offset <= committed; dead_letter wins (the
    -- quarantined head frame lives in queen.log_dlq addressed by offset).
    SELECT COALESCE(jsonb_agg(v2.obj ORDER BY v2.created_at DESC, v2.off DESC), '[]'::jsonb)
    INTO v_v2
    FROM (
        SELECT vd.created_at, vd.off,
            jsonb_build_object(
                'id', NULL,
                'transactionId', NULL,
                'txnHash', encode(vd.h, 'hex'),
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
                'segment', jsonb_build_object(
                    'seq', vd.base_offset,
                    'frameIdx', (vd.off - vd.base_offset)::int),
                'createdAt', to_char(vd.created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                'leaseExpiresAt', CASE WHEN vd.lease_expires_at IS NOT NULL
                    THEN to_char(vd.lease_expires_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END
            ) AS obj
        FROM (
            SELECT t.base_offset, t.created_at,
                   t.base_offset + th.idx AS off, th.h,
                   p.id AS partition_id, p.name AS partition_name,
                   q.name AS queue_name,
                   quv.namespace, quv.task, quv.priority AS queue_priority,
                   c.lease_expires_at,
                   CASE
                       WHEN EXISTS (SELECT 1 FROM queen.log_dlq dl
                                    WHERE dl.partition_id = t.partition_id
                                      AND dl."offset" = t.base_offset + th.idx)
                           THEN 'dead_letter'
                       WHEN c.partition_id IS NOT NULL
                            AND t.base_offset + th.idx <= c.committed
                           THEN 'completed'
                       WHEN c.lease_expires_at IS NOT NULL AND c.lease_expires_at > NOW()
                           THEN 'processing'
                       ELSE 'pending'
                   END AS final_status,
                   (SELECT COUNT(*)::integer FROM queen.log_consumers c2
                    WHERE c2.partition_id = t.partition_id
                      AND c2.consumer_group <> '__QUEUE_MODE__'
                      AND t.base_offset + th.idx <= c2.committed) AS consumed_by_groups,
                   (SELECT COUNT(*)::integer FROM queen.log_consumers c2
                    WHERE c2.partition_id = t.partition_id
                      AND c2.consumer_group <> '__QUEUE_MODE__') AS total_bus_groups
            FROM queen.log_txns t
            JOIN queen.log_partitions p ON p.id = t.partition_id
            JOIN queen.log_queues q ON q.id = p.queue_id
            -- Strict guard: only queues currently routed to the log engine.
            JOIN queen.queues quv ON quv.name = q.name AND quv.storage = 'segments'
            LEFT JOIN queen.log_consumers c
                ON c.partition_id = p.id AND c.consumer_group = '__QUEUE_MODE__'
            CROSS JOIN LATERAL queen.log_unnest_hashes(t.hashes) th
            WHERE t.created_at >= v_from_ts AND t.created_at < v_to_ts
              AND (v_queue IS NULL OR q.name = v_queue)
              AND (v_partition IS NULL OR p.name = v_partition)
              AND (v_namespace IS NULL OR quv.namespace = v_namespace)
              AND (v_task IS NULL OR quv.task = v_task)
        ) vd
        WHERE (v_status IS NULL OR vd.final_status = v_status)
        ORDER BY vd.created_at DESC, vd.off DESC
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

-- ============================================================================
-- queen.get_dlq_messages_v1 — dual-mode redefinition (rows + LOG).
-- The rows branch is the 010/027 body verbatim (same joins, filters, keys and
-- outer LIMIT/OFFSET placement — byte-identical output for rows queues);
-- queen.log_dlq rows are UNIONed in with the SAME keys so /api/v1/dlq serves
-- both engines:
--   * payload snapshots live in queen.log_dlq itself (payload), no blob
--     decode needed — identical to the seg_dlq design;
--   * retryCount: the retries consumed when the frame was dead-lettered,
--     snapshotted onto the DLQ row by 044's quarantine path (falls back to
--     the queue's retry_limit — the pre-snapshot 027 behavior — for rows
--     that predate the snapshot);
--   * partitionId: the log_partitions uuid (echoed by pop/ack wire calls);
--   * createdAt: blobs are opaque to SQL, the original enqueue time is not
--     recoverable per frame -> failed_at (the DLQ event time), as before;
--   * producerSub: not tracked -> null.
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
            -- log engine (queen.log_dlq payload snapshots), same keys
            SELECT d2.failed_at,
                   jsonb_build_object(
                       'id', d2.id,
                       'transactionId', d2.transaction_id,
                       'partitionId', d2.partition_id,
                       'queue', q2q.name,
                       'partition', p2.name,
                       'consumerGroup', d2.consumer_group,
                       'errorMessage', d2.error,
                       'retryCount', COALESCE(d2.retry_count, COALESCE(qq.retry_limit, 3)),
                       'data', d2.payload,
                       'producerSub', NULL,
                       'createdAt', to_char(d2.failed_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                       'failedAt', to_char(d2.failed_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                   ) AS msg
            FROM queen.log_dlq d2
            JOIN queen.log_partitions p2 ON p2.id = d2.partition_id
            JOIN queen.log_queues q2q ON q2q.id = p2.queue_id
            LEFT JOIN queen.queues qq ON qq.name = q2q.name
            WHERE (v_queue IS NULL OR q2q.name = v_queue)
              AND (v_consumer_group IS NULL OR d2.consumer_group = v_consumer_group)
        ) t
        LIMIT v_limit OFFSET v_offset
    );
END;
$$;

-- ============================================================================
-- Traces glue (port of 030_seg_traces.sql — engine-agnostic trace recording).
-- ============================================================================
-- 011's queen.record_trace_v1 resolves the trace's message_id from
-- queen.messages (rows engine) and BAILS with {"success":false,"error":
-- "Message not found"} when no such row exists. Log queues never write
-- queen.messages, so tracing a log message would always fail without this
-- redefinition (applied AFTER 011 in lexical order, the same versioning
-- pattern the other redefinitions here use):
--   * rows engine: unchanged — message_id still resolves from queen.messages,
--     so a traced rows message keeps its FK-linked trace;
--   * log engine: record the trace with message_id = NULL. A log message has
--     NO queen.messages row, and any non-NULL value that is not a real
--     messages.id would be meaningless. message_id is a NULLABLE column, so
--     NULL is allowed.
-- queen.message_traces is keyed by (partition_id, transaction_id), so
-- queen.get_message_traces_v1 reads back the trace regardless of message_id.
--
-- Decoupling from the rows tables: queen.message_traces originally FK'd
-- message_id -> queen.messages and partition_id -> queen.partitions. Log
-- queues use queen.log_partitions UUIDs (an independent space) and have no
-- queen.messages rows, so both FKs would reject a log trace. They are dropped
-- here (idempotent; first done by the retired 030) — the coordination table is
-- preserved, decoupled from the engine-specific stores. Traces stay keyed by
-- (partition_id, transaction_id).
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

    -- Resolve message_id from queen.messages (rows engine). For a log-engine
    -- message this returns NULL — and it MUST stay NULL: log messages have no
    -- queen.messages row (their mids live inside segment blobs, opaque to
    -- SQL), and message_traces.message_id historically pointed into
    -- queen.messages.
    SELECT id INTO v_message_id
    FROM queen.messages
    WHERE transaction_id = v_transaction_id AND partition_id = v_partition_id
    LIMIT 1;

    -- Insert main trace record. message_id is NULL for log messages — the
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

-- ============================================================================
-- queen.get_lagging_partitions_v1 — dual-mode redefinition (rows + LOG).
-- Folded in from the retired 099 (which carried the seg-native lag rewrite,
-- RUSTFIX item 22): without this, the rows-only original from 008 would report
-- [] forever for log queues (their cursors live in queen.log_consumers, not
-- queen.partition_consumers). The rows CTE is the 008 body verbatim; the log
-- branch is pure offset arithmetic: pending =
--   GREATEST(last_offset - GREATEST(committed, log_start - 1), 0)
-- and the time lag is MIN(created_at) over the not-yet-consumed log_segments
-- PK range (the only segment touch). last_consumed_at does not exist in the
-- log consumer row -> NULL, same key shape.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.get_lagging_partitions_v1(p_min_lag_seconds INTEGER DEFAULT 0)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows JSONB;
    v_log JSONB;
BEGIN
    -- ------------------------------------------------------------- rows (008)
    WITH consumer_lag AS (
        SELECT
            pc.consumer_group,
            q.name as queue_name,
            p.name as partition_name,
            p.id as partition_id,
            pc.worker_id,
            pc.last_consumed_at,
            MIN(m.created_at) as oldest_unconsumed_at,
            COUNT(m.id) as unconsumed_count,
            EXTRACT(EPOCH FROM (NOW() - MIN(m.created_at)))::integer as lag_seconds
        FROM queen.partition_consumers pc
        JOIN queen.partitions p ON p.id = pc.partition_id
        JOIN queen.queues q ON q.id = p.queue_id
        LEFT JOIN queen.consumer_groups_metadata cgm
            ON cgm.consumer_group = pc.consumer_group
            AND (
                (cgm.queue_name = q.name AND cgm.partition_name = p.name)
                OR (cgm.queue_name = q.name AND cgm.partition_name = '')
                OR (cgm.queue_name = '' AND cgm.namespace = q.namespace)
                OR (cgm.queue_name = '' AND cgm.task = q.task)
            )
        LEFT JOIN queen.messages m ON m.partition_id = pc.partition_id
            AND (
                COALESCE(pc.last_consumed_created_at, cgm.subscription_timestamp) IS NULL
                OR (m.created_at, m.id) > (
                    COALESCE(pc.last_consumed_created_at, cgm.subscription_timestamp),
                    COALESCE(pc.last_consumed_id, '00000000-0000-0000-0000-000000000000'::uuid)
                )
            )
        GROUP BY pc.consumer_group, q.name, p.name, p.id, pc.worker_id, pc.last_consumed_at
    )
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'consumer_group', consumer_group,
            'queue_name', queue_name,
            'partition_name', partition_name,
            'partition_id', partition_id,
            'worker_id', worker_id,
            'offset_lag', unconsumed_count,
            'time_lag_seconds', lag_seconds,
            'lag_hours', ROUND(lag_seconds / 3600.0, 2),
            'oldest_unconsumed_at', CASE WHEN oldest_unconsumed_at IS NOT NULL
                THEN to_char(oldest_unconsumed_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
            'last_consumed_at', CASE WHEN last_consumed_at IS NOT NULL
                THEN to_char(last_consumed_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END
        ) ORDER BY lag_seconds DESC
    ), '[]'::jsonb) INTO v_rows
    FROM consumer_lag
    WHERE lag_seconds > p_min_lag_seconds AND lag_seconds IS NOT NULL;

    -- -------------------------------------------------------------- log (v2)
    WITH log_lag AS (
        SELECT
            c.consumer_group,
            q.name AS queue_name,
            p.name AS partition_name,
            p.id AS partition_id,
            c.worker_id,
            GREATEST(p.last_offset - GREATEST(c.committed, p.log_start - 1), 0) AS pending,
            (SELECT MIN(s.created_at) FROM queen.log_segments s
             WHERE s.partition_id = c.partition_id
               AND s.end_offset > c.committed) AS oldest_unconsumed_at
        FROM queen.log_consumers c
        JOIN queen.log_partitions p ON p.id = c.partition_id
        JOIN queen.log_queues q ON q.id = p.queue_id
    )
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'consumer_group', consumer_group,
            'queue_name', queue_name,
            'partition_name', partition_name,
            'partition_id', partition_id,
            'worker_id', worker_id,
            'offset_lag', pending,
            'time_lag_seconds', lag_seconds,
            'lag_hours', ROUND(lag_seconds / 3600.0, 2),
            'oldest_unconsumed_at', CASE WHEN oldest_unconsumed_at IS NOT NULL
                THEN to_char(oldest_unconsumed_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
            'last_consumed_at', NULL
        ) ORDER BY lag_seconds DESC
    ), '[]'::jsonb) INTO v_log
    FROM (
        SELECT ll.*,
               EXTRACT(EPOCH FROM (NOW() - ll.oldest_unconsumed_at))::integer AS lag_seconds
        FROM log_lag ll
    ) t
    WHERE lag_seconds > p_min_lag_seconds AND lag_seconds IS NOT NULL;

    RETURN v_rows || v_log;
END;
$$;

-- ============================================================================
-- queen.get_consumer_group_details_v1 — dual-mode redefinition (rows + LOG).
-- Folded in from the retired 099 (same reason as above: the 008 original joins
-- the rows partition tables only, so a log-engine group would render as {}).
-- Rows body verbatim from 008; log queues are merged in via jsonb `||` (queue
-- names cannot collide across engines — a queue is routed to exactly one).
-- lastConsumedAt is NULL for log partitions (no such column on log_consumers);
-- totalConsumed maps to log_consumers.total_consumed.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.get_consumer_group_details_v1(p_consumer_group TEXT)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows JSONB;
    v_log JSONB;
BEGIN
    -- ------------------------------------------------------------- rows (008)
    WITH partition_data AS (
        SELECT
            q.name as queue_name,
            p.name as partition_name,
            pc.worker_id,
            pc.last_consumed_at,
            pc.total_messages_consumed,
            pc.lease_expires_at,
            (
                SELECT COUNT(*)
                FROM queen.messages m
                WHERE m.partition_id = pc.partition_id
                  AND (
                      COALESCE(pc.last_consumed_created_at, cgm.subscription_timestamp) IS NULL
                      OR (m.created_at, m.id) > (
                          COALESCE(pc.last_consumed_created_at, cgm.subscription_timestamp),
                          COALESCE(pc.last_consumed_id, '00000000-0000-0000-0000-000000000000'::uuid)
                      )
                  )
            )::integer as offset_lag,
            (
                SELECT EXTRACT(EPOCH FROM (NOW() - m.created_at))::integer
                FROM queen.messages m
                WHERE m.partition_id = pc.partition_id
                  AND (
                      COALESCE(pc.last_consumed_created_at, cgm.subscription_timestamp) IS NULL
                      OR (m.created_at, m.id) > (
                          COALESCE(pc.last_consumed_created_at, cgm.subscription_timestamp),
                          COALESCE(pc.last_consumed_id, '00000000-0000-0000-0000-000000000000'::uuid)
                      )
                  )
                ORDER BY m.created_at ASC
                LIMIT 1
            )::integer as time_lag_seconds
        FROM queen.partition_consumers pc
        JOIN queen.partitions p ON p.id = pc.partition_id
        JOIN queen.queues q ON q.id = p.queue_id
        LEFT JOIN queen.consumer_groups_metadata cgm
            ON cgm.consumer_group = pc.consumer_group
            AND (
                (cgm.queue_name = q.name AND cgm.partition_name = p.name)
                OR (cgm.queue_name = q.name AND cgm.partition_name = '')
                OR (cgm.queue_name = '' AND cgm.namespace = q.namespace)
                OR (cgm.queue_name = '' AND cgm.task = q.task)
            )
        WHERE pc.consumer_group = p_consumer_group
        ORDER BY q.name, p.name
    )
    SELECT jsonb_object_agg(
        queue_name,
        (SELECT jsonb_build_object(
            'partitions', jsonb_agg(
                jsonb_build_object(
                    'partition', partition_name,
                    'workerId', worker_id,
                    'lastConsumedAt', CASE WHEN last_consumed_at IS NOT NULL
                        THEN to_char(last_consumed_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
                    'totalConsumed', total_messages_consumed,
                    'offsetLag', COALESCE(offset_lag, 0),
                    'timeLagSeconds', COALESCE(time_lag_seconds, 0),
                    'leaseActive', (lease_expires_at IS NOT NULL AND lease_expires_at > NOW())
                ) ORDER BY partition_name
            )
        ) FROM partition_data pd2 WHERE pd2.queue_name = pd.queue_name)
    ) INTO v_rows
    FROM (SELECT DISTINCT queue_name FROM partition_data) pd;

    -- -------------------------------------------------------------- log (v2)
    WITH log_data AS (
        SELECT
            q.name AS queue_name,
            p.name AS partition_name,
            c.worker_id,
            c.total_consumed,
            c.lease_expires_at,
            GREATEST(p.last_offset - GREATEST(c.committed, p.log_start - 1), 0)::bigint AS offset_lag,
            EXTRACT(EPOCH FROM (NOW() - (
                SELECT MIN(s.created_at) FROM queen.log_segments s
                WHERE s.partition_id = c.partition_id
                  AND s.end_offset > c.committed
            )))::integer AS time_lag_seconds
        FROM queen.log_consumers c
        JOIN queen.log_partitions p ON p.id = c.partition_id
        JOIN queen.log_queues q ON q.id = p.queue_id
        WHERE c.consumer_group = p_consumer_group
    )
    SELECT jsonb_object_agg(
        queue_name,
        (SELECT jsonb_build_object(
            'partitions', jsonb_agg(
                jsonb_build_object(
                    'partition', partition_name,
                    'workerId', worker_id,
                    'lastConsumedAt', NULL,
                    'totalConsumed', total_consumed,
                    'offsetLag', COALESCE(offset_lag, 0),
                    'timeLagSeconds', COALESCE(time_lag_seconds, 0),
                    'leaseActive', (lease_expires_at IS NOT NULL AND lease_expires_at > NOW())
                ) ORDER BY partition_name
            )
        ) FROM log_data ld2 WHERE ld2.queue_name = ld.queue_name)
    ) INTO v_log
    FROM (SELECT DISTINCT queue_name FROM log_data) ld;

    RETURN COALESCE(v_rows, '{}'::jsonb) || COALESCE(v_log, '{}'::jsonb);
END;
$$;

GRANT EXECUTE ON FUNCTION queen.get_lagging_partitions_v1(INTEGER) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_consumer_group_details_v1(TEXT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.log_delete_consumer_group_v1(TEXT, BOOLEAN) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.log_seek_one_v1(UUID, BIGINT, BIGINT, TEXT, BOOLEAN, TIMESTAMPTZ) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.log_seek_consumer_group_v1(TEXT, TEXT, BOOLEAN, TIMESTAMPTZ) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.log_seek_partition_v1(TEXT, TEXT, TEXT, BOOLEAN, TIMESTAMPTZ) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_consumer_groups_v4() TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.list_messages_v1(JSONB) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_dlq_messages_v1(JSONB) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.record_trace_v1(JSONB) TO PUBLIC;
