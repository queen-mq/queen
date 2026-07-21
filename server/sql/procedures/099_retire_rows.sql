-- ============================================================================
-- 099_retire_rows.sql — retire the ROWS storage engine (segments-only broker).
-- Applied LAST (lexical order) so its CREATE OR REPLACE redefinitions win over
-- the earlier dual/rows definitions, and its DROPs run after everything exists.
--
-- Two-layer principle (see the plan): the storage-agnostic COORDINATION layer is
-- PRESERVED — queen.queues, queen.partitions, queen.partition_consumers (now the
-- segment cursor after the fold), queen.consumer_watermarks,
-- queen.consumer_groups_metadata, queen.message_traces(+names),
-- queen.dead_letter_queue, queen.stats/stats_history, queen.system_state,
-- queen.worker_metrics*, queen_streams.* — all stay. Only the rows MESSAGE STORE
-- and its hot path are dropped:
--   * table queen.messages (replaced by queen.seg_segments)
--   * table queen.partition_lookup (candidate selection is now
--     seg_partitions.last_write_at + its index)
--   * table queen.messages_consumed (rows ack ledger)
--   * the rows push/pop/ack/txn/lease/lookup procedures
--
-- Kept COORDINATION/OBSERVABILITY functions that still read the dropped tables
-- are REDEFINED here to their segments-only form. Behavior is preserved because
-- the dropped tables were already ALWAYS EMPTY in a segments-only deployment, so
-- every rows contribution was already NULL/empty/0 — segment data comes from the
-- seg_* tables / partition_consumers cursor.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- (1) Segments-only redefinitions of kept functions that referenced the dropped
--     rows message store. CREATE OR REPLACE (this file applies last -> wins).
-- ----------------------------------------------------------------------------

-- compute_partition_stats_v3: the rows per-partition stats scan (queen.messages
-- + partition_lookup + messages_consumed). For segments it produced empty stats
-- (queen.messages is empty; live segment stats come from queen.seg_stats_v1), so
-- it becomes a no-op that returns the same JSONB shape refresh_all_stats_v1
-- folds into its result. No rows-table access remains.
CREATE OR REPLACE FUNCTION queen.compute_partition_stats_v3(p_force BOOLEAN DEFAULT FALSE)
RETURNS JSONB
LANGUAGE sql
AS $$
    SELECT jsonb_build_object(
        'updated', 0,
        'skipped', true,
        'reason', 'rows stats retired (segments-only); live stats via seg_stats_v1'
    );
$$;

-- delete_message_v1: the rows per-message delete (DELETE FROM queen.messages).
-- The segments broker's db::delete_message already handles the real deletion via
-- queen.seg_dlq separately, then folds in this function's result as
-- (seg_deleted OR rows_deleted). queen.messages was always EMPTY for segment
-- queues, so this always returned success=false — preserve that exactly without
-- touching the dropped table (live segment-message deletion is a seg concern,
-- unchanged here).
CREATE OR REPLACE FUNCTION queen.delete_message_v1(p_partition_id UUID, p_transaction_id TEXT)
RETURNS JSONB
LANGUAGE sql
AS $$
    SELECT jsonb_build_object(
        'success', false,
        'partitionId', p_partition_id,
        'transactionId', p_transaction_id,
        'message', 'Message not found'
    );
$$;

-- ===== record_trace_v1 =====
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

    -- Segment messages have no queen.messages row, so message_id is always NULL.
    -- (The former queen.messages lookup only ever returned NULL for segment
    -- queues and is omitted now that the rows engine is retired.)
    v_message_id := NULL;

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

-- ===== queen.get_traces_by_name_v1 =====
CREATE OR REPLACE FUNCTION queen.get_traces_by_name_v1(
    p_trace_name TEXT,
    p_limit INTEGER DEFAULT 100,
    p_offset INTEGER DEFAULT 0
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_traces JSONB;
    v_total INTEGER;
BEGIN
    -- Get total count first
    SELECT COUNT(*) INTO v_total
    FROM queen.message_trace_names mtn
    WHERE mtn.trace_name = p_trace_name;

    -- Get traces with pagination
    -- Segments-only: queen.messages is dropped (was always empty); message_payload is NULL.
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
            'message_payload', NULL::jsonb,
            'trace_names', COALESCE(
                (SELECT jsonb_agg(mtn2.trace_name ORDER BY mtn2.trace_name)
                 FROM queen.message_trace_names mtn2 WHERE mtn2.trace_id = mt.id),
                '[]'::jsonb
            )
        ) ORDER BY mt.created_at ASC
    ), '[]'::jsonb) INTO v_traces
    FROM queen.message_trace_names mtn
    JOIN queen.message_traces mt ON mtn.trace_id = mt.id
    LEFT JOIN queen.partitions p ON mt.partition_id = p.id
    LEFT JOIN queen.queues q ON p.queue_id = q.id
    WHERE mtn.trace_name = p_trace_name
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

-- ===== get_lagging_partitions_v1 =====
CREATE OR REPLACE FUNCTION queen.get_lagging_partitions_v1(p_min_lag_seconds INTEGER DEFAULT 0)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_result JSONB;
BEGIN
    -- Segments-only: the rows engine table queen.messages has been dropped.
    -- It was always EMPTY at runtime, so its LEFT JOIN yielded all-NULL m.*,
    -- making MIN(m.created_at)=NULL (oldest_unconsumed_at), COUNT(m.id)=0
    -- (unconsumed_count) and lag_seconds=NULL for every (partition,group).
    -- The final filter (lag_seconds IS NOT NULL) then discarded every row,
    -- yielding '[]'. This substitutes those empty-table contributions with
    -- their NULL/0 equivalents, preserving the exact RETURNS type and return
    -- shape without referencing the dropped table. The consumer_groups_metadata
    -- join is removed because it only fed the (now removed) queen.messages
    -- subscription-timestamp predicate.
    -- RUSTFIX item 22: real segment-native lag. The old body hardcoded NULL/0
    -- (the rows engine's queen.messages is gone) and joined the ROWS partition
    -- tables, which no longer match the seg cursor's partition_id — always []. This
    -- repoints to seg_partitions/seg_queues and computes pending frames + oldest
    -- unconsumed created_at from queen.seg_segments against the cursor (next_seq,
    -- next_off).
    WITH consumer_lag AS (
        SELECT
            pc.consumer_group,
            q.name as queue_name,
            p.name as partition_name,
            p.id as partition_id,
            pc.worker_id,
            pc.last_consumed_at,
            MIN(s.created_at) FILTER (
                WHERE s.seq >= pc.next_seq
                  AND s.msg_count > CASE WHEN s.seq = pc.next_seq THEN pc.next_off ELSE 0 END
            ) as oldest_unconsumed_at,
            COALESCE(SUM(CASE WHEN s.seq >= pc.next_seq
                              THEN s.msg_count - CASE WHEN s.seq = pc.next_seq THEN pc.next_off ELSE 0 END
                              ELSE 0 END), 0) as unconsumed_count,
            EXTRACT(EPOCH FROM (NOW() - MIN(s.created_at) FILTER (
                WHERE s.seq >= pc.next_seq
                  AND s.msg_count > CASE WHEN s.seq = pc.next_seq THEN pc.next_off ELSE 0 END
            )))::integer as lag_seconds
        FROM queen.partition_consumers pc
        JOIN queen.seg_partitions p ON p.id = pc.partition_id
        JOIN queen.seg_queues q ON q.id = p.queue_id
        LEFT JOIN queen.seg_segments s ON s.partition_id = pc.partition_id
        GROUP BY pc.consumer_group, q.name, p.name, p.id, pc.worker_id, pc.last_consumed_at,
                 pc.next_seq, pc.next_off
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
    ), '[]'::jsonb) INTO v_result
    FROM consumer_lag
    WHERE lag_seconds > p_min_lag_seconds AND lag_seconds IS NOT NULL;
    
    RETURN v_result;
END;
$$;

-- ===== get_consumer_group_details_v1 =====
CREATE OR REPLACE FUNCTION queen.get_consumer_group_details_v1(p_consumer_group TEXT)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_result JSONB;
BEGIN
    -- RUSTFIX item 22: repoint to the seg tables (the ROWS partition join matched
    -- ZERO rows for seg cursors → always {}) and compute real per-partition lag
    -- from queen.seg_segments against the cursor. The old consumer_groups_metadata
    -- LEFT JOIN was unused by the output and referenced seg-absent namespace/task
    -- columns, so it is removed.
    WITH partition_data AS (
        SELECT
            q.name as queue_name,
            p.name as partition_name,
            pc.worker_id,
            pc.last_consumed_at,
            pc.total_messages_consumed,
            pc.lease_expires_at,
            (SELECT COALESCE(SUM(CASE WHEN s.seq >= pc.next_seq
                                      THEN s.msg_count - CASE WHEN s.seq = pc.next_seq THEN pc.next_off ELSE 0 END
                                      ELSE 0 END), 0)
             FROM queen.seg_segments s WHERE s.partition_id = pc.partition_id)::integer as offset_lag,
            EXTRACT(EPOCH FROM (NOW() - (
                SELECT MIN(s.created_at) FROM queen.seg_segments s
                WHERE s.partition_id = pc.partition_id
                  AND s.seq >= pc.next_seq
                  AND s.msg_count > CASE WHEN s.seq = pc.next_seq THEN pc.next_off ELSE 0 END
            )))::integer as time_lag_seconds
        FROM queen.partition_consumers pc
        JOIN queen.seg_partitions p ON p.id = pc.partition_id
        JOIN queen.seg_queues q ON q.id = p.queue_id
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
    ) INTO v_result
    FROM (SELECT DISTINCT queue_name FROM partition_data) pd;
    
    RETURN COALESCE(v_result, '{}'::jsonb);
END;
$$;

-- ===== queen.get_consumer_groups_v4 =====
CREATE OR REPLACE FUNCTION queen.get_consumer_groups_v4()
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_result JSONB;
    v_v2 JSONB;
BEGIN
    -- ------------------------------------------------------------- rows (v1)
    -- Rows storage engine retired: queen.messages / queen.partition_lookup /
    -- queen.messages_consumed are dropped and were always empty. The rows
    -- branch filtered `WHERE q.storage IS DISTINCT FROM 'segments'`, so for a
    -- segments-only broker it always yielded the empty array. Collapse it to
    -- that constant so nothing references the dropped tables.
    v_result := '[]'::jsonb;

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
               l.oldest_unconsumed_at,
               -- RUSTFIX item 22/13: durable subscription registration.
               cgm.subscription_mode,
               cgm.subscription_timestamp,
               cgm.created_at AS subscription_created_at
        FROM queen.partition_consumers c
        JOIN queen.seg_partitions p ON p.id = c.partition_id
        JOIN queen.seg_queues q ON q.id = p.queue_id
        -- Strict guard: only queues currently routed to the segments engine.
        JOIN queen.queues quv ON quv.name = q.name AND quv.storage = 'segments'
        LEFT JOIN pend l ON l.partition_id = c.partition_id
                        AND l.consumer_group = c.consumer_group
        LEFT JOIN queen.consumer_groups_metadata cgm
               ON cgm.consumer_group = c.consumer_group
              AND cgm.queue_name = q.name
              AND cgm.partition_name = ''
    ),
    v2_aggregated AS (
        SELECT consumer_group,
               queue_name,
               COUNT(*) AS member_count,
               SUM(CASE WHEN pending > 0 THEN 1 ELSE 0 END) AS partitions_with_lag,
               SUM(pending) AS total_lag,
               COALESCE(MAX(EXTRACT(EPOCH FROM (NOW() - oldest_unconsumed_at))::integer), 0) AS max_time_lag,
               MAX(subscription_mode) AS subscription_mode,
               MAX(subscription_timestamp) AS subscription_timestamp,
               MAX(subscription_created_at) AS subscription_created_at,
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
            'subscriptionMode', subscription_mode,
            'subscriptionTimestamp', CASE WHEN subscription_timestamp IS NOT NULL
                THEN to_char(subscription_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
            'subscriptionCreatedAt', CASE WHEN subscription_created_at IS NOT NULL
                THEN to_char(subscription_created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END
        ) ORDER BY consumer_group, queue_name
    ), '[]'::jsonb) INTO v_v2
    FROM v2_aggregated;

    RETURN v_result || v_v2;
END;
$$;

-- ===== queen.list_messages_v1 =====
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

    -- Mode summary from the SEGMENTS consumers (the rows engine is retired; the
    -- old inlined NULL/0 made every segment queue report mode 'none', so the
    -- webapp rendered bus queues as if nothing ever consumed them).
    SELECT bool_or(pc.consumer_group = '__QUEUE_MODE__'),
           COUNT(DISTINCT pc.consumer_group)
               FILTER (WHERE pc.consumer_group <> '__QUEUE_MODE__')::integer
    INTO v_has_queue_mode, v_total_bus_groups
    FROM queen.partition_consumers pc
    JOIN queen.seg_partitions p ON p.id = pc.partition_id
    JOIN queen.seg_queues q ON q.id = p.queue_id
    LEFT JOIN queen.queues quv ON quv.name = q.name
    WHERE (v_queue IS NULL OR q.name = v_queue)
      AND (v_partition IS NULL OR p.name = v_partition)
      AND (v_namespace IS NULL OR quv.namespace = v_namespace)
      AND (v_task IS NULL OR quv.task = v_task);

    v_result := jsonb_build_object(
        'messages', '[]'::jsonb,
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
    );

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
            -- Dual-mode status (same rule as the message-detail endpoint):
            --   dead_letter  a seg_dlq snapshot exists for this position
            --   completed    BUS queue: every named group's cursor passed it;
            --                QUEUE mode: the __QUEUE_MODE__ cursor passed it
            --   processing   any live lease on the partition
            --   pending      otherwise
            -- (The old code compared against the __QUEUE_MODE__ cursor only, so
            -- bus-consumed messages showed 'pending' forever even though every
            -- group had completed them.)
            SELECT vb.*,
                   CASE
                       WHEN vb.is_dlq THEN 'dead_letter'
                       WHEN (vb.total_bus_groups > 0 AND vb.consumed_by_groups = vb.total_bus_groups)
                            OR (vb.total_bus_groups = 0 AND vb.qmode_passed)
                           THEN 'completed'
                       WHEN vb.any_live_lease THEN 'processing'
                       ELSE 'pending'
                   END AS final_status
            FROM (
                SELECT d.message_id, d.txn_hash, d.seq, d.frame_idx, d.created_at,
                       p.id AS partition_id, p.name AS partition_name,
                       q.name AS queue_name,
                       quv.namespace, quv.task, quv.priority AS queue_priority,
                       c.lease_expires_at,
                       EXISTS (SELECT 1 FROM queen.seg_dlq dl
                               WHERE dl.partition_id = d.partition_id
                                 AND dl.seq = d.seq AND dl.frame_idx = d.frame_idx) AS is_dlq,
                       -- Frame consumed iff (seq, frame_idx) < cursor (next_seq, next_off).
                       (c.next_seq IS NOT NULL
                        AND (d.seq, d.frame_idx) < (c.next_seq, c.next_off)) AS qmode_passed,
                       EXISTS (SELECT 1 FROM queen.partition_consumers cl
                               WHERE cl.partition_id = d.partition_id
                                 AND cl.lease_expires_at IS NOT NULL
                                 AND cl.lease_expires_at > NOW()) AS any_live_lease,
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
            ) vb
        ) vd
        WHERE (v_status IS NULL OR vd.final_status = v_status)
        ORDER BY vd.created_at DESC, vd.message_id DESC
        LIMIT v_limit
    ) v2;

    -- Segments-only deployments: the rows list is always empty, so the final
    -- message set is exactly v_v2 (appended here to preserve the return shape).
    IF jsonb_array_length(v_v2) > 0 THEN
        v_result := jsonb_set(v_result, '{messages}',
                              (v_result->'messages') || v_v2);
    END IF;

    RETURN v_result;
END;
$$;

-- ===== get_dlq_messages_v1 =====
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
            -- segments engine (queen.seg_dlq payload snapshots)
            SELECT d2.failed_at,
                   jsonb_build_object(
                       'id', d2.id,
                       'transactionId', d2.transaction_id,
                       'partitionId', d2.partition_id,
                       'queue', q2q.name,
                       'partition', p2.name,
                       'consumerGroup', d2.consumer_group,
                       'errorMessage', d2.error,
                       'retryCount', COALESCE(d2.retry_count, 0),
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

-- ----------------------------------------------------------------------------
-- (2) Drop the rows hot-path procedures (none are called by the segments
--     broker). All overloads of each name are dropped by regprocedure.
-- ----------------------------------------------------------------------------
DO $$
DECLARE
    r RECORD;
    v_names TEXT[] := ARRAY[
        'push_messages_v2', 'push_messages_v3',
        'pop_unified_batch', 'pop_unified_batch_v2', 'pop_unified_batch_v3', 'pop_unified_batch_v4',
        'pop_specific_batch', 'pop_discover_batch',
        'ack_messages_v2',
        'execute_transaction_v2',
        'renew_lease_v2',
        'lock_push_partition',
        'update_partition_lookup_v1', 'reconcile_partition_lookup_v1',
        -- rows-based streams cycle (superseded by seg_streams_cycle_v1 in 029;
        -- also the last remaining writer of rows-partition ids into
        -- partition_consumers, which the FK in (5) below forbids)
        'streams_cycle_v1'
    ];
BEGIN
    FOR r IN
        SELECT p.oid::regprocedure AS sig
        FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'queen' AND p.proname = ANY(v_names)
    LOOP
        EXECUTE 'DROP FUNCTION IF EXISTS ' || r.sig::text || ' CASCADE';
    END LOOP;
END $$;

-- ----------------------------------------------------------------------------
-- (3) Drop the rows message store + rows-only tables. CASCADE removes the FK
--     constraints that pointed INTO queen.messages (queen.message_traces.message_id,
--     queen.dead_letter_queue.message_id) — the referencing TABLES are preserved,
--     only their now-dangling FK constraints go.
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS queen.messages CASCADE;
DROP TABLE IF EXISTS queen.partition_lookup CASCADE;
DROP TABLE IF EXISTS queen.messages_consumed CASCADE;

-- ----------------------------------------------------------------------------
-- (4) Segments is the only engine: make it the default and normalize any
--     existing rows.
-- ----------------------------------------------------------------------------
ALTER TABLE queen.queues ALTER COLUMN storage SET DEFAULT 'segments';
UPDATE queen.queues SET storage = 'segments' WHERE storage IS DISTINCT FROM 'segments';

-- ----------------------------------------------------------------------------
-- (5) partition_consumers is segments-only now. Every surviving writer keys it
--     by queen.seg_partitions(id) — the rows-engine writers (pop/ack/transaction
--     and streams_cycle_v1) were dropped in (2), so the FK the coordination fold
--     had to remove in 023 (when the column could reference either engine's
--     partition table) is valid again, pointing at the segments table this time.
--     Restores the ON DELETE CASCADE the rows engine used to provide: deleting a
--     seg queue/partition now cleans its cursors without relying on the explicit
--     delete paths (which remain as belt-and-braces).
--
--     Guarded so the orphan purge + full-table FK validation run only once; the
--     purge removes leftover rows-era cursor rows (their partition ids live in
--     the retired queen.partitions UUID space and can never match).
-- ----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'partition_consumers_seg_partition_fkey'
          AND conrelid = 'queen.partition_consumers'::regclass
    ) THEN
        DELETE FROM queen.partition_consumers pc
        WHERE NOT EXISTS (
            SELECT 1 FROM queen.seg_partitions sp WHERE sp.id = pc.partition_id
        );
        ALTER TABLE queen.partition_consumers
            ADD CONSTRAINT partition_consumers_seg_partition_fkey
            FOREIGN KEY (partition_id) REFERENCES queen.seg_partitions(id)
            ON DELETE CASCADE;
    END IF;
END $$;
