-- ============================================================================
-- Consumer Group Stored Procedures
-- ============================================================================
-- Async stored procedures for consumer group management and analytics
-- ============================================================================

-- ============================================================================
-- queen.get_consumer_groups_v1: List all consumer groups with stats
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.get_consumer_groups_v1()
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_result JSONB;
BEGIN
    WITH consumer_data AS (
        SELECT 
            pc.consumer_group,
            q.name as queue_name,
            p.name as partition_name,
            pc.worker_id,
            pc.last_consumed_at,
            pc.last_consumed_id,
            pc.last_consumed_created_at,
            pc.total_messages_consumed,
            pc.total_batches_consumed,
            pc.lease_expires_at,
            pc.lease_acquired_at,
            -- Calculate lag: count messages after last consumed
            -- NOTE: Must use exact timestamp comparison (not DATE_TRUNC) to preserve microsecond precision
            -- Uses COALESCE to respect subscription_timestamp for "new" mode consumers
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
            -- Calculate time lag: age of oldest unprocessed message
            -- NOTE: Must use exact timestamp comparison (not DATE_TRUNC) to preserve microsecond precision
            -- Uses COALESCE to respect subscription_timestamp for "new" mode consumers
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
            )::integer as time_lag_seconds,
            -- Join subscription metadata
            cgm.subscription_mode,
            cgm.subscription_timestamp,
            cgm.created_at as subscription_created_at
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
    ),
    aggregated AS (
        SELECT 
            consumer_group,
            queue_name,
            COUNT(*) as member_count,
            COALESCE(SUM(offset_lag), 0) as total_lag,
            COALESCE(MAX(time_lag_seconds), 0) as max_time_lag,
            MAX(subscription_mode) as subscription_mode,
            MAX(subscription_timestamp) as subscription_timestamp,
            MAX(subscription_created_at) as subscription_created_at,
            -- State determination
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
            'totalLag', total_lag,
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
    
    RETURN v_result;
END;
$$;

-- ============================================================================
-- queen.get_consumer_group_details_v1: Get detailed info for a consumer group
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.get_consumer_group_details_v1(p_consumer_group TEXT)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_result JSONB;
BEGIN
    WITH partition_data AS (
        SELECT 
            q.name as queue_name,
            p.name as partition_name,
            pc.worker_id,
            pc.last_consumed_at,
            pc.total_messages_consumed,
            pc.lease_expires_at,
            -- Calculate lag
            -- NOTE: Must use exact timestamp comparison (not DATE_TRUNC) to preserve microsecond precision
            -- Uses COALESCE to respect subscription_timestamp for "new" mode consumers
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
            -- Calculate time lag
            -- NOTE: Must use exact timestamp comparison (not DATE_TRUNC) to preserve microsecond precision
            -- Uses COALESCE to respect subscription_timestamp for "new" mode consumers
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
    ) INTO v_result
    FROM (SELECT DISTINCT queue_name FROM partition_data) pd;
    
    RETURN COALESCE(v_result, '{}'::jsonb);
END;
$$;

-- ============================================================================
-- queen.get_lagging_partitions_v1: Get partitions with significant lag
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.get_lagging_partitions_v1(p_min_lag_seconds INTEGER DEFAULT 0)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_result JSONB;
BEGIN
    WITH consumer_lag AS (
        SELECT 
            pc.consumer_group,
            q.name as queue_name,
            p.name as partition_name,
            p.id as partition_id,
            pc.worker_id,
            pc.last_consumed_at,
            -- Find oldest unconsumed message
            -- Uses COALESCE to respect subscription_timestamp for "new" mode consumers
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
        -- NOTE: Must use exact timestamp comparison (not DATE_TRUNC) to preserve microsecond precision
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
    ), '[]'::jsonb) INTO v_result
    FROM consumer_lag
    WHERE lag_seconds > p_min_lag_seconds AND lag_seconds IS NOT NULL;
    
    RETURN v_result;
END;
$$;

-- ============================================================================
-- queen.delete_consumer_group_v1: Delete consumer group and optionally metadata
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.delete_consumer_group_v1(
    p_consumer_group TEXT,
    p_delete_metadata BOOLEAN DEFAULT TRUE
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted_count INTEGER;
BEGIN
    -- Delete partition consumers
    DELETE FROM queen.partition_consumers
    WHERE consumer_group = p_consumer_group;

    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;

    -- Delete metadata if requested
    IF p_delete_metadata THEN
        DELETE FROM queen.consumer_groups_metadata
        WHERE consumer_group = p_consumer_group;
    END IF;

    -- Delete watermarks to prevent stale watermark from blocking re-consumption
    -- if a new consumer group with the same name is created later
    DELETE FROM queen.consumer_watermarks
    WHERE consumer_group = p_consumer_group;

    RETURN jsonb_build_object(
        'success', true,
        'consumerGroup', p_consumer_group,
        'deletedPartitions', v_deleted_count,
        'metadataDeleted', p_delete_metadata
    );
END;
$$;

-- ============================================================================
-- queen.update_consumer_group_subscription_v1: Update subscription timestamp
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.update_consumer_group_subscription_v1(
    p_consumer_group TEXT,
    p_new_timestamp TEXT
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_updated_count INTEGER;
BEGIN
    UPDATE queen.consumer_groups_metadata
    SET subscription_timestamp = p_new_timestamp::timestamptz
    WHERE consumer_group = p_consumer_group;

    GET DIAGNOSTICS v_updated_count = ROW_COUNT;

    -- Invalidate watermarks for this consumer group so a backward move of the
    -- subscription timestamp re-exposes historical partitions to the wildcard
    -- POP scan filter (pl.updated_at >= watermark - 2m). Without this, a stale
    -- watermark from a previously caught-up run would block the replay
    -- silently. Symmetric with the watermark cleanup already done in
    -- queen.seek_consumer_group_v1 (backward seek) and
    -- queen.delete_consumer_group_v1.
    --
    -- Deleted unconditionally (forward moves too) because:
    --   - Forward moves: the watermark is automatically re-established on
    --     the next empty scan within 30s; transient extra work is negligible.
    --   - Backward moves: deletion is REQUIRED for correctness.
    --   - Unconditional deletion removes a foot-gun and matches the
    --     idempotent always-safe pattern of sibling procedures.
    --
    -- Scope mirrors the UPDATE above (no queue_name filter): all
    -- (queue_name, consumer_group) entries for this CG. See
    -- cdocs/WATERMARK_AND_TRANSACTION_FIXES.md §2 (Fix B).
    DELETE FROM queen.consumer_watermarks
    WHERE consumer_group = p_consumer_group;

    RETURN jsonb_build_object(
        'success', true,
        'consumerGroup', p_consumer_group,
        'newTimestamp', p_new_timestamp,
        'rowsUpdated', v_updated_count
    );
END;
$$;

-- ============================================================================
-- queen.delete_consumer_group_for_queue_v1: Delete consumer group for specific queue only
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.delete_consumer_group_for_queue_v1(
    p_consumer_group TEXT,
    p_queue_name TEXT,
    p_delete_metadata BOOLEAN DEFAULT TRUE
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted_count INTEGER;
BEGIN
    -- Delete partition consumers only for this specific queue
    DELETE FROM queen.partition_consumers pc
    USING queen.partitions p
    JOIN queen.queues q ON q.id = p.queue_id
    WHERE pc.partition_id = p.id
      AND pc.consumer_group = p_consumer_group
      AND q.name = p_queue_name;

    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;

    -- Delete metadata if requested (only for this queue)
    IF p_delete_metadata THEN
        DELETE FROM queen.consumer_groups_metadata
        WHERE consumer_group = p_consumer_group
          AND queue_name = p_queue_name;
    END IF;

    -- Delete watermark for this specific (queue, consumer_group) to prevent
    -- stale watermark from blocking re-consumption if CG is recreated
    DELETE FROM queen.consumer_watermarks
    WHERE queue_name = p_queue_name
      AND consumer_group = p_consumer_group;

    RETURN jsonb_build_object(
        'success', true,
        'consumerGroup', p_consumer_group,
        'queueName', p_queue_name,
        'deletedPartitions', v_deleted_count,
        'metadataDeleted', p_delete_metadata
    );
END;
$$;

-- ============================================================================
-- queen.seek_consumer_group_v1: Move cursor to end (skip all) or specific timestamp
-- ============================================================================
-- Usage:
--   seek_to_end = true: Move cursor to latest message (skip all pending)
--   seek_to_end = false + target_timestamp: Move cursor to message at/before timestamp
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.seek_consumer_group_v1(
    p_consumer_group TEXT,
    p_queue_name TEXT,
    p_target_timestamp TIMESTAMPTZ DEFAULT NULL,
    p_seek_to_end BOOLEAN DEFAULT FALSE
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_partition RECORD;
    v_updated_count INTEGER := 0;
    v_target_msg RECORD;
BEGIN
    -- Validate parameters
    IF NOT p_seek_to_end AND p_target_timestamp IS NULL THEN
        RETURN jsonb_build_object(
            'success', false,
            'error', 'Must specify either seek_to_end=true or a target_timestamp'
        );
    END IF;
    
    -- Process each partition for this queue/consumer_group
    FOR v_partition IN
        SELECT pc.id as pc_id, pc.partition_id, p.name as partition_name
        FROM queen.partition_consumers pc
        JOIN queen.partitions p ON p.id = pc.partition_id
        JOIN queen.queues q ON q.id = p.queue_id
        WHERE pc.consumer_group = p_consumer_group
          AND q.name = p_queue_name
    LOOP
        IF p_seek_to_end THEN
            -- Move to end: use partition_lookup for latest message
            SELECT pl.last_message_id, pl.last_message_created_at
            INTO v_target_msg
            FROM queen.partition_lookup pl
            WHERE pl.partition_id = v_partition.partition_id;
            
            IF v_target_msg.last_message_id IS NOT NULL THEN
                UPDATE queen.partition_consumers
                SET last_consumed_id = v_target_msg.last_message_id,
                    last_consumed_created_at = v_target_msg.last_message_created_at,
                    last_consumed_at = NOW(),
                    -- Release any active lease
                    lease_expires_at = NULL,
                    lease_acquired_at = NULL,
                    worker_id = NULL,
                    batch_size = 0,
                    acked_count = 0
                WHERE id = v_partition.pc_id;
                v_updated_count := v_updated_count + 1;
            END IF;
        ELSE
            -- Move to specific timestamp: find the last message at or before timestamp
            SELECT m.id, m.created_at
            INTO v_target_msg
            FROM queen.messages m
            WHERE m.partition_id = v_partition.partition_id
              AND m.created_at <= p_target_timestamp
            ORDER BY m.created_at DESC, m.id DESC
            LIMIT 1;
            
            IF v_target_msg.id IS NOT NULL THEN
                -- Move cursor to this message (consumer will start AFTER this message)
                UPDATE queen.partition_consumers
                SET last_consumed_id = v_target_msg.id,
                    last_consumed_created_at = v_target_msg.created_at,
                    last_consumed_at = NOW(),
                    -- Release any active lease
                    lease_expires_at = NULL,
                    lease_acquired_at = NULL,
                    worker_id = NULL,
                    batch_size = 0,
                    acked_count = 0
                WHERE id = v_partition.pc_id;
                v_updated_count := v_updated_count + 1;
            ELSE
                -- No message at or before timestamp - reset to beginning
                UPDATE queen.partition_consumers
                SET last_consumed_id = '00000000-0000-0000-0000-000000000000',
                    last_consumed_created_at = NULL,
                    last_consumed_at = NOW(),
                    lease_expires_at = NULL,
                    lease_acquired_at = NULL,
                    worker_id = NULL,
                    batch_size = 0,
                    acked_count = 0
                WHERE id = v_partition.pc_id;
                v_updated_count := v_updated_count + 1;
            END IF;
        END IF;
    END LOOP;

    -- When seeking backwards (not to end), delete the watermark to allow
    -- re-consumption of messages. The watermark optimization tracks "last empty scan"
    -- and would otherwise filter out partitions that haven't been updated recently.
    -- Seeking to end doesn't need this since the consumer is caught up anyway.
    IF NOT p_seek_to_end THEN
        DELETE FROM queen.consumer_watermarks
        WHERE queue_name = p_queue_name
          AND consumer_group = p_consumer_group;
    END IF;

    RETURN jsonb_build_object(
        'success', true,
        'consumerGroup', p_consumer_group,
        'queueName', p_queue_name,
        'seekToEnd', p_seek_to_end,
        'targetTimestamp', CASE WHEN p_target_timestamp IS NOT NULL
            THEN to_char(p_target_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
        'partitionsUpdated', v_updated_count
    );
END;
$$;

-- ============================================================================
-- queen.get_consumer_groups_v2: Optimized version using pre-computed stats
-- ============================================================================
-- Uses queen.stats table instead of expensive COUNT(*) on messages table
-- ~45x faster than v1 (1-4ms vs 50-150ms)
-- Falls back to message query only when oldest_pending_at is NULL
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.get_consumer_groups_v2()
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_result JSONB;
BEGIN
    WITH consumer_data AS (
        SELECT 
            pc.consumer_group,
            q.name as queue_name,
            p.name as partition_name,
            pc.partition_id,
            pc.worker_id,
            pc.last_consumed_at,
            pc.last_consumed_id,
            pc.last_consumed_created_at,
            pc.total_messages_consumed,
            pc.total_batches_consumed,
            pc.lease_expires_at,
            pc.lease_acquired_at,
            -- Use pre-computed stats instead of expensive COUNT(*)
            COALESCE(s.pending_messages, 0)::integer as offset_lag,
            -- Use oldest_pending_at for time lag (with fallback for NULL cases)
            -- Respects subscription_timestamp for "new" mode consumers
            CASE 
                WHEN s.oldest_pending_at IS NOT NULL THEN
                    EXTRACT(EPOCH FROM (NOW() - s.oldest_pending_at))::integer
                WHEN s.pending_messages > 0 AND pc.last_consumed_created_at IS NULL AND cgm.subscription_timestamp IS NOT NULL THEN
                    -- "new" mode consumer never consumed: use oldest message AFTER subscription
                    (SELECT EXTRACT(EPOCH FROM (NOW() - MIN(m.created_at)))::integer
                     FROM queen.messages m WHERE m.partition_id = pc.partition_id
                       AND (m.created_at, m.id) > (cgm.subscription_timestamp, '00000000-0000-0000-0000-000000000000'::uuid))
                WHEN s.pending_messages > 0 AND pc.last_consumed_created_at IS NULL THEN
                    -- Consumer never consumed, no subscription: use oldest message in partition
                    (SELECT EXTRACT(EPOCH FROM (NOW() - MIN(m.created_at)))::integer
                     FROM queen.messages m WHERE m.partition_id = pc.partition_id)
                ELSE NULL
            END as time_lag_seconds,
            -- Join subscription metadata
            cgm.subscription_mode,
            cgm.subscription_timestamp,
            cgm.created_at as subscription_created_at
        FROM queen.partition_consumers pc
        JOIN queen.partitions p ON p.id = pc.partition_id
        JOIN queen.queues q ON q.id = p.queue_id
        LEFT JOIN queen.stats s 
            ON s.stat_type = 'partition' 
           AND s.partition_id = pc.partition_id 
           AND s.consumer_group = pc.consumer_group
        LEFT JOIN queen.consumer_groups_metadata cgm 
            ON cgm.consumer_group = pc.consumer_group
            AND (
                (cgm.queue_name = q.name AND cgm.partition_name = p.name)
                OR (cgm.queue_name = q.name AND cgm.partition_name = '')
                OR (cgm.queue_name = '' AND cgm.namespace = q.namespace)
                OR (cgm.queue_name = '' AND cgm.task = q.task)
            )
    ),
    aggregated AS (
        SELECT 
            consumer_group,
            queue_name,
            COUNT(*) as member_count,
            COALESCE(SUM(offset_lag), 0) as total_lag,
            COALESCE(MAX(time_lag_seconds), 0) as max_time_lag,
            MAX(subscription_mode) as subscription_mode,
            MAX(subscription_timestamp) as subscription_timestamp,
            MAX(subscription_created_at) as subscription_created_at,
            -- State determination
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
            'totalLag', total_lag,
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
    
    RETURN v_result;
END;
$$;

-- ============================================================================
-- queen.get_consumer_groups_v3: Fast version using partition_lookup metadata
-- ============================================================================
-- Uses partition_lookup.last_message_* vs partition_consumers.last_consumed_*
-- to detect lag WITHOUT scanning the messages table.
-- 
-- Trade-offs vs v1:
--   - totalLag: Not available (would require COUNT on messages)
--   - maxTimeLag: Age of newest unconsumed message (lower bound for actual lag)
--                 Meaning: "consumer is at least X seconds behind latest"
--   - partitionsWithLag: NEW - count of partitions that are behind
--   - Speed: ~50x faster (1-5ms vs 50-150ms)
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.get_consumer_groups_v3()
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_result JSONB;
BEGIN
    WITH consumer_data AS (
        SELECT 
            pc.consumer_group,
            q.name as queue_name,
            p.name as partition_name,
            pc.worker_id,
            pc.last_consumed_at,
            pc.last_consumed_id,
            pc.last_consumed_created_at,
            pc.total_messages_consumed,
            pc.total_batches_consumed,
            pc.lease_expires_at,
            pc.lease_acquired_at,
            -- Detect if partition has unconsumed messages (fast comparison)
            -- Uses COALESCE to respect subscription_timestamp for "new" mode consumers
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
            -- Approximate time lag: age of newest unconsumed message (lower bound for actual lag)
            -- This tells you "consumer is at least X seconds behind the latest message"
            -- Respects subscription_timestamp for "new" mode consumers
            CASE 
                WHEN pl.last_message_id IS NULL THEN NULL  -- Empty partition
                WHEN pc.last_consumed_created_at IS NOT NULL THEN
                    CASE WHEN (pl.last_message_created_at, pl.last_message_id) > 
                              (pc.last_consumed_created_at, pc.last_consumed_id)
                         THEN EXTRACT(EPOCH FROM (NOW() - pl.last_message_created_at))::integer
                         ELSE 0 END
                WHEN cgm.subscription_timestamp IS NOT NULL THEN
                    -- "new" mode: only show lag if latest message is after subscription
                    CASE WHEN (pl.last_message_created_at, pl.last_message_id) > 
                              (cgm.subscription_timestamp, '00000000-0000-0000-0000-000000000000'::uuid)
                         THEN EXTRACT(EPOCH FROM (NOW() - pl.last_message_created_at))::integer
                         ELSE 0 END
                ELSE 
                    -- Never consumed, no subscription: use age of newest message
                    EXTRACT(EPOCH FROM (NOW() - pl.last_message_created_at))::integer
            END as time_lag_seconds,
            -- Join subscription metadata
            cgm.subscription_mode,
            cgm.subscription_timestamp,
            cgm.created_at as subscription_created_at
        FROM queen.partition_consumers pc
        JOIN queen.partitions p ON p.id = pc.partition_id
        JOIN queen.queues q ON q.id = p.queue_id
        LEFT JOIN queen.partition_lookup pl ON pl.partition_id = pc.partition_id
        LEFT JOIN queen.consumer_groups_metadata cgm 
            ON cgm.consumer_group = pc.consumer_group
            AND (
                (cgm.queue_name = q.name AND cgm.partition_name = p.name)
                OR (cgm.queue_name = q.name AND cgm.partition_name = '')
                OR (cgm.queue_name = '' AND cgm.namespace = q.namespace)
                OR (cgm.queue_name = '' AND cgm.task = q.task)
            )
    ),
    aggregated AS (
        SELECT 
            consumer_group,
            queue_name,
            COUNT(*) as member_count,
            -- Count partitions with lag (instead of total message count)
            SUM(CASE WHEN has_lag THEN 1 ELSE 0 END) as partitions_with_lag,
            COALESCE(MAX(time_lag_seconds), 0) as max_time_lag,
            MAX(subscription_mode) as subscription_mode,
            MAX(subscription_timestamp) as subscription_timestamp,
            MAX(subscription_created_at) as subscription_created_at,
            -- State determination
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
    
    RETURN v_result;
END;
$$;

-- ============================================================================
-- queen.get_consumer_groups_v4: Optimized v3 with pre-resolved metadata
-- ============================================================================
-- Same logic as v3 but resolves consumer_groups_metadata per
-- (consumer_group, queue) FIRST (~26 rows), then joins with partition_consumers.
-- This eliminates the cartesian explosion in the metadata LEFT JOIN that caused
-- v3 to evaluate millions of row pairs through a complex OR filter.
--
-- Performance: ~10x faster than v3 at scale (400ms vs 4000ms at 128k+ partitions)
-- Output: Identical JSON structure to v3
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.get_consumer_groups_v4()
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_result JSONB;
BEGIN
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
    
    RETURN v_result;
END;
$$;

-- ============================================================================
-- queen.seek_partition_v1: Move cursor to end for a SINGLE partition
-- ============================================================================
-- Advances the cursor for one specific partition to the latest message,
-- effectively skipping all pending messages on that partition.
-- Used from the "lagging partitions" UI to fix stuck/stale partitions.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.seek_partition_v1(
    p_consumer_group TEXT,
    p_queue_name TEXT,
    p_partition_name TEXT
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_partition_id UUID;
    v_last_msg_id UUID;
    v_last_msg_ts TIMESTAMPTZ;
    v_updated BOOLEAN := FALSE;
BEGIN
    -- Find the partition
    SELECT p.id INTO v_partition_id
    FROM queen.partitions p
    JOIN queen.queues q ON q.id = p.queue_id
    WHERE p.name = p_partition_name
      AND q.name = p_queue_name;
    
    IF v_partition_id IS NULL THEN
        RETURN jsonb_build_object(
            'success', false,
            'error', 'Partition not found'
        );
    END IF;
    
    -- Get the latest message from partition_lookup
    SELECT pl.last_message_id, pl.last_message_created_at
    INTO v_last_msg_id, v_last_msg_ts
    FROM queen.partition_lookup pl
    WHERE pl.partition_id = v_partition_id;
    
    IF v_last_msg_id IS NOT NULL THEN
        -- Advance cursor to latest message
        UPDATE queen.partition_consumers
        SET last_consumed_id = v_last_msg_id,
            last_consumed_created_at = v_last_msg_ts,
            last_consumed_at = NOW(),
            lease_expires_at = NULL,
            lease_acquired_at = NULL,
            worker_id = NULL,
            batch_size = 0,
            acked_count = 0
        WHERE partition_id = v_partition_id
          AND consumer_group = p_consumer_group;
        
        v_updated := FOUND;
    END IF;
    
    RETURN jsonb_build_object(
        'success', true,
        'consumerGroup', p_consumer_group,
        'queueName', p_queue_name,
        'partitionName', p_partition_name,
        'updated', v_updated,
        'cursorMovedTo', CASE WHEN v_last_msg_ts IS NOT NULL 
            THEN to_char(v_last_msg_ts, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END
    );
END;
$$;

-- Grant execute permissions
GRANT EXECUTE ON FUNCTION queen.get_consumer_groups_v1() TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_consumer_groups_v2() TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_consumer_groups_v3() TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_consumer_groups_v4() TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_consumer_group_details_v1(TEXT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_lagging_partitions_v1(INTEGER) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.delete_consumer_group_v1(TEXT, BOOLEAN) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.update_consumer_group_subscription_v1(TEXT, TEXT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.delete_consumer_group_for_queue_v1(TEXT, TEXT, BOOLEAN) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.seek_consumer_group_v1(TEXT, TEXT, TIMESTAMPTZ, BOOLEAN) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.seek_partition_v1(TEXT, TEXT, TEXT) TO PUBLIC;

