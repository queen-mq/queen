-- ============================================================================
-- Messages Stored Procedures
-- ============================================================================
-- Async stored procedures for message operations
-- ============================================================================

-- ============================================================================
-- queen.list_messages_v1: List messages with filters
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
    
    RETURN v_result;
END;
$$;

-- ============================================================================
-- queen.get_message_v1: Get single message detail
-- Note: Payload decryption must be handled in C++ layer
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.get_message_v1(p_partition_id UUID, p_transaction_id TEXT)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_result JSONB;
    v_has_queue_mode BOOLEAN;
    v_bus_groups_count INTEGER;
    v_consumed_by_groups INTEGER;
    v_queue_status TEXT;
    v_final_status TEXT;
BEGIN
    -- First, get mode info for this partition
    SELECT 
        bool_or(pc.consumer_group = '__QUEUE_MODE__'),
        COUNT(CASE WHEN pc.consumer_group != '__QUEUE_MODE__' THEN 1 END)::integer
    INTO v_has_queue_mode, v_bus_groups_count
    FROM queen.partition_consumers pc
    WHERE pc.partition_id = p_partition_id;
    
    v_has_queue_mode := COALESCE(v_has_queue_mode, false);
    v_bus_groups_count := COALESCE(v_bus_groups_count, 0);
    
    -- Get message with computed status
    SELECT jsonb_build_object(
        'id', m.id,
        'transactionId', m.transaction_id,
        'partitionId', m.partition_id,
        'queuePath', q.name || '/' || p.name,
        'queue', q.name,
        'partition', p.name,
        'namespace', q.namespace,
        'task', q.task,
        'status', CASE
            -- Dead letter takes priority
            WHEN dlq.message_id IS NOT NULL THEN 'dead_letter'
            -- If bus mode only (no queue mode), check if all consumer groups consumed it
            -- NOTE: Must use exact timestamp comparison (not DATE_TRUNC) to preserve microsecond precision
            WHEN NOT v_has_queue_mode AND v_bus_groups_count > 0 THEN
                CASE WHEN (
                    SELECT COUNT(*)::integer
                    FROM queen.partition_consumers pc
                    WHERE pc.partition_id = m.partition_id
                      AND pc.consumer_group != '__QUEUE_MODE__'
                      AND pc.last_consumed_created_at IS NOT NULL
                      AND (m.created_at, m.id) <= (pc.last_consumed_created_at, pc.last_consumed_id)
                ) = v_bus_groups_count THEN 'completed' ELSE 'pending' END
            -- Queue mode: check __QUEUE_MODE__ consumer
            -- NOTE: Must use exact timestamp comparison (not DATE_TRUNC) to preserve microsecond precision
            WHEN pc_queue.last_consumed_created_at IS NOT NULL AND 
                (m.created_at, m.id) <= (pc_queue.last_consumed_created_at, pc_queue.last_consumed_id)
            THEN 'completed'
            WHEN pc_queue.lease_expires_at IS NOT NULL AND pc_queue.lease_expires_at > NOW() THEN 'processing'
            ELSE 'pending'
        END,
        'payload', m.payload,
        'isEncrypted', COALESCE(m.is_encrypted, false),
        'traceId', m.trace_id,
        'producerSub', m.producer_sub,
        'createdAt', to_char(m.created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'errorMessage', dlq.error_message,
        'retryCount', dlq.retry_count,
        'leaseExpiresAt', CASE WHEN pc_queue.lease_expires_at IS NOT NULL 
            THEN to_char(pc_queue.lease_expires_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
        'queueConfig', jsonb_build_object(
            'leaseTime', q.lease_time,
            'retryLimit', q.retry_limit,
            'retryDelay', q.retry_delay,
            'ttl', q.ttl,
            'priority', q.priority
        ),
        'mode', jsonb_build_object(
            'hasQueueMode', v_has_queue_mode,
            'busGroupsCount', v_bus_groups_count,
            -- NOTE: Must use exact timestamp comparison (not DATE_TRUNC) to preserve microsecond precision
            'consumedByGroups', (
                SELECT COUNT(*)::integer
                FROM queen.partition_consumers pc
                WHERE pc.partition_id = m.partition_id
                  AND pc.consumer_group != '__QUEUE_MODE__'
                  AND pc.last_consumed_created_at IS NOT NULL
                  AND (m.created_at, m.id) <= (pc.last_consumed_created_at, pc.last_consumed_id)
            ),
            'type', CASE 
                WHEN NOT v_has_queue_mode AND v_bus_groups_count > 0 THEN 'bus'
                WHEN v_has_queue_mode AND v_bus_groups_count = 0 THEN 'queue'
                WHEN v_has_queue_mode AND v_bus_groups_count > 0 THEN 'hybrid'
                ELSE 'none'
            END
        ),
        -- NOTE: Must use exact timestamp comparison (not DATE_TRUNC) to preserve microsecond precision
        'consumerGroups', (
            SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'name', pc.consumer_group,
                'consumed', pc.last_consumed_created_at IS NOT NULL AND 
                    (m.created_at, m.id) <= (pc.last_consumed_created_at, pc.last_consumed_id),
                'leaseExpiresAt', CASE WHEN pc.lease_expires_at IS NOT NULL 
                    THEN to_char(pc.lease_expires_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END
            )), '[]'::jsonb)
            FROM queen.partition_consumers pc
            WHERE pc.partition_id = m.partition_id
        )
    ) INTO v_result
    FROM queen.messages m
    JOIN queen.partitions p ON p.id = m.partition_id
    JOIN queen.queues q ON q.id = p.queue_id
    LEFT JOIN queen.partition_consumers pc_queue ON pc_queue.partition_id = p.id AND pc_queue.consumer_group = '__QUEUE_MODE__'
    LEFT JOIN queen.dead_letter_queue dlq ON dlq.message_id = m.id
    WHERE m.partition_id = p_partition_id AND m.transaction_id = p_transaction_id;
    
    IF v_result IS NULL THEN
        RETURN jsonb_build_object('error', 'Message not found');
    END IF;
    
    RETURN v_result;
END;
$$;

-- ============================================================================
-- queen.delete_message_v1: Delete a message
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.delete_message_v1(p_partition_id UUID, p_transaction_id TEXT)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted BOOLEAN;
BEGIN
    WITH deleted AS (
        DELETE FROM queen.messages
        WHERE partition_id = p_partition_id AND transaction_id = p_transaction_id
        RETURNING id
    )
    SELECT EXISTS (SELECT 1 FROM deleted) INTO v_deleted;
    
    RETURN jsonb_build_object(
        'success', v_deleted,
        'partitionId', p_partition_id,
        'transactionId', p_transaction_id,
        'message', CASE WHEN v_deleted THEN 'Message deleted successfully' ELSE 'Message not found' END
    );
END;
$$;

-- ============================================================================
-- queen.get_dlq_messages_v1: Get dead letter queue messages
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
            'messages', COALESCE(jsonb_agg(
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
                ) ORDER BY dlq.failed_at DESC
            ), '[]'::jsonb),
            'pagination', jsonb_build_object(
                'limit', v_limit,
                'offset', v_offset
            )
        )
        FROM queen.dead_letter_queue dlq
        JOIN queen.messages m ON m.id = dlq.message_id
        JOIN queen.partitions p ON p.id = dlq.partition_id
        JOIN queen.queues q ON q.id = p.queue_id
        WHERE (v_queue IS NULL OR q.name = v_queue)
          AND (v_consumer_group IS NULL OR dlq.consumer_group = v_consumer_group)
        LIMIT v_limit OFFSET v_offset
    );
END;
$$;

-- Grant execute permissions
GRANT EXECUTE ON FUNCTION queen.list_messages_v1(JSONB) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_message_v1(UUID, TEXT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.delete_message_v1(UUID, TEXT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_dlq_messages_v1(JSONB) TO PUBLIC;

