-- ============================================================================
-- has_pending_messages: Fast check for available messages
-- ============================================================================
-- NOTE: Using CREATE OR REPLACE (no DROP) for zero-downtime deployments

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
BEGIN
    IF p_partition_name = '' THEN
        p_partition_name := NULL;
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