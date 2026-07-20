-- ============================================================================
-- ack_messages_v2: Batch ACK with deadlock prevention
-- ============================================================================
-- NOTE: Using CREATE OR REPLACE (no DROP) for zero-downtime deployments
--
-- OPTIMIZATIONS (v2.1):
-- 1. Batch message lookup, lease validation, and metadata into single CTE
-- 2. Replace O(N²) JSONB concatenation with O(N) array append
-- 3. Only query retry state when needed ('failed' status)
-- 4. Preserve all deadlock prevention and atomic update guarantees

CREATE OR REPLACE FUNCTION queen.ack_messages_v2(
    p_acknowledgments JSONB
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_ack RECORD;
    v_success BOOLEAN;
    v_error TEXT;
    v_lease_released BOOLEAN;
    v_dlq BOOLEAN;
    v_batch_retry_count INT;
    v_retry_limit INT;
    v_result_rows JSONB[] := ARRAY[]::JSONB[];
BEGIN
    -- Process each ACK with batch-fetched data
    -- Sort by partitionId + transactionId to prevent deadlocks
    FOR v_ack IN
        WITH ack_data AS (
            SELECT 
                (value->>'index')::int AS idx,
                value->>'transactionId' AS txn_id,
                (value->>'partitionId')::uuid AS partition_id,
                COALESCE(value->>'consumerGroup', '__QUEUE_MODE__') AS consumer_group,
                value->>'leaseId' AS lease_id,
                COALESCE(value->>'status', 'completed') AS status,
                value->>'error' AS error_msg
            FROM jsonb_array_elements(p_acknowledgments) AS value
        ),
        -- Batch lookup: message info + queue/partition names (replaces N individual queries)
        enriched AS (
            SELECT 
                a.*,
                m.id AS message_id,
                m.created_at AS message_created_at,
                q.name AS queue_name,
                p.name AS partition_name,
                -- Compute validation errors upfront
                CASE 
                    WHEN m.id IS NULL THEN 'Message not found'
                    WHEN a.lease_id IS NOT NULL AND a.lease_id != '' AND NOT EXISTS (
                        SELECT 1 FROM queen.partition_consumers pc
                        WHERE pc.partition_id = a.partition_id
                          AND pc.consumer_group = a.consumer_group
                          AND pc.worker_id = a.lease_id
                          AND pc.lease_expires_at > NOW()
                    ) THEN 'Invalid or expired lease'
                    ELSE NULL
                END AS validation_error
            FROM ack_data a
            LEFT JOIN queen.messages m 
                ON m.transaction_id = a.txn_id 
                AND m.partition_id = a.partition_id
            LEFT JOIN queen.partitions p ON p.id = a.partition_id
            LEFT JOIN queen.queues q ON q.id = p.queue_id
        )
        SELECT * FROM enriched
        ORDER BY partition_id, consumer_group, message_created_at, message_id
    LOOP
        v_success := false;
        v_error := v_ack.validation_error;
        v_lease_released := false;
        v_dlq := false;
        
        -- Acquire advisory lock to prevent cross-transaction deadlocks
        -- This serializes access to each (partition_id, consumer_group) across all concurrent batches
        IF v_ack.partition_id IS NOT NULL THEN
            PERFORM pg_advisory_xact_lock(
                ('x' || substr(md5(v_ack.partition_id::text || v_ack.consumer_group), 1, 16))::bit(64)::bigint
            );
        END IF;
        
        IF v_error IS NULL THEN
            IF v_ack.status IN ('completed', 'success') THEN
                -- Atomic update uses table's current acked_count for correct batch completion logic
                -- ACKs are sorted by (message_created_at, message_id) so cursor always advances forward
                UPDATE queen.partition_consumers 
                SET last_consumed_id = v_ack.message_id,
                    last_consumed_created_at = v_ack.message_created_at,
                    last_consumed_at = NOW(),
                    total_messages_consumed = total_messages_consumed + 1,
                    acked_count = CASE 
                        WHEN acked_count + 1 >= batch_size AND batch_size > 0 THEN 0
                        ELSE acked_count + 1
                    END,
                    lease_expires_at = CASE 
                        WHEN acked_count + 1 >= batch_size AND batch_size > 0 THEN NULL 
                        ELSE lease_expires_at 
                    END,
                    lease_acquired_at = CASE 
                        WHEN acked_count + 1 >= batch_size AND batch_size > 0 THEN NULL 
                        ELSE lease_acquired_at 
                    END,
                    worker_id = CASE 
                        WHEN acked_count + 1 >= batch_size AND batch_size > 0 THEN NULL 
                        ELSE worker_id 
                    END,
                    batch_retry_count = CASE 
                        WHEN acked_count + 1 >= batch_size AND batch_size > 0 THEN 0 
                        ELSE batch_retry_count 
                    END
                WHERE partition_id = v_ack.partition_id
                  AND consumer_group = v_ack.consumer_group
                RETURNING (lease_expires_at IS NULL) INTO v_lease_released;
                
                INSERT INTO queen.messages_consumed (partition_id, consumer_group, messages_completed)
                VALUES (v_ack.partition_id, v_ack.consumer_group, 1)
                ON CONFLICT DO NOTHING;
                
                v_success := true;
                
            ELSIF v_ack.status IN ('failed', 'dlq') THEN
                -- Query current retry state (needs fresh data after potential previous updates)
                SELECT COALESCE(pc.batch_retry_count, 0), COALESCE(q.retry_limit, 3)
                INTO v_batch_retry_count, v_retry_limit
                FROM queen.partition_consumers pc
                JOIN queen.partitions p ON p.id = pc.partition_id
                JOIN queen.queues q ON q.id = p.queue_id
                WHERE pc.partition_id = v_ack.partition_id
                  AND pc.consumer_group = v_ack.consumer_group;
                
                IF v_batch_retry_count < v_retry_limit AND v_ack.status != 'dlq' THEN
                    UPDATE queen.partition_consumers
                    SET lease_expires_at = NULL,
                        lease_acquired_at = NULL,
                        message_batch = NULL,
                        batch_size = 0,
                        acked_count = 0,
                        worker_id = NULL,
                        batch_retry_count = COALESCE(batch_retry_count, 0) + 1
                    WHERE partition_id = v_ack.partition_id
                      AND consumer_group = v_ack.consumer_group
                      AND worker_id IS NOT NULL;
                    
                    v_success := true;
                    v_lease_released := true;
                ELSE
                    INSERT INTO queen.dead_letter_queue (
                        message_id, partition_id, consumer_group, error_message, 
                        retry_count, original_created_at
                    )
                    VALUES (
                        v_ack.message_id, 
                        v_ack.partition_id, 
                        v_ack.consumer_group, 
                        COALESCE(v_ack.error_msg, 'Retries exhausted'),
                        COALESCE(v_batch_retry_count, 0),
                        v_ack.message_created_at
                    );
                    
                    UPDATE queen.partition_consumers 
                    SET last_consumed_id = v_ack.message_id,
                        last_consumed_created_at = v_ack.message_created_at,
                        last_consumed_at = NOW(),
                        lease_expires_at = CASE 
                            WHEN acked_count + 1 >= batch_size AND batch_size > 0 THEN NULL 
                            ELSE lease_expires_at 
                        END,
                        lease_acquired_at = CASE 
                            WHEN acked_count + 1 >= batch_size AND batch_size > 0 THEN NULL 
                            ELSE lease_acquired_at 
                        END,
                        worker_id = CASE 
                            WHEN acked_count + 1 >= batch_size AND batch_size > 0 THEN NULL 
                            ELSE worker_id 
                        END,
                        acked_count = CASE 
                            WHEN acked_count + 1 >= batch_size AND batch_size > 0 THEN 0 
                            ELSE acked_count + 1 
                        END
                    WHERE partition_id = v_ack.partition_id
                      AND consumer_group = v_ack.consumer_group;
                    
                    INSERT INTO queen.messages_consumed (partition_id, consumer_group, messages_failed)
                    VALUES (v_ack.partition_id, v_ack.consumer_group, 1)
                    ON CONFLICT DO NOTHING;
                    
                    v_success := true;
                    v_dlq := true;
                END IF;
                
            ELSIF v_ack.status = 'retry' THEN
                UPDATE queen.partition_consumers
                SET lease_expires_at = NULL,
                    lease_acquired_at = NULL,
                    worker_id = NULL
                WHERE partition_id = v_ack.partition_id
                  AND consumer_group = v_ack.consumer_group;
                
                v_success := true;
                v_lease_released := true;
            END IF;
        END IF;
        
        -- O(1) amortized array append (vs O(N) JSONB concat)
        v_result_rows := array_append(v_result_rows, jsonb_build_object(
            'index', v_ack.idx,
            'transactionId', v_ack.txn_id,
            'success', v_success,
            'error', v_error,
            'queueName', v_ack.queue_name,
            'partitionName', v_ack.partition_name,
            'leaseReleased', v_lease_released,
            'dlq', v_dlq
        ));
    END LOOP;
    
    -- Convert array to sorted JSONB result
    RETURN (
        SELECT COALESCE(jsonb_agg(item ORDER BY (item->>'index')::int), '[]'::jsonb)
        FROM unnest(v_result_rows) AS item
    );
END;
$$;

GRANT EXECUTE ON FUNCTION queen.ack_messages_v2(jsonb) TO PUBLIC;