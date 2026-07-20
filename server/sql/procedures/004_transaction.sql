-- ============================================================================
-- execute_transaction_v2: Atomic transaction execution
-- ============================================================================
-- NOTE: Using CREATE OR REPLACE (no DROP) for zero-downtime deployments

CREATE OR REPLACE FUNCTION queen.execute_transaction_v2(
    p_operations JSONB
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_op JSONB;
    v_results JSONB := '[]'::jsonb;
    v_transaction_id TEXT;
    v_idx INT := 0;
    v_op_success BOOLEAN;
    v_op_error TEXT;
    v_queue_id UUID;
    v_partition_id UUID;
    v_queue_name TEXT;
    v_partition_name TEXT;
    v_namespace TEXT;
    v_task TEXT;
    v_message_id UUID;
    v_txn_id TEXT;
    v_payload JSONB;
    v_trace_id UUID;
    v_ack_message_id UUID;
    v_consumer_group TEXT;
    v_message_created_at TIMESTAMPTZ;
    v_lease_id TEXT;
    v_status TEXT;
    v_op_dlq BOOLEAN;
    v_lock_pid UUID;  -- PUSHSER: loop var for sorted per-partition lock pre-pass
BEGIN
    v_transaction_id := gen_random_uuid()::text;

    -- PUSHSER pre-pass: this transaction may push to several partitions. To make
    -- created_at commit-ordered per partition (the pop cursor depends on it) and
    -- to stay deadlock-free against concurrent pushes/transactions, ensure every
    -- pushed partition exists, then take the push-partition advisory lock for all
    -- of them in ascending partition_id order BEFORE inserting any message. The
    -- per-op queue/partition upserts below remain (idempotent via ON CONFLICT).
    INSERT INTO queen.queues (name, namespace, task)
    SELECT DISTINCT
        op->>'queue',
        COALESCE(op->>'namespace', split_part(op->>'queue', '.', 1)),
        COALESCE(op->>'task',
            CASE WHEN position('.' in op->>'queue') > 0
                 THEN split_part(op->>'queue', '.', 2) ELSE '' END)
    FROM jsonb_array_elements(p_operations) AS op
    WHERE op->>'type' = 'push' AND COALESCE(op->>'queue', '') <> ''
    ON CONFLICT (name) DO NOTHING;

    INSERT INTO queen.partitions (queue_id, name)
    SELECT DISTINCT qq.id, COALESCE(op->>'partition', 'Default')
    FROM jsonb_array_elements(p_operations) AS op
    JOIN queen.queues qq ON qq.name = op->>'queue'
    WHERE op->>'type' = 'push'
    ON CONFLICT (queue_id, name) DO NOTHING;

    FOR v_lock_pid IN
        SELECT DISTINCT pp.id
        FROM jsonb_array_elements(p_operations) AS op
        JOIN queen.queues     qq ON qq.name     = op->>'queue'
        JOIN queen.partitions pp ON pp.queue_id = qq.id
                                AND pp.name     = COALESCE(op->>'partition', 'Default')
        WHERE op->>'type' = 'push'
        ORDER BY pp.id
    LOOP
        PERFORM queen.lock_push_partition(v_lock_pid);
    END LOOP;
    
    FOR v_op IN SELECT * FROM jsonb_array_elements(p_operations)
    LOOP
        v_op_success := false;
        v_op_error := NULL;
        v_op_dlq := false;
        
        CASE v_op->>'type'
            WHEN 'push' THEN
                v_queue_name := v_op->>'queue';
                v_partition_name := COALESCE(v_op->>'partition', 'Default');
                v_txn_id := COALESCE(v_op->>'transactionId', gen_random_uuid()::text);
                v_payload := COALESCE(v_op->'payload', v_op->'data', '{}'::jsonb);
                v_message_id := COALESCE((v_op->>'messageId')::uuid, gen_random_uuid());
                v_namespace := COALESCE(v_op->>'namespace', split_part(v_queue_name, '.', 1));
                v_task := COALESCE(v_op->>'task', 
                                   CASE WHEN position('.' in v_queue_name) > 0 
                                        THEN split_part(v_queue_name, '.', 2) 
                                        ELSE '' END);
                
                IF v_op->>'traceId' IS NOT NULL AND v_op->>'traceId' != '' THEN
                    v_trace_id := (v_op->>'traceId')::uuid;
                ELSE
                    v_trace_id := NULL;
                END IF;
                
                INSERT INTO queen.queues (name, namespace, task)
                VALUES (v_queue_name, v_namespace, v_task)
                ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                RETURNING id INTO v_queue_id;
                
                INSERT INTO queen.partitions (queue_id, name)
                VALUES (v_queue_id, v_partition_name)
                ON CONFLICT (queue_id, name) DO UPDATE SET name = EXCLUDED.name
                RETURNING id INTO v_partition_id;
                
                -- PUSHSER: stamp created_at = clock_timestamp() under the
                -- push-partition lock taken in the pre-pass, so per-partition
                -- created_at is commit-ordered (not the txn-start NOW() default).
                INSERT INTO queen.messages (id, transaction_id, partition_id, payload, trace_id, is_encrypted, created_at)
                VALUES (v_message_id, v_txn_id, v_partition_id, v_payload, v_trace_id,
                        COALESCE((v_op->>'is_encrypted')::boolean, false), clock_timestamp())
                RETURNING created_at INTO v_message_created_at;

                -- Maintain partition_lookup with the SAME semantics as
                -- queen.update_partition_lookup_v1 (the canonical writer used
                -- by the non-transactional push path). Two invariants matter:
                --   1. updated_at MUST be refreshed to NOW() on every successful
                --      write, otherwise the wildcard POP filter
                --      (pl.updated_at >= watermark - 2m) skips this partition
                --      forever once the watermark advances past the stale
                --      timestamp. See cdocs/WATERMARK_AND_TRANSACTION_FIXES.md
                --      §3 (Fix C). The reconciler cannot rescue this state
                --      because it only writes when last_message_id changes.
                --   2. The (last_message_created_at, last_message_id) tuple
                --      must advance monotonically; concurrent pushes with
                --      slightly older NOW() must not clobber newer data.
                INSERT INTO queen.partition_lookup (
                    partition_id, queue_name, last_message_id, last_message_created_at, updated_at
                )
                VALUES (v_partition_id, v_queue_name, v_message_id, v_message_created_at, NOW())
                ON CONFLICT (queue_name, partition_id) DO UPDATE SET
                    last_message_id         = EXCLUDED.last_message_id,
                    last_message_created_at = EXCLUDED.last_message_created_at,
                    updated_at              = NOW()
                WHERE
                    EXCLUDED.last_message_created_at >  queen.partition_lookup.last_message_created_at
                 OR (EXCLUDED.last_message_created_at = queen.partition_lookup.last_message_created_at
                     AND EXCLUDED.last_message_id    >  queen.partition_lookup.last_message_id);
                
                v_op_success := true;
                
                v_results := v_results || jsonb_build_object(
                    'index', v_idx,
                    'type', 'push',
                    'success', v_op_success,
                    'transactionId', v_txn_id,
                    'messageId', v_message_id::text,
                    'partitionId', v_partition_id::text,
                    -- v_queue_name is already set above for the push branch;
                    -- libqueen uses this to attribute per-queue transaction counters.
                    'queueName', v_queue_name
                );
                
            WHEN 'ack' THEN
                v_consumer_group := COALESCE(v_op->>'consumerGroup', '__QUEUE_MODE__');
                v_partition_id := (v_op->>'partitionId')::uuid;
                v_lease_id := v_op->>'leaseId';
                v_status := COALESCE(v_op->>'status', 'completed');
                
                SELECT m.id, m.created_at
                INTO v_ack_message_id, v_message_created_at
                FROM queen.messages m
                WHERE m.transaction_id = v_op->>'transactionId'
                  AND m.partition_id = v_partition_id;
                
                IF v_ack_message_id IS NULL THEN
                    v_op_error := 'Message not found';
                ELSE
                    IF v_lease_id IS NOT NULL AND v_lease_id != '' THEN
                        IF NOT EXISTS (
                            SELECT 1 FROM queen.partition_consumers pc
                            WHERE pc.partition_id = v_partition_id
                              AND pc.consumer_group = v_consumer_group
                              AND pc.worker_id = v_lease_id
                              AND pc.lease_expires_at > NOW()
                        ) THEN
                            v_op_error := 'Invalid or expired lease';
                        END IF;
                    END IF;
                    
                    IF v_op_error IS NULL THEN
                        IF v_status IN ('completed', 'success') THEN
                            UPDATE queen.partition_consumers
                            SET last_consumed_id = v_ack_message_id,
                                last_consumed_created_at = v_message_created_at,
                                last_consumed_at = NOW(),
                                total_messages_consumed = COALESCE(total_messages_consumed, 0) + 1,
                                acked_count = COALESCE(acked_count, 0) + 1,
                                lease_expires_at = CASE 
                                    WHEN COALESCE(acked_count, 0) + 1 >= COALESCE(batch_size, 1) 
                                    THEN NULL ELSE lease_expires_at END,
                                lease_acquired_at = CASE 
                                    WHEN COALESCE(acked_count, 0) + 1 >= COALESCE(batch_size, 1) 
                                    THEN NULL ELSE lease_acquired_at END,
                                worker_id = CASE 
                                    WHEN COALESCE(acked_count, 0) + 1 >= COALESCE(batch_size, 1) 
                                    THEN NULL ELSE worker_id END,
                                batch_size = CASE 
                                    WHEN COALESCE(acked_count, 0) + 1 >= COALESCE(batch_size, 1) 
                                    THEN 0 ELSE batch_size END
                            WHERE partition_id = v_partition_id
                              AND consumer_group = v_consumer_group;
                            
                            IF NOT FOUND THEN
                                INSERT INTO queen.partition_consumers (
                                    partition_id, consumer_group, last_consumed_id, 
                                    last_consumed_created_at, last_consumed_at, total_messages_consumed
                                )
                                VALUES (v_partition_id, v_consumer_group, v_ack_message_id, 
                                        v_message_created_at, NOW(), 1);
                            END IF;
                            
                            v_op_success := true;
                        ELSIF v_status IN ('failed', 'dlq') THEN
                            INSERT INTO queen.dead_letter_queue (
                                message_id, partition_id, consumer_group, error_message, 
                                retry_count, original_created_at
                            )
                            VALUES (
                                v_ack_message_id, 
                                v_partition_id, 
                                v_consumer_group, 
                                COALESCE(v_op->>'error', 'Unknown error'),
                                0,
                                v_message_created_at
                            );
                            
                            UPDATE queen.partition_consumers
                            SET last_consumed_id = v_ack_message_id,
                                last_consumed_created_at = v_message_created_at,
                                last_consumed_at = NOW(),
                                lease_expires_at = NULL,
                                lease_acquired_at = NULL,
                                worker_id = NULL,
                                batch_size = 0,
                                acked_count = 0
                            WHERE partition_id = v_partition_id
                              AND consumer_group = v_consumer_group;
                            
                            IF NOT FOUND THEN
                                INSERT INTO queen.partition_consumers (
                                    partition_id, consumer_group, last_consumed_id, 
                                    last_consumed_created_at, last_consumed_at
                                )
                                VALUES (v_partition_id, v_consumer_group, v_ack_message_id, 
                                        v_message_created_at, NOW());
                            END IF;
                            
                            v_op_success := true;
                            v_op_dlq := true;
                        END IF;
                    END IF;
                END IF;
                
                v_results := v_results || jsonb_build_object(
                    'index', v_idx,
                    'type', 'ack',
                    'success', v_op_success,
                    'transactionId', v_op->>'transactionId',
                    'error', v_op_error,
                    'dlq', v_op_dlq
                );
                
                IF NOT v_op_success THEN
                    RAISE EXCEPTION 'ACK operation failed: %', COALESCE(v_op_error, 'Unknown error');
                END IF;
                
            ELSE
                v_results := v_results || jsonb_build_object(
                    'index', v_idx,
                    'type', v_op->>'type',
                    'success', false,
                    'error', 'Unknown operation type: ' || COALESCE(v_op->>'type', 'null')
                );
                
                RAISE EXCEPTION 'Unknown operation type: %', COALESCE(v_op->>'type', 'null');
        END CASE;
        
        v_idx := v_idx + 1;
    END LOOP;
    
    RETURN jsonb_build_object(
        'transactionId', v_transaction_id,
        'success', true,
        'results', v_results
    );
    
EXCEPTION WHEN OTHERS THEN
    RETURN jsonb_build_object(
        'transactionId', v_transaction_id,
        'success', false,
        'error', SQLERRM,
        'results', v_results
    );
END;
$$;

GRANT EXECUTE ON FUNCTION queen.execute_transaction_v2(jsonb) TO PUBLIC;