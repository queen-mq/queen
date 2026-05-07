-- ============================================================================
-- streams_cycle_v1: atomic streaming cycle commit
-- ============================================================================
--
-- One streaming cycle in the fat-client SDK runs as:
--
--   1. Pop messages for a partition (via /api/v1/pop -> pop_unified_batch_v4)
--   2. (Optional) Read state for that partition (via /streams/v1/state/get
--      -> streams_state_get_v1)
--   3. User code applies operators in JS, producing:
--        - state_ops:  upserts/deletes to apply to queen_streams.state
--        - push_items: messages to emit to the sink queue
--        - ack:        cursor advancement on the source partition
--   4. Commit all three together via this SP, in one PG transaction.
--
-- Strategic anchors this SP delivers (from the plan):
--   * Exactly-once is one Postgres transaction.    (BEGIN..COMMIT around all 3)
--   * Streaming state is queryable via plain SQL.  (queen_streams.state rows)
--   * One backup, one restore.                     (everything in PG)
--   * Time-travel debugging.                       (state PK includes
--                                                    partition_id)
--
-- Input shape: JSONB ARRAY of cycle requests (libqueen merges multiple
-- jobs of the same JobType into one SP call; see _fire_batched in
-- lib/queen.hpp). Each element has:
--
--   { "idx": 0,
--     "query_id":     "<uuid>",
--     "partition_id": "<uuid>",                 -- source partition being acked
--     "consumer_group": "streams.<queryId>",     -- defaults to "__QUEUE_MODE__"
--     "state_ops":  [
--        {"type":"upsert","key":"...","value":{...}},
--        {"type":"delete","key":"..."}
--     ],
--     "push_items": [                            -- sink push, optional
--        {"queue":"...","partition":"...","payload":{...},
--         "transactionId":"...","traceId":"..."}
--     ],
--     "ack": {                                   -- source ack, optional
--        "transactionId":"...","leaseId":"...","status":"completed"
--     }
--   }
--
-- Output shape: JSONB ARRAY mirroring input idx.
--
--   [
--     { "idx": 0,
--       "result": {
--         "success":     true|false,
--         "query_id":    "...",
--         "partition_id":"...",
--         "state_ops_applied": <int>,
--         "push_results":      [...],   -- items array from push_messages_v3
--         "ack_result":        {...},
--         "error":             "..."     -- present only when success=false
--       }
--     }
--   ]
--
-- partition_lookup refresh
-- ------------------------
-- libqueen's auto-fire for queen.update_partition_lookup_v1 is gated on
-- JobType::PUSH (see lib/queen.hpp _process_slot_result around line 1225),
-- so STREAMS_CYCLE responses don't get the auto-refresh. To keep sink-queue
-- partition_lookup fresh, we call queen.update_partition_lookup_v1 INLINE
-- here for the partition_updates returned by push_messages_v3. This keeps
-- consumer-side wakeup (UDP notifications + backoff tracker) working for
-- sink queues without modifying libqueen.
-- ============================================================================

CREATE OR REPLACE FUNCTION queen.streams_cycle_v1(p_requests JSONB)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_results             JSONB := '[]'::jsonb;
    v_req                 JSONB;
    v_idx                 INT;

    v_query_id            UUID;
    v_partition_id        UUID;
    v_consumer_group      TEXT;
    v_state_ops           JSONB;
    v_push_items          JSONB;
    v_ack                 JSONB;

    v_state_ops_applied   INT;
    v_op                  JSONB;

    v_push_response       JSONB;
    v_push_items_result   JSONB;
    v_partition_updates   JSONB;

    v_ack_txn_id          TEXT;
    v_ack_lease_id        TEXT;
    v_ack_status          TEXT;
    v_ack_message_id      UUID;
    v_ack_message_created_at TIMESTAMPTZ;
    v_ack_result          JSONB;
    v_ack_success         BOOLEAN;
    v_ack_error           TEXT;
    v_ack_dlq             BOOLEAN;
    v_ack_lease_released  BOOLEAN;

    v_one_result          JSONB;
    v_source_queue_name   TEXT;
    v_acked_count         INT;
BEGIN
    IF p_requests IS NULL OR jsonb_array_length(p_requests) = 0 THEN
        RETURN '[]'::jsonb;
    END IF;

    FOR v_req IN SELECT * FROM jsonb_array_elements(p_requests)
    LOOP
        v_idx                 := COALESCE((v_req->>'idx')::int, (v_req->>'index')::int, 0);
        v_query_id            := NULL;
        v_partition_id        := NULL;
        v_state_ops_applied   := 0;
        v_push_items_result   := '[]'::jsonb;
        v_partition_updates   := '[]'::jsonb;
        v_ack_result          := 'null'::jsonb;
        v_ack_success         := true;
        v_ack_error           := NULL;
        v_ack_dlq             := false;
        v_ack_lease_released  := false;
        v_source_queue_name   := NULL;
        v_acked_count         := 0;

        BEGIN
            v_query_id       := (v_req->>'query_id')::uuid;
            v_partition_id   := (v_req->>'partition_id')::uuid;
            v_consumer_group := COALESCE(NULLIF(v_req->>'consumer_group', ''), '__QUEUE_MODE__');
            v_state_ops      := COALESCE(v_req->'state_ops',  '[]'::jsonb);
            v_push_items     := COALESCE(v_req->'push_items', '[]'::jsonb);
            v_ack            := v_req->'ack';

            IF v_query_id IS NULL THEN
                RAISE EXCEPTION 'query_id is required';
            END IF;
            IF v_partition_id IS NULL THEN
                RAISE EXCEPTION 'partition_id is required';
            END IF;

            -- Resolve the source queue name for libqueen's per-queue ack
            -- metrics (record_ack_with_queue) so the dashboard's
            -- per-queue Ack/s chart correctly attributes streaming
            -- consumer activity. Cheap lookup; one row per partition.
            SELECT q.name INTO v_source_queue_name
            FROM queen.partitions p
            JOIN queen.queues q ON q.id = p.queue_id
            WHERE p.id = v_partition_id;

            -- Serialise concurrent cycles + idle-flush sweeps on the same
            -- (query_id, partition_id) state shard. Without this, a cycle
            -- updating the latest open window can race with an idle-flush
            -- that's reading + deleting the same row, leading to lost
            -- events. Pre-ack-branch lock so both ack-bearing cycles and
            -- ack-less flush cycles share the same serialisation key.
            PERFORM pg_advisory_xact_lock(
                hashtextextended(v_query_id::text || ':' || v_partition_id::text, 0)
            );

            -- 1. Apply state ops --------------------------------------------
            -- All ops are scoped to (v_query_id, v_partition_id), the lease
            -- shard the calling worker owns. Queen guarantees only one
            -- worker holds a partition's lease, so no cross-worker contention.
            IF jsonb_array_length(v_state_ops) > 0 THEN
                FOR v_op IN SELECT * FROM jsonb_array_elements(v_state_ops)
                LOOP
                    IF v_op->>'type' = 'upsert' THEN
                        INSERT INTO queen_streams.state (query_id, partition_id, key, value, updated_at)
                        VALUES (
                            v_query_id,
                            v_partition_id,
                            v_op->>'key',
                            COALESCE(v_op->'value', '{}'::jsonb),
                            NOW()
                        )
                        ON CONFLICT (query_id, partition_id, key) DO UPDATE SET
                            value      = EXCLUDED.value,
                            updated_at = NOW();
                        v_state_ops_applied := v_state_ops_applied + 1;
                    ELSIF v_op->>'type' = 'delete' THEN
                        DELETE FROM queen_streams.state
                        WHERE query_id     = v_query_id
                          AND partition_id = v_partition_id
                          AND key          = v_op->>'key';
                        v_state_ops_applied := v_state_ops_applied + 1;
                    ELSE
                        RAISE EXCEPTION 'Unknown state_op type: %', COALESCE(v_op->>'type', 'null');
                    END IF;
                END LOOP;
            END IF;

            -- 2. Sink push --------------------------------------------------
            -- Compose push_messages_v3 so we inherit its dedup, partition
            -- creation, and partition_updates emission. Then call
            -- update_partition_lookup_v1 inline because libqueen's auto-fire
            -- only triggers for JobType::PUSH responses.
            IF jsonb_array_length(v_push_items) > 0 THEN
                v_push_response     := queen.push_messages_v3(v_push_items, true, false);
                v_push_items_result := COALESCE(v_push_response->'items', '[]'::jsonb);
                v_partition_updates := COALESCE(v_push_response->'partition_updates', '[]'::jsonb);

                IF jsonb_array_length(v_partition_updates) > 0 THEN
                    PERFORM queen.update_partition_lookup_v1(v_partition_updates);
                END IF;
            END IF;

            -- 3. Source ack -------------------------------------------------
            IF v_ack IS NOT NULL AND v_ack <> 'null'::jsonb THEN
                v_ack_txn_id   := v_ack->>'transactionId';
                v_ack_lease_id := v_ack->>'leaseId';
                v_ack_status   := COALESCE(v_ack->>'status', 'completed');

                IF v_ack_txn_id IS NULL OR v_ack_txn_id = '' THEN
                    RAISE EXCEPTION 'ack.transactionId is required';
                END IF;

                -- Lock the (partition, consumer_group) row to serialise
                -- against concurrent ack/transaction operations. Same
                -- pattern as ack_messages_v2.
                PERFORM pg_advisory_xact_lock(
                    ('x' || substr(md5(v_partition_id::text || v_consumer_group), 1, 16))::bit(64)::bigint
                );

                SELECT m.id, m.created_at
                INTO v_ack_message_id, v_ack_message_created_at
                FROM queen.messages m
                WHERE m.transaction_id = v_ack_txn_id
                  AND m.partition_id   = v_partition_id;

                IF v_ack_message_id IS NULL THEN
                    RAISE EXCEPTION 'Message not found';
                END IF;

                IF v_ack_lease_id IS NOT NULL AND v_ack_lease_id <> ''
                   AND NOT EXISTS (
                        SELECT 1 FROM queen.partition_consumers pc
                        WHERE pc.partition_id   = v_partition_id
                          AND pc.consumer_group = v_consumer_group
                          AND pc.worker_id      = v_ack_lease_id
                          AND pc.lease_expires_at > NOW()
                   ) THEN
                    RAISE EXCEPTION 'Invalid or expired lease';
                END IF;

                IF v_ack_status IN ('completed', 'success') THEN
                    -- Streaming-cycle ack semantics: every cycle corresponds
                    -- to the FULL popped batch (the cycle's atomic unit), so
                    -- we always release the lease here regardless of
                    -- partition_consumers.batch_size. ack_messages_v2's
                    -- per-message accumulator model doesn't apply.
                    v_acked_count := COALESCE((v_ack->>'count')::INT, 1);
                    UPDATE queen.partition_consumers
                    SET last_consumed_id          = v_ack_message_id,
                        last_consumed_created_at  = v_ack_message_created_at,
                        last_consumed_at          = NOW(),
                        total_messages_consumed   = COALESCE(total_messages_consumed, 0)
                                                     + COALESCE((v_ack->>'count')::INT, 1),
                        acked_count               = 0,
                        batch_size                = 0,
                        lease_expires_at          = NULL,
                        lease_acquired_at         = NULL,
                        worker_id                 = NULL
                    WHERE partition_id   = v_partition_id
                      AND consumer_group = v_consumer_group;

                    v_ack_lease_released := true;

                    IF NOT FOUND THEN
                        INSERT INTO queen.partition_consumers (
                            partition_id, consumer_group,
                            last_consumed_id, last_consumed_created_at, last_consumed_at,
                            total_messages_consumed
                        )
                        VALUES (
                            v_partition_id, v_consumer_group,
                            v_ack_message_id, v_ack_message_created_at, NOW(),
                            COALESCE((v_ack->>'count')::INT, 1)
                        );
                    END IF;

                    INSERT INTO queen.messages_consumed (partition_id, consumer_group, messages_completed)
                    VALUES (v_partition_id, v_consumer_group, COALESCE((v_ack->>'count')::INT, 1))
                    ON CONFLICT DO NOTHING;
                ELSIF v_ack_status IN ('failed', 'dlq') THEN
                    INSERT INTO queen.dead_letter_queue (
                        message_id, partition_id, consumer_group,
                        error_message, retry_count, original_created_at
                    )
                    VALUES (
                        v_ack_message_id, v_partition_id, v_consumer_group,
                        COALESCE(v_ack->>'error', 'Streaming cycle reported failure'),
                        0, v_ack_message_created_at
                    );

                    UPDATE queen.partition_consumers
                    SET last_consumed_id          = v_ack_message_id,
                        last_consumed_created_at  = v_ack_message_created_at,
                        last_consumed_at          = NOW(),
                        lease_expires_at          = NULL,
                        lease_acquired_at         = NULL,
                        worker_id                 = NULL,
                        batch_size                = 0,
                        acked_count               = 0
                    WHERE partition_id   = v_partition_id
                      AND consumer_group = v_consumer_group;

                    v_ack_dlq := true;
                ELSE
                    RAISE EXCEPTION 'Unknown ack.status: %', v_ack_status;
                END IF;

                v_ack_result := jsonb_build_object(
                    'success',         true,
                    'count',           v_acked_count,        -- libqueen credits this to per-queue Ack/s
                    'lease_released',  v_ack_lease_released,
                    'dlq',             v_ack_dlq
                );
            END IF;

            v_one_result := jsonb_build_object(
                'success',           true,
                'query_id',          v_query_id::text,
                'partition_id',      v_partition_id::text,
                'queueName',         v_source_queue_name,   -- for libqueen per-queue Ack metric
                'state_ops_applied', v_state_ops_applied,
                'push_results',      v_push_items_result,
                'ack_result',        v_ack_result
            );
        EXCEPTION WHEN OTHERS THEN
            v_one_result := jsonb_build_object(
                'success',           false,
                'query_id',          COALESCE(v_query_id::text, ''),
                'partition_id',      COALESCE(v_partition_id::text, ''),
                'queueName',         v_source_queue_name,
                'state_ops_applied', v_state_ops_applied,
                'push_results',      v_push_items_result,
                'ack_result',        v_ack_result,
                'error',             SQLERRM
            );
        END;

        v_results := v_results || jsonb_build_object('idx', v_idx, 'result', v_one_result);
    END LOOP;

    RETURN v_results;
END;
$$;

GRANT EXECUTE ON FUNCTION queen.streams_cycle_v1(JSONB) TO PUBLIC;
