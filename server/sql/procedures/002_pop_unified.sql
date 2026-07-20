-- ============================================================================
-- POP Unified Procedures - Single Round-Trip Operations
-- ============================================================================
-- These procedures perform complete POP operations (lease + fetch + cleanup)
-- in a SINGLE database call, eliminating the need for C++ state machines.
--
-- Two specialized procedures handle the fundamentally different cases:
--   1. pop_specific_batch  - Known partitions (deterministic, no races)
--   2. pop_discover_batch  - Wildcard discovery (uses SKIP LOCKED for safety)
--
-- Both return the same result format for uniform C++ handling.
-- ============================================================================

-- ============================================================================
-- 1. pop_specific_batch: Complete POP for KNOWN partitions
-- ============================================================================
-- Use when: partition_name is specified in all requests
-- Guarantees: Deterministic, no race conditions, deadlock-safe via ordering
--
-- Input JSONB array:
-- [
--   {"idx": 0, "queue_name": "q1", "partition_name": "p1", "consumer_group": "cg1", 
--    "lease_seconds": 60, "worker_id": "uuid1", "batch_size": 10, "sub_mode": "all", 
--    "sub_from": "", "auto_ack": false},
--   ...
-- ]
--
-- Output JSONB array:
-- [
--   {"idx": 0, "success": true, "partition": "p1", "leaseId": "uuid1", 
--    "messages": [{"id": "...", "data": {...}, ...}]},
--   {"idx": 1, "success": false, "error": "lease_held", "messages": []},
--   {"idx": 2, "success": true, "partition": "p2", "leaseId": null,  -- auto_ack=true
--    "messages": [...]},
--   ...
-- ]
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.pop_specific_batch(p_requests JSONB)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_now TIMESTAMPTZ := NOW();
    v_results JSONB := '[]'::jsonb;
    v_req RECORD;
    v_partition_id UUID;
    v_queue_id UUID;
    v_lease_time INT;
    v_window_buffer INT;
    v_delayed_processing INT;
    v_effective_lease INT;
    v_cursor_id UUID;
    v_cursor_ts TIMESTAMPTZ;
    v_messages JSONB;
    v_msg_count INT;
    v_result JSONB;
    v_sub_ts TIMESTAMPTZ;
    v_last_msg_id UUID;
    v_last_msg_ts TIMESTAMPTZ;
    -- Window buffer debounce support
    v_partition_last_msg_ts TIMESTAMPTZ;
BEGIN
    -- Process requests in partition_id order to prevent deadlocks
    -- We use a cursor over the sorted requests
    FOR v_req IN 
        SELECT 
            (r->>'idx')::INT AS idx,
            (r->>'queue_name')::TEXT AS queue_name,
            (r->>'partition_name')::TEXT AS partition_name,
            COALESCE(r->>'consumer_group', '__QUEUE_MODE__') AS consumer_group,
            COALESCE((r->>'batch_size')::INT, 10) AS batch_size,
            COALESCE((r->>'lease_seconds')::INT, 0) AS lease_seconds,
            (r->>'worker_id')::TEXT AS worker_id,
            COALESCE(r->>'sub_mode', 'all') AS sub_mode,
            COALESCE(r->>'sub_from', '') AS sub_from
        FROM jsonb_array_elements(p_requests) r
        ORDER BY r->>'queue_name', r->>'partition_name'  -- Consistent ordering
    LOOP
        -- Reset per-iteration state
        v_partition_id := NULL;
        v_cursor_id := NULL;
        v_cursor_ts := NULL;
        v_messages := '[]'::jsonb;
        
        -- =====================================================================
        -- STEP 1: Resolve partition and get queue settings
        -- =====================================================================
        SELECT p.id, q.id, q.lease_time, q.window_buffer, q.delayed_processing, pl.last_message_created_at
        INTO v_partition_id, v_queue_id, v_lease_time, v_window_buffer, v_delayed_processing, v_partition_last_msg_ts
        FROM queen.queues q
        JOIN queen.partitions p ON p.queue_id = q.id
        LEFT JOIN queen.partition_lookup pl ON pl.partition_id = p.id
        WHERE q.name = v_req.queue_name AND p.name = v_req.partition_name;
        
        IF v_partition_id IS NULL THEN
            v_result := jsonb_build_object(
                'idx', v_req.idx,
                'result', jsonb_build_object(
                    'success', false,
                    'error', 'partition_not_found',
                    'messages', '[]'::jsonb
                )
            );
            v_results := v_results || v_result;
            CONTINUE;
        END IF;
        
        -- WINDOW BUFFER DEBOUNCE: Check if window has expired
        -- If not expired, return empty result (debounce still active)
        IF v_window_buffer IS NOT NULL AND v_window_buffer > 0 
           AND v_partition_last_msg_ts IS NOT NULL
           AND v_partition_last_msg_ts > v_now - (v_window_buffer || ' seconds')::interval THEN
            v_result := jsonb_build_object(
                'idx', v_req.idx,
                'result', jsonb_build_object(
                    'success', true,
                    'queue', v_req.queue_name,
                    'partition', v_req.partition_name,
                    'partitionId', v_partition_id::text,
                    'leaseId', '',
                    'consumerGroup', v_req.consumer_group,
                    'messages', '[]'::jsonb,
                    'windowBufferActive', true
                )
            );
            v_results := v_results || v_result;
            CONTINUE;
        END IF;
        
        -- =====================================================================
        -- STEP 2: Ensure consumer row exists & try to acquire lease (atomic)
        -- =====================================================================
        INSERT INTO queen.partition_consumers (partition_id, consumer_group)
        VALUES (v_partition_id, v_req.consumer_group)
        ON CONFLICT (partition_id, consumer_group) DO NOTHING;
        
        v_effective_lease := COALESCE(NULLIF(v_req.lease_seconds, 0), NULLIF(v_lease_time, 0), 60);
        
        UPDATE queen.partition_consumers pc
        SET 
            lease_acquired_at = v_now,
            lease_expires_at = v_now + (v_effective_lease * INTERVAL '1 second'),
            worker_id = v_req.worker_id,
            batch_size = v_req.batch_size,
            acked_count = 0,
            batch_retry_count = COALESCE(pc.batch_retry_count, 0)
        WHERE pc.partition_id = v_partition_id
          AND pc.consumer_group = v_req.consumer_group
          AND (pc.lease_expires_at IS NULL OR pc.lease_expires_at <= v_now)
        RETURNING pc.last_consumed_id, pc.last_consumed_created_at
        INTO v_cursor_id, v_cursor_ts;
        
        IF NOT FOUND THEN
            v_result := jsonb_build_object(
                'idx', v_req.idx,
                'result', jsonb_build_object(
                    'success', false,
                    'error', 'lease_held',
                    'messages', '[]'::jsonb
                )
            );
            v_results := v_results || v_result;
            CONTINUE;
        END IF;
        
        -- =====================================================================
        -- STEP 3: Handle subscription modes for new consumers
        -- =====================================================================
        IF v_cursor_ts IS NULL AND v_req.consumer_group != '__QUEUE_MODE__' THEN
            -- Check consumer_groups_metadata for pre-configured timestamp
            SELECT cgm.subscription_timestamp
            INTO v_sub_ts
            FROM queen.consumer_groups_metadata cgm
            WHERE cgm.consumer_group = v_req.consumer_group
              AND cgm.queue_name = v_req.queue_name
              AND (cgm.partition_name = v_req.partition_name OR cgm.partition_name = '')
            ORDER BY CASE WHEN cgm.partition_name != '' THEN 1 ELSE 2 END
            LIMIT 1;
            
            IF v_sub_ts IS NOT NULL THEN
                -- Use pre-recorded subscription timestamp
                v_cursor_ts := v_sub_ts;
                v_cursor_id := '00000000-0000-0000-0000-000000000000'::uuid;
            ELSIF v_req.sub_from != '' AND v_req.sub_from != 'now' THEN
                -- Explicit timestamp provided: parse and record it
                BEGIN
                    v_cursor_ts := v_req.sub_from::timestamptz;
                    v_cursor_id := '00000000-0000-0000-0000-000000000000'::uuid;
                    
                    -- Insert subscription metadata with explicit timestamp
                    INSERT INTO queen.consumer_groups_metadata (
                        consumer_group, queue_name, partition_name, subscription_mode, subscription_timestamp
                    ) VALUES (
                        v_req.consumer_group, v_req.queue_name, COALESCE(v_req.partition_name, ''), 'timestamp', v_cursor_ts
                    )
                    ON CONFLICT (consumer_group, queue_name, partition_name, namespace, task) DO NOTHING;
                EXCEPTION WHEN OTHERS THEN
                    -- Invalid timestamp format - fall through to default behavior
                    NULL;
                END;
            ELSIF v_req.sub_from = 'now' OR v_req.sub_mode = 'new' THEN
                -- New subscription with "new" mode: record timestamp so all workers share it
                v_cursor_ts := v_now;
                v_cursor_id := '00000000-0000-0000-0000-000000000000'::uuid;
                
                -- Insert subscription metadata (idempotent - ON CONFLICT ignores duplicates)
                INSERT INTO queen.consumer_groups_metadata (
                    consumer_group, queue_name, partition_name, subscription_mode, subscription_timestamp
                ) VALUES (
                    v_req.consumer_group, v_req.queue_name, COALESCE(v_req.partition_name, ''), 'new', v_now
                )
                ON CONFLICT (consumer_group, queue_name, partition_name, namespace, task) DO NOTHING;
            END IF;
        END IF;
        
        -- =====================================================================
        -- STEP 4: Fetch messages
        -- =====================================================================
        SELECT COALESCE(jsonb_agg(
            jsonb_build_object(
                'id', sub.id::text,
                'transactionId', sub.transaction_id,
                'traceId', sub.trace_id::text,
                'data', sub.payload,
                'producerSub', sub.producer_sub,
                'createdAt', to_char(sub.created_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
            ) ORDER BY sub.created_at, sub.id
        ), '[]'::jsonb)
        INTO v_messages
        FROM (
            SELECT m.id, m.transaction_id, m.trace_id, m.payload, m.producer_sub, m.created_at
            FROM queen.messages m
            WHERE m.partition_id = v_partition_id
              AND (v_cursor_ts IS NULL OR (m.created_at, m.id) > (v_cursor_ts, COALESCE(v_cursor_id, '00000000-0000-0000-0000-000000000000'::uuid)))
              -- NOTE: window_buffer is checked at partition level (debounce), not per-message
              -- delayed_processing: per-message delay
              AND (v_delayed_processing IS NULL OR v_delayed_processing = 0 
                   OR m.created_at <= v_now - (v_delayed_processing || ' seconds')::interval)
            ORDER BY m.created_at, m.id
            LIMIT v_req.batch_size
        ) sub;
        
        v_msg_count := jsonb_array_length(v_messages);
        
        -- =====================================================================
        -- STEP 5: Cleanup - release lease if empty, or update batch_size
        -- =====================================================================
        IF v_msg_count = 0 THEN
            -- No messages: release lease immediately
            -- CRITICAL FIX: If a subscription timestamp was applied and there were no messages,
            -- we need to update the cursor to prevent infinite re-selection of this partition.
            IF v_cursor_ts IS NOT NULL AND v_cursor_id = '00000000-0000-0000-0000-000000000000'::uuid THEN
                -- Find the last message at or before the subscription timestamp
                SELECT m.id, m.created_at 
                INTO v_last_msg_id, v_last_msg_ts
                FROM queen.messages m
                WHERE m.partition_id = v_partition_id
                  AND m.created_at <= v_cursor_ts
                ORDER BY m.created_at DESC, m.id DESC
                LIMIT 1;
                
                IF v_last_msg_id IS NOT NULL THEN
                    -- Found a message before the timestamp, set cursor to it
                    UPDATE queen.partition_consumers
                    SET lease_expires_at = NULL, 
                        lease_acquired_at = NULL, 
                        worker_id = NULL,
                        last_consumed_id = v_last_msg_id,
                        last_consumed_created_at = v_last_msg_ts,
                        last_consumed_at = v_now
                    WHERE partition_id = v_partition_id
                      AND consumer_group = v_req.consumer_group
                      AND worker_id = v_req.worker_id;
                ELSE
                    -- No messages before the timestamp, just update last_consumed_at
                    UPDATE queen.partition_consumers
                    SET lease_expires_at = NULL, 
                        lease_acquired_at = NULL, 
                        worker_id = NULL,
                        last_consumed_at = v_now
                    WHERE partition_id = v_partition_id
                      AND consumer_group = v_req.consumer_group
                      AND worker_id = v_req.worker_id;
                END IF;
            ELSE
                -- No subscription timestamp applied, just release lease
                UPDATE queen.partition_consumers
                SET lease_expires_at = NULL, lease_acquired_at = NULL, worker_id = NULL
                WHERE partition_id = v_partition_id
                  AND consumer_group = v_req.consumer_group
                  AND worker_id = v_req.worker_id;
            END IF;
        ELSIF v_msg_count < v_req.batch_size THEN
            -- Partial batch: update batch_size so ACKing all messages releases lease
            UPDATE queen.partition_consumers
            SET batch_size = v_msg_count, acked_count = 0
            WHERE partition_id = v_partition_id
              AND consumer_group = v_req.consumer_group
              AND worker_id = v_req.worker_id;
        END IF;
        
        -- =====================================================================
        -- STEP 6: Build result (wrapped in 'result' object for C++ compatibility)
        -- =====================================================================
        v_result := jsonb_build_object(
            'idx', v_req.idx,
            'result', jsonb_build_object(
                'success', true,
                'queue', v_req.queue_name,
                'partition', v_req.partition_name,
                'partitionId', v_partition_id::text,
                'leaseId', v_req.worker_id,
                'consumerGroup', v_req.consumer_group,
                'messages', v_messages
            )
        );
        v_results := v_results || v_result;
    END LOOP;
    
    RETURN v_results;
END;
$$;

-- ============================================================================
-- 2. pop_discover_batch: Complete POP with WILDCARD partition discovery
-- ============================================================================
-- Use when: partition_name is NULL/empty (discover available partition)
-- Guarantees: Uses FOR UPDATE SKIP LOCKED to avoid race conditions
--
-- Key difference from specific:
--   - Multiple workers calling simultaneously will each get DIFFERENT partitions
--   - SKIP LOCKED ensures no blocking on contested partitions
--   - If no available partition, returns success=false with error='no_available_partition'
--
-- Input/Output format: Same as pop_specific_batch
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.pop_discover_batch(p_requests JSONB)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_now TIMESTAMPTZ := NOW();
    v_results JSONB := '[]'::jsonb;
    v_req RECORD;
    v_partition_id UUID;
    v_partition_name TEXT;
    v_queue_id UUID;
    v_lease_time INT;
    v_window_buffer INT;
    v_delayed_processing INT;
    v_effective_lease INT;
    v_cursor_id UUID;
    v_cursor_ts TIMESTAMPTZ;
    v_messages JSONB;
    v_msg_count INT;
    v_result JSONB;
    v_sub_ts TIMESTAMPTZ;
    v_consumer_row_id UUID;
    v_last_msg_id UUID;
    v_last_msg_ts TIMESTAMPTZ;
    v_seeded INT;
BEGIN
    -- Process each request - order doesn't matter for discovery since SKIP LOCKED
    FOR v_req IN 
        SELECT 
            (r->>'idx')::INT AS idx,
            (r->>'queue_name')::TEXT AS queue_name,
            COALESCE(r->>'consumer_group', '__QUEUE_MODE__') AS consumer_group,
            COALESCE((r->>'batch_size')::INT, 10) AS batch_size,
            COALESCE((r->>'lease_seconds')::INT, 0) AS lease_seconds,
            (r->>'worker_id')::TEXT AS worker_id,
            COALESCE(r->>'sub_mode', 'all') AS sub_mode,
            COALESCE(r->>'sub_from', '') AS sub_from
        FROM jsonb_array_elements(p_requests) r
        ORDER BY (r->>'idx')::INT
    LOOP
        -- Reset per-iteration state
        v_partition_id := NULL;
        v_partition_name := NULL;
        v_cursor_id := NULL;
        v_cursor_ts := NULL;
        v_messages := '[]'::jsonb;
        
        -- =====================================================================
        -- STEP 1: Discover available partition using SKIP LOCKED
        -- This is the key difference - atomically finds AND locks an available partition
        -- =====================================================================
        SELECT 
            pl.partition_id,
            p.name,
            q.id,
            q.lease_time,
            q.window_buffer,
            q.delayed_processing
        INTO v_partition_id, v_partition_name, v_queue_id, v_lease_time, v_window_buffer, v_delayed_processing
        FROM queen.partition_lookup pl
        JOIN queen.partitions p ON p.id = pl.partition_id
        JOIN queen.queues q ON q.id = p.queue_id
        LEFT JOIN queen.partition_consumers pc 
            ON pc.partition_id = pl.partition_id 
            AND pc.consumer_group = v_req.consumer_group
        WHERE pl.queue_name = v_req.queue_name
          -- Partition must be free (no lease or expired lease)
          AND (pc.lease_expires_at IS NULL OR pc.lease_expires_at <= v_now)
          -- Partition must have unconsumed messages
          AND (pc.last_consumed_created_at IS NULL 
               OR pl.last_message_created_at > pc.last_consumed_created_at
               OR (pl.last_message_created_at = pc.last_consumed_created_at 
                   AND pl.last_message_id > pc.last_consumed_id))
          -- WINDOW BUFFER DEBOUNCE: Only select partitions where window has expired
          AND (q.window_buffer IS NULL OR q.window_buffer = 0
               OR pl.last_message_created_at <= v_now - (q.window_buffer || ' seconds')::interval)
        -- Fair distribution: prefer least-recently consumed partitions
        -- NULLS LAST ensures new consumer groups prioritize already-discovered
        -- partitions (with pending messages) over undiscovered ones, preventing
        -- starvation of known partitions during the discovery phase.
        ORDER BY pc.last_consumed_at ASC NULLS LAST
        LIMIT 1
        -- CRITICAL: FOR UPDATE SKIP LOCKED prevents race conditions
        -- Other concurrent workers will skip this row and get a different partition
        FOR UPDATE OF pl SKIP LOCKED;
        
        IF v_partition_id IS NULL THEN
            v_result := jsonb_build_object(
                'idx', v_req.idx,
                'result', jsonb_build_object(
                    'success', false,
                    'error', 'no_available_partition',
                    'messages', '[]'::jsonb
                )
            );
            v_results := v_results || v_result;
            CONTINUE;
        END IF;
        
        -- =====================================================================
        -- STEP 2: Ensure consumer row exists
        -- =====================================================================
        INSERT INTO queen.partition_consumers (partition_id, consumer_group)
        VALUES (v_partition_id, v_req.consumer_group)
        ON CONFLICT (partition_id, consumer_group) DO NOTHING;
        
        -- =====================================================================
        -- STEP 3: Acquire lease (should succeed since we have SKIP LOCKED)
        -- =====================================================================
        v_effective_lease := COALESCE(NULLIF(v_req.lease_seconds, 0), NULLIF(v_lease_time, 0), 60);
        
        UPDATE queen.partition_consumers pc
        SET 
            lease_acquired_at = v_now,
            lease_expires_at = v_now + (v_effective_lease * INTERVAL '1 second'),
            worker_id = v_req.worker_id,
            batch_size = v_req.batch_size,
            acked_count = 0,
            batch_retry_count = COALESCE(pc.batch_retry_count, 0)
        WHERE pc.partition_id = v_partition_id
          AND pc.consumer_group = v_req.consumer_group
          AND (pc.lease_expires_at IS NULL OR pc.lease_expires_at <= v_now)
        RETURNING pc.last_consumed_id, pc.last_consumed_created_at
        INTO v_cursor_id, v_cursor_ts;
        
        IF NOT FOUND THEN
            -- Edge case: lease was acquired between our check and update
            -- This shouldn't happen with proper SKIP LOCKED, but safety first
            v_result := jsonb_build_object(
                'idx', v_req.idx,
                'result', jsonb_build_object(
                    'success', false,
                    'error', 'lease_race_condition',
                    'messages', '[]'::jsonb
                )
            );
            v_results := v_results || v_result;
            CONTINUE;
        END IF;
        
        -- =====================================================================
        -- STEP 4: Handle subscription modes for new consumers
        -- =====================================================================
        IF v_cursor_ts IS NULL AND v_req.consumer_group != '__QUEUE_MODE__' THEN
            SELECT cgm.subscription_timestamp
            INTO v_sub_ts
            FROM queen.consumer_groups_metadata cgm
            WHERE cgm.consumer_group = v_req.consumer_group
              AND cgm.queue_name = v_req.queue_name
              AND (cgm.partition_name = v_partition_name OR cgm.partition_name = '')
            ORDER BY CASE WHEN cgm.partition_name != '' THEN 1 ELSE 2 END
            LIMIT 1;
            
            IF v_sub_ts IS NOT NULL THEN
                -- Use pre-recorded subscription timestamp
                v_cursor_ts := v_sub_ts;
                v_cursor_id := '00000000-0000-0000-0000-000000000000'::uuid;
            ELSIF v_req.sub_from != '' AND v_req.sub_from != 'now' THEN
                -- Explicit timestamp provided: parse and record it
                BEGIN
                    v_cursor_ts := v_req.sub_from::timestamptz;
                    v_cursor_id := '00000000-0000-0000-0000-000000000000'::uuid;
                    
                    -- Insert subscription metadata with explicit timestamp
                    -- Use partition_name = '' (queue-wide) so ALL partitions share
                    -- the same subscription timestamp via the metadata lookup
                    INSERT INTO queen.consumer_groups_metadata (
                        consumer_group, queue_name, partition_name, subscription_mode, subscription_timestamp
                    ) VALUES (
                        v_req.consumer_group, v_req.queue_name, '', 'timestamp', v_cursor_ts
                    )
                    ON CONFLICT (consumer_group, queue_name, partition_name, namespace, task) DO NOTHING;
                    
                    -- Bootstrap: if this is a new registration, pre-seed partition_consumers
                    -- for ALL existing partitions to avoid slow one-at-a-time discovery
                    GET DIAGNOSTICS v_seeded = ROW_COUNT;
                    IF v_seeded > 0 THEN
                        INSERT INTO queen.partition_consumers (partition_id, consumer_group, last_consumed_at)
                        SELECT pl.partition_id, v_req.consumer_group, v_now
                        FROM queen.partition_lookup pl
                        WHERE pl.queue_name = v_req.queue_name
                        ON CONFLICT (partition_id, consumer_group) DO NOTHING;
                    END IF;
                EXCEPTION WHEN OTHERS THEN
                    -- Invalid timestamp format - fall through to default behavior
                    NULL;
                END;
            ELSIF v_req.sub_from = 'now' OR v_req.sub_mode = 'new' THEN
                -- New subscription with "new" mode: record timestamp so all workers share it
                v_cursor_ts := v_now;
                v_cursor_id := '00000000-0000-0000-0000-000000000000'::uuid;
                
                -- Insert subscription metadata (idempotent - ON CONFLICT ignores duplicates)
                -- Use partition_name = '' (queue-wide) so ALL partitions share
                -- the same subscription timestamp via the metadata lookup
                INSERT INTO queen.consumer_groups_metadata (
                    consumer_group, queue_name, partition_name, subscription_mode, subscription_timestamp
                ) VALUES (
                    v_req.consumer_group, v_req.queue_name, '', 'new', v_now
                )
                ON CONFLICT (consumer_group, queue_name, partition_name, namespace, task) DO NOTHING;
                
                -- Bootstrap: if this is a new registration, pre-seed partition_consumers
                -- for ALL existing partitions to avoid slow one-at-a-time discovery
                GET DIAGNOSTICS v_seeded = ROW_COUNT;
                IF v_seeded > 0 THEN
                    INSERT INTO queen.partition_consumers (partition_id, consumer_group, last_consumed_at)
                    SELECT pl.partition_id, v_req.consumer_group, v_now
                    FROM queen.partition_lookup pl
                    WHERE pl.queue_name = v_req.queue_name
                    ON CONFLICT (partition_id, consumer_group) DO NOTHING;
                END IF;
            END IF;
        END IF;
        
        -- =====================================================================
        -- STEP 5: Fetch messages
        -- =====================================================================
        SELECT COALESCE(jsonb_agg(
            jsonb_build_object(
                'id', sub.id::text,
                'transactionId', sub.transaction_id,
                'traceId', sub.trace_id::text,
                'data', sub.payload,
                'producerSub', sub.producer_sub,
                'createdAt', to_char(sub.created_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
            ) ORDER BY sub.created_at, sub.id
        ), '[]'::jsonb)
        INTO v_messages
        FROM (
            SELECT m.id, m.transaction_id, m.trace_id, m.payload, m.producer_sub, m.created_at
            FROM queen.messages m
            WHERE m.partition_id = v_partition_id
              AND (v_cursor_ts IS NULL OR (m.created_at, m.id) > (v_cursor_ts, COALESCE(v_cursor_id, '00000000-0000-0000-0000-000000000000'::uuid)))
              -- NOTE: window_buffer is checked at partition level (debounce), not per-message
              -- delayed_processing: per-message delay
              AND (v_delayed_processing IS NULL OR v_delayed_processing = 0 
                   OR m.created_at <= v_now - (v_delayed_processing || ' seconds')::interval)
            ORDER BY m.created_at, m.id
            LIMIT v_req.batch_size
        ) sub;
        
        v_msg_count := jsonb_array_length(v_messages);
        
        -- =====================================================================
        -- STEP 6: Cleanup
        -- =====================================================================
        IF v_msg_count = 0 THEN
            -- No messages: release lease immediately
            -- CRITICAL FIX: If a subscription timestamp was applied and there were no messages,
            -- we need to update the cursor to prevent infinite re-selection of this partition.
            IF v_cursor_ts IS NOT NULL AND v_cursor_id = '00000000-0000-0000-0000-000000000000'::uuid THEN
                -- Find the last message at or before the subscription timestamp
                SELECT m.id, m.created_at 
                INTO v_last_msg_id, v_last_msg_ts
                FROM queen.messages m
                WHERE m.partition_id = v_partition_id
                  AND m.created_at <= v_cursor_ts
                ORDER BY m.created_at DESC, m.id DESC
                LIMIT 1;
                
                IF v_last_msg_id IS NOT NULL THEN
                    -- Found a message before the timestamp, set cursor to it
                    UPDATE queen.partition_consumers
                    SET lease_expires_at = NULL, 
                        lease_acquired_at = NULL, 
                        worker_id = NULL,
                        last_consumed_id = v_last_msg_id,
                        last_consumed_created_at = v_last_msg_ts,
                        last_consumed_at = v_now
                    WHERE partition_id = v_partition_id
                      AND consumer_group = v_req.consumer_group
                      AND worker_id = v_req.worker_id;
                ELSE
                    -- No messages before the timestamp, just update last_consumed_at
                    UPDATE queen.partition_consumers
                    SET lease_expires_at = NULL, 
                        lease_acquired_at = NULL, 
                        worker_id = NULL,
                        last_consumed_at = v_now
                    WHERE partition_id = v_partition_id
                      AND consumer_group = v_req.consumer_group
                      AND worker_id = v_req.worker_id;
                END IF;
            ELSE
                -- No subscription timestamp applied, just release lease
                UPDATE queen.partition_consumers
                SET lease_expires_at = NULL, lease_acquired_at = NULL, worker_id = NULL
                WHERE partition_id = v_partition_id
                  AND consumer_group = v_req.consumer_group
                  AND worker_id = v_req.worker_id;
            END IF;
        ELSIF v_msg_count < v_req.batch_size THEN
            UPDATE queen.partition_consumers
            SET batch_size = v_msg_count, acked_count = 0
            WHERE partition_id = v_partition_id
              AND consumer_group = v_req.consumer_group
              AND worker_id = v_req.worker_id;
        END IF;
        
        -- =====================================================================
        -- STEP 7: Build result (wrapped in 'result' object for C++ compatibility)
        -- =====================================================================
        v_result := jsonb_build_object(
            'idx', v_req.idx,
            'result', jsonb_build_object(
                'success', true,
                'queue', v_req.queue_name,
                'partition', v_partition_name,
                'partitionId', v_partition_id::text,
                'leaseId', v_req.worker_id,
                'consumerGroup', v_req.consumer_group,
                'messages', v_messages
            )
        );
        v_results := v_results || v_result;
    END LOOP;
    
    RETURN v_results;
END;
$$;

-- ============================================================================
-- Grant Permissions
-- ============================================================================
GRANT EXECUTE ON FUNCTION queen.pop_specific_batch(JSONB) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.pop_discover_batch(JSONB) TO PUBLIC;

-- ============================================================================
-- 3. pop_unified_batch: Single entry point handling BOTH specific and wildcard
-- ============================================================================
-- Use when: You want a single C++ code path regardless of request type
-- Internally routes to appropriate logic based on partition_name presence
--
-- This is the RECOMMENDED procedure for simplest C++ integration.
-- It eliminates the need for any state machine - just fire and receive results.
--
-- AUTO-ACK SUPPORT (auto_ack: true):
--   When auto_ack=true, messages are automatically acknowledged in the same
--   transaction. The cursor is advanced to the last message and the lease is
--   released immediately. No separate ACK call is needed.
--   - Use case: Fire-and-forget processing, QoS 0 (at-most-once delivery)
--   - Response: leaseId will be NULL to signal no ACK is needed
-- ============================================================================

CREATE OR REPLACE FUNCTION queen.pop_unified_batch(p_requests JSONB)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_now TIMESTAMPTZ := NOW();
    v_results JSONB := '[]'::jsonb;
    v_req RECORD;
    v_is_wildcard BOOLEAN;
    v_partition_id UUID;
    v_partition_name TEXT;
    v_queue_id UUID;
    v_lease_time INT;
    v_window_buffer INT;
    v_delayed_processing INT;
    v_effective_lease INT;
    v_cursor_id UUID;
    v_cursor_ts TIMESTAMPTZ;
    v_messages JSONB;
    v_msg_count INT;
    v_result JSONB;
    v_sub_ts TIMESTAMPTZ;
    -- AutoAck support: advance cursor and release lease in single operation
    v_auto_ack BOOLEAN;
    v_last_msg JSONB;
    v_last_msg_id UUID;
    v_last_msg_ts TIMESTAMPTZ;
    -- Window buffer debounce support: track last message timestamp for partition
    v_partition_last_msg_ts TIMESTAMPTZ;
    v_seeded INT;
    -- Watermark for efficient wildcard discovery
    v_watermark TIMESTAMPTZ;
    v_watermark_verified_at TIMESTAMPTZ;
    v_has_pending_data BOOLEAN;
BEGIN
    -- ==========================================================================
    -- Process all requests in a single pass
    -- Order by: wildcards LAST (so specific partitions are locked first in 
    -- deterministic order, reducing contention window for wildcards)
    -- ==========================================================================
    FOR v_req IN 
        SELECT 
            (r->>'idx')::INT AS idx,
            (r->>'queue_name')::TEXT AS queue_name,
            NULLIF(NULLIF(r->>'partition_name', ''), 'null') AS partition_name,
            COALESCE(r->>'consumer_group', '__QUEUE_MODE__') AS consumer_group,
            COALESCE((r->>'batch_size')::INT, 10) AS batch_size,
            COALESCE((r->>'lease_seconds')::INT, 0) AS lease_seconds,
            (r->>'worker_id')::TEXT AS worker_id,
            COALESCE(r->>'sub_mode', 'all') AS sub_mode,
            COALESCE(r->>'sub_from', '') AS sub_from,
            COALESCE((r->>'auto_ack')::BOOLEAN, false) AS auto_ack
        FROM jsonb_array_elements(p_requests) r
        -- Sort: specific partitions first (alphabetically for deadlock prevention),
        -- then wildcards (by idx to maintain request order)
        ORDER BY 
            CASE WHEN NULLIF(NULLIF(r->>'partition_name', ''), 'null') IS NULL THEN 1 ELSE 0 END,
            r->>'queue_name', 
            r->>'partition_name',
            (r->>'idx')::INT
    LOOP
        -- Reset per-iteration state
        v_partition_id := NULL;
        v_partition_name := NULL;
        v_cursor_id := NULL;
        v_cursor_ts := NULL;
        v_messages := '[]'::jsonb;
        v_is_wildcard := (v_req.partition_name IS NULL);
        v_auto_ack := v_req.auto_ack;
        v_last_msg := NULL;
        v_last_msg_id := NULL;
        v_last_msg_ts := NULL;
        
        -- =====================================================================
        -- BRANCH: Wildcard Discovery vs Specific Partition
        -- =====================================================================
        IF v_is_wildcard THEN
            -- =================================================================
            -- WILDCARD: Discover available partition with SKIP LOCKED
            -- Uses "Caught-Up Watermark" optimization:
            -- - Fetch watermark (last time we found no data for this consumer)
            -- - Only scan partitions updated since (watermark - 2 minutes)
            -- - If nothing found, advance watermark to NOW()
            -- This avoids full scans for up-to-date consumers (~99% savings)
            -- =================================================================
            
            -- Step 1: Fetch watermark AND last verification time
            SELECT last_empty_scan_at, updated_at 
            INTO v_watermark, v_watermark_verified_at
            FROM queen.consumer_watermarks
            WHERE queue_name = v_req.queue_name
              AND consumer_group = v_req.consumer_group;
            
            -- Default to epoch for new consumers (will scan all partitions)
            IF v_watermark IS NULL THEN
                v_watermark := '1970-01-01 00:00:00+00'::TIMESTAMPTZ;
                v_watermark_verified_at := NULL;
            END IF;
            
            -- Step 2: Single query - only partitions updated since watermark
            -- The 2-minute buffer handles clock skew and race conditions
            SELECT 
                pl.partition_id,
                p.name,
                q.id,
                q.lease_time,
                q.window_buffer,
                q.delayed_processing,
                pl.last_message_created_at
            INTO v_partition_id, v_partition_name, v_queue_id, v_lease_time, v_window_buffer, v_delayed_processing, v_partition_last_msg_ts
            FROM queen.partition_lookup pl
            JOIN queen.partitions p ON p.id = pl.partition_id
            JOIN queen.queues q ON q.id = p.queue_id
            LEFT JOIN queen.partition_consumers pc 
                ON pc.partition_id = pl.partition_id 
                AND pc.consumer_group = v_req.consumer_group
            WHERE pl.queue_name = v_req.queue_name
              AND pl.updated_at >= (v_watermark - interval '2 minutes')
              AND (pc.lease_expires_at IS NULL OR pc.lease_expires_at <= v_now)
              AND (pc.last_consumed_created_at IS NULL 
                   OR pl.last_message_created_at > pc.last_consumed_created_at
                   OR (pl.last_message_created_at = pc.last_consumed_created_at 
                       AND pl.last_message_id > pc.last_consumed_id))
              AND (q.window_buffer IS NULL OR q.window_buffer = 0
                   OR pl.last_message_created_at <= v_now - (q.window_buffer || ' seconds')::interval)
            LIMIT 1
            FOR UPDATE OF pl SKIP LOCKED;
            
            -- Step 3: If nothing found, maybe verify with EXISTS check
            -- EXISTS is expensive (~13ms) so we cache the result for 30 seconds
            -- EXISTS does NOT check lease status - it detects if unconsumed data EXISTS
            -- This prevents advancing watermark past data that's temporarily locked
            IF v_partition_id IS NULL THEN
                -- Only run EXISTS if we haven't verified recently (within 30 seconds)
                -- 30s is safe given production data shows 0.01% lock contention and 2+ minute leases
                IF v_watermark_verified_at IS NULL OR v_watermark_verified_at <= v_now - interval '30 seconds' THEN
                    -- Time to re-verify: run EXISTS check
                    -- NOTE: No lease check here! We want to know if unconsumed data EXISTS,
                    -- regardless of whether it's currently leased by another worker.
                    -- This prevents advancing watermark past data that's temporarily locked.
                    -- Uses watermark filter for performance (safe now that Bug 1 is fixed).
                    SELECT EXISTS (
                        SELECT 1
                        FROM queen.partition_lookup pl
                        JOIN queen.partitions p ON p.id = pl.partition_id
                        JOIN queen.queues q ON q.id = p.queue_id
                        LEFT JOIN queen.partition_consumers pc 
                            ON pc.partition_id = pl.partition_id 
                            AND pc.consumer_group = v_req.consumer_group
                        WHERE pl.queue_name = v_req.queue_name
                          AND pl.updated_at >= (v_watermark - interval '2 minutes')
                          AND (pc.last_consumed_created_at IS NULL 
                               OR pl.last_message_created_at > pc.last_consumed_created_at
                               OR (pl.last_message_created_at = pc.last_consumed_created_at 
                                   AND pl.last_message_id > pc.last_consumed_id))
                          AND (q.window_buffer IS NULL OR q.window_buffer = 0
                               OR pl.last_message_created_at <= v_now - (q.window_buffer || ' seconds')::interval)
                    ) INTO v_has_pending_data;
                    
                    -- Only advance watermark if truly no pending data
                    IF NOT v_has_pending_data THEN
                        -- TRULY EMPTY: Advance both the watermark and the cache timestamp
                        INSERT INTO queen.consumer_watermarks (queue_name, consumer_group, last_empty_scan_at, updated_at)
                        VALUES (v_req.queue_name, v_req.consumer_group, v_now, v_now)
                        ON CONFLICT (queue_name, consumer_group) 
                        DO UPDATE SET last_empty_scan_at = v_now, updated_at = v_now;
                    ELSE
                        -- LOCKED DATA: Do NOT advance watermark, but DO update the cache timestamp.
                        -- This prevents EXISTS spam while data is locked (would otherwise run 10x/sec)
                        UPDATE queen.consumer_watermarks 
                        SET updated_at = v_now 
                        WHERE queue_name = v_req.queue_name AND consumer_group = v_req.consumer_group;
                    END IF;
                END IF;
                -- If recently verified, skip EXISTS - trust the watermark
            END IF;
            
            IF v_partition_id IS NULL THEN
                v_result := jsonb_build_object(
                    'idx', v_req.idx,
                    'result', jsonb_build_object(
                        'success', false,
                        'error', 'no_available_partition',
                        'messages', '[]'::jsonb
                    )
                );
                v_results := v_results || v_result;
                CONTINUE;
            END IF;
        ELSE
            -- =================================================================
            -- SPECIFIC: Direct partition lookup
            -- =================================================================
            v_partition_name := v_req.partition_name;
            
            SELECT p.id, q.id, q.lease_time, q.window_buffer, q.delayed_processing, pl.last_message_created_at
            INTO v_partition_id, v_queue_id, v_lease_time, v_window_buffer, v_delayed_processing, v_partition_last_msg_ts
            FROM queen.queues q
            JOIN queen.partitions p ON p.queue_id = q.id
            LEFT JOIN queen.partition_lookup pl ON pl.partition_id = p.id
            WHERE q.name = v_req.queue_name AND p.name = v_req.partition_name;
        
            IF v_partition_id IS NULL THEN
                v_result := jsonb_build_object(
                    'idx', v_req.idx, 
                    'result', jsonb_build_object(
                        'success', false, 
                        'error', 'partition_not_found',
                        'messages', '[]'::jsonb
                    )
                );
                v_results := v_results || v_result;
                CONTINUE;
            END IF;
            
            -- WINDOW BUFFER DEBOUNCE: Check if window has expired for specific partition
            -- If not expired, return empty result (debounce still active)
            IF v_window_buffer IS NOT NULL AND v_window_buffer > 0 
               AND v_partition_last_msg_ts IS NOT NULL
               AND v_partition_last_msg_ts > v_now - (v_window_buffer || ' seconds')::interval THEN
                v_result := jsonb_build_object(
                    'idx', v_req.idx,
                    'result', jsonb_build_object(
                        'success', true,
                        'queue', v_req.queue_name,
                        'partition', v_partition_name,
                        'partitionId', v_partition_id::text,
                        'leaseId', '',
                        'consumerGroup', v_req.consumer_group,
                        'messages', '[]'::jsonb,
                        'windowBufferActive', true
                    )
                );
                v_results := v_results || v_result;
                CONTINUE;
            END IF;
        END IF;
        
        -- =====================================================================
        -- COMMON PATH: Lease acquisition, fetch, cleanup
        -- =====================================================================
        
        -- Ensure consumer row exists
        INSERT INTO queen.partition_consumers (partition_id, consumer_group)
        VALUES (v_partition_id, v_req.consumer_group)
        ON CONFLICT (partition_id, consumer_group) DO NOTHING;
        
        -- Acquire lease
        v_effective_lease := COALESCE(NULLIF(v_req.lease_seconds, 0), NULLIF(v_lease_time, 0), 60);
        
        UPDATE queen.partition_consumers pc
        SET 
            lease_acquired_at = v_now,
            lease_expires_at = v_now + (v_effective_lease * INTERVAL '1 second'),
            worker_id = v_req.worker_id,
            batch_size = v_req.batch_size,
            acked_count = 0,
            batch_retry_count = COALESCE(pc.batch_retry_count, 0)
        WHERE pc.partition_id = v_partition_id
          AND pc.consumer_group = v_req.consumer_group
          AND (pc.lease_expires_at IS NULL OR pc.lease_expires_at <= v_now)
        RETURNING pc.last_consumed_id, pc.last_consumed_created_at
        INTO v_cursor_id, v_cursor_ts;
        
        IF NOT FOUND THEN
            v_result := jsonb_build_object(
                'idx', v_req.idx, 
                'result', jsonb_build_object(
                'success', false, 
                    'error', CASE WHEN v_is_wildcard THEN 'lease_race_condition' ELSE 'lease_held' END,
                    'messages', '[]'::jsonb
                )
            );
            v_results := v_results || v_result;
            CONTINUE;
        END IF;
        
        -- Handle subscription modes
        IF v_cursor_ts IS NULL AND v_req.consumer_group != '__QUEUE_MODE__' THEN
            SELECT cgm.subscription_timestamp
            INTO v_sub_ts
            FROM queen.consumer_groups_metadata cgm
            WHERE cgm.consumer_group = v_req.consumer_group
              AND cgm.queue_name = v_req.queue_name
              AND (cgm.partition_name = v_partition_name OR cgm.partition_name = '')
            ORDER BY CASE WHEN cgm.partition_name != '' THEN 1 ELSE 2 END
            LIMIT 1;
            
            IF v_sub_ts IS NOT NULL THEN
                -- Use pre-recorded subscription timestamp
                v_cursor_ts := v_sub_ts;
                v_cursor_id := '00000000-0000-0000-0000-000000000000'::uuid;
            ELSIF v_req.sub_from != '' AND v_req.sub_from != 'now' THEN
                -- Explicit timestamp provided: parse and record it
                BEGIN
                    v_cursor_ts := v_req.sub_from::timestamptz;
                    v_cursor_id := '00000000-0000-0000-0000-000000000000'::uuid;
                    
                    -- Insert subscription metadata with explicit timestamp
                    -- Wildcard: use '' (queue-wide) so ALL partitions share the timestamp
                    -- Specific: use the partition name for per-partition subscriptions
                    INSERT INTO queen.consumer_groups_metadata (
                        consumer_group, queue_name, partition_name, subscription_mode, subscription_timestamp
                    ) VALUES (
                        v_req.consumer_group, v_req.queue_name,
                        CASE WHEN v_is_wildcard THEN '' ELSE COALESCE(v_partition_name, '') END,
                        'timestamp', v_cursor_ts
                    )
                    ON CONFLICT (consumer_group, queue_name, partition_name, namespace, task) DO NOTHING;
                    
                    -- Bootstrap: if this is a new registration, pre-seed partition_consumers
                    -- for ALL existing partitions to avoid slow one-at-a-time discovery
                    GET DIAGNOSTICS v_seeded = ROW_COUNT;
                    IF v_seeded > 0 THEN
                        INSERT INTO queen.partition_consumers (partition_id, consumer_group, last_consumed_at)
                        SELECT pl.partition_id, v_req.consumer_group, v_now
                        FROM queen.partition_lookup pl
                        WHERE pl.queue_name = v_req.queue_name
                        ON CONFLICT (partition_id, consumer_group) DO NOTHING;
                    END IF;
                EXCEPTION WHEN OTHERS THEN
                    -- Invalid timestamp format - fall through to default behavior
                    NULL;
                END;
            ELSIF v_req.sub_from = 'now' OR v_req.sub_mode = 'new' THEN
                -- New subscription with "new" mode: record timestamp so all workers share it
                v_cursor_ts := v_now;
                v_cursor_id := '00000000-0000-0000-0000-000000000000'::uuid;
                
                -- Insert subscription metadata (idempotent - ON CONFLICT ignores duplicates)
                -- Wildcard: use '' (queue-wide) so ALL partitions share the timestamp
                -- Specific: use the partition name for per-partition subscriptions
                INSERT INTO queen.consumer_groups_metadata (
                    consumer_group, queue_name, partition_name, subscription_mode, subscription_timestamp
                ) VALUES (
                    v_req.consumer_group, v_req.queue_name,
                    CASE WHEN v_is_wildcard THEN '' ELSE COALESCE(v_partition_name, '') END,
                    'new', v_now
                )
                ON CONFLICT (consumer_group, queue_name, partition_name, namespace, task) DO NOTHING;
                
                -- Bootstrap: if this is a new registration, pre-seed partition_consumers
                -- for ALL existing partitions to avoid slow one-at-a-time discovery
                GET DIAGNOSTICS v_seeded = ROW_COUNT;
                IF v_seeded > 0 THEN
                    INSERT INTO queen.partition_consumers (partition_id, consumer_group, last_consumed_at)
                    SELECT pl.partition_id, v_req.consumer_group, v_now
                    FROM queen.partition_lookup pl
                    WHERE pl.queue_name = v_req.queue_name
                    ON CONFLICT (partition_id, consumer_group) DO NOTHING;
                END IF;
            END IF;
        END IF;
        
        -- Fetch messages
        -- NOTE: window_buffer is now checked at partition level (debounce), not per-message
        -- Only delayed_processing applies per-message delay
        SELECT COALESCE(jsonb_agg(
            jsonb_build_object(
                'id', sub.id::text,
                'transactionId', sub.transaction_id,
                'traceId', sub.trace_id::text,
                'data', sub.payload,
                'producerSub', sub.producer_sub,
                'createdAt', to_char(sub.created_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
            ) ORDER BY sub.created_at, sub.id
        ), '[]'::jsonb)
        INTO v_messages
        FROM (
            SELECT m.id, m.transaction_id, m.trace_id, m.payload, m.producer_sub, m.created_at
            FROM queen.messages m
            WHERE m.partition_id = v_partition_id
              AND (v_cursor_ts IS NULL OR (m.created_at, m.id) > (v_cursor_ts, COALESCE(v_cursor_id, '00000000-0000-0000-0000-000000000000'::uuid)))
              -- delayed_processing: per-message delay (each message waits N seconds after creation)
              AND (v_delayed_processing IS NULL OR v_delayed_processing = 0 
                   OR m.created_at <= v_now - (v_delayed_processing || ' seconds')::interval)
            ORDER BY m.created_at, m.id
            LIMIT v_req.batch_size
        ) sub;
        
        v_msg_count := jsonb_array_length(v_messages);
        
        -- Cleanup / AutoAck handling
        IF v_auto_ack AND v_msg_count > 0 THEN
            -- =====================================================================
            -- AUTO-ACK: Advance cursor to last message AND release lease immediately
            -- This combines POP + ACK in a single atomic operation (QoS 0)
            -- Also updates consumption counters (same as explicit ACK would do)
            -- =====================================================================
            -- Extract last message ID from JSON (messages are ordered by created_at, id)
            v_last_msg := v_messages->(v_msg_count - 1);
            v_last_msg_id := (v_last_msg->>'id')::UUID;
            
            -- Get exact timestamp from database (preserves microsecond precision)
            -- This avoids precision loss from JSON string formatting
            SELECT created_at INTO v_last_msg_ts FROM queen.messages WHERE id = v_last_msg_id;
            
            -- Update cursor to last message AND release lease AND increment consumption counter
            UPDATE queen.partition_consumers
            SET 
                last_consumed_id = v_last_msg_id,
                last_consumed_created_at = v_last_msg_ts,
                last_consumed_at = v_now,
                lease_expires_at = NULL,
                lease_acquired_at = NULL,
                worker_id = NULL,
                batch_size = 0,
                acked_count = 0,
                -- INCREMENT consumption counter (same as ACK does)
                total_messages_consumed = COALESCE(total_messages_consumed, 0) + v_msg_count,
                total_batches_consumed = COALESCE(total_batches_consumed, 0) + 1
            WHERE partition_id = v_partition_id
              AND consumer_group = v_req.consumer_group
              AND worker_id = v_req.worker_id;
            
            -- Record consumption for throughput tracking (same as ACK does)
            INSERT INTO queen.messages_consumed (partition_id, consumer_group, messages_completed, acked_at)
            VALUES (v_partition_id, v_req.consumer_group, v_msg_count, v_now);
        ELSIF v_msg_count = 0 THEN
            -- No messages: release lease immediately
            -- CRITICAL FIX: If a subscription timestamp was applied and there were no messages,
            -- we need to update the cursor to prevent infinite re-selection of this partition.
            -- We find the last message AT or BEFORE the subscription timestamp and use that as cursor.
            IF v_cursor_ts IS NOT NULL AND v_cursor_id = '00000000-0000-0000-0000-000000000000'::uuid THEN
                -- Find the last message at or before the subscription timestamp
                SELECT m.id, m.created_at 
                INTO v_last_msg_id, v_last_msg_ts
                FROM queen.messages m
                WHERE m.partition_id = v_partition_id
                  AND m.created_at <= v_cursor_ts
                ORDER BY m.created_at DESC, m.id DESC
                LIMIT 1;
                
                IF v_last_msg_id IS NOT NULL THEN
                    -- Found a message before the timestamp, set cursor to it
                    UPDATE queen.partition_consumers
                    SET lease_expires_at = NULL, 
                        lease_acquired_at = NULL, 
                        worker_id = NULL,
                        last_consumed_id = v_last_msg_id,
                        last_consumed_created_at = v_last_msg_ts,
                        last_consumed_at = v_now
                    WHERE partition_id = v_partition_id
                      AND consumer_group = v_req.consumer_group
                      AND worker_id = v_req.worker_id;
                ELSE
                    -- No messages before the timestamp, just update last_consumed_at
                    -- to move this partition to the back of the discovery queue
                    UPDATE queen.partition_consumers
                    SET lease_expires_at = NULL, 
                        lease_acquired_at = NULL, 
                        worker_id = NULL,
                        last_consumed_at = v_now
                    WHERE partition_id = v_partition_id
                      AND consumer_group = v_req.consumer_group
                      AND worker_id = v_req.worker_id;
                END IF;
            ELSE
                -- No subscription timestamp applied, just release lease
                UPDATE queen.partition_consumers
                SET lease_expires_at = NULL, lease_acquired_at = NULL, worker_id = NULL
                WHERE partition_id = v_partition_id
                  AND consumer_group = v_req.consumer_group
                  AND worker_id = v_req.worker_id;
            END IF;
        ELSIF v_msg_count < v_req.batch_size THEN
            -- Partial batch: update batch_size so ACKing all messages releases lease
            UPDATE queen.partition_consumers
            SET batch_size = v_msg_count, acked_count = 0
            WHERE partition_id = v_partition_id
              AND consumer_group = v_req.consumer_group
              AND worker_id = v_req.worker_id;
        END IF;
        
        -- Build result (wrapped in 'result' object for compatibility with existing C++ code)
        -- For auto_ack: return empty string leaseId to signal no ACK is needed
        v_result := jsonb_build_object(
            'idx', v_req.idx,
            'result', jsonb_build_object(
                'success', true,
                'queue', v_req.queue_name,
                'partition', v_partition_name,
                'partitionId', v_partition_id::text,
                'leaseId', CASE WHEN v_auto_ack THEN '' ELSE v_req.worker_id END,
                'consumerGroup', v_req.consumer_group,
                'messages', v_messages
            )
        );
        v_results := v_results || v_result;
    END LOOP;
    
    RETURN v_results;
END;
$$;

GRANT EXECUTE ON FUNCTION queen.pop_unified_batch(JSONB) TO PUBLIC;

-- ============================================================================
-- Usage Examples
-- ============================================================================
/*
-- Example 1: Pop from known partitions (uses pop_specific_batch logic internally)
SELECT queen.pop_specific_batch('[
    {"idx": 0, "queue_name": "orders", "partition_name": "region-us", "consumer_group": "processor", "batch_size": 10, "worker_id": "worker-1"},
    {"idx": 1, "queue_name": "orders", "partition_name": "region-eu", "consumer_group": "processor", "batch_size": 10, "worker_id": "worker-2"}
]'::jsonb);

-- Example 2: Discover available partitions (uses pop_discover_batch logic internally)
SELECT queen.pop_discover_batch('[
    {"idx": 0, "queue_name": "orders", "consumer_group": "processor", "batch_size": 10, "worker_id": "worker-1"},
    {"idx": 1, "queue_name": "orders", "consumer_group": "processor", "batch_size": 10, "worker_id": "worker-2"}
]'::jsonb);
-- ^ Each will get a DIFFERENT partition due to SKIP LOCKED

-- Example 3: UNIFIED - Mix of specific and wildcard in one call
SELECT queen.pop_unified_batch('[
    {"idx": 0, "queue_name": "orders", "partition_name": "priority", "consumer_group": "processor", "batch_size": 10, "worker_id": "worker-1"},
    {"idx": 1, "queue_name": "orders", "consumer_group": "processor", "batch_size": 10, "worker_id": "worker-2"},
    {"idx": 2, "queue_name": "orders", "consumer_group": "processor", "batch_size": 10, "worker_id": "worker-3"}
]'::jsonb);
-- ^ idx=0 gets specific "priority" partition, idx=1 and idx=2 each discover different available partitions

-- Example 4: AUTO-ACK - Pop and immediately acknowledge (QoS 0, at-most-once delivery)
SELECT queen.pop_unified_batch('[
    {"idx": 0, "queue_name": "logs", "consumer_group": "analytics", "batch_size": 100, "worker_id": "worker-1", "auto_ack": true}
]'::jsonb);
-- ^ Messages are auto-acknowledged: cursor is advanced, lease is released, leaseId returned as NULL
-- ^ No separate ACK call needed - single round-trip for fire-and-forget processing
*/
