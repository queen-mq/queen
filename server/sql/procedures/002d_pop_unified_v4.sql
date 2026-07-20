-- ============================================================================
-- pop_unified_batch_v4: multi-partition wildcard pop
-- ============================================================================
-- Descendant of pop_unified_batch_v3 (002c_pop_unified_v3.sql). Adds the
-- ability to drain messages from up to N partitions in a single call,
-- aggregating them into one response. Designed for the "many partitions,
-- each with few new messages" workload where claiming one partition per
-- network round-trip is wasteful.
--
-- Per-request input (in addition to v3 fields):
--   max_partitions   INT, default 1.   Hard cap on partitions claimed.
--   batch_size       INT, default 10.  Now a GLOBAL cap on total messages
--                                       returned across all claimed partitions
--                                       (not per-partition). This is the key
--                                       difference from v3's batch_size.
--
-- Inner-loop exit conditions (whichever fires first):
--   * v_remaining          <= 0   (global message budget exhausted)
--   * v_claimed_count      >= max_partitions
--   * candidate scan ends         (no more eligible partitions)
--
-- Per-message wire format:
--   Each element of result.messages now carries its own partitionId,
--   leaseId, partition, and consumerGroup. The top-level result.partitionId /
--   leaseId / partition are set to the FIRST claimed partition for back-compat
--   with code that reads them; the per-message fields are authoritative when
--   the batch spans multiple partitions.
--
-- Lease semantics:
--   All N claimed partitions share the same worker_id (= leaseId), so a
--   single renew_lease_v2 call extends every partition's lease atomically
--   (renew_lease_v2 filters on worker_id alone). ACK validation in
--   ack_messages_v2 keys on (partition_id, consumer_group, worker_id) so
--   each ACK targets the correct row regardless of partition count.
--
-- Auto-release-on-full-batch:
--   Each partition's partition_consumers.batch_size is set to ITS OWN message
--   count (not the request's batch_size), so the auto-release logic in
--   ack_messages_v2 (acked_count >= batch_size) fires correctly per-partition.
--
-- Backwards compatibility:
--   With max_partitions=1 (the default), behavior is byte-equivalent to v3
--   plus per-message field bake-in. The libqueen dispatcher ships v4 with
--   default 1, so existing callers see no change.
-- ============================================================================

CREATE OR REPLACE FUNCTION queen.pop_unified_batch_v4(p_requests JSONB)
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
    v_auto_ack BOOLEAN;
    v_last_msg JSONB;
    v_last_msg_id UUID;
    v_last_msg_ts TIMESTAMPTZ;
    v_partition_last_msg_ts TIMESTAMPTZ;
    v_seeded INT;
    v_watermark TIMESTAMPTZ;
    v_watermark_verified_at TIMESTAMPTZ;
    v_has_pending_data BOOLEAN;
    v_cand RECORD;
    -- v4 multi-partition state (wildcard branch)
    v_max_partitions INT;
    v_remaining INT;
    v_claimed_count INT;
    v_all_messages JSONB;
    v_first_partition_id UUID;
    v_first_partition_name TEXT;
    c_lock_ns CONSTANT BIGINT := 12648430;
BEGIN
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
            COALESCE((r->>'auto_ack')::BOOLEAN, false) AS auto_ack,
            GREATEST(COALESCE((r->>'max_partitions')::INT, 1), 1) AS max_partitions
        FROM jsonb_array_elements(p_requests) r
        ORDER BY
            CASE WHEN NULLIF(NULLIF(r->>'partition_name', ''), 'null') IS NULL THEN 1 ELSE 0 END,
            r->>'queue_name',
            r->>'partition_name',
            (r->>'idx')::INT
    LOOP
        v_is_wildcard := (v_req.partition_name IS NULL);
        v_auto_ack := v_req.auto_ack;
        v_max_partitions := v_req.max_partitions;
        v_remaining := GREATEST(v_req.batch_size, 1);
        v_claimed_count := 0;
        v_all_messages := '[]'::jsonb;
        v_first_partition_id := NULL;
        v_first_partition_name := NULL;

        IF v_is_wildcard THEN
            -- Watermark prelude (unchanged from v3).
            SELECT last_empty_scan_at, updated_at
            INTO v_watermark, v_watermark_verified_at
            FROM queen.consumer_watermarks
            WHERE queue_name = v_req.queue_name
              AND consumer_group = v_req.consumer_group;

            IF v_watermark IS NULL THEN
                v_watermark := '1970-01-01 00:00:00+00'::TIMESTAMPTZ;
                v_watermark_verified_at := NULL;
            END IF;

            -- Multi-claim candidate loop. Iterates eligible partitions and
            -- attempts a non-blocking advisory-lock claim on each. Continues
            -- past the first claim (unlike v3's EXIT) and accumulates
            -- messages from up to v_max_partitions partitions or until the
            -- global v_remaining budget runs out.
            FOR v_cand IN
                SELECT
                    pl.partition_id,
                    p.name               AS partition_name,
                    q.id                 AS queue_id,
                    q.lease_time         AS lease_time,
                    q.window_buffer      AS window_buffer,
                    q.delayed_processing AS delayed_processing,
                    pl.last_message_created_at AS last_message_ts
                FROM queen.partition_lookup pl
                JOIN queen.partitions p ON p.id = pl.partition_id
                JOIN queen.queues     q ON q.id = p.queue_id
                LEFT JOIN queen.partition_consumers pc
                    ON pc.partition_id   = pl.partition_id
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
            LOOP
                EXIT WHEN v_remaining <= 0;
                EXIT WHEN v_claimed_count >= v_max_partitions;

                IF NOT pg_try_advisory_xact_lock(
                        hashtextextended(v_cand.partition_id::text, c_lock_ns)
                   ) THEN
                    CONTINUE;
                END IF;

                -- Successful claim. Process this partition inline.
                v_partition_id          := v_cand.partition_id;
                v_partition_name        := v_cand.partition_name;
                v_queue_id              := v_cand.queue_id;
                v_lease_time            := v_cand.lease_time;
                v_window_buffer         := v_cand.window_buffer;
                v_delayed_processing    := v_cand.delayed_processing;
                v_partition_last_msg_ts := v_cand.last_message_ts;

                INSERT INTO queen.partition_consumers (partition_id, consumer_group)
                VALUES (v_partition_id, v_req.consumer_group)
                ON CONFLICT (partition_id, consumer_group) DO NOTHING;

                v_effective_lease := COALESCE(NULLIF(v_req.lease_seconds, 0), NULLIF(v_lease_time, 0), 60);
                v_cursor_id := NULL;
                v_cursor_ts := NULL;

                UPDATE queen.partition_consumers pc
                SET
                    lease_acquired_at  = v_now,
                    lease_expires_at   = v_now + (v_effective_lease * INTERVAL '1 second'),
                    worker_id          = v_req.worker_id,
                    batch_size         = v_req.batch_size,
                    acked_count        = 0,
                    batch_retry_count  = COALESCE(pc.batch_retry_count, 0)
                WHERE pc.partition_id   = v_partition_id
                  AND pc.consumer_group = v_req.consumer_group
                  AND (pc.lease_expires_at IS NULL OR pc.lease_expires_at <= v_now)
                RETURNING pc.last_consumed_id, pc.last_consumed_created_at
                INTO v_cursor_id, v_cursor_ts;

                IF NOT FOUND THEN
                    -- Lease race lost despite holding the advisory lock —
                    -- pathological. Skip without counting against the cap.
                    CONTINUE;
                END IF;

                -- Cursor seeding for new consumer groups. The bulk-seed
                -- side effect (consumer_groups_metadata + bulk
                -- partition_consumers insert) is idempotent via
                -- ON CONFLICT, so subsequent claims in the same call
                -- are no-ops past the first seed.
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
                        v_cursor_ts := v_sub_ts;
                        v_cursor_id := '00000000-0000-0000-0000-000000000000'::uuid;
                    ELSIF v_req.sub_from != '' AND v_req.sub_from != 'now' THEN
                        BEGIN
                            v_cursor_ts := v_req.sub_from::timestamptz;
                            v_cursor_id := '00000000-0000-0000-0000-000000000000'::uuid;

                            INSERT INTO queen.consumer_groups_metadata (
                                consumer_group, queue_name, partition_name, subscription_mode, subscription_timestamp
                            ) VALUES (
                                v_req.consumer_group, v_req.queue_name,
                                '',
                                'timestamp', v_cursor_ts
                            )
                            ON CONFLICT (consumer_group, queue_name, partition_name, namespace, task) DO NOTHING;

                            GET DIAGNOSTICS v_seeded = ROW_COUNT;
                            IF v_seeded > 0 THEN
                                INSERT INTO queen.partition_consumers (partition_id, consumer_group, last_consumed_at)
                                SELECT pl.partition_id, v_req.consumer_group, v_now
                                FROM queen.partition_lookup pl
                                WHERE pl.queue_name = v_req.queue_name
                                ON CONFLICT (partition_id, consumer_group) DO NOTHING;
                            END IF;
                        EXCEPTION WHEN OTHERS THEN
                            NULL;
                        END;
                    ELSIF v_req.sub_from = 'now' OR v_req.sub_mode = 'new' THEN
                        v_cursor_ts := v_now;
                        v_cursor_id := '00000000-0000-0000-0000-000000000000'::uuid;

                        INSERT INTO queen.consumer_groups_metadata (
                            consumer_group, queue_name, partition_name, subscription_mode, subscription_timestamp
                        ) VALUES (
                            v_req.consumer_group, v_req.queue_name,
                            '',
                            'new', v_now
                        )
                        ON CONFLICT (consumer_group, queue_name, partition_name, namespace, task) DO NOTHING;

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

                -- Read messages with global cap. v_remaining shrinks across
                -- the multi-claim loop; the LIMIT here is the per-partition
                -- contribution to the global budget.
                SELECT COALESCE(jsonb_agg(
                    jsonb_build_object(
                        'id', sub.id::text,
                        'transactionId', sub.transaction_id,
                        'traceId', sub.trace_id::text,
                        'data', sub.payload,
                        'producerSub', sub.producer_sub,
                        'createdAt', to_char(sub.created_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                        'partitionId', v_partition_id::text,
                        'partition', v_partition_name,
                        'leaseId', CASE WHEN v_auto_ack THEN '' ELSE v_req.worker_id END,
                        'consumerGroup', v_req.consumer_group
                    ) ORDER BY sub.created_at, sub.id
                ), '[]'::jsonb)
                INTO v_messages
                FROM (
                    SELECT m.id, m.transaction_id, m.trace_id, m.payload, m.producer_sub, m.created_at
                    FROM queen.messages m
                    WHERE m.partition_id = v_partition_id
                      AND (v_cursor_ts IS NULL OR (m.created_at, m.id) > (v_cursor_ts, COALESCE(v_cursor_id, '00000000-0000-0000-0000-000000000000'::uuid)))
                      AND (v_delayed_processing IS NULL OR v_delayed_processing = 0
                           OR m.created_at <= v_now - (v_delayed_processing || ' seconds')::interval)
                    ORDER BY m.created_at, m.id
                    LIMIT v_remaining
                ) sub;

                v_msg_count := jsonb_array_length(v_messages);

                IF v_auto_ack AND v_msg_count > 0 THEN
                    v_last_msg := v_messages->(v_msg_count - 1);
                    v_last_msg_id := (v_last_msg->>'id')::UUID;
                    SELECT created_at INTO v_last_msg_ts FROM queen.messages WHERE id = v_last_msg_id;

                    UPDATE queen.partition_consumers
                    SET
                        last_consumed_id          = v_last_msg_id,
                        last_consumed_created_at  = v_last_msg_ts,
                        last_consumed_at          = v_now,
                        lease_expires_at          = NULL,
                        lease_acquired_at         = NULL,
                        worker_id                 = NULL,
                        batch_size                = 0,
                        acked_count               = 0,
                        total_messages_consumed   = COALESCE(total_messages_consumed, 0) + v_msg_count,
                        total_batches_consumed    = COALESCE(total_batches_consumed, 0) + 1
                    WHERE partition_id   = v_partition_id
                      AND consumer_group = v_req.consumer_group
                      AND worker_id      = v_req.worker_id;

                    INSERT INTO queen.messages_consumed (partition_id, consumer_group, messages_completed, acked_at)
                    VALUES (v_partition_id, v_req.consumer_group, v_msg_count, v_now);
                ELSIF v_msg_count = 0 THEN
                    -- Per-partition cleanup-on-empty (cursor sweep + lease release).
                    IF v_cursor_ts IS NOT NULL AND v_cursor_id = '00000000-0000-0000-0000-000000000000'::uuid THEN
                        SELECT m.id, m.created_at
                        INTO v_last_msg_id, v_last_msg_ts
                        FROM queen.messages m
                        WHERE m.partition_id = v_partition_id
                          AND m.created_at <= v_cursor_ts
                        ORDER BY m.created_at DESC, m.id DESC
                        LIMIT 1;

                        IF v_last_msg_id IS NOT NULL THEN
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
                        UPDATE queen.partition_consumers
                        SET lease_expires_at = NULL, lease_acquired_at = NULL, worker_id = NULL
                        WHERE partition_id = v_partition_id
                          AND consumer_group = v_req.consumer_group
                          AND worker_id = v_req.worker_id;
                    END IF;
                ELSIF v_msg_count > 0 AND v_msg_count <> v_req.batch_size THEN
                    -- Per-partition batch_size MUST match the actual
                    -- contribution from this partition. The ACK
                    -- auto-release-on-full-batch logic in ack_messages_v2
                    -- (acked_count >= batch_size) depends on this being
                    -- the per-partition count, not the request's global
                    -- budget — otherwise the lease would never auto-release.
                    UPDATE queen.partition_consumers
                    SET batch_size = v_msg_count, acked_count = 0
                    WHERE partition_id = v_partition_id
                      AND consumer_group = v_req.consumer_group
                      AND worker_id = v_req.worker_id;
                END IF;

                IF v_first_partition_id IS NULL THEN
                    v_first_partition_id   := v_partition_id;
                    v_first_partition_name := v_partition_name;
                END IF;

                IF v_msg_count > 0 THEN
                    v_all_messages := v_all_messages || v_messages;
                    v_remaining := v_remaining - v_msg_count;
                END IF;
                v_claimed_count := v_claimed_count + 1;
            END LOOP;

            -- Empty-scan watermark write fires only if zero partitions were
            -- claimed (otherwise we made progress and shouldn't mark the
            -- queue as idle).
            IF v_claimed_count = 0 THEN
                IF v_watermark_verified_at IS NULL
                   OR v_watermark_verified_at <= v_now - interval '30 seconds' THEN
                    SELECT EXISTS (
                        SELECT 1
                        FROM queen.partition_lookup pl
                        JOIN queen.partitions p ON p.id = pl.partition_id
                        JOIN queen.queues     q ON q.id = p.queue_id
                        LEFT JOIN queen.partition_consumers pc
                            ON pc.partition_id   = pl.partition_id
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

                    IF NOT v_has_pending_data THEN
                        INSERT INTO queen.consumer_watermarks (queue_name, consumer_group, last_empty_scan_at, updated_at)
                        VALUES (v_req.queue_name, v_req.consumer_group, v_now, v_now)
                        ON CONFLICT (queue_name, consumer_group)
                        DO UPDATE SET last_empty_scan_at = v_now, updated_at = v_now;
                    ELSE
                        UPDATE queen.consumer_watermarks
                        SET updated_at = v_now
                        WHERE queue_name = v_req.queue_name AND consumer_group = v_req.consumer_group;
                    END IF;
                END IF;

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

            -- Multi-partition success result. Top-level partitionId/partition
            -- reflect the FIRST claimed partition (back-compat); per-message
            -- fields are authoritative when partitionsClaimed > 1.
            v_result := jsonb_build_object(
                'idx', v_req.idx,
                'result', jsonb_build_object(
                    'success', true,
                    'queue', v_req.queue_name,
                    'partition', v_first_partition_name,
                    'partitionId', v_first_partition_id::text,
                    'leaseId', CASE WHEN v_auto_ack THEN '' ELSE v_req.worker_id END,
                    'consumerGroup', v_req.consumer_group,
                    'messages', v_all_messages,
                    'partitionsClaimed', v_claimed_count
                )
            );
            v_results := v_results || v_result;
        ELSE
            -- Specific-partition branch: max_partitions is implicitly 1 here.
            -- Logic mirrors v3 with per-message field bake-in added.
            v_partition_name := v_req.partition_name;
            v_partition_id := NULL;
            v_cursor_id := NULL;
            v_cursor_ts := NULL;
            v_messages := '[]'::jsonb;

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

            INSERT INTO queen.partition_consumers (partition_id, consumer_group)
            VALUES (v_partition_id, v_req.consumer_group)
            ON CONFLICT (partition_id, consumer_group) DO NOTHING;

            v_effective_lease := COALESCE(NULLIF(v_req.lease_seconds, 0), NULLIF(v_lease_time, 0), 60);

            UPDATE queen.partition_consumers pc
            SET
                lease_acquired_at  = v_now,
                lease_expires_at   = v_now + (v_effective_lease * INTERVAL '1 second'),
                worker_id          = v_req.worker_id,
                batch_size         = v_req.batch_size,
                acked_count        = 0,
                batch_retry_count  = COALESCE(pc.batch_retry_count, 0)
            WHERE pc.partition_id   = v_partition_id
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
                    v_cursor_ts := v_sub_ts;
                    v_cursor_id := '00000000-0000-0000-0000-000000000000'::uuid;
                ELSIF v_req.sub_from != '' AND v_req.sub_from != 'now' THEN
                    BEGIN
                        v_cursor_ts := v_req.sub_from::timestamptz;
                        v_cursor_id := '00000000-0000-0000-0000-000000000000'::uuid;

                        INSERT INTO queen.consumer_groups_metadata (
                            consumer_group, queue_name, partition_name, subscription_mode, subscription_timestamp
                        ) VALUES (
                            v_req.consumer_group, v_req.queue_name,
                            COALESCE(v_partition_name, ''),
                            'timestamp', v_cursor_ts
                        )
                        ON CONFLICT (consumer_group, queue_name, partition_name, namespace, task) DO NOTHING;

                        GET DIAGNOSTICS v_seeded = ROW_COUNT;
                        IF v_seeded > 0 THEN
                            INSERT INTO queen.partition_consumers (partition_id, consumer_group, last_consumed_at)
                            SELECT pl.partition_id, v_req.consumer_group, v_now
                            FROM queen.partition_lookup pl
                            WHERE pl.queue_name = v_req.queue_name
                            ON CONFLICT (partition_id, consumer_group) DO NOTHING;
                        END IF;
                    EXCEPTION WHEN OTHERS THEN
                        NULL;
                    END;
                ELSIF v_req.sub_from = 'now' OR v_req.sub_mode = 'new' THEN
                    v_cursor_ts := v_now;
                    v_cursor_id := '00000000-0000-0000-0000-000000000000'::uuid;

                    INSERT INTO queen.consumer_groups_metadata (
                        consumer_group, queue_name, partition_name, subscription_mode, subscription_timestamp
                    ) VALUES (
                        v_req.consumer_group, v_req.queue_name,
                        COALESCE(v_partition_name, ''),
                        'new', v_now
                    )
                    ON CONFLICT (consumer_group, queue_name, partition_name, namespace, task) DO NOTHING;

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

            SELECT COALESCE(jsonb_agg(
                jsonb_build_object(
                    'id', sub.id::text,
                    'transactionId', sub.transaction_id,
                    'traceId', sub.trace_id::text,
                    'data', sub.payload,
                    'producerSub', sub.producer_sub,
                    'createdAt', to_char(sub.created_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                    'partitionId', v_partition_id::text,
                    'partition', v_partition_name,
                    'leaseId', CASE WHEN v_auto_ack THEN '' ELSE v_req.worker_id END,
                    'consumerGroup', v_req.consumer_group
                ) ORDER BY sub.created_at, sub.id
            ), '[]'::jsonb)
            INTO v_messages
            FROM (
                SELECT m.id, m.transaction_id, m.trace_id, m.payload, m.producer_sub, m.created_at
                FROM queen.messages m
                WHERE m.partition_id = v_partition_id
                  AND (v_cursor_ts IS NULL OR (m.created_at, m.id) > (v_cursor_ts, COALESCE(v_cursor_id, '00000000-0000-0000-0000-000000000000'::uuid)))
                  AND (v_delayed_processing IS NULL OR v_delayed_processing = 0
                       OR m.created_at <= v_now - (v_delayed_processing || ' seconds')::interval)
                ORDER BY m.created_at, m.id
                LIMIT v_req.batch_size
            ) sub;

            v_msg_count := jsonb_array_length(v_messages);

            IF v_auto_ack AND v_msg_count > 0 THEN
                v_last_msg := v_messages->(v_msg_count - 1);
                v_last_msg_id := (v_last_msg->>'id')::UUID;
                SELECT created_at INTO v_last_msg_ts FROM queen.messages WHERE id = v_last_msg_id;

                UPDATE queen.partition_consumers
                SET
                    last_consumed_id          = v_last_msg_id,
                    last_consumed_created_at  = v_last_msg_ts,
                    last_consumed_at          = v_now,
                    lease_expires_at          = NULL,
                    lease_acquired_at         = NULL,
                    worker_id                 = NULL,
                    batch_size                = 0,
                    acked_count               = 0,
                    total_messages_consumed   = COALESCE(total_messages_consumed, 0) + v_msg_count,
                    total_batches_consumed    = COALESCE(total_batches_consumed, 0) + 1
                WHERE partition_id   = v_partition_id
                  AND consumer_group = v_req.consumer_group
                  AND worker_id      = v_req.worker_id;

                INSERT INTO queen.messages_consumed (partition_id, consumer_group, messages_completed, acked_at)
                VALUES (v_partition_id, v_req.consumer_group, v_msg_count, v_now);
            ELSIF v_msg_count = 0 THEN
                IF v_cursor_ts IS NOT NULL AND v_cursor_id = '00000000-0000-0000-0000-000000000000'::uuid THEN
                    SELECT m.id, m.created_at
                    INTO v_last_msg_id, v_last_msg_ts
                    FROM queen.messages m
                    WHERE m.partition_id = v_partition_id
                      AND m.created_at <= v_cursor_ts
                    ORDER BY m.created_at DESC, m.id DESC
                    LIMIT 1;

                    IF v_last_msg_id IS NOT NULL THEN
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

            v_result := jsonb_build_object(
                'idx', v_req.idx,
                'result', jsonb_build_object(
                    'success', true,
                    'queue', v_req.queue_name,
                    'partition', v_partition_name,
                    'partitionId', v_partition_id::text,
                    'leaseId', CASE WHEN v_auto_ack THEN '' ELSE v_req.worker_id END,
                    'consumerGroup', v_req.consumer_group,
                    'messages', v_messages,
                    'partitionsClaimed', 1
                )
            );
            v_results := v_results || v_result;
        END IF;
    END LOOP;

    RETURN v_results;
END;
$$;

GRANT EXECUTE ON FUNCTION queen.pop_unified_batch_v4(JSONB) TO PUBLIC;
