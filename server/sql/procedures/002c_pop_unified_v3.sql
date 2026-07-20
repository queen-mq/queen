-- ============================================================================
-- pop_unified_batch_v3: the production wildcard-pop procedure
-- ============================================================================
-- This is the procedure libqueen dispatches for JobType::POP (see
-- lib/queen/pending_job.hpp). It is a descendant of pop_unified_batch_v2
-- with two deliberate simplifications that were validated at scale:
--
--   (a) NO ORDER BY in the wildcard candidate scan.
--       ORDER BY pc.last_consumed_at ASC NULLS LAST sorts the full
--       eligible set on every pop — catastrophic at 15k-30k partitions
--       (full-table sort per pop call).
--   (b) NO LIMIT on the candidate scan.
--       An earlier variant used LIMIT 16 / LIMIT 64. At 1001+ partitions
--       this caused severe starvation: concurrent pops only saw the first
--       N partitions by physical index order and competed for that same
--       small set. Without LIMIT the candidate scan rides the
--       idx_partition_lookup_queue index; the advisory-lock probe exits
--       the loop as soon as a partition is claimed, so total work per
--       pop is O(number of concurrent claim misses) not O(partitions).
--
-- Coordination is by pg_try_advisory_xact_lock(partition_id) — a
-- non-blocking probe that releases on xact end. Push never takes this
-- lock (partition_lookup writes are now async via PUSHPOPLOOKUPSOL;
-- see cdocs/PUSHPOPLOOKUPSOL.md), so push-vs-pop row-lock contention
-- on partition_lookup is eliminated by design.
--
-- History: this is the procedure previously named
-- pop_unified_batch_v2_noorder. The rename to _v3 aligns it with the
-- push_messages_v3 naming scheme; the body is byte-for-byte identical.
-- The schema drops the old name on re-apply (see schema.sql).
--
-- Identical to pop_unified_batch_v2 in every other respect:
--   - Watermark prelude + EXISTS maintenance.
--   - Common path unchanged.
-- ============================================================================

CREATE OR REPLACE FUNCTION queen.pop_unified_batch_v3(p_requests JSONB)
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
    v_claimed BOOLEAN;
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
            COALESCE((r->>'auto_ack')::BOOLEAN, false) AS auto_ack
        FROM jsonb_array_elements(p_requests) r
        ORDER BY
            CASE WHEN NULLIF(NULLIF(r->>'partition_name', ''), 'null') IS NULL THEN 1 ELSE 0 END,
            r->>'queue_name',
            r->>'partition_name',
            (r->>'idx')::INT
    LOOP
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

        IF v_is_wildcard THEN
            SELECT last_empty_scan_at, updated_at
            INTO v_watermark, v_watermark_verified_at
            FROM queen.consumer_watermarks
            WHERE queue_name = v_req.queue_name
              AND consumer_group = v_req.consumer_group;

            IF v_watermark IS NULL THEN
                v_watermark := '1970-01-01 00:00:00+00'::TIMESTAMPTZ;
                v_watermark_verified_at := NULL;
            END IF;

            v_claimed := FALSE;
            -- Candidate loop: scan eligible partitions and claim the first
            -- one whose advisory lock we can acquire non-blockingly. See the
            -- file header for the design rationale on the absence of
            -- ORDER BY and LIMIT. Short version:
            --   * ORDER BY (removed): full-set sort per call, unacceptable
            --     at 15k-30k partitions.
            --   * LIMIT 64 (removed): caused partition starvation — only
            --     the first 64 partitions by index order were ever visible
            --     to concurrent pops (measured at 1001 partitions: pop
            --     coverage 64/1001 with LIMIT, 1001/1001 without).
            -- Fair distribution across concurrent consumers comes from
            -- pg_try_advisory_xact_lock; distribution for a single consumer
            -- follows the physical scan order of the UNIQUE(queue_name,
            -- partition_id) index, which is stable but arbitrary — the
            -- advisory-lock EXIT keeps per-call work bounded in practice.
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
                -- NO LIMIT: advisory-loop EXIT is the implicit bound.
            LOOP
                IF pg_try_advisory_xact_lock(
                        hashtextextended(v_cand.partition_id::text, c_lock_ns)
                   ) THEN
                    v_partition_id            := v_cand.partition_id;
                    v_partition_name          := v_cand.partition_name;
                    v_queue_id                := v_cand.queue_id;
                    v_lease_time              := v_cand.lease_time;
                    v_window_buffer           := v_cand.window_buffer;
                    v_delayed_processing      := v_cand.delayed_processing;
                    v_partition_last_msg_ts   := v_cand.last_message_ts;
                    v_claimed                 := TRUE;
                    EXIT;
                END IF;
            END LOOP;

            IF NOT v_claimed THEN
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
        ELSE
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
                    'error', CASE WHEN v_is_wildcard THEN 'lease_race_condition' ELSE 'lease_held' END,
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
                        CASE WHEN v_is_wildcard THEN '' ELSE COALESCE(v_partition_name, '') END,
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
                    CASE WHEN v_is_wildcard THEN '' ELSE COALESCE(v_partition_name, '') END,
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
                'createdAt', to_char(sub.created_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
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
                'messages', v_messages
            )
        );
        v_results := v_results || v_result;
    END LOOP;

    RETURN v_results;
END;
$$;

GRANT EXECUTE ON FUNCTION queen.pop_unified_batch_v3(JSONB) TO PUBLIC;
