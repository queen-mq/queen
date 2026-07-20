-- ============================================================================
-- pop_unified_batch_v2: advisory-lock partition claim (non-blocking to push)
-- ============================================================================
--
-- PROBLEM WE FIX
--
-- v1's wildcard discovery uses:
--
--   SELECT pl.partition_id, ...
--   FROM queen.partition_lookup pl
--   JOIN queen.partitions p ...
--   JOIN queen.queues q ...
--   LEFT JOIN queen.partition_consumers pc ...
--   WHERE pl.queue_name = ? AND ...
--   ORDER BY pc.last_consumed_at ASC NULLS LAST
--   LIMIT 1
--   FOR UPDATE OF pl SKIP LOCKED;
--
-- The `FOR UPDATE OF pl` takes a *row-level* lock on a `partition_lookup`
-- row and holds it for the entire pop transaction (lease acquire, subscription
-- metadata, message fetch, cleanup, maybe auto-ack). That same row is also
-- the target of the statement-level trigger `trg_update_partition_lookup`,
-- which push runs after every successful INSERT batch with:
--
--   INSERT INTO queen.partition_lookup (...) ... ON CONFLICT ... DO UPDATE
--
-- Push's UPDATE has no SKIP LOCKED and simply blocks on the row lock pop
-- is holding. Under concurrent load this surfaces in pg_stat_activity as
-- `Lock: tuple` / `Lock: transactionid` waits attributed to push_messages_v3.
-- At high concurrency the wait times cascade and throughput collapses.
--
-- HOW v2 FIXES IT
--
-- v2 replaces the SELECT-with-FOR-UPDATE-SKIP-LOCKED with a candidate loop:
--
--   1. SELECT up to N candidate rows from `partition_lookup` (NO row lock).
--   2. For each candidate, call `pg_try_advisory_xact_lock(KEY(partition_id))`.
--      - If it acquires: this pop "owns" the partition for the rest of xact.
--      - If not: another pop owns it; try the next candidate.
--   3. If all candidates are claimed: same no-partition-found outcome as v1.
--
-- Advisory locks live entirely inside PostgreSQL's lock manager, keyed by
-- a `bigint`. They are invisible to row-level locking. Push's trigger
-- UPSERT on `partition_lookup` takes its ordinary row-exclusive lock,
-- sees no conflicting holder, and proceeds. Pop is still mutually
-- exclusive with other pops (they all race for the same advisory key).
--
-- Correctness properties preserved:
--   - Mutual exclusion across concurrent wildcard consumers (same contract
--     as SKIP LOCKED).
--   - No-partition-found branch still triggers watermark EXISTS verification
--     and advance, identical to v1.
--   - Lease acquisition atomicity is still enforced by the conditional
--     UPDATE on partition_consumers (unchanged).
--
-- Correctness properties relaxed vs v1:
--   - v1 took a row lock on `partition_lookup`; while held, no other session
--     could UPSERT that row. v2 allows push's trigger to advance
--     `last_message_created_at` while pop is mid-transaction. This is a
--     *cache-freshness* concern, not correctness: pop still queries
--     `queen.messages` directly for the actual payload, and its cursor
--     advances based on the returned rows, not on the lookup row contents.
--     The lookup row is only used for the `window_buffer` debounce check
--     (a coarse-grained "is the partition quiet enough to consume?"
--     heuristic). A stale read here can at worst cause one extra or one
--     missed batch, both self-correcting on the next call.
--
-- The function signature, parameters, and JSONB response shape are IDENTICAL
-- to `pop_unified_batch`, so the C++ layer can switch procedures by name
-- without any other code change.
-- ============================================================================

CREATE OR REPLACE FUNCTION queen.pop_unified_batch_v2(p_requests JSONB)
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
    -- v2 additions: candidate loop state for advisory-lock partition claim
    v_cand RECORD;
    v_claimed BOOLEAN;
    -- Namespace for the advisory lock key. Any stable bigint works; the
    -- value just needs to be distinct from any other `pg_advisory_*` caller
    -- in this database so we never collide with another feature's key
    -- space. 0xC0FFEE is arbitrary but memorable.
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

        -- =====================================================================
        -- BRANCH: Wildcard Discovery (v2: advisory-lock claim, no row lock)
        -- =====================================================================
        IF v_is_wildcard THEN
            -- Watermark prelude (unchanged from v1)
            SELECT last_empty_scan_at, updated_at
            INTO v_watermark, v_watermark_verified_at
            FROM queen.consumer_watermarks
            WHERE queue_name = v_req.queue_name
              AND consumer_group = v_req.consumer_group;

            IF v_watermark IS NULL THEN
                v_watermark := '1970-01-01 00:00:00+00'::TIMESTAMPTZ;
                v_watermark_verified_at := NULL;
            END IF;

            -- Candidate loop: fetch up to 16 eligible partitions WITHOUT any
            -- row lock, then try to claim one via advisory lock. 16 is a
            -- safety pool in case the first few candidates are already
            -- claimed by other in-flight pops; empirically > number of
            -- expected contemporaneous pop workers in most deployments.
            --
            -- ORDER BY pc.last_consumed_at ASC NULLS LAST: fair partition
            -- distribution. Matches the pattern from pop_discover_batch.
            -- NOTE: v1's pop_unified_batch omits this ORDER BY, which
            -- causes severe starvation at low consumer counts -- all
            -- consumers cluster on the low-partition-id side of the index
            -- and msgs/pop stays in the single digits regardless of how
            -- long partitions have been accumulating. With the ORDER BY
            -- consumers always pick the least-recently-consumed partition,
            -- which is also the one with the most accumulated messages.
            -- v1 should adopt this change too; v2 is the place where it
            -- lives until that happens.
            v_claimed := FALSE;
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
                ORDER BY pc.last_consumed_at ASC NULLS LAST
                LIMIT 16
            LOOP
                -- pg_try_advisory_xact_lock(bigint): non-blocking.
                -- Returns true on first success, false if another xact holds it.
                -- Automatically released at end of this transaction.
                -- We combine namespace + partition hash into ONE bigint using
                -- hashtextextended()'s seed argument (this is exactly what the
                -- seed is for: two different features using the same hash
                -- input get different keys by choosing different seeds). The
                -- two-argument overload `pg_try_advisory_xact_lock(int, int)`
                -- takes int4 args, not bigint — we want the wider keyspace,
                -- so we stay with the single-arg bigint overload.
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

            -- Watermark maintenance (unchanged behavior from v1): if no
            -- candidate was claimed, maybe re-verify with EXISTS and
            -- advance the watermark.
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
            -- =================================================================
            -- SPECIFIC: unchanged from v1 (no partition_lookup row lock)
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
        -- COMMON PATH (unchanged from v1)
        -- =====================================================================
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

GRANT EXECUTE ON FUNCTION queen.pop_unified_batch_v2(JSONB) TO PUBLIC;
