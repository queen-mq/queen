-- ============================================================================
-- seg_streams_cycle_v1: atomic streaming cycle commit on the SEGMENTS engine.
-- ============================================================================
--
-- Segments sibling of queen.streams_cycle_v1 (021_streams_cycle_v1.sql, the
-- rows engine). Same contract, same response envelope, but every data touch
-- is retargeted from the rows store (queen.messages / push_messages_v3 /
-- partition_consumers) to the segments store:
--
--   * source cursor      -> queen.seg_consumers (next_seq/next_off + lease),
--                            NOT queen.partition_consumers.
--   * sink push          -> queen.seg_push_segment_v1 INLINE (same txn), on
--                            broker-prepacked segments (metas + base64 zstd
--                            blob), NOT push_messages_v3.
--   * source ack         -> cursor advance over the leased segment batch, NOT
--                            a txn->row resolve. Full batch advances to the
--                            recorded batch_end; a gate partial-ack walks K
--                            frames forward and RETAINS the lease on the tail.
--
-- One streaming cycle (fat JS-client Runner) runs as:
--   1. Pop a segment batch for a partition (non-autoAck -> lease on
--      queen.seg_consumers, batch_end_* recorded). The leaseId is the pop
--      worker id, threaded back into the response by the Rust pop handler.
--   2. (optional) read state for the partition (streams_state_get_v1).
--   3. user operators run client-side -> state_ops, sink frames, ack.
--   4. commit all three atomically via this SP.
--
-- Input: JSONB ARRAY of cycle requests (the runtime merges same-JobType jobs
-- into one call). Each element:
--
--   { "idx": 0,
--     "query_id":     "<uuid>",
--     "partition_id": "<seg_partitions.id uuid>",   -- source partition (seg)
--     "consumer_group": "streams.<queryId>",         -- default __QUEUE_MODE__
--     "worker":       "<leaseId from the source pop>",
--     "release_lease": true | false,                 -- default true
--     "state_ops": [
--        {"type":"upsert","key":"...","value":{...}},
--        {"type":"delete","key":"..."}
--     ],
--     "sink_segments": [                             -- broker-prepacked, opt.
--        {"queue":"...","partition":"...",
--         "metas":[{"i":0,"mid":"<uuid>","txn":"<txn>"}],
--         "blobB64":"<base64 zstd frames>","count":N}
--     ],
--     "ack": { "ok": true, "count": K } | null       -- null on idle-flush
--   }
--
-- release_lease semantics (mirrors 021's rows-engine gate backpressure)
-- --------------------------------------------------------------------
--   ack.ok = true, release_lease = true  (default, full batch):
--       advance next_seq/next_off to the NORMALIZED batch_end (batch_end_seq/
--       batch_end_off), clear the lease (worker_id/lease_expires_at/
--       batch_end_*), total_consumed += count. Identical to
--       seg_ack_segments_v1 called with upto = batch_end.
--   ack.ok = true, release_lease = false (gate partial ack of K):
--       advance the cursor by EXACTLY K frames forward from the batch start
--       (next_seq/next_off) across seg_segments.msg_count boundaries, but
--       RETAIN worker_id/lease_expires_at/batch_end_*. The un-acked tail stays
--       leased; on lease expiry a re-pop resumes at the advanced cursor and
--       redelivers only the tail, in order. This is the one behavior plain
--       seg_ack_segments_v1 lacks (it always releases) — without it a rate
--       limiter built on .gate() would busy-spin re-popping the denied tail.
--   ack.ok = false (nack / failed batch):
--       release the lease, cursor UNTOUCHED (full redelivery).
--   ack = null (idle-flush cycle):
--       skip the lease block entirely (state ops + sink push only).
--
-- Exactly-once holds because the sink push precedes the ack and the ack RAISEs
-- (aborts the per-element savepoint) on an invalid/expired lease: a lost-
-- response retry re-packs a NEW sink segment, then the ack finds the lease gone
-- -> the savepoint rolls back the inline sink push -> no double consume, no
-- double sink write.
--
-- Output: JSONB ARRAY mirroring input idx, envelope shape IDENTICAL to
-- queen.streams_cycle_v1 (021): [{ "idx": N, "result": { ... } }] with the
-- inner result carrying success / query_id / partition_id / queueName /
-- state_ops_applied / push_results / ack_result (+ error only on failure).
-- ============================================================================

CREATE OR REPLACE FUNCTION queen.seg_streams_cycle_v1(p_requests JSONB)
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
    v_worker              TEXT;
    v_state_ops           JSONB;
    v_sink_segments       JSONB;
    v_ack                 JSONB;
    v_release_lease       BOOLEAN;

    v_state_ops_applied   INT;
    v_op                  JSONB;

    v_sink                JSONB;
    v_push_res            JSONB;
    v_push_results        JSONB;
    v_sink_pid            UUID;

    v_ack_ok              BOOLEAN;
    v_ack_count           INT;
    v_ack_result          JSONB;
    v_ack_lease_released  BOOLEAN;

    v_c                   RECORD;
    v_end_count           INTEGER;
    v_new_seq             BIGINT;
    v_new_off             INTEGER;

    -- frame-walk locals (gate partial ack)
    v_seg                 RECORD;
    v_seg_start           INTEGER;
    v_avail               INTEGER;
    v_remaining           INTEGER;
    v_pos_seq             BIGINT;
    v_pos_off             INTEGER;

    v_source_queue_name   TEXT;
    v_one_result          JSONB;
BEGIN
    IF p_requests IS NULL OR jsonb_array_length(p_requests) = 0 THEN
        RETURN '[]'::jsonb;
    END IF;

    FOR v_req IN SELECT * FROM jsonb_array_elements(p_requests)
    LOOP
        v_idx                := COALESCE((v_req->>'idx')::int, (v_req->>'index')::int, 0);
        v_query_id           := NULL;
        v_partition_id       := NULL;
        v_state_ops_applied  := 0;
        v_push_results       := '[]'::jsonb;
        v_ack_result         := 'null'::jsonb;
        v_ack_lease_released := false;
        v_source_queue_name  := NULL;
        v_release_lease      := COALESCE((v_req->>'release_lease')::BOOLEAN, TRUE);

        BEGIN
            v_query_id       := (v_req->>'query_id')::uuid;
            v_partition_id   := (v_req->>'partition_id')::uuid;
            v_consumer_group := COALESCE(NULLIF(v_req->>'consumer_group', ''), '__QUEUE_MODE__');
            v_worker         := v_req->>'worker';
            v_state_ops      := COALESCE(v_req->'state_ops',     '[]'::jsonb);
            v_sink_segments  := COALESCE(v_req->'sink_segments', '[]'::jsonb);
            v_ack            := v_req->'ack';

            IF v_query_id IS NULL THEN
                RAISE EXCEPTION 'query_id is required';
            END IF;
            IF v_partition_id IS NULL THEN
                RAISE EXCEPTION 'partition_id is required';
            END IF;

            -- Resolve the source queue name for the runtime's per-queue ack
            -- metric (record_ack_with_queue). The source partition is a
            -- seg_partitions.id, so resolve via the segments tables.
            SELECT q2.name INTO v_source_queue_name
            FROM queen.seg_partitions sp
            JOIN queen.seg_queues q2 ON q2.id = sp.queue_id
            WHERE sp.id = v_partition_id;

            -- Serialise concurrent cycles + idle-flush sweeps on the same
            -- (query_id, partition_id) state shard. Pre-ack-branch lock so both
            -- ack-bearing cycles and ack-less flush cycles share the key.
            -- Identical key derivation to 021.
            PERFORM pg_advisory_xact_lock(
                hashtextextended(v_query_id::text || ':' || v_partition_id::text, 0)
            );

            -- 1. Apply state ops (verbatim from 021) ------------------------
            -- All ops are scoped to (query_id, partition_id), the lease shard
            -- the calling worker owns; only one worker holds a partition's
            -- lease, so no cross-worker contention.
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

            -- 2. Sink push (inline segment insert, same txn) ----------------
            -- Broker-prepacked segments: metas + base64 zstd blob. Push each
            -- via seg_push_segment_v1 INLINE so exactly-once holds (the sink
            -- write shares the savepoint with the ack). Deadlock-safety
            -- pre-pass: lazily create queue/partition rows, then lock the
            -- target seg_partitions rows in ASCENDING id order before the
            -- first push, mirroring seg_transaction_wire_v1 (026).
            IF jsonb_array_length(v_sink_segments) > 0 THEN
                INSERT INTO queen.seg_queues (name)
                SELECT DISTINCT s->>'queue'
                FROM jsonb_array_elements(v_sink_segments) s
                WHERE COALESCE(s->>'queue', '') <> ''
                ON CONFLICT (name) DO NOTHING;

                INSERT INTO queen.seg_partitions (queue_id, name)
                SELECT DISTINCT q2.id, s->>'partition'
                FROM jsonb_array_elements(v_sink_segments) s
                JOIN queen.seg_queues q2 ON q2.name = s->>'queue'
                ON CONFLICT (queue_id, name) DO NOTHING;

                FOR v_sink_pid IN
                    SELECT DISTINCT p2.id
                    FROM jsonb_array_elements(v_sink_segments) s
                    JOIN queen.seg_queues q2 ON q2.name = s->>'queue'
                    JOIN queen.seg_partitions p2
                      ON p2.queue_id = q2.id AND p2.name = s->>'partition'
                    ORDER BY p2.id
                LOOP
                    PERFORM 1 FROM queen.seg_partitions WHERE id = v_sink_pid FOR UPDATE;
                END LOOP;

                FOR v_sink IN SELECT * FROM jsonb_array_elements(v_sink_segments)
                LOOP
                    v_push_res := queen.seg_push_segment_v1(
                        v_sink->>'queue',
                        v_sink->>'partition',
                        v_sink->'metas',
                        decode(v_sink->>'blobB64', 'base64'),
                        (v_sink->>'count')::integer);
                    v_push_results := v_push_results || jsonb_build_array(
                        jsonb_build_object(
                            'queue',     v_sink->>'queue',
                            'partition', v_sink->>'partition'
                        ) || v_push_res);
                END LOOP;
            END IF;

            -- 3. Source ack = cursor advance over the leased batch ----------
            IF v_ack IS NOT NULL AND v_ack <> 'null'::jsonb THEN
                v_ack_ok    := COALESCE((v_ack->>'ok')::boolean, true);
                v_ack_count := COALESCE((v_ack->>'count')::int, 0);

                -- Load + lock the source cursor row. The lease check is the
                -- exactly-once guard: a RAISE here rolls back the inline sink
                -- push above. (IS DISTINCT FROM for null-safety, as in
                -- seg_ack_segments_v1.)
                SELECT * INTO v_c FROM queen.seg_consumers c
                WHERE c.partition_id = v_partition_id
                  AND c.consumer_group = v_consumer_group
                FOR UPDATE;

                IF NOT FOUND
                   OR v_c.worker_id IS DISTINCT FROM v_worker
                   OR v_c.lease_expires_at IS NULL
                   OR v_c.lease_expires_at < clock_timestamp() THEN
                    RAISE EXCEPTION 'invalid or expired lease';
                END IF;

                IF NOT v_ack_ok THEN
                    -- nack: release the lease, cursor untouched (redelivery).
                    UPDATE queen.seg_consumers SET
                        worker_id = NULL, lease_expires_at = NULL,
                        batch_end_seq = NULL, batch_end_off = NULL
                    WHERE partition_id = v_partition_id
                      AND consumer_group = v_consumer_group;
                    v_ack_lease_released := true;
                    v_ack_count := 0;

                ELSIF v_release_lease THEN
                    -- Full batch: advance to the normalized batch_end, release
                    -- the lease. Same normalization as seg_ack_segments_v1.
                    IF v_c.batch_end_seq IS NULL OR v_c.batch_end_off IS NULL THEN
                        RAISE EXCEPTION 'no leased batch';
                    END IF;
                    SELECT msg_count INTO v_end_count FROM queen.seg_segments
                    WHERE partition_id = v_partition_id AND seq = v_c.batch_end_seq;

                    v_new_seq := CASE WHEN v_end_count IS NOT NULL AND v_c.batch_end_off >= v_end_count
                                      THEN v_c.batch_end_seq + 1 ELSE v_c.batch_end_seq END;
                    v_new_off := CASE WHEN v_end_count IS NOT NULL AND v_c.batch_end_off >= v_end_count
                                      THEN 0 ELSE v_c.batch_end_off END;

                    UPDATE queen.seg_consumers SET
                        next_seq = v_new_seq,
                        next_off = v_new_off,
                        worker_id = NULL, lease_expires_at = NULL,
                        batch_end_seq = NULL, batch_end_off = NULL,
                        attempt_seq = NULL, attempt_off = NULL, attempt_count = 0,
                        total_consumed = total_consumed + GREATEST(v_ack_count, 0)
                    WHERE partition_id = v_partition_id
                      AND consumer_group = v_consumer_group;
                    v_ack_lease_released := true;

                ELSE
                    -- Gate partial ack of K: walk K frames forward from the
                    -- batch start (next_seq/next_off) across msg_count
                    -- boundaries, then advance the cursor there but RETAIN the
                    -- lease + batch_end_* so the un-acked tail redelivers in
                    -- order on lease expiry. Iterating actual segments makes
                    -- the walk gap-tolerant (retention deletions skipped).
                    v_pos_seq   := v_c.next_seq;
                    v_pos_off   := v_c.next_off;
                    v_remaining := GREATEST(v_ack_count, 0);

                    IF v_remaining > 0 THEN
                        FOR v_seg IN
                            SELECT s.seq, s.msg_count FROM queen.seg_segments s
                            WHERE s.partition_id = v_partition_id AND s.seq >= v_c.next_seq
                            ORDER BY s.seq
                        LOOP
                            v_seg_start := CASE WHEN v_seg.seq = v_c.next_seq THEN v_c.next_off ELSE 0 END;
                            v_avail := v_seg.msg_count - v_seg_start;
                            IF v_avail <= 0 THEN
                                CONTINUE;  -- head segment already consumed (normalization)
                            END IF;

                            IF v_remaining <= v_avail THEN
                                -- lands inside this segment; normalize a full
                                -- consume of the segment to (seq+1, 0).
                                IF v_seg_start + v_remaining >= v_seg.msg_count THEN
                                    v_pos_seq := v_seg.seq + 1; v_pos_off := 0;
                                ELSE
                                    v_pos_seq := v_seg.seq; v_pos_off := v_seg_start + v_remaining;
                                END IF;
                                v_remaining := 0;
                                EXIT;
                            ELSE
                                v_remaining := v_remaining - v_avail;
                                v_pos_seq := v_seg.seq + 1; v_pos_off := 0;
                            END IF;
                        END LOOP;
                    END IF;

                    UPDATE queen.seg_consumers SET
                        next_seq = v_pos_seq,
                        next_off = v_pos_off,
                        total_consumed = total_consumed + GREATEST(v_ack_count, 0)
                        -- worker_id / lease_expires_at / batch_end_* RETAINED
                    WHERE partition_id = v_partition_id
                      AND consumer_group = v_consumer_group;
                    v_ack_lease_released := false;
                END IF;

                -- NB: unlike 021 (rows), we do NOT write queen.messages_consumed
                -- here. That table's ledger FKs queen.partitions (rows engine);
                -- the segments engine tracks consumption via
                -- seg_consumers.total_consumed (advanced above), exactly like
                -- seg_ack_segments_v1 / seg_ack_by_txn_v1 which never touch it.

                v_ack_result := jsonb_build_object(
                    'success',        true,
                    'count',          GREATEST(v_ack_count, 0),
                    'lease_released', v_ack_lease_released,
                    'dlq',            false
                );
            END IF;

            v_one_result := jsonb_build_object(
                'success',           true,
                'query_id',          v_query_id::text,
                'partition_id',      v_partition_id::text,
                'queueName',         v_source_queue_name,
                'state_ops_applied', v_state_ops_applied,
                'push_results',      v_push_results,
                'ack_result',        v_ack_result
            );
        EXCEPTION WHEN OTHERS THEN
            v_one_result := jsonb_build_object(
                'success',           false,
                'query_id',          COALESCE(v_query_id::text, ''),
                'partition_id',      COALESCE(v_partition_id::text, ''),
                'queueName',         v_source_queue_name,
                'state_ops_applied', v_state_ops_applied,
                'push_results',      v_push_results,
                'ack_result',        v_ack_result,
                'error',             SQLERRM
            );
        END;

        v_results := v_results || jsonb_build_object('idx', v_idx, 'result', v_one_result);
    END LOOP;

    RETURN v_results;
END;
$$;

GRANT EXECUTE ON FUNCTION queen.seg_streams_cycle_v1(JSONB) TO PUBLIC;
