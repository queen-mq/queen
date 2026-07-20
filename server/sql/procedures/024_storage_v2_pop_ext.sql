-- ============================================================================
-- Storage v2 (segments engine) — pop extensions.
-- Builds on lib/schema/procedures/023_storage_v2.sql (schema q2): wildcard
-- pop across a queue's partitions, cross-process ack-by-transaction fallback,
-- and a cheap pending probe. Applied idempotently at boot after 023.
-- ============================================================================

-- ============================================================================
-- Wildcard pop: claim frames from several partitions of a queue in one call.
-- Candidate filter is COARSE on purpose (no consumer row yet, OR
-- last_seq >= next_seq): queen.seg_pop_segments_v1 re-verifies the cursor and the
-- lease under the row lock, so a stale candidate just yields zero rows.
-- Deterministic partition-name order keeps concurrent wildcard poppers from
-- interleaving lock acquisition (and SKIP LOCKED inside pop_segments_v1 keeps
-- them non-blocking anyway). All claimed partitions share p_worker, mirroring
-- v1 pop_unified_batch_v4's one-lease-id-per-batch semantics.
-- Returns {"partitions":[{"partition":name,"partitionId":uuid,
--                         "segments":[{seq,startOff,take,msgCount,createdAt,
--                                      blob(b64)}, ...]}, ...]}.
-- p_sub_mode / p_sub_from are forwarded to queen.seg_pop_segments_v1 (subscription
-- seeding on first contact per partition — see 025); they default to the
-- no-seeding values so callers of the historical 7-arg form keep working.
-- The 7-arg overload is dropped (an overload pair would make 7-arg calls
-- ambiguous).
-- ============================================================================
DROP FUNCTION IF EXISTS queen.seg_pop_wildcard_wire_v1(TEXT, TEXT, INTEGER, INTEGER, TEXT, BOOLEAN, INTEGER);
DROP FUNCTION IF EXISTS queen.seg_pop_wildcard_wire_v1(TEXT, TEXT, INTEGER, INTEGER, TEXT, BOOLEAN, INTEGER, TEXT, TEXT);
CREATE FUNCTION queen.seg_pop_wildcard_wire_v1(
    p_queue TEXT,
    p_group TEXT,
    p_budget INTEGER,
    p_lease_seconds INTEGER,
    p_worker TEXT,
    p_auto_ack BOOLEAN,
    p_max_partitions INTEGER,
    p_sub_mode TEXT DEFAULT 'all',
    p_sub_from TEXT DEFAULT ''
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_qid UUID;
    v_p RECORD;
    v_remaining INTEGER := GREATEST(p_budget, 1);
    -- NULL / non-positive p_max_partitions means "no partition cap".
    v_max_parts INTEGER := CASE WHEN p_max_partitions IS NULL OR p_max_partitions <= 0
                                THEN 2147483647 ELSE p_max_partitions END;
    v_claimed INTEGER := 0;
    v_out JSONB := '[]'::jsonb;
    v_segments JSONB;
    v_taken INTEGER;
    v_now TIMESTAMPTZ := clock_timestamp();
    v_watermark TIMESTAMPTZ;
    v_cand_cap INTEGER;
    v_first_seen INTEGER;
    v_from_ts TIMESTAMPTZ;
BEGIN
    SELECT id INTO v_qid FROM queen.seg_queues WHERE name = p_queue;
    IF v_qid IS NULL THEN
        RETURN jsonb_build_object('partitions', '[]'::jsonb);
    END IF;

    -- Bulk subscription bootstrap (mirrors the rows engine's pop_unified_batch_v4:
    -- consumer_groups_metadata insert -> bulk partition_consumers seed). When a
    -- consumer group first registers with subscriptionMode='new' or a
    -- subscriptionFrom timestamp, seed the cursor of EVERY partition of the queue
    -- in one set-based pass, BEFORE any messages the group is meant to skip can be
    -- overtaken by newer writes. Without this, seg_pop_segments_v1 only seeds a
    -- partition on first contact: partitions first touched AFTER post-registration
    -- messages arrive would compute their 'new' seed (last_seq+1) past those new
    -- messages and never deliver them (the 5000-partition testCgBootstrapNew bug —
    -- only the ~cand_cap partitions scanned by the registering pop were seeded in
    -- time). The watermark row doubles as the once-per-(queue,group) marker: its
    -- first insertion (ROW_COUNT>0) means this group has never scanned this queue.
    -- Queue mode never seeds a backlog, so it is excluded.
    IF p_group <> '__QUEUE_MODE__'
       AND (COALESCE(p_sub_mode, 'all') = 'new' OR COALESCE(p_sub_from, '') <> '') THEN
        INSERT INTO queen.consumer_watermarks (queue_name, consumer_group)
        VALUES (p_queue, p_group)
        ON CONFLICT (queue_name, consumer_group) DO NOTHING;
        GET DIAGNOSTICS v_first_seen = ROW_COUNT;

        IF v_first_seen > 0 THEN
            IF p_sub_from <> '' AND p_sub_from <> 'now' THEN
                BEGIN
                    v_from_ts := p_sub_from::timestamptz;
                EXCEPTION WHEN OTHERS THEN
                    v_from_ts := NULL;  -- unparsable: fall back to full backlog seed
                END;
            END IF;

            IF v_from_ts IS NOT NULL THEN
                -- timestamp seed: first segment at/after v_from_ts per partition
                -- (created_at monotone in seq -> forward walk from retention_seq),
                -- else last_seq+1 (nothing that recent yet -> future-only cursor).
                -- Identical rule to seg_pop_segments_v1's lazy timestamp seed.
                INSERT INTO queen.partition_consumers (partition_id, consumer_group, next_seq)
                SELECT p.id, p_group,
                       COALESCE(
                           (SELECT s.seq FROM queen.seg_segments s
                            WHERE s.partition_id = p.id
                              AND s.seq >= p.retention_seq
                              AND s.created_at >= v_from_ts
                            ORDER BY s.seq LIMIT 1),
                           p.last_seq + 1)
                FROM queen.seg_partitions p
                WHERE p.queue_id = v_qid
                ON CONFLICT (partition_id, consumer_group) DO NOTHING;
            ELSE
                -- 'new' (or 'now') seed: skip the entire existing backlog.
                INSERT INTO queen.partition_consumers (partition_id, consumer_group, next_seq)
                SELECT p.id, p_group, p.last_seq + 1
                FROM queen.seg_partitions p
                WHERE p.queue_id = v_qid
                ON CONFLICT (partition_id, consumer_group) DO NOTHING;
            END IF;
        END IF;
    END IF;

    -- Empty-scan watermark: only consider partitions written since this group last
    -- found the queue empty. A caught-up consumer skips cold partitions instead of
    -- scanning the whole partition set every long-poll (decisive at 10-20k parts).
    SELECT last_empty_scan_at INTO v_watermark
    FROM queen.consumer_watermarks
    WHERE queue_name = p_queue AND consumer_group = p_group;
    IF v_watermark IS NULL THEN
        v_watermark := '1970-01-01 00:00:00+00'::timestamptz;
    END IF;

    -- Fetch more candidates than requested: some will be claimed by concurrent
    -- pops or turn out empty by the time we call pop_segments_v1.
    v_cand_cap := LEAST(GREATEST(v_max_parts * 4, 64), 512);

    -- Candidate set: hot (write watermark >= consumer cursor), lease-free, written
    -- since the empty-scan floor. NOT ordered by name (ORDER BY random) so 200
    -- consumers spread across partitions instead of convoying on the low names.
    FOR v_p IN
        SELECT p.id, p.name
        FROM queen.seg_partitions p
        LEFT JOIN queen.partition_consumers c
          ON c.partition_id = p.id AND c.consumer_group = p_group
        WHERE p.queue_id = v_qid
          AND p.last_write_at >= v_watermark - interval '2 minutes'
          AND (c.partition_id IS NULL OR p.last_seq >= c.next_seq)
          AND (c.worker_id IS NULL OR c.lease_expires_at IS NULL
               OR c.lease_expires_at < v_now)
        ORDER BY random()
        LIMIT v_cand_cap
    LOOP
        -- Same row shape / keys as queen.seg_pop_segments_wire_v1.
        SELECT COALESCE(jsonb_agg(jsonb_build_object(
                   'seq', r_seq,
                   'startOff', r_start_off,
                   'take', r_take,
                   'msgCount', r_msg_count,
                   'createdAt', to_char(r_created_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                   'blob', encode(r_blob, 'base64')
               )), '[]'::jsonb),
               COALESCE(SUM(r_take), 0)
        INTO v_segments, v_taken
        FROM queen.seg_pop_segments_v1(p_queue, v_p.name, p_group,
                                v_remaining, p_lease_seconds, p_worker, p_auto_ack,
                                p_sub_mode, p_sub_from);

        IF v_taken > 0 THEN
            v_out := v_out || jsonb_build_object(
                'partition', v_p.name,
                'partitionId', v_p.id,
                'segments', v_segments);
            v_claimed := v_claimed + 1;   -- only partitions that yielded rows
            v_remaining := v_remaining - v_taken;
            EXIT WHEN v_remaining <= 0 OR v_claimed >= v_max_parts;
        END IF;
    END LOOP;

    -- Nothing claimable this cycle: advance the empty-scan watermark so the next
    -- long-poll only re-checks partitions written after now (skips the quiet ones).
    IF v_claimed = 0 THEN
        INSERT INTO queen.consumer_watermarks
            (queue_name, consumer_group, last_empty_scan_at, updated_at)
        VALUES (p_queue, p_group, v_now, v_now)
        ON CONFLICT (queue_name, consumer_group)
        DO UPDATE SET last_empty_scan_at = EXCLUDED.last_empty_scan_at,
                      updated_at = EXCLUDED.updated_at;
    END IF;

    RETURN jsonb_build_object('partitions', v_out);
END;
$$;

-- ============================================================================
-- Ack-by-transaction fallback: used when the broker's in-memory LeaseRegistry
-- (server/include/queen/storage_v2.hpp) misses — e.g. the pop was answered by
-- another broker process. p_acks = [{"txn":text,"ok":bool}].
--
-- Each txn is resolved to its (seq, frame_idx) through the queen.seg_dedup window
-- (partition_id, hashtextextended(txn, 0)). The delivered positions are
-- enumerated from (next_seq, next_off) to (batch_end_seq, batch_end_off) by
-- walking queen.seg_segments msg_counts, and the cursor advances to the highest
-- CONTIGUOUS prefix whose positions are all acked ok — same rule the
-- LeaseRegistry applies in memory. The lease is released either way.
--
-- Unresolvable txns (dedup entry expired/purged, or dedup disabled for the
-- queue) count as NOT acked: the cursor stops right before their position and
-- those frames are redelivered. Redelivery over data loss — by design.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.seg_ack_by_txn_v1(
    p_partition_id UUID,
    p_group TEXT,
    p_worker TEXT,
    p_acks JSONB
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_c RECORD;
    v_seg RECORD;
    -- Acked-ok positions as a {"<seq>:<frame_idx>": true} lookup object.
    v_acked JSONB;
    -- Explicitly nacked positions (ok=false), same shape. Distinguishes a
    -- genuine failure (retry/DLQ) from a frame the client simply did not ack
    -- in this request (partial ack -> plain redelivery, no attempt charged).
    v_nacked JSONB;
    v_pos_seq BIGINT;
    v_pos_off INTEGER;
    v_adv BIGINT := 0;
    v_stop BOOLEAN := FALSE;
    v_start INTEGER;
    v_end INTEGER;
    v_i INTEGER;
    v_end_count INTEGER;
    -- Retry/DLQ decision inputs (from queen.queues, the config source of truth).
    v_retry_limit INTEGER;
    v_dlq_enabled BOOLEAN;
    v_attempt INTEGER;
    v_poison_count INTEGER;
    -- Durable per-position path (Trap 2): the leased batch's ordered position map
    -- (queen.partition_consumers.batch_positions) + the contiguous-prefix computation.
    v_bp JSONB;
    v_el JSONB;
    v_prefix_last_seq BIGINT;
    v_prefix_last_off INTEGER;
    v_head_seq BIGINT;
    v_head_off INTEGER;
    v_head_nacked BOOLEAN;
    v_new_seq BIGINT;
    v_new_off INTEGER;
    v_pc INTEGER;
    v_delta BIGINT;
BEGIN
    SELECT * INTO v_c FROM queen.partition_consumers c
    WHERE c.partition_id = p_partition_id AND c.consumer_group = p_group
    FOR UPDATE;
    IF NOT FOUND OR v_c.worker_id IS DISTINCT FROM p_worker
       OR v_c.lease_expires_at IS NULL OR v_c.lease_expires_at < clock_timestamp() THEN
        RETURN jsonb_build_object('ok', false, 'error', 'invalid or expired lease');
    END IF;
    IF v_c.batch_end_seq IS NULL OR v_c.batch_end_off IS NULL THEN
        RETURN jsonb_build_object('ok', false, 'error', 'no leased batch');
    END IF;

    -- ================================================================
    -- Durable per-position ack (Trap 2). When the pop recorded a position
    -- map, each ack MARKS its position(s) here and the lease is NOT released
    -- on the first ack: the cursor advances over the highest CONTIGUOUS
    -- acked-OR-dlq prefix and the lease is held until every position is
    -- resolved (or it expires). This is what lets .batch(N).each() acks land
    -- in any order (and on any replica — the map is on the row) without the
    -- first ack finalizing the batch out from under the other in-flight acks.
    -- ================================================================
    IF v_c.batch_positions IS NOT NULL THEN
        -- Resolve THIS request's acked-ok / nacked txns to (seq:frame_idx) keys
        -- through the dedup window (same resolver as the legacy walk below).
        SELECT COALESCE(jsonb_object_agg(d.seq::text || ':' || d.frame_idx::text, true),
                        '{}'::jsonb)
        INTO v_acked
        FROM jsonb_to_recordset(p_acks) AS a(txn TEXT, ok BOOLEAN)
        JOIN queen.seg_dedup d
          ON d.partition_id = p_partition_id
         AND d.txn_hash = hashtextextended(a.txn, 0)
        WHERE a.ok;

        SELECT COALESCE(jsonb_object_agg(d.seq::text || ':' || d.frame_idx::text, true),
                        '{}'::jsonb)
        INTO v_nacked
        FROM jsonb_to_recordset(p_acks) AS a(txn TEXT, ok BOOLEAN)
        JOIN queen.seg_dedup d
          ON d.partition_id = p_partition_id
         AND d.txn_hash = hashtextextended(a.txn, 0)
        WHERE NOT a.ok;

        -- Apply the acked-ok marks to the durable map (idempotent; positions
        -- already acked/dlq are left as-is).
        SELECT COALESCE(jsonb_agg(
                 CASE WHEN NOT COALESCE((e->>'acked')::boolean, false)
                       AND NOT COALESCE((e->>'dlq')::boolean, false)
                       AND (v_acked ? ((e->>'seq') || ':' || (e->>'off')))
                      THEN jsonb_set(e, '{acked}', 'true'::jsonb)
                      ELSE e END), '[]'::jsonb)
        INTO v_bp
        FROM jsonb_array_elements(v_c.batch_positions) e;

        -- Highest contiguous acked/dlq prefix + first unresolved (head) position.
        v_prefix_last_seq := NULL; v_prefix_last_off := NULL;
        v_head_seq := NULL; v_head_off := NULL; v_head_nacked := FALSE;
        FOR v_el IN SELECT value FROM jsonb_array_elements(v_bp) LOOP
            IF COALESCE((v_el->>'acked')::boolean, false)
               OR COALESCE((v_el->>'dlq')::boolean, false) THEN
                v_prefix_last_seq := (v_el->>'seq')::bigint;
                v_prefix_last_off := (v_el->>'off')::int;
            ELSE
                v_head_seq := (v_el->>'seq')::bigint;
                v_head_off := (v_el->>'off')::int;
                v_head_nacked := (v_nacked ? ((v_el->>'seq') || ':' || (v_el->>'off')));
                EXIT;
            END IF;
        END LOOP;

        -- New cursor = position just past the contiguous prefix (normalized on
        -- the segment boundary via msg_count). Empty prefix keeps the cursor.
        IF v_prefix_last_seq IS NULL THEN
            v_new_seq := v_c.next_seq; v_new_off := v_c.next_off;
        ELSE
            SELECT msg_count INTO v_pc FROM queen.seg_segments
            WHERE partition_id = p_partition_id AND seq = v_prefix_last_seq;
            IF v_pc IS NOT NULL AND v_prefix_last_off + 1 >= v_pc THEN
                v_new_seq := v_prefix_last_seq + 1; v_new_off := 0;
            ELSE
                v_new_seq := v_prefix_last_seq; v_new_off := v_prefix_last_off + 1;
            END IF;
        END IF;
        -- Positions newly crossed by the cursor this call (for total_consumed).
        SELECT count(*) INTO v_delta
        FROM jsonb_array_elements(v_bp) e
        WHERE ((e->>'seq')::bigint, (e->>'off')::int) >= (v_c.next_seq, v_c.next_off)
          AND ((e->>'seq')::bigint, (e->>'off')::int) <  (v_new_seq, v_new_off);

        -- Fully resolved: every position acked or dlq'd -> release the lease.
        IF v_head_seq IS NULL THEN
            UPDATE queen.partition_consumers SET
                next_seq = v_new_seq, next_off = v_new_off,
                worker_id = NULL, lease_expires_at = NULL,
                batch_end_seq = NULL, batch_end_off = NULL,
                batch_positions = NULL,
                total_consumed = total_consumed + v_delta
            WHERE partition_id = p_partition_id AND consumer_group = p_group;
            RETURN jsonb_build_object('ok', true, 'acked', v_delta,
                                      'seq', v_new_seq, 'off', v_new_off);
        END IF;

        -- Head unresolved AND explicitly nacked -> retry / DLQ / drop, keyed off
        -- the queue's retry_limit + the delivery attempt of the poison head.
        IF v_head_nacked THEN
            SELECT COALESCE(qq.retry_limit, 3),
                   COALESCE(qq.dead_letter_queue, false)
                   OR COALESCE(qq.dlq_after_max_retries, false)
            INTO v_retry_limit, v_dlq_enabled
            FROM queen.seg_partitions sp
            JOIN queen.seg_queues sq ON sq.id = sp.queue_id
            LEFT JOIN queen.queues qq ON qq.name = sq.name
            WHERE sp.id = p_partition_id;
            v_retry_limit := COALESCE(v_retry_limit, 3);
            v_dlq_enabled := COALESCE(v_dlq_enabled, false);
            v_attempt := CASE
                WHEN (v_c.attempt_seq, v_c.attempt_off)
                     IS NOT DISTINCT FROM (v_head_seq, v_head_off)
                THEN COALESCE(v_c.attempt_count, 1) ELSE 1 END;

            IF v_attempt > v_retry_limit THEN
                IF v_dlq_enabled THEN
                    -- Persist this call's acks + advance to the prefix end, but
                    -- KEEP the lease so the broker can decode+snapshot the poison
                    -- head frame and call queen.seg_dlq_head_v1 (which marks the
                    -- position dlq, re-extends the prefix past it, and releases
                    -- the lease iff the whole batch is then resolved).
                    UPDATE queen.partition_consumers SET
                        next_seq = v_new_seq, next_off = v_new_off,
                        batch_positions = v_bp,
                        total_consumed = total_consumed + v_delta
                    WHERE partition_id = p_partition_id AND consumer_group = p_group;
                    RETURN jsonb_build_object(
                        'ok', true, 'dlq', true,
                        'partitionId', p_partition_id, 'group', p_group,
                        'worker', p_worker, 'seq', v_head_seq, 'frameIdx', v_head_off,
                        'acked', v_delta);
                ELSE
                    -- No DLQ: DROP the poison head (mark it dlq/disposed), then
                    -- re-extend the contiguous prefix past it and re-resolve.
                    SELECT COALESCE(jsonb_agg(
                             CASE WHEN (e->>'seq')::bigint = v_head_seq
                                   AND (e->>'off')::int = v_head_off
                                  THEN jsonb_set(e, '{dlq}', 'true'::jsonb)
                                  ELSE e END), '[]'::jsonb)
                    INTO v_bp
                    FROM jsonb_array_elements(v_bp) e;

                    v_prefix_last_seq := NULL; v_prefix_last_off := NULL;
                    v_head_seq := NULL; v_head_off := NULL;
                    FOR v_el IN SELECT value FROM jsonb_array_elements(v_bp) LOOP
                        IF COALESCE((v_el->>'acked')::boolean, false)
                           OR COALESCE((v_el->>'dlq')::boolean, false) THEN
                            v_prefix_last_seq := (v_el->>'seq')::bigint;
                            v_prefix_last_off := (v_el->>'off')::int;
                        ELSE
                            v_head_seq := (v_el->>'seq')::bigint;
                            EXIT;
                        END IF;
                    END LOOP;
                    SELECT msg_count INTO v_pc FROM queen.seg_segments
                    WHERE partition_id = p_partition_id AND seq = v_prefix_last_seq;
                    IF v_pc IS NOT NULL AND v_prefix_last_off + 1 >= v_pc THEN
                        v_new_seq := v_prefix_last_seq + 1; v_new_off := 0;
                    ELSE
                        v_new_seq := v_prefix_last_seq; v_new_off := v_prefix_last_off + 1;
                    END IF;
                    SELECT count(*) INTO v_delta
                    FROM jsonb_array_elements(v_bp) e
                    WHERE ((e->>'seq')::bigint, (e->>'off')::int) >= (v_c.next_seq, v_c.next_off)
                      AND ((e->>'seq')::bigint, (e->>'off')::int) <  (v_new_seq, v_new_off);

                    IF v_head_seq IS NULL THEN
                        UPDATE queen.partition_consumers SET
                            next_seq = v_new_seq, next_off = v_new_off,
                            worker_id = NULL, lease_expires_at = NULL,
                            batch_end_seq = NULL, batch_end_off = NULL,
                            batch_positions = NULL,
                            attempt_seq = NULL, attempt_off = NULL, attempt_count = 0,
                            total_consumed = total_consumed + v_delta
                        WHERE partition_id = p_partition_id AND consumer_group = p_group;
                    ELSE
                        UPDATE queen.partition_consumers SET
                            next_seq = v_new_seq, next_off = v_new_off,
                            batch_positions = v_bp,
                            total_consumed = total_consumed + v_delta
                        WHERE partition_id = p_partition_id AND consumer_group = p_group;
                    END IF;
                    RETURN jsonb_build_object('ok', true, 'dropped', true,
                        'acked', v_delta, 'seq', v_new_seq, 'off', v_new_off);
                END IF;
            ELSE
                -- Retry budget remains: release the lease so the poison head (and
                -- everything after the acked prefix) is redelivered; the attempt
                -- columns are preserved so the next delivery increments the count.
                UPDATE queen.partition_consumers SET
                    next_seq = v_new_seq, next_off = v_new_off,
                    worker_id = NULL, lease_expires_at = NULL,
                    batch_end_seq = NULL, batch_end_off = NULL,
                    batch_positions = NULL,
                    total_consumed = total_consumed + v_delta
                WHERE partition_id = p_partition_id AND consumer_group = p_group;
                RETURN jsonb_build_object('ok', true, 'acked', v_delta,
                                          'seq', v_new_seq, 'off', v_new_off);
            END IF;
        END IF;

        -- Head is merely un-acked (waiting for its own ok-ack, arriving in a
        -- separate call): advance over the contiguous prefix but RETAIN the lease
        -- and the (updated) map. This is the out-of-order accumulation fix.
        UPDATE queen.partition_consumers SET
            next_seq = v_new_seq, next_off = v_new_off,
            batch_positions = v_bp,
            total_consumed = total_consumed + v_delta
        WHERE partition_id = p_partition_id AND consumer_group = p_group;
        RETURN jsonb_build_object('ok', true, 'acked', v_delta,
                                  'seq', v_new_seq, 'off', v_new_off);
    END IF;

    -- Resolve acked-ok txns to positions. Txns with ok=false, or without a
    -- surviving dedup entry, simply never enter the lookup => not acked.
    SELECT COALESCE(jsonb_object_agg(d.seq::text || ':' || d.frame_idx::text, true),
                    '{}'::jsonb)
    INTO v_acked
    FROM jsonb_to_recordset(p_acks) AS a(txn TEXT, ok BOOLEAN)
    JOIN queen.seg_dedup d
      ON d.partition_id = p_partition_id
     AND d.txn_hash = hashtextextended(a.txn, 0)
    WHERE a.ok;

    -- ... and the explicitly-nacked (ok=false) positions, so the head-frame
    -- retry/DLQ logic below fires ONLY on a real nack, never on a frame that
    -- was merely left un-acked (which keeps the redeliver-on-release behavior).
    SELECT COALESCE(jsonb_object_agg(d.seq::text || ':' || d.frame_idx::text, true),
                    '{}'::jsonb)
    INTO v_nacked
    FROM jsonb_to_recordset(p_acks) AS a(txn TEXT, ok BOOLEAN)
    JOIN queen.seg_dedup d
      ON d.partition_id = p_partition_id
     AND d.txn_hash = hashtextextended(a.txn, 0)
    WHERE NOT a.ok;

    -- Walk the delivered range. Offset rules mirror queen.seg_pop_segments_v1:
    -- next_off applies only to the exact cursor segment; batch_end_off caps
    -- the exact batch-end segment.
    v_pos_seq := v_c.next_seq;
    v_pos_off := v_c.next_off;
    FOR v_seg IN
        SELECT s.seq, s.msg_count FROM queen.seg_segments s
        WHERE s.partition_id = p_partition_id
          AND s.seq >= v_c.next_seq AND s.seq <= v_c.batch_end_seq
        ORDER BY s.seq
    LOOP
        v_start := CASE WHEN v_seg.seq = v_c.next_seq THEN v_c.next_off ELSE 0 END;
        v_end   := CASE WHEN v_seg.seq = v_c.batch_end_seq
                        THEN LEAST(v_c.batch_end_off, v_seg.msg_count)
                        ELSE v_seg.msg_count END;
        v_i := v_start;
        WHILE v_i < v_end LOOP
            IF v_acked ? (v_seg.seq::text || ':' || v_i::text) THEN
                v_adv := v_adv + 1;
                IF v_i + 1 >= v_seg.msg_count THEN
                    v_pos_seq := v_seg.seq + 1; v_pos_off := 0;  -- normalized
                ELSE
                    v_pos_seq := v_seg.seq; v_pos_off := v_i + 1;
                END IF;
            ELSE
                v_stop := TRUE;
                EXIT;
            END IF;
            v_i := v_i + 1;
        END LOOP;
        EXIT WHEN v_stop;
    END LOOP;

    -- --------------------------------------------------------- retry / DLQ
    -- The walk stopped at the head un-acked frame (v_pos_seq, v_pos_off). If
    -- that frame was EXPLICITLY nacked, charge the delivery attempt against the
    -- queue's retry_limit and decide: redeliver (budget remains), DROP (budget
    -- exhausted, no DLQ), or hand the poison to the broker for dead-lettering
    -- (budget exhausted, DLQ enabled). A frame that is only un-acked (not in the
    -- request) falls through to the plain lease-release redelivery below.
    IF v_stop AND (v_nacked ? (v_pos_seq::text || ':' || v_pos_off::text)) THEN
        SELECT COALESCE(qq.retry_limit, 3),
               COALESCE(qq.dead_letter_queue, false)
               OR COALESCE(qq.dlq_after_max_retries, false)
        INTO v_retry_limit, v_dlq_enabled
        FROM queen.seg_partitions sp
        JOIN queen.seg_queues sq ON sq.id = sp.queue_id
        LEFT JOIN queen.queues qq ON qq.name = sq.name
        WHERE sp.id = p_partition_id;
        v_retry_limit := COALESCE(v_retry_limit, 3);
        v_dlq_enabled := COALESCE(v_dlq_enabled, false);

        -- Attempt count for THIS poison head: the accumulated per-(partition,
        -- group) count when the poison is the tracked batch-start frame
        -- (attempt_seq/off set by seg_pop_segments_v1 on delivery), else a fresh
        -- 1 (this frame is failing at this position for the first time).
        v_attempt := CASE
            WHEN (v_c.attempt_seq, v_c.attempt_off)
                 IS NOT DISTINCT FROM (v_pos_seq, v_pos_off)
            THEN COALESCE(v_c.attempt_count, 1) ELSE 1 END;

        -- v1 parity: redeliver while attempt_count <= retry_limit; the delivery
        -- that EXCEEDS retry_limit is dead-lettered/dropped (retry_limit=0 =>
        -- the very first nack, attempt 1 > 0, is exhausted).
        IF v_attempt > v_retry_limit THEN
            IF v_dlq_enabled THEN
                -- Hand off to the broker: it alone can decompress the frame,
                -- snapshot its payload, and call queen.seg_dlq_head_v1 (which
                -- files the DLQ row, advances the cursor past this one frame,
                -- resets attempt state, and releases the lease). Leave the lease
                -- held so that follow-up call validates the worker.
                RETURN jsonb_build_object(
                    'ok', true, 'dlq', true,
                    'partitionId', p_partition_id, 'group', p_group,
                    'worker', p_worker, 'seq', v_pos_seq, 'frameIdx', v_pos_off,
                    'acked', v_adv);
            ELSE
                -- No DLQ: DROP the poison frame — advance the cursor past it so
                -- it is not redelivered (normalized on the segment boundary),
                -- reset attempt state, release the lease.
                SELECT msg_count INTO v_poison_count FROM queen.seg_segments
                WHERE partition_id = p_partition_id AND seq = v_pos_seq;
                IF v_poison_count IS NOT NULL AND v_pos_off + 1 >= v_poison_count THEN
                    v_pos_seq := v_pos_seq + 1; v_pos_off := 0;
                ELSE
                    v_pos_off := v_pos_off + 1;
                END IF;
                v_adv := v_adv + 1;   -- the dropped frame is disposed of
                UPDATE queen.partition_consumers SET
                    next_seq = v_pos_seq, next_off = v_pos_off,
                    worker_id = NULL, lease_expires_at = NULL,
                    batch_end_seq = NULL, batch_end_off = NULL,
                    attempt_seq = NULL, attempt_off = NULL, attempt_count = 0,
                    total_consumed = total_consumed + v_adv
                WHERE partition_id = p_partition_id AND consumer_group = p_group;
                RETURN jsonb_build_object('ok', true, 'dropped', true,
                    'acked', v_adv, 'seq', v_pos_seq, 'off', v_pos_off);
            END IF;
        END IF;
        -- else: budget remains -> fall through to the plain release below;
        -- cursor stays at the poison and the attempt columns are preserved, so
        -- the next delivery from this position increments attempt_count.
    END IF;

    IF NOT v_stop THEN
        -- Every delivered position acked ok (or none survive retention):
        -- advance straight to batch_end, normalized like ack_segments_v1.
        SELECT msg_count INTO v_end_count FROM queen.seg_segments
        WHERE partition_id = p_partition_id AND seq = v_c.batch_end_seq;
        IF v_end_count IS NOT NULL AND v_c.batch_end_off >= v_end_count THEN
            v_pos_seq := v_c.batch_end_seq + 1; v_pos_off := 0;
        ELSE
            v_pos_seq := v_c.batch_end_seq; v_pos_off := v_c.batch_end_off;
        END IF;
    END IF;

    UPDATE queen.partition_consumers SET
        next_seq = v_pos_seq,
        next_off = v_pos_off,
        worker_id = NULL, lease_expires_at = NULL,
        batch_end_seq = NULL, batch_end_off = NULL,
        total_consumed = total_consumed + v_adv
    WHERE partition_id = p_partition_id AND consumer_group = p_group;

    RETURN jsonb_build_object('ok', true, 'acked', v_adv,
                              'seq', v_pos_seq, 'off', v_pos_off);
END;
$$;

-- ============================================================================
-- Pending probe: does any partition of the queue have frames the group has
-- not consumed yet? Coarse on the missing-consumer-row and retention-gap
-- cases (may report true when retention already ate the backlog); the head
-- next_off vs msg_count refinement is a single PK lookup, so it's included.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.seg_has_pending_v1(p_queue TEXT, p_group TEXT)
RETURNS BOOLEAN
LANGUAGE sql STABLE
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM queen.seg_partitions p
        JOIN queen.seg_queues q ON q.id = p.queue_id AND q.name = p_queue
        LEFT JOIN queen.partition_consumers c
          ON c.partition_id = p.id AND c.consumer_group = p_group
        WHERE (c.partition_id IS NULL AND p.last_seq >= 1)
           OR p.last_seq > c.next_seq
           OR (p.last_seq = c.next_seq AND EXISTS (
                 SELECT 1 FROM queen.seg_segments s
                 WHERE s.partition_id = p.id AND s.seq = c.next_seq
                   AND s.msg_count > c.next_off))
    );
$$;
