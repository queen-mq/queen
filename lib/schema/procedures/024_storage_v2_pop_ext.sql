-- ============================================================================
-- Storage v2 (segments engine) — pop extensions.
-- Builds on lib/schema/procedures/023_storage_v2.sql (schema q2): wildcard
-- pop across a queue's partitions, cross-process ack-by-transaction fallback,
-- and a cheap pending probe. Applied idempotently at boot after 023.
-- ============================================================================

-- ============================================================================
-- Wildcard pop: claim frames from several partitions of a queue in one call.
-- Candidate filter is COARSE on purpose (no consumer row yet, OR
-- last_seq >= next_seq): q2.pop_segments_v1 re-verifies the cursor and the
-- lease under the row lock, so a stale candidate just yields zero rows.
-- Deterministic partition-name order keeps concurrent wildcard poppers from
-- interleaving lock acquisition (and SKIP LOCKED inside pop_segments_v1 keeps
-- them non-blocking anyway). All claimed partitions share p_worker, mirroring
-- v1 pop_unified_batch_v4's one-lease-id-per-batch semantics.
-- Returns {"partitions":[{"partition":name,"partitionId":uuid,
--                         "segments":[{seq,startOff,take,msgCount,createdAt,
--                                      blob(b64)}, ...]}, ...]}.
-- ============================================================================
CREATE OR REPLACE FUNCTION q2.pop_wildcard_wire_v1(
    p_queue TEXT,
    p_group TEXT,
    p_budget INTEGER,
    p_lease_seconds INTEGER,
    p_worker TEXT,
    p_auto_ack BOOLEAN,
    p_max_partitions INTEGER
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
BEGIN
    SELECT id INTO v_qid FROM q2.queues WHERE name = p_queue;
    IF v_qid IS NULL THEN
        RETURN jsonb_build_object('partitions', '[]'::jsonb);
    END IF;

    FOR v_p IN
        SELECT p.id, p.name
        FROM q2.partitions p
        LEFT JOIN q2.consumers c
          ON c.partition_id = p.id AND c.consumer_group = p_group
        WHERE p.queue_id = v_qid
          AND (c.partition_id IS NULL OR p.last_seq >= c.next_seq)
        ORDER BY p.name
    LOOP
        -- Same row shape / keys as q2.pop_segments_wire_v1.
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
        FROM q2.pop_segments_v1(p_queue, v_p.name, p_group,
                                v_remaining, p_lease_seconds, p_worker, p_auto_ack);

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

    RETURN jsonb_build_object('partitions', v_out);
END;
$$;

-- ============================================================================
-- Ack-by-transaction fallback: used when the broker's in-memory LeaseRegistry
-- (server/include/queen/storage_v2.hpp) misses — e.g. the pop was answered by
-- another broker process. p_acks = [{"txn":text,"ok":bool}].
--
-- Each txn is resolved to its (seq, frame_idx) through the q2.dedup window
-- (partition_id, hashtextextended(txn, 0)). The delivered positions are
-- enumerated from (next_seq, next_off) to (batch_end_seq, batch_end_off) by
-- walking q2.segments msg_counts, and the cursor advances to the highest
-- CONTIGUOUS prefix whose positions are all acked ok — same rule the
-- LeaseRegistry applies in memory. The lease is released either way.
--
-- Unresolvable txns (dedup entry expired/purged, or dedup disabled for the
-- queue) count as NOT acked: the cursor stops right before their position and
-- those frames are redelivered. Redelivery over data loss — by design.
-- ============================================================================
CREATE OR REPLACE FUNCTION q2.ack_by_txn_v1(
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
    v_pos_seq BIGINT;
    v_pos_off INTEGER;
    v_adv BIGINT := 0;
    v_stop BOOLEAN := FALSE;
    v_start INTEGER;
    v_end INTEGER;
    v_i INTEGER;
    v_end_count INTEGER;
BEGIN
    SELECT * INTO v_c FROM q2.consumers c
    WHERE c.partition_id = p_partition_id AND c.consumer_group = p_group
    FOR UPDATE;
    IF NOT FOUND OR v_c.worker_id IS DISTINCT FROM p_worker
       OR v_c.lease_expires_at IS NULL OR v_c.lease_expires_at < clock_timestamp() THEN
        RETURN jsonb_build_object('ok', false, 'error', 'invalid or expired lease');
    END IF;
    IF v_c.batch_end_seq IS NULL OR v_c.batch_end_off IS NULL THEN
        RETURN jsonb_build_object('ok', false, 'error', 'no leased batch');
    END IF;

    -- Resolve acked-ok txns to positions. Txns with ok=false, or without a
    -- surviving dedup entry, simply never enter the lookup => not acked.
    SELECT COALESCE(jsonb_object_agg(d.seq::text || ':' || d.frame_idx::text, true),
                    '{}'::jsonb)
    INTO v_acked
    FROM jsonb_to_recordset(p_acks) AS a(txn TEXT, ok BOOLEAN)
    JOIN q2.dedup d
      ON d.partition_id = p_partition_id
     AND d.txn_hash = hashtextextended(a.txn, 0)
    WHERE a.ok;

    -- Walk the delivered range. Offset rules mirror q2.pop_segments_v1:
    -- next_off applies only to the exact cursor segment; batch_end_off caps
    -- the exact batch-end segment.
    v_pos_seq := v_c.next_seq;
    v_pos_off := v_c.next_off;
    FOR v_seg IN
        SELECT s.seq, s.msg_count FROM q2.segments s
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

    IF NOT v_stop THEN
        -- Every delivered position acked ok (or none survive retention):
        -- advance straight to batch_end, normalized like ack_segments_v1.
        SELECT msg_count INTO v_end_count FROM q2.segments
        WHERE partition_id = p_partition_id AND seq = v_c.batch_end_seq;
        IF v_end_count IS NOT NULL AND v_c.batch_end_off >= v_end_count THEN
            v_pos_seq := v_c.batch_end_seq + 1; v_pos_off := 0;
        ELSE
            v_pos_seq := v_c.batch_end_seq; v_pos_off := v_c.batch_end_off;
        END IF;
    END IF;

    UPDATE q2.consumers SET
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
CREATE OR REPLACE FUNCTION q2.has_pending_v1(p_queue TEXT, p_group TEXT)
RETURNS BOOLEAN
LANGUAGE sql STABLE
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM q2.partitions p
        JOIN q2.queues q ON q.id = p.queue_id AND q.name = p_queue
        LEFT JOIN q2.consumers c
          ON c.partition_id = p.id AND c.consumer_group = p_group
        WHERE (c.partition_id IS NULL AND p.last_seq >= 1)
           OR p.last_seq > c.next_seq
           OR (p.last_seq = c.next_seq AND EXISTS (
                 SELECT 1 FROM q2.segments s
                 WHERE s.partition_id = p.id AND s.seq = c.next_seq
                   AND s.msg_count > c.next_off))
    );
$$;
