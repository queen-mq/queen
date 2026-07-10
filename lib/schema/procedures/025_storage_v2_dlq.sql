-- ============================================================================
-- Storage v2 — retry tracking + poison-message DLQ for segment queues.
-- Applied after 023_storage_v2.sql (lexical boot order): redefines
-- q2.pop_segments_v1 / q2.pop_segments_wire_v1 to carry the delivery attempt
-- count, and adds q2.dlq + the broker-facing dead-letter procedures.
--
-- Retry model: attempt state lives on the (partition, group) consumer row.
-- A non-auto-ack pop delivering a batch that STARTS at the same position as
-- the previous delivery is a redelivery (nack / lease expiry left the cursor
-- in place) -> attempt_count increments; a batch starting anywhere else is
-- fresh work -> attempt_count resets to 1. Acks need no changes: advancing
-- the cursor moves the start position, which resets the counter naturally.
--
-- When attempt_count exceeds the queue's retry_limit the broker (which alone
-- can decompress frames) extracts the poisoned head frame and calls
-- q2.dlq_head_v1 with a payload SNAPSHOT. Storing the snapshot instead of a
-- reference keeps q2.dlq decoupled from segment retention (v1's FK-cascade
-- coupling silently drops DLQ rows when the source row is deleted).
-- ============================================================================

-- Per-(partition, group) attempt tracking: position of the last non-auto-ack
-- delivery's first frame + how many times a batch starting there was handed out.
ALTER TABLE q2.consumers ADD COLUMN IF NOT EXISTS attempt_seq BIGINT;
ALTER TABLE q2.consumers ADD COLUMN IF NOT EXISTS attempt_off INTEGER;
ALTER TABLE q2.consumers ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0;

-- Dead-lettered frames. payload is a snapshot extracted by the broker
-- (blobs are opaque to SQL); (partition_id, seq, frame_idx) records where the
-- poison frame lived for tracing, without any FK into q2.segments.
CREATE TABLE IF NOT EXISTS q2.dlq (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    partition_id UUID NOT NULL,
    consumer_group TEXT NOT NULL,
    seq BIGINT NOT NULL,
    frame_idx INTEGER NOT NULL,
    message_id UUID,
    transaction_id TEXT,
    payload JSONB,
    error TEXT,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_q2_dlq_partition_failed_at
    ON q2.dlq (partition_id, failed_at DESC);

-- ============================================================================
-- pop (redefinition of 023's q2.pop_segments_v1; supersedes it at boot).
-- Identical to the 023 body except for attempt tracking and v1-parity queue
-- semantics:
--   * new OUT column r_attempt: the attempt count of THIS delivery (same
--     value on every returned row). auto_ack pops return 0 and leave the
--     attempt state untouched.
--   * the lease UPDATE also persists (attempt_seq, attempt_off,
--     attempt_count) for the delivered batch's first frame.
--   * delayed_processing / window_buffer (read from queen.queues by name,
--     the config source of truth — q2.queues carries only engine-side
--     options): segments younger than delayed_processing seconds are not
--     delivered yet, and a partition whose NEWEST segment is younger than
--     window_buffer seconds delivers nothing (002d_pop_unified_v4.sql:486-504
--     semantics). Both checks are cheap because created_at is monotone in
--     seq per partition: the window probe is one backward PK step, and the
--     delayed filter cuts a suffix the budget loop would not read anyway.
--   * subscription seeding (v1 'new'/'from' semantics): on FIRST contact of
--     a (partition, group) — i.e. when the consumer row insert actually
--     inserts — p_sub_mode='new' or p_sub_from='now' seeds the cursor past
--     the existing backlog (next_seq = last_seq + 1); a timestamp p_sub_from
--     seeds it at the first segment with created_at >= that ts (forward walk
--     from retention_seq; everything older is skipped). Default
--     ('all' / '') keeps the full-backlog cursor (next_seq = 1). Invalid
--     timestamps are ignored like v1 (full backlog). Existing consumer rows
--     are never re-seeded.
-- DROP+CREATE instead of CREATE OR REPLACE: adding an OUT column changes the
-- return type, which OR REPLACE refuses. Both the 023 signature and this
-- file's previous (pre-seeding) one are dropped so re-applies never leave an
-- ambiguous overload behind. Callers using the old 7-arg form keep working
-- through the two trailing defaults.
-- ============================================================================
DROP FUNCTION IF EXISTS q2.pop_segments_v1(TEXT, TEXT, TEXT, INTEGER, INTEGER, TEXT, BOOLEAN);
DROP FUNCTION IF EXISTS q2.pop_segments_v1(TEXT, TEXT, TEXT, INTEGER, INTEGER, TEXT, BOOLEAN, TEXT, TEXT);

CREATE FUNCTION q2.pop_segments_v1(
    p_queue TEXT,
    p_partition TEXT,
    p_group TEXT,
    p_budget INTEGER,
    p_lease_seconds INTEGER,
    p_worker TEXT,
    p_auto_ack BOOLEAN DEFAULT FALSE,
    p_sub_mode TEXT DEFAULT 'all',
    p_sub_from TEXT DEFAULT ''
) RETURNS TABLE (
    r_seq BIGINT,
    r_start_off INTEGER,
    r_take INTEGER,
    r_msg_count INTEGER,
    r_created_at TIMESTAMPTZ,
    r_blob BYTEA,
    r_attempt INTEGER
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_pid UUID;
    v_last_seq BIGINT;
    v_retention_seq BIGINT;
    v_delayed INTEGER := 0;
    v_window INTEGER := 0;
    v_deadline TIMESTAMPTZ;
    v_newest TIMESTAMPTZ;
    v_seed_seq BIGINT;
    v_from_ts TIMESTAMPTZ;
    v_next_seq BIGINT;
    v_next_off INTEGER;
    v_att_seq BIGINT;
    v_att_off INTEGER;
    v_att_count INTEGER;
    v_attempt INTEGER := 0;
    v_start_seq BIGINT;
    v_start_off INTEGER;
    v_budget INTEGER := GREATEST(p_budget, 1);
    v_taken INTEGER := 0;
    v_row RECORD;
    v_off INTEGER;
    v_avail INTEGER;
    v_take INTEGER;
    v_end_seq BIGINT;
    v_end_off INTEGER;
    v_end_count INTEGER;
    v_now TIMESTAMPTZ := clock_timestamp();
BEGIN
    SELECT p.id, p.last_seq, p.retention_seq INTO v_pid, v_last_seq, v_retention_seq
    FROM q2.partitions p JOIN q2.queues q ON q.id = p.queue_id
    WHERE q.name = p_queue AND p.name = p_partition;
    IF v_pid IS NULL THEN RETURN; END IF;

    -- Visibility knobs live on the v1 config row (created by the configure
    -- path for both engines). Absent row = both disabled.
    SELECT COALESCE(qq.delayed_processing, 0), COALESCE(qq.window_buffer, 0)
    INTO v_delayed, v_window
    FROM queen.queues qq WHERE qq.name = p_queue;
    IF NOT FOUND THEN
        v_delayed := 0; v_window := 0;
    END IF;

    -- window_buffer: if the partition received a segment within the last
    -- window_buffer seconds, deliver NOTHING (v1 checks the partition's
    -- newest message the same way, before touching the consumer row).
    -- created_at is monotone in seq, so the newest segment is the max seq.
    IF v_window > 0 THEN
        SELECT s.created_at INTO v_newest
        FROM q2.segments s
        WHERE s.partition_id = v_pid
        ORDER BY s.seq DESC LIMIT 1;
        IF v_newest IS NOT NULL
           AND v_newest > v_now - make_interval(secs => v_window) THEN
            RETURN;
        END IF;
    END IF;

    -- Subscription seeding: only worth computing when the row might not
    -- exist yet AND a non-default subscription was requested.
    IF (COALESCE(p_sub_mode, 'all') = 'new' OR COALESCE(p_sub_from, '') <> '')
       AND NOT EXISTS (SELECT 1 FROM q2.consumers c
                       WHERE c.partition_id = v_pid AND c.consumer_group = p_group) THEN
        IF p_sub_from <> '' AND p_sub_from <> 'now' THEN
            BEGIN
                v_from_ts := p_sub_from::timestamptz;
            EXCEPTION WHEN OTHERS THEN
                v_from_ts := NULL;  -- unparsable: ignore, v1 does the same
            END;
            IF v_from_ts IS NOT NULL THEN
                -- First segment at/after the requested timestamp (created_at
                -- is monotone in seq: single forward walk from the retention
                -- watermark). Nothing that recent yet -> future-only cursor.
                SELECT s.seq INTO v_seed_seq
                FROM q2.segments s
                WHERE s.partition_id = v_pid
                  AND s.seq >= v_retention_seq
                  AND s.created_at >= v_from_ts
                ORDER BY s.seq LIMIT 1;
                IF v_seed_seq IS NULL THEN
                    v_seed_seq := v_last_seq + 1;
                END IF;
            END IF;
        ELSIF p_sub_from = 'now' OR p_sub_mode = 'new' THEN
            v_seed_seq := v_last_seq + 1;
        END IF;
    END IF;

    INSERT INTO q2.consumers (partition_id, consumer_group, next_seq)
    VALUES (v_pid, p_group, COALESCE(v_seed_seq, 1))
    ON CONFLICT DO NOTHING;

    -- Claim: skip if another worker holds a live lease (or the row is being
    -- popped concurrently — SKIP LOCKED keeps concurrent pops non-blocking).
    SELECT c.next_seq, c.next_off, c.attempt_seq, c.attempt_off, c.attempt_count
    INTO v_next_seq, v_next_off, v_att_seq, v_att_off, v_att_count
    FROM q2.consumers c
    WHERE c.partition_id = v_pid AND c.consumer_group = p_group
      AND (c.worker_id IS NULL OR c.lease_expires_at IS NULL OR c.lease_expires_at < v_now)
    FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN RETURN; END IF;

    -- delayed_processing: only segments at least v_delayed seconds old are
    -- visible. Monotone created_at => the filter cuts a contiguous suffix;
    -- the loop stops at the budget before ever scanning past it, and the
    -- filtered tail is bounded by rate x delayed_processing.
    IF v_delayed > 0 THEN
        v_deadline := v_now - make_interval(secs => v_delayed);
    END IF;

    FOR v_row IN
        SELECT s.seq, s.msg_count, s.created_at, s.blob
        FROM q2.segments s
        WHERE s.partition_id = v_pid AND s.seq >= v_next_seq
          AND (v_deadline IS NULL OR s.created_at <= v_deadline)
        ORDER BY s.seq
    LOOP
        -- Offset applies ONLY to the exact cursor segment. If retention
        -- deleted it (consumer far behind), the first row has seq > next_seq
        -- and starts at frame 0.
        v_off := CASE WHEN v_row.seq = v_next_seq THEN v_next_off ELSE 0 END;
        v_avail := v_row.msg_count - v_off;
        IF v_avail <= 0 THEN
            CONTINUE;  -- head segment already fully consumed (normalization)
        END IF;

        IF v_taken = 0 THEN
            -- First delivered frame: the batch's identity for retry tracking.
            -- Tracked from the ACTUAL first frame (not the raw cursor) so a
            -- retention-shifted redelivery still matches its previous attempt.
            v_start_seq := v_row.seq;
            v_start_off := v_off;
            IF NOT p_auto_ack THEN
                v_attempt := CASE WHEN (v_att_seq, v_att_off)
                                       IS NOT DISTINCT FROM (v_start_seq, v_start_off)
                                  THEN v_att_count + 1 ELSE 1 END;
            END IF;
        END IF;

        v_take := LEAST(v_avail, v_budget - v_taken);

        r_seq        := v_row.seq;
        r_start_off  := v_off;
        r_take       := v_take;
        r_msg_count  := v_row.msg_count;
        r_created_at := v_row.created_at;
        r_blob       := v_row.blob;
        r_attempt    := v_attempt;
        RETURN NEXT;

        v_taken   := v_taken + v_take;
        v_end_seq := v_row.seq;
        v_end_off := v_off + v_take;
        v_end_count := v_row.msg_count;
        EXIT WHEN v_taken >= v_budget;
    END LOOP;

    IF v_taken = 0 THEN RETURN; END IF;

    IF p_auto_ack THEN
        -- auto_ack never redelivers: attempt state untouched, r_attempt = 0.
        UPDATE q2.consumers SET
            next_seq = CASE WHEN v_end_off >= v_end_count THEN v_end_seq + 1 ELSE v_end_seq END,
            next_off = CASE WHEN v_end_off >= v_end_count THEN 0 ELSE v_end_off END,
            worker_id = NULL, lease_expires_at = NULL,
            batch_end_seq = NULL, batch_end_off = NULL,
            total_consumed = total_consumed + v_taken
        WHERE partition_id = v_pid AND consumer_group = p_group;
    ELSE
        UPDATE q2.consumers SET
            worker_id = p_worker,
            lease_expires_at = v_now + make_interval(secs => p_lease_seconds),
            batch_end_seq = v_end_seq,
            batch_end_off = v_end_off,
            attempt_seq = v_start_seq,
            attempt_off = v_start_off,
            attempt_count = v_attempt
        WHERE partition_id = v_pid AND consumer_group = p_group;
    END IF;
END;
$$;

-- ============================================================================
-- Wire wrapper (redefinition of 023's v2 wire shape; supersedes it at boot).
-- Same "segments"/"partitionId" contract, plus top-level "attempt": the
-- attempt count of this delivery (0 for auto_ack or empty pops).
-- The two trailing subscription args default to the no-seeding values, so
-- callers of the historical 7-arg form keep working; the 7-arg overload is
-- dropped (an overload pair would make 7-arg calls ambiguous).
-- ============================================================================
DROP FUNCTION IF EXISTS q2.pop_segments_wire_v1(TEXT, TEXT, TEXT, INTEGER, INTEGER, TEXT, BOOLEAN);
DROP FUNCTION IF EXISTS q2.pop_segments_wire_v1(TEXT, TEXT, TEXT, INTEGER, INTEGER, TEXT, BOOLEAN, TEXT, TEXT);
CREATE FUNCTION q2.pop_segments_wire_v1(
    p_queue TEXT, p_partition TEXT, p_group TEXT,
    p_budget INTEGER, p_lease_seconds INTEGER, p_worker TEXT, p_auto_ack BOOLEAN,
    p_sub_mode TEXT DEFAULT 'all', p_sub_from TEXT DEFAULT ''
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_segments JSONB;
    v_attempt INTEGER;
    v_pid UUID;
BEGIN
    SELECT p.id INTO v_pid
    FROM q2.partitions p JOIN q2.queues q ON q.id = p.queue_id
    WHERE q.name = p_queue AND p.name = p_partition;

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'seq', r_seq,
        'startOff', r_start_off,
        'take', r_take,
        'msgCount', r_msg_count,
        'createdAt', to_char(r_created_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
        'blob', encode(r_blob, 'base64')
    )), '[]'::jsonb), COALESCE(max(r_attempt), 0)
    INTO v_segments, v_attempt
    FROM q2.pop_segments_v1(p_queue, p_partition, p_group,
                            p_budget, p_lease_seconds, p_worker, p_auto_ack,
                            p_sub_mode, p_sub_from);
    RETURN jsonb_build_object('segments', v_segments,
                              'partitionId', COALESCE(v_pid::text, ''),
                              'attempt', v_attempt);
END;
$$;

-- ============================================================================
-- dlq_head: dead-letter the poisoned HEAD frame of the leased batch.
-- Called by the broker when a delivery's attempt count exceeded the queue's
-- retry_limit; the broker passes the frame it extracted (position, ids,
-- payload snapshot) plus the failure reason. Under the validated lease it
-- inserts the q2.dlq row, advances the cursor past that one frame (segment
-- boundary handled via msg_count), resets the attempt state, and releases
-- the lease. Calling it twice is safe: the first call released the lease, so
-- the second fails the worker match and returns {ok:false} without side
-- effects.
-- ============================================================================
CREATE OR REPLACE FUNCTION q2.dlq_head_v1(
    p_partition_id UUID,
    p_group TEXT,
    p_worker TEXT,
    p_seq BIGINT,
    p_frame_idx INTEGER,
    p_message_id UUID,
    p_txn TEXT,
    p_payload JSONB,
    p_error TEXT
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_c RECORD;
    v_count INTEGER;
    v_id UUID;
BEGIN
    SELECT * INTO v_c FROM q2.consumers c
    WHERE c.partition_id = p_partition_id AND c.consumer_group = p_group
    FOR UPDATE;
    IF NOT FOUND OR v_c.worker_id IS DISTINCT FROM p_worker
       OR v_c.lease_expires_at IS NULL OR v_c.lease_expires_at < clock_timestamp() THEN
        RETURN jsonb_build_object('ok', false, 'error', 'invalid or expired lease');
    END IF;

    INSERT INTO q2.dlq (partition_id, consumer_group, seq, frame_idx,
                        message_id, transaction_id, payload, error)
    VALUES (p_partition_id, p_group, p_seq, p_frame_idx,
            p_message_id, p_txn, p_payload, p_error)
    RETURNING id INTO v_id;

    -- Advance past the poison frame. If it was the segment's last frame the
    -- cursor rolls over to (seq+1, 0); if the segment is already gone
    -- (retention) the unnormalized offset is still safe — pop skips segments
    -- whose frames are exhausted and applies the offset only to the exact
    -- cursor segment.
    SELECT msg_count INTO v_count FROM q2.segments
    WHERE partition_id = p_partition_id AND seq = p_seq;

    UPDATE q2.consumers SET
        next_seq = CASE WHEN v_count IS NOT NULL AND p_frame_idx + 1 >= v_count
                        THEN p_seq + 1 ELSE p_seq END,
        next_off = CASE WHEN v_count IS NOT NULL AND p_frame_idx + 1 >= v_count
                        THEN 0 ELSE p_frame_idx + 1 END,
        worker_id = NULL, lease_expires_at = NULL,
        batch_end_seq = NULL, batch_end_off = NULL,
        attempt_seq = NULL, attempt_off = NULL, attempt_count = 0,
        -- the frame is disposed of (moved to the DLQ), count it as consumed
        total_consumed = total_consumed + 1
    WHERE partition_id = p_partition_id AND consumer_group = p_group;

    RETURN jsonb_build_object('ok', true, 'id', v_id);
END;
$$;

-- ============================================================================
-- dlq_list: browse dead-lettered frames of a queue (v1-compatible keys).
-- ============================================================================
CREATE OR REPLACE FUNCTION q2.dlq_list_v1(
    p_queue TEXT,
    p_partition TEXT DEFAULT NULL,
    p_group TEXT DEFAULT NULL,
    p_limit INTEGER DEFAULT 100
) RETURNS JSONB
LANGUAGE sql STABLE
AS $$
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'id', t.id,
        'transactionId', t.transaction_id,
        'consumerGroup', t.consumer_group,
        'error', t.error,
        'failedAt', to_char(t.failed_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'data', t.payload,
        'queue', t.queue,
        'partition', t.partition
    ) ORDER BY t.failed_at DESC), '[]'::jsonb)
    FROM (
        SELECT d.id, d.transaction_id, d.consumer_group, d.error, d.failed_at,
               d.payload, q.name AS queue, p.name AS partition
        FROM q2.dlq d
        JOIN q2.partitions p ON p.id = d.partition_id
        JOIN q2.queues q ON q.id = p.queue_id
        WHERE q.name = p_queue
          AND (p_partition IS NULL OR p.name = p_partition)
          AND (p_group IS NULL OR d.consumer_group = p_group)
        ORDER BY d.failed_at DESC
        LIMIT GREATEST(COALESCE(p_limit, 100), 1)
    ) t;
$$;
