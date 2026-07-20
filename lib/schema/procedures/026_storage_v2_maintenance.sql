-- ============================================================================
-- Storage v2 (segments engine) — maintenance procedures.
-- Companion to 023_storage_v2.sql (schema q2). Applied idempotently at boot.
--
--   queen.seg_retention_boundary_v1  boundary walk helper (same walk as queen.seg_retention_v1)
--   queen.seg_retention_sweep_v1     per-queue retention + completed-retention + dedup purge
--   queen.seg_evict_v1               max_queue_size enforcement for v2 queues
--   queen.seg_transaction_wire_v1    atomic push+ack for segment queues (wire, base64)
--
-- Configuration lives in queen.queues (retention_enabled, retention_seconds,
-- completed_retention_seconds, max_queue_size, storage) — the v2 engine only
-- applies to rows with storage = 'segments'; queen.queues and queen.seg_queues are
-- correlated by name. Dedup window lives in queen.seg_queues.dedup_window_seconds.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Boundary walk helper: first retained seq for a (partition, cutoff), i.e.
-- the smallest seq >= p_from whose created_at >= p_cutoff. created_at is
-- monotone in seq per partition (stamped under the seq-allocator row lock),
-- so this is a single forward index walk starting at the previous watermark —
-- identical to the walk inside queen.seg_retention_v1 in 023. Returns p_from when
-- there is nothing to delete (boundary == watermark => empty delete range).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.seg_retention_boundary_v1(
    p_partition_id UUID,
    p_from BIGINT,
    p_cutoff TIMESTAMPTZ
) RETURNS BIGINT
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    v_boundary BIGINT;
BEGIN
    SELECT s.seq INTO v_boundary
    FROM queen.seg_segments s
    WHERE s.partition_id = p_partition_id
      AND s.seq >= p_from
      AND s.created_at >= p_cutoff
    ORDER BY s.seq LIMIT 1;

    IF v_boundary IS NULL THEN
        -- everything (if anything) above the watermark is older than the cutoff
        SELECT max(seq) + 1 INTO v_boundary FROM queen.seg_segments
        WHERE partition_id = p_partition_id AND seq >= p_from;
    END IF;

    RETURN COALESCE(v_boundary, p_from);
END;
$$;

-- ----------------------------------------------------------------------------
-- retention_sweep_v1: per-queue retention for v2 queues, mirroring the v1
-- RetentionService semantics:
--
--   * retention_seconds ("all"): segments older than now()-retention_seconds
--     are deleted regardless of consumption state. Gated (like v1) on
--     retention_enabled AND retention_seconds > 0.
--   * completed_retention_seconds ("consumed"): v1 deletes consumed messages
--     older than this cutoff independently of retention_seconds. In v2 there
--     is no per-message state: a segment is 'consumed' when its seq is below
--     EVERY consumer group's next_seq for the partition (the cursor contract:
--     seq < next_seq means fully consumed). Segments both consumed and older
--     than now()-completed_retention_seconds are deleted, regardless of
--     retention_seconds. A partition with no consumer groups has nothing
--     consumed (v1 likewise requires a consumer row per group). Gated on
--     retention_enabled AND completed_retention_seconds > 0.
--
-- Both rules delete a contiguous seq prefix starting at the watermark
-- (retention_seq); the combined boundary is the max of the applicable rules,
-- so the watermark invariant "every seq < retention_seq is deleted" holds and
-- repeated sweeps never re-scan the dead head of the PK index.
--
-- Afterwards the dedup window is purged per q2 queue (entries older than
-- queen.seg_queues.dedup_window_seconds), in LIMIT-batched deletes so a huge backlog
-- can't hold one giant delete open. The dedup purge intentionally covers ALL
-- q2 queues with a window (not just retention-enabled ones): the window is a
-- dedup property, unrelated to retention.
--
-- Returns {"queues":N,"segments_deleted":N,"dedup_purged":N} where "queues"
-- counts the v2 queues examined for retention.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.seg_retention_sweep_v1()
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_q RECORD;
    v_p RECORD;
    v_queues BIGINT := 0;
    v_segments_deleted BIGINT := 0;
    v_dedup_purged BIGINT := 0;
    v_boundary BIGINT;
    v_min_next BIGINT;
    v_n BIGINT;
    c_batch CONSTANT INTEGER := 50000;  -- dedup purge batch size
BEGIN
    -- ---------------------------------------------------------- retention
    -- LOCK ORDER (deadlock discipline): the watermark UPDATE takes a
    -- queen.seg_partitions row lock that is held to commit, and
    -- queen.seg_transaction_wire_v1 pre-locks its push partitions in ascending id
    -- order. Iterating partitions in that same GLOBAL ascending-id order —
    -- one flat loop across all retention-enabled queues, not per-queue
    -- batches whose ids could interleave — gives every multi-partition
    -- locker one total order (live-reproduced 40P01 without it).
    SELECT COUNT(*) INTO v_queues
    FROM queen.queues qq
    JOIN queen.seg_queues q2q ON q2q.name = qq.name
    WHERE qq.storage = 'segments'
      AND qq.retention_enabled
      AND (qq.retention_seconds > 0 OR qq.completed_retention_seconds > 0);

    FOR v_p IN
        SELECT p.id, p.retention_seq,
               qq.retention_seconds, qq.completed_retention_seconds
        FROM queen.queues qq
        JOIN queen.seg_queues q2q ON q2q.name = qq.name
        JOIN queen.seg_partitions p ON p.queue_id = q2q.id
        WHERE qq.storage = 'segments'
          AND qq.retention_enabled
          AND (qq.retention_seconds > 0 OR qq.completed_retention_seconds > 0)
        ORDER BY p.id
    LOOP
        v_boundary := v_p.retention_seq;

        -- Rule 1: time-based retention over ALL segments.
        IF v_p.retention_seconds > 0 THEN
            v_boundary := GREATEST(v_boundary, queen.seg_retention_boundary_v1(
                v_p.id, v_p.retention_seq,
                now() - make_interval(secs => v_p.retention_seconds)));
        END IF;

        -- Rule 2: completed retention over CONSUMED segments only
        -- (seq < min(next_seq) across all groups). The time boundary is
        -- capped at the slowest cursor so we never delete anything a
        -- group still has to read (except via rule 1, which is the
        -- explicit "drop unconsumed data after N seconds" knob).
        IF v_p.completed_retention_seconds > 0 THEN
            SELECT MIN(c.next_seq) INTO v_min_next
            FROM queen.partition_consumers c WHERE c.partition_id = v_p.id;

            IF v_min_next IS NOT NULL AND v_min_next > v_p.retention_seq THEN
                v_boundary := GREATEST(v_boundary, LEAST(
                    v_min_next,
                    queen.seg_retention_boundary_v1(
                        v_p.id, v_p.retention_seq,
                        now() - make_interval(secs => v_p.completed_retention_seconds))));
            END IF;
        END IF;

        IF v_boundary > v_p.retention_seq THEN
            DELETE FROM queen.seg_segments
            WHERE partition_id = v_p.id
              AND seq >= v_p.retention_seq AND seq < v_boundary;
            GET DIAGNOSTICS v_n = ROW_COUNT;
            v_segments_deleted := v_segments_deleted + v_n;

            UPDATE queen.seg_partitions SET retention_seq = v_boundary
            WHERE id = v_p.id AND retention_seq < v_boundary;
        END IF;
    END LOOP;

    -- --------------------------------------------------------- dedup purge
    FOR v_q IN
        SELECT q2q.id AS q2_queue_id, q2q.dedup_window_seconds
        FROM queen.seg_queues q2q
        WHERE q2q.dedup_window_seconds > 0
    LOOP
        LOOP
            DELETE FROM queen.seg_dedup d
            WHERE d.ctid IN (
                SELECT d2.ctid
                FROM queen.seg_dedup d2
                JOIN queen.seg_partitions p ON p.id = d2.partition_id
                WHERE p.queue_id = v_q.q2_queue_id
                  AND d2.created_at < now() - make_interval(secs => v_q.dedup_window_seconds)
                LIMIT c_batch
            );
            GET DIAGNOSTICS v_n = ROW_COUNT;
            v_dedup_purged := v_dedup_purged + v_n;
            EXIT WHEN v_n < c_batch;
        END LOOP;
    END LOOP;

    RETURN jsonb_build_object(
        'queues', v_queues,
        'segments_deleted', v_segments_deleted,
        'dedup_purged', v_dedup_purged
    );
END;
$$;

-- ----------------------------------------------------------------------------
-- evict_v1: enforce queen.queues.max_queue_size on v2 queues.
--
-- Per partition, "pending" = SUM(msg_count) over segments with
-- seq >= MIN(next_seq) across the partition's consumer groups — i.e. what the
-- slowest group still has to read (partial consumption of the cursor segment
-- is ignored: whole-segment granularity, a bounded over-count of at most one
-- segment's frames). With no consumer groups, ALL segments are pending.
--
-- If pending > max_queue_size, the oldest segments are deleted (WHOLE
-- segments, contiguous prefix from the retention watermark, in seq order)
-- until pending <= max. The walk naturally deletes fully-consumed segments
-- below every cursor first (free — no data loss, they don't count as
-- pending), then continues into unconsumed territory.
--
-- DATA LOSS: like v1 eviction, messages beyond the cap are dropped for every
-- consumer group, including messages of an in-flight leased batch. The pop
-- path tolerates the resulting gap (a cursor pointing at a deleted segment
-- resumes at the next existing seq, frame 0). Because deletion is
-- whole-segment, up to segment_size-1 extra messages may be dropped past the
-- exact cap boundary.
--
-- Returns {"queues":N,"segments_deleted":N,"messages_evicted":N} where
-- messages_evicted counts PENDING (unconsumed) messages dropped.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.seg_evict_v1()
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_p RECORD;
    v_s RECORD;
    v_queues BIGINT := 0;
    v_segments_deleted BIGINT := 0;
    v_messages_evicted BIGINT := 0;
    v_min_next BIGINT;
    v_pending BIGINT;
    v_excess BIGINT;
    v_boundary BIGINT;
    v_n BIGINT;
BEGIN
    -- LOCK ORDER: same discipline as queen.seg_retention_sweep_v1 — one flat
    -- partition loop in GLOBAL ascending queen.seg_partitions.id order, matching
    -- queen.seg_transaction_wire_v1's pre-lock order, so concurrent evictions,
    -- sweeps and transactions never acquire partition row locks in
    -- conflicting orders (40P01).
    SELECT COUNT(*) INTO v_queues
    FROM queen.queues qq
    JOIN queen.seg_queues q2q ON q2q.name = qq.name
    WHERE qq.storage = 'segments'
      AND qq.max_queue_size > 0;

    FOR v_p IN
        SELECT p.id, p.retention_seq, qq.max_queue_size
        FROM queen.queues qq
        JOIN queen.seg_queues q2q ON q2q.name = qq.name
        JOIN queen.seg_partitions p ON p.queue_id = q2q.id
        WHERE qq.storage = 'segments'
          AND qq.max_queue_size > 0
        ORDER BY p.id
    LOOP
        -- Slowest cursor; no groups => everything counts as pending.
        SELECT MIN(c.next_seq) INTO v_min_next
        FROM queen.partition_consumers c WHERE c.partition_id = v_p.id;
        v_min_next := COALESCE(v_min_next, 0);

        SELECT COALESCE(SUM(s.msg_count), 0) INTO v_pending
        FROM queen.seg_segments s
        WHERE s.partition_id = v_p.id AND s.seq >= v_min_next;

        CONTINUE WHEN v_pending <= v_p.max_queue_size;
        v_excess := v_pending - v_p.max_queue_size;

        -- Oldest-first walk from the watermark; stop once enough pending
        -- messages are covered. Consumed segments (seq < min cursor) are
        -- swept along for free but reduce nothing.
        v_boundary := v_p.retention_seq;
        FOR v_s IN
            SELECT s.seq, s.msg_count
            FROM queen.seg_segments s
            WHERE s.partition_id = v_p.id AND s.seq >= v_p.retention_seq
            ORDER BY s.seq
        LOOP
            v_boundary := v_s.seq + 1;
            IF v_s.seq >= v_min_next THEN
                v_excess := v_excess - v_s.msg_count;
                v_messages_evicted := v_messages_evicted + v_s.msg_count;
            END IF;
            EXIT WHEN v_excess <= 0;
        END LOOP;

        IF v_boundary > v_p.retention_seq THEN
            DELETE FROM queen.seg_segments
            WHERE partition_id = v_p.id
              AND seq >= v_p.retention_seq AND seq < v_boundary;
            GET DIAGNOSTICS v_n = ROW_COUNT;
            v_segments_deleted := v_segments_deleted + v_n;

            UPDATE queen.seg_partitions SET retention_seq = v_boundary
            WHERE id = v_p.id AND retention_seq < v_boundary;
        END IF;
    END LOOP;

    RETURN jsonb_build_object(
        'queues', v_queues,
        'segments_deleted', v_segments_deleted,
        'messages_evicted', v_messages_evicted
    );
END;
$$;

-- ----------------------------------------------------------------------------
-- transaction_wire_v1: atomic push+ack for segment queues (wire form: blobs
-- travel as base64 text, like the other q2 *_wire_* functions).
--
--   p = {
--     "pushes": [{"queue","partition","metas","blobB64","count"}, ...],
--     "acks":   [{"partitionId","group","worker","uptoSeq","uptoOff",
--                 "ok","count"}, ...]
--   }
--
-- An ack element may ALSO come in the txn-resolved form
--   {"partitionId","group","worker","txns":[{"txn","ok"},...]}
-- (detected by the presence of "txns"): used when the broker's in-memory
-- LeaseRegistry cannot resolve batch positions because the pop was served by
-- ANOTHER broker process. The txns are resolved to positions SQL-side via
-- queen.seg_ack_by_txn_v1 (024) — cursor advances to the highest contiguous
-- acked-ok prefix, lease released — so cross-broker transactions work
-- without any broker registry.
--
-- All-or-nothing by construction: one function call = one transaction, and
-- every failure path RAISEs, so a duplicate push or a rejected ack rolls back
-- every other operation in the batch.
--
--   * pushes reuse queen.seg_push_segment_v1 verbatim (seq allocation under the
--     partition row lock, dedup probe, segment insert). A duplicate
--     (unique_violation, message 'QDUP ...') is re-raised with transaction
--     context, keeping ERRCODE unique_violation so callers can classify it.
--   * acks reuse queen.seg_ack_segments_v1 / queen.seg_ack_by_txn_v1; their soft failures
--     (ok:false — invalid or expired lease, position beyond leased batch,
--     ...) are escalated to an exception so the whole transaction rolls back.
--   * acks address the q2 partition by uuid ("partitionId", echoed by
--     pop_segments_wire_v1); it is resolved back to (queue, partition) names.
--   * acks execute in ascending (partition_id, group) order — NOT input
--     order: each ack locks its (partition, group) queen.partition_consumers row until
--     commit, so concurrent transactions acking overlapping sets must take
--     those locks in one total order (the same ascending-uuid discipline the
--     push pre-pass and the maintenance sweeps use for queen.seg_partitions).
--
-- Mixed-engine guard: a queue that exists in queen.queues with
-- storage <> 'segments' may not appear in a v2 transaction (its data lives in
-- queen.messages; combining engines in one atomic batch is a broker bug).
-- Queues absent from queen.queues are allowed — like queen.seg_push_segment_v1, the
-- q2 side is created lazily and the broker already routed them to v2.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.seg_transaction_wire_v1(p JSONB)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_op JSONB;
    v_res JSONB;
    v_pushes JSONB := '[]'::jsonb;
    v_acks JSONB := '[]'::jsonb;
    v_bad TEXT;
    v_pid UUID;
    v_queue TEXT;
    v_partition TEXT;
BEGIN
    -- Guard: no push may target a rows-engine queue.
    SELECT qq.name INTO v_bad
    FROM jsonb_array_elements(COALESCE(p->'pushes', '[]'::jsonb)) op
    JOIN queen.queues qq ON qq.name = op->>'queue'
    WHERE qq.storage <> 'segments'
    LIMIT 1;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'QTXN queue "%" has storage != ''segments''; mixed-engine transactions are not supported', v_bad;
    END IF;

    -- Guard: no ack may target a partition whose queue is rows-engine.
    SELECT qq.name INTO v_bad
    FROM jsonb_array_elements(COALESCE(p->'acks', '[]'::jsonb)) op
    JOIN queen.seg_partitions p2 ON p2.id = (op->>'partitionId')::uuid
    JOIN queen.seg_queues q2q ON q2q.id = p2.queue_id
    JOIN queen.queues qq ON qq.name = q2q.name
    WHERE qq.storage <> 'segments'
    LIMIT 1;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'QTXN queue "%" has storage != ''segments''; mixed-engine transactions are not supported', v_bad;
    END IF;

    -- Deadlock-safety pre-pass (mirrors PUSHSER in queen.execute_transaction_v2):
    -- a transaction may push to several partitions, and each push takes the
    -- queen.seg_partitions row lock. Ensure the rows exist, then lock them in
    -- ascending id order BEFORE the first push, so two concurrent
    -- transactions can never lock the same pair in opposite order.
    INSERT INTO queen.seg_queues (name)
    SELECT DISTINCT op->>'queue'
    FROM jsonb_array_elements(COALESCE(p->'pushes', '[]'::jsonb)) op
    WHERE COALESCE(op->>'queue', '') <> ''
    ON CONFLICT (name) DO NOTHING;

    INSERT INTO queen.seg_partitions (queue_id, name)
    SELECT DISTINCT q2q.id, op->>'partition'
    FROM jsonb_array_elements(COALESCE(p->'pushes', '[]'::jsonb)) op
    JOIN queen.seg_queues q2q ON q2q.name = op->>'queue'
    ON CONFLICT (queue_id, name) DO NOTHING;

    FOR v_pid IN
        SELECT DISTINCT p2.id
        FROM jsonb_array_elements(COALESCE(p->'pushes', '[]'::jsonb)) op
        JOIN queen.seg_queues q2q ON q2q.name = op->>'queue'
        JOIN queen.seg_partitions p2 ON p2.queue_id = q2q.id AND p2.name = op->>'partition'
        ORDER BY p2.id
    LOOP
        PERFORM 1 FROM queen.seg_partitions WHERE id = v_pid FOR UPDATE;
    END LOOP;

    -- ------------------------------------------------------------- pushes
    FOR v_op IN SELECT * FROM jsonb_array_elements(COALESCE(p->'pushes', '[]'::jsonb))
    LOOP
        BEGIN
            v_res := queen.seg_push_segment_v1(
                v_op->>'queue',
                v_op->>'partition',
                v_op->'metas',
                decode(v_op->>'blobB64', 'base64'),
                (v_op->>'count')::integer);
        EXCEPTION WHEN unique_violation THEN
            RAISE EXCEPTION 'QDUP duplicate messages in queue "%" partition "%"; transaction rolled back',
                v_op->>'queue', v_op->>'partition'
                USING ERRCODE = 'unique_violation';
        END;
        v_pushes := v_pushes || jsonb_build_array(v_res);
    END LOOP;

    -- --------------------------------------------------------------- acks
    -- Ascending (partition_id, group) execution order — see header. The
    -- result array follows execution order; the broker treats the arrays as
    -- opaque (it only inspects top-level "ok").
    FOR v_op IN
        SELECT op.value
        FROM jsonb_array_elements(COALESCE(p->'acks', '[]'::jsonb)) op
        ORDER BY (op.value->>'partitionId')::uuid, op.value->>'group'
    LOOP
        SELECT q2q.name, p2.name INTO v_queue, v_partition
        FROM queen.seg_partitions p2
        JOIN queen.seg_queues q2q ON q2q.id = p2.queue_id
        WHERE p2.id = (v_op->>'partitionId')::uuid;
        IF v_queue IS NULL THEN
            RAISE EXCEPTION 'QTXN ack references unknown partition %', v_op->>'partitionId';
        END IF;

        IF v_op ? 'txns' THEN
            -- Cross-broker form: positions resolved from the queen.seg_dedup
            -- window, cursor advanced to the highest contiguous acked-ok
            -- prefix, lease released (exactly the plain wire fallback path).
            v_res := queen.seg_ack_by_txn_v1(
                (v_op->>'partitionId')::uuid,
                v_op->>'group',
                v_op->>'worker',
                COALESCE(v_op->'txns', '[]'::jsonb));
        ELSE
            v_res := queen.seg_ack_segments_v1(
                v_queue,
                v_partition,
                v_op->>'group',
                v_op->>'worker',
                (v_op->>'uptoSeq')::bigint,
                (v_op->>'uptoOff')::integer,
                COALESCE((v_op->>'ok')::boolean, true),
                COALESCE((v_op->>'count')::integer, 0));
        END IF;

        IF NOT COALESCE((v_res->>'ok')::boolean, false) THEN
            RAISE EXCEPTION 'QTXN ack failed for partition % group "%": %; transaction rolled back',
                v_op->>'partitionId', v_op->>'group', v_res->>'error';
        END IF;
        v_acks := v_acks || jsonb_build_array(v_res);
    END LOOP;

    RETURN jsonb_build_object('ok', true, 'pushes', v_pushes, 'acks', v_acks);
END;
$$;
