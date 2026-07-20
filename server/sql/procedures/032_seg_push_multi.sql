-- ============================================================================
-- Storage v2 (segments) — cross-partition BUNDLING (the second fusion axis).
-- Companion to 023_storage_v2.sql. Applied idempotently at boot.
--
-- queen.seg_push_segments_multi_v1(p_segments jsonb, p_blobs bytea[])
--   Bundle N ready, DISJOINT partitions' segments into ONE transaction (one
--   commit / one fsync). p_segments is a JSON array index-aligned with p_blobs:
--     p_segments[i] = {"queue","partition","metas","msg_count"}
--     p_blobs[i]    = zstd(frames) for that segment (BINARY bytea, NOT base64 —
--                     the bundling path binds blobs natively, dropping the text
--                     base64 round-trip the single-segment wire wrapper uses).
--
--   Deadlock-safety: segments are processed in ascending seg_partitions.id
--   order — the SAME total lock order seg_transaction_wire_v1 and the 026
--   maintenance sweeps (seg_retention_sweep_v1 / seg_evict_v1) use — so a bundle
--   holding N partition-row locks never deadlocks (40P01) against a concurrent
--   bundle, sweep or transaction.
--
--   Per-segment isolation: each segment runs the seg_push_segment_v1 body under
--   its OWN savepoint (a BEGIN ... EXCEPTION block). A dedup unique_violation on
--   one partition rolls back JUST that segment (its seq bump, dedup rows and
--   segment insert are undone — no gap, no partial state) and records
--   {"status":"duplicate","dups":[{i,mid},...]}, while every OTHER segment in
--   the bundle still commits. A clean insert records
--   {"status":"queued","seq":N,"createdAt":...}.
--
--   Returns a JSON array of per-segment results in INPUT order (index-aligned
--   with p_segments) so the broker can demux each verdict back to that
--   partition's contributing push requests.
--
-- One call = one transaction = one commit / one fsync for all N partitions.
-- Because the bundled partitions are disjoint (the broker's per-partition
-- in-flight gate guarantees it) each keeps its own monotone seq with a single
-- writer; the txn just touches N independent seg_partitions rows.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.seg_push_segments_multi_v1(
    p_segments JSONB,
    p_blobs    BYTEA[]
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_n       INT;
    v_rec     RECORD;
    v_res     JSONB;
    v_results JSONB[];
BEGIN
    v_n := jsonb_array_length(COALESCE(p_segments, '[]'::jsonb));
    IF v_n = 0 THEN
        RETURN '[]'::jsonb;
    END IF;
    -- Pre-sized (1-based) result buffer, written by input ordinal so the
    -- returned array stays in input order even though the loop runs in id order.
    v_results := array_fill(NULL::jsonb, ARRAY[v_n]);

    -- Ensure every queue + partition exists up front so each segment resolves to
    -- a seg_partitions.id we can ORDER BY (seg_push_segment_v1 also creates them
    -- lazily, but the loop must know the id BEFORE the push to lock in id order).
    INSERT INTO queen.seg_queues (name)
    SELECT DISTINCT seg->>'queue'
    FROM jsonb_array_elements(p_segments) seg
    WHERE COALESCE(seg->>'queue', '') <> ''
    ON CONFLICT (name) DO NOTHING;

    -- RUSTFIX item 26: create the queen.queues config rows too (the loop's
    -- seg_push_segment_v1 skips creation because the partition already exists).
    INSERT INTO queen.queues (name, namespace, task, storage)
    SELECT DISTINCT seg->>'queue',
           split_part(seg->>'queue', '.', 1),
           CASE WHEN position('.' in seg->>'queue') > 0 THEN split_part(seg->>'queue', '.', 2) ELSE '' END,
           'segments'
    FROM jsonb_array_elements(p_segments) seg
    WHERE COALESCE(seg->>'queue', '') <> ''
    ON CONFLICT (name) DO NOTHING;

    INSERT INTO queen.seg_partitions (queue_id, name)
    SELECT DISTINCT q.id, seg->>'partition'
    FROM jsonb_array_elements(p_segments) seg
    JOIN queen.seg_queues q ON q.name = seg->>'queue'
    ON CONFLICT (queue_id, name) DO NOTHING;

    FOR v_rec IN
        SELECT t.ord::int              AS ord,
               t.seg->>'queue'         AS queue,
               t.seg->>'partition'     AS partition,
               t.seg->'metas'          AS metas,
               (t.seg->>'msg_count')::int AS msg_count,
               p.id                    AS pid
        FROM jsonb_array_elements(p_segments) WITH ORDINALITY AS t(seg, ord)
        JOIN queen.seg_queues q ON q.name = t.seg->>'queue'
        JOIN queen.seg_partitions p
          ON p.queue_id = q.id AND p.name = t.seg->>'partition'
        ORDER BY p.id
    LOOP
        BEGIN
            -- Same body as the single-segment path (seq alloc under the
            -- partition row lock, dedup probe, seg_segments insert, seg_dedup
            -- populate) — blob bound as native binary bytea, no base64.
            v_res := queen.seg_push_segment_v1(
                v_rec.queue,
                v_rec.partition,
                v_rec.metas,
                p_blobs[v_rec.ord],
                v_rec.msg_count);
        EXCEPTION WHEN unique_violation THEN
            -- Only this segment's savepoint rolled back; the rest of the bundle
            -- is untouched. Report the ORIGINAL message ids of the duplicate
            -- frames (identical contract to seg_push_segment_wire_v1's dup path).
            v_res := jsonb_build_object(
                'status', 'duplicate',
                'dups', (
                    SELECT COALESCE(
                        jsonb_agg(jsonb_build_object('i', m.i, 'mid', d.message_id)),
                        '[]'::jsonb)
                    FROM jsonb_to_recordset(v_rec.metas) AS m(i INT, mid UUID, txn TEXT)
                    JOIN queen.seg_dedup d
                      ON d.partition_id = v_rec.pid
                     AND d.txn_hash = hashtextextended(m.txn, 0)));
        END;
        v_results[v_rec.ord] := v_res;
    END LOOP;

    RETURN to_jsonb(v_results);
END;
$$;
