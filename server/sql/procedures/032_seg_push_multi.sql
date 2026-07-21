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

-- ============================================================================
-- queen.seg_push_segments_multi_v2 — the CPU-lean bundling path.
--
-- Same contract and guarantees as v1, but built to cut PG CPU per message:
--
--   * Typed parallel arrays instead of one jsonb payload. v1 took
--     (p_segments jsonb, p_blobs bytea[]) and the broker bound the segments as
--     $1::text::jsonb, so PG PARSED the entire payload — including every
--     message's metadata — on every call, then re-walked it with
--     jsonb_array_elements up to four times. With dedup off (the common case)
--     that parsed metadata is never even read. v2 takes each field as its own
--     array (text[] queues / partitions, int[] msg_counts, bytea[] blobs) plus
--     the per-segment metadata as text[] that is cast to jsonb ONLY for a
--     segment whose queue actually has dedup enabled. The hot path touches zero
--     jsonb.
--
--   * No per-segment savepoint on the common path. v1 ran every segment inside
--     its own BEGIN ... EXCEPTION subtransaction so one partition's dedup
--     unique_violation could roll back just that segment. When NO segment in the
--     bundle targets a dedup-enabled queue, a unique_violation is impossible, so
--     the whole bundle commits as one set-based seq bump + one set-based insert
--     with no subtransactions at all. The savepoint (and its subtransaction XID)
--     is paid only for segments that can actually conflict.
--
--   * Set-based writes. The seq allocation and the segment inserts are single
--     statements over the whole bundle instead of a per-segment nested
--     seg_push_segment_v1 call (which also re-resolved each partition id the
--     bundle had already computed).
--
-- Preserved verbatim from v1 (the guardrails):
--   * Cross-partition bundling + 100≈1000 partition scaling: v2 does the same
--     amount of per-partition work; it just removes per-message overhead.
--   * Deadlock-safety: partition rows are locked in ascending seg_partitions.id
--     order — the SAME total order seg_transaction_wire_v1 and the 026 sweeps
--     use — before any write.
--   * Per-partition monotonic seq (single writer per partition; the bundle's
--     partitions are DISJOINT, guaranteed by the broker's per-partition
--     in-flight gate) and created_at monotone in commit order (stamped after the
--     locks are held).
--   * Per-segment dedup-conflict isolation for dedup-enabled queues, and the
--     identical {"status":"queued",...} / {"status":"duplicate","dups":[...]}
--     result contract, index-aligned with the input arrays.
--
-- The p_metas[i] element is the SAME JSON array v1 embedded as p_segments[i].metas
-- ([{"i","mid","txn"}...]); it is only inspected on the dedup path.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.seg_push_segments_multi_v2(
    p_queues     TEXT[],
    p_partitions TEXT[],
    p_msg_counts INT[],
    p_metas      TEXT[],
    p_blobs      BYTEA[]
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_n        INT;
    v_now      TIMESTAMPTZ;
    v_results  JSONB[];
    v_missing  INT;
    v_dedup_on INT;
    v_pid      UUID;
    v_rec      RECORD;
    v_res      JSONB;
    v_seq      BIGINT;
    v_inserted INT;
BEGIN
    v_n := COALESCE(array_length(p_queues, 1), 0);
    IF v_n = 0 THEN
        RETURN '[]'::jsonb;
    END IF;

    -- Auto-provision queues + partitions ONLY when something is missing. Steady
    -- state (every partition already exists) skips the three ON CONFLICT probes
    -- entirely — no per-call ShareLock churn on queen.queues (the convoy v1's
    -- single-segment header warns about), just a cheap array-vs-index existence
    -- count. Malformed (empty) queue names are skipped, exactly as v1.
    SELECT count(*) INTO v_missing
    FROM unnest(p_queues, p_partitions) AS u(queue, partition)
    LEFT JOIN queen.seg_queues q ON q.name = u.queue
    LEFT JOIN queen.seg_partitions p ON p.queue_id = q.id AND p.name = u.partition
    WHERE p.id IS NULL;

    IF v_missing > 0 THEN
        INSERT INTO queen.seg_queues (name)
        SELECT DISTINCT queue FROM unnest(p_queues) AS u(queue)
        WHERE COALESCE(queue, '') <> ''
        ON CONFLICT (name) DO NOTHING;

        -- RUSTFIX item 26: create the queen.queues config rows too.
        INSERT INTO queen.queues (name, namespace, task, storage)
        SELECT DISTINCT queue,
               split_part(queue, '.', 1),
               CASE WHEN position('.' in queue) > 0 THEN split_part(queue, '.', 2) ELSE '' END,
               'segments'
        FROM unnest(p_queues) AS u(queue)
        WHERE COALESCE(queue, '') <> ''
        ON CONFLICT (name) DO NOTHING;

        INSERT INTO queen.seg_partitions (queue_id, name)
        SELECT DISTINCT q.id, u.partition
        FROM unnest(p_queues, p_partitions) AS u(queue, partition)
        JOIN queen.seg_queues q ON q.name = u.queue
        ON CONFLICT (queue_id, name) DO NOTHING;
    END IF;

    -- Deadlock-safe pre-lock: take every partition row lock in ascending
    -- seg_partitions.id order BEFORE any write — the one total order every
    -- multi-partition locker uses (seg_transaction_wire_v1, the 026 sweeps).
    FOR v_pid IN
        SELECT p.id
        FROM unnest(p_queues, p_partitions) AS u(queue, partition)
        JOIN queen.seg_queues q ON q.name = u.queue
        JOIN queen.seg_partitions p ON p.queue_id = q.id AND p.name = u.partition
        ORDER BY p.id
    LOOP
        PERFORM 1 FROM queen.seg_partitions WHERE id = v_pid FOR UPDATE;
    END LOOP;

    -- Stamp created_at AFTER the locks are held, so it is monotone per partition
    -- in commit order (v1's PUSHSER invariant): a later pusher to the same
    -- partition can only acquire the row lock after this bundle commits, so it
    -- stamps a strictly later time. One stamp for the bundle is correct because
    -- each partition contributes exactly one segment (disjoint bundle).
    v_now := clock_timestamp();

    -- How many segments target a dedup-ENABLED queue? Zero ⇒ the fast path.
    SELECT count(*) INTO v_dedup_on
    FROM unnest(p_queues) AS u(queue)
    JOIN queen.seg_queues q ON q.name = u.queue
    WHERE q.dedup_window_seconds > 0;

    IF v_dedup_on = 0 THEN
        -- ===================== FAST PATH (no dedup in the bundle) =============
        -- No unique_violation is possible, so no subtransaction is needed. The
        -- whole bundle is one set-based seq bump + one set-based insert. seg is
        -- referenced 3× (bumped, ins, final) so it is materialized once; bumped
        -- and ins are data-modifying CTEs, executed exactly once; new seqs flow
        -- through RETURNING (not a re-read), so the shared snapshot is a non-issue.
        WITH seg AS (
            SELECT u.ord, u.msg_count, u.blob, p.id AS pid
            FROM unnest(p_queues, p_partitions, p_msg_counts, p_blobs)
                 WITH ORDINALITY AS u(queue, partition, msg_count, blob, ord)
            JOIN queen.seg_queues q ON q.name = u.queue
            JOIN queen.seg_partitions p ON p.queue_id = q.id AND p.name = u.partition
        ),
        bumped AS (
            UPDATE queen.seg_partitions p
            SET last_seq = last_seq + 1, last_write_at = v_now
            FROM seg
            WHERE p.id = seg.pid
            RETURNING p.id AS pid, p.last_seq AS seq
        ),
        ins AS (
            INSERT INTO queen.seg_segments (partition_id, seq, created_at, msg_count, blob)
            SELECT seg.pid, b.seq, v_now, seg.msg_count, seg.blob
            FROM seg JOIN bumped b ON b.pid = seg.pid
            RETURNING partition_id AS pid, seq
        )
        SELECT array_agg(r.res ORDER BY r.ord)
        INTO v_results
        FROM (
            SELECT seg.ord AS ord,
                   jsonb_build_object(
                       'status', 'queued',
                       'seq', ins.seq,
                       'createdAt', to_char(v_now, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')) AS res
            FROM ins JOIN seg ON seg.pid = ins.pid
        ) r;

        -- Every input ordinal must resolve to exactly one result (queue/partition
        -- are always non-empty for a real bundle). A short array would be a
        -- silent demux misalignment — fail the whole bundle loudly instead.
        IF COALESCE(array_length(v_results, 1), 0) <> v_n THEN
            RAISE EXCEPTION 'QMULTI resolved % of % segments (empty/unknown queue or partition)',
                COALESCE(array_length(v_results, 1), 0), v_n;
        END IF;

        RETURN to_jsonb(v_results);
    END IF;

    -- ===================== DEDUP / MIXED PATH ===========================
    -- At least one segment can conflict. Process in ascending pid order (locks
    -- already held). A dedup-ENABLED segment runs under its own savepoint so its
    -- unique_violation rolls back only itself; a dedup-OFF segment in the same
    -- bundle can't conflict, so it skips the savepoint. Results are scattered by
    -- input ordinal, so processing order never disturbs alignment.
    v_results := array_fill(NULL::jsonb, ARRAY[v_n]);
    FOR v_rec IN
        SELECT u.ord, u.msg_count, u.blob, u.meta,
               p.id AS pid, q.dedup_window_seconds AS window
        FROM unnest(p_queues, p_partitions, p_msg_counts, p_blobs, p_metas)
             WITH ORDINALITY AS u(queue, partition, msg_count, blob, meta, ord)
        JOIN queen.seg_queues q ON q.name = u.queue
        JOIN queen.seg_partitions p ON p.queue_id = q.id AND p.name = u.partition
        ORDER BY p.id
    LOOP
        IF v_rec.window > 0 THEN
            BEGIN
                UPDATE queen.seg_partitions SET last_seq = last_seq + 1, last_write_at = v_now
                WHERE id = v_rec.pid RETURNING last_seq INTO v_seq;

                WITH ins AS (
                    INSERT INTO queen.seg_dedup (partition_id, txn_hash, seq, frame_idx, message_id, created_at)
                    SELECT v_rec.pid, hashtextextended(m.txn, 0), v_seq, m.i, m.mid, v_now
                    FROM jsonb_to_recordset(v_rec.meta::jsonb) AS m(i INT, mid UUID, txn TEXT)
                    ON CONFLICT (partition_id, txn_hash) DO NOTHING
                    RETURNING 1
                )
                SELECT count(*) INTO v_inserted FROM ins;

                IF v_inserted < v_rec.msg_count THEN
                    RAISE EXCEPTION 'QDUP % of % frames are duplicates', v_rec.msg_count - v_inserted, v_rec.msg_count
                        USING ERRCODE = 'unique_violation';
                END IF;

                INSERT INTO queen.seg_segments (partition_id, seq, created_at, msg_count, blob)
                VALUES (v_rec.pid, v_seq, v_now, v_rec.msg_count, v_rec.blob);

                v_res := jsonb_build_object(
                    'status', 'queued',
                    'seq', v_seq,
                    'createdAt', to_char(v_now, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
            EXCEPTION WHEN unique_violation THEN
                v_res := jsonb_build_object(
                    'status', 'duplicate',
                    'dups', (
                        SELECT COALESCE(
                            jsonb_agg(jsonb_build_object('i', m.i, 'mid', d.message_id)),
                            '[]'::jsonb)
                        FROM jsonb_to_recordset(v_rec.meta::jsonb) AS m(i INT, mid UUID, txn TEXT)
                        JOIN queen.seg_dedup d
                          ON d.partition_id = v_rec.pid
                         AND d.txn_hash = hashtextextended(m.txn, 0)));
            END;
        ELSE
            -- Dedup-off segment in a mixed bundle: no conflict possible, so no
            -- savepoint. (Row already locked by the pre-lock pass.)
            UPDATE queen.seg_partitions SET last_seq = last_seq + 1, last_write_at = v_now
            WHERE id = v_rec.pid RETURNING last_seq INTO v_seq;
            INSERT INTO queen.seg_segments (partition_id, seq, created_at, msg_count, blob)
            VALUES (v_rec.pid, v_seq, v_now, v_rec.msg_count, v_rec.blob);
            v_res := jsonb_build_object(
                'status', 'queued',
                'seq', v_seq,
                'createdAt', to_char(v_now, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
        END IF;
        v_results[v_rec.ord] := v_res;
    END LOOP;

    RETURN to_jsonb(v_results);
END;
$$;
