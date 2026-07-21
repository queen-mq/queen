-- Live probe: streamline seg_push_segments_multi_v2's per-call cost.
-- Change vs shipped v2: the deadlock-safe pre-lock is now ONE set-based ordered
-- statement (FOR UPDATE OF p over the joined+ordered set) instead of a plpgsql
-- FOR-loop issuing one `SELECT ... FOR UPDATE` per partition (K SPI round-trips).
-- Same ascending seg_partitions.id lock order (deadlock-safe); everything else
-- verbatim. Applied directly to the running PG to measure the CPU effect.
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
    v_rec      RECORD;
    v_res      JSONB;
    v_seq      BIGINT;
    v_inserted INT;
BEGIN
    v_n := COALESCE(array_length(p_queues, 1), 0);
    IF v_n = 0 THEN
        RETURN '[]'::jsonb;
    END IF;

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

    -- Deadlock-safe pre-lock in ONE statement: lock every partition row in
    -- ascending seg_partitions.id order (FOR UPDATE OF p, rows fetched in id
    -- order). Replaces the per-partition plpgsql SPI loop.
    PERFORM p.id
    FROM unnest(p_queues, p_partitions) AS u(queue, partition)
    JOIN queen.seg_queues q ON q.name = u.queue
    JOIN queen.seg_partitions p ON p.queue_id = q.id AND p.name = u.partition
    ORDER BY p.id
    FOR UPDATE OF p;

    v_now := clock_timestamp();

    SELECT count(*) INTO v_dedup_on
    FROM unnest(p_queues) AS u(queue)
    JOIN queen.seg_queues q ON q.name = u.queue
    WHERE q.dedup_window_seconds > 0;

    IF v_dedup_on = 0 THEN
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

        IF COALESCE(array_length(v_results, 1), 0) <> v_n THEN
            RAISE EXCEPTION 'QMULTI resolved % of % segments (empty/unknown queue or partition)',
                COALESCE(array_length(v_results, 1), 0), v_n;
        END IF;

        RETURN to_jsonb(v_results);
    END IF;

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

                v_res := jsonb_build_object('status','queued','seq',v_seq,
                    'createdAt', to_char(v_now, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
            EXCEPTION WHEN unique_violation THEN
                v_res := jsonb_build_object('status','duplicate','dups', (
                    SELECT COALESCE(jsonb_agg(jsonb_build_object('i', m.i, 'mid', d.message_id)), '[]'::jsonb)
                    FROM jsonb_to_recordset(v_rec.meta::jsonb) AS m(i INT, mid UUID, txn TEXT)
                    JOIN queen.seg_dedup d ON d.partition_id = v_rec.pid
                     AND d.txn_hash = hashtextextended(m.txn, 0)));
            END;
        ELSE
            UPDATE queen.seg_partitions SET last_seq = last_seq + 1, last_write_at = v_now
            WHERE id = v_rec.pid RETURNING last_seq INTO v_seq;
            INSERT INTO queen.seg_segments (partition_id, seq, created_at, msg_count, blob)
            VALUES (v_rec.pid, v_seq, v_now, v_rec.msg_count, v_rec.blob);
            v_res := jsonb_build_object('status','queued','seq',v_seq,
                'createdAt', to_char(v_now, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
        END IF;
        v_results[v_rec.ord] := v_res;
    END LOOP;

    RETURN to_jsonb(v_results);
END;
$$;
