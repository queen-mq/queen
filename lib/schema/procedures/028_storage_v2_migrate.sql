-- ============================================================================
-- Storage v2 — rows→segments big-bang OFFLINE migration (SQL layer).
-- Applied after 023..027_storage_v2*.sql (lexical boot order). Companion to
-- the `queen-seg migrate` subcommand of the Rust broker: Rust owns the frame
-- codec (frames.rs pack + zstd) and the keyset pagination; SQL owns the
-- dedup hash (hashtextextended stays authoritative), the atomic segment
-- insert at an EXPLICIT seq (bypassing the last_seq allocator), the
-- cursor-translation arithmetic (K in one place), and the DLQ snapshot copy.
--
-- Identity mapping (done by the tool BEFORE these calls): seg_queues /
-- seg_partitions reuse the rows-engine UUIDs (INSERT ... SELECT id,... FROM
-- queen.queues / queen.partitions), so `partition_id` is one identity across
-- the message store (seg_segments) and the coordination layer
-- (partition_consumers, dead_letter_queue) — consumers/dedup/DLQ key off the
-- same UUID with no lookup table.
--
-- All objects are idempotent (CREATE OR REPLACE / CREATE TABLE IF NOT EXISTS);
-- re-running a partition is safe because seq is a deterministic function of
-- the survivor rank (see the plan's Migration section).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Resumability control table. One row per partition; the tool drives each
--    partition through pending → in_progress → done in its own txn
--    (delete-partial → write segments → seed consumers → finalize → mark done),
--    so a crash resumes at the last unfinished partition instead of restarting.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS queen.seg_migration_progress (
    partition_id     UUID PRIMARY KEY,
    status           TEXT NOT NULL DEFAULT 'pending',
    segments_written BIGINT DEFAULT 0,
    consumers_done   BOOLEAN DEFAULT false,
    started_at       TIMESTAMPTZ,
    done_at          TIMESTAMPTZ
);

-- ----------------------------------------------------------------------------
-- 2. Write ONE pre-packed segment at an EXPLICIT seq, then seed its dedup rows.
--
--    The live push path (seg_push_segment_v1) bumps seg_partitions.last_seq to
--    ALLOCATE the seq. Migration must NOT do that: it replays the rows-engine
--    survivor ranks, so the tool computes seq = rank/K + 1 deterministically
--    and inserts the segment at exactly that seq. last_seq is set once, at the
--    end, by seg_migrate_finalize_partition_v1.
--
--    p_metas: [{"i":frame_idx,"mid":uuid,"txn":text,"createdAt":timestamptz,
--              "dedup":bool}, ...] — one element per frame in this segment.
--    Only frames with dedup=true get a seg_dedup row (the tool applies
--    --dedup-scope {window|unconsumed|all} when building p_metas, so dedup
--    stays bounded and we don't rehydrate the whole backlog into seg_dedup).
--    Each dedup row carries the frame's REAL created_at (the per-message
--    timestamp), so the dedup window purge (seg_purge_dedup_v1) ages rows out
--    on the same clock the live engine uses.
--
--    "createdAt" is quoted in the recordset column list so it matches the
--    camelCase JSON key exactly (jsonb_to_recordset key matching is
--    case-sensitive; an unquoted identifier would fold to createdat and miss).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.seg_migrate_write_segment_v1(
    p_partition_id UUID,
    p_seq          BIGINT,
    p_created_at   TIMESTAMPTZ,
    p_metas        JSONB,
    p_blob_b64     TEXT,
    p_msg_count    INT
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
BEGIN
    -- The segment blob (zstd(length-prefixed frames), packed by the broker).
    INSERT INTO queen.seg_segments (partition_id, seq, created_at, msg_count, blob)
    VALUES (p_partition_id, p_seq, p_created_at, p_msg_count,
            decode(p_blob_b64, 'base64'));

    -- Dedup / txn→position resolver rows for the dedup-eligible frames.
    -- ON CONFLICT DO NOTHING: within a partition the first txn wins (matches
    -- the live engine's dedup semantics); a re-run of this partition is a no-op
    -- on already-present hashes.
    INSERT INTO queen.seg_dedup (partition_id, txn_hash, seq, frame_idx, message_id, created_at)
    SELECT p_partition_id, hashtextextended(m.txn, 0), p_seq, m.i, m.mid, m."createdAt"
    FROM jsonb_to_recordset(p_metas)
        AS m(i INT, mid UUID, txn TEXT, "createdAt" TIMESTAMPTZ, dedup BOOLEAN)
    WHERE m.dedup IS TRUE
    ON CONFLICT (partition_id, txn_hash) DO NOTHING;

    RETURN jsonb_build_object('seq', p_seq, 'written', p_msg_count);
END;
$$;

-- ----------------------------------------------------------------------------
-- 3. Cursor seeding: translate a consumed-MESSAGE count into the segment
--    cursor (next_seq, next_off) and UPSERT the (partition, group) consumer
--    row. K (segment frame count) lives ONLY here, so the tool never open-codes
--    the arithmetic.
--
--      next_seq = consumed / K + 1   (segments < next_seq are fully consumed)
--      next_off = consumed % K       (frames [0..next_off) of next_seq consumed)
--      total_consumed = consumed
--
--    GREATEST(K,1) guards against a caller passing 0 frames/segment.
--    The lease / attempt columns are left at their table defaults (no lease,
--    attempt_count 0) — migration produces a clean, unleased cursor.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.seg_migrate_seed_consumer_v1(
    p_partition_id   UUID,
    p_group          TEXT,
    p_consumed_count BIGINT,
    p_seg_frames     INT
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_k        BIGINT := GREATEST(p_seg_frames, 1);
    v_consumed BIGINT := GREATEST(COALESCE(p_consumed_count, 0), 0);
    v_next_seq BIGINT;
    v_next_off INTEGER;
BEGIN
    v_next_seq := v_consumed / v_k + 1;
    v_next_off := (v_consumed % v_k)::INTEGER;

    INSERT INTO queen.seg_consumers
        (partition_id, consumer_group, next_seq, next_off, total_consumed)
    VALUES (p_partition_id, p_group, v_next_seq, v_next_off, v_consumed)
    ON CONFLICT (partition_id, consumer_group) DO UPDATE SET
        next_seq       = EXCLUDED.next_seq,
        next_off       = EXCLUDED.next_off,
        total_consumed = EXCLUDED.total_consumed;

    RETURN jsonb_build_object('next_seq', v_next_seq, 'next_off', v_next_off);
END;
$$;

-- ----------------------------------------------------------------------------
-- 4. Finalize a partition after all its segments are written: publish the
--    write watermark (last_seq — the seq allocator resumes from here for live
--    pushes), reset the retention low-watermark to 1 (nothing swept yet), and
--    stamp last_write_at so wildcard pop treats the partition as freshly
--    written (candidate-selection floor).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.seg_migrate_finalize_partition_v1(
    p_partition_id UUID,
    p_last_seq     BIGINT
) RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE queen.seg_partitions SET
        last_seq      = p_last_seq,
        retention_seq = 1,
        last_write_at = now()
    WHERE id = p_partition_id;
END;
$$;

-- ----------------------------------------------------------------------------
-- 5. Cursor-translation counter (the crux). Given a rows-engine cursor
--    (last_consumed_created_at, last_consumed_id), return C_g = the number of
--    surviving messages at/before that cursor = the rank of the first
--    UNCONSUMED message. The tool feeds C_g into seg_migrate_seed_consumer_v1.
--
--    Retention-correct: deleted messages are neither counted here nor migrated,
--    so survivor ranks stay contiguous and the segment cursor lands exactly
--    where the rows cursor did.
--
--    Rides idx_messages_partition_created via the (created_at, id) row
--    comparison. Handles the rows never-consumed sentinel
--    (last_consumed_id = all-zeros / last_consumed_created_at NULL, enforced by
--    the partition_consumers CHECK) → 0, so a never-consumed group seeds at
--    (next_seq=1, next_off=0) = full backlog pending.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.seg_migrate_consumed_count_v1(
    p_partition_id           UUID,
    p_last_consumed_created_at TIMESTAMPTZ,
    p_last_consumed_id         UUID
) RETURNS BIGINT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_count BIGINT;
BEGIN
    -- never-consumed sentinel: nothing consumed yet.
    IF p_last_consumed_created_at IS NULL
       OR p_last_consumed_id IS NULL
       OR p_last_consumed_id = '00000000-0000-0000-0000-000000000000'::uuid THEN
        RETURN 0;
    END IF;

    SELECT count(*) INTO v_count
    FROM queen.messages
    WHERE partition_id = p_partition_id
      AND (created_at, id) <= (p_last_consumed_created_at, p_last_consumed_id);

    RETURN COALESCE(v_count, 0);
END;
$$;

-- ----------------------------------------------------------------------------
-- 6. DLQ carry-over: snapshot queen.dead_letter_queue into queen.seg_dlq.
--    LEFT JOIN queen.messages by message_id to recover the transaction id and
--    payload SNAPSHOT; if the source message was already retention-deleted the
--    join yields NULL (transaction_id/payload NULL) — the DLQ row is still
--    carried, decoupled from segment retention (seg_dlq has no FK to messages
--    or segments). seq/frame_idx are 0: the poison frame's original segment
--    position does not exist in the rows engine, and browse/list paths key on
--    (transaction_id, error, failed_at), not (seq, frame_idx).
--
--    One-shot global step (not per-partition); run once by the tool after the
--    per-partition passes. Returns the number of DLQ rows carried over.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.seg_migrate_dlq_v1()
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_n BIGINT;
BEGIN
    INSERT INTO queen.seg_dlq
        (partition_id, consumer_group, seq, frame_idx,
         message_id, transaction_id, payload, error, failed_at)
    SELECT d.partition_id,
           d.consumer_group,
           0,                       -- seq: no segment position in the rows engine
           0,                       -- frame_idx
           d.message_id,
           m.transaction_id,        -- NULL if the source message was retention-deleted
           m.payload,               -- payload snapshot; NULL if already deleted
           d.error_message,
           d.failed_at
    FROM queen.dead_letter_queue d
    LEFT JOIN queen.messages m ON m.id = d.message_id;

    GET DIAGNOSTICS v_n = ROW_COUNT;
    RETURN v_n;
END;
$$;
