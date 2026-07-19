-- ============================================================================
-- Storage v2 (segments engine) — schema + procedures.
-- Source of truth: benchmark-queen/storage-v2-spike/schema_v2.sql (validated
-- by its correctness suite + measured head-to-head vs v1; see its README).
-- Applied idempotently at boot like every procedures file. Queues opt in
-- via queen.queues.storage = 'segments' (flag added in schema.sql).
-- ============================================================================
-- ============================================================================
-- Queen storage v2 — greenfield segments engine (spike)
-- ============================================================================
-- Design goals vs v1 (queen.messages):
--   * one row = one SEGMENT of K messages (frames packed + zstd'd by the
--     broker); per-tuple and per-index overhead amortized /K, group
--     compression across similar payloads.
--   * segment identity = (partition_id, seq bigint), seq allocated under the
--     queen.seg_partitions row lock -> per-partition commit-order serialization
--     (replaces the PUSHSER advisory lock) and gap-tolerant total order.
--   * ZERO secondary indexes on the hot table: the PK is also the pop path.
--   * dedup lives in a window-bounded side table (queen.seg_dedup), keyed by
--     (partition_id, hashtextextended(txn)). Probe runs under the partition
--     serializer, so probe-then-insert is race-free within a partition.
--   * consumption is a cursor (next_seq, next_off) per (partition, group):
--     "frames [0..next_off) of segment next_seq consumed; everything below
--     next_seq fully consumed". No per-message state anywhere.
-- ============================================================================

-- storage-v2 objects live in the queen schema (seg_ prefix); no separate q2 schema.

CREATE TABLE IF NOT EXISTS queen.seg_queues (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,
    lease_time INTEGER NOT NULL DEFAULT 300,
    retention_seconds INTEGER NOT NULL DEFAULT 3600,
    dedup_window_seconds INTEGER NOT NULL DEFAULT 3600,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS queen.seg_partitions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_id UUID NOT NULL REFERENCES queen.seg_queues(id) ON DELETE CASCADE,
    name TEXT NOT NULL DEFAULT 'Default',
    -- seq allocator. The UPDATE that bumps it is the per-partition write
    -- serializer: concurrent pushes to the same partition queue up on this
    -- row lock, so seq order == commit order (same guarantee PUSHSER gives
    -- v1, minus the advisory-lock syscalls).
    last_seq BIGINT NOT NULL DEFAULT 0,
    -- retention low-watermark: every segment with seq < retention_seq is
    -- deleted. Lets the retention boundary search start where the previous
    -- sweep stopped instead of walking dead index entries from seq=1 (the
    -- classic queue-table head-scan poison) — and keeps the segments table
    -- at ZERO secondary indexes.
    retention_seq BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (queue_id, name)
);
-- One UPDATE per segment lands on this row; headroom keeps them HOT
-- (last_seq/retention_seq are non-indexed columns) so the two unique
-- indexes stay O(#partitions) instead of O(#updates).
ALTER TABLE queen.seg_partitions SET (fillfactor = 70);
-- last_write_at: bumped on every push (same UPDATE that allocates the seq). Acts
-- as the per-partition write timestamp so wildcard pop can skip partitions that
-- have not received writes since the consumer last drained (the consumer_watermark
-- empty-scan floor), instead of scanning every partition — decisive at 10-20k
-- partitions/queue. last_seq stays the authoritative write watermark for "hot".
ALTER TABLE queen.seg_partitions
    ADD COLUMN IF NOT EXISTS last_write_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
-- Candidate-selection index: range-scan only recently-written partitions of a
-- queue (queue_id, last_write_at) instead of the full partition set.
CREATE INDEX IF NOT EXISTS idx_seg_partitions_queue_write
    ON queen.seg_partitions (queue_id, last_write_at);

CREATE TABLE IF NOT EXISTS queen.seg_segments (
    partition_id UUID NOT NULL REFERENCES queen.seg_partitions(id) ON DELETE CASCADE,
    seq BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    msg_count INTEGER NOT NULL,
    -- zstd(concatenated length-prefixed frames), compressed by the broker.
    -- EXTERNAL: TOAST must not burn CPU re-compressing incompressible bytes.
    blob BYTEA NOT NULL,
    PRIMARY KEY (partition_id, seq)
);
ALTER TABLE queen.seg_segments ALTER COLUMN blob SET STORAGE EXTERNAL;
ALTER TABLE queen.seg_segments SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_insert_scale_factor = 0.05,
    autovacuum_vacuum_cost_limit = 4000,
    autovacuum_vacuum_cost_delay = 0
);

-- Window-bounded dedup + txn->position resolver (for acks-by-txn, DLQ, traces).
-- Purged by queen.seg_purge_dedup_v1; its steady-state size is O(rate x window),
-- independent of retention/backlog.
CREATE TABLE IF NOT EXISTS queen.seg_dedup (
    partition_id UUID NOT NULL,
    txn_hash BIGINT NOT NULL,
    seq BIGINT NOT NULL,
    frame_idx INTEGER NOT NULL,
    message_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (partition_id, txn_hash)
);
ALTER TABLE queen.seg_dedup SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_cost_delay = 0
);

CREATE TABLE IF NOT EXISTS queen.seg_consumers (
    partition_id UUID NOT NULL REFERENCES queen.seg_partitions(id) ON DELETE CASCADE,
    consumer_group TEXT NOT NULL DEFAULT '__QUEUE_MODE__',
    -- Cursor: frames [0..next_off) of segment next_seq are consumed;
    -- all segments with seq < next_seq are fully consumed.
    next_seq BIGINT NOT NULL DEFAULT 1,
    next_off INTEGER NOT NULL DEFAULT 0,
    -- Lease (one in-flight batch per (partition, group), like v1).
    worker_id TEXT,
    lease_expires_at TIMESTAMPTZ,
    -- End position of the leased batch (ack must not advance past it).
    batch_end_seq BIGINT,
    batch_end_off INTEGER,
    total_consumed BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (partition_id, consumer_group)
);
ALTER TABLE queen.seg_consumers SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 50,
    autovacuum_vacuum_cost_delay = 0,
    fillfactor = 50
);
-- Durable per-position ack map for the currently leased batch (Trap 2): the
-- ordered list of delivered positions the pop handed out, one JSON element each
--   {"seq":<bigint>, "off":<frame_idx>, "acked":<bool>, "dlq":<bool>}
-- recorded by the pop in the SAME UPDATE that sets worker_id/lease_expires_at/
-- batch_end_*. Each ack MARKS its position here instead of releasing the lease,
-- and the cursor (next_seq,next_off) advances over the highest CONTIGUOUS
-- acked-OR-dlq prefix. This is what makes out-of-order per-message ack within a
-- single leased batch (.batch(N).each().concurrency(N)) work: the first ack no
-- longer finalizes+releases the lease under the other in-flight acks. The map is
-- durable on the row, so an ack landing on ANY replica resolves by reading it
-- (no in-process LeaseRegistry as the source of truth). NULL when no batch is
-- leased (or after a full resolve / release).
ALTER TABLE queen.seg_consumers ADD COLUMN IF NOT EXISTS batch_positions JSONB;

-- Per-(queue, consumer_group) empty-scan watermark — the segments analogue of
-- queen.consumer_watermarks in the rows engine. `last_empty_scan_at` records when
-- this group last found the queue empty; wildcard pop uses it as a time floor
-- (only consider partitions written since) so a caught-up consumer skips cold
-- partitions instead of re-scanning all of them on every long-poll cycle.
CREATE TABLE IF NOT EXISTS queen.seg_consumer_watermarks (
    queue_name TEXT NOT NULL,
    consumer_group TEXT NOT NULL,
    last_empty_scan_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (queue_name, consumer_group)
);
ALTER TABLE queen.seg_consumer_watermarks SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 50,
    fillfactor = 50
);

-- ============================================================================
-- push: one call = one segment for one partition.
--   p_metas: [{"i":0,"mid":"<uuid>","txn":"<transaction id>"}, ...]
--   p_blob:  zstd(frames), packed by the caller (broker).
-- Returns {"status":"queued","seq":N,"createdAt":...}
--      or {"status":"duplicate","dups":[i,...]} (nothing written; caller
--         repacks without the dup frames and retries — rare path).
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.seg_push_segment_v1(
    p_queue TEXT,
    p_partition TEXT,
    p_metas JSONB,
    p_blob BYTEA,
    p_msg_count INTEGER
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_pid UUID;
    v_seq BIGINT;
    v_now TIMESTAMPTZ;
    v_window INT;
    v_inserted INT;
BEGIN
    -- Ensure queue + partition exist (same contract as v1 push A/B statements).
    SELECT p.id, q.dedup_window_seconds INTO v_pid, v_window
    FROM queen.seg_partitions p JOIN queen.seg_queues q ON q.id = p.queue_id
    WHERE q.name = p_queue AND p.name = p_partition;

    IF v_pid IS NULL THEN
        INSERT INTO queen.seg_queues (name) VALUES (p_queue)
        ON CONFLICT (name) DO NOTHING;
        INSERT INTO queen.seg_partitions (queue_id, name)
        SELECT q.id, p_partition FROM queen.seg_queues q WHERE q.name = p_queue
        ON CONFLICT (queue_id, name) DO NOTHING;
        SELECT p.id, q.dedup_window_seconds INTO v_pid, v_window
        FROM queen.seg_partitions p JOIN queen.seg_queues q ON q.id = p.queue_id
        WHERE q.name = p_queue AND p.name = p_partition;
    END IF;

    -- Serialize the partition + allocate seq (row lock held to commit). Same
    -- UPDATE stamps last_write_at so the pop candidate-select can skip partitions
    -- that have gone quiet (empty-scan floor) — no extra write cost.
    UPDATE queen.seg_partitions SET last_seq = last_seq + 1, last_write_at = clock_timestamp()
    WHERE id = v_pid
    RETURNING last_seq INTO v_seq;

    -- created_at stamped AFTER the serializer: monotonic per partition in
    -- commit order (v1's PUSHSER invariant, needed by time-based retention).
    v_now := clock_timestamp();

    -- Dedup is ON by default (dedup_window_seconds defaults to 3600) and
    -- per-queue OPT-OUT via queen.seg_set_queue_options_v1 (window=0 disables).
    -- When a queue opted out, skipping the probe is free
    -- correctness-preserving work v1 cannot avoid (its UNIQUE index is
    -- unconditional).
    IF v_window > 0 THEN
        -- Single-statement probe+insert: ON CONFLICT counts the survivors.
        -- Any duplicate aborts the whole call (RAISE rolls back seq + dedup
        -- rows: no gap, no partial state); the caller resolves the dup list
        -- with queen.seg_find_dups_v1, repacks and retries — rare path by design.
        WITH ins AS (
            INSERT INTO queen.seg_dedup (partition_id, txn_hash, seq, frame_idx, message_id, created_at)
            SELECT v_pid, hashtextextended(m.txn, 0), v_seq, m.i, m.mid, v_now
            FROM jsonb_to_recordset(p_metas) AS m(i INT, mid UUID, txn TEXT)
            ON CONFLICT (partition_id, txn_hash) DO NOTHING
            RETURNING 1
        )
        SELECT count(*) INTO v_inserted FROM ins;

        IF v_inserted < p_msg_count THEN
            RAISE EXCEPTION 'QDUP % of % frames are duplicates', p_msg_count - v_inserted, p_msg_count
                USING ERRCODE = 'unique_violation';
        END IF;
    END IF;

    INSERT INTO queen.seg_segments (partition_id, seq, created_at, msg_count, blob)
    VALUES (v_pid, v_seq, v_now, p_msg_count, p_blob);

    RETURN jsonb_build_object(
        'status', 'queued',
        'seq', v_seq,
        'createdAt', to_char(v_now, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
    );
END;
$$;

-- Dup-list resolver for the rare QDUP path: returns the frame indices whose
-- transaction ids already sit in the dedup window.
CREATE OR REPLACE FUNCTION queen.seg_find_dups_v1(
    p_queue TEXT, p_partition TEXT, p_metas JSONB
) RETURNS JSONB
LANGUAGE sql STABLE
AS $$
    SELECT COALESCE(jsonb_agg(m.i), '[]'::jsonb)
    FROM jsonb_to_recordset(p_metas) AS m(i INT, mid UUID, txn TEXT)
    JOIN queen.seg_partitions p ON p.name = p_partition
    JOIN queen.seg_queues q ON q.id = p.queue_id AND q.name = p_queue
    JOIN queen.seg_dedup d
      ON d.partition_id = p.id
     AND d.txn_hash = hashtextextended(m.txn, 0);
$$;

-- ============================================================================
-- pop: returns the raw segment rows covering the budget + slicing bounds.
-- The caller (broker) decompresses and slices frames [r_start_off ..
-- r_start_off + r_take) of each row. Acquires the (partition, group) lease
-- unless p_auto_ack.
-- ============================================================================
-- DROP first: later files (025) legitimately change the OUT signature, and
-- CREATE OR REPLACE cannot alter a return type. Boot applies 023 then 025 in
-- one sequence before serving traffic, so the gap is invisible.
DROP FUNCTION IF EXISTS queen.seg_pop_segments_v1(TEXT, TEXT, TEXT, INTEGER, INTEGER, TEXT, BOOLEAN);
CREATE FUNCTION queen.seg_pop_segments_v1(
    p_queue TEXT,
    p_partition TEXT,
    p_group TEXT,
    p_budget INTEGER,
    p_lease_seconds INTEGER,
    p_worker TEXT,
    p_auto_ack BOOLEAN DEFAULT FALSE
) RETURNS TABLE (
    r_seq BIGINT,
    r_start_off INTEGER,
    r_take INTEGER,
    r_msg_count INTEGER,
    r_created_at TIMESTAMPTZ,
    r_blob BYTEA
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_pid UUID;
    v_next_seq BIGINT;
    v_next_off INTEGER;
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
    SELECT p.id INTO v_pid
    FROM queen.seg_partitions p JOIN queen.seg_queues q ON q.id = p.queue_id
    WHERE q.name = p_queue AND p.name = p_partition;
    IF v_pid IS NULL THEN RETURN; END IF;

    -- Claim: skip if another worker holds a live lease (or the row is being
    -- popped concurrently — SKIP LOCKED keeps concurrent pops non-blocking).
    -- NB: we do NOT unconditionally INSERT the consumer row on every pop. An
    -- INSERT ... ON CONFLICT DO NOTHING per pop makes concurrent pops of the same
    -- partition serialize on the inserter's transactionid (ShareLock) — the
    -- convoy that stalled wildcard pop. Claim first; create the row only on the
    -- first pop for this (partition, group).
    SELECT c.next_seq, c.next_off INTO v_next_seq, v_next_off
    FROM queen.seg_consumers c
    WHERE c.partition_id = v_pid AND c.consumer_group = p_group
      AND (c.worker_id IS NULL OR c.lease_expires_at IS NULL OR c.lease_expires_at < v_now)
    FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN
        -- Distinguish "row missing" (create it) from "present but leased/locked"
        -- (skip, non-blocking). Only the missing case touches ON CONFLICT, so the
        -- steady-state hot path never contends.
        PERFORM 1 FROM queen.seg_consumers
        WHERE partition_id = v_pid AND consumer_group = p_group;
        IF FOUND THEN RETURN; END IF;
        INSERT INTO queen.seg_consumers (partition_id, consumer_group)
        VALUES (v_pid, p_group)
        ON CONFLICT DO NOTHING;
        SELECT c.next_seq, c.next_off INTO v_next_seq, v_next_off
        FROM queen.seg_consumers c
        WHERE c.partition_id = v_pid AND c.consumer_group = p_group
          AND (c.worker_id IS NULL OR c.lease_expires_at IS NULL OR c.lease_expires_at < v_now)
        FOR UPDATE SKIP LOCKED;
        IF NOT FOUND THEN RETURN; END IF;
    END IF;

    FOR v_row IN
        SELECT s.seq, s.msg_count, s.created_at, s.blob
        FROM queen.seg_segments s
        WHERE s.partition_id = v_pid AND s.seq >= v_next_seq
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

        v_take := LEAST(v_avail, v_budget - v_taken);

        r_seq        := v_row.seq;
        r_start_off  := v_off;
        r_take       := v_take;
        r_msg_count  := v_row.msg_count;
        r_created_at := v_row.created_at;
        r_blob       := v_row.blob;
        RETURN NEXT;

        v_taken   := v_taken + v_take;
        v_end_seq := v_row.seq;
        v_end_off := v_off + v_take;
        v_end_count := v_row.msg_count;
        EXIT WHEN v_taken >= v_budget;
    END LOOP;

    IF v_taken = 0 THEN RETURN; END IF;

    IF p_auto_ack THEN
        UPDATE queen.seg_consumers SET
            next_seq = CASE WHEN v_end_off >= v_end_count THEN v_end_seq + 1 ELSE v_end_seq END,
            next_off = CASE WHEN v_end_off >= v_end_count THEN 0 ELSE v_end_off END,
            worker_id = NULL, lease_expires_at = NULL,
            batch_end_seq = NULL, batch_end_off = NULL,
            total_consumed = total_consumed + v_taken
        WHERE partition_id = v_pid AND consumer_group = p_group;
    ELSE
        UPDATE queen.seg_consumers SET
            worker_id = p_worker,
            lease_expires_at = v_now + make_interval(secs => p_lease_seconds),
            batch_end_seq = v_end_seq,
            batch_end_off = v_end_off
        WHERE partition_id = v_pid AND consumer_group = p_group;
    END IF;
END;
$$;

-- ============================================================================
-- ack: advance the cursor to an absolute position (upto_seq, upto_off),
-- validated against the lease and the delivered batch end. p_ok=false
-- releases the lease without advancing (full redelivery), or advances to the
-- given mid-batch position first (partial ack), leaving the rest for
-- redelivery.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.seg_ack_segments_v1(
    p_queue TEXT,
    p_partition TEXT,
    p_group TEXT,
    p_worker TEXT,
    p_upto_seq BIGINT,
    p_upto_off INTEGER,
    p_ok BOOLEAN DEFAULT TRUE,
    p_acked_count INTEGER DEFAULT 0
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_pid UUID;
    v_c RECORD;
    v_count INTEGER;
    v_acked BIGINT := 0;
BEGIN
    SELECT p.id INTO v_pid
    FROM queen.seg_partitions p JOIN queen.seg_queues q ON q.id = p.queue_id
    WHERE q.name = p_queue AND p.name = p_partition;
    IF v_pid IS NULL THEN
        RETURN jsonb_build_object('ok', false, 'error', 'partition not found');
    END IF;

    SELECT * INTO v_c FROM queen.seg_consumers c
    WHERE c.partition_id = v_pid AND c.consumer_group = p_group
    FOR UPDATE;
    IF NOT FOUND OR v_c.worker_id IS DISTINCT FROM p_worker
       OR v_c.lease_expires_at IS NULL OR v_c.lease_expires_at < clock_timestamp() THEN
        RETURN jsonb_build_object('ok', false, 'error', 'invalid or expired lease');
    END IF;

    IF p_upto_seq IS NOT NULL AND p_ok THEN
        -- Clamp to the delivered batch end (can't ack beyond what was leased).
        -- Explicit NULL guard: a live lease always carries batch_end_*, but a
        -- row comparison against NULLs would yield NULL (treated as false),
        -- silently skipping the clamp if this invariant ever broke.
        IF v_c.batch_end_seq IS NULL OR v_c.batch_end_off IS NULL THEN
            RETURN jsonb_build_object('ok', false, 'error', 'no leased batch');
        END IF;
        IF (p_upto_seq, p_upto_off) > (v_c.batch_end_seq, v_c.batch_end_off) THEN
            RETURN jsonb_build_object('ok', false, 'error', 'position beyond leased batch');
        END IF;
        SELECT msg_count INTO v_count FROM queen.seg_segments
        WHERE partition_id = v_pid AND seq = p_upto_seq;

        UPDATE queen.seg_consumers SET
            next_seq = CASE WHEN v_count IS NOT NULL AND p_upto_off >= v_count
                            THEN p_upto_seq + 1 ELSE p_upto_seq END,
            next_off = CASE WHEN v_count IS NOT NULL AND p_upto_off >= v_count
                            THEN 0 ELSE p_upto_off END,
            worker_id = NULL, lease_expires_at = NULL,
            batch_end_seq = NULL, batch_end_off = NULL,
            batch_positions = NULL,
            total_consumed = total_consumed + GREATEST(p_acked_count, 0)
        WHERE partition_id = v_pid AND consumer_group = p_group;
        v_acked := GREATEST(p_acked_count, 0);
    ELSE
        -- nack / failed batch: release the lease, cursor untouched.
        UPDATE queen.seg_consumers SET
            worker_id = NULL, lease_expires_at = NULL,
            batch_end_seq = NULL, batch_end_off = NULL,
            batch_positions = NULL
        WHERE partition_id = v_pid AND consumer_group = p_group;
    END IF;

    RETURN jsonb_build_object('ok', true, 'acked', v_acked);
END;
$$;

-- ============================================================================
-- retention: created_at is monotone in seq per partition, so the boundary is
-- a single forward index walk; the delete is a contiguous PK range.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.seg_retention_v1(p_cutoff TIMESTAMPTZ)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_p RECORD;
    v_boundary BIGINT;
    v_deleted BIGINT := 0;
    v_n BIGINT;
BEGIN
    -- The boundary walk starts at the previous sweep's watermark
    -- (retention_seq), never at seq=1: repeated sweeps therefore never
    -- re-scan the dead head of the index.
    -- ORDER BY id: the watermark UPDATE takes partition row locks held to
    -- commit; ascending id is the one total order every multi-partition
    -- locker uses (see 026 queen.seg_retention_sweep_v1 / queen.seg_transaction_wire_v1).
    FOR v_p IN SELECT id, retention_seq FROM queen.seg_partitions ORDER BY id LOOP
        SELECT s.seq INTO v_boundary
        FROM queen.seg_segments s
        WHERE s.partition_id = v_p.id
          AND s.seq >= v_p.retention_seq
          AND s.created_at >= p_cutoff
        ORDER BY s.seq LIMIT 1;

        IF v_boundary IS NULL THEN
            -- everything (if anything) is older than the cutoff
            SELECT max(seq) + 1 INTO v_boundary FROM queen.seg_segments
            WHERE partition_id = v_p.id AND seq >= v_p.retention_seq;
            IF v_boundary IS NULL THEN CONTINUE; END IF;
        END IF;

        DELETE FROM queen.seg_segments
        WHERE partition_id = v_p.id
          AND seq >= v_p.retention_seq AND seq < v_boundary;
        GET DIAGNOSTICS v_n = ROW_COUNT;
        v_deleted := v_deleted + v_n;

        UPDATE queen.seg_partitions SET retention_seq = v_boundary
        WHERE id = v_p.id AND retention_seq < v_boundary;
    END LOOP;
    RETURN v_deleted;
END;
$$;

-- Window purge for dedup (batched; caller loops until 0).
CREATE OR REPLACE FUNCTION queen.seg_purge_dedup_v1(p_cutoff TIMESTAMPTZ, p_limit INTEGER DEFAULT 50000)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_n BIGINT;
BEGIN
    DELETE FROM queen.seg_dedup d
    WHERE ctid IN (
        SELECT ctid FROM queen.seg_dedup
        WHERE created_at < p_cutoff
        LIMIT p_limit
    );
    GET DIAGNOSTICS v_n = ROW_COUNT;
    RETURN v_n;
END;
$$;

-- ============================================================================
-- Wire wrappers for the C++ server (text-mode libpq: blobs travel as base64
-- inside text/jsonb params and results; no binary protocol needed).
-- ============================================================================

CREATE OR REPLACE FUNCTION queen.seg_push_segment_wire_v1(
    p_queue TEXT, p_partition TEXT, p_metas JSONB, p_blob_b64 TEXT, p_msg_count INTEGER
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN queen.seg_push_segment_v1(p_queue, p_partition, p_metas,
                              decode(p_blob_b64, 'base64'), p_msg_count);
EXCEPTION WHEN unique_violation THEN
    RETURN jsonb_build_object(
        'status', 'duplicate',
        'dups', queen.seg_find_dups_v1(p_queue, p_partition, p_metas));
END;
$$;

CREATE OR REPLACE FUNCTION queen.seg_pop_segments_wire_v1(
    p_queue TEXT, p_partition TEXT, p_group TEXT,
    p_budget INTEGER, p_lease_seconds INTEGER, p_worker TEXT, p_auto_ack BOOLEAN
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_segments JSONB;
BEGIN
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'seq', r_seq,
        'startOff', r_start_off,
        'take', r_take,
        'msgCount', r_msg_count,
        'createdAt', to_char(r_created_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
        'blob', encode(r_blob, 'base64')
    )), '[]'::jsonb)
    INTO v_segments
    FROM queen.seg_pop_segments_v1(p_queue, p_partition, p_group,
                            p_budget, p_lease_seconds, p_worker, p_auto_ack);
    RETURN jsonb_build_object('segments', v_segments);
END;
$$;

-- Renews every v2 lease held by this worker (multi-partition pops share the
-- worker id, mirroring queen.renew_lease_v2 semantics).
-- GREATEST: renewing must never SHORTEN a lease (v1 parity —
-- 005_renew_lease.sql:38-47 applies the same rule to partition_consumers).
-- 'expiresAt' reports the MIN across the renewed rows: the earliest deadline
-- is what a caller needs to schedule the next renew (v1's choice as well);
-- NULL when nothing was renewed.
CREATE OR REPLACE FUNCTION queen.seg_renew_lease_v1(p_worker TEXT, p_seconds INTEGER)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_n BIGINT;
    v_exp TIMESTAMPTZ;
BEGIN
    WITH updated AS (
        UPDATE queen.seg_consumers
        SET lease_expires_at = GREATEST(
                lease_expires_at,
                clock_timestamp() + make_interval(secs => GREATEST(p_seconds, 1)))
        WHERE worker_id = p_worker
          AND lease_expires_at IS NOT NULL
          AND lease_expires_at > clock_timestamp()
        RETURNING lease_expires_at
    )
    SELECT COUNT(*), MIN(lease_expires_at) INTO v_n, v_exp FROM updated;
    RETURN jsonb_build_object('renewed', v_n,
        'expiresAt', CASE WHEN v_exp IS NOT NULL
            THEN to_char(v_exp, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END);
END;
$$;

-- ============================================================================
-- Per-queue engine options. Dedup is ON by default (dedup_window_seconds
-- defaults to 3600) and per-queue OPT-OUT: the broker calls this from the
-- configure path (routes/configure.cpp) so a queue can shrink/disable its
-- window (0 = disabled). p_dedup_window NULL = leave the current value (or
-- the column default on first contact) untouched.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.seg_set_queue_options_v1(
    p_queue TEXT,
    p_dedup_window INTEGER DEFAULT NULL
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_window INTEGER;
BEGIN
    INSERT INTO queen.seg_queues (name) VALUES (p_queue)
    ON CONFLICT (name) DO NOTHING;

    IF p_dedup_window IS NOT NULL THEN
        UPDATE queen.seg_queues
        SET dedup_window_seconds = GREATEST(p_dedup_window, 0)
        WHERE name = p_queue;
    END IF;

    SELECT dedup_window_seconds INTO v_window FROM queen.seg_queues WHERE name = p_queue;
    RETURN jsonb_build_object('queue', p_queue, 'dedupWindowSeconds', v_window);
END;
$$;

-- v2 of the wire wrappers (supersede the definitions above at apply time):
-- pop also returns the q2 partition uuid (clients echo partitionId in acks);
-- push's duplicate path returns the ORIGINAL message ids like v1 does.
CREATE OR REPLACE FUNCTION queen.seg_push_segment_wire_v1(
    p_queue TEXT, p_partition TEXT, p_metas JSONB, p_blob_b64 TEXT, p_msg_count INTEGER
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN queen.seg_push_segment_v1(p_queue, p_partition, p_metas,
                              decode(p_blob_b64, 'base64'), p_msg_count);
EXCEPTION WHEN unique_violation THEN
    RETURN jsonb_build_object(
        'status', 'duplicate',
        'dups', (
            SELECT COALESCE(jsonb_agg(jsonb_build_object('i', m.i, 'mid', d.message_id)), '[]'::jsonb)
            FROM jsonb_to_recordset(p_metas) AS m(i INT, mid UUID, txn TEXT)
            JOIN queen.seg_partitions p ON p.name = p_partition
            JOIN queen.seg_queues q ON q.id = p.queue_id AND q.name = p_queue
            JOIN queen.seg_dedup d
              ON d.partition_id = p.id
             AND d.txn_hash = hashtextextended(m.txn, 0)));
END;
$$;

CREATE OR REPLACE FUNCTION queen.seg_pop_segments_wire_v1(
    p_queue TEXT, p_partition TEXT, p_group TEXT,
    p_budget INTEGER, p_lease_seconds INTEGER, p_worker TEXT, p_auto_ack BOOLEAN
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_segments JSONB;
    v_pid UUID;
BEGIN
    SELECT p.id INTO v_pid
    FROM queen.seg_partitions p JOIN queen.seg_queues q ON q.id = p.queue_id
    WHERE q.name = p_queue AND p.name = p_partition;

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'seq', r_seq,
        'startOff', r_start_off,
        'take', r_take,
        'msgCount', r_msg_count,
        'createdAt', to_char(r_created_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
        'blob', encode(r_blob, 'base64')
    )), '[]'::jsonb)
    INTO v_segments
    FROM queen.seg_pop_segments_v1(p_queue, p_partition, p_group,
                            p_budget, p_lease_seconds, p_worker, p_auto_ack);
    RETURN jsonb_build_object('segments', v_segments,
                              'partitionId', COALESCE(v_pid::text, ''));
END;
$$;
