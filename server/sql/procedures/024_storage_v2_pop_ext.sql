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
    -- RUSTFIX item 12: throttle floor + lease-ignoring re-check for the watermark.
    v_verified_at TIMESTAMPTZ;
    v_has_pending BOOLEAN;
    -- RUSTFIX item 13: durable subscription mode written to consumer_groups_metadata.
    v_sub_mode_stored TEXT;
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
    -- RUSTFIX item 13: register the subscription DURABLY in
    -- queen.consumer_groups_metadata (parity with 002d_pop_unified_v4.sql:206-271),
    -- which doubles as the once-per-(queue,group) bulk-seed marker. This fixes
    -- collision (a): the empty-scan path (item 12) still writes consumer_watermarks,
    -- so row-existence there can no longer be the marker. The stored timestamp lets
    -- a LATER-created partition seed from the subscription time on first contact
    -- (seg_pop_segments_v1), instead of last_seq+1 which skips its backlog.
    IF p_group <> '__QUEUE_MODE__'
       AND (COALESCE(p_sub_mode, 'all') = 'new' OR COALESCE(p_sub_from, '') <> '') THEN
        IF p_sub_from <> '' AND p_sub_from <> 'now' THEN
            BEGIN
                v_from_ts := p_sub_from::timestamptz;
            EXCEPTION WHEN OTHERS THEN
                v_from_ts := NULL;  -- unparsable: fall back to registration-time 'new'
            END;
        END IF;
        IF v_from_ts IS NULL THEN
            v_from_ts := v_now; v_sub_mode_stored := 'new';
        ELSE
            v_sub_mode_stored := 'timestamp';
        END IF;

        INSERT INTO queen.consumer_groups_metadata
            (consumer_group, queue_name, partition_name, subscription_mode, subscription_timestamp)
        VALUES (p_group, p_queue, '', v_sub_mode_stored, v_from_ts)
        ON CONFLICT (consumer_group, queue_name, partition_name, namespace, task) DO NOTHING;
        GET DIAGNOSTICS v_first_seen = ROW_COUNT;

        IF v_first_seen > 0 THEN
            IF v_sub_mode_stored = 'timestamp' THEN
                -- timestamp seed: first segment at/after v_from_ts per partition
                -- (created_at monotone in seq -> forward walk from retention_seq),
                -- else last_seq+1 (nothing that recent yet -> future-only cursor).
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
    -- RUSTFIX item 12: also read updated_at as the 30s re-check throttle floor.
    SELECT last_empty_scan_at, updated_at INTO v_watermark, v_verified_at
    FROM queen.consumer_watermarks
    WHERE queue_name = p_queue AND consumer_group = p_group;
    IF v_watermark IS NULL THEN
        v_watermark := '1970-01-01 00:00:00+00'::timestamptz;
        v_verified_at := NULL;
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

    -- Nothing claimable this cycle. RUSTFIX item 12: DO NOT advance the empty-scan
    -- watermark blindly — a partition may have been skipped only because another
    -- worker held its lease, not because it is empty. Throttled to 30s, re-check for
    -- pending data IGNORING the lease filter (parity with 002d_pop_unified_v4:395-426):
    -- advance only if genuinely empty; otherwise just record the verification time so
    -- a lease-held backlog is never stranded once the lease expires with no new push.
    IF v_claimed = 0 THEN
        IF v_verified_at IS NULL OR v_verified_at <= v_now - interval '30 seconds' THEN
            SELECT EXISTS (
                SELECT 1 FROM queen.seg_partitions p
                LEFT JOIN queen.partition_consumers c
                  ON c.partition_id = p.id AND c.consumer_group = p_group
                WHERE p.queue_id = v_qid
                  AND p.last_write_at >= v_watermark - interval '2 minutes'
                  AND (c.partition_id IS NULL OR p.last_seq >= c.next_seq)
            ) INTO v_has_pending;
            IF NOT v_has_pending THEN
                INSERT INTO queen.consumer_watermarks
                    (queue_name, consumer_group, last_empty_scan_at, updated_at)
                VALUES (p_queue, p_group, v_now, v_now)
                ON CONFLICT (queue_name, consumer_group)
                DO UPDATE SET last_empty_scan_at = EXCLUDED.last_empty_scan_at,
                              updated_at = EXCLUDED.updated_at;
            ELSE
                -- Pending backlog exists (possibly lease-held): record the check time
                -- only, keep the floor so the partition stays a candidate.
                INSERT INTO queen.consumer_watermarks
                    (queue_name, consumer_group, last_empty_scan_at, updated_at)
                VALUES (p_queue, p_group, '1970-01-01 00:00:00+00'::timestamptz, v_now)
                ON CONFLICT (queue_name, consumer_group)
                DO UPDATE SET updated_at = EXCLUDED.updated_at;
            END IF;
        END IF;
    END IF;

    RETURN jsonb_build_object('partitions', v_out);
END;
$$;

-- ============================================================================
-- Binary wildcard pop (broker-internal): same claim algorithm as
-- seg_pop_wildcard_wire_v1 above, but segment blobs are returned as a NATIVE
-- bytea[] instead of base64 text inside the JSON — no encode() on the PG side,
-- no whitespace-stripping + base64 decode on the broker side, ~25% fewer bytes
-- on the wire. `meta` carries the partition/segment metadata WITHOUT blobs
-- (and without seq/msgCount, which the broker renderer never reads); `blobs`
-- is flattened in traversal order (partitions in claim order, segments in seq
-- order — both aggregates below share ORDER BY r_seq, keeping them aligned).
-- KEEP THE CLAIM LOGIC IN SYNC with seg_pop_wildcard_wire_v1.
-- ============================================================================
DROP FUNCTION IF EXISTS queen.seg_pop_wildcard_bin_v1(TEXT, TEXT, INTEGER, INTEGER, TEXT, BOOLEAN, INTEGER, TEXT, TEXT);
CREATE FUNCTION queen.seg_pop_wildcard_bin_v1(
    p_queue TEXT,
    p_group TEXT,
    p_budget INTEGER,
    p_lease_seconds INTEGER,
    p_worker TEXT,
    p_auto_ack BOOLEAN,
    p_max_partitions INTEGER,
    p_sub_mode TEXT DEFAULT 'all',
    p_sub_from TEXT DEFAULT ''
) RETURNS TABLE(meta JSONB, blobs BYTEA[])
LANGUAGE plpgsql
AS $$
DECLARE
    v_qid UUID;
    v_p RECORD;
    v_remaining INTEGER := GREATEST(p_budget, 1);
    v_max_parts INTEGER := CASE WHEN p_max_partitions IS NULL OR p_max_partitions <= 0
                                THEN 2147483647 ELSE p_max_partitions END;
    v_claimed INTEGER := 0;
    v_out JSONB := '[]'::jsonb;
    v_segments JSONB;
    v_part_blobs BYTEA[];
    v_all_blobs BYTEA[] := '{}'::bytea[];
    v_taken INTEGER;
    v_now TIMESTAMPTZ := clock_timestamp();
    v_watermark TIMESTAMPTZ;
    v_cand_cap INTEGER;
    v_first_seen INTEGER;
    v_from_ts TIMESTAMPTZ;
    v_verified_at TIMESTAMPTZ;
    v_has_pending BOOLEAN;
    v_sub_mode_stored TEXT;
BEGIN
    SELECT id INTO v_qid FROM queen.seg_queues WHERE name = p_queue;
    IF v_qid IS NULL THEN
        meta := jsonb_build_object('partitions', '[]'::jsonb);
        blobs := '{}'::bytea[];
        RETURN NEXT;
        RETURN;
    END IF;

    -- Bulk subscription bootstrap — identical to seg_pop_wildcard_wire_v1.
    IF p_group <> '__QUEUE_MODE__'
       AND (COALESCE(p_sub_mode, 'all') = 'new' OR COALESCE(p_sub_from, '') <> '') THEN
        IF p_sub_from <> '' AND p_sub_from <> 'now' THEN
            BEGIN
                v_from_ts := p_sub_from::timestamptz;
            EXCEPTION WHEN OTHERS THEN
                v_from_ts := NULL;
            END;
        END IF;
        IF v_from_ts IS NULL THEN
            v_from_ts := v_now; v_sub_mode_stored := 'new';
        ELSE
            v_sub_mode_stored := 'timestamp';
        END IF;

        INSERT INTO queen.consumer_groups_metadata
            (consumer_group, queue_name, partition_name, subscription_mode, subscription_timestamp)
        VALUES (p_group, p_queue, '', v_sub_mode_stored, v_from_ts)
        ON CONFLICT (consumer_group, queue_name, partition_name, namespace, task) DO NOTHING;
        GET DIAGNOSTICS v_first_seen = ROW_COUNT;

        IF v_first_seen > 0 THEN
            IF v_sub_mode_stored = 'timestamp' THEN
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
                INSERT INTO queen.partition_consumers (partition_id, consumer_group, next_seq)
                SELECT p.id, p_group, p.last_seq + 1
                FROM queen.seg_partitions p
                WHERE p.queue_id = v_qid
                ON CONFLICT (partition_id, consumer_group) DO NOTHING;
            END IF;
        END IF;
    END IF;

    SELECT last_empty_scan_at, updated_at INTO v_watermark, v_verified_at
    FROM queen.consumer_watermarks
    WHERE queue_name = p_queue AND consumer_group = p_group;
    IF v_watermark IS NULL THEN
        v_watermark := '1970-01-01 00:00:00+00'::timestamptz;
        v_verified_at := NULL;
    END IF;

    v_cand_cap := LEAST(GREATEST(v_max_parts * 4, 64), 512);

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
        -- Blobs go to the aligned bytea[]; meta keeps only what the broker
        -- renderer reads (startOff/take/createdAt). Both aggregates ORDER BY
        -- r_seq so meta position k maps to v_part_blobs[k].
        SELECT COALESCE(jsonb_agg(jsonb_build_object(
                   'startOff', r_start_off,
                   'take', r_take,
                   'createdAt', to_char(r_created_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
               ) ORDER BY r_seq), '[]'::jsonb),
               COALESCE(SUM(r_take), 0),
               COALESCE(array_agg(r_blob ORDER BY r_seq), '{}'::bytea[])
        INTO v_segments, v_taken, v_part_blobs
        FROM queen.seg_pop_segments_v1(p_queue, v_p.name, p_group,
                                v_remaining, p_lease_seconds, p_worker, p_auto_ack,
                                p_sub_mode, p_sub_from);

        IF v_taken > 0 THEN
            v_out := v_out || jsonb_build_object(
                'partition', v_p.name,
                'partitionId', v_p.id,
                'segments', v_segments);
            v_all_blobs := v_all_blobs || v_part_blobs;
            v_claimed := v_claimed + 1;
            v_remaining := v_remaining - v_taken;
            EXIT WHEN v_remaining <= 0 OR v_claimed >= v_max_parts;
        END IF;
    END LOOP;

    -- Empty-scan watermark maintenance — identical to seg_pop_wildcard_wire_v1.
    IF v_claimed = 0 THEN
        IF v_verified_at IS NULL OR v_verified_at <= v_now - interval '30 seconds' THEN
            SELECT EXISTS (
                SELECT 1 FROM queen.seg_partitions p
                LEFT JOIN queen.partition_consumers c
                  ON c.partition_id = p.id AND c.consumer_group = p_group
                WHERE p.queue_id = v_qid
                  AND p.last_write_at >= v_watermark - interval '2 minutes'
                  AND (c.partition_id IS NULL OR p.last_seq >= c.next_seq)
            ) INTO v_has_pending;
            IF NOT v_has_pending THEN
                INSERT INTO queen.consumer_watermarks
                    (queue_name, consumer_group, last_empty_scan_at, updated_at)
                VALUES (p_queue, p_group, v_now, v_now)
                ON CONFLICT (queue_name, consumer_group)
                DO UPDATE SET last_empty_scan_at = EXCLUDED.last_empty_scan_at,
                              updated_at = EXCLUDED.updated_at;
            ELSE
                INSERT INTO queen.consumer_watermarks
                    (queue_name, consumer_group, last_empty_scan_at, updated_at)
                VALUES (p_queue, p_group, '1970-01-01 00:00:00+00'::timestamptz, v_now)
                ON CONFLICT (queue_name, consumer_group)
                DO UPDATE SET updated_at = EXCLUDED.updated_at;
            END IF;
        END IF;
    END IF;

    meta := jsonb_build_object('partitions', v_out);
    blobs := v_all_blobs;
    RETURN NEXT;
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
-- ============================================================================
-- seg_ack_by_txn_v1 — RUSTFIX items 10 & 11: v0.16.0 ack semantics.
--
-- Restores the exact client-observable contract of the row engine's
-- ack_messages_v2 (003_ack.sql), replacing the Rust-only contiguous-prefix /
-- batch_positions machinery:
--
--   * IMPLICIT-ACK: acking position P advances the cursor to just past the MAX
--     acked-ok position in the leased range — every earlier UNACKED position is
--     below the cursor and is completed, never redelivered. "Ack the last
--     message of the batch" completes the whole batch.
--   * EXPLICIT SIGNALS ARE NEVER SKIPPED (ack-as-commit honesty): the cursor is
--     clamped at the LOWEST explicit failed/dlq/retry position in the same ack
--     call, even when a later position was acked completed. That lowest signal
--     decides the action (retry charge / DLQ hand-off / budget-free release);
--     completed positions above it simply redeliver (at-least-once duplicates,
--     never a lost nack). Only silent gaps are implicitly completed.
--   * BELOW-CURSOR HONESTY: an ack whose position resolved BELOW the committed
--     cursor cannot have any effect anymore. The txn is reported back so the
--     broker can answer per item: completed -> success with noop:true (harmless
--     duplicate commit); failed/dlq/retry -> rejected ('already committed')
--     instead of a silent no-op. Wire: 'noopTxns' / 'staleTxns' arrays on every
--     ok:true return.
--   * RETRY COUNTING: a single per-(partition,group) counter, batch_retry_count,
--     charged ONCE per explicit `failed` ack while budget remains, reset on batch
--     completion, and NEVER charged on lease expiry or a plain lease release.
--   * status = 'retry': release the lease, cursor untouched (past completed),
--     counter untouched — budget-free explicit retry.
--   * status = 'dlq': force immediate dead-letter of that message, bypassing any
--     remaining retry budget (broker hand-off via the dlq:true signal, unchanged).
--   * Exhausted `failed` on a DLQ-enabled queue: dead-letter (item 2 default true);
--     on a DLQ-disabled queue: drop the poison and advance past it.
--
-- Item 11: the worker/expiry lease check runs ONLY when a non-empty p_worker is
-- supplied — a lease-less ack (client sent no leaseId → p_worker='') still
-- advances the cursor, matching 003_ack.sql:52-59.
--
-- p_acks is a JSON array [{"txn":..,"status":..}] where status is
-- completed|success|acked|ok (default) | failed | retry | dlq.
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
    v_has_lease BOOLEAN;
    -- max acked-ok position within the ack range
    v_max_ok_seq BIGINT;
    v_max_ok_off INTEGER;
    -- cursor after implicit-ack of the completed prefix
    v_new_seq BIGINT;
    v_new_off INTEGER;
    v_delta BIGINT := 0;
    v_pc INTEGER;
    -- normalized position just past the leased batch end
    v_bend_seq BIGINT;
    v_bend_off INTEGER;
    v_reached_end BOOLEAN := FALSE;
    -- nack detection
    v_nack_kind TEXT;          -- 'dlq' | 'failed' | NULL
    v_has_retry BOOLEAN := FALSE;
    v_poison_seq BIGINT;
    v_poison_off INTEGER;
    v_retry_limit INTEGER;
    v_dlq_enabled BOOLEAN;
    v_retry_ct INTEGER;
    -- lowest explicit signal (failed/dlq/retry) at/after the old cursor
    v_sig_seq BIGINT;
    v_sig_off INTEGER;
    v_sig_kind TEXT;
    -- below-cursor honesty: txns whose position was already committed
    v_noop_txns JSONB := '[]'::jsonb;
    v_stale_txns JSONB := '[]'::jsonb;
BEGIN
    SELECT * INTO v_c FROM queen.partition_consumers c
    WHERE c.partition_id = p_partition_id AND c.consumer_group = p_group
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('ok', false, 'error', 'consumer not found');
    END IF;

    -- RUSTFIX item 11: validate the lease ONLY when a non-empty leaseId/worker was
    -- supplied. A lease-less ack (p_worker = '' or NULL) falls straight through and
    -- still advances the cursor, matching 003_ack.sql:52-59 / 004_transaction.sql.
    IF p_worker IS NOT NULL AND p_worker <> ''
       AND (v_c.worker_id IS DISTINCT FROM p_worker
            OR v_c.lease_expires_at IS NULL
            OR v_c.lease_expires_at < clock_timestamp()) THEN
        RETURN jsonb_build_object('ok', false, 'error', 'invalid or expired lease');
    END IF;

    v_has_lease := (v_c.batch_end_seq IS NOT NULL AND v_c.batch_end_off IS NOT NULL);

    -- ------------------------------------------------------------------ (0)
    -- BELOW-CURSOR HONESTY: acks that resolve below the committed cursor can no
    -- longer have any effect. Report them back per txn instead of silently
    -- swallowing them: completed -> noop (harmless duplicate commit);
    -- failed/dlq/retry -> stale (an explicit signal the store cannot honor —
    -- the broker rejects the item as 'already committed').
    SELECT COALESCE(jsonb_agg(t.txn) FILTER (
               WHERE t.st IN ('completed', 'success', 'acked', 'ok')), '[]'::jsonb),
           COALESCE(jsonb_agg(t.txn) FILTER (
               WHERE t.st IN ('failed', 'dlq', 'retry')), '[]'::jsonb)
    INTO v_noop_txns, v_stale_txns
    FROM (
        SELECT a.txn,
               COALESCE(a.status, CASE WHEN a.ok IS FALSE THEN 'failed' ELSE 'completed' END) AS st
        FROM jsonb_to_recordset(p_acks) AS a(txn TEXT, status TEXT, ok BOOLEAN)
        JOIN queen.seg_dedup d
          ON d.partition_id = p_partition_id
         AND d.txn_hash = hashtextextended(a.txn, 0)
        WHERE (d.seq, d.frame_idx) < (v_c.next_seq, v_c.next_off)
    ) t;

    -- ------------------------------------------------------------------ (0b)
    -- HEAD SIGNAL: the LOWEST explicit failed/dlq/retry position in the ack
    -- range. The cursor may never advance past it — an explicit signal is never
    -- implicitly completed by a later acked-ok position in the same call. At
    -- the same position dlq > failed > retry.
    SELECT t.seq, t.frame_idx, t.st
    INTO v_sig_seq, v_sig_off, v_sig_kind
    FROM (
        SELECT d.seq, d.frame_idx,
               COALESCE(a.status, CASE WHEN a.ok IS FALSE THEN 'failed' ELSE 'completed' END) AS st
        FROM jsonb_to_recordset(p_acks) AS a(txn TEXT, status TEXT, ok BOOLEAN)
        JOIN queen.seg_dedup d
          ON d.partition_id = p_partition_id
         AND d.txn_hash = hashtextextended(a.txn, 0)
        WHERE (d.seq, d.frame_idx) >= (v_c.next_seq, v_c.next_off)
          AND (NOT v_has_lease
               OR (d.seq, d.frame_idx) <= (v_c.batch_end_seq, v_c.batch_end_off))
    ) t
    WHERE t.st IN ('failed', 'dlq', 'retry')
    ORDER BY t.seq, t.frame_idx,
             CASE t.st WHEN 'dlq' THEN 0 WHEN 'failed' THEN 1 ELSE 2 END
    LIMIT 1;

    -- ------------------------------------------------------------------ (1)
    -- IMPLICIT-ACK: the MAX acked-ok position in the ack range, CLAMPED below
    -- the head signal. Resolved through the dedup window. With a live lease the
    -- batch_end clamps the range; without one (lease-less ack) any position
    -- at/after the cursor counts. Silent gaps below the max still commit
    -- implicitly; acked-ok positions above the head signal are ignored here and
    -- simply redeliver (at-least-once duplicates, never a lost signal).
    SELECT d.seq, d.frame_idx INTO v_max_ok_seq, v_max_ok_off
    FROM jsonb_to_recordset(p_acks) AS a(txn TEXT, status TEXT, ok BOOLEAN)
    JOIN queen.seg_dedup d
      ON d.partition_id = p_partition_id
     AND d.txn_hash = hashtextextended(a.txn, 0)
    WHERE COALESCE(a.status, CASE WHEN a.ok IS FALSE THEN 'failed' ELSE 'completed' END)
          IN ('completed', 'success', 'acked', 'ok')
      AND (d.seq, d.frame_idx) >= (v_c.next_seq, v_c.next_off)
      AND (NOT v_has_lease
           OR (d.seq, d.frame_idx) <= (v_c.batch_end_seq, v_c.batch_end_off))
      AND (v_sig_seq IS NULL
           OR (d.seq, d.frame_idx) < (v_sig_seq, v_sig_off))
    ORDER BY d.seq DESC, d.frame_idx DESC
    LIMIT 1;

    -- New cursor = just past the max acked-ok position (normalized on the segment
    -- boundary). No acked-ok positions -> cursor unchanged.
    IF v_max_ok_seq IS NULL THEN
        v_new_seq := v_c.next_seq; v_new_off := v_c.next_off;
    ELSE
        SELECT msg_count INTO v_pc FROM queen.seg_segments
        WHERE partition_id = p_partition_id AND seq = v_max_ok_seq;
        IF v_pc IS NOT NULL AND v_max_ok_off + 1 >= v_pc THEN
            v_new_seq := v_max_ok_seq + 1; v_new_off := 0;
        ELSE
            v_new_seq := v_max_ok_seq; v_new_off := v_max_ok_off + 1;
        END IF;
    END IF;

    -- Frames crossed old->new cursor (for total_consumed). Retention gaps are
    -- simply absent from seg_segments and don't count.
    SELECT COALESCE(SUM(
             (CASE WHEN s.seq = v_new_seq THEN v_new_off ELSE s.msg_count END)
           - (CASE WHEN s.seq = v_c.next_seq THEN v_c.next_off ELSE 0 END)
           ), 0)
    INTO v_delta
    FROM queen.seg_segments s
    WHERE s.partition_id = p_partition_id
      AND s.seq >= v_c.next_seq AND s.seq <= v_new_seq;
    v_delta := GREATEST(v_delta, 0);

    -- ------------------------------------------------------------------ (2)
    -- The head signal (0b) decides the action. By construction the new cursor
    -- is clamped at or below it, so the signal is always at/after the cursor:
    -- the poison head is exactly the lowest explicit signal.
    IF v_sig_kind IN ('failed', 'dlq') THEN
        v_nack_kind := v_sig_kind;
        v_poison_seq := v_sig_seq;
        v_poison_off := v_sig_off;
    ELSIF v_sig_kind = 'retry' THEN
        v_has_retry := TRUE;
    END IF;

    -- ------------------------------------------------------------------ (3)
    -- Forced DLQ ('dlq' status): dead-letter immediately, bypassing retry budget.
    -- Advance over the completed prefix, KEEP the lease so the broker can snapshot
    -- the poison frame and call seg_dlq_head_v1 (which advances past it + releases).
    IF v_nack_kind = 'dlq' AND v_has_lease THEN
        UPDATE queen.partition_consumers SET
            next_seq = v_new_seq, next_off = v_new_off,
            batch_positions = NULL,
            total_consumed = total_consumed + v_delta
        WHERE partition_id = p_partition_id AND consumer_group = p_group;
        RETURN jsonb_build_object(
            'ok', true, 'dlq', true,
            'partitionId', p_partition_id, 'group', p_group, 'worker', p_worker,
            'seq', v_poison_seq, 'frameIdx', v_poison_off, 'acked', v_delta,
            'noopTxns', v_noop_txns, 'staleTxns', v_stale_txns);
    END IF;

    -- ------------------------------------------------------------------ (4)
    -- Explicit 'failed' nack with a live lease: charge the per-(partition,group)
    -- retry counter and decide redeliver / DLQ / drop.
    IF v_nack_kind = 'failed' AND v_has_lease THEN
        -- RUSTFIX item 2: DLQ defaults to TRUE (unconfigured queue always DLQs).
        SELECT COALESCE(qq.retry_limit, 3),
               COALESCE(qq.dead_letter_queue, true)
               OR COALESCE(qq.dlq_after_max_retries, true)
        INTO v_retry_limit, v_dlq_enabled
        FROM queen.seg_partitions sp
        JOIN queen.seg_queues sq ON sq.id = sp.queue_id
        LEFT JOIN queen.queues qq ON qq.name = sq.name
        WHERE sp.id = p_partition_id;
        v_retry_limit := COALESCE(v_retry_limit, 3);
        v_dlq_enabled := COALESCE(v_dlq_enabled, true);
        v_retry_ct := COALESCE(v_c.batch_retry_count, 0);

        IF v_retry_ct < v_retry_limit THEN
            -- Budget remains: release the lease so the poison (and everything after
            -- the completed prefix) redelivers; charge the counter ONCE. Cursor
            -- stays at the completed prefix.
            UPDATE queen.partition_consumers SET
                next_seq = v_new_seq, next_off = v_new_off,
                worker_id = NULL, lease_expires_at = NULL,
                batch_end_seq = NULL, batch_end_off = NULL,
                batch_positions = NULL,
                batch_retry_count = v_retry_ct + 1,
                total_consumed = total_consumed + v_delta
            WHERE partition_id = p_partition_id AND consumer_group = p_group;
            RETURN jsonb_build_object('ok', true, 'acked', v_delta,
                                      'seq', v_new_seq, 'off', v_new_off,
                                      'noopTxns', v_noop_txns, 'staleTxns', v_stale_txns);
        ELSIF v_dlq_enabled THEN
            -- Budget exhausted + DLQ: hand the poison to the broker (keep the lease).
            UPDATE queen.partition_consumers SET
                next_seq = v_new_seq, next_off = v_new_off,
                batch_positions = NULL,
                total_consumed = total_consumed + v_delta
            WHERE partition_id = p_partition_id AND consumer_group = p_group;
            RETURN jsonb_build_object(
                'ok', true, 'dlq', true,
                'partitionId', p_partition_id, 'group', p_group, 'worker', p_worker,
                'seq', v_poison_seq, 'frameIdx', v_poison_off, 'acked', v_delta,
                'noopTxns', v_noop_txns, 'staleTxns', v_stale_txns);
        ELSE
            -- Budget exhausted, no DLQ: DROP the poison — advance the cursor past it
            -- (normalized), reset the counter, release the lease.
            SELECT msg_count INTO v_pc FROM queen.seg_segments
            WHERE partition_id = p_partition_id AND seq = v_poison_seq;
            IF v_pc IS NOT NULL AND v_poison_off + 1 >= v_pc THEN
                v_new_seq := v_poison_seq + 1; v_new_off := 0;
            ELSE
                v_new_seq := v_poison_seq; v_new_off := v_poison_off + 1;
            END IF;
            UPDATE queen.partition_consumers SET
                next_seq = v_new_seq, next_off = v_new_off,
                worker_id = NULL, lease_expires_at = NULL,
                batch_end_seq = NULL, batch_end_off = NULL,
                batch_positions = NULL,
                batch_retry_count = 0,
                total_consumed = total_consumed + v_delta + 1
            WHERE partition_id = p_partition_id AND consumer_group = p_group;
            RETURN jsonb_build_object('ok', true, 'dropped', true,
                'acked', v_delta, 'seq', v_new_seq, 'off', v_new_off,
                'noopTxns', v_noop_txns, 'staleTxns', v_stale_txns);
        END IF;
    END IF;

    -- ------------------------------------------------------------------ (5)
    -- Explicit 'retry' (no failure): release the lease, cursor at the completed
    -- prefix, counter UNTOUCHED — budget-free retry (003_ack.sql:193-203).
    IF v_has_retry AND v_has_lease THEN
        UPDATE queen.partition_consumers SET
            next_seq = v_new_seq, next_off = v_new_off,
            worker_id = NULL, lease_expires_at = NULL,
            batch_end_seq = NULL, batch_end_off = NULL,
            batch_positions = NULL,
            total_consumed = total_consumed + v_delta
        WHERE partition_id = p_partition_id AND consumer_group = p_group;
        RETURN jsonb_build_object('ok', true, 'acked', v_delta,
                                  'seq', v_new_seq, 'off', v_new_off,
                                  'noopTxns', v_noop_txns, 'staleTxns', v_stale_txns);
    END IF;

    -- ------------------------------------------------------------------ (6)
    -- All completed (or a lease-less ack). Advance the cursor. If it reached the
    -- leased batch end, release the lease AND reset the retry counter (batch
    -- complete); otherwise keep the lease so the rest of the batch can be acked.
    IF v_has_lease THEN
        SELECT msg_count INTO v_pc FROM queen.seg_segments
        WHERE partition_id = p_partition_id AND seq = v_c.batch_end_seq;
        IF v_pc IS NOT NULL AND v_c.batch_end_off >= v_pc THEN
            v_bend_seq := v_c.batch_end_seq + 1; v_bend_off := 0;
        ELSE
            v_bend_seq := v_c.batch_end_seq; v_bend_off := v_c.batch_end_off;
        END IF;
        v_reached_end := (v_new_seq, v_new_off) >= (v_bend_seq, v_bend_off);
    END IF;

    IF v_has_lease AND v_reached_end THEN
        UPDATE queen.partition_consumers SET
            next_seq = v_new_seq, next_off = v_new_off,
            worker_id = NULL, lease_expires_at = NULL,
            batch_end_seq = NULL, batch_end_off = NULL,
            batch_positions = NULL,
            attempt_seq = NULL, attempt_off = NULL, attempt_count = 0,
            batch_retry_count = 0,
            total_consumed = total_consumed + v_delta
        WHERE partition_id = p_partition_id AND consumer_group = p_group;
    ELSE
        UPDATE queen.partition_consumers SET
            next_seq = v_new_seq, next_off = v_new_off,
            total_consumed = total_consumed + v_delta
        WHERE partition_id = p_partition_id AND consumer_group = p_group;
    END IF;

    RETURN jsonb_build_object('ok', true, 'acked', v_delta,
                              'seq', v_new_seq, 'off', v_new_off,
                              'noopTxns', v_noop_txns, 'staleTxns', v_stale_txns);
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
