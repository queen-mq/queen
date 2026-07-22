-- ============================================================================
-- Log engine (18-log-engine.md §7) — ack, lease renewal, DLQ, transaction wire.
--
--   queen.log_dlq                  dead-letter snapshots (offset-addressed)
--   queen.log_ack_v1               positional ack (port of seg_ack_segments_v1)
--   queen.log_ack_by_hash_v1       hash-resolved ack (port of seg_ack_by_txn_v1)
--   queen.log_renew_lease_v1       renew every live lease of a worker
--   queen.log_dlq_head_v1          file the poison frame + advance past it
--   queen.get_dlq_messages_v1      DLQ browsing (broker: db::get_dlq_messages)
--   queen.log_transaction_wire_v1  atomic push+ack (port of seg_transaction_wire_v1)
--
-- Positions are single per-partition BIGINT offsets. The consumer cursor is
-- queen.log_consumers.committed = LAST ACKED offset (next wanted = committed+1);
-- a leased batch is the inclusive span (committed, batch_end]. Everything that
-- was a (seq, frame_idx) tuple comparison in the seg engine is plain BIGINT
-- arithmetic here, and the seg_segments msg_count normalization lookups are
-- gone entirely (there is no "past the last frame" cursor form to normalize).
--
-- The v0.16.0 ack semantics of seg_ack_by_txn_v1 (RUSTFIX items 10 & 11) are
-- preserved EXACTLY — implicit-ack, explicit-signal-never-skipped, below-cursor
-- honesty, single retry budget, forced-DLQ keep-lease handoff — see
-- log_ack_by_hash_v1's header for the full contract restatement.
--
-- Txn→position resolution goes through queen.log_txns (one row per pushed
-- segment, 16B xxh3_128 per frame — ALWAYS written, dedup on or off), so
-- ack-by-hash works on dedup-disabled queues too: the seg engine's
-- "window=0 kills explicit acks" landmine is dead (§12.6).
--
-- Applied idempotently at boot after 041 (schema) / 042 (push, whose
-- log_push_one_v1 is the single allocator code path the transaction wire
-- reuses) / 043 (pop, which creates the leases acked here).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Dead-lettered frames. payload is a SNAPSHOT extracted by the broker (blobs
-- are opaque to SQL); (partition_id, "offset") records where the poison frame
-- lived for tracing, WITHOUT any FK into queen.log_segments — storing the
-- snapshot keeps the DLQ decoupled from segment retention (the rows engine's
-- FK-cascade coupling silently dropped DLQ rows when the source row died).
-- Mirrors the seg-era DLQ table with one BIGINT "offset" replacing (seq, frame_idx).
-- NB "offset" is a reserved word: quoted in DDL/INSERT column lists, but plain
-- qualified references (d.offset) parse fine everywhere else.
-- retry_count: retries consumed when the frame was dead-lettered — a snapshot
-- of the (partition, group) batch_retry_count taken by log_dlq_head_v1 (the
-- old dead_letter_queue.retry_count analogue for the wire's retryCount field).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS queen.log_dlq (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    partition_id UUID NOT NULL,
    consumer_group TEXT NOT NULL,
    "offset" BIGINT NOT NULL,
    message_id UUID,
    transaction_id TEXT,
    payload JSONB,
    error TEXT,
    retry_count INTEGER DEFAULT 0,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_log_dlq_partition_failed_at
    ON queen.log_dlq (partition_id, failed_at DESC);

-- ============================================================================
-- log_ack_v1 — positional ack: advance the cursor to the absolute offset
-- p_upto, validated against the lease and the delivered batch end. Port of
-- seg_ack_segments_v1 (023): p_ok=false releases the lease without advancing
-- (full redelivery); the batch_end clamp rejects positions beyond what was
-- leased. NO segment reads: committed is "last acked", batch_end is inclusive,
-- so there is no msg_count normalization to perform (the seg engine needed a
-- seg_segments PK lookup here; that cost is deleted, not moved).
--
-- RUSTFIX item 11: the worker/expiry lease check runs ONLY when a non-empty
-- p_worker is supplied — a lease-less ack (client sent no leaseId) still
-- advances the cursor, matching 003_ack.sql:52-59 / 004_transaction.sql.
--
-- Faithful port notes: like the seg version, a completing positional ack does
-- NOT reset batch_retry_count / attempt state (only the hash path's
-- reached-end branch does), and the lease is released whether or not p_upto
-- reached batch_end — the caller (broker LeaseRegistry / transaction wire)
-- acks a whole delivered batch in one call.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.log_ack_v1(
    p_queue TEXT,
    p_partition TEXT,
    p_group TEXT,
    p_worker TEXT,
    p_upto BIGINT,
    p_ok BOOLEAN DEFAULT TRUE,
    p_acked_count INTEGER DEFAULT 0
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_pid UUID;
    v_c RECORD;
    v_acked BIGINT := 0;
BEGIN
    SELECT p.id INTO v_pid
    FROM queen.log_partitions p JOIN queen.log_queues q ON q.id = p.queue_id
    WHERE q.name = p_queue AND p.name = p_partition;
    IF v_pid IS NULL THEN
        RETURN jsonb_build_object('ok', false, 'error', 'partition not found');
    END IF;

    -- The (partition, group) consumer row lock is the ONLY lock the ack path
    -- takes (never a log_partitions row lock): acks serialize against pops and
    -- other acks of the same group, and stay entirely out of the push
    -- serializer's way. Multi-ack callers (the transaction wire) take these
    -- locks in ascending (partition_id, group) order.
    SELECT * INTO v_c FROM queen.log_consumers c
    WHERE c.partition_id = v_pid AND c.consumer_group = p_group
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('ok', false, 'error', 'consumer not found');
    END IF;

    -- RUSTFIX item 11: validate the lease only when a non-empty worker/leaseId
    -- is supplied. A lease-less ack falls straight through.
    IF p_worker IS NOT NULL AND p_worker <> ''
       AND (v_c.worker_id IS DISTINCT FROM p_worker
            OR v_c.lease_expires_at IS NULL
            OR v_c.lease_expires_at < clock_timestamp()) THEN
        RETURN jsonb_build_object('ok', false, 'error', 'invalid or expired lease');
    END IF;

    IF p_upto IS NOT NULL AND p_ok THEN
        -- Clamp to the delivered batch end (can't ack beyond what was leased).
        -- A live lease always carries batch_end; guard explicitly so a broken
        -- invariant surfaces as a rejected ack, not a silent skipped clamp.
        IF v_c.batch_end IS NULL THEN
            RETURN jsonb_build_object('ok', false, 'error', 'no leased batch');
        END IF;
        IF p_upto > v_c.batch_end THEN
            RETURN jsonb_build_object('ok', false, 'error', 'position beyond leased batch');
        END IF;

        UPDATE queen.log_consumers SET
            committed = p_upto,
            worker_id = NULL, lease_expires_at = NULL,
            batch_end = NULL,
            total_consumed = total_consumed + GREATEST(p_acked_count, 0)
        WHERE partition_id = v_pid AND consumer_group = p_group;
        v_acked := GREATEST(p_acked_count, 0);
    ELSE
        -- nack / failed batch: release the lease, cursor untouched — the whole
        -- batch redelivers (at-least-once).
        UPDATE queen.log_consumers SET
            worker_id = NULL, lease_expires_at = NULL,
            batch_end = NULL
        WHERE partition_id = v_pid AND consumer_group = p_group;
    END IF;

    RETURN jsonb_build_object('ok', true, 'acked', v_acked);
END;
$$;

-- ============================================================================
-- log_ack_by_hash_v1 — RUSTFIX items 10 & 11: the v0.16.0 ack semantics,
-- ported verbatim from seg_ack_by_txn_v1 (024:474-773) with scalar offsets.
-- The broker maps wire txn strings → xxh3_128 hashes (§3; SQL never hashes)
-- and sends aligned arrays; hashes resolve to absolute offsets through
-- queen.log_txns (+ log_unnest_hashes). The client-observable contract of the
-- rows engine's ack_messages_v2, unchanged:
--
--   * IMPLICIT-ACK: acking position P advances the cursor to the MAX acked-ok
--     position in the leased range — every earlier UNACKED position is below
--     the cursor and is completed, never redelivered. "Ack the last message of
--     the batch" completes the whole batch.
--   * EXPLICIT SIGNALS ARE NEVER SKIPPED (ack-as-commit honesty): the cursor
--     is clamped at the LOWEST explicit failed/dlq/retry position in the same
--     call, even when a later position was acked completed. That lowest signal
--     decides the action (retry charge / DLQ hand-off / budget-free release);
--     completed positions above it simply redeliver (at-least-once duplicates,
--     never a lost nack). Only silent gaps are implicitly completed. At the
--     same offset dlq > failed > retry.
--   * BELOW-CURSOR HONESTY: an ack whose position resolved AT/BELOW committed
--     can have no effect anymore. It is reported back per hash so the broker
--     can answer per item: completed → noop:true (harmless duplicate commit);
--     failed/dlq/retry → rejected ('already committed'). Wire keys:
--     'noopHashes' / 'staleHashes', arrays of 32-char hex strings — the BROKER
--     maps them back to txn strings (it holds the request's txn↔hash list).
--   * RETRY COUNTING: one per-(partition,group) counter, batch_retry_count,
--     charged ONCE per explicit `failed` ack while budget remains, reset on
--     batch completion, NEVER charged on lease expiry or a plain release.
--   * status = 'retry': release the lease, cursor at the completed prefix,
--     counter untouched — budget-free explicit retry.
--   * status = 'dlq': force immediate dead-letter, bypassing remaining budget
--     (broker hand-off via dlq:true, lease KEPT so the broker can decode the
--     leased segment; log_dlq_head_v1 then advances past it + releases).
--   * Exhausted `failed` on a DLQ-enabled queue: dead-letter (RUSTFIX item 2:
--     DLQ defaults TRUE); DLQ-disabled: drop the poison and advance past it.
--
-- RUSTFIX item 11: worker/expiry validation only when p_worker is non-empty.
--
-- Resolution (spec §7): ONE scan of the partition's log_txns window (its
-- steady-state size is O(rate × dedup/txn window), enforced by the txns_start
-- purge), hash-joined against the input array — per input hash we keep
--   eff   = MIN matching offset inside the ackable span
--           [GREATEST(committed+1, txns_start), batch_end]   (lease)
--           (committed, ∞)                                   (lease-less)
--   below = does any match sit at/below committed?
-- MIN picks the ORIGINAL occurrence when a dedup-off queue holds the same
-- hash at several offsets, and "prefer the in-span occurrence" means a re-push
-- below the cursor never shadows the copy the client is actually acking.
-- Unresolvable hashes (txns row purged, or mis-sized hash) count as NOT acked:
-- the cursor stops before their position and those frames redeliver —
-- redelivery over data loss, by design (unchanged from the seg engine).
--
-- delta (total_consumed / 'acked') is pure arithmetic: new_committed -
-- old_committed. The seg engine had to sum msg_counts across segment rows to
-- count frames between two (seq, off) tuples; offsets ARE the frame count.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.log_ack_by_hash_v1(
    p_partition_id UUID,
    p_group TEXT,
    p_worker TEXT,
    p_hashes BYTEA[],
    p_statuses TEXT[]
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_c RECORD;
    v_has_lease BOOLEAN;
    v_txns_start BIGINT := 0;
    v_n INT := COALESCE(array_length(p_hashes, 1), 0);
    v_st TEXT[];
    v_eff BIGINT[];        -- per input ordinal: resolved in-span offset (MIN), NULL = none
    v_below BOOLEAN[];     -- per input ordinal: some occurrence at/below committed
    v_lo BIGINT;           -- ackable-span lower bound
    -- lowest explicit signal (failed/dlq/retry) in the ackable span
    v_sig_off BIGINT;
    v_sig_kind TEXT;
    -- max acked-ok offset (clamped below the signal) → new committed
    v_max_ok BIGINT;
    v_new BIGINT;
    v_delta BIGINT := 0;
    -- nack action state
    v_nack_kind TEXT;      -- 'dlq' | 'failed' | NULL
    v_has_retry BOOLEAN := FALSE;
    v_poison_off BIGINT;
    v_retry_limit INTEGER;
    v_dlq_enabled BOOLEAN;
    v_retry_ct INTEGER;
    v_reached_end BOOLEAN := FALSE;
    -- below-cursor honesty
    v_noop JSONB := '[]'::jsonb;
    v_stale JSONB := '[]'::jsonb;
BEGIN
    IF p_statuses IS NOT NULL
       AND COALESCE(array_length(p_statuses, 1), 0) <> v_n THEN
        -- A silent misalignment would ack the wrong offsets; fail loudly.
        RAISE EXCEPTION 'QACK misaligned arrays: % hashes, % statuses',
            v_n, COALESCE(array_length(p_statuses, 1), 0);
    END IF;

    SELECT * INTO v_c FROM queen.log_consumers c
    WHERE c.partition_id = p_partition_id AND c.consumer_group = p_group
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('ok', false, 'error', 'consumer not found');
    END IF;

    -- RUSTFIX item 11: validate the lease ONLY when a non-empty leaseId/worker
    -- was supplied. A lease-less ack (p_worker = '' or NULL) falls straight
    -- through and still advances the cursor.
    IF p_worker IS NOT NULL AND p_worker <> ''
       AND (v_c.worker_id IS DISTINCT FROM p_worker
            OR v_c.lease_expires_at IS NULL
            OR v_c.lease_expires_at < clock_timestamp()) THEN
        RETURN jsonb_build_object('ok', false, 'error', 'invalid or expired lease');
    END IF;

    v_has_lease := (v_c.batch_end IS NOT NULL);

    -- Plain read (no partition row lock — the ack path must stay out of the
    -- push serializer): the purge watermark bounds the resolvable span.
    SELECT txns_start INTO v_txns_start
    FROM queen.log_partitions WHERE id = p_partition_id;
    v_txns_start := COALESCE(v_txns_start, 0);
    v_lo := GREATEST(v_c.committed + 1, v_txns_start);

    -- Status normalization: explicit signals are exactly failed|retry|dlq;
    -- completed|success|acked|ok (and the empty/absent default) are acks-ok.
    -- Anything else is ignored by every branch (neither ok nor signal) —
    -- identical to the seg engine's IN-list classification.
    IF p_statuses IS NULL THEN
        v_st := array_fill('completed'::text, ARRAY[GREATEST(v_n, 1)]);
    ELSE
        SELECT array_agg(COALESCE(NULLIF(s.s, ''), 'completed') ORDER BY s.ord)
        INTO v_st
        FROM unnest(p_statuses) WITH ORDINALITY AS s(s, ord);
    END IF;

    -- ------------------------------------------------------------ resolution
    -- One pass: scan the partition's log_txns rows once, explode each row's
    -- hash blob, hash-join against the input array (the input side is tiny).
    IF v_n > 0 THEN
        SELECT array_agg(x.eff ORDER BY x.ord),
               array_agg(x.below ORDER BY x.ord)
        INTO v_eff, v_below
        FROM (
            SELECT i.ord,
                   MIN(m.off) FILTER (
                       WHERE m.off >= v_lo
                         AND (NOT v_has_lease OR m.off <= v_c.batch_end)) AS eff,
                   COALESCE(bool_or(m.off <= v_c.committed), false) AS below
            FROM unnest(p_hashes) WITH ORDINALITY AS i(h, ord)
            LEFT JOIN (
                SELECT th.h AS h, t.base_offset + th.idx AS off
                FROM queen.log_txns t
                CROSS JOIN LATERAL queen.log_unnest_hashes(t.hashes) th
                WHERE t.partition_id = p_partition_id
            ) m ON m.h = i.h
            GROUP BY i.ord
        ) x;
    END IF;

    -- ------------------------------------------------------------------ (0)
    -- BELOW-CURSOR HONESTY: hashes whose ONLY resolution sits at/below
    -- committed can no longer have any effect. Report them per hash (hex)
    -- instead of silently swallowing: completed → noop (harmless duplicate
    -- commit); failed/dlq/retry → stale (an explicit signal the store cannot
    -- honor — the broker rejects the item as 'already committed').
    SELECT COALESCE(jsonb_agg(encode(p_hashes[g], 'hex')) FILTER (
               WHERE v_below[g] AND v_eff[g] IS NULL
                 AND v_st[g] IN ('completed', 'success', 'acked', 'ok')), '[]'::jsonb),
           COALESCE(jsonb_agg(encode(p_hashes[g], 'hex')) FILTER (
               WHERE v_below[g] AND v_eff[g] IS NULL
                 AND v_st[g] IN ('failed', 'dlq', 'retry')), '[]'::jsonb)
    INTO v_noop, v_stale
    FROM generate_series(1, v_n) g;

    -- ----------------------------------------------------------------- (0b)
    -- HEAD SIGNAL: the LOWEST explicit failed/dlq/retry offset in the ackable
    -- span. The cursor may never advance past it — an explicit signal is never
    -- implicitly completed by a later acked-ok position in the same call. At
    -- the same offset dlq > failed > retry.
    SELECT v_eff[g], v_st[g]
    INTO v_sig_off, v_sig_kind
    FROM generate_series(1, v_n) g
    WHERE v_eff[g] IS NOT NULL
      AND v_st[g] IN ('failed', 'dlq', 'retry')
    ORDER BY v_eff[g],
             CASE v_st[g] WHEN 'dlq' THEN 0 WHEN 'failed' THEN 1 ELSE 2 END
    LIMIT 1;

    -- ------------------------------------------------------------------ (1)
    -- IMPLICIT-ACK: MAX acked-ok offset in the ackable span, CLAMPED strictly
    -- below the head signal. Silent gaps below the max commit implicitly;
    -- acked-ok positions above the head signal are ignored here and simply
    -- redeliver (at-least-once duplicates, never a lost signal).
    SELECT MAX(v_eff[g])
    INTO v_max_ok
    FROM generate_series(1, v_n) g
    WHERE v_eff[g] IS NOT NULL
      AND v_st[g] IN ('completed', 'success', 'acked', 'ok')
      AND (v_sig_off IS NULL OR v_eff[g] < v_sig_off);

    -- New committed = the max acked-ok offset itself (committed IS the last
    -- acked offset — no "+1 then normalize" step exists in offset land).
    -- No acked-ok positions → cursor unchanged.
    v_new := COALESCE(v_max_ok, v_c.committed);
    -- Frames crossed old→new cursor: pure arithmetic (spec §7). Retention gaps
    -- inside the span were still delivered offsets in this engine's accounting.
    v_delta := GREATEST(v_new - v_c.committed, 0);

    -- ------------------------------------------------------------------ (2)
    -- The head signal decides the action. By construction the new cursor is
    -- clamped below it, so the signal is always ahead of the cursor: the
    -- poison head is exactly the lowest explicit signal.
    IF v_sig_kind IN ('failed', 'dlq') THEN
        v_nack_kind := v_sig_kind;
        v_poison_off := v_sig_off;
    ELSIF v_sig_kind = 'retry' THEN
        v_has_retry := TRUE;
    END IF;

    -- ------------------------------------------------------------------ (3)
    -- Forced DLQ ('dlq' status): dead-letter immediately, bypassing retry
    -- budget. Advance over the completed prefix, KEEP the lease so the broker
    -- can snapshot the poison frame and call log_dlq_head_v1 (which advances
    -- past it + releases). 'off' carries the POISON offset in this return (the
    -- old seq/frameIdx pair), not the cursor.
    IF v_nack_kind = 'dlq' AND v_has_lease THEN
        UPDATE queen.log_consumers SET
            committed = v_new,
            total_consumed = total_consumed + v_delta
        WHERE partition_id = p_partition_id AND consumer_group = p_group;
        RETURN jsonb_build_object(
            'ok', true, 'dlq', true,
            'partitionId', p_partition_id, 'group', p_group, 'worker', p_worker,
            'off', v_poison_off, 'acked', v_delta,
            'noopHashes', v_noop, 'staleHashes', v_stale);
    END IF;

    -- ------------------------------------------------------------------ (4)
    -- Explicit 'failed' nack with a live lease: charge the per-(partition,
    -- group) retry counter and decide redeliver / DLQ / drop.
    IF v_nack_kind = 'failed' AND v_has_lease THEN
        -- RUSTFIX item 2: DLQ defaults to TRUE (unconfigured queue always DLQs).
        SELECT COALESCE(qq.retry_limit, 3),
               COALESCE(qq.dead_letter_queue, true)
               OR COALESCE(qq.dlq_after_max_retries, true)
        INTO v_retry_limit, v_dlq_enabled
        FROM queen.log_partitions lp
        JOIN queen.log_queues lq ON lq.id = lp.queue_id
        LEFT JOIN queen.queues qq ON qq.name = lq.name
        WHERE lp.id = p_partition_id;
        v_retry_limit := COALESCE(v_retry_limit, 3);
        v_dlq_enabled := COALESCE(v_dlq_enabled, true);
        v_retry_ct := COALESCE(v_c.batch_retry_count, 0);

        IF v_retry_ct < v_retry_limit THEN
            -- Budget remains: release the lease so the poison (and everything
            -- after the completed prefix) redelivers; charge the counter ONCE.
            -- Cursor stays at the completed prefix.
            UPDATE queen.log_consumers SET
                committed = v_new,
                worker_id = NULL, lease_expires_at = NULL,
                batch_end = NULL,
                batch_retry_count = v_retry_ct + 1,
                total_consumed = total_consumed + v_delta
            WHERE partition_id = p_partition_id AND consumer_group = p_group;
            RETURN jsonb_build_object('ok', true, 'acked', v_delta, 'off', v_new,
                                      'noopHashes', v_noop, 'staleHashes', v_stale);
        ELSIF v_dlq_enabled THEN
            -- Budget exhausted + DLQ: hand the poison to the broker (keep the
            -- lease). Same return shape as the forced-DLQ branch.
            UPDATE queen.log_consumers SET
                committed = v_new,
                total_consumed = total_consumed + v_delta
            WHERE partition_id = p_partition_id AND consumer_group = p_group;
            RETURN jsonb_build_object(
                'ok', true, 'dlq', true,
                'partitionId', p_partition_id, 'group', p_group, 'worker', p_worker,
                'off', v_poison_off, 'acked', v_delta,
                'noopHashes', v_noop, 'staleHashes', v_stale);
        ELSE
            -- Budget exhausted, no DLQ: DROP the poison — advance the cursor
            -- past it (committed = the poison offset), reset the counter,
            -- release the lease. The poison frame is disposed of, so it counts
            -- as consumed on top of the completed prefix.
            v_new := v_poison_off;
            UPDATE queen.log_consumers SET
                committed = v_new,
                worker_id = NULL, lease_expires_at = NULL,
                batch_end = NULL,
                batch_retry_count = 0,
                total_consumed = total_consumed + v_delta + 1
            WHERE partition_id = p_partition_id AND consumer_group = p_group;
            RETURN jsonb_build_object('ok', true, 'dropped', true,
                'acked', v_delta, 'off', v_new,
                'noopHashes', v_noop, 'staleHashes', v_stale);
        END IF;
    END IF;

    -- ------------------------------------------------------------------ (5)
    -- Explicit 'retry' (no failure): release the lease, cursor at the
    -- completed prefix, counter UNTOUCHED — budget-free retry
    -- (003_ack.sql:193-203 semantics).
    IF v_has_retry AND v_has_lease THEN
        UPDATE queen.log_consumers SET
            committed = v_new,
            worker_id = NULL, lease_expires_at = NULL,
            batch_end = NULL,
            total_consumed = total_consumed + v_delta
        WHERE partition_id = p_partition_id AND consumer_group = p_group;
        RETURN jsonb_build_object('ok', true, 'acked', v_delta, 'off', v_new,
                                  'noopHashes', v_noop, 'staleHashes', v_stale);
    END IF;

    -- ------------------------------------------------------------------ (6)
    -- All completed (or a lease-less ack). Advance the cursor. If it reached
    -- the leased batch end (batch_end is the inclusive last delivered offset —
    -- no msg_count normalization needed), release the lease AND reset the
    -- retry/attempt state (batch complete); otherwise keep the lease so the
    -- rest of the batch can still be acked.
    IF v_has_lease THEN
        v_reached_end := v_new >= v_c.batch_end;
    END IF;

    IF v_has_lease AND v_reached_end THEN
        UPDATE queen.log_consumers SET
            committed = v_new,
            worker_id = NULL, lease_expires_at = NULL,
            batch_end = NULL,
            attempt_offset = NULL, attempt_count = 0,
            batch_retry_count = 0,
            total_consumed = total_consumed + v_delta
        WHERE partition_id = p_partition_id AND consumer_group = p_group;
    ELSE
        UPDATE queen.log_consumers SET
            committed = v_new,
            total_consumed = total_consumed + v_delta
        WHERE partition_id = p_partition_id AND consumer_group = p_group;
    END IF;

    RETURN jsonb_build_object('ok', true, 'acked', v_delta, 'off', v_new,
                              'noopHashes', v_noop, 'staleHashes', v_stale);
END;
$$;

-- ============================================================================
-- log_renew_lease_v1 — renew every live log-engine lease held by this worker
-- (multi-partition pops share the worker id). Port of seg_renew_lease_v1 (023).
-- GREATEST: renewing must never SHORTEN a lease (005_renew_lease.sql parity).
-- 'expiresAt' reports the MIN across the renewed rows: the earliest deadline
-- is what a caller needs to schedule the next renew; NULL when nothing renewed.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.log_renew_lease_v1(p_worker TEXT, p_seconds INTEGER)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_n BIGINT;
    v_exp TIMESTAMPTZ;
BEGIN
    WITH updated AS (
        UPDATE queen.log_consumers
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
-- log_dlq_head_v1 — dead-letter the poison HEAD frame of the leased batch.
-- Port of seg_dlq_head_v1 (025), single offset. Called by the broker after
-- log_ack_by_hash_v1 signalled dlq:true (forced or budget-exhausted): the
-- broker alone can decompress frames, so it extracts (message_id, txn,
-- payload) from the leased segment and passes them as a SNAPSHOT. Under the
-- validated lease this files the queen.log_dlq row (snapshotting the retry
-- budget consumed), advances the cursor past that one frame, resets the
-- attempt/retry state (RUSTFIX item 10: the batch completes via DLQ — the
-- NEXT batch starts with a fresh budget), and releases the lease.
--
-- Calling it twice is safe: the first call released the lease, so the second
-- fails the worker match and returns {ok:false} without side effects.
-- The seg engine's batch_positions branch is retired with the durable map
-- itself (RUSTFIX item 10 removed it from pop) — only the direct path remains.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.log_dlq_head_v1(
    p_partition_id UUID,
    p_group TEXT,
    p_worker TEXT,
    p_off BIGINT,
    p_message_id UUID,
    p_txn TEXT,
    p_payload JSONB,
    p_error TEXT
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_c RECORD;
    v_id UUID;
BEGIN
    SELECT * INTO v_c FROM queen.log_consumers c
    WHERE c.partition_id = p_partition_id AND c.consumer_group = p_group
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('ok', false, 'error', 'consumer not found');
    END IF;
    -- RUSTFIX item 11: validate the lease only when a non-empty worker is supplied.
    IF p_worker IS NOT NULL AND p_worker <> ''
       AND (v_c.worker_id IS DISTINCT FROM p_worker
            OR v_c.lease_expires_at IS NULL
            OR v_c.lease_expires_at < clock_timestamp()) THEN
        RETURN jsonb_build_object('ok', false, 'error', 'invalid or expired lease');
    END IF;

    INSERT INTO queen.log_dlq (partition_id, consumer_group, "offset",
                               message_id, transaction_id, payload, error, retry_count)
    VALUES (p_partition_id, p_group, p_off,
            p_message_id, p_txn, p_payload, p_error,
            COALESCE(v_c.batch_retry_count, 0))
    RETURNING id INTO v_id;

    -- Advance past the poison frame: committed = its offset (the DLQ hand-off
    -- left the cursor at the completed prefix, i.e. at most p_off-1, so this
    -- normally advances by exactly the one disposed frame; GREATEST guards a
    -- misbehaving/stale caller from ever moving the cursor BACKWARD — the seg
    -- engine relied on the same "cursor <= poison" invariant implicitly).
    UPDATE queen.log_consumers SET
        committed = GREATEST(committed, p_off),
        worker_id = NULL, lease_expires_at = NULL,
        batch_end = NULL,
        attempt_offset = NULL, attempt_count = 0,
        batch_retry_count = 0,
        -- the frame is disposed of (moved to the DLQ), count it as consumed
        total_consumed = total_consumed + 1
    WHERE partition_id = p_partition_id AND consumer_group = p_group;

    RETURN jsonb_build_object('ok', true, 'id', v_id);
END;
$$;

-- ============================================================================
-- get_dlq_messages_v1 — DLQ browsing for /api/v1/dlq (broker:
-- db::get_dlq_messages). Port of the 099 (segments-only) redefinition over
-- queen.log_dlq: payload snapshots live in the DLQ row itself (no segment
-- join); retryCount is the snapshot taken at dead-letter time; createdAt: the
-- original enqueue time is not recoverable per frame from an opaque blob →
-- failed_at (the DLQ event time); producerSub: not tracked → null. Same JSON
-- keys as the rows-era 010 version — the wire contract (§11) is unchanged.
-- Defined here (not 047) because it is part of the broker's DLQ workflow:
-- dlq list → re-push → delete the snapshot.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.get_dlq_messages_v1(p_filters JSONB DEFAULT '{}'::jsonb)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_queue TEXT;
    v_consumer_group TEXT;
    v_limit INTEGER;
    v_offset INTEGER;
BEGIN
    v_queue := p_filters->>'queue';
    v_consumer_group := p_filters->>'consumerGroup';
    v_limit := COALESCE((p_filters->>'limit')::integer, 100);
    v_offset := COALESCE((p_filters->>'offset')::integer, 0);

    RETURN (
        SELECT jsonb_build_object(
            'messages', COALESCE(jsonb_agg(t.msg ORDER BY t.failed_at DESC), '[]'::jsonb),
            'pagination', jsonb_build_object(
                'limit', v_limit,
                'offset', v_offset
            )
        )
        FROM (
            SELECT d.failed_at,
                   jsonb_build_object(
                       'id', d.id,
                       'transactionId', d.transaction_id,
                       'partitionId', d.partition_id,
                       'queue', lq.name,
                       'partition', lp.name,
                       'consumerGroup', d.consumer_group,
                       'errorMessage', d.error,
                       'retryCount', COALESCE(d.retry_count, 0),
                       'data', d.payload,
                       'producerSub', NULL,
                       'createdAt', to_char(d.failed_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                       'failedAt', to_char(d.failed_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                   ) AS msg
            FROM queen.log_dlq d
            JOIN queen.log_partitions lp ON lp.id = d.partition_id
            JOIN queen.log_queues lq ON lq.id = lp.queue_id
            WHERE (v_queue IS NULL OR lq.name = v_queue)
              AND (v_consumer_group IS NULL OR d.consumer_group = v_consumer_group)
        ) t
        LIMIT v_limit OFFSET v_offset
    );
END;
$$;

GRANT EXECUTE ON FUNCTION queen.get_dlq_messages_v1(JSONB) TO PUBLIC;

-- ============================================================================
-- log_transaction_wire_v1 — atomic push+ack for log queues (wire form: blobs
-- travel as base64 text, hashes as hex text). Port of seg_transaction_wire_v1
-- (026) onto the log engine's single allocator code path (log_push_one_v1).
--
--   p = {
--     "pushes": [{"queue","partition","count","hashesHex","blobB64",
--                 "verified"?}, ...],
--     "acks":   [  {"partitionId","group","worker","uptoOff","ok","count"}
--                | {"partitionId","group","worker",
--                   "acks":[{"h":"<hex32>","status":...}, ...]} , ...]
--   }
--
--   * hashesHex: 32 hex chars per frame (xxh3_128 big-endian, §3), frame
--     order — decoded once here, then log_push_one_v1 stores/probes bytea.
--   * verified: the broker's dedup-cache watermark (§5); absent/null = -1
--     ("no cache — probe the whole window", always correct).
--   * An ack element with an "acks" array is the hash-resolved form (used when
--     the pop was answered by another broker process — no LeaseRegistry);
--     otherwise the positional form addressed by uptoOff. Hash elements also
--     tolerate the legacy {"ok":bool} in place of "status" (seg parity).
--
-- All-or-nothing by construction: one call = one transaction, every failure
-- path RAISEs, so a duplicate push or a rejected ack rolls back every other
-- operation in the batch:
--   * a duplicate verdict from log_push_one_v1 (which itself wrote NOTHING —
--     probe-before-allocate) is escalated to RAISE 'QDUP ...' with ERRCODE
--     unique_violation, keeping the seg engine's classification contract for
--     callers, and taking every OTHER push of the batch down with it (§1).
--   * ack soft failures (ok:false — invalid or expired lease, position beyond
--     leased batch, ...) escalate to an exception likewise.
--
-- Deadlock discipline (the one total order every multi-partition locker uses):
--   1. set-based provisioning (only when something is missing — no per-call
--      ShareLock churn on queen.queues),
--   2. ONE set-based pre-lock over the bundle's push partitions in ascending
--      log_partitions.id order — shared with log_push_multi_v1 (042) and
--      retention (045),
--   3. pushes (partition row locks already held; pid/window pre-resolved and
--      passed down so log_push_one_v1 never re-resolves — the nested-lookup
--      CPU regression the seg v2 rework eliminated),
--   4. acks in ascending (partition_id, group) order — each ack locks its
--      queen.log_consumers row until commit, so concurrent transactions acking
--      overlapping sets take those locks in one total order. Consumer-row
--      locks are only ever taken AFTER all partition-row locks, so the two
--      lock spaces can never form a cross-space cycle.
--
-- Mixed-engine guard: a queue that exists in queen.queues with
-- storage <> 'segments' may not appear here (its data lives in queen.messages;
-- combining engines in one atomic batch is a broker bug). The /configure
-- contract keeps the value 'segments' for log queues (§11). Queues absent from
-- queen.queues are allowed — the log side provisions lazily like push does.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.log_transaction_wire_v1(p JSONB)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_op JSONB;
    v_rec RECORD;
    v_res JSONB;
    v_pushes JSONB := '[]'::jsonb;
    v_acks JSONB := '[]'::jsonb;
    v_bad TEXT;
    v_missing INT;
    v_push_n INT;
    v_done INT := 0;
    v_queue TEXT;
    v_partition TEXT;
    v_hashes BYTEA[];
    v_statuses TEXT[];
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
    JOIN queen.log_partitions lp ON lp.id = (op->>'partitionId')::uuid
    JOIN queen.log_queues lq ON lq.id = lp.queue_id
    JOIN queen.queues qq ON qq.name = lq.name
    WHERE qq.storage <> 'segments'
    LIMIT 1;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'QTXN queue "%" has storage != ''segments''; mixed-engine transactions are not supported', v_bad;
    END IF;

    -- Set-based provisioning with the steady-state skip (042 pattern): one
    -- cheap existence count; only when something is missing run the three
    -- ON CONFLICT statements (log_queues, the queen.queues config row —
    -- RUSTFIX item 26, storage stays 'segments' per the public /configure
    -- contract — and log_partitions).
    SELECT count(*) INTO v_missing
    FROM jsonb_array_elements(COALESCE(p->'pushes', '[]'::jsonb)) op
    LEFT JOIN queen.log_queues q ON q.name = op->>'queue'
    LEFT JOIN queen.log_partitions pt ON pt.queue_id = q.id AND pt.name = op->>'partition'
    WHERE pt.id IS NULL;

    IF v_missing > 0 THEN
        INSERT INTO queen.log_queues (name)
        SELECT DISTINCT op->>'queue'
        FROM jsonb_array_elements(COALESCE(p->'pushes', '[]'::jsonb)) op
        WHERE COALESCE(op->>'queue', '') <> ''
        ON CONFLICT (name) DO NOTHING;

        INSERT INTO queen.queues (name, namespace, task, storage)
        SELECT DISTINCT op->>'queue',
               split_part(op->>'queue', '.', 1),
               CASE WHEN position('.' in op->>'queue') > 0 THEN split_part(op->>'queue', '.', 2) ELSE '' END,
               'segments'
        FROM jsonb_array_elements(COALESCE(p->'pushes', '[]'::jsonb)) op
        WHERE COALESCE(op->>'queue', '') <> ''
        ON CONFLICT (name) DO NOTHING;

        INSERT INTO queen.log_partitions (queue_id, name)
        SELECT DISTINCT q.id, op->>'partition'
        FROM jsonb_array_elements(COALESCE(p->'pushes', '[]'::jsonb)) op
        JOIN queen.log_queues q ON q.name = op->>'queue'
        ON CONFLICT (queue_id, name) DO NOTHING;
    END IF;

    -- Deadlock-safe pre-lock: ascending log_partitions.id, one statement (the
    -- LockRows node sits above the Sort, so rows lock in ORDER BY order).
    PERFORM 1
    FROM (
        SELECT DISTINCT pt.id
        FROM jsonb_array_elements(COALESCE(p->'pushes', '[]'::jsonb)) op
        JOIN queen.log_queues q ON q.name = op->>'queue'
        JOIN queen.log_partitions pt ON pt.queue_id = q.id AND pt.name = op->>'partition'
    ) ids
    JOIN queen.log_partitions pl ON pl.id = ids.id
    ORDER BY pl.id
    FOR UPDATE OF pl;

    -- ------------------------------------------------------------- pushes
    -- Input order (locks already held, so order is about the result array's
    -- demux contract, not correctness). pid/window resolved once per element.
    v_push_n := jsonb_array_length(COALESCE(p->'pushes', '[]'::jsonb));
    FOR v_rec IN
        SELECT op.value AS op, pt.id AS pid, q.dedup_window_seconds AS window
        FROM jsonb_array_elements(COALESCE(p->'pushes', '[]'::jsonb))
             WITH ORDINALITY AS op(value, ord)
        JOIN queen.log_queues q ON q.name = op.value->>'queue'
        JOIN queen.log_partitions pt ON pt.queue_id = q.id AND pt.name = op.value->>'partition'
        ORDER BY op.ord
    LOOP
        v_res := queen.log_push_one_v1(
            v_rec.op->>'queue',
            v_rec.op->>'partition',
            (v_rec.op->>'count')::integer,
            decode(v_rec.op->>'hashesHex', 'hex'),
            COALESCE((v_rec.op->>'verified')::bigint, -1),
            decode(v_rec.op->>'blobB64', 'base64'),
            v_rec.pid,
            v_rec.window);

        -- A duplicate is a SOFT verdict for plain pushes but a HARD error
        -- inside a transaction: all-or-nothing means the whole batch rolls
        -- back, including pushes already executed above (their writes die with
        -- this exception). ERRCODE unique_violation keeps the caller's
        -- QDUP classification from the seg engine.
        IF v_res->>'status' = 'duplicate' THEN
            RAISE EXCEPTION 'QDUP duplicate messages in queue "%" partition "%"; transaction rolled back',
                v_rec.op->>'queue', v_rec.op->>'partition'
                USING ERRCODE = 'unique_violation';
        END IF;
        v_pushes := v_pushes || jsonb_build_array(v_res);
        v_done := v_done + 1;
    END LOOP;

    -- Every push element must have resolved (an empty/unknown name would have
    -- been silently dropped by the join — a demux misalignment). Fail loudly.
    IF v_done <> v_push_n THEN
        RAISE EXCEPTION 'QTXN resolved % of % pushes (empty or unknown queue/partition)',
            v_done, v_push_n;
    END IF;

    -- --------------------------------------------------------------- acks
    -- Ascending (partition_id, group) execution order — see header. The result
    -- array follows execution order; the broker treats it as opaque except for
    -- top-level "ok" and the dlq:true hand-off entries (partitionId/group/off).
    FOR v_op IN
        SELECT op.value
        FROM jsonb_array_elements(COALESCE(p->'acks', '[]'::jsonb)) op
        ORDER BY (op.value->>'partitionId')::uuid, op.value->>'group'
    LOOP
        IF v_op ? 'acks' THEN
            -- Hash-resolved form → aligned arrays for log_ack_by_hash_v1.
            -- Status normalization mirrors the seg txn path: explicit status
            -- wins, legacy ok:false maps to 'failed', default 'completed'.
            SELECT array_agg(decode(a.value->>'h', 'hex') ORDER BY a.ord),
                   array_agg(COALESCE(NULLIF(a.value->>'status', ''),
                                 CASE WHEN (a.value->>'ok')::boolean IS FALSE
                                      THEN 'failed' ELSE 'completed' END)
                             ORDER BY a.ord)
            INTO v_hashes, v_statuses
            FROM jsonb_array_elements(COALESCE(v_op->'acks', '[]'::jsonb))
                 WITH ORDINALITY AS a(value, ord);

            v_res := queen.log_ack_by_hash_v1(
                (v_op->>'partitionId')::uuid,
                v_op->>'group',
                v_op->>'worker',
                COALESCE(v_hashes, '{}'::bytea[]),
                COALESCE(v_statuses, '{}'::text[]));
        ELSE
            -- Positional form: resolve the partition uuid back to names for
            -- log_ack_v1 (which validates lease + batch_end clamp itself).
            SELECT lq.name, lp.name INTO v_queue, v_partition
            FROM queen.log_partitions lp
            JOIN queen.log_queues lq ON lq.id = lp.queue_id
            WHERE lp.id = (v_op->>'partitionId')::uuid;
            IF v_queue IS NULL THEN
                RAISE EXCEPTION 'QTXN ack references unknown partition %', v_op->>'partitionId';
            END IF;

            v_res := queen.log_ack_v1(
                v_queue,
                v_partition,
                v_op->>'group',
                v_op->>'worker',
                (v_op->>'uptoOff')::bigint,
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
