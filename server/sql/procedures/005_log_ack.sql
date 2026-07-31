-- ============================================================================
-- Log engine (18-log-engine.md §7) — ack, lease renewal, DLQ, transaction wire.
--
--   queen.log_dlq                  dead-letter snapshots (offset-addressed)
--   queen.log_ack_v1               positional ack (port of seg_ack_segments_v1)
--   queen.log_ack_by_hash_v1       hash-resolved ack (port of seg_ack_by_txn_v1)
--   queen.log_renew_lease_v1       renew every live lease of a worker
--   queen.log_dlq_head_v1          file the poison frame + advance past it
--   queen.log_transaction_wire_v1  atomic push+ack (port of seg_transaction_wire_v1)
--
-- get_dlq_messages_v1 used to be defined here too; it is gone. 010_log_admin.sql
-- applies AFTER this file and CREATE OR REPLACEs the same (JSONB) signature, so
-- this copy was overwritten at every boot and never ran. 010_log_admin owns it
-- (and its GRANT) now.
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
-- Applied idempotently at boot after 001_log_schema (schema) / 003_log_push
-- (push, whose log_push_one_v1 is the single allocator code path the
-- transaction wire reuses) / 004_log_pop (pop, which creates the leases acked
-- here).
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
-- the retired seg_ack_segments_v1: p_ok=false releases the lease without advancing
-- (full redelivery); the batch_end clamp rejects positions beyond what was
-- leased. NO segment reads: committed is "last acked", batch_end is inclusive,
-- so there is no msg_count normalization to perform (the seg engine needed a
-- seg_segments PK lookup here; that cost is deleted, not moved).
--
-- RUSTFIX item 11: the worker/expiry lease check runs ONLY when a non-empty
-- p_worker is supplied — a lease-less ack (client sent no leaseId) still
-- advances the cursor, matching the retired rows-era ack and transaction
-- procedures.
--
-- Faithful port notes: like the seg version, a completing positional ack does
-- NOT reset batch_retry_count / attempt state (only the hash path's
-- reached-end branch does), and the lease is released whether or not p_upto
-- reached batch_end — the caller (broker LeaseRegistry / transaction wire)
-- acks a whole delivered batch in one call.
-- ============================================================================
-- Track B (§5): p_tenant scopes the (queue,partition) resolution. Added last
-- with a DEFAULT.
CREATE OR REPLACE FUNCTION queen.log_ack_v1(
    p_queue TEXT,
    p_partition TEXT,
    p_group TEXT,
    p_worker TEXT,
    p_upto BIGINT,
    p_ok BOOLEAN DEFAULT TRUE,
    p_acked_count INTEGER DEFAULT 0,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001'
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_pid UUID;
    v_c RECORD;
    v_acked BIGINT := 0;
BEGIN
    -- Queue identity is the queen.queues id now (log_partitions.queue_id FKs it
    -- directly; queen.log_queues is gone).
    SELECT p.id INTO v_pid
    FROM queen.log_partitions p JOIN queen.queues q ON q.id = p.queue_id
    WHERE q.name = p_queue AND p.name = p_partition AND q.tenant_id = p_tenant;
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
-- log_ack_at_v1 — partition-id-addressed twin of log_ack_v1, for the broker's
-- ACK REGISTRY fast path (server/src/ack_registry.rs). The registry, the ack
-- wire, and log_ack_by_hash_v1 are ALL partition_id-keyed; the ack handler
-- groups by (partition_id, worker) and never carries the partition NAME (nor,
-- for a discovery pop, the queue name). So the fast path advances the cursor by
-- pid directly instead of forcing a (queue, partition) name→id JOIN per ack —
-- which would just reintroduce per-ack PG work the fast path exists to remove.
--
-- The body is IDENTICAL to log_ack_v1 from the consumer row lock onward — same
-- lease validation (RUSTFIX item 11: only when p_worker is non-empty), same
-- clamp to batch_end, same positional commit / lease release / counters. The
-- ONLY difference is that v_pid is the supplied argument rather than resolved
-- from names. So it is provably the same decision procedure; the fast path can
-- ONLY reach here with p_ok=true and p_upto=batch_end (full-batch complete),
-- and PG still re-validates the lease under the consumer lock, so a stale
-- registry HIT for an expired/reassigned lease is rejected here exactly as the
-- hash path would reject it (the caller then falls back to log_ack_by_hash_v1).
CREATE OR REPLACE FUNCTION queen.log_ack_at_v1(
    p_partition_id UUID,
    p_group TEXT,
    p_worker TEXT,
    p_upto BIGINT,
    p_ok BOOLEAN DEFAULT TRUE,
    p_acked_count INTEGER DEFAULT 0
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_c RECORD;
    v_acked BIGINT := 0;
BEGIN
    -- Same (partition, group) consumer row lock as log_ack_v1 — the ONLY lock
    -- the ack path takes. A missing row means the partition/group is unknown or
    -- deleted; the caller falls back to the hash path (which reports likewise).
    SELECT * INTO v_c FROM queen.log_consumers c
    WHERE c.partition_id = p_partition_id AND c.consumer_group = p_group
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('ok', false, 'error', 'consumer not found');
    END IF;

    -- RUSTFIX item 11: validate the lease only when a non-empty leaseId is
    -- supplied. The registry always supplies the pop's worker, so this is the
    -- authoritative expiry/ownership check behind the in-memory HIT.
    IF p_worker IS NOT NULL AND p_worker <> ''
       AND (v_c.worker_id IS DISTINCT FROM p_worker
            OR v_c.lease_expires_at IS NULL
            OR v_c.lease_expires_at < clock_timestamp()) THEN
        RETURN jsonb_build_object('ok', false, 'error', 'invalid or expired lease');
    END IF;

    IF p_upto IS NOT NULL AND p_ok THEN
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
        WHERE partition_id = p_partition_id AND consumer_group = p_group;
        v_acked := GREATEST(p_acked_count, 0);
    ELSE
        UPDATE queen.log_consumers SET
            worker_id = NULL, lease_expires_at = NULL,
            batch_end = NULL
        WHERE partition_id = p_partition_id AND consumer_group = p_group;
    END IF;

    RETURN jsonb_build_object('ok', true, 'acked', v_acked);
END;
$$;

-- ============================================================================
-- log_ack_multi_v1 — set-based twin of log_ack_at_v1 for the broker's ACK
-- FUSION path (server/src/ack_fusion.rs). N positional full-batch cursor
-- advances committed in ONE transaction (one commit / one fsync) instead of N
-- separate log_ack_at_v1 calls. This is the ack-side analogue of
-- log_push_multi_v1 (003_log_push): the whole point is to collapse the ack commit rate
-- so the push+lease commit pipeline stops queueing behind a flood of tiny ack
-- fsyncs.
--
-- Each input row (p_pids[i], p_groups[i], p_ends[i], p_workers[i], p_counts[i])
-- runs the IDENTICAL per-row decision procedure as log_ack_at_v1 with p_ok=true:
-- lock the (partition, group) consumer row, validate the worker/lease exactly as
-- RUSTFIX item 11 (only when the worker is non-empty — the fusion path always
-- supplies the pop's worker), clamp p_end to the leased batch_end, and on success
-- advance committed = p_end, release the lease, bump total_consumed. The fusion
-- path only ever enqueues FULL-BATCH COMPLETED acks (p_ok is implicitly true and
-- p_end == the leased batch_end), so this function has no nack/dlq/retry branch —
-- a stale/rejected row simply reports ok:false and the broker falls back to the
-- unchanged log_ack_by_hash_v1 path for that ONE client (redelivery contract
-- unchanged). PG re-validating the lease under the consumer lock means a stale
-- fusion enqueue (lease expired/reassigned between pop and ack) is rejected here
-- exactly as log_ack_at_v1 / log_ack_by_hash_v1 would reject it.
--
-- INVARIANT — deterministic lock order. The rows are processed ORDER BY
-- (partition_id, consumer_group), so every multi-ack flush takes the
-- log_consumers row locks in ONE total order — the SAME discipline as the push
-- pre-lock (003_log_push) and the transaction wire's ack loop (log_transaction_wire_v1
-- step 4). Ack flushes are sharded by partition broker-side so two flushes never
-- share a consumer row, but this ORDER BY also keeps a flush from deadlocking
-- against the transaction wire or a pop that touches the same rows. The ack path
-- takes ONLY the consumer row lock (never a log_partitions row lock), so it can
-- never form a cross-space cycle with the push serializer.
--
-- INVARIANT — result realignment. The result array is emitted BY INPUT ORDINAL
-- (v_out[u.ord]) even though rows execute in (pid, group) order, so the caller
-- realigns each verdict to its enqueue slot: results[i] belongs to input row i.
-- Per-row verdict: {"ok":bool, "acked":bigint, "error":text|null}.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.log_ack_multi_v1(
    p_pids TEXT[],       -- partition uuids as text ($::uuid inside — broker binds &str)
    p_groups TEXT[],
    p_ends BIGINT[],
    p_workers TEXT[],
    p_counts INT[]
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_n INT := COALESCE(array_length(p_pids, 1), 0);
    v_rec RECORD;
    v_c RECORD;
    v_pid UUID;
    v_out JSONB[];
    v_ok BOOLEAN;
    v_err TEXT;
    v_acked BIGINT;
BEGIN
    -- Array-length agreement guard: a silent misalignment would advance the
    -- WRONG cursor. Fail loudly (mirrors log_ack_by_hash_v1's misalign RAISE).
    IF COALESCE(array_length(p_groups, 1), 0) <> v_n
       OR COALESCE(array_length(p_ends, 1), 0) <> v_n
       OR COALESCE(array_length(p_workers, 1), 0) <> v_n
       OR COALESCE(array_length(p_counts, 1), 0) <> v_n THEN
        RAISE EXCEPTION 'QACKM misaligned arrays: pids=% groups=% ends=% workers=% counts=%',
            v_n, COALESCE(array_length(p_groups, 1), 0), COALESCE(array_length(p_ends, 1), 0),
            COALESCE(array_length(p_workers, 1), 0), COALESCE(array_length(p_counts, 1), 0);
    END IF;

    IF v_n = 0 THEN
        RETURN jsonb_build_object('ok', true, 'results', '[]'::jsonb);
    END IF;

    -- Preallocate so verdicts can be written by ORIGINAL ordinal, out of the
    -- (pid, group) execution order.
    v_out := array_fill('null'::jsonb, ARRAY[v_n]);

    FOR v_rec IN
        SELECT u.pid, u.grp, u.end_off, u.worker, u.cnt, u.ord
        FROM unnest(p_pids, p_groups, p_ends, p_workers, p_counts)
             WITH ORDINALITY AS u(pid, grp, end_off, worker, cnt, ord)
        ORDER BY u.pid::uuid, u.grp    -- deterministic lock order (see header)
    LOOP
        v_pid := v_rec.pid::uuid;
        v_ok := false; v_err := NULL; v_acked := 0;

        -- Per-row: identical to log_ack_at_v1 (p_ok=true branch). The consumer
        -- row lock is the ONLY lock taken; re-entrant if two rows in THIS call
        -- share (pid, group) with different workers (the stale worker is then
        -- rejected by the lease check below — one holder per consumer row).
        SELECT * INTO v_c FROM queen.log_consumers c
        WHERE c.partition_id = v_pid AND c.consumer_group = v_rec.grp
        FOR UPDATE;
        IF NOT FOUND THEN
            v_err := 'consumer not found';
        ELSIF v_rec.worker IS NOT NULL AND v_rec.worker <> ''
              AND (v_c.worker_id IS DISTINCT FROM v_rec.worker
                   OR v_c.lease_expires_at IS NULL
                   OR v_c.lease_expires_at < clock_timestamp()) THEN
            v_err := 'invalid or expired lease';
        ELSIF v_c.batch_end IS NULL THEN
            v_err := 'no leased batch';
        ELSIF v_rec.end_off > v_c.batch_end THEN
            v_err := 'position beyond leased batch';
        ELSE
            UPDATE queen.log_consumers SET
                committed = v_rec.end_off,
                worker_id = NULL, lease_expires_at = NULL,
                batch_end = NULL,
                total_consumed = total_consumed + GREATEST(v_rec.cnt, 0)
            WHERE partition_id = v_pid AND consumer_group = v_rec.grp;
            v_ok := true;
            v_acked := GREATEST(v_rec.cnt, 0);
        END IF;

        v_out[v_rec.ord] := jsonb_build_object('ok', v_ok, 'acked', v_acked, 'error', v_err);
    END LOOP;

    RETURN jsonb_build_object('ok', true, 'results', to_jsonb(v_out));
END;
$$;

-- ============================================================================
-- log_ack_by_hash_v1 — RUSTFIX items 10 & 11: the v0.16.0 ack semantics,
-- ported verbatim from the retired seg_ack_by_txn_v1 with scalar offsets.
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
-- Resolution (spec §7): ONE join. A single CTE pipeline unnests the incoming
-- hashes once (carrying status + array position) and joins them ONCE against
-- the partition's log_txns rows in the ackable span, materializing a resolved
-- set (in_idx, hash, status, eff, below) from which EVERY downstream decision
-- is derived with scalar aggregates — no further log_txns hash scans. The join
-- reads only rows with base_offset <= batch_end (under a lease): a row above
-- the leased batch cannot resolve to an in-span offset (off <= batch_end) nor
-- to a below-cursor one (off <= committed < batch_end), so bounding it out is
-- result-preserving AND it is the whole optimization — an ack at the head no
-- longer explodes the entire partition's hash blobs, only the batch's. The
-- below-cursor window [txns_start, committed] lives inside [.., batch_end], so
-- noop/stale still resolve from the same single scan; a lease-less ack
-- (batch_end NULL) keeps the unbounded window. Per input hash we keep
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
-- 2026-07-30: they are additionally reported back in 'unresolvedHashes'
-- (same 32-hex tokens as noop/stale) so the broker rejects those items
-- explicitly — previously they appeared in NO list and the broker's
-- default-success fill told the client the ack landed while the cursor
-- never moved (silent redelivery livelock; a failed/dlq signal there
-- vanished with no retry charge and no DLQ hand-off).
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
    -- unresolvable honesty (2026-07-30): hashes with NO occurrence anywhere
    v_unresolved JSONB := '[]'::jsonb;
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
    -- ONE join, ONE materialized resolved set. Downstream (below-cursor
    -- honesty, head signal, implicit-ack max-ok, delta) is scalar over it.
    --   input    — the incoming hashes unnested once, with status + position.
    --   occ      — log_txns hash occurrences in the ackable span, exploded
    --              ONCE. Bounded to base_offset <= batch_end under a lease
    --              (rows above the batch resolve to nothing — see header);
    --              lease-less keeps the full window.
    --   resolved — per input hash: eff (MIN in-span offset) + below (any
    --              occurrence at/below committed).
    --   sig      — the head signal (lowest failed/dlq/retry offset, tie
    --              dlq>failed>retry), evaluated once and reused for the max-ok
    --              clamp.
    -- The final aggregate emits every scalar the branches need in one shot:
    -- sig_off/sig_kind, noopHashes/staleHashes (input order), max-ok.
    IF v_n > 0 THEN
        WITH input(in_idx, hash, status) AS (
            SELECT u.ord, u.h, u.st
            FROM unnest(p_hashes, v_st) WITH ORDINALITY AS u(h, st, ord)
        ),
        occ(h, voff) AS (
            SELECT th.h, t.base_offset + th.idx
            FROM queen.log_txns t
            CROSS JOIN LATERAL queen.log_unnest_hashes(t.hashes) th
            WHERE t.partition_id = p_partition_id
              AND (NOT v_has_lease OR t.base_offset <= v_c.batch_end)
        ),
        resolved(in_idx, hash, status, eff, below) AS (
            SELECT i.in_idx, i.hash, i.status,
                   MIN(o.voff) FILTER (
                       WHERE o.voff >= v_lo
                         AND (NOT v_has_lease OR o.voff <= v_c.batch_end)),
                   COALESCE(bool_or(o.voff <= v_c.committed), false)
            FROM input i
            LEFT JOIN occ o ON o.h = i.hash
            GROUP BY i.in_idx, i.hash, i.status
        ),
        sig(sig_off, sig_kind) AS (
            SELECT eff, status
            FROM resolved
            WHERE eff IS NOT NULL AND status IN ('failed', 'dlq', 'retry')
            ORDER BY eff,
                     CASE status WHEN 'dlq' THEN 0 WHEN 'failed' THEN 1 ELSE 2 END
            LIMIT 1
        )
        SELECT
            (SELECT sig_off  FROM sig),
            (SELECT sig_kind FROM sig),
            COALESCE(jsonb_agg(encode(r.hash, 'hex') ORDER BY r.in_idx) FILTER (
                WHERE r.below AND r.eff IS NULL
                  AND r.status IN ('completed', 'success', 'acked', 'ok')), '[]'::jsonb),
            COALESCE(jsonb_agg(encode(r.hash, 'hex') ORDER BY r.in_idx) FILTER (
                WHERE r.below AND r.eff IS NULL
                  AND r.status IN ('failed', 'dlq', 'retry')), '[]'::jsonb),
            -- Unresolvable: no occurrence in the span AND none below the
            -- cursor — the log_txns row was purged, or the txn never existed.
            -- The cursor math already treats these as NOT acked (redelivery
            -- over loss); this list makes the broker SAY so per item instead
            -- of defaulting them to success (the silent-livelock bug: a
            -- client acking beyond the hash window was told ok while the
            -- cursor never moved, and a failed/dlq signal there vanished
            -- without retry charge or DLQ hand-off).
            COALESCE(jsonb_agg(encode(r.hash, 'hex') ORDER BY r.in_idx) FILTER (
                WHERE r.eff IS NULL AND NOT r.below), '[]'::jsonb),
            MAX(r.eff) FILTER (
                WHERE r.eff IS NOT NULL
                  AND r.status IN ('completed', 'success', 'acked', 'ok')
                  AND ((SELECT sig_off FROM sig) IS NULL
                       OR r.eff < (SELECT sig_off FROM sig)))
        INTO v_sig_off, v_sig_kind, v_noop, v_stale, v_unresolved, v_max_ok
        FROM resolved r;
    END IF;

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
            'noopHashes', v_noop, 'staleHashes', v_stale, 'unresolvedHashes', v_unresolved);
    END IF;

    -- ------------------------------------------------------------------ (4)
    -- Explicit 'failed' nack with a live lease: charge the per-(partition,
    -- group) retry counter and decide redeliver / DLQ / drop.
    IF v_nack_kind = 'failed' AND v_has_lease THEN
        -- RUSTFIX item 2: DLQ defaults to TRUE (unconfigured queue always DLQs).
        -- Queue identity is the queues id: the config lives on the very row the
        -- partition FKs to, so the old name-join (and its cross-tenant footgun)
        -- is gone — one lookup, inherently tenant-correct.
        SELECT COALESCE(qq.retry_limit, 3),
               COALESCE(qq.dead_letter_queue, true)
               OR COALESCE(qq.dlq_after_max_retries, true)
        INTO v_retry_limit, v_dlq_enabled
        FROM queen.log_partitions lp
        JOIN queen.queues qq ON qq.id = lp.queue_id
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
                                      'noopHashes', v_noop, 'staleHashes', v_stale, 'unresolvedHashes', v_unresolved);
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
                'noopHashes', v_noop, 'staleHashes', v_stale, 'unresolvedHashes', v_unresolved);
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
                'noopHashes', v_noop, 'staleHashes', v_stale, 'unresolvedHashes', v_unresolved);
        END IF;
    END IF;

    -- ------------------------------------------------------------------ (5)
    -- Explicit 'retry' (no failure): release the lease, cursor at the
    -- completed prefix, counter UNTOUCHED — budget-free retry
    -- (the retired rows-era ack's budget-free-retry semantics).
    IF v_has_retry AND v_has_lease THEN
        UPDATE queen.log_consumers SET
            committed = v_new,
            worker_id = NULL, lease_expires_at = NULL,
            batch_end = NULL,
            total_consumed = total_consumed + v_delta
        WHERE partition_id = p_partition_id AND consumer_group = p_group;
        RETURN jsonb_build_object('ok', true, 'acked', v_delta, 'off', v_new,
                                  'noopHashes', v_noop, 'staleHashes', v_stale, 'unresolvedHashes', v_unresolved);
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
                              'noopHashes', v_noop, 'staleHashes', v_stale, 'unresolvedHashes', v_unresolved);
END;
$$;

-- ============================================================================
-- log_renew_lease_v1 — renew every live log-engine lease held by this worker
-- (multi-partition pops share the worker id). Port of the retired
-- seg_renew_lease_v1. GREATEST: renewing must never SHORTEN a lease (retired
-- rows-era renew_lease parity).
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
-- Port of the retired seg_dlq_head_v1, single offset. Called by the broker after
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

-- get_dlq_messages_v1 (DLQ browsing for /api/v1/dlq) was defined here, between
-- log_dlq_head_v1 and the transaction wire, because it is part of the broker's
-- DLQ workflow. It is DELETED: 010_log_admin.sql applies later in the boot
-- sequence (server/src/schema.rs) and CREATE OR REPLACEs the identical (JSONB)
-- signature, so this body was dead on arrival every boot. 010_log_admin carries
-- the live definition and the GRANT (dropped here with it).

-- ============================================================================
-- log_transaction_wire_v1 — atomic push+ack for log queues (wire form: blobs
-- travel as base64 text, hashes as hex text). Port of the retired
-- seg_transaction_wire_v1 onto the log engine's single allocator code path
-- (log_push_one_v1).
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
--      log_partitions.id order — shared with log_push_multi_v1 (003_log_push)
--      and retention (006_log_maintenance),
--   3. pushes (partition row locks already held; pid/window pre-resolved and
--      passed down so log_push_one_v1 never re-resolves — the nested-lookup
--      CPU regression the seg v2 rework eliminated),
--   4. acks in ascending (partition_id, group) order — each ack locks its
--      queen.log_consumers row until commit, so concurrent transactions acking
--      overlapping sets take those locks in one total order. Consumer-row
--      locks are only ever taken AFTER all partition-row locks, so the two
--      lock spaces can never form a cross-space cycle.
--
-- The old mixed-engine guard (storage <> 'segments') is gone with the storage
-- column itself: the rows engine no longer exists, every queue is log-backed,
-- and 'segments' survives only as a literal in wire JSON. Queues absent from
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
    -- Track B (§5): the tenant travels inside the wire payload (`_tenant` key), so
    -- the (JSONB) signature is unchanged (the broker binds by signature).
    -- Absent ⇒ default tenant (byte-identical to pre-Track-B).
    v_tenant UUID := COALESCE((p->>'_tenant')::uuid, '00000000-0000-0000-0000-000000000001');
BEGIN
    -- (The old storage <> 'segments' mixed-engine guards are deleted: the
    -- storage column is gone with the rows engine.)

    -- Track B: every ack's partition must belong to this tenant (the ack path is
    -- pid-keyed, so without this a forged/leaked pid could ack across tenants).
    -- Partition→queue is queue_id = queues.id; the queues row carries the tenant.
    SELECT (op->>'partitionId') INTO v_bad
    FROM jsonb_array_elements(COALESCE(p->'acks', '[]'::jsonb)) op
    LEFT JOIN queen.log_partitions lp ON lp.id = (op->>'partitionId')::uuid
    LEFT JOIN queen.queues qq ON qq.id = lp.queue_id AND qq.tenant_id = v_tenant
    WHERE qq.id IS NULL
    LIMIT 1;
    IF v_bad IS NOT NULL THEN
        RAISE EXCEPTION 'QTXN ack references partition % not owned by this tenant', v_bad;
    END IF;

    -- Set-based provisioning with the steady-state skip (003_log_push pattern): one
    -- cheap existence count; only when something is missing run the two
    -- ON CONFLICT statements. Queue identity is the queen.queues id, so the
    -- old log_queues insert is gone: ONE queues insert (RUSTFIX item 26 —
    -- namespace/task derived from the dotted name), then log_partitions keyed
    -- by that id. Inserts ORDER BY their unique key (lock-order discipline).
    SELECT count(*) INTO v_missing
    FROM jsonb_array_elements(COALESCE(p->'pushes', '[]'::jsonb)) op
    LEFT JOIN queen.queues q ON q.name = op->>'queue' AND q.tenant_id = v_tenant
    LEFT JOIN queen.log_partitions pt ON pt.queue_id = q.id AND pt.name = op->>'partition'
    WHERE pt.id IS NULL;

    IF v_missing > 0 THEN
        INSERT INTO queen.queues (tenant_id, name, namespace, task)
        SELECT DISTINCT v_tenant, op->>'queue',
               split_part(op->>'queue', '.', 1),
               CASE WHEN position('.' in op->>'queue') > 0 THEN split_part(op->>'queue', '.', 2) ELSE '' END
        FROM jsonb_array_elements(COALESCE(p->'pushes', '[]'::jsonb)) op
        WHERE COALESCE(op->>'queue', '') <> ''
        ORDER BY 2
        ON CONFLICT (tenant_id, name) DO NOTHING;

        INSERT INTO queen.log_partitions (queue_id, name)
        SELECT DISTINCT q.id, op->>'partition'
        FROM jsonb_array_elements(COALESCE(p->'pushes', '[]'::jsonb)) op
        JOIN queen.queues q ON q.name = op->>'queue' AND q.tenant_id = v_tenant
        ORDER BY 1, 2
        ON CONFLICT (queue_id, name) DO NOTHING;
    END IF;

    -- Deadlock-safe pre-lock: ascending log_partitions.id, one statement (the
    -- LockRows node sits above the Sort, so rows lock in ORDER BY order).
    PERFORM 1
    FROM (
        SELECT DISTINCT pt.id
        FROM jsonb_array_elements(COALESCE(p->'pushes', '[]'::jsonb)) op
        JOIN queen.queues q ON q.name = op->>'queue' AND q.tenant_id = v_tenant
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
        -- dedup_window_seconds lives on queen.queues now (the one queue table).
        SELECT op.value AS op, pt.id AS pid, q.dedup_window_seconds AS window
        FROM jsonb_array_elements(COALESCE(p->'pushes', '[]'::jsonb))
             WITH ORDINALITY AS op(value, ord)
        JOIN queen.queues q ON q.name = op.value->>'queue' AND q.tenant_id = v_tenant
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
            v_rec.window,
            v_tenant);

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
            SELECT qq.name, lp.name INTO v_queue, v_partition
            FROM queen.log_partitions lp
            JOIN queen.queues qq ON qq.id = lp.queue_id AND qq.tenant_id = v_tenant
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
                COALESCE((v_op->>'count')::integer, 0),
                v_tenant);
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
