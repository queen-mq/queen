-- ============================================================================
-- Log engine (18-log-engine.md §8) — maintenance step functions.
--
--   queen.log_retention_boundary_v1   boundary walk helper (offset flavor of
--                                     026's seg_retention_boundary_v1)
--   queen.log_retention_step_v1       ONE bounded retention transaction for ONE
--                                     partition (rules 1+2 of the old sweep)
--   queen.log_txns_purge_step_v1      same pattern over log_txns / txns_start
--   queen.log_evict_max_wait_step_v1  max_wait_time_seconds eviction (former
--                                     db.rs::seg_evict_max_wait, now in SQL)
--
-- STEP functions, not sweeps: the old seg_retention_sweep_v1 was one call =
-- one transaction over EVERY partition, so a big backlog held one giant
-- transaction (and its locks) open for the whole sweep — the "no sweep
-- stalls" acceptance item (§12.5) is exactly about that. Here one call = ONE
-- partition, AT MOST p_max_rows segment deletes, one bounded transaction.
-- retention.rs (§8) loops each step until {"done":true}, each call
-- autocommitting, under the cycle-level advisory lock 737001.
--
-- LOCK ORDER (deadlock discipline, ported from 026's header): every step
-- takes exactly ONE queen.log_partitions row lock (SELECT .. FOR UPDATE — the
-- same per-partition serializer the push allocator uses) and holds it only
-- for its own bounded transaction. Because no call ever holds two partition
-- locks, the steps cannot deadlock with the multi-partition lockers
-- (log_push_multi_v1 / log_transaction_wire_v1 pre-lock ascending by id) no
-- matter the iteration order; retention.rs still iterates partitions in
-- ascending id order to keep the one global total order everywhere.
--
-- CUTOFFS are computed by the CALLER (retention.rs) and passed as absolute
-- timestamps: SQL stays policy-free, and one cycle uses one consistent
-- now() for all its steps. A NULL cutoff disables that rule for the call
-- (the old sweep's "retention_seconds > 0" gating, decided Rust-side).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Boundary walk helper: smallest base_offset >= p_from of a segment fresh
-- enough to keep (created_at >= p_cutoff). created_at is monotone in
-- base_offset per partition (PUSHSER invariant, stamped under the allocator
-- row lock — 041 header), so this is a single forward PK walk from the
-- previous watermark, identical in shape to 026's seg_retention_boundary_v1.
-- When EVERYTHING at/above the watermark is stale, the boundary is one past
-- the newest segment (MAX(end_offset)+1 — offsets are per-message now, so
-- "+1 past the last frame", not "+1 past the last segment seq"). Returns
-- p_from when there is nothing to delete (empty delete range).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.log_retention_boundary_v1(
    p_partition_id UUID,
    p_from BIGINT,
    p_cutoff TIMESTAMPTZ
) RETURNS BIGINT
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    v_boundary BIGINT;
BEGIN
    SELECT s.base_offset INTO v_boundary
    FROM queen.log_segments s
    WHERE s.partition_id = p_partition_id
      AND s.base_offset >= p_from
      AND s.created_at >= p_cutoff
    ORDER BY s.base_offset LIMIT 1;

    IF v_boundary IS NULL THEN
        -- everything (if anything) above the watermark is older than the cutoff
        SELECT max(s.end_offset) + 1 INTO v_boundary FROM queen.log_segments s
        WHERE s.partition_id = p_partition_id AND s.base_offset >= p_from;
    END IF;

    RETURN COALESCE(v_boundary, p_from);
END;
$$;

GRANT EXECUTE ON FUNCTION queen.log_retention_boundary_v1(UUID, BIGINT, TIMESTAMPTZ) TO PUBLIC;

-- ----------------------------------------------------------------------------
-- log_retention_step_v1: one bounded retention step for one partition.
-- Returns {"deleted":N,"new_log_start":X,"done":bool} — N = segment ROWS
-- deleted this call, X = the partition's log_start after this call, done =
-- nothing deletable remains below the computed boundary (caller stops
-- looping). Ported rules (026's seg_retention_sweep_v1):
--
--   * Rule 1 (p_all_cutoff, from retention_seconds): segments older than the
--     cutoff are deleted regardless of consumption state — the explicit
--     "drop unconsumed data after N seconds" knob.
--   * Rule 2 (p_completed_cutoff, from completed_retention_seconds): only
--     CONSUMED data may go — the time boundary is capped at
--     MIN(committed)+1 across the partition's log_consumers rows (the
--     slowest group's next wanted offset). A partition with NO consumer
--     rows has nothing consumed (v1 semantics: consumption requires a
--     consumer row), so rule 2 contributes nothing and old-but-unconsumed
--     segments survive. That cap is exactly why unconsumed backlog is never
--     lost to rule 2.
--
-- The combined boundary is GREATEST of the applicable rules starting from
-- log_start, so the watermark invariant "every offset < log_start is
-- deleted" holds and repeated steps never re-scan the dead head of the PK.
--
-- WHOLE-SEGMENT granularity: only segments ENTIRELY below the boundary
-- (end_offset < boundary) are deleted. Rule 2's cap is a MESSAGE offset and
-- may fall mid-segment; the covering segment then survives (its unconsumed
-- tail is still needed) and log_start advances only to the last deleted
-- segment's end_offset+1 — NOT to the mid-segment boundary — preserving the
-- invariant "log_start = first live segment's base_offset (or
-- last_offset+1)" that the O(partitions) stats math (§9) and the pop head
-- probe rely on. With no mid-log gaps (deletes are prefix-contiguous, 041
-- header) a rule-1 boundary is always segment-aligned and the two advances
-- coincide.
--
-- Batch bounding per spec: the p_max_rows lowest base_offsets below the
-- boundary are located first (ORDER BY base_offset LIMIT), then ONE ranged
-- DELETE covers [log_start, max chosen base] — correct because base and end
-- are equi-monotone (disjoint ranges), so everything in that base range is
-- in the chosen batch.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.log_retention_step_v1(
    p_pid             UUID,
    p_all_cutoff       TIMESTAMPTZ,
    p_completed_cutoff TIMESTAMPTZ,
    p_max_rows         INT
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_from     BIGINT;
    v_boundary BIGINT;
    v_min_next BIGINT;
    v_max_base BIGINT;
    v_max_end  BIGINT;
    v_have     BIGINT;
    v_deleted  BIGINT := 0;
    v_new      BIGINT;
    v_done     BOOLEAN;
    v_rows     INT := GREATEST(COALESCE(p_max_rows, 1), 1);
BEGIN
    -- The partition row lock serializes this step against the push allocator
    -- and the other maintenance steps; held only to this call's commit.
    SELECT p.log_start INTO v_from
    FROM queen.log_partitions p WHERE p.id = p_pid FOR UPDATE;
    IF NOT FOUND THEN
        -- Partition deleted under us (queue drop): nothing to do, ever.
        RETURN jsonb_build_object('deleted', 0, 'new_log_start', NULL, 'done', true);
    END IF;

    v_boundary := v_from;

    -- Rule 1: time-based retention over ALL segments.
    IF p_all_cutoff IS NOT NULL THEN
        v_boundary := GREATEST(v_boundary,
            queen.log_retention_boundary_v1(p_pid, v_from, p_all_cutoff));
    END IF;

    -- Rule 2: completed retention over CONSUMED offsets only, capped at the
    -- slowest cursor's next wanted offset (MIN(committed)+1).
    IF p_completed_cutoff IS NOT NULL THEN
        SELECT MIN(c.committed) + 1 INTO v_min_next
        FROM queen.log_consumers c WHERE c.partition_id = p_pid;

        IF v_min_next IS NOT NULL AND v_min_next > v_from THEN
            v_boundary := GREATEST(v_boundary, LEAST(
                v_min_next,
                queen.log_retention_boundary_v1(p_pid, v_from, p_completed_cutoff)));
        END IF;
    END IF;

    IF v_boundary <= v_from THEN
        RETURN jsonb_build_object('deleted', 0, 'new_log_start', v_from, 'done', true);
    END IF;

    -- Locate the batch: the v_rows lowest segments FULLY below the boundary.
    -- base_offset < boundary is implied by end_offset < boundary but stated
    -- anyway as the PK-index stop condition (without it a done-scan would run
    -- past the boundary to the partition tail testing end_offset in vain).
    SELECT max(b.base_offset), max(b.end_offset), count(*)
    INTO v_max_base, v_max_end, v_have
    FROM (
        SELECT s.base_offset, s.end_offset
        FROM queen.log_segments s
        WHERE s.partition_id = p_pid
          AND s.base_offset >= v_from
          AND s.base_offset <  v_boundary
          AND s.end_offset  <  v_boundary
        ORDER BY s.base_offset
        LIMIT v_rows
    ) b;

    IF COALESCE(v_have, 0) = 0 THEN
        -- Boundary > log_start but no whole segment below it: the head
        -- segment is only PARTIALLY consumed past a rule-2 cap (or the
        -- prefix is already gone). Nothing to delete, watermark stays.
        RETURN jsonb_build_object('deleted', 0, 'new_log_start', v_from, 'done', true);
    END IF;

    DELETE FROM queen.log_segments s
    WHERE s.partition_id = p_pid
      AND s.base_offset >= v_from
      AND s.base_offset <= v_max_base;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    -- Advance to one past the last deleted frame (see header for why NOT to
    -- the mid-segment rule-2 boundary). Guarded update: log_start never moves
    -- backwards.
    v_new := v_max_end + 1;
    UPDATE queen.log_partitions SET log_start = v_new
    WHERE id = p_pid AND log_start < v_new;

    v_done := NOT EXISTS (
        SELECT 1 FROM queen.log_segments s
        WHERE s.partition_id = p_pid
          AND s.base_offset > v_max_base
          AND s.base_offset < v_boundary
          AND s.end_offset  < v_boundary);

    RETURN jsonb_build_object('deleted', v_deleted, 'new_log_start', v_new, 'done', v_done);
END;
$$;

GRANT EXECUTE ON FUNCTION queen.log_retention_step_v1(UUID, TIMESTAMPTZ, TIMESTAMPTZ, INT) TO PUBLIC;

-- ----------------------------------------------------------------------------
-- log_txns_purge_step_v1: same step pattern over log_txns / txns_start.
-- Returns {"deleted":N,"new_txns_start":X,"done":bool}.
--
-- log_txns is the hash sidecar (dedup probe + ack-by-hash resolution); its
-- rows expire on their OWN clock — the caller passes
-- cutoff = now() - GREATEST(dedup_window_seconds, completed_retention_seconds,
-- 900) (041 header / §8) so a row is never purged while the push probe or a
-- plausible late ack could still need it. A hash that outlives even that
-- window resolves as "unknown" on ack, which the ack path counts as NOT
-- acked — redelivery over loss, the engine's standing bias.
--
-- Same single-row-lock discipline as the retention step: the push dedup
-- probe reads txns_start under the partition row lock, so moving the
-- watermark under that same lock keeps the probe's lower bound stable within
-- any push. Rows are whole (no rule-2 mid-row cap can exist here), so the
-- boundary is always row-aligned and the watermark advances to the last
-- purged end_offset+1 == boundary when done.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.log_txns_purge_step_v1(
    p_pid      UUID,
    p_cutoff   TIMESTAMPTZ,
    p_max_rows INT
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_from     BIGINT;
    v_boundary BIGINT;
    v_max_base BIGINT;
    v_max_end  BIGINT;
    v_have     BIGINT;
    v_deleted  BIGINT := 0;
    v_new      BIGINT;
    v_done     BOOLEAN;
    v_rows     INT := GREATEST(COALESCE(p_max_rows, 1), 1);
BEGIN
    SELECT p.txns_start INTO v_from
    FROM queen.log_partitions p WHERE p.id = p_pid FOR UPDATE;
    IF NOT FOUND THEN
        RETURN jsonb_build_object('deleted', 0, 'new_txns_start', NULL, 'done', true);
    END IF;

    -- Boundary walk (created_at monotone per partition — same PUSHSER
    -- invariant as the segment walk; log_txns rows are stamped with the same
    -- v_now as their segment, 042).
    SELECT t.base_offset INTO v_boundary
    FROM queen.log_txns t
    WHERE t.partition_id = p_pid
      AND t.base_offset >= v_from
      AND t.created_at >= p_cutoff
    ORDER BY t.base_offset LIMIT 1;

    IF v_boundary IS NULL THEN
        SELECT max(t.end_offset) + 1 INTO v_boundary FROM queen.log_txns t
        WHERE t.partition_id = p_pid AND t.base_offset >= v_from;
    END IF;
    v_boundary := COALESCE(v_boundary, v_from);

    IF v_boundary <= v_from THEN
        RETURN jsonb_build_object('deleted', 0, 'new_txns_start', v_from, 'done', true);
    END IF;

    SELECT max(b.base_offset), max(b.end_offset), count(*)
    INTO v_max_base, v_max_end, v_have
    FROM (
        SELECT t.base_offset, t.end_offset
        FROM queen.log_txns t
        WHERE t.partition_id = p_pid
          AND t.base_offset >= v_from
          AND t.base_offset <  v_boundary
        ORDER BY t.base_offset
        LIMIT v_rows
    ) b;

    IF COALESCE(v_have, 0) = 0 THEN
        RETURN jsonb_build_object('deleted', 0, 'new_txns_start', v_from, 'done', true);
    END IF;

    DELETE FROM queen.log_txns t
    WHERE t.partition_id = p_pid
      AND t.base_offset >= v_from
      AND t.base_offset <= v_max_base;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    v_new := v_max_end + 1;
    UPDATE queen.log_partitions SET txns_start = v_new
    WHERE id = p_pid AND txns_start < v_new;

    v_done := NOT EXISTS (
        SELECT 1 FROM queen.log_txns t
        WHERE t.partition_id = p_pid
          AND t.base_offset > v_max_base
          AND t.base_offset < v_boundary);

    RETURN jsonb_build_object('deleted', v_deleted, 'new_txns_start', v_new, 'done', v_done);
END;
$$;

GRANT EXECUTE ON FUNCTION queen.log_txns_purge_step_v1(UUID, TIMESTAMPTZ, INT) TO PUBLIC;

-- ----------------------------------------------------------------------------
-- log_evict_max_wait_step_v1: max_wait_time_seconds age eviction — the rule
-- the seg engine had NO SP for (db.rs::seg_evict_max_wait inlined it:
-- boundary walk with cutoff now()-max_wait_time_seconds, prefix delete,
-- watermark advance; C++ EvictionService parity, retention.js::
-- retentionTestMaxTime). Semantically that IS rule 1 with a different cutoff
-- source (queen.queues.max_wait_time_seconds instead of retention_seconds),
-- so this delegates to log_retention_step_v1 with rule 2 disabled — ONE
-- delete/advance code path, same batch bound, same
-- {"deleted":N,"new_log_start":X,"done":bool} return.
--
-- Row-count contract for the caller (retention.rs): the old Rust helper
-- returned total segment rows deleted across all partitions; the port sums
-- this function's "deleted" over its per-partition done-loops to report the
-- same number. Like the old inline SQL, eviction applies regardless of
-- retention_enabled (a queue configured with ONLY maxWaitTimeSeconds still
-- gets swept); the caller selects eligible queues via
-- queen.queues.max_wait_time_seconds > 0 and passes
-- cutoff = now() - make_interval(secs => max_wait_time_seconds).
-- DATA LOSS by design, like the old rule: whole segments older than the
-- cutoff are dropped for every group, in-flight leases included; a cursor
-- left below the new log_start resumes at the next existing offset (the pop
-- scan tolerates the gap).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.log_evict_max_wait_step_v1(
    p_pid      UUID,
    p_cutoff   TIMESTAMPTZ,
    p_max_rows INT
) RETURNS JSONB
LANGUAGE sql
AS $$
    SELECT queen.log_retention_step_v1(p_pid, p_cutoff, NULL, p_max_rows)
$$;

GRANT EXECUTE ON FUNCTION queen.log_evict_max_wait_step_v1(UUID, TIMESTAMPTZ, INT) TO PUBLIC;
