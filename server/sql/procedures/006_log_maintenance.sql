-- ============================================================================
-- Log engine (18-log-engine.md §8) — maintenance step functions.
--
--   queen.log_retention_boundary_v1   boundary walk helper (offset flavor of
--                                     the retired seg_retention_boundary_v1)
--   queen.log_retention_step_v1       ONE bounded retention transaction for ONE
--                                     partition (rules 1+2 of the old sweep)
--   queen.log_txns_purge_step_v1      same pattern over log_txns / txns_start
--   queen.log_evict_max_wait_step_v1  max_wait_time_seconds eviction (former
--                                     db.rs::seg_evict_max_wait, now in SQL)
--   queen.log_partition_dead_v1       eligibility predicate for the below
--   queen.log_partition_cleanup_step_v1
--                                     delete EMPTY, long-inactive partitions
--                                     (PARTITION_CLEANUP_DAYS — the C++
--                                     cleanup_inactive_partitions, restored)
--
-- STEP functions, not sweeps: the old seg_retention_sweep_v1 was one call =
-- one transaction over EVERY partition, so a big backlog held one giant
-- transaction (and its locks) open for the whole sweep — the "no sweep
-- stalls" acceptance item (§12.5) is exactly about that. Here one call = ONE
-- partition, AT MOST p_max_rows segment deletes, one bounded transaction.
-- retention.rs (§8) loops each step until {"done":true}, each call
-- autocommitting, under the cycle-level advisory lock 737001.
--
-- LOCK ORDER (deadlock discipline, ported from the retired seg retention
-- sweep's header): every per-
-- partition step takes exactly ONE queen.log_partitions row lock (SELECT ..
-- FOR UPDATE — the same per-partition serializer the push allocator uses) and
-- holds it only for its own bounded transaction. Because those calls never
-- hold two partition locks, they cannot deadlock with the multi-partition
-- lockers (log_push_multi_v1 / log_transaction_wire_v1 pre-lock ascending by
-- id) no matter the iteration order; retention.rs still iterates partitions in
-- ascending id order to keep the one global total order everywhere.
--
-- log_partition_cleanup_step_v1 is the one step that locks MANY partitions in
-- a call (it deletes whole partition rows in batches). It takes them in the
-- SAME ascending-id order as those two writers, in one statement, with SKIP
-- LOCKED — so it never waits on a pusher and never inverts the order, and the
-- global total order still holds.
--
-- CUTOFFS are computed by the CALLER (retention.rs) and passed as absolute
-- timestamps: SQL stays policy-free, and one cycle uses one consistent
-- now() for all its steps. A NULL cutoff disables that rule for the call
-- (the old sweep's "retention_seconds > 0" gating, decided Rust-side).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- queen.retention_history is the table 022_retention_analytics reads and the
-- webapp's Retention row/panel render. It was written only by the retired rows
-- engine, so under the log engine those surfaces showed "0 messages evicted"
-- while segments were being deleted continuously — a dead panel presented as a
-- measurement. The steps below write it.
--
-- Its partition_id carries a queen.log_partitions id and has no foreign key:
-- the audit row must outlive the partition, which the cleanup phase below
-- deletes while writing one. Growth is bounded by the age purge in
-- queen.cleanup_worker_metrics_v1 (019_worker_metrics).
-- ----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_retention_history_executed_at
    ON queen.retention_history (executed_at DESC);

-- ----------------------------------------------------------------------------
-- Boundary walk helper: smallest base_offset >= p_from of a segment fresh
-- enough to keep (created_at >= p_cutoff). created_at is monotone in
-- base_offset per partition (PUSHSER invariant, stamped under the allocator
-- row lock — 001_log_schema header), so this is a single forward PK walk from
-- the previous watermark, identical in shape to the retired
-- seg_retention_boundary_v1.
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
-- looping). Ported rules (the retired seg_retention_sweep_v1's):
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
-- probe rely on. With no mid-log gaps (deletes are prefix-contiguous,
-- 001_log_schema header) a rule-1 boundary is always segment-aligned and the
-- two advances coincide.
--
-- Batch bounding per spec: the p_max_rows lowest base_offsets below the
-- boundary are located first (ORDER BY base_offset LIMIT), then ONE ranged
-- DELETE covers [log_start, max chosen base] — correct because base and end
-- are equi-monotone (disjoint ranges), so everything in that base range is
-- in the chosen batch.
-- ----------------------------------------------------------------------------
-- p_history_type names the rule for the queen.retention_history row this step
-- writes when it actually deletes something ('retention' / 'completed_retention'
-- are chosen per-call from the rule that moved the boundary; the eviction
-- wrapper passes 'max_wait_time_eviction').
CREATE OR REPLACE FUNCTION queen.log_retention_step_v1(
    p_pid             UUID,
    p_all_cutoff       TIMESTAMPTZ,
    p_completed_cutoff TIMESTAMPTZ,
    p_max_rows         INT,
    p_history_type     TEXT DEFAULT NULL
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_from     BIGINT;
    v_boundary BIGINT;
    v_b_all    BIGINT;
    v_type     TEXT;
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
    v_type := COALESCE(p_history_type, 'retention');

    -- Rule 1: time-based retention over ALL segments.
    IF p_all_cutoff IS NOT NULL THEN
        v_boundary := GREATEST(v_boundary,
            queen.log_retention_boundary_v1(p_pid, v_from, p_all_cutoff));
    END IF;
    v_b_all := v_boundary;

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

    -- Attribute the delete to the rule that actually moved the boundary (the
    -- caller's explicit type, when given, always wins).
    IF p_history_type IS NULL AND v_boundary > v_b_all THEN
        v_type := 'completed_retention';
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

    -- Audit row for the Retention analytics (022_retention_analytics).
    -- messages_deleted is the FRAME
    -- count removed (v_new - v_from), not the segment-row count — the readers
    -- label it "messages evicted". Written in the step's own transaction, so it
    -- is committed iff the delete was.
    INSERT INTO queen.retention_history (partition_id, messages_deleted, retention_type, executed_at)
    VALUES (p_pid, GREATEST(v_new - v_from, 0)::integer, v_type, NOW());

    v_done := NOT EXISTS (
        SELECT 1 FROM queen.log_segments s
        WHERE s.partition_id = p_pid
          AND s.base_offset > v_max_base
          AND s.base_offset < v_boundary
          AND s.end_offset  < v_boundary);

    RETURN jsonb_build_object('deleted', v_deleted, 'new_log_start', v_new, 'done', v_done);
END;
$$;

GRANT EXECUTE ON FUNCTION queen.log_retention_step_v1(UUID, TIMESTAMPTZ, TIMESTAMPTZ, INT, TEXT) TO PUBLIC;

-- ----------------------------------------------------------------------------
-- log_txns_purge_step_v1: same step pattern over log_txns / txns_start.
-- Returns {"deleted":N,"new_txns_start":X,"done":bool}.
--
-- log_txns is the hash sidecar (dedup probe + ack-by-hash resolution); its
-- rows expire on their OWN clock — the caller passes
-- cutoff = now() - GREATEST(dedup_window_seconds, completed_retention_seconds,
-- 900) (001_log_schema header / §8) so a row is never purged while the push probe or a
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
    -- v_now as their segment, 003_log_push).
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
    SELECT queen.log_retention_step_v1(p_pid, p_cutoff, NULL, p_max_rows,
                                      'max_wait_time_eviction')
$$;

GRANT EXECUTE ON FUNCTION queen.log_evict_max_wait_step_v1(UUID, TIMESTAMPTZ, INT) TO PUBLIC;

-- ----------------------------------------------------------------------------
-- log_partition_dead_v1: the eligibility predicate for partition cleanup, in
-- ONE place so the candidate scan and the under-lock re-check below cannot
-- drift apart. TRUE = this partition holds nothing and nobody has touched it
-- since p_cutoff. NULL (no such partition) is not TRUE, so a row that vanished
-- between the two passes is simply not deleted twice.
--
-- "Empty" is decided by log_segments, not by a message count: either nothing
-- was ever pushed, or retention already dropped everything.
--
-- Three of the legs veto a delete because their table is keyed by partition_id
-- with NO foreign key — cancelling the partition would make them permanently
-- unreachable rather than cleaning them up:
--   * queen.log_dlq        dead-lettered payloads. Retention never purges them
--                          (only DELETE /messages/:pid/:txn does), so a
--                          partition still holding DLQ rows is NOT empty.
--   * queen_streams.state  002_streams_schema declines the FK on purpose — "a
--                          partition-cleanup must not silently delete
--                          still-relevant state". This honours that.
--   * a live lease         batch_end IS NOT NULL with the lease still unexpired:
--                          someone is mid-batch, whatever the timestamps say.
--                          An EXPIRED lease is deliberately not a veto — the
--                          engine treats expiry as "the batch is up for grabs",
--                          and on an empty partition no pop will ever come to
--                          clear a batch_end left behind by a dead worker, so
--                          vetoing on it would pin the row forever. The
--                          lease_expires_at >= p_cutoff leg still spares any
--                          lease that expired inside the window. A NULL expiry
--                          under a set batch_end is a broken invariant
--                          (005_log_ack rejects acks on it), so it counts as
--                          live.
--
-- INACTIVITY mirrors the retired C++ predicate (GREATEST of the partition's own
-- age and every consumer group's activity — retention_service.cpp:380) on the
-- columns this engine actually has: p.created_at, p.last_write_at (quantized to
-- one real bump per second by push — 001_log_schema), and per group
-- c.created_at / c.lease_acquired_at / c.lease_expires_at.
--
-- 004_log_pop NULLs lease_acquired_at on an AUTO-ACK pop, so an auto-ack-only group
-- leaves no activity trace. That is harmless here rather than unhandled:
-- last_write_at < p_cutoff means nothing was pushed for the whole window, so
-- such a group drained the partition long ago and its cursor addresses no data.
-- What a delete can cost it is total_consumed, not a position — a group's
-- durable subscription record is per (queue, group), not per partition, so a
-- partition recreated by a later push re-seeds from it exactly as a brand-new
-- partition does.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.log_partition_dead_v1(
    p_pid    UUID,
    p_cutoff TIMESTAMPTZ
) RETURNS BOOLEAN
LANGUAGE sql STABLE
AS $$
    SELECT p.created_at    < p_cutoff
       AND p.last_write_at < p_cutoff
       AND NOT EXISTS (SELECT 1 FROM queen.log_segments s   WHERE s.partition_id = p.id)
       AND NOT EXISTS (SELECT 1 FROM queen.log_dlq d        WHERE d.partition_id = p.id)
       AND NOT EXISTS (SELECT 1 FROM queen_streams.state st WHERE st.partition_id = p.id)
       AND NOT EXISTS (
               SELECT 1 FROM queen.log_consumers c
               WHERE c.partition_id = p.id
                 AND ((c.batch_end IS NOT NULL
                       AND (c.lease_expires_at IS NULL OR c.lease_expires_at > now()))
                      OR c.lease_expires_at  >= p_cutoff
                      OR c.lease_acquired_at >= p_cutoff
                      OR c.created_at        >= p_cutoff))
    FROM queen.log_partitions p
    WHERE p.id = p_pid
$$;

GRANT EXECUTE ON FUNCTION queen.log_partition_dead_v1(UUID, TIMESTAMPTZ) TO PUBLIC;

-- ----------------------------------------------------------------------------
-- log_partition_cleanup_step_v1: delete EMPTY, long-inactive partitions — the
-- log-engine successor of the retired C++ RetentionService::
-- cleanup_inactive_partitions() (retention_service.cpp:356), which this engine
-- dropped: PARTITION_CLEANUP_DAYS was parsed and then ignored, so partitions
-- accumulated forever. One call deletes at most p_max_rows partitions in ONE
-- bounded transaction; retention.rs loops it to {"done":true} like every other
-- step here, and gates the phase to its own slow sub-cadence (a 30-day window
-- does not need 5-second resolution, and the candidate scan is O(partitions) —
-- the one cost class this engine keeps off the 5s cycle).
--
-- Deleting the row CASCADES to log_consumers (the cursors) and log_segments
-- (none, by definition). queen.log_txns has NO FK (001_log_schema, deliberately
-- — the purge path must not pay FK-trigger cost), so this deletes it EXPLICITLY:
-- nothing else ever would, because the phase-2 sidecar purge is driven by the
-- log_partitions work list, and an orphaned sidecar row would leak forever.
--
-- RACE with a push, and why the lock is not decoration: the candidate scan
-- takes each partition's row lock with SKIP LOCKED — a partition a pusher holds
-- is active by definition, and skipping keeps retention off the push
-- serializer's critical path instead of queueing behind it. The predicate is
-- then RE-EVALUATED under the held locks, and THAT pass is the authoritative
-- one: a push must take the same row lock before it can insert a segment or
-- bump last_write_at, so nothing slips in between check and delete. (PG's
-- EvalPlanQual recheck covers the locked row's own columns, not the NOT EXISTS
-- legs, which is exactly why one pass is not enough.)
--
-- What remains is a window on the OTHER side: a push that resolved this
-- partition id just before the delete committed. It fails LOUDLY —
-- 003_log_push raises
-- 'QMULTI resolved N of M segments', or the segment insert trips the
-- log_segments FK — and the client's retry re-provisions the partition under a
-- new id. No write is silently lost, and reaching it takes a push landing in
-- the same instant as the delete after the partition has been silent for the
-- whole cleanup window.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.log_partition_cleanup_step_v1(
    p_cutoff   TIMESTAMPTZ,
    p_max_rows INT
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INT := GREATEST(COALESCE(p_max_rows, 1000), 1);
    v_cand UUID[];
    v_kill UUID[];
    v_deleted INT := 0;
BEGIN
    -- NULL cutoff = rule disabled for this call (header contract, same as the
    -- retention steps above).
    IF p_cutoff IS NULL THEN
        RETURN jsonb_build_object('deleted', 0, 'done', true);
    END IF;

    -- Candidates, ascending id (the one global lock order). The two cheap
    -- timestamp quals are stated inline so the planner prunes on them BEFORE
    -- the predicate function — its default cost sorts it last — which keeps the
    -- steady-state cost at one scan of a small table with ZERO subquery
    -- executions.
    SELECT array_agg(c.id ORDER BY c.id) INTO v_cand
    FROM (
        SELECT p.id
        FROM queen.log_partitions p
        WHERE p.created_at    < p_cutoff
          AND p.last_write_at < p_cutoff
          AND queen.log_partition_dead_v1(p.id, p_cutoff)
        ORDER BY p.id
        LIMIT v_rows
        FOR UPDATE OF p SKIP LOCKED
    ) c;

    IF v_cand IS NULL THEN
        RETURN jsonb_build_object('deleted', 0, 'done', true);
    END IF;

    -- Authoritative re-check under the held row locks (see header).
    SELECT array_agg(u ORDER BY u) INTO v_kill
    FROM unnest(v_cand) u
    WHERE queen.log_partition_dead_v1(u, p_cutoff);

    IF v_kill IS NULL THEN
        -- Every candidate was disqualified after locking. Report done so the
        -- caller's loop stops instead of re-selecting the same rows.
        RETURN jsonb_build_object('deleted', 0, 'done', true);
    END IF;

    -- No FK (001_log_schema): explicit, or the sidecar outlives its partition
    -- with nothing left able to reach it.
    DELETE FROM queen.log_txns t WHERE t.partition_id = ANY(v_kill);

    -- Audit trail, in the same transaction as the delete. '__partition_deleted__'
    -- is the reserved event shape 022_retention_analytics's header names: the
    -- __%__ prefix keeps
    -- these OUT of the messages-evicted timeseries, which counts deleted
    -- MESSAGES and would otherwise gain zero-valued events.
    INSERT INTO queen.retention_history (partition_id, messages_deleted, retention_type, executed_at)
    SELECT u, 0, '__partition_deleted__', NOW() FROM unnest(v_kill) u;

    -- CASCADEs to log_consumers + log_segments (001_log_schema).
    DELETE FROM queen.log_partitions p WHERE p.id = ANY(v_kill);
    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    -- done when the candidate scan itself came up short: a full batch means
    -- there may be more eligible partitions behind it. deleted < candidates is
    -- NOT a stop condition (the re-check may have spared some) but the caller
    -- also stops on deleted = 0, so a batch that is entirely spared terminates.
    RETURN jsonb_build_object('deleted', v_deleted,
                              'done', COALESCE(array_length(v_cand, 1), 0) < v_rows);
END;
$$;

GRANT EXECUTE ON FUNCTION queen.log_partition_cleanup_step_v1(TIMESTAMPTZ, INT) TO PUBLIC;
