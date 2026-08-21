-- ============================================================================
-- Log engine (18-log-engine.md §6) — pop.
--
--   queen.log_pop_v1                specific-partition pop (the claim core;
--                                   every wildcard/discover variant funnels
--                                   through it)
--   queen.log_pop_wildcard_wire_v1  wildcard pop across a queue's partitions
--                                   (base64 blobs inside the JSON)
--   queen.log_pop_wildcard_bin_v1   same claim algorithm, blobs as a native
--                                   bytea[] out-of-band of the meta JSON
--   queen.log_pop_discover_wire_v1  namespace/task discovery pop (across
--                                   queues)
--   queen.log_has_pending_v1        cheap pending probe for long-poll wakeups
--                                   (queue+group scope, the wildcard gate)
--   queen.log_partition_has_pending_v1  single-partition sibling (pinned gate)
--   queen.log_discover_has_pending_v1   namespace/task sibling (discovery gate)
--
-- Semantics are the retired seg engine's pop family's verbatim — per-batch claim,
-- lease pop→ack, no assignment — re-addressed on single per-partition BIGINT
-- offsets: the consumer cursor is `committed` (last acked offset; next wanted
-- = committed+1), a leased batch is the inclusive span (committed, batch_end].
-- The old (seq, frame_idx) pair arithmetic and every msg_count normalization
-- lookup disappear; a segment covers [base_offset, end_offset] and slicing is
-- pure offset subtraction.
--
-- Wire compat (§11): the JSON keys are UNCHANGED. `seq` now carries
-- base_offset and `startOff` carries the start frame index inside the segment
-- — clients treat both as opaque; `take`/`msgCount`/`createdAt`/`partition`/
-- `partitionId` are identical.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- log_pop_v1: claim frames of ONE (queue, partition) for a group.
-- Returns the raw segment rows covering the budget + slicing bounds; the
-- broker decompresses and slices frames [r_start_idx .. r_start_idx + r_take)
-- of each row. Acquires the (partition, group) lease unless p_auto_ack.
--
-- Ported EXACTLY from the retired seg engine's seg_pop_segments_v1 (its final
-- body, which had superseded the original):
--   * claim-first SKIP LOCKED dance — see the NB below (the deadlock lesson).
--   * subscription seeding on FIRST contact of a (partition, group), durable
--     record first (RUSTFIX item 13), then the pop-carried intent.
--   * delayed_processing / window_buffer visibility (read from queen.queues —
--     the ONLY queue table since the 2026-07-31 identity merge; the resolve
--     join reads them off the same row; retired rows-era pop_unified_v4
--     semantics).
--   * attempt_offset/attempt_count redelivery tracking (successor of the seg
--     attempt_seq/off): the lease UPDATE records the batch's first delivered
--     offset; a non-auto-ack batch that STARTS at the same offset as the
--     previous delivery is a redelivery (nack / lease expiry left the cursor
--     in place) → attempt_count increments; a batch starting anywhere else is
--     fresh work → attempt_count resets to 1. NB (RUSTFIX item 10): the RETRY
--     BUDGET is batch_retry_count, charged only by the ack path on explicit
--     `failed` — attempt_count is redelivery telemetry and never consumes
--     budget, so lease expiry never eats retries.
--
-- The r_attempt OUT column of the seg signature is gone per §6: the display
-- delivery count is batch_retry_count-derived (RUSTFIX item 10) and the
-- broker owns that presentation.
--
-- DROP+CREATE instead of CREATE OR REPLACE: the OUT row type is part of the
-- signature and OR REPLACE refuses return-type changes, so a future column
-- addition would brick boot re-apply. Boot applies the whole procedures
-- sequence before serving traffic, so the gap is invisible.
-- ----------------------------------------------------------------------------
-- p_skip_window_debounce (19-wildcard-hotlist §6): when TRUE, the partition-level
-- windowBuffer quiet-debounce below is BYPASSED — visibility becomes immediate at
-- the SQL level so the broker-side hot-list timer wheel is the sole windowBuffer
-- authority (min(first-mark + windowBuffer, batch full)). It defaults FALSE, so
-- EVERY existing caller (the wildcard/discover pops, db::pop_specific — all pass
-- 9 positional args) is byte-identical: the debounce fires exactly as before.
-- delayed_processing is NEVER skipped (it stays SQL-enforced; the wheel only
-- schedules revisits for it, §6).
-- ----------------------------------------------------------------------------
-- MINIMUM POP WAIT (queen.queues.min_pop_wait_time, TASK M 2026-07-29) is
-- deliberately NOT implemented here, and this note is the record of why. Making
-- an under-full claim wait INSIDE this function (a pg_sleep before or between the
-- per-partition claims) would hold, for the whole window: a pooled PG connection,
-- the broker's pop_vegas serving permit, a PG backend, and — after the first
-- log_pop_v1 — the claimed rows' locks. That trades one cheap commit for a scarce
-- connection and a held lock, which on a 2-core cell is worse than the commit it
-- saves. The wait therefore lives in the broker (handlers/data.rs,
-- serve_pop_hotlist), BEFORE the permit and the connection are taken, following
-- the parked-long-poll precedent: a waiting pop costs the broker a timer and
-- nothing else. This function is unchanged by the feature; it simply gets called
-- later, with more marks accumulated, and claims a fatter batch in one commit.
-- Track B (§5): p_tenant scopes the (queue,partition) resolution + the durable
-- subscription lookup. Added last with a DEFAULT. The DROP of the CURRENT
-- signature is the boot-idempotency half of DROP+CREATE (see header), not an
-- upgrade step.
-- CONFLATION (PLAN_CONFLATION §2.3): p_conflate appended last with a DEFAULT,
-- the same discipline p_tenant used — every existing caller is byte-identical.
-- TRUE = last-value delivery: this pop serves exactly ONE frame, the newest
-- VISIBLE one, and leases the span (committed, tail]. The broker resolves the
-- EFFECTIVE flag from the durable group policy before calling (§3.3); this
-- function never second-guesses it except on first contact, where it is the
-- registrar and reads the stored value back.
DROP FUNCTION IF EXISTS queen.log_pop_v1(TEXT, TEXT, TEXT, INTEGER, INTEGER, TEXT, BOOLEAN, TEXT, TEXT, BOOLEAN, UUID);
DROP FUNCTION IF EXISTS queen.log_pop_v1(TEXT, TEXT, TEXT, INTEGER, INTEGER, TEXT, BOOLEAN, TEXT, TEXT, BOOLEAN, UUID, BOOLEAN);
CREATE FUNCTION queen.log_pop_v1(
    p_queue TEXT,
    p_partition TEXT,
    p_group TEXT,
    p_budget INTEGER,
    p_lease_seconds INTEGER,
    p_worker TEXT,
    p_auto_ack BOOLEAN DEFAULT FALSE,
    p_sub_mode TEXT DEFAULT 'all',
    p_sub_from TEXT DEFAULT '',
    p_skip_window_debounce BOOLEAN DEFAULT FALSE,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001',
    p_conflate BOOLEAN DEFAULT FALSE
) RETURNS TABLE (
    r_base BIGINT,
    r_start_idx INTEGER,
    r_take INTEGER,
    r_msg_count INTEGER,
    r_created_at TIMESTAMPTZ,
    r_blob BYTEA
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_qid UUID;
    v_pid UUID;
    v_last_offset BIGINT;
    v_log_start BIGINT;
    v_delayed INTEGER := 0;
    v_win_buf INTEGER := 0;
    v_deadline TIMESTAMPTZ;
    v_newest TIMESTAMPTZ;
    v_seed BIGINT;
    v_from_ts TIMESTAMPTZ;
    v_committed BIGINT;
    v_wanted BIGINT;
    v_budget INTEGER := GREATEST(p_budget, 1);
    v_taken INTEGER := 0;
    v_start BIGINT;   -- first delivered offset: the batch's identity for
                      -- attempt (redelivery) tracking
    v_last BIGINT;    -- last delivered offset: batch_end / auto-ack committed
    v_avail BIGINT;
    v_take INTEGER;
    v_head RECORD;
    v_row RECORD;
    v_now TIMESTAMPTZ := clock_timestamp();
    -- CONFLATION (§2.3/§3.3): the EFFECTIVE policy for this claim. Starts as the
    -- caller's resolved value and is overwritten by the STORED one on first
    -- contact, where this function is the registrar (M4).
    v_conflate BOOLEAN := COALESCE(p_conflate, FALSE);
    v_conflate_stored BOOLEAN;
    v_cgm_seen INTEGER;
    v_reg_mode TEXT;
    v_reg_ts TIMESTAMPTZ;
BEGIN
    -- Queue identity is the queen.queues id (log_queues is gone): ONE row read
    -- resolves the partition AND the visibility knobs (delayed_processing /
    -- window_buffer live on the same queues row the resolve joins).
    SELECT p.id, p.last_offset, p.log_start, q.id,
           COALESCE(q.delayed_processing, 0), COALESCE(q.window_buffer, 0)
    INTO v_pid, v_last_offset, v_log_start, v_qid, v_delayed, v_win_buf
    FROM queen.log_partitions p JOIN queen.queues q ON q.id = p.queue_id
    WHERE q.name = p_queue AND p.name = p_partition AND q.tenant_id = p_tenant;
    IF v_pid IS NULL THEN RETURN; END IF;

    -- window_buffer: if the partition received a segment within the last
    -- window_buffer seconds, deliver NOTHING (v1 checks the partition's
    -- newest message the same way, before touching the consumer row).
    -- created_at is monotone in base_offset, so the newest segment is the
    -- max base_offset — one backward PK step.
    IF v_win_buf > 0 AND NOT p_skip_window_debounce THEN
        SELECT s.created_at INTO v_newest
        FROM queen.log_segments s
        WHERE s.partition_id = v_pid
        ORDER BY s.base_offset DESC LIMIT 1;
        IF v_newest IS NOT NULL
           AND v_newest > v_now - make_interval(secs => v_win_buf) THEN
            RETURN;
        END IF;
    END IF;

    -- Subscription seeding on first contact of a (partition, group). RUSTFIX
    -- item 13: (b) exclude __QUEUE_MODE__ (a queue-mode pop carrying
    -- sub_mode='new' must never skip backlog); and consult the DURABLE
    -- subscription record first, so a partition created AFTER the group
    -- subscribed seeds from the stored subscription timestamp (delivering
    -- everything pushed after subscription) rather than last_offset, which
    -- would skip that partition's whole backlog.
    IF p_group <> '__QUEUE_MODE__'
       AND NOT EXISTS (SELECT 1 FROM queen.log_consumers c
                       WHERE c.partition_id = v_pid AND c.consumer_group = p_group) THEN
        -- Queue-scoped metadata is keyed by queue_id now (tenant scoping is
        -- inherited from the queues row the resolve already read).
        SELECT cgm.subscription_timestamp, cgm.conflation
        INTO v_from_ts, v_conflate_stored
        FROM queen.consumer_groups_metadata cgm
        WHERE cgm.consumer_group = p_group AND cgm.queue_id = v_qid
          AND cgm.partition_name = ''
        LIMIT 1;

        -- CONFLATION (§2.3 step 2, closes M4). A group that only ever uses the
        -- PINNED route never went through the wildcard/discover registration, so
        -- it had no durable group row and no place to hang a delivery policy —
        -- which for conflation is fatal: a second consumer could never learn it.
        -- Write the same row the wildcard path writes (:612-615), carrying the
        -- pop-derived sub_mode/sub_from, then read the STORED conflation back so
        -- a concurrent registrar's value wins, never the requested one.
        --
        -- DELIBERATE NARROWING vs the plan's literal text: this runs only for a
        -- CONFLATING pop. That row doubles as the group-first-contact BULK SEED
        -- marker (db::group_policy_lookup / AppState::group_seeded), and
        -- this branch does NOT run that set-based seed — writing the marker
        -- unconditionally would flip the broker's targeted/ring fast path on for
        -- every pinned-pop-only group, ahead of the seed the marker exists to
        -- prove (the 51e50c4 anti-convoy invariant). Gating on p_conflate keeps
        -- every flag-off deployment byte-identical (§8 "default off"), which is
        -- the stronger promise; the per-partition advisory-lock guard below is
        -- still the correctness floor for a conflating group that mixes routes.
        IF v_from_ts IS NULL AND v_conflate THEN
            -- Registration mode/timestamp: the SAME decision the wildcard path
            -- makes (:596-610), verbatim, so a group registered here and one
            -- registered there are indistinguishable afterwards.
            IF COALESCE(p_sub_from, '') <> '' AND COALESCE(p_sub_from, '') <> 'now' THEN
                BEGIN
                    v_reg_ts := p_sub_from::timestamptz;
                    v_reg_mode := 'timestamp';
                EXCEPTION WHEN OTHERS THEN
                    v_reg_ts := v_now; v_reg_mode := 'new';  -- unparsable: registration-time 'new'
                END;
            ELSIF COALESCE(p_sub_mode, 'all') = 'new' OR COALESCE(p_sub_from, '') = 'now' THEN
                v_reg_ts := v_now; v_reg_mode := 'new';
            ELSE
                v_reg_ts := '1970-01-01 00:00:00+00'::timestamptz; v_reg_mode := 'all';
            END IF;

            INSERT INTO queen.consumer_groups_metadata
                (tenant_id, consumer_group, queue_id, partition_name,
                 subscription_mode, subscription_timestamp, conflation)
            VALUES (p_tenant, p_group, v_qid, '', v_reg_mode, v_reg_ts, v_conflate)
            ON CONFLICT (tenant_id, consumer_group, queue_id, partition_name, namespace, task)
            DO NOTHING;
            GET DIAGNOSTICS v_cgm_seen = ROW_COUNT;
            -- Re-read only when somebody else won the race; our own insert is
            -- known to carry v_conflate. NB v_from_ts is deliberately left NULL
            -- either way: the seeding decision below must stay the pop-carried
            -- one this call already had, or a 'new' subscription could seed from
            -- a timestamp we just stamped instead of from last_offset.
            IF v_cgm_seen = 0 THEN
                SELECT cgm.conflation INTO v_conflate_stored
                FROM queen.consumer_groups_metadata cgm
                WHERE cgm.consumer_group = p_group AND cgm.queue_id = v_qid
                  AND cgm.partition_name = ''
                LIMIT 1;
                v_conflate := COALESCE(v_conflate_stored, v_conflate);
            END IF;
        ELSIF v_from_ts IS NOT NULL THEN
            -- Registered group: the STORED policy wins for every consumer of it
            -- (§1.1), so a stale broker cache can never conflate a plain group
            -- (or vice versa) on this partition's first contact.
            v_conflate := COALESCE(v_conflate_stored, v_conflate);
        END IF;

        IF v_from_ts IS NOT NULL THEN
            -- Durable subscription: cursor lands just BEFORE the first segment
            -- at/after the stored registration timestamp (created_at monotone
            -- in base_offset; the walk starts at the retention watermark, never
            -- at offset 0 — no dead-head index scan). Nothing that recent yet
            -- → future-only cursor (= last_offset).
            SELECT s.base_offset - 1 INTO v_seed
            FROM queen.log_segments s
            WHERE s.partition_id = v_pid
              AND s.base_offset >= v_log_start
              AND s.created_at >= v_from_ts
            ORDER BY s.base_offset LIMIT 1;
            IF v_seed IS NULL THEN
                v_seed := v_last_offset;
            END IF;
        ELSIF COALESCE(p_sub_mode, 'all') = 'new' OR COALESCE(p_sub_from, '') <> '' THEN
            -- No durable record (a direct single-partition pop that never went
            -- through the wildcard/discover registration): fall back to the
            -- pop-carried subscription intent.
            IF p_sub_from <> '' AND p_sub_from <> 'now' THEN
                BEGIN
                    v_from_ts := p_sub_from::timestamptz;
                EXCEPTION WHEN OTHERS THEN
                    v_from_ts := NULL;  -- unparsable: ignore, v1 does the same
                END;
                IF v_from_ts IS NOT NULL THEN
                    SELECT s.base_offset - 1 INTO v_seed
                    FROM queen.log_segments s
                    WHERE s.partition_id = v_pid
                      AND s.base_offset >= v_log_start
                      AND s.created_at >= v_from_ts
                    ORDER BY s.base_offset LIMIT 1;
                    IF v_seed IS NULL THEN
                        v_seed := v_last_offset;
                    END IF;
                END IF;
            ELSIF p_sub_from = 'now' OR p_sub_mode = 'new' THEN
                -- 'new' (or 'now') seed: skip the entire existing backlog.
                v_seed := v_last_offset;
            END IF;
        END IF;
    END IF;

    -- Claim: skip if another worker holds a live lease (or the row is being
    -- popped concurrently — SKIP LOCKED keeps concurrent pops non-blocking).
    -- NB (the deadlock lesson, ported from the retired seg pop procedures):
    -- we do NOT unconditionally
    -- INSERT the consumer row on every pop. An INSERT ... ON CONFLICT DO
    -- NOTHING per pop makes concurrent pops serialize on the inserter's
    -- transactionid (ShareLock), and because the wildcard pop claims several
    -- partitions per transaction IN RANDOM ORDER, those waits form cycles —
    -- the "deadlock detected" storms that stalled every high-concurrency run.
    -- Claim first; create the row only on the first pop for this
    -- (partition, group).
    SELECT c.committed INTO v_committed
    FROM queen.log_consumers c
    WHERE c.partition_id = v_pid AND c.consumer_group = p_group
      AND (c.worker_id IS NULL OR c.lease_expires_at IS NULL OR c.lease_expires_at < v_now)
    FOR UPDATE SKIP LOCKED;
    IF NOT FOUND THEN
        -- Distinguish "row missing" (create it, seeded) from "present but
        -- leased/locked" (skip, non-blocking). Only the missing case touches
        -- ON CONFLICT, so the steady-state hot path never contends.
        PERFORM 1 FROM queen.log_consumers
        WHERE partition_id = v_pid AND consumer_group = p_group;
        IF FOUND THEN RETURN; END IF;
        -- Non-blocking creation guard (2026-07-23, the mass-creation livelock):
        -- ON CONFLICT DO NOTHING on a key being inserted by ANOTHER open
        -- transaction waits on that transaction's xid until it COMMITS — and
        -- the caller here is often a multi-candidate wildcard pop whose txn
        -- lives for the whole candidate loop. Under mass partition creation
        -- (thousands of new (partition, group) pairs at once) those full-txn
        -- waits chain: measured 200+ backends queued on Lock:transactionid
        -- with pop txns minutes old and commit/s ~ 0. No cycle, so the
        -- deadlock detector never fires — it's a livelock. The xact-scoped
        -- advisory TRY-lock serializes creation per key WITHOUT waiting:
        -- exactly one pop creates the row, every concurrent pop skips this
        -- partition for one cycle (the backlog is retried next pop). A hash
        -- collision only false-skips an unrelated partition for one cycle.
        IF NOT pg_try_advisory_xact_lock(
                   hashtextextended(v_pid::text || '/' || p_group, 42)) THEN
            RETURN;
        END IF;
        INSERT INTO queen.log_consumers (partition_id, consumer_group, committed)
        VALUES (v_pid, p_group, COALESCE(v_seed, -1))
        ON CONFLICT DO NOTHING;
        SELECT c.committed INTO v_committed
        FROM queen.log_consumers c
        WHERE c.partition_id = v_pid AND c.consumer_group = p_group
          AND (c.worker_id IS NULL OR c.lease_expires_at IS NULL OR c.lease_expires_at < v_now)
        FOR UPDATE SKIP LOCKED;
        IF NOT FOUND THEN RETURN; END IF;
    END IF;

    v_wanted := v_committed + 1;

    -- delayed_processing: only segments at least v_delayed seconds old are
    -- visible. Monotone created_at ⇒ the filter cuts a contiguous suffix; the
    -- scan stops at the budget before ever reaching it, and the filtered tail
    -- is bounded by rate × delayed_processing.
    IF v_delayed > 0 THEN
        v_deadline := v_now - make_interval(secs => v_delayed);
    END IF;

    -- Head probe (§6): one BACKWARD PK step for the greatest base_offset <=
    -- wanted. If that segment covers `wanted` the scan starts mid-segment at
    -- start_idx = wanted - base_offset. If it does not cover it (fully
    -- consumed head, or the cursor sits in a retention gap) the next segment
    -- MUST have base_offset > wanted — ranges are disjoint, so any segment with
    -- base <= wanted other than the probed one is even further below — and the
    -- forward scan below finds it starting at frame 0. This replaces the seg
    -- engine's "offset applies only to the exact cursor segment" rule with
    -- pure range arithmetic (no msg_count normalization lookups).
    IF v_conflate THEN
        -- CONFLATION (§1.2/§2.3 step 3). ONE backward PK step: the newest
        -- VISIBLE segment. delayed_processing is applied HERE (not in a walk,
        -- because there is no walk); created_at is monotone in base_offset, so
        -- the deferred set is a contiguous SUFFIX and a filtered backward scan
        -- lands exactly on the newest visible offset. Committing to it can
        -- therefore never skip an offset that becomes visible later — everything
        -- not yet visible sits strictly above it (§1.3).
        SELECT s.base_offset, s.end_offset, s.created_at, s.blob
        INTO v_head
        FROM queen.log_segments s
        WHERE s.partition_id = v_pid
          AND (v_deadline IS NULL OR s.created_at <= v_deadline)
        ORDER BY s.base_offset DESC LIMIT 1;

        IF v_head.base_offset IS NOT NULL AND v_head.end_offset >= v_wanted THEN
            r_base       := v_head.base_offset;
            r_start_idx  := (v_head.end_offset - v_head.base_offset)::int;
            r_take       := 1;
            r_msg_count  := (v_head.end_offset - v_head.base_offset + 1)::int;
            r_created_at := v_head.created_at;
            r_blob       := v_head.blob;
            RETURN NEXT;

            v_taken := 1;
            v_start := v_wanted;              -- EPISODE ANCHOR (§1.4), not v_last
            v_last  := v_head.end_offset;     -- batch_end = commit_to
        END IF;
    ELSE
    SELECT s.base_offset, s.end_offset, s.created_at, s.blob
    INTO v_head
    FROM queen.log_segments s
    WHERE s.partition_id = v_pid AND s.base_offset <= v_wanted
    ORDER BY s.base_offset DESC LIMIT 1;

    IF v_head.base_offset IS NOT NULL AND v_head.end_offset >= v_wanted
       AND (v_deadline IS NULL OR v_head.created_at <= v_deadline) THEN
        v_avail := v_head.end_offset - v_wanted + 1;
        v_take  := LEAST(v_avail, v_budget)::int;

        r_base       := v_head.base_offset;
        r_start_idx  := (v_wanted - v_head.base_offset)::int;
        r_take       := v_take;
        r_msg_count  := (v_head.end_offset - v_head.base_offset + 1)::int;
        r_created_at := v_head.created_at;
        r_blob       := v_head.blob;
        RETURN NEXT;

        v_taken := v_take;
        v_start := v_wanted;
        v_last  := v_wanted + v_take - 1;
    END IF;

    -- Forward scan: every segment strictly after `wanted` starts at frame 0.
    -- The plpgsql FOR-query loop is a cursor: EXIT at budget stops the index
    -- walk, so a deep backlog costs only the delivered rows.
    IF v_taken < v_budget THEN
        FOR v_row IN
            SELECT s.base_offset, s.end_offset, s.created_at, s.blob
            FROM queen.log_segments s
            WHERE s.partition_id = v_pid
              AND s.base_offset > v_wanted
              AND (v_deadline IS NULL OR s.created_at <= v_deadline)
            ORDER BY s.base_offset
        LOOP
            v_avail := v_row.end_offset - v_row.base_offset + 1;
            v_take  := LEAST(v_avail, v_budget - v_taken)::int;

            r_base       := v_row.base_offset;
            r_start_idx  := 0;
            r_take       := v_take;
            r_msg_count  := v_avail::int;
            r_created_at := v_row.created_at;
            r_blob       := v_row.blob;
            RETURN NEXT;

            v_taken := v_taken + v_take;
            IF v_start IS NULL THEN
                v_start := v_row.base_offset;   -- retention gap: batch starts here
            END IF;
            v_last := v_row.base_offset + v_take - 1;
            EXIT WHEN v_taken >= v_budget;
        END LOOP;
    END IF;
    END IF;

    IF v_taken = 0 THEN
        -- Empty-partition cursor seal (2026-07-30; port of the C++
        -- pop_unified_batch_v4 starvation fix). A partition whose segments
        -- were ALL removed by retention keeps last_offset > committed forever
        -- (this empty return used to write nothing), so the (partition, group)
        -- row stays "phantom pending": a permanent wildcard candidate, and —
        -- through the v_claimed=0 re-check's identical predicate — it pins the
        -- (queue, group) empty-scan watermark at epoch, forcing full
        -- partition-set scans every long-poll. Seal the cursor to the
        -- partition row's last_offset snapshot, ONLY when the partition holds
        -- NO segments at all, UNFILTERED: v_taken = 0 also happens when
        -- segments exist but are deferred (delayed_processing deadline), and
        -- sealing past those would skip live messages. Race with a concurrent
        -- push is safe: offsets are monotone, so anything pushed after our
        -- v_last_offset snapshot stays above the seal and remains pending.
        -- No lease/attempt fields are touched — this is a cursor seal, not a
        -- claim; the row lock is already held (the FOR UPDATE claim above).
        -- Nested IF: the caught-up common case (committed == last_offset)
        -- must not even pay the EXISTS probe — parked long-polls stay
        -- read-only on the segments PK.
        IF v_last_offset > v_committed THEN
            IF NOT EXISTS (SELECT 1 FROM queen.log_segments s
                           WHERE s.partition_id = v_pid) THEN
                UPDATE queen.log_consumers SET
                    committed = v_last_offset
                WHERE partition_id = v_pid AND consumer_group = p_group;
            END IF;
        END IF;
        RETURN;
    END IF;

    IF p_auto_ack THEN
        -- auto-ack: commit the whole delivery in the same transaction; no
        -- lease, and the attempt state is untouched (auto-ack never
        -- redelivers — retired seg pop parity).
        -- CONFLATION (§2.3 step 5): an auto-acking conflating pop commits
        -- committed = v_last = tail in-transaction, which is the right thing;
        -- there is no lease to describe, so the marker is cleared.
        UPDATE queen.log_consumers SET
            committed = v_last,
            worker_id = NULL, lease_expires_at = NULL, lease_acquired_at = NULL,
            batch_end = NULL,
            lease_conflated = FALSE,
            total_consumed = total_consumed + v_taken
        WHERE partition_id = v_pid AND consumer_group = p_group;
    ELSE
        -- Lease the batch (committed, v_last]. attempt tracking (see header):
        -- same start offset as the previous non-auto-ack delivery = the cursor
        -- did not move since then (nack / lease expiry) = redelivery → charge
        -- attempt_count; anywhere else = fresh work → reset to 1. IS NOT
        -- DISTINCT FROM: a NULL attempt_offset (fresh row, or reset by the ack
        -- path on batch completion) never matches — fresh batch.
        UPDATE queen.log_consumers SET
            worker_id = p_worker,
            lease_expires_at = v_now + make_interval(secs => p_lease_seconds),
            lease_acquired_at = v_now,
            batch_end = v_last,
            lease_conflated = v_conflate,
            attempt_count = CASE WHEN attempt_offset IS NOT DISTINCT FROM v_start
                                 THEN attempt_count + 1 ELSE 1 END,
            attempt_offset = v_start
        WHERE partition_id = v_pid AND consumer_group = p_group;
    END IF;
END;
$$;

-- ----------------------------------------------------------------------------
-- log_pop_specific_v1: single-partition pop wire assembly, moved INSIDE a
-- VOLATILE plpgsql function so the partitionId resolution runs in a LATER
-- command than the claim (queen.log_pop_v1) and therefore under a snapshot
-- taken AFTER it. The previous shape (db::pop_specific assembling the JSON in
-- the outer statement) resolved partitionId under the OUTER statement's
-- snapshot: a pop statement that begins just before a concurrent push commits
-- the partition's creation, but whose log_pop_v1 internals (fresh snapshots,
-- READ COMMITTED) run just after that commit, delivered segments with
-- "partitionId":"" — which the JS client's mandatory-partitionId ack guard
-- rejects with a throw ("Message must have partitionId property..."; the
-- js/ha consumerGroupWithPartition 149/150 failure, 2026-07-31).
-- Snapshot monotonicity makes this ordering airtight: anything the claim saw
-- committed is visible to every later command of this function.
-- CONFLATION (§2.3): p_conflate threaded straight through to log_pop_v1.
DROP FUNCTION IF EXISTS queen.log_pop_specific_v1(TEXT, TEXT, TEXT, INTEGER, INTEGER, TEXT, BOOLEAN, TEXT, TEXT, UUID);
DROP FUNCTION IF EXISTS queen.log_pop_specific_v1(TEXT, TEXT, TEXT, INTEGER, INTEGER, TEXT, BOOLEAN, TEXT, TEXT, UUID, BOOLEAN);
CREATE FUNCTION queen.log_pop_specific_v1(
    p_queue TEXT,
    p_partition TEXT,
    p_group TEXT,
    p_budget INTEGER,
    p_lease_seconds INTEGER,
    p_worker TEXT,
    p_auto_ack BOOLEAN,
    p_sub_mode TEXT,
    p_sub_from TEXT,
    p_tenant UUID,
    p_conflate BOOLEAN DEFAULT FALSE
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_segments JSONB;
    v_pid UUID;
BEGIN
    -- Claim first (same call db::pop_specific made, p_skip_window_debounce FALSE).
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'seq', r_base,
               'startOff', r_start_idx,
               'take', r_take,
               'msgCount', r_msg_count,
               'createdAt', to_char(r_created_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
               'blob', encode(r_blob, 'base64')
           ) ORDER BY r_base), '[]'::jsonb)
    INTO v_segments
    FROM queen.log_pop_v1(p_queue, p_partition, p_group, p_budget, p_lease_seconds,
                          p_worker, p_auto_ack, p_sub_mode, p_sub_from, FALSE, p_tenant,
                          p_conflate);

    -- Resolve the partition id AFTER the claim: this command's snapshot is at
    -- least as new as any snapshot the claim used, so a delivery can never be
    -- rendered with an empty partitionId.
    SELECT p.id INTO v_pid
    FROM queen.log_partitions p JOIN queen.queues q ON q.id = p.queue_id
    WHERE q.name = p_queue AND p.name = p_partition AND q.tenant_id = p_tenant;

    -- Same wire shape db::pop_specific assembled ("attempt" is shape compat).
    RETURN jsonb_build_object(
        'segments', v_segments,
        'partitionId', COALESCE(v_pid::text, ''),
        'attempt', 0);
END;
$$;

-- ============================================================================
-- Wildcard pop: claim frames from several partitions of a queue in one call.
-- Ported verbatim from the retired seg_pop_wildcard_wire_v1 on offsets. Candidate
-- filter is COARSE on purpose (no consumer row yet, OR last_offset >
-- committed): queen.log_pop_v1 re-verifies the cursor and the lease under the
-- row lock, so a stale candidate just yields zero rows. Randomized candidate
-- order (not by name) so 200 consumers spread across partitions instead of
-- convoying on the low names; SKIP LOCKED inside log_pop_v1 keeps them
-- non-blocking. All claimed partitions share p_worker, mirroring v1
-- pop_unified_batch_v4's one-lease-id-per-batch semantics.
-- Returns {"partitions":[{"partition":name,"partitionId":uuid,
--                         "segments":[{seq,startOff,take,msgCount,createdAt,
--                                      blob(b64)}, ...]}, ...]}
-- (`seq` carries base_offset — opaque to clients, §11).
-- p_sub_mode / p_sub_from are forwarded to queen.log_pop_v1 (subscription
-- seeding on first contact per partition); they default to the no-seeding
-- values so callers of a historical shorter form keep working.
-- ============================================================================
-- Track B (§5): p_tenant scopes the queue resolution + the per-(queue,group)
-- watermark/metadata rows. Added last with a DEFAULT. Current-signature DROP =
-- boot-idempotency half of DROP+CREATE (see log_pop_v1's header).
-- CONFLATION (§2.3): p_conflate is stored on the registration row and forwarded
-- to every per-candidate log_pop_v1 call.
DROP FUNCTION IF EXISTS queen.log_pop_wildcard_wire_v1(TEXT, TEXT, INTEGER, INTEGER, TEXT, BOOLEAN, INTEGER, TEXT, TEXT, UUID);
DROP FUNCTION IF EXISTS queen.log_pop_wildcard_wire_v1(TEXT, TEXT, INTEGER, INTEGER, TEXT, BOOLEAN, INTEGER, TEXT, TEXT, UUID, BOOLEAN);
CREATE FUNCTION queen.log_pop_wildcard_wire_v1(
    p_queue TEXT,
    p_group TEXT,
    p_budget INTEGER,
    p_lease_seconds INTEGER,
    p_worker TEXT,
    p_auto_ack BOOLEAN,
    p_max_partitions INTEGER,
    p_sub_mode TEXT DEFAULT 'all',
    p_sub_from TEXT DEFAULT '',
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001',
    p_conflate BOOLEAN DEFAULT FALSE
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
    -- Queue identity is the queen.queues id (log_queues is gone). A group may
    -- legitimately subscribe BEFORE any push creates the queue: this pop writes
    -- a durable queue-scoped consumer_groups_metadata row below, so on the
    -- missing case UPSERT the queues row first (same provisioning insert as the
    -- push path, namespace/task derived from the dotted name) — the FK must not
    -- turn an early subscription into an error. Kept inside the missing branch
    -- only, so the steady-state pop never touches ON CONFLICT.
    SELECT id INTO v_qid FROM queen.queues WHERE name = p_queue AND tenant_id = p_tenant;
    IF v_qid IS NULL AND COALESCE(p_queue, '') <> '' THEN
        INSERT INTO queen.queues (tenant_id, name, namespace, task)
        VALUES (p_tenant, p_queue,
                split_part(p_queue, '.', 1),
                CASE WHEN position('.' in p_queue) > 0 THEN split_part(p_queue, '.', 2) ELSE '' END)
        ON CONFLICT (tenant_id, name) DO NOTHING;
        SELECT id INTO v_qid FROM queen.queues WHERE name = p_queue AND tenant_id = p_tenant;
    END IF;
    IF v_qid IS NULL THEN
        RETURN jsonb_build_object('partitions', '[]'::jsonb);
    END IF;

    -- Group-first-contact bulk seed (extends RUSTFIX item 13). The wildcard/
    -- discover pop claims several partitions per transaction IN RANDOM ORDER, and
    -- log_pop_v1's claim-first dance lazily does INSERT INTO queen.log_consumers
    -- ... ON CONFLICT DO NOTHING on the FIRST contact of each (partition, group).
    -- Under many concurrent consumers of a FRESH queue that first-contact insert
    -- convoys: a waiter blocks on the inserter's transactionid (ShareLock), and
    -- because every backend holds several such pending inserts across partitions
    -- claimed in random order, the waits form CHAINS — and chains (unlike cycles)
    -- are never broken by the deadlock detector, so backends wedge for minutes.
    --
    -- Fix: on the FIRST contact of a (queue, group), seed the cursor of EVERY
    -- partition once, set-based, behind a SINGLE-ROW metadata marker. The other
    -- callers then wait briefly on ONE transactionid (the marker-row conflict, a
    -- fan-in — not a chain) and proceed; log_pop_v1's per-partition lazy creation
    -- stays as the fallback (specific-partition pops, and partitions created after
    -- the seed), rare by construction.
    --
    -- This now runs for the DEFAULT 'all' mode and for '__QUEUE_MODE__' too — the
    -- storm the fix targets is many wildcard consumers of ONE (default) group on a
    -- fresh queue, i.e. exactly sub_mode='all' / __QUEUE_MODE__ (the goload/
    -- wildcard path uses '__QUEUE_MODE__' when no group is set). RUSTFIX item 13
    -- had bootstrapped only the 'new'/timestamp subscriptions.
    --
    -- Marker: the consumer_groups_metadata row (UNIQUE(tenant, consumer_group,
    -- queue_id, partition_name='', namespace, task) — queue identity is the
    -- queues id now); its first insertion (ROW_COUNT>0)
    -- elects the single seeder. It is the RIGHT marker precisely because item 12's
    -- empty-scan path also writes consumer_watermarks, so watermark existence
    -- cannot mean "seeded". __QUEUE_MODE__ is admitted here: log_pop_v1's lazy
    -- seeding branch explicitly ignores __QUEUE_MODE__ metadata rows (no delivery
    -- side effect), and the admin/stats views derive groups from the consumers
    -- tables (metadata is enrichment-only), so no phantom group appears.
    --
    -- Seed value per mode — delivery-IDENTICAL to log_pop_v1's lazy first contact:
    --   'all' / __QUEUE_MODE__ : committed = GREATEST(log_start-1, -1) → next
    --        wanted lands on the oldest retained offset (or 0), never skipping
    --        backlog; matches the lazy default committed=-1.
    --   'new' / 'now'          : committed = last_offset (skip the whole backlog).
    --   'timestamp'            : committed = first-seg-at/after-ts.base_offset - 1,
    --        else last_offset.
    -- subscription_timestamp is NOT NULL, so 'all' stores the epoch sentinel: the
    -- "new mode" lag enrichment reads it as "everything is lag" (== all-mode), and
    -- a LATER-created partition's lazy timestamp-walk in log_pop_v1
    -- (created_at >= epoch) lands on its first retained segment → deliver
    -- everything, identical to committed=-1.
    IF p_group = '__QUEUE_MODE__' THEN
        v_sub_mode_stored := 'all';
        v_from_ts := '1970-01-01 00:00:00+00'::timestamptz;
    ELSIF COALESCE(p_sub_from, '') <> '' AND COALESCE(p_sub_from, '') <> 'now' THEN
        BEGIN
            v_from_ts := p_sub_from::timestamptz;
            v_sub_mode_stored := 'timestamp';
        EXCEPTION WHEN OTHERS THEN
            v_from_ts := v_now; v_sub_mode_stored := 'new';  -- unparsable: registration-time 'new'
        END;
    ELSIF COALESCE(p_sub_mode, 'all') = 'new' OR COALESCE(p_sub_from, '') = 'now' THEN
        v_from_ts := v_now; v_sub_mode_stored := 'new';
    ELSE
        v_from_ts := '1970-01-01 00:00:00+00'::timestamptz; v_sub_mode_stored := 'all';
    END IF;

    -- CONFLATION (§2.3): the declared policy is persisted on this FIRST
    -- registration and never re-negotiated — ON CONFLICT DO NOTHING is exactly
    -- "group-setting-wins" (§3.3); the broker detects and reports the conflict.
    INSERT INTO queen.consumer_groups_metadata
        (tenant_id, consumer_group, queue_id, partition_name, subscription_mode,
         subscription_timestamp, conflation)
    VALUES (p_tenant, p_group, v_qid, '', v_sub_mode_stored, v_from_ts,
            COALESCE(p_conflate, FALSE))
    ON CONFLICT (tenant_id, consumer_group, queue_id, partition_name, namespace, task) DO NOTHING;
    GET DIAGNOSTICS v_first_seen = ROW_COUNT;

    IF v_first_seen > 0 THEN
        IF v_sub_mode_stored = 'timestamp' THEN
            -- timestamp seed: cursor just before the first segment at/after
            -- v_from_ts per partition (created_at monotone in base_offset →
            -- forward walk from the retention watermark), else last_offset
            -- (nothing that recent yet → future-only cursor).
            INSERT INTO queen.log_consumers (partition_id, consumer_group, committed)
            SELECT p.id, p_group,
                   COALESCE(
                       (SELECT s.base_offset - 1 FROM queen.log_segments s
                        WHERE s.partition_id = p.id
                          AND s.base_offset >= p.log_start
                          AND s.created_at >= v_from_ts
                        ORDER BY s.base_offset LIMIT 1),
                       p.last_offset)
            FROM queen.log_partitions p
            WHERE p.queue_id = v_qid
            ON CONFLICT (partition_id, consumer_group) DO NOTHING;
        ELSIF v_sub_mode_stored = 'new' THEN
            -- 'new' (or 'now') seed: skip the entire existing backlog.
            INSERT INTO queen.log_consumers (partition_id, consumer_group, committed)
            SELECT p.id, p_group, p.last_offset
            FROM queen.log_partitions p
            WHERE p.queue_id = v_qid
            ON CONFLICT (partition_id, consumer_group) DO NOTHING;
        ELSE
            -- 'all' (default) / __QUEUE_MODE__: deliver the full retained backlog.
            -- committed = GREATEST(log_start-1, -1) → next wanted is the oldest
            -- retained offset (or 0); delivery-identical to the lazy default -1.
            INSERT INTO queen.log_consumers (partition_id, consumer_group, committed)
            SELECT p.id, p_group, GREATEST(p.log_start - 1, -1)
            FROM queen.log_partitions p
            WHERE p.queue_id = v_qid
            ON CONFLICT (partition_id, consumer_group) DO NOTHING;
        END IF;
    END IF;

    -- Empty-scan watermark: only consider partitions written since this group
    -- last found the queue empty. A caught-up consumer skips cold partitions
    -- instead of scanning the whole partition set every long-poll (decisive at
    -- 10-20k partitions). RUSTFIX item 12: also read updated_at as the 30s
    -- re-check throttle floor.
    -- Watermarks are keyed by (queue_id, group) now — tenant scoping is
    -- inherited from the queues row.
    SELECT last_empty_scan_at, updated_at INTO v_watermark, v_verified_at
    FROM queen.consumer_watermarks
    WHERE queue_id = v_qid AND consumer_group = p_group;
    IF v_watermark IS NULL THEN
        v_watermark := '1970-01-01 00:00:00+00'::timestamptz;
        v_verified_at := NULL;
    END IF;

    -- Fetch more candidates than requested: some will be claimed by concurrent
    -- pops or turn out empty by the time we call log_pop_v1.
    -- (>=128 short-circuits to the 512 ceiling BEFORE multiplying: the
    -- uncapped sentinel is INT_MAX and the seg-era `v_max_parts * 4` formula
    -- overflowed int32 on it — same result, no overflow.)
    v_cand_cap := CASE WHEN v_max_parts >= 128 THEN 512
                       ELSE LEAST(GREATEST(v_max_parts * 4, 64), 512) END;

    -- Candidate set: hot (write watermark, with 2 minutes of slack absorbing
    -- the push-side last_write_at quantization), possibly-pending
    -- (last_offset > committed), lease-free, written since the empty-scan
    -- floor.
    FOR v_p IN
        SELECT p.id, p.name
        FROM queen.log_partitions p
        LEFT JOIN queen.log_consumers c
          ON c.partition_id = p.id AND c.consumer_group = p_group
        WHERE p.queue_id = v_qid
          AND p.last_write_at >= v_watermark - interval '2 minutes'
          AND (c.partition_id IS NULL OR p.last_offset > c.committed)
          AND (c.worker_id IS NULL OR c.lease_expires_at IS NULL
               OR c.lease_expires_at < v_now)
        ORDER BY random()
        LIMIT v_cand_cap
    LOOP
        -- Same row shape / keys as the seg wire pop (§11): `seq` carries
        -- base_offset, `startOff` the start frame index.
        SELECT COALESCE(jsonb_agg(jsonb_build_object(
                   'seq', r_base,
                   'startOff', r_start_idx,
                   'take', r_take,
                   'msgCount', r_msg_count,
                   'createdAt', to_char(r_created_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                   'blob', encode(r_blob, 'base64')
               ) ORDER BY r_base), '[]'::jsonb),
               COALESCE(SUM(r_take), 0)
        INTO v_segments, v_taken
        FROM queen.log_pop_v1(p_queue, v_p.name, p_group,
                              v_remaining, p_lease_seconds, p_worker, p_auto_ack,
                              p_sub_mode, p_sub_from, FALSE, p_tenant, p_conflate);

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

    -- Nothing claimable this cycle. RUSTFIX item 12: DO NOT advance the
    -- empty-scan watermark blindly — a partition may have been skipped only
    -- because another worker held its lease, not because it is empty.
    -- Throttled to 30s, re-check for pending data IGNORING the lease filter
    -- (parity with the retired rows-era pop_unified_v4's re-check block):
    -- advance only if genuinely
    -- empty; otherwise just record the verification time so a lease-held
    -- backlog is never stranded once the lease expires with no new push.
    IF v_claimed = 0 THEN
        IF v_verified_at IS NULL OR v_verified_at <= v_now - interval '30 seconds' THEN
            SELECT EXISTS (
                SELECT 1 FROM queen.log_partitions p
                LEFT JOIN queen.log_consumers c
                  ON c.partition_id = p.id AND c.consumer_group = p_group
                WHERE p.queue_id = v_qid
                  AND p.last_write_at >= v_watermark - interval '2 minutes'
                  AND (c.partition_id IS NULL OR p.last_offset > c.committed)
            ) INTO v_has_pending;
            IF NOT v_has_pending THEN
                INSERT INTO queen.consumer_watermarks
                    (queue_id, consumer_group, last_empty_scan_at, updated_at)
                VALUES (v_qid, p_group, v_now, v_now)
                ON CONFLICT (queue_id, consumer_group)
                DO UPDATE SET last_empty_scan_at = EXCLUDED.last_empty_scan_at,
                              updated_at = EXCLUDED.updated_at;
            ELSE
                -- Pending backlog exists (possibly lease-held): record the
                -- check time only, keep the floor so the partition stays a
                -- candidate.
                INSERT INTO queen.consumer_watermarks
                    (queue_id, consumer_group, last_empty_scan_at, updated_at)
                VALUES (v_qid, p_group, '1970-01-01 00:00:00+00'::timestamptz, v_now)
                ON CONFLICT (queue_id, consumer_group)
                DO UPDATE SET updated_at = EXCLUDED.updated_at;
            END IF;
        END IF;
    END IF;

    RETURN jsonb_build_object('partitions', v_out);
END;
$$;

-- ============================================================================
-- Binary wildcard pop (broker-internal): same claim algorithm as
-- log_pop_wildcard_wire_v1 above, but segment blobs are returned as a NATIVE
-- bytea[] instead of base64 text inside the JSON — no encode() on the PG side,
-- no whitespace-stripping + base64 decode on the broker side, ~25% fewer bytes
-- on the wire. `meta` carries the partition/segment metadata WITHOUT blobs
-- (and without seq/msgCount, which the broker renderer never reads — parity
-- with the retired seg_pop_wildcard_wire_v1); `blobs` is flattened in
-- traversal order (partitions in claim order,
-- segments in base_offset order — both aggregates below share
-- ORDER BY r_base, keeping them aligned).
-- KEEP THE CLAIM LOGIC IN SYNC with log_pop_wildcard_wire_v1.
-- ============================================================================
-- Track B (§5): p_tenant, as in log_pop_wildcard_wire_v1. Current-signature
-- DROP = boot-idempotency half of DROP+CREATE (see log_pop_v1's header).
-- CONFLATION (§2.3): as in log_pop_wildcard_wire_v1 — stored on registration,
-- forwarded to every per-candidate claim.
DROP FUNCTION IF EXISTS queen.log_pop_wildcard_bin_v1(TEXT, TEXT, INTEGER, INTEGER, TEXT, BOOLEAN, INTEGER, TEXT, TEXT, UUID);
DROP FUNCTION IF EXISTS queen.log_pop_wildcard_bin_v1(TEXT, TEXT, INTEGER, INTEGER, TEXT, BOOLEAN, INTEGER, TEXT, TEXT, UUID, BOOLEAN);
CREATE FUNCTION queen.log_pop_wildcard_bin_v1(
    p_queue TEXT,
    p_group TEXT,
    p_budget INTEGER,
    p_lease_seconds INTEGER,
    p_worker TEXT,
    p_auto_ack BOOLEAN,
    p_max_partitions INTEGER,
    p_sub_mode TEXT DEFAULT 'all',
    p_sub_from TEXT DEFAULT '',
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001',
    p_conflate BOOLEAN DEFAULT FALSE
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
    -- Queue resolve + subscription-before-queue provisioning — identical to
    -- log_pop_wildcard_wire_v1 (queue identity is the queen.queues id; the
    -- durable metadata row below needs the queues row to exist).
    SELECT id INTO v_qid FROM queen.queues WHERE name = p_queue AND tenant_id = p_tenant;
    IF v_qid IS NULL AND COALESCE(p_queue, '') <> '' THEN
        INSERT INTO queen.queues (tenant_id, name, namespace, task)
        VALUES (p_tenant, p_queue,
                split_part(p_queue, '.', 1),
                CASE WHEN position('.' in p_queue) > 0 THEN split_part(p_queue, '.', 2) ELSE '' END)
        ON CONFLICT (tenant_id, name) DO NOTHING;
        SELECT id INTO v_qid FROM queen.queues WHERE name = p_queue AND tenant_id = p_tenant;
    END IF;
    IF v_qid IS NULL THEN
        meta := jsonb_build_object('partitions', '[]'::jsonb);
        blobs := '{}'::bytea[];
        RETURN NEXT;
        RETURN;
    END IF;

    -- Group-first-contact bulk seed — identical mechanism to
    -- log_pop_wildcard_wire_v1 (see its header for the full convoy rationale):
    -- extends RUSTFIX item 13 to the DEFAULT 'all' mode and to '__QUEUE_MODE__',
    -- seeding EVERY partition once behind the single-row consumer_groups_metadata
    -- marker so the first-contact log_consumers insert never convoys.
    IF p_group = '__QUEUE_MODE__' THEN
        v_sub_mode_stored := 'all';
        v_from_ts := '1970-01-01 00:00:00+00'::timestamptz;
    ELSIF COALESCE(p_sub_from, '') <> '' AND COALESCE(p_sub_from, '') <> 'now' THEN
        BEGIN
            v_from_ts := p_sub_from::timestamptz;
            v_sub_mode_stored := 'timestamp';
        EXCEPTION WHEN OTHERS THEN
            v_from_ts := v_now; v_sub_mode_stored := 'new';
        END;
    ELSIF COALESCE(p_sub_mode, 'all') = 'new' OR COALESCE(p_sub_from, '') = 'now' THEN
        v_from_ts := v_now; v_sub_mode_stored := 'new';
    ELSE
        v_from_ts := '1970-01-01 00:00:00+00'::timestamptz; v_sub_mode_stored := 'all';
    END IF;

    -- CONFLATION (§2.3): the declared policy is persisted on this FIRST
    -- registration and never re-negotiated — ON CONFLICT DO NOTHING is exactly
    -- "group-setting-wins" (§3.3); the broker detects and reports the conflict.
    INSERT INTO queen.consumer_groups_metadata
        (tenant_id, consumer_group, queue_id, partition_name, subscription_mode,
         subscription_timestamp, conflation)
    VALUES (p_tenant, p_group, v_qid, '', v_sub_mode_stored, v_from_ts,
            COALESCE(p_conflate, FALSE))
    ON CONFLICT (tenant_id, consumer_group, queue_id, partition_name, namespace, task) DO NOTHING;
    GET DIAGNOSTICS v_first_seen = ROW_COUNT;

    IF v_first_seen > 0 THEN
        IF v_sub_mode_stored = 'timestamp' THEN
            INSERT INTO queen.log_consumers (partition_id, consumer_group, committed)
            SELECT p.id, p_group,
                   COALESCE(
                       (SELECT s.base_offset - 1 FROM queen.log_segments s
                        WHERE s.partition_id = p.id
                          AND s.base_offset >= p.log_start
                          AND s.created_at >= v_from_ts
                        ORDER BY s.base_offset LIMIT 1),
                       p.last_offset)
            FROM queen.log_partitions p
            WHERE p.queue_id = v_qid
            ON CONFLICT (partition_id, consumer_group) DO NOTHING;
        ELSIF v_sub_mode_stored = 'new' THEN
            INSERT INTO queen.log_consumers (partition_id, consumer_group, committed)
            SELECT p.id, p_group, p.last_offset
            FROM queen.log_partitions p
            WHERE p.queue_id = v_qid
            ON CONFLICT (partition_id, consumer_group) DO NOTHING;
        ELSE
            -- 'all' / __QUEUE_MODE__: committed = GREATEST(log_start-1, -1) →
            -- full retained backlog, delivery-identical to the lazy default -1.
            INSERT INTO queen.log_consumers (partition_id, consumer_group, committed)
            SELECT p.id, p_group, GREATEST(p.log_start - 1, -1)
            FROM queen.log_partitions p
            WHERE p.queue_id = v_qid
            ON CONFLICT (partition_id, consumer_group) DO NOTHING;
        END IF;
    END IF;

    -- Watermarks are keyed by (queue_id, group) now — tenant scoping is
    -- inherited from the queues row.
    SELECT last_empty_scan_at, updated_at INTO v_watermark, v_verified_at
    FROM queen.consumer_watermarks
    WHERE queue_id = v_qid AND consumer_group = p_group;
    IF v_watermark IS NULL THEN
        v_watermark := '1970-01-01 00:00:00+00'::timestamptz;
        v_verified_at := NULL;
    END IF;

    -- (>=128 short-circuits to the 512 ceiling BEFORE multiplying: the
    -- uncapped sentinel is INT_MAX and the seg-era `v_max_parts * 4` formula
    -- overflowed int32 on it — same result, no overflow.)
    v_cand_cap := CASE WHEN v_max_parts >= 128 THEN 512
                       ELSE LEAST(GREATEST(v_max_parts * 4, 64), 512) END;

    FOR v_p IN
        SELECT p.id, p.name
        FROM queen.log_partitions p
        LEFT JOIN queen.log_consumers c
          ON c.partition_id = p.id AND c.consumer_group = p_group
        WHERE p.queue_id = v_qid
          AND p.last_write_at >= v_watermark - interval '2 minutes'
          AND (c.partition_id IS NULL OR p.last_offset > c.committed)
          AND (c.worker_id IS NULL OR c.lease_expires_at IS NULL
               OR c.lease_expires_at < v_now)
        ORDER BY random()
        LIMIT v_cand_cap
    LOOP
        -- Blobs go to the aligned bytea[]; meta keeps what the broker renderer
        -- reads (startOff/take/createdAt) PLUS `seq` (= base_offset), which the
        -- ACK REGISTRY needs to derive the leased batch_end broker-side
        -- (batch_end = seq + startOff + take - 1 of the last segment; see
        -- server/src/ack_registry.rs). `seq` is internal broker↔PG meta — the
        -- client never sees it — so adding it changes no wire contract; blobs
        -- stay position-aligned (both aggregates still ORDER BY r_base).
        SELECT COALESCE(jsonb_agg(jsonb_build_object(
                   'seq', r_base,
                   'startOff', r_start_idx,
                   'take', r_take,
                   'createdAt', to_char(r_created_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
               ) ORDER BY r_base), '[]'::jsonb),
               COALESCE(SUM(r_take), 0),
               COALESCE(array_agg(r_blob ORDER BY r_base), '{}'::bytea[])
        INTO v_segments, v_taken, v_part_blobs
        FROM queen.log_pop_v1(p_queue, v_p.name, p_group,
                              v_remaining, p_lease_seconds, p_worker, p_auto_ack,
                              p_sub_mode, p_sub_from, FALSE, p_tenant, p_conflate);

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

    -- Empty-scan watermark maintenance — identical to log_pop_wildcard_wire_v1
    -- (RUSTFIX item 12: 30s-throttled, lease-ignoring re-check).
    IF v_claimed = 0 THEN
        IF v_verified_at IS NULL OR v_verified_at <= v_now - interval '30 seconds' THEN
            SELECT EXISTS (
                SELECT 1 FROM queen.log_partitions p
                LEFT JOIN queen.log_consumers c
                  ON c.partition_id = p.id AND c.consumer_group = p_group
                WHERE p.queue_id = v_qid
                  AND p.last_write_at >= v_watermark - interval '2 minutes'
                  AND (c.partition_id IS NULL OR p.last_offset > c.committed)
            ) INTO v_has_pending;
            IF NOT v_has_pending THEN
                INSERT INTO queen.consumer_watermarks
                    (queue_id, consumer_group, last_empty_scan_at, updated_at)
                VALUES (v_qid, p_group, v_now, v_now)
                ON CONFLICT (queue_id, consumer_group)
                DO UPDATE SET last_empty_scan_at = EXCLUDED.last_empty_scan_at,
                              updated_at = EXCLUDED.updated_at;
            ELSE
                INSERT INTO queen.consumer_watermarks
                    (queue_id, consumer_group, last_empty_scan_at, updated_at)
                VALUES (v_qid, p_group, '1970-01-01 00:00:00+00'::timestamptz, v_now)
                ON CONFLICT (queue_id, consumer_group)
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
-- Namespace/task discovery pop. Ported whole from the retired
-- seg_pop_discover_wire_v1 on offsets. Client parity: the JS/Go/Py clients expose
-- `client.queue().namespace_name(ns).consume(...)` (no queue in the path),
-- which issues a bare `GET /api/v1/pop?namespace=&task=&consumerGroup=...`.
-- This pops across ALL log queues whose queen.queues row matches the requested
-- namespace/task, returning the SAME wire shape as log_pop_wildcard_wire_v1.
--
-- This is log_pop_wildcard_wire_v1 with a broader candidate set: instead of
-- one queue's partitions, the candidates are every log_partitions row whose
-- queen.queues row (queue identity is the queues id — p.queue_id = q.id)
-- matches the namespace/task filter. Everything else — the per-partition queen.log_pop_v1 loop, the
-- shared p_worker/one-lease-id-per-batch, budget, and the consumer_watermarks
-- empty-scan floor — is identical, so ack / leaseId behave exactly as for a
-- queue-scoped pop.
--
-- Filter: (p_namespace='' OR qq.namespace=p_namespace)
--     AND (p_task='' OR qq.task=p_task). At least one of namespace/task is
-- expected (the broker 400s when both are empty); an all-empty call here would
-- match every log queue, which is a harmless (if broad) superset.
--
-- Per-partition lease: candidates span queues that may carry different
-- leaseTime, so each partition is leased with ITS queue's queues.lease_time
-- (falling back to p_lease_seconds when unset) rather than one global value.
--
-- Empty-scan floor: the canonical queen.consumer_watermarks rows are keyed by
-- (queue_id, group) — same rows the queue-scoped wildcard pop uses. The
-- floor for the candidate scan is the OLDEST last_empty_scan_at across the
-- matching queues (a partition written since ANY matching queue last went
-- empty is still a candidate — never wrongly skipped); when the whole scope
-- yields nothing, all matching queues' watermarks advance to now.
-- ============================================================================
-- Track B (§5): p_tenant scopes the namespace/task queue set + watermarks/metadata.
-- Current-signature DROP = boot-idempotency half of DROP+CREATE (see
-- log_pop_v1's header).
-- CONFLATION (§2.3): as in the wildcard SPs — stored on every matched queue's
-- registration row, forwarded to every per-candidate claim.
DROP FUNCTION IF EXISTS queen.log_pop_discover_wire_v1(TEXT, TEXT, TEXT, INTEGER, INTEGER, TEXT, BOOLEAN, INTEGER, TEXT, TEXT, UUID);
DROP FUNCTION IF EXISTS queen.log_pop_discover_wire_v1(TEXT, TEXT, TEXT, INTEGER, INTEGER, TEXT, BOOLEAN, INTEGER, TEXT, TEXT, UUID, BOOLEAN);
CREATE FUNCTION queen.log_pop_discover_wire_v1(
    p_namespace TEXT,
    p_task TEXT,
    p_group TEXT,
    p_budget INTEGER,
    p_lease_seconds INTEGER,
    p_worker TEXT,
    p_auto_ack BOOLEAN,
    p_max_partitions INTEGER,
    p_sub_mode TEXT DEFAULT 'all',
    p_sub_from TEXT DEFAULT '',
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001',
    p_conflate BOOLEAN DEFAULT FALSE
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_ns TEXT := COALESCE(p_namespace, '');
    v_task TEXT := COALESCE(p_task, '');
    v_q RECORD;
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
    -- Group-first-contact bulk seed (mirrors log_pop_wildcard_wire_v1; see its
    -- header for the full convoy rationale). Extends RUSTFIX item 13 to the
    -- DEFAULT 'all' mode and to '__QUEUE_MODE__': on the FIRST contact of a
    -- (queue, group) seed the cursor of EVERY partition of EVERY matching queue
    -- once, set-based, behind the single-row consumer_groups_metadata marker, so
    -- log_pop_v1's per-partition first-contact insert never convoys across the
    -- randomly-ordered candidates. The mode/timestamp are decided once, then each
    -- matching queue's marker+seed runs in the loop (a queue matching later still
    -- seeds on the discover pop that first reaches it).
    IF p_group = '__QUEUE_MODE__' THEN
        v_sub_mode_stored := 'all';
        v_from_ts := '1970-01-01 00:00:00+00'::timestamptz;
    ELSIF COALESCE(p_sub_from, '') <> '' AND COALESCE(p_sub_from, '') <> 'now' THEN
        BEGIN
            v_from_ts := p_sub_from::timestamptz;
            v_sub_mode_stored := 'timestamp';
        EXCEPTION WHEN OTHERS THEN
            v_from_ts := v_now; v_sub_mode_stored := 'new';  -- unparsable: registration-time 'new'
        END;
    ELSIF COALESCE(p_sub_mode, 'all') = 'new' OR COALESCE(p_sub_from, '') = 'now' THEN
        v_from_ts := v_now; v_sub_mode_stored := 'new';
    ELSE
        v_from_ts := '1970-01-01 00:00:00+00'::timestamptz; v_sub_mode_stored := 'all';
    END IF;

    -- Queue identity is the queues id (log_queues is gone): the matching set is
    -- enumerated straight off queen.queues. No provisioning here — a queue that
    -- matches the namespace/task filter exists by definition.
    FOR v_q IN
        SELECT qq.id AS qid, qq.name AS qname
        FROM queen.queues qq
        WHERE qq.tenant_id = p_tenant
          AND (v_ns = '' OR qq.namespace = v_ns)
          AND (v_task = '' OR qq.task = v_task)
    LOOP
        -- CONFLATION (§2.3): declared policy persisted on first registration of
        -- THIS queue; group-setting-wins afterwards (ON CONFLICT DO NOTHING).
        INSERT INTO queen.consumer_groups_metadata
            (tenant_id, consumer_group, queue_id, partition_name, subscription_mode,
             subscription_timestamp, conflation)
        VALUES (p_tenant, p_group, v_q.qid, '', v_sub_mode_stored, v_from_ts,
                COALESCE(p_conflate, FALSE))
        ON CONFLICT (tenant_id, consumer_group, queue_id, partition_name, namespace, task) DO NOTHING;
        GET DIAGNOSTICS v_first_seen = ROW_COUNT;

        IF v_first_seen > 0 THEN
            IF v_sub_mode_stored = 'timestamp' THEN
                -- timestamp seed: cursor just before the first segment
                -- at/after v_from_ts per partition, else last_offset
                -- (identical to the lazy seed in queen.log_pop_v1).
                INSERT INTO queen.log_consumers (partition_id, consumer_group, committed)
                SELECT p.id, p_group,
                       COALESCE(
                           (SELECT s.base_offset - 1 FROM queen.log_segments s
                            WHERE s.partition_id = p.id
                              AND s.base_offset >= p.log_start
                              AND s.created_at >= v_from_ts
                            ORDER BY s.base_offset LIMIT 1),
                           p.last_offset)
                FROM queen.log_partitions p
                WHERE p.queue_id = v_q.qid
                ON CONFLICT (partition_id, consumer_group) DO NOTHING;
            ELSIF v_sub_mode_stored = 'new' THEN
                -- 'new' (or 'now') seed: skip the entire existing backlog.
                INSERT INTO queen.log_consumers (partition_id, consumer_group, committed)
                SELECT p.id, p_group, p.last_offset
                FROM queen.log_partitions p
                WHERE p.queue_id = v_q.qid
                ON CONFLICT (partition_id, consumer_group) DO NOTHING;
            ELSE
                -- 'all' (default) / __QUEUE_MODE__: committed =
                -- GREATEST(log_start-1, -1) → full retained backlog,
                -- delivery-identical to log_pop_v1's lazy default -1.
                INSERT INTO queen.log_consumers (partition_id, consumer_group, committed)
                SELECT p.id, p_group, GREATEST(p.log_start - 1, -1)
                FROM queen.log_partitions p
                WHERE p.queue_id = v_q.qid
                ON CONFLICT (partition_id, consumer_group) DO NOTHING;
            END IF;
        END IF;
    END LOOP;

    -- Empty-scan floor: the OLDEST last_empty_scan_at across matching queues
    -- (missing rows count as the epoch ⇒ full scan). Only consider partitions
    -- written since some matching queue last found itself empty.
    -- RUSTFIX item 12: also compute the throttle floor = MIN(updated_at)
    -- across matching queues (missing rows ⇒ epoch ⇒ re-check every poll,
    -- acceptable).
    SELECT MIN(COALESCE(cw.last_empty_scan_at, '1970-01-01 00:00:00+00'::timestamptz)),
           MIN(COALESCE(cw.updated_at, '1970-01-01 00:00:00+00'::timestamptz))
    INTO v_watermark, v_verified_at
    FROM queen.queues qq
    LEFT JOIN queen.consumer_watermarks cw
      ON cw.queue_id = qq.id AND cw.consumer_group = p_group
    WHERE qq.tenant_id = p_tenant
      AND (v_ns = '' OR qq.namespace = v_ns)
      AND (v_task = '' OR qq.task = v_task);
    IF v_watermark IS NULL THEN
        v_watermark := '1970-01-01 00:00:00+00'::timestamptz;
        v_verified_at := NULL;
    END IF;

    -- Fetch more candidates than requested: some will be claimed by concurrent
    -- pops or turn out empty by the time we call log_pop_v1.
    -- (>=128 short-circuits to the 512 ceiling BEFORE multiplying: the
    -- uncapped sentinel is INT_MAX and the seg-era `v_max_parts * 4` formula
    -- overflowed int32 on it — same result, no overflow.)
    v_cand_cap := CASE WHEN v_max_parts >= 128 THEN 512
                       ELSE LEAST(GREATEST(v_max_parts * 4, 64), 512) END;

    -- Candidate set: matching queues' partitions that are hot, possibly
    -- pending (last_offset > committed), lease-free, written since the
    -- empty-scan floor. Randomized order (not by name) so many concurrent
    -- poppers spread across partitions. lease_time is carried per-partition so
    -- each queue's configured leaseTime is honored even when a single
    -- discovery pop spans several queues.
    FOR v_p IN
        SELECT p.id, p.name AS pname, qq.name AS qname,
               -- RUSTFIX item 18: request override (p_lease_seconds>0) wins per
               -- partition, else the queue's own lease_time, else the 60s floor.
               COALESCE(NULLIF(p_lease_seconds, 0), qq.lease_time, 60) AS lease_time,
               -- CONFLATION (§3.3): a discovery pop spans QUEUES, so the broker
               -- has no single (queue, group) policy to cache — SQL resolves it
               -- here instead, per matched queue, from the durable registration
               -- row this call has just ensured exists. The request flag is the
               -- fallback only for a queue with no row yet.
               COALESCE(m.conflation, COALESCE(p_conflate, FALSE)) AS conflate
        FROM queen.log_partitions p
        JOIN queen.queues qq ON qq.id = p.queue_id
        LEFT JOIN queen.consumer_groups_metadata m
          ON m.queue_id = qq.id AND m.consumer_group = p_group
         AND m.partition_name = ''
        LEFT JOIN queen.log_consumers c
          ON c.partition_id = p.id AND c.consumer_group = p_group
        WHERE qq.tenant_id = p_tenant
          AND (v_ns = '' OR qq.namespace = v_ns)
          AND (v_task = '' OR qq.task = v_task)
          AND p.last_write_at >= v_watermark - interval '2 minutes'
          AND (c.partition_id IS NULL OR p.last_offset > c.committed)
          AND (c.worker_id IS NULL OR c.lease_expires_at IS NULL
               OR c.lease_expires_at < v_now)
        ORDER BY random()
        LIMIT v_cand_cap
    LOOP
        -- Same row shape / keys as log_pop_wildcard_wire_v1.
        SELECT COALESCE(jsonb_agg(jsonb_build_object(
                   'seq', r_base,
                   'startOff', r_start_idx,
                   'take', r_take,
                   'msgCount', r_msg_count,
                   'createdAt', to_char(r_created_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                   'blob', encode(r_blob, 'base64')
               ) ORDER BY r_base), '[]'::jsonb),
               COALESCE(SUM(r_take), 0)
        INTO v_segments, v_taken
        FROM queen.log_pop_v1(v_p.qname, v_p.pname, p_group,
                              v_remaining, v_p.lease_time, p_worker, p_auto_ack,
                              p_sub_mode, p_sub_from, FALSE, p_tenant, v_p.conflate);

        IF v_taken > 0 THEN
            v_out := v_out || jsonb_build_object(
                'partition', v_p.pname,
                'partitionId', v_p.id,
                'segments', v_segments);
            v_claimed := v_claimed + 1;   -- only partitions that yielded rows
            v_remaining := v_remaining - v_taken;
            EXIT WHEN v_remaining <= 0 OR v_claimed >= v_max_parts;
        END IF;
    END LOOP;

    -- Nothing claimable this cycle. RUSTFIX item 12: throttle (30s) + re-check
    -- for pending data IGNORING the lease filter across matching queues before
    -- advancing the empty-scan floor, so a lease-held backlog is never
    -- stranded once the lease expires with no new push (parity with the
    -- retired rows-era pop_unified_v4's re-check block).
    IF v_claimed = 0 THEN
        IF v_verified_at IS NULL OR v_verified_at <= v_now - interval '30 seconds' THEN
            SELECT EXISTS (
                SELECT 1 FROM queen.log_partitions p
                JOIN queen.queues qq ON qq.id = p.queue_id
                LEFT JOIN queen.log_consumers c
                  ON c.partition_id = p.id AND c.consumer_group = p_group
                WHERE qq.tenant_id = p_tenant
                  AND (v_ns = '' OR qq.namespace = v_ns)
                  AND (v_task = '' OR qq.task = v_task)
                  AND p.last_write_at >= v_watermark - interval '2 minutes'
                  AND (c.partition_id IS NULL OR p.last_offset > c.committed)
            ) INTO v_has_pending;
            IF NOT v_has_pending THEN
                -- Multi-row upsert: ORDER BY the conflict key (queue_id) so
                -- concurrent discover pops take the row locks in one order.
                INSERT INTO queen.consumer_watermarks
                    (queue_id, consumer_group, last_empty_scan_at, updated_at)
                SELECT qq.id, p_group, v_now, v_now
                FROM queen.queues qq
                WHERE qq.tenant_id = p_tenant
                  AND (v_ns = '' OR qq.namespace = v_ns)
                  AND (v_task = '' OR qq.task = v_task)
                ORDER BY qq.id
                ON CONFLICT (queue_id, consumer_group)
                DO UPDATE SET last_empty_scan_at = EXCLUDED.last_empty_scan_at,
                              updated_at = EXCLUDED.updated_at;
            ELSE
                -- Pending backlog exists (possibly lease-held): record the
                -- check time only, keep the floor so the partitions stay
                -- candidates.
                UPDATE queen.consumer_watermarks cw SET updated_at = v_now
                FROM queen.queues qq
                WHERE cw.queue_id = qq.id AND cw.consumer_group = p_group
                  AND qq.tenant_id = p_tenant
                  AND (v_ns = '' OR qq.namespace = v_ns)
                  AND (v_task = '' OR qq.task = v_task);
            END IF;
        END IF;
    END IF;

    RETURN jsonb_build_object('partitions', v_out);
END;
$$;

-- ============================================================================
-- Pending probe: does any partition of the queue have frames the group has
-- not consumed yet? Trivial with offsets: last_offset > committed IS the
-- pending predicate — no head msg_count refinement needed (the seg engine
-- paid a PK lookup to compare next_off vs msg_count; a single-offset cursor
-- makes that arithmetic vanish). A missing consumer row counts as
-- committed = -1 (full backlog pending), so a never-touched partition with
-- any data (last_offset >= 0) reports true. Coarse on the retention-gap case
-- (may report true when retention already ate the backlog) — same contract as
-- seg_has_pending_v1: the pop resolves the truth under the row lock.
-- ============================================================================
-- Track B (§5): p_tenant scopes the queue resolution.
CREATE OR REPLACE FUNCTION queen.log_has_pending_v1(
    p_queue TEXT, p_group TEXT,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001')
RETURNS BOOLEAN
LANGUAGE sql STABLE
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM queen.log_partitions p
        -- queue identity is the queen.queues id (log_queues is gone)
        JOIN queen.queues q ON q.id = p.queue_id AND q.name = p_queue AND q.tenant_id = p_tenant
        LEFT JOIN queen.log_consumers c
          ON c.partition_id = p.id AND c.consumer_group = p_group
        WHERE p.last_offset > COALESCE(c.committed, -1)
    );
$$;

-- ----------------------------------------------------------------------------
-- log_partition_has_pending_v1: the single-partition sibling of the probe
-- above, for the PINNED pop's long-poll gate (handlers/data.rs
-- handle_pop_specific). One indexed row read; no admission permit broker-side.
--
-- Semantics differ from the queue-wide probe in two deliberate ways:
--   * `log_start` participates: `last_offset > GREATEST(committed, log_start-1)`
--     is "retained undelivered frames exist" — exact, so a group that abandoned
--     a partition whose backlog retention already deleted probes FALSE instead
--     of paying the full pop on every re-poll forever (the queue-wide probe is
--     a deliberate superset and keeps its shape).
--   * a MISSING consumer row probes TRUE unconditionally. First contact of a
--     (partition, group) is when log_pop_v1 registers subscription intent and
--     seeds the cursor — and for sub_mode='new' the seed is the partition tail
--     AT SEED TIME, so deferring that first call until data appears would move
--     the seed past the push that woke us and silently skip it. TRUE forces
--     the full pop immediately (exactly today's first-contact timing); the
--     call creates the row even when it delivers nothing, so every later
--     re-poll takes the cheap exact branch.
--
-- A missing partition (or queue) probes TRUE: the full pop owns those
-- semantics (404 shape, provisioning), the gate must never preempt them.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.log_partition_has_pending_v1(
    p_queue TEXT, p_partition TEXT, p_group TEXT,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001')
RETURNS BOOLEAN
LANGUAGE sql STABLE
AS $$
    SELECT COALESCE((
        SELECT c.partition_id IS NULL
               OR p.last_offset > GREATEST(c.committed, p.log_start - 1)
        FROM queen.log_partitions p
        JOIN queen.queues q ON q.id = p.queue_id
             AND q.name = p_queue AND q.tenant_id = p_tenant
        LEFT JOIN queen.log_consumers c
          ON c.partition_id = p.id AND c.consumer_group = p_group
        WHERE p.name = p_partition
    ), TRUE);
$$;

GRANT EXECUTE ON FUNCTION queen.log_partition_has_pending_v1(TEXT, TEXT, TEXT, UUID) TO PUBLIC;

-- ----------------------------------------------------------------------------
-- log_discover_has_pending_v1: the namespace/task-scoped sibling, for the
-- DISCOVERY pop's long-poll gate (handlers/data.rs handle_pop_discover, which
-- spans every queue matching the namespace/task filter). Index-only walk of
-- the matched queues' partitions joined to the group's cursors.
--
-- Two arms, both required:
--   * retained undelivered frames under the worst case of a missing cursor row
--     (COALESCE -1, the queue-wide probe's superset semantics — a registered
--     group's unseeded partition seeds from its stored subscription timestamp,
--     so one eager full pop per partition is the worst cost);
--   * an UNREGISTERED group (no consumer_groups_metadata row for that queue)
--     probes TRUE even on a fully-empty queue: the first discovery pop is what
--     writes the durable registration timestamp, and that timestamp is the
--     seed anchor for every partition the group meets later — deferring it
--     until data exists would stamp the subscription AFTER the push that woke
--     us and skip it (sub_mode='new'). Group-less '__QUEUE_MODE__' skips this
--     arm: its cursors always seed at the head, which is deferral-safe.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.log_discover_has_pending_v1(
    p_namespace TEXT, p_task TEXT, p_group TEXT,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001')
RETURNS BOOLEAN
LANGUAGE sql STABLE
AS $$
    -- Arm 1: any matched queue where the group holds no durable registration
    -- yet — INDEPENDENT of partitions, so a matched queue with zero partitions
    -- (subscribe-before-first-push) still forces the registering pop through.
    SELECT EXISTS (
        SELECT 1
        FROM queen.queues q
        WHERE q.tenant_id = p_tenant
          AND (p_namespace = '' OR q.namespace = p_namespace)
          AND (p_task = '' OR q.task = p_task)
          AND p_group <> '__QUEUE_MODE__'
          AND NOT EXISTS (
              SELECT 1 FROM queen.consumer_groups_metadata m
              WHERE m.queue_id = q.id
                AND m.consumer_group = p_group
                AND m.partition_name = '')
    )
    -- Arm 2: retained undelivered frames, worst-case on a missing cursor row
    -- (a registered group's unseeded partition seeds from its stored
    -- subscription timestamp, so one eager full pop per partition is the
    -- worst cost of the superset).
    OR EXISTS (
        SELECT 1
        FROM queen.queues q
        JOIN queen.log_partitions p ON p.queue_id = q.id
        LEFT JOIN queen.log_consumers c
          ON c.partition_id = p.id AND c.consumer_group = p_group
        WHERE q.tenant_id = p_tenant
          AND (p_namespace = '' OR q.namespace = p_namespace)
          AND (p_task = '' OR q.task = p_task)
          AND p.last_offset > GREATEST(COALESCE(c.committed, -1), p.log_start - 1)
    );
$$;

GRANT EXECUTE ON FUNCTION queen.log_discover_has_pending_v1(TEXT, TEXT, TEXT, UUID) TO PUBLIC;

-- ============================================================================
-- 19-wildcard-hotlist §7/§9 — candidate-list pop (broker hot-list serve path).
--
-- The broker's in-memory hot-list (server/src/hotlist.rs) hands this the K
-- candidate partition NAMES it pulled from the (queue, group) ring, INSTEAD of
-- the SQL candidate scan (log_partitions ⋈ log_consumers, ORDER BY random()).
-- The claim core is BATCHED (see the IMPLEMENTATION block below); first-contact
-- candidates still route through queen.log_pop_v1. What is NEW is a TRI-STATE
-- verdict per candidate so the broker maintains the ring with NO second probe
-- (§7 — the lease_expires_at is already on the consumer row the claim read):
--
--   took   — log_pop_v1 returned >0 frames (segments in `meta`, blobs aligned);
--   empty  — 0 frames AND no live foreign lease AND no pending backlog in this
--            snapshot → the broker CAS-clears the ring entry (epoch-guarded,
--            §4), or on a deferral queue schedules a revisit instead (the
--            broker knows the queue config, never clears). A 0-frame candidate
--            whose snapshot STILL shows pending rows is NOT empty: the claim
--            was lock-skipped by a concurrent pop transaction (SKIP LOCKED /
--            advisory guard) — an autoAck winner exposes no lease, so this
--            snapshot check is the only discriminator. Reporting 'empty' there
--            let the broker clear a backlogged partition (the epoch-CAS passes
--            whenever every push predates the group ring, e.g. push-then-drain)
--            and strand it until the reseed floor — the 2026-07-30 bench-smoke
--            25-message tail stall. Such a candidate reports 'leased' with a
--            short horizon instead → bounded re-probe, never a clear;
--   leased — 0 frames because ANOTHER worker holds a live lease until T → the
--            broker parks the entry in the wheel at T + pad (NEVER cleared, §7).
--
-- p_skip_window (§6) forwards to log_pop_v1's p_skip_window_debounce so a
-- windowBuffer queue's visibility is immediate here and the broker wheel owns
-- the hold. delayed_processing stays SQL-enforced (never skipped).
--
-- Returns ONE row: meta = {"partitions":[{partition,partitionId,segments}]}
-- (bin shape — blobs out-of-band, IDENTICAL to log_pop_wildcard_bin_v1 so the
-- broker's render_pop_parts is unchanged); blobs = aligned bytea[]; states =
-- JSONB [{"p":name,"s":"took|empty|leased","until":iso?}] for every EVALUATED
-- candidate (a budget-exhausted tail is omitted → the broker re-appends anything
-- it sent that got no verdict). NO group-first-contact bulk seed here: the
-- broker only routes a (queue,group) to this path AFTER the wildcard/reseed path
-- carried the seed (st.group_seeded gate), so the per-partition lazy insert in
-- log_pop_v1 never storms (the 51e50c4 anti-convoy invariant is preserved).
-- ============================================================================
-- Track B (§5): p_tenant scopes the queue resolution + the per-candidate pops.
-- Current-signature DROP = boot-idempotency half of DROP+CREATE (see
-- log_pop_v1's header).
-- ----------------------------------------------------------------------------
-- IMPLEMENTATION (2026-08-02 rewrite — the contract above is UNCHANGED: same
-- signature, same meta/blobs/states wire shape, same tri-state semantics).
--
-- The previous body FOREACHed the candidates calling queen.log_pop_v1 once per
-- partition: ~6 statement executions per partition (queue re-join, seeding
-- probe, claim, head probe, forward scan, lease UPDATE) plus a per-partition
-- jsonb_agg. Measured on the sparse-partition shape (1000 partitions, ~1 msg
-- each, pg_stat_statements track=all): 3.42 ms/pop of which ~0.17 ms is data
-- work — the rest per-partition statement overhead, paid INSIDE the pop
-- admission permit and the pop's WAL-committing transaction. That pinned the
-- broker at ~1.2k pops/s with the box idle. This body does the same work in
-- ~6 statements TOTAL, independent of the candidate count:
--
--   1. queue resolve ONCE (the knobs are per-queue; the old shape re-read them
--      once per partition);
--   2. ONE batched claim: FOR UPDATE OF c SKIP LOCKED over every requested
--      consumer row. The deadlock lesson holds — no INSERT here, and SKIP
--      LOCKED never waits, so claim order cannot form lock cycles;
--   3. ONE no-lock probe of what the claim did not take (missing rows /
--      foreign leases), feeding the tri-state verdicts byte-identically;
--   4. ONE batched segment-METADATA read (head probe + forward scan per
--      claimed partition, LATERAL over the segments PK). The blob column is
--      deliberately NOT selected — nothing detoasts here. delayed_processing
--      is applied in the walk: created_at is monotone in base_offset, so
--      deferred rows are a contiguous suffix and cutting in plpgsql is
--      equivalent to the old per-row SQL filter;
--   5. ONE batched lease (or auto-ack) UPDATE for the partitions actually
--      served — attempt tracking verbatim — RETURNING each partition's CURRENT
--      last_offset: a post-claim read, preserving the drained/lastOff compose
--      argument of the old per-partition re-read;
--   6. ONE blob fetch for exactly the chosen (partition, base) pairs, ordered
--      by selection ordinality = emission order, so the out-of-band blobs stay
--      positionally aligned with meta by construction.
--
-- Budget allocation still walks candidates in INPUT order with the same
-- running remainder and the same EXIT (budget / partition cap): which
-- partitions are served under a tight budget is unchanged, and a
-- budget-exhausted tail still gets NO state entry (checkin maps absent →
-- re-append, as before).
--
-- FIRST CONTACT (no consumer row) takes the old path: one queen.log_pop_v1
-- call for that partition — subscription seeding and the advisory-guarded row
-- creation (the 2026-07-23 mass-creation livelock fix) live THERE, not
-- duplicated here. Steady state never pays it.
--
-- ACCEPTED divergences from the FOREACH body (reviewed 2026-08-02):
--   * the batched claim locks every claimable requested row up-front, so rows
--     past a budget-exhaustion EXIT sit locked (no lease written) until
--     commit. Serve candidates come from take_batch, which hands DISJOINT sets
--     to concurrent pops, so the serve path never contends; a racing wildcard
--     pop sees SKIP LOCKED → its own short-horizon 'leased' verdict, the same
--     outcome any transient claim overlap already produced.
--   * with window_buffer active and p_skip_window unset, the quiet-partition
--     skip is decided AFTER the claim (the old body returned before claiming):
--     same verdict, one transiently-locked row wider.
--   * the states array is grouped by verdict phase rather than strict input
--     order; the broker builds a name-keyed map (data.rs checkin), so order
--     was never part of the contract. meta.partitions and the blob array keep
--     exact serve order — those ARE positional.
-- ----------------------------------------------------------------------------
-- CONFLATION (§2.3): p_conflate switches PASS 2's LATERAL to a single backward
-- step per candidate and the walk to a one-frame serve. The tri-state verdicts,
-- the lastOff RETURNING, the blob fetch and the assembly are UNTOUCHED, so
-- `drained` keeps working — and works better: a conflating serve sets
-- batch_end = tail, so `be >= lo` is true exactly when the partition is fully
-- retired for that group.
DROP FUNCTION IF EXISTS queen.log_pop_list_v1(TEXT, TEXT, TEXT[], INTEGER, INTEGER, TEXT, BOOLEAN, INTEGER, TEXT, TEXT, BOOLEAN, UUID);
DROP FUNCTION IF EXISTS queen.log_pop_list_v1(TEXT, TEXT, TEXT[], INTEGER, INTEGER, TEXT, BOOLEAN, INTEGER, TEXT, TEXT, BOOLEAN, UUID, BOOLEAN);
CREATE FUNCTION queen.log_pop_list_v1(
    p_queue TEXT,
    p_group TEXT,
    p_partitions TEXT[],
    p_budget INTEGER,
    p_lease_seconds INTEGER,
    p_worker TEXT,
    p_auto_ack BOOLEAN,
    p_max_partitions INTEGER,
    p_sub_mode TEXT DEFAULT 'all',
    p_sub_from TEXT DEFAULT '',
    p_skip_window BOOLEAN DEFAULT FALSE,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001',
    p_conflate BOOLEAN DEFAULT FALSE
) RETURNS TABLE(meta JSONB, blobs BYTEA[], states JSONB)
LANGUAGE plpgsql
AS $$
DECLARE
    -- Deduped candidates, first occurrence kept (filled in BEGIN). The old
    -- FOREACH was inherently duplicate-safe: the second claim of a name saw
    -- the fresh lease this transaction had just written and skipped. The
    -- cached-metadata walk below would double-serve instead, so duplicates
    -- are dropped up front (the hot-list ring cannot emit them anyway; a
    -- dropped duplicate simply gets no verdict → the broker re-appends).
    v_parts TEXT[];
    v_n INTEGER := 0;
    v_qid UUID;
    v_delayed INTEGER := 0;
    v_win_buf INTEGER := 0;
    v_deadline TIMESTAMPTZ;
    v_now TIMESTAMPTZ := clock_timestamp();
    v_remaining INTEGER := GREATEST(p_budget, 1);
    v_max_parts INTEGER := CASE WHEN p_max_partitions IS NULL OR p_max_partitions <= 0
                                THEN 2147483647 ELSE p_max_partitions END;
    v_claimed INTEGER := 0;

    -- 1:1 with p_partitions positions (ordinality-indexed writes; no searches).
    a_claimed BOOLEAN[];
    a_probed  BOOLEAN[];
    a_rowmiss BOOLEAN[];
    a_pid     UUID[];
    a_lastoff BIGINT[];
    a_comm    BIGINT[];
    a_lworker TEXT[];
    a_lexp    TIMESTAMPTZ[];
    a_newest  TIMESTAMPTZ[];

    -- segment metadata blocks, grouped by input ordinal; consumed by a moving
    -- pointer in the walk (both sides are in input order → O(total) pointer).
    m_ord  INTEGER[]     := '{}';
    m_base BIGINT[]      := '{}';
    m_end  BIGINT[]      := '{}';
    m_cre  TIMESTAMPTZ[] := '{}';
    -- CONFLATION only (§2.3): 1 = the newest VISIBLE segment (the servable
    -- tail), 0 = the seal-guard row that merely proves segments EXIST when
    -- delayed_processing hides every one of them.
    m_kind INTEGER[]     := '{}';
    m_len  INTEGER := 0;
    m_i    INTEGER := 1;

    -- served entries, in serve order. kind 'f' = fast (batched claim),
    -- 'w' = slow (first-contact via log_pop_v1, already leased in there).
    e_kind  TEXT[]    := '{}';
    e_ord   INTEGER[] := '{}';
    e_segj  JSONB[]   := '{}';
    e_pid   UUID[]    := '{}';
    -- fast entries: slice bounds into the batched blob fetch + lease inputs
    e_bfrom INTEGER[] := '{}';
    e_bcnt  INTEGER[] := '{}';
    e_start BIGINT[]  := '{}';
    e_last  BIGINT[]  := '{}';
    e_take  INTEGER[] := '{}';
    -- slow entries: blobs captured inline + lastOff read inline (old shape)
    e_sfrom INTEGER[] := '{}';
    e_scnt  INTEGER[] := '{}';
    e_lastoff BIGINT[] := '{}';   -- slow: filled inline; fast: from RETURNING

    slow_blobs BYTEA[] := '{}'::bytea[];

    -- flat blob selection for the fast path, in emission order
    sel_pid  UUID[]   := '{}';
    sel_base BIGINT[] := '{}';
    v_fast_blobs BYTEA[] := '{}'::bytea[];

    -- zero-taken claimed partitions: seal + fresh re-check batch
    z_ord     INTEGER[] := '{}';
    z_seal    BOOLEAN[] := '{}';
    z_pending BOOLEAN[] := '{}';

    v_out       JSONB := '[]'::jsonb;
    v_states    JSONB := '[]'::jsonb;
    v_all_blobs BYTEA[] := '{}'::bytea[];

    rec RECORD;
    i INTEGER;
    v_ord INTEGER;
    v_name TEXT;
    v_wanted BIGINT;
    v_pbudget INTEGER;
    v_taken INTEGER;
    v_take INTEGER;
    v_avail BIGINT;
    v_start BIGINT;
    v_last BIGINT;
    v_segj JSONB;
    v_has_rows BOOLEAN;
    v_bfrom INTEGER;
    v_segments JSONB;
    v_part_blobs BYTEA[];
    v_sp_taken INTEGER;
    v_pid2 UUID;
    v_lo2 BIGINT;
    v_flworker TEXT;
    v_flexp TIMESTAMPTZ;
    v_fpend BOOLEAN;
BEGIN
    SELECT array_agg(name ORDER BY ord)
    INTO v_parts
    FROM (SELECT DISTINCT ON (name) name, ord
          FROM unnest(p_partitions) WITH ORDINALITY AS t(name, ord)
          ORDER BY name, ord) d;
    v_n := COALESCE(array_length(v_parts, 1), 0);
    a_claimed := array_fill(FALSE, ARRAY[GREATEST(v_n, 1)]);
    a_probed  := array_fill(FALSE, ARRAY[GREATEST(v_n, 1)]);
    a_rowmiss := array_fill(FALSE, ARRAY[GREATEST(v_n, 1)]);
    a_pid     := array_fill(NULL::uuid, ARRAY[GREATEST(v_n, 1)]);
    a_lastoff := array_fill(NULL::bigint, ARRAY[GREATEST(v_n, 1)]);
    a_comm    := array_fill(NULL::bigint, ARRAY[GREATEST(v_n, 1)]);
    a_lworker := array_fill(NULL::text, ARRAY[GREATEST(v_n, 1)]);
    a_lexp    := array_fill(NULL::timestamptz, ARRAY[GREATEST(v_n, 1)]);
    a_newest  := array_fill(NULL::timestamptz, ARRAY[GREATEST(v_n, 1)]);
    IF v_n = 0 THEN
        meta := jsonb_build_object('partitions', '[]'::jsonb);
        blobs := '{}'::bytea[]; states := '[]'::jsonb;
        RETURN NEXT; RETURN;
    END IF;

    -- Queue identity + visibility knobs, ONCE for the whole candidate list.
    SELECT q.id, COALESCE(q.delayed_processing, 0), COALESCE(q.window_buffer, 0)
    INTO v_qid, v_delayed, v_win_buf
    FROM queen.queues q
    WHERE q.name = p_queue AND q.tenant_id = p_tenant;
    IF v_qid IS NULL THEN
        meta := jsonb_build_object('partitions', '[]'::jsonb);
        blobs := '{}'::bytea[]; states := '[]'::jsonb;
        RETURN NEXT; RETURN;
    END IF;
    IF v_delayed > 0 THEN
        v_deadline := v_now - make_interval(secs => v_delayed);
    END IF;

    -- ------------------------------------------------------------------
    -- PASS 1 — batched claim. INNER joins only (FOR UPDATE forbids the
    -- nullable side of an outer join); anything missing falls to the probe.
    -- ------------------------------------------------------------------
    FOR rec IN
        SELECT req.ord, p.id AS pid, p.last_offset, c.committed
        FROM unnest(v_parts) WITH ORDINALITY AS req(name, ord)
        JOIN queen.log_partitions p
          ON p.queue_id = v_qid AND p.name = req.name
        JOIN queen.log_consumers c
          ON c.partition_id = p.id AND c.consumer_group = p_group
        WHERE (c.worker_id IS NULL OR c.lease_expires_at IS NULL
               OR c.lease_expires_at < v_now)
        ORDER BY req.ord
        FOR UPDATE OF c SKIP LOCKED
    LOOP
        a_claimed[rec.ord] := TRUE;
        a_pid[rec.ord]     := rec.pid;
        a_lastoff[rec.ord] := rec.last_offset;
        a_comm[rec.ord]    := rec.committed;
    END LOOP;

    -- ------------------------------------------------------------------
    -- PASS 1b — no-lock probe of everything the claim did not take: missing
    -- consumer row (first contact → slow path), foreign lease, or lock-skip.
    -- Partition-row-missing candidates match nothing here and stay unprobed.
    -- ------------------------------------------------------------------
    FOR rec IN
        SELECT req.ord, p.id AS pid, p.last_offset,
               (c.partition_id IS NULL) AS rowmiss,
               c.committed, c.worker_id, c.lease_expires_at
        FROM unnest(v_parts) WITH ORDINALITY AS req(name, ord)
        JOIN queen.log_partitions p
          ON p.queue_id = v_qid AND p.name = req.name
        LEFT JOIN queen.log_consumers c
          ON c.partition_id = p.id AND c.consumer_group = p_group
        WHERE NOT a_claimed[req.ord]
        ORDER BY req.ord
    LOOP
        a_probed[rec.ord]  := TRUE;
        a_pid[rec.ord]     := rec.pid;
        a_lastoff[rec.ord] := rec.last_offset;
        a_comm[rec.ord]    := rec.committed;
        a_rowmiss[rec.ord] := rec.rowmiss;
        a_lworker[rec.ord] := rec.worker_id;
        a_lexp[rec.ord]    := rec.lease_expires_at;
    END LOOP;

    -- ------------------------------------------------------------------
    -- PASS 2 — segment METADATA for the claimed set: head probe (greatest
    -- base <= wanted, one backward PK step) + forward scan. No blob column.
    -- No deadline filter (walk applies it; deferred rows are a monotone
    -- suffix). Forward LIMIT = full budget: a segment holds >= 1 frame, so
    -- more rows can never be needed.
    -- ------------------------------------------------------------------
    IF p_conflate THEN
        -- CONFLATION (§2.3): ONE backward PK step per candidate for the newest
        -- VISIBLE segment. The second leg exists ONLY to preserve the seal
        -- decision when delayed_processing hides every segment (its `WHERE
        -- v_delayed > 0` makes it free otherwise): without it a fully-deferred
        -- partition would look segment-less and the zero-taken seal would jump
        -- the cursor past live messages. Ordered by (ord, kind) so the walk sees
        -- the servable row first.
        FOR rec IN
            SELECT req.ord, s.kind, s.base_offset, s.end_offset, s.created_at
            FROM unnest(v_parts) WITH ORDINALITY AS req(name, ord)
            CROSS JOIN LATERAL (
                (SELECT 1 AS kind, s1.base_offset, s1.end_offset, s1.created_at
                 FROM queen.log_segments s1
                 WHERE s1.partition_id = a_pid[req.ord]
                   AND (v_deadline IS NULL OR s1.created_at <= v_deadline)
                 ORDER BY s1.base_offset DESC LIMIT 1)
                UNION ALL
                -- seal guard only: proves segments EXIST even when all are deferred.
                (SELECT 0 AS kind, s0.base_offset, s0.end_offset, s0.created_at
                 FROM queen.log_segments s0
                 WHERE v_delayed > 0 AND s0.partition_id = a_pid[req.ord]
                 ORDER BY s0.base_offset DESC LIMIT 1)
            ) s
            WHERE a_claimed[req.ord]
            ORDER BY req.ord, s.kind DESC
        LOOP
            m_ord := m_ord || rec.ord;
            m_base := m_base || rec.base_offset;
            m_end := m_end || rec.end_offset;
            m_cre := m_cre || rec.created_at;
            m_kind := m_kind || rec.kind;
        END LOOP;
    ELSE
    FOR rec IN
        SELECT req.ord, s.base_offset, s.end_offset, s.created_at
        FROM unnest(v_parts) WITH ORDINALITY AS req(name, ord)
        CROSS JOIN LATERAL (
            (SELECT s1.base_offset, s1.end_offset, s1.created_at
             FROM queen.log_segments s1
             WHERE s1.partition_id = a_pid[req.ord]
               AND s1.base_offset <= a_comm[req.ord] + 1
             ORDER BY s1.base_offset DESC LIMIT 1)
            UNION ALL
            (SELECT s2.base_offset, s2.end_offset, s2.created_at
             FROM queen.log_segments s2
             WHERE s2.partition_id = a_pid[req.ord]
               AND s2.base_offset > a_comm[req.ord] + 1
             ORDER BY s2.base_offset LIMIT v_remaining)
        ) s
        WHERE a_claimed[req.ord]
        ORDER BY req.ord, s.base_offset
    LOOP
        m_ord := m_ord || rec.ord;
        m_base := m_base || rec.base_offset;
        m_end := m_end || rec.end_offset;
        m_cre := m_cre || rec.created_at;
        m_kind := m_kind || 1;
    END LOOP;
    END IF;
    m_len := COALESCE(array_length(m_ord, 1), 0);

    -- window_buffer newest-segment probe, only when the check is live.
    IF v_win_buf > 0 AND NOT p_skip_window THEN
        FOR rec IN
            SELECT req.ord, s.created_at
            FROM unnest(v_parts) WITH ORDINALITY AS req(name, ord)
            CROSS JOIN LATERAL (
                SELECT s3.created_at FROM queen.log_segments s3
                WHERE s3.partition_id = a_pid[req.ord]
                ORDER BY s3.base_offset DESC LIMIT 1
            ) s
            WHERE a_claimed[req.ord]
            ORDER BY req.ord
        LOOP
            a_newest[rec.ord] := rec.created_at;
        END LOOP;
    END IF;

    -- ------------------------------------------------------------------
    -- THE WALK — input order, running budget, same EXIT as the FOREACH body.
    -- Pure in-memory except the (rare) first-contact slow path.
    -- ------------------------------------------------------------------
    FOR v_ord IN 1..v_n LOOP
        EXIT WHEN v_remaining <= 0 OR v_claimed >= v_max_parts;
        v_name := v_parts[v_ord];

        IF a_claimed[v_ord] THEN
            -- window_buffer quiet-skip (claim-then-skip; header note): the old
            -- outcome for a fresh-write partition was taken=0 with segments
            -- present → pending → short-horizon 'leased'.
            IF v_win_buf > 0 AND NOT p_skip_window
               AND a_newest[v_ord] IS NOT NULL
               AND a_newest[v_ord] > v_now - make_interval(secs => v_win_buf) THEN
                v_states := v_states || jsonb_build_object(
                    'p', v_name, 's', 'leased',
                    'until', to_char(v_now + interval '250 milliseconds',
                                     'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
                WHILE m_i <= m_len AND m_ord[m_i] = v_ord LOOP
                    m_i := m_i + 1;   -- skip this partition's metadata block
                END LOOP;
                CONTINUE;
            END IF;

            v_wanted := a_comm[v_ord] + 1;
            v_pbudget := v_remaining;
            v_taken := 0; v_start := NULL; v_last := NULL;
            v_segj := '[]'::jsonb;
            v_has_rows := FALSE;
            v_bfrom := COALESCE(array_length(sel_pid, 1), 0) + 1;

            IF p_conflate THEN
                -- CONFLATION (§2.3): consume this partition's metadata block,
                -- serve AT MOST the kind=1 row (the newest visible segment) and
                -- exactly ONE frame of it — its last. Any row at all (including
                -- the kind=0 seal guard) proves segments exist, so the
                -- zero-taken seal below stays correct for a fully-deferred
                -- partition. Budget accounting and the EXIT are unchanged: each
                -- served partition costs 1 of the batch budget, which is why the
                -- broker raises max_parts for a conflating pop (M5).
                WHILE m_i <= m_len AND m_ord[m_i] = v_ord LOOP
                    v_has_rows := TRUE;
                    IF v_taken = 0 AND m_kind[m_i] = 1 AND m_end[m_i] >= v_wanted THEN
                        v_segj := v_segj || jsonb_build_object(
                            'seq', m_base[m_i],
                            'startOff', (m_end[m_i] - m_base[m_i])::int,
                            'take', 1,
                            'createdAt', to_char(m_cre[m_i], 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
                        sel_pid := sel_pid || a_pid[v_ord];
                        sel_base := sel_base || m_base[m_i];
                        v_taken := 1;
                        v_start := v_wanted;          -- EPISODE ANCHOR (§1.4)
                        v_last := m_end[m_i];         -- batch_end = commit_to
                    END IF;
                    m_i := m_i + 1;
                END LOOP;
            ELSE
            WHILE m_i <= m_len AND m_ord[m_i] = v_ord LOOP
                v_has_rows := TRUE;
                IF v_taken < v_pbudget
                   AND (v_deadline IS NULL OR m_cre[m_i] <= v_deadline) THEN
                    IF m_base[m_i] <= v_wanted THEN
                        -- head row: counts only if it still covers `wanted`
                        IF m_end[m_i] >= v_wanted THEN
                            v_avail := m_end[m_i] - v_wanted + 1;
                            v_take := LEAST(v_avail, v_pbudget - v_taken)::int;
                            v_segj := v_segj || jsonb_build_object(
                                'seq', m_base[m_i],
                                'startOff', (v_wanted - m_base[m_i])::int,
                                'take', v_take,
                                'createdAt', to_char(m_cre[m_i], 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
                            sel_pid := sel_pid || a_pid[v_ord];
                            sel_base := sel_base || m_base[m_i];
                            v_taken := v_taken + v_take;
                            v_start := v_wanted;
                            v_last := v_wanted + v_take - 1;
                        END IF;
                    ELSE
                        -- forward row: starts at frame 0
                        v_avail := m_end[m_i] - m_base[m_i] + 1;
                        v_take := LEAST(v_avail, v_pbudget - v_taken)::int;
                        v_segj := v_segj || jsonb_build_object(
                            'seq', m_base[m_i],
                            'startOff', 0,
                            'take', v_take,
                            'createdAt', to_char(m_cre[m_i], 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
                        sel_pid := sel_pid || a_pid[v_ord];
                        sel_base := sel_base || m_base[m_i];
                        v_taken := v_taken + v_take;
                        IF v_start IS NULL THEN
                            v_start := m_base[m_i];  -- retention gap: batch starts here
                        END IF;
                        v_last := m_base[m_i] + v_take - 1;
                    END IF;
                END IF;
                m_i := m_i + 1;
            END LOOP;
            END IF;

            IF v_taken > 0 THEN
                e_kind := array_append(e_kind, 'f');
                e_ord  := e_ord || v_ord;
                e_pid  := e_pid || a_pid[v_ord];
                e_segj := e_segj || v_segj;
                e_bfrom := e_bfrom || v_bfrom;
                e_bcnt := e_bcnt || (COALESCE(array_length(sel_pid, 1), 0) - v_bfrom + 1);
                e_start := e_start || v_start;
                e_last := e_last || v_last;
                e_take := e_take || v_taken;
                e_sfrom := e_sfrom || 0;  -- unused for fast
                e_scnt := e_scnt || 0;
                e_lastoff := e_lastoff || NULL::bigint;  -- from RETURNING below
                v_remaining := v_remaining - v_taken;
                v_claimed := v_claimed + 1;
            ELSE
                -- claimed, nothing visible: seal decision + tri-state deferred
                -- to the batched fresh re-check (parity with the old body's
                -- post-serve re-check statement and its snapshot ordering).
                z_ord := z_ord || v_ord;
                z_seal := z_seal || (NOT v_has_rows AND a_lastoff[v_ord] > a_comm[v_ord]);
                z_pending := z_pending || FALSE;  -- placeholder
            END IF;

        ELSIF a_probed[v_ord] AND a_rowmiss[v_ord] THEN
            -- ----------------------------------------------------------
            -- SLOW PATH — first contact: the old per-partition serve,
            -- verbatim. log_pop_v1 owns seeding + advisory-guarded creation.
            -- ----------------------------------------------------------
            SELECT COALESCE(jsonb_agg(jsonb_build_object(
                       'seq', r_base,
                       'startOff', r_start_idx,
                       'take', r_take,
                       'createdAt', to_char(r_created_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
                   ) ORDER BY r_base), '[]'::jsonb),
                   COALESCE(SUM(r_take), 0),
                   COALESCE(array_agg(r_blob ORDER BY r_base), '{}'::bytea[])
            INTO v_segments, v_sp_taken, v_part_blobs
            FROM queen.log_pop_v1(p_queue, v_name, p_group,
                                  v_remaining, p_lease_seconds, p_worker, p_auto_ack,
                                  p_sub_mode, p_sub_from, p_skip_window, p_tenant,
                                  p_conflate);

            IF v_sp_taken > 0 THEN
                SELECT p.id, p.last_offset INTO v_pid2, v_lo2
                FROM queen.log_partitions p
                WHERE p.queue_id = v_qid AND p.name = v_name;
                e_kind := array_append(e_kind, 'w');
                e_ord  := e_ord || v_ord;
                e_pid  := e_pid || v_pid2;
                e_segj := e_segj || v_segments;
                e_bfrom := e_bfrom || 0;  -- unused for slow
                e_bcnt := e_bcnt || 0;
                e_start := e_start || NULL::bigint;
                e_last := e_last || NULL::bigint;
                e_take := e_take || v_sp_taken;
                e_sfrom := e_sfrom || (COALESCE(array_length(slow_blobs, 1), 0) + 1);
                e_scnt := e_scnt || COALESCE(array_length(v_part_blobs, 1), 0);
                e_lastoff := e_lastoff || v_lo2;
                slow_blobs := slow_blobs || v_part_blobs;
                v_remaining := v_remaining - v_sp_taken;
                v_claimed := v_claimed + 1;
            ELSE
                -- Zero-serve on first contact: decide from a FRESH snapshot,
                -- exactly as the old body did — its re-check ran as a NEW
                -- statement and therefore saw log_pop_v1's OWN writes (the
                -- consumer row it may have just created and seeded, or its
                -- cursor seal). The pass-1b probe data is stale here by
                -- construction: judging from it reported 'leased' for a
                -- freshly-seeded caught-up partition where the old body said
                -- 'empty' (safe direction, but a parity break — found by the
                -- 2026-08-02 adversarial review).
                SELECT c.worker_id, c.lease_expires_at,
                       (p.last_offset > COALESCE(c.committed, -1))
                INTO v_flworker, v_flexp, v_fpend
                FROM queen.log_partitions p
                LEFT JOIN queen.log_consumers c
                  ON c.partition_id = p.id AND c.consumer_group = p_group
                WHERE p.queue_id = v_qid AND p.name = v_name;
                IF v_flworker IS NOT NULL AND v_flworker <> p_worker
                   AND v_flexp IS NOT NULL AND v_flexp > v_now THEN
                    v_states := v_states || jsonb_build_object(
                        'p', v_name, 's', 'leased',
                        'until', to_char(v_flexp, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
                ELSIF COALESCE(v_fpend, FALSE) THEN
                    v_states := v_states || jsonb_build_object(
                        'p', v_name, 's', 'leased',
                        'until', to_char(v_now + interval '250 milliseconds',
                                         'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
                ELSE
                    v_states := v_states || jsonb_build_object('p', v_name, 's', 'empty');
                END IF;
            END IF;

        ELSIF a_probed[v_ord] THEN
            -- row exists, claim skipped it: live foreign lease, or locked by a
            -- concurrent pop. Same verdicts as the old body.
            IF a_lworker[v_ord] IS NOT NULL AND a_lworker[v_ord] <> p_worker
               AND a_lexp[v_ord] IS NOT NULL AND a_lexp[v_ord] > v_now THEN
                v_states := v_states || jsonb_build_object(
                    'p', v_name, 's', 'leased',
                    'until', to_char(a_lexp[v_ord], 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
            ELSIF a_lastoff[v_ord] > COALESCE(a_comm[v_ord], -1) THEN
                v_states := v_states || jsonb_build_object(
                    'p', v_name, 's', 'leased',
                    'until', to_char(v_now + interval '250 milliseconds',
                                     'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
            ELSE
                v_states := v_states || jsonb_build_object('p', v_name, 's', 'empty');
            END IF;

        ELSE
            -- partition row itself missing: the old body's per-partition pop
            -- returned nothing and its pending re-check found no row → empty.
            v_states := v_states || jsonb_build_object('p', v_name, 's', 'empty');
        END IF;
    END LOOP;

    -- ------------------------------------------------------------------
    -- WRITES — one statement per kind, only for what was actually used
    -- (batched-lease design point, 2026-08-02).
    -- ------------------------------------------------------------------
    IF COALESCE(array_length(z_ord, 1), 0) > 0 THEN
        -- Cursor seal (empty-partition starvation fix, 2026-07-30): only when
        -- the partition holds NO segments at all, sealed to the resolve-time
        -- last_offset snapshot — monotone offsets keep a racing push above
        -- the seal, exactly as before.
        UPDATE queen.log_consumers c SET committed = u.seal_off
        FROM (SELECT a_pid[zz.z] AS pid, a_lastoff[zz.z] AS seal_off
              FROM unnest(z_ord) WITH ORDINALITY AS zz(z, zi)
              WHERE z_seal[zi]) u
        WHERE c.partition_id = u.pid AND c.consumer_group = p_group;

        -- Fresh pending re-check (new snapshot: sees our own seal), batched.
        FOR rec IN
            SELECT zz.zi, (p.last_offset > COALESCE(c.committed, -1)) AS pending
            FROM unnest(z_ord) WITH ORDINALITY AS zz(z, zi)
            JOIN queen.log_partitions p ON p.id = a_pid[zz.z]
            LEFT JOIN queen.log_consumers c
              ON c.partition_id = p.id AND c.consumer_group = p_group
        LOOP
            z_pending[rec.zi] := COALESCE(rec.pending, FALSE);
        END LOOP;

        FOR i IN 1..array_length(z_ord, 1) LOOP
            IF z_pending[i] THEN
                v_states := v_states || jsonb_build_object(
                    'p', v_parts[z_ord[i]], 's', 'leased',
                    'until', to_char(v_now + interval '250 milliseconds',
                                     'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
            ELSE
                v_states := v_states || jsonb_build_object(
                    'p', v_parts[z_ord[i]], 's', 'empty');
            END IF;
        END LOOP;
    END IF;

    -- Batched lease / auto-ack for the FAST-path served partitions (slow-path
    -- ones were already written inside log_pop_v1).
    IF EXISTS (SELECT 1 FROM unnest(e_kind) k WHERE k = 'f') THEN
        IF p_auto_ack THEN
            -- auto-ack: commit the delivery in-transaction; no lease; attempt
            -- state untouched (auto-ack never redelivers).
            FOR rec IN
                UPDATE queen.log_consumers c SET
                    committed = u.last_off,
                    worker_id = NULL, lease_expires_at = NULL, lease_acquired_at = NULL,
                    batch_end = NULL,
                    lease_conflated = FALSE,   -- no lease to describe (§2.3 step 5)
                    total_consumed = c.total_consumed + u.take
                FROM (SELECT ei, e_pid[ei] AS pid, e_last[ei] AS last_off, e_take[ei] AS take
                      FROM generate_series(1, array_length(e_kind, 1)) ei
                      WHERE e_kind[ei] = 'f') u
                JOIN queen.log_partitions p ON p.id = u.pid
                WHERE c.partition_id = u.pid AND c.consumer_group = p_group
                RETURNING u.ei, p.last_offset
            LOOP
                e_lastoff[rec.ei] := rec.last_offset;
            END LOOP;
        ELSE
            -- Lease the batches (committed, last]. Attempt tracking verbatim:
            -- same start offset as the previous non-auto-ack delivery =
            -- redelivery → attempt_count++; anywhere else = fresh → 1.
            FOR rec IN
                UPDATE queen.log_consumers c SET
                    worker_id = p_worker,
                    lease_expires_at = v_now + make_interval(secs => p_lease_seconds),
                    lease_acquired_at = v_now,
                    batch_end = u.last_off,
                    lease_conflated = COALESCE(p_conflate, FALSE),
                    attempt_count = CASE WHEN c.attempt_offset IS NOT DISTINCT FROM u.start_off
                                         THEN c.attempt_count + 1 ELSE 1 END,
                    attempt_offset = u.start_off
                FROM (SELECT ei, e_pid[ei] AS pid, e_start[ei] AS start_off, e_last[ei] AS last_off
                      FROM generate_series(1, array_length(e_kind, 1)) ei
                      WHERE e_kind[ei] = 'f') u
                JOIN queen.log_partitions p ON p.id = u.pid
                WHERE c.partition_id = u.pid AND c.consumer_group = p_group
                RETURNING u.ei, p.last_offset
            LOOP
                e_lastoff[rec.ei] := rec.last_offset;
            END LOOP;
        END IF;

        -- ONE blob fetch, in selection (= emission) order.
        SELECT COALESCE(array_agg(s.blob ORDER BY u.ord), '{}'::bytea[])
        INTO v_fast_blobs
        FROM unnest(sel_pid, sel_base) WITH ORDINALITY AS u(pid, base, ord)
        JOIN queen.log_segments s
          ON s.partition_id = u.pid AND s.base_offset = u.base;
    END IF;

    -- ------------------------------------------------------------------
    -- ASSEMBLY — served entries in serve order; per-entry blob splice keeps
    -- the flat array positionally aligned with meta.
    -- ------------------------------------------------------------------
    FOR i IN 1..COALESCE(array_length(e_kind, 1), 0) LOOP
        v_out := v_out || jsonb_build_object(
            'partition', v_parts[e_ord[i]],
            'partitionId', e_pid[i],
            'segments', e_segj[i]);
        v_states := v_states || jsonb_build_object(
            'p', v_parts[e_ord[i]], 's', 'took',
            'lastOff', e_lastoff[i]);
        IF e_kind[i] = 'f' THEN
            IF e_bcnt[i] > 0 THEN
                v_all_blobs := v_all_blobs
                    || v_fast_blobs[e_bfrom[i] : e_bfrom[i] + e_bcnt[i] - 1];
            END IF;
        ELSE
            IF e_scnt[i] > 0 THEN
                v_all_blobs := v_all_blobs
                    || slow_blobs[e_sfrom[i] : e_sfrom[i] + e_scnt[i] - 1];
            END IF;
        END IF;
    END LOOP;

    meta := jsonb_build_object('partitions', v_out);
    blobs := v_all_blobs;
    states := v_states;
    RETURN NEXT;
END;
$$;

-- ============================================================================
-- 19-wildcard-hotlist §8 — reseed / cold-start discovery (correctness floor).
--
-- Enumerates the (queue, group)'s probably-pending partitions the SAME way the
-- wildcard candidate scan does (last_offset > committed OR no consumer row), but
-- KEYSET-paginated in id order (never ORDER BY random() on a large set, §8) so
-- the broker walks it in bounded ~10k chunks, staggered/throttled, to (re)seed
-- the ring. Lease-held partitions ARE included (a lease-held partition is still
-- pending; the ring must hold it and the tri-state will mark it leased) — reseed
-- is the correctness floor, so it errs toward over-inclusion. Returns (id, name)
-- so the broker interns the name and remembers the id for the ack bridge.
-- p_after_id is the keyset cursor; pass the NIL uuid to start a fresh walk.
-- ============================================================================
-- Track B (§5): p_tenant scopes the queue resolution.
CREATE OR REPLACE FUNCTION queen.log_hotlist_reseed_v1(
    p_queue TEXT,
    p_group TEXT,
    p_after_id UUID,
    p_limit INTEGER,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001'
) RETURNS TABLE(r_id UUID, r_name TEXT)
LANGUAGE sql STABLE
AS $$
    SELECT p.id, p.name
    FROM queen.log_partitions p
    -- queue identity is the queen.queues id (log_queues is gone)
    JOIN queen.queues q ON q.id = p.queue_id AND q.name = p_queue AND q.tenant_id = p_tenant
    LEFT JOIN queen.log_consumers c
      ON c.partition_id = p.id AND c.consumer_group = p_group
    WHERE p.id > p_after_id
      AND p.last_offset > COALESCE(c.committed, -1)
    ORDER BY p.id
    LIMIT GREATEST(p_limit, 1);
$$;

-- ============================================================================
-- 19-wildcard-hotlist §8 — WINDOWED reseed: the same enumeration bounded to the
-- partitions written in the last p_window_ms.
--
-- Why it exists (measured in prod 2026-08-11, 9.5k partitions on one queue, 63
-- rings, 4 sources, reseed every 30s): the full walk above is Θ(partitions in
-- the queue) per ring per interval, and its cost is NOT the partition scan — the
-- bitmap over (queue_id) costs 5ms — it is the 9,573 log_consumers PK probes the
-- LEFT JOIN pays, 38,292 of the query's 39,700 shared buffers. It ran 8.2×/s to
-- return ZERO rows and was the single largest consumer of the database: 2,089,774
-- ms/hour, more total time than the entire pop path at 24× the call count.
--
-- The bound is sound because of an invariant the push path already maintains: a
-- partition can only BECOME pending by being written, and every push bumps
-- last_write_at (quantized to ≤1/s, 003_log_push). Acks and retention only ever
-- REMOVE pendingness. So "pending and not already in the ring" is a subset of
-- "written recently", for any window wider than the interval since the ring was
-- last known good. The events that create pendingness WITHOUT a push — a
-- backward/timestamp seek, which moves committed down — are covered by their own
-- forced full walk at the call site (handlers/consumer_groups.rs) and, across
-- brokers, by the mesh hint that walk broadcasts.
--
-- The window is a SEPARATE FUNCTION rather than a `p_window_ms <= 0 OR …` branch
-- inside v1 on purpose. The broker calls these through prepare_cached, so the plan
-- can be GENERIC, and a parameter inside an OR does not fold at plan time: the index
-- bound on (queue_id, last_write_at) would be lost exactly in the case that matters.
-- Two statements, two plans, no branch. v1 is left byte-identical — it is still the
-- full walk, and it is the revert path (QUEEN_HOTLIST_RESEED_FULL_MS=0).
--
-- ORDERING IS LOAD-BEARING, and this is the part that is not obvious. The keyset
-- runs on (last_write_at, id), NOT on id, because under a GENERIC plan `ORDER BY
-- p.id` lets the planner satisfy the sort from log_partitions_pkey and apply the
-- window as a mere filter — measured: it walks the PK and merge-joins the whole of
-- log_consumers, i.e. the optimisation silently evaporates on exactly the plan the
-- broker ends up with. Ordering by the index's own leading columns removes that
-- alternative structurally, so the access path no longer depends on the planner
-- estimating an unknown parameter. Verified under `plan_cache_mode =
-- force_generic_plan`: Index Cond carries both the window and the cursor bound.
--
-- Measured on the prod shape (9.5k partitions, one queue): 49.049 ms → 0.375 ms,
-- 39,700 → 160 buffers, on idx_log_partitions_queue_write (queue_id, last_write_at),
-- which already exists. No new index, no DDL beyond this function.
--
-- The cursor is the (r_write, r_id) of the last row of the previous page, so the
-- caller echoes back what this returned and never has to hold a clock of its own.
-- Pass ('-infinity', NIL uuid) to start a walk. Ties on last_write_at are ordered by
-- id, and the push path quantizes that column to 1s, so a page boundary landing
-- inside a tie group re-reads that group's index entries and the row comparison
-- discards the ones already seen: progress is guaranteed, and the re-read is bounded
-- by one second of writes.
--
-- THE CUTOFF IS ONE VALUE PER WALK, NOT ONE PER PAGE (B1). Each page is its own
-- statement, so `now()` differs between them: the lower bound crept forward while the
-- cursor climbed from the OLDEST row, and a partition written into the band between
-- the two — [cursor, now_of_this_page - window) — was excluded by the bound although
-- the cursor had not reached it yet, so that pass never saw it. Bounded rather than
-- lost (the next pass restarts its cursor and the default window is 4x the cadence),
-- but it made the walk's coverage a race instead of an invariant.
--
-- So the cutoff is derived HERE on the first page — the broker has no clock of the
-- database's and must not grow one — and returned as r_cutoff, exactly the way the
-- keyset is returned as (r_write, r_id): the caller echoes an opaque timestamp back
-- on every later page and the walk's lower bound stops moving. A NULL p_cutoff means
-- "derive it", which is the first page and every single-page walk. The set the walk
-- can return then only GROWS while it runs (a concurrent push bumps last_write_at
-- forward, above the cursor, where the walk still reaches it), which is the property
-- that makes one pass self-contained.
-- ============================================================================
-- DROP of the CURRENT signature = the boot-idempotency half of DROP+CREATE, the same
-- convention log_pop_v1 states in its header: CREATE OR REPLACE cannot change a
-- return type, and a differing argument list silently creates an OVERLOAD instead of
-- replacing — which every later boot would then recreate forever. The second DROP is
-- the interim 6-argument shape that existed only inside this branch (keyset on id,
-- returning two columns); it never shipped, but a rig that ran an intermediate build
-- would otherwise keep a dead overload whose defaulted tenant makes 6-argument calls
-- ambiguous (SQLSTATE 42725). The third is 1.0.1-beta.1's shape, which this revision
-- replaces: leaving it in place would make it the SECOND candidate for the 6-argument
-- call the tests make (both trail-default their remaining parameters), i.e. that same
-- 42725, and the two would answer different questions.
-- The CURRENT shape, dropped first and for the least obvious reason: schema.rs re-applies
-- every file at every boot, and the CREATE below is a plain CREATE (it cannot be CREATE OR
-- REPLACE — the return type gained r_cutoff). Without this line the first boot succeeds and
-- EVERY LATER ONE dies with "function already exists with same argument types", i.e. a
-- broker that comes up green and then crash-loops the next time it restarts.
DROP FUNCTION IF EXISTS queen.log_hotlist_reseed_window_v1(TEXT, TEXT, TIMESTAMPTZ, UUID, INTEGER, BIGINT, UUID, TIMESTAMPTZ);
-- The interim shapes: the 8-argument one with p_cutoff BEFORE p_tenant (which broke the
-- previous release's 7-argument call — see the parameter list), the 6-argument keyset-on-id
-- original, and the 7-argument shape 1.0.1-beta.1 shipped. The last must go even though the
-- new function absorbs its calls through defaults: two candidates matching one 7-argument
-- call is the 42725 ambiguity.
DROP FUNCTION IF EXISTS queen.log_hotlist_reseed_window_v1(TEXT, TEXT, TIMESTAMPTZ, UUID, INTEGER, BIGINT, TIMESTAMPTZ, UUID);
DROP FUNCTION IF EXISTS queen.log_hotlist_reseed_window_v1(TEXT, TEXT, UUID, INTEGER, BIGINT, UUID);
DROP FUNCTION IF EXISTS queen.log_hotlist_reseed_window_v1(TEXT, TEXT, TIMESTAMPTZ, UUID, INTEGER, BIGINT, UUID);
CREATE FUNCTION queen.log_hotlist_reseed_window_v1(
    p_queue TEXT,
    p_group TEXT,
    p_after_write TIMESTAMPTZ,
    p_after_id UUID,
    p_limit INTEGER,
    p_window_ms BIGINT,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001',
    -- NULL = "this is the first page, derive the cutoff from p_window_ms and hand it
    -- back". Every later page passes the r_cutoff it received.
    --
    -- APPENDED LAST, after p_tenant, and that position is load-bearing even though it
    -- reads worse next to the window it pins. schema.rs re-applies every file at every
    -- boot, so the FIRST new broker of a rolling upgrade drops and recreates this
    -- function while the other replicas are still running the previous release and still
    -- calling the 7-argument shape. With p_cutoff in the middle, their seventh argument
    -- is a uuid landing on a timestamptz parameter, the call resolves to nothing, and
    -- every windowed reseed on every not-yet-upgraded pod fails until it is replaced —
    -- silently, because those binaries predate the error logging added alongside this.
    -- Verified against a real PostgreSQL, not reasoned about: with p_cutoff seventh the
    -- old call is `function ... does not exist`; appended eighth it resolves and defaults
    -- to NULL, which COALESCE below turns into exactly the pre-cutoff behaviour.
    --
    -- The 7-argument function must still be DROPped above rather than left beside this
    -- one: two candidates both matching a 7-argument call is the 42725 ambiguity this
    -- file's other headers warn about.
    p_cutoff TIMESTAMPTZ DEFAULT NULL
) RETURNS TABLE(r_id UUID, r_name TEXT, r_write TIMESTAMPTZ, r_cutoff TIMESTAMPTZ)
LANGUAGE sql STABLE
AS $$
    SELECT p.id, p.name, p.last_write_at,
           -- The bound this page actually applied, for the next page to echo. now() is
           -- transaction_timestamp, so every evaluation inside one statement returns the
           -- same instant and this can never disagree with the WHERE clause below.
           COALESCE(p_cutoff,
                    now() - make_interval(secs => p_window_ms::double precision / 1000.0))
    FROM queen.log_partitions p
    LEFT JOIN queen.log_consumers c
      ON c.partition_id = p.id AND c.consumer_group = p_group
    -- The queue is resolved by SCALAR SUBQUERY, not by joining queen.queues as a
    -- relation, and that is load-bearing under a GENERIC plan — which is the plan the
    -- broker gets, because it calls this through prepare_cached.
    --
    -- Joined as a relation, `queues` can only ever drive the nested loop, so `p` is the
    -- inner side and PostgreSQL drops its (queue_id, last_write_at) ordering from the
    -- join pathkeys. The planner then reaches this ORDER BY with an unordered Bitmap
    -- Index Scan and has to materialize and SORT the entire window before the LIMIT can
    -- look at it — measured start-up cost 4391 against a real schema. The bound still
    -- holds, so the query stays correct and fast in steady state, but the work stops
    -- being proportional to the LIMIT and becomes proportional to the whole window: a
    -- burst that writes far more partitions than one page would sort all of them.
    --
    -- As a subquery it is an InitPlan pseudo-constant, so log_partitions becomes the
    -- OUTER relation, keeps an ordered Index Scan, and the tie groups collapse into an
    -- Incremental Sort the LIMIT can terminate early: start-up cost 310 on the same
    -- data. queen.queues has a UNIQUE (tenant_id, name), so the subquery is
    -- single-valued by construction; an absent queue yields NULL and therefore no rows,
    -- exactly what the join did.
    WHERE p.queue_id = (SELECT q.id FROM queen.queues q
                        WHERE q.name = p_queue AND q.tenant_id = p_tenant)
      -- The index bound. Both branches are STABLE and the window is a parameter, so
      -- this is one constant per execution, not a per-row expression: the index scan
      -- evaluates it once as a runtime key, which is why an ABSOLUTE p_cutoff costs
      -- exactly what the derived one did (verified under force_generic_plan — the
      -- Index Cond still carries the window and the cursor bound together).
      AND p.last_write_at >= COALESCE(p_cutoff,
              now() - make_interval(secs => p_window_ms::double precision / 1000.0))
      AND (p.last_write_at, p.id) > (p_after_write, p_after_id)
      AND p.last_offset > COALESCE(c.committed, -1)
    ORDER BY p.last_write_at, p.id
    LIMIT GREATEST(p_limit, 1);
$$;
