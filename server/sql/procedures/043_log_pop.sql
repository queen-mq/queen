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
--
-- Semantics are the seg engine's (023/024/025/033) verbatim — per-batch claim,
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
-- Ported EXACTLY from seg_pop_segments_v1 (025 body, which superseded 023's):
--   * claim-first SKIP LOCKED dance — see the NB below (the deadlock lesson).
--   * subscription seeding on FIRST contact of a (partition, group), durable
--     record first (RUSTFIX item 13), then the pop-carried intent.
--   * delayed_processing / window_buffer visibility (read from queen.queues by
--     name, the config source of truth — queen.log_queues carries only
--     engine-side options; 002d_pop_unified_v4 semantics).
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
-- addition would brick boot re-apply. Boot applies the whole 04x sequence
-- before serving traffic, so the gap is invisible.
-- ----------------------------------------------------------------------------
DROP FUNCTION IF EXISTS queen.log_pop_v1(TEXT, TEXT, TEXT, INTEGER, INTEGER, TEXT, BOOLEAN, TEXT, TEXT);
CREATE FUNCTION queen.log_pop_v1(
    p_queue TEXT,
    p_partition TEXT,
    p_group TEXT,
    p_budget INTEGER,
    p_lease_seconds INTEGER,
    p_worker TEXT,
    p_auto_ack BOOLEAN DEFAULT FALSE,
    p_sub_mode TEXT DEFAULT 'all',
    p_sub_from TEXT DEFAULT ''
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
BEGIN
    SELECT p.id, p.last_offset, p.log_start
    INTO v_pid, v_last_offset, v_log_start
    FROM queen.log_partitions p JOIN queen.log_queues q ON q.id = p.queue_id
    WHERE q.name = p_queue AND p.name = p_partition;
    IF v_pid IS NULL THEN RETURN; END IF;

    -- Visibility knobs live on the v1 config row (created by the configure
    -- path for both engines). Absent row = both disabled.
    SELECT COALESCE(qq.delayed_processing, 0), COALESCE(qq.window_buffer, 0)
    INTO v_delayed, v_win_buf
    FROM queen.queues qq WHERE qq.name = p_queue;
    IF NOT FOUND THEN
        v_delayed := 0; v_win_buf := 0;
    END IF;

    -- window_buffer: if the partition received a segment within the last
    -- window_buffer seconds, deliver NOTHING (v1 checks the partition's
    -- newest message the same way, before touching the consumer row).
    -- created_at is monotone in base_offset, so the newest segment is the
    -- max base_offset — one backward PK step.
    IF v_win_buf > 0 THEN
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
        SELECT cgm.subscription_timestamp INTO v_from_ts
        FROM queen.consumer_groups_metadata cgm
        WHERE cgm.consumer_group = p_group AND cgm.queue_name = p_queue
          AND cgm.partition_name = ''
        LIMIT 1;

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
    -- NB (the deadlock lesson, ported from 023/025): we do NOT unconditionally
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

    IF v_taken = 0 THEN RETURN; END IF;

    IF p_auto_ack THEN
        -- auto-ack: commit the whole delivery in the same transaction; no
        -- lease, and the attempt state is untouched (auto-ack never
        -- redelivers — 025 parity).
        UPDATE queen.log_consumers SET
            committed = v_last,
            worker_id = NULL, lease_expires_at = NULL, lease_acquired_at = NULL,
            batch_end = NULL,
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
            attempt_count = CASE WHEN attempt_offset IS NOT DISTINCT FROM v_start
                                 THEN attempt_count + 1 ELSE 1 END,
            attempt_offset = v_start
        WHERE partition_id = v_pid AND consumer_group = p_group;
    END IF;
END;
$$;

-- ============================================================================
-- Wildcard pop: claim frames from several partitions of a queue in one call.
-- Ported verbatim from seg_pop_wildcard_wire_v1 (024) on offsets. Candidate
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
DROP FUNCTION IF EXISTS queen.log_pop_wildcard_wire_v1(TEXT, TEXT, INTEGER, INTEGER, TEXT, BOOLEAN, INTEGER, TEXT, TEXT);
CREATE FUNCTION queen.log_pop_wildcard_wire_v1(
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
    SELECT id INTO v_qid FROM queen.log_queues WHERE name = p_queue;
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
    -- Marker: the consumer_groups_metadata row (UNIQUE(consumer_group, queue_name,
    -- partition_name='', namespace, task)); its first insertion (ROW_COUNT>0)
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

    INSERT INTO queen.consumer_groups_metadata
        (consumer_group, queue_name, partition_name, subscription_mode, subscription_timestamp)
    VALUES (p_group, p_queue, '', v_sub_mode_stored, v_from_ts)
    ON CONFLICT (consumer_group, queue_name, partition_name, namespace, task) DO NOTHING;
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
    SELECT last_empty_scan_at, updated_at INTO v_watermark, v_verified_at
    FROM queen.consumer_watermarks
    WHERE queue_name = p_queue AND consumer_group = p_group;
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

    -- Nothing claimable this cycle. RUSTFIX item 12: DO NOT advance the
    -- empty-scan watermark blindly — a partition may have been skipped only
    -- because another worker held its lease, not because it is empty.
    -- Throttled to 30s, re-check for pending data IGNORING the lease filter
    -- (parity with 002d_pop_unified_v4:395-426): advance only if genuinely
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
                    (queue_name, consumer_group, last_empty_scan_at, updated_at)
                VALUES (p_queue, p_group, v_now, v_now)
                ON CONFLICT (queue_name, consumer_group)
                DO UPDATE SET last_empty_scan_at = EXCLUDED.last_empty_scan_at,
                              updated_at = EXCLUDED.updated_at;
            ELSE
                -- Pending backlog exists (possibly lease-held): record the
                -- check time only, keep the floor so the partition stays a
                -- candidate.
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
-- log_pop_wildcard_wire_v1 above, but segment blobs are returned as a NATIVE
-- bytea[] instead of base64 text inside the JSON — no encode() on the PG side,
-- no whitespace-stripping + base64 decode on the broker side, ~25% fewer bytes
-- on the wire. `meta` carries the partition/segment metadata WITHOUT blobs
-- (and without seq/msgCount, which the broker renderer never reads — 024
-- parity); `blobs` is flattened in traversal order (partitions in claim order,
-- segments in base_offset order — both aggregates below share
-- ORDER BY r_base, keeping them aligned).
-- KEEP THE CLAIM LOGIC IN SYNC with log_pop_wildcard_wire_v1.
-- ============================================================================
DROP FUNCTION IF EXISTS queen.log_pop_wildcard_bin_v1(TEXT, TEXT, INTEGER, INTEGER, TEXT, BOOLEAN, INTEGER, TEXT, TEXT);
CREATE FUNCTION queen.log_pop_wildcard_bin_v1(
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
    SELECT id INTO v_qid FROM queen.log_queues WHERE name = p_queue;
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

    INSERT INTO queen.consumer_groups_metadata
        (consumer_group, queue_name, partition_name, subscription_mode, subscription_timestamp)
    VALUES (p_group, p_queue, '', v_sub_mode_stored, v_from_ts)
    ON CONFLICT (consumer_group, queue_name, partition_name, namespace, task) DO NOTHING;
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

    SELECT last_empty_scan_at, updated_at INTO v_watermark, v_verified_at
    FROM queen.consumer_watermarks
    WHERE queue_name = p_queue AND consumer_group = p_group;
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
-- Namespace/task discovery pop. Ported whole from seg_pop_discover_wire_v1
-- (033) on offsets. Client parity: the JS/Go/Py clients expose
-- `client.queue().namespace_name(ns).consume(...)` (no queue in the path),
-- which issues a bare `GET /api/v1/pop?namespace=&task=&consumerGroup=...`.
-- This pops across ALL log queues whose queen.queues row matches the requested
-- namespace/task, returning the SAME wire shape as log_pop_wildcard_wire_v1.
--
-- This is log_pop_wildcard_wire_v1 with a broader candidate set: instead of
-- one queue's partitions, the candidates are every log_partitions row whose
-- queue (log_queues.name = queen.queues.name) matches the namespace/task
-- filter. Everything else — the per-partition queen.log_pop_v1 loop, the
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
-- leaseTime, so each partition is leased with ITS queue's log_queues.lease_time
-- (falling back to p_lease_seconds when unset) rather than one global value.
--
-- Empty-scan floor: the canonical queen.consumer_watermarks rows are keyed by
-- (queue_name, group) — same rows the queue-scoped wildcard pop uses. The
-- floor for the candidate scan is the OLDEST last_empty_scan_at across the
-- matching queues (a partition written since ANY matching queue last went
-- empty is still a candidate — never wrongly skipped); when the whole scope
-- yields nothing, all matching queues' watermarks advance to now.
-- ============================================================================
DROP FUNCTION IF EXISTS queen.log_pop_discover_wire_v1(TEXT, TEXT, TEXT, INTEGER, INTEGER, TEXT, BOOLEAN, INTEGER, TEXT, TEXT);
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
    p_sub_from TEXT DEFAULT ''
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

    FOR v_q IN
        SELECT lq.id AS qid, lq.name AS qname
        FROM queen.log_queues lq
        JOIN queen.queues qq ON qq.name = lq.name
        WHERE (v_ns = '' OR qq.namespace = v_ns)
          AND (v_task = '' OR qq.task = v_task)
    LOOP
        INSERT INTO queen.consumer_groups_metadata
            (consumer_group, queue_name, partition_name, subscription_mode, subscription_timestamp)
        VALUES (p_group, v_q.qname, '', v_sub_mode_stored, v_from_ts)
        ON CONFLICT (consumer_group, queue_name, partition_name, namespace, task) DO NOTHING;
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
    FROM queen.log_queues lq
    JOIN queen.queues qq ON qq.name = lq.name
    LEFT JOIN queen.consumer_watermarks cw
      ON cw.queue_name = lq.name AND cw.consumer_group = p_group
    WHERE (v_ns = '' OR qq.namespace = v_ns)
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
        SELECT p.id, p.name AS pname, lq.name AS qname,
               -- RUSTFIX item 18: request override (p_lease_seconds>0) wins per
               -- partition, else the queue's own lease_time, else the 60s floor.
               COALESCE(NULLIF(p_lease_seconds, 0), lq.lease_time, 60) AS lease_time
        FROM queen.log_partitions p
        JOIN queen.log_queues lq ON lq.id = p.queue_id
        JOIN queen.queues qq ON qq.name = lq.name
        LEFT JOIN queen.log_consumers c
          ON c.partition_id = p.id AND c.consumer_group = p_group
        WHERE (v_ns = '' OR qq.namespace = v_ns)
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
                              p_sub_mode, p_sub_from);

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
    -- stranded once the lease expires with no new push (parity with
    -- 002d_pop_unified_v4:395-426).
    IF v_claimed = 0 THEN
        IF v_verified_at IS NULL OR v_verified_at <= v_now - interval '30 seconds' THEN
            SELECT EXISTS (
                SELECT 1 FROM queen.log_partitions p
                JOIN queen.log_queues lq ON lq.id = p.queue_id
                JOIN queen.queues qq ON qq.name = lq.name
                LEFT JOIN queen.log_consumers c
                  ON c.partition_id = p.id AND c.consumer_group = p_group
                WHERE (v_ns = '' OR qq.namespace = v_ns)
                  AND (v_task = '' OR qq.task = v_task)
                  AND p.last_write_at >= v_watermark - interval '2 minutes'
                  AND (c.partition_id IS NULL OR p.last_offset > c.committed)
            ) INTO v_has_pending;
            IF NOT v_has_pending THEN
                INSERT INTO queen.consumer_watermarks
                    (queue_name, consumer_group, last_empty_scan_at, updated_at)
                SELECT lq.name, p_group, v_now, v_now
                FROM queen.log_queues lq
                JOIN queen.queues qq ON qq.name = lq.name
                WHERE (v_ns = '' OR qq.namespace = v_ns)
                  AND (v_task = '' OR qq.task = v_task)
                ON CONFLICT (queue_name, consumer_group)
                DO UPDATE SET last_empty_scan_at = EXCLUDED.last_empty_scan_at,
                              updated_at = EXCLUDED.updated_at;
            ELSE
                -- Pending backlog exists (possibly lease-held): record the
                -- check time only, keep the floor so the partitions stay
                -- candidates.
                UPDATE queen.consumer_watermarks cw SET updated_at = v_now
                FROM queen.log_queues lq
                JOIN queen.queues qq ON qq.name = lq.name
                WHERE cw.queue_name = lq.name AND cw.consumer_group = p_group
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
CREATE OR REPLACE FUNCTION queen.log_has_pending_v1(p_queue TEXT, p_group TEXT)
RETURNS BOOLEAN
LANGUAGE sql STABLE
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM queen.log_partitions p
        JOIN queen.log_queues q ON q.id = p.queue_id AND q.name = p_queue
        LEFT JOIN queen.log_consumers c
          ON c.partition_id = p.id AND c.consumer_group = p_group
        WHERE p.last_offset > COALESCE(c.committed, -1)
    );
$$;
