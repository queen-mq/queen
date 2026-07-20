-- ============================================================================
-- Storage v2 (segments engine) — namespace/task discovery pop.
-- Builds on 024_storage_v2_pop_ext.sql. Client parity: the JS/Go/C++ clients
-- expose `client.queue().namespace_name(ns).consume(...)` (no queue in the path),
-- which issues a bare `GET /api/v1/pop?namespace=&task=&consumerGroup=...`. This
-- pops across ALL segment queues whose queen.queues row matches the requested
-- namespace/task, returning the SAME wire shape as seg_pop_wildcard_wire_v1.
--
-- This is seg_pop_wildcard_wire_v1 (024) with a broader candidate set: instead of
-- one queue's partitions, the candidates are every seg_partitions row whose
-- queue (seg_queues.name = queen.queues.name) matches the namespace/task filter.
-- Everything else — the per-partition queen.seg_pop_segments_v1 loop, the shared
-- p_worker/one-lease-id-per-batch, budget, per-position batch_positions recording,
-- and the consumer_watermarks empty-scan floor — is identical, so ack / leaseId /
-- attempt behave exactly as for a queue-scoped pop.
--
-- Filter: (p_namespace='' OR qq.namespace=p_namespace)
--     AND (p_task='' OR qq.task=p_task). At least one of namespace/task is
-- expected (the broker 400s when both are empty); an all-empty call here would
-- match every segment queue, which is a harmless (if broad) superset.
--
-- Per-partition lease: candidates span queues that may carry different
-- leaseTime, so each partition is leased with ITS queue's seg_queues.lease_time
-- (falling back to p_lease_seconds when unset) rather than one global value.
--
-- Empty-scan floor: the canonical queen.consumer_watermarks rows are keyed by
-- (queue_name, group) — same rows the queue-scoped wildcard pop uses. The floor
-- for the candidate scan is the OLDEST last_empty_scan_at across the matching
-- queues (a partition written since ANY matching queue last went empty is still
-- a candidate — never wrongly skipped); when the whole scope yields nothing, all
-- matching queues' watermarks advance to now.
--
-- Returns {"partitions":[{"partition":name,"partitionId":uuid,
--                         "segments":[{seq,startOff,take,msgCount,createdAt,
--                                      blob(b64)}, ...]}, ...]}.
-- ============================================================================
DROP FUNCTION IF EXISTS queen.seg_pop_discover_wire_v1(TEXT, TEXT, TEXT, INTEGER, INTEGER, TEXT, BOOLEAN, INTEGER, TEXT, TEXT);
CREATE FUNCTION queen.seg_pop_discover_wire_v1(
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
    -- Bulk subscription bootstrap (mirrors seg_pop_wildcard_wire_v1). When a
    -- consumer group first registers with subscriptionMode='new' or a
    -- subscriptionFrom timestamp, seed the cursor of EVERY partition of EVERY
    -- matching queue before newer writes can overtake the backlog the group is
    -- meant to skip. The per-queue consumer_watermarks row is the once-per-
    -- (queue,group) marker (first insert => never scanned). Queue mode never
    -- seeds a backlog, so it is excluded.
    -- RUSTFIX item 13: register the subscription DURABLY per matching queue in
    -- consumer_groups_metadata (the marker, replacing consumer_watermarks-existence
    -- which item 12's empty-scan path now also writes). A late-created partition of
    -- any matching queue seeds from the stored timestamp on first contact.
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

        FOR v_q IN
            SELECT sq.id AS qid, sq.name AS qname
            FROM queen.seg_queues sq
            JOIN queen.queues qq ON qq.name = sq.name
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
                    -- timestamp seed: first segment at/after v_from_ts per
                    -- partition, else last_seq+1 (identical to the lazy seed in
                    -- queen.seg_pop_segments_v1).
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
                    WHERE p.queue_id = v_q.qid
                    ON CONFLICT (partition_id, consumer_group) DO NOTHING;
                ELSE
                    -- 'new' (or 'now') seed: skip the entire existing backlog.
                    INSERT INTO queen.partition_consumers (partition_id, consumer_group, next_seq)
                    SELECT p.id, p_group, p.last_seq + 1
                    FROM queen.seg_partitions p
                    WHERE p.queue_id = v_q.qid
                    ON CONFLICT (partition_id, consumer_group) DO NOTHING;
                END IF;
            END IF;
        END LOOP;
    END IF;

    -- Empty-scan floor: the OLDEST last_empty_scan_at across matching queues
    -- (missing rows count as the epoch => full scan). Only consider partitions
    -- written since some matching queue last found itself empty.
    -- RUSTFIX item 12: also compute the throttle floor = MIN(updated_at) across
    -- matching queues (missing rows => epoch => re-check every poll, acceptable).
    SELECT MIN(COALESCE(cw.last_empty_scan_at, '1970-01-01 00:00:00+00'::timestamptz)),
           MIN(COALESCE(cw.updated_at, '1970-01-01 00:00:00+00'::timestamptz))
    INTO v_watermark, v_verified_at
    FROM queen.seg_queues sq
    JOIN queen.queues qq ON qq.name = sq.name
    LEFT JOIN queen.consumer_watermarks cw
      ON cw.queue_name = sq.name AND cw.consumer_group = p_group
    WHERE (v_ns = '' OR qq.namespace = v_ns)
      AND (v_task = '' OR qq.task = v_task);
    IF v_watermark IS NULL THEN
        v_watermark := '1970-01-01 00:00:00+00'::timestamptz;
        v_verified_at := NULL;
    END IF;

    -- Fetch more candidates than requested: some will be claimed by concurrent
    -- pops or turn out empty by the time we call pop_segments_v1.
    v_cand_cap := LEAST(GREATEST(v_max_parts * 4, 64), 512);

    -- Candidate set: matching queues' partitions that are hot (write watermark >=
    -- consumer cursor), lease-free, written since the empty-scan floor. Randomized
    -- order (not by name) so many concurrent poppers spread across partitions.
    -- lease_time is carried per-partition so each queue's configured leaseTime is
    -- honored even when a single discovery pop spans several queues.
    FOR v_p IN
        SELECT p.id, p.name AS pname, sq.name AS qname,
               -- RUSTFIX item 18: request override (p_lease_seconds>0) wins per
               -- partition, else the queue's own lease_time, else the 60s floor.
               COALESCE(NULLIF(p_lease_seconds, 0), sq.lease_time, 60) AS lease_time
        FROM queen.seg_partitions p
        JOIN queen.seg_queues sq ON sq.id = p.queue_id
        JOIN queen.queues qq ON qq.name = sq.name
        LEFT JOIN queen.partition_consumers c
          ON c.partition_id = p.id AND c.consumer_group = p_group
        WHERE (v_ns = '' OR qq.namespace = v_ns)
          AND (v_task = '' OR qq.task = v_task)
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
        FROM queen.seg_pop_segments_v1(v_p.qname, v_p.pname, p_group,
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

    -- Nothing claimable this cycle. RUSTFIX item 12: throttle (30s) + re-check for
    -- pending data IGNORING the lease filter across matching queues before advancing
    -- the empty-scan floor, so a lease-held backlog is never stranded once the lease
    -- expires with no new push (parity with 002d_pop_unified_v4:395-426).
    IF v_claimed = 0 THEN
        IF v_verified_at IS NULL OR v_verified_at <= v_now - interval '30 seconds' THEN
            SELECT EXISTS (
                SELECT 1 FROM queen.seg_partitions p
                JOIN queen.seg_queues sq ON sq.id = p.queue_id
                JOIN queen.queues qq ON qq.name = sq.name
                LEFT JOIN queen.partition_consumers c
                  ON c.partition_id = p.id AND c.consumer_group = p_group
                WHERE (v_ns = '' OR qq.namespace = v_ns)
                  AND (v_task = '' OR qq.task = v_task)
                  AND p.last_write_at >= v_watermark - interval '2 minutes'
                  AND (c.partition_id IS NULL OR p.last_seq >= c.next_seq)
            ) INTO v_has_pending;
            IF NOT v_has_pending THEN
                INSERT INTO queen.consumer_watermarks
                    (queue_name, consumer_group, last_empty_scan_at, updated_at)
                SELECT sq.name, p_group, v_now, v_now
                FROM queen.seg_queues sq
                JOIN queen.queues qq ON qq.name = sq.name
                WHERE (v_ns = '' OR qq.namespace = v_ns)
                  AND (v_task = '' OR qq.task = v_task)
                ON CONFLICT (queue_name, consumer_group)
                DO UPDATE SET last_empty_scan_at = EXCLUDED.last_empty_scan_at,
                              updated_at = EXCLUDED.updated_at;
            ELSE
                -- Pending backlog exists (possibly lease-held): record the check
                -- time only, keep the floor so the partitions stay candidates.
                UPDATE queen.consumer_watermarks cw SET updated_at = v_now
                FROM queen.seg_queues sq
                JOIN queen.queues qq ON qq.name = sq.name
                WHERE cw.queue_name = sq.name AND cw.consumer_group = p_group
                  AND (v_ns = '' OR qq.namespace = v_ns)
                  AND (v_task = '' OR qq.task = v_task);
            END IF;
        END IF;
    END IF;

    RETURN jsonb_build_object('partitions', v_out);
END;
$$;
