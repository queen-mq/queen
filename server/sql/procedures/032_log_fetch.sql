-- ============================================================================
-- Log engine — FETCH (PLAN_QUEEN_KAFKA.md core change C2).
--
--   queen.log_fetch_bin_v1      batched, multi-partition, READ-ONLY read from
--                               an ABSOLUTE offset, blobs as a native bytea[]
--                               out-of-band of the meta JSON.
--   queen.log_fetch_changed_v1  the long poll's permit-free re-probe gate:
--                               "did any of these lanes move since the bounds
--                               I already hold?" Reads log_partitions only.
--
-- What this is NOT, and the distinction is the whole point: it is not a pop.
-- No lease is taken, no queen.log_consumers row is read, created or advanced,
-- no watermark is written, nothing is claimed and no SKIP LOCKED dance runs.
-- Two consumers fetching the same offsets both get the same records, and a
-- fetch interleaved with a pop of the same partition changes neither. The
-- function is STABLE and touches exactly three tables, all read-only:
-- queen.queues and queen.log_partitions (one indexed row per entry each) and
-- queen.log_segments (a PK range scan bounded by the byte budget).
--
-- Per entry the caller gets `high` (the next offset to be assigned,
-- last_offset + 1) and `logStart` (the retention watermark = the first live
-- segment's base_offset) WHETHER OR NOT any record came back, so an empty
-- fetch doubles as the bounds probe a Kafka ListOffsets needs.
--
-- OFFSET SEMANTICS, and they are Kafka's:
--   * offset < logStart   → 'OFFSET_OUT_OF_RANGE'. Retention passed the
--     caller by; there is no honest way to serve it and a silent jump forward
--     would hide data loss from a consumer that must know about it.
--   * offset > high       → 'OFFSET_OUT_OF_RANGE'. The caller is asking for a
--     record the log has not allocated; answering "empty" would let a
--     corrupted cursor poll for ever without ever saying why.
--   * offset = high       → VALID and empty. This is the caught-up consumer,
--     and it is the entry the broker's long-poll parks on.
--
-- A LANE THAT HAS NEVER BEEN WRITTEN IS EMPTY, NOT MISSING. The resolve is two
-- steps, not one join, because the two NULLs are different facts:
--   * no queen.queues row for (name, tenant)  → 'UNKNOWN_TOPIC_OR_PARTITION'.
--   * queue yes, no queen.log_partitions row  → high 0, logStart 0, no error.
-- queen.log_partitions rows are materialised LAZILY, by the first push to that
-- lane (003_log_push) — nothing in /configure or the admin surface creates
-- them — so on a queue produced to on lane 7 only, lanes 0..1023 all have no
-- row. Answering those UNKNOWN would be false (the topic is right there), it
-- would make a Kafka facade refresh metadata for ever, and — because ANY
-- per-entry error releases the long poll at once (handlers/fetch.rs) — a single
-- such entry would defeat parking for the whole 1024-entry batch and turn a
-- parked consumer into a busy-poller. An empty lane's honest bounds are the
-- bounds of an empty log: offset 0 is valid and empty, anything above it is
-- OFFSET_OUT_OF_RANGE, exactly as after the first push it will be.
--
-- TENANCY (Track B §5): the queue resolve reads queen.queues on
-- (name, tenant_id), so a queue owned by another tenant resolves to NOTHING
-- and lands in the SAME 'UNKNOWN_TOPIC_OR_PARTITION' arm as a queue that does
-- not exist anywhere. The two answers are byte-identical: a tenant cannot use
-- this endpoint to discover that another tenant's queue exists. Note that the
-- empty-lane arm above cannot leak either: it is reached only once the QUEUE
-- has resolved for this tenant.
--
-- BOUNDS. Three, all caller-supplied because the broker clamps them at the
-- HTTP boundary and this function must never be the place where an unbounded
-- read is possible:
--   * p_max_bytes[i]  per-entry ceiling on the COMPRESSED segment bytes read.
--   * p_budget        whole-call ceiling on the same, spent in entry order.
--   * p_max_records   per-entry ceiling on frames, so the broker's rendered
--                     JSON is bounded even for a partition of fat segments.
--
-- Exactly ONE segment escapes all three, and it is the FIRST OF THE WHOLE CALL,
-- never the first of each entry. A segment larger than the entire budget must
-- still be delivered or a consumer that meets one stalls for ever with no way
-- to step past it; but granting that exemption per entry would make the
-- whole-call ceiling meaningless (1024 entries × one over-large segment each),
-- which is precisely what p_budget exists to prevent. This is Kafka's own
-- rule — a fetch response always carries at least one record batch — and it
-- makes the response bounded by `p_budget + one segment` rather than by
-- nothing.
--
-- Alignment with the pop bin path (004_log_pop): `blobs` is flattened in
-- traversal order — entries in INPUT order, segments in base_offset order —
-- so the broker walks meta and blobs with one shared index, exactly like
-- log_pop_wildcard_bin_v1. No base64 on PG, no decode on the broker.
-- ============================================================================

-- DROP+CREATE, not CREATE OR REPLACE: the OUT row type is part of the
-- signature and OR REPLACE refuses return-type changes, so a future column
-- addition would brick boot re-apply (the log_pop_wildcard_bin_v1 precedent).
-- The applier groups a DROP with the statement that follows it into one
-- transaction, so a live HA peer never observes the gap.
DROP FUNCTION IF EXISTS queen.log_fetch_bin_v1(TEXT[], TEXT[], INT8[], INT4[], BIGINT, INT, UUID);
CREATE FUNCTION queen.log_fetch_bin_v1(
    p_queues      TEXT[],
    p_partitions  TEXT[],
    p_offsets     INT8[],
    p_max_bytes   INT4[],
    p_budget      BIGINT DEFAULT 67108864,
    p_max_records INT    DEFAULT 10000,
    p_tenant      UUID   DEFAULT '00000000-0000-0000-0000-000000000001'
) RETURNS TABLE(meta JSONB, blobs BYTEA[])
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    v_n        INT;
    v_i        INT;
    v_qid      UUID;
    v_pid      UUID;
    v_last     BIGINT;
    v_start    BIGINT;
    v_wanted   BIGINT;
    v_from     BIGINT;
    v_entries  JSONB := '[]'::jsonb;
    v_segs     JSONB;
    v_blobs    BYTEA[] := '{}'::bytea[];
    v_budget   BIGINT;
    v_maxb     BIGINT;
    v_cap      INT;
    v_taken    INT;
    v_bytes    BIGINT;
    v_startidx INT;
    v_take     INT;
    v_err      TEXT;
    v_row      RECORD;
    -- Has this CALL emitted any segment yet? The one exemption from the three
    -- bounds is the first segment of the response, not the first of each entry
    -- (see the header).
    v_any      BOOLEAN := FALSE;
BEGIN
    v_n := COALESCE(array_length(p_queues, 1), 0);
    IF v_n = 0 THEN
        meta := jsonb_build_object('entries', '[]'::jsonb);
        blobs := '{}'::bytea[];
        RETURN NEXT;
        RETURN;
    END IF;
    -- Misalignment is a demux bug on the broker side, and a short array would
    -- silently answer the WRONG partition's bounds to a consumer that then
    -- commits them. Fail the whole call loudly instead (the QMULTI guard of
    -- 003_log_push, same reasoning).
    IF array_length(p_partitions, 1) IS DISTINCT FROM v_n
       OR array_length(p_offsets, 1)   IS DISTINCT FROM v_n
       OR array_length(p_max_bytes, 1) IS DISTINCT FROM v_n THEN
        RAISE EXCEPTION 'QFETCH misaligned arrays (% queues)', v_n;
    END IF;

    v_budget := GREATEST(COALESCE(p_budget, 0), 0);
    v_cap    := GREATEST(COALESCE(p_max_records, 1), 1);

    FOR v_i IN 1..v_n LOOP
        v_qid := NULL;
        v_pid := NULL;
        -- Queue identity is the queen.queues id (log_queues is gone), and
        -- queues_tenant_name_uk covers this predicate exactly: ONE indexed row
        -- read. Resolved SEPARATELY from the lane, because "no such queue" and
        -- "queue with an unwritten lane" are different answers — see the header.
        SELECT q.id INTO v_qid
        FROM queen.queues q
        WHERE q.name = p_queues[v_i] AND q.tenant_id = p_tenant;

        IF v_qid IS NULL THEN
            -- Unknown here means unknown TO THIS TENANT — see the header.
            v_entries := v_entries || jsonb_build_object(
                'error', 'UNKNOWN_TOPIC_OR_PARTITION',
                'high', 0, 'logStart', 0, 'segments', '[]'::jsonb);
            CONTINUE;
        END IF;

        -- The lane. UNIQUE (queue_id, name) on queen.log_partitions
        -- (001_log_schema): one indexed row read for the partition id AND both
        -- watermarks.
        SELECT p.id, p.last_offset, p.log_start
        INTO v_pid, v_last, v_start
        FROM queen.log_partitions p
        WHERE p.queue_id = v_qid AND p.name = p_partitions[v_i];

        IF v_pid IS NULL THEN
            -- Never written: the bounds of an empty log, which is what it is.
            -- The segment walk below is unreachable from here (nothing is <=
            -- last_offset = -1), so no read is issued for a lane with no rows.
            v_last  := -1;
            v_start := 0;
        END IF;

        v_wanted := COALESCE(p_offsets[v_i], 0);
        v_segs   := '[]'::jsonb;
        v_err    := NULL;

        IF v_wanted < v_start OR v_wanted > v_last + 1 THEN
            v_err := 'OFFSET_OUT_OF_RANGE';
        ELSIF v_wanted <= v_last THEN
            v_taken := 0;
            v_bytes := 0;
            v_maxb  := GREATEST(COALESCE(p_max_bytes[v_i], 0), 1);

            -- Scan start (§6 of 004_log_pop, same arithmetic): ONE BACKWARD PK
            -- step for the greatest base_offset <= wanted. That segment either
            -- COVERS wanted (the scan starts mid-segment) or it does not (a
            -- fully-consumed head, or wanted sits in a retention gap), in which
            -- case the next segment necessarily starts above wanted and is
            -- taken whole. Filtering on end_offset instead would have no index
            -- to use and would walk the partition from offset zero.
            SELECT s.base_offset INTO v_from
            FROM queen.log_segments s
            WHERE s.partition_id = v_pid AND s.base_offset <= v_wanted
            ORDER BY s.base_offset DESC LIMIT 1;
            IF v_from IS NULL THEN
                v_from := v_wanted;
            END IF;

            -- The forward walk is a cursor: EXIT at the first bound stops the
            -- index scan, so a deep backlog costs only the segments delivered.
            FOR v_row IN
                SELECT s.base_offset, s.end_offset, s.created_at, s.blob
                FROM queen.log_segments s
                WHERE s.partition_id = v_pid AND s.base_offset >= v_from
                ORDER BY s.base_offset
            LOOP
                -- The one row the head probe can hand us that holds nothing we
                -- want: the nearest-below segment that ends before `wanted`.
                CONTINUE WHEN v_row.end_offset < v_wanted;

                -- Bounds bite from the second segment of the CALL onward, so
                -- one over-large segment is delivered rather than stalling the
                -- consumer for ever, and no later entry can claim the same
                -- exemption (see the header).
                EXIT WHEN (v_taken > 0 OR v_any)
                      AND (v_bytes >= v_maxb OR v_budget <= 0 OR v_taken >= v_cap);

                v_startidx := GREATEST(v_wanted - v_row.base_offset, 0)::int;
                v_take := (v_row.end_offset - v_row.base_offset + 1 - v_startidx)::int;
                IF v_taken + v_take > v_cap THEN
                    -- Truncating is safe: records are contiguous from startIdx,
                    -- so the caller resumes at the first offset we did not send.
                    v_take := v_cap - v_taken;
                END IF;
                CONTINUE WHEN v_take <= 0;

                -- AT TIME ZONE 'UTC' before to_char, and it is load-bearing.
                -- `created_at` is TIMESTAMPTZ, so to_char renders it in the
                -- SESSION's TimeZone while the format string ends in a literal
                -- "Z": on any connection whose timezone is not UTC — a
                -- PGTZ/TimeZone set in the environment, in postgresql.conf, or
                -- ALTER ROLE ... SET timezone — every segment would be stamped
                -- local time and LABELLED UTC. The caller parses that back into
                -- epoch milliseconds (server/src/handlers/fetch.rs), so the
                -- error lands in the record timestamp a Kafka consumer reads,
                -- hours out and with nothing on the wire to say so. The
                -- conversion yields a plain TIMESTAMP already in UTC, which
                -- to_char formats verbatim.
                v_segs := v_segs || jsonb_build_object(
                    'base', v_row.base_offset,
                    'startIdx', v_startidx,
                    'take', v_take,
                    'createdAt', to_char(v_row.created_at AT TIME ZONE 'UTC',
                                         'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
                v_blobs := v_blobs || v_row.blob;
                v_taken := v_taken + v_take;
                v_bytes := v_bytes + octet_length(v_row.blob);
                -- The whole-call budget is spent as it is consumed, in entry
                -- order: a later entry can come back empty because an earlier
                -- one filled the response. It still reports its bounds, and its
                -- next fetch makes progress.
                v_budget := GREATEST(v_budget - octet_length(v_row.blob), 0);
                v_any := TRUE;
            END LOOP;
        END IF;

        v_entries := v_entries || (
            CASE WHEN v_err IS NULL
                 THEN jsonb_build_object('high', v_last + 1, 'logStart', v_start,
                                         'segments', v_segs)
                 ELSE jsonb_build_object('error', v_err,
                                         'high', v_last + 1, 'logStart', v_start,
                                         'segments', '[]'::jsonb)
            END);
    END LOOP;

    meta := jsonb_build_object('entries', v_entries);
    blobs := v_blobs;
    RETURN NEXT;
END;
$$;

-- ============================================================================
-- queen.log_fetch_changed_v1 — the long poll's re-probe gate.
--
-- "Has anything about these lanes moved since the (high, logStart) pair I am
-- already holding?" TRUE means re-read, FALSE means the answer the caller
-- already rendered is still, byte for byte, the answer.
--
-- WHY IT EXISTS. A parked fetch re-probes ~5 times a second, and the pop paths
-- learned the hard way (the multitenant discovery-latency regression of
-- 2026-07-24, 004_log_pop and handlers/data.rs) that an EMPTY re-probe must
-- cost zero admission budget: the rate of empty re-polls is O(#parked
-- consumers), and taking the SHARED pop permit for each of them queues real
-- deliveries behind a storm whose wait grows LINEARLY with the consumer count.
-- This is the fetch path's `log_has_pending_v1`: the broker runs it WITHOUT a
-- permit and only spends one on a re-read when it answers TRUE.
--
-- WHY (high, logStart) IS THE WHOLE STATE. Segments are immutable once
-- written: nothing in this schema ever UPDATEs queen.log_segments, the only
-- writers are the push INSERT (003) and the retention DELETE (006), and
-- retention moves log_start in the SAME row update as the delete
-- (006_log_maintenance §"guarded update"). So for a fixed pair of watermarks
-- the byte range [logStart, high) is fixed, and re-running the fetch over the
-- same offsets would rebuild the same records — which is why the broker keeps
-- the body it already rendered instead of paying for the read, the
-- decompression and the render again on every recheck.
--
-- A queue that has vanished (dropped, or a tenant wipe) answers TRUE, so the
-- re-read is the thing that reports UNKNOWN_TOPIC_OR_PARTITION — this function
-- never composes an answer, only a verdict.
--
-- Set-based over the four aligned arrays, so the cost is one indexed row read
-- per entry against log_partitions and queen.queues and NOTHING against
-- queen.log_segments. Misaligned arrays are a broker bug and multi-array
-- unnest() would silently pad them with NULLs, so the lengths are checked
-- first (the QFETCH guard above, same reasoning).
DROP FUNCTION IF EXISTS queen.log_fetch_changed_v1(TEXT[], TEXT[], INT8[], INT8[], UUID);
CREATE FUNCTION queen.log_fetch_changed_v1(
    p_queues     TEXT[],
    p_partitions TEXT[],
    p_highs      INT8[],
    p_starts     INT8[],
    p_tenant     UUID DEFAULT '00000000-0000-0000-0000-000000000001'
) RETURNS BOOLEAN
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    v_n INT;
BEGIN
    v_n := COALESCE(array_length(p_queues, 1), 0);
    IF v_n = 0 THEN
        RETURN FALSE;
    END IF;
    IF array_length(p_partitions, 1) IS DISTINCT FROM v_n
       OR array_length(p_highs, 1)   IS DISTINCT FROM v_n
       OR array_length(p_starts, 1)  IS DISTINCT FROM v_n THEN
        RAISE EXCEPTION 'QFETCHGATE misaligned arrays (% queues)', v_n;
    END IF;

    RETURN EXISTS (
        SELECT 1
        FROM unnest(p_queues, p_partitions, p_highs, p_starts)
             AS w(qname, pname, high, log_start)
        LEFT JOIN queen.queues q
               ON q.name = w.qname AND q.tenant_id = p_tenant
        LEFT JOIN queen.log_partitions p
               ON p.queue_id = q.id AND p.name = w.pname
        -- An unwritten lane is high 0 / logStart 0, the same empty-log bounds
        -- log_fetch_bin_v1 reports for it, so its FIRST push is a change and
        -- its continued absence is not.
        WHERE q.id IS NULL
           OR COALESCE(p.last_offset, -1) + 1 IS DISTINCT FROM w.high
           OR COALESCE(p.log_start, 0)        IS DISTINCT FROM w.log_start
    );
END;
$$;
