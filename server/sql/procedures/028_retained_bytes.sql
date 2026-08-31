-- ============================================================================
-- Retained-bytes slow lane (PLAN_STATS_REFRESH.md T1.0).
--
--   queen.log_refresh_retained_bytes_v1   recompute queen.stats.retained_bytes
--                                         and queen.stats.segment_count
--                                         ('queue' rows) from live segments
--
-- The ONE O(segments) aggregate the stats family still needs: retained payload
-- bytes per queue = SUM(octet_length(blob)) over live segments, and — since
-- 2026-08-23, out of the same scan — the live segment COUNT per queue, which
-- the queue list (log_queue_stats_all_v1) used to recount on every call,
-- Θ(the tenant's segments) per list and growing with retention. An unqualified
-- heap scan of queen.log_segments by construction (octet_length is not in the
-- PK, and the zero-secondary-index property of that table is load-bearing for
-- the push path; the answer to this scan's cost is to run it RARELY, never to
-- index it). octet_length on STORAGE EXTERNAL blobs reads the length from the
-- TOAST pointer (no chunk detoast); it measures the compressed (zstd)
-- on-the-wire bytes AS STORED — TOAST bookkeeping, indexes, WAL and the
-- log_txns hash sidecar are excluded.
--
-- It used to run inside EVERY log_refresh_all_stats_v1 cycle (the largest
-- single term of the #1 query on the prod instance) to feed exactly one
-- consequential reader: the proxy storage quota (hard 403,
-- proxy/src/registry.rs), which is hysteretic by contract. So it lives here on
-- its own slow cadence instead — stats.rs::spawn_retained_bytes,
-- RETAINED_BYTES_INTERVAL_MS, advisory lock 737_003 — and 011's refresh
-- self-assigns the column and never writes it.
--
-- Write discipline (PLAN_STATS_REFRESH.md §8.4):
--   * drive from queen.queues LEFT JOIN the aggregate — a drained queue is
--     written 0, not skipped (a skipped queue could never satisfy the proxy's
--     release band and would stay 403-blocked forever);
--   * materialise the scan into arrays FIRST, holding no row locks; only then
--     pre-lock the O(queues) stat rows FOR UPDATE in queue_id order (the same
--     order 011's driving SELECT upserts in) and write from unnest;
--   * write ONLY retained_bytes and segment_count (this lane's two columns) —
--     never last_computed_at (that stamp is the counters cycle's freshness
--     signal), never the prev_* rate trio.
-- ============================================================================

CREATE OR REPLACE FUNCTION queen.log_refresh_retained_bytes_v1()
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_now       TIMESTAMPTZ := NOW();
    v_queue_ids UUID[];
    v_bytes     BIGINT[];
    v_segs      BIGINT[];
    v_updated   INTEGER := 0;
BEGIN
    -- Phase 1 — the scan, lock-free. One value per EXISTING queue: a queue the
    -- aggregate has no row for holds no live segments and is written 0.
    SELECT COALESCE(array_agg(t.id    ORDER BY t.id), '{}'),
           COALESCE(array_agg(t.bytes ORDER BY t.id), '{}'),
           COALESCE(array_agg(t.segs  ORDER BY t.id), '{}')
      INTO v_queue_ids, v_bytes, v_segs
      FROM (
        SELECT q.id,
               COALESCE(sb.retained_bytes, 0)::bigint AS bytes,
               COALESCE(sb.segment_count, 0)::bigint  AS segs
        FROM queen.queues q
        LEFT JOIN (
            SELECT lp.queue_id,
                   SUM(octet_length(s.blob))::bigint AS retained_bytes,
                   COUNT(*)::bigint                  AS segment_count
            FROM queen.log_segments s
            JOIN queen.log_partitions lp ON lp.id = s.partition_id
            GROUP BY lp.queue_id
        ) sb ON sb.queue_id = q.id
      ) t;

    IF cardinality(v_queue_ids) = 0 THEN
        RETURN jsonb_build_object(
            'queuesUpdated', 0,
            'refreshedAt', to_char(v_now AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'));
    END IF;

    -- Phase 2 — canonical-order pre-lock, then the narrow write. A stat row
    -- that does not exist yet (queue created after the last counters cycle) is
    -- simply skipped here: 011 inserts it with the DDL default 0 and the next
    -- pass of this lane fills it (the accepted new-queue blind window).
    PERFORM 1
      FROM queen.stats st
     WHERE st.stat_type = 'queue' AND st.queue_id = ANY (v_queue_ids)
     ORDER BY st.queue_id
       FOR UPDATE;

    UPDATE queen.stats st
       SET retained_bytes = u.bytes,
           segment_count  = u.segs
      FROM unnest(v_queue_ids, v_bytes, v_segs) AS u(queue_id, bytes, segs)
     WHERE st.stat_type = 'queue'
       AND st.queue_id  = u.queue_id;

    GET DIAGNOSTICS v_updated = ROW_COUNT;

    RETURN jsonb_build_object(
        'queuesUpdated', v_updated,
        'bytesTotal', (SELECT COALESCE(SUM(b), 0) FROM unnest(v_bytes) AS b),
        'segmentsTotal', (SELECT COALESCE(SUM(s), 0) FROM unnest(v_segs) AS s),
        'refreshedAt', to_char(v_now AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'));
END;
$$;

GRANT EXECUTE ON FUNCTION queen.log_refresh_retained_bytes_v1() TO PUBLIC;
