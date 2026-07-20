-- ============================================================================
-- partition_lookup maintenance (PUSHPOPLOOKUPSOL)
-- ============================================================================
--
-- Context
-- -------
-- Previously, queen.partition_lookup was maintained by the statement-level
-- trigger trg_update_partition_lookup firing inside every push transaction.
-- At high concurrency this became the dominant serialization point (push-vs-
-- push row-lock waits on partition_lookup rows — see cdocs/PUSHVSPOP.md).
--
-- The trigger has been dropped. Maintenance now happens in two places:
--
--   1. queen.update_partition_lookup_v1(p_updates jsonb)
--      Called by libqueen AFTER a push transaction commits, with the list of
--      {queue_name, partition_id, last_message_id, last_message_created_at}
--      tuples the push just inserted. This is the primary writer and keeps
--      partition_lookup fresh within a few milliseconds of the push.
--
--   2. queen.reconcile_partition_lookup_v1(p_lookback_seconds int)
--      Called on a timer by the server's PartitionLookupReconcileService
--      (default every 5s). Scans queen.messages for recent activity and
--      advances any partition_lookup rows that fell behind (e.g. because
--      the server crashed in the microsecond window between push commit and
--      update_partition_lookup_v1 scheduling, or because the call itself
--      failed). Uses FOR UPDATE SKIP LOCKED so it never blocks the primary
--      writer.
--
-- Both procedures are idempotent under concurrent callers: the WHERE clause
-- on the UPSERT only advances last_message_created_at (and tie-breaks on
-- last_message_id), so order-of-arrival is irrelevant.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- queen.update_partition_lookup_v1
-- ----------------------------------------------------------------------------
--
-- Primary writer: UPSERT one row per partition in p_updates.
--
-- Input: JSONB array of objects, each with:
--   queue_name               TEXT
--   partition_id             UUID (as string)
--   last_message_id          UUID (as string)
--   last_message_created_at  TIMESTAMPTZ-parseable string
--
-- Returns the number of rows actually inserted or updated (useful for
-- observability; ignored by the caller on the happy path).
--
-- The WHERE clause on ON CONFLICT DO UPDATE makes this monotonic: stale
-- updates from a slow pusher never clobber newer data from a faster one.
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION queen.update_partition_lookup_v1(p_updates JSONB)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_count INT;
BEGIN
    IF p_updates IS NULL OR jsonb_array_length(p_updates) = 0 THEN
        RETURN 0;
    END IF;

    INSERT INTO queen.partition_lookup (
        queue_name, partition_id, last_message_id, last_message_created_at, updated_at
    )
    SELECT
        (u->>'queue_name')::TEXT,
        (u->>'partition_id')::UUID,
        (u->>'last_message_id')::UUID,
        (u->>'last_message_created_at')::TIMESTAMPTZ,
        NOW()
    FROM jsonb_array_elements(p_updates) u
    -- Consistent lock ordering across concurrent callers prevents deadlocks.
    ORDER BY (u->>'partition_id')::UUID
    ON CONFLICT (queue_name, partition_id) DO UPDATE SET
        last_message_id         = EXCLUDED.last_message_id,
        last_message_created_at = EXCLUDED.last_message_created_at,
        updated_at              = NOW()
    WHERE
        EXCLUDED.last_message_created_at > queen.partition_lookup.last_message_created_at
        OR (EXCLUDED.last_message_created_at = queen.partition_lookup.last_message_created_at
            AND EXCLUDED.last_message_id > queen.partition_lookup.last_message_id);

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$;

GRANT EXECUTE ON FUNCTION queen.update_partition_lookup_v1(JSONB) TO PUBLIC;


-- ----------------------------------------------------------------------------
-- queen.reconcile_partition_lookup_v1
-- ----------------------------------------------------------------------------
--
-- Safety-net reconciler. Scans messages created in the last
-- p_lookback_seconds (default 60s), computes the per-partition max, and
-- advances any partition_lookup rows that are behind.
--
-- FOR UPDATE OF pl SKIP LOCKED guarantees this never blocks the primary
-- writer. Partitions that are locked by a concurrent
-- update_partition_lookup_v1 call at the moment of the scan are simply
-- skipped and picked up on the next cycle (they'll have already been
-- updated by the primary writer anyway).
--
-- The lookback window bounds the amount of data scanned. At default 60s
-- and a ~10k msg/s push rate this scans ~600k index entries via
-- idx_messages_partition_created as a skip scan — a few tens of ms.
--
-- If the server has been down for longer than the lookback window, some
-- messages inserted before the crash may never be reflected in
-- partition_lookup. Operators can set p_lookback_seconds to a larger value
-- after a known outage to catch up.
-- ----------------------------------------------------------------------------

-- Implementation note:
-- The naive form of this query ("SELECT DISTINCT partition_id FROM messages
-- WHERE created_at > NOW() - lookback") triggers a parallel seq scan on
-- queen.messages because there is no index on created_at alone (and we don't
-- want one — see cdocs/PUSHVSPOP.md §index cleanup).
--
-- Instead we drive the scan from partition_lookup (small table, one row per
-- partition) and use a LATERAL subquery to find each partition's true latest
-- message via idx_messages_partition_created (partition_id, created_at, id).
-- That gives O(P) index seeks per cycle, each one sub-millisecond. At 1000
-- partitions this is a few ms total, regardless of how many rows are in
-- queen.messages.
--
-- p_lookback_seconds is still honored: we only look at partition_lookup rows
-- whose current last_message_created_at is within the lookback window OR
-- whose updated_at is stale. This limits the per-cycle work when the queue
-- is truly idle.
CREATE OR REPLACE FUNCTION queen.reconcile_partition_lookup_v1(
    p_lookback_seconds INT DEFAULT 60
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_count    INT;
    v_cutoff   TIMESTAMPTZ := NOW() - (p_lookback_seconds || ' seconds')::interval;
BEGIN
    WITH
    -- Partition_lookup rows that are candidates for reconciliation. We
    -- consider a row stale if either:
    --   (a) its stored last_message_created_at is newer than the cutoff
    --       (it saw activity recently, so a newer message might have been
    --        inserted via a path that didn't update partition_lookup); or
    --   (b) its updated_at is older than the cutoff (something is off).
    -- In practice (a) covers the hot set and keeps the working scan tiny.
    stale AS (
        SELECT pl.queue_name,
               pl.partition_id,
               pl.last_message_id,
               pl.last_message_created_at
        FROM queen.partition_lookup pl
        WHERE pl.last_message_created_at >= v_cutoff
           OR pl.updated_at               <  v_cutoff
    ),
    -- For each candidate, find the actual max (created_at, id) in
    -- queen.messages via an index lookup on idx_messages_partition_created.
    fresh AS (
        SELECT s.queue_name,
               s.partition_id,
               m.id         AS last_message_id,
               m.created_at AS last_message_created_at
        FROM stale s,
        LATERAL (
            SELECT id, created_at
            FROM queen.messages
            WHERE partition_id = s.partition_id
            ORDER BY created_at DESC, id DESC
            LIMIT 1
        ) m
        WHERE (m.created_at, m.id) > (s.last_message_created_at, s.last_message_id)
    ),
    -- SKIP LOCKED so we never block the primary writer.
    lockable AS (
        SELECT f.queue_name, f.partition_id
        FROM queen.partition_lookup pl
        JOIN fresh f
          ON pl.queue_name = f.queue_name AND pl.partition_id = f.partition_id
        FOR UPDATE OF pl SKIP LOCKED
    )
    UPDATE queen.partition_lookup pl
    SET last_message_id         = f.last_message_id,
        last_message_created_at = f.last_message_created_at,
        updated_at              = NOW()
    FROM fresh f, lockable l
    WHERE pl.queue_name   = l.queue_name
      AND pl.partition_id = l.partition_id
      AND pl.queue_name   = f.queue_name
      AND pl.partition_id = f.partition_id;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$;

GRANT EXECUTE ON FUNCTION queen.reconcile_partition_lookup_v1(INT) TO PUBLIC;
