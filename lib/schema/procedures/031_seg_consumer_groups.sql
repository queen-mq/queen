-- ============================================================================
-- Storage v2 (segments) — consumer-group management surface.
-- ============================================================================
-- The coordination-layer consumer-group SPs in 008 (list/details/lagging/
-- delete/subscription) operate on queen.partition_consumers (the rows cursor).
-- For a segments queue the cursor lives in queen.partition_consumers instead, so the
-- three MUTATING operations the management API drives — DELETE a group, and
-- SEEK its cursor (per-queue and per-partition) — need segment-native
-- implementations. The read paths (get_consumer_groups_v4 / get_lagging /
-- get_details) are already handled: 027 redefined get_consumer_groups_v4 to be
-- dual-engine, and the broker calls the 008 lag/detail readers directly.
--
-- Applied idempotently at boot after 023..030 (lexical order): the seg_* tables
-- it references all exist by 025.
-- ============================================================================

-- ============================================================================
-- queen.seg_delete_consumer_group_v1: drop a group's SEGMENT cursor state.
-- Removes every queen.partition_consumers row for the group (across all partitions of
-- every queue) and its per-(queue, group) empty-scan watermarks. The rows-side
-- coordination tables (queen.partition_consumers / consumer_groups_metadata /
-- consumer_watermarks) are handled by the sibling queen.delete_consumer_group_v1
-- the broker also calls — p_delete_metadata is echoed for a consistent response
-- shape. deletedPartitions = number of seg_consumers cursors removed.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.seg_delete_consumer_group_v1(
    p_group TEXT,
    p_delete_metadata BOOLEAN DEFAULT TRUE
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted INTEGER;
BEGIN
    -- Scope to SEGMENT cursors (partition_id in queen.seg_partitions): the fold
    -- put both engines' cursors in queen.partition_consumers, so an unscoped
    -- delete would also remove rows-engine cursors that the sibling
    -- queen.delete_consumer_group_v1 owns. Scoping keeps the two engines'
    -- deletes disjoint (exact pre-fold semantics) and the count order-independent.
    DELETE FROM queen.partition_consumers pc
    USING queen.seg_partitions sp
    WHERE pc.partition_id = sp.id AND pc.consumer_group = p_group;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    -- Drop the empty-scan watermark so a later group of the same name does not
    -- inherit a stale "queue is empty since T" floor and silently skip cold
    -- partitions (symmetric with the watermark cleanup in seg_seek_* below).
    DELETE FROM queen.consumer_watermarks WHERE consumer_group = p_group;

    RETURN jsonb_build_object(
        'success', true,
        'consumerGroup', p_group,
        'deletedPartitions', v_deleted,
        'metadataDeleted', p_delete_metadata
    );
END;
$$;

-- ============================================================================
-- Cursor seek — shared helper: position ONE (partition, group) seg cursor.
--   p_to_end     -> next_seq = last_seq + 1, next_off = 0 (skip everything).
--   p_timestamp  -> seq of the first segment with created_at >= p_timestamp
--                   (created_at is monotone in seq: a forward walk from the
--                   retention watermark), next_off = 0; last_seq + 1 when
--                   nothing that recent exists yet (future-only cursor).
-- Any live lease is released and the retry state reset; the row is created when
-- the group has never popped this partition (INSERT ... ON CONFLICT DO UPDATE).
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.seg_seek_one_v1(
    p_partition_id UUID,
    p_last_seq BIGINT,
    p_retention_seq BIGINT,
    p_group TEXT,
    p_to_end BOOLEAN,
    p_timestamp TIMESTAMPTZ
) RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_seq BIGINT;
BEGIN
    IF p_to_end THEN
        v_seq := p_last_seq + 1;
    ELSE
        SELECT s.seq INTO v_seq
        FROM queen.seg_segments s
        WHERE s.partition_id = p_partition_id
          AND s.seq >= p_retention_seq
          AND s.created_at >= p_timestamp
        ORDER BY s.seq LIMIT 1;
        IF v_seq IS NULL THEN
            v_seq := p_last_seq + 1;
        END IF;
    END IF;

    INSERT INTO queen.partition_consumers (
        partition_id, consumer_group, next_seq, next_off,
        worker_id, lease_expires_at, batch_end_seq, batch_end_off,
        attempt_seq, attempt_off, attempt_count)
    VALUES (p_partition_id, p_group, v_seq, 0,
            NULL, NULL, NULL, NULL, NULL, NULL, 0)
    ON CONFLICT (partition_id, consumer_group) DO UPDATE SET
        next_seq = EXCLUDED.next_seq,
        next_off = 0,
        worker_id = NULL, lease_expires_at = NULL,
        batch_end_seq = NULL, batch_end_off = NULL,
        attempt_seq = NULL, attempt_off = NULL, attempt_count = 0;
END;
$$;

-- ============================================================================
-- queen.seg_seek_consumer_group_v1: seek the segment cursor for EVERY partition
-- of a queue. Body of the POST .../seek route (per-queue variant).
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.seg_seek_consumer_group_v1(
    p_group TEXT,
    p_queue TEXT,
    p_to_end BOOLEAN DEFAULT FALSE,
    p_timestamp TIMESTAMPTZ DEFAULT NULL
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_p RECORD;
    v_updated INTEGER := 0;
BEGIN
    IF NOT p_to_end AND p_timestamp IS NULL THEN
        RETURN jsonb_build_object(
            'success', false,
            'error', 'Must specify either toEnd=true or a timestamp');
    END IF;

    FOR v_p IN
        SELECT p.id, p.last_seq, p.retention_seq
        FROM queen.seg_partitions p
        JOIN queen.seg_queues q ON q.id = p.queue_id
        WHERE q.name = p_queue
    LOOP
        PERFORM queen.seg_seek_one_v1(v_p.id, v_p.last_seq, v_p.retention_seq,
                                      p_group, p_to_end, p_timestamp);
        v_updated := v_updated + 1;
    END LOOP;

    -- Clear the empty-scan watermark so a backward seek re-exposes cold
    -- partitions to the wildcard candidate filter on the next pop.
    DELETE FROM queen.consumer_watermarks
    WHERE queue_name = p_queue AND consumer_group = p_group;

    RETURN jsonb_build_object(
        'success', true,
        'consumerGroup', p_group,
        'queueName', p_queue,
        'seekToEnd', p_to_end,
        'targetTimestamp', CASE WHEN p_timestamp IS NOT NULL
            THEN to_char(p_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
        'partitionsUpdated', v_updated
    );
END;
$$;

-- ============================================================================
-- queen.seg_seek_partition_v1: seek the segment cursor for ONE named partition.
-- Body of the POST .../partitions/:partition/seek route.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.seg_seek_partition_v1(
    p_group TEXT,
    p_queue TEXT,
    p_partition TEXT,
    p_to_end BOOLEAN DEFAULT FALSE,
    p_timestamp TIMESTAMPTZ DEFAULT NULL
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_id UUID;
    v_last_seq BIGINT;
    v_retention_seq BIGINT;
BEGIN
    IF NOT p_to_end AND p_timestamp IS NULL THEN
        RETURN jsonb_build_object(
            'success', false,
            'error', 'Must specify either toEnd=true or a timestamp');
    END IF;

    SELECT p.id, p.last_seq, p.retention_seq
    INTO v_id, v_last_seq, v_retention_seq
    FROM queen.seg_partitions p
    JOIN queen.seg_queues q ON q.id = p.queue_id
    WHERE q.name = p_queue AND p.name = p_partition;

    IF v_id IS NULL THEN
        RETURN jsonb_build_object('success', false, 'error', 'Partition not found');
    END IF;

    PERFORM queen.seg_seek_one_v1(v_id, v_last_seq, v_retention_seq,
                                  p_group, p_to_end, p_timestamp);

    DELETE FROM queen.consumer_watermarks
    WHERE queue_name = p_queue AND consumer_group = p_group;

    RETURN jsonb_build_object(
        'success', true,
        'consumerGroup', p_group,
        'queueName', p_queue,
        'partitionName', p_partition,
        'seekToEnd', p_to_end,
        'targetTimestamp', CASE WHEN p_timestamp IS NOT NULL
            THEN to_char(p_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
        'updated', true
    );
END;
$$;

GRANT EXECUTE ON FUNCTION queen.seg_delete_consumer_group_v1(TEXT, BOOLEAN) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.seg_seek_one_v1(UUID, BIGINT, BIGINT, TEXT, BOOLEAN, TIMESTAMPTZ) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.seg_seek_consumer_group_v1(TEXT, TEXT, BOOLEAN, TIMESTAMPTZ) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.seg_seek_partition_v1(TEXT, TEXT, TEXT, BOOLEAN, TIMESTAMPTZ) TO PUBLIC;
