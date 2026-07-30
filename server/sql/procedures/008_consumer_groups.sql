-- ============================================================================
-- Consumer Group Stored Procedures
-- ============================================================================
-- Async stored procedures for consumer group management and analytics.
--
-- Segments-only cleanup: this file used to carry the rows-engine consumer-group
-- readers and the rows seek path. All of it is gone —
--   * get_consumer_groups_v1 / _v2 / _v3, seek_consumer_group_v1,
--     seek_partition_v1: zero callers; the live seek path is
--     queen.log_seek_consumer_group_v1 / queen.log_seek_partition_v1 (047).
--   * get_consumer_group_details_v1, get_consumer_groups_v4,
--     get_lagging_partitions_v1: 047_log_admin.sql redefines all three (and
--     drops these old signatures), so the copies here were overwritten
--     milliseconds later at every boot and could never run.
-- What remains is the engine-agnostic MANAGEMENT plane over the SHARED tables
-- queen.consumer_groups_metadata and queen.consumer_watermarks.
-- ============================================================================

-- Old DBs that already applied the previous revision keep these functions until
-- something drops them; nothing else does, and their bodies reference tables the
-- rows-engine teardown removes. Drop them here, where they were created.
DROP FUNCTION IF EXISTS queen.get_consumer_groups_v1();
DROP FUNCTION IF EXISTS queen.get_consumer_groups_v2();
DROP FUNCTION IF EXISTS queen.get_consumer_groups_v3();
DROP FUNCTION IF EXISTS queen.seek_consumer_group_v1(TEXT, TEXT, TIMESTAMPTZ, BOOLEAN);
DROP FUNCTION IF EXISTS queen.seek_partition_v1(TEXT, TEXT, TEXT);

-- ============================================================================
-- queen.delete_consumer_group_v1: Delete consumer group and optionally metadata
-- ============================================================================
-- Track B (PLAN_QUEEN_PROXY_CLOUD.md §5): p_tenant scopes the deletes of the
-- SHARED (tenant_id-keyed) tables consumer_groups_metadata + consumer_watermarks —
-- else this cleanup would nuke EVERY tenant's rows for a group name. Old
-- 2-arg form dropped so the new defaulted arg is unambiguous.
-- The log cursors themselves are cleared by queen.log_delete_consumer_group_v1
-- (047), which the broker calls alongside this one.
DROP FUNCTION IF EXISTS queen.delete_consumer_group_v1(TEXT, BOOLEAN);
CREATE OR REPLACE FUNCTION queen.delete_consumer_group_v1(
    p_consumer_group TEXT,
    p_delete_metadata BOOLEAN DEFAULT TRUE,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001'
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    -- The rows-engine cursor delete that used to feed this counter is gone with
    -- the rows engine; it already returned 0 on a segments-only deployment.
    -- Kept at 0 so the JSON shape (deletedPartitions) the broker sums stays put.
    v_deleted_count INTEGER := 0;
BEGIN
    -- Delete metadata if requested (scoped to the tenant's rows only)
    IF p_delete_metadata THEN
        DELETE FROM queen.consumer_groups_metadata
        WHERE consumer_group = p_consumer_group
          AND tenant_id = p_tenant;
    END IF;

    -- Delete watermarks to prevent stale watermark from blocking re-consumption
    -- if a new consumer group with the same name is created later (tenant-scoped).
    DELETE FROM queen.consumer_watermarks
    WHERE consumer_group = p_consumer_group
      AND tenant_id = p_tenant;

    RETURN jsonb_build_object(
        'success', true,
        'consumerGroup', p_consumer_group,
        'deletedPartitions', v_deleted_count,
        'metadataDeleted', p_delete_metadata
    );
END;
$$;

-- ============================================================================
-- queen.update_consumer_group_subscription_v1: Update subscription timestamp
-- ============================================================================
-- Track B (§5): p_tenant scopes the subscription update + watermark reset to ONE
-- tenant's (queue_name, consumer_group) rows. Old 2-arg form dropped.
DROP FUNCTION IF EXISTS queen.update_consumer_group_subscription_v1(TEXT, TEXT);
CREATE OR REPLACE FUNCTION queen.update_consumer_group_subscription_v1(
    p_consumer_group TEXT,
    p_new_timestamp TEXT,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001'
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_updated_count INTEGER;
BEGIN
    UPDATE queen.consumer_groups_metadata
    SET subscription_timestamp = p_new_timestamp::timestamptz
    WHERE consumer_group = p_consumer_group
      AND tenant_id = p_tenant;

    GET DIAGNOSTICS v_updated_count = ROW_COUNT;

    -- Invalidate watermarks for this consumer group so a backward move of the
    -- subscription timestamp re-exposes historical partitions to the wildcard
    -- POP scan filter (log_partitions.last_write_at >= watermark - 2m). Without
    -- this, a stale watermark from a previously caught-up run would block the
    -- replay silently. Symmetric with the watermark cleanup already done in
    -- queen.log_seek_consumer_group_v1 (047, backward seek) and
    -- queen.delete_consumer_group_v1.
    --
    -- Deleted unconditionally (forward moves too) because:
    --   - Forward moves: the watermark is automatically re-established on
    --     the next empty scan within 30s; transient extra work is negligible.
    --   - Backward moves: deletion is REQUIRED for correctness.
    --   - Unconditional deletion removes a foot-gun and matches the
    --     idempotent always-safe pattern of sibling procedures.
    --
    -- Scope mirrors the UPDATE above (no queue_name filter): all
    -- (queue_name, consumer_group) entries for this CG. See
    -- cdocs/WATERMARK_AND_TRANSACTION_FIXES.md §2 (Fix B).
    -- Track B (§5): also tenant-scoped, mirroring the UPDATE.
    DELETE FROM queen.consumer_watermarks
    WHERE consumer_group = p_consumer_group
      AND tenant_id = p_tenant;

    RETURN jsonb_build_object(
        'success', true,
        'consumerGroup', p_consumer_group,
        'newTimestamp', p_new_timestamp,
        'rowsUpdated', v_updated_count
    );
END;
$$;

-- ============================================================================
-- queen.delete_consumer_group_for_queue_v1: Delete consumer group for specific queue only
-- ============================================================================
-- Track B (§5): p_tenant scopes the shared-table deletes to ONE tenant. Old
-- 3-arg form dropped so the new defaulted arg is unambiguous.
-- The per-queue log cursor clear lives broker-side (db.rs
-- delete_consumer_group_for_queue_seg, over queen.log_consumers).
DROP FUNCTION IF EXISTS queen.delete_consumer_group_for_queue_v1(TEXT, TEXT, BOOLEAN);
CREATE OR REPLACE FUNCTION queen.delete_consumer_group_for_queue_v1(
    p_consumer_group TEXT,
    p_queue_name TEXT,
    p_delete_metadata BOOLEAN DEFAULT TRUE,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001'
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    -- Same as delete_consumer_group_v1: the rows-engine per-queue cursor delete
    -- that fed this counter is gone and already returned 0 here. Held at 0 to
    -- keep the deletedPartitions key the broker sums.
    v_deleted_count INTEGER := 0;
BEGIN
    -- Delete metadata if requested (only for this queue, tenant-scoped)
    IF p_delete_metadata THEN
        DELETE FROM queen.consumer_groups_metadata
        WHERE consumer_group = p_consumer_group
          AND queue_name = p_queue_name
          AND tenant_id = p_tenant;
    END IF;

    -- Delete watermark for this specific (queue, consumer_group) to prevent
    -- stale watermark from blocking re-consumption if CG is recreated (tenant-scoped).
    DELETE FROM queen.consumer_watermarks
    WHERE queue_name = p_queue_name
      AND consumer_group = p_consumer_group
      AND tenant_id = p_tenant;

    RETURN jsonb_build_object(
        'success', true,
        'consumerGroup', p_consumer_group,
        'queueName', p_queue_name,
        'deletedPartitions', v_deleted_count,
        'metadataDeleted', p_delete_metadata
    );
END;
$$;

-- Grant execute permissions
GRANT EXECUTE ON FUNCTION queen.delete_consumer_group_v1(TEXT, BOOLEAN, UUID) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.update_consumer_group_subscription_v1(TEXT, TEXT, UUID) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.delete_consumer_group_for_queue_v1(TEXT, TEXT, BOOLEAN, UUID) TO PUBLIC;
