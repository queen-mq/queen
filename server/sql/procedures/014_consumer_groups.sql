-- ============================================================================
-- Consumer Group Stored Procedures
-- ============================================================================
-- Async stored procedures for consumer group management and analytics.
--
-- Segments-only cleanup: this file used to carry the rows-engine consumer-group
-- readers and the rows seek path. All of it is gone —
--   * get_consumer_groups_v1 / _v2 / _v3, seek_consumer_group_v1,
--     seek_partition_v1: zero callers; the live seek path is
--     queen.log_seek_consumer_group_v1 / queen.log_seek_partition_v1
--     (010_log_admin).
--   * get_consumer_group_details_v1, get_consumer_groups_v4,
--     get_lagging_partitions_v1: 010_log_admin.sql owns the live definitions,
--     so the copies here were shadowed at every boot and could never run.
-- What remains is the engine-agnostic MANAGEMENT plane over the SHARED tables
-- queen.consumer_groups_metadata and queen.consumer_watermarks.
-- ============================================================================

-- ============================================================================
-- queen.delete_consumer_group_v1: Delete consumer group and optionally metadata
-- ============================================================================
-- Track B (PLAN_QUEEN_PROXY_CLOUD.md §5): p_tenant scopes the deletes of the
-- SHARED tables consumer_groups_metadata (tenant_id-keyed) and
-- consumer_watermarks (queue_id-keyed; tenant inherited from the queue row) —
-- else this cleanup would nuke EVERY tenant's rows for a group name.
-- The log cursors themselves are cleared by queen.log_delete_consumer_group_v1
-- (010_log_admin), which the broker calls alongside this one.
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
    -- if a new consumer group with the same name is created later. Watermarks
    -- key on the queen.queues id now (no tenant_id column of their own), so the
    -- tenant scoping resolves through the queue row.
    DELETE FROM queen.consumer_watermarks w
    USING queen.queues q
    WHERE w.queue_id = q.id
      AND w.consumer_group = p_consumer_group
      AND q.tenant_id = p_tenant;

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
-- tenant's (queue, consumer_group) rows.
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
    -- queen.log_seek_consumer_group_v1 (010_log_admin, backward seek) and
    -- queen.delete_consumer_group_v1.
    --
    -- Deleted unconditionally (forward moves too) because:
    --   - Forward moves: the watermark is automatically re-established on
    --     the next empty scan within 30s; transient extra work is negligible.
    --   - Backward moves: deletion is REQUIRED for correctness.
    --   - Unconditional deletion removes a foot-gun and matches the
    --     idempotent always-safe pattern of sibling procedures.
    --
    -- Scope mirrors the UPDATE above (no queue filter): all
    -- (queue, consumer_group) entries for this CG. See
    -- cdocs/WATERMARK_AND_TRANSACTION_FIXES.md §2 (Fix B).
    -- Track B (§5): also tenant-scoped, mirroring the UPDATE. Watermarks key on
    -- the queen.queues id, so the tenant resolves through the queue row.
    DELETE FROM queen.consumer_watermarks w
    USING queen.queues q
    WHERE w.queue_id = q.id
      AND w.consumer_group = p_consumer_group
      AND q.tenant_id = p_tenant;

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
-- Track B (§5): p_tenant scopes the shared-table deletes to ONE tenant.
-- The per-queue log cursor clear lives broker-side (db.rs
-- delete_consumer_group_for_queue_seg, over queen.log_consumers).
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
    v_queue_id UUID;
BEGIN
    -- Queue identity is the queen.queues id: resolve (name, tenant) once; a
    -- missing queue makes v_queue_id NULL and both deletes match nothing,
    -- exactly as the old name-keyed deletes did.
    SELECT id INTO v_queue_id
    FROM queen.queues
    WHERE name = p_queue_name AND tenant_id = p_tenant;

    -- Delete metadata if requested (only this queue's rows; the queue id
    -- already carries the tenant scope, and discovery rows — queue_id NULL —
    -- are untouched by construction)
    IF p_delete_metadata THEN
        DELETE FROM queen.consumer_groups_metadata
        WHERE consumer_group = p_consumer_group
          AND queue_id = v_queue_id;
    END IF;

    -- Delete watermark for this specific (queue, consumer_group) to prevent
    -- stale watermark from blocking re-consumption if CG is recreated.
    DELETE FROM queen.consumer_watermarks
    WHERE queue_id = v_queue_id
      AND consumer_group = p_consumer_group;

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
