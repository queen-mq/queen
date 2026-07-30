-- ============================================================================
-- Analytics Stored Procedures
-- ============================================================================
-- Queue management operations
-- Note: get_queues_v1, get_queue_v1, get_namespaces_v1, get_tasks_v1, 
--       get_system_overview_v1 have been replaced by v2 versions in 013_stats.sql
-- ============================================================================

-- ============================================================================
-- queen.delete_queue_v1: Delete queue with all related data
-- ============================================================================
-- Track B (§5): p_tenant scopes the delete to one tenant's queue (name alone is
-- no longer unique). DROP the (TEXT) form so the scoped (TEXT, UUID) is unambiguous.
DROP FUNCTION IF EXISTS queen.delete_queue_v1(TEXT);
CREATE OR REPLACE FUNCTION queen.delete_queue_v1(
    p_queue_name TEXT,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001')
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_existed BOOLEAN;
BEGIN
    -- The rows-engine cleanup that used to open this body (partition consumers,
    -- reached through the rows partition table) went with the rows message plane:
    -- both tables are dropped, and only the log engine can produce consumers now.
    -- This function only ever owned the SHARED rows of a queue; the log-engine
    -- state (log_txns / log_dlq / log_queues -> log_partitions -> log_segments /
    -- log_consumers) is dropped by the caller, db::delete_seg_queue in src/db.rs,
    -- which runs immediately after this call on the delete-queue path.

    -- Delete watermarks for this queue to prevent orphaned entries
    DELETE FROM queen.consumer_watermarks
    WHERE queue_name = p_queue_name AND tenant_id = p_tenant;

    -- Delete queue (CASCADE handles the queen.stats rows keyed on queue_id)
    WITH deleted AS (
        DELETE FROM queen.queues WHERE name = p_queue_name AND tenant_id = p_tenant
        RETURNING id
    )
    SELECT EXISTS (SELECT 1 FROM deleted) INTO v_existed;

    RETURN jsonb_build_object(
        'deleted', true,
        'queue', p_queue_name,
        'existed', v_existed
    );
END;
$$;

-- Grant execute permissions
GRANT EXECUTE ON FUNCTION queen.delete_queue_v1(TEXT, UUID) TO PUBLIC;
