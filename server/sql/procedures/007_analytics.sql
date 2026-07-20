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
CREATE OR REPLACE FUNCTION queen.delete_queue_v1(p_queue_name TEXT)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_existed BOOLEAN;
BEGIN
    -- Delete partition consumers first
    DELETE FROM queen.partition_consumers
    WHERE partition_id IN (
        SELECT p.id FROM queen.partitions p
        JOIN queen.queues q ON p.queue_id = q.id
        WHERE q.name = p_queue_name
    );

    -- Delete watermarks for this queue to prevent orphaned entries
    DELETE FROM queen.consumer_watermarks
    WHERE queue_name = p_queue_name;

    -- Delete queue (CASCADE handles partitions and messages)
    WITH deleted AS (
        DELETE FROM queen.queues WHERE name = p_queue_name
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
GRANT EXECUTE ON FUNCTION queen.delete_queue_v1(TEXT) TO PUBLIC;
