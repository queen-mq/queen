-- ============================================================================
-- Analytics Stored Procedures
-- ============================================================================
-- Queue management operations
-- Note: get_queues_v1, get_queue_v1, get_namespaces_v1, get_tasks_v1,
--       get_system_overview_v1 have been replaced by v2 versions in 018_stats.sql
-- ============================================================================

-- ============================================================================
-- queen.delete_queue_v1: Delete queue with all related data
-- ============================================================================
-- Track B (§5): p_tenant scopes the delete to one tenant's queue (name alone is
-- no longer unique).
--
-- Queue-identity merge (2026-07-31): this SP is now the ONE implementation of
-- queue deletion. queen.log_partitions hangs off queen.queues(id) directly, so
-- a single DELETE on queen.queues cascades to log_partitions -> log_segments /
-- log_consumers, consumer_watermarks, queue-scoped consumer_groups_metadata,
-- queue_lag_metrics and queen.stats. The broker-side log-table teardown that
-- used to run after this call (db::delete_seg_queue) is retired.
CREATE OR REPLACE FUNCTION queen.delete_queue_v1(
    p_queue_name TEXT,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001')
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_queue_id UUID;
BEGIN
    -- Queue identity is the queen.queues id: one lookup, everything below keys
    -- off it.
    SELECT id INTO v_queue_id
    FROM queen.queues
    WHERE name = p_queue_name AND tenant_id = p_tenant;

    IF v_queue_id IS NOT NULL THEN
        -- queen.log_txns and queen.log_dlq are FK-less BY DESIGN (the purge
        -- path must never pay FK-trigger cost — 001_log_schema), so the cascade below
        -- cannot reach them. Enumerate the queue's partition ids and clear
        -- both tables first, while the log_partitions rows still exist.
        DELETE FROM queen.log_txns
        WHERE partition_id IN (
            SELECT id FROM queen.log_partitions WHERE queue_id = v_queue_id);

        DELETE FROM queen.log_dlq
        WHERE partition_id IN (
            SELECT id FROM queen.log_partitions WHERE queue_id = v_queue_id);

        -- One delete; the FK cascades handle the rest: log_partitions (->
        -- log_segments, log_consumers), consumer_watermarks, queue-scoped
        -- consumer_groups_metadata rows, queue_lag_metrics and queen.stats.
        -- (message_traces stay behind by design — they carry no partition FK;
        -- the trace readers in 017_traces filter unattributable traces out.)
        DELETE FROM queen.queues WHERE id = v_queue_id;
    END IF;

    RETURN jsonb_build_object(
        'deleted', true,
        'queue', p_queue_name,
        'existed', v_queue_id IS NOT NULL
    );
END;
$$;

-- Grant execute permissions
GRANT EXECUTE ON FUNCTION queen.delete_queue_v1(TEXT, UUID) TO PUBLIC;
