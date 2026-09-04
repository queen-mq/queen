-- ============================================================================
-- Messages Stored Procedures
-- ============================================================================
-- Async stored procedures for message operations.
--
-- 2026-07-30, log-engine-only cleanup — three of the four functions that used
-- to live here are gone:
--   * get_message_v1 (single-message detail): DELETED. A call-graph closure
--     over server/src + the surviving SQL found zero callers, and its whole
--     body was a rows-engine read.
--   * list_messages_v1 and get_dlq_messages_v1: DELETED from THIS file.
--     010_log_admin.sql owns both definitions, so the copies here were
--     shadowed at every boot and never ran. The live definitions are the
--     ones in 010_log_admin.
-- What is left is delete_message_v1, which the broker still calls
-- (db::delete_message).
-- ============================================================================

-- ============================================================================
-- queen.delete_message_v1: Delete a message
-- ============================================================================
-- Log engine: live payloads live inside immutable segment blobs and are not
-- individually deletable, so the only per-message row a delete can remove is
-- the dead-letter snapshot in queen.log_dlq (the DLQ manual-requeue workflow
-- drops one). Addressed by (partition_id, transaction_id) exactly as
-- 005_log_ack/010_log_admin address log_dlq elsewhere; every consumer group's
-- snapshot for that
-- address goes, matching the broker's own raw DELETE.
-- The former rows-engine leg went with the rest of the rows message plane.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.delete_message_v1(p_partition_id UUID, p_transaction_id TEXT)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted BOOLEAN;
BEGIN
    WITH deleted AS (
        DELETE FROM queen.log_dlq
        WHERE partition_id = p_partition_id AND transaction_id = p_transaction_id
        RETURNING id
    )
    SELECT EXISTS (SELECT 1 FROM deleted) INTO v_deleted;

    RETURN jsonb_build_object(
        'success', v_deleted,
        'partitionId', p_partition_id,
        'transactionId', p_transaction_id,
        'message', CASE WHEN v_deleted THEN 'Message deleted successfully' ELSE 'Message not found' END
    );
END;
$$;

-- Grant execute permissions
-- (list_messages_v1 / get_dlq_messages_v1 are granted by 010_log_admin, which
--  owns the live definitions; get_message_v1 no longer exists.)
GRANT EXECUTE ON FUNCTION queen.delete_message_v1(UUID, TEXT) TO PUBLIC;

-- ============================================================================
-- queen.purge_dlq_v1: Delete dead-letter snapshots by queue criteria
-- ============================================================================
-- A queue is mandatory by signature. Tenant is part of the join predicate so
-- the same queue name in another tenant is never touched. Consumer group is an
-- optional additional exact-match criterion.
CREATE OR REPLACE FUNCTION queen.purge_dlq_v1(
    p_tenant_id UUID,
    p_queue TEXT,
    p_consumer_group TEXT DEFAULT NULL
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted BIGINT;
BEGIN
    WITH deleted AS (
        DELETE FROM queen.log_dlq d
        USING queen.log_partitions p, queen.queues q
        WHERE d.partition_id = p.id
          AND p.queue_id = q.id
          AND q.tenant_id = p_tenant_id
          AND q.name = p_queue
          AND (p_consumer_group IS NULL OR d.consumer_group = p_consumer_group)
        RETURNING d.id
    )
    SELECT count(*) INTO v_deleted FROM deleted;

    RETURN v_deleted;
END;
$$;

GRANT EXECUTE ON FUNCTION queen.purge_dlq_v1(UUID, TEXT, TEXT) TO PUBLIC;
