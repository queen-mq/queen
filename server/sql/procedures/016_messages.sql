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
-- (db::delete_message), plus the two dead-letter operations that grew here
-- afterwards because they address the same rows: purge_dlq_v1 (bulk delete by
-- queue criteria) and log_dlq_move_v1 (the replay/redrive primitive — claim,
-- push, delete, one transaction).
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
-- queen.log_dlq_move_v1: move ONE dead-letter row back into the log
-- ============================================================================
-- The primitive behind BOTH replay routes (POST /api/v1/dlq/:id/replay and the
-- older POST /api/v1/messages/:pid/:txn/retry, which resolves its address to a
-- row id and then calls this). Nothing here is DLQ-specific beyond the source
-- table: the source row is addressed BY ID and the destination is named
-- explicitly, which is what makes the same body the "move" a redrive and the
-- routing rules need.
--
-- The shape is log_timers_fire_v1's (025_log_timers), minus the parts a
-- single-frame move does not have: claim the source row under a lock, push the
-- already-packed segment through the ONE allocator (queen.log_push_one_v1),
-- delete the source row — all in ONE transaction. The broker packed the frame
-- (pack_frames + zstd + xxh3) OUTSIDE this transaction and passes the two blobs;
-- SQL never hashes and never looks inside a blob (§3).
--
-- WHY THE FOUR DEFECTS OF THE OLD REPLAY CANNOT RECUR. The route this replaces
-- read the snapshot, called the push handler, and then deleted the row in a
-- second, unrelated statement:
--   1. it minted a FRESH transaction id per attempt, so the dedup window could
--      not recognise a second replay and every double click appended another
--      copy. Here the transaction id is deterministic — the broker spells it
--      `dlq:<log_dlq.id>` — so a second push of the same row is a `duplicate`
--      verdict rather than a second message (and the row is gone anyway);
--   2. it read the row without FOR UPDATE, so two concurrent callers both
--      pushed. Here they serialise on the row lock and the loser finds nothing:
--      `gone`;
--   3. it could report "pushed, but the cleanup failed" — a state whose only
--      cure was a replay that duplicated the message. One transaction has no
--      such state: either the frame is in the log and the row is gone, or
--      neither happened;
--   4. it addressed (partition_id, transaction_id), which can carry ONE ROW PER
--      CONSUMER GROUP, and deleted every one of them while replaying a single
--      snapshot. Here the address is a row id, so moving one group's record
--      leaves the other groups' records exactly where they were.
--
-- LOCK ORDER: D (queen.log_dlq, one row, by primary key) then Q/P (the
-- destination queue + partition rows log_push_one_v1 resolves and locks). No
-- other path takes those in the opposite order — log_dlq_head_v1 INSERTs its
-- row while holding a log_consumers lock and never locks a log_dlq row, and
-- purge_dlq_v1 is a plain DELETE — so this prefix cannot close a cycle.
--
-- p_verified = -1, DELIBERATELY, and it is the one number here worth a
-- paragraph. The timer fire hands log_push_one_v1 the destination partition's
-- own last_offset so the dedup probe's span is empty (025's header explains what
-- that probe costs inside the push serializer). The fire can only do that
-- because it pre-locks every destination partition for deadlock safety and reads
-- last_offset under that lock. A move has neither: its destination may not exist
-- yet and is resolved — and PROVISIONED — inside log_push_one_v1 itself, so
-- there is no earlier lock under which a watermark could be read. Vouching a
-- last_offset read without the lock would be a lie of exactly the kind the
-- vouching protocol forbids (the broker promises it has compared the incoming
-- hashes against everything at or below it). So the move passes -1, "no cache,
-- probe the window", which is always correct; the cost is one window probe for
-- ONE frame on an operator-driven call, not per message on a sweep.
--
-- p_window is NULL for the same single-allocator reason: log_push_one_v1
-- resolves the DESTINATION queue's own dedup_window_seconds. A move into a queue
-- with dedup off therefore runs no probe at all, and a move into a different
-- queue obeys that queue's policy rather than the source's.
--
-- CAVEATS THAT STAY, because they are properties of appending to a log and not
-- bugs to be fixed here: the frame lands at the TAIL of the destination
-- partition (the offset the original occupied stays committed — a move is a
-- re-push, not a revival), and created_at is stamped at the destination, so the
-- replayed message's lag clock starts at zero.
--
-- Returns {result, queue, partition, offset, messageId, transactionId,
-- consumerGroup}, or {result:'gone'} when the row is not there under this
-- tenant. `messageId` / `transactionId` / `consumerGroup` are the SOURCE row's
-- own snapshot identity — what was moved — because the destination identity is
-- inside the blob, which SQL cannot read; the broker minted it and echoes it
-- itself. `offset` is where the message now is: the allocated offset for
-- `moved`, and the PRE-EXISTING occurrence's offset for `duplicate`.
--
-- ONLY `moved` DELETES THE SOURCE ROW. `duplicate` means the push wrote
-- nothing, and the dedup identity it matched on is the txn hash alone — which
-- is derivable from the row id this function is addressed by — so it is not
-- proof that the frame already in the window IS this row's snapshot. Deleting
-- on that would turn a producer credential into a way to destroy a
-- dead-letter record without replaying it; the branch below carries the
-- reasoning in full.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.log_dlq_move_v1(
    p_tenant    UUID,
    p_dlq_id    UUID,
    p_queue     TEXT,
    p_partition TEXT,
    p_hashes    BYTEA,
    p_blob      BYTEA
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_row    RECORD;
    v_push   JSONB;
    v_result TEXT;
    v_off    BIGINT;
BEGIN
    -- Alignment guard, 003_log_push's (and 025's) discipline for exactly one
    -- frame: the hash blob is the dedup identity AND what ack-by-hash resolves
    -- through, so a stride mismatch must fail loudly instead of mis-addressing
    -- silently. A move carries one frame by construction — there is no
    -- p_msg_count to disagree with.
    IF octet_length(COALESCE(p_hashes, ''::bytea)) <> 16 THEN
        RAISE EXCEPTION 'QMOVE bad segment: hashes=% bytes (want 16 for one frame)',
            octet_length(COALESCE(p_hashes, ''::bytea));
    END IF;
    -- An empty destination name would PROVISION a queue named '' (the
    -- provisioning branch of log_push_one_v1 inserts what it is given), which is
    -- an unreachable queue nobody can pop. Refuse it here, where the caller
    -- still learns why.
    IF COALESCE(p_queue, '') = '' OR COALESCE(p_partition, '') = '' THEN
        RAISE EXCEPTION 'QMOVE unnamed destination %/%', p_queue, p_partition;
    END IF;

    -- The claim. FOR UPDATE OF d locks the dead-letter row and NOTHING else:
    -- the two joins are the tenant boundary (a queue name is not globally
    -- unique), and locking queen.queues rows here would convoy every mover of
    -- the same queue behind each other for no gain.
    SELECT d.message_id, d.transaction_id, d.consumer_group
    INTO v_row
    FROM queen.log_dlq d
    JOIN queen.log_partitions p ON p.id = d.partition_id
    JOIN queen.queues q ON q.id = p.queue_id
    WHERE d.id = p_dlq_id AND q.tenant_id = p_tenant
    FOR UPDATE OF d;

    IF NOT FOUND THEN
        -- Already replayed, purged, or another tenant's row: one verdict for all
        -- three. A distinct answer for "exists, but not yours" would confirm the
        -- row's existence across the tenant boundary, which is the same reason
        -- the pid-addressed routes answer 404 rather than 403.
        RETURN jsonb_build_object('result', 'gone');
    END IF;

    -- The push. p_pid/p_window NULL so the single allocator resolves — and, when
    -- the destination is new, provisions — queue and partition under p_tenant,
    -- exactly as a first-contact producer push does.
    v_push := queen.log_push_one_v1(
        p_queue,
        p_partition,
        1,
        p_hashes,
        -1,            -- p_verified: no broker cache, probe the window (see header)
        p_blob,
        NULL,          -- p_pid: resolve + provision inside the allocator
        NULL,          -- p_window: the DESTINATION queue's dedup window
        p_tenant);

    IF v_push->>'status' = 'duplicate' THEN
        -- Nothing was written: the destination's dedup window already holds a
        -- frame under this transaction id, and `dups` carries one entry (one
        -- frame) whose `off` says where that frame is.
        --
        -- AND THE SOURCE ROW STAYS. The dedup identity is the txn hash and
        -- NOTHING else (util.rs `txn_hash128`), so this branch does not prove
        -- the frame at `off` is the snapshot this row holds — only that
        -- something in the window carries the same id. The id is derivable by
        -- anyone who can read the DLQ listing (`dlq:<log_dlq.id>`), so deleting
        -- here would let a producer credential destroy a dead-letter record by
        -- pushing a decoy under that id: the record goes, nothing of it is
        -- written, and the consumer group's cursor is already past the
        -- original. A move that moved nothing removes nothing; the caller is
        -- told the offset and decides what to do with what is there.
        --
        -- The normal path pays nothing for this: the row is deleted in the same
        -- transaction as the push, so a genuine second move of the same row
        -- answers `gone` above and never reaches here.
        v_result := 'duplicate';
        v_off := (v_push->'dups'->0->>'off')::bigint;
    ELSE
        v_result := 'moved';
        v_off := (v_push->>'baseOffset')::bigint;
        -- The row has been held since the claim, so this acquires nothing new.
        -- It is also why there is no "moved but still dead-lettered" state to
        -- report: the delete and the push commit together or not at all.
        DELETE FROM queen.log_dlq WHERE id = p_dlq_id;
    END IF;

    RETURN jsonb_build_object(
        'result', v_result,
        'queue', p_queue,
        'partition', p_partition,
        'offset', v_off,
        'messageId', v_row.message_id,
        'transactionId', v_row.transaction_id,
        'consumerGroup', v_row.consumer_group);
END;
$$;

GRANT EXECUTE ON FUNCTION queen.log_dlq_move_v1(UUID, UUID, TEXT, TEXT, BYTEA, BYTEA) TO PUBLIC;

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
