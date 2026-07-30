-- ============================================================================
-- Drop the rows engine's MESSAGE PLANE (2026-07-30).
--
-- The log engine (040-048) replaced push/pop/ack/transaction/renew_lease/
-- has_pending wholesale, but the files that defined the rows versions kept being
-- applied at every boot: 001_push, 002/002b/002c/002d_pop_unified (four
-- generations of pop), 003_ack, 004_transaction, 005_renew_lease,
-- 006_has_pending — plus 016_partition_lookup (whose two functions were only
-- ever called from the rows push) and 021_streams_cycle_v1 (superseded by
-- queen.log_streams_cycle_v1, which has a DIFFERENT name, so it was never even
-- overwritten). A call-graph closure over server/src + the surviving SQL found
-- ZERO callers for every function below; the only references left in the tree
-- are comments.
--
-- Those files are deleted, which stops re-creating the functions on fresh
-- databases. This file is what removes them from databases that already applied
-- them — the same job 040 does for the seg_* family, and the same shape:
-- dynamic over pg_proc, so overloads each get their own DROP and a database that
-- never had them is a no-op.
--
-- What is deliberately NOT dropped:
--   * The rows TABLES (queen.messages, queen.partitions, queen.partition_lookup,
--     queen.dead_letter_queue, queen.partition_consumers). Management readers
--     still consult them — the DLQ listing unions the old dead-letter table with
--     the log engine's own — and reference/compatibility documents them as
--     surviving with their contents intact. Dropping data is not a cleanup.
--   * The rows MANAGEMENT plane (007-015, 017-020, 022): configure, consumer
--     groups, stats readers, worker metrics, traces, streams register/state. It
--     was never rows-specific — it reads queen.queues / queen.stats /
--     queen.worker_metrics, which both engines share.
--   * The dead stats-aggregation functions inside 013_stats.sql
--     (compute_partition_stats_v1/v2/v3, aggregate_queue_stats_v1/v2,
--     cleanup_orphaned_stats_v1/v2, refresh_all_stats_v1, is_stats_leader,
--     increment_message_counts_v1). They became unreachable when
--     POST /api/v1/stats/refresh stopped calling the rows reconciler, but they
--     live in a file whose OTHER functions are load-bearing, so removing them is
--     surgery inside a live file rather than a file deletion. Left for a
--     follow-up so this change stays mechanical.
--
-- NO CASCADE, unlike 040. There is nothing left that legitimately depends on
-- these, so a dependency error here is information: it means the closure was
-- wrong and something still points at the rows message plane. Fail loudly at
-- boot rather than quietly amputating the dependent object.
-- ============================================================================

DO $$
DECLARE
    v_drops TEXT[];
    v_sql   TEXT;
BEGIN
    SELECT array_agg(
               format('DROP FUNCTION IF EXISTS queen.%I(%s)',
                      p.proname,
                      pg_get_function_identity_arguments(p.oid)))
    INTO v_drops
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'queen'
      AND p.prokind = 'f'
      AND p.proname = ANY (ARRAY[
          -- 001_push.sql
          'push_messages_v2', 'push_messages_v3', 'lock_push_partition',
          -- 002*_pop_unified*.sql (four generations)
          'pop_unified_batch', 'pop_unified_batch_v2', 'pop_unified_batch_v3',
          'pop_unified_batch_v4', 'pop_specific_batch', 'pop_discover_batch',
          -- 003_ack.sql / 004_transaction.sql / 005_renew_lease.sql / 006_has_pending.sql
          'ack_messages_v2', 'execute_transaction_v2', 'renew_lease_v2',
          'has_pending_messages',
          -- 016_partition_lookup.sql
          'update_partition_lookup_v1', 'reconcile_partition_lookup_v1',
          -- 021_streams_cycle_v1.sql (queen.streams_cycle_v1; the live one is
          -- queen.log_streams_cycle_v1 in 046, a different name)
          'streams_cycle_v1'
      ]);

    IF v_drops IS NOT NULL THEN
        FOREACH v_sql IN ARRAY v_drops LOOP
            EXECUTE v_sql;
        END LOOP;
    END IF;
END $$;
