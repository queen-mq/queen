-- ============================================================================
-- Attach the partition-lifecycle counters to queen.log_partitions.
--
-- The trigger FUNCTIONS live in 014_worker_metrics.sql with the rest of the
-- metrics machinery. Only the attachment is here, and it is here for two
-- reasons that are easy to get wrong:
--
-- 1. APPLY ORDER. 014 is applied before 041_log_schema.sql creates
--    queen.log_partitions. An attachment sitting in 014 and guarded on the table
--    existing is therefore skipped entirely on the FIRST boot of a fresh
--    database, and only lands on the second — so a new install silently reports
--    zero partitions created for one process lifetime. This file runs after 041,
--    so the table is always there.
--
-- 2. LOCKS. DROP TRIGGER takes ACCESS EXCLUSIVE and CREATE TRIGGER takes SHARE
--    ROW EXCLUSIVE, both of which conflict with the ROW EXCLUSIVE / ROW SHARE
--    locks every push and pop hold on queen.log_partitions. Re-running the
--    DROP/CREATE pair on every boot would make each replica restart wait for
--    in-flight pushes to drain while queueing every new one behind it — a full
--    DML stall on the engine's hottest table, on every rolling restart, with no
--    lock_timeout set by the applier. So the attachment is CONDITIONAL: it takes
--    a lock only when the trigger is genuinely absent (first boot, or after a
--    manual DROP). A steady-state boot reads pg_trigger and does nothing.
--
--    The trade-off this accepts: changing a trigger's DEFINITION (its timing,
--    its transition tables, which function it calls) no longer takes effect by
--    restarting. Redefining the FUNCTION body still does, because CREATE OR
--    REPLACE FUNCTION in 014 rebinds it with no lock on the table. If you ever
--    change the trigger definition itself, drop the trigger by hand once and let
--    the next boot re-attach it.
--
-- What these counters mean under the log engine: partitions_created counts real
-- partitions allocated by push (042's provisioning INSERT), and
-- partitions_deleted counts the retention cleanup reclaiming empty ones
-- (queen.log_partition_cleanup_step_v1, 045). Before 2026-07-30 both were
-- attached to the rows engine's queen.partitions, where the only writer was
-- /configure — so the dashboard's "Partitions Created" series counted configure
-- calls and never saw a single real partition.
-- ============================================================================

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'queen'
          AND c.relname = 'log_partitions'
          AND t.tgname  = 'trg_partitions_created_counter'
          AND NOT t.tgisinternal
    ) THEN
        CREATE TRIGGER trg_partitions_created_counter
            AFTER INSERT ON queen.log_partitions
            REFERENCING NEW TABLE AS new_partitions
            FOR EACH STATEMENT
            EXECUTE FUNCTION queen.bump_partitions_created_trigger();
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'queen'
          AND c.relname = 'log_partitions'
          AND t.tgname  = 'trg_partitions_deleted_counter'
          AND NOT t.tgisinternal
    ) THEN
        CREATE TRIGGER trg_partitions_deleted_counter
            AFTER DELETE ON queen.log_partitions
            REFERENCING OLD TABLE AS old_partitions
            FOR EACH STATEMENT
            EXECUTE FUNCTION queen.bump_partitions_deleted_trigger();
    END IF;
END $$;
