-- ============================================================================
-- Drop the rows engine's TABLES (2026-07-30) — the last of the old storage.
--
-- 049 removed the rows message-plane FUNCTIONS. This removes the tables they
-- used, and with them the last reason for any procedure in this tree to know
-- the rows engine ever existed:
--
--   queen.messages             one row per message
--   queen.partitions           rows-engine partitions (log partitions live in
--                              queen.log_partitions, an independent UUID space)
--   queen.partition_consumers  rows-engine cursor / lease / retry state
--   queen.dead_letter_queue    rows-engine DLQ (the log engine uses log_dlq)
--   queen.messages_consumed    rows-engine per-message consumption records
--   queen.partition_lookup     rows-engine last-message cache
--
-- WHY THIS IS NOT A DATA DELETION IN PRACTICE. This broker has no importer and
-- no in-place upgrade (reference/compatibility): a deployment starts from a
-- clean database, so these tables are empty by construction. The one exception
-- was queen.partitions, which queen.configure_queue_v1 populated with a phantom
-- 'Default' row per configured queue — a row the engine never used, and whose
-- only effect was to corrupt three reported numbers (the namespace/task
-- partition counts and the partitions-created lifecycle counter). That INSERT
-- is gone in the same change.
--
-- If you are pointing this build at a database that DID carry rows-engine data,
-- take a dump first. This is the point of no return for it.
--
-- ORDER. Constraints on SURVIVING tables that pointed at these are dropped
-- first, so the DROP TABLE below does not need CASCADE to take them out and
-- cannot silently remove something we did not enumerate. queen.stats,
-- queen.retention_history and queen.message_traces keep their partition_id /
-- message_id COLUMNS (they now carry log-engine ids); only the constraints go.
--
-- CASCADE is still used on the DROP TABLE itself, deliberately: these six
-- reference each other (messages -> partitions, dead_letter_queue -> messages,
-- partition_lookup -> partitions, ...), so a plain DROP would depend on
-- enumerating them in a valid topological order. CASCADE within a set we are
-- dropping in full is exact, not lazy.
--
-- Idempotent: IF EXISTS throughout, a no-op on a database that never had them.
-- ============================================================================

-- 1. Constraints on tables that SURVIVE. Named explicitly where PostgreSQL's
--    default naming applies; the catalog sweep after it catches any that were
--    created under a different name by an older schema.
ALTER TABLE IF EXISTS queen.stats             DROP CONSTRAINT IF EXISTS stats_partition_id_fkey;
ALTER TABLE IF EXISTS queen.retention_history DROP CONSTRAINT IF EXISTS retention_history_partition_id_fkey;
ALTER TABLE IF EXISTS queen.message_traces    DROP CONSTRAINT IF EXISTS message_traces_partition_id_fkey;
ALTER TABLE IF EXISTS queen.message_traces    DROP CONSTRAINT IF EXISTS message_traces_message_id_fkey;

DO $$
DECLARE
    v_drops TEXT[];
    v_sql   TEXT;
BEGIN
    -- Any remaining FK from a surviving relation INTO one of the six. Dynamic
    -- over pg_constraint so a differently-named constraint from an older schema
    -- version cannot block the drop.
    SELECT array_agg(
               format('ALTER TABLE %I.%I DROP CONSTRAINT IF EXISTS %I',
                      n.nspname, c.relname, con.conname))
    INTO v_drops
    FROM pg_constraint con
    JOIN pg_class     c ON c.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_class     f ON f.oid = con.confrelid
    JOIN pg_namespace fn ON fn.oid = f.relnamespace
    WHERE con.contype = 'f'
      AND fn.nspname = 'queen'
      AND f.relname = ANY (ARRAY['messages', 'partitions', 'partition_consumers',
                                 'dead_letter_queue', 'messages_consumed',
                                 'partition_lookup'])
      AND NOT (n.nspname = 'queen'
               AND c.relname = ANY (ARRAY['messages', 'partitions', 'partition_consumers',
                                          'dead_letter_queue', 'messages_consumed',
                                          'partition_lookup']));

    IF v_drops IS NOT NULL THEN
        FOREACH v_sql IN ARRAY v_drops LOOP
            EXECUTE v_sql;
        END LOOP;
    END IF;
END $$;

-- 2. The rows-engine trigger function, and any trigger still attached to one of
--    the six (the partition-lookup trigger on queen.messages, the partition
--    counters on queen.partitions — the latter are re-attached to
--    queen.log_partitions by 014).
DROP TRIGGER  IF EXISTS trg_update_partition_lookup   ON queen.messages;
DROP TRIGGER  IF EXISTS trg_partitions_created_counter ON queen.partitions;
DROP TRIGGER  IF EXISTS trg_partitions_deleted_counter ON queen.partitions;
DROP FUNCTION IF EXISTS queen.update_partition_lookup_trigger();

-- 3. The tables.
DROP TABLE IF EXISTS queen.partition_lookup    CASCADE;
DROP TABLE IF EXISTS queen.messages_consumed   CASCADE;
DROP TABLE IF EXISTS queen.dead_letter_queue   CASCADE;
DROP TABLE IF EXISTS queen.partition_consumers CASCADE;
DROP TABLE IF EXISTS queen.messages            CASCADE;
DROP TABLE IF EXISTS queen.partitions          CASCADE;
