-- ============================================================================
-- Log engine (18-log-engine.md) — drop the legacy seg_* engine.
--
-- The log engine is a greenfield replacement of the seg_* family (023-034,
-- patch_multi_v2, 099): no data migration (dev version), so an existing dev DB
-- boots clean by simply REMOVING every legacy object before 041/042+ apply the
-- new schema. This file is applied idempotently at boot, forever, on BOTH:
--   * old DBs that still carry seg_* functions/tables and the seg-fold columns
--     on queen.partition_consumers (added by the retired 023), and
--   * fresh DBs where none of it ever existed.
-- Hence: everything is dynamic-over-catalogs or IF EXISTS — a no-op when there
-- is nothing to drop, and a full teardown when there is.
--
-- Order matters loosely: functions first (some legacy SQL-language bodies and
-- the CASCADE chains reference the tables), then tables, then the
-- partition_consumers column fold-back. plpgsql late binding means nothing
-- here can break the apply of later files.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Drop every function whose name starts with seg_ in schema queen.
--    Dynamic over pg_proc: the legacy roster changed across builds (023-034
--    plus hotfix files), so a hardcoded list would silently miss stragglers.
--    Signatures are reconstructed with pg_get_function_identity_arguments so
--    overloads (e.g. the 025 re-signature of seg_pop_segments_v1) each get
--    their own DROP. Collected into an array BEFORE executing any DROP so the
--    catalog scan never iterates over rows it is concurrently invalidating
--    (CASCADE may take out dependent functions mid-loop; IF EXISTS makes the
--    later, already-cascaded DROPs harmless no-ops).
--    prokind = 'f' only: every seg_* object is a plain function (no aggregates
--    or window functions to special-case), and DROP FUNCTION would error on
--    anything else.
-- ----------------------------------------------------------------------------
DO $$
DECLARE
    v_drops TEXT[];
    v_sql   TEXT;
BEGIN
    SELECT array_agg(
               format('DROP FUNCTION IF EXISTS queen.%I(%s) CASCADE',
                      p.proname,
                      pg_get_function_identity_arguments(p.oid)))
    INTO v_drops
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'queen'
      AND p.proname LIKE 'seg\_%'
      AND p.prokind = 'f';

    IF v_drops IS NOT NULL THEN
        FOREACH v_sql IN ARRAY v_drops LOOP
            EXECUTE v_sql;
        END LOOP;
    END IF;
END $$;

-- ----------------------------------------------------------------------------
-- 2. Drop the legacy tables. Explicit list first (the known roster: 023's
--    core tables, 025's DLQ, 028's migration bookkeeping), then a dynamic
--    pg_class sweep so ANY other seg_% table a past build may have left behind
--    (e.g. the pre-fold seg_consumers / seg_consumer_watermarks on a DB that
--    never ran the 023 fold) is caught too. CASCADE takes the tables' own
--    indexes and any straggling dependent objects with them. No data is worth
--    preserving: dev-version contract, no migration.
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS queen.seg_dedup CASCADE;
DROP TABLE IF EXISTS queen.seg_dlq CASCADE;
DROP TABLE IF EXISTS queen.seg_segments CASCADE;
DROP TABLE IF EXISTS queen.seg_partitions CASCADE;
DROP TABLE IF EXISTS queen.seg_queues CASCADE;
DROP TABLE IF EXISTS queen.seg_migration_progress CASCADE;

DO $$
DECLARE
    v_tables TEXT[];
    v_tbl    TEXT;
BEGIN
    SELECT array_agg(c.relname)
    INTO v_tables
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'queen'
      AND c.relname LIKE 'seg\_%'
      AND c.relkind IN ('r', 'p');   -- ordinary + partitioned tables

    IF v_tables IS NOT NULL THEN
        FOREACH v_tbl IN ARRAY v_tables LOOP
            EXECUTE format('DROP TABLE IF EXISTS queen.%I CASCADE', v_tbl);
        END LOOP;
    END IF;
END $$;

-- ----------------------------------------------------------------------------
-- 3. Fold back queen.partition_consumers. The retired 023 grafted the segment
--    cursor columns onto the canonical coordination table (the "v2b fold");
--    the log engine keeps its cursor on its OWN table (queen.log_consumers,
--    created in 041) keyed by a single BIGINT offset, so the seg-era pair
--    columns are dead weight — drop them. DROP COLUMN IF EXISTS is a no-op on
--    a fresh DB (schema.sql never creates them) and drops dependent
--    indexes/constraints implicitly on an old one. Wrapped in a to_regclass
--    guard so the file even survives a DB where partition_consumers itself is
--    absent (defense in depth; schema.sql always creates it first).
-- ----------------------------------------------------------------------------
DO $$
BEGIN
    IF to_regclass('queen.partition_consumers') IS NOT NULL THEN
        ALTER TABLE queen.partition_consumers
            DROP COLUMN IF EXISTS next_seq,
            DROP COLUMN IF EXISTS next_off,
            DROP COLUMN IF EXISTS batch_end_seq,
            DROP COLUMN IF EXISTS batch_end_off,
            DROP COLUMN IF EXISTS batch_positions,
            DROP COLUMN IF EXISTS attempt_seq,
            DROP COLUMN IF EXISTS attempt_off,
            DROP COLUMN IF EXISTS attempt_count,
            DROP COLUMN IF EXISTS total_consumed;
    END IF;
END $$;
