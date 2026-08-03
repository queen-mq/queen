-- pgmq setup for CM-BENCH.
--
-- The adapter's preflight refuses to run if the grouped-read function is
-- missing, so a build without per-group FIFO fails loudly at startup instead of
-- silently producing an unordered benchmark.
CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Surface what we actually got, so the run log records the pgmq build that
-- produced the numbers.
DO $$
DECLARE
  v text;
  has_rr boolean;
  has_head boolean;
  has_fifo_idx boolean;
BEGIN
  SELECT extversion INTO v FROM pg_extension WHERE extname = 'pgmq';
  SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
                 WHERE n.nspname='pgmq' AND p.proname='read_grouped_rr')   INTO has_rr;
  SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
                 WHERE n.nspname='pgmq' AND p.proname='read_grouped_head') INTO has_head;
  SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
                 WHERE n.nspname='pgmq' AND p.proname='create_fifo_index') INTO has_fifo_idx;

  RAISE NOTICE 'pgmq version=% read_grouped_rr=% read_grouped_head=% create_fifo_index=%',
    v, has_rr, has_head, has_fifo_idx;

  IF NOT has_rr AND NOT has_head THEN
    RAISE WARNING 'This pgmq build has NO grouped read: it cannot express per-property ordering. Record the run as "cannot express the workload" (SPEC.md 6.1).';
  END IF;
END $$;
