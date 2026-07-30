-- ============================================================================
-- 005_usage_prune -- bound the growth of queen_proxy.usage_minutes.
--
-- usage_minutes gains one row per (cluster, op_class, minute) and nothing ever
-- removed them, while rollup_usage_days (004) re-aggregates every closed day
-- still present in the table on each pass. Unbounded, that is both a storage
-- problem and an hourly full scan that grows without limit.
--
-- Pruning is safe by construction against the 004 design:
--   * rollup_usage_days emits rows only for days that HAVE minutes, so a pruned
--     day produces no row and its ON CONFLICT DO UPDATE never fires -- the
--     already-rolled usage_days figures are never overwritten with zeros.
--   * cluster_month_msgs takes GREATEST(rolled, live) per day precisely so a
--     pruned day reads from usage_days instead of the empty minute range.
-- Both properties are load-bearing here; do not "simplify" either into a plain
-- add or a live-only read without revisiting this file.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- prune_usage_minutes(p_keep_days) -> rows deleted
--
-- Deletes minute rows older than p_keep_days whose day has ALREADY been rolled
-- up. The rolled-up precondition is what makes this lossless: an un-rolled day
-- (rollup task down, pxdb unreachable during its window) keeps its minutes
-- until the rollup catches up, so the invoice source data can always be
-- rebuilt. Retention is deliberately generous -- minute granularity is the
-- evidence behind a billing dispute, and the rows are small.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen_proxy.prune_usage_minutes(
    p_keep_days INTEGER DEFAULT 90
) RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_cutoff DATE;
    v_rows   INTEGER;
BEGIN
    IF p_keep_days IS NULL OR p_keep_days < 1 THEN
        RAISE EXCEPTION 'prune_usage_minutes: p_keep_days must be >= 1 (got %)', p_keep_days;
    END IF;

    v_cutoff := ((now() AT TIME ZONE 'UTC')::date - p_keep_days);

    DELETE FROM queen_proxy.usage_minutes um
     WHERE um.minute < (v_cutoff::timestamp AT TIME ZONE 'UTC')
       AND EXISTS (
           SELECT 1
             FROM queen_proxy.usage_days ud
            WHERE ud.cluster_id = um.cluster_id
              AND ud.day        = (um.minute AT TIME ZONE 'UTC')::date
              AND ud.op_class   = um.op_class
       );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows;
END;
$$;
