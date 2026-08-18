-- ============================================================================
--  026_kv_sweeper.sql — the two slow phases of the sweeper (PLAN_KV_TIMERS §7.5)
--
--  queen.kv_expire_step_v1  — bounded prune of expired queen.kv rows
--  queen.kv_usage_step_v1   — the per-tenant usage rollup behind the quota gauges
--
--  WHY THESE ARE NOT IN 024_kv.sql. 024 owns the request path: every function
--  there is called by a user's request, inside the user's transaction, under the
--  lock order of §2. These two are called by nobody's request. They run on the
--  sweeper's own clock (QUEEN_KV_EXPIRE_EVERY_MS / QUEEN_KV_USAGE_EVERY_MS), on
--  a Lane::Maint slot, and they are the only two functions in the KV feature
--  that are ALLOWED TO BE SKIPPED ENTIRELY: sweeper.rs degrades each of them to
--  PhaseState::OffConfig on SQLSTATE class 42 and keeps serving. Keeping them in
--  their own file is what makes that separation legible — and it is why the
--  broker boots and answers correctly with this file absent, which is exactly
--  what it did before this file existed.
--
--  ---------------------------------------------------------------------------
--  THE FAILURE THIS FILE EXISTS TO PREVENT, stated plainly.
--
--  An expired KV row is INVISIBLE but not GONE. queen.kv_live_v1 hides it from
--  every read, so with no prune at all the product's answers stay perfectly
--  correct — and queen.kv grows forever. §3.2 sets `vacuum_truncate = off` on
--  this table (heap truncation takes ACCESS EXCLUSIVE and caused the soak's
--  wobble), so the heap NEVER gives pages back: the peak is permanent. A
--  customer using the KV as an idempotency ledger with a 24 h TTL writes a
--  bounded working set and an unbounded table.
--
--  That is a failure mode that disguises itself as health. `queen_kv_expired_not_pruned`
--  (fed from `unpruned` below) is the only signal that separates it from "all is
--  well" — §14.3 point 4 — which is why this function computes it even on the
--  passes where it deletes nothing.
--  ---------------------------------------------------------------------------
--
--  LOCK ORDER. Both functions are ACTOR 7 of §2.3: singletons in the queen.kv
--  space and nothing else. `kv_expire_step_v1` takes `FOR UPDATE SKIP LOCKED`
--  and therefore NEVER WAITS on a queen.kv row; `kv_usage_step_v1` takes no row
--  locks in queen.kv at all. Neither touches queen.log_timers, queen.queues,
--  queen.log_partitions or queen.log_consumers. A vertex that never waits cannot
--  be an edge of the wait-for graph, so neither can appear in any cycle,
--  whatever the rest of the system is doing. This property is the whole reason
--  SKIP LOCKED is written here rather than a plain FOR UPDATE that would "only
--  block for a moment": the prune runs continuously against the space that §18.2
--  accepts is held the longest.
--
--  The usage rollup DOES write queen.kv_usage. That table is a leaf: nothing
--  reads it inside another transaction and nothing locks it before locking
--  something else. §3.2 puts its PK on tenant_id (not on shard) precisely so the
--  write is last-writer-wins on `computed_at`, and the SKIP LOCKED on the shard
--  scan keeps two brokers from redoing each other's work rather than from
--  corrupting it.
--
--  IDEMPOTENT APPLY. CREATE OR REPLACE only; no DROP, because neither return
--  type ever changes (both return JSONB). Applied at every boot, in lexical
--  order, after 024 and 025 — it depends on queen.kv, queen.kv_usage and
--  queen.kv_quota existing, which is the ordering the numeric prefix buys.
-- ============================================================================


-- ============================================================================
-- kv_expire_step_v1 — delete at most p_max_rows expired rows, and report.
--
--   p_shards:   the shard set this broker sweeps (all 64 today; the parameter
--               exists so a future cell can split them across brokers).
--   p_now:      ONE instant for the whole call. Never clock_timestamp() inside
--               the predicate: with two evaluations a row can be spared by the
--               scan and counted by the tally, and the two numbers this returns
--               would disagree with each other by a race nobody can reproduce.
--   p_max_rows: the delete budget for THIS call. sweeper.rs loops on `done`.
--
--   returns: {"deleted":N, "done":bool, "unpruned":N, "unprunedCapped":bool,
--             "lagMs":N, "now":...}
--
-- WHY A LATERAL PER SHARD, and not `WHERE shard = ANY(p_shards) ORDER BY expires_at`.
-- The only secondary index on queen.kv is `idx_kv_shard_expires (shard, expires_at)
-- WHERE expires_at IS NOT NULL` — one partial index, by the budget of §3.4. A
-- ScalarArrayOpExpr on the LEADING column does not produce output ordered by the
-- second, so the planner puts a Sort on top and the "cheap bounded step" becomes
-- a sort of every expired row in the table. The LATERAL gives 64 index seeks,
-- each already ordered, and the outer LIMIT stops early. This is the same shape
-- log_timers_due_v1 uses and for the same reason.
--
-- WHY `done` IS NOT `deleted < p_max_rows`. A batch can select p_max_rows rows
-- and delete fewer, because SKIP LOCKED walks past rows another transaction is
-- writing. Reporting `done` from the DELETE count would end the loop early and
-- leave a permanent residue of exactly the hot rows. `done` is "the SELECT could
-- not fill its budget", which is the real end-of-work signal; sweeper.rs
-- independently stops on `deleted = 0` so a batch that selects a full page and
-- deletes none cannot spin.
--
-- `unpruned` is CAPPED (p_max_rows * 8, + 1 to witness the cap): it is a gauge,
-- and an exact count would be O(backlog) on precisely the incident it is meant
-- to report. `unprunedCapped` says the number is a floor, so a dashboard cannot
-- read a clamped value as a real one.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.kv_expire_step_v1(
    p_shards   SMALLINT[],
    p_now      TIMESTAMPTZ,
    p_max_rows INT
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted   BIGINT := 0;
    v_picked    BIGINT := 0;
    v_unpruned  BIGINT := 0;
    v_cap       BIGINT;
    v_oldest    TIMESTAMPTZ;
BEGIN
    IF p_shards IS NULL OR array_length(p_shards, 1) IS NULL THEN
        RETURN jsonb_build_object(
            'deleted', 0, 'done', TRUE, 'unpruned', 0,
            'unprunedCapped', FALSE, 'lagMs', 0, 'now', p_now);
    END IF;
    IF p_max_rows IS NULL OR p_max_rows <= 0 THEN
        p_max_rows := 1000;
    END IF;
    v_cap := p_max_rows::BIGINT * 8;

    -- Pick under SKIP LOCKED, then delete by primary key. The row lock is taken
    -- in the CTE and held to commit, so the DELETE cannot wait either.
    WITH picked AS (
        SELECT k.tenant_id, k.namespace, k.key
        FROM unnest(p_shards) AS s(shard)
        CROSS JOIN LATERAL (
            SELECT kk.tenant_id, kk.namespace, kk.key
            FROM queen.kv kk
            WHERE kk.shard = s.shard
              AND kk.expires_at IS NOT NULL
              AND kk.expires_at <= p_now
            ORDER BY kk.expires_at
            LIMIT p_max_rows
            FOR UPDATE SKIP LOCKED
        ) k
        LIMIT p_max_rows
    ),
    counted AS (
        SELECT count(*) AS n FROM picked
    ),
    gone AS (
        DELETE FROM queen.kv t
        USING picked p
        WHERE t.tenant_id = p.tenant_id
          AND t.namespace = p.namespace
          AND t.key       = p.key
          -- Re-checked under the lock: a row rewritten with a later expiry
          -- between the scan and here must be SPARED, or the prune would delete
          -- a live key. This is why `deleted` and `picked` are two numbers.
          AND t.expires_at IS NOT NULL
          AND t.expires_at <= p_now
        RETURNING 1
    )
    SELECT (SELECT n FROM counted), (SELECT count(*) FROM gone)
    INTO v_picked, v_deleted;

    -- The backlog gauge, and the oldest expiry still in the table: computed
    -- AFTER the delete so it describes the state the caller is leaving behind.
    SELECT count(*), min(x.expires_at)
    INTO v_unpruned, v_oldest
    FROM (
        SELECT kk.expires_at
        FROM unnest(p_shards) AS s(shard)
        CROSS JOIN LATERAL (
            SELECT k2.expires_at
            FROM queen.kv k2
            WHERE k2.shard = s.shard
              AND k2.expires_at IS NOT NULL
              AND k2.expires_at <= p_now
            ORDER BY k2.expires_at
            LIMIT v_cap + 1
        ) kk
        LIMIT v_cap + 1
    ) x;

    RETURN jsonb_build_object(
        'deleted',        v_deleted,
        -- "the scan could not fill its budget", NOT "the delete was short".
        'done',           v_picked < p_max_rows,
        'unpruned',       LEAST(v_unpruned, v_cap),
        'unprunedCapped', v_unpruned > v_cap,
        -- How far behind the prune is, in ms, from the oldest row it left.
        'lagMs',          COALESCE(
                              GREATEST(0, (EXTRACT(EPOCH FROM (p_now - v_oldest)) * 1000)::BIGINT),
                              0),
        'now',            p_now);
END;
$$;

COMMENT ON FUNCTION queen.kv_expire_step_v1(SMALLINT[], TIMESTAMPTZ, INT) IS
    'Bounded prune of expired queen.kv rows (PLAN_KV_TIMERS §7.5). SKIP LOCKED: never waits on a kv row.';


-- ============================================================================
-- kv_usage_step_v1 — per-tenant usage, for the quota gauges of §9 and §14.
--
--   returns: {"tenants":[{"tenant","kvRows","kvBytes","timerRows","timerBytes",
--                         "timerOldest","kvQuotaRatio","timerQuotaRatio"}, ...],
--             "sampled":bool, "scale":N, "now":...}
--
-- THE MEASUREMENT IS THE HARD PART, and §7.5 says both obvious forms are wrong.
--
--   * `sum(pg_column_size(k.*))` on a WHOLE-ROW var either flattens the
--     composite datum — which DETOASTS EVERY VALUE IN THE TABLE, every pass, on
--     one Lane::Maint slot — or it does not, and then it counts the 18-byte
--     external pointer instead of the value, systematically UNDER-COUNTING
--     exactly the large rows the quota exists to bound, by orders of magnitude.
--   * So it is `pg_column_size(k.value)` on the COLUMN: no detoast, and the
--     size counted is the stored (possibly compressed, possibly external) size,
--     which is what actually occupies the tenant's space.
--
-- THE SCAN IS THE OTHER HARD PART. queen.kv has ONE secondary index and it is
-- partial on `expires_at IS NOT NULL` (§3.4), so there is NO access path for
-- "every row of a tenant" and this is unavoidably a scan. Combined with
-- `vacuum_truncate = off`, a tenant who schedules a million timers for one
-- campaign leaves the heap permanently at its peak — and a naive rollup would
-- then scan that peak every five minutes forever, to count zero. The cost of
-- maintenance would become a function of the historical PEAK rather than of the
-- pending work, which is the exact shape the seg-v2 rework removed from another
-- dimension.
--
-- Correction, as decided: above QUEEN_KV_USAGE_SAMPLE_ABOVE (hardcoded here at
-- 200 000 live rows, the same class of number as the plan's other soft
-- thresholds) the scan degrades to a SHARD SAMPLE — a fraction of the 64 shards,
-- scaled up — and the result is labelled `sampled` so a gauge built on it can
-- never be read as exact. The shard is `hashtextextended(key)`, i.e. uniform and
-- independent of the tenant, so a shard subset is an unbiased sample of every
-- tenant simultaneously. Below the threshold the count is exact and `scale` is 1.
--
-- SKIP LOCKED per tenant on the WRITE, not on the read: two brokers may both
-- compute (the read is cheap relative to the coordination) but they must not
-- queue behind each other to store, and the row is last-writer-wins on
-- `computed_at` (§3.2's reason for the tenant-keyed PK).
--
-- p_max_tenants bounds the RESULT, not the scan: the JSONB goes into a metrics
-- registry whose cardinality rule (§14.1) is the reason this feature is the
-- first that could really blow it up.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.kv_usage_step_v1(
    p_shards      SMALLINT[],
    p_now         TIMESTAMPTZ,
    p_max_tenants INT
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    -- Above this many live KV rows the exact scan stops being affordable at the
    -- rollup's cadence and degrades to a labelled sample.
    c_sample_above CONSTANT BIGINT := 200000;
    c_sample_shards CONSTANT INT   := 8;
    v_est          BIGINT;
    v_sampled      BOOLEAN := FALSE;
    v_scale        NUMERIC := 1;
    v_shards       SMALLINT[];
    v_tenants      JSONB;
BEGIN
    IF p_shards IS NULL OR array_length(p_shards, 1) IS NULL THEN
        RETURN jsonb_build_object('tenants', '[]'::jsonb, 'sampled', FALSE,
                                  'scale', 1, 'now', p_now);
    END IF;
    IF p_max_tenants IS NULL OR p_max_tenants <= 0 THEN
        p_max_tenants := 10000;
    END IF;
    v_shards := p_shards;

    -- The planner's own estimate, not a count: asking count(*) whether a
    -- count(*) is affordable is the joke that writes itself.
    SELECT GREATEST(c.reltuples, 0)::BIGINT INTO v_est
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'queen' AND c.relname = 'kv';

    IF COALESCE(v_est, 0) > c_sample_above
       AND array_length(p_shards, 1) > c_sample_shards THEN
        v_sampled := TRUE;
        SELECT array_agg(s.shard ORDER BY s.shard)
        INTO v_shards
        FROM (SELECT unnest(p_shards) AS shard ORDER BY 1 LIMIT c_sample_shards) s;
        v_scale := array_length(p_shards, 1)::NUMERIC
                   / GREATEST(array_length(v_shards, 1), 1)::NUMERIC;
    END IF;

    WITH kv_agg AS (
        SELECT k.tenant_id,
               count(*)                             AS rows_n,
               COALESCE(sum(pg_column_size(k.value)), 0) AS bytes_n
        FROM queen.kv k
        WHERE k.shard = ANY(v_shards)
          AND queen.kv_live_v1(k.expires_at, p_now)
        GROUP BY k.tenant_id
    ),
    -- Timers are counted on the SAME sampled shard set, so the two numbers in a
    -- row are always on the same footing and `scale` applies to both.
    tm_agg AS (
        SELECT t.tenant_id,
               count(*)                              AS rows_n,
               COALESCE(sum(octet_length(t.payload)), 0) AS bytes_n,
               min(t.deliver_at)                     AS oldest
        FROM queen.log_timers t
        WHERE t.shard = ANY(v_shards)
        GROUP BY t.tenant_id
    ),
    merged AS (
        SELECT COALESCE(k.tenant_id, t.tenant_id)                       AS tenant_id,
               (COALESCE(k.rows_n, 0)  * v_scale)::BIGINT               AS kv_rows,
               (COALESCE(k.bytes_n, 0) * v_scale)::BIGINT               AS kv_bytes,
               (COALESCE(t.rows_n, 0)  * v_scale)::BIGINT               AS timer_rows,
               (COALESCE(t.bytes_n, 0) * v_scale)::BIGINT               AS timer_bytes,
               t.oldest                                                 AS timer_oldest
        FROM kv_agg k
        FULL OUTER JOIN tm_agg t ON t.tenant_id = k.tenant_id
    ),
    stored AS (
        INSERT INTO queen.kv_usage AS u
              (tenant_id, computed_at, kv_rows, kv_bytes, timer_rows, timer_bytes, timer_oldest)
        SELECT m.tenant_id, p_now, m.kv_rows, m.kv_bytes, m.timer_rows, m.timer_bytes, m.timer_oldest
        FROM merged m
        ORDER BY m.tenant_id          -- ON CONFLICT takes speculative locks in ROW order
        ON CONFLICT (tenant_id) DO UPDATE SET
               computed_at  = EXCLUDED.computed_at,
               kv_rows      = EXCLUDED.kv_rows,
               kv_bytes     = EXCLUDED.kv_bytes,
               timer_rows   = EXCLUDED.timer_rows,
               timer_bytes  = EXCLUDED.timer_bytes,
               timer_oldest = EXCLUDED.timer_oldest
        -- Last-writer-wins, but never BACKWARDS: a slow broker committing a
        -- stale pass after a fresh one must not un-report a spike.
        WHERE u.computed_at <= EXCLUDED.computed_at
        RETURNING 1
    ),
    ranked AS (
        SELECT m.*,
               q.max_rows, q.max_bytes, q.max_timers
        FROM merged m
        LEFT JOIN queen.kv_quota q ON q.tenant_id = m.tenant_id AND q.enabled
        ORDER BY (m.kv_rows + m.timer_rows) DESC
        LIMIT p_max_tenants
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'tenant',      r.tenant_id::text,
               'kvRows',      r.kv_rows,
               'kvBytes',     r.kv_bytes,
               'timerRows',   r.timer_rows,
               'timerBytes',  r.timer_bytes,
               'timerOldest', r.timer_oldest,
               -- NULL quota = unlimited (the dedicated tier), and unlimited has
               -- no ratio: 0, never a division by zero and never a fake 1.0.
               'kvQuotaRatio',
                   CASE WHEN COALESCE(r.max_rows, 0) > 0
                        THEN round(r.kv_rows::NUMERIC / r.max_rows, 4)
                        WHEN COALESCE(r.max_bytes, 0) > 0
                        THEN round(r.kv_bytes::NUMERIC / r.max_bytes, 4)
                        ELSE 0 END,
               'timerQuotaRatio',
                   CASE WHEN COALESCE(r.max_timers, 0) > 0
                        THEN round(r.timer_rows::NUMERIC / r.max_timers, 4)
                        ELSE 0 END)
           -- The aggregate carries its own ORDER BY: a plain jsonb_agg over a
           -- subquery that "already sorted" is not ordered by the standard, and
           -- the top-N is the whole point of p_max_tenants.
           ORDER BY (r.kv_rows + r.timer_rows) DESC), '[]'::jsonb)
    INTO v_tenants
    FROM ranked r
    -- Force the CTE to run: a data-modifying CTE nobody references is still
    -- executed, but leaving that to the reader's memory is how it gets deleted.
    WHERE (SELECT count(*) FROM stored) >= 0;

    RETURN jsonb_build_object(
        'tenants', v_tenants,
        'sampled', v_sampled,
        'scale',   v_scale,
        'now',     p_now);
END;
$$;

COMMENT ON FUNCTION queen.kv_usage_step_v1(SMALLINT[], TIMESTAMPTZ, INT) IS
    'Per-tenant KV/timer usage rollup for the quota gauges (PLAN_KV_TIMERS §7.5). Samples shards above 200k rows.';


-- The full argument list, spelled out. A GRANT whose type list does not match
-- an existing function does not warn: it FAILS the statement, the apply dies,
-- and the boot is fail-fast — so this is the line that takes the broker down if
-- either signature above is edited without editing here.
GRANT EXECUTE ON FUNCTION queen.kv_expire_step_v1(SMALLINT[], TIMESTAMPTZ, INT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.kv_usage_step_v1(SMALLINT[], TIMESTAMPTZ, INT) TO PUBLIC;
