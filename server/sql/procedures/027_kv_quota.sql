-- ============================================================================
--  027_kv_quota.sql — the broker's view of quota and measurement
--                     (PLAN_KV_TIMERS §9.3, §9.4, §12.1)
--
--  queen.kv_quota_refresh_v1  — one read, every QUEEN_KV_QUOTA_REFRESH_MS, that
--                               feeds the in-process enforcer of src/quota.rs
--
--  WHY A FILE OF ITS OWN, and not a function in 024 or 026.
--
--  024 owns the REQUEST path: everything in it runs inside a user's transaction,
--  under the lock order of §2. 026 owns the SWEEPER's two slow phases, which run
--  on the maintenance lane and WRITE. This function is neither: it is a
--  read-only poll that every broker runs whether or not it sweeps — including a
--  broker with QUEEN_SWEEPER=false, which still has to enforce quotas — and the
--  numbering keeps that third actor legible. The apply order is lexical, so this
--  file lands after the tables it reads exist.
--
--  ---------------------------------------------------------------------------
--  WHAT THIS FUNCTION IS FOR, in one paragraph, because the shape only makes
--  sense with the decision behind it.
--
--  §9.3: an exact quota would need a per-tenant counter that every write locks,
--  in the OUTERMOST lock space, held for the whole bundle — the exact
--  anti-pattern §2.4 D7 forbids. So the enforcer is the broker's own in-process
--  delta, and this read supplies the two things a delta cannot know: what the
--  tenant is ALLOWED (queen.kv_quota, written by an operator or the control
--  plane) and what the tenant ACTUALLY holds (queen.kv_usage, written by the
--  rollup). The measurement is what RELEASES a block; the delta is what applies
--  one. Fast block, slow release.
--
--  ---------------------------------------------------------------------------
--  THE FOUR THINGS THIS FUNCTION MUST NOT DO, each of which the obvious version
--  does.
--
--   1. IT MUST NOT COUNT ANYTHING. No count(*) over queen.kv, no sum over
--      queen.log_timers. This runs every 30 s on every broker; the rollup runs
--      every 300 s on one. A count here would make the cost of enforcing the
--      quota a function of the data it is bounding, which is how a limit becomes
--      the incident. It reads queen.kv_usage and nothing else.
--
--   2. IT MUST NOT DROP A TENANT THAT HAS A QUOTA ROW BUT NO USAGE ROW. With
--      QUEEN_TENANCY_HEADER on, the ABSENCE of a quota row is a DENIAL (§9.4
--      point 1) — so a tenant the operator has granted, and which has not
--      written a byte yet, MUST appear here or its first write is refused with
--      `feature_gated`. Hence the FULL OUTER JOIN, and hence usage columns that
--      COALESCE to zero rather than to NULL.
--
--   3. IT MUST NOT INVENT A ROW. The rollup creates a queen.kv_usage row for
--      every tenant it sees; a tenant with neither quota nor usage is one nobody
--      has ever configured and which holds nothing, and it belongs in neither
--      table (§9.4 point 3). This function therefore reads two tables and writes
--      none — it is the only function in the feature with no side effects at
--      all, which is also why it needs no lock-order argument.
--
--   4. IT MUST NOT BE UNBOUNDED. p_max_tenants caps the result because it lands
--      in a per-tenant map in RAM whose cap DENIES rather than evicts (§9.4
--      point 2), and because §14.1 makes this feature the first that could
--      really blow up the metric cardinality. The ORDER BY decides WHICH tenants
--      survive the cap, and it is deliberately "closest to a limit first":
--      dropping the tenant that is at 99% in favour of one at 2% would silently
--      turn the enforcer off for exactly the tenant it exists for. Tenants with
--      a quota row always sort ahead of tenants without one, because a missing
--      row is a denial and a denial needs no data to be applied.
--
--  NULL = unlimited, the same convention as the proxy's plan columns, and it is
--  only reachable when the tenancy header is off — i.e. when the operator IS the
--  customer (§9.4 point 1).
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.kv_quota_refresh_v1(
    p_max_tenants INT
) RETURNS JSONB
LANGUAGE sql
STABLE
AS $$
    WITH merged AS (
        SELECT COALESCE(q.tenant_id, u.tenant_id)      AS tenant_id,
               -- A tenant with usage but no quota row: no limits at all, which
               -- the broker reads as "unlimited, unless a grant is required".
               (q.tenant_id IS NOT NULL)               AS granted,
               COALESCE(q.enabled, TRUE)               AS enabled,
               q.max_rows, q.max_bytes, q.max_timers,
               q.max_timer_horizon_s,
               q.max_reads_per_sec, q.max_writes_per_sec,
               COALESCE(u.kv_rows, 0)                  AS kv_rows,
               COALESCE(u.kv_bytes, 0)                 AS kv_bytes,
               COALESCE(u.timer_rows, 0)               AS timer_rows,
               u.computed_at
          FROM queen.kv_quota q
          FULL OUTER JOIN queen.kv_usage u ON u.tenant_id = q.tenant_id
    ),
    ranked AS (
        SELECT m.*,
               -- Proximity to the nearest cap, 0 for an unlimited resource: an
               -- unlimited resource has no ratio, and a fake 1.0 would sort the
               -- tenants that cannot overrun to the top of the list.
               GREATEST(
                   CASE WHEN COALESCE(m.max_rows, 0)   > 0
                        THEN m.kv_rows::NUMERIC    / m.max_rows   ELSE 0 END,
                   CASE WHEN COALESCE(m.max_bytes, 0)  > 0
                        THEN m.kv_bytes::NUMERIC   / m.max_bytes  ELSE 0 END,
                   CASE WHEN COALESCE(m.max_timers, 0) > 0
                        THEN m.timer_rows::NUMERIC / m.max_timers ELSE 0 END
               ) AS pressure
          FROM merged m
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'tenant',        r.tenant_id::text,
               'granted',       r.granted,
               'enabled',       r.enabled,
               'maxRows',       r.max_rows,
               'maxBytes',      r.max_bytes,
               'maxTimers',     r.max_timers,
               'maxHorizonS',   r.max_timer_horizon_s,
               'maxReadsPerSec',  r.max_reads_per_sec,
               'maxWritesPerSec', r.max_writes_per_sec,
               'kvRows',        r.kv_rows,
               'kvBytes',       r.kv_bytes,
               'timerRows',     r.timer_rows,
               -- Epoch MILLISECONDS, and it is load-bearing rather than
               -- decorative: the broker clears its local delta only when the
               -- measurement it adopts is a NEW one, and this is how it tells.
               -- Adopting the same measurement ten times per rollup and clearing
               -- the delta each time would re-grant the whole headroom every
               -- 30 s and quietly restore the rate-and-time overrun §9.3 exists
               -- to remove.
               'computedAtMs',
                   CASE WHEN r.computed_at IS NULL THEN 0
                        ELSE (EXTRACT(EPOCH FROM r.computed_at) * 1000)::BIGINT END)
           -- The aggregate carries its own ORDER BY: a jsonb_agg over a subquery
           -- that "already sorted" is not ordered by the standard, and the
           -- top-N is the entire point of p_max_tenants.
           ORDER BY r.granted DESC, r.pressure DESC, (r.kv_rows + r.timer_rows) DESC),
           '[]'::jsonb)
      FROM (SELECT * FROM ranked
             ORDER BY granted DESC, pressure DESC, (kv_rows + timer_rows) DESC
             LIMIT GREATEST(COALESCE(p_max_tenants, 10000), 1)) r;
$$;

COMMENT ON FUNCTION queen.kv_quota_refresh_v1(INT) IS
    'Per-tenant limits + measured occupancy for the in-process quota enforcer (PLAN_KV_TIMERS §9.3). Reads only; never counts.';

-- The full argument list, spelled out. A GRANT whose type list does not match an
-- existing function does not warn: it FAILS the statement, the apply dies, and
-- the boot is fail-fast — so this is the line that takes the broker down if the
-- signature above is edited without editing here.
GRANT EXECUTE ON FUNCTION queen.kv_quota_refresh_v1(INT) TO PUBLIC;
