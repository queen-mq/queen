-- ============================================================================
-- Durable per-task maintenance scheduler (PLAN_STATS_REFRESH.md §2 / T2.1).
--
--   queen.maintenance_leases   one row per cluster-singleton maintenance task
--
-- The advisory locks the loops used before this table (737_001 retention,
-- 737_002 stats, 737_003 retained-bytes) are MUTUAL EXCLUSION, not cadence:
-- the lock dies with its holder's transaction/session, nothing durable records
-- that the period was served, and each replica keeps its own timer — so the
-- cluster ran every task at interval/replicas ("x3 FOR THE REPLICA COUNT" in
-- the prod chart), and a fleet resize silently rescaled all maintenance.
--
-- This table is the cadence. One row per task; the DB clock arbitrates
-- (`next_due_at <= now()`, pod clocks are irrelevant); a lease bounds failover
-- when a holder dies mid-cycle; a fencing token makes a resurrected holder's
-- release a no-op. Claim and release are each ONE autocommitted single-row
-- UPDATE issued by the Rust loops (server/src/lease.rs) — there is no SQL
-- function wrapper on purpose: the statements are trivial and the semantics
-- (advance-at-release-only) live next to the loop that depends on them.
--
--   claim:    UPDATE ... SET lease_until = now()+lease, fence = fence+1,
--             last_start_at = now()
--             WHERE task = $1 AND enabled AND next_due_at <= now()
--               AND (lease_until IS NULL OR lease_until <= now())
--             RETURNING fence
--   release:  UPDATE ... SET next_due_at = GREATEST(next_due_at + period, now()),
--             lease_until = NULL, ... WHERE task = $1 AND fence = $fence
--
-- TWO TRAPS the shape encodes (do not "fix" them):
--   * next_due_at advances at RELEASE only, exactly once, fixed-rate with a
--     catch-up clamp. Advancing it at claim time too delivers claim + 2P — the
--     configured cadence, silently halved.
--   * the claim without the lease (stamping last_start_at alone) permits two
--     concurrent runners, which is strictly worse than the advisory lock.
--
-- The advisory locks stay in the loops as BELT during the transition (a mixed
-- fleet has old-image pods that know only the locks); they retire once every
-- deployment schedules through this table.
--
-- Per-replica loops (hotlist reseed, syscollect, KV sweeper shards) must NOT
-- move onto rows here: each replica genuinely needs its own run — this table
-- is only for tasks that are one-per-cluster.
--
-- `enabled` is an operator kill switch (one UPDATE pauses a task cluster-wide);
-- boot never overwrites it. `period_ms` follows the broker env config at boot
-- (last boot wins, like the SQL bundle itself), and shrinking it also clamps
-- next_due_at so the shorter period takes effect within one new period.
-- ============================================================================

CREATE TABLE IF NOT EXISTS queen.maintenance_leases (
    task             TEXT PRIMARY KEY,       -- 'stats_refresh' | 'retained_bytes' | 'retention'
    period_ms        BIGINT      NOT NULL,
    next_due_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_until      TIMESTAMPTZ,
    holder           TEXT,
    fence            BIGINT      NOT NULL DEFAULT 0,
    last_start_at    TIMESTAMPTZ,
    last_end_at      TIMESTAMPTZ,
    last_duration_ms INTEGER,
    runs             BIGINT      NOT NULL DEFAULT 0,
    enabled          BOOLEAN     NOT NULL DEFAULT TRUE
);
