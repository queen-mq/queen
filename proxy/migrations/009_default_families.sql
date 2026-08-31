-- kv, timers and ephemeral are base surfaces, granted on every plan.
-- Applied as migration "009_default_families" after 008_bootstrap_password.
--
-- ============================================================================
-- WHAT THIS FIXES
-- ============================================================================
--
-- The plan seed in 002_functions.sql predates all three families. It names
-- `streams` and `traces` and nothing else, because on the day it was written
-- nothing else existed. kv and timers shipped with PLAN_KV_TIMERS, ephemeral
-- with EPHEMERAL_QUEUES, and neither release went back to the seed.
--
-- `cache::parse_features` reads a missing key as false, by design -- "the plan
-- row that does not name it is the plan that does not have it". Which means
-- that from the day each family shipped, all three answered 403 feature_gated
-- on EVERY plan, free through dedicated-s, on every cloud cell. Nobody decided
-- that. It is the seed's age showing through a default that is otherwise
-- exactly right.
--
-- The decision this migration writes down: these three are part of what a
-- Queen cluster IS, not upsells. `streams` and `traces` stay per-plan and this
-- file does not touch them.
--
-- ============================================================================
-- WHY IT TAKES TWO STATEMENTS
-- ============================================================================
--
-- The UPDATE is not just for databases that already exist. On a FRESH install
-- 002 runs first and inserts its four plans with `features` spelled out
-- ('{}' for free and dev, streams+traces for pro and dedicated-s), so the
-- column default below never fires for them either. The UPDATE is what covers
-- both cases; the ALTER is what covers plans added LATER.
--
-- And the ALTER covers less than it looks like it does. A column default fires
-- only when an INSERT OMITS the column: a future `INSERT ... (code, features)
-- VALUES ('x', '{}')` still produces a plan with all three off. That is the
-- trap in this file. If a plan is ever added by hand, name the three keys or
-- leave `features` out of the column list entirely.
--
-- NOT CHANGED: parse_features still defaults a missing key to false. The gate
-- mechanism is untouched and the NEXT family to ship is still default-off,
-- which is the property that makes the sentence quoted above true. This
-- migration grants three specific families; it does not invert the rule.
--
-- CONSEQUENCE WORTH KNOWING. kv and timers are Postgres-backed, and the
-- proxy's storage reconciler measures only `retainedBytes` -- the durable log
-- (registry.rs). `max_retained_bytes` therefore does not bound them on any
-- plan, including free on a shared cell. The bound for those two is a
-- `queen.kv_quota` row on the broker (max_rows / max_bytes / max_timers /
-- max_timer_horizon_s), which is enforced whether or not the cell requires
-- grants. Ephemeral needs no such row: it is RAM, capped by the cell's own
-- QUEEN_EPHEMERAL_MAX_BYTES, and the broker meters itself.
--
-- Idempotent: `||` is last-writer-wins per key, so re-running sets the same
-- three to true and preserves streams/traces and anything added later.
-- ============================================================================

UPDATE queen_proxy.plans
   SET features = features || '{"kv": true, "timers": true, "ephemeral": true}'::jsonb;

ALTER TABLE queen_proxy.plans
  ALTER COLUMN features
    SET DEFAULT '{"kv": true, "timers": true, "ephemeral": true}'::jsonb;
