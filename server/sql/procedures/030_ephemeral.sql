-- ============================================================================
-- 030_ephemeral.sql — the ONLY database footprint of the ephemeral queue class.
--
-- EPHEMERAL_QUEUES.md §2, and the whole file is one sentence long in intent:
-- PostgreSQL holds the CONFIGURATION of an ephemeral queue and the GRANT that
-- lets a tenant have one, and never a message.
--
-- ---------------------------------------------------------------------------
-- WHY THERE IS NO USAGE TABLE AND NO REFRESH SP (M7).
--
-- The KV quota needs `queen.kv_usage` because occupancy lives in a table nobody
-- can count cheaply, so a rollup measures it and every broker reads the
-- measurement. Here the broker IS the meter: the bytes are in its own heap and
-- `Ephemeral::global_bytes` is exact, in-process, for free. A usage table would
-- be a slower copy of a number the enforcer already holds, and a rollup would
-- add a periodic write for it. Neither exists, deliberately.
--
-- ---------------------------------------------------------------------------
-- WHY NEITHER TABLE IS ON ANY HOT PATH, AND HOW THAT IS ENFORCED (M12).
--
-- push / pop / ack never construct SQL and never take a pooled connection —
-- that is the entire performance premise of the class, not an optimization.
-- These two tables are read at boot and written by `configure` / `delete`, and
-- `tests/kv_handler_isolation.rs` is the mechanical guard: no source under
-- `src/handlers/` may spell either table name, in code OR in prose.
--
-- ---------------------------------------------------------------------------
-- LOCK ORDER (024_kv.sql §2). These tables participate in NO lock space of the
-- engine: nothing here is ever touched inside a message transaction, so no
-- actor can acquire one of these locks while holding queen.queues,
-- queen.log_partitions or queen.log_consumers — there is no path that reaches
-- this file from inside a push, a pop, an ack or the transaction wire.
--
-- ADD COLUMN IF NOT EXISTS alongside CREATE TABLE IF NOT EXISTS, the
-- 019_worker_metrics.sql / 024_kv.sql precedent: CREATE TABLE IF NOT EXISTS is
-- a SILENT no-op ON THE SHAPE, so a cell that already booted an earlier version
-- would keep the old table and meet the missing column as a 42703 in
-- production — classified as configuration, therefore never retried.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- DECLARED configs (§1.1 tier 2). Implicit queues — the ones a push or a pop
-- auto-vivifies, which is the tier req/reply inboxes live in — never appear
-- here at all: one row per short-lived per-client inbox would rebuild exactly
-- the partition-churn cost this class exists to kill.
--
-- What survives a restart is this row, NOT its contents (§1.2): a declared
-- queue comes back configured and EMPTY.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS queen.ephemeral_queues (
    tenant_id  UUID NOT NULL,
    queue      TEXT NOT NULL,
    PRIMARY KEY (tenant_id, queue)
);
ALTER TABLE queen.ephemeral_queues ADD COLUMN IF NOT EXISTS options    JSONB       NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE queen.ephemeral_queues ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- ---------------------------------------------------------------------------
-- The GRANT rows (§1.6 rung 2), same shape and same provisioning story as
-- queen.kv_quota: the orchestrator writes them, the broker only reads.
--
-- Same convention as the KV grant, and it is the reason the table is separate
-- rather than a set of columns on kv_quota: NULL = unlimited, and the ABSENCE
-- of the row is a DENIAL once QUEEN_EPHEMERAL_REQUIRE_GRANT is on (which
-- defaults to the tenancy header, i.e. off for OSS and on for cloud). A tenant
-- id is opaque and unvalidated, so a fail-open default would mint a fresh
-- unlimited tenant on every rotated header.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS queen.ephemeral_quota (
    tenant_id  UUID PRIMARY KEY,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE queen.ephemeral_quota ADD COLUMN IF NOT EXISTS enabled          BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE queen.ephemeral_quota ADD COLUMN IF NOT EXISTS max_bytes        BIGINT;
ALTER TABLE queen.ephemeral_quota ADD COLUMN IF NOT EXISTS max_queues       INTEGER;
ALTER TABLE queen.ephemeral_quota ADD COLUMN IF NOT EXISTS max_msgs_per_sec INTEGER;

-- Not `cost_delay 0`: autovacuum_max_workers is a GLOBAL budget (default 3) and
-- these two tables are tiny and cold. The aggressive settings belong to the
-- tables the engine writes on every message (024_kv.sql states the same rule).
ALTER TABLE queen.ephemeral_queues SET (autovacuum_vacuum_scale_factor = 0,
                                        autovacuum_vacuum_threshold = 500,
                                        vacuum_truncate = off);
ALTER TABLE queen.ephemeral_quota  SET (autovacuum_vacuum_scale_factor = 0,
                                        autovacuum_vacuum_threshold = 500,
                                        vacuum_truncate = off);

-- Every SP in this schema is SECURITY INVOKER (there is not one SECURITY
-- DEFINER anywhere), so the GRANT is required for a non-owner caller.
-- CONSEQUENCE, written here rather than discovered in an audit: per-tenant
-- isolation on these tables is EXACTLY the WHERE clause inside the functions
-- below. There is no RLS. Any direct SQL reader bypasses it, and no HTTP route
-- may ever build a query against them outside these SPs.
GRANT SELECT, INSERT, UPDATE, DELETE ON queen.ephemeral_queues TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON queen.ephemeral_quota  TO PUBLIC;

-- ---------------------------------------------------------------------------
-- queen.eph_config_set_v1 — declare or reconfigure one queue.
--
-- The options blob is stored WHOLE and is not validated here. That is the
-- opposite of the KV rule (§9.2 puts shape limits in SQL so seven clients
-- inherit one copy) and the difference is deliberate: an ephemeral option is
-- interpreted only by the in-RAM engine, which clamps every one of them against
-- the broker's own knobs at load time. A CHECK here would be a SECOND authority
-- on a value the engine must clamp anyway, and the two would drift the first
-- time an operator lowered QUEEN_EPHEMERAL_QUEUE_MAX_BYTES.
--
-- `p_tenant` is an ARGUMENT, never a field read out of the options blob — the
-- §13.1 rule the KV surface is held to, restated because this table is keyed by
-- (tenant_id, queue) and a tenant read from user JSON would silently write
-- another customer's row.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.eph_config_set_v1(
    p_tenant  TEXT,
    p_queue   TEXT,
    p_options JSONB
) RETURNS JSONB
LANGUAGE sql
VOLATILE
AS $$
    INSERT INTO queen.ephemeral_queues AS e (tenant_id, queue, options, updated_at)
    VALUES (p_tenant::uuid, p_queue, COALESCE(p_options, '{}'::jsonb), now())
    ON CONFLICT (tenant_id, queue) DO UPDATE
        SET options = EXCLUDED.options, updated_at = now()
    RETURNING jsonb_build_object(
        'queue',     e.queue,
        'options',   e.options,
        'updatedAt', to_char(e.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'));
$$;

COMMENT ON FUNCTION queen.eph_config_set_v1(TEXT, TEXT, JSONB) IS
    'Declare/reconfigure one ephemeral queue (EPHEMERAL_QUEUES §2). Config is durable; contents never are.';

-- ---------------------------------------------------------------------------
-- queen.eph_config_get_v1 — one declared config, or SQL NULL when the queue is
-- implicit (or gone). NULL and not an {"error":...} object: an implicit queue is
-- the NORMAL case for this class, not a failure, and `sp_result_to_response`
-- maps an embedded error onto a 404/500 that this answer must never become.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.eph_config_get_v1(
    p_tenant TEXT,
    p_queue  TEXT
) RETURNS JSONB
LANGUAGE sql
STABLE
AS $$
    SELECT jsonb_build_object(
               'queue',     e.queue,
               'options',   e.options,
               'updatedAt', to_char(e.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'))
      FROM queen.ephemeral_queues e
     WHERE e.tenant_id = p_tenant::uuid
       AND e.queue = p_queue;
$$;

COMMENT ON FUNCTION queen.eph_config_get_v1(TEXT, TEXT) IS
    'One declared ephemeral queue config, or NULL when the queue is implicit.';

-- ---------------------------------------------------------------------------
-- queen.eph_config_delete_v1 — forget the declaration.
--
-- Returns `{"deleted": bool}` and NOT an error on a miss, the house rule the KV
-- surface states at length and queue deletion already follows: the status
-- describes the outcome of the CALL, never the verdict of the predicate. The
-- RAM side of a delete (drop the rings, void the leases) is the broker's job and
-- has no representation here — there is nothing durable to drop.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.eph_config_delete_v1(
    p_tenant TEXT,
    p_queue  TEXT
) RETURNS JSONB
LANGUAGE sql
VOLATILE
AS $$
    WITH gone AS (
        DELETE FROM queen.ephemeral_queues
         WHERE tenant_id = p_tenant::uuid
           AND queue = p_queue
        RETURNING 1
    )
    SELECT jsonb_build_object('deleted', EXISTS (SELECT 1 FROM gone));
$$;

COMMENT ON FUNCTION queen.eph_config_delete_v1(TEXT, TEXT) IS
    'Drop one declared ephemeral queue config. 200 with deleted:false on a miss, never an error.';

-- ---------------------------------------------------------------------------
-- queen.eph_config_list_v1 — every declared config of ONE tenant, for the boot
-- load (§2: read before the listener opens, like the KV grants).
--
-- p_limit is not decoration. The result lands in a per-tenant map in broker RAM,
-- and an unbounded list is how a boot read becomes the incident on the cell with
-- the most declared queues. ORDER BY queue so the truncation is deterministic
-- and an operator can tell WHICH configs a capped boot dropped.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.eph_config_list_v1(
    p_tenant TEXT,
    p_limit  INT DEFAULT 10000
) RETURNS JSONB
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'queue',     r.queue,
               'options',   r.options,
               'updatedAt', to_char(r.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'))
           -- The aggregate carries its own ORDER BY: a jsonb_agg over a
           -- subquery that "already sorted" is not ordered by the standard.
           ORDER BY r.queue), '[]'::jsonb)
      FROM (SELECT e.queue, e.options, e.updated_at
              FROM queen.ephemeral_queues e
             WHERE e.tenant_id = p_tenant::uuid
             ORDER BY e.queue
             LIMIT GREATEST(COALESCE(p_limit, 10000), 1)) r;
$$;

COMMENT ON FUNCTION queen.eph_config_list_v1(TEXT, INT) IS
    'Declared ephemeral queue configs of one tenant, for the boot load. Bounded and ordered.';

-- ---------------------------------------------------------------------------
-- queen.eph_quota_list_v1 — the grants, for the in-process ladder.
--
-- Reads ONE table and counts NOTHING, the first rule of 027_kv_quota.sql: this
-- poll runs on every broker on a cadence, and a count(*) here would make the
-- cost of enforcing the limit a function of the data it bounds. There is nothing
-- to join against either — occupancy is in-process (M7), so unlike the KV
-- refresh this is a plain bounded read of the grant table.
--
-- The cap ORDER is "smallest allowance first": a tenant with a tight limit is
-- the one whose grant actually decides something, and dropping it in favour of
-- an unlimited one would silently turn the enforcer off for exactly the tenant
-- it exists for. NULLS LAST puts the unlimited rows at the end for that reason.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.eph_quota_list_v1(
    p_max_tenants INT DEFAULT 10000
) RETURNS JSONB
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'tenant',        r.tenant_id::text,
               'enabled',       r.enabled,
               'maxBytes',      r.max_bytes,
               'maxQueues',     r.max_queues,
               'maxMsgsPerSec', r.max_msgs_per_sec)
           ORDER BY r.max_bytes NULLS LAST, r.tenant_id), '[]'::jsonb)
      FROM (SELECT q.tenant_id, q.enabled, q.max_bytes, q.max_queues, q.max_msgs_per_sec
              FROM queen.ephemeral_quota q
             ORDER BY q.max_bytes NULLS LAST, q.tenant_id
             LIMIT GREATEST(COALESCE(p_max_tenants, 10000), 1)) r;
$$;

COMMENT ON FUNCTION queen.eph_quota_list_v1(INT) IS
    'Ephemeral grant rows for the in-process ladder (EPHEMERAL_QUEUES §1.6 rung 2). Reads one table, counts nothing.';
