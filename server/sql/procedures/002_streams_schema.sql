-- ============================================================================
-- queen-streams v0.1: schema and tables
-- ============================================================================
--
-- The streaming engine stores its query metadata and per-partition state in a
-- dedicated `queen_streams` schema, so it can never collide with the broker's
-- `queen.*` namespace and so backups / RBAC can target it independently.
--
-- The state PK is (query_id, partition_id, key). `partition_id` is part of
-- the PK because Queen's partition leases are exclusive: only one streaming
-- worker holds a given partition at a time, so all writes to state rows for
-- (query_id, partition_id) come from one writer. That gives per-partition
-- state isolation with zero cross-worker lock contention — see plan
-- "Partition leverage" section.
--
-- TENANCY (Track B §5, 2026-08-27). A query belongs to ONE tenant.
-- `queries.tenant_id` scopes the row and `name` is unique PER TENANT, not
-- globally — the identity shape queue names have carried since the 2026-07-31
-- merge (schema.sql's queues_tenant_name_uk). Two tenants may both register
-- 'orders.per_customer_per_min' and neither can see, update or reset the
-- other's.
--
-- `state` deliberately carries NO tenant column, and that is not an omission.
-- Every state row hangs off a `query_id` whose FK reaches `queries`, so it is
-- tenant-attributable THROUGH THAT FK regardless of partition lifecycle — which
-- is what a tenant purge needs (031_tenant_purge.sql) and what the row's
-- `partition_id` alone can never give once the partition is gone. A second
-- column would be a second truth to keep in sync on the hottest write in the
-- engine (the per-cycle state upsert), bought for nothing.
--
-- `quota` is the grant: with tenancy ON the ABSENCE of a row is a DENIAL, the
-- same posture as queen.kv_quota (024_kv.sql, and config.rs's
-- `kv_require_grant`) and for the same reason — the tenant header is opaque and
-- validated against nothing, so a fail-open default would let a client mint a
-- fresh unlimited tenant on every request. The default tenant is exempt: an OSS
-- / self-hosted broker has no control plane to write the row and must never
-- need one.
--
-- Auto-loaded by libqueen.initialize_schema in alphabetical order, so this
-- file (002_) runs after the broker's base schema and before the streams_*
-- procedures (008_streams_register_query_v1 / 009_streams_state_get_v1).
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS queen_streams;

-- ----------------------------------------------------------------------------
-- queen_streams.queries
-- ----------------------------------------------------------------------------
-- One row per registered streaming query, owned by one tenant. `name` is the
-- user-facing queryId (e.g. 'orders.per_customer_per_min') and is unique
-- WITHIN A TENANT — see queries_tenant_name_uk below. `config_hash`
-- fingerprints the operator chain shape so a redeploy with changed operators
-- against the same queryId is caught by the SDK and refused unless
-- { reset: true } is passed.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS queen_streams.queries (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- No inline UNIQUE: uniqueness is the (tenant_id, name) index below. The
    -- pre-tenancy revision of this file declared `name TEXT UNIQUE NOT NULL`,
    -- which is why the constraint drop below exists at all.
    name         TEXT NOT NULL,
    source_queue TEXT NOT NULL,
    sink_queue   TEXT,
    config_hash  TEXT NOT NULL,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    updated_at   TIMESTAMPTZ DEFAULT NOW()
);

-- ADD COLUMN IF NOT EXISTS, not a column in the CREATE above: CREATE TABLE IF
-- NOT EXISTS is a SILENT no-op ON THE SHAPE (precedent 024_kv.sql:243-253,
-- 019_worker_metrics.sql), so a cell that already booted the pre-tenancy file
-- keeps its old table and would meet the missing column as a 42703 at the first
-- register — in production, on a path classified as configuration and therefore
-- never retried. The DEFAULT is what makes the whole surface byte-identical
-- with tenancy off: existing rows and every caller that omits p_tenant land on
-- the same constant every other tenant_id column in this deployment defaults to.
ALTER TABLE queen_streams.queries
    ADD COLUMN IF NOT EXISTS tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';

-- Uniqueness moves from (name) to (tenant_id, name). CREATE FIRST, DROP SECOND:
-- in that order there is no instant, on a live cell whose peer brokers are
-- serving registers, in which nothing at all guards the name. The reverse order
-- opens exactly such a window between two statements of the same boot.
CREATE UNIQUE INDEX IF NOT EXISTS queries_tenant_name_uk
    ON queen_streams.queries(tenant_id, name);

-- `queries_name_key` is PostgreSQL's auto-generated name for the column-level
-- UNIQUE of the pre-tenancy CREATE TABLE (<table>_<column>_key), and a
-- column-level UNIQUE always materializes as a CONSTRAINT (its backing index
-- goes with it). The DROP INDEX after it is not redundant: it covers the one
-- shape the constraint drop cannot reach — a plain unique index someone created
-- by hand under the same name — which would otherwise keep enforcing GLOBAL
-- name uniqueness and make a second tenant's identically-named query fail with
-- a duplicate key nobody can explain. Both are IF EXISTS, so a fresh database
-- (where neither was ever created) passes straight through.
ALTER TABLE queen_streams.queries DROP CONSTRAINT IF EXISTS queries_name_key;
DROP INDEX IF EXISTS queen_streams.queries_name_key;

-- ----------------------------------------------------------------------------
-- queen_streams.state
-- ----------------------------------------------------------------------------
-- Per-(query, partition, key) state for stateful operators. The composite
-- PK clusters state physically per partition, so the worker holding the
-- partition lease reads/writes state from a tight set of index pages.
--
-- Foreign key to queries with ON DELETE CASCADE so dropping a query also
-- drops its state. partition_id carries a queen.log_partitions id and is
-- deliberately NOT a foreign key: that would couple lifecycles, and a partition
-- cleanup must not silently delete still-relevant state. The cleanup phase in
-- 006_log_maintenance honours this the other way round — a partition holding
-- stream state is
-- never reclaimed. State is dropped by the application on a query reset.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS queen_streams.state (
    query_id     UUID NOT NULL REFERENCES queen_streams.queries(id) ON DELETE CASCADE,
    partition_id UUID NOT NULL,
    key          TEXT NOT NULL,
    value        JSONB NOT NULL,
    updated_at   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (query_id, partition_id, key)
);

-- Supporting index for "list state for one (query, partition)" reads — the
-- common shape of streams_state_get_v1 queries when a worker fetches all keys
-- it needs at the start of a cycle. The PK already indexes
-- (query_id, partition_id) as a prefix, so this is just for clarity; PG can
-- use the PK directly. No separate index needed.

-- Optional: index by updated_at for state-aging dashboards.
CREATE INDEX IF NOT EXISTS idx_streams_state_updated_at
    ON queen_streams.state (updated_at);

-- ----------------------------------------------------------------------------
-- queen_streams.quota
-- ----------------------------------------------------------------------------
-- The GRANT, and the only quota the streaming engine enforces. One row per
-- tenant, written by the control plane (nothing in the broker creates one).
--
-- DENY BY DEFAULT with tenancy ON: no row, or `enabled = false`, means the
-- tenant may not register NEW queries — 008_streams_register_query_v1 refuses
-- on the fresh-insert path with {"denied": true}. Absence is a denial and not a
-- permission for the same reason as queen.kv_quota (024_kv.sql's header, §9.4):
-- the x-queen-tenant header is opaque and validated against nothing, so a
-- fail-open default hands unlimited query space to any invented id.
--
-- THE DEFAULT TENANT IS NEVER CHECKED. That is the whole of the OSS story: a
-- self-hosted broker, an embedded broker and every pre-tenancy caller land on
-- '...0001', where the register path skips this table entirely and needs no row
-- to exist. Behaviour with QUEEN_TENANCY_HEADER unset is byte-identical to the
-- pre-tenancy engine.
--
-- ONLY ENFORCED COLUMNS LIVE HERE. `max_queries` is a cap on registered
-- queries and is checked under a FOR UPDATE lock so it is exact. There is
-- deliberately no max_state_bytes / max_state_rows knob: streams STORAGE quota
-- is not implemented, and a column that reads like a limit while nothing reads
-- it is worse than an absent one — it is a promise the engine does not keep.
-- (Any column added later needs its own ADD COLUMN IF NOT EXISTS, above the
-- same silent-no-op trap CREATE TABLE IF NOT EXISTS sets for `queries`.)
--
-- This table carries tenant_id, so 031_tenant_purge.sql's coverage check
-- REQUIRES it in that function's table list — see the comment there.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS queen_streams.quota (
    tenant_id   UUID PRIMARY KEY,
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    max_queries BIGINT,          -- NULL = unlimited
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

GRANT USAGE ON SCHEMA queen_streams TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON queen_streams.queries TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON queen_streams.state TO PUBLIC;
-- Same grant as the two tables above, same reasoning: the broker connects as
-- ONE role, and the register path both reads this row and locks it FOR UPDATE.
GRANT SELECT, INSERT, UPDATE, DELETE ON queen_streams.quota TO PUBLIC;
