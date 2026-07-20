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
-- Auto-loaded by libqueen.initialize_schema in alphabetical order, so this
-- file (019_) runs after the broker's base schema and before the streams_*
-- procedures (020_..022_).
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS queen_streams;

-- ----------------------------------------------------------------------------
-- queen_streams.queries
-- ----------------------------------------------------------------------------
-- One row per registered streaming query. `name` is the user-facing queryId
-- (e.g. 'orders.per_customer_per_min'). `config_hash` fingerprints the
-- operator chain shape so a redeploy with changed operators against the same
-- queryId is caught by the SDK and refused unless { reset: true } is passed.
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS queen_streams.queries (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name         TEXT UNIQUE NOT NULL,
    source_queue TEXT NOT NULL,
    sink_queue   TEXT,
    config_hash  TEXT NOT NULL,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    updated_at   TIMESTAMPTZ DEFAULT NOW()
);

-- ----------------------------------------------------------------------------
-- queen_streams.state
-- ----------------------------------------------------------------------------
-- Per-(query, partition, key) state for stateful operators. The composite
-- PK clusters state physically per partition, so the worker holding the
-- partition lease reads/writes state from a tight set of index pages.
--
-- Foreign key to queries with ON DELETE CASCADE so dropping a query also
-- drops its state. We do NOT FK partition_id to queen.partitions because
-- that would couple lifecycles: a partition-cleanup must not silently delete
-- still-relevant state. Instead we rely on the application to drop state
-- explicitly when it issues a query reset.
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

GRANT USAGE ON SCHEMA queen_streams TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON queen_streams.queries TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON queen_streams.state TO PUBLIC;
