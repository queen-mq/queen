-- queen_proxy schema v1 -- PLAN_QUEEN_PROXY_CLOUD.md (rev 1.2) §3, CONTRACTS.md.
-- Applied by db::apply_migrations (queen_proxy.schema_migrations tracks it by
-- name "001_init"); also safe to run by hand via `psql -f` / `\i` against an
-- empty pxdb -- every statement is idempotent (IF NOT EXISTS / OR REPLACE).
--
-- Conventions used throughout this file (also load-bearing for src/cache.rs
-- and src/registry.rs, which read these tables):
--   * All objects live in schema `queen_proxy` (never `public`), fully
--     qualified everywhere -- no reliance on search_path for OUR objects.
--   * Primary keys are UUID DEFAULT public.gen_random_uuid() (pgcrypto).
--     Explicitly schema-qualified (`public.gen_random_uuid()`) rather than a
--     bare call: CREATE EXTENSION's target schema and a role's search_path
--     are both environment-dependent (managed/hardened PG instances vary),
--     so we pin both the extension's install schema and every call site.
--   * Lifecycle/status columns are TEXT + CHECK, not a PG enum type (per
--     brief: enums are painful to extend; a CHECK is a one-line migration).
--   * Plan/limit columns are INTEGER or BIGINT, NULL = unlimited, CHECK(>0)
--     when set. This is the ONLY place raw limits live; queen_proxy.clusters
--     never duplicates them (see limit_overrides below).
--   * Every foreign key's ON DELETE is chosen deliberately (see inline
--     comments) -- this schema models lifecycle via `status` columns, not
--     hard deletes, so most FKs default to RESTRICT (protects history);
--     child/derived rows that are meaningless without their parent CASCADE.

CREATE SCHEMA IF NOT EXISTS queen_proxy;

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;

-- ============================================================================
-- tenants -- the paying org. Clusters hang off tenants, not users (PLAN §3:
-- "users churn; orgs don't").
-- ============================================================================
CREATE TABLE IF NOT EXISTS queen_proxy.tenants (
    id         UUID PRIMARY KEY DEFAULT public.gen_random_uuid(),
    slug       TEXT NOT NULL UNIQUE
               CHECK (slug ~ '^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$'),
    name       TEXT NOT NULL CHECK (btrim(name) <> ''),
    status     TEXT NOT NULL DEFAULT 'active'
               CHECK (status IN ('active', 'grace', 'suspended', 'deleting')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================================
-- users -- login identity, scoped to one tenant. Roles live on cluster_roles,
-- not here (a user's permissions are per-cluster, PLAN §3).
-- ============================================================================
CREATE TABLE IF NOT EXISTS queen_proxy.users (
    id            UUID PRIMARY KEY DEFAULT public.gen_random_uuid(),
    tenant_id     UUID NOT NULL REFERENCES queen_proxy.tenants(id) ON DELETE CASCADE,
    email         TEXT NOT NULL UNIQUE,
    -- bcrypt hash; NULL for users who have only ever authenticated via an
    -- OAuth identity (see `identities` below).
    password_hash TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_users_tenant ON queen_proxy.users(tenant_id);

-- ============================================================================
-- identities -- multi-IdP linkage (local | google | github), one row per
-- linked provider per user. `local` gets a row too (uniform identity model,
-- per PLAN §3) with provider_id left to the caller's convention (e.g. the
-- user's own id) -- no function in the CONTRACTS.md control-plane interface
-- writes this table; see the report for who's expected to (auth.rs/Agent C).
-- ============================================================================
CREATE TABLE IF NOT EXISTS queen_proxy.identities (
    id          UUID PRIMARY KEY DEFAULT public.gen_random_uuid(),
    user_id     UUID NOT NULL REFERENCES queen_proxy.users(id) ON DELETE CASCADE,
    provider    TEXT NOT NULL CHECK (provider IN ('local', 'google', 'github')),
    provider_id TEXT NOT NULL,
    email       TEXT NOT NULL,
    verified    BOOLEAN NOT NULL DEFAULT false,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (provider, provider_id)
);

CREATE INDEX IF NOT EXISTS idx_identities_user ON queen_proxy.identities(user_id);

-- ============================================================================
-- plans -- the catalog. Per-cluster deltas live in clusters.limit_overrides,
-- so support can bump one limit without forking a plan (PLAN §3).
--
-- Every *_bytes/_per_sec/_pops/_items/_queues/_seconds/_quota column: NULL
-- means unlimited, otherwise must be positive. `code` is an open catalog key
-- (free|dev|pro|dedicated-s|dedicated-m|...), deliberately NOT a CHECK-list
-- enum -- see 001_init header note on TEXT+CHECK for *lifecycle* columns;
-- plan codes are catalog data, not a fixed state machine, so they only get a
-- light format check.
-- ============================================================================
CREATE TABLE IF NOT EXISTS queen_proxy.plans (
    id                        UUID PRIMARY KEY DEFAULT public.gen_random_uuid(),
    code                      TEXT NOT NULL UNIQUE CHECK (code ~ '^[a-z][a-z0-9-]*$'),
    cell_class                TEXT NOT NULL CHECK (cell_class IN ('shared', 'dedicated')),
    max_req_per_sec           INTEGER CHECK (max_req_per_sec IS NULL OR max_req_per_sec > 0),
    req_burst                 INTEGER CHECK (req_burst IS NULL OR req_burst > 0),
    max_msgs_per_sec          INTEGER CHECK (max_msgs_per_sec IS NULL OR max_msgs_per_sec > 0),
    msgs_burst                INTEGER CHECK (msgs_burst IS NULL OR msgs_burst > 0),
    max_queues                INTEGER CHECK (max_queues IS NULL OR max_queues > 0),
    max_partitions_per_queue  INTEGER CHECK (max_partitions_per_queue IS NULL OR max_partitions_per_queue > 0),
    max_parked_pops           INTEGER CHECK (max_parked_pops IS NULL OR max_parked_pops > 0),
    max_payload_bytes         INTEGER CHECK (max_payload_bytes IS NULL OR max_payload_bytes > 0),
    max_batch_items           INTEGER CHECK (max_batch_items IS NULL OR max_batch_items > 0),
    max_retained_bytes        BIGINT  CHECK (max_retained_bytes IS NULL OR max_retained_bytes > 0),
    max_retention_seconds     INTEGER CHECK (max_retention_seconds IS NULL OR max_retention_seconds > 0),
    -- soft monthly ceiling, warn->block from usage_days rollups (PLAN §6.7);
    -- distinct from the per-second buckets above. NULL = unlimited. Left NULL
    -- on every seeded v1 plan (002_functions.sql) -- no monthly figure was
    -- briefed, so none is invented.
    monthly_msgs_quota        BIGINT  CHECK (monthly_msgs_quota IS NULL OR monthly_msgs_quota > 0),
    -- {"streams": bool, "traces": bool}; a missing key means false (cache.rs
    -- parses it that way). Deliberately open jsonb (not columns) so a new
    -- gated feature is a data change, not a migration.
    features                  JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================================
-- cells -- one physical Queen stack (broker+PG+proxy). Infrastructure, not
-- tenant business data: provisioned directly by ops (no queen_proxy.* SQL
-- function creates a cell -- CONTRACTS.md's control-plane contract covers
-- tenant/cluster/user/key lifecycle, not fleet infra).
--
-- Deviations from the PLAN §3 ERD, both deliberate (see report):
--   * `slug` added -- the ERD gave cells no human-referenceable key at all,
--     only `id`; create_cluster(cell_id) and any admin/seed tooling need a
--     stable way to name a cell ("local", "eu1-shared-a", ...).
--   * `cell_secret` (raw bearer token) added in place of the ERD's
--     `cell_secret_ref`. The proxy has no secret-store integration in v1, so
--     rather than adding a `_ref` column that resolves nowhere, this stores
--     the actual token the proxy presents to the cell broker's JWT_ENABLED
--     check (src/cache.rs reads it as ClusterCtx.cell_token). TODO before
--     multi-region/hardening: move to a vault-backed reference.
-- ============================================================================
CREATE TABLE IF NOT EXISTS queen_proxy.cells (
    id              UUID PRIMARY KEY DEFAULT public.gen_random_uuid(),
    slug            TEXT NOT NULL UNIQUE CHECK (btrim(slug) <> ''),
    region          TEXT NOT NULL CHECK (btrim(region) <> ''),
    base_url        TEXT NOT NULL CHECK (btrim(base_url) <> ''),
    class           TEXT NOT NULL CHECK (class IN ('shared', 'dedicated')),
    capacity_slots  INTEGER NOT NULL DEFAULT 0 CHECK (capacity_slots >= 0),
    used_slots      INTEGER NOT NULL DEFAULT 0 CHECK (used_slots >= 0),
    broker_version  TEXT,
    status          TEXT NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'draining', 'dead')),
    -- MVP plaintext secret -- see header note above.
    cell_secret     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================================
-- clusters -- the tenant-visible logical Queen (endpoint + quota bundle).
-- N:1 tenant->cluster is fine; cluster->cell is N:1 on shared cells, 1:1 on
-- dedicated (enforced by ops/placement, not a DB constraint).
-- ============================================================================
CREATE TABLE IF NOT EXISTS queen_proxy.clusters (
    id                 UUID PRIMARY KEY DEFAULT public.gen_random_uuid(),
    tenant_id          UUID NOT NULL REFERENCES queen_proxy.tenants(id) ON DELETE CASCADE,
    cell_id            UUID NOT NULL REFERENCES queen_proxy.cells(id) ON DELETE RESTRICT,
    plan_id            UUID NOT NULL REFERENCES queen_proxy.plans(id) ON DELETE RESTRICT,
    -- the subdomain: <slug>.<region>.queenmq.cloud (PLAN §2). DNS-label
    -- shaped since it's literally used as one.
    slug               TEXT NOT NULL UNIQUE
                       CHECK (slug ~ '^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$'),
    -- the X-Queen-Tenant value injected upstream (Track B, PLAN §5). Own
    -- default so callers never have to invent one; not the tenants.id.
    broker_tenant_uuid UUID NOT NULL DEFAULT public.gen_random_uuid(),
    status             TEXT NOT NULL DEFAULT 'active'
                       CHECK (status IN ('active', 'push_blocked', 'suspended', 'deleting')),
    -- Per-cluster deltas over the plan. Convention (cache.rs merge_limits):
    -- object keys match the plans.* limit column names 1:1
    -- (max_req_per_sec, req_burst, max_msgs_per_sec, msgs_burst, max_queues,
    -- max_partitions_per_queue, max_parked_pops, max_payload_bytes,
    -- max_batch_items, max_retained_bytes, max_retention_seconds). A key
    -- ABSENT from the object inherits the plan's value; a key present with
    -- JSON null explicitly forces "unlimited" (overriding a plan cap, not
    -- just falling back to it); a key present with a number overrides it.
    -- No queen_proxy.* function in CONTRACTS.md's list writes this column
    -- today -- see report ("set_limit_override" gap).
    limit_overrides    JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_clusters_cell ON queen_proxy.clusters(cell_id);
-- Powers set_tenant_status's per-cluster NOTIFY fan-out (002_functions.sql).
CREATE INDEX IF NOT EXISTS idx_clusters_tenant ON queen_proxy.clusters(tenant_id);

-- ============================================================================
-- cluster_roles -- a user's role on a specific cluster (the brief's "user
-- permissions on operations"). One role per (user, cluster).
-- ============================================================================
CREATE TABLE IF NOT EXISTS queen_proxy.cluster_roles (
    user_id    UUID NOT NULL REFERENCES queen_proxy.users(id) ON DELETE CASCADE,
    cluster_id UUID NOT NULL REFERENCES queen_proxy.clusters(id) ON DELETE CASCADE,
    role       TEXT NOT NULL CHECK (role IN ('admin', 'producer', 'consumer', 'viewer')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, cluster_id)
);

CREATE INDEX IF NOT EXISTS idx_cluster_roles_cluster ON queen_proxy.cluster_roles(cluster_id);

-- ============================================================================
-- api_keys -- cluster-scoped credentials (`qk_<env>_<43 b64url chars>`,
-- stored as sha256 hex per CONTRACTS.md's auth::key_hash_hex).
-- ============================================================================
CREATE TABLE IF NOT EXISTS queen_proxy.api_keys (
    id            UUID PRIMARY KEY DEFAULT public.gen_random_uuid(),
    cluster_id    UUID NOT NULL REFERENCES queen_proxy.clusters(id) ON DELETE CASCADE,
    name          TEXT NOT NULL CHECK (btrim(name) <> ''),
    key_hash      TEXT NOT NULL UNIQUE CHECK (key_hash ~ '^[0-9a-f]{64}$'),
    scopes        TEXT[] NOT NULL DEFAULT '{}'
                  CHECK (scopes <@ ARRAY['produce', 'consume', 'admin', 'read']::text[]),
    created_by    UUID REFERENCES queen_proxy.users(id) ON DELETE SET NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at  TIMESTAMPTZ,
    revoked_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_api_keys_cluster ON queen_proxy.api_keys(cluster_id);

-- ============================================================================
-- queues -- registry CACHE for plan-cap admission (src/registry.rs) and the
-- broker-inventory reconciler. NOT the source of truth for queue existence
-- (the broker is, per PLAN §5) -- this table exists so the proxy can answer
-- "how many queues/partitions does this cluster have" without asking the
-- broker on every push.
-- ============================================================================
CREATE TABLE IF NOT EXISTS queen_proxy.queues (
    id               UUID PRIMARY KEY DEFAULT public.gen_random_uuid(),
    cluster_id       UUID NOT NULL REFERENCES queen_proxy.clusters(id) ON DELETE CASCADE,
    name             TEXT NOT NULL CHECK (btrim(name) <> ''),
    partitions_count INTEGER NOT NULL DEFAULT 0 CHECK (partitions_count >= 0),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at       TIMESTAMPTZ
);

-- Partial unique index doubles as the admission upsert's ON CONFLICT target
-- (src/registry.rs) and lets a queue name be reused after its row is
-- soft-deleted (reconciler-observed disappearance).
CREATE UNIQUE INDEX IF NOT EXISTS uq_queues_cluster_name_live
    ON queen_proxy.queues(cluster_id, name) WHERE deleted_at IS NULL;

-- ============================================================================
-- usage_minutes -- the metering rollup (src/meter.rs writes this; Agent D).
-- One row per (cluster, minute, op_class), UPSERT-added-to by the flush
-- loop. Rolls up further into usage_days (not created here -- no consumer
-- for it yet; add when the daily-rollup job lands).
-- ============================================================================
CREATE TABLE IF NOT EXISTS queen_proxy.usage_minutes (
    cluster_id UUID NOT NULL REFERENCES queen_proxy.clusters(id) ON DELETE CASCADE,
    minute     TIMESTAMPTZ NOT NULL,
    op_class   TEXT NOT NULL CHECK (op_class IN ('push', 'delivery', 'txn', 'configure', 'read')),
    msgs       BIGINT NOT NULL DEFAULT 0 CHECK (msgs >= 0),
    reqs       BIGINT NOT NULL DEFAULT 0 CHECK (reqs >= 0),
    bytes_in   BIGINT NOT NULL DEFAULT 0 CHECK (bytes_in >= 0),
    bytes_out  BIGINT NOT NULL DEFAULT 0 CHECK (bytes_out >= 0),
    PRIMARY KEY (cluster_id, minute, op_class)
);

-- Cross-cluster time-range scans for the rollup job.
CREATE INDEX IF NOT EXISTS idx_usage_minutes_minute ON queen_proxy.usage_minutes(minute);

-- ============================================================================
-- operations -- append-only audit history. Every queen_proxy.* mutating
-- function writes exactly one row here via record_operation()
-- (002_functions.sql), which is also the sole pg_notify('queen_proxy_inval')
-- emitter. tenant_id is required (every action is about some tenant);
-- cluster_id is optional (tenant-level actions -- e.g. tenant_status_changed
-- -- have none). cluster_id uses ON DELETE SET NULL rather than CASCADE: a
-- hard-deleted cluster's history should outlive the row, unlike tenant_id
-- (RESTRICT, default) which protects against orphaning the audit trail
-- entirely -- tenant lifecycle in this design is the `status` column, not
-- DELETE FROM tenants.
-- ============================================================================
CREATE TABLE IF NOT EXISTS queen_proxy.operations (
    id         UUID PRIMARY KEY DEFAULT public.gen_random_uuid(),
    tenant_id  UUID NOT NULL REFERENCES queen_proxy.tenants(id),
    cluster_id UUID REFERENCES queen_proxy.clusters(id) ON DELETE SET NULL,
    actor      TEXT NOT NULL CHECK (actor IN ('user', 'api_key', 'control_plane', 'system')),
    actor_id   UUID,
    action     TEXT NOT NULL CHECK (btrim(action) <> ''),
    target     TEXT,
    meta       JSONB NOT NULL DEFAULT '{}'::jsonb,
    at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_operations_tenant_at ON queen_proxy.operations(tenant_id, at DESC);
CREATE INDEX IF NOT EXISTS idx_operations_cluster_at
    ON queen_proxy.operations(cluster_id, at DESC) WHERE cluster_id IS NOT NULL;

-- ============================================================================
-- revoked_tokens -- JWT jti deny-list (ported from rev 2.3, CONTRACTS.md
-- auth section). `jti` is TEXT rather than UUID: it is minted by whichever
-- JWT library Agent C settles on, and while this crate's own uuid v4/v7
-- would produce UUID-shaped jtis, the deny-list itself has no reason to
-- reject a non-UUID jti from, say, a library default -- TEXT is the more
-- liberal, equally-indexable choice.
-- ============================================================================
CREATE TABLE IF NOT EXISTS queen_proxy.revoked_tokens (
    jti        TEXT PRIMARY KEY,
    expires_at TIMESTAMPTZ NOT NULL
);

-- For the (not-yet-written) periodic sweep of rows past their own token's
-- expiry -- once expired, the JWT itself is already invalid so the deny-list
-- row is dead weight.
CREATE INDEX IF NOT EXISTS idx_revoked_tokens_expires ON queen_proxy.revoked_tokens(expires_at);

-- ============================================================================
-- outbox -- control-plane-bound events (thresholds, limit hits, signups;
-- PLAN §2). The proxy only ever inserts; a CP-side worker drains it.
-- ============================================================================
CREATE TABLE IF NOT EXISTS queen_proxy.outbox (
    id          UUID PRIMARY KEY DEFAULT public.gen_random_uuid(),
    kind        TEXT NOT NULL CHECK (btrim(kind) <> ''),
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    consumed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_unconsumed ON queen_proxy.outbox(created_at) WHERE consumed_at IS NULL;
