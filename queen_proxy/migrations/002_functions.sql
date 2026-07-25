-- queen_proxy control-plane SQL functions -- PLAN_QUEEN_PROXY_CLOUD.md §2
-- (rev 1.2 "CP writes ONLY via queen_proxy.* SQL functions") and
-- CONTRACTS.md's control-plane contract list. Applied as migration
-- "002_functions" after 001_init.
--
-- Discipline every mutating function below follows: validate input, write
-- its own table(s), then call queen_proxy.record_operation(...) exactly
-- once -- which appends the append-only `operations` audit row AND (when
-- given a non-null cluster_id) does `PERFORM pg_notify('queen_proxy_inval',
-- cluster_id::text)`. This makes record_operation() the single place that
-- implements "append operations row + notify", per CONTRACTS.md, rather
-- than duplicating that pair in all nine functions.
--
-- `actor` is hardcoded to 'control_plane' in the eight higher-level
-- functions (create_tenant, create_user, ...) because their signatures are
-- fixed by CONTRACTS.md/PLAN §2 with no actor parameter -- these are framed
-- throughout the plan as the control plane's write path ("console -- writes
-- ONLY via queen_proxy.* SQL functions --> pxs"). record_operation() itself
-- takes an explicit p_actor/p_actor_id and is the one meant for callers that
-- aren't the control plane (e.g. a future self-serve admin API acting as
-- 'user'). See report for this being a deliberate reading of an
-- underspecified point, not an oversight.

-- ----------------------------------------------------------------------------
-- record_operation: the single append+notify primitive. Every other mutating
-- function below calls this instead of inserting into `operations` directly.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen_proxy.record_operation(
    p_tenant_id  UUID,
    p_cluster_id UUID,
    p_actor      TEXT,
    p_actor_id   UUID,
    p_action     TEXT,
    p_target     TEXT,
    p_meta       JSONB
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
    v_id UUID;
BEGIN
    IF p_tenant_id IS NULL THEN
        RAISE EXCEPTION 'record_operation: tenant_id is required';
    END IF;
    IF p_actor IS NULL OR p_actor NOT IN ('user', 'api_key', 'control_plane', 'system') THEN
        RAISE EXCEPTION 'record_operation: invalid actor %', p_actor;
    END IF;
    IF p_action IS NULL OR btrim(p_action) = '' THEN
        RAISE EXCEPTION 'record_operation: action must not be empty';
    END IF;

    INSERT INTO queen_proxy.operations(tenant_id, cluster_id, actor, actor_id, action, target, meta)
    VALUES (p_tenant_id, p_cluster_id, p_actor, p_actor_id, p_action, p_target, COALESCE(p_meta, '{}'::jsonb))
    RETURNING id INTO v_id;

    IF p_cluster_id IS NOT NULL THEN
        PERFORM pg_notify('queen_proxy_inval', p_cluster_id::text);
    END IF;

    RETURN v_id;
END;
$$;

-- ----------------------------------------------------------------------------
-- create_tenant(slug, name) -> tenant id
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen_proxy.create_tenant(
    p_slug TEXT,
    p_name TEXT
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
    v_id UUID;
BEGIN
    IF p_slug IS NULL OR btrim(p_slug) = '' THEN
        RAISE EXCEPTION 'create_tenant: slug must not be empty';
    END IF;
    IF p_name IS NULL OR btrim(p_name) = '' THEN
        RAISE EXCEPTION 'create_tenant: name must not be empty';
    END IF;

    INSERT INTO queen_proxy.tenants(slug, name)
    VALUES (lower(btrim(p_slug)), btrim(p_name))
    RETURNING id INTO v_id;

    -- No cluster exists yet for a brand new tenant -- nothing to invalidate.
    PERFORM queen_proxy.record_operation(
        v_id, NULL, 'control_plane', NULL, 'tenant_created', v_id::text,
        jsonb_build_object('slug', lower(btrim(p_slug)), 'name', btrim(p_name))
    );

    RETURN v_id;
END;
$$;

-- ----------------------------------------------------------------------------
-- create_user(tenant, email, password_hash, provider) -> user id
--
-- Only writes `users`. Does NOT populate `identities` -- provider_id/verified
-- aren't in this signature (see 001_init's comment on the identities table
-- and the report). password_hash may be NULL (OAuth-first users).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen_proxy.create_user(
    p_tenant_id     UUID,
    p_email         TEXT,
    p_password_hash TEXT,
    p_provider      TEXT
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
    v_id UUID;
BEGIN
    IF p_email IS NULL OR position('@' IN p_email) = 0 THEN
        RAISE EXCEPTION 'create_user: invalid email %', p_email;
    END IF;
    IF p_provider IS NULL OR p_provider NOT IN ('local', 'google', 'github') THEN
        RAISE EXCEPTION 'create_user: invalid provider %', p_provider;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM queen_proxy.tenants WHERE id = p_tenant_id) THEN
        RAISE EXCEPTION 'create_user: unknown tenant %', p_tenant_id;
    END IF;

    INSERT INTO queen_proxy.users(tenant_id, email, password_hash)
    VALUES (p_tenant_id, lower(btrim(p_email)), p_password_hash)
    RETURNING id INTO v_id;

    PERFORM queen_proxy.record_operation(
        p_tenant_id, NULL, 'control_plane', v_id, 'user_created', v_id::text,
        jsonb_build_object('email', lower(btrim(p_email)), 'provider', p_provider)
    );

    RETURN v_id;
END;
$$;

-- ----------------------------------------------------------------------------
-- create_cluster(tenant_id, slug, plan_code, cell_id) -> cluster id
-- broker_tenant_uuid is assigned by the column default (public.gen_random_uuid()).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen_proxy.create_cluster(
    p_tenant_id UUID,
    p_slug      TEXT,
    p_plan_code TEXT,
    p_cell_id   UUID
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
    v_id      UUID;
    v_plan_id UUID;
BEGIN
    IF p_slug IS NULL OR btrim(p_slug) = '' THEN
        RAISE EXCEPTION 'create_cluster: slug must not be empty';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM queen_proxy.tenants WHERE id = p_tenant_id) THEN
        RAISE EXCEPTION 'create_cluster: unknown tenant %', p_tenant_id;
    END IF;
    SELECT id INTO v_plan_id FROM queen_proxy.plans WHERE code = p_plan_code;
    IF v_plan_id IS NULL THEN
        RAISE EXCEPTION 'create_cluster: unknown plan code %', p_plan_code;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM queen_proxy.cells WHERE id = p_cell_id) THEN
        RAISE EXCEPTION 'create_cluster: unknown cell %', p_cell_id;
    END IF;

    INSERT INTO queen_proxy.clusters(tenant_id, cell_id, plan_id, slug)
    VALUES (p_tenant_id, p_cell_id, v_plan_id, lower(btrim(p_slug)))
    RETURNING id INTO v_id;

    -- Always notify: even on first creation this primes any proxy that had
    -- (somehow) cached a miss for this slug, and it's the same call path
    -- every other mutator uses -- no special-casing "nothing to invalidate
    -- yet".
    PERFORM queen_proxy.record_operation(
        p_tenant_id, v_id, 'control_plane', NULL, 'cluster_created', v_id::text,
        jsonb_build_object('slug', lower(btrim(p_slug)), 'plan_code', p_plan_code, 'cell_id', p_cell_id)
    );

    RETURN v_id;
END;
$$;

-- ----------------------------------------------------------------------------
-- assign_plan(cluster_id, plan_code)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen_proxy.assign_plan(
    p_cluster_id UUID,
    p_plan_code  TEXT
) RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_plan_id   UUID;
    v_tenant_id UUID;
BEGIN
    SELECT id INTO v_plan_id FROM queen_proxy.plans WHERE code = p_plan_code;
    IF v_plan_id IS NULL THEN
        RAISE EXCEPTION 'assign_plan: unknown plan code %', p_plan_code;
    END IF;
    SELECT tenant_id INTO v_tenant_id FROM queen_proxy.clusters WHERE id = p_cluster_id;
    IF v_tenant_id IS NULL THEN
        RAISE EXCEPTION 'assign_plan: unknown cluster %', p_cluster_id;
    END IF;

    UPDATE queen_proxy.clusters SET plan_id = v_plan_id WHERE id = p_cluster_id;

    PERFORM queen_proxy.record_operation(
        v_tenant_id, p_cluster_id, 'control_plane', NULL, 'plan_assigned', p_cluster_id::text,
        jsonb_build_object('plan_code', p_plan_code)
    );
END;
$$;

-- ----------------------------------------------------------------------------
-- set_tenant_status(tenant_id, status)
--
-- Fans out one pg_notify per cluster owned by the tenant: cache.rs merges
-- tenant.status + cluster.status ("worst wins") into every cached
-- ClusterCtx, so a tenant-level status change can change the EFFECTIVE
-- status of N clusters at once and each of their cache entries needs
-- invalidating -- not just a single cluster_id the way every other function
-- here notifies.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen_proxy.set_tenant_status(
    p_tenant_id UUID,
    p_status    TEXT
) RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    r RECORD;
BEGIN
    IF p_status IS NULL OR p_status NOT IN ('active', 'grace', 'suspended', 'deleting') THEN
        RAISE EXCEPTION 'set_tenant_status: invalid status %', p_status;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM queen_proxy.tenants WHERE id = p_tenant_id) THEN
        RAISE EXCEPTION 'set_tenant_status: unknown tenant %', p_tenant_id;
    END IF;

    UPDATE queen_proxy.tenants SET status = p_status WHERE id = p_tenant_id;

    -- tenant-level audit row (cluster_id NULL -> record_operation won't
    -- itself notify anything, hence the explicit per-cluster loop below).
    PERFORM queen_proxy.record_operation(
        p_tenant_id, NULL, 'control_plane', NULL, 'tenant_status_changed', p_tenant_id::text,
        jsonb_build_object('status', p_status)
    );

    FOR r IN SELECT id FROM queen_proxy.clusters WHERE tenant_id = p_tenant_id LOOP
        PERFORM pg_notify('queen_proxy_inval', r.id::text);
    END LOOP;
END;
$$;

-- ----------------------------------------------------------------------------
-- set_cluster_status(cluster_id, status)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen_proxy.set_cluster_status(
    p_cluster_id UUID,
    p_status     TEXT
) RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_tenant_id UUID;
BEGIN
    IF p_status IS NULL OR p_status NOT IN ('active', 'push_blocked', 'suspended', 'deleting') THEN
        RAISE EXCEPTION 'set_cluster_status: invalid status %', p_status;
    END IF;
    SELECT tenant_id INTO v_tenant_id FROM queen_proxy.clusters WHERE id = p_cluster_id;
    IF v_tenant_id IS NULL THEN
        RAISE EXCEPTION 'set_cluster_status: unknown cluster %', p_cluster_id;
    END IF;

    UPDATE queen_proxy.clusters SET status = p_status WHERE id = p_cluster_id;

    PERFORM queen_proxy.record_operation(
        v_tenant_id, p_cluster_id, 'control_plane', NULL, 'cluster_status_changed', p_cluster_id::text,
        jsonb_build_object('status', p_status)
    );
END;
$$;

-- ----------------------------------------------------------------------------
-- issue_api_key(cluster_id, name, key_hash, scopes) -> key id
-- key_hash must already be the sha256 hex of the key (auth::key_hash_hex);
-- this function never sees the plaintext key.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen_proxy.issue_api_key(
    p_cluster_id UUID,
    p_name       TEXT,
    p_key_hash   TEXT,
    p_scopes     TEXT[]
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
    v_id        UUID;
    v_tenant_id UUID;
    v_scope     TEXT;
BEGIN
    SELECT tenant_id INTO v_tenant_id FROM queen_proxy.clusters WHERE id = p_cluster_id;
    IF v_tenant_id IS NULL THEN
        RAISE EXCEPTION 'issue_api_key: unknown cluster %', p_cluster_id;
    END IF;
    IF p_name IS NULL OR btrim(p_name) = '' THEN
        RAISE EXCEPTION 'issue_api_key: name must not be empty';
    END IF;
    IF p_key_hash IS NULL OR p_key_hash !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'issue_api_key: key_hash must be 64 lowercase hex chars (sha256)';
    END IF;
    IF p_scopes IS NULL OR array_length(p_scopes, 1) IS NULL THEN
        RAISE EXCEPTION 'issue_api_key: at least one scope is required';
    END IF;
    FOREACH v_scope IN ARRAY p_scopes LOOP
        IF v_scope NOT IN ('produce', 'consume', 'admin', 'read') THEN
            RAISE EXCEPTION 'issue_api_key: invalid scope %', v_scope;
        END IF;
    END LOOP;

    INSERT INTO queen_proxy.api_keys(cluster_id, name, key_hash, scopes)
    VALUES (p_cluster_id, btrim(p_name), p_key_hash, p_scopes)
    RETURNING id INTO v_id;

    PERFORM queen_proxy.record_operation(
        v_tenant_id, p_cluster_id, 'control_plane', NULL, 'api_key_issued', v_id::text,
        jsonb_build_object('name', btrim(p_name), 'scopes', to_jsonb(p_scopes))
    );

    RETURN v_id;
END;
$$;

-- ----------------------------------------------------------------------------
-- revoke_api_key(key_id)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen_proxy.revoke_api_key(
    p_key_id UUID
) RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_cluster_id UUID;
    v_tenant_id  UUID;
BEGIN
    SELECT ak.cluster_id, c.tenant_id INTO v_cluster_id, v_tenant_id
    FROM queen_proxy.api_keys ak
    JOIN queen_proxy.clusters c ON c.id = ak.cluster_id
    WHERE ak.id = p_key_id AND ak.revoked_at IS NULL
    FOR UPDATE OF ak;

    IF v_cluster_id IS NULL THEN
        RAISE EXCEPTION 'revoke_api_key: unknown or already-revoked key %', p_key_id;
    END IF;

    UPDATE queen_proxy.api_keys SET revoked_at = now() WHERE id = p_key_id;

    PERFORM queen_proxy.record_operation(
        v_tenant_id, v_cluster_id, 'control_plane', NULL, 'api_key_revoked', p_key_id::text, '{}'::jsonb
    );
END;
$$;

-- ============================================================================
-- Seed plans. Free's numbers are exactly the brief's; dev/pro/dedicated-s
-- widen from there (brief: "limiti più larghi, features streams+traces",
-- exact values left to this migration -- "valori ragionevoli, documentali in
-- commento", so the reasoning for each is spelled out below). All byte
-- figures are binary (KiB/MiB/GiB = 1024^n), matching config.rs's existing
-- convention (e.g. QUEEN_PROXY_MAX_BODY_BYTES default 16*1024*1024).
-- monthly_msgs_quota is left NULL (unlimited) on every seeded plan -- no
-- monthly figure was briefed for v1.
--
-- free: shared cell, the brief's exact numbers verbatim. No gated features.
--   5 req/s (burst 25), 20 msg/s (burst 100), 20 queues, 8 partitions/queue,
--   50 parked pops, 256 KiB payload, 1000 batch items, 1 GiB retained,
--   7-day retention ceiling.
--
-- dev: shared cell, a notch above free for local/staging use -- NOT a step
--   toward paid features (still features={}); it exists so a developer's
--   test cluster doesn't fight the free tier's abuse-oriented caps while
--   staying on the same cheap shared infra. ~2x free's rates, more headroom
--   on queues/parked/retained since dev/test workloads fan out more queues
--   than they push messages.
--
-- pro: shared cell, the top shared-tier plan -- meaningfully higher
--   throughput and the first plan with streams+traces unlocked. Still
--   shared-cell (not every paying customer needs a dedicated box); the
--   PLAN §4 free-cell ceiling (~480 msg/s commit-bound, "sustained <= ~60-
--   70% of cell ceiling") caps how far a SHARED plan's msg/s can reasonably
--   go, so pro's sustained rate (200/s) leaves headroom for multiple pro
--   tenants to share a cell alongside free/dev traffic.
--
-- dedicated-s: dedicated cell (own broker+PG), so it isn't bound by shared-
--   cell fairness -- sized off PLAN §4's measured "~500-640 msg/s per PG
--   core on dedicated" for a small single-ish-core dedicated box, with
--   generous queue/partition/retention room since nothing else competes for
--   the cell's resources. features streams+traces on, as briefed.
-- ============================================================================
INSERT INTO queen_proxy.plans (
    code, cell_class,
    max_req_per_sec, req_burst, max_msgs_per_sec, msgs_burst,
    max_queues, max_partitions_per_queue, max_parked_pops,
    max_payload_bytes, max_batch_items, max_retained_bytes, max_retention_seconds,
    features
) VALUES
    ('free', 'shared',
     5, 25, 20, 100,
     20, 8, 50,
     256 * 1024, 1000, 1024::bigint * 1024 * 1024, 7 * 86400,
     '{}'::jsonb),
    ('dev', 'shared',
     10, 50, 40, 200,
     50, 16, 150,
     512 * 1024, 2000, 3::bigint * 1024 * 1024 * 1024, 14 * 86400,
     '{}'::jsonb),
    ('pro', 'shared',
     50, 200, 200, 800,
     200, 32, 1000,
     1024 * 1024, 5000, 20::bigint * 1024 * 1024 * 1024, 30 * 86400,
     jsonb_build_object('streams', true, 'traces', true)),
    ('dedicated-s', 'dedicated',
     200, 800, 600, 2000,
     1000, 64, 5000,
     4 * 1024 * 1024, 10000, 200::bigint * 1024 * 1024 * 1024, 90 * 86400,
     jsonb_build_object('streams', true, 'traces', true))
ON CONFLICT (code) DO NOTHING;
