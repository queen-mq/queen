-- queen_proxy lifecycle SQL: the writers 001/002 left unbuilt -- session
-- revocation, cluster-role management, one-call tenant onboarding, the daily
-- usage rollup and the outbox producer. Applied as migration "004_lifecycle"
-- after 003_limit_override.
--
-- Same discipline as 002_functions: validate input, write, then call
-- queen_proxy.record_operation(...) exactly once -- which appends the
-- append-only `operations` row AND (non-null cluster_id) does the
-- pg_notify('queen_proxy_inval', cluster_id) the cell proxies listen on.
-- record_operation stays the single notify emitter; nothing here hand-rolls
-- one.
--
-- pgcrypto is guaranteed by 001_init.sql ("CREATE EXTENSION IF NOT EXISTS
-- pgcrypto WITH SCHEMA public"), so gen_random_bytes/digest/crypt are
-- available here without creating anything -- and, per 001's header, every
-- call site stays explicitly `public.`-qualified rather than trusting
-- search_path.
--
-- Every statement is idempotent (IF NOT EXISTS / OR REPLACE): re-applying the
-- whole file is a no-op.

-- ============================================================================
-- SESSION REVOCATION
--
-- queen_proxy.revoked_tokens (001_init) is read on every JWT verify
-- (auth::is_revoked) but had no writer, so a session could not be ended
-- before its own exp. These two are that writer plus the sweep 001's index
-- comment anticipated.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- revoke_session(jti, expires_at, actor, actor_id)
--
-- p_expires_at is the revoked token's OWN exp: once past it the JWT is
-- invalid on its own and sweep_revoked_tokens can drop the row.
--
-- p_actor_id must identify a known user -- operations.tenant_id is NOT NULL
-- and this signature carries no tenant, so the audit row's tenant is resolved
-- through the user. Every caller has it: it is the session's `sub` claim.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen_proxy.revoke_session(
    p_jti        TEXT,
    p_expires_at TIMESTAMPTZ,
    p_actor      TEXT DEFAULT 'user',
    p_actor_id   UUID DEFAULT NULL
) RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_tenant_id UUID;
BEGIN
    IF p_jti IS NULL OR btrim(p_jti) = '' THEN
        RAISE EXCEPTION 'revoke_session: jti must not be empty';
    END IF;
    IF p_expires_at IS NULL THEN
        RAISE EXCEPTION 'revoke_session: expires_at is required';
    END IF;
    SELECT tenant_id INTO v_tenant_id FROM queen_proxy.users WHERE id = p_actor_id;
    IF v_tenant_id IS NULL THEN
        RAISE EXCEPTION 'revoke_session: actor_id must be a known user %', p_actor_id;
    END IF;

    -- A jti is minted once per token, so a conflict means the same session was
    -- revoked twice (double logout) -- not an error, and the stored expiry is
    -- already the right one.
    INSERT INTO queen_proxy.revoked_tokens(jti, expires_at)
    VALUES (btrim(p_jti), p_expires_at)
    ON CONFLICT (jti) DO NOTHING;

    -- Sessions are not cluster-scoped -> no cluster_id, hence no notify: the
    -- deny-list is read straight from pxdb (auth::is_revoked, 60 s TTL), not
    -- from the ClusterCtx cache the inval channel refreshes.
    PERFORM queen_proxy.record_operation(
        v_tenant_id, NULL, p_actor, p_actor_id, 'session_revoked', btrim(p_jti),
        jsonb_build_object('expires_at', p_expires_at)
    );
END;
$$;

-- ----------------------------------------------------------------------------
-- sweep_revoked_tokens() -> rows deleted
--
-- Maintenance, not a tenant action: no operations row (operations.tenant_id is
-- NOT NULL and a sweep belongs to no single tenant) and nothing to invalidate
-- -- an expired deny-list row can no longer change any verify's outcome.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen_proxy.sweep_revoked_tokens()
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted INTEGER;
BEGIN
    DELETE FROM queen_proxy.revoked_tokens WHERE expires_at <= now();
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RETURN v_deleted;
END;
$$;

-- ============================================================================
-- CLUSTER ROLE MANAGEMENT
--
-- queen_proxy.cluster_roles (001_init) gates every console endpoint but had no
-- runtime writer: a second human on a cluster, or an OAuth-provisioned user,
-- needed a hand-written INSERT or got a 403 on everything.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- grant_cluster_role(cluster, email, role)
--
-- The user and the cluster MUST belong to the same tenant. This is a security
-- boundary, not a convenience check: cluster_roles is what authorizes console
-- access, so a cross-tenant row would hand one tenant's user a seat on another
-- tenant's cluster.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen_proxy.grant_cluster_role(
    p_cluster UUID,
    p_email   TEXT,
    p_role    TEXT
) RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_tenant_id      UUID;
    v_user_id        UUID;
    v_user_tenant_id UUID;
    v_email          TEXT;
BEGIN
    IF p_role IS NULL OR p_role NOT IN ('admin', 'producer', 'consumer', 'viewer') THEN
        RAISE EXCEPTION 'grant_cluster_role: invalid role %', p_role;
    END IF;
    IF p_email IS NULL OR position('@' IN p_email) = 0 THEN
        RAISE EXCEPTION 'grant_cluster_role: invalid email %', p_email;
    END IF;
    v_email := lower(btrim(p_email));

    SELECT tenant_id INTO v_tenant_id FROM queen_proxy.clusters WHERE id = p_cluster;
    IF v_tenant_id IS NULL THEN
        RAISE EXCEPTION 'grant_cluster_role: unknown cluster %', p_cluster;
    END IF;
    SELECT id, tenant_id INTO v_user_id, v_user_tenant_id
    FROM queen_proxy.users WHERE email = v_email;
    IF v_user_id IS NULL THEN
        RAISE EXCEPTION 'grant_cluster_role: unknown user %', v_email;
    END IF;
    IF v_user_tenant_id <> v_tenant_id THEN
        RAISE EXCEPTION 'grant_cluster_role: user % belongs to tenant %, cluster % to tenant %',
            v_email, v_user_tenant_id, p_cluster, v_tenant_id;
    END IF;

    INSERT INTO queen_proxy.cluster_roles(user_id, cluster_id, role)
    VALUES (v_user_id, p_cluster, p_role)
    ON CONFLICT (user_id, cluster_id) DO UPDATE SET role = EXCLUDED.role;

    PERFORM queen_proxy.record_operation(
        v_tenant_id, p_cluster, 'control_plane', v_user_id,
        'cluster_role_granted', p_cluster::text,
        jsonb_build_object('email', v_email, 'role', p_role)
    );
END;
$$;

-- ----------------------------------------------------------------------------
-- revoke_cluster_role(cluster, email)
--
-- Raises when there is no grant to remove, matching revoke_api_key's "unknown
-- or already-revoked" stance: silently succeeding would let an operator
-- believe access was withdrawn when the email was simply mistyped.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen_proxy.revoke_cluster_role(
    p_cluster UUID,
    p_email   TEXT
) RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_tenant_id UUID;
    v_user_id   UUID;
    v_role      TEXT;
    v_email     TEXT;
BEGIN
    IF p_email IS NULL OR position('@' IN p_email) = 0 THEN
        RAISE EXCEPTION 'revoke_cluster_role: invalid email %', p_email;
    END IF;
    v_email := lower(btrim(p_email));

    SELECT tenant_id INTO v_tenant_id FROM queen_proxy.clusters WHERE id = p_cluster;
    IF v_tenant_id IS NULL THEN
        RAISE EXCEPTION 'revoke_cluster_role: unknown cluster %', p_cluster;
    END IF;
    SELECT id INTO v_user_id FROM queen_proxy.users WHERE email = v_email;
    IF v_user_id IS NULL THEN
        RAISE EXCEPTION 'revoke_cluster_role: unknown user %', v_email;
    END IF;

    DELETE FROM queen_proxy.cluster_roles
     WHERE user_id = v_user_id AND cluster_id = p_cluster
    RETURNING role INTO v_role;
    IF v_role IS NULL THEN
        RAISE EXCEPTION 'revoke_cluster_role: user % has no role on cluster %', v_email, p_cluster;
    END IF;

    PERFORM queen_proxy.record_operation(
        v_tenant_id, p_cluster, 'control_plane', v_user_id,
        'cluster_role_revoked', p_cluster::text,
        jsonb_build_object('email', v_email, 'role', v_role)
    );
END;
$$;

-- ============================================================================
-- OUTBOX
--
-- queen_proxy.outbox (001_init) is drained by a control-plane worker and had
-- no producer. PLAN §2 scopes it to CP-bound events (thresholds, limit hits,
-- signups) -- kinds are coined by their emitters, so this stays a thin insert
-- rather than a taxonomy.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen_proxy.emit_outbox(
    p_kind    TEXT,
    p_payload JSONB
) RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
    v_id UUID;
BEGIN
    IF p_kind IS NULL OR btrim(p_kind) = '' THEN
        RAISE EXCEPTION 'emit_outbox: kind must not be empty';
    END IF;

    INSERT INTO queen_proxy.outbox(kind, payload)
    VALUES (btrim(p_kind), COALESCE(p_payload, '{}'::jsonb))
    RETURNING id INTO v_id;

    RETURN v_id;
END;
$$;

-- ============================================================================
-- TENANT ONBOARDING
-- ============================================================================

-- ----------------------------------------------------------------------------
-- bootstrap_tenant(...) -> {tenant_id, cluster_id, user_id, api_key}
--
-- Composes the existing 002 functions (create_tenant, create_cluster,
-- create_user, issue_api_key) plus grant_cluster_role, so onboarding a
-- customer is one call instead of five manual steps. A plpgsql function runs
-- inside the caller's transaction, so any RAISE below unwinds the whole
-- bootstrap -- no half-created tenant.
--
-- api_key in the result is the PLAINTEXT key, shown exactly once: only its
-- sha256 hex is stored (api_keys.key_hash), so it cannot be recovered later.
-- The `<env>` segment of `qk_<env>_<43>` is informational -- auth::authenticate
-- routes on the `qk_` prefix and matches on the hash alone.
--
-- p_password NULL creates the admin password-less (OAuth-only, as create_user
-- supports); otherwise it is bcrypt-hashed here with the same pgcrypto call
-- scripts/seed-dev.sql uses, whose $2a$ output the bcrypt crate in
-- oauth::verify_local accepts.
--
-- Idempotent on the slugs: a re-run returns the existing ids instead of
-- raising, with api_key NULL (the plaintext is unrecoverable). It still
-- re-asserts the admin role -- that grant is an upsert.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen_proxy.bootstrap_tenant(
    p_tenant_slug  TEXT,
    p_tenant_name  TEXT,
    p_cluster_slug TEXT,
    p_plan_code    TEXT,
    p_cell         UUID,
    p_admin_email  TEXT,
    p_password     TEXT DEFAULT NULL,
    p_key_name     TEXT DEFAULT 'default'
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_tenant_slug   TEXT;
    v_cluster_slug  TEXT;
    v_email         TEXT;
    v_key_name      TEXT;
    v_tenant_id     UUID;
    v_cluster_id    UUID;
    v_user_id       UUID;
    v_owner         UUID;
    v_new_tenant    BOOLEAN := false;
    v_api_key       TEXT;
BEGIN
    IF p_tenant_slug IS NULL OR btrim(p_tenant_slug) = '' THEN
        RAISE EXCEPTION 'bootstrap_tenant: tenant_slug must not be empty';
    END IF;
    IF p_cluster_slug IS NULL OR btrim(p_cluster_slug) = '' THEN
        RAISE EXCEPTION 'bootstrap_tenant: cluster_slug must not be empty';
    END IF;
    IF p_admin_email IS NULL OR position('@' IN p_admin_email) = 0 THEN
        RAISE EXCEPTION 'bootstrap_tenant: invalid admin_email %', p_admin_email;
    END IF;
    IF p_cell IS NULL THEN
        RAISE EXCEPTION 'bootstrap_tenant: cell is required';
    END IF;
    IF p_key_name IS NULL OR btrim(p_key_name) = '' THEN
        RAISE EXCEPTION 'bootstrap_tenant: key_name must not be empty';
    END IF;
    v_tenant_slug  := lower(btrim(p_tenant_slug));
    v_cluster_slug := lower(btrim(p_cluster_slug));
    v_email        := lower(btrim(p_admin_email));
    v_key_name     := btrim(p_key_name);

    -- tenant --------------------------------------------------------------
    SELECT id INTO v_tenant_id FROM queen_proxy.tenants WHERE slug = v_tenant_slug;
    IF v_tenant_id IS NULL THEN
        v_tenant_id := queen_proxy.create_tenant(v_tenant_slug, COALESCE(btrim(p_tenant_name), v_tenant_slug));
        v_new_tenant := true;
    END IF;

    -- cluster (plan + cell are validated by create_cluster) ----------------
    SELECT id, tenant_id INTO v_cluster_id, v_owner
    FROM queen_proxy.clusters WHERE slug = v_cluster_slug;
    IF v_cluster_id IS NULL THEN
        v_cluster_id := queen_proxy.create_cluster(v_tenant_id, v_cluster_slug, p_plan_code, p_cell);
    ELSIF v_owner <> v_tenant_id THEN
        -- clusters.slug is globally unique (it is the subdomain): re-running
        -- someone else's slug is a collision, not an idempotent repeat.
        RAISE EXCEPTION 'bootstrap_tenant: cluster slug % already belongs to tenant %', v_cluster_slug, v_owner;
    END IF;

    -- admin user ----------------------------------------------------------
    SELECT id, tenant_id INTO v_user_id, v_owner
    FROM queen_proxy.users WHERE email = v_email;
    IF v_user_id IS NULL THEN
        v_user_id := queen_proxy.create_user(
            v_tenant_id, v_email,
            CASE WHEN p_password IS NULL THEN NULL
                 ELSE public.crypt(p_password, public.gen_salt('bf', 10)) END,
            'local'
        );
    ELSIF v_owner <> v_tenant_id THEN
        -- users.email is globally unique, so an existing address outside this
        -- tenant cannot be adopted into it.
        RAISE EXCEPTION 'bootstrap_tenant: user % already belongs to tenant %', v_email, v_owner;
    END IF;

    PERFORM queen_proxy.grant_cluster_role(v_cluster_id, v_email, 'admin');

    -- api key: full scopes, the operator's first credential on the cluster.
    -- Keyed off the name so a re-run neither duplicates nor pretends to
    -- return a key it can no longer produce.
    IF NOT EXISTS (
        SELECT 1 FROM queen_proxy.api_keys
        WHERE cluster_id = v_cluster_id AND name = v_key_name AND revoked_at IS NULL
    ) THEN
        -- `qk_<env>_<43 base64url chars>` from 32 CSPRNG bytes, exactly
        -- auth::generate_api_key's shape: base64 of 32 bytes is 44 chars, so
        -- no line wrap to strip, and translate maps +/ -> -_ while dropping
        -- the single '=' pad (and any wrap, defensively).
        v_api_key := 'qk_live_' || translate(
            encode(public.gen_random_bytes(32), 'base64'), '+/=' || E'\n', '-_'
        );
        PERFORM queen_proxy.issue_api_key(
            v_cluster_id, v_key_name,
            encode(public.digest(v_api_key, 'sha256'), 'hex'),
            ARRAY['produce', 'consume', 'admin', 'read']
        );
    END IF;

    PERFORM queen_proxy.record_operation(
        v_tenant_id, v_cluster_id, 'control_plane', v_user_id,
        'tenant_bootstrapped', v_tenant_id::text,
        jsonb_build_object(
            'tenant_slug', v_tenant_slug, 'cluster_slug', v_cluster_slug,
            'plan_code', p_plan_code, 'admin_email', v_email,
            'key_issued', v_api_key IS NOT NULL
        )
    );

    -- Signup event (PLAN §2/§6) -- only on the run that actually created the
    -- tenant, so a repeated bootstrap does not re-announce a customer.
    IF v_new_tenant THEN
        PERFORM queen_proxy.emit_outbox(
            'tenant_bootstrapped',
            jsonb_build_object(
                'tenant_id', v_tenant_id, 'tenant_slug', v_tenant_slug,
                'cluster_id', v_cluster_id, 'cluster_slug', v_cluster_slug,
                'plan_code', p_plan_code, 'admin_email', v_email
            )
        );
    END IF;

    RETURN jsonb_build_object(
        'tenant_id', v_tenant_id,
        'cluster_id', v_cluster_id,
        'user_id', v_user_id,
        'api_key', v_api_key
    );
END;
$$;

-- ============================================================================
-- usage_days -- the daily rollup 001_init deferred ("no consumer for it yet");
-- monthly quota enforcement (plans.monthly_msgs_quota, PLAN §6.7) and billing
-- export are that consumer. Same shape as usage_minutes with `day` in place of
-- `minute`.
--
-- `day` is the UTC calendar day of usage_minutes.minute -- pinned to UTC
-- rather than the server's TimeZone so a rollup produces the same rows on any
-- instance, and so month boundaries in cluster_month_msgs are stable.
-- ============================================================================
CREATE TABLE IF NOT EXISTS queen_proxy.usage_days (
    cluster_id UUID NOT NULL REFERENCES queen_proxy.clusters(id) ON DELETE CASCADE,
    day        DATE NOT NULL,
    op_class   TEXT NOT NULL CHECK (op_class IN ('push', 'delivery', 'txn', 'configure', 'read')),
    msgs       BIGINT NOT NULL DEFAULT 0 CHECK (msgs >= 0),
    reqs       BIGINT NOT NULL DEFAULT 0 CHECK (reqs >= 0),
    bytes_in   BIGINT NOT NULL DEFAULT 0 CHECK (bytes_in >= 0),
    bytes_out  BIGINT NOT NULL DEFAULT 0 CHECK (bytes_out >= 0),
    PRIMARY KEY (cluster_id, day, op_class)
);

-- Cross-cluster day scans for billing export.
CREATE INDEX IF NOT EXISTS idx_usage_days_day ON queen_proxy.usage_days(day);

-- ----------------------------------------------------------------------------
-- rollup_usage_days(p_through) -> rows written
--
-- Aggregates CLOSED days only: p_through is clamped to yesterday (UTC), since
-- today's minutes are still arriving. NULL p_through means "everything closed".
--
-- Recomputes each day from usage_minutes instead of adding to what is already
-- in usage_days: usage_minutes is the source of truth and rows can still land
-- late for a closed day when a proxy drains its disk spool, so a blind add
-- would double-count every earlier run. The pass covers every closed day still
-- present in usage_minutes -- late arrivals are corrected however long after
-- the fact they show up.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen_proxy.rollup_usage_days(
    p_through DATE DEFAULT NULL
) RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_last_closed DATE := ((now() AT TIME ZONE 'UTC')::date - 1);
    v_through     DATE;
    v_rows        INTEGER;
BEGIN
    v_through := LEAST(COALESCE(p_through, v_last_closed), v_last_closed);

    INSERT INTO queen_proxy.usage_days AS ud (cluster_id, day, op_class, msgs, reqs, bytes_in, bytes_out)
    SELECT um.cluster_id,
           (um.minute AT TIME ZONE 'UTC')::date,
           um.op_class,
           SUM(um.msgs), SUM(um.reqs), SUM(um.bytes_in), SUM(um.bytes_out)
      FROM queen_proxy.usage_minutes um
     WHERE um.minute < ((v_through + 1)::timestamp AT TIME ZONE 'UTC')
     GROUP BY 1, 2, 3
    ON CONFLICT (cluster_id, day, op_class) DO UPDATE
       SET msgs      = EXCLUDED.msgs,
           reqs      = EXCLUDED.reqs,
           bytes_in  = EXCLUDED.bytes_in,
           bytes_out = EXCLUDED.bytes_out;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows;
END;
$$;

-- ----------------------------------------------------------------------------
-- cluster_month_msgs(cluster, month) -> msgs
--
-- Monthly msgs for one cluster, for the calendar month (UTC) containing
-- p_month. Reads usage_days AND the usage_minutes remainder the rollup has not
-- folded in yet, so a quota check never under-counts today's traffic.
--
-- Per day it takes the larger of the two: usage_days is a plain SUM of
-- usage_minutes, so the figures differ only when a day is not yet rolled up
-- (live minutes are larger -- today, always) or when usage_minutes has been
-- pruned behind the rollup (the rolled figure is larger). GREATEST is right in
-- both directions and can never double-count a day.
--
-- Unknown cluster returns 0, like a cluster with no traffic: this is a read
-- helper on a path where the caller has already resolved the cluster.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen_proxy.cluster_month_msgs(
    p_cluster UUID,
    p_month   DATE
) RETURNS BIGINT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_start DATE;
    v_end   DATE;
    v_msgs  BIGINT;
BEGIN
    IF p_month IS NULL THEN
        RAISE EXCEPTION 'cluster_month_msgs: month is required';
    END IF;
    v_start := date_trunc('month', p_month::timestamp)::date;
    v_end   := (v_start + INTERVAL '1 month')::date;

    SELECT COALESCE(SUM(GREATEST(COALESCE(d.msgs, 0), COALESCE(m.msgs, 0))), 0)
      INTO v_msgs
      FROM (
            SELECT ud.day, SUM(ud.msgs) AS msgs
              FROM queen_proxy.usage_days ud
             WHERE ud.cluster_id = p_cluster
               AND ud.day >= v_start AND ud.day < v_end
             GROUP BY ud.day
           ) d
      FULL JOIN (
            SELECT (um.minute AT TIME ZONE 'UTC')::date AS day, SUM(um.msgs) AS msgs
              FROM queen_proxy.usage_minutes um
             WHERE um.cluster_id = p_cluster
               AND um.minute >= (v_start::timestamp AT TIME ZONE 'UTC')
               AND um.minute <  (v_end::timestamp AT TIME ZONE 'UTC')
             GROUP BY 1
           ) m USING (day);

    RETURN v_msgs;
END;
$$;
