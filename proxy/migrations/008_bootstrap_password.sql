-- bootstrap_tenant: say out loud when the admin it creates cannot sign in.
-- Applied as migration "008_bootstrap_password" after 007_tenant_delete.
--
-- ============================================================================
-- WHAT THIS FIXES
-- ============================================================================
--
-- `bootstrap_tenant(..., p_password => NULL)` is legal and useful: it is how you
-- onboard a tenant that will only ever use the API key the same call returns.
-- 004_lifecycle says so, and this migration does not change it.
--
-- What it changes is the SILENCE. Passing NULL writes `users.password_hash =
-- NULL` and writes no `identities` row either (create_user never does), so:
--
--   * `oauth::verify_local` reads the NULL hash, short-circuits to None, and
--     answers the same "Invalid email or password." it gives a typo'd password;
--   * OAuth can still adopt the account, but only if a provider is configured on
--     this cell AND it returns this exact address VERIFIED (oauth.rs's
--     `decide_resolution`: linked identity -> Login, else verified email match ->
--     Link, else Deny);
--   * so on a cell with no OAuth, the call creates a console admin that can
--     never log in, returns `{"api_key": "qk_live_..."}`, and says nothing.
--
-- Nobody finds out until a human tries to sign in — usually the customer, on
-- day one. Same failure class as a proxy that mints nothing and 500s every
-- login (spec §9b), and the same remedy: make the machine say it at the moment
-- it happens, not at first use.
--
-- Three things now say it, loudest first:
--   1. a RAISE WARNING, which psql prints and which tokio-postgres surfaces as
--      a connection NOTICE (the proxy's own pool logs those at INFO);
--   2. `password_set` and `can_login` in the returned JSONB, for a caller that
--      reads results rather than stderr — a control plane, a provisioning
--      script, `scripts/sdk-smoke/run.sh`;
--   3. the same two flags in the `tenant_bootstrapped` operations row, so the
--      fact outlives the session that created it.
--
-- NOT an exception. Passwordless-with-an-API-key is a real shape and raising
-- would break it, and every existing caller, for a condition that is a warning.
--
-- ============================================================================
-- WHY THE WHOLE FUNCTION IS RESTATED
-- ============================================================================
--
-- The proxy's migration runner (proxy/src/db.rs) applies each file ONCE and
-- records its name; editing an already-applied file has no effect, ever, on any
-- database that has already run it. So a change to a function shipped in 004
-- can only arrive as a new numbered file that re-declares it in full. This copy
-- supersedes the one in 004_lifecycle.sql -- 004 is left exactly as it was, as
-- the historical record of what that migration did.
--
-- THE NEXT EDIT TO bootstrap_tenant GOES IN A NEW FILE TOO, and starts from
-- THIS copy, not 004's.
--
-- Differences from the 004 body, and there are no others:
--   * v_password_set / v_can_login are computed;
--   * the RAISE WARNING;
--   * two extra keys in the audit meta, two in the returned JSONB.
--
-- Every statement is idempotent (OR REPLACE): re-applying the file is a no-op.
-- SQL is include_str!-embedded -- `cargo build` after editing, or you test the
-- old text.
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
    -- Did THIS call set a password? (A re-run against an existing user does
    -- not touch the hash, so this is not the same question as can_login.)
    v_password_set  BOOLEAN := (p_password IS NOT NULL);
    -- Can the admin authenticate AT ALL afterwards: a local password, or a
    -- linked OAuth identity. Read back from the tables rather than inferred
    -- from the argument, so a re-run over an already-usable account is quiet.
    v_can_login     BOOLEAN;
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

    -- CAN THIS ADMIN SIGN IN? ---------------------------------------------
    SELECT u.password_hash IS NOT NULL
           OR EXISTS (SELECT 1 FROM queen_proxy.identities i WHERE i.user_id = u.id)
      INTO v_can_login
      FROM queen_proxy.users u
     WHERE u.id = v_user_id;

    IF NOT v_can_login THEN
        RAISE WARNING 'bootstrap_tenant: admin % has NO password and NO linked identity — it cannot sign in to the console with a password. The tenant, cluster, admin role and API key were all created normally.', v_email
            USING HINT = 'Legal for API-key-only onboarding. If a human is meant to log in here, pass p_password, or configure an OAuth provider on this cell that returns this address verified (it is then linked on first sign-in). See can_login/password_set in the returned JSONB.';
    END IF;

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
            'key_issued', v_api_key IS NOT NULL,
            -- Durable copy of the warning above: stderr is gone by the time
            -- anyone asks why the customer cannot log in.
            'password_set', v_password_set,
            'can_login', v_can_login
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
        'api_key', v_api_key,
        -- Additive: existing callers read tenant_id/cluster_id/user_id/api_key
        -- and are unaffected.
        'password_set', v_password_set,
        'can_login', v_can_login
    );
END;
$$;
