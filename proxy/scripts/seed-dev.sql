-- queen_proxy dev seed data -- NOT a migration (not in migrations/, not
-- registered in db::migrations(), not applied automatically at boot).
-- Run by hand against a dev pxdb that has already had 001_init.sql and
-- 002_functions.sql applied, e.g.:
--   psql "$PXDB_URL" -f proxy/scripts/seed-dev.sql
--
-- Creates the local dev fixture referenced by CONTRACTS.md's dev cell ports
-- (pxdb :5465, cell PG :5466, broker :6710, proxy :6711):
--   * tenant 'dev', user dev@localhost, password 'devpass' (bcrypt via
--     pgcrypto; dev fixture only), admin role on cluster 'dev' (console)
--   * cell 'local', base_url http://127.0.0.1:6710, cell_secret NULL (dev
--     broker runs without JWT_ENABLED), class shared
--   * cluster slug 'dev' on cell 'local', plan 'free'
--   * one API key with FULL scopes (produce/consume/admin/read), for local
--     end-to-end testing against QUEEN_PROXY_DEV_INSECURE=false setups.
--
-- THE PLAINTEXT DEV KEY (for local testing only -- never use in anything
-- that isn't a throwaway local pxdb):
--
--   qk_dev_devdevdevdevdevdevdevdevdevdevdevdevdev
--
-- sha256 hex of the string above (verified with both `shasum -a 256` and
-- Python's hashlib -- see the report):
--
--   a492a78dbb0a44288f1ff363823b58f4e6fad1367018104e07f7dc118eee781b
--
-- Note this literal key does NOT match the CONTRACTS.md production format
-- (`qk_<env>_<43 base64url chars>`) -- it's a memorable, deliberately
-- fake/short fixture string, hashed exactly as given. auth::key_hash_hex
-- only cares about the hash of whatever bearer token arrives; it doesn't
-- validate the qk_ literal's shape.
--
-- THE PLAINTEXT DEV LOGIN (console/local login, dev fixture only):
--
--   dev@localhost / devpass
--
-- hashed at seed time with pgcrypto's crypt(gen_salt('bf', 10)) -- the Rust
-- bcrypt crate verifies pgcrypto's $2a$ output (verified in the Track A and
-- console smokes). The user also gets an 'admin' cluster_roles row on the
-- 'dev' cluster, without which every /api/console endpoint is 403.
--
-- Idempotent: safe to re-run against a pxdb this script already seeded
-- (every step is guarded by a "does it already exist" check).
--
-- SINGLE-TENANT ON PURPOSE. This seed is the "one cluster, one key, one
-- console login" fixture; it is NOT the isolation fixture. scripts/
-- isolation-smoke.sh provisions what a two-tenant test needs on top of it, at
-- run time and idempotently: clusters `two` (plan free, the adversary tenant),
-- `pro1`/`pro2` (plan pro -- the only seeded plan carrying the streams+traces
-- features, which come from the plan and cannot be switched on by a limit
-- override) and `rl` (plan free, left on the stock 5 req/s so the live 429
-- check has something to overrun), plus a short-lived api key per cluster per
-- run. Keep those out of here: a cluster whose limits this file pinned would
-- make the smoke's own overrides ambiguous.

DO $$
DECLARE
    v_tenant  UUID;
    v_user    UUID;
    v_cell    UUID;
    v_cluster UUID;
    v_key     UUID;
BEGIN
    -- tenant 'dev' ---------------------------------------------------------
    SELECT id INTO v_tenant FROM queen_proxy.tenants WHERE slug = 'dev';
    IF v_tenant IS NULL THEN
        v_tenant := queen_proxy.create_tenant('dev', 'Dev Tenant');
        RAISE NOTICE 'created tenant % (dev)', v_tenant;
    ELSE
        RAISE NOTICE 'tenant dev already exists (%)', v_tenant;
    END IF;

    -- user dev@localhost -----------------------------------------------
    SELECT id INTO v_user FROM queen_proxy.users WHERE email = 'dev@localhost';
    IF v_user IS NULL THEN
        v_user := queen_proxy.create_user(v_tenant, 'dev@localhost', NULL, 'local');
        RAISE NOTICE 'created user % (dev@localhost)', v_user;
    ELSE
        RAISE NOTICE 'user dev@localhost already exists (%)', v_user;
    END IF;

    -- dev password 'devpass' (fixture only; needs pgcrypto from 001_init) --
    UPDATE queen_proxy.users
       SET password_hash = public.crypt('devpass', public.gen_salt('bf', 10))
     WHERE id = v_user AND password_hash IS NULL;
    IF FOUND THEN
        RAISE NOTICE 'set dev password for dev@localhost (devpass)';
    END IF;

    -- Human-readable fixture name for the operator user management page.
    UPDATE queen_proxy.users
       SET name = 'Dev Operator'
     WHERE id = v_user AND name IS NULL;

    -- cell 'local' -----------------------------------------------------
    -- Infrastructure row, not tenant business data -- no queen_proxy.*
    -- function creates cells (see 001_init.sql's comment on the cells
    -- table), so this is a direct INSERT, exactly as ops tooling would do
    -- it against a real pxdb.
    SELECT id INTO v_cell FROM queen_proxy.cells WHERE slug = 'local';
    IF v_cell IS NULL THEN
        INSERT INTO queen_proxy.cells (slug, region, base_url, class, cell_secret)
        VALUES ('local', 'local', 'http://127.0.0.1:6710', 'shared', NULL)
        RETURNING id INTO v_cell;
        RAISE NOTICE 'created cell % (local)', v_cell;
    ELSE
        RAISE NOTICE 'cell local already exists (%)', v_cell;
    END IF;

    -- cluster 'dev' on cell 'local', plan 'free' ------------------------
    SELECT id INTO v_cluster FROM queen_proxy.clusters WHERE slug = 'dev';
    IF v_cluster IS NULL THEN
        v_cluster := queen_proxy.create_cluster(v_tenant, 'dev', 'free', v_cell);
        RAISE NOTICE 'created cluster % (dev)', v_cluster;
    ELSE
        RAISE NOTICE 'cluster dev already exists (%)', v_cluster;
    END IF;

    -- dev API key, full scopes -------------------------------------------
    IF NOT EXISTS (
        SELECT 1 FROM queen_proxy.api_keys
        WHERE key_hash = 'a492a78dbb0a44288f1ff363823b58f4e6fad1367018104e07f7dc118eee781b'
    ) THEN
        v_key := queen_proxy.issue_api_key(
            v_cluster,
            'dev key (full scopes)',
            'a492a78dbb0a44288f1ff363823b58f4e6fad1367018104e07f7dc118eee781b',
            ARRAY['produce', 'consume', 'admin', 'read']
        );
        RAISE NOTICE 'issued api key % for cluster dev -- plaintext: qk_dev_devdevdevdevdevdevdevdevdevdevdevdevdev', v_key;
    ELSE
        RAISE NOTICE 'dev api key already exists';
    END IF;

    -- console role: dev@localhost admin on cluster 'dev' -------------------
    INSERT INTO queen_proxy.cluster_roles (user_id, cluster_id, role)
    VALUES (v_user, v_cluster, 'admin')
    ON CONFLICT DO NOTHING;
END;
$$;
