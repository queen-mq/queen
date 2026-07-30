-- queen_proxy operator capability -- the super-admin bit the internal webapp
-- boots from. Applied as migration "006_operator" after 005_usage_prune.
--
-- Same discipline as 002_functions / 004_lifecycle: validate input, write,
-- then call queen_proxy.record_operation(...) exactly once. Sessions are not
-- cluster-scoped, so like revoke_session this passes cluster_id NULL and
-- therefore emits no pg_notify -- the proxy caches the flag for its own short
-- TTL (auth::Keys::is_operator), which bounds how long a revoked operator
-- keeps the capability on an already-running proxy.
--
-- WHY THE ONLY WRITER IS A CONTROL-PLANE FUNCTION: the routes an operator may
-- open (cell-wide status, PG internals, host metrics, maintenance) are blocked
-- at the proxy precisely because they are NOT tenant-scopable. If any console
-- or API endpoint could set this bit, a cluster admin could escalate to
-- reading -- and pausing -- every other tenant on the same cell. There is
-- deliberately no HTTP path to this column: granting it requires a psql
-- session on pxdb, i.e. whoever already runs the fleet.
--
-- Every statement is idempotent (IF NOT EXISTS / OR REPLACE): re-applying the
-- whole file is a no-op.

ALTER TABLE queen_proxy.users
    ADD COLUMN IF NOT EXISTS is_operator BOOLEAN NOT NULL DEFAULT false;

-- Operators are a handful of rows in a table otherwise reached by email or id.
-- The partial index keeps "who holds the capability" -- the question an audit
-- asks -- a lookup rather than a seq scan over every customer user.
CREATE INDEX IF NOT EXISTS idx_users_operator
    ON queen_proxy.users(email) WHERE is_operator;

-- ----------------------------------------------------------------------------
-- set_operator(email, enabled)
--
-- The ONLY supported way to grant or withdraw the operator capability. Takes
-- an email rather than an id for the same reason grant_cluster_role does: an
-- address is what a human types, and it is what the audit row should read back.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen_proxy.set_operator(
    p_email   TEXT,
    p_enabled BOOLEAN
) RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_email     TEXT;
    v_user_id   UUID;
    v_tenant_id UUID;
    v_was       BOOLEAN;
BEGIN
    IF p_enabled IS NULL THEN
        RAISE EXCEPTION 'set_operator: enabled is required';
    END IF;
    IF p_email IS NULL OR position('@' IN p_email) = 0 THEN
        RAISE EXCEPTION 'set_operator: invalid email %', p_email;
    END IF;
    v_email := lower(btrim(p_email));

    SELECT id, tenant_id, is_operator
      INTO v_user_id, v_tenant_id, v_was
      FROM queen_proxy.users
     WHERE email = v_email
       FOR UPDATE;
    IF v_user_id IS NULL THEN
        RAISE EXCEPTION 'set_operator: unknown user %', v_email;
    END IF;

    UPDATE queen_proxy.users SET is_operator = p_enabled WHERE id = v_user_id;

    -- Audited even when the bit does not change: the row records that someone
    -- asked for cell-wide access at a given moment, which is the point of
    -- having it, and a repeated grant is still an attempt worth keeping.
    PERFORM queen_proxy.record_operation(
        v_tenant_id, NULL, 'control_plane', v_user_id,
        CASE WHEN p_enabled THEN 'operator_granted' ELSE 'operator_revoked' END,
        v_email,
        jsonb_build_object('is_operator', p_enabled, 'was', v_was)
    );
END;
$$;
