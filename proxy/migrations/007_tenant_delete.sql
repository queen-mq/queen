-- queen_proxy tenant deletion -- the writer 002_functions left unbuilt.
-- Applied as migration "007_tenant_delete" after 006_operator.
--
-- 002 gave this schema `set_tenant_status(tenant, 'deleting')` and stopped
-- there. Marking a tenant `deleting` removes NOTHING: it flips a status column
-- the proxy merges into every cached ClusterCtx, every request for that tenant
-- 403s, and every row -- users, clusters, keys, roles, usage -- stays exactly
-- where it was, forever. This file is the step that actually removes them.
--
-- Same discipline as 002_functions / 004_lifecycle / 006_operator: validate
-- input, write, audit, notify. One deviation, and it is forced by the schema
-- rather than chosen -- see "WHERE THE AUDIT ROW GOES" below.
--
-- Every statement is idempotent (OR REPLACE): re-applying the whole file is a
-- no-op.
--
-- ============================================================================
-- THE ORDER OF A WIPE, AND WHY THIS FUNCTION IS LAST
-- ============================================================================
--
--   1. queen_proxy.set_tenant_status(tenant, 'deleting')
--        Stops the traffic. The data plane's status gate (gateway.rs) runs
--        before authentication and the console's (console.rs `console_ctx`)
--        right after it, so from this moment every request for that tenant --
--        data plane AND console -- answers 403 `cluster_suspended`. The console
--        half is deliberate and was once missing: /api/console/* is nested
--        separately in main.rs and never reaches gateway::handle, so a wipe
--        could be under way while an admin session still minted live API keys
--        on the tenant being erased. NOTE for anyone reusing this status for a
--        PAUSE rather than a delete: every SDK treats 429 as the only retryable
--        status and throws a 403 to the application, so `deleting` is right for
--        a wipe (it must not be retried) and wrong for a freeze.
--
--   2. queen.delete_tenant_data_v1(<broker_tenant_uuid>) on the CELL
--        The broker half, run once per cluster with that cluster's
--        `broker_tenant_uuid` -- the x-queen-tenant value the proxy injects
--        upstream, NOT this schema's tenants.id (they are different uuids by
--        construction: clusters.broker_tenant_uuid has its own
--        gen_random_uuid() default). Loop until it answers {"done": true}.
--
--   3. queen_proxy.delete_tenant(tenant)   <- THIS FUNCTION
--
-- Step 3 is last because it destroys the only mapping from a tenant to the
-- broker_tenant_uuid step 2 needs. Run out of order and the cell keeps the
-- data with nothing left able to name its owner. This function cannot check
-- that step 2 ran -- the cell's database is a DIFFERENT database (pxdb is
-- shared by a whole cell; a self-hoster may run only one of the two), and
-- queen_proxy deliberately references nothing in the `queen` schema. What it
-- can do, and does, is RETURN every broker_tenant_uuid it is about to
-- destroy, and put them in the outbox event as well, so an operator who got
-- the order wrong still has them.
--
-- ============================================================================
-- WHERE THE AUDIT ROW GOES (the one deviation from the house discipline)
-- ============================================================================
--
-- Every other mutating function here calls record_operation() exactly once,
-- which appends to the append-only `operations` table and emits the
-- `queen_proxy_inval` NOTIFY. This one cannot:
--
--   * operations.tenant_id is NOT NULL and its FK to tenants(id) has NO
--     ON DELETE clause -- i.e. RESTRICT (001_init spells out why: "tenant
--     lifecycle in this design is the `status` column, not DELETE FROM
--     tenants"). A tenant with any history at all therefore CANNOT be deleted
--     until its operations rows are, so this function must delete them; an
--     audit row it wrote a statement earlier would go with them.
--   * and it should delete them regardless: operations.target and
--     operations.meta carry email addresses (grant_cluster_role,
--     bootstrap_tenant, set_operator). A "delete my account" that leaves the
--     addresses in an audit table is not a deletion.
--
-- The surviving record is therefore an `emit_outbox('tenant_deleted', ...)`
-- event: the outbox is insert-only, has no tenant FK, and is drained by the
-- control plane, which is where fleet-level history belongs anyway. There is
-- precedent for a function here that writes no operations row when the row
-- would be meaningless -- sweep_revoked_tokens (004_lifecycle).
--
-- The NOTIFY is then hand-rolled per cluster, exactly as set_tenant_status
-- does and for the same reason: record_operation only notifies for a non-null
-- cluster_id, and a tenant delete changes the fate of N of them at once.
-- Getting this wrong is not cosmetic -- a proxy that keeps a cached
-- ClusterCtx keeps serving a deleted tenant for up to the host TTL, and the
-- stale-while-revalidate grace window is far longer than that.
--
-- ============================================================================
-- WHAT THIS DOES *NOT* REACH
-- ============================================================================
--
--   * queen_proxy.revoked_tokens -- the JWT deny-list is keyed by jti alone
--     and carries no tenant, so live sessions cannot be enumerated here. They
--     die anyway, and by a better mechanism: deleting the `users` rows makes
--     auth::Keys::user_exists false, which the proxy turns into a 401 ("session
--     no longer valid") rather than a 403 dead end. The only residue is the
--     30 s positive cluster-role cache, and the cluster it names is gone by
--     then, so the request fails at resolution instead.
--   * queen_proxy.outbox ROWS -- the events themselves stay: the outbox is a
--     queue a drain worker is mid-way through reading, and silently dropping
--     entries from it would break that contract (a `tenant_bootstrapped` that
--     vanishes before it is drained is a customer the control plane never
--     hears about). Their PII does NOT stay: the payload keys carrying an
--     address are stripped in place, below. Deleting operations rows because
--     "a delete my account that leaves the addresses in an audit table is not
--     a deletion" while leaving the same address in an undrained outbox row
--     would have been exactly that -- and there is no drain worker in the OSS
--     to prune it later (grep: meter.rs writes the outbox, nothing reads it),
--     so on a self-hosted cell the address would simply have stayed forever.
--     Redaction keeps both contracts: the event still arrives, with its ids,
--     its kind and its counts intact.
--   * anything in the cell's `queen` schema -- step 2 above.

-- ----------------------------------------------------------------------------
-- delete_tenant(tenant_id, force) -> JSONB
--
-- Idempotent by design, not by accident: the tenant row is deleted LAST and
-- everything else goes with it in one transaction, so a re-run after success
-- finds no tenant and answers {"deleted": false, "existed": false} rather than
-- raising -- the same shape (and the same reasoning) as
-- queen.delete_queue_v1's `existed` flag, which exists because the SDKs use
-- delete-before-create as a cleanup idiom. A re-run after a PARTIAL failure is
-- a re-run of the whole thing: a plpgsql function runs inside the caller's
-- transaction, so any RAISE below unwinds every statement above it and there
-- is no half-deleted state to resume from.
--
-- p_force skips only the status precondition. It exists because an operator
-- cleaning up a botched provision should not have to walk a fresh tenant
-- through `deleting` first; it does not make the function delete anything it
-- would not otherwise delete.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen_proxy.delete_tenant(
    p_tenant_id UUID,
    p_force     BOOLEAN DEFAULT false
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_slug      TEXT;
    v_status    TEXT;
    v_clusters  JSONB;
    v_ids       UUID[];
    v_id        UUID;
    v_keys      INT := 0;
    v_revoked   INT := 0;
    v_users     INT := 0;
    v_roles     INT := 0;
    v_queues    INT := 0;
    v_ops       INT := 0;
    v_outbox    INT := 0;
    v_result    JSONB;
BEGIN
    IF p_tenant_id IS NULL THEN
        RAISE EXCEPTION 'delete_tenant: tenant_id is required';
    END IF;

    -- FOR UPDATE, not a bare SELECT: two concurrent deletes of the same tenant
    -- would otherwise both pass the status check and the loser would fail deep
    -- inside the cascade with a foreign-key error instead of a clean answer.
    SELECT slug, status INTO v_slug, v_status
      FROM queen_proxy.tenants
     WHERE id = p_tenant_id
       FOR UPDATE;

    IF v_slug IS NULL THEN
        RETURN jsonb_build_object(
            'deleted', false, 'existed', false, 'tenant_id', p_tenant_id);
    END IF;

    -- The two-step order is ENFORCED here, not merely documented: a tenant
    -- that was never marked `deleting` is a tenant whose traffic was never
    -- stopped and whose cell data was, at best, purged while it was still
    -- being written to.
    IF v_status <> 'deleting' AND NOT p_force THEN
        RAISE EXCEPTION 'delete_tenant: tenant % is %, not deleting -- call '
                        'queen_proxy.set_tenant_status(%, ''deleting'') first (and purge '
                        'the cell with queen.delete_tenant_data_v1), or pass '
                        'p_force => true', v_slug, v_status, p_tenant_id;
    END IF;

    -- ------------------------------------------------------------------
    -- Capture the clusters BEFORE anything is deleted. Three consumers:
    -- the NOTIFY loop at the end, the returned result, and the outbox
    -- event -- and broker_tenant_uuid in particular exists nowhere else
    -- once this transaction commits.
    -- ------------------------------------------------------------------
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'cluster_id',         c.id,
               'slug',               c.slug,
               'cell_id',            c.cell_id,
               'broker_tenant_uuid', c.broker_tenant_uuid) ORDER BY c.slug),
                    '[]'::jsonb),
           COALESCE(array_agg(c.id), ARRAY[]::UUID[])
      INTO v_clusters, v_ids
      FROM queen_proxy.clusters c
     WHERE c.tenant_id = p_tenant_id;

    SELECT count(*) INTO v_users FROM queen_proxy.users WHERE tenant_id = p_tenant_id;
    SELECT count(*) INTO v_roles
      FROM queen_proxy.cluster_roles r WHERE r.cluster_id = ANY (v_ids);
    SELECT count(*) INTO v_queues
      FROM queen_proxy.queues q WHERE q.cluster_id = ANY (v_ids);
    SELECT count(*) INTO v_keys
      FROM queen_proxy.api_keys k WHERE k.cluster_id = ANY (v_ids);

    -- ------------------------------------------------------------------
    -- Revoke every live key. The rows are deleted moments later by the
    -- cluster cascade, so this is not about the rows -- it is about the
    -- ORDER being observable and about the count being auditable.
    --
    -- Not a loop over queen_proxy.revoke_api_key(): that function RAISEs on
    -- an already-revoked key (so a tenant holding one would abort the whole
    -- wipe) and writes one operations row per key into a table this
    -- function is about to empty. `revoked_at IS NULL` is the filter that
    -- makes the same work idempotent.
    -- ------------------------------------------------------------------
    UPDATE queen_proxy.api_keys
       SET revoked_at = now()
     WHERE cluster_id = ANY (v_ids)
       AND revoked_at IS NULL;
    GET DIAGNOSTICS v_revoked = ROW_COUNT;

    -- The surviving audit. Emitted BEFORE the deletes purely so that the
    -- counts it carries are the ones measured above rather than an empty
    -- schema's; the outbox row has no FK to any of it and would be valid
    -- either way.
    PERFORM queen_proxy.emit_outbox(
        'tenant_deleted',
        jsonb_build_object(
            'tenant_id',   p_tenant_id,
            'tenant_slug', v_slug,
            'status_was',  v_status,
            'forced',      p_force,
            'clusters',    v_clusters,
            'counts',      jsonb_build_object(
                'clusters',         jsonb_array_length(v_clusters),
                'users',            v_users,
                'cluster_roles',    v_roles,
                'queues',           v_queues,
                'api_keys',         v_keys,
                'api_keys_revoked', v_revoked),
            'at', now()));

    -- ------------------------------------------------------------------
    -- The outbox is a QUEUE, not an audit table: its undrained rows are
    -- events the control plane has not read yet, and dropping them would
    -- lose a signup rather than tidy one up. So the rows stay and the
    -- ADDRESSES go -- `payload - 'admin_email'` leaves every id, kind and
    -- count intact and removes the only PII the schema ever writes here
    -- (bootstrap_tenant, 004_lifecycle / 008_bootstrap_password; the
    -- `tenant_deleted` event emitted above carries none by construction).
    --
    -- Without this, a tenant erasure left its admin's address in
    -- queen_proxy.outbox forever on a self-hosted cell: there is no drain
    -- worker in the OSS to prune it, and the same function two statements
    -- below deletes `operations` precisely because an address left in a
    -- table is not a deletion.
    --
    -- Consumed rows are redacted too. Drained means the control plane has a
    -- copy and will apply its own retention to it; it does not mean this
    -- database may keep one.
    -- ------------------------------------------------------------------
    UPDATE queen_proxy.outbox
       SET payload = payload - 'admin_email'
     WHERE payload ? 'admin_email'
       AND (payload->>'tenant_id' = p_tenant_id::text
            OR payload->>'cluster_id' = ANY (ARRAY(SELECT u::text FROM unnest(v_ids) u)));
    GET DIAGNOSTICS v_outbox = ROW_COUNT;

    -- ------------------------------------------------------------------
    -- operations must go first: its tenant_id FK is RESTRICT, so it is the
    -- one thing standing between this function and the tenant row. See the
    -- header -- this is also the GDPR-relevant delete, because meta/target
    -- carry email addresses.
    --
    -- operations.cluster_id is ON DELETE SET NULL, which is what would have
    -- preserved this history across a cluster delete had the tenant stayed.
    -- ------------------------------------------------------------------
    DELETE FROM queen_proxy.operations WHERE tenant_id = p_tenant_id;
    GET DIAGNOSTICS v_ops = ROW_COUNT;

    -- One delete; every FK in 001_init/004_lifecycle that hangs off a tenant
    -- or a cluster is ON DELETE CASCADE and does the rest:
    --   tenants -> users      -> identities, cluster_roles
    --           -> clusters   -> api_keys, cluster_roles, queues,
    --                            usage_minutes, usage_days
    -- The two RESTRICTs on clusters point OUTWARD (cell_id, plan_id): a
    -- cluster row referencing a cell does not stop the cluster from being
    -- deleted, it stops the CELL from being deleted, which is the intended
    -- direction and is unaffected here.
    DELETE FROM queen_proxy.tenants WHERE id = p_tenant_id;

    v_result := jsonb_build_object(
        'deleted',     true,
        'existed',     true,
        'tenant_id',   p_tenant_id,
        'tenant_slug', v_slug,
        'status_was',  v_status,
        'forced',      p_force,
        -- Keep these. Once this transaction commits they exist nowhere else,
        -- and they are what names the cell-side data (step 2).
        'clusters',    v_clusters,
        'counts',      jsonb_build_object(
            'clusters',         jsonb_array_length(v_clusters),
            'users',            v_users,
            'cluster_roles',    v_roles,
            'queues',           v_queues,
            'api_keys',         v_keys,
            'api_keys_revoked', v_revoked,
            'operations',       v_ops,
            -- Undrained (and drained) events whose payload carried an address.
            -- The rows survive -- the control plane still gets its events --
            -- and the address does not.
            'outbox_redacted',  v_outbox));

    -- ------------------------------------------------------------------
    -- Invalidate, one NOTIFY per cluster (set_tenant_status's pattern).
    -- Position is cosmetic -- NOTIFY is delivered at COMMIT, so it cannot
    -- reach a proxy before the rows are actually gone -- but it reads in
    -- the order it happens.
    --
    -- Without this, cache.rs keeps each deleted cluster's ClusterCtx until
    -- its own TTL expires and only then discovers the row is gone. The
    -- payload is the cluster_id; invalidate_caches retains by it, so the
    -- host entry AND every cached api-key entry for that cluster drop
    -- together.
    -- ------------------------------------------------------------------
    FOREACH v_id IN ARRAY v_ids LOOP
        PERFORM pg_notify('queen_proxy_inval', v_id::text);
    END LOOP;

    RETURN v_result;
END;
$$;

COMMENT ON FUNCTION queen_proxy.delete_tenant(UUID, BOOLEAN) IS
    'Hard-delete one tenant and everything under it (clusters, users, keys, roles, usage, '
    'audit). Refuses a tenant not in status ''deleting'' unless p_force. Idempotent: a '
    'second call answers {"deleted": false, "existed": false}. Wipe order: '
    'set_tenant_status(deleting) -> queen.delete_tenant_data_v1 on the cell -> '
    'delete_tenant. Returns the broker_tenant_uuids, which exist nowhere else afterwards.';
