-- set_limit_override: the control-plane lever for per-cluster limit tweaks
-- (PLAN §3: plans are a catalog + per-cluster overrides so support can bump
-- one limit without forking a plan). Was the one mandated-function gap left
-- by 002 (Agent B report §5.4). Same discipline as every mutating function:
-- validate, write, append operations row, pg_notify the cell proxies.
--
-- Passing NULL or '{}' clears every override (cluster falls back to plan).

CREATE OR REPLACE FUNCTION queen_proxy.set_limit_override(
    p_cluster_id UUID,
    p_overrides  JSONB
) RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_tenant UUID;
BEGIN
    SELECT tenant_id INTO v_tenant FROM queen_proxy.clusters WHERE id = p_cluster_id;
    IF v_tenant IS NULL THEN
        RAISE EXCEPTION 'set_limit_override: unknown cluster %', p_cluster_id;
    END IF;

    UPDATE queen_proxy.clusters
       SET limit_overrides = COALESCE(p_overrides, '{}'::jsonb)
     WHERE id = p_cluster_id;

    PERFORM queen_proxy.record_operation(
        v_tenant, p_cluster_id, 'control_plane', NULL,
        'limit_override_set', p_cluster_id::text,
        COALESCE(p_overrides, '{}'::jsonb)
    );
    PERFORM pg_notify('queen_proxy_inval', p_cluster_id::text);
END;
$$;
