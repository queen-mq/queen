-- ============================================================================
-- streams_register_query_v1: idempotent query registration
-- ============================================================================
--
-- Called by the SDK once at the start of `Stream.run({ queryId })`. Inserts
-- (or finds) a row in queen_streams.queries by `name`, validates the
-- caller-supplied config_hash against the stored one, and returns the
-- internal query UUID plus the success/conflict outcome.
--
-- Input shape: JSONB ARRAY of requests (one per drain-batched job).
-- libqueen's _fire_batched merges the route's single-element array with
-- other concurrent register requests; the SP iterates and processes each.
--
--   [
--     { "idx": 0,
--       "name":         "orders.per_customer_per_min",
--       "source_queue": "orders",
--       "sink_queue":   "orders.totals_per_customer_per_min",  -- optional
--       "config_hash":  "<hex>",
--       "reset":        false }                                 -- optional
--   ]
--
-- Output shape: JSONB ARRAY mirroring input idx.
--
--   [
--     { "idx": 0,
--       "result": {
--         "success":     true|false,
--         "query_id":    "<uuid>",
--         "name":        "orders.per_customer_per_min",
--         "config_hash": "<hex>",
--         "fresh":       true|false,    -- true if the row was just inserted
--         "reset":       true|false,    -- true if state was wiped on hash mismatch
--         "denied":      true,          -- ONLY on a grant/quota refusal (403);
--                                       --   absent on every other outcome, so a
--                                       --   config_hash conflict stays a 409
--         "error":       "..."          -- present only when success=false
--       }
--     }
--   ]
--
-- Reset semantics
-- ---------------
-- When `reset=true` is passed, any rows in queen_streams.state for the same
-- name (resolved to its current id) are deleted before the upsert proceeds.
-- This is the safety net for an operator-shape change that the SDK detected
-- via config_hash mismatch — passing `reset=true` is the user's explicit
-- consent to drop the now-incompatible state.
--
-- Conflict on hash mismatch (without reset)
-- -----------------------------------------
-- If a row exists under `name` and `config_hash` doesn't match the supplied
-- one and `reset=false`, the result is success=false with a clear error.
-- The SDK propagates this as a thrown exception so the user sees the
-- mismatch immediately at startup rather than silently feeding old state
-- into a new operator graph.
--
-- TENANCY (Track B §5, 2026-08-27)
-- --------------------------------
-- `p_tenant` scopes EVERY name resolution here: the lookup, the upsert's
-- conflict target ((tenant_id, name), 002_streams_schema), the two UPDATEs and
-- the reset's state delete. A query name is unique per tenant, so two tenants
-- registering 'orders.per_customer_per_min' get two independent queries and
-- neither can observe, mutate or reset the other's.
--
-- THE GRANT, and where it is checked
-- ----------------------------------
-- On the FRESH-INSERT path only, and only when p_tenant is not the default:
-- queen_streams.quota must hold an `enabled` row for the tenant, and
-- `max_queries` (when not NULL) must not already be reached. Refusals come back
-- as a per-element result carrying "denied": true (the broker maps that to 403,
-- distinct from the 409 a config_hash conflict gets).
--
-- Re-registering an EXISTING query — the plain redeploy, the hash-match update
-- AND the reset path — is NEVER gate-checked. Revoking a tenant's streams grant
-- stops NEW queries; it must not stop a Runner that is already draining, which
-- would strand a source backlog nobody can consume and turn a billing decision
-- into data loss. Same drain posture the proxy's GatedOp documents for the
-- cycle route (routes.rs): the growing half is gated, the draining half is not.
--
-- The default tenant skips the table entirely: an OSS / self-hosted / embedded
-- broker has no control plane to write a grant row and must never need one.
-- ============================================================================

-- Track B (§5): p_tenant added LAST with a DEFAULT, so any caller that omits it
-- lands on the default tenant (OSS behaviour, byte-identical). The DROP is the
-- pre-tenancy ONE-ARGUMENT signature: leaving it in the catalog would make a
-- one-argument call AMBIGUOUS against this defaulted one ("function is not
-- unique") instead of silently resolving, and would leave the old body reachable
-- under a GRANT this file no longer names. Same DROP-then-create discipline as
-- 004_log_pop.sql's tenant pass.
DROP FUNCTION IF EXISTS queen.streams_register_query_v1(JSONB);
CREATE OR REPLACE FUNCTION queen.streams_register_query_v1(
    p_requests JSONB,
    p_tenant   UUID DEFAULT '00000000-0000-0000-0000-000000000001'
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    C_DEFAULT_TENANT CONSTANT UUID := '00000000-0000-0000-0000-000000000001';
    v_results      JSONB := '[]'::jsonb;
    v_req          JSONB;
    v_idx          INT;
    v_name         TEXT;
    v_source_queue TEXT;
    v_sink_queue   TEXT;
    v_config_hash  TEXT;
    v_reset        BOOLEAN;
    v_existing     queen_streams.queries%ROWTYPE;
    v_quota        queen_streams.quota%ROWTYPE;
    v_query_count  BIGINT;
    v_query_id     UUID;
    v_fresh        BOOLEAN;
    v_did_reset    BOOLEAN;
    v_one          JSONB;
BEGIN
    IF p_requests IS NULL OR jsonb_array_length(p_requests) = 0 THEN
        RETURN '[]'::jsonb;
    END IF;

    FOR v_req IN SELECT * FROM jsonb_array_elements(p_requests)
    LOOP
        v_idx          := COALESCE((v_req->>'idx')::int, (v_req->>'index')::int, 0);
        v_name         := v_req->>'name';
        v_source_queue := v_req->>'source_queue';
        v_sink_queue   := NULLIF(v_req->>'sink_queue', '');
        v_config_hash  := v_req->>'config_hash';
        v_reset        := COALESCE((v_req->>'reset')::boolean, false);
        v_fresh        := false;
        v_did_reset    := false;
        v_query_id     := NULL;

        IF v_name IS NULL OR v_name = '' THEN
            v_one := jsonb_build_object('success', false, 'error', 'name is required');
        ELSIF v_source_queue IS NULL OR v_source_queue = '' THEN
            v_one := jsonb_build_object('success', false, 'error', 'source_queue is required');
        ELSIF v_config_hash IS NULL OR v_config_hash = '' THEN
            v_one := jsonb_build_object('success', false, 'error', 'config_hash is required');
        ELSE
            SELECT * INTO v_existing FROM queen_streams.queries
             WHERE name = v_name AND tenant_id = p_tenant;

            IF NOT FOUND THEN
                -- ---- GRANT + QUOTA, fresh registrations only ----------------
                -- Not reached for the default tenant (see the header): that is
                -- the OSS path, and it stays byte-identical — no row read, no
                -- lock taken, no way to configure anything.
                --
                -- The row is taken FOR UPDATE and that lock is the SERIALIZATION
                -- POINT for this tenant's registrations: concurrent registers
                -- queue behind it, so the count below is exact and the cap
                -- cannot be overshot by a race. (An absent row locks nothing —
                -- it is also a denial, so there is nothing to serialize.)
                IF p_tenant <> C_DEFAULT_TENANT THEN
                    SELECT * INTO v_quota FROM queen_streams.quota
                     WHERE tenant_id = p_tenant
                     FOR UPDATE;

                    IF NOT FOUND OR NOT v_quota.enabled THEN
                        -- ABSENCE IS A DENIAL, not a permission (024_kv.sql's
                        -- posture): the tenant header is opaque and validated
                        -- against nothing, so fail-open would mint an unlimited
                        -- tenant for any invented id. "denied" is what the
                        -- broker maps to 403 — a config_hash conflict is a 409
                        -- and must stay distinguishable from this.
                        v_one := jsonb_build_object(
                            'success', false,
                            'denied',  true,
                            'error',   'streams not granted for this tenant'
                        );
                        v_results := v_results || jsonb_build_object('idx', v_idx, 'result', v_one);
                        CONTINUE;
                    END IF;

                    IF v_quota.max_queries IS NOT NULL THEN
                        SELECT count(*) INTO v_query_count
                          FROM queen_streams.queries
                         WHERE tenant_id = p_tenant;
                        IF v_query_count >= v_quota.max_queries THEN
                            v_one := jsonb_build_object(
                                'success', false,
                                'denied',  true,
                                'error',   'streams query quota exceeded (max ' ||
                                           v_quota.max_queries::text || ')'
                            );
                            v_results := v_results || jsonb_build_object('idx', v_idx, 'result', v_one);
                            CONTINUE;
                        END IF;
                    END IF;
                END IF;

                -- Conflict target is (tenant_id, name), matching
                -- queries_tenant_name_uk (002_streams_schema) — the index that
                -- replaced the pre-tenancy global UNIQUE(name). The row STAMPS
                -- the tenant, so ownership is established at insert and every
                -- read path above and below resolves through it; a bare-name
                -- resolve would match no index here and could reach another
                -- tenant's query.
                INSERT INTO queen_streams.queries (tenant_id, name, source_queue, sink_queue, config_hash)
                VALUES (p_tenant, v_name, v_source_queue, v_sink_queue, v_config_hash)
                ON CONFLICT (tenant_id, name) DO UPDATE SET
                    source_queue = EXCLUDED.source_queue,
                    sink_queue   = EXCLUDED.sink_queue,
                    config_hash  = EXCLUDED.config_hash,
                    updated_at   = NOW()
                RETURNING id INTO v_query_id;
                v_fresh := true;
            ELSIF v_existing.config_hash = v_config_hash THEN
                -- Plain redeploy of a query this tenant already owns. NOT
                -- gate-checked, by design (see the header): a revoked grant
                -- stops new queries, never a Runner that is already draining.
                -- `id` is the PK of the row the tenant-scoped SELECT returned,
                -- so this UPDATE is scoped by construction.
                UPDATE queen_streams.queries
                SET source_queue = v_source_queue,
                    sink_queue   = v_sink_queue,
                    updated_at   = NOW()
                WHERE id = v_existing.id;
                v_query_id := v_existing.id;
            ELSIF v_reset THEN
                -- Same reasoning, and the state delete needs no tenant
                -- predicate of its own: query_id came from the tenant-scoped
                -- lookup, and every state row is attributable through exactly
                -- that FK (002_streams_schema). The reset path is also
                -- deliberately NOT gate-checked — it is how an existing query
                -- recovers from an operator-shape change, not a new one.
                DELETE FROM queen_streams.state WHERE query_id = v_existing.id;
                UPDATE queen_streams.queries
                SET source_queue = v_source_queue,
                    sink_queue   = v_sink_queue,
                    config_hash  = v_config_hash,
                    updated_at   = NOW()
                WHERE id = v_existing.id;
                v_query_id  := v_existing.id;
                v_did_reset := true;
            ELSE
                -- The conflict result hands back the EXISTING row's query_id.
                -- Now that the lookup above is tenant-scoped, that id can only
                -- ever be the caller's own: pre-tenancy this same line handed a
                -- bare-name collision the other owner's internal UUID, which was
                -- a cross-tenant leak the moment two tenants shared a cell.
                v_one := jsonb_build_object(
                    'success',     false,
                    'query_id',    v_existing.id::text,
                    'name',        v_name,
                    'error',       'config_hash mismatch: operator chain changed for queryId ''' ||
                                   v_name || '''. Pass reset:true to wipe existing state, or use a new queryId.'
                );
                v_results := v_results || jsonb_build_object('idx', v_idx, 'result', v_one);
                CONTINUE;
            END IF;

            v_one := jsonb_build_object(
                'success',     true,
                'query_id',    v_query_id::text,
                'name',        v_name,
                'config_hash', v_config_hash,
                'fresh',       v_fresh,
                'reset',       v_did_reset
            );
        END IF;

        v_results := v_results || jsonb_build_object('idx', v_idx, 'result', v_one);
    END LOOP;

    RETURN v_results;
END;
$$;

GRANT EXECUTE ON FUNCTION queen.streams_register_query_v1(JSONB, UUID) TO PUBLIC;
