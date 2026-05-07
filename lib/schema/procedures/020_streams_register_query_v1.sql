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
-- ============================================================================

CREATE OR REPLACE FUNCTION queen.streams_register_query_v1(p_requests JSONB)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_results      JSONB := '[]'::jsonb;
    v_req          JSONB;
    v_idx          INT;
    v_name         TEXT;
    v_source_queue TEXT;
    v_sink_queue   TEXT;
    v_config_hash  TEXT;
    v_reset        BOOLEAN;
    v_existing     queen_streams.queries%ROWTYPE;
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
            SELECT * INTO v_existing FROM queen_streams.queries WHERE name = v_name;

            IF NOT FOUND THEN
                INSERT INTO queen_streams.queries (name, source_queue, sink_queue, config_hash)
                VALUES (v_name, v_source_queue, v_sink_queue, v_config_hash)
                ON CONFLICT (name) DO UPDATE SET
                    source_queue = EXCLUDED.source_queue,
                    sink_queue   = EXCLUDED.sink_queue,
                    config_hash  = EXCLUDED.config_hash,
                    updated_at   = NOW()
                RETURNING id INTO v_query_id;
                v_fresh := true;
            ELSIF v_existing.config_hash = v_config_hash THEN
                UPDATE queen_streams.queries
                SET source_queue = v_source_queue,
                    sink_queue   = v_sink_queue,
                    updated_at   = NOW()
                WHERE id = v_existing.id;
                v_query_id := v_existing.id;
            ELSIF v_reset THEN
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

GRANT EXECUTE ON FUNCTION queen.streams_register_query_v1(JSONB) TO PUBLIC;
