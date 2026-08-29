-- ============================================================================
-- streams_state_get_v1: batched per-partition state reads
-- ============================================================================
--
-- Called by stateful operators at the start of a cycle, and by the SDK's
-- idle-flush timer to discover ripe windows on quiet partitions. Given a
-- (query_id, partition_id) and an optional filter, returns the matching
-- state rows. Missing keys are simply absent from the result.
--
-- This SP is BATCHABLE in libqueen — multiple workers' reads of the same
-- type can be merged into one drain pass via the per-type queue +
-- BatchPolicy machinery. The per-row idx field threads the merging back to
-- per-call results.
--
-- Input shape (JSONB array, one element per logical request):
--   [
--     { "idx": 0,
--       "query_id":     "<uuid>",
--       "partition_id": "<uuid>",
--       "keys":         ["window:2026-05-06T10:01", ...]   -- explicit keys, optional
--       "key_prefix":   "tumb:60\u001f",                    -- prefix filter, optional
--       "ripe_at_or_before": 1717000000000                  -- epoch ms; only rows whose
--                                                            --   value->>'windowEnd' <= this
--                                                            --   are returned (idle flush)
--     },
--     ...
--   ]
--
-- Filter precedence (in order, evaluated as AND):
--   0. Always: the query must BELONG to p_tenant (Track B §5). A foreign
--      (query_id, partition_id) therefore reads exactly like a missing key —
--      an empty `rows` array, success:true — so this endpoint is not an oracle
--      for the existence of another tenant's query.
--   1. Always: query_id + partition_id match
--   2. If `keys` is non-empty: key IN keys[]
--   3. If `key_prefix` is non-empty: key LIKE prefix || '%'
--   4. If `ripe_at_or_before` is set: (value->>'windowEnd')::bigint <= ripe_at_or_before
--
-- Output shape (JSONB array, one element per input idx):
--   [
--     { "idx": 0,
--       "result": {
--         "success": true,
--         "rows": [
--            {"key":"...","value":{...},"updated_at":"..."},
--            ...
--         ]
--       }
--     },
--     ...
--   ]
-- ============================================================================

-- Track B (§5): p_tenant added LAST with a DEFAULT, so a caller that omits it
-- lands on the default tenant (OSS behaviour, byte-identical). The DROP is the
-- pre-tenancy ONE-ARGUMENT signature — left in the catalog it would make a
-- one-argument call ambiguous against this defaulted one rather than resolve
-- (same discipline as 004_log_pop.sql's tenant pass).
DROP FUNCTION IF EXISTS queen.streams_state_get_v1(JSONB);
CREATE OR REPLACE FUNCTION queen.streams_state_get_v1(
    p_requests JSONB,
    p_tenant   UUID DEFAULT '00000000-0000-0000-0000-000000000001'
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_results JSONB := '[]'::jsonb;
    v_req     RECORD;
    v_rows    JSONB;
BEGIN
    IF p_requests IS NULL OR jsonb_array_length(p_requests) = 0 THEN
        RETURN '[]'::jsonb;
    END IF;

    FOR v_req IN
        SELECT
            (r->>'idx')::INT          AS idx,
            (r->>'query_id')::UUID    AS query_id,
            (r->>'partition_id')::UUID AS partition_id,
            -- Coerce `keys` to an array defensively. COALESCE alone only catches
            -- an ABSENT key (SQL NULL); a client that sends `"keys": null`
            -- (e.g. the Go SDK's GetState with a nil slice) yields a jsonb
            -- 'null' SCALAR, which survives COALESCE and makes the
            -- jsonb_array_length(keys) filter below throw "cannot get array
            -- length of a scalar". Treat anything that isn't a JSON array as [].
            CASE WHEN jsonb_typeof(r->'keys') = 'array'
                 THEN r->'keys' ELSE '[]'::jsonb END AS keys,
            NULLIF(r->>'key_prefix', '') AS key_prefix,
            CASE
                WHEN r ? 'ripe_at_or_before' AND jsonb_typeof(r->'ripe_at_or_before') = 'number'
                THEN (r->>'ripe_at_or_before')::BIGINT
                ELSE NULL
            END AS ripe_at_or_before
        FROM jsonb_array_elements(p_requests) AS r
    LOOP
        IF v_req.query_id IS NULL OR v_req.partition_id IS NULL THEN
            v_results := v_results || jsonb_build_object(
                'idx', v_req.idx,
                'result', jsonb_build_object(
                    'success', false,
                    'error',   'query_id and partition_id are required',
                    'rows',    '[]'::jsonb
                )
            );
            CONTINUE;
        END IF;

        SELECT COALESCE(jsonb_agg(
            jsonb_build_object(
                'key',        s.key,
                'value',      s.value,
                'updated_at', s.updated_at
            ) ORDER BY s.key
        ), '[]'::jsonb)
        INTO v_rows
        FROM queen_streams.state s
        WHERE s.query_id     = v_req.query_id
          AND s.partition_id = v_req.partition_id
          -- OWNERSHIP (Track B §5). state carries no tenant column on purpose
          -- (002_streams_schema): it is attributable through this FK, so the
          -- ownership test is one PK probe on queries. Written as a predicate
          -- rather than a pre-check + error so a foreign query_id returns the
          -- SAME empty result a never-written key does — the no-oracle posture
          -- the pid-addressed routes use their 404 for.
          AND EXISTS (
              SELECT 1 FROM queen_streams.queries q
               WHERE q.id = s.query_id AND q.tenant_id = p_tenant
          )
          AND (
              jsonb_array_length(v_req.keys) = 0
              OR EXISTS (
                  SELECT 1 FROM jsonb_array_elements_text(v_req.keys) AS k(key)
                  WHERE k.key = s.key
              )
          )
          AND (
              v_req.key_prefix IS NULL
              OR s.key LIKE v_req.key_prefix || '%'
          )
          AND (
              v_req.ripe_at_or_before IS NULL
              OR (
                  jsonb_typeof(s.value->'windowEnd') = 'number'
                  AND (s.value->>'windowEnd')::BIGINT <= v_req.ripe_at_or_before
              )
          );

        v_results := v_results || jsonb_build_object(
            'idx', v_req.idx,
            'result', jsonb_build_object(
                'success', true,
                'rows',    COALESCE(v_rows, '[]'::jsonb)
            )
        );
    END LOOP;

    RETURN v_results;
END;
$$;

GRANT EXECUTE ON FUNCTION queen.streams_state_get_v1(JSONB, UUID) TO PUBLIC;
