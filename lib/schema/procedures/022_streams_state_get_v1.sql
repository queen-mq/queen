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

CREATE OR REPLACE FUNCTION queen.streams_state_get_v1(p_requests JSONB)
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
            COALESCE(r->'keys', '[]'::jsonb) AS keys,
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

GRANT EXECUTE ON FUNCTION queen.streams_state_get_v1(JSONB) TO PUBLIC;
