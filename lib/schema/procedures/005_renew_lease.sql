-- ============================================================================
-- renew_lease_v2: Batch lease renewal
-- ============================================================================
-- NOTE: Using CREATE OR REPLACE (no DROP) for zero-downtime deployments

CREATE OR REPLACE FUNCTION queen.renew_lease_v2(
    p_items JSONB
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_item JSONB;
    v_results JSONB := '[]'::jsonb;
    v_success BOOLEAN;
    v_new_expires TIMESTAMPTZ;
    v_error TEXT;
    v_lease_id TEXT;
    v_extend_seconds INT;
BEGIN
    FOR v_item IN 
        SELECT value FROM jsonb_array_elements(p_items)
        ORDER BY (value->>'leaseId')
    LOOP
        v_success := false;
        v_error := NULL;
        v_new_expires := NULL;
        v_lease_id := v_item->>'leaseId';
        v_extend_seconds := COALESCE((v_item->>'extendSeconds')::int, 60);
        
        -- v4 multi-partition pops produce N partition_consumers rows
        -- sharing the same worker_id (= leaseId). The UPDATE intentionally
        -- fans out to all of them. PL/pgSQL forbids
        -- "UPDATE ... RETURNING ... INTO scalar" when more than one row is
        -- returned (even without STRICT), so we wrap the UPDATE in a CTE
        -- and aggregate. MIN is the conservative choice: it reports the
        -- earliest deadline among the renewed rows, which is what a
        -- caller needs to schedule the next renew.
        WITH updated AS (
            UPDATE queen.partition_consumers
            SET lease_expires_at = GREATEST(
                lease_expires_at,
                NOW() + (v_extend_seconds || ' seconds')::interval
            )
            WHERE worker_id = v_lease_id
              AND lease_expires_at > NOW()
            RETURNING lease_expires_at
        )
        SELECT MIN(lease_expires_at) INTO v_new_expires FROM updated;
        
        IF v_new_expires IS NOT NULL THEN
            v_success := true;
        ELSE
            IF NOT EXISTS (
                SELECT 1 FROM queen.partition_consumers 
                WHERE worker_id = v_lease_id
            ) THEN
                v_error := 'Lease not found';
            ELSIF EXISTS (
                SELECT 1 FROM queen.partition_consumers 
                WHERE worker_id = v_lease_id
                  AND lease_expires_at <= NOW()
            ) THEN
                v_error := 'Lease already expired';
            ELSE
                v_error := 'Lease not active';
            END IF;
        END IF;
        
        v_results := v_results || jsonb_build_object(
            'index', (v_item->>'index')::int,
            'leaseId', v_lease_id,
            'success', v_success,
            'error', v_error,
            'expiresAt', CASE WHEN v_new_expires IS NOT NULL 
                              THEN to_char(v_new_expires, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                              ELSE NULL END
        );
    END LOOP;
    
    SELECT COALESCE(jsonb_agg(item ORDER BY (item->>'index')::int), '[]'::jsonb)
    INTO v_results
    FROM jsonb_array_elements(v_results) item;
    
    RETURN v_results;
END;
$$;

GRANT EXECUTE ON FUNCTION queen.renew_lease_v2(jsonb) TO PUBLIC;