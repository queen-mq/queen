-- ============================================================================
-- Log engine (18-log-engine.md §9) — streams port of the retired seg engine's
-- streams cycle SP (seg_streams).
--
-- queen.log_streams_cycle_v1: atomic streaming cycle commit on the LOG engine.
-- Same contract and response envelope as the retired seg-engine cycle SP
-- (itself the segments sibling of the retired rows-engine
-- queen.streams_cycle_v1),
-- with every data touch retargeted from the seg store to the log store and
-- every position re-addressed from (seq, frame_idx) pairs to a single
-- per-partition BIGINT message offset:
--
--   * source cursor      -> queen.log_consumers (committed/batch_end + lease),
--                            NOT the seg-fold cursor columns of the retired
--                            engines (those tables are gone).
--   * sink push          -> queen.log_push_one_v1 INLINE (same txn), on
--                            broker-prepacked segments (hashes + base64 zstd
--                            blob) — ONE allocator code path shared with
--                            003_log_push's push and 005_log_ack's
--                            transaction wire.
--   * source ack         -> cursor advance over the leased offset span. Full
--                            batch advances committed to the recorded
--                            batch_end (no normalization: offsets have no
--                            frame carry, the msg_count lookups of the seg
--                            version are simply GONE); a gate partial-ack
--                            walks K frames forward over log_segments and
--                            RETAINS the lease on the tail.
--
-- One streaming cycle (fat JS-client Runner) runs as:
--   1. Pop a batch for a partition (non-autoAck -> lease on
--      queen.log_consumers, batch_end recorded). The leaseId is the pop worker
--      id, threaded back into the response by the Rust pop handler.
--   2. (optional) read state for the partition (streams_state_get_v1,
--      009_streams_state_get_v1 — engine-neutral, untouched).
--   3. user operators run client-side -> state_ops, sink frames, ack.
--   4. commit all three atomically via this SP.
--
-- Input: JSONB ARRAY of cycle requests (the runtime merges same-JobType jobs
-- into one call). Each element:
--
--   { "idx": 0,
--     "query_id":     "<uuid>",
--     "partition_id": "<log_partitions.id uuid>",   -- source partition (log)
--     "consumer_group": "streams.<queryId>",         -- default __QUEUE_MODE__
--     "worker":       "<leaseId from the source pop>",
--     "release_lease": true | false,                 -- default true
--     "state_ops": [
--        {"type":"upsert","key":"...","value":{...}},
--        {"type":"delete","key":"..."}
--     ],
--     "sink_segments": [                             -- broker-prepacked, opt.
--        {"queue":"...","partition":"...",
--         "hashesB64":"<base64 of 16*count bytes, xxh3_128 BE, frame order>",
--         "blobB64":"<base64 zstd frames>","count":N}
--     ],
--     "ack": { "ok": true, "count": K } | null       -- null on idle-flush
--   }
--
-- WIRE CHANGE vs the retired seg version (Rust phase, handlers/streams.rs packing + db.rs:1348):
-- sink segments carry "hashesB64" INSTEAD of "metas":[{i,mid,txn}]. The log
-- engine's per-frame metadata is the 16B xxh3_128 txn hash (§3: the BROKER
-- hashes, SQL only stores bytea — log_txns is ALWAYS written because
-- ack-by-hash resolution depends on it), and mids/txn text live only inside
-- the blob. A missing/misaligned hashesB64 fails the element loudly via
-- log_push_one_v1's stride guard (better than silently mis-addressing acks).
--
-- release_lease semantics (mirrors the retired rows-engine streams_cycle's
-- gate backpressure)
-- --------------------------------------------------------------------
--   ack.ok = true, release_lease = true  (default, full batch):
--       committed = batch_end, clear the lease (worker_id/lease_expires_at/
--       batch_end) and the retry/redelivery state (batch_retry_count/
--       attempt_offset/attempt_count — the reached-end release+reset of
--       005_log_ack's log_ack_v1, which "ack the whole leased batch" is by
--       definition),
--       total_consumed += count. Identical to log_ack_v1 with p_upto =
--       batch_end.
--   ack.ok = true, release_lease = false (gate partial ack of K):
--       advance committed by EXACTLY K frames forward from the batch start
--       (committed+1) by walking actual queen.log_segments ranges, but RETAIN
--       worker_id/lease_expires_at/batch_end. The un-acked tail stays leased;
--       on lease expiry a re-pop resumes at the advanced cursor and redelivers
--       only the tail, in order. Walking real segment ranges (not committed+K
--       arithmetic) keeps the walk gap-tolerant: offsets eaten by retention
--       are skipped exactly like the seg version skipped deleted seqs —
--       counting them would silently over-advance past live frames.
--   ack.ok = false (nack / failed batch):
--       release the lease, cursor UNTOUCHED (full redelivery). Retry state is
--       left as-is, exactly like the retired seg version (streams manage
--       retries client-side; the
--       pop-path budget is not consumed by a cycle nack).
--   ack = null (idle-flush cycle):
--       skip the lease block entirely (state ops + sink push only).
--
-- Exactly-once holds because the sink push precedes the ack and the ack RAISEs
-- (aborts the per-element savepoint) on an invalid/expired lease: a lost-
-- response retry re-packs a NEW sink segment, then the ack finds the lease gone
-- -> the savepoint rolls back the inline sink push -> no double consume, no
-- double sink write. (With dedup enabled on the sink queue the push probe adds
-- a second, independent guard — it returns "duplicate" having written NOTHING
-- — but correctness never depends on it.)
--
-- TENANCY (Track B §5, 2026-08-27)
-- --------------------------------
-- `p_tenant` scopes BOTH ends of a cycle, and neither was scoped before:
--
--   * SOURCE. The source partition is addressed by RAW UUID, so the resolve of
--     its queue name now also asserts ownership (q2.tenant_id = p_tenant) and
--     RAISEs when it matches nothing. Pre-tenancy that lookup was decorative —
--     it fed a metric label — so a bogus or foreign partition_id sailed through
--     and the cycle happily wrote state rows attributed to a partition of
--     somebody else's cell, or to no partition at all.
--   * QUERY. The query_id is likewise a raw UUID and is checked against
--     queen_streams.queries before any state op runs.
--   * SINKS. Every by-NAME resolve of queen.queues carries the tenant
--     predicate, and the lazy provisioning INSERT stamps it. A bare-name join
--     could otherwise resolve — or worse, multi-match — another tenant's
--     identically-named queue, and sink emits would land in it.
--
-- Failures are generic ('partition not found', 'query not found'): they name no
-- tenant and do not distinguish "yours and missing" from "someone else's", so a
-- cycle is not an existence oracle. They RAISE rather than return, which is the
-- right failure mode INSIDE the per-element block below — the savepoint rolls
-- back the whole element, exactly like an expired lease does.
--
-- With tenancy off every request resolves to the default tenant, which is what
-- every queue row and every query row already carries by column default, so all
-- of the above is a no-op and the path is byte-identical.
--
-- Output: JSONB ARRAY mirroring input idx, envelope shape IDENTICAL to the
-- retired rows-era queen.streams_cycle_v1 and the retired seg version: [{ "idx": N,
-- "result": { ... } }] with the inner result carrying success / query_id /
-- partition_id / queueName / state_ops_applied / push_results / ack_result
-- (+ error only on failure). Per-segment push_results now carry
-- {"status":"queued","baseOffset":B,...} — 'baseOffset' where the seg engine
-- said 'seq' (§4 push contract; the runtime only inspects success/error).
-- ============================================================================

-- Track B (§5): p_tenant added LAST with a DEFAULT, so a caller that omits it
-- lands on the default tenant (OSS behaviour, byte-identical). The DROP is the
-- pre-tenancy ONE-ARGUMENT signature — left in the catalog it would make a
-- one-argument call ambiguous against this defaulted one rather than resolve
-- (same discipline as 004_log_pop.sql's tenant pass).
DROP FUNCTION IF EXISTS queen.log_streams_cycle_v1(JSONB);
CREATE OR REPLACE FUNCTION queen.log_streams_cycle_v1(
    p_requests JSONB,
    p_tenant   UUID DEFAULT '00000000-0000-0000-0000-000000000001'
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_results             JSONB := '[]'::jsonb;
    v_req                 JSONB;
    v_idx                 INT;

    v_query_id            UUID;
    v_partition_id        UUID;
    v_consumer_group      TEXT;
    v_worker              TEXT;
    v_state_ops           JSONB;
    v_sink_segments       JSONB;
    v_ack                 JSONB;
    v_release_lease       BOOLEAN;

    v_state_ops_applied   INT;
    v_op                  JSONB;

    v_sink                JSONB;
    v_push_res            JSONB;
    v_push_results        JSONB;
    v_sink_pid            UUID;
    v_sink_window         INT;
    v_missing             INT;

    v_ack_ok              BOOLEAN;
    v_ack_count           INT;
    v_ack_result          JSONB;
    v_ack_lease_released  BOOLEAN;

    v_c                   RECORD;

    -- offset-walk locals (gate partial ack)
    v_seg                 RECORD;
    v_wanted              BIGINT;
    v_start               BIGINT;
    v_avail               BIGINT;
    v_remaining           BIGINT;
    v_new_committed       BIGINT;

    v_source_queue_name   TEXT;
    v_one_result          JSONB;
BEGIN
    IF p_requests IS NULL OR jsonb_array_length(p_requests) = 0 THEN
        RETURN '[]'::jsonb;
    END IF;

    FOR v_req IN SELECT * FROM jsonb_array_elements(p_requests)
    LOOP
        v_idx                := COALESCE((v_req->>'idx')::int, (v_req->>'index')::int, 0);
        v_query_id           := NULL;
        v_partition_id       := NULL;
        v_state_ops_applied  := 0;
        v_push_results       := '[]'::jsonb;
        v_ack_result         := 'null'::jsonb;
        v_ack_lease_released := false;
        v_source_queue_name  := NULL;
        v_release_lease      := COALESCE((v_req->>'release_lease')::BOOLEAN, TRUE);

        BEGIN
            v_query_id       := (v_req->>'query_id')::uuid;
            v_partition_id   := (v_req->>'partition_id')::uuid;
            v_consumer_group := COALESCE(NULLIF(v_req->>'consumer_group', ''), '__QUEUE_MODE__');
            v_worker         := v_req->>'worker';
            v_state_ops      := COALESCE(v_req->'state_ops',     '[]'::jsonb);
            v_sink_segments  := COALESCE(v_req->'sink_segments', '[]'::jsonb);
            v_ack            := v_req->'ack';

            IF v_query_id IS NULL THEN
                RAISE EXCEPTION 'query_id is required';
            END IF;
            IF v_partition_id IS NULL THEN
                RAISE EXCEPTION 'partition_id is required';
            END IF;

            -- Resolve the source queue name for the runtime's per-queue ack
            -- metric (record_ack_with_queue). Queue identity is the
            -- queen.queues id (log_partitions.queue_id FKs it directly).
            --
            -- Track B (§5) SOURCE OWNERSHIP GATE. This lookup used to be
            -- decorative — a metric label — and NOT FOUND was silently fine, so
            -- a partition_id belonging to another tenant, or to nothing at all,
            -- ran the whole cycle and left state rows keyed by a partition this
            -- cell could never attribute. It is now the gate: the tenant
            -- predicate closes the foreign-pid hole and the RAISE closes the
            -- bogus-uuid one. Generic message on purpose — "not found" is the
            -- same answer for a missing partition and for somebody else's, so
            -- nothing here can be probed to learn which.
            SELECT q2.name INTO v_source_queue_name
            FROM queen.log_partitions lp
            JOIN queen.queues q2 ON q2.id = lp.queue_id
            WHERE lp.id = v_partition_id
              AND q2.tenant_id = p_tenant;

            IF v_source_queue_name IS NULL THEN
                RAISE EXCEPTION 'partition not found';
            END IF;

            -- Track B (§5) QUERY OWNERSHIP GATE, before any state op touches
            -- queen_streams.state. query_id is a raw uuid off the wire, and the
            -- state ops below are keyed by it alone: without this, a cycle
            -- could upsert into (or delete from) another tenant's query state
            -- for a partition it does own. queries.id is the PK, so this is one
            -- probe.
            IF NOT EXISTS (
                SELECT 1 FROM queen_streams.queries
                 WHERE id = v_query_id AND tenant_id = p_tenant
            ) THEN
                RAISE EXCEPTION 'query not found';
            END IF;

            -- Serialise concurrent cycles + idle-flush sweeps on the same
            -- (query_id, partition_id) state shard. Pre-ack-branch lock so both
            -- ack-bearing cycles and ack-less flush cycles share the key.
            -- Identical key derivation to the retired rows and seg cycle SPs —
            -- and deliberately still tenant-FREE: both halves are uuids, which
            -- do not collide across tenants, so folding p_tenant in would
            -- change every existing key for no isolation gain. Both ids are
            -- verified above to belong to p_tenant before this lock is taken.
            PERFORM pg_advisory_xact_lock(
                hashtextextended(v_query_id::text || ':' || v_partition_id::text, 0)
            );

            -- 1. Apply state ops (verbatim from the retired rows-era cycle) --
            -- All ops are scoped to (query_id, partition_id), the lease shard
            -- the calling worker owns; only one worker holds a partition's
            -- lease, so no cross-worker contention.
            IF jsonb_array_length(v_state_ops) > 0 THEN
                FOR v_op IN SELECT * FROM jsonb_array_elements(v_state_ops)
                LOOP
                    IF v_op->>'type' = 'upsert' THEN
                        INSERT INTO queen_streams.state (query_id, partition_id, key, value, updated_at)
                        VALUES (
                            v_query_id,
                            v_partition_id,
                            v_op->>'key',
                            COALESCE(v_op->'value', '{}'::jsonb),
                            NOW()
                        )
                        ON CONFLICT (query_id, partition_id, key) DO UPDATE SET
                            value      = EXCLUDED.value,
                            updated_at = NOW();
                        v_state_ops_applied := v_state_ops_applied + 1;
                    ELSIF v_op->>'type' = 'delete' THEN
                        DELETE FROM queen_streams.state
                        WHERE query_id     = v_query_id
                          AND partition_id = v_partition_id
                          AND key          = v_op->>'key';
                        v_state_ops_applied := v_state_ops_applied + 1;
                    ELSE
                        RAISE EXCEPTION 'Unknown state_op type: %', COALESCE(v_op->>'type', 'null');
                    END IF;
                END LOOP;
            END IF;

            -- 2. Sink push (inline segment insert, same txn) ----------------
            -- Broker-prepacked segments: hashes + base64 zstd blob. Push each
            -- via log_push_one_v1 INLINE so exactly-once holds (the sink
            -- write shares the savepoint with the ack). Deadlock-safety
            -- pre-pass: lazily create queue/partition rows (gated on missing,
            -- like 003_log_push — an unconditional per-cycle ON CONFLICT would
            -- churn ShareLocks on queen.queues), then lock the target
            -- log_partitions rows in ASCENDING id order before the first push
            -- — the global total lock order shared with push bundles
            -- (003_log_push), acks (005_log_ack) and retention
            -- (006_log_maintenance).
            -- Track B (§5) SINK SCOPING: every by-name resolve of queen.queues
            -- in this block carries `q2.tenant_id = p_tenant`. Queue identity
            -- is (tenant_id, name) since the 2026-07-31 merge, so a bare-name
            -- join is not merely unscoped — it can MULTI-MATCH, silently
            -- fanning one sink emit across the same-named queues of every
            -- tenant on the cell (and, in the pre-lock, locking their
            -- partitions).
            IF jsonb_array_length(v_sink_segments) > 0 THEN
                SELECT count(*) INTO v_missing
                FROM jsonb_array_elements(v_sink_segments) s
                LEFT JOIN queen.queues q2
                  ON q2.name = s->>'queue' AND q2.tenant_id = p_tenant
                LEFT JOIN queen.log_partitions p2
                  ON p2.queue_id = q2.id AND p2.name = s->>'partition'
                WHERE COALESCE(s->>'queue', '') <> '' AND p2.id IS NULL;

                IF v_missing > 0 THEN
                    -- RUSTFIX item 26 (same rationale as 003_log_push's
                    -- provisioning):
                    -- queue identity is the queen.queues id now, so this is
                    -- ONE insert — the row that makes sink-created queues
                    -- visible to namespace/task discovery pops and carries
                    -- retry/DLQ/retention config IS the row log_partitions
                    -- FKs. ON CONFLICT preserves a /configure row; the queue
                    -- is STAMPED with the cycle's tenant (it used to take the
                    -- column default, which is right only for the default
                    -- tenant), and the conflict target is the same
                    -- (tenant_id, name) 003_log_push uses.
                    -- ORDER BY the unique key (lock-order discipline).
                    INSERT INTO queen.queues (tenant_id, name, namespace, task)
                    SELECT DISTINCT p_tenant,
                           s->>'queue',
                           split_part(s->>'queue', '.', 1),
                           CASE WHEN position('.' in s->>'queue') > 0
                                THEN split_part(s->>'queue', '.', 2) ELSE '' END
                    FROM jsonb_array_elements(v_sink_segments) s
                    WHERE COALESCE(s->>'queue', '') <> ''
                    ORDER BY 1, 2
                    ON CONFLICT (tenant_id, name) DO NOTHING;

                    INSERT INTO queen.log_partitions (queue_id, name)
                    SELECT DISTINCT q2.id, s->>'partition'
                    FROM jsonb_array_elements(v_sink_segments) s
                    JOIN queen.queues q2
                      ON q2.name = s->>'queue' AND q2.tenant_id = p_tenant
                    ORDER BY 1, 2
                    ON CONFLICT (queue_id, name) DO NOTHING;
                END IF;

                -- Set-based ascending-id pre-lock (the 003_log_push pattern: the
                -- LockRows node sits above the Sort, so rows lock in ORDER BY
                -- order; duplicate pairs re-lock a held row, a no-op).
                PERFORM 1
                FROM jsonb_array_elements(v_sink_segments) s
                JOIN queen.queues q2
                  ON q2.name = s->>'queue' AND q2.tenant_id = p_tenant
                JOIN queen.log_partitions p2
                  ON p2.queue_id = q2.id AND p2.name = s->>'partition'
                ORDER BY p2.id
                FOR UPDATE OF p2;

                FOR v_sink IN SELECT * FROM jsonb_array_elements(v_sink_segments)
                LOOP
                    -- Resolve pid/window here and pass them down: the callee
                    -- must not re-resolve per segment (the nested-lookup CPU
                    -- the seg v2 rework eliminated, §4). dedup_window_seconds
                    -- lives on queen.queues now (the one queue table).
                    SELECT p2.id, q2.dedup_window_seconds
                    INTO v_sink_pid, v_sink_window
                    FROM queen.queues q2
                    JOIN queen.log_partitions p2 ON p2.queue_id = q2.id
                    WHERE q2.name = v_sink->>'queue'
                      AND p2.name = v_sink->>'partition'
                      AND q2.tenant_id = p_tenant;

                    -- p_verified = -1: no broker dedup-cache claim on this
                    -- inline path — the probe covers the whole window, which
                    -- is always correct (§5). A duplicate verdict wrote
                    -- NOTHING (probe-before-allocate), so it coexists with
                    -- the other sinks' commits without subtransactions.
                    -- p_tenant is passed even though pid/window are already
                    -- resolved: the callee re-resolves BY NAME whenever either
                    -- is NULL (003_log_push), and that fallback must not be
                    -- able to land on another tenant's identically-named queue
                    -- — or provision one under the default tenant.
                    v_push_res := queen.log_push_one_v1(
                        v_sink->>'queue',
                        v_sink->>'partition',
                        (v_sink->>'count')::integer,
                        decode(v_sink->>'hashesB64', 'base64'),
                        -1,
                        decode(v_sink->>'blobB64', 'base64'),
                        v_sink_pid,
                        v_sink_window,
                        p_tenant);
                    v_push_results := v_push_results || jsonb_build_array(
                        jsonb_build_object(
                            'queue',     v_sink->>'queue',
                            'partition', v_sink->>'partition'
                        ) || v_push_res);
                END LOOP;
            END IF;

            -- 3. Source ack = cursor advance over the leased batch ----------
            IF v_ack IS NOT NULL AND v_ack <> 'null'::jsonb THEN
                v_ack_ok    := COALESCE((v_ack->>'ok')::boolean, true);
                v_ack_count := COALESCE((v_ack->>'count')::int, 0);

                -- Load + lock the source cursor row. The lease check is the
                -- exactly-once guard: a RAISE here rolls back the inline sink
                -- push above. (IS DISTINCT FROM for null-safety, as in the
                -- log ack path.)
                SELECT * INTO v_c FROM queen.log_consumers c
                WHERE c.partition_id = v_partition_id
                  AND c.consumer_group = v_consumer_group
                FOR UPDATE;

                IF NOT FOUND
                   OR v_c.worker_id IS DISTINCT FROM v_worker
                   OR v_c.lease_expires_at IS NULL
                   OR v_c.lease_expires_at < clock_timestamp() THEN
                    RAISE EXCEPTION 'invalid or expired lease';
                END IF;

                IF NOT v_ack_ok THEN
                    -- nack: release the lease, cursor untouched (redelivery).
                    UPDATE queen.log_consumers SET
                        worker_id = NULL, lease_expires_at = NULL,
                        batch_end = NULL
                    WHERE partition_id = v_partition_id
                      AND consumer_group = v_consumer_group;
                    v_ack_lease_released := true;
                    v_ack_count := 0;

                ELSIF v_release_lease THEN
                    -- Full batch: committed jumps to the recorded batch_end,
                    -- lease released, retry/redelivery state reset — the
                    -- reached-end release+reset, identical to log_ack_v1 with
                    -- p_upto = batch_end. NO segment reads: with offsets there
                    -- is no frame carry to normalize (the seg version's
                    -- msg_count lookup is gone by construction).
                    IF v_c.batch_end IS NULL THEN
                        RAISE EXCEPTION 'no leased batch';
                    END IF;

                    UPDATE queen.log_consumers SET
                        committed = v_c.batch_end,
                        worker_id = NULL, lease_expires_at = NULL,
                        batch_end = NULL,
                        batch_retry_count = 0,
                        attempt_offset = NULL, attempt_count = 0,
                        total_consumed = total_consumed + GREATEST(v_ack_count, 0)
                    WHERE partition_id = v_partition_id
                      AND consumer_group = v_consumer_group;
                    v_ack_lease_released := true;

                ELSE
                    -- Gate partial ack of K: walk K frames forward from the
                    -- batch start (committed+1) across actual
                    -- queen.log_segments ranges, then advance committed there
                    -- but RETAIN worker_id/lease_expires_at/batch_end so the
                    -- un-acked tail redelivers in order on lease expiry.
                    -- Iterating real ranges keeps the walk gap-tolerant
                    -- (offsets eaten by retention are skipped, never counted).
                    -- This is the one behavior plain log_ack_v1 lacks (it
                    -- always releases) — without it a rate limiter built on
                    -- .gate() would busy-spin re-popping the denied tail.
                    v_wanted        := v_c.committed + 1;
                    v_new_committed := v_c.committed;
                    v_remaining     := GREATEST(v_ack_count, 0);

                    IF v_remaining > 0 THEN
                        FOR v_seg IN
                            SELECT s.base_offset, s.end_offset
                            FROM queen.log_segments s
                            WHERE s.partition_id = v_partition_id
                              AND s.end_offset >= v_wanted
                            ORDER BY s.base_offset
                        LOOP
                            -- Head segment may be partially consumed: start
                            -- inside it. Ranges are disjoint and end >= wanted
                            -- here, so avail >= 1 always.
                            v_start := GREATEST(v_wanted, v_seg.base_offset);
                            v_avail := v_seg.end_offset - v_start + 1;

                            IF v_remaining <= v_avail THEN
                                v_new_committed := v_start + v_remaining - 1;
                                v_remaining := 0;
                                EXIT;
                            ELSE
                                v_remaining := v_remaining - v_avail;
                                v_new_committed := v_seg.end_offset;
                            END IF;
                        END LOOP;
                    END IF;

                    UPDATE queen.log_consumers SET
                        committed = v_new_committed,
                        total_consumed = total_consumed + GREATEST(v_ack_count, 0)
                        -- worker_id / lease_expires_at / batch_end RETAINED
                    WHERE partition_id = v_partition_id
                      AND consumer_group = v_consumer_group;
                    v_ack_lease_released := false;
                END IF;

                -- NB: no per-message consumption ledger is written here. The
                -- rows engine's ledger table went with the rest of that
                -- engine; the log engine tracks consumption via
                -- log_consumers.total_consumed (advanced above), exactly like
                -- log_ack_v1 / log_ack_by_hash_v1.

                v_ack_result := jsonb_build_object(
                    'success',        true,
                    'count',          GREATEST(v_ack_count, 0),
                    'lease_released', v_ack_lease_released,
                    'dlq',            false
                );
            END IF;

            v_one_result := jsonb_build_object(
                'success',           true,
                'query_id',          v_query_id::text,
                'partition_id',      v_partition_id::text,
                'queueName',         v_source_queue_name,
                'state_ops_applied', v_state_ops_applied,
                'push_results',      v_push_results,
                'ack_result',        v_ack_result
            );
        EXCEPTION WHEN OTHERS THEN
            v_one_result := jsonb_build_object(
                'success',           false,
                'query_id',          COALESCE(v_query_id::text, ''),
                'partition_id',      COALESCE(v_partition_id::text, ''),
                'queueName',         v_source_queue_name,
                'state_ops_applied', v_state_ops_applied,
                'push_results',      v_push_results,
                'ack_result',        v_ack_result,
                'error',             SQLERRM
            );
        END;

        v_results := v_results || jsonb_build_object('idx', v_idx, 'result', v_one_result);
    END LOOP;

    RETURN v_results;
END;
$$;

GRANT EXECUTE ON FUNCTION queen.log_streams_cycle_v1(JSONB, UUID) TO PUBLIC;
