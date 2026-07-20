
-- ============================================================================
-- PUSHSER: per-partition push serialization lock
-- ============================================================================
-- Serializes concurrent pushes to the SAME partition so queen.messages.created_at
-- is assigned in commit order per partition. The pop cursor (created_at, id) only
-- moves forward, so without this a message whose committing transaction started
-- earlier (smaller created_at) but commits later than an already-consumed message
-- lands below the cursor and is skipped forever.
--
-- Uses the TWO-INT advisory form pg_advisory_xact_lock(int4, int4), which occupies
-- a structurally separate lock space from EVERY single-bigint advisory lock in this
-- schema (pop's partition claim hashtextextended(partition_id, 12648430), ack/streams
-- md5(partition||cg)::bigint, streams query:partition, stats fixed keys). So a push
-- holding a partition NEVER blocks pop's claim, ack, streams, or stats; push-vs-push
-- on the same partition is the only intended conflict. Class tag 1347439944 = ASCII
-- 'PUSH'. The lock is transaction-scoped: it releases at commit, so the next push to
-- the same partition begins only after this one commits (commit-ordered created_at).
CREATE OR REPLACE FUNCTION queen.lock_push_partition(p_partition_id UUID)
RETURNS void
LANGUAGE sql
AS $$
    SELECT pg_advisory_xact_lock(1347439944, hashtext(p_partition_id::text));
$$;

-- ============================================================================
-- push_messages_v2: High-performance batch push
-- ============================================================================
-- NOTE: Using CREATE OR REPLACE (no DROP) for zero-downtime deployments

CREATE OR REPLACE FUNCTION queen.push_messages_v2(
    p_items jsonb,
    p_check_duplicates boolean DEFAULT true,
    p_check_capacity boolean DEFAULT true  -- Ignored for performance
)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    v_results jsonb;
BEGIN
    -- Create temp table from input JSON (single pass, no array_append)
    CREATE TEMP TABLE tmp_items ON COMMIT DROP AS
    SELECT 
        idx,
        COALESCE((item->>'messageId')::uuid, gen_random_uuid()) as message_id,
        COALESCE(item->>'transactionId', gen_random_uuid()::text) as transaction_id,
        item->>'queue' as queue_name,
        COALESCE(item->>'partition', 'Default') as partition_name,
        COALESCE(item->>'namespace', split_part(item->>'queue', '.', 1)) as namespace,
        COALESCE(item->>'task', 
            CASE WHEN position('.' in item->>'queue') > 0 
                 THEN split_part(item->>'queue', '.', 2) 
                 ELSE '' END) as task,
        COALESCE(item->'payload', '{}'::jsonb) as payload,
        CASE WHEN item->>'traceId' IS NOT NULL AND item->>'traceId' != '' 
             THEN (item->>'traceId')::uuid ELSE NULL END as trace_id,
        COALESCE((item->>'is_encrypted')::boolean, false) as is_encrypted,
        -- producerSub is server-stamped (from validated JWT 'sub' claim); NULL when auth disabled
        NULLIF(item->>'producerSub', '') as producer_sub
    FROM jsonb_array_elements(p_items) WITH ORDINALITY AS t(item, idx);

    -- Ensure queues exist (batch upsert)
    INSERT INTO queen.queues (name, namespace, task)
    SELECT DISTINCT queue_name, namespace, task FROM tmp_items
    ON CONFLICT (name) DO NOTHING;

    -- Ensure partitions exist (batch upsert)
    INSERT INTO queen.partitions (queue_id, name)
    SELECT DISTINCT q.id, t.partition_name
    FROM tmp_items t
    JOIN queen.queues q ON q.name = t.queue_name
    ON CONFLICT (queue_id, name) DO NOTHING;

    -- Add partition_id to temp table
    ALTER TABLE tmp_items ADD COLUMN partition_id uuid;
    UPDATE tmp_items t
    SET partition_id = p.id
    FROM queen.partitions p
    JOIN queen.queues q ON q.id = p.queue_id
    WHERE q.name = t.queue_name AND p.name = t.partition_name;

    -- Handle duplicates if requested
    IF p_check_duplicates THEN
        ALTER TABLE tmp_items ADD COLUMN is_duplicate boolean DEFAULT false;
        ALTER TABLE tmp_items ADD COLUMN existing_message_id uuid;
        
        UPDATE tmp_items t
        SET is_duplicate = true,
            existing_message_id = m.id
        FROM queen.messages m
        WHERE m.transaction_id = t.transaction_id
          AND m.partition_id = t.partition_id;
    END IF;

    -- Insert non-duplicate messages
    INSERT INTO queen.messages (id, transaction_id, partition_id, payload, trace_id, is_encrypted, producer_sub)
    SELECT message_id, transaction_id, partition_id, payload, trace_id, is_encrypted, producer_sub
    FROM tmp_items
    WHERE NOT COALESCE(is_duplicate, false);

    -- Build results (single query, no loops)
    SELECT COALESCE(jsonb_agg(
        CASE 
            WHEN COALESCE(t.is_duplicate, false) THEN
                jsonb_build_object(
                    'index', t.idx - 1,
                    'transaction_id', t.transaction_id,
                    'status', 'duplicate',
                    'message_id', t.existing_message_id::text
                )
            ELSE
                jsonb_build_object(
                    'index', t.idx - 1,
                    'transaction_id', t.transaction_id,
                    'status', 'queued',
                    'message_id', t.message_id::text,
                    'partition_id', t.partition_id::text,
                    'trace_id', t.trace_id::text
                )
        END
        ORDER BY t.idx
    ), '[]'::jsonb)
    INTO v_results
    FROM tmp_items t;

    RETURN v_results;
END;
$$;

GRANT EXECUTE ON FUNCTION queen.push_messages_v2(jsonb, boolean, boolean) TO PUBLIC;

-- ============================================================================
-- push_messages_v3: TEMP-TABLE-free batch push
-- ============================================================================
--
-- Rewrite of push_messages_v2 that eliminates the TEMP TABLE + ALTER DDL
-- pattern. The body runs as three statements:
--
--   A) Upsert queues from a DISTINCT scan of the JSON input.
--   B) Upsert partitions from a DISTINCT scan joined against queues.
--   C) Single data-modifying-CTE pipeline that parses the JSON once, resolves
--      partition_id, computes an intra-batch dup_rank via ROW_NUMBER, INSERTs
--      the dup_rank=1 rows with ON CONFLICT DO NOTHING, then aggregates one
--      result row per input item (queued | duplicate).
--
-- Properties:
--   - Zero catalog writes per call (no CREATE TEMP, no ALTER, no DROP).
--   - Single JSONB parse pass (vs four in v2).
--   - Single INSERT INTO queen.messages statement so the statement-level
--     trigger trg_update_partition_lookup still observes the full batch via
--     REFERENCING NEW TABLE AS new_messages.
--   - Relies on the UNIQUE INDEX messages_partition_transaction_unique for
--     correctness; the scalar response annotation is a pure lookup, not a
--     correctness mechanism.
--
-- External contract (signature + response JSON shape) matches v2. The only
-- intentional divergence: when p_check_duplicates=false, v2's legacy path
-- could abort a whole batch on a pre-existing duplicate via unique-index
-- violation; v3 always returns status='duplicate' for those rows. Callers
-- that relied on whole-batch abort (unknown in practice) would notice.
--
-- Parameter p_check_duplicates is accepted for backward-compat only; it no
-- longer toggles behavior.
-- Parameter p_check_capacity remains unused (same as v2).

CREATE OR REPLACE FUNCTION queen.push_messages_v3(
    p_items jsonb,
    p_check_duplicates boolean DEFAULT true,
    p_check_capacity boolean DEFAULT true  -- Ignored for performance
)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
    -- Lean per-item metadata that survives between statements. We explicitly
    -- keep `payload` OUT of this jsonb to avoid round-tripping large blobs.
    v_items    jsonb;
    -- Map of "partition_id<US>transaction_id" -> inserted message id::text.
    -- Only contains keys we actually inserted in Statement C.
    v_inserted jsonb;
    -- PUSHPOPLOOKUPSOL: per-partition {queue_name, partition_id, last_message_id,
    -- last_message_created_at} for the rows we just inserted. Returned as the
    -- `partition_updates` field of the response so libqueen can fire a
    -- follow-up queen.update_partition_lookup_v1() call after push commits.
    -- Empty array (not NULL) when no rows were actually inserted.
    v_partition_updates jsonb;
    v_results  jsonb;
    -- PUSHSER: loop var for sorted per-partition advisory-lock acquisition.
    v_lock_pid uuid;
BEGIN
    -- Statement A: ensure queues exist (single DISTINCT scan of input).
    INSERT INTO queen.queues (name, namespace, task)
    SELECT DISTINCT
        item->>'queue',
        COALESCE(item->>'namespace', split_part(item->>'queue', '.', 1)),
        COALESCE(item->>'task',
            CASE WHEN position('.' in item->>'queue') > 0
                 THEN split_part(item->>'queue', '.', 2)
                 ELSE '' END)
    FROM jsonb_array_elements(p_items) AS item
    ON CONFLICT (name) DO NOTHING;

    -- Statement B: ensure partitions exist.
    INSERT INTO queen.partitions (queue_id, name)
    SELECT DISTINCT q.id, COALESCE(item->>'partition', 'Default')
    FROM jsonb_array_elements(p_items) AS item
    JOIN queen.queues q ON q.name = item->>'queue'
    ON CONFLICT (queue_id, name) DO NOTHING;

    -- Statement B.5 (PUSHSER): take the push-partition advisory lock for every
    -- distinct partition in this batch, in ascending partition_id order. The
    -- deterministic order makes concurrent multi-partition pushes (and cross-
    -- instance pushes) deadlock-free. Held until commit; the created_at stamped
    -- in Statement C is therefore assigned in per-partition commit order. A FOR
    -- loop (not a set-returning PERFORM) guarantees the locks are acquired in the
    -- sorted order regardless of planner choices.
    FOR v_lock_pid IN
        SELECT DISTINCT p.id
        FROM jsonb_array_elements(p_items) AS item
        JOIN queen.queues     q ON q.name     = item->>'queue'
        JOIN queen.partitions p ON p.queue_id = q.id
                               AND p.name     = COALESCE(item->>'partition', 'Default')
        ORDER BY p.id
    LOOP
        PERFORM queen.lock_push_partition(v_lock_pid);
    END LOOP;

    -- Statement C: parse + resolve partition_id + compute dup_rank + INSERT.
    --
    -- This single statement performs the write. We capture *two* jsonb
    -- aggregations from the same CTE graph:
    --   - `v_items`: lean metadata (idx, message_id, transaction_id,
    --     partition_id, trace_id, dup_rank) for every input row, used to
    --     build the response in Statement D.
    --   - `v_inserted`: map of composite-key -> inserted-message-id for rows
    --     that actually got inserted.
    --
    -- `parsed` is declared AS MATERIALIZED so gen_random_uuid() is evaluated
    -- exactly once per input row. Without this hint a future PG version could
    -- inline the CTE and re-evaluate the volatile function per reference,
    -- breaking intra-batch dedup when transactionId is missing.
    --
    -- IMPORTANT: the composite-key lookup of duplicates for non-inserted rows
    -- is intentionally NOT done in this statement. Under concurrent callers
    -- racing on the same (partition_id, transaction_id), ON CONFLICT DO
    -- NOTHING makes the loser skip the insert, but the winning row committed
    -- by the other transaction is NOT visible in the loser's statement
    -- snapshot. Resolving the winner id must happen in a separate statement
    -- (D below) so it runs under a fresh snapshot that does see the winner.
    WITH parsed AS MATERIALIZED (
        SELECT
            (t.idx - 1)::int                                   AS idx,
            COALESCE((t.item->>'messageId')::uuid,
                     gen_random_uuid())                        AS message_id,
            COALESCE(t.item->>'transactionId',
                     gen_random_uuid()::text)                  AS transaction_id,
            t.item->>'queue'                                   AS queue_name,
            COALESCE(t.item->>'partition', 'Default')          AS partition_name,
            COALESCE(t.item->'payload', '{}'::jsonb)           AS payload,
            NULLIF(t.item->>'traceId', '')::uuid               AS trace_id,
            COALESCE((t.item->>'is_encrypted')::boolean, false) AS is_encrypted,
            -- producerSub is server-stamped (from validated JWT 'sub' claim);
            -- empty string must map to SQL NULL.
            NULLIF(t.item->>'producerSub', '')                 AS producer_sub
        FROM jsonb_array_elements(p_items) WITH ORDINALITY AS t(item, idx)
    ),
    items AS (
        SELECT
            p.id AS partition_id,
            -- Per-queue metrics attribution (PR 3c followup): keep queue_name
            -- alongside partition_id so the response can echo it per item.
            -- libqueen's PUSH handler aggregates by this field to credit
            -- queen.queue_lag_metrics.push_message_count per queue. Without
            -- it the per-queue Push/s chart on the System view stays at 0
            -- (the route can't put a single queue_name on the JobRequest
            -- because a push call may target many queues).
            par.queue_name,
            par.idx,
            par.message_id,
            par.transaction_id,
            par.payload,
            par.trace_id,
            par.is_encrypted,
            par.producer_sub,
            ROW_NUMBER() OVER (
                PARTITION BY p.id, par.transaction_id
                ORDER BY par.idx
            ) AS dup_rank
        FROM parsed par
        JOIN queen.queues     q ON q.name     = par.queue_name
        JOIN queen.partitions p ON p.queue_id = q.id
                               AND p.name     = par.partition_name
    ),
    inserted AS (
        INSERT INTO queen.messages
            (id, transaction_id, partition_id, payload,
             trace_id, is_encrypted, producer_sub, created_at)
        -- PUSHSER: created_at = clock_timestamp() (read NOW, under the partition
        -- locks acquired in Statement B.5), NOT the txn-start NOW() default. With
        -- the per-partition lock serializing commits, this makes created_at
        -- monotonic in commit order per partition.
        SELECT message_id, transaction_id, partition_id, payload,
               trace_id, is_encrypted, producer_sub, clock_timestamp()
        FROM items
        WHERE dup_rank = 1
        ON CONFLICT (partition_id, transaction_id) DO NOTHING
        -- PUSHPOPLOOKUPSOL: also RETURN created_at so we can build
        -- partition_updates below for the post-commit partition_lookup refresh.
        RETURNING id, transaction_id, partition_id, created_at
    ),
    items_json AS (
        SELECT jsonb_agg(jsonb_build_object(
            'idx',            idx,
            'queue_name',     queue_name,
            'message_id',     message_id::text,
            'transaction_id', transaction_id,
            'partition_id',   partition_id::text,
            'trace_id',       trace_id::text,
            'dup_rank',       dup_rank
        )) AS v FROM items
    ),
    inserted_json AS (
        SELECT COALESCE(
            jsonb_object_agg(
                partition_id::text || E'\x1f' || transaction_id,
                id::text
            ),
            '{}'::jsonb
        ) AS v FROM inserted
    ),
    -- PUSHPOPLOOKUPSOL: DISTINCT ON (partition_id) over the rows we just
    -- inserted, picking the latest (created_at, id) tuple per partition.
    -- This mirrors what the old trigger's `batch_max` CTE computed.
    partition_max AS (
        SELECT DISTINCT ON (partition_id)
            partition_id,
            id         AS last_message_id,
            created_at AS last_message_created_at
        FROM inserted
        ORDER BY partition_id, created_at DESC, id DESC
    ),
    partition_updates_json AS (
        SELECT COALESCE(
            jsonb_agg(jsonb_build_object(
                'queue_name',              q.name,
                'partition_id',            pm.partition_id::text,
                'last_message_id',         pm.last_message_id::text,
                'last_message_created_at', pm.last_message_created_at
            )),
            '[]'::jsonb
        ) AS v
        FROM partition_max pm
        JOIN queen.partitions p ON p.id       = pm.partition_id
        JOIN queen.queues     q ON q.id       = p.queue_id
    )
    SELECT items_json.v, inserted_json.v, partition_updates_json.v
    INTO v_items, v_inserted, v_partition_updates
    FROM items_json, inserted_json, partition_updates_json;

    -- Fast-exit for empty input (jsonb_agg returns NULL on empty set).
    IF v_items IS NULL THEN
        RETURN jsonb_build_object(
            'items',             '[]'::jsonb,
            'partition_updates', '[]'::jsonb
        );
    END IF;

    -- Statement D: build the response under a FRESH statement snapshot.
    --
    -- Performance note: we intentionally LOOK UP ONLY the keys that we did
    -- NOT insert ourselves. In the common path (all-new batch, no
    -- concurrency) v_inserted already covers every item and this CTE hits
    -- zero rows in queen.messages. We only probe the unique index for the
    -- tail cases:
    --   - intra-batch duplicates (dup_rank > 1 items whose dup_rank=1
    --     sibling was inserted — their id is already in v_inserted, so this
    --     branch still skips the DB); or
    --   - cross-call losers: dup_rank=1 items that hit ON CONFLICT because
    --     a concurrent transaction won the race. These are absent from
    --     v_inserted and require a lookup.
    -- The fresh snapshot of Statement D is what makes the concurrent winner
    -- visible (unlike a scalar subquery inside Statement C's CTE graph).
    WITH flat AS (
        SELECT
            (i->>'idx')::int          AS idx,
            i->>'queue_name'          AS queue_name,
            i->>'message_id'          AS message_id,
            i->>'transaction_id'      AS transaction_id,
            (i->>'partition_id')::uuid AS partition_id,
            NULLIF(i->>'trace_id', '') AS trace_id,
            (i->>'dup_rank')::int     AS dup_rank,
            -- Composite key matching the one v_inserted was built with.
            i->>'partition_id' || E'\x1f' || (i->>'transaction_id') AS key
        FROM jsonb_array_elements(v_items) AS i
    ),
    missing_keys AS (
        -- One row per distinct (partition_id, transaction_id) whose winner
        -- we still need to resolve. Empty set on the fast path.
        SELECT DISTINCT partition_id, transaction_id, key
        FROM flat
        WHERE NOT (v_inserted ? key)
    ),
    missing_lookup AS (
        SELECT mk.key, m.id::text AS id
        FROM missing_keys mk
        JOIN queen.messages m
          ON m.partition_id   = mk.partition_id
         AND m.transaction_id = mk.transaction_id
    ),
    winners AS (
        -- Exactly one row: merged map of self-inserted ids plus resolved
        -- duplicates. Always produced even when missing_lookup is empty.
        SELECT
            v_inserted
            || (SELECT COALESCE(jsonb_object_agg(key, id), '{}'::jsonb)
                FROM missing_lookup)
            AS m
    )
    SELECT COALESCE(jsonb_agg(
        CASE
            WHEN (v_inserted ? f.key) AND f.dup_rank = 1 THEN
                jsonb_build_object(
                    'index',          f.idx,
                    'transaction_id', f.transaction_id,
                    'status',         'queued',
                    'message_id',     v_inserted ->> f.key,
                    'partition_id',   f.partition_id::text,
                    'trace_id',       f.trace_id,
                    -- queueName: per-item attribution for libqueen's PUSH
                    -- handler. See the `items` CTE comment above.
                    'queueName',      f.queue_name
                )
            ELSE
                jsonb_build_object(
                    'index',          f.idx,
                    'transaction_id', f.transaction_id,
                    'status',         'duplicate',
                    'message_id',     w.m ->> f.key,
                    'queueName',      f.queue_name
                )
        END
        ORDER BY f.idx
    ), '[]'::jsonb)
    INTO v_results
    FROM flat f CROSS JOIN winners w;

    -- PUSHPOPLOOKUPSOL: response shape is now an object with two fields:
    --   items:             the per-input-row result array (as before).
    --   partition_updates: per-partition max {queue_name, partition_id,
    --                      last_message_id, last_message_created_at} for the
    --                      rows actually inserted by this call. Empty array
    --                      when every input row was a duplicate.
    -- The libqueen PUSH handler unwraps `items` before returning to the HTTP
    -- caller and fires a follow-up SELECT queen.update_partition_lookup_v1()
    -- with `partition_updates`.
    RETURN jsonb_build_object(
        'items',             COALESCE(v_results,          '[]'::jsonb),
        'partition_updates', COALESCE(v_partition_updates, '[]'::jsonb)
    );
END;
$$;

GRANT EXECUTE ON FUNCTION queen.push_messages_v3(jsonb, boolean, boolean) TO PUBLIC;
