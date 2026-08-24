-- ============================================================================
-- Log engine (18-log-engine.md §9) — admin surface.
-- ============================================================================
-- Port of the seg parts of three retired seg-era files:
--   * the retired seg_consumer_groups file — the MUTATING consumer-group
--     management ops
--     (delete a group, seek its cursor per-queue and per-partition) plus the
--     shared one-cursor seek helper;
--   * the retired storage_v2_observability file — the v2 branches of the
--     redefinitions the Rust broker actually calls: get_consumer_groups_v4
--     (group listing), list_messages_v1 (messages browsing) and
--     get_dlq_messages_v1 (DLQ browsing). NOT ported: get_queue_messages_v1 /
--     has_pending_messages / get_prometheus_metrics_v1 — the first two have no
--     Rust call site (C++-era routes; the pop path's pending check is
--     004_log_pop's log_has_pending_v1), prometheus support belongs to
--     011_log_stats;
--   * the retired seg_traces file — the engine-agnostic record_trace_v1
--     redefinition + the message_traces FK decoupling. seg_traces itself
--     referenced no seg tables, but its redefinition had to survive that
--     file's deletion or tracing a log message would regress to the old traces
--     file's "Message not found" bail (017_traces no longer defines
--     record_trace_v1 — this file owns it).
--
-- Positions are re-addressed to single per-partition BIGINT offsets. Every
-- JSON key is UNCHANGED (§11): keys that used to carry a segment seq now carry
-- base_offset, and frameIdx carries (offset - base_offset) — clients treat
-- both as opaque.
--
-- 2026-07-30 — SINGLE ENGINE. The rows-engine legs are gone from this file. They
-- came in two shapes, and it is worth being precise about which:
--
--   * get_consumer_groups_v4 and list_messages_v1 carried a real second branch,
--     guarded on queen.queues.storage. No queue can hold any value but
--     'segments' (schema default + /configure + mark_queue_segments, §11), so
--     the branch was unreachable; it is deleted and the log body now runs
--     unconditionally.
--   * get_lagging_partitions_v1, get_consumer_group_details_v1 and
--     record_trace_v1 had no guard at all — they read queen.partition_consumers
--     / queen.messages outright, over tables that were always empty. Those legs
--     are deleted too.
--
-- The remaining four functions (log_delete_consumer_group_v1, log_seek_one_v1,
-- log_seek_consumer_group_v1, log_seek_partition_v1) never had a rows branch.
--
-- 2026-07-31 — QUEUE IDENTITY MERGE. queen.log_queues is gone: queue identity
-- is the queen.queues id, and log_partitions.queue_id references it directly.
-- The former name-join to queen.queues (with its `storage = 'segments'`
-- predicate, kept as a queue-existence guard) is gone too — with one table the
-- JOIN to queen.queues on p.queue_id IS the existence guard, and it supplies
-- namespace/task/priority/tenant_id in the same touch. consumer_watermarks is
-- keyed by (queue_id, consumer_group) now, so the watermark cleanups below
-- resolve the queue through queen.queues instead of matching queue_name.
--
-- No JSON key changed — the deleted branches only ever contributed an empty
-- array / empty object to the result.
--
-- Applied idempotently at boot after the 001-007 engine files (lexical order):
-- every log_* table it references exists by 005_log_ack (log_dlq).
-- ============================================================================

-- ============================================================================
-- queen.hotlist_repair_publish_v1: announce that a (queue, group)'s cursors moved
-- BACKWARDS, durably, to every broker (A3, PLAN_HOTLIST_FOLLOWUP.md).
--
-- Called from inside the operation that moves the cursor, so the announcement shares
-- its transaction: a marker written as a second statement afterwards is lost if the
-- broker dies in the gap, which is the one property this exists to provide. Being SQL
-- rather than broker code also means an operator (or an older broker) calling the seek
-- functions directly still publishes.
--
-- p_partition scopes the repair: a name marks that one partition on the reader's ring,
-- NULL makes it walk the queue. On conflict with a pending repair of a DIFFERENT
-- partition the scope WIDENS to NULL — the primary key is what keeps this table at one
-- row per (tenant, queue, group), so the alternative to widening is dropping one of the
-- two repairs, and over-repairing costs one empty SKIP LOCKED probe per partition while
-- under-repairing costs a replay.
--
-- The prune rides along: a broker offline longer than the window cold-starts every ring
-- it holds anyway (full_reseed_ms = 0), so an hour is generous rather than load-bearing.
-- Callers publishing several queues at once call this per queue and re-run the prune,
-- which is an indexed no-op after the first.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.hotlist_repair_publish_v1(
    p_tenant UUID,
    p_queue TEXT,
    p_group TEXT,
    p_partition TEXT,
    p_reason TEXT
) RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO queen.hotlist_repairs AS r
        (tenant_id, queue_name, consumer_group, partition_name, repair_at, reason)
    VALUES (p_tenant, p_queue, p_group, p_partition, now(), p_reason)
    ON CONFLICT (tenant_id, queue_name, consumer_group) DO UPDATE SET
        partition_name = CASE
            WHEN r.partition_name IS NOT DISTINCT FROM EXCLUDED.partition_name
            THEN r.partition_name ELSE NULL END,
        repair_at = EXCLUDED.repair_at,
        reason = EXCLUDED.reason;

    DELETE FROM queen.hotlist_repairs WHERE repair_at < now() - interval '1 hour';
END;
$$;

-- ============================================================================
-- queen.log_delete_consumer_group_v1: drop a group's LOG cursor state.
-- Removes every queen.log_consumers row for the group (across all partitions
-- of every queue) and its per-(queue, group) empty-scan watermarks. The shared
-- coordination tables (queen.consumer_groups_metadata /
-- queen.consumer_watermarks) are handled by the sibling
-- queen.delete_consumer_group_v1 the broker also calls — p_delete_metadata is
-- echoed for a consistent response shape. deletedPartitions = number of
-- log_consumers cursors removed.
--
-- There is NO engine-scoping join: queen.log_consumers holds the only cursors
-- that exist, so the plain group predicate is already exact.
-- ============================================================================
-- Track B (§5): p_tenant scopes the cross-queue group delete to ONE tenant's
-- cursors/watermarks — the old body deleted every tenant's rows for the name.
CREATE OR REPLACE FUNCTION queen.log_delete_consumer_group_v1(
    p_group TEXT,
    p_delete_metadata BOOLEAN DEFAULT TRUE,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001'
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted INTEGER;
    v_queues TEXT[];
    v_q TEXT;
BEGIN
    -- Queue identity is the queen.queues id now (log_queues is merged away).
    --
    -- A3/A1: the delete RETURNS the queues it touched, so the announcement below costs
    -- no second pass over log_consumers (a group predicate alone is not indexed — the
    -- PK is (partition_id, consumer_group) — so re-deriving the set would repeat this
    -- statement's whole scan).
    WITH del AS (
        DELETE FROM queen.log_consumers c
        USING queen.log_partitions p
        JOIN queen.queues q ON q.id = p.queue_id
        WHERE c.partition_id = p.id AND c.consumer_group = p_group
          AND q.tenant_id = p_tenant
        RETURNING q.name AS queue_name
    )
    SELECT count(*), array_agg(DISTINCT queue_name) INTO v_deleted, v_queues FROM del;

    -- One repair per queue whose cursors actually went. A queue where the group held
    -- none needs none: with no row, COALESCE(c.committed, -1) already read -1 there, so
    -- this delete changed nothing a ring could be wrong about. Locally the ring is
    -- dropped by the handler and cold-starts; this is for the PEERS, which keep a
    -- stamped ring for the same group name and cannot see a pendingness no write
    -- created. Same transaction as the delete, so no broker can read one without the
    -- other.
    IF v_queues IS NOT NULL THEN
        FOREACH v_q IN ARRAY v_queues LOOP
            PERFORM queen.hotlist_repair_publish_v1(
                p_tenant, v_q, p_group, NULL, 'consumer-group-delete');
        END LOOP;
    END IF;

    -- Drop the empty-scan watermark so a later group of the same name does not
    -- inherit a stale "queue is empty since T" floor and silently skip cold
    -- partitions (symmetric with the watermark cleanup in log_seek_* below).
    -- Watermarks are keyed by queue_id; tenant scoping is inherited from queues.
    DELETE FROM queen.consumer_watermarks w
    USING queen.queues q
    WHERE w.queue_id = q.id AND w.consumer_group = p_group
      AND q.tenant_id = p_tenant;

    RETURN jsonb_build_object(
        'success', true,
        'consumerGroup', p_group,
        'deletedPartitions', v_deleted,
        'metadataDeleted', p_delete_metadata
    );
END;
$$;

-- ============================================================================
-- Cursor seek — shared helper: position ONE (partition, group) log cursor.
--   p_to_end     -> committed = last_offset (skip everything: next wanted =
--                   last_offset + 1, exactly the seg version's last_seq+1/0).
--   p_timestamp  -> committed = first base_offset with created_at >=
--                   p_timestamp, minus 1 (created_at is monotone in
--                   base_offset — the PUSHSER invariant — so a forward walk
--                   from the retention watermark log_start finds the boundary;
--                   the same walk 004_log_pop uses for timestamp subscriptions
--                   and 006_log_maintenance uses for the retention boundary).
--                   committed =
--                   last_offset when nothing that recent exists yet
--                   (future-only cursor). Timestamp resolution is segment-
--                   granular, exactly like the seg version's (seq, off=0).
-- Any live lease is released and the retry/redelivery state reset; the row is
-- created when the group has never popped this partition (INSERT ... ON
-- CONFLICT DO UPDATE).
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.log_seek_one_v1(
    p_partition_id UUID,
    p_last_offset BIGINT,
    p_log_start BIGINT,
    p_group TEXT,
    p_to_end BOOLEAN,
    p_timestamp TIMESTAMPTZ
) RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_committed BIGINT;
BEGIN
    IF p_to_end THEN
        v_committed := p_last_offset;
    ELSE
        SELECT s.base_offset - 1 INTO v_committed
        FROM queen.log_segments s
        WHERE s.partition_id = p_partition_id
          AND s.base_offset >= p_log_start
          AND s.created_at >= p_timestamp
        ORDER BY s.base_offset LIMIT 1;
        IF v_committed IS NULL THEN
            v_committed := p_last_offset;
        END IF;
    END IF;

    INSERT INTO queen.log_consumers (
        partition_id, consumer_group, committed, batch_end,
        worker_id, lease_expires_at, lease_acquired_at,
        batch_retry_count, attempt_offset, attempt_count)
    VALUES (p_partition_id, p_group, v_committed, NULL,
            NULL, NULL, NULL, 0, NULL, 0)
    ON CONFLICT (partition_id, consumer_group) DO UPDATE SET
        committed = EXCLUDED.committed,
        batch_end = NULL,
        worker_id = NULL, lease_expires_at = NULL, lease_acquired_at = NULL,
        -- a seek releases whatever lease was held: no stale conflation marker
        -- may survive it (PLAN_CONFLATION §2.4)
        lease_conflated = FALSE,
        batch_retry_count = 0,
        attempt_offset = NULL, attempt_count = 0;
END;
$$;

-- ============================================================================
-- queen.log_seek_consumer_group_v1: seek the log cursor for EVERY partition
-- of a queue. Body of the POST .../seek route (per-queue variant).
-- ============================================================================
-- Track B (§5): p_tenant scopes the per-queue seek + watermark clear.
CREATE OR REPLACE FUNCTION queen.log_seek_consumer_group_v1(
    p_group TEXT,
    p_queue TEXT,
    p_to_end BOOLEAN DEFAULT FALSE,
    p_timestamp TIMESTAMPTZ DEFAULT NULL,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001'
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_p RECORD;
    v_updated INTEGER := 0;
BEGIN
    IF NOT p_to_end AND p_timestamp IS NULL THEN
        RETURN jsonb_build_object(
            'success', false,
            'error', 'Must specify either toEnd=true or a timestamp');
    END IF;

    FOR v_p IN
        SELECT p.id, p.last_offset, p.log_start
        FROM queen.log_partitions p
        -- Queue identity is the queen.queues id now (log_queues is merged away).
        JOIN queen.queues q ON q.id = p.queue_id
        WHERE q.name = p_queue AND q.tenant_id = p_tenant
    LOOP
        PERFORM queen.log_seek_one_v1(v_p.id, v_p.last_offset, v_p.log_start,
                                      p_group, p_to_end, p_timestamp);
        v_updated := v_updated + 1;
    END LOOP;

    -- No partition matched: the queue does not exist for this tenant (or has no
    -- partitions yet). Reporting success with partitionsUpdated=0 makes a
    -- misdirected seek look like it worked, so fail loudly instead — the route
    -- layer maps an `error` key to a non-200.
    IF v_updated = 0 THEN
        RETURN jsonb_build_object(
            'success', false,
            'error', 'Queue not found or has no partitions',
            'consumerGroup', p_group,
            'queueName', p_queue,
            'partitionsUpdated', 0);
    END IF;

    -- Clear the empty-scan watermark so a backward seek re-exposes cold
    -- partitions to the wildcard candidate filter on the next pop.
    -- Watermarks are keyed by queue_id; resolve it by (name, tenant).
    DELETE FROM queen.consumer_watermarks w
    USING queen.queues q
    WHERE w.queue_id = q.id AND q.name = p_queue AND q.tenant_id = p_tenant
      AND w.consumer_group = p_group;

    -- A3: publish the repair in the seek's own transaction. Unconditionally, including
    -- toEnd: deciding whether a cursor actually moved BACKWARDS means making
    -- log_seek_one_v1 report per partition, which changes its return type (a DROP +
    -- CREATE migration hazard) to save a handful of walks on an operation that happens
    -- a few times a day. If precision is ever wanted, that is where it goes.
    PERFORM queen.hotlist_repair_publish_v1(p_tenant, p_queue, p_group, NULL, 'seek-queue');

    RETURN jsonb_build_object(
        'success', true,
        'consumerGroup', p_group,
        'queueName', p_queue,
        'seekToEnd', p_to_end,
        'targetTimestamp', CASE WHEN p_timestamp IS NOT NULL
            THEN to_char(p_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
        'partitionsUpdated', v_updated
    );
END;
$$;

-- ============================================================================
-- queen.log_seek_partition_v1: seek the log cursor for ONE named partition.
-- Body of the POST .../partitions/:partition/seek route.
-- ============================================================================
-- Track B (§5): p_tenant scopes the partition resolution + watermark clear.
CREATE OR REPLACE FUNCTION queen.log_seek_partition_v1(
    p_group TEXT,
    p_queue TEXT,
    p_partition TEXT,
    p_to_end BOOLEAN DEFAULT FALSE,
    p_timestamp TIMESTAMPTZ DEFAULT NULL,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001'
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_id UUID;
    v_last_offset BIGINT;
    v_log_start BIGINT;
BEGIN
    IF NOT p_to_end AND p_timestamp IS NULL THEN
        RETURN jsonb_build_object(
            'success', false,
            'error', 'Must specify either toEnd=true or a timestamp');
    END IF;

    SELECT p.id, p.last_offset, p.log_start
    INTO v_id, v_last_offset, v_log_start
    FROM queen.log_partitions p
    -- Queue identity is the queen.queues id now (log_queues is merged away).
    JOIN queen.queues q ON q.id = p.queue_id
    WHERE q.name = p_queue AND p.name = p_partition AND q.tenant_id = p_tenant;

    IF v_id IS NULL THEN
        RETURN jsonb_build_object('success', false, 'error', 'Partition not found');
    END IF;

    PERFORM queen.log_seek_one_v1(v_id, v_last_offset, v_log_start,
                                  p_group, p_to_end, p_timestamp);

    -- Watermarks are keyed by queue_id; resolve it by (name, tenant).
    DELETE FROM queen.consumer_watermarks w
    USING queen.queues q
    WHERE w.queue_id = q.id AND q.name = p_queue AND q.tenant_id = p_tenant
      AND w.consumer_group = p_group;

    -- A3, scoped to the one partition that moved: a reader marks exactly it, which is
    -- what the seeking broker did locally (A2) and what its mesh hint carried. The
    -- whole-queue walk only comes back if a SECOND partition of the same (queue, group)
    -- is seeked before the readers catch up (hotlist_repair_publish_v1 widens).
    PERFORM queen.hotlist_repair_publish_v1(
        p_tenant, p_queue, p_group, p_partition, 'seek-partition');

    RETURN jsonb_build_object(
        'success', true,
        'consumerGroup', p_group,
        'queueName', p_queue,
        'partitionName', p_partition,
        'seekToEnd', p_to_end,
        'targetTimestamp', CASE WHEN p_timestamp IS NOT NULL
            THEN to_char(p_timestamp, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
        'updated', true
    );
END;
$$;

-- ============================================================================
-- queen.get_consumer_groups_v4 — LOG redefinition.
-- Groups are computed from queen.log_consumers / log_partitions. The former
-- rows-engine block (a CTE pipeline over the dropped rows coordination tables,
-- prepended to this array and always empty in practice) is deleted. Entries
-- carry the historical keys plus "storage":"segments" (the wire storage value,
-- §11); totalLag IS populated — with offsets it is pure
-- arithmetic (§9): pending per (partition, group) =
--   GREATEST(last_offset - GREATEST(committed, log_start - 1), 0)
-- — NO log_segments scan for the count. maxTimeLag = age of the oldest
-- unconsumed segment (a covering-segment PK probe per lagging cursor — the
-- only segment touch here; rationale at the probe). Subscription metadata does
-- not exist per-partition in the log engine -> nulls, as before.
-- ============================================================================
-- Track B (§5): p_tenant scopes the (global, cross-queue) group listing to one
-- tenant's queues — two tenants can run the same group name on their own queues.
CREATE OR REPLACE FUNCTION queen.get_consumer_groups_v4(
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001')
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_v2 JSONB;
BEGIN
    -- The rows-engine CTE pipeline (cg_metadata / consumer_data / aggregated,
    -- over the dropped coordination tables) that produced the first half of the
    -- array is gone; the log block below is the whole answer.
    -- -------------------------------------------------------------- log (v2)
    -- Pending frames are pure allocator/cursor arithmetic (§9) — the seg
    -- version's pre-aggregated segments×consumers SUM is gone. Only the time
    -- lag still touches log_segments, via the covering probe in v2_data.
    --
    -- THE PROBE, not a MIN over the unconsumed range (2026-08-24, prod at 60k
    -- partitions). The former free-standing lag CTE — MIN(created_at) over
    -- end_offset > committed, grouped per (partition, group) — had no usable
    -- index (log_segments deliberately carries only its (partition_id,
    -- base_offset) PK), so the planner hash-joined a SEQ SCAN OF THE ENTIRE
    -- SEGMENTS HEAP on every console load, cross-tenant (the tenant join sat
    -- outside the GROUP BY, unreachable for pushdown), and a never-consumed
    -- cursor (committed=-1: an abandoned group) fed every retained segment of
    -- its every partition into the aggregate. Measured on a 60k-partition /
    -- 6M-segment rig: 3.7-6.3s and ~1.9 GB of buffers per call — evicting the
    -- pop path's working set as a side effect. Probe form, same rig: 0.6-1.1s,
    -- output verified field-identical.
    --
    -- Correct because segments append in offset order, so created_at is
    -- monotone in base_offset and the OLDEST unconsumed segment is the one
    -- covering committed+1 — or the first after it when the cursor sits on a
    -- segment boundary (an NTP slew can bend the monotonicity by seconds; the
    -- 300s Lagging threshold absorbs that). Two O(log n) PK probes, and only
    -- for cursors with pending > 0: a caught-up cursor provably has no
    -- unconsumed segment (every end_offset <= last_offset <= committed).
    WITH v2_base AS (
        SELECT c.partition_id, c.consumer_group, c.committed, c.total_consumed,
               q.name AS queue_name,
               -- committed=-1 (never consumed) counts the whole live log;
               -- log_start-1 dominates once retention has eaten past the
               -- cursor (evicted frames are not lag).
               GREATEST(p.last_offset - GREATEST(c.committed, p.log_start - 1), 0) AS pending
        FROM queen.log_consumers c
        JOIN queen.log_partitions p ON p.id = c.partition_id
        -- Queue identity is the queen.queues id now: this join IS the
        -- queue-existence guard (the old name-join with its storage='segments'
        -- predicate is merged into it). Track B: scoped to p_tenant — and the
        -- probe below sits after this join, so other tenants' cursors are
        -- never priced.
        JOIN queen.queues q ON q.id = p.queue_id AND q.tenant_id = p_tenant
    ),
    v2_data AS (
        SELECT b.consumer_group, b.queue_name, b.total_consumed, b.pending,
               l.oldest_unconsumed_at
        FROM v2_base b
        LEFT JOIN LATERAL (
            -- The inner probe pins the scan start at the covering segment, so
            -- a lagging cursor never walks its consumed-but-retained prefix;
            -- COALESCE 0 = retention already deleted past the cursor, start at
            -- the log head. LIMIT 1 keeps the join row-preserving.
            SELECT s2.created_at AS oldest_unconsumed_at
            FROM queen.log_segments s2
            WHERE b.pending > 0
              AND s2.partition_id = b.partition_id
              AND s2.base_offset >= COALESCE((
                    SELECT s1.base_offset
                    FROM queen.log_segments s1
                    WHERE s1.partition_id = b.partition_id
                      AND s1.base_offset <= b.committed + 1
                    ORDER BY s1.base_offset DESC
                    LIMIT 1), 0)
              AND s2.end_offset > b.committed
            ORDER BY s2.base_offset
            LIMIT 1
        ) l ON TRUE
    ),
    -- PLAN_CONFLATION §2.6 (fixes M7). The three subscription fields below were
    -- hard-coded NULL, so subscription mode had no console surface at all — and
    -- a group-level policy the group view cannot display repeats that mistake.
    -- One indexed cgm_identity_uk lookup per aggregated group, on a console route.
    cgm AS (
        SELECT m.consumer_group, q.name AS queue_name,
               m.subscription_mode, m.subscription_timestamp, m.created_at,
               m.conflation
        FROM queen.consumer_groups_metadata m
        JOIN queen.queues q ON q.id = m.queue_id AND q.tenant_id = p_tenant
        WHERE m.partition_name = ''
    ),
    v2_aggregated AS (
        SELECT consumer_group,
               queue_name,
               COUNT(*) AS member_count,
               SUM(CASE WHEN pending > 0 THEN 1 ELSE 0 END) AS partitions_with_lag,
               SUM(pending) AS total_lag,
               COALESCE(MAX(EXTRACT(EPOCH FROM (NOW() - oldest_unconsumed_at))::integer), 0) AS max_time_lag,
               CASE
                   WHEN COALESCE(MAX(EXTRACT(EPOCH FROM (NOW() - oldest_unconsumed_at))::integer), 0) > 300 THEN 'Lagging'
                   WHEN MAX(total_consumed) > 0 THEN 'Stable'
                   ELSE 'Dead'
               END AS state
        FROM v2_data
        GROUP BY consumer_group, queue_name
    )
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'name', a.consumer_group,
            'topics', jsonb_build_array(a.queue_name),
            'queueName', a.queue_name,
            'members', member_count,
            -- v2_data is one row per (partition, group) CURSOR, so member_count
            -- is the number of partitions this group has a cursor on, NOT the
            -- number of consumer processes (one consumer on a 32-partition queue
            -- counts 32). Exposed under its true name so a reader has something
            -- accurate to render; `members` is kept for wire compatibility.
            'partitionCursors', member_count,
            'partitionsWithLag', partitions_with_lag,
            'totalLag', total_lag,
            'maxTimeLag', max_time_lag,
            'state', state,
            'storage', 'segments',
            -- §2.6: the real durable declaration, no longer three NULLs.
            'subscriptionMode', g.subscription_mode,
            'subscriptionTimestamp', g.subscription_timestamp,
            'subscriptionCreatedAt', g.created_at,
            'conflation', COALESCE(g.conflation, false)
        ) ORDER BY a.consumer_group, a.queue_name
    ), '[]'::jsonb) INTO v_v2
    FROM v2_aggregated a
    LEFT JOIN cgm g ON g.consumer_group = a.consumer_group
                   AND g.queue_name = a.queue_name;

    -- Was `v_result || v_v2` with v_result the (always empty) rows array.
    RETURN v_v2;
END;
$$;

-- ============================================================================
-- queen.list_messages_v1 — LOG redefinition.
-- The former rows pipeline (016_messages's original body, redefined by the
-- retired storage_v2_observability file) that used to fill result.messages
-- ahead of the log
-- entries is deleted (its tables are gone); the log entries ARE result.messages
-- now. Per-message metadata comes from queen.log_txns exploded to frames (the
-- ONLY per-message data SQL has — 16B xxh3_128 per frame; mids and txn text
-- live inside the blob):
--   { id:null, transactionId:null, txnHash:"<32 hex>", partitionId, queuePath,
--     queue, partition, namespace, task, status, queueStatus, busStatus,
--     payloadAvailable:false, segment:{seq, frameIdx}, queuePriority,
--     traceId:null, producerSub:null, createdAt, leaseExpiresAt }
-- KEY SEMANTICS (§11): segment.seq carries base_offset and segment.frameIdx
-- carries (offset - base_offset) — exactly the address the route layer's
-- enrichment pass needs to fetch the covering blob (log_segments PK) and
-- decode the frame, which fills data/payload/transactionId (and can fill id:
-- the frame carries the mid — Rust phase). txnHash switches from the seg
-- engine's hashtextextended bigint-text to 16B hex; both are opaque tokens.
-- ROUTE-LAYER CONTRACT: entries exist only within the log_txns purge window
-- (GREATEST(dedup_window, completed_retention, 900s) — a wider net than the
-- seg engine's dedup-only window, and no longer dedup-gated). The page is
-- capped at v_limit/v_offset by the log query itself (the old two-engine page
-- could return up to 2x limit; a single engine cannot). 'mode' detection reads
-- queen.log_consumers only — the second probe over the rows coordination
-- tables, which reported 'none' for every log queue, is gone.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.list_messages_v1(p_filters JSONB DEFAULT '{}'::jsonb)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_from_ts TIMESTAMPTZ;
    v_to_ts TIMESTAMPTZ;
    v_queue TEXT;
    v_partition TEXT;
    v_namespace TEXT;
    v_task TEXT;
    v_status TEXT;
    v_limit INTEGER;
    v_offset INTEGER;
    v_has_queue_mode BOOLEAN;
    v_total_bus_groups INTEGER;
    v_v2 JSONB;
    -- Track B (§5): tenant travels in the filter JSON (`_tenant`), so the (JSONB)
    -- signature is unchanged. Absent ⇒ default tenant.
    v_tenant UUID := COALESCE((p_filters->>'_tenant')::uuid, '00000000-0000-0000-0000-000000000001');
BEGIN
    -- Parse filters
    -- from: inclusive of the exact timestamp
    v_from_ts := COALESCE((p_filters->>'from')::timestamptz, NOW() - INTERVAL '1 hour');
    -- to: inclusive of the entire minute (truncate to minute, add 1 minute, use < comparison)
    v_to_ts := date_trunc('minute', COALESCE((p_filters->>'to')::timestamptz, NOW())) + INTERVAL '1 minute';
    v_queue := p_filters->>'queue';
    v_partition := p_filters->>'partition';
    v_namespace := p_filters->>'namespace';
    v_task := p_filters->>'task';
    v_status := p_filters->>'status';
    v_limit := COALESCE((p_filters->>'limit')::integer, 200);
    v_offset := COALESCE((p_filters->>'offset')::integer, 0);

    -- Detect mode for the filtered messages: queen.log_consumers cursors are
    -- the only mode evidence there is (the twin probe over the rows
    -- coordination tables, and the merge of the two results, are deleted).
    SELECT
        bool_or(c.consumer_group = '__QUEUE_MODE__'),
        COUNT(DISTINCT c.consumer_group) FILTER (WHERE c.consumer_group != '__QUEUE_MODE__')
    INTO v_has_queue_mode, v_total_bus_groups
    FROM queen.log_consumers c
    JOIN queen.log_partitions p ON p.id = c.partition_id
    -- Queue identity is the queen.queues id now (log_queues is merged away).
    JOIN queen.queues q ON q.id = p.queue_id
    WHERE q.tenant_id = v_tenant
      AND (v_queue IS NULL OR q.name = v_queue)
      AND (v_partition IS NULL OR p.name = v_partition);

    -- The rows message pipeline (message_data / filtered_messages over the
    -- dropped message tables, whose page was emitted BEFORE the log entries) is
    -- deleted. The envelope it used to build — {messages, mode} — is now built
    -- at the RETURN below, straight over the log page.

    -- ------------------------------------------------- log-engine entries
    -- No index on queen.log_txns.created_at: the scan is bounded by the txns
    -- window's steady-state size (O(rate x window)) — acceptable for a
    -- dashboard listing, same trade the seg version made over seg_dedup.
    -- STATUS PRECEDENCE — identical to the message-DETAIL path (db.rs
    -- ::seg_message_detail's `g` CTE + handlers/messages.rs): a frame is
    -- completed once every NAMED consumer group has committed past its offset;
    -- the group-less '__QUEUE_MODE__' cursor decides only when the partition
    -- carries no named group at all. Deriving the status from the __QUEUE_MODE__
    -- row ALONE (what this select used to do) made every bus-consumed frame read
    -- 'pending' forever: a partition consumed by named groups has no
    -- __QUEUE_MODE__ row, so that join was all-NULLs, neither the completed nor
    -- the processing branch could fire, and the frame fell through to the ELSE —
    -- contradicting the busStatus counters built two lines below it, which DO
    -- count named groups ("pending" next to "1/1 groups"). dead_letter still
    -- wins (the quarantined head frame lives in queen.log_dlq addressed by
    -- offset).
    SELECT COALESCE(jsonb_agg(v2.obj ORDER BY v2.created_at DESC, v2.off DESC), '[]'::jsonb)
    INTO v_v2
    FROM (
        SELECT vd.created_at, vd.off,
            jsonb_build_object(
                'id', NULL,
                'transactionId', NULL,
                'txnHash', encode(vd.h, 'hex'),
                'partitionId', vd.partition_id,
                'queuePath', vd.queue_name || '/' || vd.partition_name,
                'queue', vd.queue_name,
                'partition', vd.partition_name,
                'namespace', vd.namespace,
                'task', vd.task,
                'status', vd.final_status,
                'queueStatus', vd.final_status,
                'busStatus', jsonb_build_object(
                    'consumedBy', vd.consumed_by_groups,
                    'totalGroups', vd.total_bus_groups
                ),
                'traceId', NULL,
                'producerSub', NULL,
                'queuePriority', vd.queue_priority,
                'payloadAvailable', false,
                'segment', jsonb_build_object(
                    'seq', vd.base_offset,
                    'frameIdx', (vd.off - vd.base_offset)::int),
                'createdAt', to_char(vd.created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                'leaseExpiresAt', CASE WHEN vd.lease_expires_at IS NOT NULL
                    THEN to_char(vd.lease_expires_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END
            ) AS obj
        FROM (
            SELECT t.base_offset, t.created_at,
                   t.base_offset + th.idx AS off, th.h,
                   p.id AS partition_id, p.name AS partition_name,
                   q.name AS queue_name,
                   q.namespace, q.task, q.priority AS queue_priority,
                   c.lease_expires_at,
                   CASE
                       WHEN EXISTS (SELECT 1 FROM queen.log_dlq dl
                                    WHERE dl.partition_id = t.partition_id
                                      AND dl."offset" = t.base_offset + th.idx)
                           THEN 'dead_letter'
                       -- named groups decide whenever the partition has any...
                       WHEN g.bus_groups > 0 AND g.bus_passed = g.bus_groups
                           THEN 'completed'
                       -- ...and the group-less cursor only when it has none.
                       WHEN g.bus_groups = 0 AND COALESCE(g.qmode_passed, false)
                           THEN 'completed'
                       WHEN COALESCE(g.any_lease_live, false)
                           THEN 'processing'
                       ELSE 'pending'
                   END AS final_status,
                   g.bus_passed::integer AS consumed_by_groups,
                   g.bus_groups::integer AS total_bus_groups
            FROM queen.log_txns t
            JOIN queen.log_partitions p ON p.id = t.partition_id
            -- Queue identity is the queen.queues id now: this join IS the
            -- queue-existence guard (the old name-join with its
            -- storage='segments' predicate is merged into it) and it supplies
            -- namespace/task/priority directly. Track B: scoped to v_tenant
            -- (from the `_tenant` filter key).
            JOIN queen.queues q ON q.id = p.queue_id AND q.tenant_id = v_tenant
            -- Kept for the emitted `leaseExpiresAt` ONLY (not for the status):
            -- that key means the queue-mode lease, exactly as the detail path's
            -- 'leaseExpiresAt' does (seg_message_detail's qmode_lease).
            LEFT JOIN queen.log_consumers c
                ON c.partition_id = p.id AND c.consumer_group = '__QUEUE_MODE__'
            CROSS JOIN LATERAL queen.log_unnest_hashes(t.hashes) th
            -- Per-frame group aggregate — the list-side twin of
            -- seg_message_detail's `g` CTE. LATERAL because the offset under test
            -- (base_offset + idx) only exists after the frame unnest. One indexed
            -- aggregate over the partition's cursors (PK-prefixed by
            -- partition_id) now answers all four questions and REPLACES the two
            -- correlated COUNT subqueries this select ran per frame, so the fix
            -- costs a scan less than the bug did. An aggregate with no GROUP BY
            -- always returns exactly one row (bus_groups 0, the bool_ors NULL,
            -- hence the COALESCEs above), so no frame can be dropped here.
            CROSS JOIN LATERAL (
                SELECT COUNT(*) FILTER (
                           WHERE cg.consumer_group <> '__QUEUE_MODE__')       AS bus_groups,
                       COUNT(*) FILTER (
                           WHERE cg.consumer_group <> '__QUEUE_MODE__'
                             AND t.base_offset + th.idx <= cg.committed)      AS bus_passed,
                       bool_or(cg.consumer_group = '__QUEUE_MODE__'
                               AND t.base_offset + th.idx <= cg.committed)    AS qmode_passed,
                       bool_or(cg.lease_expires_at IS NOT NULL
                               AND cg.lease_expires_at > NOW())               AS any_lease_live
                FROM queen.log_consumers cg
                WHERE cg.partition_id = t.partition_id
            ) g
            WHERE t.created_at >= v_from_ts AND t.created_at < v_to_ts
              AND (v_queue IS NULL OR q.name = v_queue)
              AND (v_partition IS NULL OR p.name = v_partition)
              AND (v_namespace IS NULL OR q.namespace = v_namespace)
              AND (v_task IS NULL OR q.task = v_task)
        ) vd
        WHERE (v_status IS NULL OR vd.final_status = v_status)
        ORDER BY vd.created_at DESC, vd.off DESC
        LIMIT v_limit OFFSET v_offset
    ) v2;

    -- Same envelope as before, with the (always empty) rows page dropped from
    -- the `messages` concatenation: '[]' || v_v2 = v_v2.
    RETURN jsonb_build_object(
        'messages', v_v2,
        'mode', jsonb_build_object(
            'hasQueueMode', COALESCE(v_has_queue_mode, false),
            'busGroupsCount', COALESCE(v_total_bus_groups, 0),
            'type', CASE
                WHEN NOT COALESCE(v_has_queue_mode, false) AND COALESCE(v_total_bus_groups, 0) > 0 THEN 'bus'
                WHEN COALESCE(v_has_queue_mode, false) AND COALESCE(v_total_bus_groups, 0) = 0 THEN 'queue'
                WHEN COALESCE(v_has_queue_mode, false) AND COALESCE(v_total_bus_groups, 0) > 0 THEN 'hybrid'
                ELSE 'none'
            END
        )
    );
END;
$$;

-- ============================================================================
-- queen.get_dlq_messages_v1 — LOG redefinition.
-- The former rows leg of the UNION (016_messages's original / the retired
-- storage_v2_observability file's, over the dropped dead-letter/message
-- tables) is deleted; queen.log_dlq is the only source left, keeps the exact
-- same key list, and LIMIT/OFFSET still page the ROWS (inside the subquery),
-- not the aggregate:
--   * payload snapshots live in queen.log_dlq itself (payload), no blob
--     decode needed — identical to the seg_dlq design;
--   * retryCount: the retries consumed when the frame was dead-lettered,
--     snapshotted onto the DLQ row by 005_log_ack's quarantine path (falls
--     back to the queue's retry_limit — the retired observability file's
--     pre-snapshot behavior — for rows
--     that predate the snapshot);
--   * partitionId: the log_partitions uuid (echoed by pop/ack wire calls);
--   * createdAt: blobs are opaque to SQL, the original enqueue time is not
--     recoverable per frame -> failed_at (the DLQ event time), as before;
--   * producerSub: not tracked -> null.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.get_dlq_messages_v1(p_filters JSONB DEFAULT '{}'::jsonb)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_queue TEXT;
    v_consumer_group TEXT;
    v_limit INTEGER;
    v_offset INTEGER;
    -- Track B (§5): tenant travels in the filter JSON (`_tenant`); signature unchanged.
    v_tenant UUID := COALESCE((p_filters->>'_tenant')::uuid, '00000000-0000-0000-0000-000000000001');
BEGIN
    v_queue := p_filters->>'queue';
    v_consumer_group := p_filters->>'consumerGroup';
    v_limit := COALESCE((p_filters->>'limit')::integer, 100);
    v_offset := COALESCE((p_filters->>'offset')::integer, 0);

    -- LIMIT/OFFSET belong INSIDE the row subquery, before jsonb_agg: applied
    -- outside they page the single aggregate row instead of the DLQ rows —
    -- offset 0 returned the ENTIRE dead-letter queue and offset>0 returned no
    -- row at all, i.e. a NULL body the route layer cannot parse.
    RETURN (
        SELECT jsonb_build_object(
            'messages', COALESCE(jsonb_agg(t.msg ORDER BY t.failed_at DESC), '[]'::jsonb),
            'pagination', jsonb_build_object(
                'limit', v_limit,
                'offset', v_offset
            )
        )
        FROM (
          SELECT * FROM (
            -- log engine (queen.log_dlq payload snapshots). The rows-engine leg
            -- that used to be UNION ALL'd in front of this one is deleted; the
            -- (failed_at, msg) column list and the ordering are unchanged.
            SELECT d2.failed_at,
                   jsonb_build_object(
                       'id', d2.id,
                       'transactionId', d2.transaction_id,
                       'partitionId', d2.partition_id,
                       'queue', q2q.name,
                       'partition', p2.name,
                       'consumerGroup', d2.consumer_group,
                       'errorMessage', d2.error,
                       'retryCount', COALESCE(d2.retry_count, COALESCE(q2q.retry_limit, 3)),
                       'data', d2.payload,
                       'producerSub', NULL,
                       'createdAt', to_char(d2.failed_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                       'failedAt', to_char(d2.failed_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                   ) AS msg
            FROM queen.log_dlq d2
            JOIN queen.log_partitions p2 ON p2.id = d2.partition_id
            -- Queue identity is the queen.queues id now: one join resolves the
            -- display name AND retry_limit (the old by-name LEFT JOIN is gone).
            JOIN queen.queues q2q ON q2q.id = p2.queue_id
            WHERE q2q.tenant_id = v_tenant
              AND (v_queue IS NULL OR q2q.name = v_queue)
              AND (v_consumer_group IS NULL OR d2.consumer_group = v_consumer_group)
          ) u
          ORDER BY u.failed_at DESC
          LIMIT v_limit OFFSET v_offset
        ) t
    );
END;
$$;

-- ============================================================================
-- Traces glue (port of the retired seg_traces file — engine-agnostic trace
-- recording).
-- ============================================================================
-- queen.record_trace_v1 used to live in the traces file (now 017_traces): it
-- resolved the trace's message_id from the rows
-- engine's message table and BAILED with {"success":false,"error":
-- "Message not found"} when no such row existed. Log queues never wrote that
-- table, so tracing a log message would always fail without this redefinition;
-- 017_traces no longer defines the function, so this file owns the only
-- definition. The lookup is now gone with its table: the trace is
-- recorded with message_id = NULL, which is exactly what the log branch always
-- did (a log message's mid lives inside a segment blob, opaque to SQL, and any
-- non-NULL value would be meaningless). message_id is a NULLABLE column.
-- queen.message_traces is keyed by (partition_id, transaction_id), so
-- queen.get_message_traces_v1 reads back the trace regardless of message_id.
--
-- Decoupling from the rows tables: queen.message_traces originally FK'd
-- message_id and partition_id into the rows engine's message/partition tables.
-- Log queues use queen.log_partitions UUIDs (an independent space), so both
-- FKs would reject a log trace. The base schema (schema.sql) now creates the
-- table with NO FKs — the decoupling first done by the retired seg_traces file
-- is the table's native shape — so the coordination table stays decoupled from
-- the engine-specific stores. Traces stay keyed by (partition_id,
-- transaction_id).
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.record_trace_v1(p_data JSONB)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_trace_id UUID;
    v_transaction_id TEXT;
    v_partition_id UUID;
    v_consumer_group TEXT;
    v_event_type TEXT;
    v_trace_data JSONB;
    v_worker_id TEXT;
    v_trace_names TEXT[];
    v_name TEXT;
BEGIN
    -- Extract fields from input
    v_transaction_id := p_data->>'transactionId';
    v_partition_id := (p_data->>'partitionId')::uuid;
    v_consumer_group := COALESCE(p_data->>'consumerGroup', '__QUEUE_MODE__');
    v_event_type := COALESCE(p_data->>'eventType', 'info');
    v_trace_data := COALESCE(p_data->'data', '{}'::jsonb);
    v_worker_id := p_data->>'workerId';

    -- Parse traceNames array
    IF p_data->'traceNames' IS NOT NULL AND jsonb_typeof(p_data->'traceNames') = 'array' THEN
        SELECT array_agg(elem::text) INTO v_trace_names
        FROM jsonb_array_elements_text(p_data->'traceNames') elem;
    END IF;

    -- Insert main trace record. The rows-engine message_id lookup that used to
    -- run here is deleted with its table; message_id is always NULL — the trace
    -- is keyed by (partition_id, transaction_id) and readable via
    -- queen.get_message_traces_v1.
    INSERT INTO queen.message_traces
        (message_id, partition_id, transaction_id, consumer_group, event_type, data, worker_id)
    VALUES
        (NULL, v_partition_id, v_transaction_id, v_consumer_group, v_event_type, v_trace_data, v_worker_id)
    RETURNING id INTO v_trace_id;

    -- Insert trace names if any
    IF v_trace_names IS NOT NULL AND array_length(v_trace_names, 1) > 0 THEN
        FOREACH v_name IN ARRAY v_trace_names LOOP
            INSERT INTO queen.message_trace_names (trace_id, trace_name)
            VALUES (v_trace_id, v_name)
            ON CONFLICT (trace_id, trace_name) DO NOTHING;
        END LOOP;
    END IF;

    RETURN jsonb_build_object(
        'success', true,
        'traceId', v_trace_id
    );
END;
$$;

-- ============================================================================
-- queen.get_lagging_partitions_v1 — LOG redefinition.
-- Folded in from the retired 099_retire_rows.sql (which carried the seg-native
-- lag rewrite, RUSTFIX item 22): without this, the rows-only original from the
-- consumer-groups file (now 014_consumer_groups) would report
-- [] forever for log queues (their cursors live in queen.log_consumers). Its
-- rows CTE that used to run first, prepending its own (always empty) array,
-- is deleted. The log branch is pure offset arithmetic: pending =
--   GREATEST(last_offset - GREATEST(committed, log_start - 1), 0)
-- and the time lag is the covering-segment probe of get_consumer_groups_v4
-- (same 2026-08-24 rewrite; the correlated MIN it replaces walked every
-- partition's whole PK range per cursor — measured 14-36s at 60k partitions,
-- probe form ~1s). last_consumed_at does not exist in the log consumer row ->
-- NULL, same key shape.
-- ============================================================================
-- Track B (§5): p_tenant scopes the (global) lagging-partitions listing.
CREATE OR REPLACE FUNCTION queen.get_lagging_partitions_v1(
    p_min_lag_seconds INTEGER DEFAULT 0,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001')
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_log JSONB;
BEGIN
    -- The former rows CTE (consumer_lag, over the dropped coordination/message
    -- tables) that produced the first half of the array is gone.
    -- -------------------------------------------------------------- log (v2)
    WITH log_lag AS (
        SELECT
            c.consumer_group,
            q.name AS queue_name,
            p.name AS partition_name,
            p.id AS partition_id,
            c.worker_id,
            GREATEST(p.last_offset - GREATEST(c.committed, p.log_start - 1), 0) AS pending,
            l.oldest_unconsumed_at
        FROM queen.log_consumers c
        JOIN queen.log_partitions p ON p.id = c.partition_id
        -- Queue identity is the queen.queues id now (log_queues is merged away).
        JOIN queen.queues q ON q.id = p.queue_id
        -- The pending gate + covering probe (rationale at
        -- get_consumer_groups_v4): a caught-up cursor touches no segment, a
        -- lagging one pays two PK probes instead of a walk of its partition's
        -- whole segment range. The pending>0 gate is spelled inline (the alias
        -- above is not in scope here), same arithmetic.
        LEFT JOIN LATERAL (
            SELECT s2.created_at AS oldest_unconsumed_at
            FROM queen.log_segments s2
            WHERE p.last_offset > GREATEST(c.committed, p.log_start - 1)
              AND s2.partition_id = c.partition_id
              AND s2.base_offset >= COALESCE((
                    SELECT s1.base_offset
                    FROM queen.log_segments s1
                    WHERE s1.partition_id = c.partition_id
                      AND s1.base_offset <= c.committed + 1
                    ORDER BY s1.base_offset DESC
                    LIMIT 1), 0)
              AND s2.end_offset > c.committed
            ORDER BY s2.base_offset
            LIMIT 1
        ) l ON TRUE
        WHERE q.tenant_id = p_tenant
    )
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'consumer_group', consumer_group,
            'queue_name', queue_name,
            'partition_name', partition_name,
            'partition_id', partition_id,
            'worker_id', worker_id,
            'offset_lag', pending,
            'time_lag_seconds', lag_seconds,
            'lag_hours', ROUND(lag_seconds / 3600.0, 2),
            'oldest_unconsumed_at', CASE WHEN oldest_unconsumed_at IS NOT NULL
                THEN to_char(oldest_unconsumed_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
            'last_consumed_at', NULL
        ) ORDER BY lag_seconds DESC
    ), '[]'::jsonb) INTO v_log
    FROM (
        SELECT ll.*,
               EXTRACT(EPOCH FROM (NOW() - ll.oldest_unconsumed_at))::integer AS lag_seconds
        FROM log_lag ll
    ) t
    WHERE lag_seconds > p_min_lag_seconds AND lag_seconds IS NOT NULL;

    -- Was `v_rows || v_log` with v_rows the (always empty) rows array.
    RETURN v_log;
END;
$$;

-- ============================================================================
-- queen.get_consumer_group_details_v1 — LOG redefinition.
-- Folded in from the retired 099_retire_rows.sql (same reason as above: the
-- original from the consumer-groups file joined
-- the rows partition tables only, so a log-engine group would render as {}).
-- That old rows body — merged in front of this one via jsonb `||` and always
-- empty — is deleted along with its tables.
-- lastConsumedAt is NULL for log partitions (no such column on log_consumers);
-- totalConsumed maps to log_consumers.total_consumed.
-- ============================================================================
-- Track B (§5): p_tenant scopes the group-details listing to one tenant's queues.
CREATE OR REPLACE FUNCTION queen.get_consumer_group_details_v1(
    p_consumer_group TEXT,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001')
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_log JSONB;
BEGIN
    -- The former rows body (partition_data, over the dropped coordination/message
    -- tables) that was merged in front of the log object is gone.
    -- -------------------------------------------------------------- log (v2)
    WITH log_data AS (
        SELECT
            q.name AS queue_name,
            p.name AS partition_name,
            c.worker_id,
            c.total_consumed,
            c.lease_expires_at,
            GREATEST(p.last_offset - GREATEST(c.committed, p.log_start - 1), 0)::bigint AS offset_lag,
            EXTRACT(EPOCH FROM (NOW() - l.oldest_unconsumed_at))::integer AS time_lag_seconds
        FROM queen.log_consumers c
        JOIN queen.log_partitions p ON p.id = c.partition_id
        -- Queue identity is the queen.queues id now (log_queues is merged away).
        JOIN queen.queues q ON q.id = p.queue_id
        -- The pending gate + covering probe (rationale at
        -- get_consumer_groups_v4) replacing the correlated MIN that walked the
        -- partition's whole segment range per cursor — this is the per-group
        -- console view, but one group over 60k partitions still measured
        -- 0.6-2.1s the old way, 0.1-0.7s with the probe.
        LEFT JOIN LATERAL (
            SELECT s2.created_at AS oldest_unconsumed_at
            FROM queen.log_segments s2
            WHERE p.last_offset > GREATEST(c.committed, p.log_start - 1)
              AND s2.partition_id = c.partition_id
              AND s2.base_offset >= COALESCE((
                    SELECT s1.base_offset
                    FROM queen.log_segments s1
                    WHERE s1.partition_id = c.partition_id
                      AND s1.base_offset <= c.committed + 1
                    ORDER BY s1.base_offset DESC
                    LIMIT 1), 0)
              AND s2.end_offset > c.committed
            ORDER BY s2.base_offset
            LIMIT 1
        ) l ON TRUE
        WHERE c.consumer_group = p_consumer_group AND q.tenant_id = p_tenant
    )
    SELECT jsonb_object_agg(
        queue_name,
        (SELECT jsonb_build_object(
            -- PLAN_CONFLATION §2.6: the group's durable delivery policy on THIS
            -- queue, so the details view can say why one queue's lag is log lag
            -- and another's is work.
            'conflation', COALESCE((
                SELECT m.conflation
                FROM queen.consumer_groups_metadata m
                JOIN queen.queues q2 ON q2.id = m.queue_id AND q2.tenant_id = p_tenant
                WHERE m.consumer_group = p_consumer_group
                  AND m.partition_name = ''
                  AND q2.name = ld.queue_name
                LIMIT 1), false),
            'partitions', jsonb_agg(
                jsonb_build_object(
                    'partition', partition_name,
                    'workerId', worker_id,
                    'lastConsumedAt', NULL,
                    'totalConsumed', total_consumed,
                    'offsetLag', COALESCE(offset_lag, 0),
                    'timeLagSeconds', COALESCE(time_lag_seconds, 0),
                    'leaseActive', (lease_expires_at IS NOT NULL AND lease_expires_at > NOW())
                ) ORDER BY partition_name
            )
        ) FROM log_data ld2 WHERE ld2.queue_name = ld.queue_name)
    ) INTO v_log
    FROM (SELECT DISTINCT queue_name FROM log_data) ld;

    -- Was `COALESCE(v_rows,'{}') || COALESCE(v_log,'{}')`; jsonb_object_agg
    -- returns NULL over zero rows, so the COALESCE on the log side stays.
    RETURN COALESCE(v_log, '{}'::jsonb);
END;
$$;

-- Track B: GRANTs target the tenant-scoped signatures (the pre-tenant
-- signatures do not exist on this schema, so granting them would error the
-- schema apply).
GRANT EXECUTE ON FUNCTION queen.get_lagging_partitions_v1(INTEGER, UUID) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_consumer_group_details_v1(TEXT, UUID) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.log_delete_consumer_group_v1(TEXT, BOOLEAN, UUID) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.log_seek_one_v1(UUID, BIGINT, BIGINT, TEXT, BOOLEAN, TIMESTAMPTZ) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.log_seek_consumer_group_v1(TEXT, TEXT, BOOLEAN, TIMESTAMPTZ, UUID) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.log_seek_partition_v1(TEXT, TEXT, TEXT, BOOLEAN, TIMESTAMPTZ, UUID) TO PUBLIC;
-- A3: the group delete and both seeks above publish a repair marker, and they are
-- SECURITY INVOKER, so a non-owner caller needs the table rights too — otherwise
-- granting the seek and then calling it fails on the announcement instead of the seek.
-- SELECT is for the brokers' poll (queen.hotlist_repairs), DELETE for the prune that
-- rides in the publish.
GRANT EXECUTE ON FUNCTION queen.hotlist_repair_publish_v1(UUID, TEXT, TEXT, TEXT, TEXT) TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON queen.hotlist_repairs TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_consumer_groups_v4(UUID) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.list_messages_v1(JSONB) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.get_dlq_messages_v1(JSONB) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.record_trace_v1(JSONB) TO PUBLIC;
