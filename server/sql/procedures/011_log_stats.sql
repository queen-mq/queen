-- ============================================================================
-- Log engine (18-log-engine.md §9) — stats refresh + analytics/status support.
--
--   queen.log_oldest_pending_at_v1   two-probe helper: created_at of the
--                                    segment covering (or next after) an offset
--   queen.log_refresh_all_stats_v1   queen.stats reconciler (former seg-era
--                                    seg_refresh_all_stats_v1; stats.rs calls
--                                    it once per cadence under advisory lock)
--   queen.get_queue_detail_v2        log-native redefinition (018_stats no
--                                    longer defines it — this file owns it)
--   queen.get_analytics_v1           log-native redefinition (time-series)
--   queen.log_queue_message_stats_v1 per-queue (segments, messages) counters
--                                    (former db.rs::seg_queue_message_stats)
--   queen.log_queue_stats_all_v1     broker-wide per-queue counters (former
--                                    db.rs::seg_queue_stats_all)
--   queen.log_queue_depth_v1         minimal pending/lease/ready depth read
--                                    for relays/schedulers (…/:queue/depth)
--
-- THE O(partitions) CONTRACT (§9): pending per (queue, group) is pure
-- watermark arithmetic —
--     pending = SUM over partitions of
--               GREATEST(last_offset - GREATEST(committed, log_start-1), 0)
-- — NO queen.log_segments scans. The old seg-era refresh aggregated
-- SUM(msg_count) over every live segment each cycle, i.e. O(segments) per
-- refresh; offsets make every count a subtraction:
--     retained (live) frames = last_offset - log_start + 1
-- which is EXACT, not an estimate, because deletes are prefix-contiguous
-- (001_log_schema header: no mid-log gaps, ever). The only log_segments touch
-- left in the refresh is a CASE-guarded per-partition O(log n) PK probe for ONE
-- timestamp (oldest pending) that offsets cannot carry. The two former
-- violations of this contract are gone (2026-08-20, PLAN_STATS_REFRESH.md
-- T1.0/T1.1): the retained-bytes heap scan lives in
-- queen.log_refresh_retained_bytes_v1 (028_retained_bytes, its own slow
-- cadence) and the per-partition newest_message_at LATERAL was deleted (the
-- stored column had no reader).
--
-- Queue identity: queen.stats rows key off queen.queues.id, reached DIRECTLY
-- via log_partitions.queue_id (queues is the only queue table now; the wire
-- keeps emitting the literal 'segments' where a storage/engine value is
-- expected, §11), so every existing queen.stats reader
-- (get_status_v3 / get_status_queues_v2 / get_system_overview_v3 /
-- aggregate_*) finds the rows with no change. partition_id stays NULL: this
-- file writes 'queue'-type stat rows only, never per-partition ones.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- log_oldest_pending_at_v1: created_at of the segment holding offset
-- p_wanted, or of the next existing segment when p_wanted fell into a
-- retention gap. Two O(log n) PK probes — the pop scan's head-probe pattern
-- (§6), NOT a filtered backward scan (a "base<=wanted AND end>=wanted" LIMIT 1
-- would walk the whole dead prefix when no covering segment exists). NULL when
-- nothing exists at/after p_wanted (fully drained partition).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.log_oldest_pending_at_v1(
    p_pid    UUID,
    p_wanted BIGINT
) RETURNS TIMESTAMPTZ
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    v_created TIMESTAMPTZ;
    v_end     BIGINT;
BEGIN
    SELECT s.created_at, s.end_offset INTO v_created, v_end
    FROM queen.log_segments s
    WHERE s.partition_id = p_pid AND s.base_offset <= p_wanted
    ORDER BY s.base_offset DESC LIMIT 1;

    IF FOUND AND v_end >= p_wanted THEN
        RETURN v_created;   -- head segment covers the wanted offset
    END IF;

    -- Retention gap (or nothing yet): first segment past the wanted offset.
    SELECT s.created_at INTO v_created
    FROM queen.log_segments s
    WHERE s.partition_id = p_pid AND s.base_offset > p_wanted
    ORDER BY s.base_offset LIMIT 1;

    RETURN v_created;
END;
$$;

GRANT EXECUTE ON FUNCTION queen.log_oldest_pending_at_v1(UUID, BIGINT) TO PUBLIC;

-- ----------------------------------------------------------------------------
-- log_refresh_all_stats_v1: write queen.stats 'queue' rows directly from the
-- log tables, then reuse the EXISTING queue->namespace/task/system rollups
-- (018_stats — they read 'queue' rows and never touch partition_id). Same return
-- keys as the seg-era function it replaces ('engine' stays the LITERAL 'segments':
-- a wire-compat telemetry label — no table carries a storage column anymore —
-- and stats.rs prints the summary verbatim).
--
-- Per-partition math (worst = most-behind cursor that still matters:
-- MIN(committed) across the partition's NAMED groups, falling back to the
-- group-less '__QUEUE_MODE__' cursor only when the partition has no named group
-- — see the `cons` CTE below for why the plain MIN was wrong; no consumer rows
-- => committed -1 => the whole retained range is pending, matching the seg-era
-- refresh's COALESCE(next_seq, 0)):
--   total    = last_offset - log_start + 1          (retained frames, exact)
--   pending  = last_offset - GREATEST(worst, log_start-1)
--   processing = SUM over live leases of batch_end - committed (the leased
--                span (committed, batch_end]; the seg-era refresh read
--                batch_size-acked_count)
--   completed  = total - pending - dlq (floored at 0, as before)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.log_refresh_all_stats_v1()
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_now       TIMESTAMPTZ := NOW();
    v_queues    INTEGER := 0;
    v_ns        JSONB;
    v_task      JSONB;
    v_sys       JSONB;
    v_log_parts BIGINT;
BEGIN
    -- ------------------------------------------------------------------ queues
    -- A frame is consumed when every NAMED group has committed past it; the
    -- group-less '__QUEUE_MODE__' cursor speaks for the partition only when no
    -- named group does (the same precedence the message-detail path applies —
    -- db.rs::seg_message_detail). A plain MIN over ALL rows, which is what this
    -- CTE used to take, let ONE abandoned group-less pop pin pending high
    -- forever: on first contact 004_log_pop's WILDCARD and DISCOVERY paths seed
    -- a __QUEUE_MODE__ cursor at GREATEST(log_start-1, -1) on EVERY partition of
    -- the queue at once (the partition-addressed log_pop_v1 seeds only the
    -- partition it was aimed at), and they do so BEFORE delivering, so even a
    -- group-less pop that returns nothing parks a cursor at the head of the
    -- backlog. MIN then reported the whole retained range as pending forever,
    -- with every named group fully caught up and nothing left to deliver.
    -- COALESCE = precedence: the named-group MIN when it exists, the queue-mode
    -- cursor otherwise. Partitions with no named group compute exactly as before.
    --
    -- ONE pass over log_consumers for BOTH quantities (this CTE absorbed the
    -- old lease_agg — two unqualified GROUP BY passes over the same O(P×groups)
    -- table were the #2 term of the refresh). It must carry NO WHERE clause:
    -- `committed` aggregates EVERY row (restricting the scan to live-lease rows
    -- would drop idle partitions' watermarks and report the whole retained
    -- range as pending, broker-wide). The lease predicate lives ONLY in the
    -- FILTER on the processing SUM — `NULL > v_now` is NULL, so the old
    -- `IS NOT NULL AND > v_now` collapses into the one comparison. processing =
    -- in-flight (leased-but-unacked) span (committed, batch_end]; batch_end
    -- NULL means no lease, and committed can only move up to batch_end, so the
    -- span is never negative — GREATEST guards a torn read anyway.
    WITH cons AS (
        SELECT c.partition_id,
               COALESCE(
                   MIN(c.committed) FILTER (WHERE c.consumer_group <> '__QUEUE_MODE__'),
                   MIN(c.committed) FILTER (WHERE c.consumer_group =  '__QUEUE_MODE__')
               ) AS committed,
               (SUM(GREATEST(0, COALESCE(c.batch_end, c.committed) - c.committed))
                    FILTER (WHERE c.lease_expires_at > v_now))::bigint AS processing
        FROM queen.log_consumers c
        GROUP BY c.partition_id
    ),
    part_agg AS (
        SELECT p.id AS partition_id,
               p.queue_id,
               GREATEST(p.last_offset - p.log_start + 1, 0)::bigint AS total_frames,
               GREATEST(p.last_offset
                        - GREATEST(COALESCE(w.committed, -1), p.log_start - 1),
                        0)::bigint AS pending,
               -- Timestamp: ONE bounded PK probe per partition, and only when
               -- something IS pending (CASE-guarded). The unconditional
               -- newest_message_at LATERAL that used to sit here (a backward
               -- descent + a random heap fetch per partition, ~30% of the
               -- refresh at 54k partitions) is GONE: the stored column it fed
               -- has no reader — the wire's newestMessage is computed live per
               -- request (get_queue_detail_v2 / log_queue_message_stats_v1).
               CASE WHEN p.last_offset
                         > GREATEST(COALESCE(w.committed, -1), p.log_start - 1)
                    THEN queen.log_oldest_pending_at_v1(
                             p.id,
                             GREATEST(COALESCE(w.committed, -1), p.log_start - 1) + 1)
               END AS oldest_pending_at,
               COALESCE(w.processing, 0)::bigint AS processing
        FROM queen.log_partitions p
        LEFT JOIN cons w ON w.partition_id = p.id
    ),
    -- dead-letter frames per queue (log DLQ, 005_log_ack). Queue identity is the id:
    -- log_partitions.queue_id references queen.queues directly, so keying by
    -- queue_id is tenant-safe by construction.
    dlq_agg AS (
        SELECT p.queue_id, COUNT(*)::bigint AS dlq
        FROM queen.log_dlq d
        JOIN queen.log_partitions p ON p.id = d.partition_id
        GROUP BY p.queue_id
    ),
    -- NOTE: the retained-bytes CTE (seg_bytes, the 1 GB unqualified heap scan
    -- of log_segments) moved to queen.log_refresh_retained_bytes_v1
    -- (028_retained_bytes) on its own slow cadence. This function no longer
    -- touches queen.stats.retained_bytes except to self-assign it below.
    --
    -- Per-queue rollup driven from part_agg — NOT from a re-scan of
    -- log_partitions: part_agg is strictly 1:1 with log_partitions (its LEFT
    -- JOIN is on the unique partition_id, no fan-out possible), so the row set
    -- is identical, and COUNT(*) counts exactly what COUNT(DISTINCT lp.id) did
    -- (lp.id is the PK) without the DISTINCT that blocked HashAggregate and
    -- forced a 54k-row Sort. processing rides THROUGH part_agg so `cons` is
    -- referenced exactly once (a CTE referenced twice is materialised —
    -- one scan would become a scan plus a 54k-row tuplestore plus two probes).
    -- Queues with no partitions produce no row here; the driving SELECT below
    -- LEFT JOINs and COALESCEs to 0 — do NOT fold queen.queues into this CTE
    -- (COUNT(*) would count the NULL-extended row and report 1 partition for
    -- every empty queue, moving get_queues_v2's `partitions` and the proxy's
    -- db_partition_floor with it).
    per_queue AS (
        SELECT pa.queue_id,
               COUNT(*)::bigint                                 AS child_count,
               COALESCE(SUM(pa.total_frames), 0)::bigint        AS total_messages,
               COALESCE(SUM(pa.pending), 0)::bigint             AS pending_messages,
               COALESCE(SUM(pa.processing), 0)::bigint          AS processing_messages,
               MIN(pa.oldest_pending_at)                        AS oldest_pending_at
        FROM part_agg pa
        GROUP BY pa.queue_id
    )
    -- retained_bytes, segment_count and newest_message_at are ABSENT from the
    -- column list on purpose:
    --   * retained_bytes and segment_count are owned by
    --     log_refresh_retained_bytes_v1 (028_retained_bytes) on its own slow
    --     cadence. A NEW queue therefore inserts at the DDL default 0 until
    --     that lane's next pass — the accepted blind window; the proxy storage
    --     quota is hysteretic by contract (schema.sql), and the queue list's
    --     segment count is informational.
    --   * newest_message_at has no reader anywhere; new rows get NULL.
    INSERT INTO queen.stats (
        stat_type, stat_key, queue_id, partition_id, consumer_group,
        child_count, total_messages, pending_messages, processing_messages,
        completed_messages, dead_letter_messages,
        oldest_pending_at,
        avg_lag_seconds, max_lag_seconds, avg_offset_lag, max_offset_lag,
        ingested_per_second, processed_per_second,
        last_computed_at
    )
    SELECT
        'queue',
        q.id::text,
        q.id,
        NULL,
        '__QUEUE_MODE__',
        COALESCE(pq.child_count, 0),
        COALESCE(pq.total_messages, 0),
        COALESCE(pq.pending_messages, 0),
        LEAST(COALESCE(pq.processing_messages, 0), COALESCE(pq.pending_messages, 0)),
        GREATEST(0, COALESCE(pq.total_messages, 0)
                    - COALESCE(pq.pending_messages, 0)
                    - COALESCE(da.dlq, 0)),
        COALESCE(da.dlq, 0),
        pq.oldest_pending_at,
        COALESCE(EXTRACT(EPOCH FROM (v_now - pq.oldest_pending_at))::integer, 0),
        COALESCE(EXTRACT(EPOCH FROM (v_now - pq.oldest_pending_at))::integer, 0),
        COALESCE(pq.pending_messages, 0)::integer,
        COALESCE(pq.pending_messages, 0)::integer,
        0, 0,
        v_now
    FROM queen.queues q
    -- Queue identity is the id: log_partitions.queue_id references
    -- queen.queues(id) directly, so the log-derived aggregates join by id —
    -- tenant separation is inherent (ids never collide across tenants).
    LEFT JOIN per_queue pq ON pq.queue_id = q.id
    LEFT JOIN dlq_agg   da ON da.queue_id = q.id
    -- Deterministic upsert order: lock the 'queue' stat rows in q.id order —
    -- the same order log_refresh_retained_bytes_v1's FOR UPDATE pre-lock takes.
    -- Unordered upsert lock order is plan-dependent (the deadlock class
    -- 003_log_push measured), and this statement now shares its target rows
    -- with the bytes lane's second writer and with the UNLOCKED manual refresh
    -- (POST /api/v1/stats/refresh, handlers/status.rs).
    ORDER BY q.id
    ON CONFLICT (stat_type, stat_key) DO UPDATE SET
        queue_id             = EXCLUDED.queue_id,
        child_count          = EXCLUDED.child_count,
        total_messages       = EXCLUDED.total_messages,
        pending_messages     = EXCLUDED.pending_messages,
        processing_messages  = EXCLUDED.processing_messages,
        completed_messages   = EXCLUDED.completed_messages,
        dead_letter_messages = EXCLUDED.dead_letter_messages,
        -- NOT EXCLUDED.retained_bytes: the column is absent from the INSERT
        -- list above, so EXCLUDED here would carry the DDL default and write a
        -- literal 0 over every queue's gauge each cycle — silently turning the
        -- proxy's hard-403 storage gate off fleet-wide. The self-assignment
        -- keeps log_refresh_retained_bytes_v1 (028) the only writer. It is
        -- deliberately EXPLICIT (omitting the arm is value-identical but
        -- invisible in a SET-list diff, which is how the fatal EXCLUDED
        -- variant would ship by accident).
        retained_bytes       = queen.stats.retained_bytes,
        -- Same ownership, same explicit self-assignment, same reason.
        segment_count        = queen.stats.segment_count,
        oldest_pending_at    = EXCLUDED.oldest_pending_at,
        -- Dead column: no reader anywhere (the wire's newestMessage is computed
        -- live per request), no writer since the LATERAL was deleted. NULL is
        -- kept EXPLICITLY in the SET list — dropping the arm would freeze
        -- existing rows at their last value forever while new queues get NULL.
        -- Do not drop the COLUMN either: schema.sql is CREATE TABLE IF NOT
        -- EXISTS (a dropped column is never re-created) and an old image's 011
        -- body still names it, failing at runtime as 42703 on re-apply.
        newest_message_at    = NULL,
        avg_lag_seconds      = EXCLUDED.avg_lag_seconds,
        max_lag_seconds      = EXCLUDED.max_lag_seconds,
        avg_offset_lag       = EXCLUDED.avg_offset_lag,
        max_offset_lag       = EXCLUDED.max_offset_lag,
        -- throughput from delta vs the previous snapshot (per-second rates).
        -- NOTE (semantics shift vs the seg-era refresh, inherent to R1):
        -- total_messages is the
        -- RETAINED count, so ingest/processed rates dip transiently right
        -- after a retention sweep shrinks the retained window — same class of
        -- artifact the seg-era numbers had (its SUM(msg_count) shrank on sweep
        -- too), not a regression.
        -- >= 3 s elapsed floor on the rate window: replicas are staggered, not
        -- phase-locked, so two writers can land near-simultaneously and a
        -- sub-second denominator turns a handful of frames into a garbage
        -- spike. Below the floor the ELSE arm writes 0, exactly as before —
        -- these two columns reach no wire surface (the overview reads
        -- queue_lag_metrics instead), so preserving the previous value would
        -- be a behaviour change with no reader.
        ingested_per_second = CASE
            WHEN queen.stats.prev_snapshot_at IS NOT NULL
                AND v_now > queen.stats.prev_snapshot_at
                AND EXTRACT(EPOCH FROM (v_now - queen.stats.prev_snapshot_at)) >= 3
            THEN GREATEST(0, ((EXCLUDED.total_messages - queen.stats.prev_total_messages)::numeric /
                  EXTRACT(EPOCH FROM (v_now - queen.stats.prev_snapshot_at))))
            ELSE 0
        END,
        processed_per_second = CASE
            WHEN queen.stats.prev_snapshot_at IS NOT NULL
                AND v_now > queen.stats.prev_snapshot_at
                AND EXTRACT(EPOCH FROM (v_now - queen.stats.prev_snapshot_at)) >= 3
            THEN GREATEST(0, ((EXCLUDED.completed_messages - queen.stats.prev_completed_messages)::numeric /
                  EXTRACT(EPOCH FROM (v_now - queen.stats.prev_snapshot_at))))
            ELSE 0
        END,
        prev_total_messages     = queen.stats.total_messages,
        prev_completed_messages = queen.stats.completed_messages,
        prev_snapshot_at        = queen.stats.last_computed_at,
        last_computed_at        = v_now;

    GET DIAGNOSTICS v_queues = ROW_COUNT;

    -- ---------------------------------------------- namespace / task / system
    -- These read queen.stats 'queue' rows (queue_id join) and never touch
    -- partition_id, so they work unchanged for the log queue rows above.
    v_ns   := queen.aggregate_namespace_stats_v1();
    v_task := queen.aggregate_task_stats_v1();
    v_sys  := queen.aggregate_system_stats_v2();

    -- Belt-and-braces, no longer a correction: 018_stats's aggregate_system_stats_v2
    -- used to count the rows-engine partition table (near-zero here) and this
    -- overwrite existed to fix the overview 'partitions' tile. 018_stats now counts
    -- queen.log_partitions itself, so the two agree; the re-write is kept
    -- because it is authoritative, cheap, and pins the value at THIS snapshot.
    SELECT COUNT(*) INTO v_log_parts FROM queen.log_partitions;
    UPDATE queen.stats SET child_count = v_log_parts
    WHERE stat_type = 'system' AND stat_key = 'global';

    RETURN jsonb_build_object(
        'engine', 'segments',
        'queuesUpdated', v_queues,
        'segPartitions', v_log_parts,
        'namespaces', v_ns,
        'tasks', v_task,
        'system', v_sys,
        'refreshedAt', to_char(v_now AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
    );
END;
$$;

GRANT EXECUTE ON FUNCTION queen.log_refresh_all_stats_v1() TO PUBLIC;

-- ============================================================================
-- queen.get_queue_detail_v2 — log-native redefinition.
-- ============================================================================
-- This file owns the ONLY definition (018_stats no longer defines it —
-- historically the seg-era stats file superseded the rows version by loading
-- later in the roster), computing the same output shape (queue.config,
-- partitions[].{messages,stats,cursor,...}, totals.messages.*) from the log
-- tables with the watermark math above. The former dual-mode rows branch
-- (a verbatim copy of 018_stats's old body over the rows-engine tables) is GONE: this
-- broker is log-engine only (the storage discriminator column itself is gone),
-- so it was dead code and its tables no longer exist. Two log-schema honesty
-- notes, keys
-- preserved for the webapp:
--   * cursor.batchesConsumed is 0 — queen.log_consumers does not carry the
--     old total_batches_consumed counter (deliberate schema diet, 001_log_schema).
--   * lastActivity = MAX(lease_acquired_at) — the closest activity signal
--     the log schema keeps (former last_consumed_at is gone).
-- ============================================================================
-- Track B (§5): p_tenant scopes the single-queue detail; the scoped
-- (TEXT, UUID) signature is the only one this schema ever defines.
CREATE OR REPLACE FUNCTION queen.get_queue_detail_v2(
    p_queue_name TEXT,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001')
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_queue_info JSONB;
    v_partitions JSONB;
    v_totals JSONB;
    v_queue_id UUID;
BEGIN
    -- Resolve + existence check: queue identity is the queen.queues id
    -- (log_partitions.queue_id references it directly).
    SELECT q.id INTO v_queue_id
    FROM queen.queues q WHERE q.name = p_queue_name AND q.tenant_id = p_tenant;

    IF v_queue_id IS NULL THEN
        RETURN jsonb_build_object('error', 'Queue not found');
    END IF;

    -- --------------------------------------------------------------- log queue
    SELECT jsonb_build_object(
        'queue', jsonb_build_object(
            'id', q.id, 'name', q.name, 'namespace', q.namespace, 'task', q.task,
            'priority', q.priority,
            'createdAt', to_char(q.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'config', jsonb_build_object(
                'leaseTime', q.lease_time, 'retryLimit', q.retry_limit,
                'retryDelay', q.retry_delay, 'ttl', q.ttl,
                'maxQueueSize', q.max_queue_size, 'deadLetterQueue', q.dead_letter_queue)))
    INTO v_queue_info
    FROM queen.queues q WHERE q.id = v_queue_id;

    WITH parts AS (
        SELECT lp.id, lp.name, lp.created_at, lp.last_offset, lp.log_start
        FROM queen.log_partitions lp
        WHERE lp.queue_id = v_queue_id
    ),
    -- Named groups first, '__QUEUE_MODE__' only in their absence — same
    -- precedence, and the same abandoned-group-less-cursor failure, as
    -- log_refresh_all_stats_v1's `cons` above (see that comment); this reader
    -- must agree with the queue's stats row, not contradict it.
    worst AS (
        SELECT c.partition_id,
               COALESCE(
                   MIN(c.committed) FILTER (WHERE c.consumer_group <> '__QUEUE_MODE__'),
                   MIN(c.committed) FILTER (WHERE c.consumer_group =  '__QUEUE_MODE__')
               ) AS committed
        FROM queen.log_consumers c
        JOIN parts pa ON pa.id = c.partition_id
        GROUP BY c.partition_id
    ),
    -- Watermark arithmetic (see log_refresh_all_stats_v1 header) + the two
    -- per-partition timestamp probes.
    base AS (
        SELECT pa.id, pa.name, pa.created_at,
               GREATEST(pa.last_offset - pa.log_start + 1, 0)::bigint AS total,
               GREATEST(pa.last_offset
                        - GREATEST(COALESCE(w.committed, -1), pa.log_start - 1),
                        0)::bigint AS pending,
               GREATEST(COALESCE(w.committed, -1), pa.log_start - 1) AS floor_off
        FROM parts pa
        LEFT JOIN worst w ON w.partition_id = pa.id
    ),
    enrich AS (
        SELECT b.id, b.name, b.created_at, b.total, b.pending,
               CASE WHEN b.pending > 0
                    THEN queen.log_oldest_pending_at_v1(b.id, b.floor_off + 1)
               END AS oldest_message,
               tail.created_at AS newest_message
        FROM base b
        LEFT JOIN LATERAL (
            SELECT s.created_at FROM queen.log_segments s
            WHERE s.partition_id = b.id
            ORDER BY s.base_offset DESC LIMIT 1
        ) tail ON true
    ),
    cur AS (
        SELECT c.partition_id,
               SUM(GREATEST(0, COALESCE(c.batch_end, c.committed) - c.committed))
                   FILTER (WHERE c.lease_expires_at IS NOT NULL AND c.lease_expires_at > NOW())::bigint AS processing,
               COALESCE(SUM(c.total_consumed), 0) AS total_consumed,
               MAX(c.lease_acquired_at) AS last_activity
        FROM queen.log_consumers c
        JOIN parts pa ON pa.id = c.partition_id
        GROUP BY c.partition_id
    ),
    dlq AS (
        SELECT d.partition_id, COUNT(*)::bigint AS dead_letter
        FROM queen.log_dlq d JOIN parts pa ON pa.id = d.partition_id
        GROUP BY d.partition_id
    ),
    per_part AS (
        SELECT en.id, en.name, en.created_at,
               en.total,
               en.pending,
               LEAST(COALESCE(cu.processing, 0), en.pending) AS processing,
               GREATEST(0, en.total - en.pending - COALESCE(dl.dead_letter, 0)) AS completed,
               COALESCE(dl.dead_letter, 0) AS dead_letter,
               en.oldest_message, en.newest_message,
               COALESCE(cu.total_consumed, 0) AS total_consumed,
               cu.last_activity
        FROM enrich en
        LEFT JOIN cur cu ON cu.partition_id = en.id
        LEFT JOIN dlq dl ON dl.partition_id = en.id
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'id', id, 'name', name,
                'createdAt', to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                -- 'failed' is NULL, not 0: the log engine keeps no per-partition
                -- failure counter (ack failures are per (queue, minute) in
                -- queue_lag_metrics), and a literal 0 next to a live ack-failure
                -- chart for the same queue reads as "no failures".
                'messages', jsonb_build_object('total', total, 'pending', pending,
                    'processing', processing, 'completed', completed, 'failed', NULL, 'deadLetter', dead_letter),
                'stats', jsonb_build_object('total', total, 'pending', pending,
                    'processing', processing, 'completed', completed, 'failed', NULL, 'deadLetter', dead_letter),
                'cursor', jsonb_build_object('totalConsumed', total_consumed, 'batchesConsumed', 0),
                'lastActivity', CASE WHEN last_activity IS NOT NULL THEN to_char(last_activity AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
                'oldestMessage', CASE WHEN oldest_message IS NOT NULL THEN to_char(oldest_message AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
                'newestMessage', CASE WHEN newest_message IS NOT NULL THEN to_char(newest_message AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END
            ) ORDER BY name), '[]'::jsonb),
           jsonb_build_object('messages', jsonb_build_object(
                'total', COALESCE(SUM(total),0), 'pending', COALESCE(SUM(pending),0),
                'processing', COALESCE(SUM(processing),0), 'completed', COALESCE(SUM(completed),0),
                'failed', NULL, 'deadLetter', COALESCE(SUM(dead_letter),0)))
    INTO v_partitions, v_totals FROM per_part;

    RETURN v_queue_info || jsonb_build_object('partitions', v_partitions, 'totals', v_totals);
END;
$$;

GRANT EXECUTE ON FUNCTION queen.get_queue_detail_v2(TEXT, UUID) TO PUBLIC;

-- ============================================================================
-- queen.get_queue_v2 — log-native redefinition.
-- ============================================================================
-- 018_stats's former body drilled through the rows-engine partition table + the
-- 'partition' stat rows, both of which are EMPTY under the log engine (the
-- refresh above writes only
-- 'queue'-type stat rows, and log partitions live in queen.log_partitions). It
-- therefore described every healthy log queue as "0 partitions / 0 messages" at
-- HTTP 200 — the shape a caller cannot distinguish from a real empty queue.
-- This redefinition computes the output shape from the log tables (same
-- watermark arithmetic as get_queue_detail_v2 above). `failed` is NULL, not 0:
-- the log engine keeps no per-partition failure counter.
-- The rows companion (queen.get_queue_v2_rows, a verbatim copy of 018_stats's
-- old body
-- that this entry point used to delegate to for storage <> 'segments') is
-- DELETED along with its GRANT: no route can create such a queue, so the
-- delegation was unreachable and its tables are gone.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.get_queue_v2(
    p_queue_name TEXT,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001')
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_queue_id UUID;
    v_queue_info JSONB;
    v_partitions JSONB;
    v_totals JSONB;
BEGIN
    -- Resolve + existence check; the storage discriminator went with the
    -- deleted rows delegation.
    SELECT q.id INTO v_queue_id
    FROM queen.queues q WHERE q.name = p_queue_name AND q.tenant_id = p_tenant;

    IF v_queue_id IS NULL THEN
        RETURN jsonb_build_object('error', 'Queue not found');
    END IF;

    SELECT jsonb_build_object(
        'id', q.id, 'name', q.name, 'namespace', q.namespace, 'task', q.task,
        'createdAt', to_char(q.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'))
    INTO v_queue_info
    FROM queen.queues q WHERE q.id = v_queue_id;

    -- Partition->queue is lp.queue_id = queues.id (already resolved above).
    WITH parts AS (
        SELECT lp.id, lp.name, lp.created_at, lp.last_offset, lp.log_start
        FROM queen.log_partitions lp
        WHERE lp.queue_id = v_queue_id
    ),
    -- Named groups first, '__QUEUE_MODE__' only in their absence — same
    -- precedence, and the same abandoned-group-less-cursor failure, as
    -- log_refresh_all_stats_v1's `cons` above (see that comment); this reader
    -- must agree with the queue's stats row, not contradict it.
    worst AS (
        SELECT c.partition_id,
               COALESCE(
                   MIN(c.committed) FILTER (WHERE c.consumer_group <> '__QUEUE_MODE__'),
                   MIN(c.committed) FILTER (WHERE c.consumer_group =  '__QUEUE_MODE__')
               ) AS committed
        FROM queen.log_consumers c
        JOIN parts pa ON pa.id = c.partition_id
        GROUP BY c.partition_id
    ),
    base AS (
        SELECT pa.id, pa.name, pa.created_at,
               GREATEST(pa.last_offset - pa.log_start + 1, 0)::bigint AS total,
               GREATEST(pa.last_offset
                        - GREATEST(COALESCE(w.committed, -1), pa.log_start - 1),
                        0)::bigint AS pending,
               GREATEST(COALESCE(w.committed, -1), pa.log_start - 1) AS floor_off
        FROM parts pa
        LEFT JOIN worst w ON w.partition_id = pa.id
    ),
    enrich AS (
        SELECT b.id, b.name, b.created_at, b.total, b.pending,
               CASE WHEN b.pending > 0
                    THEN queen.log_oldest_pending_at_v1(b.id, b.floor_off + 1)
               END AS oldest_message,
               tail.created_at AS newest_message
        FROM base b
        LEFT JOIN LATERAL (
            SELECT s.created_at FROM queen.log_segments s
            WHERE s.partition_id = b.id
            ORDER BY s.base_offset DESC LIMIT 1
        ) tail ON true
    ),
    cur AS (
        SELECT c.partition_id,
               SUM(GREATEST(0, COALESCE(c.batch_end, c.committed) - c.committed))
                   FILTER (WHERE c.lease_expires_at IS NOT NULL AND c.lease_expires_at > NOW())::bigint AS processing
        FROM queen.log_consumers c
        JOIN parts pa ON pa.id = c.partition_id
        GROUP BY c.partition_id
    ),
    dlq AS (
        SELECT d.partition_id, COUNT(*)::bigint AS dead_letter
        FROM queen.log_dlq d JOIN parts pa ON pa.id = d.partition_id
        GROUP BY d.partition_id
    ),
    per_part AS (
        SELECT en.id, en.name, en.created_at, en.total, en.pending,
               LEAST(COALESCE(cu.processing, 0), en.pending) AS processing,
               GREATEST(0, en.total - en.pending - COALESCE(dl.dead_letter, 0)) AS completed,
               COALESCE(dl.dead_letter, 0) AS dead_letter,
               en.oldest_message, en.newest_message
        FROM enrich en
        LEFT JOIN cur cu ON cu.partition_id = en.id
        LEFT JOIN dlq dl ON dl.partition_id = en.id
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'id', id, 'name', name,
                'createdAt', to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                'stats', jsonb_build_object(
                    'total', total, 'pending', pending, 'processing', processing,
                    'completed', completed, 'failed', NULL, 'deadLetter', dead_letter),
                'oldestMessage', CASE WHEN oldest_message IS NOT NULL
                    THEN to_char(oldest_message AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END,
                'newestMessage', CASE WHEN newest_message IS NOT NULL
                    THEN to_char(newest_message AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') ELSE NULL END
            ) ORDER BY name), '[]'::jsonb),
           jsonb_build_object(
                'total', COALESCE(SUM(total), 0), 'pending', COALESCE(SUM(pending), 0),
                'processing', COALESCE(SUM(processing), 0), 'completed', COALESCE(SUM(completed), 0),
                'failed', NULL, 'deadLetter', COALESCE(SUM(dead_letter), 0))
    INTO v_partitions, v_totals FROM per_part;

    RETURN v_queue_info || jsonb_build_object(
        'partitions', v_partitions,
        'totals', v_totals,
        'retainedBytes', COALESCE((SELECT retained_bytes FROM queen.stats
                                   WHERE stat_type = 'queue' AND queue_id = v_queue_id), 0)
    );
END;
$$;

GRANT EXECUTE ON FUNCTION queen.get_queue_v2(TEXT, UUID) TO PUBLIC;

-- ============================================================================
-- queen.get_analytics_v1 — log-native redefinition.
-- ============================================================================
-- Same shape/interval logic as the seg-era version: the message-volume series
-- buckets queen.log_segments by created_at, attributing a segment's frames
-- (end_offset - base_offset + 1 — exact, replaces msg_count) to its creation
-- bucket; per-message granularity inside a segment is not recoverable, as
-- before. This is the one intentionally O(segments-in-range) reader: it is a
-- paid-per-request dashboard endpoint with a time filter, not the refresh
-- loop, so the §9 no-scan rule does not apply.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.get_analytics_v1(p_filters JSONB DEFAULT '{}'::jsonb)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_from TIMESTAMPTZ;
    v_to TIMESTAMPTZ;
    v_interval TEXT;
    v_queue TEXT;
    v_namespace TEXT;
    v_task TEXT;
    v_bw INTERVAL;
    v_result JSONB;
    -- Track B (§5): tenant travels in the filter JSON (`_tenant`); signature unchanged.
    v_tenant UUID := COALESCE((p_filters->>'_tenant')::uuid, '00000000-0000-0000-0000-000000000001');
BEGIN
    v_from := COALESCE((p_filters->>'from')::timestamptz, NOW() - INTERVAL '24 hours');
    v_to := COALESCE((p_filters->>'to')::timestamptz, NOW());
    v_interval := COALESCE(p_filters->>'interval', 'hour');
    v_queue := p_filters->>'queue';
    v_namespace := p_filters->>'namespace';
    v_task := p_filters->>'task';
    v_bw := CASE v_interval
        WHEN 'minute' THEN INTERVAL '1 minute'
        WHEN 'hour'   THEN INTERVAL '1 hour'
        WHEN 'day'    THEN INTERVAL '1 day'
        ELSE INTERVAL '1 hour'
    END;

    WITH time_buckets AS (
        SELECT generate_series(date_trunc(v_interval, v_from),
                               date_trunc(v_interval, v_to), v_bw) AS bucket
    ),
    message_buckets AS (
        -- Queue identity is the id: p.queue_id references queen.queues
        -- directly, so name/namespace/task/tenant all come from ONE join.
        SELECT date_trunc(v_interval, s.created_at) AS bucket,
               SUM(s.end_offset - s.base_offset + 1) AS message_count
        FROM queen.log_segments s
        JOIN queen.log_partitions p ON p.id = s.partition_id
        JOIN queen.queues q         ON q.id = p.queue_id
        WHERE s.created_at >= v_from AND s.created_at <= v_to
          AND q.tenant_id = v_tenant
          AND (v_queue IS NULL OR q.name = v_queue)
          AND (v_namespace IS NULL OR q.namespace = v_namespace)
          AND (v_task IS NULL OR q.task = v_task)
        GROUP BY date_trunc(v_interval, s.created_at)
    )
    SELECT jsonb_build_object(
        'dataPoints', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                       'timestamp', to_char(tb.bucket AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
                       'messages', COALESCE(mb.message_count, 0)
                   ) ORDER BY tb.bucket)
            FROM time_buckets tb
            LEFT JOIN message_buckets mb ON mb.bucket = tb.bucket
        ), '[]'::jsonb),
        'interval', v_interval,
        'from', to_char(v_from AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'to', to_char(v_to AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
    ) INTO v_result;

    RETURN v_result;
END;
$$;

GRANT EXECUTE ON FUNCTION queen.get_analytics_v1(JSONB) TO PUBLIC;

-- ----------------------------------------------------------------------------
-- log_queue_message_stats_v1: per-queue (segments, messages) counters — the
-- SQL home for db.rs::seg_queue_message_stats (queue DETAIL enrichment).
-- messages = watermark arithmetic (exact retained frames, O(partitions));
-- segments = COUNT over live queen.log_segments rows via the PK (index-only,
-- bounded by the queue's live segment count — a per-request admin read, not
-- the refresh loop). NOT pg_relation_size: the row count is what the wire
-- field reports and dev partitions are small; size estimates would change
-- the meaning of an existing field.
-- ----------------------------------------------------------------------------
-- Track B (§5): p_tenant scopes the per-queue counters.
CREATE OR REPLACE FUNCTION queen.log_queue_message_stats_v1(
    p_queue TEXT,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001')
RETURNS TABLE (segments BIGINT, messages BIGINT)
LANGUAGE sql STABLE
AS $$
    -- Queue identity is the id: log_partitions.queue_id references
    -- queen.queues directly (log_queues is gone).
    SELECT COALESCE((
               SELECT count(*)
               FROM queen.log_segments s
               JOIN queen.log_partitions p ON p.id = s.partition_id
               JOIN queen.queues q         ON q.id = p.queue_id
               WHERE q.name = p_queue AND q.tenant_id = p_tenant
           ), 0)::bigint,
           COALESCE((
               SELECT SUM(GREATEST(p.last_offset - p.log_start + 1, 0))
               FROM queen.log_partitions p
               JOIN queen.queues q ON q.id = p.queue_id
               WHERE q.name = p_queue AND q.tenant_id = p_tenant
           ), 0)::bigint
$$;

GRANT EXECUTE ON FUNCTION queen.log_queue_message_stats_v1(TEXT, UUID) TO PUBLIC;

-- ----------------------------------------------------------------------------
-- log_queue_stats_all_v1: per-queue counters for ONE tenant's queues — the SQL
-- home for db.rs::seg_queue_stats_all (queue LIST enrichment). partitions and
-- messages are LIVE and Θ(the tenant's partitions): a count of log_partitions
-- rows and the watermark arithmetic above. segments is NOT counted here:
-- it is read from queen.stats.segment_count, which log_refresh_retained_bytes_v1
-- (028_retained_bytes) fills out of the one scan of log_segments it already
-- makes for retained_bytes, on its own slow cadence — so the list's cost is flat
-- at a fixed partition count whatever retention accumulates, and 0 for a queue
-- the lane has not passed over yet. The queue DETAIL keeps the exact live count
-- (log_queue_message_stats_v1, O(that queue's segments) per click).
--
-- History, because both earlier shapes looked right and were not: until
-- 2026-08-23 this pre-aggregated `SELECT partition_id, count(*) FROM
-- log_segments GROUP BY partition_id` in a CTE — no tenant predicate, and a join
-- qualification cannot be pushed into a GROUP BY subquery, so every call scanned
-- EVERY segment in the cell (a 1k-partition tenant paid 135 ms for 1.4M rows).
-- The first fix, a LATERAL count per partition through the PK, was bounded by
-- the tenant but still Θ(the tenant's segments): on the soak it went from 635 to
-- 2,204 ms as segments grew 2M -> 9M at a constant 827k partitions, because
-- retention only starts deleting after the plan's window. Counting is the wrong
-- shape for a LIST; it belongs to the lane that scans segments anyway.
-- ----------------------------------------------------------------------------
-- Track B (§5): p_tenant scopes the per-queue counters to one tenant.
CREATE OR REPLACE FUNCTION queen.log_queue_stats_all_v1(
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001')
RETURNS TABLE (queue_name TEXT, partitions BIGINT, segments BIGINT, messages BIGINT)
LANGUAGE sql STABLE
AS $$
    -- queen.queues is the only queue table; partition->queue is p.queue_id = q.id.
    -- The 'queue' stat row is one per queue ((stat_type, stat_key) is unique and
    -- stat_key is the queue id), so MAX() is only there to satisfy the GROUP BY —
    -- it never merges two rows.
    SELECT q.name,
           count(p.id)::bigint,
           COALESCE(MAX(st.segment_count), 0)::bigint,
           COALESCE(SUM(GREATEST(p.last_offset - p.log_start + 1, 0)), 0)::bigint
    FROM queen.queues q
    LEFT JOIN queen.stats st ON st.stat_type = 'queue' AND st.queue_id = q.id
    LEFT JOIN queen.log_partitions p ON p.queue_id = q.id
    WHERE q.tenant_id = p_tenant
    GROUP BY q.id, q.name
$$;

GRANT EXECUTE ON FUNCTION queen.log_queue_stats_all_v1(UUID) TO PUBLIC;

-- ----------------------------------------------------------------------------
-- log_queue_depth_v1: minimal per-partition backlog read for relays and
-- schedulers (GET /api/v1/resources/queues/:queue/depth). The console-grade
-- get_queue_detail_v2 above pays for timestamps, DLQ counts and a LATERAL over
-- log_segments per pending partition; a depth poller reads only watermark and
-- lease arithmetic per partition, so this is §9 coordination state and nothing else:
--     pending = GREATEST(last_offset - GREATEST(committed, log_start-1), 0)
--     processing = live leased span (committed, batch_end], clamped to pending
--     ready = pending - processing
-- touching only log_partitions ⋈ log_consumers (the named-group form is a
-- consumer-PK lookup), no segments, no message timestamps.
--
-- p_group NULL = queue-level pending under the same worst-cursor precedence
-- as the stats refresh (`cons` above — named groups win, '__QUEUE_MODE__'
-- speaks only when no named group exists), so the number agrees with what
-- get_queue_v2 and the dashboard publish. A named p_group = that group's own
-- backlog per partition (a group with no cursor row on a partition owes the
-- whole retained range, committed -1 — same convention as everywhere else).
-- The per-group form is also the scaling/ETA input: pending, processing and
-- ready against that consumer's own cursor and lease.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.log_queue_depth_v1(
    p_queue  TEXT,
    p_group  TEXT DEFAULT NULL,
    p_tenant UUID DEFAULT '00000000-0000-0000-0000-000000000001')
RETURNS JSONB
LANGUAGE sql STABLE
AS $$
    -- No matching queue → no row → SQL NULL, which the handler turns into the
    -- same 404 shape as GET /resources/queues/:queue.
    SELECT jsonb_build_object(
        'queue', q.name,
        'group', p_group,
        'pending', COALESCE(d.total, 0),
        'processing', COALESCE(d.processing, 0),
        'ready', COALESCE(d.ready, 0),
        -- PLAN_CONFLATION §2.5/§5.3. `pending` stays LOG depth (positions to
        -- retire). partitionsPending is the count of partitions that owe work —
        -- useful for every group (queenctl computed it client-side and called it
        -- partitionsNonEmpty). For a CONFLATING group it is also WORK depth:
        -- one handler invocation per non-empty partition, so effectivePending
        -- reads it instead of the log depth. A conflating queue at
        -- pending: 4 000 000, effectivePending: 12 is healthy; the same numbers
        -- on a non-conflating group are an incident.
        'partitionsPending', COALESCE(d.nonempty, 0),
        'partitionsReady', COALESCE(d.ready_parts, 0),
        'conflation', COALESCE(g.conflation, false),
        'effectivePending', CASE WHEN COALESCE(g.conflation, false)
                                 THEN COALESCE(d.nonempty, 0)
                                 ELSE COALESCE(d.total, 0) END,
        'effectiveReady', CASE WHEN COALESCE(g.conflation, false)
                               THEN COALESCE(d.ready_parts, 0)
                               ELSE COALESCE(d.ready, 0) END,
        'partitions', COALESCE(d.parts, '[]'::jsonb))
    FROM queen.queues q
    -- One indexed cgm_identity_uk lookup; NULL (⇒ conflation false) when the
    -- caller asked for queue-level depth or the group never registered.
    LEFT JOIN queen.consumer_groups_metadata g
      ON p_group IS NOT NULL
     AND g.queue_id = q.id
     AND g.consumer_group = p_group
     AND g.partition_name = ''
    LEFT JOIN LATERAL (
        SELECT SUM(t.pending)::bigint AS total,
               SUM(t.processing)::bigint AS processing,
               SUM(t.ready)::bigint AS ready,
               SUM(CASE WHEN t.pending > 0 THEN 1 ELSE 0 END)::bigint AS nonempty,
               SUM(CASE WHEN t.ready > 0 THEN 1 ELSE 0 END)::bigint AS ready_parts,
               jsonb_agg(jsonb_build_object('partition', t.pname,
                                            'pending', t.pending,
                                            'processing', t.processing,
                                            'ready', t.ready)
                         ORDER BY t.pname) AS parts
        FROM (
            SELECT b.pname,
                   b.pending,
                   LEAST(b.pending, b.leased)::bigint AS processing,
                   GREATEST(b.pending - LEAST(b.pending, b.leased), 0)::bigint AS ready
            FROM (
                SELECT p.name AS pname,
                       GREATEST(p.last_offset
                                - GREATEST(COALESCE(c.committed, -1), p.log_start - 1),
                                0)::bigint AS pending,
                       COALESCE(c.processing, 0)::bigint AS leased
                FROM queen.log_partitions p
                LEFT JOIN LATERAL (
                    -- Named depth is strictly group-scoped: the same PK row
                    -- supplies both its cursor and its live leased span.
                    SELECT lc.committed,
                           CASE WHEN lc.lease_expires_at > NOW()
                                THEN GREATEST(0, COALESCE(lc.batch_end, lc.committed)
                                                   - lc.committed)
                                ELSE 0 END::bigint AS processing
                    FROM queen.log_consumers lc
                    WHERE p_group IS NOT NULL
                      AND lc.partition_id = p.id
                      AND lc.consumer_group = p_group

                    UNION ALL

                    -- Queue-level depth keeps the existing cursor precedence:
                    -- named groups win as a class; queue mode is considered
                    -- only if no named cursor exists. Processing follows the
                    -- queue stats contract and sums every live lease; the
                    -- outer LEAST caps overlapping group work at pending.
                    SELECT COALESCE(
                               MIN(lc.committed) FILTER
                                   (WHERE lc.consumer_group <> '__QUEUE_MODE__'),
                               MIN(lc.committed) FILTER
                                   (WHERE lc.consumer_group = '__QUEUE_MODE__')) AS committed,
                           COALESCE(SUM(GREATEST(0,
                                      COALESCE(lc.batch_end, lc.committed) - lc.committed))
                                    FILTER (WHERE lc.lease_expires_at > NOW()), 0)::bigint
                               AS processing
                    FROM queen.log_consumers lc
                    WHERE lc.partition_id = p.id
                    -- An aggregate without GROUP BY emits one row even over
                    -- an empty/false WHERE. HAVING is the branch gate that
                    -- keeps this UNION arm absent for a named p_group.
                    HAVING p_group IS NULL
                ) c ON true
                WHERE p.queue_id = q.id
            ) b
        ) t
    ) d ON true
    WHERE q.name = p_queue AND q.tenant_id = p_tenant
$$;

GRANT EXECUTE ON FUNCTION queen.log_queue_depth_v1(TEXT, TEXT, UUID) TO PUBLIC;
