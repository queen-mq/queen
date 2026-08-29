-- ============================================================================
-- Tenant purge — queen.delete_tenant_data_v1
-- ============================================================================
--
-- The BROKER half of a tenant wipe. Deletes everything one tenant owns in the
-- `queen` schema and nothing anybody else owns.
--
-- ---------------------------------------------------------------------------
-- ORCHESTRATION ORDER (the whole wipe, both databases)
-- ---------------------------------------------------------------------------
--
--   1. queen_proxy.set_tenant_status(tenant, 'deleting')
--        -> the proxy 403s every request for that tenant (gateway.rs's status
--           gate, BEFORE authentication). Traffic stops. Nothing here fences
--           the data plane on its own: this function is not a lock, and a
--           producer still writing while it runs simply gets its rows purged
--           on the next call.
--
--   2. queen.delete_tenant_data_v1(<broker_tenant_uuid>)   <- THIS FUNCTION
--        run against the CELL's database, once per cluster of the tenant,
--        with the cluster's `queen_proxy.clusters.broker_tenant_uuid` (the
--        x-queen-tenant value the proxy injects) — NOT queen_proxy.tenants.id.
--        The two are different uuids by construction (001_init.sql gives
--        broker_tenant_uuid its own gen_random_uuid() default). Call it in a
--        loop until it answers {"done": true}.
--
--   3. queen_proxy.delete_tenant(tenant)
--        removes the control-plane rows. It is LAST because it is the step
--        that destroys the broker_tenant_uuid step 2 needs: run it first and
--        the cell's data is orphaned with no way left to name its owner.
--        (queen_proxy.delete_tenant returns those uuids in its result and in
--        its outbox event precisely so an operator who got the order wrong
--        can still recover them.)
--
-- The two halves are DELIBERATELY separate functions in separate schemas, and
-- neither references the other: `queen_proxy` may live in a different database
-- — in the cloud it always does (pxdb is shared by a whole cell), and a
-- self-hoster may have no `queen_proxy` at all. Nothing in this file mentions
-- a control plane, and nothing in the control plane mentions this file.
--
-- ---------------------------------------------------------------------------
-- WHY A QUEUE-DELETE LOOP IS NOT ENOUGH
-- ---------------------------------------------------------------------------
--
-- queen.delete_queue_v1 + the FK cascades off queen.queues(id) reach
-- log_partitions -> log_segments/log_consumers, consumer_watermarks,
-- queue-scoped consumer_groups_metadata, queue_lag_metrics and queen.stats.
-- That is the whole of a queue. It is NOT the whole of a TENANT: eleven other
-- tables carry `tenant_id` and are not queue-scoped, so a loop over
-- get_queues_v2 leaves every one of them behind. Two of those matter beyond
-- tidiness:
--
--   * queen.log_timers — timers fire SERVER-SIDE from the sweeper and never
--     pass through the proxy (verdict 2026-08-18), so a surviving timer keeps
--     producing after the tenant is 403'd, into a queue this purge just
--     deleted, which push then AUTO-CREATES. That is why timers are phase 1
--     and why the queue loop does not start until they are gone.
--   * queen.consumer_groups_metadata rows with queue_id IS NULL — the
--     namespace/task DISCOVERY subscriptions. They name no queue at all
--     (schema.sql), which is exactly why they carry tenant_id, and exactly
--     why no cascade can reach them.
--
-- The other nine (kv, kv_quota, kv_usage, ephemeral_queues, ephemeral_quota,
-- hotlist_repairs, queue_parked_replica, and the streaming engine's
-- queen_streams.queries + queen_streams.quota) are swept by tenant_id directly.
--
-- queen.consumer_watermarks is NOT in that list and must not be: it carries no
-- tenant_id (its scoping is inherited through the queen.queues FK — the
-- 2026-07-31 queue-identity merge) and the cascade clears it. A purge
-- statement written against a tenant_id column it does not have would fail at
-- schema-apply time, i.e. at every boot of every broker.
--
-- ---------------------------------------------------------------------------
-- COVERAGE IS CHECKED, NOT ASSUMED
-- ---------------------------------------------------------------------------
--
-- The list below is a constant, and the function REFUSES to run if the catalog
-- holds a `queen` OR `queen_streams` table with a `tenant_id` column that is
-- not in it. A tenant purge that silently misses a table added later is the
-- worst failure this function can have — it reports success and leaves customer
-- data behind, on the one code path whose entire job is that this does not
-- happen. Refusing is loud, and the fix is one line here.
--
-- ---------------------------------------------------------------------------
-- THE STREAMING SCHEMA
-- ---------------------------------------------------------------------------
--
-- `queen_streams` (002_streams_schema.sql) is a second schema, outside every
-- cascade here, and it holds three tables. ALL THREE are purged — the previous
-- revision of this section named queries as permanent residue that "CANNOT be
-- purged, by construction", and that construction (a globally-unique, tenantless
-- name) is gone as of 2026-08-27:
--
--   * queen_streams.queries — the registered query metadata, now carrying
--     `tenant_id`, with `name` unique PER TENANT. An ordinary phase-3 sweep,
--     listed in C_TENANT_TABLES like any other tenant-carrying table.
--   * queen_streams.quota — the per-tenant streams grant (one row). Same sweep.
--     Purged for the same reason kv_quota is: the tenant it grants no longer
--     exists, and a surviving grant row would silently re-authorize a recycled
--     uuid.
--   * queen_streams.state — per-(query, partition, key) aggregate values,
--     DERIVED FROM PAYLOADS (the same argument that pulls message_traces into
--     the queue loop). It is reached TWICE, on purpose:
--       - PHASE 2, per queue, keyed by that queue's partition ids. The cheap,
--         index-driven path (the state PK's partition prefix), and it STAYS:
--         log_partition_dead_v1 vetoes reclaiming a partition that still holds
--         stream state, so the state has to go before the partitions do.
--       - PHASE 3, keyed through the `query_id` FK, BEFORE any queries row is
--         deleted. This is the path that reaches ORPHAN state — rows whose
--         partition ids no longer resolve, left by a routine queue delete or
--         written by a pre-2026-08-27 cycle that accepted a bogus partition
--         uuid. The partition-driven sweep structurally cannot see those.
--     It carries no tenant_id and needs none: attribution is the FK to
--     `queries`, which survives the partitions. That is why the old caveat here
--     ("once the partitions are gone nothing can attribute it ever again") is
--     retired — it was true only while queries itself was tenantless.
--
-- ---------------------------------------------------------------------------
-- BATCHING
-- ---------------------------------------------------------------------------
--
-- A plpgsql function runs inside the CALLER's transaction, so "batched" here
-- means bounded work per CALL, with the caller providing the commit boundary
-- between calls.
--
-- BE PRECISE ABOUT LOCKS, because the temptation is to overclaim. PostgreSQL
-- releases neither row locks nor relation locks at statement end: they are held
-- to the end of the TRANSACTION, and this function has none of its own.
-- Measured on a throwaway database, on a tenant with ONE small queue: the
-- backend still held 172 locks (AccessShare, RowShare, RowExclusive, Exclusive)
-- after the function returned `{"phase": "complete"}`, and 2 immediately after
-- the caller's COMMIT. So:
--
--   * FOR UPDATE SKIP LOCKED protects THIS call from waiting on rows another
--     session holds — it leaves them for the next call;
--   * it does NOT stop other sessions waiting on the rows THIS call takes.
--     Those are held until the caller commits.
--
-- EVERY BOUNDED DELETE IS `ctid = ANY (ARRAY(SELECT ... LIMIT n FOR UPDATE SKIP
-- LOCKED))`, NEVER `ctid IN (SELECT ...)`, and that is not a style choice. With
-- IN, the planner is free to flatten the sublink into a semi-join and
-- RE-EXECUTE it once per outer row — measured here, `EXPLAIN` showed
--
--     Nested Loop Semi Join -> Subquery Scan -> Limit -> LockRows -> Seq Scan
--
-- so the LIMIT bounded each re-execution and a call with p_max_rows => 2
-- deleted all 5 of the tenant's timers in one statement. The budget was
-- decorative, and silently so: the result JSON reported the real count, and the
-- plan flip is data-dependent, so the same call is bounded on one cell and
-- unbounded on the next. ARRAY() forces an InitPlan, evaluated exactly once
-- (`Seq Scan ... Filter: (ctid = ANY ($1))`). Scoping is unaffected either way
-- — ctid is unique within a table and the subquery only ever yields this
-- tenant's rows — so this was a bound, never an isolation hole.
--
-- The safe contract is therefore the CALLER'S COMMIT CADENCE, and the defaults
-- are chosen to keep it short rather than to finish in few calls: one call is
-- meant to be a handful of queue deletes and a bounded sweep, not the whole
-- wipe. This is the lesson of the 2026-08-24 retention incident (a 28-minute
-- lock leak on a loaded pool): the danger on a live cell is never the total
-- work, it is one transaction that holds while it does all of it. Raise
-- p_max_queues only against a cell whose queues are known to be small, and
-- commit between calls — always.
--
--   p_max_queues  queues deleted per call (default 10). One queue is the
--                 indivisible unit: a queue's traces and stream state must be
--                 purged in the same transaction that deletes its partitions,
--                 or the partition_id that attributes them is gone.
--   p_max_rows    rows deleted per call PER SWEPT TABLE (default 20000). This
--                 bounds EVERY delete in the function, including the per-queue
--                 residue tables (message_traces, retention_history,
--                 queen_streams.state) — each of those keeps its own budget
--                 across the queue loop, and when one runs out the call returns
--                 `{"phase": "queue-residue"}` BEFORE deleting the queue whose
--                 rows are still there. Nothing here issues an unbounded
--                 DELETE; on a cell, the trace sweep of one busy queue is
--                 otherwise the single statement that decides how long the wipe
--                 transaction runs. queen_streams.state spends ONE budget
--                 across both of its passes (the per-queue one and the
--                 FK-keyed orphan sweep that opens phase 3), so a call cannot
--                 delete 2 x p_max_rows state rows in one transaction.
--
-- Call until the result says {"done": true}. Re-running after completion is a
-- no-op that answers done immediately; re-running after a partial failure
-- resumes — every phase is idempotent and none depends on the previous call
-- having finished.
-- ============================================================================

CREATE OR REPLACE FUNCTION queen.delete_tenant_data_v1(
    p_tenant              UUID,
    p_max_queues          INT     DEFAULT 10,
    p_max_rows            INT     DEFAULT 20000,
    p_allow_default_tenant BOOLEAN DEFAULT FALSE
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    -- EVERY table in schema `queen` OR `queen_streams` that carries a tenant_id
    -- column. Verified against the catalog on every call (see the coverage check
    -- below), so this list cannot silently go stale.
    C_TENANT_TABLES CONSTANT TEXT[] := ARRAY[
        -- Deleted by the queue loop, through queen.delete_queue_v1, NOT by the
        -- sweep: the queue delete has to clear the FK-less queen.log_txns and
        -- queen.log_dlq first, and a bare DELETE here would orphan both.
        'queen.queues',
        -- PHASE 1, before the queue loop. See the header: a surviving timer
        -- re-creates a deleted queue.
        'queen.log_timers',
        -- PHASE 3, after the queue loop.
        'queen.consumer_groups_metadata',
        'queen.hotlist_repairs',
        'queen.queue_parked_replica',
        'queen.kv',
        'queen.kv_quota',
        'queen.kv_usage',
        'queen.ephemeral_queues',
        'queen.ephemeral_quota',
        -- PHASE 3, and in a SECOND schema. These two are here because
        -- 002_streams_schema gave queen_streams.queries a tenant_id and added
        -- queen_streams.quota: the coverage check below scans BOTH schemas, so
        -- shipping that column without these two lines makes EVERY wipe on
        -- EVERY cell refuse, loudly, until someone adds them. That coupling is
        -- the design working — the check exists precisely so a new
        -- tenant-carrying table cannot be forgotten quietly.
        --
        -- Order matters for one of them: deleting a queries row CASCADEs into
        -- queen_streams.state (002's FK), so the bounded orphan sweep that
        -- opens phase 3 must empty the state first — see it below.
        'queen_streams.queries',
        'queen_streams.quota'
    ];
    C_DEFAULT_TENANT CONSTANT UUID := '00000000-0000-0000-0000-000000000001';

    v_unknown   TEXT[];
    v_tbl       TEXT;
    v_n         BIGINT;
    -- The two per-queue counters are pre-declared so `rows` has the SAME KEY
    -- SET on a call that deleted no queue as on one that deleted ten: a caller
    -- summing this object across a loop should not have to special-case a
    -- missing key, and an operator reading two consecutive results should not
    -- have to wonder whether an absent key means "zero" or "not attempted".
    v_rows      JSONB := jsonb_build_object('queen.message_traces', 0,
                                            'queen.retention_history', 0,
                                            'queen_streams.state', 0);
    v_queues    INT := 0;
    v_remaining BIGINT;
    v_left      BOOLEAN;
    v_q         RECORD;
    -- Per-call, per-table budgets for the three tables the queue loop sweeps
    -- (see BATCHING). Spent across queues, so one call cannot delete
    -- p_max_queues x p_max_rows trace rows in one transaction.
    v_bud_trace  INT := p_max_rows;
    v_bud_reten  INT := p_max_rows;
    v_bud_state  INT := p_max_rows;
    -- queen_streams may legitimately be absent (a database whose schema apply
    -- has not reached 002_streams_schema yet). Checked once, not per queue.
    v_streams   BOOLEAN := to_regclass('queen_streams.state') IS NOT NULL;
    -- UPGRADE WINDOW. This function's BODY and the streaming schema's SHAPE are
    -- two different artifacts, installed by two different statements, and a cell
    -- can hold a newer body than shape: schema apply is per-file and per-
    -- statement, cells that run QUEEN_APPLY_SCHEMA=false get their SQL applied
    -- BY HAND file by file, and on a mixed-version HA pair the last boot to
    -- apply wins for a function body while an applied ALTER is never reverted
    -- (an old binary's 002 cannot take `tenant_id` away again). So the two
    -- queen_streams entries in the list above are PROBED, not assumed: on a cell
    -- whose queries table predates tenancy, the generic sweep's
    -- `DELETE ... WHERE tenant_id = $1` would fail at runtime with a 42703, mid
    -- wipe, on the one code path that must not be surprised. Skipping them
    -- instead is the same posture as v_streams above — do the part that exists.
    --
    -- pg_catalog, NOT information_schema, for the same reason as the coverage
    -- check: information_schema is filtered by privilege, so a role that cannot
    -- see the column would read "absent" and silently skip a table that is
    -- really there, holding real customer rows.
    v_streams_q BOOLEAN := EXISTS (
        SELECT 1
          FROM pg_attribute a
          JOIN pg_class     cl ON cl.oid = a.attrelid
          JOIN pg_namespace n  ON n.oid = cl.relnamespace
         WHERE n.nspname = 'queen_streams'
           AND cl.relname = 'queries'
           AND cl.relkind = 'r'
           AND a.attname = 'tenant_id'
           AND a.attnum > 0
           AND NOT a.attisdropped);
    v_streams_quota BOOLEAN := to_regclass('queen_streams.quota') IS NOT NULL;
BEGIN
    IF p_tenant IS NULL THEN
        RAISE EXCEPTION 'delete_tenant_data_v1: tenant is required';
    END IF;
    -- The default tenant is where EVERY pre-tenancy caller lands (schema.sql's
    -- column default). On a single-tenant self-hosted broker it is the whole
    -- database, so an accidental call — a NULL coalesced somewhere upstream, a
    -- copy-paste of delete_queue_v1's defaulted signature — would wipe the
    -- installation. p_tenant therefore has NO default of its own, and naming
    -- the default one explicitly still requires saying so twice.
    IF p_tenant = C_DEFAULT_TENANT AND NOT p_allow_default_tenant THEN
        RAISE EXCEPTION 'delete_tenant_data_v1: refusing to purge the DEFAULT tenant % '
                        '(this is every pre-tenancy caller''s data); pass '
                        'p_allow_default_tenant => true if that is really the intent',
                        C_DEFAULT_TENANT;
    END IF;
    IF p_max_queues IS NULL OR p_max_queues < 1 THEN
        RAISE EXCEPTION 'delete_tenant_data_v1: max_queues must be >= 1';
    END IF;
    IF p_max_rows IS NULL OR p_max_rows < 1 THEN
        RAISE EXCEPTION 'delete_tenant_data_v1: max_rows must be >= 1';
    END IF;

    -- ------------------------------------------------------------------
    -- COVERAGE CHECK. pg_catalog, not information_schema: the latter is
    -- filtered by privilege, so a role that cannot see a table would read
    -- "covered" for it — the exact false negative this check exists to
    -- prevent.
    -- ------------------------------------------------------------------
    -- Both schemas: a future queen_streams table carrying tenant_id would
    -- otherwise be invisible to the only check that keeps this list honest.
    SELECT array_agg(n.nspname || '.' || cl.relname ORDER BY n.nspname, cl.relname)
      INTO v_unknown
      FROM pg_attribute a
      JOIN pg_class     cl ON cl.oid = a.attrelid
      JOIN pg_namespace n  ON n.oid = cl.relnamespace
     WHERE n.nspname IN ('queen', 'queen_streams')
       AND cl.relkind = 'r'
       AND a.attname = 'tenant_id'
       AND a.attnum > 0
       AND NOT a.attisdropped
       AND (n.nspname || '.' || cl.relname) <> ALL (C_TENANT_TABLES);
    IF v_unknown IS NOT NULL THEN
        RAISE EXCEPTION 'delete_tenant_data_v1: % carries tenant_id but is not in this '
                        'function''s table list — refusing to report a purge that would '
                        'leave tenant data behind. Add it to C_TENANT_TABLES in '
                        '031_tenant_purge.sql.', array_to_string(v_unknown, ', ');
    END IF;

    -- ==================================================================
    -- PHASE 1 — timers, before anything else.
    -- ==================================================================
    DELETE FROM queen.log_timers
     WHERE ctid = ANY (ARRAY(SELECT ctid FROM queen.log_timers
                              WHERE tenant_id = p_tenant
                              LIMIT p_max_rows
                              FOR UPDATE SKIP LOCKED));
    GET DIAGNOSTICS v_n = ROW_COUNT;
    v_rows := v_rows || jsonb_build_object('queen.log_timers', v_n);

    SELECT EXISTS (SELECT 1 FROM queen.log_timers WHERE tenant_id = p_tenant) INTO v_left;
    IF v_left THEN
        -- Budget spent (or a row is locked elsewhere) and timers remain. Stop
        -- HERE rather than deleting queues underneath them: a timer that fires
        -- into a purged queue re-creates it, and the wipe would chase its own
        -- tail. The caller commits and calls again.
        RETURN jsonb_build_object(
            'tenant', p_tenant, 'done', false, 'phase', 'timers',
            'queues', jsonb_build_object('deleted', 0,
                                         'remaining', (SELECT count(*) FROM queen.queues
                                                        WHERE tenant_id = p_tenant)),
            'rows', v_rows);
    END IF;

    -- ==================================================================
    -- PHASE 2 — the queues, one delete_queue_v1 each.
    -- ==================================================================
    FOR v_q IN
        SELECT id, name FROM queen.queues
         WHERE tenant_id = p_tenant
         ORDER BY id
         LIMIT p_max_queues
    LOOP
        -- Traces, retention audit rows and stream state all carry a
        -- queen.log_partitions id and NO foreign key to it (each for its own
        -- stated reason — schema.sql for the first two, 002_streams_schema for
        -- the third), so no queue cascade reaches them. For the first two, that
        -- also means nothing can attribute them to a tenant once the partitions
        -- are gone; stream state is the exception — its query FK still names an
        -- owner, which is what phase 3a below sweeps. They are all purged HERE
        -- anyway, in the same transaction that deletes the queue, because this
        -- is the CHEAP path: index-driven both ways
        -- (idx_message_traces_partition_id, idx_retention_history_partition,
        -- the state PK's partition prefix), and log_partition_dead_v1 vetoes
        -- reclaiming a partition that still holds stream state.
        --
        -- This is a deliberate exception to those "outlive the partition"
        -- rules, and it is narrow: they outlive a QUEUE delete, which is a
        -- routine operation whose audit is worth keeping. They do not outlive
        -- a TENANT delete, whose subject no longer exists — and message_traces
        -- and stream state are both payload-derived, so leaving them behind
        -- would make a GDPR erasure through this path a lie.
        --
        -- BOUNDED, and bounded BEFORE the queue is deleted. An unbounded
        -- `DELETE ... WHERE partition_id IN (...)` here is the one statement
        -- that decides how long the whole wipe transaction runs: measured, a
        -- single queue with 50 000 traces was deleted in ONE statement even at
        -- p_max_queues => 1, p_max_rows => 1, cascading into
        -- queen.message_trace_names as it went. Each table keeps its own budget
        -- across the loop; when a queue still has residue the call returns
        -- WITHOUT deleting that queue, so the partition ids that name the rest
        -- are still there on the next call.
        IF v_bud_trace > 0 THEN
            DELETE FROM queen.message_traces
             WHERE ctid = ANY (ARRAY(SELECT t.ctid FROM queen.message_traces t
                                      WHERE t.partition_id IN (SELECT id FROM queen.log_partitions
                                                                WHERE queue_id = v_q.id)
                                      LIMIT v_bud_trace
                                      FOR UPDATE SKIP LOCKED));
            GET DIAGNOSTICS v_n = ROW_COUNT;
            v_bud_trace := v_bud_trace - v_n;
            v_rows := jsonb_set(v_rows, '{queen.message_traces}',
                                to_jsonb(COALESCE((v_rows->>'queen.message_traces')::bigint, 0) + v_n));
        END IF;

        IF v_bud_reten > 0 THEN
            DELETE FROM queen.retention_history
             WHERE ctid = ANY (ARRAY(SELECT r.ctid FROM queen.retention_history r
                                      WHERE r.partition_id IN (SELECT id FROM queen.log_partitions
                                                                WHERE queue_id = v_q.id)
                                      LIMIT v_bud_reten
                                      FOR UPDATE SKIP LOCKED));
            GET DIAGNOSTICS v_n = ROW_COUNT;
            v_bud_reten := v_bud_reten - v_n;
            v_rows := jsonb_set(v_rows, '{queen.retention_history}',
                                to_jsonb(COALESCE((v_rows->>'queen.retention_history')::bigint, 0) + v_n));
        END IF;

        -- Dynamic only because the schema may be absent; the statement is a
        -- constant and the two values are bound.
        IF v_streams AND v_bud_state > 0 THEN
            EXECUTE
                'DELETE FROM queen_streams.state
                  WHERE ctid = ANY (ARRAY(SELECT s.ctid FROM queen_streams.state s
                                           WHERE s.partition_id IN (SELECT id FROM queen.log_partitions
                                                                     WHERE queue_id = $1)
                                           LIMIT $2
                                           FOR UPDATE SKIP LOCKED))'
                USING v_q.id, v_bud_state;
            GET DIAGNOSTICS v_n = ROW_COUNT;
            v_bud_state := v_bud_state - v_n;
            v_rows := jsonb_set(v_rows, '{queen_streams.state}',
                                to_jsonb(COALESCE((v_rows->>'queen_streams.state')::bigint, 0) + v_n));
        END IF;

        -- Anything left for THIS queue — budget spent, or a row another session
        -- holds — stops the call here, before delete_queue_v1 takes the
        -- partitions away. Three index probes on the tenant's own partitions.
        v_left := EXISTS (SELECT 1 FROM queen.message_traces t
                           WHERE t.partition_id IN (SELECT id FROM queen.log_partitions
                                                     WHERE queue_id = v_q.id))
               OR EXISTS (SELECT 1 FROM queen.retention_history r
                           WHERE r.partition_id IN (SELECT id FROM queen.log_partitions
                                                     WHERE queue_id = v_q.id));
        IF NOT v_left AND v_streams THEN
            EXECUTE
                'SELECT EXISTS (SELECT 1 FROM queen_streams.state s
                                 WHERE s.partition_id IN (SELECT id FROM queen.log_partitions
                                                           WHERE queue_id = $1))'
                INTO v_left USING v_q.id;
        END IF;
        IF v_left THEN
            RETURN jsonb_build_object(
                'tenant', p_tenant, 'done', false, 'phase', 'queue-residue',
                'queues', jsonb_build_object(
                    'deleted', v_queues,
                    'remaining', (SELECT count(*) FROM queen.queues WHERE tenant_id = p_tenant)),
                'rows', v_rows);
        END IF;

        -- The one implementation of queue deletion (013_analytics.sql). Called
        -- by NAME because that is its signature; the tenant argument is what
        -- keeps a same-named queue of another tenant out of reach.
        PERFORM queen.delete_queue_v1(v_q.name, p_tenant);
        v_queues := v_queues + 1;
    END LOOP;

    SELECT count(*) INTO v_remaining FROM queen.queues WHERE tenant_id = p_tenant;
    IF v_remaining > 0 THEN
        -- More queues than this call's budget. The sweeps below wait: several
        -- of them (hotlist_repairs, queue_parked_replica) are written by the
        -- live engine for queues that still exist, so sweeping them now would
        -- delete rows the remaining queues are about to write again.
        RETURN jsonb_build_object(
            'tenant', p_tenant, 'done', false, 'phase', 'queues',
            'queues', jsonb_build_object('deleted', v_queues, 'remaining', v_remaining),
            'rows', v_rows);
    END IF;

    -- ==================================================================
    -- PHASE 3a — ORPHAN STREAM STATE, through the query FK.
    --
    -- Runs BEFORE the generic sweep, and the ordering is not cosmetic: deleting
    -- a queen_streams.queries row CASCADEs into queen_streams.state (002's
    -- `ON DELETE CASCADE`), and a cascade is NOT bounded by the LIMIT of the
    -- statement that triggers it. Left to the generic loop, one "bounded"
    -- delete of 20 000 query rows would drag an unbounded state delete into the
    -- same transaction — the exact shape of the 2026-08-24 lock incident this
    -- function's BATCHING section is written against. Emptying the state first
    -- keeps that cascade empty by the time it fires.
    --
    -- It is also the ONLY sweep that reaches ORPHAN state: rows whose
    -- partition_id no longer resolves (a routine queue delete took the
    -- partitions, or a pre-2026-08-27 cycle wrote a bogus partition uuid). The
    -- phase-2 pass is keyed BY partition and structurally cannot see them; this
    -- one is keyed by the query FK, which is what makes every state row
    -- tenant-attributable no matter what happened to its partition.
    --
    -- Index-driven: `query_id` is the leading column of the state PK.
    -- Same budget as phase 2's per-queue pass (v_bud_state), deliberately —
    -- one call, one p_max_rows worth of state deletes, however it is split.
    -- ==================================================================
    IF v_streams AND v_streams_q THEN
        IF v_bud_state > 0 THEN
            EXECUTE
                'DELETE FROM queen_streams.state
                  WHERE ctid = ANY (ARRAY(SELECT s.ctid FROM queen_streams.state s
                                           WHERE s.query_id IN (SELECT id FROM queen_streams.queries
                                                                 WHERE tenant_id = $1)
                                           LIMIT $2
                                           FOR UPDATE SKIP LOCKED))'
                USING p_tenant, v_bud_state;
            GET DIAGNOSTICS v_n = ROW_COUNT;
            v_bud_state := v_bud_state - v_n;
            v_rows := jsonb_set(v_rows, '{queen_streams.state}',
                                to_jsonb(COALESCE((v_rows->>'queen_streams.state')::bigint, 0) + v_n));
        END IF;

        -- Budget spent, or a row another session holds: stop HERE, before the
        -- generic loop deletes a single queries row. The caller commits and
        -- calls again; the queries rows that attribute the rest are still there.
        EXECUTE
            'SELECT EXISTS (SELECT 1 FROM queen_streams.state s
                             WHERE s.query_id IN (SELECT id FROM queen_streams.queries
                                                   WHERE tenant_id = $1))'
            INTO v_left USING p_tenant;
        IF v_left THEN
            RETURN jsonb_build_object(
                'tenant', p_tenant, 'done', false, 'phase', 'streams-state',
                'queues', jsonb_build_object('deleted', v_queues, 'remaining', 0),
                'rows', v_rows);
        END IF;
    END IF;

    -- ==================================================================
    -- PHASE 3 — the tables no queue delete can reach.
    --
    -- Order inside this phase carries no FK argument: none of these tables
    -- references another, by design (kv and log_timers are deliberately
    -- FK-free for the same reason log_txns is — the write path must never pay
    -- FK-trigger cost). The one FK that does exist, queries -> state, is
    -- handled by phase 3a above, which empties the child before the parent is
    -- swept. They are swept in the constant's order so the audit trail in
    -- `rows` reads the same way every time.
    -- ==================================================================
    FOREACH v_tbl IN ARRAY C_TENANT_TABLES LOOP
        CONTINUE WHEN v_tbl = 'queen.queues';       -- phase 2
        CONTINUE WHEN v_tbl = 'queen.log_timers';   -- phase 1
        -- Upgrade window (see v_streams_q / v_streams_quota): a shape older
        -- than this body has no tenant_id on queries and no quota table at all,
        -- and the statement below would then fail with a 42703 / 42P01 mid
        -- wipe. Skip what does not exist; there is nothing there to leave behind.
        CONTINUE WHEN v_tbl = 'queen_streams.queries' AND NOT v_streams_q;
        CONTINUE WHEN v_tbl = 'queen_streams.quota'   AND NOT v_streams_quota;

        -- Dynamic only in the table NAME, which comes from the constant above
        -- and never from an argument. The tenant is bound, not interpolated.
        -- The alternative — eleven hand-written DELETEs — drifts from the
        -- coverage check the moment someone edits one and not the other, and
        -- that drift is precisely the failure this function must not have.
        EXECUTE format(
            'DELETE FROM %s WHERE ctid = ANY (ARRAY('
            '  SELECT ctid FROM %s WHERE tenant_id = $1 LIMIT $2 FOR UPDATE SKIP LOCKED))',
            v_tbl, v_tbl)
        USING p_tenant, p_max_rows;
        GET DIAGNOSTICS v_n = ROW_COUNT;
        v_rows := v_rows || jsonb_build_object(v_tbl, v_n);
    END LOOP;

    -- ==================================================================
    -- Done only when NOTHING is left anywhere. Every probe is an index
    -- lookup on the tenant prefix, so this costs one page per table.
    -- ==================================================================
    v_left := false;
    FOREACH v_tbl IN ARRAY C_TENANT_TABLES LOOP
        -- Same two guards as the sweep above, and they must match it exactly: a
        -- probe of a column the sweep skipped would raise instead of answering,
        -- and a probe the sweep ran but this loop skipped would report `done`
        -- over rows still sitting there.
        CONTINUE WHEN v_tbl = 'queen_streams.queries' AND NOT v_streams_q;
        CONTINUE WHEN v_tbl = 'queen_streams.quota'   AND NOT v_streams_quota;
        EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s WHERE tenant_id = $1)', v_tbl)
        INTO v_left USING p_tenant;
        EXIT WHEN v_left;
    END LOOP;

    -- 'sweep' here means a bounded delete above hit its p_max_rows budget (or
    -- skipped a row another session had locked) — not a failure. Call again.
    RETURN jsonb_build_object(
        'tenant', p_tenant,
        'done', NOT v_left,
        'phase', CASE WHEN v_left THEN 'sweep' ELSE 'complete' END,
        'queues', jsonb_build_object('deleted', v_queues, 'remaining', 0),
        'rows', v_rows);
END;
$$;

COMMENT ON FUNCTION queen.delete_tenant_data_v1(UUID, INT, INT, BOOLEAN) IS
    'Purge one tenant''s data from the queen AND queen_streams schemas: timers first, then '
    'queues via delete_queue_v1 (with their traces, retention history and stream state), '
    'then the orphan stream state reachable through the query FK, then every '
    'tenant-carrying table no cascade reaches — including queen_streams.queries and '
    'queen_streams.quota. Bounded per call — loop until {"done": true}. Wipe order: '
    'queen_proxy.set_tenant_status(deleting) -> queen.delete_tenant_data_v1 -> '
    'queen_proxy.delete_tenant.';

-- Same grant as queen.delete_queue_v1 (013_analytics.sql), and the same
-- reasoning: the broker connects as one role, so PUBLIC here is not a wider
-- blast radius than the queue delete this function loops over.
GRANT EXECUTE ON FUNCTION queen.delete_tenant_data_v1(UUID, INT, INT, BOOLEAN) TO PUBLIC;
