-- ============================================================================
-- PLAN_KV_TIMERS.md §3.3, §4, §6.2 — scheduled messages (timers).
--
--   queen.log_timers               staging for messages that are not in the log YET
--   queen.log_timers_apply_v1      schedule / reschedule / cancel, ONE code path
--   queen.log_timers_due_v1        the sweeper's probe (next wake-up, due count, lateness)
--   queen.log_timers_claim_v1      lease N due rows, SKIP LOCKED, fair across tenants
--   queen.log_timers_fire_v1       verify + provision + pre-lock + push + DELETE, ONE txn
--   queen.log_timers_fail_v1       back off a failed fire without losing the row
--   queen.log_timers_dlq_v1        archive an exhausted timer straight into queen.log_dlq
--   queen.log_timers_peek_v1       one key, with payload
--   queen.log_timers_list_v1       keyset over the PK, QUEUE MANDATORY
--
-- A timer cannot live in the log, and that is a proof and not a preference
-- (§1.1): the pop is a CONTIGUOUS offset scan from committed+1, so a future
-- frame in the middle either blocks the cursor for everyone or is skipped and
-- lost; a row inside a solid zstd blob is not deletable (queen.log_segments.blob
-- is opaque to SQL, 001_log_schema.sql:85-86); and a pending future frame would
-- pin its segment against log_start for the whole wait, i.e. a 90-day timer
-- would keep 90 days of segments alive. So: staging here, and the log sees the
-- message only at the fire, inside ONE transaction with the DELETE.
--
-- ============================================================================
-- LOCK ORDER (PLAN_KV_TIMERS §2, the gate of the whole plan)
-- ============================================================================
-- Six spaces, one total order, shared with 003_log_push, 005_log_ack,
-- 006_log_maintenance and 024_kv:
--
--     queen.kv -> queen.log_timers -> ADV -> queen.queues
--              -> queen.log_partitions -> queen.log_consumers -> leaves
--
-- The rule, in one line (§2.2):
--
--   **No actor may ACQUIRE a lock on queen.kv or queen.log_timers after
--   acquiring a lock on queen.queues, queen.log_partitions or
--   queen.log_consumers.**
--
-- The word ACQUIRE is load-bearing. Re-touching a row THIS SAME TRANSACTION
-- ALREADY HOLDS is not an acquisition, it is a no-op on the wait graph (the
-- in-house precedent is the multi pre-lock note at 003_log_push.sql:132-134).
-- Without that distinction the rule is unfalsifiable, and the first person to
-- apply it literally rewrites the fire — whose last step deletes timer rows it
-- locked in its FIRST step.
--
--   **THE FIRE NEVER TAKES A LOCK THAT WAITS IN THE queen.log_timers SPACE.**
--
-- That sentence is the whole of §2.4 C2, and C2 is a CANCELLED correction: a
-- design document prescribed
--     PERFORM 1 FROM queen.log_timers t WHERE t.id = ANY(v_claimed)
--             ORDER BY ... FOR UPDATE
-- before the final DELETE. It is deleted here for two independent reasons.
-- First, this table HAS NO `id` COLUMN — the primary key is
-- (tenant_id, queue, timer_key) — so the statement does not compile and the
-- boot dies. Second, and this is the one that matters: a `FOR UPDATE` without
-- `SKIP LOCKED` would make the fire a WAITER in the timer space, contradicting
-- the invariant three paragraphs above. The concrete failure: a cancel inside a
-- transaction wire holds its row for the whole bundle (provisioning, pre-lock,
-- push, ack, fsync), and the fire — which used to skip the row and throw one
-- segment away — would now WAIT, holding the lease of 199 other timers, a
-- Lane::Maint slot and a connection. With PARALLELISM=1 one slow cancel stops
-- the fire of the entire broker. The correction fixed a bug that does not
-- exist: claim and delete in the canonical body are in the SAME transaction and
-- the delete acquires nothing new.
--
-- Whoever adds an advisory lock to the sweeper: the sweeper takes none today
-- (§1.9), and the rule for the day it does is that a BLOCKING advisory lock
-- must be taken BEFORE touching kv or timers, or not at all. 737_003 stays
-- reserved for PLAN_S3_ARCHIVE.md.
--
-- Intra-space order inside queen.log_timers (§2.3), because several actors take
-- more than one row:
--   * the wire and the standalone apply:   (queue, timer_key) ascending, tenant fixed
--   * the sweeper (claim, fire, fail, dlq): (tenant_id, queue, timer_key) ascending
-- The asymmetry is deliberate and is the kind of thing the next reader
-- "corrects": the sweeper's batch is multi-tenant, the wire's is not. Restricted
-- to the rows the two can ever contend for — which by definition share a
-- tenant_id — the two orders coincide.
--
-- Every ORDER BY that ranks a JSONB EXTRACTION pins the collation explicitly
-- (`(o.value->>'queue') COLLATE "C"`): an extraction does NOT inherit the
-- column's collation, and under the database default two pods with different
-- lc_collate or ICU minors order two keys in opposite directions, so two
-- overlapping bundles deadlock on some installations only (§2.4 C3).
--
-- ============================================================================
-- WHY THIS FILE IS NUMBERED 025, AND WHY THAT IS A COMPILE CHECK (§1.2)
-- ============================================================================
-- Nothing here names a partition_id. That is not an omission, it is the reason
-- this table is keyed by NAMES: no leg is added to queen.log_partition_dead_v1
-- (006_log_maintenance.sql) and its O(partitions) scan does not get more
-- expensive. And the check is FREE and MECHANICAL: log_partition_dead_v1 is
-- LANGUAGE sql, so it resolves its tables AT CREATION TIME; it lives in 006 and
-- this table in 025, so adding that veto leg KILLS THE BOOT. The file number is
-- the compilation proof of the invariant.
--
-- Symmetrically, 005_log_ack.sql applies BEFORE this file and calls
-- queen.log_timers_apply_v1 from the transaction wire. That works only because
-- the body of log_transaction_wire_v1 is plpgsql, which resolves names at
-- RUNTIME. Rewriting it as LANGUAGE sql would kill the boot at creation time.
--
-- GOTCHA: this SQL is include_str!-embedded in server/src/schema.rs, and a new
-- file that is not in the PROCEDURES list is NEVER applied (green boot, absent
-- tables, 42883 at runtime). Editing this file has no effect until `cargo build`.
--
-- INVARIANT OF MAINTENANCE: no path of this feature creates a partition, per
-- request or per timer. Queen's background maintenance scales with PARTITIONS.
-- ============================================================================

CREATE TABLE IF NOT EXISTS queen.log_timers (
    -- Same default as queen.hotlist_repairs (001_log_schema.sql:187): a
    -- pre-tenancy caller lands in the default tenant, byte-identical behaviour.
    tenant_id     UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    -- COLLATE "C" for the same reason as queen.kv, and the FIRST of the two is
    -- the lock order, not sargability:
    --   1. LOCK ORDER. This is the foundation of the intra-space order above.
    --      Under the default collation two pods with different lc_collate or
    --      ICU minors sort two keys in opposite directions and two overlapping
    --      bundles deadlock, on some installations only, which is the worst
    --      shape a deadlock can have.
    --   2. Byte order, hence identical between machines and stable across a
    --      libc/ICU upgrade, which is what makes the list keyset stable.
    -- Consequence to document: list results come back in BYTE order, so
    -- non-ASCII keys are not in locale-alphabetical order.
    queue         TEXT COLLATE "C" NOT NULL,
    timer_key     TEXT COLLATE "C" NOT NULL,
    "partition"   TEXT NOT NULL DEFAULT 'Default',
    deliver_at    TIMESTAMPTZ NOT NULL,
    -- The FIXED transactionId of the future frame. It is the SECONDARY net
    -- against double delivery; the PRIMARY guarantee is that the DELETE and the
    -- push share one transaction. Correctness NEVER depends on the dedup window.
    -- §20.2 (ratified): a reschedule OVERWRITES it — a rescheduled timer is a
    -- new message — so the dedup net does not survive a reschedule, and `txn`
    -- stays MANDATORY in a schedule op (the wire shape does not change).
    txn           TEXT NOT NULL,
    -- Stable id, minted by the broker (util::uuidv7_bytes) at SCHEDULE time, so
    -- the schedule API can answer "this is the id you will see" and a re-fire
    -- after a rolled-back attempt delivers the same id.
    -- NOT an input field: see the header of log_timers_apply_v1.
    message_id    UUID NOT NULL,
    payload       BYTEA NOT NULL,
    payload_zstd  BOOLEAN NOT NULL DEFAULT FALSE,
    -- Encryption happens at SCHEDULE, like on the push, so the payload is not
    -- in clear text here. CONSEQUENCE to declare: a queue whose encryption is
    -- enabled AFTER a timer was scheduled delivers that frame in clear. The
    -- flag travels with the frame.
    encrypted     BOOLEAN NOT NULL DEFAULT FALSE,
    -- The AUTHENTICATED sub of whoever scheduled. NOT a field of the JSON op:
    -- it reaches log_timers_apply_v1 as a SEPARATE ARGUMENT, from the
    -- middleware (server/src/auth.rs:31-36: "it is the ONLY source of that
    -- value, a client-supplied producerSub is never honored"). A producerSub
    -- inside an op is a RAISE 22023, never a field quietly ignored.
    producer_sub  TEXT,
    -- PERMANENT failures only. A transient error must NOT spend budget, or five
    -- minutes of database trouble would send every timer in the system to the
    -- DLQ, i.e. an infrastructure failure would become product loss.
    attempts      INT NOT NULL DEFAULT 0,
    last_error    TEXT,
    -- The fire's lease. "claim_token IS NOT NULL AND claimed_until > now()" is
    -- the ONLY definition of "in somebody's hands". A row merely BACKING OFF
    -- after a failure has claimed_until in the future and claim_token NULL, and
    -- stays cancellable. Two columns because collapsing them would make a
    -- poisoned timer uncancellable for the whole backoff, i.e. the user could
    -- not remove the broken thing.
    claimed_until TIMESTAMPTZ,
    claim_token   UUID,
    -- Contention spreader, NOT an ownership partition: every broker scans every
    -- shard (§1.10). GENERATED, so an update can never move it. The modulus is
    -- FIXED AT 64 FOREVER: the deployment model is always-virgin for the SCHEMA,
    -- not for the DATA, so changing it would silently re-shard rows already
    -- written. There is no env that changes it and there must not be
    -- (QUEEN_SWEEPER_SHARDS does not exist).
    -- NOTE: hashtextextended is not guaranteed stable across PostgreSQL majors.
    -- STORED freezes it at write time, so that is fine; but NO QUERY may ever
    -- recompute it for comparison, or on a different major it would get a shard
    -- different from the stored one.
    shard         SMALLINT NOT NULL
                  GENERATED ALWAYS AS ((hashtextextended(timer_key, 0) & 63)::smallint) STORED,
    -- The one and only scan key: "not visible to the sweeper before this
    -- instant". GENERATED and INDEXED on purpose, and the price is declared:
    -- the claim's UPDATE touches an indexed column, so it is never HOT. That is
    -- bought DELIBERATELY. With an index on (shard, deliver_at) the claim would
    -- stay HOT, but every poisoned timer in backoff would sit at the HEAD of the
    -- index and be re-read by every pass of every broker for the whole backoff:
    -- the cost would grow with the number of BROKEN timers, the one direction
    -- that must not degrade. Timer rates are control-plane rates, orders of
    -- magnitude below message rates.
    -- CASE rather than GREATEST+COALESCE with an '-infinity' literal, so the
    -- expression is unambiguously immutable and the generated-column check does
    -- not reject it.
    visible_at    TIMESTAMPTZ NOT NULL
                  GENERATED ALWAYS AS (
                      CASE WHEN claimed_until IS NULL OR claimed_until < deliver_at
                           THEN deliver_at ELSE claimed_until END) STORED,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, queue, timer_key)
);

-- The ONLY secondary index, with a single reader: the sweeper. shard first
-- because the expiry probe and the wake-up computation are both a LATERAL of 64
-- single-row seeks; visible_at second because the claim is a range scan in
-- expiry order per shard with ZERO rows filtered, which is the whole point of
-- having visible_at generated instead of a runtime predicate.
-- Deliberately NOT created: an index on deliver_at (no reader — the sweeper
-- orders by visible_at), one on (tenant_id, queue) (the leading columns of the
-- PK already serve peek and list), one on claim_token (the fire addresses by PK).
CREATE INDEX IF NOT EXISTS idx_log_timers_visible
    ON queen.log_timers (shard, visible_at);

-- Churn control, copied verbatim from queen.log_partitions
-- (001_log_schema.sql:47-68): same shape of problem, and between two passes the
-- dead tuples dwarf the live ones.
--
-- vacuum_truncate = off for the reason written out at 001_log_schema.sql:54-63
-- (2026-07-24, caught live with pg_stat_progress_vacuum in phase "truncating
-- heap"): heap truncation takes an ACCESS EXCLUSIVE lock, and THIS table is
-- exactly the shape that provokes it, because it empties and refills in steady
-- state. That lock was the root cause of the entire "wobble" class. Never
-- re-enable it.
ALTER TABLE queen.log_timers SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 500,
    autovacuum_vacuum_cost_delay = 0,
    vacuum_truncate = off);
-- fillfactor: BY ANALOGY with 001_log_schema.sql:39-46, NOT MEASURED — and on
-- this table the analogy is even weak, because the claim is non-HOT by
-- construction (visible_at is indexed), so the headroom buys page locality
-- rather than HOT chains. To be replaced with a MEASURED number after the first
-- soak, in the style of the dated blocks of 001 (§18.6 point 1).
ALTER TABLE queen.log_timers SET (fillfactor = 70);

-- NO "ALTER COLUMN payload SET STORAGE" here (§2.4 D8). EXTENDED is ALREADY the
-- default for bytea, so the statement would do nothing except take an ACCESS
-- EXCLUSIVE on this table AT EVERY BOOT (schema.rs re-applies everything every
-- time). On this table the damage is concrete: during a rolling restart every
-- in-flight transaction wire carrying a `timers` array stops at step 0b, i.e.
-- BEFORE the partition pre-lock, dragging its pushes and its acks with it —
-- exactly the scenario the vacuum_truncate comment above exists to prevent,
-- self-inflicted three lines below it. EXTENDED is the right value, and this
-- comment is where that gets said.
--
-- (Collateral, out of scope and NOT fixed here: 001_log_schema.sql:86 has the
-- same unguarded form on queen.log_segments.blob. It is the precedent to NOT
-- copy.)

-- Precedent and caveat in the same place: 010_log_admin.sql grants
-- queen.hotlist_repairs the same way. Every SP here is SECURITY INVOKER (there
-- is not one SECURITY DEFINER in the whole schema), so the GRANT is needed.
-- CONSEQUENCE, written here so an audit does not have to find it: per-tenant
-- isolation on this table is EXACTLY a WHERE clause inside these SPs. There is
-- no RLS. Any direct SQL reader bypasses it, and NO HTTP route may ever build a
-- query against queen.log_timers outside these SPs. There is a test that checks
-- that mechanically (§15).
GRANT SELECT, INSERT, UPDATE, DELETE ON queen.log_timers TO PUBLIC;

-- queen.log_timers READS queen.kv_quota and WRITES queen.kv_usage, both created
-- by 024_kv.sql. It creates neither: ONE quota row per tenant for ALL the state
-- of both features, or two quota tables diverge (§3.3).


-- ============================================================================
-- log_timers_apply_v1 — schedule, reschedule and cancel. ONE code path.
--
--   p_ops:  [{"op":"schedule"|"reschedule"|"cancel", "queue", "timerKey",
--            "partition"?, "delayMs", "txn", "payload" (base64),
--            "payloadZstd"?, "encrypted"?, "_messageId"?}, ...]
--
-- Returns a JSONB array INDEX-ALIGNED with p_ops. Closed status taxonomy
-- (§4.1): scheduled | rescheduled | cancelled | absent | too_late. Closed
-- because a client that has to tell them apart writes a switch, not a regex.
--
-- p_producer_sub is a SEPARATE ARGUMENT, never read from the ops: the same
-- discipline auth.rs:31-36 declares for the push (single source, a
-- client-supplied value is never honored). Without it a timer would be the only
-- way in the whole product to forge the provenance of a frame, because
-- producer_sub is a frame's only non-repudiable field. Same for p_tenant, and
-- for the message id, which the handler mints with util::uuidv7_bytes and hands
-- in as `_messageId` (a name a client cannot spell, the same trick 005_log_ack
-- uses for `_tenant` at :925) so that the schedule API can answer "this is the
-- id you will see".
--
-- Therefore: producerSub, messageId, tenant/tenantId/_tenant, an absolute
-- deliverAt and delaySeconds present in an op are a RAISE 22023, never a field
-- quietly ignored. Ignored, the caller sees success and the audit sees a normal
-- frame, which is exactly how such a hole survives review.
--
-- ONLY RELATIVE DURATIONS on the wire, `delayMs` (§4.2, §20.6): deliver_at is
-- computed HERE as p_now + make_interval, so there is one clock and it is
-- Postgres's, and no broker's skew can enter. The declared rule of the product
-- is "durations that can be sub-second are in milliseconds, durations that
-- cannot are in seconds" — hence delayMs here and ttlSeconds in 024 (§20.1).
--
-- VALIDATE-THEN-APPLY, so NO SAVEPOINTS: every recoverable rejection is decided
-- in one pass BEFORE the first write. Per-element BEGIN/EXCEPTION would cost a
-- subxid per element, and past 64 live subxids per top-level transaction the
-- subxid cache overflows and EVERY snapshot on the machine becomes suboverflowed.
--
-- ONE (queue, timer_key) AT MOST ONCE PER CALL, checked in validation. This is
-- LOAD-BEARING, not a convenience: it is what makes the intra-space order TOTAL.
-- Forbidding it is relaxable later; relaxing it without redoing the proof of §2
-- reopens the cycle.
--
-- Application order is ascending (queue, timer_key) COLLATE "C", NOT input
-- order; results are reported by INPUT ordinal (§6.4).
--
-- LOCK ORDER: this actor touches T and nothing else (§2.3 actor 13). In
-- particular it NEVER writes queen.queues — no destination-queue validation at
-- schedule time, the queue is born at the FIRE inside log_push_one_v1's missing
-- branch (§2.4 C6). If it provisioned, the wire would have TWO statements
-- writing queen.queues in one transaction with different sets, and two such
-- transactions wait on each other on the (tenant_id, name) unique index xid —
-- which the per-statement ORDER BY does not prevent, because they are DIFFERENT
-- statements.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.log_timers_apply_v1(
    p_ops          JSONB,
    p_tenant       UUID,
    p_producer_sub TEXT,
    p_now          TIMESTAMPTZ
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_n        INT;
    v_i        INT;
    v_op       JSONB;
    v_kind     TEXT;
    v_field    TEXT;
    v_dups     INT;
    v_results  JSONB[];
    v_filled   INT := 0;
    v_rec      RECORD;
    v_inserted BOOLEAN;
    v_mid      UUID;
    v_deliver  TIMESTAMPTZ;
    v_txn      TEXT;
    v_present  BOOLEAN;
BEGIN
    IF p_ops IS NULL OR jsonb_typeof(p_ops) <> 'array' THEN
        RAISE EXCEPTION 'QTIMER ops must be a JSON array' USING ERRCODE = '22023';
    END IF;
    v_n := jsonb_array_length(p_ops);
    IF v_n = 0 THEN
        RETURN '[]'::jsonb;
    END IF;
    IF p_tenant IS NULL THEN
        RAISE EXCEPTION 'QTIMER tenant is required' USING ERRCODE = '22023';
    END IF;

    -- ------------------------------------------------------------- VALIDATE
    FOR v_i IN 0 .. v_n - 1 LOOP
        v_op := p_ops -> v_i;
        IF jsonb_typeof(v_op) <> 'object' THEN
            RAISE EXCEPTION 'QTIMER op % is not an object', v_i USING ERRCODE = '22023';
        END IF;
        v_kind := v_op->>'op';
        IF v_kind IS NULL OR v_kind NOT IN ('schedule', 'reschedule', 'cancel') THEN
            RAISE EXCEPTION 'QTIMER op %: unknown operation', v_i USING ERRCODE = '22023';
        END IF;

        -- Server-owned fields. Rejected, never ignored (§4.2). The names are
        -- listed in both spellings a client might reach for, because the point
        -- is to fail at the first test rather than in an audit.
        FOREACH v_field IN ARRAY ARRAY[
            'producerSub', 'producer_sub',
            'messageId', 'message_id',
            'tenant', 'tenantId', '_tenant', 'tenant_id',
            'deliverAt', 'deliver_at', 'delaySeconds', 'delay_seconds',
            'attempts', 'claimToken', 'claim_token', 'claimedUntil'
        ] LOOP
            IF v_op ? v_field THEN
                RAISE EXCEPTION 'QTIMER op %: field % is server-owned and cannot be supplied',
                    v_i, v_field
                    USING ERRCODE = '22023',
                          HINT = 'producerSub and the tenant come from the authenticated request; '
                                 'the message id is minted by the broker; deliverAt is not '
                                 'expressible — the wire carries only the relative delayMs';
            END IF;
        END LOOP;

        IF COALESCE(v_op->>'queue', '') = '' THEN
            RAISE EXCEPTION 'QTIMER op %: queue is required', v_i USING ERRCODE = '22023';
        END IF;
        IF COALESCE(v_op->>'timerKey', '') = '' THEN
            RAISE EXCEPTION 'QTIMER op %: timerKey is required', v_i USING ERRCODE = '22023';
        END IF;
        IF v_op ? 'partition' AND COALESCE(v_op->>'partition', '') = '' THEN
            RAISE EXCEPTION 'QTIMER op %: partition, when present, must be non-empty', v_i
                USING ERRCODE = '22023';
        END IF;

        IF v_kind <> 'cancel' THEN
            IF jsonb_typeof(v_op->'delayMs') <> 'number' THEN
                RAISE EXCEPTION 'QTIMER op %: delayMs (a number of milliseconds) is required', v_i
                    USING ERRCODE = '22023',
                          HINT = 'a delayMs in the past is LEGAL and fires on the first cycle; '
                                 'an absolute instant is not expressible on this wire';
            END IF;
            -- §20.2: the txn is overwritten by every reschedule, and stays
            -- MANDATORY, so the wire shape never had to become optional.
            IF COALESCE(v_op->>'txn', '') = '' THEN
                RAISE EXCEPTION 'QTIMER op %: txn is required', v_i USING ERRCODE = '22023';
            END IF;
            IF jsonb_typeof(v_op->'payload') <> 'string' THEN
                RAISE EXCEPTION 'QTIMER op %: payload (base64) is required', v_i
                    USING ERRCODE = '22023';
            END IF;
        END IF;
    END LOOP;

    -- One (queue, timer_key) at most once per call — see the header for why
    -- this is load-bearing and not hygiene.
    SELECT count(*) INTO v_dups FROM (
        SELECT 1
        FROM jsonb_array_elements(p_ops) o
        GROUP BY (o.value->>'queue'), (o.value->>'timerKey')
        HAVING count(*) > 1
    ) d;
    IF v_dups > 0 THEN
        RAISE EXCEPTION 'QTIMER a (queue, timerKey) appears more than once in one call'
            USING ERRCODE = '22023',
                  HINT = 'one key at most once per call is what makes the intra-space lock '
                         'order total (PLAN_KV_TIMERS §2.2)';
    END IF;

    -- ---------------------------------------------------------------- APPLY
    v_results := array_fill(NULL::jsonb, ARRAY[v_n]);

    FOR v_rec IN
        SELECT o.value AS op, o.ord::int AS ord
        FROM jsonb_array_elements(p_ops) WITH ORDINALITY AS o(value, ord)
        -- The collation is pinned EXPLICITLY: a JSONB extraction does not
        -- inherit queue's COLLATE "C" (§2.4 C3 point 2).
        ORDER BY (o.value->>'queue') COLLATE "C", (o.value->>'timerKey') COLLATE "C"
    LOOP
        IF (v_rec.op->>'op') = 'cancel' THEN
            -- Same claim guard as the upsert: "is it out of your hands?".
            DELETE FROM queen.log_timers t
             WHERE t.tenant_id = p_tenant
               AND t.queue     = v_rec.op->>'queue'
               AND t.timer_key = v_rec.op->>'timerKey'
               AND (t.claim_token IS NULL OR t.claimed_until <= p_now)
            RETURNING t.txn INTO v_txn;

            IF FOUND THEN
                v_results[v_rec.ord] := jsonb_build_object(
                    'ok', true, 'status', 'cancelled',
                    'queue', v_rec.op->>'queue', 'timerKey', v_rec.op->>'timerKey',
                    'txn', v_txn);
            ELSE
                -- Zero rows is two different verdicts; one EXISTS probe under
                -- the same tenant separates them.
                SELECT EXISTS (
                    SELECT 1 FROM queen.log_timers t
                     WHERE t.tenant_id = p_tenant
                       AND t.queue     = v_rec.op->>'queue'
                       AND t.timer_key = v_rec.op->>'timerKey')
                INTO v_present;

                IF v_present THEN
                    -- §4.3: a VERDICT, with HTTP 200. The holder has already
                    -- decompressed and packed that payload and is about to
                    -- commit it; granting the cancel would leave "is it sent?"
                    -- with no answer. Bounded by the lease.
                    v_results[v_rec.ord] := jsonb_build_object(
                        'ok', false, 'status', 'too_late',
                        'queue', v_rec.op->>'queue', 'timerKey', v_rec.op->>'timerKey');
                ELSE
                    -- §4.4, and this is where a user gets hurt: DELETE and not
                    -- mark-done means THERE IS NO TOMBSTONE, so `absent` means
                    -- "no longer pending" and MAY MEAN ALREADY DELIVERED. The
                    -- authority is the log: look for the timer's txn in the
                    -- destination queue. Hence ok:false — the same lesson
                    -- already paid in-house on queue delete, where deleted:false
                    -- with a 200 read as success to every client that trusted
                    -- the field. The expected txn is echoed back so the check
                    -- needs no second API.
                    v_results[v_rec.ord] := jsonb_build_object(
                        'ok', false, 'status', 'absent',
                        'queue', v_rec.op->>'queue', 'timerKey', v_rec.op->>'timerKey',
                        'txn', v_rec.op->>'txn');
                END IF;
            END IF;
        ELSE
            -- Schedule and reschedule are the SAME upsert, so a client retry
            -- after a crash is safe by construction. attempts goes back to 0
            -- and last_error to NULL: a rescheduled timer is a NEW timer under
            -- an OLD name, and a freshly corrected payload must not inherit the
            -- budget spent by the one that was poisoning things.
            INSERT INTO queen.log_timers AS t (
                tenant_id, queue, timer_key, "partition", deliver_at, txn, message_id,
                payload, payload_zstd, encrypted, producer_sub,
                attempts, last_error, claimed_until, claim_token, created_at, updated_at)
            VALUES (
                p_tenant,
                v_rec.op->>'queue',
                v_rec.op->>'timerKey',
                COALESCE(v_rec.op->>'partition', 'Default'),
                p_now + make_interval(secs => (v_rec.op->>'delayMs')::double precision / 1000.0),
                v_rec.op->>'txn',
                COALESCE((v_rec.op->>'_messageId')::uuid, gen_random_uuid()),
                decode(v_rec.op->>'payload', 'base64'),
                COALESCE((v_rec.op->>'payloadZstd')::boolean, false),
                COALESCE((v_rec.op->>'encrypted')::boolean, false),
                p_producer_sub,
                0, NULL, NULL, NULL, p_now, p_now)
            ON CONFLICT (tenant_id, queue, timer_key) DO UPDATE SET
                "partition"   = EXCLUDED."partition",
                deliver_at    = EXCLUDED.deliver_at,
                txn           = EXCLUDED.txn,
                message_id    = EXCLUDED.message_id,
                payload       = EXCLUDED.payload,
                payload_zstd  = EXCLUDED.payload_zstd,
                encrypted     = EXCLUDED.encrypted,
                producer_sub  = EXCLUDED.producer_sub,
                attempts      = 0,
                last_error    = NULL,
                claimed_until = NULL,
                claim_token   = NULL,
                updated_at    = p_now
            -- "is it out of your hands?" Zero rows means the row exists and is
            -- claimed, i.e. too_late — ONE rule for cancel and reschedule alike,
            -- because granting the reschedule would deliver the OLD payload
            -- after the client believes it replaced it.
            WHERE t.claim_token IS NULL OR t.claimed_until <= p_now
            -- xmax = 0 is the standard discriminator between "the conflict arm
            -- did not run" and the opposite, and it is the only thing that tells
            -- scheduled from rescheduled without a second probe under the same lock.
            RETURNING (xmax = 0), t.message_id, t.deliver_at
                 INTO v_inserted, v_mid, v_deliver;

            IF FOUND THEN
                v_results[v_rec.ord] := jsonb_build_object(
                    'ok', true,
                    'status', CASE WHEN v_inserted THEN 'scheduled' ELSE 'rescheduled' END,
                    'queue', v_rec.op->>'queue', 'timerKey', v_rec.op->>'timerKey',
                    'txn', v_rec.op->>'txn',
                    'messageId', v_mid::text,
                    'deliverAt', to_char(v_deliver AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
            ELSE
                v_results[v_rec.ord] := jsonb_build_object(
                    'ok', false, 'status', 'too_late',
                    'queue', v_rec.op->>'queue', 'timerKey', v_rec.op->>'timerKey');
            END IF;
        END IF;
    END LOOP;

    -- Index-alignment guard BEFORE to_jsonb (§6.4). Without it an unfilled
    -- ordinal becomes a silent JSON null IN POSITION, which is precisely the
    -- misalignment class the guards of 003_log_push.sql:372-375 fail loudly on.
    SELECT count(*) INTO v_filled FROM unnest(v_results) r WHERE r IS NOT NULL;
    IF v_filled <> v_n THEN
        RAISE EXCEPTION 'QTIMER produced % results for % ops', v_filled, v_n;
    END IF;

    RETURN to_jsonb(v_results);
END;
$$;


-- ============================================================================
-- log_timers_due_v1 — the sweeper's probe. ONE call, one read-only transaction,
-- ONE server now().
--
-- Returns {"now","nextInMs","due","dueCapped","lateMs"}, EVERYTHING RELATIVE TO
-- THE SERVER CLOCK, so the broker never does arithmetic on a timestamp and no
-- inter-broker skew can enter. nextInMs NULL means the table is empty; it is
-- SIGNED, because "already overdue" is the state the sweeper loop reacts to
-- (§7.2 tests nextInMs <= 0).
--
-- Two statements, both LATERAL per shard, because
-- `shard = ANY(p_shards) ORDER BY visible_at` is NOT index-ordered: a
-- ScalarArrayOpExpr on the leading column does not produce output ordered on
-- the second, and the planner inserts a Sort.
--
-- `due` is CAPPED: LIMIT p_cap + 1 with dueCapped saying whether the ceiling was
-- touched, and p_cap comes down to 2000, because this probe runs EVERY cycle and
-- an exact count would be O(due) precisely in the failure the probe exists to
-- detect.
--
-- lateMs is a 64-seek APPROXIMATION and that has to be said: it UNDERSTATES when
-- a row with an old deliver_at has had its visible_at pushed forward by a
-- backoff, which is exactly the case where the lateness is intended.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.log_timers_due_v1(
    p_shards SMALLINT[],
    p_now    TIMESTAMPTZ,
    p_cap    INT
) RETURNS JSONB
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_shards SMALLINT[] := COALESCE(p_shards, '{}'::smallint[]);
    v_cap    INT        := GREATEST(COALESCE(p_cap, 2000), 1);
    v_next   TIMESTAMPTZ;
    v_cnt    BIGINT;
BEGIN
    IF COALESCE(array_length(v_shards, 1), 0) = 0 THEN
        RETURN jsonb_build_object(
            'now', to_char(p_now AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
            'nextInMs', NULL, 'due', 0, 'dueCapped', false, 'lateMs', 0);
    END IF;

    -- Earliest visible_at anywhere in the scanned shards: 64 single-row seeks.
    -- It answers BOTH the wake-up and the lateness, which is why lateMs costs
    -- nothing extra.
    SELECT min(x.visible_at) INTO v_next
    FROM unnest(v_shards) AS s(shard)
    CROSS JOIN LATERAL (
        SELECT t.visible_at
        FROM queen.log_timers t
        WHERE t.shard = s.shard
        ORDER BY t.visible_at
        LIMIT 1
    ) x;

    SELECT count(*) INTO v_cnt FROM (
        SELECT 1
        FROM unnest(v_shards) AS s(shard)
        CROSS JOIN LATERAL (
            SELECT 1
            FROM queen.log_timers t
            WHERE t.shard = s.shard AND t.visible_at <= p_now
            ORDER BY t.visible_at
            LIMIT v_cap + 1
        ) y
        LIMIT v_cap + 1
    ) z;

    RETURN jsonb_build_object(
        'now',      to_char(p_now AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
        'nextInMs', CASE WHEN v_next IS NULL THEN NULL
                         ELSE (EXTRACT(EPOCH FROM (v_next - p_now)) * 1000)::bigint END,
        'due',       LEAST(v_cnt, v_cap::bigint),
        'dueCapped', v_cnt > v_cap,
        'lateMs',   CASE WHEN v_next IS NULL OR v_next > p_now THEN 0::bigint
                         ELSE (EXTRACT(EPOCH FROM (p_now - v_next)) * 1000)::bigint END);
END;
$$;


-- ============================================================================
-- log_timers_claim_v1 — lease p_max_rows due rows and hand them to the broker.
--
-- DROP FUNCTION IF EXISTS from day one, because the RETURN TYPE is part of the
-- signature and CREATE OR REPLACE refuses to change it: this pair is the
-- boot-idempotence half of the house DROP+CREATE convention (004_log_pop.sql
-- does the same for every pop signature), NOT an upgrade step.
--
-- Returns TEXT for uuids: the broker has no uuid crate.
--
-- ---------------------------------------------------------------------------
-- FOR UPDATE SKIP LOCKED LIVES INSIDE THE LATERAL. The sweeper NEVER waits on a
-- timer row, and that is what keeps it out of every deadlock cycle (§2.3 actor
-- 5: an actor that never waits cannot be an edge of the wait graph, whatever
-- order it visits).
--
-- HOW `LIMIT` BEHAVES INSIDE A LATERAL WITH `FOR UPDATE SKIP LOCKED`, because
-- the correctness of multi-broker draining rests entirely on it: the inner
-- SELECT locks rows AS IT PRODUCES THEM, and rows another broker already holds
-- are skipped rather than waited on, so two brokers scanning the same shard get
-- DISJOINT sets and neither blocks. The outer LIMITs stop pulling once satisfied,
-- so the over-locking is bounded by the innermost LIMIT of the last tenant
-- visited; those extra rows are locked but NOT updated, so they stay unclaimed
-- and become available again at commit, which happens in milliseconds.
--
-- FAIRNESS ACROSS TENANTS, inside the claim and not after it. The naive form is
-- `WHERE shard = s AND visible_at <= now ORDER BY visible_at LIMIT v_per`, i.e.
-- strictly oldest-first and BLIND TO THE TENANT. Since the shard is a hash of
-- the timer_key and carries no tenant, one tenant's timers spread over all 64
-- shards: a tenant scheduling 200 000 timers all due at 09:00 fills EVERY claim
-- batch of EVERY broker for as long as it takes to drain, and another tenant's
-- single 09:00:01 reminder arrives minutes late with queen_timers_oldest_late_seconds
-- alarming for the whole cell and no metric naming the culprit. A per-tenant
-- budget applied AFTER the claim does not work: discarding already-claimed rows
-- puts them back at the head of the index (visible_at did not change) and they
-- get reselected next cycle — a livelock that burns the whole batch every pass.
-- Hence: round robin over tenants INSIDE the claim, a LATERAL over
-- (SELECT DISTINCT tenant_id ... LIMIT k) with LIMIT p_per_tenant inside.
-- QUEEN_SWEEPER_FIRE_RATE_PER_TENANT then becomes applicable, because it acts on
-- SELECTION and not on DISCARD.
--
-- THE PER-SHARD FLOOR. v_per = p_max_rows / n_shards is 3 with the defaults
-- (200/64) and 1 if an operator lowers the batch, and with v_per = 1 a single
-- late shard can never catch up. So v_per = GREATEST(ceil(p_max_rows/n_shards), 8).
--
-- ROW ORDER IS A CONTRACT, not an artifact: §4.2 says the order of a timer
-- inside its key is decided AT THE FIRE, and that order is ORDER BY visible_at,
-- i.e. expiry order and not schedule order. A plain UPDATE ... RETURNING does
-- NOT preserve it, and it cannot be recovered afterwards either, because the
-- claim itself pushes every claimed row's visible_at out to the lease. Hence the
-- CTE: pick FOR UPDATE SKIP LOCKED, remember visible_at there, and order the
-- final SELECT by the value captured before the update.
-- ============================================================================
DROP FUNCTION IF EXISTS queen.log_timers_claim_v1(SMALLINT[], TIMESTAMPTZ, INT, INT, INT);

CREATE OR REPLACE FUNCTION queen.log_timers_claim_v1(
    p_shards     SMALLINT[],
    p_now        TIMESTAMPTZ,
    p_lease_ms   INT,
    p_max_rows   INT,
    p_per_tenant INT
) RETURNS TABLE (
    r_tenant       TEXT,
    r_queue        TEXT,
    r_timer_key    TEXT,
    r_partition    TEXT,
    r_txn          TEXT,
    r_message_id   TEXT,
    r_payload      BYTEA,
    r_payload_zstd BOOLEAN,
    r_encrypted    BOOLEAN,
    r_producer_sub TEXT,
    r_attempts     INT,
    r_late_ms      BIGINT,
    r_token        TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_shards SMALLINT[] := COALESCE(p_shards, '{}'::smallint[]);
    v_ns     INT := COALESCE(array_length(COALESCE(p_shards, '{}'::smallint[]), 1), 0);
    v_max    INT := GREATEST(COALESCE(p_max_rows, 200), 1);
    v_per    INT;
    v_pt     INT;
    v_tn     INT;
BEGIN
    IF v_ns = 0 THEN
        RETURN;
    END IF;

    v_per := GREATEST(ceil(v_max::numeric / v_ns)::int, 8);
    -- The per-tenant knob can never exceed the per-shard cap: past it, it is the
    -- shard cap that decides and the extra rows would only be locked and dropped.
    v_pt  := GREATEST(LEAST(COALESCE(p_per_tenant, v_per), v_per), 1);
    -- At most v_per tenants can contribute to a shard's v_per rows, so that is
    -- how many distinct tenants the round robin needs to look at.
    v_tn  := v_per;

    RETURN QUERY
    WITH picked AS MATERIALIZED (
        SELECT c.tenant_id, c.queue, c.timer_key, c.visible_at
        FROM unnest(v_shards) AS s(shard)
        CROSS JOIN LATERAL (
            SELECT b.tenant_id, b.queue, b.timer_key, b.visible_at
            FROM (
                SELECT DISTINCT d.tenant_id
                FROM queen.log_timers d
                WHERE d.shard = s.shard AND d.visible_at <= p_now
                ORDER BY d.tenant_id
                LIMIT v_tn
            ) tn
            CROSS JOIN LATERAL (
                SELECT z.tenant_id, z.queue, z.timer_key, z.visible_at
                FROM queen.log_timers z
                WHERE z.shard = s.shard
                  AND z.tenant_id = tn.tenant_id
                  AND z.visible_at <= p_now
                ORDER BY z.visible_at
                LIMIT v_pt
                FOR UPDATE SKIP LOCKED
            ) b
            ORDER BY b.visible_at
            LIMIT v_per
        ) c
        LIMIT v_max
    ),
    upd AS (
        UPDATE queen.log_timers t
           SET claimed_until = p_now + make_interval(
                   secs => GREATEST(COALESCE(p_lease_ms, 30000), 1)::double precision / 1000.0),
               -- One token PER ROW: gen_random_uuid() is volatile, so it is
               -- evaluated per updated row. The token is the only thing that
               -- makes the lease safe — a reclaim after an expiry must not be
               -- able to reuse the dead holder's proof of ownership.
               claim_token   = gen_random_uuid(),
               updated_at    = p_now
          FROM picked k
         WHERE t.tenant_id = k.tenant_id AND t.queue = k.queue AND t.timer_key = k.timer_key
        RETURNING t.tenant_id, t.queue, t.timer_key, t."partition", t.deliver_at, t.txn,
                  t.message_id, t.payload, t.payload_zstd, t.encrypted, t.producer_sub,
                  t.attempts, t.claim_token
    )
    SELECT u.tenant_id::text, u.queue, u.timer_key, u."partition", u.txn,
           u.message_id::text, u.payload, u.payload_zstd, u.encrypted, u.producer_sub,
           u.attempts,
           GREATEST(0::bigint, (EXTRACT(EPOCH FROM (p_now - u.deliver_at)) * 1000)::bigint),
           u.claim_token::text
    FROM upd u
    JOIN picked k
      ON k.tenant_id = u.tenant_id AND k.queue = u.queue AND k.timer_key = u.timer_key
    ORDER BY k.visible_at, u.tenant_id, u.queue, u.timer_key;
END;
$$;


-- ============================================================================
-- log_timers_fire_v1 — THE CANONICAL BODY, AND THERE IS ONLY ONE.
--
-- The broker decompressed, grouped by (tenant, queue, partition), packed with
-- pack_frames + zstd + xxh3 OUTSIDE any transaction, and makes exactly ONE call.
-- Here, in ONE transaction: verify the lease, release what will not be
-- delivered, provision, pre-lock, push, delete.
--
--   segment-parallel arrays (byte-for-byte the order of log_push_multi_v1):
--     p_tenants, p_queues, p_partitions, p_msg_counts, p_hashes, p_blobs
--   timer-parallel arrays (the frame -> timer map):
--     p_keys, p_seg_of (1-BASED segment index), p_tokens
--
-- Returns a JSONB array index-aligned with the SEGMENT arrays; closed taxonomy
-- result = fired | stale | duplicate, which is also what
-- queen_timers_fired_total{result} counts.
--
-- ---------------------------------------------------------------------------
-- THE ORDER OF THE STEPS IS THE PROOF, NOT A STYLE (§2.4 C1, §6.2):
--
--  1. ALIGNMENT GUARDS, in the style of 003_log_push.sql. Equal array lengths,
--     octet_length(hashes[i]) = count[i]*16, and — the one that is easy to
--     forget — EXACTLY count[i] keys carrying seg_of = i. Without that last
--     guard a wrong frame->timer map would DELETE THE WRONG TIMER instead of
--     failing.
--  2. VERIFY THE CLAIM, WITH THE LOCK. FOR UPDATE SKIP LOCKED over every named
--     row, comparing claim_token AND claimed_until > p_now, so the definition of
--     "leased" exists in exactly one place. A segment is LIVE if and only if
--     ALL of its rows were locked with a matching token: all-or-nothing per
--     segment, because the blob is already packed and one missing timer makes
--     that blob wrong.
--     SKIP LOCKED here is not an optimisation: it is the invariant. The fire
--     never waits in the timer space.
--  3. SYMMETRIC RELEASE of the rows that will not be delivered. A stale segment
--     is thrown away by the broker, but its OTHER rows were locked successfully
--     by this transaction and still carry their claim: valid token, claimed_until
--     in the future, therefore visible_at in the future, therefore INVISIBLE to
--     the next claim for the whole lease. Concrete failure: 200 timers maturing
--     together on one (tenant, queue, partition), one concurrent cancel on one of
--     them, and all 200 vanish for 30 seconds while lateMs climbs in steps and no
--     metric names the cause. So they are released — written once, used twice:
--     the DUPLICATE arm releases exactly the same way.
--  4. PROVISIONING gated-on-missing, for LIVE segments only, ORDER BY the unique
--     key so ON CONFLICT does not convoy on queen.queues. A stale segment must
--     not even provision its destination, which is why verification comes first.
--  5. ONE set-based PRE-LOCK over the live segments' partitions, ascending
--     log_partitions.id, byte for byte the form of 003_log_push.sql:325-330 and
--     005_log_ack.sql:973-984. Mandatory: without it a multi-segment fire would
--     take partition locks in arbitrary order and could deadlock with a fusion
--     bundle.
--  6. PUSH per segment through queen.log_push_one_v1 — the single allocator code
--     path, with pid and window ALREADY RESOLVED so it does not re-resolve them
--     (the nested-lookup CPU the seg v2 rework eliminated).
--  7. DELETE of the delivered timers: rows ALREADY HELD since step 2, so no new
--     acquisition in the T space and no violation of §2.2. This is also why the
--     fire is exactly-once with nothing to reconcile: there is no "fired" state,
--     the row is simply gone, and "was it sent?" is answered by the log.
--
--  8. GROUPING IS BY (tenant_id, queue, partition), NEVER BY (queue, partition).
--     That is the caller's job, but this SP enforces its half: every segment
--     resolves its destination UNDER ITS OWN p_tenants[i], and two live segments
--     naming the same (tenant, queue, partition) are rejected. A tenant-blind
--     grouping would fuse two tenants' timers into one segment — the worst
--     isolation hole this feature can have, one word shorter to write, and it
--     produces no error at all.
--
-- p_verified = v_last, NEVER -1 (§6.2). Passing -1 means "no broker cache, probe
-- the whole window", which is always CORRECT but not cheap: the dedup probe would
-- scan the destination partition's ENTIRE RETAINED log_txns window, unrolling
-- hashes row by row — the very work the bloom front-dedup took from 60% to 3.7%
-- of verify in the 1.0.0 baseline. Worse, the probe happens AFTER the FOR UPDATE
-- on the log_partitions row, so the cost is not isolated in the Maint lane: it is
-- inside the destination partition's push serializer, shared with ordinary
-- producers. Scenario: dedupWindowSeconds = 3600 at 50k msg/hour, a sweep firing
-- 20 segments at 20 partitions, each push scanning an hour of log_txns while
-- holding the lock. The symptom is "the push got worse" and the fire's telemetry
-- does not explain it, because the time is billed to the push. So the fire hands
-- log_push_one_v1 the partition's own last_offset, read under the pre-lock, which
-- makes its probe span empty. The secondary net stays the fixed txn, and the real
-- guarantee stays DELETE + push in one transaction.
--
-- CONSEQUENCE, stated rather than discovered: with p_verified = v_last the window
-- probe never runs, so log_push_one_v1 cannot return `duplicate` to THIS caller.
-- The duplicate arm below is nevertheless implemented in full, because
-- `duplicate` is a legal return of the shared allocator and an arm that silently
-- assumed "queued" would delete timers it never delivered. See the report note
-- on §4.1/§12/§14.2, which still list `duplicate` in the fire's taxonomy.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.log_timers_fire_v1(
    p_tenants    UUID[],
    p_queues     TEXT[],
    p_partitions TEXT[],
    p_msg_counts INT[],
    p_hashes     BYTEA[],
    p_blobs      BYTEA[],
    p_keys       TEXT[],
    p_seg_of     INT[],
    p_tokens     UUID[],
    p_now        TIMESTAMPTZ
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_ns       INT;
    v_nk       INT;
    v_i        INT;
    v_cnt      BIGINT;
    v_live     BOOLEAN[];
    v_nlive    INT := 0;
    v_held     INT[];
    v_results  JSONB[];
    v_missing  INT;
    v_resolved INT := 0;
    v_rec      RECORD;
    v_res      JSONB;
    v_segords  INT[];
    v_del      INT[] := '{}'::int[];
    v_rel      INT[] := '{}'::int[];
    v_d        JSONB;
BEGIN
    -- --------------------------------------------------------------- (1)
    v_ns := COALESCE(array_length(p_tenants, 1), 0);
    IF v_ns = 0 THEN
        RETURN '[]'::jsonb;
    END IF;
    IF array_length(p_queues, 1)     IS DISTINCT FROM v_ns
       OR array_length(p_partitions, 1) IS DISTINCT FROM v_ns
       OR array_length(p_msg_counts, 1) IS DISTINCT FROM v_ns
       OR array_length(p_hashes, 1)     IS DISTINCT FROM v_ns
       OR array_length(p_blobs, 1)      IS DISTINCT FROM v_ns THEN
        RAISE EXCEPTION 'QFIRE misaligned segment arrays (% segments)', v_ns;
    END IF;

    v_nk := COALESCE(array_length(p_keys, 1), 0);
    IF array_length(p_seg_of, 1) IS DISTINCT FROM v_nk
       OR array_length(p_tokens, 1) IS DISTINCT FROM v_nk THEN
        RAISE EXCEPTION 'QFIRE misaligned timer arrays (% keys)', v_nk;
    END IF;
    IF v_nk = 0 THEN
        RAISE EXCEPTION 'QFIRE % segments carry no timer keys', v_ns;
    END IF;

    IF EXISTS (SELECT 1 FROM generate_subscripts(p_seg_of, 1) j
                WHERE p_seg_of[j] IS NULL OR p_seg_of[j] < 1 OR p_seg_of[j] > v_ns) THEN
        RAISE EXCEPTION 'QFIRE seg_of out of range (1..%)', v_ns;
    END IF;

    FOR v_i IN 1 .. v_ns LOOP
        IF p_msg_counts[v_i] IS NULL OR p_msg_counts[v_i] < 1
           OR octet_length(COALESCE(p_hashes[v_i], ''::bytea)) <> p_msg_counts[v_i] * 16 THEN
            RAISE EXCEPTION 'QFIRE segment %: msg_count=%, hashes=% bytes (want 16/msg)',
                v_i, p_msg_counts[v_i], octet_length(COALESCE(p_hashes[v_i], ''::bytea));
        END IF;
        -- The guard that keeps a wrong map from deleting the wrong timer.
        SELECT count(*) INTO v_cnt
        FROM generate_subscripts(p_seg_of, 1) j WHERE p_seg_of[j] = v_i;
        IF v_cnt <> p_msg_counts[v_i] THEN
            RAISE EXCEPTION 'QFIRE segment %: % timer keys map to it, blob carries % frames',
                v_i, v_cnt, p_msg_counts[v_i];
        END IF;
    END LOOP;

    -- --------------------------------------------------------------- (2)
    -- Lock every named row in the declared intra-space order and keep the
    -- ordinals of the frames whose row we hold with a MATCHING, LIVE lease.
    -- Rows held by somebody else are skipped, not waited on, and therefore
    -- simply are not held by us: their segment is stale.
    WITH want AS (
        SELECT k.ord::int AS ord,
               p_tenants[k.seg] AS tenant_id,
               p_queues[k.seg]  AS queue,
               k.tkey           AS timer_key,
               k.tok            AS tok
        FROM unnest(p_keys, p_seg_of, p_tokens) WITH ORDINALITY AS k(tkey, seg, tok, ord)
    ),
    locked AS MATERIALIZED (
        SELECT t.tenant_id, t.queue, t.timer_key, t.claim_token, t.claimed_until
        FROM queen.log_timers t
        WHERE EXISTS (SELECT 1 FROM want w
                       WHERE w.tenant_id = t.tenant_id
                         AND w.queue     = t.queue
                         AND w.timer_key = t.timer_key)
        ORDER BY t.tenant_id, t.queue, t.timer_key
        FOR UPDATE SKIP LOCKED
    )
    SELECT COALESCE(array_agg(w.ord ORDER BY w.ord), '{}'::int[])
    INTO v_held
    FROM want w
    JOIN locked l
      ON l.tenant_id = w.tenant_id AND l.queue = w.queue AND l.timer_key = w.timer_key
    WHERE l.claim_token = w.tok
      AND l.claimed_until > p_now;

    v_live := array_fill(false, ARRAY[v_ns]);
    FOR v_i IN 1 .. v_ns LOOP
        SELECT count(*) INTO v_cnt
        FROM generate_subscripts(p_seg_of, 1) j
        WHERE p_seg_of[j] = v_i AND j = ANY(v_held);
        v_live[v_i] := (v_cnt = p_msg_counts[v_i]);
        IF v_live[v_i] THEN
            v_nlive := v_nlive + 1;
        END IF;
    END LOOP;

    v_results := array_fill(NULL::jsonb, ARRAY[v_ns]);

    IF v_nlive > 0 THEN
        -- ----------------------------------------------------------- (8)
        -- Two live segments on one destination would hand the second a stale
        -- last_offset for p_verified. The broker groups by
        -- (tenant, queue, partition) and splits a batch across CALLS, so this
        -- is a packer bug and must be loud.
        SELECT count(*) INTO v_cnt FROM (
            SELECT 1
            FROM unnest(p_tenants, p_queues, p_partitions)
                 WITH ORDINALITY AS u(tenant, queue, part, ord)
            WHERE v_live[u.ord::int]
            GROUP BY u.tenant, u.queue, u.part
            HAVING count(*) > 1
        ) d;
        IF v_cnt > 0 THEN
            RAISE EXCEPTION 'QFIRE two live segments target the same (tenant, queue, partition)';
        END IF;

        -- ----------------------------------------------------------- (4)
        -- Steady-state provisioning skip (003_log_push pattern): one cheap
        -- existence count, and only when something is missing do the two
        -- ON CONFLICT statements run. No per-call ShareLock churn on
        -- queen.queues. Every provisioning INSERT is ORDERED on its unique key
        -- (2026-07-23): ON CONFLICT DO NOTHING takes speculative locks in ROW
        -- order, and two transactions creating overlapping sets in different
        -- orders convoy exactly like unordered row locks would.
        SELECT count(*) INTO v_missing
        FROM unnest(p_tenants, p_queues, p_partitions)
             WITH ORDINALITY AS u(tenant, queue, part, ord)
        LEFT JOIN queen.queues q ON q.name = u.queue AND q.tenant_id = u.tenant
        LEFT JOIN queen.log_partitions pt ON pt.queue_id = q.id AND pt.name = u.part
        WHERE v_live[u.ord::int] AND pt.id IS NULL;

        IF v_missing > 0 THEN
            INSERT INTO queen.queues (tenant_id, name, namespace, task)
            SELECT DISTINCT u.tenant, u.queue,
                   split_part(u.queue, '.', 1),
                   CASE WHEN position('.' in u.queue) > 0
                        THEN split_part(u.queue, '.', 2) ELSE '' END
            FROM unnest(p_tenants, p_queues) WITH ORDINALITY AS u(tenant, queue, ord)
            WHERE v_live[u.ord::int] AND COALESCE(u.queue, '') <> ''
            ORDER BY 1, 2
            ON CONFLICT (tenant_id, name) DO NOTHING;

            INSERT INTO queen.log_partitions (queue_id, name)
            SELECT DISTINCT q.id, u.part
            FROM unnest(p_tenants, p_queues, p_partitions)
                 WITH ORDINALITY AS u(tenant, queue, part, ord)
            JOIN queen.queues q ON q.name = u.queue AND q.tenant_id = u.tenant
            WHERE v_live[u.ord::int]
            ORDER BY 1, 2
            ON CONFLICT (queue_id, name) DO NOTHING;
        END IF;

        -- ----------------------------------------------------------- (5)
        -- Deadlock-safe pre-lock: ascending log_partitions.id, ONE statement
        -- (the LockRows node sits above the Sort, so rows lock in ORDER BY
        -- order). This is the FIRST partition-space acquisition of this
        -- transaction, and every timer-row lock was taken before it: T -> Q -> P,
        -- the declared prefix.
        PERFORM 1
        FROM (
            SELECT DISTINCT pt.id
            FROM unnest(p_tenants, p_queues, p_partitions)
                 WITH ORDINALITY AS u(tenant, queue, part, ord)
            JOIN queen.queues q ON q.name = u.queue AND q.tenant_id = u.tenant
            JOIN queen.log_partitions pt ON pt.queue_id = q.id AND pt.name = u.part
            WHERE v_live[u.ord::int]
        ) ids
        JOIN queen.log_partitions pl ON pl.id = ids.id
        ORDER BY pl.id
        FOR UPDATE OF pl;

        -- ----------------------------------------------------------- (6)
        FOR v_rec IN
            SELECT u.ord::int AS ord, u.tenant, u.queue, u.part,
                   pt.id AS pid, q.dedup_window_seconds AS win,
                   pt.last_offset AS last_off
            FROM unnest(p_tenants, p_queues, p_partitions)
                 WITH ORDINALITY AS u(tenant, queue, part, ord)
            JOIN queen.queues q ON q.name = u.queue AND q.tenant_id = u.tenant
            JOIN queen.log_partitions pt ON pt.queue_id = q.id AND pt.name = u.part
            WHERE v_live[u.ord::int]
            ORDER BY pt.id
        LOOP
            v_res := queen.log_push_one_v1(
                v_rec.queue,
                v_rec.part,
                p_msg_counts[v_rec.ord],
                p_hashes[v_rec.ord],
                v_rec.last_off,          -- p_verified = v_last, never -1 (see header)
                p_blobs[v_rec.ord],
                v_rec.pid,
                v_rec.win,
                v_rec.tenant);
            v_resolved := v_resolved + 1;

            SELECT COALESCE(array_agg(j ORDER BY j), '{}'::int[]) INTO v_segords
            FROM generate_subscripts(p_seg_of, 1) j WHERE p_seg_of[j] = v_rec.ord;

            IF v_res->>'status' = 'duplicate' THEN
                -- The fixed txn is already in the log: those timers are DONE.
                -- Delete them, push nothing, and let the rest of the group go
                -- back to being claimable so it can be repacked without them —
                -- the group makes progress on every pass. `i` in dups is the
                -- 0-based INCOMING frame index, so it addresses this segment's
                -- keys in array order.
                v_results[v_rec.ord] := jsonb_build_object(
                    'result', 'duplicate',
                    'queue', v_rec.queue, 'partition', v_rec.part,
                    'dups', v_res->'dups');
                FOR v_d IN SELECT value FROM jsonb_array_elements(v_res->'dups') LOOP
                    v_del := v_del || v_segords[(v_d->>'i')::int + 1];
                END LOOP;
            ELSE
                v_results[v_rec.ord] := jsonb_build_object(
                    'result', 'fired',
                    'queue', v_rec.queue, 'partition', v_rec.part,
                    'timers', p_msg_counts[v_rec.ord],
                    'baseOffset', (v_res->>'baseOffset')::bigint,
                    'createdAt', v_res->>'createdAt');
                v_del := v_del || v_segords;
            END IF;
        END LOOP;

        IF v_resolved <> v_nlive THEN
            RAISE EXCEPTION 'QFIRE resolved % of % live segments (empty/unknown queue or partition)',
                v_resolved, v_nlive;
        END IF;
    END IF;

    -- Segments that did not verify. Filled last so the taxonomy has no hole and
    -- to_jsonb never sees a NULL ordinal (§6.4).
    FOR v_i IN 1 .. v_ns LOOP
        IF v_results[v_i] IS NULL THEN
            v_results[v_i] := jsonb_build_object(
                'result', 'stale',
                'queue', p_queues[v_i], 'partition', p_partitions[v_i]);
        END IF;
    END LOOP;

    -- --------------------------------------------------------------- (7)
    -- Rows ALREADY HELD since step 2: a no-op on the wait graph (§2.2).
    IF COALESCE(array_length(v_del, 1), 0) > 0 THEN
        DELETE FROM queen.log_timers t
        USING (
            SELECT p_tenants[p_seg_of[d.j]] AS tenant_id,
                   p_queues[p_seg_of[d.j]]  AS queue,
                   p_keys[d.j]              AS timer_key
            FROM unnest(v_del) AS d(j)
        ) x
        WHERE t.tenant_id = x.tenant_id AND t.queue = x.queue AND t.timer_key = x.timer_key;
    END IF;

    -- --------------------------------------------------------------- (3)
    -- Everything we hold and did not deliver goes back: token NULL, lease pulled
    -- back to p_now, so visible_at falls back into the scan window immediately.
    -- Written once, used by BOTH the stale and the duplicate arms.
    SELECT COALESCE(array_agg(h.ord), '{}'::int[]) INTO v_rel
    FROM unnest(v_held) AS h(ord)
    WHERE NOT (h.ord = ANY(v_del));

    IF COALESCE(array_length(v_rel, 1), 0) > 0 THEN
        UPDATE queen.log_timers t
           SET claimed_until = p_now,
               claim_token   = NULL,
               updated_at    = p_now
        FROM (
            SELECT p_tenants[p_seg_of[d.j]] AS tenant_id,
                   p_queues[p_seg_of[d.j]]  AS queue,
                   p_keys[d.j]              AS timer_key
            FROM unnest(v_rel) AS d(j)
        ) x
        WHERE t.tenant_id = x.tenant_id AND t.queue = x.queue AND t.timer_key = x.timer_key;
    END IF;

    RETURN to_jsonb(v_results);
END;
$$;


-- ============================================================================
-- log_timers_fail_v1 — the fire failed; push claimed_until out by a backoff and
-- NULL the claim token, so the row is IN BACKOFF and IN NOBODY'S HANDS.
--
-- The two columns are separate precisely for this state: a cancel during a
-- backoff SUCCEEDS, or a poisoned timer would be uncancellable for minutes and
-- the user could not remove the broken thing.
--
-- p_count_attempt = false is the TRANSIENT path (40001, 40P01, classes 08/53/57/58,
-- and the absence of a SQLSTATE). The budget is never spent on infrastructure, or
-- five minutes of database trouble would send every timer in the system to the DLQ,
-- i.e. an infrastructure failure would become product loss.
--
-- Returns {"failed":N,"exhausted":[...]}. The exhausted rows are NOT deleted here:
-- the DLQ needs a JSONB snapshot of the payload and only the broker can decompress.
--
-- LOCK ORDER: T only, addressed by the full PK, visited in ascending
-- (tenant_id, queue, timer_key). The token match is the authorisation: a broker
-- whose lease expired and was reclaimed elsewhere updates nothing.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.log_timers_fail_v1(
    p_tenants       UUID[],
    p_queues        TEXT[],
    p_keys          TEXT[],
    p_tokens        UUID[],
    p_backoff_ms    INT,
    p_error         TEXT,
    p_count_attempt BOOLEAN,
    p_max_attempts  INT,
    p_now           TIMESTAMPTZ
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_n         INT;
    v_failed    BIGINT := 0;
    v_exhausted JSONB  := '[]'::jsonb;
BEGIN
    v_n := COALESCE(array_length(p_tenants, 1), 0);
    IF v_n = 0 THEN
        RETURN jsonb_build_object('failed', 0, 'exhausted', '[]'::jsonb);
    END IF;
    IF array_length(p_queues, 1) IS DISTINCT FROM v_n
       OR array_length(p_keys, 1)   IS DISTINCT FROM v_n
       OR array_length(p_tokens, 1) IS DISTINCT FROM v_n THEN
        RAISE EXCEPTION 'QTIMERFAIL misaligned arrays (% tenants)', v_n;
    END IF;

    WITH want AS (
        SELECT u.tenant, u.queue, u.tkey, u.tok
        FROM unnest(p_tenants, p_queues, p_keys, p_tokens) AS u(tenant, queue, tkey, tok)
    ),
    locked AS MATERIALIZED (
        SELECT t.tenant_id, t.queue, t.timer_key
        FROM queen.log_timers t
        JOIN want w ON w.tenant = t.tenant_id AND w.queue = t.queue AND w.tkey = t.timer_key
        WHERE t.claim_token = w.tok
        ORDER BY t.tenant_id, t.queue, t.timer_key
        FOR UPDATE OF t
    ),
    upd AS (
        UPDATE queen.log_timers t
           SET claimed_until = p_now + make_interval(
                   secs => GREATEST(COALESCE(p_backoff_ms, 1000), 0)::double precision / 1000.0),
               claim_token   = NULL,
               attempts      = t.attempts
                               + CASE WHEN COALESCE(p_count_attempt, true) THEN 1 ELSE 0 END,
               last_error    = p_error,
               updated_at    = p_now
        FROM locked l
        WHERE t.tenant_id = l.tenant_id AND t.queue = l.queue AND t.timer_key = l.timer_key
        RETURNING t.tenant_id, t.queue, t.timer_key, t.attempts
    )
    SELECT count(*),
           COALESCE(
               jsonb_agg(jsonb_build_object(
                   'tenant',   u.tenant_id::text,
                   'queue',    u.queue,
                   'timerKey', u.timer_key,
                   'attempts', u.attempts) ORDER BY u.queue, u.timer_key)
               FILTER (WHERE u.attempts >= COALESCE(p_max_attempts, 5)),
               '[]'::jsonb)
    INTO v_failed, v_exhausted
    FROM upd u;

    RETURN jsonb_build_object('failed', v_failed, 'exhausted', v_exhausted);
END;
$$;


-- ============================================================================
-- log_timers_dlq_v1 — archive an exhausted timer straight into queen.log_dlq.
--
-- It does NOT reuse log_dlq_head_v1, and the reason is not tidiness: that one
-- demands a valid lease on (partition, group) and advances a consumer's cursor.
-- A poisoned timer has no lease, no group and no cursor to advance. The
-- non-secondary benefit is that the fire path therefore NEVER touches
-- queen.log_consumers, so its sequence stays T -> Q -> P, without the last space
-- (§2.3 actor 15).
--
-- Three notes, all load-bearing:
--   * queen.log_dlq has NO tenant column (005_log_ack.sql:52-63): its only
--     scoping is partition_id, so the partition is resolved UNDER THE TIMER'S
--     TENANT, from the row itself. A DLQ row on the wrong partition is one
--     customer reading another customer's failed payload.
--   * PROVISIONING IS NOT OPTIONAL: get_dlq_messages_v1 INNER JOINs
--     log_partitions and queen.queues, so a DLQ row on a missing partition is
--     archived and unfindable, which is worse than not archiving it.
--   * the DELETE — and the INSERT with it — is guarded by
--     attempts >= p_min_attempts AND claim_token IS NULL, which closes the race
--     with a reschedule landing between fail_v1 and here: a reschedule zeroes
--     attempts, so the archive finds nothing and the row lives. It is a new timer
--     under an old name and must not be archived for the sins of the one it
--     replaced.
--
-- consumer_group = '__timer__' and "offset" = -1 (§4.5): a timer never had a
-- group and its offset is not a position. Replay of a DLQ row whose group
-- matches '\_\_%\_\_' is refused by the replay path, for that reason.
--
-- Declared consequence: a DLQ row PINS ITS PARTITION against partition cleanup.
-- That is intended — the payload has to stay reachable, and retention never
-- purges the DLQ.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.log_timers_dlq_v1(
    p_tenants      UUID[],
    p_queues       TEXT[],
    p_keys         TEXT[],
    p_payloads     JSONB[],
    p_errors       TEXT[],
    p_min_attempts INT,
    p_now          TIMESTAMPTZ
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_n        INT;
    v_min      INT := COALESCE(p_min_attempts, 5);
    v_ords     INT[];
    v_archived BIGINT := 0;
BEGIN
    v_n := COALESCE(array_length(p_tenants, 1), 0);
    IF v_n = 0 THEN
        RETURN jsonb_build_object('archived', 0, 'skipped', 0);
    END IF;
    IF array_length(p_queues, 1) IS DISTINCT FROM v_n
       OR array_length(p_keys, 1)     IS DISTINCT FROM v_n
       OR array_length(p_payloads, 1) IS DISTINCT FROM v_n
       OR array_length(p_errors, 1)   IS DISTINCT FROM v_n THEN
        RAISE EXCEPTION 'QTIMERDLQ misaligned arrays (% tenants)', v_n;
    END IF;

    -- T first, in the declared intra-space order, and only the rows that still
    -- satisfy the exhaustion guard.
    WITH want AS (
        SELECT u.ord::int AS ord, u.tenant, u.queue, u.tkey
        FROM unnest(p_tenants, p_queues, p_keys) WITH ORDINALITY AS u(tenant, queue, tkey, ord)
    ),
    locked AS MATERIALIZED (
        SELECT t.tenant_id, t.queue, t.timer_key
        FROM queen.log_timers t
        WHERE EXISTS (SELECT 1 FROM want w
                       WHERE w.tenant = t.tenant_id AND w.queue = t.queue AND w.tkey = t.timer_key)
          AND t.attempts >= v_min
          AND t.claim_token IS NULL
        ORDER BY t.tenant_id, t.queue, t.timer_key
        FOR UPDATE
    )
    SELECT COALESCE(array_agg(w.ord ORDER BY w.ord), '{}'::int[])
    INTO v_ords
    FROM want w
    JOIN locked l ON l.tenant_id = w.tenant AND l.queue = w.queue AND l.timer_key = w.tkey;

    IF COALESCE(array_length(v_ords, 1), 0) = 0 THEN
        RETURN jsonb_build_object('archived', 0, 'skipped', v_n);
    END IF;

    -- Q, then P. Ordered on the unique key, same discipline as every other
    -- provisioning site.
    INSERT INTO queen.queues (tenant_id, name, namespace, task)
    SELECT DISTINCT p_tenants[d.j], p_queues[d.j],
           split_part(p_queues[d.j], '.', 1),
           CASE WHEN position('.' in p_queues[d.j]) > 0
                THEN split_part(p_queues[d.j], '.', 2) ELSE '' END
    FROM unnest(v_ords) AS d(j)
    WHERE COALESCE(p_queues[d.j], '') <> ''
    ORDER BY 1, 2
    ON CONFLICT (tenant_id, name) DO NOTHING;

    INSERT INTO queen.log_partitions (queue_id, name)
    SELECT DISTINCT q.id, t."partition"
    FROM unnest(v_ords) AS d(j)
    JOIN queen.log_timers t
      ON t.tenant_id = p_tenants[d.j] AND t.queue = p_queues[d.j] AND t.timer_key = p_keys[d.j]
    JOIN queen.queues q ON q.tenant_id = t.tenant_id AND q.name = t.queue
    ORDER BY 1, 2
    ON CONFLICT (queue_id, name) DO NOTHING;

    -- The leaf.
    INSERT INTO queen.log_dlq (partition_id, consumer_group, "offset",
                               message_id, transaction_id, payload, error,
                               retry_count, failed_at)
    SELECT pt.id, '__timer__', -1,
           t.message_id, t.txn, p_payloads[d.j], p_errors[d.j],
           t.attempts, p_now
    FROM unnest(v_ords) AS d(j)
    JOIN queen.log_timers t
      ON t.tenant_id = p_tenants[d.j] AND t.queue = p_queues[d.j] AND t.timer_key = p_keys[d.j]
    JOIN queen.queues q ON q.tenant_id = t.tenant_id AND q.name = t.queue
    JOIN queen.log_partitions pt ON pt.queue_id = q.id AND pt.name = t."partition";
    GET DIAGNOSTICS v_archived = ROW_COUNT;

    -- Rows already held above. The guard is repeated verbatim: it is the whole
    -- reason the reschedule race is closed.
    DELETE FROM queen.log_timers t
    USING unnest(v_ords) AS d(j)
    WHERE t.tenant_id = p_tenants[d.j]
      AND t.queue     = p_queues[d.j]
      AND t.timer_key = p_keys[d.j]
      AND t.attempts >= v_min
      AND t.claim_token IS NULL;

    RETURN jsonb_build_object('archived', v_archived, 'skipped', v_n - v_archived);
END;
$$;


-- ============================================================================
-- log_timers_peek_v1 — ONE key, WITH the payload. The payload is returned here
-- and never on list: a list that carried payloads would be an unbounded read
-- whose cost the caller does not fix.
--
-- A key belonging to another tenant reads exactly like a key that does not
-- exist. Not revealing is right (§13.1); the tenant is part of the primary key,
-- not a filter applied to an id the caller presented.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.log_timers_peek_v1(
    p_tenant    UUID,
    p_queue     TEXT,
    p_timer_key TEXT
) RETURNS JSONB
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v JSONB;
BEGIN
    IF COALESCE(p_queue, '') = '' OR COALESCE(p_timer_key, '') = '' THEN
        RAISE EXCEPTION 'QTIMER peek needs a queue and a timerKey' USING ERRCODE = '22023';
    END IF;

    SELECT jsonb_build_object(
               'found', true,
               'queue', t.queue,
               'timerKey', t.timer_key,
               'partition', t."partition",
               'deliverAt', to_char(t.deliver_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
               'txn', t.txn,
               'messageId', t.message_id::text,
               'payload', encode(t.payload, 'base64'),
               'payloadZstd', t.payload_zstd,
               'encrypted', t.encrypted,
               'producerSub', t.producer_sub,
               'attempts', t.attempts,
               'lastError', t.last_error,
               -- The ONE definition of "in somebody's hands". A row in backoff
               -- has claimed_until in the future and claim_token NULL, and is
               -- still cancellable — so it reads claimed:false, deliberately.
               'claimed', (t.claim_token IS NOT NULL AND t.claimed_until > now()),
               'createdAt', to_char(t.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
               'updatedAt', to_char(t.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'))
    INTO v
    FROM queen.log_timers t
    WHERE t.tenant_id = p_tenant AND t.queue = p_queue AND t.timer_key = p_timer_key;

    RETURN COALESCE(v, jsonb_build_object('found', false,
                                          'queue', p_queue, 'timerKey', p_timer_key));
END;
$$;


-- ============================================================================
-- log_timers_list_v1 — keyset over the PK, THE QUEUE IS MANDATORY (§4.1).
--
-- There is no tenant-wide list: that would be a scan an end user of the customer
-- could trigger, on the first endpoint of the product whose call rate is decided
-- by somebody else's web traffic.
--
-- p_after is an EXCLUSIVE keyset cursor, not an offset, and it is stable because
-- timer_key carries COLLATE "C": the order is byte order, identical between
-- machines and unchanged by a libc/ICU upgrade. A cursor whose meaning depended
-- on the operating system's locale is a corruption waiting for a minor version.
--
-- limit defaults to 100 and is CLAMPED, never rejected, with `truncated` telling
-- the truth: a 400 on a too-large limit is an error the user cannot fix without
-- reading the server's configuration. The page is built with LIMIT n + 1 in the
-- subquery, so the extra row decides `truncated` without a second query.
--
-- No payload here: see log_timers_peek_v1.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.log_timers_list_v1(
    p_tenant UUID,
    p_queue  TEXT,
    p_after  TEXT,
    p_limit  INT
) RETURNS JSONB
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_limit INT := LEAST(GREATEST(COALESCE(p_limit, 100), 1), 1000);
    v_rows  JSONB;
    v_n     INT;
BEGIN
    IF COALESCE(p_queue, '') = '' THEN
        RAISE EXCEPTION 'QTIMER list needs a queue' USING ERRCODE = '22023';
    END IF;

    SELECT COALESCE(jsonb_agg(r.row ORDER BY r.timer_key), '[]'::jsonb), count(*)
    INTO v_rows, v_n
    FROM (
        SELECT t.timer_key,
               jsonb_build_object(
                   'queue', t.queue,
                   'timerKey', t.timer_key,
                   'partition', t."partition",
                   'deliverAt', to_char(t.deliver_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                   'txn', t.txn,
                   'messageId', t.message_id::text,
                   'payloadZstd', t.payload_zstd,
                   'encrypted', t.encrypted,
                   'producerSub', t.producer_sub,
                   'attempts', t.attempts,
                   'lastError', t.last_error,
                   'claimed', (t.claim_token IS NOT NULL AND t.claimed_until > now()),
                   'createdAt', to_char(t.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
                   'updatedAt', to_char(t.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')) AS row
        FROM queen.log_timers t
        WHERE t.tenant_id = p_tenant
          AND t.queue = p_queue
          AND (p_after IS NULL OR t.timer_key > p_after)
        ORDER BY t.timer_key
        LIMIT v_limit + 1
    ) r;

    IF v_n > v_limit THEN
        -- Drop the probe row; the cursor is the last row actually returned.
        v_rows := (SELECT jsonb_agg(e) FROM (
                       SELECT e FROM jsonb_array_elements(v_rows) e LIMIT v_limit) k);
        RETURN jsonb_build_object(
            'rows', v_rows, 'truncated', true,
            'nextAfter', v_rows -> (v_limit - 1) ->> 'timerKey');
    END IF;

    RETURN jsonb_build_object('rows', v_rows, 'truncated', false, 'nextAfter', NULL);
END;
$$;


-- ============================================================================
-- GRANTs. The type list must be COMPLETE and EXACT: a wrong list fails the
-- apply, and a failed apply kills the process (fail-fast boot).
-- ============================================================================
GRANT EXECUTE ON FUNCTION queen.log_timers_apply_v1(JSONB, UUID, TEXT, TIMESTAMPTZ) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.log_timers_due_v1(SMALLINT[], TIMESTAMPTZ, INT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.log_timers_claim_v1(SMALLINT[], TIMESTAMPTZ, INT, INT, INT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.log_timers_fire_v1(
    UUID[], TEXT[], TEXT[], INT[], BYTEA[], BYTEA[], TEXT[], INT[], UUID[], TIMESTAMPTZ) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.log_timers_fail_v1(
    UUID[], TEXT[], TEXT[], UUID[], INT, TEXT, BOOLEAN, INT, TIMESTAMPTZ) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.log_timers_dlq_v1(
    UUID[], TEXT[], TEXT[], JSONB[], TEXT[], INT, TIMESTAMPTZ) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.log_timers_peek_v1(UUID, TEXT, TEXT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.log_timers_list_v1(UUID, TEXT, TEXT, INT) TO PUBLIC;
