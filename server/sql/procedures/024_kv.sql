-- ============================================================================
-- 024_kv.sql — queen.kv: transactional key/value state on the log engine.
--
-- PLAN_KV_TIMERS.md §3.2 (schema), §5 (semantics), §6.1 (SQL surface).
--
-- WHAT THIS IS FOR. The product value is not the KV store. It is that the
-- idempotency marker, the effect and the cursor advance COMMIT TOGETHER: a KV
-- on the side cannot give that at any price. Everything below exists so the
-- shape rules are written in SQL exactly ONCE — seven clients, the HTTP routes,
-- the transaction wire and the embedded broker (which never passes through an
-- HTTP handler) inherit them without a per-language copy (§9.2).
--
-- ---------------------------------------------------------------------------
-- LOCK ORDER (§2). This file participates in the FIRST space of six. The total
-- order across the whole engine is:
--
--     queen.kv -> queen.log_timers -> ADV -> queen.queues
--              -> queen.log_partitions -> queen.log_consumers -> leaves
--
-- The rule, in one line, and the word "ACQUIRE" is load-bearing:
--
--     No actor may **ACQUIRE** a lock on queen.kv or queen.log_timers after
--     acquiring a lock on queen.queues, queen.log_partitions or
--     queen.log_consumers.
--
-- Re-touching a row THIS SAME TRANSACTION ALREADY HOLDS is not an acquisition;
-- it is a no-op on the wait-for graph (precedent: the multi pre-lock comment at
-- 003_log_push.sql:132-134). Without that distinction the rule is unfalsifiable
-- and the first person to apply it literally rewrites the timer fire, whose
-- final step deletes rows it locked in its first step (§2.4 C1/C2).
--
-- Whoever adds a **blocking** advisory lock to any of these paths must take it
-- BEFORE touching kv or timers, or not take it at all. The sweeper takes none
-- (§1.9), so 737_003 stays reserved for PLAN_S3_ARCHIVE.md.
--
-- INTRA-SPACE ORDER for queen.kv: ascending (namespace, key) with an EXPLICIT
-- COLLATE "C" (§2.3). Not the input order. An extraction from JSONB does NOT
-- inherit the column's collation, so every ORDER BY on op->>'key' spells the
-- collation out; and the broker NEVER pre-sorts — the ordering is a property of
-- this SP, in one place, or one differing libc between two pods reopens the
-- cycle (§2.4 C3 piece 3).
--
-- ---------------------------------------------------------------------------
-- WHY THIS FILE IS NUMBERED 024, AND WHY THAT IS A COMPILE-TIME CHECK (§1.2).
-- Nothing here names a partition_id. That is what keeps queen.kv out of
-- queen.log_partition_dead_v1 (006_log_maintenance.sql), whose O(partitions)
-- scan must not get more expensive. And the check is free and mechanical:
-- log_partition_dead_v1 is LANGUAGE sql, so it resolves its tables AT CREATION
-- TIME; it lives in 006 and this table lives in 024, so adding a veto leg on
-- queen.kv **kills the boot**. The file number IS the verification of the
-- invariant. Same reason and same shape as queen.hotlist_repairs
-- (001_log_schema.sql:186-195), which is keyed by names for the same motive.
--
-- Conversely 005_log_ack.sql applies BEFORE this file and calls
-- queen.kv_apply_v1. That works ONLY because the body of
-- log_transaction_wire_v1 is plpgsql, which resolves names at RUNTIME. Rewriting
-- it as LANGUAGE sql would kill the boot at creation time.
--
-- MAINTENANCE INVARIANT: no path of this feature creates a partition, per
-- request or per key. Queen's background maintenance scales with PARTITIONS,
-- and that is the one dimension this plan does not touch.
--
-- HOT PATH: the kv work of the wire lives in SEPARATE statements behind an IF
-- and never enters an existing statement (§6.3). Folding a count into the
-- provisioning query, or UNIONing jsonb_array_elements(p->'kv') with the pushes,
-- adds a Function Scan and a nested loop to the plan of a statement that runs
-- ALWAYS — including when the array is empty.
--
-- QUOTA: no write path here touches queen.kv_usage or queen.kv_quota. A per-
-- tenant counter row updated by every write would be ONE ROW held for the whole
-- bundle in the OUTERMOST of the six lock spaces — not a deadlock, worse: a
-- throughput ceiling of one-over-bundle-duration (§2.4 D7). The enforcer is the
-- in-process delta in AppState, zero SQL (§9.3).
--
-- SECURITY (§13.1). PK is (tenant_id, namespace, key): the tenant is not a
-- filter applied to a received id, it is PART OF THE KEY, and the caller never
-- presents an opaque identifier — only names it chose itself. p_tenant is an
-- ARGUMENT, never a field of an op. There is no RLS and the GRANTs are to
-- PUBLIC, so tenant isolation is EXACTLY a WHERE clause inside these SPs: no
-- HTTP route may ever build SQL against queen.kv outside them (§15 has a
-- mechanical grep for it). queen.kv is stored IN CLEAR in Postgres (§13.4).
--
-- MINIMUM POSTGRES 14: GENERATED ALWAYS AS ... STORED needs 12, starts_with()
-- needs 11, and neither has a precedent in the pre-024 files (§0). schema.rs
-- checks the version before the first DDL statement.
-- ============================================================================


-- ---------------------------------------------------------------------------
-- queen.kv
--
-- Identity = (tenant, namespace, key) and NOTHING ELSE. Deliberately no
-- partition_id, unlike queen_streams.state (002_streams_schema.sql:58-65): a KV
-- key is a BUSINESS identity and the caller does not know, and must not need to
-- know, which partition it falls on.
--
-- The price of dropping partition_id is exact and is written here so nobody has
-- to rediscover it: the streams model gets serialization for free because the
-- operator holds an exclusive lease on the partition. THIS table has no lease.
-- Two workers CAN hit the same key at the same time. That is why put/expect,
-- putIfAbsent and incr are mandatory primitives and not conveniences, and why a
-- read-modify-write performed by the CALLER over two round trips is unsound
-- unless the key derives from the partition key (§5.2).
--
-- Namespaces are registered NOWHERE: like queues (003_log_push.sql:96-126) a
-- namespace exists if and only if a row exists. Unknown namespace = empty
-- result, never an error. The charset IS validated (queen.kv_check_names_v1)
-- precisely because nothing registers it: without validation a typo does not
-- fail, it MINTS a phantom namespace that reads empty forever.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS queen.kv (
    -- Same default as queen.hotlist_repairs (001_log_schema.sql:187): a
    -- pre-tenancy caller lands in the default tenant, byte-identical behaviour.
    tenant_id  UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    -- COLLATE "C" on BOTH name columns, holding up two distinct things:
    --   1. LOCK ORDER. It is the foundation of the intra-space order of
    --      queen.kv (§2.3). Under the database default collation two pods with
    --      different lc_collate or different ICU minors — the real case in a
    --      rolling base-image upgrade — order two keys oppositely, two
    --      overlapping bundles take the rows in inverted order, and the 40P01
    --      reproduces on some installations only and for some keys only.
    --   2. Prefix sargability and cursor stability: the order is BYTE order, so
    --      it is identical across machines and survives a libc/ICU upgrade. A
    --      cursor whose meaning depends on the operating system's locale is a
    --      corruption waiting for a minor version.
    -- Consequence to document: prefix results come back in BYTE order, so
    -- non-ASCII keys are not in locale alphabetical order.
    -- WARNING: an extraction from JSONB does NOT inherit this collation. Every
    -- ORDER BY on op->>'key' must spell COLLATE "C" out.
    namespace  TEXT COLLATE "C" NOT NULL,
    key        TEXT COLLATE "C" NOT NULL,
    value      JSONB NOT NULL,        -- 'null'::jsonb is a LEGAL value
    -- Opaque, UNIQUE token. NOT monotonic and NOT a write count: with CACHE each
    -- backend draws a block, so backend A can emit 91005 after B emitted 92000,
    -- in real time, for the same key. Callers do EQUALITY ONLY, never arithmetic
    -- and never an ordering comparison. It comes from a sequence and never from
    -- version+1, so a key that expired, was pruned and was recreated cannot
    -- re-issue a version an old holder still carries (the ABA of a per-lineage
    -- counter). Gaps are expected.
    version    BIGINT NOT NULL,
    -- LOAD-BEARING, and MANDATORY on the wire (queen.kv_apply_v1). NULL =
    -- forever, and forever is an explicit, greppable, auditable opt-in — never
    -- a default. A table with no natural retention whose TTL is optional grows
    -- in silence, and that growth becomes a product liability paid by the
    -- operator (§1.6).
    expires_at TIMESTAMPTZ,
    -- Contention spreader, NOT an ownership partition: every broker scans every
    -- shard (§1.10). GENERATED, so no UPDATE can ever move it. The modulus is
    -- FIXED AT 64 FOREVER: the deployment model is always-virgin for the SCHEMA,
    -- not for the DATA, so changing it would silently re-shard rows already
    -- written. No env changes it and none must ever exist.
    -- NOTE: hashtextextended is not guaranteed stable across PostgreSQL majors.
    -- STORED freezes it at write time, which is fine; but no QUERY may ever
    -- recompute it for comparison, or on a different major it would derive a
    -- shard other than the stored one.
    shard      SMALLINT NOT NULL
               GENERATED ALWAYS AS ((hashtextextended(key, 0) & 63)::smallint) STORED,
    -- created_at is the ONE field of the datum that never comes back to the
    -- caller, hence the one where a mistake goes unnoticed: a put with expect:0
    -- that resurrects an expired lineage RESETS it, deliberately, because that
    -- is a new lineage (§5.7).
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- The conflict target used by every write is this COMPLETE key, so a
    -- conflict can never be cross-tenant (§3.4). A unique index on
    -- (namespace, key) "for efficiency" would break exactly that.
    PRIMARY KEY (tenant_id, namespace, key)
);

-- High CACHE for one reason, and it must be written down: a nextval per write on
-- the hot path would produce a WAL record per write. It is NOT a defence against
-- the sequence side channel of §13.3 — with a pool, connections are handed to
-- arbitrary tenants, so a backend's block is consumed by interleaved tenants and
-- the gap stays informative. The channel becomes COARSER, not closed.
CREATE SEQUENCE IF NOT EXISTS queen.kv_version_seq AS BIGINT START 1 CACHE 1000;

-- Churn control, copied verbatim from queen.log_partitions
-- (001_log_schema.sql:47-68): same shape of problem. Counters are UPDATE-heavy
-- and TTL'd rows are DELETE-heavy, so between passes the dead tuples outnumber
-- the live ones and scale_factor 0 keeps autovacuum re-firing every naptime.
--
-- vacuum_truncate = off for the reason written out at 001_log_schema.sql:54-63
-- (2026-07-24, caught live with pg_stat_progress_vacuum in phase "truncating
-- heap"): heap truncation takes an ACCESS EXCLUSIVE lock, and THIS table is
-- exactly the shape that provokes it, because it empties and refills in steady
-- state — which is the whole point of expires_at. That lock was the root cause
-- of the entire "wobble" class. Never re-enable it.
--
-- And on this table it is not only a defence against wobble: an MVCC read still
-- takes ACCESS SHARE, which conflicts with ACCESS EXCLUSIVE, so vacuum_truncate
-- = off is PART OF THE LOCK ORDER (§2.4 C4 corollary).
ALTER TABLE queen.kv SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 500,
    autovacuum_vacuum_cost_delay = 0,
    vacuum_truncate = off);

-- fillfactor: BY ANALOGY with 001_log_schema.sql:39-46, NOT measured. Steady
-- state is in-place UPDATEs of non-indexed columns (value, version, updated_at)
-- on rows that must stay HOT (the counters). Replace with a MEASURED number
-- after the first soak, in the style of the dated blocks of 001.
ALTER TABLE queen.kv SET (fillfactor = 70);

-- NO "ALTER COLUMN value SET STORAGE EXTENDED" here (§2.4 D8). EXTENDED is
-- already the default for jsonb, so the statement would do nothing except take
-- an ACCESS EXCLUSIVE on this table AT EVERY BOOT (schema.rs re-applies
-- everything, always). EXTENDED is the right value, and this comment is where to
-- say so: a log blob is EXTERNAL because zstd already ran, a KV value is raw
-- JSON that TOAST compression genuinely shortens, and at the 64 KiB ceiling a
-- compressed inline datum is one buffer where an out-of-line chunk is three.
-- If a different value were ever really needed it is a DO $$ guard on
-- pg_attribute.attstorage, in the style of 020_log_partition_counters.sql.

-- The ONE secondary index, with exactly ONE reader: the sweeper. PARTIAL, so a
-- key written with "forever" costs nothing here. shard leads because the sweeper
-- filters by shard and then range-scans expires_at.
-- Deliberately NOT created: an index on updated_at (queen_streams.state carries
-- one and no reader queries it) would make EVERY counter update non-HOT; an
-- index on value has no query to serve, because the declared boundary is keys
-- and prefixes.
CREATE INDEX IF NOT EXISTS idx_kv_shard_expires
    ON queen.kv (shard, expires_at) WHERE expires_at IS NOT NULL;


-- ---------------------------------------------------------------------------
-- Per-tenant QUOTA CONFIGURATION. ONE row per tenant for ALL the state of both
-- features: two quota tables would diverge. Deliberately separate from the
-- measurement table below — configuration is written by an operator, measurement
-- is overwritten by the sweeper, and mixing them leaves "who wrote this?"
-- without an answer.
--
-- NULL = unlimited ONLY when tenancy is off. With QUEEN_TENANCY_HEADER active
-- the ABSENCE of the row is a DENIAL, not a permission (§9.4): the tenant is
-- validated against nothing (server/src/tenant.rs:16-19), so a fail-open default
-- means unlimited key space for an invented id.
--
-- ADD COLUMN IF NOT EXISTS, not just CREATE: precedent 019_worker_metrics.sql:95-119.
-- CREATE TABLE IF NOT EXISTS is a SILENT no-op ON THE SHAPE, so without these
-- ALTERs a cell that already booted an earlier version keeps the old table and
-- discovers the missing column as a 42703 in production, on the schedule path,
-- classified as configuration and therefore never retried.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS queen.kv_quota (
    tenant_id  UUID PRIMARY KEY,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE queen.kv_quota ADD COLUMN IF NOT EXISTS enabled             BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE queen.kv_quota ADD COLUMN IF NOT EXISTS max_rows            BIGINT;
ALTER TABLE queen.kv_quota ADD COLUMN IF NOT EXISTS max_bytes           BIGINT;
ALTER TABLE queen.kv_quota ADD COLUMN IF NOT EXISTS max_timers          BIGINT;
ALTER TABLE queen.kv_quota ADD COLUMN IF NOT EXISTS max_timer_horizon_s BIGINT;
ALTER TABLE queen.kv_quota ADD COLUMN IF NOT EXISTS max_reads_per_sec   INTEGER;
ALTER TABLE queen.kv_quota ADD COLUMN IF NOT EXISTS max_writes_per_sec  INTEGER;

-- MEASUREMENT, written by the sweeper on its slow sub-cadence and cached in
-- AppState by every broker. PK on (tenant_id) and not on (tenant_id, shard): the
-- decided model is "any broker may measure any shard" (§1.10), so a per-shard PK
-- would produce write-write contention between N brokers on the same rows and N
-- times the scan cost. Last-writer-wins on computed_at.
-- Enforcement is SOFT AND LATE by construction: the overshoot is bounded in
-- §9.3, and the published formula is the true one, not the optimistic one.
CREATE TABLE IF NOT EXISTS queen.kv_usage (
    tenant_id   UUID PRIMARY KEY,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE queen.kv_usage ADD COLUMN IF NOT EXISTS kv_rows      BIGINT NOT NULL DEFAULT 0;
ALTER TABLE queen.kv_usage ADD COLUMN IF NOT EXISTS kv_bytes     BIGINT NOT NULL DEFAULT 0;
ALTER TABLE queen.kv_usage ADD COLUMN IF NOT EXISTS timer_rows   BIGINT NOT NULL DEFAULT 0;
ALTER TABLE queen.kv_usage ADD COLUMN IF NOT EXISTS timer_bytes  BIGINT NOT NULL DEFAULT 0;
ALTER TABLE queen.kv_usage ADD COLUMN IF NOT EXISTS timer_oldest TIMESTAMPTZ;

-- cost_delay 0 ONLY on queen.kv and queen.log_timers, never on the configuration
-- and measurement tables: autovacuum_max_workers is a GLOBAL budget (default 3),
-- and seven tables with scale_factor 0, threshold 500 and unthrottled I/O would
-- contend for those workers with log_partitions, log_consumers and log_segments,
-- which the engine depends on. These two are tiny and have no need to be
-- aggressive.
ALTER TABLE queen.kv_quota SET (autovacuum_vacuum_scale_factor = 0,
                                autovacuum_vacuum_threshold = 500,
                                vacuum_truncate = off);
ALTER TABLE queen.kv_usage SET (autovacuum_vacuum_scale_factor = 0,
                                autovacuum_vacuum_threshold = 500,
                                vacuum_truncate = off);

-- Precedent and caveat in the same place: 010_log_admin.sql grants
-- queen.hotlist_repairs the same way, on the grounds that "a non-owner caller
-- needs the table rights too". Every SP here is SECURITY INVOKER (there is not a
-- single SECURITY DEFINER in the whole schema), so the GRANT is needed.
-- CONSEQUENCE, written here so it is not discovered in an audit: per-tenant
-- isolation on this table is EXACTLY a WHERE clause inside the SPs. There is no
-- RLS. Any direct SQL reader bypasses it, and NO HTTP route may ever build a
-- query against queen.kv outside these SPs. §15 verifies that mechanically.
--
-- On the sequence: USAGE and NOT SELECT. With SELECT anyone could read
-- last_value and turn the side channel of §13.3 into a global write counter.
GRANT SELECT, INSERT, UPDATE, DELETE ON queen.kv TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON queen.kv_quota TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON queen.kv_usage TO PUBLIC;
GRANT USAGE ON SEQUENCE queen.kv_version_seq TO PUBLIC;


-- ============================================================================
-- PURE HELPERS (§6.1)
--
-- No table is touched, so their position in the file is free. They are written
-- ONCE because the real risk is the seventh call site rewriting them
-- differently. All IMMUTABLE PARALLEL SAFE.
--
-- NONE of them is STRICT, and that is deliberate in three of them: a STRICT
-- declaration turns the documented TOTAL function into NULL on its single most
-- important input (an absent row, a forever key), and a NULL inside a WHERE is a
-- row that has silently vanished.
--
-- `now` is a PARAMETER, never a call: one call to kv_apply_v1 uses ONE instant
-- for every op of the batch (§5.7). A getMany can never see one key alive and
-- the next dead by a few microseconds.
--
-- THE FIVE HELPERS DECLARE THEIR PARAMETERS POSITIONALLY (no names), against the
-- p_-prefix house convention, and it is deliberate. Their identity is asserted
-- with pg_get_function_identity_arguments (tests/kv_sql_helpers.rs, §15 row 1),
-- which RENDERS ARGUMENT NAMES when they exist: a named signature reads back as
-- "p_expires timestamp with time zone, ..." instead of the bare type list §6.1
-- writes. These are pure, two- and three-argument predicates whose whole identity
-- IS the type list, so nothing is lost; plpgsql bodies recover the readability
-- with ALIAS FOR, which is a language-level binding and leaves the catalogue
-- signature untouched. kv_apply_v1 keeps its named parameters — its identity is
-- not asserted that way, and there the names carry real information.
-- ============================================================================

-- expires IS NULL OR expires > now.
--
-- The boundary is half the contract: the sweeper deletes with
-- expires_at <= cutoff and readers accept with expires_at > now, so the two
-- coincide and a row exactly at now() is dead for BOTH. One boundary for the
-- whole feature, or a key is alive for one side and pruned by the other.
-- $1 = expires_at, $2 = now
CREATE OR REPLACE FUNCTION queen.kv_live_v1(TIMESTAMPTZ, TIMESTAMPTZ)
RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT $1 IS NULL OR $1 > $2
$$;

-- Effective version under the expiry rule. version(absent) = 0 is a TOTAL
-- function, and it is THAT definition which lets expect:0 mean "must not exist"
-- with no magic flag. An expired row reads as version 0, i.e. as ABSENT, so a
-- CAS against a dead lineage cannot succeed.
-- $1 = version, $2 = expires_at, $3 = now
CREATE OR REPLACE FUNCTION queen.kv_ver_v1(BIGINT, TIMESTAMPTZ, TIMESTAMPTZ)
RETURNS BIGINT
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN $1 IS NOT NULL AND queen.kv_live_v1($2, $3) THEN $1
        ELSE 0::bigint
    END
$$;

-- Effective numeric value, zero when the row is expired or absent or not a JSON
-- number.
--
-- This exists for §5.4 repair 1 and it must be TOTAL. The naive form writes
-- `AND (p_max IS NULL OR v_next <= p_max)` with v_next a plpgsql variable; v_next
-- depends on the conflicting row, so it is knowable only INSIDE the statement and
-- stays NULL as a variable. `NULL <= p_max` is NULL, the WHERE is not true, zero
-- rows are updated: a rate limiter admits the first request (INSERT branch) and
-- REFUSES EVERY SUBSEQUENT ONE FOREVER, with a reason that says "quota exceeded"
-- while the counter reads 1 — and since `applied` IS the admission decision, the
-- customer is 100% blocked with the wrong message.
--
-- Being total also matters on the other side: the obvious `(value->>0)::numeric`
-- raises 22P02 on a string/boolean/object datum, INSIDE THE WHERE of the rate
-- limiter's UPDATE. A rate limiter that throws is the same failure class §5.4
-- repair 3 describes.
-- $1 = value, $2 = expires_at, $3 = now
CREATE OR REPLACE FUNCTION queen.kv_num_v1(JSONB, TIMESTAMPTZ, TIMESTAMPTZ)
RETURNS NUMERIC
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN queen.kv_live_v1($2, $3) AND jsonb_typeof($1) = 'number'
        THEN ($1 #>> '{}')::numeric
        ELSE 0::numeric
    END
$$;

-- Upper bound of the BYTE range for a prefix, or NULL when no bound is
-- computable. NULL is a value, not an error: the range clause is ONLY the index
-- driver, starts_with() carries the semantics (§5.5), so dropping the bound is
-- always safe and never wrong.
--
-- Two traps, both verified on the rig and neither decorative:
--   * chr() REFUSES a surrogate in UTF8 (chr(55296) => "requested character not
--     valid for encoding"), so the D800-DFFF block must be jumped to U+E000.
--     Without the jump a prefix ending at U+D7FF does not return a wrong answer,
--     it RAISES, on a perfectly legal key.
--   * chr(1114112) is "too large", so U+10FFFF cannot be advanced. The helper
--     backtracks and advances the previous code point instead — a LOOP, not one
--     step, because a prefix can end in several of them.
--
-- plpgsql and not sql: the backtracking is a loop. It is still IMMUTABLE, so
-- with a constant argument the planner folds it once instead of per row.
CREATE OR REPLACE FUNCTION queen.kv_prefix_end_v1(TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    -- ALIAS FOR is a plpgsql binding: the catalogue signature stays a bare type
    -- list (see the block above), the body stays readable.
    p_prefix ALIAS FOR $1;
    v_s  TEXT := p_prefix;
    v_cp INT;
BEGIN
    IF v_s IS NULL THEN
        RETURN NULL;
    END IF;
    -- No string is an upper bound for every string, so an exhausted prefix
    -- yields NULL and the caller leans on starts_with alone.
    WHILE length(v_s) > 0 LOOP
        -- In UTF8, ascii() returns the Unicode code point of the character.
        v_cp := ascii(right(v_s, 1));
        IF v_cp = 55295 THEN            -- U+D7FF, just below the surrogate block
            v_cp := 57344;              -- U+E000, just above it
        ELSIF v_cp >= 1114111 THEN      -- U+10FFFF: cannot advance, backtrack
            v_s := left(v_s, -1);
            CONTINUE;
        ELSE
            v_cp := v_cp + 1;
        END IF;
        RETURN left(v_s, -1) || chr(v_cp);
    END LOOP;
    RETURN NULL;
END;
$$;

-- Namespace charset, non-empty key, key byte cap, no NUL. RAISE, not CHECK: the
-- house style has exactly one CHECK in the whole schema, and an always-virgin
-- schema has no migration with which to change a table constraint (§3.4).
--
-- WHY THE 512 IS A CONSTANT HERE AND NOT AN ENV. §9.2 names
-- QUEEN_KV_MAX_KEY_BYTES with default 512, but this function is IMMUTABLE and
-- takes two arguments, so it can read neither an env nor a GUC. The env lives at
-- the broker/HTTP edge; the floor that protects the index lives here. 512 is
-- chosen so that uuid(16) + namespace(64) + key(512) sits at about a fifth of
-- the ~2704-byte btree tuple ceiling: without the cap the failure is a 54000 at
-- the first INSERT, i.e. at runtime instead of at validation, and only for the
-- unlucky key.
--
-- The cap is in BYTES, not characters: octet_length, never length. 200 euro
-- signs are 200 characters and 600 bytes.
--
-- NUL: §6.1 lists it, and it cannot be checked the way the others are —
-- PostgreSQL text cannot REPRESENT a NUL, so the value never reaches this
-- function and the refusal comes from the protocol layer. Either way it can
-- never land in queen.kv.
CREATE OR REPLACE FUNCTION queen.kv_check_names_v1(TEXT, TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
PARALLEL SAFE
AS $$
DECLARE
    p_ns  ALIAS FOR $1;
    p_key ALIAS FOR $2;
BEGIN
    -- The charset exists because a namespace is registered nowhere: a typo does
    -- not fail, it mints a phantom namespace that reads empty forever. Case is
    -- the first of the common typo classes, hence lowercase only.
    IF p_ns IS NULL OR p_ns !~ '^[a-z0-9][a-z0-9._-]{0,63}$' THEN
        RAISE EXCEPTION 'kv_bad_namespace'
            USING ERRCODE = '22023',
                  DETAIL  = format('namespace %L does not match ^[a-z0-9][a-z0-9._-]{0,63}$',
                                   p_ns);
    END IF;

    IF p_key IS NULL OR p_key = '' THEN
        RAISE EXCEPTION 'kv_bad_key'
            USING ERRCODE = '22023',
                  DETAIL  = 'the key must be non-empty';
    END IF;

    IF octet_length(p_key) > 512 THEN
        RAISE EXCEPTION 'kv_key_too_large'
            USING ERRCODE = '22001',
                  DETAIL  = format('key is %s bytes, the ceiling is 512',
                                   octet_length(p_key));
    END IF;

    RETURN TRUE;
END;
$$;


-- ============================================================================
-- queen.kv_apply_v1(p_ops, p_tenant, p_now, p_in_wire) -> JSONB
--
-- Seven names, five code paths: get, getMany, getPrefix, put, putIfAbsent
-- (alias), delete, incr.
--
-- p_in_wire is a FLAG, NOT A SECOND SP. The differences between the two surfaces
-- are two (getPrefix forbidden, op budget 64 instead of 256), and two SPs would
-- be two implementations that drift. Same logic that makes putIfAbsent an alias
-- and not a second code path.
--
-- RETURN SHAPE: a bare JSONB ARRAY, index-aligned to p_ops (§6.4). Built as a
-- JSONB[] pre-filled by array_fill and written BY ORDINAL, with a count guard
-- before to_jsonb: without that guard an unfilled ordinal becomes a silent JSON
-- null in position, which is precisely the class of misalignment the guards at
-- 003_log_push.sql:372-375 fail loudly on. The map from this index space to the
-- FLAT HTTP one is the handler's job (§8.2), and that is where the guard is
-- extended.
--
-- THE SIX POINTS OF THE CONTRACT (§6.1):
--
--  1. VALIDATE-THEN-APPLY, therefore NO SAVEPOINT. Every recoverable rejection
--     (shape, size, duplicate key in the batch, missing expiry, getPrefix in the
--     wire, tenant field in an op) is decided in a pass BEFORE the first write.
--     Do not copy the per-element BEGIN/EXCEPTION of the streams cycle: there
--     the elements are independent units, here the transaction is all-or-nothing
--     by definition and a per-element rollback buys nothing. And the cost would
--     be real: at tens of ops per bundle that is tens of subxids, and past 64
--     live subxids per top-level transaction the subxid cache overflows and
--     EVERY SNAPSHOT ON THE MACHINE becomes suboverflowed.
--
--  2. APPLY ORDER ascending (namespace, key) COLLATE "C", NOT input order, with
--     results reported by input ordinal (§2.3, §6.4).
--
--  3. ONE KEY AT MOST ONCE PER CALL among the WRITE ops, checked in validation.
--     LOAD-BEARING: it is what makes the intra-space order TOTAL, not a
--     convenience. Relaxing it without redoing the proof reopens the cycle.
--     (Read ops are exempt: they take no row lock, so a get and a getMany may
--     name the same key.)
--
--  4. BUDGETS PER CALL, not per operation. An op count alone bounds nothing: 63
--     getMany of 256 keys read 16 128 rows, and with values near the ceiling that
--     is up to ~1 GiB of detoast BEFORE the first partition lock is taken and
--     WHILE holding row locks on queen.kv. So three budgets apply together: op
--     count, the SUM of the keys of all ops, and an aggregate READ BYTE budget —
--     the last one applied ALSO when p_in_wire, not only on the HTTP routes.
--
--  5. A LOST PRECONDITION IS A VERDICT, NOT AN EXCEPTION. applied:false rolls
--     back nothing. Escalation is opt-in PER ELEMENT with "required": true and
--     becomes a RAISE with ERRCODE check_violation (23514, class 23 = permanent
--     for the classifier, which is correct: blindly retrying a lost race loses it
--     again forever). The DETAIL carries the JSON — index, op, ns, key, reason,
--     version AND the winner's value, capped at 4 KiB — and the broker reads
--     db_error.detail() and NEVER parses the message. The MESSAGE stays OPAQUE:
--     namespace and key names end up in shared logs and error aggregators, so the
--     names live only in the DETAIL (§13.5). The winner's value in the DETAIL is
--     not a luxury: without it the loser needs an extra kv.get on the single most
--     frequent path of the product, and the whole point of putIfAbsent is that
--     the loser already knows what the winner did.
--
--  6. p_tenant IS AN ARGUMENT, NEVER A FIELD OF THE OPS. A "tenant", "tenantId"
--     or "_tenant" key inside an op is a RAISE 22023.
--
-- EXPIRY IS MANDATORY (§5.1): every put, putIfAbsent and incr carries EXACTLY
-- ONE of ttlSeconds (integer > 0) and forever:true. Zero or two declarations are
-- kv_expiry_not_specified, SQLSTATE 22023. There is no expiresAt input field: the
-- convenient `until: <date>` form lives in the SDKs and is converted to a delta at
-- send time. A put without ttlSeconds does NOT inherit the previous expiry — it
-- is not expressible, because a put that silently inherited the TTL is the
-- fastest way to make a marker immortal.
--
-- EXPIRY IS ALSO A READ RULE (§5.7): an expired key is NEVER returned and NEVER
-- counts as existing, even before the sweeper prunes it. The predicate is never
-- copied by hand — it is queen.kv_live_v1 (directly, or through kv_ver_v1 /
-- kv_num_v1) at every one of its sites.
--
-- WHY expect:0 IS NOT A RACE: in ON CONFLICT DO UPDATE, PostgreSQL takes the ROW
-- LOCK BEFORE evaluating the WHERE. Two concurrent putIfAbsent serialize, the
-- second re-evaluates against the new row and does not apply. Exactly one wins.
-- The cost, which belongs next to the benefit: even a FAILED conditional holds
-- that row lock until commit, and that is what feeds accepted risk §18.2.
--
-- WHY THE LOSER'S DATA IS DISCRIMINATED IN THE SAME STATEMENT (§5.3): for
-- expect:N>0 and for delete-with-expect the rows that fail the predicate are NOT
-- locked, so a separate discriminating SELECT would run on a different snapshot
-- and could hand back a version that is already stale, producing a CAS loop that
-- never converges on a contended key (there is no server-side backoff). Hence the
-- CTE + LEFT JOIN. The one documented exception is the expect:0 loser, where the
-- ON CONFLICT arm DID take the row lock: there, and only there, a fresh statement
-- snapshot is guaranteed to see the winner, and it is the only way to see a row
-- the winner committed after our snapshot was taken.
-- ============================================================================
CREATE OR REPLACE FUNCTION queen.kv_apply_v1(
    p_ops     JSONB,
    p_tenant  UUID,
    p_now     TIMESTAMPTZ,
    p_in_wire BOOLEAN
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    -- Shape ceilings live here, in SQL, so all seven clients and the embedded
    -- broker inherit them (§9.2). The matching envs (QUEEN_KV_MAX_VALUE_BYTES,
    -- QUEEN_KV_PREFIX_LIMIT, QUEEN_KV_MAX_READ_BYTES, QUEEN_KV_MAX_OPS_PER_CALL,
    -- QUEEN_KV_MAX_KEYS_PER_CALL) are the broker-edge body guard; these are the
    -- floor nothing can get under.
    C_MAX_VALUE_BYTES CONSTANT INT    := 65536;      -- QUEEN_KV_MAX_VALUE_BYTES
    C_MAX_READ_BYTES  CONSTANT BIGINT := 4194304;    -- QUEEN_KV_MAX_READ_BYTES, 4 MiB
    C_PREFIX_DEFAULT  CONSTANT INT    := 100;
    C_PREFIX_CAP      CONSTANT INT    := 1000;       -- QUEEN_KV_PREFIX_LIMIT
    C_MAX_OPS_WIRE    CONSTANT INT    := 64;
    C_MAX_OPS_HTTP    CONSTANT INT    := 256;
    C_MAX_KEYS_WIRE   CONSTANT INT    := 256;
    C_MAX_KEYS_HTTP   CONSTANT INT    := 4096;
    C_DETAIL_CAP      CONSTANT INT    := 4096;

    -- ONE instant for the whole call (§5.7). now() is the transaction timestamp,
    -- already in memory, no syscall — not clock_timestamp(), which is the push's
    -- and must not be confused with it.
    v_now       TIMESTAMPTZ := COALESCE(p_now, now());
    v_wire      BOOLEAN     := COALESCE(p_in_wire, FALSE);
    v_n         INT;
    v_out       JSONB[];
    v_nulls     INT;
    -- Resolved once, into variables, and NOT written inline in the IF conditions
    -- below: plpgsql ends an IF condition at the FIRST unparenthesised THEN, so a
    -- bare `IF x > CASE WHEN ... THEN ... END THEN` does not compile.
    v_max_ops   INT;
    v_max_keys  INT;

    -- Aggregate read budget for the whole call (§6.1 point 4).
    v_read_left BIGINT := C_MAX_READ_BYTES;

    v_rec       RECORD;
    v_op        JSONB;
    v_ord       INT;
    v_kind      TEXT;
    v_ns        TEXT;
    v_key       TEXT;
    v_keys      TEXT[];
    v_prefix    TEXT;
    v_end       TEXT;
    v_after     TEXT;
    v_limit     INT;
    v_keys_only BOOLEAN;
    v_value     JSONB;
    v_forever   BOOLEAN;
    v_ttl       BIGINT;
    v_expiry    TIMESTAMPTZ;
    v_has_exp   BOOLEAN;
    v_expect    BIGINT;
    v_required  BOOLEAN;
    v_delta     NUMERIC;
    v_min       NUMERIC;
    v_max       NUMERIC;
    v_decls     INT;
    v_pure_ok   BOOLEAN;

    v_applied   BOOLEAN;
    v_reason    TEXT;
    v_newval    JSONB;
    v_newver    BIGINT;
    v_curval    JSONB;
    v_curver    BIGINT;
    v_curexp    TIMESTAMPTZ;
    v_curupd    TIMESTAMPTZ;
    v_exists    BOOLEAN;
    v_eff_ver   BIGINT;
    v_eff_num   NUMERIC;
    v_res       JSONB;

    v_rows      JSONB;
    v_missing   JSONB;
    v_kept      BIGINT;
    v_total     BIGINT;
    v_bytes     BIGINT;
    v_last      TEXT;
    v_trunc     BOOLEAN;
    v_found     BOOLEAN;
BEGIN
    IF p_ops IS NULL OR jsonb_typeof(p_ops) <> 'array' THEN
        RAISE EXCEPTION 'kv_bad_request'
            USING ERRCODE = '22023', DETAIL = 'kv operations must be a JSON array';
    END IF;

    v_n := jsonb_array_length(p_ops);
    IF v_n = 0 THEN
        RETURN '[]'::jsonb;
    END IF;

    v_max_ops  := CASE WHEN v_wire THEN C_MAX_OPS_WIRE  ELSE C_MAX_OPS_HTTP  END;
    v_max_keys := CASE WHEN v_wire THEN C_MAX_KEYS_WIRE ELSE C_MAX_KEYS_HTTP END;

    IF v_n > v_max_ops THEN
        RAISE EXCEPTION 'kv_too_many_ops'
            USING ERRCODE = '22023',
                  DETAIL  = format('%s ops in one call, the ceiling is %s', v_n, v_max_ops);
    END IF;

    -- =====================================================================
    -- PASS 1 — VALIDATION. No write happens in this pass, and no savepoint is
    -- taken anywhere in this function (§6.1 point 1).
    -- =====================================================================
    FOR v_rec IN
        SELECT o.ordinality::int AS ord, o.value AS op
          FROM jsonb_array_elements(p_ops) WITH ORDINALITY o
         ORDER BY o.ordinality
    LOOP
        v_op   := v_rec.op;
        v_ord  := v_rec.ord;

        IF jsonb_typeof(v_op) <> 'object' THEN
            RAISE EXCEPTION 'kv_bad_request'
                USING ERRCODE = '22023',
                      DETAIL  = format('op at index %s is not an object', v_ord - 1);
        END IF;

        v_kind := v_op->>'op';
        IF v_kind IS NULL OR v_kind NOT IN
           ('get','getMany','getPrefix','put','putIfAbsent','delete','incr') THEN
            RAISE EXCEPTION 'kv_unknown_op'
                USING ERRCODE = '22023',
                      DETAIL  = format('op at index %s: unknown operation %L', v_ord - 1, v_kind);
        END IF;

        -- §6.1 point 6: the tenant is an argument. A tenant field inside an op is
        -- never ignored in silence, because ignoring it would make it a vector
        -- the day someone reads it "for convenience".
        IF v_op ? 'tenant' OR v_op ? 'tenantId' OR v_op ? '_tenant' THEN
            RAISE EXCEPTION 'kv_tenant_not_an_input'
                USING ERRCODE = '22023',
                      DETAIL  = format('op at index %s carries a tenant field; the tenant is an argument of kv_apply_v1',
                                       v_ord - 1);
        END IF;

        v_ns := v_op->>'ns';

        IF v_kind = 'getPrefix' THEN
            -- §5.5: forbidden in the transaction wire. Read work whose cost the
            -- caller does not bound a priori, inside the transaction that holds
            -- the outermost lock space and, downstream, the partition ones. get
            -- and getMany are allowed because the caller fixes their cost: the
            -- boundary is COST, not the kind of operation.
            IF v_wire THEN
                RAISE EXCEPTION 'kv_get_prefix_not_allowed_in_transaction'
                    USING ERRCODE = '22023',
                          DETAIL  = format('op at index %s: getPrefix is only available on POST /api/v1/kv',
                                           v_ord - 1);
            END IF;
            v_prefix := v_op->>'prefix';
            IF v_prefix IS NULL OR v_prefix = '' THEN
                -- A namespace is not a table to enumerate. It is the declared
                -- boundary, not an oversight.
                RAISE EXCEPTION 'kv_prefix_required'
                    USING ERRCODE = '22023',
                          DETAIL  = format('op at index %s: getPrefix needs a non-empty prefix',
                                           v_ord - 1);
            END IF;
            PERFORM queen.kv_check_names_v1(v_ns, v_prefix);

        ELSIF v_kind = 'getMany' THEN
            IF jsonb_typeof(v_op->'keys') <> 'array' THEN
                RAISE EXCEPTION 'kv_bad_request'
                    USING ERRCODE = '22023',
                          DETAIL  = format('op at index %s: getMany needs a keys array', v_ord - 1);
            END IF;
            PERFORM queen.kv_check_names_v1(v_ns, t.k)
               FROM jsonb_array_elements_text(v_op->'keys') AS t(k);

        ELSE
            PERFORM queen.kv_check_names_v1(v_ns, v_op->>'key');
        END IF;

        -- ---------------------------------------------------------- writes
        IF v_kind IN ('put','putIfAbsent','incr') THEN
            -- §5.1: EXACTLY ONE of ttlSeconds and forever:true. Zero or two
            -- declarations are the same error, because both mean the caller did
            -- not decide, and a default here is how a marker becomes immortal.
            v_decls := 0;
            IF v_op ? 'ttlSeconds' THEN
                v_decls := v_decls + 1;
                IF jsonb_typeof(v_op->'ttlSeconds') <> 'number' THEN
                    RAISE EXCEPTION 'kv_bad_ttl'
                        USING ERRCODE = '22023',
                              DETAIL  = format('op at index %s: ttlSeconds must be an integer greater than zero',
                                               v_ord - 1);
                END IF;
                -- ttlSeconds, never ttlMillis, anywhere on the wire (§20.1).
                IF (v_op->>'ttlSeconds')::numeric <= 0
                   OR (v_op->>'ttlSeconds')::numeric <> trunc((v_op->>'ttlSeconds')::numeric) THEN
                    RAISE EXCEPTION 'kv_bad_ttl'
                        USING ERRCODE = '22023',
                              DETAIL  = format('op at index %s: ttlSeconds must be an integer greater than zero',
                                               v_ord - 1);
                END IF;
            END IF;
            IF (v_op->'forever') = 'true'::jsonb THEN
                v_decls := v_decls + 1;
            END IF;
            IF v_decls <> 1 THEN
                -- MESSAGE opaque: no namespace, no key. Names live in the DETAIL
                -- (§13.5), which the broker reads programmatically.
                RAISE EXCEPTION 'kv_expiry_not_specified'
                    USING ERRCODE = '22023',
                          DETAIL  = format('op at index %s: exactly one of ttlSeconds (integer > 0) and forever:true is required, got %s',
                                           v_ord - 1, v_decls);
            END IF;
        END IF;

        IF v_kind IN ('put','putIfAbsent') THEN
            IF NOT (v_op ? 'value') THEN
                RAISE EXCEPTION 'kv_bad_request'
                    USING ERRCODE = '22023',
                          DETAIL  = format('op at index %s: %s needs a value ("value": null is legal, an absent value is not)',
                                           v_ord - 1, v_kind);
            END IF;
            IF octet_length((v_op->'value')::text) > C_MAX_VALUE_BYTES THEN
                -- Two places measure DIFFERENT things and it must be documented:
                -- the HTTP edge measures the raw body bytes, this measures the
                -- canonical JSONB text, normally shorter (§9.2).
                RAISE EXCEPTION 'kv_value_too_large'
                    USING ERRCODE = '22001',
                          DETAIL  = format('op at index %s: value is %s bytes, the ceiling is %s',
                                           v_ord - 1, octet_length((v_op->'value')::text),
                                           C_MAX_VALUE_BYTES);
            END IF;
        END IF;

        IF v_kind IN ('put','putIfAbsent','delete') AND v_op ? 'expect' THEN
            -- §5.3: an explicitly undefined/null expect is a client-side bug, not
            -- a silent downgrade to upsert. The user wrote the word "expect", so
            -- they declared the intention to fence.
            IF jsonb_typeof(v_op->'expect') <> 'number'
               OR (v_op->>'expect')::numeric < 0
               OR (v_op->>'expect')::numeric <> trunc((v_op->>'expect')::numeric) THEN
                RAISE EXCEPTION 'kv_bad_expect'
                    USING ERRCODE = '22023',
                          DETAIL  = format('op at index %s: expect must be a non-negative integer version (0 = must not exist)',
                                           v_ord - 1);
            END IF;
            IF v_kind = 'putIfAbsent' AND (v_op->>'expect')::bigint <> 0 THEN
                RAISE EXCEPTION 'kv_bad_expect'
                    USING ERRCODE = '22023',
                          DETAIL  = format('op at index %s: putIfAbsent desugars to put with expect:0; a different expect is a contradiction',
                                           v_ord - 1);
            END IF;
        END IF;

        IF v_kind = 'incr' THEN
            -- incr has NO expect: it is the way OUT of CAS, and a precondition
            -- would reintroduce the very loop incr exists to remove (§5.4).
            IF v_op ? 'expect' THEN
                RAISE EXCEPTION 'kv_bad_request'
                    USING ERRCODE = '22023',
                          DETAIL  = format('op at index %s: incr takes no expect', v_ord - 1);
            END IF;
            IF jsonb_typeof(v_op->'delta') <> 'number' THEN
                RAISE EXCEPTION 'kv_bad_request'
                    USING ERRCODE = '22023',
                          DETAIL  = format('op at index %s: incr needs a numeric delta', v_ord - 1);
            END IF;
            IF v_op ? 'min' AND jsonb_typeof(v_op->'min') <> 'number' THEN
                RAISE EXCEPTION 'kv_bad_request'
                    USING ERRCODE = '22023',
                          DETAIL  = format('op at index %s: min must be numeric', v_ord - 1);
            END IF;
            IF v_op ? 'max' AND jsonb_typeof(v_op->'max') <> 'number' THEN
                RAISE EXCEPTION 'kv_bad_request'
                    USING ERRCODE = '22023',
                          DETAIL  = format('op at index %s: max must be numeric', v_ord - 1);
            END IF;
        END IF;

        IF v_op ? 'required' AND jsonb_typeof(v_op->'required') <> 'boolean' THEN
            RAISE EXCEPTION 'kv_bad_request'
                USING ERRCODE = '22023',
                      DETAIL  = format('op at index %s: required must be a boolean', v_ord - 1);
        END IF;
    END LOOP;

    -- §6.1 point 3, LOAD-BEARING: at most one WRITE op per key per call. This is
    -- what makes the intra-space order TOTAL. Read ops are exempt because they
    -- take no row lock.
    PERFORM 1
       FROM jsonb_array_elements(p_ops) o
      WHERE o.value->>'op' IN ('put','putIfAbsent','delete','incr')
      GROUP BY (o.value->>'ns') COLLATE "C", (o.value->>'key') COLLATE "C"
     HAVING count(*) > 1
      LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION 'kv_duplicate_key_in_call'
            USING ERRCODE = '22023',
                  DETAIL  = 'a key may be written at most once per kv_apply_v1 call; it is what makes the intra-space lock order total';
    END IF;

    -- §6.1 point 4: the SUM of the keys of every op, not the op count. getPrefix
    -- counts as its clamped limit, which is the most it can return.
    SELECT COALESCE(sum(
               CASE o.value->>'op'
                   WHEN 'getMany'   THEN jsonb_array_length(o.value->'keys')
                   WHEN 'getPrefix' THEN LEAST(GREATEST(COALESCE((o.value->>'limit')::int,
                                                                 C_PREFIX_DEFAULT), 1),
                                               C_PREFIX_CAP)
                   ELSE 1
               END), 0)
      INTO v_total
      FROM jsonb_array_elements(p_ops) o;
    IF v_total > v_max_keys THEN
        RAISE EXCEPTION 'kv_too_many_keys'
            USING ERRCODE = '22023',
                  DETAIL  = format('%s keys in one call, the ceiling is %s', v_total, v_max_keys);
    END IF;

    -- =====================================================================
    -- PASS 2 — APPLY, in ascending (namespace, key) COLLATE "C" (§6.1 point 2).
    -- The COLLATE is explicit on both sort keys because an extraction from JSONB
    -- does NOT inherit the column's collation (§2.4 C3 piece 2). The ordinal is
    -- only the tiebreaker; results are written by ordinal, never appended.
    --
    -- THE PHASE COLUMN, and why it does not weaken any of that. Every op that
    -- takes a ROW LOCK names exactly one key, so all of them are phase 0 and the
    -- order among them is exactly ascending (namespace, key) — which is the whole
    -- of the intra-space requirement of §2.3, since the multi-key READS take no
    -- lock at all and cannot be an edge of the wait-for graph.
    -- What the phase buys is that getMany and getPrefix run LAST, so a read in a
    -- batch always observes that batch's own writes. Without it a getMany sorts
    -- at ('ns','') — before every key in its namespace — and reports as `missing`
    -- a key the very same call is about to write, which is a silent wrong answer
    -- rather than an error. Single-key ops keep their input order against each
    -- other through the ordinal tiebreak, so a get after a put on the same key
    -- still sees the put.
    -- =====================================================================
    v_out := array_fill(NULL::jsonb, ARRAY[v_n]);

    FOR v_rec IN
        SELECT o.ordinality::int AS ord,
               o.value AS op,
               CASE WHEN o.value->>'op' IN ('getMany','getPrefix') THEN 1 ELSE 0 END AS phase,
               COALESCE(o.value->>'ns', '') COLLATE "C" AS sort_ns,
               COALESCE(o.value->>'key', o.value->>'prefix', '') COLLATE "C" AS sort_key
          FROM jsonb_array_elements(p_ops) WITH ORDINALITY o
         ORDER BY 3, 4, 5, 1
    LOOP
        v_op       := v_rec.op;
        v_ord      := v_rec.ord;
        v_kind     := v_op->>'op';
        v_ns       := v_op->>'ns';
        v_key      := v_op->>'key';
        v_required := COALESCE((v_op->>'required')::boolean, FALSE);
        v_applied  := FALSE;
        v_reason   := NULL;
        v_newval   := NULL;
        v_newver   := NULL;
        v_curval   := NULL;
        v_curver   := NULL;
        v_curexp   := NULL;
        v_res      := NULL;

        -- putIfAbsent DESUGARS to put with expect:0 at the entry of this loop:
        -- one code path. It exists under its own name because that is the name of
        -- the thing, and because "did I win?" is the question most often asked of
        -- this API (§5.3).
        IF v_kind = 'putIfAbsent' THEN
            v_kind   := 'put';
            v_expect := 0;
        ELSIF v_op ? 'expect' THEN
            v_expect := (v_op->>'expect')::bigint;
        ELSE
            v_expect := NULL;
        END IF;

        IF v_kind IN ('put','incr') THEN
            v_forever := ((v_op->'forever') = 'true'::jsonb);
            IF v_forever THEN
                v_expiry := NULL;
            ELSE
                v_ttl    := (v_op->>'ttlSeconds')::bigint;
                v_expiry := v_now + make_interval(secs => v_ttl::double precision);
            END IF;
        END IF;

        -- =================================================================
        CASE v_kind

        -- --------------------------------------------------------- put
        WHEN 'put' THEN
            v_value := v_op->'value';

            IF v_expect IS NULL THEN
                -- Unconditional upsert: replaces value AND expiry. It never
                -- inherits the previous expiry (§5.1).
                INSERT INTO queen.kv AS k
                       (tenant_id, namespace, key, value, version, expires_at, created_at, updated_at)
                VALUES (p_tenant, v_ns, v_key, v_value,
                        nextval('queen.kv_version_seq'), v_expiry, v_now, v_now)
                ON CONFLICT (tenant_id, namespace, key) DO UPDATE
                   SET value      = EXCLUDED.value,
                       version    = EXCLUDED.version,
                       expires_at = EXCLUDED.expires_at,
                       -- An expired row being overwritten is a NEW lineage, so
                       -- created_at restarts; a live one keeps its birthday.
                       created_at = CASE WHEN queen.kv_live_v1(k.expires_at, v_now)
                                         THEN k.created_at ELSE EXCLUDED.created_at END,
                       updated_at = EXCLUDED.updated_at
                RETURNING k.value, k.version INTO v_newval, v_newver;
                v_applied := TRUE;

            ELSIF v_expect = 0 THEN
                -- "Must not exist", and it WINS against an expired-but-unpruned
                -- row (the resurrection of §5.3/§5.7). The row lock is taken
                -- before the WHERE is evaluated, so exactly one of N concurrent
                -- callers applies.
                WITH ins AS (
                    INSERT INTO queen.kv AS k
                           (tenant_id, namespace, key, value, version, expires_at, created_at, updated_at)
                    VALUES (p_tenant, v_ns, v_key, v_value,
                            nextval('queen.kv_version_seq'), v_expiry, v_now, v_now)
                    ON CONFLICT (tenant_id, namespace, key) DO UPDATE
                       SET value      = EXCLUDED.value,
                           version    = EXCLUDED.version,
                           expires_at = EXCLUDED.expires_at,
                           created_at = EXCLUDED.created_at,   -- new lineage
                           updated_at = EXCLUDED.updated_at
                     WHERE NOT queen.kv_live_v1(k.expires_at, v_now)
                    RETURNING k.value AS value, k.version AS version
                )
                SELECT (SELECT count(*) FROM ins) > 0,
                       (SELECT value   FROM ins),
                       (SELECT version FROM ins),
                       cur.value, cur.version, cur.expires_at
                  INTO v_applied, v_newval, v_newver, v_curval, v_curver, v_curexp
                  FROM (SELECT 1) d
                  LEFT JOIN queen.kv cur
                         ON cur.tenant_id = p_tenant
                        AND cur.namespace = v_ns
                        AND cur.key       = v_key;

                IF NOT v_applied THEN
                    IF v_curver IS NULL THEN
                        -- Our statement snapshot predates the winner's commit.
                        -- Safe here, and ONLY here: the ON CONFLICT arm took the
                        -- row lock, so this statement could not have finished
                        -- before the winner committed, and a fresh snapshot is
                        -- guaranteed to see it.
                        SELECT value, version, expires_at
                          INTO v_curval, v_curver, v_curexp
                          FROM queen.kv
                         WHERE tenant_id = p_tenant AND namespace = v_ns AND key = v_key;
                    END IF;
                    v_reason := 'exists';
                END IF;

            ELSE
                -- expect: N > 0 — a PURE UPDATE, never the INSERT branch. THE
                -- REPAIR THAT MATTERS MOST (§5.3): in the naive ON CONFLICT form
                -- an expect:N>0 on an absent key falls into the INSERT branch
                -- (the conflict arm never runs) and CREATES the row, i.e. in a
                -- saga it fires the compensating command that expect existed to
                -- prevent. An expect that matches zero rows must create NOTHING.
                --
                -- The loser is discriminated in the SAME statement (CTE upd +
                -- LEFT JOIN), because rows failing the predicate are not locked.
                WITH upd AS (
                    UPDATE queen.kv k
                       SET value      = v_value,
                           version    = nextval('queen.kv_version_seq'),
                           expires_at = v_expiry,
                           updated_at = v_now
                     WHERE k.tenant_id = p_tenant
                       AND k.namespace = v_ns
                       AND k.key       = v_key
                       AND queen.kv_ver_v1(k.version, k.expires_at, v_now) = v_expect
                    RETURNING k.value AS value, k.version AS version
                )
                SELECT (SELECT count(*) FROM upd) > 0,
                       (SELECT value   FROM upd),
                       (SELECT version FROM upd),
                       cur.value, cur.version, cur.expires_at
                  INTO v_applied, v_newval, v_newver, v_curval, v_curver, v_curexp
                  FROM (SELECT 1) d
                  LEFT JOIN queen.kv cur
                         ON cur.tenant_id = p_tenant
                        AND cur.namespace = v_ns
                        AND cur.key       = v_key;

                IF NOT v_applied THEN
                    v_eff_ver := queen.kv_ver_v1(v_curver, v_curexp, v_now);
                    v_reason  := CASE WHEN v_eff_ver = 0 THEN 'absent' ELSE 'version' END;
                END IF;
            END IF;

        -- ------------------------------------------------------ delete
        WHEN 'delete' THEN
            IF v_expect IS NULL THEN
                DELETE FROM queen.kv k
                 WHERE k.tenant_id = p_tenant AND k.namespace = v_ns AND k.key = v_key
                RETURNING k.value, k.version, k.expires_at
                     INTO v_curval, v_curver, v_curexp;
                -- An expired row is removed physically but was never there
                -- logically (§5.7), so it does not count as a deletion.
                v_applied := queen.kv_ver_v1(v_curver, v_curexp, v_now) <> 0;
                IF v_applied THEN
                    v_newval := v_curval;
                    v_newver := v_curver;
                ELSE
                    v_reason := 'absent';
                    v_curval := NULL;
                    v_curver := NULL;
                    v_curexp := NULL;
                END IF;

            ELSIF v_expect = 0 THEN
                -- "It must not exist": idempotent success when it does not, a
                -- verdict when it does.
                SELECT value, version, expires_at
                  INTO v_curval, v_curver, v_curexp
                  FROM queen.kv
                 WHERE tenant_id = p_tenant AND namespace = v_ns AND key = v_key;
                v_applied := queen.kv_ver_v1(v_curver, v_curexp, v_now) = 0;
                IF v_applied THEN
                    v_newver := 0;          -- nothing there, and that was the ask
                    v_newval := NULL;
                ELSE
                    v_reason := 'exists';
                END IF;

            ELSE
                WITH del AS (
                    DELETE FROM queen.kv k
                     WHERE k.tenant_id = p_tenant
                       AND k.namespace = v_ns
                       AND k.key       = v_key
                       AND queen.kv_ver_v1(k.version, k.expires_at, v_now) = v_expect
                    RETURNING k.value AS value, k.version AS version
                )
                SELECT (SELECT count(*) FROM del) > 0,
                       (SELECT value   FROM del),
                       (SELECT version FROM del),
                       cur.value, cur.version, cur.expires_at
                  INTO v_applied, v_newval, v_newver, v_curval, v_curver, v_curexp
                  FROM (SELECT 1) d
                  LEFT JOIN queen.kv cur
                         ON cur.tenant_id = p_tenant
                        AND cur.namespace = v_ns
                        AND cur.key       = v_key;

                IF NOT v_applied THEN
                    v_eff_ver := queen.kv_ver_v1(v_curver, v_curexp, v_now);
                    v_reason  := CASE WHEN v_eff_ver = 0 THEN 'absent' ELSE 'version' END;
                END IF;
            END IF;

        -- -------------------------------------------------------- incr
        WHEN 'incr' THEN
            v_delta := (v_op->>'delta')::numeric;
            v_min   := (v_op->>'min')::numeric;
            v_max   := (v_op->>'max')::numeric;

            -- REPAIR 2 (§5.4): the INSERT branch applies no guard in the naive
            -- form, so the FIRST incr of a window with delta > max returns
            -- applied:true and blows the quota on the first call — and the scene
            -- repeats at EVERY window rotation, i.e. exactly when a limiter is
            -- being attacked. delta against min/max is a PURE comparison: it
            -- needs no row, and it gates the create branch below.
            v_pure_ok := (v_min IS NULL OR v_delta >= v_min)
                     AND (v_max IS NULL OR v_delta <= v_max);

            -- The update branch. Three things are inline in the WHERE and none of
            -- them may become a plpgsql variable:
            --   * REPAIR 1: the resulting value is knowable only INSIDE the
            --     statement, so it is kv_num_v1(...) + delta, three times, never
            --     a v_next computed above.
            --   * REPAIR 3: the type guard must not contradict the expiry rule.
            --     `jsonb_typeof(k.value) = 'number'` alone is evaluated on the OLD
            --     row unconditionally while the SET already treats an expired row
            --     as zero — so someone who did put({count:0}) to "initialise",
            --     and whose key then expired, gets reason:'type' on EVERY request
            --     until the sweeper runs, with a reason no client treats as
            --     retryable. The guard is widened, not deleted: a LIVE
            --     non-numeric row is still 'type'.
            --   * The TTL is CREATE-ONLY: a live row keeps its expiry. If incr
            --     extended it, a fixed-window limiter on an always-active client
            --     would never close its window, i.e. it would stop limiting
            --     exactly under load. It also keeps the UPDATE HOT, since
            --     expires_at is the only indexed column.
            WITH upd AS (
                UPDATE queen.kv k
                   SET value      = to_jsonb(trim_scale(
                                        queen.kv_num_v1(k.value, k.expires_at, v_now) + v_delta)),
                       version    = nextval('queen.kv_version_seq'),
                       expires_at = CASE WHEN queen.kv_live_v1(k.expires_at, v_now)
                                         THEN k.expires_at ELSE v_expiry END,
                       created_at = CASE WHEN queen.kv_live_v1(k.expires_at, v_now)
                                         THEN k.created_at ELSE v_now END,
                       updated_at = v_now
                 WHERE k.tenant_id = p_tenant
                   AND k.namespace = v_ns
                   AND k.key       = v_key
                   AND (NOT queen.kv_live_v1(k.expires_at, v_now)
                        OR jsonb_typeof(k.value) = 'number')
                   AND (v_min IS NULL
                        OR queen.kv_num_v1(k.value, k.expires_at, v_now) + v_delta >= v_min)
                   AND (v_max IS NULL
                        OR queen.kv_num_v1(k.value, k.expires_at, v_now) + v_delta <= v_max)
                RETURNING k.value AS value, k.version AS version
            )
            SELECT (SELECT count(*) FROM upd) > 0,
                   (SELECT value   FROM upd),
                   (SELECT version FROM upd),
                   cur.value, cur.version, cur.expires_at,
                   cur.tenant_id IS NOT NULL
              INTO v_applied, v_newval, v_newver, v_curval, v_curver, v_curexp, v_exists
              FROM (SELECT 1) d
              LEFT JOIN queen.kv cur
                     ON cur.tenant_id = p_tenant
                    AND cur.namespace = v_ns
                    AND cur.key       = v_key;

            IF NOT v_applied AND NOT COALESCE(v_exists, FALSE) THEN
                -- No row at all: the create branch, gated by the pure guard.
                -- ON CONFLICT DO UPDATE with the same row guards closes the race
                -- with a concurrent creator without a second round of plpgsql.
                IF v_pure_ok THEN
                    INSERT INTO queen.kv AS k
                           (tenant_id, namespace, key, value, version, expires_at, created_at, updated_at)
                    VALUES (p_tenant, v_ns, v_key, to_jsonb(trim_scale(v_delta)),
                            nextval('queen.kv_version_seq'), v_expiry, v_now, v_now)
                    ON CONFLICT (tenant_id, namespace, key) DO UPDATE
                       SET value      = to_jsonb(trim_scale(
                                            queen.kv_num_v1(k.value, k.expires_at, v_now) + v_delta)),
                           version    = EXCLUDED.version,
                           expires_at = CASE WHEN queen.kv_live_v1(k.expires_at, v_now)
                                             THEN k.expires_at ELSE EXCLUDED.expires_at END,
                           created_at = CASE WHEN queen.kv_live_v1(k.expires_at, v_now)
                                             THEN k.created_at ELSE EXCLUDED.created_at END,
                           updated_at = EXCLUDED.updated_at
                     WHERE (NOT queen.kv_live_v1(k.expires_at, v_now)
                            OR jsonb_typeof(k.value) = 'number')
                       AND (v_min IS NULL
                            OR queen.kv_num_v1(k.value, k.expires_at, v_now) + v_delta >= v_min)
                       AND (v_max IS NULL
                            OR queen.kv_num_v1(k.value, k.expires_at, v_now) + v_delta <= v_max)
                    RETURNING k.value, k.version INTO v_newval, v_newver;
                    v_applied := v_newver IS NOT NULL;
                END IF;

                IF NOT v_applied THEN
                    -- Nothing was written: the counter must read as it is, which
                    -- for an absent key is the effective zero of kv_num_v1.
                    SELECT value, version, expires_at
                      INTO v_curval, v_curver, v_curexp
                      FROM queen.kv
                     WHERE tenant_id = p_tenant AND namespace = v_ns AND key = v_key;
                    v_reason := 'limit';
                END IF;
            ELSIF NOT v_applied THEN
                -- A row exists and the guards refused it. `max` and `min` do NOT
                -- saturate and do NOT truncate: the call that would overflow does
                -- not apply and returns the CURRENT value, never the would-be
                -- one. Without that, the limiter compares client-side AFTER
                -- incrementing, so the request that breaks the ceiling has
                -- already consumed budget and cannot be undone. With `max`,
                -- `applied` IS the admission decision.
                v_reason := CASE
                    WHEN queen.kv_live_v1(v_curexp, v_now)
                         AND jsonb_typeof(v_curval) <> 'number' THEN 'type'
                    ELSE 'limit'
                END;
            END IF;

            IF v_applied THEN
                v_res := jsonb_build_object(
                    'index',   v_ord - 1,
                    'op',      'incr',
                    'applied', TRUE,
                    'key',     v_key,
                    'value',   v_newval,
                    'version', v_newver);
            ELSE
                v_eff_num := queen.kv_num_v1(v_curval, v_curexp, v_now);
                v_res := jsonb_build_object(
                    'index',   v_ord - 1,
                    'op',      'incr',
                    'applied', FALSE,
                    'reason',  v_reason,
                    'key',     v_key,
                    'value',   to_jsonb(trim_scale(v_eff_num)),
                    'version', queen.kv_ver_v1(v_curver, v_curexp, v_now));
            END IF;

        -- --------------------------------------------------------- get
        WHEN 'get' THEN
            SELECT TRUE, k.value, k.version, k.expires_at, k.updated_at
              INTO v_found, v_curval, v_curver, v_curexp, v_curupd
              FROM queen.kv k
             WHERE k.tenant_id = p_tenant
               AND k.namespace = v_ns
               AND k.key       = v_key
               AND queen.kv_live_v1(k.expires_at, v_now);

            IF COALESCE(v_found, FALSE) THEN
                -- A single key is bounded by C_MAX_VALUE_BYTES on the way in, so
                -- it is always returned; it still spends from the call budget.
                v_read_left := v_read_left - octet_length(v_curval::text);
                v_res := jsonb_build_object(
                    'index',     v_ord - 1,
                    'op',        'get',
                    'found',     TRUE,
                    'key',       v_key,
                    'value',     v_curval,
                    'version',   v_curver,
                    'expiresAt', v_curexp,
                    'updatedAt', v_curupd);
            ELSE
                -- `found` is separate from the value because 'null'::jsonb is a
                -- legal value: {found:true, value:null} and {found:false} are
                -- different things no SDK may collapse (§5.5).
                v_res := jsonb_build_object(
                    'index', v_ord - 1, 'op', 'get', 'found', FALSE, 'key', v_key);
            END IF;

        -- ----------------------------------------------------- getMany
        WHEN 'getMany' THEN
            SELECT array_agg(k ORDER BY o) INTO v_keys
              FROM jsonb_array_elements_text(v_op->'keys') WITH ORDINALITY t(k, o);

            WITH want AS (
                SELECT t.k AS key FROM unnest(v_keys) AS t(k)
            ),
            hit AS (
                SELECT k.key, k.value, k.version, k.expires_at, k.updated_at,
                       octet_length(k.value::text) AS blen,
                       row_number() OVER (ORDER BY k.key) AS rn
                  FROM want w
                  JOIN queen.kv k
                    ON k.tenant_id = p_tenant AND k.namespace = v_ns AND k.key = w.key
                 WHERE queen.kv_live_v1(k.expires_at, v_now)
            ),
            acc AS (
                SELECT h.*,
                       sum(h.blen) OVER (ORDER BY h.rn ROWS UNBOUNDED PRECEDING) - h.blen
                           AS before_bytes
                  FROM hit h
            ),
            kept AS (
                SELECT * FROM acc WHERE before_bytes < v_read_left
            )
            SELECT COALESCE(jsonb_agg(jsonb_build_object(
                       'key', kept.key, 'value', kept.value, 'version', kept.version,
                       'expiresAt', kept.expires_at, 'updatedAt', kept.updated_at)
                       ORDER BY kept.key), '[]'::jsonb),
                   COALESCE(sum(kept.blen), 0)::bigint,
                   count(*)::bigint,
                   (SELECT count(*) FROM acc)::bigint
              INTO v_rows, v_bytes, v_kept, v_total
              FROM kept;

            v_read_left := v_read_left - v_bytes;

            -- Absence must be a DATUM, not a hole the client computes by
            -- difference (§5.5). Keys cut by the byte budget are NOT reported as
            -- missing — they are neither in rows nor in missing, and `truncated`
            -- says so, because calling them absent would be a lie.
            SELECT COALESCE(jsonb_agg(to_jsonb(w.k) ORDER BY w.o), '[]'::jsonb)
              INTO v_missing
              FROM unnest(v_keys) WITH ORDINALITY AS w(k, o)
             WHERE NOT EXISTS (
                   SELECT 1 FROM queen.kv k
                    WHERE k.tenant_id = p_tenant AND k.namespace = v_ns AND k.key = w.k
                      AND queen.kv_live_v1(k.expires_at, v_now));

            v_res := jsonb_build_object(
                'index',     v_ord - 1,
                'op',        'getMany',
                'rows',      v_rows,
                'missing',   v_missing,
                'truncated', v_kept < v_total);

        -- --------------------------------------------------- getPrefix
        WHEN 'getPrefix' THEN
            v_prefix    := v_op->>'prefix';
            v_after     := NULLIF(v_op->>'after', '');
            v_keys_only := COALESCE((v_op->>'keysOnly')::boolean, FALSE);
            -- CLAMPED, never rejected: a 400 on a too-high limit is an error the
            -- user cannot fix without reading the server's configuration (§5.5).
            v_limit     := LEAST(GREATEST(COALESCE((v_op->>'limit')::int, C_PREFIX_DEFAULT), 1),
                                 C_PREFIX_CAP);
            v_end       := queen.kv_prefix_end_v1(v_prefix);

            -- The prefix predicate is ALWAYS this triple, never a LIKE:
            --   * the range is ONLY the index driver, and stays correct when the
            --     bound is NULL;
            --   * starts_with() is the semantics, exactly, and knows no
            --     metacharacters. It kills at the root the hole
            --     009_streams_state_get_v1.sql carries today (LIKE prefix || '%'
            --     with no escaping) and removes % and _ from the user's mental
            --     model instead of asking them to remember an escape.
            -- LIMIT v_limit + 1 in the SUBQUERY, so the extra row decides
            -- `truncated` without a second query and the ceiling is real.
            WITH page AS (
                SELECT k.key, k.value, k.version, k.expires_at, k.updated_at,
                       CASE WHEN v_keys_only THEN 0 ELSE octet_length(k.value::text) END AS blen,
                       row_number() OVER (ORDER BY k.key) AS rn
                  FROM queen.kv k
                 WHERE k.tenant_id = p_tenant
                   AND k.namespace = v_ns
                   AND k.key >= v_prefix
                   AND (v_end IS NULL OR k.key < v_end)
                   AND starts_with(k.key, v_prefix)
                   AND (v_after IS NULL OR k.key > v_after)
                   AND queen.kv_live_v1(k.expires_at, v_now)
                 ORDER BY k.key
                 LIMIT v_limit + 1
            ),
            acc AS (
                SELECT p.*,
                       sum(p.blen) OVER (ORDER BY p.rn ROWS UNBOUNDED PRECEDING) - p.blen
                           AS before_bytes
                  FROM page p
            ),
            kept AS (
                -- A ceiling on the number of keys is not a ceiling on bytes: 1000
                -- keys of 64 KiB are 64 MB, and the real resource is the byte
                -- (§5.5). The row that straddles the budget is included, then the
                -- page stops.
                SELECT * FROM acc WHERE rn <= v_limit AND before_bytes < v_read_left
            )
            SELECT COALESCE(jsonb_agg(
                       CASE WHEN v_keys_only
                            THEN jsonb_build_object('key', kept.key, 'version', kept.version,
                                                    'expiresAt', kept.expires_at,
                                                    'updatedAt', kept.updated_at)
                            ELSE jsonb_build_object('key', kept.key, 'value', kept.value,
                                                    'version', kept.version,
                                                    'expiresAt', kept.expires_at,
                                                    'updatedAt', kept.updated_at)
                       END ORDER BY kept.key), '[]'::jsonb),
                   COALESCE(sum(kept.blen), 0)::bigint,
                   count(*)::bigint,
                   max(kept.key),
                   (SELECT count(*) FROM acc)::bigint
              INTO v_rows, v_bytes, v_kept, v_last, v_total
              FROM kept;

            v_read_left := v_read_left - v_bytes;
            v_trunc     := v_kept < v_total;

            -- `after` is an EXCLUSIVE keyset cursor, not an offset: stable under
            -- COLLATE "C" and aligned with the index order, so no Sort node.
            -- Every page is its own READ COMMITTED snapshot: it is NOT a snapshot
            -- of the namespace, and with `after` it may miss a key inserted behind
            -- the cursor. Fine for compacting state, not for an exact count.
            v_res := jsonb_build_object(
                'index',     v_ord - 1,
                'op',        'getPrefix',
                'rows',      v_rows,
                'truncated', v_trunc,
                'nextAfter', CASE WHEN v_trunc THEN v_last ELSE NULL END);

        ELSE
            -- Unreachable: pass 1 closed the taxonomy.
            RAISE EXCEPTION 'kv_unknown_op'
                USING ERRCODE = '22023', DETAIL = format('unknown operation %L', v_kind);
        END CASE;

        -- Uniform return for put and delete, WITH THE CURRENT VALUE AND VERSION
        -- EVEN WHEN IT DID NOT APPLY (§5.3): the loser must not need a second
        -- round trip — that is the entire point of the idempotency marker. The
        -- version handed to a loser is ADVISORY, never a fencing token to reuse
        -- blindly, and that sentence belongs in the documentation.
        -- (incr and the three reads build their own shape above.)
        IF v_res IS NULL THEN
            IF v_applied THEN
                v_res := jsonb_build_object(
                    'index',   v_ord - 1,
                    'op',      v_op->>'op',
                    'applied', TRUE,
                    'key',     v_key,
                    'value',   v_newval,
                    'version', COALESCE(v_newver, 0));
            ELSE
                v_res := jsonb_build_object(
                    'index',   v_ord - 1,
                    'op',      v_op->>'op',
                    'applied', FALSE,
                    'reason',  v_reason,
                    'key',     v_key,
                    -- An expired row is not a value: it never counts as existing
                    -- (§5.7), so the loser sees the same nothing a reader would.
                    'value',   CASE WHEN queen.kv_live_v1(v_curexp, v_now)
                                    THEN v_curval ELSE NULL END,
                    'version', queen.kv_ver_v1(v_curver, v_curexp, v_now));
            END IF;
        END IF;

        -- §6.1 point 5: escalation is opt-in PER ELEMENT.
        IF v_required AND v_kind IN ('put','delete','incr') AND NOT v_applied THEN
            RAISE EXCEPTION 'kv_precondition_failed'
                USING ERRCODE = 'check_violation',
                      DETAIL  = left(jsonb_build_object(
                                    'index',   v_ord - 1,
                                    'op',      v_op->>'op',
                                    'ns',      v_ns,
                                    'key',     v_key,
                                    'reason',  v_reason,
                                    'version', v_res->'version',
                                    'value',   v_res->'value')::text, C_DETAIL_CAP);
        END IF;

        v_out[v_ord] := v_res;
    END LOOP;

    -- §6.4: the count guard BEFORE to_jsonb. Without it an unfilled ordinal
    -- becomes a silent JSON null in position, which is exactly the class of
    -- misalignment 003_log_push.sql:372-375 fails loudly on.
    SELECT count(*) INTO v_nulls FROM unnest(v_out) AS t(x) WHERE t.x IS NULL;
    IF COALESCE(array_length(v_out, 1), 0) <> v_n OR v_nulls > 0 THEN
        RAISE EXCEPTION 'kv_result_misaligned'
            USING ERRCODE = 'internal_error',
                  DETAIL  = format('%s ops in, %s results out, %s unfilled ordinals',
                                   v_n, COALESCE(array_length(v_out, 1), 0), v_nulls);
    END IF;

    RETURN to_jsonb(v_out);
END;
$$;

GRANT EXECUTE ON FUNCTION queen.kv_live_v1(TIMESTAMPTZ, TIMESTAMPTZ) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.kv_ver_v1(BIGINT, TIMESTAMPTZ, TIMESTAMPTZ) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.kv_num_v1(JSONB, TIMESTAMPTZ, TIMESTAMPTZ) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.kv_prefix_end_v1(TEXT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.kv_check_names_v1(TEXT, TEXT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION queen.kv_apply_v1(JSONB, UUID, TIMESTAMPTZ, BOOLEAN) TO PUBLIC;
