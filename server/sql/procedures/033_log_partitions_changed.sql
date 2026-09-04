-- ============================================================================
-- Log engine — PARTITION DISCOVERY (PLAN_S3_SINK.md §5.1, §5.2).
--
--   queen.log_partitions_changed_v1  "which partitions of these queues exist,
--                                     and which of them have been written to
--                                     since T" — batched over queues, paged,
--                                     READ-ONLY, tenant-scoped.
--
-- WHY IT EXISTS. Nothing in the HTTP surface lists partition NAMES (F6 of the
-- plan): `GET /api/v1/resources/queues` answers a per-queue COUNT, and the
-- Kafka facade only gets away with that because its partitions are named
-- "0".."N-1". A queue partitioned by entity — "one entity, one ordered
-- partition", 10k-1M lazily-created lanes — is undiscoverable over HTTP, so a
-- sink that mirrors a whole queue into an object store cannot even learn what
-- to fetch. This is that endpoint's only job.
--
-- WHAT IT IS NOT. Not a pop, and not even a fetch: it reads exactly two tables
-- (queen.queues, queen.log_partitions), takes no lease, moves no cursor, and
-- reads and writes nothing on queen.log_segments. It writes nothing anywhere.
-- Two callers asking
-- the same question get the same answer and neither disturbs a consumer group.
-- It is the discovery half of what `POST /api/v1/fetch` (032_log_fetch) is the
-- read half of, and it deliberately answers `lastOffset` / `logStart` so a
-- caller can start fetching without a second round trip.
--
-- COST. O(rows returned), never O(#partitions). Two modes, one index each:
--
--   * p_since[i] IS NULL — FULL ENUMERATION, ordered by name, through the
--     UNIQUE (queue_id, name) index of 001_log_schema. This is the cold-start
--     sweep: it walks the whole partition set of a queue once, `p_limits[i]`
--     rows at a time.
--
--   * p_since[i] IS NOT NULL — WHAT MOVED, ordered by (last_write_at, name),
--     through idx_log_partitions_queue_write (queue_id, last_write_at)
--     (001_log_schema:70-74, added precisely so a scan can visit "recently
--     written partitions of a queue instead of the full partition set —
--     decisive at 10-20k partitions/queue"). This is the steady-state sweep:
--     the fifty lanes that moved cost fifty rows, not a million.
--
-- WHY (last_write_at, name) IS A SOUND PAGING KEY. `last_write_at` is QUANTIZED
-- to at most one real change per second per partition (001_log_schema:39-45,
-- 003_log_push:193-194) so that the allocator UPDATE stays HOT, and it only
-- ever moves UP — the push path never writes it backwards and nothing else
-- writes it at all. A row whose timestamp is bumped between two pages of the
-- same sweep therefore moves FORWARD past the cursor and is seen a SECOND time
-- on a later page; it can never move behind the cursor and be missed. Seen
-- twice is free (the caller re-reads bounds it already had); missed would be
-- silent data loss in the sink. That asymmetry is the whole argument.
--
-- THE CURSOR IS OPAQUE, AND IT IS MODE-TAGGED. `next` is a string the caller
-- echoes back in `p_after[i]` and must not parse:
--
--     full enumeration :  'n|' || name
--     since mode       :  't|' || <last_write_at as epoch MICROSECONDS>
--                              || '|' || name
--
-- Microseconds, not seconds: `last_write_at` is quantized to a second in the
-- steady state but the column is a full TIMESTAMPTZ and a partition's FIRST
-- push stamps it unquantized, so a second-resolution cursor would round two
-- distinct rows onto one key and page-skip between them. The name is the whole
-- remainder after the second '|', so a name containing '|' round-trips
-- unharmed.
--
-- The mode tag is not decoration. A caller that pages a since-sweep with a
-- cursor from an enumeration sweep (or the reverse) is asking a question whose
-- answer would be silently wrong — a fabricated timestamp, or a name compared
-- against the wrong column — so the mismatch is an explicit per-entry
-- 'BAD_CURSOR' rather than a quiet restart from the beginning. Restarting
-- quietly would loop a paging client for ever on the same first page.
--
-- TENANCY (Track B §5), and it is 032_log_fetch's argument verbatim: the queue
-- resolve reads queen.queues on (name, tenant_id), so a queue owned by another
-- tenant resolves to NOTHING and lands in the SAME
-- 'UNKNOWN_TOPIC_OR_PARTITION' arm as a queue that does not exist anywhere.
-- The two answers are byte-identical — same key set, same order, same bytes —
-- so this endpoint cannot be used to discover that another tenant's queue
-- exists. There is no second arm to leak through: unlike the fetch path there
-- is no "queue yes, lane never written" case here, because an unwritten lane
-- has no queen.log_partitions row and is simply not enumerated.
--
-- BOUNDS. `p_limits[i]` is clamped to 1..1000 here as well as at the HTTP
-- boundary, so an unbounded read is not expressible even by a caller that
-- reaches the function directly (the embedded broker, a test, psql).
--
-- ----------------------------------------------------------------------------
-- VOLATILE, AND THE ORDER OF THE FIRST STATEMENT IS LOAD-BEARING
-- ----------------------------------------------------------------------------
--
-- This function writes nothing, so `STABLE` is what its body looks like it
-- deserves — and `STABLE` is WRONG here, for a reason that has nothing to do
-- with writes and everything to do with snapshots. It was found by the engine's
-- randomized test, and it is a real soundness corner, not a theoretical one.
--
-- WHAT THE SINK ASSUMES. It bounds every window close by the `safeTime` of a
-- discovery pass, and it relies on the converse of the enumeration: "any
-- partition this pass did not name was created by a transaction whose
-- xact_start >= that safeTime". Everything below serves that one sentence.
--
-- THE HOLE UNDER `STABLE`. A `STABLE` function's statements all execute under
-- the snapshot taken at the start of the CALLING query — but `pg_stat_activity`
-- is not read through that snapshot at all: it reports LIVE backend state.
-- The two therefore observe the database at different instants, and a
-- transaction can slip through the gap between them:
--
--   1. the calling query takes its snapshot; a long write transaction T is in
--      flight, so its new queen.log_partitions row is not visible in it;
--   2. T commits;
--   3. the body reads pg_stat_activity — T is gone, so it contributes nothing
--      to min(xact_start);
--   4. the partition scan runs under the snapshot from (1), which still cannot
--      see T's row.
--
-- T is in neither the scan nor the watermark. safeTime can therefore exceed
-- T's `created_at`, its records sit BELOW a window the sink then closes, and a
-- later pass discards them as already committed. Silent loss, of exactly the
-- rows the whole protocol exists to not lose.
--
-- WHY `VOLATILE` CLOSES IT. A `VOLATILE` function obtains a FRESH snapshot for
-- each statement of its body (PostgreSQL docs, "Function Volatility
-- Categories", xfunc-volatility), instead of inheriting the caller's. Combined
-- with reading pg_stat_activity FIRST — before the queue resolve and before
-- any partition scan — every case is covered:
--
--   * T committed before the scan's snapshot  → VISIBLE in the scan, so the
--     partition is named and the sink reads it. No claim about safeTime is
--     needed.
--   * T had not committed by then → at the earlier activity read it was either
--     already in flight, so its xact_start is inside min(xact_start) and
--     safeTime <= xact_start <= created_at; or it had not started, so
--     xact_start > the activity read >= safeTime. Either way
--     created_at >= safeTime and its records are ABOVE the window.
--
-- Which is why the order is not stylistic. Reading the activity AFTER the scan
-- would re-open the gap in the other direction, and any table read placed
-- before it does the same for that table.
--
-- THE GUARD IS BELT, NOT THE ARGUMENT. `p_guard_s` (5 s) exists for stats
-- propagation skew between backends, and it makes safeTime strictly older than
-- the activity read, which is convenient in the second case above. It is not
-- what makes the function sound: with the guard at zero the case analysis still
-- holds. Do not let a future edit present it as the mechanism.
--
-- ONE PRECONDITION, and the broker meets it: the fresh-snapshot-per-statement
-- rule is READ COMMITTED behaviour. A caller that wrapped this in an explicit
-- REPEATABLE READ or SERIALIZABLE transaction would pin one snapshot for the
-- whole transaction and reinstate the hole, `VOLATILE` or not. `db.rs` issues
-- it as a single autocommit statement at the default isolation.
--
-- WHAT IT COSTS. One extra snapshot acquisition per statement — a few
-- microseconds each, against two indexed reads per entry. It also means two
-- entries of one batch can observe states microseconds apart; harmless, because
-- the case analysis above is per entry and each entry's snapshot is later than
-- the single activity read that produced the one safeTime.
-- ============================================================================

-- ============================================================================
-- safeTime — the plan's §5.2 watermark, computed ONCE per call.
--
-- WHAT IT MEANS. "No segment that will ever be committed to this database can
-- carry a created_at at or below this instant." It is what makes a sink's time
-- window a DETERMINISTIC SET: a window that ends at or below safeTime can be
-- re-read after a crash and yield exactly the same rows, so a retried upload is
-- byte-identical and the commit is exactly-once without naming a million
-- offsets.
--
-- WHY IT IS SOUND. `created_at` is `clock_timestamp()` taken while the
-- partition allocator row lock is held (003_log_push:138-142, :219-223), so it
-- is stamped at some instant during the inserting transaction — necessarily at
-- or after that transaction's `xact_start`. Therefore the oldest `xact_start`
-- among all in-transaction sessions is a floor under every `created_at` still
-- to arrive, and anything strictly below it is settled.
--
--   * min(xact_start) is taken over EVERY in-transaction session, not only
--     those holding a backend_xid: a transaction that has begun and not yet
--     written will still write with created_at >= its xact_start.
--   * an IDLE session has xact_start NULL and pins nothing.
--   * a long READ-ONLY transaction (a stats refresh) pins safeTime and delays
--     the sink's windows by its own duration. That is a latency cost, never a
--     correctness one, and it is what `queen_s3_safe_lag_seconds` is for.
--   * `guard` (default 5 s) covers stats-propagation skew between backends:
--     a session that started a transaction microseconds ago may not be visible
--     in this backend's snapshot of pg_stat_activity yet.
--   * HA needs nothing: pg_stat_activity is cluster-wide, so a second broker's
--     sessions pin correctly with no mesh involvement.
--
-- THE MASKING FALLBACK (F14 of the plan, learned live — see the
-- "`pg_locks` decides, `pg_stat_activity` only decorates" note in
-- server/src/schema.rs). PostgreSQL MASKS pg_stat_activity across roles: for a
-- session owned by a role the reader is not a member of, and without
-- `pg_read_all_stats`, `state`, `xact_start`, `query_start` and `backend_start`
-- all come back NULL. A masked row therefore contributes NOTHING to
-- min(xact_start) while being exactly the kind of session that could be holding
-- a decades-old write transaction open. min() over the visible rows would then
-- be an OVERESTIMATE of the safe instant — the one direction that is silent
-- data loss in the sink.
--
-- So masked rows are COUNTED, and one is enough to make the honest answer
-- unknowable: the function falls back to `now() - floor` (default 30 s, which
-- must exceed any write statement's timeout) and says so with
-- `safeTimeDegraded: true`. The deploy page's rule is the other half: run every
-- broker of one cell under ONE Postgres role, or grant `pg_read_all_stats`.
--
-- `state IS NULL AND xact_start IS NULL` is what a masked row looks like and
-- nothing else does: an idle session of our own role has xact_start NULL but
-- `state` = 'idle'. Background workers (checkpointer, autovacuum, walwriter)
-- have both NULL too, which is why the filter keeps only client backends —
-- and why it ALSO keeps rows whose backend_type is itself NULL. That last
-- clause is the fail-safe direction: if a future PostgreSQL masked
-- `backend_type` as well, an equality filter would silently DROP every masked
-- row and the degrade would never fire, which is precisely the failure this
-- whole paragraph exists to prevent.
-- ============================================================================

-- DROP+CREATE, not CREATE OR REPLACE, for 032_log_fetch's reason: the return
-- type is part of the signature and OR REPLACE refuses to change it, so a
-- future shape change would brick boot re-apply. The applier groups a DROP with
-- the statement that follows it into one transaction, so a live HA peer never
-- observes the gap.
DROP FUNCTION IF EXISTS queen.log_partitions_changed_v1(TEXT[], TIMESTAMPTZ[], TEXT[], INT4[], INT, INT, UUID);
CREATE FUNCTION queen.log_partitions_changed_v1(
    p_queues  TEXT[],
    p_since   TIMESTAMPTZ[],
    p_after   TEXT[],
    p_limits  INT4[],
    p_guard_s INT  DEFAULT 5,
    p_floor_s INT  DEFAULT 30,
    p_tenant  UUID DEFAULT '00000000-0000-0000-0000-000000000001'
) RETURNS JSONB
-- VOLATILE despite writing nothing, and it is a CORRECTNESS declaration: it is
-- what gives each statement of the body its own snapshot, so the partition scan
-- observes the database strictly LATER than the pg_stat_activity read that
-- produced safeTime. See "VOLATILE, AND THE ORDER OF THE FIRST STATEMENT IS
-- LOAD-BEARING" in the header before changing it to STABLE because the body
-- looks read-only. It does look read-only. That is not the question.
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    v_n        INT;
    v_i        INT;
    v_qid      UUID;
    v_since    TIMESTAMPTZ;
    v_after    TEXT;
    v_lim      INT;
    v_cur_ts   TIMESTAMPTZ;
    v_cur_name TEXT;
    v_lo       TIMESTAMPTZ;
    v_parts    JSONB;
    v_count    INT;
    v_next     TEXT;
    v_bad      BOOLEAN;
    v_row      RECORD;
    v_entries  JSONB := '[]'::jsonb;
    v_safe     TIMESTAMPTZ;
    v_masked   BIGINT;
    v_degraded BOOLEAN := FALSE;
    v_sep      INT;
    v_rest     TEXT;
BEGIN
    -- ---------------------------------------------------------------- safeTime
    -- THE FIRST STATEMENT OF THE BODY, AND THAT POSITION IS LOAD-BEARING: every
    -- table read below runs under a LATER snapshot (the function is VOLATILE),
    -- which is what makes "a partition this pass did not name was created by a
    -- transaction whose xact_start >= safeTime" true. Nothing that reads a table
    -- may be moved above it — not the array guard, not the queue resolve, not a
    -- future validation that seems harmless. See the header.
    --
    -- ONE scan of pg_stat_activity per call, whatever the batch holds: the CTE
    -- is referenced twice so PostgreSQL materialises it.
    WITH s AS (
        SELECT a.xact_start, a.state
        FROM pg_stat_activity a
        WHERE a.pid <> pg_backend_pid()
          -- Client backends only — a checkpointer would otherwise be counted as
          -- masked. `backend_type IS NULL` is kept deliberately; see the header.
          AND (a.backend_type = 'client backend' OR a.backend_type IS NULL)
    )
    SELECT LEAST(now() - make_interval(secs => GREATEST(COALESCE(p_guard_s, 5), 0)),
                 COALESCE((SELECT min(x.xact_start) FROM s x), now())),
           (SELECT count(*) FROM s x WHERE x.state IS NULL AND x.xact_start IS NULL)
    INTO v_safe, v_masked;

    IF COALESCE(v_masked, 0) > 0 THEN
        v_safe     := now() - make_interval(secs => GREATEST(COALESCE(p_floor_s, 30), 0));
        v_degraded := TRUE;
    END IF;

    -- ----------------------------------------------------------------- entries
    v_n := COALESCE(array_length(p_queues, 1), 0);
    -- Misalignment is a demux bug on the broker side and multi-array unnest()
    -- would silently pad the short ones with NULLs, answering one queue's
    -- partitions under another queue's name. Fail the whole call loudly instead
    -- (the QFETCH guard of 032_log_fetch, same reasoning). Checked even at
    -- v_n = 0, where a non-empty companion array is the same bug.
    IF COALESCE(array_length(p_since, 1), 0)  IS DISTINCT FROM v_n
       OR COALESCE(array_length(p_after, 1), 0)  IS DISTINCT FROM v_n
       OR COALESCE(array_length(p_limits, 1), 0) IS DISTINCT FROM v_n THEN
        RAISE EXCEPTION 'QPARTS misaligned arrays (% queues)', v_n;
    END IF;

    FOR v_i IN 1..v_n LOOP
        v_qid := NULL;
        -- queues_tenant_name_uk covers this predicate exactly: ONE indexed row
        -- read. Unknown here means unknown TO THIS TENANT — see the header.
        SELECT q.id INTO v_qid
        FROM queen.queues q
        WHERE q.name = p_queues[v_i] AND q.tenant_id = p_tenant;

        IF v_qid IS NULL THEN
            v_entries := v_entries || jsonb_build_object(
                'queue', p_queues[v_i],
                'error', 'UNKNOWN_TOPIC_OR_PARTITION');
            CONTINUE;
        END IF;

        v_since := p_since[v_i];
        v_after := NULLIF(p_after[v_i], '');
        -- Clamped, not rejected, and clamped HERE as well as at the HTTP
        -- boundary: this function must never be the place where an unbounded
        -- read is possible.
        v_lim := LEAST(GREATEST(COALESCE(p_limits[v_i], 1000), 1), 1000);

        v_cur_ts   := NULL;
        v_cur_name := NULL;
        v_bad      := FALSE;

        IF v_after IS NOT NULL THEN
            IF v_since IS NULL THEN
                -- Enumeration cursor: 'n|' || name.
                IF left(v_after, 2) = 'n|' THEN
                    v_cur_name := substr(v_after, 3);
                ELSE
                    v_bad := TRUE;
                END IF;
            ELSE
                -- Since cursor: 't|' || epoch micros || '|' || name.
                IF left(v_after, 2) = 't|' THEN
                    v_rest := substr(v_after, 3);
                    v_sep  := position('|' IN v_rest);
                    IF v_sep < 2 OR left(v_rest, v_sep - 1) !~ '^[0-9]+$' THEN
                        v_bad := TRUE;
                    ELSE
                        v_cur_ts := to_timestamp(
                            left(v_rest, v_sep - 1)::numeric / 1000000.0);
                        v_cur_name := substr(v_rest, v_sep + 1);
                    END IF;
                ELSE
                    v_bad := TRUE;
                END IF;
            END IF;
        END IF;

        IF v_bad THEN
            -- The cursor does not belong to the sweep it was sent with. Loud,
            -- because the quiet alternative loops a paging client for ever on
            -- its own first page — see the header.
            v_entries := v_entries || jsonb_build_object(
                'queue', p_queues[v_i],
                'error', 'BAD_CURSOR');
            CONTINUE;
        END IF;

        v_parts := '[]'::jsonb;
        v_count := 0;
        v_next  := NULL;

        IF v_since IS NULL THEN
            -- FULL ENUMERATION, UNIQUE (queue_id, name).
            FOR v_row IN
                SELECT p.name, p.last_offset, p.log_start, p.last_write_at
                FROM queen.log_partitions p
                WHERE p.queue_id = v_qid
                  AND (v_cur_name IS NULL OR p.name > v_cur_name)
                ORDER BY p.name
                LIMIT v_lim
            LOOP
                -- AT TIME ZONE 'UTC' before to_char, and it is load-bearing:
                -- to_char renders a timestamptz in the SESSION's TimeZone while
                -- this format string ends in a literal "Z", so on any
                -- connection that is not UTC — a PGTZ, a postgresql.conf, an
                -- ALTER ROLE ... SET timezone — every partition would be
                -- stamped local time and LABELLED UTC. A sink compares this
                -- value against its window boundaries; hours of skew there is
                -- silent data loss or silent duplication, with nothing on the
                -- wire to say so. The conversion yields a plain TIMESTAMP
                -- already in UTC, which to_char formats verbatim. Pinned
                -- repo-wide by server/tests/procedures_timezone.rs.
                v_parts := v_parts || jsonb_build_object(
                    'name', v_row.name,
                    'lastOffset', v_row.last_offset,
                    'logStart', v_row.log_start,
                    'lastWriteAt', to_char(v_row.last_write_at AT TIME ZONE 'UTC',
                                           'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
                v_count := v_count + 1;
                v_next  := 'n|' || v_row.name;
            END LOOP;
        ELSE
            -- WHAT MOVED, idx_log_partitions_queue_write.
            --
            -- The lower bound is the LATER of the caller's `since` and its
            -- cursor's timestamp, so the index range scan starts at the cursor
            -- instead of at `since` and re-walking every page already served.
            -- The row comparison then drops the exact row the cursor names and
            -- everything ordered before it within the same second.
            v_lo := GREATEST(v_since, COALESCE(v_cur_ts, v_since));
            FOR v_row IN
                SELECT p.name, p.last_offset, p.log_start, p.last_write_at
                FROM queen.log_partitions p
                WHERE p.queue_id = v_qid
                  AND p.last_write_at >= v_lo
                  AND (v_cur_ts IS NULL
                       OR (p.last_write_at, p.name) > (v_cur_ts, v_cur_name))
                ORDER BY p.last_write_at, p.name
                LIMIT v_lim
            LOOP
                v_parts := v_parts || jsonb_build_object(
                    'name', v_row.name,
                    'lastOffset', v_row.last_offset,
                    'logStart', v_row.log_start,
                    'lastWriteAt', to_char(v_row.last_write_at AT TIME ZONE 'UTC',
                                           'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
                v_count := v_count + 1;
                -- Epoch MICROSECONDS, so the cursor is exact at the column's
                -- own resolution — see the header.
                v_next  := 't|'
                        || trunc(extract(epoch FROM v_row.last_write_at) * 1000000)::int8::text
                        || '|' || v_row.name;
            END LOOP;
        END IF;

        -- `next` is non-null only when this page FILLED, i.e. there may be
        -- more. A short page is the end of the sweep, and the caller's next
        -- question is a fresh one (a new `since`, or a new enumeration). The
        -- cost of the rule is one extra empty round trip when the partition
        -- count is an exact multiple of the limit; the alternative — a
        -- look-ahead row — buys nothing a caller can act on.
        IF v_count < v_lim THEN
            v_next := NULL;
        END IF;

        v_entries := v_entries || jsonb_build_object(
            'queue', p_queues[v_i],
            'partitions', v_parts,
            'next', v_next);
    END LOOP;

    RETURN jsonb_build_object(
        'safeTime', to_char(v_safe AT TIME ZONE 'UTC',
                            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
        'safeTimeDegraded', v_degraded,
        'entries', v_entries);
END;
$$;
