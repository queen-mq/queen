-- ============================================================================
-- Log engine (18-log-engine.md §4 + §1 push refinement) — push.
--
--   queen.log_push_one_v1    one segment for one partition (also the shared
--                            allocator body for 005_log_ack's
--                            log_transaction_wire_v1: keep ONE allocator code
--                            path).
--   queen.log_push_multi_v1  cross-partition bundling (the fusion axis): N
--                            ready, DISJOINT partitions' segments in ONE
--                            transaction = one commit / one fsync.
--   queen.log_segment_at_v1  covering-segment fetch for the broker's rare
--                            duplicate-mid resolution.
--
-- NO SAVEPOINTS — the load-bearing refinement over the seg engine. The old
-- v2 push detected duplicates via a unique-index violation, which forces a
-- per-segment subtransaction (savepoint + subxid) to contain the rollback.
-- Here dedup-enabled pushes PROBE BEFORE ALLOCATING, under the partition row
-- lock: a duplicate returns {"status":"duplicate",...} having written NOTHING
-- (no rollback needed), and only a clean probe proceeds to the allocator
-- UPDATE + inserts. Dedup-off pushes skip the probe SELECT entirely and go
-- straight to the single UPDATE .. RETURNING fast path. Per-segment duplicate
-- results in a bundle therefore coexist with other segments' commits without
-- subtransactions.
--
-- Duplicate detection is exact and window-bounded: the broker computes
-- xxh3_128 per message (16B big-endian, §3 — SQL never hashes) and vouches a
-- watermark p_verified per segment: it has already checked the incoming hashes
-- against every message in (-∞, p_verified] (its dedup cache, §5), so SQL only
-- probes log_txns rows overlapping (GREATEST(p_verified, txns_start-1),
-- last_offset], time-capped by the queue's dedup window. p_verified = -1 means
-- "no broker cache — probe the whole window" (always correct; the cache is an
-- optimization, never a correctness dependency).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- log_push_one_v1: push one segment (p_msg_count frames packed+zstd'd in
-- p_blob, hashes = 16*p_msg_count bytes in frame order).
--
-- Returns {"status":"queued","baseOffset":B,"createdAt":"..."}
--      or {"status":"duplicate","dups":[{"i":k,"off":O}, ...]} — k = incoming
--         frame index, O = the ORIGINAL occurrence's absolute offset (the
--         broker resolves the canonical mid from it via log_segment_at_v1).
--
-- p_pid/p_window are resolved from the names only when not supplied — the
-- bundling caller (log_push_multi_v1, and 005_log_ack's transaction wire) has already
-- resolved them and must not pay a re-resolve per segment (the nested-lookup
-- CPU the seg v2 rework eliminated).
--
-- Lock protocol (dedup ON): SELECT .. FOR UPDATE on the partition row FIRST,
-- then probe, then allocate. The row lock is the per-partition write
-- serializer, so probe-then-allocate is race-free: no concurrent pusher can
-- commit new rows into the probed span while we hold the lock. (Dedup OFF: the
-- allocator UPDATE itself takes the same lock; nothing to probe.)
-- ----------------------------------------------------------------------------
-- Track B (§5): p_tenant scopes queue resolution + auto-create. Added last with a
-- DEFAULT so any caller that omits it lands on the default tenant (OSS behaviour).
CREATE OR REPLACE FUNCTION queen.log_push_one_v1(
    p_queue     TEXT,
    p_partition TEXT,
    p_msg_count INT,
    p_hashes    BYTEA,
    p_verified  BIGINT,
    p_blob      BYTEA,
    p_pid       UUID DEFAULT NULL,
    p_window    INT  DEFAULT NULL,
    p_tenant    UUID DEFAULT '00000000-0000-0000-0000-000000000001'
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_pid        UUID := p_pid;
    v_window     INT  := p_window;
    v_last       BIGINT;
    v_txns_start BIGINT;
    v_from       BIGINT;
    v_base       BIGINT;
    v_now        TIMESTAMPTZ;
    v_dups       JSONB;
BEGIN
    -- The hash blob is load-bearing beyond dedup (ack-by-hash resolves through
    -- it), so a stride mismatch must fail loudly, not mis-address silently.
    IF p_msg_count IS NULL OR p_msg_count < 1
       OR octet_length(COALESCE(p_hashes, ''::bytea)) <> p_msg_count * 16 THEN
        RAISE EXCEPTION 'QPUSH bad segment: msg_count=%, hashes=% bytes (want 16/msg)',
            p_msg_count, octet_length(COALESCE(p_hashes, ''::bytea));
    END IF;

    IF v_pid IS NULL OR v_window IS NULL THEN
        -- Queue identity is now the queen.queues id: log_partitions.queue_id
        -- references it directly (log_queues is gone).
        SELECT COALESCE(v_pid, p.id), COALESCE(v_window, q.dedup_window_seconds)
        INTO v_pid, v_window
        FROM queen.log_partitions p JOIN queen.queues q ON q.id = p.queue_id
        WHERE q.name = p_queue AND p.name = p_partition AND q.tenant_id = p_tenant;

        IF v_pid IS NULL THEN
            -- First contact: provision queue + partition. Kept inside the
            -- missing branch only — an unconditional per-push ON CONFLICT
            -- would reintroduce the ShareLock convoy on queen.queues the seg
            -- engine's header warned about. Track B: the queue rows carry the
            -- resolved tenant, and the ON CONFLICT targets (tenant_id, name).
            -- ONE insert: queen.queues is the only queue table, deriving
            -- namespace/task from the dotted name exactly like the retired
            -- rows-era push did,
            -- so push-only queues are visible to namespace/task discovery pops
            -- and pick up retry/DLQ/retention config.
            -- ON CONFLICT preserves a /configure-created row.
            INSERT INTO queen.queues (tenant_id, name, namespace, task)
            VALUES (p_tenant, p_queue,
                    split_part(p_queue, '.', 1),
                    CASE WHEN position('.' in p_queue) > 0 THEN split_part(p_queue, '.', 2) ELSE '' END)
            ON CONFLICT (tenant_id, name) DO NOTHING;
            INSERT INTO queen.log_partitions (queue_id, name)
            SELECT q.id, p_partition FROM queen.queues q WHERE q.name = p_queue AND q.tenant_id = p_tenant
            ON CONFLICT (queue_id, name) DO NOTHING;

            SELECT p.id, q.dedup_window_seconds INTO v_pid, v_window
            FROM queen.log_partitions p JOIN queen.queues q ON q.id = p.queue_id
            WHERE q.name = p_queue AND p.name = p_partition AND q.tenant_id = p_tenant;
        END IF;

        IF v_pid IS NULL THEN
            -- Empty/malformed names never provision; fail loudly rather than
            -- return a misaligned nothing.
            RAISE EXCEPTION 'QPUSH unknown queue/partition %/%', p_queue, p_partition;
        END IF;
    END IF;

    IF v_window > 0 THEN
        -- ============ DEDUP-ON: probe BEFORE allocate, under the lock ========
        -- Row lock FIRST: freezes last_offset and shuts out concurrent writers
        -- for the probe→allocate span. (Re-locking a row this transaction
        -- already holds — the multi pre-lock — is a no-op.)
        SELECT last_offset, txns_start INTO v_last, v_txns_start
        FROM queen.log_partitions WHERE id = v_pid
        FOR UPDATE;

        -- created_at stamp taken while the serializer is held → monotone per
        -- partition in commit order (PUSHSER invariant: a later pusher can
        -- only stamp after this transaction commits and releases the lock).
        -- Also the probe's window cap, so probe and stamp agree on "now".
        v_now := clock_timestamp();

        -- Probe span (v_from, last_offset]: the broker vouches for
        -- (-∞, p_verified] and rows below txns_start are purged (nothing to
        -- compare against — redelivery of a hash older than the txns window is
        -- accepted, identical to the seg engine's window semantics).
        v_from := GREATEST(COALESCE(p_verified, -1), v_txns_start - 1);
        IF v_last > v_from THEN
            -- One incoming hash can match several historical rows (e.g. pushed
            -- while dedup was off); MIN picks the ORIGINAL occurrence per §4.
            SELECT jsonb_agg(jsonb_build_object('i', d.idx, 'off', d.off) ORDER BY d.idx)
            INTO v_dups
            FROM (
                SELECT ih.idx AS idx, MIN(t.base_offset + th.idx) AS off
                FROM queen.log_txns t
                CROSS JOIN LATERAL queen.log_unnest_hashes(t.hashes) th
                JOIN queen.log_unnest_hashes(p_hashes) ih ON ih.h = th.h
                WHERE t.partition_id = v_pid
                  AND t.end_offset > v_from
                  AND t.created_at >= v_now - make_interval(secs => v_window)
                GROUP BY ih.idx
            ) d;

            IF v_dups IS NOT NULL THEN
                -- NOTHING was written (no allocator bump, no inserts): the
                -- duplicate verdict needs no rollback, hence no savepoint —
                -- in a bundle the other segments' writes commit untouched.
                RETURN jsonb_build_object('status', 'duplicate', 'dups', v_dups);
            END IF;
        END IF;

        -- Clean probe → allocate. Row already locked, so this UPDATE cannot
        -- wait. last_write_at quantization: the column is indexed
        -- (idx_log_partitions_queue_write), so bumping it every push would
        -- make every allocator update non-HOT; at most one real change per
        -- second per partition keeps ~all of them HOT. The pop candidate scan
        -- tolerates 2 minutes of slack, so 1s staleness is absorbed.
        -- Same discipline for the two retention work-list watermarks
        -- (001_log_schema): they are ALSO indexed, so the write has to be
        -- byte-identical in steady state — COALESCE returns the existing value
        -- unchanged, and only the rare EMPTY -> NON-EMPTY transition (this push
        -- creating the partition's first live row) actually touches the index.
        -- Sub-millisecond skew against the segment's own created_at is
        -- deliberate and harmless in both directions: a value a hair LATE hides
        -- the partition for at most one cycle, a hair EARLY costs one no-op
        -- step call, and both are measured in microseconds against cutoffs
        -- measured in hours.
        UPDATE queen.log_partitions SET
            last_offset    = last_offset + p_msg_count,
            oldest_live_at = COALESCE(oldest_live_at, clock_timestamp()),
            oldest_txn_at  = COALESCE(oldest_txn_at,  clock_timestamp()),
            last_write_at  = CASE WHEN clock_timestamp() - last_write_at > interval '1 second'
                                  THEN clock_timestamp() ELSE last_write_at END
        WHERE id = v_pid
        RETURNING last_offset INTO v_last;
        v_base := v_last - p_msg_count + 1;
    ELSE
        -- ============ DEDUP-OFF: single UPDATE .. RETURNING fast path ========
        -- No probe SELECT at all; the allocator UPDATE takes the row lock and
        -- is the whole serialization story (correctness-free work the seg v1
        -- engine could not skip, preserved from seg v2). The two watermark
        -- COALESCEs are byte-identical for a partition that already holds data
        -- — see the dedup-ON branch above for the full rationale; the two
        -- branches must stay textually identical here.
        UPDATE queen.log_partitions SET
            last_offset    = last_offset + p_msg_count,
            oldest_live_at = COALESCE(oldest_live_at, clock_timestamp()),
            oldest_txn_at  = COALESCE(oldest_txn_at,  clock_timestamp()),
            last_write_at  = CASE WHEN clock_timestamp() - last_write_at > interval '1 second'
                                  THEN clock_timestamp() ELSE last_write_at END
        WHERE id = v_pid
        RETURNING last_offset INTO v_last;
        IF v_last IS NULL THEN
            RAISE EXCEPTION 'QPUSH partition % vanished mid-push', v_pid;
        END IF;
        v_base := v_last - p_msg_count + 1;
        -- Stamp AFTER the allocator UPDATE (= after the row lock): PUSHSER
        -- invariant, created_at monotone per partition in commit order.
        v_now := clock_timestamp();
    END IF;

    INSERT INTO queen.log_segments (partition_id, base_offset, end_offset, created_at, blob)
    VALUES (v_pid, v_base, v_base + p_msg_count - 1, v_now, p_blob);

    -- log_txns is ALWAYS written (dedup on or off): ack-by-hash resolution
    -- (005_log_ack) depends on it. Purged by its own watermark (txns_start), so its
    -- steady-state size is O(rate × window), independent of retention/backlog.
    INSERT INTO queen.log_txns (partition_id, base_offset, end_offset, created_at, hashes)
    VALUES (v_pid, v_base, v_base + p_msg_count - 1, v_now, p_hashes);

    RETURN jsonb_build_object(
        'status', 'queued',
        'baseOffset', v_base,
        'createdAt', to_char(v_now, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
END;
$$;

-- ----------------------------------------------------------------------------
-- log_push_multi_v1: bundle N ready, DISJOINT partitions' segments into ONE
-- transaction (one commit / one fsync). Typed parallel arrays, index-aligned;
-- zero jsonb parsed on the hot path (the seg v2 lesson: the broker used to
-- bind $1::text::jsonb and PG parsed the whole payload every call).
--
-- Returns a JSONB array of per-segment results in INPUT order (index-aligned
-- with the arrays) so the broker can demux each verdict back to that
-- partition's contributing push requests.
--
-- Deadlock-safety: ONE set-based pre-lock takes every partition row lock in
-- ascending log_partitions.id order — the global total lock order shared with
-- acks (005_log_ack's transaction wire) and retention (006_log_maintenance) —
-- before any write. The
-- LockRows node sits above the Sort, so rows are locked in ORDER BY order.
-- ----------------------------------------------------------------------------
-- Track B (§5): p_tenants is index-aligned with the other arrays — ONE tenant per
-- segment, so a single fused bundle can span tenants (the broker fusion groups by
-- (tenant, queue, partition), so in practice a bundle is single-tenant, but the
-- array form keeps the SP correct if it ever isn't). Added last with a DEFAULT so
-- a NULL/short caller degrades every segment to the default tenant.
CREATE OR REPLACE FUNCTION queen.log_push_multi_v1(
    p_queues     TEXT[],
    p_partitions TEXT[],
    p_msg_counts INT[],
    p_hashes     BYTEA[],
    p_verified   INT8[],
    p_blobs      BYTEA[],
    p_tenants    UUID[] DEFAULT NULL
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_n        INT;
    v_missing  INT;
    v_resolved INT := 0;
    v_now      TIMESTAMPTZ;
    v_rec      RECORD;
    v_results  JSONB[];
    v_tenants  UUID[];
BEGIN
    v_n := COALESCE(array_length(p_queues, 1), 0);
    IF v_n = 0 THEN
        RETURN '[]'::jsonb;
    END IF;
    IF array_length(p_partitions, 1) IS DISTINCT FROM v_n
       OR array_length(p_msg_counts, 1) IS DISTINCT FROM v_n
       OR array_length(p_hashes, 1)     IS DISTINCT FROM v_n
       OR array_length(p_verified, 1)   IS DISTINCT FROM v_n
       OR array_length(p_blobs, 1)      IS DISTINCT FROM v_n THEN
        RAISE EXCEPTION 'QMULTI misaligned arrays (% queues)', v_n;
    END IF;
    -- A NULL/short p_tenants (legacy caller) means "all default tenant"; a present
    -- one must be exactly aligned. Materialize a per-index tenant array either way.
    IF p_tenants IS NULL THEN
        v_tenants := array_fill('00000000-0000-0000-0000-000000000001'::uuid, ARRAY[v_n]);
    ELSIF array_length(p_tenants, 1) IS DISTINCT FROM v_n THEN
        RAISE EXCEPTION 'QMULTI misaligned tenants (% vs % queues)',
            COALESCE(array_length(p_tenants, 1), 0), v_n;
    ELSE
        v_tenants := p_tenants;
    END IF;

    -- Steady-state provisioning skip: one cheap array-vs-index existence
    -- count; only when something is missing run the two ON CONFLICT
    -- provisioning statements. No per-call ShareLock churn on queen.queues.
    -- Malformed (empty) queue names are skipped, exactly as the seg engine;
    -- they then fail the alignment guard below, loudly.
    -- Queue identity is now the queen.queues id (log_queues is gone).
    SELECT count(*) INTO v_missing
    FROM unnest(p_queues, p_partitions, v_tenants) AS u(queue, partition, tenant)
    LEFT JOIN queen.queues q ON q.name = u.queue AND q.tenant_id = u.tenant
    LEFT JOIN queen.log_partitions p ON p.queue_id = q.id AND p.name = u.partition
    WHERE p.id IS NULL;

    IF v_missing > 0 THEN
        -- Every provisioning INSERT is ORDERED on its unique key (2026-07-23):
        -- ON CONFLICT DO NOTHING acquires speculative locks in ROW order, and
        -- two bundles creating overlapping partition sets in different orders
        -- deadlock/convoy exactly like unordered row locks would (measured
        -- during mass creation: xid convoys wedging the whole pool). Same
        -- canonical-order discipline as the FOR UPDATE pre-lock below.
        -- ONE queue insert: queen.queues is the only queue table now. See
        -- log_push_one_v1's provisioning branch for the full rationale.
        INSERT INTO queen.queues (tenant_id, name, namespace, task)
        SELECT DISTINCT u.tenant, u.queue,
               split_part(u.queue, '.', 1),
               CASE WHEN position('.' in u.queue) > 0 THEN split_part(u.queue, '.', 2) ELSE '' END
        FROM unnest(p_queues, v_tenants) AS u(queue, tenant)
        WHERE COALESCE(u.queue, '') <> ''
        ORDER BY 1, 2
        ON CONFLICT (tenant_id, name) DO NOTHING;

        INSERT INTO queen.log_partitions (queue_id, name)
        SELECT DISTINCT q.id, u.partition
        FROM unnest(p_queues, p_partitions, v_tenants) AS u(queue, partition, tenant)
        JOIN queen.queues q ON q.name = u.queue AND q.tenant_id = u.tenant
        ORDER BY 1, 2
        ON CONFLICT (queue_id, name) DO NOTHING;
    END IF;

    -- Deadlock-safe pre-lock (see header). Set-based: one statement, not a
    -- per-partition loop.
    PERFORM 1
    FROM unnest(p_queues, p_partitions, v_tenants) AS u(queue, partition, tenant)
    JOIN queen.queues q ON q.name = u.queue AND q.tenant_id = u.tenant
    JOIN queen.log_partitions p ON p.queue_id = q.id AND p.name = u.partition
    ORDER BY p.id
    FOR UPDATE OF p;

    -- Bundle timestamp floor, stamped once AFTER the locks (PUSHSER: every
    -- lock is already held, so no earlier-committing pusher can stamp later
    -- than this). log_push_one_v1 re-stamps per segment — each stamp is also
    -- taken under that partition's held lock, so per-partition monotonicity
    -- holds either way; the per-segment stamp just tightens createdAt.
    v_now := clock_timestamp();

    -- Per segment in ascending partition id order (locks already held, so the
    -- order here is about determinism, not correctness). pid/window resolved
    -- ONCE here and passed down — log_push_one_v1 must not re-resolve (the
    -- nested re-resolve was part of the seg push CPU regression). Results are
    -- scattered by input ordinal, so processing order never disturbs
    -- alignment. A duplicate verdict wrote nothing (probe-before-allocate),
    -- so it coexists with the other segments' commits — NO savepoints.
    v_results := array_fill(NULL::jsonb, ARRAY[v_n]);
    FOR v_rec IN
        SELECT u.ord, u.queue, u.partition, u.tenant,
               p.id AS pid, q.dedup_window_seconds AS window
        FROM unnest(p_queues, p_partitions, v_tenants)
             WITH ORDINALITY AS u(queue, partition, tenant, ord)
        JOIN queen.queues q ON q.name = u.queue AND q.tenant_id = u.tenant
        JOIN queen.log_partitions p ON p.queue_id = q.id AND p.name = u.partition
        ORDER BY p.id
    LOOP
        v_results[v_rec.ord] := queen.log_push_one_v1(
            v_rec.queue,
            v_rec.partition,
            p_msg_counts[v_rec.ord],
            p_hashes[v_rec.ord],
            COALESCE(p_verified[v_rec.ord], -1),
            p_blobs[v_rec.ord],
            v_rec.pid,
            v_rec.window,
            v_rec.tenant);
        v_resolved := v_resolved + 1;
    END LOOP;

    -- Every input ordinal must resolve to exactly one result. A short set
    -- would be a silent demux misalignment — fail the whole bundle loudly
    -- instead (same guard as seg v2).
    IF v_resolved <> v_n THEN
        RAISE EXCEPTION 'QMULTI resolved % of % segments (empty/unknown queue or partition)',
            v_resolved, v_n;
    END IF;

    RETURN to_jsonb(v_results);
END;
$$;

-- ----------------------------------------------------------------------------
-- log_segment_at_v1: fetch the segment COVERING absolute offset p_off. Rare
-- path: the broker resolves a duplicate's original message id by unpacking
-- frame (p_off - r_base) of r_blob in Rust (§4). Zero rows when retention
-- already deleted the covering segment (broker reports the zero uuid) or the
-- offset sits in a retention gap / beyond the tail.
--
-- Shape: one descending PK probe for the greatest base_offset <= p_off, then
-- the covering check on that SINGLE candidate — ranges are disjoint, so if the
-- nearest-below segment does not cover p_off, no earlier one can. (Filtering
-- end_offset inside the LIMIT would let a miss walk the whole partition head.)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION queen.log_segment_at_v1(p_pid UUID, p_off BIGINT)
RETURNS TABLE (r_base BIGINT, r_end BIGINT, r_created TIMESTAMPTZ, r_blob BYTEA)
LANGUAGE sql STABLE
AS $$
    SELECT c.base_offset, c.end_offset, c.created_at, c.blob
    FROM (
        SELECT s.base_offset, s.end_offset, s.created_at, s.blob
        FROM queen.log_segments s
        WHERE s.partition_id = p_pid
          AND s.base_offset <= p_off
        ORDER BY s.base_offset DESC
        LIMIT 1
    ) c
    WHERE c.end_offset >= p_off
$$;
