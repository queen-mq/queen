-- Top Postgres consumers by total time. Run on the CELL:
--   docker exec -i cell-pg psql -U queenadm -p 54987 -d queen -f - < pgtop.sql
--
-- What to look for on the 2026-08-23 rerun, in order:
--
--  1. log_queue_stats_all_v1  -- the reconciler's per-queue stats aggregate.
--     CORRECTED 2026-08-23: it IS tenant-scoped (WHERE q.tenant_id = p_tenant);
--     an earlier note here claimed otherwise. The cost is not a missing filter,
--     it is the LATERAL:
--         LEFT JOIN LATERAL (SELECT count(*) FROM queen.log_segments s
--                            WHERE s.partition_id = p.id) sc ON true
--     a correlated count run ONCE PER PARTITION, each counting that partition's
--     accumulated segments. Work per call = sum(segments per partition) = the
--     tenant's WHOLE segment count, so it grows with stored messages while
--     partition count stays pinned. Measured on the 2026-08-23 soak:
--     338 ms -> 635 ms -> 2 204 ms as segments went ~1M -> 2M -> 9M, reaching
--     11% of all database time. Segments stop growing only when retention
--     starts deleting (1 day free / 3 dev / 7 pro).
--
--  2. INSERT INTO queen_proxy.queues  -- registry admit. BEFORE the fix this
--     was one synchronous row per new (queue, partition) on the request path
--     (743,149 calls). AFTER, it should be a coalesced UNNEST upsert roughly
--     once per QUEEN_PROXY_REGISTRY_PERSIST_MS (1 s default), so call count
--     should collapse by orders of magnitude.
--
--  3. UPDATE queen_proxy.api_keys SET last_used_at  -- BEFORE: 103,726 calls,
--     a thundering herd at the 30 s key-TTL with no single-flight. AFTER, it
--     should be one batched statement per QUEEN_PROXY_KEY_TOUCH_MS (10 s).
--
--  4. log_refresh_all_stats_v1  -- the BROKER's periodic sweep, unrelated to
--     the proxy work. Measured 3,707 ms per call at 733k partitions (it was
--     ~1 s at 54k). Expect it to still be there; it is the next thing to fix.
--
--  5. The data path itself -- the pop SELECT, log_push_multi_v1,
--     log_ack_multi_v1. Previously 47% / 30% / 5%. If the proxy work landed,
--     these should now be a LARGER share of a SMALLER total.

-- pg_stat_statements COLLECTS cluster-wide but its VIEW exists only where
-- CREATE EXTENSION ran (here: the `queen` database, not `queen_proxy`). Querying
-- from `queen` therefore still sees the proxy's statements against queen_proxy —
-- but only if dbid is resolved, otherwise broker cost and proxy cost are
-- indistinguishable in the output. Hence the join to pg_database.
SELECT round(100*total_exec_time/NULLIF(sum(total_exec_time) OVER (),0))::int AS pct,
       d.datname AS db,
       s.calls,
       round(s.mean_exec_time::numeric,2) AS mean_ms,
       round((s.total_exec_time/1000)::numeric,1) AS total_s,
       left(regexp_replace(s.query, '\s+', ' ', 'g'), 58) AS q
FROM pg_stat_statements s
LEFT JOIN pg_database d ON d.oid = s.dbid
ORDER BY s.total_exec_time DESC
LIMIT 14;

-- The four specific answers this rerun exists to produce.
\echo
\echo '=== the four watched statements (yesterday -> today) ==='
SELECT left(regexp_replace(s.query,'\s+',' ','g'),46) AS q,
       s.calls, round(s.mean_exec_time::numeric,2) AS mean_ms,
       round((s.total_exec_time/1000)::numeric,1) AS total_s
FROM pg_stat_statements s
WHERE s.query ILIKE '%INSERT INTO queen_proxy.queues%'      -- was 743,149 calls
   OR s.query ILIKE '%last_used_at%'                        -- was 103,726 calls
   OR s.query ILIKE '%log_queue_stats_all_v1%'              -- never measured
   OR s.query ILIKE '%log_refresh_all_stats_v1%'            -- was 3,707 ms/call
ORDER BY s.total_exec_time DESC;
