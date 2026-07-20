-- ============================================================================
-- PostgreSQL Statistics & Diagnostics
-- ============================================================================
-- Provides visibility into PostgreSQL internals for debugging and monitoring.
-- Includes cache hit ratios, dead tuples, slow queries, HOT updates, etc.
-- ============================================================================

-- Main function: Get comprehensive Postgres stats
CREATE OR REPLACE FUNCTION queen.get_postgres_stats_v1()
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_database_cache JSONB;
    v_table_cache JSONB;
    v_index_cache JSONB;
    v_cache_summary JSONB;
    v_dead_tuples JSONB;
    v_hot_updates JSONB;
    v_active_queries JSONB;
    v_autovacuum_status JSONB;
    v_buffer_config JSONB;
    v_buffer_usage JSONB;
    v_table_sizes JSONB;
BEGIN
    -- Database-level cache hit ratio
    SELECT jsonb_build_object(
        'database', datname,
        'diskReads', blks_read,
        'cacheHits', blks_hit,
        'cacheHitRatio', ROUND(blks_hit::numeric / NULLIF(blks_hit + blks_read, 0) * 100, 2)
    ) INTO v_database_cache
    FROM pg_stat_database
    WHERE datname = current_database();

    -- Per-table cache hit ratios (queen schema)
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'schema', schemaname,
            'table', relname,
            'diskReads', heap_blks_read,
            'cacheHits', heap_blks_hit,
            'cacheHitRatio', ROUND(heap_blks_hit::numeric / NULLIF(heap_blks_hit + heap_blks_read, 0) * 100, 2)
        ) ORDER BY heap_blks_read DESC
    ), '[]'::jsonb) INTO v_table_cache
    FROM pg_statio_user_tables
    WHERE schemaname = 'queen';

    -- Per-index cache hit ratios (queen schema, top 20)
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'schema', schemaname,
            'table', relname,
            'index', indexrelname,
            'diskReads', idx_blks_read,
            'cacheHits', idx_blks_hit,
            'cacheHitRatio', ROUND(idx_blks_hit::numeric / NULLIF(idx_blks_hit + idx_blks_read, 0) * 100, 2)
        ) ORDER BY idx_blks_read DESC
    ), '[]'::jsonb) INTO v_index_cache
    FROM pg_statio_user_indexes
    WHERE schemaname = 'queen'
    LIMIT 20;

    -- Overall cache summary (tables vs indexes)
    SELECT jsonb_build_object(
        'tables', (
            SELECT jsonb_build_object(
                'diskReads', SUM(heap_blks_read),
                'cacheHits', SUM(heap_blks_hit),
                'hitRatio', ROUND(SUM(heap_blks_hit)::numeric / NULLIF(SUM(heap_blks_hit) + SUM(heap_blks_read), 0) * 100, 2)
            )
            FROM pg_statio_user_tables
            WHERE schemaname = 'queen'
        ),
        'indexes', (
            SELECT jsonb_build_object(
                'diskReads', SUM(idx_blks_read),
                'cacheHits', SUM(idx_blks_hit),
                'hitRatio', ROUND(SUM(idx_blks_hit)::numeric / NULLIF(SUM(idx_blks_hit) + SUM(idx_blks_read), 0) * 100, 2)
            )
            FROM pg_statio_user_indexes
            WHERE schemaname = 'queen'
        )
    ) INTO v_cache_summary;

    -- Dead tuples / vacuum status
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'schema', schemaname,
            'table', relname,
            'deadTuples', n_dead_tup,
            'liveTuples', n_live_tup,
            'deadPercentage', ROUND(n_dead_tup::numeric / NULLIF(n_live_tup, 0) * 100, 2),
            'lastVacuum', to_char(last_vacuum, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
            'lastAutovacuum', to_char(last_autovacuum, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
        ) ORDER BY n_dead_tup DESC
    ), '[]'::jsonb) INTO v_dead_tuples
    FROM pg_stat_user_tables
    WHERE schemaname = 'queen'
      AND n_dead_tup > 0
    LIMIT 15;

    -- HOT update statistics (for queen tables)
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'schema', schemaname,
            'table', relname,
            'totalUpdates', n_tup_upd,
            'hotUpdates', n_tup_hot_upd,
            'hotUpdatePercentage', ROUND(n_tup_hot_upd::numeric / NULLIF(n_tup_upd, 0) * 100, 2)
        ) ORDER BY n_tup_upd DESC
    ), '[]'::jsonb) INTO v_hot_updates
    FROM pg_stat_user_tables
    WHERE schemaname = 'queen'
      AND n_tup_upd > 0
    LIMIT 15;

    -- Active/slow queries (> 1 second)
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'pid', pid,
            'duration', EXTRACT(EPOCH FROM (now() - query_start)),
            'state', state,
            'query', LEFT(query, 200),
            'waitEventType', wait_event_type,
            'waitEvent', wait_event
        ) ORDER BY query_start
    ), '[]'::jsonb) INTO v_active_queries
    FROM pg_stat_activity
    WHERE state != 'idle'
      AND query_start < now() - interval '1 second'
      AND pid != pg_backend_pid()
    LIMIT 10;

    -- Autovacuum status (tables with many dead tuples)
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'table', relname,
            'lastAutovacuum', to_char(last_autovacuum, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
            'deadTuples', n_dead_tup,
            'autovacuumCount', autovacuum_count,
            'autoanalyzeCount', autoanalyze_count
        ) ORDER BY n_dead_tup DESC
    ), '[]'::jsonb) INTO v_autovacuum_status
    FROM pg_stat_user_tables
    WHERE schemaname = 'queen'
      AND n_dead_tup > 1000
    LIMIT 10;

    -- Buffer/memory configuration
    SELECT jsonb_build_object(
        'sharedBuffers', setting,
        'sharedBuffersSize', pg_size_pretty(setting::bigint * 8192)
    ) INTO v_buffer_config
    FROM pg_settings
    WHERE name = 'shared_buffers';

    -- Table sizes
    SELECT COALESCE(jsonb_agg(
        jsonb_build_object(
            'table', relname,
            'totalSize', pg_size_pretty(pg_total_relation_size(c.oid)),
            'totalSizeBytes', pg_total_relation_size(c.oid),
            'tableSize', pg_size_pretty(pg_relation_size(c.oid)),
            'tableSizeBytes', pg_relation_size(c.oid),
            'indexSize', pg_size_pretty(pg_indexes_size(c.oid)),
            'indexSizeBytes', pg_indexes_size(c.oid)
        ) ORDER BY pg_total_relation_size(c.oid) DESC
    ), '[]'::jsonb) INTO v_table_sizes
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'queen'
      AND c.relkind = 'r'
    LIMIT 20;

    -- Try to get buffer cache usage (requires pg_buffercache extension)
    BEGIN
        SELECT COALESCE(jsonb_agg(
            jsonb_build_object(
                'object', relname,
                'bufferedSize', pg_size_pretty(count * 8192),
                'bufferedBytes', count * 8192,
                'percentOfCache', ROUND(100.0 * count / (SELECT setting FROM pg_settings WHERE name = 'shared_buffers')::integer, 2)
            ) ORDER BY count DESC
        ), '[]'::jsonb) INTO v_buffer_usage
        FROM (
            SELECT c.relname, count(*) as count
            FROM pg_buffercache b
            JOIN pg_class c ON b.relfilenode = pg_relation_filenode(c.oid)
            WHERE c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'queen')
            GROUP BY c.relname
            ORDER BY count(*) DESC
            LIMIT 20
        ) t;
    EXCEPTION WHEN undefined_table THEN
        v_buffer_usage := '[]'::jsonb;
    END;

    RETURN jsonb_build_object(
        'timestamp', to_char(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'database', current_database(),
        'databaseCache', COALESCE(v_database_cache, '{}'::jsonb),
        'tableCache', COALESCE(v_table_cache, '[]'::jsonb),
        'indexCache', COALESCE(v_index_cache, '[]'::jsonb),
        'cacheSummary', COALESCE(v_cache_summary, '{}'::jsonb),
        'deadTuples', COALESCE(v_dead_tuples, '[]'::jsonb),
        'hotUpdates', COALESCE(v_hot_updates, '[]'::jsonb),
        'activeQueries', COALESCE(v_active_queries, '[]'::jsonb),
        'autovacuumStatus', COALESCE(v_autovacuum_status, '[]'::jsonb),
        'bufferConfig', COALESCE(v_buffer_config, '{}'::jsonb),
        'bufferUsage', COALESCE(v_buffer_usage, '[]'::jsonb),
        'tableSizes', COALESCE(v_table_sizes, '[]'::jsonb)
    );
END;
$$;

-- Grant permissions
GRANT EXECUTE ON FUNCTION queen.get_postgres_stats_v1() TO PUBLIC;

