SELECT 'wal_records' k, wal_records::text v FROM pg_stat_wal
UNION ALL SELECT 'wal_fpi', wal_fpi::text FROM pg_stat_wal
UNION ALL SELECT 'wal_bytes', wal_bytes::text FROM pg_stat_wal
UNION ALL SELECT 'wal_writes', coalesce(sum(writes),0)::text FROM pg_stat_io WHERE object='wal' AND context='normal'
UNION ALL SELECT 'wal_write_time_ms', round(coalesce(sum(write_time),0)::numeric,1)::text FROM pg_stat_io WHERE object='wal' AND context='normal'
UNION ALL SELECT 'wal_fsyncs', coalesce(sum(fsyncs),0)::text FROM pg_stat_io WHERE object='wal' AND context='normal'
UNION ALL SELECT 'wal_fsync_time_ms', round(coalesce(sum(fsync_time),0)::numeric,1)::text FROM pg_stat_io WHERE object='wal' AND context='normal'
UNION ALL SELECT 'xact_commit', xact_commit::text FROM pg_stat_database WHERE datname='postgres'
UNION ALL SELECT 'stats_reset', stats_reset::text FROM pg_stat_wal;
