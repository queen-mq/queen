\pset border 2
select backend_type, object, context, writes, write_time, fsyncs, fsync_time
from pg_stat_io where object='wal' and (writes>0 or fsyncs>0 or write_time>0);
