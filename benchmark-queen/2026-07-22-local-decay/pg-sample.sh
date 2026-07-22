#!/usr/bin/env bash
# pg-sample.sh <container> <outdir> — 1s wait-event samples + 10s table growth samples.
# Usage: ./pg-sample.sh queen-perf-pg16 out/e1 & (kill to stop)
set -u
C="$1"; OUT="$2"; mkdir -p "$OUT"
PSQL="docker exec $C psql -U postgres -d queen -tA -F,"

# 10s cadence: relation + TOAST + index sizes, dead tuples, autovacuum counters, xact rate
sizes() {
  while true; do
    TS=$(date +%s)
    $PSQL -c "
      SELECT '$TS', c.relname,
             pg_relation_size(c.oid),
             COALESCE(pg_relation_size(c.reltoastrelid),0),
             COALESCE((SELECT pg_total_relation_size(i.indexrelid) FROM pg_index i WHERE i.indrelid=c.oid LIMIT 1),0),
             COALESCE(s.n_live_tup,0), COALESCE(s.n_dead_tup,0),
             COALESCE(s.autovacuum_count,0), COALESCE(s.vacuum_count,0)
      FROM pg_class c
      LEFT JOIN pg_stat_all_tables s ON s.relid=c.oid
      WHERE c.relname IN ('seg_segments','seg_dedup','seg_partitions','partition_consumers','consumer_watermarks')
        AND c.relkind='r'" >> "$OUT/sizes.csv"
    # TOAST dead tuples + autovacuum on the toast table itself
    $PSQL -c "
      SELECT '$TS', t.relname, s.n_live_tup, s.n_dead_tup, s.autovacuum_count
      FROM pg_class c JOIN pg_class t ON t.oid=c.reltoastrelid
      JOIN pg_stat_all_tables s ON s.relid=t.oid
      WHERE c.relname='seg_segments'" >> "$OUT/toast.csv"
    $PSQL -c "SELECT '$TS', xact_commit, xact_rollback, blks_read, blks_hit, tup_inserted, tup_deleted
              FROM pg_stat_database WHERE datname='queen'" >> "$OUT/db.csv"
    $PSQL -c "SELECT '$TS', wal_records, wal_fpi, wal_bytes FROM pg_stat_wal" >> "$OUT/wal.csv"
    $PSQL -c "SELECT '$TS', checkpoints_timed, checkpoints_req, buffers_checkpoint, buffers_backend
              FROM pg_stat_bgwriter" >> "$OUT/ckpt.csv"
    # progress of any running vacuum (phase + heap blks) — catches TOAST vacuums
    $PSQL -c "SELECT '$TS', p.relid::regclass, p.phase, p.heap_blks_scanned, p.heap_blks_total
              FROM pg_stat_progress_vacuum p" >> "$OUT/vacuum.csv"
    sleep 10
  done
}

# 1s cadence: wait-event histogram of active backends
waits() {
  while true; do
    TS=$(date +%s)
    $PSQL -c "
      SELECT '$TS', COALESCE(wait_event_type,'CPU'), COALESCE(wait_event,'-'), count(*),
             left(regexp_replace(COALESCE(query,''),'[\n,]',' ','g'),40)
      FROM pg_stat_activity
      WHERE state='active' AND pid<>pg_backend_pid()
      GROUP BY 2,3,5 ORDER BY 4 DESC" >> "$OUT/waits.csv"
    sleep 1
  done
}

sizes & SP=$!
waits & WP=$!
trap "kill $SP $WP 2>/dev/null" EXIT
wait
