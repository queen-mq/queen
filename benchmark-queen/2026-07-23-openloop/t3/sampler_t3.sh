#!/bin/bash
# sampler_t3.sh LABEL DUR
# Snapshots cumulative WAL + IO counters every 5s (fresh backend per sample, so
# stats are current), plus a 5x1s pg_stat_activity wait histogram mid-run.
# PG18: wal write/sync timing lives in pg_stat_io (object='wal'), not pg_stat_wal.
LABEL="$1"; DUR="${2:-90}"
OUT=/root/t3out; mkdir -p "$OUT"
CSV="$OUT/wal_${LABEL}.csv"
WHIST="$OUT/waits_${LABEL}.txt"
SQL="SELECT extract(epoch from clock_timestamp())::bigint,\
(select wal_records from pg_stat_wal),\
(select wal_fpi from pg_stat_wal),\
(select wal_bytes from pg_stat_wal),\
(select coalesce(sum(writes),0)::bigint from pg_stat_io where object='wal' and context='normal'),\
(select coalesce(sum(write_time),0) from pg_stat_io where object='wal' and context='normal'),\
(select coalesce(sum(fsyncs),0)::bigint from pg_stat_io where object='wal' and context='normal'),\
(select coalesce(sum(fsync_time),0) from pg_stat_io where object='wal' and context='normal'),\
(select xact_commit from pg_stat_database where datname='postgres'),\
(select count(*) from pg_stat_activity where state='active' and pid<>pg_backend_pid())"
echo "epoch,wal_records,wal_fpi,wal_bytes,wal_writes,wal_write_time_ms,wal_fsyncs,wal_fsync_time_ms,commits,active_bk" > "$CSV"
: > "$WHIST"
START=$(date +%s); END=$((START+DUR)); WAITDONE=0
while :; do
  NOW=$(date +%s); [ "$NOW" -ge "$END" ] && break
  docker exec qbench-pg psql -U postgres -At -F',' -c "$SQL" </dev/null >> "$CSV" 2>/dev/null
  ELAPSED=$((NOW-START))
  if [ "$WAITDONE" -eq 0 ] && [ "$ELAPSED" -ge $((DUR/2)) ]; then
    for i in 1 2 3 4 5; do
      echo "=== wait sample $i $(date +%s) ===" >> "$WHIST"
      docker exec qbench-pg psql -U postgres -At -F'|' -c "SELECT coalesce(wait_event_type,'CPU'),coalesce(wait_event,'run'),count(*) FROM pg_stat_activity WHERE state='active' AND pid<>pg_backend_pid() GROUP BY 1,2 ORDER BY 3 DESC" </dev/null >> "$WHIST" 2>/dev/null
      sleep 1
    done
    WAITDONE=1
  fi
  sleep 5
done
echo "SAMPLER_T3_DONE ${LABEL}"
