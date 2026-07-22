#!/usr/bin/env bash
# sampler.sh OUTDIR DURATION_SECONDS
# Runs on the BENCH VM alongside a goload run. Independent background loops:
#   - waits:      1s  aggregate of pg_stat_activity active waits  -> waits.csv
#   - dockstats:  15s docker CPU/mem for broker + pg              -> dockstats.csv
#   - walio:      60s full pg_stat_wal + pg_stat_io snapshots     -> wal_stat.csv / io_stat.csv
#                     + db size / commits / deadlocks             -> dbstat.csv
# Captures FULL rows of pg_stat_wal/pg_stat_io (all columns) so PG18 column
# renames don't break collection. Start/final snapshots bracket the run.
set -uo pipefail
OUT="${1:?outdir}"; DUR="${2:?duration}"; PG=qbench-pg; RN="${RN:-r6682}"
mkdir -p "$OUT"
q(){  docker exec "$PG" psql -U postgres -tAc "$1" </dev/null 2>/dev/null; }
qc(){ docker exec "$PG" psql -U postgres --csv -c "$1" </dev/null 2>/dev/null; }

# ---- start snapshots ----
{ echo "=== START $(date -u +%s) ==="; qc "select * from pg_stat_wal"; } > "$OUT/wal_stat.csv"
{ echo "=== START $(date -u +%s) ==="; qc "select * from pg_stat_io"; }  > "$OUT/io_stat.csv"
: > "$OUT/waits.csv"        # t,wait_event_type,wait_event,count
: > "$OUT/dockstats.csv"    # t name cpu% memusage
: > "$OUT/dbstat.csv"       # t,db_size_bytes,xact_commit,xact_rollback,deadlocks

# ---- 1s waits loop ----
( for ((s=0; s<DUR; s++)); do
    T=$(date -u +%s)
    q "select $T||','||coalesce(wait_event_type,'CPU')||','||coalesce(wait_event,'run')||','||count(*) from pg_stat_activity where state='active' and pid<>pg_backend_pid() group by coalesce(wait_event_type,'CPU'), coalesce(wait_event,'run')" >> "$OUT/waits.csv"
    sleep 1
  done ) &
WPID=$!

# ---- 15s docker stats loop ----
( for ((s=0; s<DUR; s+=15)); do
    T=$(date -u +%s)
    docker stats --no-stream --format '{{.Name}} {{.CPUPerc}} {{.MemUsage}}' "$RN" "$PG" 2>/dev/null | sed "s/^/$T /" >> "$OUT/dockstats.csv"
    sleep 15
  done ) &
DPID=$!

# ---- 60s WAL/IO/db loop ----
( for ((s=60; s<=DUR; s+=60)); do
    sleep 60
    T=$(date -u +%s)
    { echo "=== t=$T ==="; qc "select * from pg_stat_wal"; }              >> "$OUT/wal_stat.csv"
    { echo "=== t=$T ==="; qc "select * from pg_stat_io"; }               >> "$OUT/io_stat.csv"
    q "select $T||','||pg_database_size('postgres')||','||xact_commit||','||xact_rollback||','||deadlocks from pg_stat_database where datname='postgres'" >> "$OUT/dbstat.csv"
  done ) &
IPID=$!

wait $WPID $DPID $IPID 2>/dev/null

# ---- final snapshots ----
{ echo "=== FINAL $(date -u +%s) ==="; qc "select * from pg_stat_wal"; } >> "$OUT/wal_stat.csv"
{ echo "=== FINAL $(date -u +%s) ==="; qc "select * from pg_stat_io"; }  >> "$OUT/io_stat.csv"
q "select 'FINAL,'||pg_database_size('postgres')||','||xact_commit||','||xact_rollback||','||deadlocks from pg_stat_database where datname='postgres'" >> "$OUT/dbstat.csv"
q "select 'FINAL_PRETTY db_size='||pg_size_pretty(pg_database_size('postgres'))" >> "$OUT/dbstat.csv"
echo "SAMPLER_DONE $OUT"
