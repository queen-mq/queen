#!/usr/bin/env bash
# run-experiment.sh <name> <duration_s> [broker env pairs...] -- [goload extra args...]
# Fresh PG DB + fresh broker + samplers + goload, archived under <name>/.
# Fixed base shape: 100 partitions, 16 prod / 12 cons, push-batch 100,
# pop-batch 500, pop-partitions 10, pop-wait, completed-retention 300.
set -uo pipefail
S="$(cd "$(dirname "$0")" && pwd)"
NAME="$1"; DUR="$2"; shift 2
BROKER_ENV=()
while [ $# -gt 0 ] && [ "$1" != "--" ]; do BROKER_ENV+=("$1"); shift; done
[ "${1:-}" = "--" ] && shift
GOLOAD_EXTRA=("$@")

echo "[exp] $NAME dur=${DUR}s broker_env=${BROKER_ENV[*]:-} goload_extra=${GOLOAD_EXTRA[*]:-}"
pkill -f "goload -url http://localhost:6640" 2>/dev/null; pkill -f pg-sample.sh 2>/dev/null
pkill -x queen-seg 2>/dev/null; sleep 2

docker exec queen-perf-pg16 psql -U postgres -q -c "DROP DATABASE IF EXISTS queen;" -c "CREATE DATABASE queen;"
mkdir -p "$S/$NAME"

cd /Users/alice/Work/queen/server
env PG_HOST=localhost PG_PORT=5457 PG_USER=postgres PG_PASSWORD=postgres PG_DATABASE=queen \
    PORT=6640 DB_POOL_SIZE=60 QUEEN_V2_ZSTD_LEVEL=1 QUEEN_V2_FUSION_SHARDS=8 RETENTION_INTERVAL=5000 \
    ${BROKER_ENV[@]+"${BROKER_ENV[@]}"} nohup ./target/release/queen-seg > "$S/$NAME/broker.log" 2>&1 &
BROKER_PID=$!
for i in $(seq 1 30); do curl -sf http://localhost:6640/status >/dev/null 2>&1 && break; sleep 1; done

nohup "$S/pg-sample.sh" queen-perf-pg16 "$S/$NAME" > "$S/$NAME/sampler.log" 2>&1 &
SAMPLER_PID=$!
nohup bash -c 'while true; do TS=$(date +%s); L=$(curl -sf http://localhost:6640/metrics/prometheus 2>/dev/null | awk "/^queen_seg_push_vegas_limit/ {p=\$2} /^queen_seg_pop_vegas_limit/ {o=\$2} END {print p\",\"o}"); echo "$TS,$L" >> '"$S/$NAME"'/vegas.csv; sleep 2; done' >/dev/null 2>&1 &
VEGAS_PID=$!
# CPU accounting: cumulative broker CPU time (ps etime fmt) + PG container cpu%
nohup bash -c 'while true; do TS=$(date +%s); B=$(ps -o cputime= -p '"$BROKER_PID"' 2>/dev/null | tr -d " "); P=$(docker stats --no-stream --format "{{.CPUPerc}}" queen-perf-pg16 2>/dev/null); echo "$TS,$B,$P" >> '"$S/$NAME"'/cpu.csv; sleep 5; done' >/dev/null 2>&1 &
CPU_PID=$!

"$S/goload" -url http://localhost:6640 -queue expq -partitions 100 -producers 16 -consumers 12 \
  -push-batch 100 -pop-batch 500 -pop-partitions 10 -pop-wait -completed-retention 300 \
  -duration "$DUR" -report 60 ${GOLOAD_EXTRA[@]+"${GOLOAD_EXTRA[@]}"} > "$S/$NAME/goload.log" 2>&1

kill $SAMPLER_PID $VEGAS_PID $CPU_PID 2>/dev/null
pkill -f pg-sample.sh 2>/dev/null
# post-run PG snapshot
docker exec queen-perf-pg16 psql -U postgres -d queen -tA -F, -c \
  "SELECT relname, n_tup_upd, n_tup_hot_upd, n_dead_tup, n_live_tup, pg_total_relation_size(relid) FROM pg_stat_user_tables ORDER BY relname" \
  > "$S/$NAME/final-tables.csv" 2>/dev/null
docker exec queen-perf-pg16 psql -U postgres -d queen -tA -F, -c \
  "SELECT wal_records, wal_fpi, wal_bytes FROM pg_stat_wal" > "$S/$NAME/final-wal.csv" 2>/dev/null
kill $BROKER_PID 2>/dev/null
echo "[exp] $NAME done"
tail -3 "$S/$NAME/goload.log"
