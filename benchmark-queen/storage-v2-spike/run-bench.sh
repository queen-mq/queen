#!/usr/bin/env bash
# Head-to-head bench: v1 (queen.messages) vs v2 (q2.segments) on a pinned PG.
# Usage: ./run-bench.sh   (env knobs: MSGS, PARTITIONS, BATCH, POP_BATCH,
#                          CONCURRENCY, PG_HOST_PORT, PG_CPUS)
set -euo pipefail
cd "$(dirname "$0")"
REPO_ROOT="$(cd ../.. && pwd)"

PG_HOST_PORT="${PG_HOST_PORT:-5440}"
MSGS="${MSGS:-600000}"
PARTITIONS="${PARTITIONS:-8}"
BATCH="${BATCH:-50}"
POP_BATCH="${POP_BATCH:-100}"
CONCURRENCY="${CONCURRENCY:-4}"
STAMP="$(date +%Y%m%d-%H%M%S)"
RUN_DIR="runs/run-${STAMP}-K${BATCH}"
mkdir -p "$RUN_DIR"

export PGPASSWORD=postgres
PSQL="psql -h localhost -p ${PG_HOST_PORT} -U postgres -v ON_ERROR_STOP=1 -q"

echo "== docker compose up (port ${PG_HOST_PORT}) =="
PG_HOST_PORT="$PG_HOST_PORT" docker compose up -d --wait

echo "== create fresh databases =="
$PSQL -d postgres -c "DROP DATABASE IF EXISTS queen_v1" >/dev/null
$PSQL -d postgres -c "DROP DATABASE IF EXISTS queen_v2" >/dev/null
$PSQL -d postgres -c "CREATE DATABASE queen_v1" >/dev/null
$PSQL -d postgres -c "CREATE DATABASE queen_v2" >/dev/null

echo "== apply v1 schema + procedures =="
$PSQL -d queen_v1 -f "$REPO_ROOT/lib/schema/schema.sql" >/dev/null
for f in "$REPO_ROOT"/lib/schema/procedures/*.sql; do
  psql -h localhost -p "$PG_HOST_PORT" -U postgres -d queen_v1 -q -f "$f" \
    >>"$RUN_DIR/v1-schema-apply.log" 2>&1 || echo "  (non-fatal) $f failed, see log"
done
$PSQL -d queen_v1 -t -c "SELECT CASE WHEN to_regproc('queen.push_messages_v3') IS NOT NULL
                                      AND to_regproc('queen.pop_unified_batch_v4') IS NOT NULL
                                      AND to_regproc('queen.ack_messages_v2') IS NOT NULL
                                     THEN 'procs_ok' END" \
  | grep -q procs_ok || { echo "v1 procedures missing"; exit 1; }

echo "== apply v2 schema =="
$PSQL -d queen_v2 -f schema_v2.sql >/dev/null

cpu_sample() {  # cpu_sample <outfile>  — sample container CPU% until killed
  while true; do
    docker stats --no-stream --format '{{.CPUPerc}}' queen-spike-pg 2>/dev/null \
      | tr -d '%' >> "$1" || true
  done
}

run_phase() {  # run_phase <phase> <db>
  local phase="$1" db="$2"
  local cpu_file="$RUN_DIR/cpu-${phase}.log"
  cpu_sample "$cpu_file" & local sampler=$!
  PGURL="postgres://postgres:postgres@localhost:${PG_HOST_PORT}/${db}" \
  MSGS="$MSGS" PARTITIONS="$PARTITIONS" BATCH="$BATCH" POP_BATCH="$POP_BATCH" \
  CONCURRENCY="$CONCURRENCY" \
    node bench-driver.mjs "$phase" >> "$RUN_DIR/results.jsonl"
  kill "$sampler" 2>/dev/null; wait "$sampler" 2>/dev/null || true
  local cpu_avg="0"
  if [[ -s "$cpu_file" ]]; then
    cpu_avg=$(awk '{s+=$1; n++} END {if (n>0) printf "%.1f", s/n; else print 0}' "$cpu_file")
  fi
  echo "   ${phase}: done (avg PG CPU ${cpu_avg}%)"
  echo "{\"phase\":\"${phase}-cpu\",\"cpu_avg_pct\":${cpu_avg}}" >> "$RUN_DIR/results.jsonl"
}

echo "== v1: ingest -> sizes -> consume -> retention =="
run_phase ingest-v1 queen_v1
PGURL="postgres://postgres:postgres@localhost:${PG_HOST_PORT}/queen_v1" node bench-driver.mjs sizes-v1 >> "$RUN_DIR/results.jsonl"
run_phase consume-v1 queen_v1
run_phase retention-v1 queen_v1

echo "== quiesce: drop v1 residue so its autovacuum can't pollute v2 phases =="
$PSQL -d postgres -c "DROP DATABASE queen_v1" >/dev/null
sleep 5
$PSQL -d postgres -c "CHECKPOINT" >/dev/null

echo "== v2: ingest -> sizes -> consume -> retention =="
run_phase ingest-v2 queen_v2
PGURL="postgres://postgres:postgres@localhost:${PG_HOST_PORT}/queen_v2" node bench-driver.mjs sizes-v2 >> "$RUN_DIR/results.jsonl"
run_phase consume-v2 queen_v2
run_phase retention-v2 queen_v2

echo "== results: $RUN_DIR/results.jsonl =="
node report.mjs "$RUN_DIR/results.jsonl" | tee "$RUN_DIR/report.txt"
