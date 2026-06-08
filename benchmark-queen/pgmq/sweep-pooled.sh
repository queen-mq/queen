#!/usr/bin/env bash
# pgmq saturation sweep — POOLED via PgBouncer (transaction mode, default_pool_size=64).
# Thousands of client connections multiplex onto ~64 real PG backends, which bounds
# the single-table contention. Compare against sweep-direct.sh (each client = a backend).
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; cd "$SCRIPT_DIR"

CLIENTS="${CLIENTS:-25 50 100 200 400 800 1600}"
export MODE=plain QUEUE=bench MSGS_PER_PUSH="${MSGS_PER_PUSH:-10}" READ_QTY="${READ_QTY:-100}"
export DURATION="${DURATION:-60}" PGPORT="${PGPORT:-6432}"   # 6432 = PgBouncer (pooled)

echo ">>> pgmq POOLED sweep (PgBouncer pool=64): clients/role=[$CLIENTS] send=$MSGS_PER_PUSH read=$READ_QTY dur=${DURATION}s port=$PGPORT"

for c in $CLIENTS; do
  name="plain-pooled-c${c}"
  echo
  echo "######## STEP: ${c} clients/role (PgBouncer -> ~64 PG backends) ########"
  ( while true; do
      printf '%s,' "$(date +%s)"
      docker stats --no-stream --format '{{.CPUPerc}},{{.MemUsage}}' pgmq-postgres 2>/dev/null
      sleep 3
    done ) > "results/${name}.dockerstats.csv" 2>/dev/null &
  DS=$!
  CONNECTIONS="$c" bash run.sh "$name" || echo "!! step c=${c} failed (continuing)"
  kill "$DS" 2>/dev/null || true
  wait "$DS" 2>/dev/null || true
  sleep 4
done

echo
echo ">>> pooled sweep complete. Artifacts in results/plain-pooled-c*/ + *.dockerstats.csv"
