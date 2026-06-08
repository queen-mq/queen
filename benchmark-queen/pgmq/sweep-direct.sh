#!/usr/bin/env bash
# pgmq saturation sweep — DIRECT connections (each client = one PG backend).
# Single unordered queue (MODE=plain), pgmq best-case batch (send 10 / read 100).
# Ramps client count per role until Postgres saturates; run.sh resets the queue
# and samples PG state (backends, dead tuples, UPDATE+DELETE churn, autovacuum)
# each step. We also sample PG container CPU/mem per step.
#
# Usage:  CLIENTS="25 50 100 200 400 600 800 1200" DURATION=60 bash sweep-direct.sh
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; cd "$SCRIPT_DIR"

CLIENTS="${CLIENTS:-25 50 100 200 400 600 800 1200}"
export MODE=plain QUEUE=bench MSGS_PER_PUSH="${MSGS_PER_PUSH:-10}" READ_QTY="${READ_QTY:-100}"
export DURATION="${DURATION:-60}" PGPORT="${PGPORT:-55432}"   # 55432 = direct PG (bypass PgBouncer)

echo ">>> pgmq DIRECT sweep: clients/role = [$CLIENTS], send=$MSGS_PER_PUSH read=$READ_QTY dur=${DURATION}s port=$PGPORT"

for c in $CLIENTS; do
  name="plain-direct-c${c}"
  echo
  echo "######## STEP: ${c} clients/role (~$((c*2)) PG backends) ########"
  # PG container CPU/mem sampler for this step
  ( while true; do
      printf '%s,' "$(date +%s)"
      docker stats --no-stream --format '{{.CPUPerc}},{{.MemUsage}}' pgmq-postgres 2>/dev/null
      sleep 3
    done ) > "results/${name}.dockerstats.csv" 2>/dev/null &
  DS=$!
  CONNECTIONS="$c" bash run.sh "$name" || echo "!! step c=${c} failed (continuing)"
  kill "$DS" 2>/dev/null || true
  wait "$DS" 2>/dev/null || true
  sleep 4   # let autovacuum/queue settle between steps
done

echo
echo ">>> sweep complete. Per-step artifacts in results/plain-direct-c*/  + *.dockerstats.csv"
