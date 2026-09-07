#!/usr/bin/env bash
# One pgmq benchmark run: bring up the stack, reset the queue, run producer +
# consumer + metrics sampler concurrently for DURATION, then print a summary.
#
# Mirrors the parameters of the Queen run_test_v3.sh so results are comparable.
#
# Usage (all via env vars, with defaults):
#   MODE=fifo CONNECTIONS=100 MSGS_PER_PUSH=10 READ_QTY=100 \
#   NUM_PARTITIONS=1000 DURATION=60 ./run.sh [run_name]
#
# MODE=fifo  -> ordered, NUM_PARTITIONS groups (apples-to-apples vs Queen).
# MODE=plain -> SQS-style competing consumers (pgmq best case, no ordering).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ---- parameters ----
MODE="${MODE:-fifo}"
CONNECTIONS="${CONNECTIONS:-100}"        # per role (producer opens this many, consumer too)
PROD_CONNECTIONS="${PROD_CONNECTIONS:-$CONNECTIONS}"
CONS_CONNECTIONS="${CONS_CONNECTIONS:-$CONNECTIONS}"
MSGS_PER_PUSH="${MSGS_PER_PUSH:-10}"
READ_QTY="${READ_QTY:-100}"
NUM_PARTITIONS="${NUM_PARTITIONS:-1000}"
DURATION="${DURATION:-60}"
VT="${VT:-30}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-0}"
QUEUE="${QUEUE:-bench}"
PGPORT="${PGPORT:-6432}"                 # pgbouncer
RUN_NAME="${1:-${MODE}-c${CONNECTIONS}-p${NUM_PARTITIONS}-$(date +%Y%m%d-%H%M%S)}"
OUTDIR="results/${RUN_NAME}"

export NVM_DIR="${NVM_DIR:-$HOME/.nvm}"
load_node() { [ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh" && nvm use 24 >/dev/null 2>&1; return 0; }

psql_pg() { docker exec pgmq-postgres psql -U postgres -d postgres -v ON_ERROR_STOP=0 "$@"; }

mkdir -p "$OUTDIR"
echo ">> run: $RUN_NAME"
echo ">> mode=$MODE conns(prod/cons)=$PROD_CONNECTIONS/$CONS_CONNECTIONS msgsPerPush=$MSGS_PER_PUSH readQty=$READ_QTY partitions=$NUM_PARTITIONS duration=${DURATION}s"

# ---- 1. stack up ----
echo ">> bringing up stack (postgres+pgmq + pgbouncer)…"
docker compose up -d

echo -n ">> waiting for Postgres…"
for i in $(seq 1 60); do
  if docker exec pgmq-postgres pg_isready -U postgres -d postgres >/dev/null 2>&1; then echo " ready"; break; fi
  echo -n "."; sleep 1
done

echo -n ">> waiting for PgBouncer…"
for i in $(seq 1 30); do
  if docker exec pgmq-pgbouncer sh -c 'pg_isready -h 127.0.0.1 -p 6432 -U postgres' >/dev/null 2>&1 \
     || nc -z localhost "$PGPORT" >/dev/null 2>&1; then echo " ready"; break; fi
  echo -n "."; sleep 1
done

# ---- 2. node deps ----
if [ ! -d node_modules/pg ]; then
  echo ">> installing node deps (pg)…"
  ( load_node; npm install --no-fund --no-audit ) >"$OUTDIR/npm-install.log" 2>&1 || {
    echo "!! npm install failed, see $OUTDIR/npm-install.log"; exit 1; }
fi

# ---- 3. reset queue ----
echo ">> resetting queue '$QUEUE'…"
psql_pg -c "CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;" >/dev/null 2>&1
psql_pg -c "DO \$\$ BEGIN PERFORM pgmq.drop_queue('$QUEUE'); EXCEPTION WHEN OTHERS THEN NULL; END \$\$;" >/dev/null 2>&1
psql_pg -c "SELECT pgmq.create('$QUEUE');" >/dev/null 2>&1
if [ "$MODE" = "fifo" ]; then
  psql_pg -c "SELECT pgmq.create_fifo_index('$QUEUE');" >/dev/null 2>&1 \
    && echo "   fifo index created" \
    || echo "   !! create_fifo_index failed — does this pgmq image have FIFO support? (see README)"
fi

# record the pgmq function inventory + version for the report
psql_pg -c "SELECT extversion FROM pg_extension WHERE extname='pgmq';" >"$OUTDIR/pgmq-version.txt" 2>&1
psql_pg -c "\df pgmq.read_grouped*" >>"$OUTDIR/pgmq-version.txt" 2>&1

# ---- 4. metrics sampler ----
echo ">> starting metrics sampler…"
bash sample-metrics.sh "$OUTDIR/metrics.csv" 1 pgmq-postgres pgmq "q_${QUEUE}" &
SAMPLER_PID=$!

cleanup() { kill "$SAMPLER_PID" >/dev/null 2>&1; }
trap cleanup EXIT

common_env() {
  export MODE PGPORT QUEUE DURATION MSGS_PER_PUSH READ_QTY NUM_PARTITIONS VT PAYLOAD_BYTES
  export PGHOST=localhost
}

# ---- 5. consumer (start first so it's ready), then producer ----
echo ">> starting consumer ($CONS_CONNECTIONS conns)…"
( load_node; common_env; ROLE=consumer CONNECTIONS=$CONS_CONNECTIONS \
  node pgmq-bench.js ) >"$OUTDIR/consumer.json" 2>"$OUTDIR/consumer.err.log" &
CONS_PID=$!

sleep 2

echo ">> starting producer ($PROD_CONNECTIONS conns)…"
( load_node; common_env; ROLE=producer CONNECTIONS=$PROD_CONNECTIONS \
  node pgmq-bench.js ) >"$OUTDIR/producer.json" 2>"$OUTDIR/producer.err.log" &
PROD_PID=$!

echo ">> running for ${DURATION}s…"
wait "$PROD_PID"; PROD_RC=$?
wait "$CONS_PID"; CONS_RC=$?
sleep 1
cleanup; trap - EXIT

# final table sizes
psql_pg -c "SELECT pg_size_pretty(pg_total_relation_size('pgmq.q_${QUEUE}')) AS queue_table_size;" >"$OUTDIR/final-sizes.txt" 2>&1

# ---- 6. summary ----
echo
echo "==================== SUMMARY ($RUN_NAME) ===================="
if [ "$PROD_RC" != "0" ]; then echo "!! producer exited rc=$PROD_RC — see $OUTDIR/producer.err.log"; fi
if [ "$CONS_RC" != "0" ]; then echo "!! consumer exited rc=$CONS_RC — see $OUTDIR/consumer.err.log"; fi

read_json() { ( load_node; node -e "try{const d=require('./$1');console.log(d.$2)}catch(e){console.log('n/a')}" ); }

echo "PRODUCER:  msg/s=$(read_json "$OUTDIR/producer.json" msgPerSec)  p50=$(read_json "$OUTDIR/producer.json" 'latency.p50')ms  p99=$(read_json "$OUTDIR/producer.json" 'latency.p99')ms  errors=$(read_json "$OUTDIR/producer.json" errors)"
echo "CONSUMER:  msg/s=$(read_json "$OUTDIR/consumer.json" msgPerSec)  p50=$(read_json "$OUTDIR/consumer.json" 'latency.p50')ms  p99=$(read_json "$OUTDIR/consumer.json" 'latency.p99')ms  errors=$(read_json "$OUTDIR/consumer.json" errors)"

# metrics: average active backends, peak total backends, peak dead tuples, autovacuum runs
if [ -f "$OUTDIR/metrics.csv" ]; then
  awk -F',' 'NR>1 && NF>=10 {
    na+=$2; nc++; if($3>maxtot)maxtot=$3; if($5>maxdead)maxdead=$5; lastav=$9; if($2>maxact)maxact=$2;
  } END {
    if(nc>0) printf "PG BACKENDS: avg_active=%.1f  peak_active=%d  peak_total=%d\nPG CHURN:    peak_dead_tuples=%d  autovacuum_runs=%d\n", na/nc, maxact, maxtot, maxdead, lastav;
  }' "$OUTDIR/metrics.csv"
fi
echo "pgmq version: $(head -3 "$OUTDIR/pgmq-version.txt" 2>/dev/null | tail -1 | tr -d ' ')"
echo "queue table:  $(grep -i 'MB\|kB\|GB\|bytes' "$OUTDIR/final-sizes.txt" 2>/dev/null | head -1 | tr -d ' ')"
echo "artifacts:    $OUTDIR/"
echo "============================================================"
echo
echo "Compare against Queen's hi-part-1000 / bp-10 (≈25–39k msg/s, ~2.5 active PG conns)."
echo "Tear down with:  docker compose down -v"
