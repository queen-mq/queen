#!/usr/bin/env bash
# One Queen benchmark run on the Mac, driven by the UNCHANGED clustered
# producer/consumer from examples/long-running/. Same params and same Docker
# resource budget as the pgmq run, so the two are directly comparable.
#
# Usage (env, with defaults matching the pgmq run):
#   CONNECTIONS=100 MSGS_PER_PUSH=10 READ_QTY=100 NUM_PARTITIONS=1000 \
#   DURATION=120 ./run-queen.sh [run_name]
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
EXAMPLES="$REPO_ROOT/examples"

# ---- parameters (mirror run.sh) ----
CONNECTIONS="${CONNECTIONS:-100}"             # total per role
MSGS_PER_PUSH="${MSGS_PER_PUSH:-10}"
READ_QTY="${READ_QTY:-100}"                   # pop batch
NUM_PARTITIONS="${NUM_PARTITIONS:-1000}"
DURATION="${DURATION:-120}"
SERVER_URL="${SERVER_URL:-http://localhost:6633}"
QUEUE_NAME="queen-long-running"               # hardcoded in the example scripts
WORKERS="${WORKERS:-2}"                        # cluster forks per role
CONNS_PER_WORKER=$(( CONNECTIONS / WORKERS ))
RUN_NAME="${1:-queen-c${CONNECTIONS}-p${NUM_PARTITIONS}-$(date +%Y%m%d-%H%M%S)}"
OUTDIR="results/${RUN_NAME}"

export NVM_DIR="${NVM_DIR:-$HOME/.nvm}"
load_node() { [ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh" && nvm use 22 >/dev/null 2>&1; return 0; }

mkdir -p "$OUTDIR"
echo ">> run: $RUN_NAME (QUEEN)"
echo ">> conns(per role)=$CONNECTIONS (=${WORKERS}x${CONNS_PER_WORKER}) msgsPerPush=$MSGS_PER_PUSH popBatch=$READ_QTY partitions=$NUM_PARTITIONS duration=${DURATION}s"

# ---- 1. stack up ----
echo ">> bringing up Queen stack (broker + postgres)…"
docker compose -f queen-compose.yml up -d

echo -n ">> waiting for Queen broker…"
for i in $(seq 1 90); do
  if curl -sf "$SERVER_URL/api/v1/status" >/dev/null 2>&1; then echo " ready"; break; fi
  echo -n "."; sleep 1
done
if ! curl -sf "$SERVER_URL/api/v1/status" >/dev/null 2>&1; then
  echo " FAILED"; echo "!! Queen broker not reachable at $SERVER_URL — recent logs:"
  docker compose -f queen-compose.yml logs queen 2>&1 | tail -25
  docker compose -f queen-compose.yml down -v >/dev/null 2>&1 || true
  exit 1
fi

# ---- 2. node deps for examples ----
if [ ! -d "$EXAMPLES/node_modules/autocannon" ]; then
  echo ">> installing examples node deps (autocannon, axios)…"
  ( cd "$EXAMPLES"; load_node; npm install --no-fund --no-audit ) >"$OUTDIR/npm-install.log" 2>&1 || {
    echo "!! npm install failed in examples, see $OUTDIR/npm-install.log"; exit 1; }
fi

# ---- 3. clean queue (fresh DB via -v anyway, but be explicit) ----
curl -sf -X DELETE "$SERVER_URL/api/v1/resources/queues/$QUEUE_NAME" >/dev/null 2>&1 || true

# ---- 4. metrics sampler on queen-pg (append-only messages table) ----
echo ">> starting metrics sampler…"
bash sample-metrics.sh "$OUTDIR/metrics.csv" 1 queen-pg queen messages &
SAMPLER_PID=$!
cleanup() { kill "$SAMPLER_PID" >/dev/null 2>&1; }
trap cleanup EXIT

# ---- 5. producer (creates queue), then consumer ----
echo ">> starting producer ($CONNECTIONS conns)…"
( load_node; SERVER_URL="$SERVER_URL" NUM_WORKERS=$WORKERS CONNECTIONS_PER_WORKER=$CONNS_PER_WORKER \
  DURATION=$DURATION MAX_PARTITION=$NUM_PARTITIONS NUMBER_OF_MESSAGES_PER_PER_PUSH=$MSGS_PER_PUSH \
  node "$EXAMPLES/long-running/producer-cluster.js" ) >"$OUTDIR/producer.log" 2>&1 &
PROD_PID=$!

sleep 3  # let the queue get created + producer warm up

echo ">> starting consumer ($CONNECTIONS conns, batch=$READ_QTY)…"
( load_node; SERVER_URL="$SERVER_URL" NUM_WORKERS=$WORKERS CONNECTIONS_PER_WORKER=$CONNS_PER_WORKER \
  DURATION=$DURATION MAX_PARTITION=$NUM_PARTITIONS BATCH_SIZE=$READ_QTY \
  node "$EXAMPLES/long-running/consumer-clustered.js" ) >"$OUTDIR/consumer.log" 2>&1 &
CONS_PID=$!

echo ">> running for ${DURATION}s…"
wait "$PROD_PID"; PROD_RC=$?
wait "$CONS_PID"; CONS_RC=$?
sleep 1

# ---- 6. authoritative server-side totals (lifetime counters; survive retention) ----
curl -sf "$SERVER_URL/api/v1/status" >"$OUTDIR/status.json" 2>/dev/null || true
curl -sf "$SERVER_URL/api/v1/resources/queues/$QUEUE_NAME" >"$OUTDIR/queue-final.json" 2>/dev/null || true
cleanup; trap - EXIT

# ---- 7. summary ----
echo
echo "==================== SUMMARY ($RUN_NAME / QUEEN) ===================="
[ "$PROD_RC" != "0" ] && echo "!! producer rc=$PROD_RC (see $OUTDIR/producer.log)"
[ "$CONS_RC" != "0" ] && echo "!! consumer rc=$CONS_RC (see $OUTDIR/consumer.log)"

# Latency from the example client logs. Anchor to "ms" so we don't grab the
# digits inside the label (e.g. the "99" in "p99").
pull_ms() { grep -m1 "$2" "$1" 2>/dev/null | sed -E 's/.*: *//; s/[[:space:]]*ms.*//'; }
PROD_P50=$(pull_ms "$OUTDIR/producer.log" 'Latency p50:')
PROD_P99=$(pull_ms "$OUTDIR/producer.log" 'Latency p99:')
CONS_P50=$(pull_ms "$OUTDIR/consumer.log" 'Latency p50:')
CONS_P99=$(pull_ms "$OUTDIR/consumer.log" 'Latency p99:')

# Authoritative throughput from Queen's lifetime counters in /api/v1/status
# (messages.total = pushed, messages.completed = consumed). Survives retention.
read PUSH_MS POP_MS < <( ( load_node; node -e "
  const fs=require('fs');
  try{ const j=JSON.parse(fs.readFileSync('$OUTDIR/status.json','utf8'));
    const m=j.messages||j;
    console.log(Math.round((m.total||0)/$DURATION), Math.round((m.completed||0)/$DURATION));
  }catch(e){ console.log('na','na'); }" ) )

echo "PRODUCER:  push msg/s=${PUSH_MS}   p50=${PROD_P50}ms  p99=${PROD_P99}ms"
echo "CONSUMER:  pop  msg/s=${POP_MS}    p50=${CONS_P50}ms  p99=${CONS_P99}ms"

if [ -f "$OUTDIR/metrics.csv" ]; then
  awk -F',' 'NR>1 && NF>=10 { na+=$2; nc++; if($2>maxact)maxact=$2; if($3>maxtot)maxtot=$3; if($5>maxdead)maxdead=$5; lastav=$9; lastupd=$7; lastdel=$8 }
    END { if(nc>0) printf "PG BACKENDS: avg_active=%.1f  peak_active=%d  peak_total=%d\nPG CHURN:    peak_dead=%d  upd=%d  del=%d  autovacuum_runs=%d\n", na/nc, maxact, maxtot, maxdead, lastupd, lastdel, lastav }' "$OUTDIR/metrics.csv"
fi
echo "artifacts:   $OUTDIR/"
echo "===================================================================="
echo "Tear down with:  docker compose -f queen-compose.yml down -v"
