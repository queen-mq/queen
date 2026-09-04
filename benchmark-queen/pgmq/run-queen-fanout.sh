#!/usr/bin/env bash
# Queen-only fan-out run: 1 queue, GROUPS native consumer groups. Fast to re-run.
# Tunable broker config via env (defaults from queen-compose.yml):
#   QUEEN_CPUS, QUEEN_NUM_WORKERS, QUEEN_SIDECAR_POOL_SIZE, QUEEN_DB_POOL_SIZE
#
#   GROUPS=10 DURATION=120 QUEEN_CPUS=8 QUEEN_SIDECAR_POOL_SIZE=200 QUEEN_NUM_WORKERS=8 ./run-queen-fanout.sh
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; cd "$SCRIPT_DIR"

GROUPS="${GROUPS:-10}"
DURATION="${DURATION:-120}"
PROD_CONNS="${PROD_CONNS:-100}"
CONS_CONNS_PER_GROUP="${CONS_CONNS_PER_GROUP:-10}"
MSGS_PER_PUSH="${MSGS_PER_PUSH:-10}"
READ_QTY="${READ_QTY:-100}"
NUM_PARTITIONS="${NUM_PARTITIONS:-1000}"
RUN_NAME="${1:-queen-fanout-$(date +%Y%m%d-%H%M%S)}"
OUTDIR="results/$RUN_NAME"
last_idx=$((GROUPS - 1))
export NVM_DIR="${NVM_DIR:-$HOME/.nvm}"
load_node() { [ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh" && nvm use 24 >/dev/null 2>&1; return 0; }
mkdir -p "$OUTDIR"

echo ">> QUEEN fan-out: groups=$GROUPS duration=${DURATION}s | broker: cpus=${QUEEN_CPUS:-3} workers=${QUEEN_NUM_WORKERS:-2} sidecar=${QUEEN_SIDECAR_POOL_SIZE:-30} dbpool=${QUEEN_DB_POOL_SIZE:-10}"
docker compose -f queen-compose.yml down -v >/dev/null 2>&1 || true
docker compose -f queen-compose.yml up -d
echo -n "   waiting broker…"
for i in $(seq 1 90); do curl -sf http://localhost:6633/api/v1/status >/dev/null 2>&1 && break; sleep 1; done; echo " ok"
curl -sf -X POST http://localhost:6633/api/v1/configure -H 'Content-Type: application/json' \
  -d '{"queue":"fanout-bench","options":{"leaseTime":60,"retryLimit":3,"retentionEnabled":true,"retentionSeconds":7200,"completedRetentionSeconds":1800}}' >/dev/null 2>&1 || true

bash sample-metrics.sh "$OUTDIR/metrics.csv" 1 queen-pg queen messages & SQ=$!
trap 'kill "$SQ" >/dev/null 2>&1' EXIT

QCPIDS=()
for i in $(seq 0 $last_idx); do
  ( load_node; SERVER_URL=http://localhost:6633 QUEUE_NAMES=fanout-bench CONSUMER_GROUP=cg$i \
    NUM_WORKERS=1 CONNECTIONS_PER_WORKER=$CONS_CONNS_PER_GROUP CONSUMER_BATCH=$READ_QTY DURATION=$DURATION \
    node queen-consumer.js ) >"$OUTDIR/consumer-cg$i.json" 2>"$OUTDIR/consumer-cg$i.err" &
  QCPIDS+=($!)
done
sleep 2
( load_node; SERVER_URL=http://localhost:6633 QUEUE_NAMES=fanout-bench NUM_WORKERS=2 \
  CONNECTIONS_PER_WORKER=$((PROD_CONNS / 2)) MAX_PARTITION=$NUM_PARTITIONS MSGS_PER_PUSH=$MSGS_PER_PUSH DURATION=$DURATION \
  node queen-producer.js ) >"$OUTDIR/producer.json" 2>"$OUTDIR/producer.err" &
QPP=$!
echo "   running ${DURATION}s…"
wait "$QPP"; for p in "${QCPIDS[@]}"; do wait "$p"; done; sleep 1

curl -sf http://localhost:6633/api/v1/status >"$OUTDIR/status.json" 2>/dev/null || true
kill "$SQ" >/dev/null 2>&1; trap - EXIT

echo; echo "==================== QUEEN FAN-OUT ($RUN_NAME) ===================="
( load_node; node -e "
const fs=require('fs');
const D=$DURATION, G=$GROUPS;
const s=JSON.parse(fs.readFileSync('$OUTDIR/status.json','utf8')); const m=s.messages||s;
const rows=fs.readFileSync('$OUTDIR/metrics.csv','utf8').trim().split('\n').slice(1);
let na=0,nc=0,maxtot=0,maxact=0; for(const r of rows){const c=r.split(','); if(c.length<10)continue; na+=+c[1]; nc++; if(+c[2]>maxtot)maxtot=+c[2]; if(+c[1]>maxact)maxact=+c[1];}
console.log('logical push msg/s :', Math.round((m.total||0)/D));
console.log('delivered msg/s    :', Math.round((m.completed||0)/D), '   (delivered/logical =', ((m.completed||0)/Math.max(m.total||1,1)).toFixed(1)+'x of', G+')');
console.log('pending at end     :', m.pending||0);
console.log('PG active backends :', (na/Math.max(nc,1)).toFixed(1), 'avg, ', maxact, 'peak');
console.log('PG backends total  :', maxtot, 'peak');
console.log('batch eff (pop)    :', (m.batchEfficiency&&m.batchEfficiency.pop));
"
)
echo "===================================================================="
if [ -n "${NO_TEARDOWN:-}" ]; then
  echo "Stack left UP. Dashboard: http://localhost:6633"
  echo "Tear down when done with:  docker compose -f queen-compose.yml down -v"
else
  docker compose -f queen-compose.yml down -v >/dev/null 2>&1 || true
fi
echo "artifacts: $OUTDIR/  (pgmq fan-out ref: ~63k delivered, 9.9x writes, 15M churn, 59 active backends)"
