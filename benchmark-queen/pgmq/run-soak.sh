#!/usr/bin/env bash
# Endurance / bloat soak: sustained high load for DURATION (default 30 min) on
# each system, tracking throughput drift, dead-tuple churn, autovacuum activity
# and total storage over time. The question: does pgmq's per-message
# UPDATE(vt)+DELETE outrun autovacuum (bloat + decay), while Queen's append-only
# messages table stays flat?
#
#   DURATION=1800 ./run-soak.sh
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; cd "$SCRIPT_DIR"
DURATION="${DURATION:-1800}"
PROD_CONNS="${PROD_CONNS:-50}"; CONS_CONNS="${CONS_CONNS:-100}"
MSGS_PER_PUSH="${MSGS_PER_PUSH:-10}"; READ_QTY="${READ_QTY:-100}"; NUM_PARTITIONS="${NUM_PARTITIONS:-1000}"
QUEEN_IMAGE="${QUEEN_IMAGE:-queen-mq:arm64-local-0.15.2}"
RET="${RETENTION_SECONDS:-600}"; CRET="${COMPLETED_RETENTION_SECONDS:-300}"
STAMP="$(date +%Y%m%d-%H%M%S)"
PGMQ_DIR="results/soak-$STAMP-pgmq"; QUEEN_DIR="results/soak-$STAMP-queen"
mkdir -p "$PGMQ_DIR" "$QUEEN_DIR"
export NVM_DIR="${NVM_DIR:-$HOME/.nvm}"
load_node() { [ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh" && nvm use 24 >/dev/null 2>&1; return 0; }

echo "##### SOAK ${DURATION}s each | prod=$PROD_CONNS cons=$CONS_CONNS msgs/push=$MSGS_PER_PUSH popBatch=$READ_QTY parts=$NUM_PARTITIONS"
docker compose -f queen-compose.yml down -v >/dev/null 2>&1 || true
docker compose down -v >/dev/null 2>&1 || true

############ PGMQ ############
echo; echo "### pgmq soak (read+delete churn)…"
docker compose up -d
for i in $(seq 1 60); do docker exec pgmq-postgres pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done
[ -d node_modules/pg ] || ( load_node; npm install --no-fund --no-audit >/dev/null 2>&1 )
docker exec pgmq-postgres psql -U postgres -d postgres -c \
  "CREATE EXTENSION IF NOT EXISTS pgmq CASCADE; DO \$\$ BEGIN PERFORM pgmq.drop_queue('bench'); EXCEPTION WHEN OTHERS THEN NULL; END \$\$; SELECT pgmq.create('bench');" >/dev/null 2>&1
bash sample-soak.sh "$PGMQ_DIR/soak.csv" 5 pgmq-postgres pgmq q_bench & SP=$!
trap 'kill "$SP" >/dev/null 2>&1' EXIT
( load_node; ENGINE=pgmq ROLE=consumer MODE=plain QUEUE=bench CONNECTIONS=$CONS_CONNS READ_QTY=$READ_QTY \
  DURATION=$DURATION PGHOST=localhost PGPORT=6432 node pgmq-bench.js ) >"$PGMQ_DIR/consumer.json" 2>"$PGMQ_DIR/consumer.err" & PC=$!
sleep 2
( load_node; ENGINE=pgmq ROLE=producer MODE=plain QUEUE=bench CONNECTIONS=$PROD_CONNS MSGS_PER_PUSH=$MSGS_PER_PUSH \
  NUM_PARTITIONS=$NUM_PARTITIONS DURATION=$DURATION PGHOST=localhost PGPORT=6432 node pgmq-bench.js ) >"$PGMQ_DIR/producer.json" 2>"$PGMQ_DIR/producer.err" & PP=$!
echo "   running ${DURATION}s… (started $(date +%H:%M:%S))"; wait "$PP"; wait "$PC"; sleep 1
kill "$SP" >/dev/null 2>&1; trap - EXIT
docker compose down -v >/dev/null 2>&1 || true

############ QUEEN ############
echo; echo "### Queen soak (append-only messages, retention ${RET}s)…"
QUEEN_IMAGE="$QUEEN_IMAGE" QUEEN_CPUS=3 QUEEN_NUM_WORKERS=2 QUEEN_PUSH_MAX_HOLD_MS=20 docker compose -f queen-compose.yml up -d
for i in $(seq 1 90); do curl -sf http://localhost:6633/api/v1/status >/dev/null 2>&1 && break; sleep 1; done
bash sample-soak.sh "$QUEEN_DIR/soak.csv" 5 queen-pg queen messages & SQ=$!
trap 'kill "$SQ" >/dev/null 2>&1' EXIT
( load_node; SERVER_URL=http://localhost:6633 QUEUE=bench CONNECTIONS=$CONS_CONNS READ_QTY=$READ_QTY ROLE=consumer \
  DURATION=$DURATION node queen-bench.js ) >"$QUEEN_DIR/consumer.json" 2>"$QUEEN_DIR/consumer.err" & QC=$!
sleep 2
( load_node; SERVER_URL=http://localhost:6633 QUEUE=bench CONNECTIONS=$PROD_CONNS MSGS_PER_PUSH=$MSGS_PER_PUSH ROLE=producer \
  NUM_PARTITIONS=$NUM_PARTITIONS RETENTION_SECONDS=$RET COMPLETED_RETENTION_SECONDS=$CRET \
  DURATION=$DURATION node queen-bench.js ) >"$QUEEN_DIR/producer.json" 2>"$QUEEN_DIR/producer.err" & QP=$!
echo "   running ${DURATION}s… (started $(date +%H:%M:%S))"; wait "$QP"; wait "$QC"; sleep 1
curl -sf http://localhost:6633/api/v1/status >"$QUEEN_DIR/status.json" 2>/dev/null || true
kill "$SQ" >/dev/null 2>&1; trap - EXIT
docker compose -f queen-compose.yml down -v >/dev/null 2>&1 || true

############ REPORT ############
echo; echo "### Soak trends:"
( load_node; node -e "
const fs=require('fs');
function rows(d){try{return fs.readFileSync(d+'/soak.csv','utf8').trim().split('\n').slice(1).map(l=>l.split(',').map(Number)).filter(r=>r.length>=8)}catch{return[]}}
// rate of column c (cumulative) over [t-w, t]
function rate(R,ci,fromTs,toTs){let a=null,b=null;for(const r of R){if(a===null&&r[0]>=fromTs)a=r;if(r[0]<=toTs)b=r;}if(!a||!b||b[0]<=a[0])return 0;return Math.round((b[ci]-a[ci])/(b[0]-a[0]));}
function mb(b){return (b/1048576).toFixed(0)+' MB';}
function report(name,d){
  const R=rows(d); if(!R.length){console.log(name+': no data');return;}
  const t0=R[0][0], tN=R[R.length-1][0], span=tN-t0;
  const first=rate(R,5,t0,t0+300);          // push rate, first 5 min (col5=main_ins)
  const last=rate(R,5,tN-300,tN);           // push rate, last 5 min
  const drift=first? Math.round((last-first)/first*100):0;
  let maxDead=0,finalDead=R[R.length-1][3],av=R[R.length-1][4];
  let maxBytes=0; for(const r of R){if(r[3]>maxDead)maxDead=r[3]; if(r[2]>maxBytes)maxBytes=r[2];}
  let na=0; for(const r of R) na+=r[1]; na=(na/R.length).toFixed(1);
  const bytes0=R[0][2], bytesN=R[R.length-1][2];
  console.log('— '+name+' (span '+span+'s) —');
  console.log('  push msg/s:   first5min='+first+'  last5min='+last+'  drift='+drift+'%');
  console.log('  dead tuples:  peak='+maxDead.toLocaleString()+'  final='+finalDead.toLocaleString());
  console.log('  autovacuum runs (schema): '+av);
  console.log('  schema size:  start='+mb(bytes0)+'  end='+mb(bytesN)+'  peak='+mb(maxBytes));
  console.log('  PG active backends avg: '+na);
}
console.log('='.repeat(64));
report('QUEEN  (append-only + retention)','$QUEEN_DIR');
console.log('');
report('pgmq   (read+delete churn)','$PGMQ_DIR');
console.log('='.repeat(64));
console.log('drift≈0% = throughput held; large negative = degraded under sustained load.');
" )
echo "artifacts: $PGMQ_DIR/ , $QUEEN_DIR/ (soak.csv = per-5s time series for charts)"
