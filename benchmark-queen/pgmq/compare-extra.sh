#!/usr/bin/env bash
# Two extra scenarios with a SYMMETRIC closed-loop client on both sides
# (pgmq-bench.js over SQL vs queen-bench.js over HTTP):
#
#   ./compare-extra.sh latency   -> #5: single-message latency at concurrency=1
#                                   (pgmq connects DIRECT to Postgres = its best case)
#   ./compare-extra.sh worker    -> #6: worker-bound (PROCESS_MS work/msg) -> both
#                                   converge to a worker-limited rate, broker idle
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; cd "$SCRIPT_DIR"
TEST="${1:-latency}"
STAMP="$(date +%Y%m%d-%H%M%S)"
QUEEN_IMAGE="${QUEEN_IMAGE:-queen-mq:arm64-local}"
export QUEEN_IMAGE QUEEN_CPUS="${QUEEN_CPUS:-3}" QUEEN_NUM_WORKERS="${QUEEN_NUM_WORKERS:-2}" \
       QUEEN_SIDECAR_POOL_SIZE="${QUEEN_SIDECAR_POOL_SIZE:-150}" QUEEN_DB_POOL_SIZE="${QUEEN_DB_POOL_SIZE:-20}"
export NVM_DIR="${NVM_DIR:-$HOME/.nvm}"
load_node() { [ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh" && nvm use 24 >/dev/null 2>&1; return 0; }

if [ "$TEST" = "latency" ]; then
  DURATION="${DURATION:-60}"; PROD_CONNS=1; CONS_CONNS=1; MPP=1; RQTY=1; PROCESS_MS=0; RECORD_EMPTY=false; PGMQ_PGPORT=55432; NUMP=1000
  HEAD="#5 single-message latency @ concurrency=1 (pgmq direct-to-PG, sync_commit=on both)"
else
  DURATION="${DURATION:-120}"; PROD_CONNS=20; CONS_CONNS="${CONS_CONNS:-100}"; MPP=10; RQTY=10; PROCESS_MS="${PROCESS_MS:-20}"; RECORD_EMPTY=true; PGMQ_PGPORT=6432; NUMP=1000
  HEAD="#6 worker-bound: ${PROCESS_MS}ms work/msg, ${CONS_CONNS} consumer conns (expect ~$((CONS_CONNS*1000/PROCESS_MS)) msg/s both)"
fi
PGMQ_DIR="results/extra-$STAMP-$TEST-pgmq"; QUEEN_DIR="results/extra-$STAMP-$TEST-queen"
mkdir -p "$PGMQ_DIR" "$QUEEN_DIR"
echo "##### EXTRA TEST: $HEAD"
docker compose -f queen-compose.yml down -v >/dev/null 2>&1 || true
docker compose down -v >/dev/null 2>&1 || true

############ PGMQ ############
echo; echo "### pgmq side…"
docker compose up -d
for i in $(seq 1 60); do docker exec pgmq-postgres pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done
[ -d node_modules/pg ] || ( load_node; npm install --no-fund --no-audit >/dev/null 2>&1 )
docker exec pgmq-postgres psql -U postgres -d postgres -c \
  "CREATE EXTENSION IF NOT EXISTS pgmq CASCADE; DO \$\$ BEGIN PERFORM pgmq.drop_queue('bench'); EXCEPTION WHEN OTHERS THEN NULL; END \$\$; SELECT pgmq.create('bench');" >/dev/null 2>&1
bash sample-metrics.sh "$PGMQ_DIR/metrics.csv" 1 pgmq-postgres pgmq q_bench & SP=$!
trap 'kill "$SP" >/dev/null 2>&1' EXIT
( load_node; ROLE=consumer MODE=plain QUEUE=bench CONNECTIONS=$CONS_CONNS READ_QTY=$RQTY PROCESS_MS=$PROCESS_MS \
  RECORD_EMPTY=$RECORD_EMPTY DURATION=$DURATION PGHOST=localhost PGPORT=$PGMQ_PGPORT node pgmq-bench.js ) \
  >"$PGMQ_DIR/consumer.json" 2>"$PGMQ_DIR/consumer.err" & PC=$!
sleep 2
( load_node; ROLE=producer MODE=plain QUEUE=bench CONNECTIONS=$PROD_CONNS MSGS_PER_PUSH=$MPP NUM_PARTITIONS=$NUMP \
  DURATION=$DURATION PGHOST=localhost PGPORT=$PGMQ_PGPORT node pgmq-bench.js ) \
  >"$PGMQ_DIR/producer.json" 2>"$PGMQ_DIR/producer.err" & PP=$!
echo "   running ${DURATION}s…"; wait "$PP"; wait "$PC"; sleep 1
kill "$SP" >/dev/null 2>&1; trap - EXIT
docker compose down -v >/dev/null 2>&1 || true

############ QUEEN ############
echo; echo "### Queen side…"
docker compose -f queen-compose.yml up -d
for i in $(seq 1 90); do curl -sf http://localhost:6633/api/v1/status >/dev/null 2>&1 && break; sleep 1; done
bash sample-metrics.sh "$QUEEN_DIR/metrics.csv" 1 queen-pg queen messages & SQ=$!
trap 'kill "$SQ" >/dev/null 2>&1' EXIT
( load_node; ROLE=consumer SERVER_URL=http://localhost:6633 QUEUE=bench CONNECTIONS=$CONS_CONNS READ_QTY=$RQTY \
  PROCESS_MS=$PROCESS_MS RECORD_EMPTY=$RECORD_EMPTY WAIT=false DURATION=$DURATION node queen-bench.js ) \
  >"$QUEEN_DIR/consumer.json" 2>"$QUEEN_DIR/consumer.err" & QC=$!
sleep 2
( load_node; ROLE=producer SERVER_URL=http://localhost:6633 QUEUE=bench CONNECTIONS=$PROD_CONNS MSGS_PER_PUSH=$MPP \
  NUM_PARTITIONS=$NUMP DURATION=$DURATION node queen-bench.js ) \
  >"$QUEEN_DIR/producer.json" 2>"$QUEEN_DIR/producer.err" & QP=$!
echo "   running ${DURATION}s…"; wait "$QP"; wait "$QC"; sleep 1
kill "$SQ" >/dev/null 2>&1; trap - EXIT
docker compose -f queen-compose.yml down -v >/dev/null 2>&1 || true

############ REPORT ############
echo; echo "### Result ($TEST):"
( load_node; node -e "
const fs=require('fs');
const rd=p=>{try{return JSON.parse(fs.readFileSync(p,'utf8'))}catch{return{}}};
const back=d=>{try{const r=fs.readFileSync(d+'/metrics.csv','utf8').trim().split('\n').slice(1);let a=0,n=0;for(const x of r){const c=x.split(',');if(c.length<3)continue;a+=+c[1];n++}return n?(a/n).toFixed(1):'n/a'}catch{return'n/a'}};
const qp=rd('$QUEEN_DIR/producer.json'),qc=rd('$QUEEN_DIR/consumer.json');
const pp=rd('$PGMQ_DIR/producer.json'),pc=rd('$PGMQ_DIR/consumer.json');
const L=(o)=>o&&o.latency?o.latency.p50+' / '+o.latency.p99:'n/a';
const pad=(s,n)=>String(s).padEnd(n);
console.log('='.repeat(64));
if('$TEST'==='latency'){
  console.log(pad('metric',26)+pad('QUEEN',18)+'pgmq');
  console.log('-'.repeat(64));
  console.log(pad('push  p50/p99 (ms)',26)+pad(L(qp),18)+L(pp));
  console.log(pad('pop   p50/p99 (ms)',26)+pad(L(qc),18)+L(pc));
  console.log('-'.repeat(64));
  console.log('Note: concurrency=1; pgmq via DIRECT libpq (no pooler) = its best case.');
  console.log('Queen carries an HTTP/JSON hop -> expected to lose here. That is the point.');
} else {
  console.log(pad('metric',26)+pad('QUEEN',18)+'pgmq');
  console.log('-'.repeat(64));
  console.log(pad('delivered msg/s',26)+pad(qc.msgPerSec,18)+pc.msgPerSec);
  console.log(pad('consumer op p99 (ms)',26)+pad(qc.latency?qc.latency.p99:'n/a',18)+(pc.latency?pc.latency.p99:'n/a'));
  console.log(pad('PG active backends(avg)',26)+pad(back('$QUEEN_DIR'),18)+back('$PGMQ_DIR'));
  console.log('-'.repeat(64));
  console.log('Both bounded by the ${PROCESS_MS}ms/msg work, not the broker -> rates converge,');
  console.log('PG stays idle. At realistic per-message work the broker is not the bottleneck.');
}
console.log('='.repeat(64));
" )
echo "artifacts: $PGMQ_DIR/ , $QUEEN_DIR/"
