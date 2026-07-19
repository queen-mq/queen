#!/usr/bin/env bash
# Engine-vs-engine latency: Queen's stored procedures vs pgmq's
# functions, BOTH called directly over libpq (no broker, no HTTP, no pooler),
# concurrency=1, single message. Isolates the SQL engine from the broker hop.
#
#   ./run-sp-latency.sh   (DURATION=60 by default)
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; cd "$SCRIPT_DIR"
DURATION="${DURATION:-60}"
STAMP="$(date +%Y%m%d-%H%M%S)"
QUEEN_IMAGE="${QUEEN_IMAGE:-queen-mq:arm64-local}"
PGMQ_DIR="results/sp-$STAMP-pgmq"; QUEEN_DIR="results/sp-$STAMP-queensp"
mkdir -p "$PGMQ_DIR" "$QUEEN_DIR"
export NVM_DIR="${NVM_DIR:-$HOME/.nvm}"
load_node() { [ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh" && nvm use 22 >/dev/null 2>&1; return 0; }

echo "##### ENGINE-vs-ENGINE latency (direct libpq, concurrency=1, single msg, sync_commit=on)"
docker compose -f queen-compose.yml down -v >/dev/null 2>&1 || true
docker compose down -v >/dev/null 2>&1 || true

############ pgmq (functions, direct to PG :55432) ############
echo; echo "### pgmq functions, direct…"
docker compose up -d
for i in $(seq 1 60); do docker exec pgmq-postgres pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done
[ -d node_modules/pg ] || ( load_node; npm install --no-fund --no-audit >/dev/null 2>&1 )
docker exec pgmq-postgres psql -U postgres -d postgres -c \
  "CREATE EXTENSION IF NOT EXISTS pgmq CASCADE; DO \$\$ BEGIN PERFORM pgmq.drop_queue('bench'); EXCEPTION WHEN OTHERS THEN NULL; END \$\$; SELECT pgmq.create('bench');" >/dev/null 2>&1
( load_node; ENGINE=pgmq ROLE=consumer MODE=plain QUEUE=bench CONNECTIONS=1 READ_QTY=1 RECORD_EMPTY=false \
  DURATION=$DURATION PGHOST=localhost PGPORT=55432 node pgmq-bench.js ) >"$PGMQ_DIR/consumer.json" 2>"$PGMQ_DIR/consumer.err" & PC=$!
sleep 2
( load_node; ENGINE=pgmq ROLE=producer MODE=plain QUEUE=bench CONNECTIONS=1 MSGS_PER_PUSH=1 \
  DURATION=$DURATION PGHOST=localhost PGPORT=55432 node pgmq-bench.js ) >"$PGMQ_DIR/producer.json" 2>"$PGMQ_DIR/producer.err" & PP=$!
echo "   running ${DURATION}s…"; wait "$PP"; wait "$PC"
docker compose down -v >/dev/null 2>&1 || true

############ Queen stored procedures (direct to queen-pg :55433) ############
echo; echo "### Queen stored procedures, direct…"
QUEEN_IMAGE="$QUEEN_IMAGE" docker compose -f queen-compose.yml up -d
echo -n "   waiting broker (schema init)…"; for i in $(seq 1 90); do curl -sf http://localhost:6633/api/v1/status >/dev/null 2>&1 && break; sleep 1; done; echo " ok"
( load_node; ENGINE=queen ROLE=consumer QUEUE=bench QUEEN_PARTITION=p0 CONNECTIONS=1 READ_QTY=1 RECORD_EMPTY=false \
  DURATION=$DURATION PGHOST=localhost PGPORT=55433 node pgmq-bench.js ) >"$QUEEN_DIR/consumer.json" 2>"$QUEEN_DIR/consumer.err" & QC=$!
sleep 2
( load_node; ENGINE=queen ROLE=producer QUEUE=bench QUEEN_PARTITION=p0 CONNECTIONS=1 MSGS_PER_PUSH=1 \
  DURATION=$DURATION PGHOST=localhost PGPORT=55433 node pgmq-bench.js ) >"$QUEEN_DIR/producer.json" 2>"$QUEEN_DIR/producer.err" & QP=$!
echo "   running ${DURATION}s…"; wait "$QP"; wait "$QC"
docker compose -f queen-compose.yml down -v >/dev/null 2>&1 || true

############ report ############
echo; echo "### Result — engine latency (no broker, no hop):"
( load_node; node -e "
const fs=require('fs');const rd=p=>{try{return JSON.parse(fs.readFileSync(p,'utf8'))}catch{return{}}};
const L=o=>o&&o.latency?(o.latency.p50+' / '+o.latency.p99):'n/a';
const pad=(s,n)=>String(s).padEnd(n);
const qp=rd('$QUEEN_DIR/producer.json'),qc=rd('$QUEEN_DIR/consumer.json');
const pp=rd('$PGMQ_DIR/producer.json'),pc=rd('$PGMQ_DIR/consumer.json');
console.log('='.repeat(70));
console.log(pad('p50 / p99 (ms)',24)+pad('Queen SP (direct)',22)+'pgmq (direct)');
console.log('-'.repeat(70));
console.log(pad('push',24)+pad(L(qp),22)+L(pp));
console.log(pad('pop+consume',24)+pad(L(qc),22)+L(pc));
console.log('='.repeat(70));
console.log('Reference: Queen via HTTP broker (test #5) was push 7.9/15.2, pop 7.9/15.7 ms.');
console.log('=> (Queen HTTP) - (Queen SP) = the broker/HTTP hop cost; the rest is the engine.');
" )
echo "artifacts: $PGMQ_DIR/ , $QUEEN_DIR/"
