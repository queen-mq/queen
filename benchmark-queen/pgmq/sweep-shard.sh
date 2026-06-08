#!/usr/bin/env bash
# pgmq SHARDED sweep — spread a FIXED client load across N queues (N separate tables).
# Each client round-robins onto queue benchq_{i%N}. DIRECT connections (PGPORT=55432).
# Tests pgmq's intended scaling path: does splitting the single-table contention across
# N tables lift the single-queue ceiling, and how far before PG (CPU/WAL/locks) caps it?
set -uo pipefail
cd "$(dirname "$0")"
QUEUES="${QUEUES:-1 2 4 8 16 32}"
TOTAL="${TOTAL:-300}"            # clients per role, split across the N queues
export MODE=plain MSGS_PER_PUSH="${MSGS_PER_PUSH:-10}" READ_QTY="${READ_QTY:-100}"
export DURATION="${DURATION:-60}" PGPORT="${PGPORT:-55432}" PGHOST=localhost QUEUE=benchq
psql_pg(){ docker exec pgmq-postgres psql -U postgres -d postgres -v ON_ERROR_STOP=0 -tAc "$1" 2>/dev/null; }

echo ">>> pgmq SHARD sweep: queues=[$QUEUES] total=${TOTAL} clients/role  send=$MSGS_PER_PUSH read=$READ_QTY dur=${DURATION}s (direct)"
psql_pg "CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;" >/dev/null

for N in $QUEUES; do
  name="shard-direct-q${N}"; out="results/$name"; mkdir -p "$out"
  echo; echo "######## SHARD: ${N} queues · ${TOTAL} clients/role (~$((TOTAL/N)) per queue) ########"
  # fresh set of queues
  psql_pg "DO \$\$ DECLARE r record; BEGIN FOR r IN SELECT queue_name FROM pgmq.list_queues() WHERE queue_name LIKE 'benchq%' LOOP PERFORM pgmq.drop_queue(r.queue_name); END LOOP; END \$\$;" >/dev/null
  if [ "$N" -eq 1 ]; then psql_pg "SELECT pgmq.create('benchq');" >/dev/null
  else for i in $(seq 0 $((N-1))); do psql_pg "SELECT pgmq.create('benchq_${i}');" >/dev/null; done; fi
  # sampler: ts,cpu%,mem,active_backends  (docker stats blocks ~1-2s -> ~2s period)
  ( while true; do
      ts=$(date +%s)
      ds=$(docker stats --no-stream --format '{{.CPUPerc}},{{.MemUsage}}' pgmq-postgres 2>/dev/null)
      ab=$(docker exec pgmq-postgres psql -U postgres -d postgres -tAc "SELECT count(*) FROM pg_stat_activity WHERE state='active' AND datname='postgres'" 2>/dev/null)
      echo "${ts},${ds},${ab}"
    done ) > "$out/sampler.csv" 2>/dev/null &
  DS=$!
  QUEUE=benchq NUM_QUEUES=$N ROLE=consumer CONNECTIONS=$TOTAL node pgmq-bench.js > "$out/consumer.json" 2>"$out/consumer.err" &
  CONS_PID=$!
  sleep 2
  QUEUE=benchq NUM_QUEUES=$N ROLE=producer CONNECTIONS=$TOTAL node pgmq-bench.js > "$out/producer.json" 2>"$out/producer.err" &
  PROD_PID=$!
  wait "$PROD_PID"; wait "$CONS_PID"
  kill "$DS" 2>/dev/null || true; wait "$DS" 2>/dev/null || true
  agg=$(psql_pg "SELECT COALESCE(sum(n_dead_tup),0)||'|'||COALESCE(sum(n_live_tup),0)||'|'||pg_size_pretty(COALESCE(sum(pg_total_relation_size('pgmq.'||relname)),0)) FROM pg_stat_user_tables WHERE schemaname='pgmq' AND relname LIKE 'q_benchq%'")
  push=$(node -e "try{console.log(require('./$out/producer.json').msgPerSec)}catch(e){console.log('NA')}")
  pop=$(node -e "try{console.log(require('./$out/consumer.json').msgPerSec)}catch(e){console.log('NA')}")
  pp99=$(node -e "try{console.log(require('./$out/producer.json').latency.p99)}catch(e){console.log('NA')}")
  cp99=$(node -e "try{console.log(require('./$out/consumer.json').latency.p99)}catch(e){console.log('NA')}")
  echo "RESULT q=${N} push=${push} pop=${pop} push_p99=${pp99}ms pop_p99=${cp99}ms dead|live|size=${agg}"
  sleep 3
done
echo; echo ">>> shard sweep complete. Artifacts in results/shard-direct-q*/"
