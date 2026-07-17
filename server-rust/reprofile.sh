#!/usr/bin/env bash
# reprofile.sh — fair re-profile: C++ broker (all machinery on, its default) vs
# the Rust hot-path WITH QUEEN_FULL_FEATURES=1 (per-queue metric attribution +
# per-message pop lag + per-push config-cache lookup + background metrics-flush +
# retention). Same PG, same procedures, same goload, same tuning. Brokers run
# SEQUENTIALLY on a freshly-reset schema. perf stat captures the CPU micro-arch
# picture (IPC, context-switches, cpu-migrations) per broker under steady load.
set -uo pipefail
NET=qbench; PG=qbench-pg; GOLOAD=/root/goload
DURATION="${DURATION:-60}"; PROF_AT="${PROF_AT:-25}"; PROF_FOR="${PROF_FOR:-20}"

sysctl -w kernel.perf_event_paranoid=-1 >/dev/null 2>&1 || true
sysctl -w kernel.kptr_restrict=0 >/dev/null 2>&1 || true

q(){ docker exec "$PG" psql -U postgres -tAc "$1" </dev/null 2>/dev/null; }
reset(){
  q "DROP SCHEMA IF EXISTS queen CASCADE;" >/dev/null
  docker cp /root/queen/lib/schema/schema.sql "$PG":/tmp/s.sql >/dev/null
  docker exec -i "$PG" psql -U postgres -q -f /tmp/s.sql </dev/null >/dev/null 2>&1
  for f in $(ls /root/queen/lib/schema/procedures/*.sql|sort); do
    docker cp "$f" "$PG":/tmp/p.sql >/dev/null
    docker exec -i "$PG" psql -U postgres -q -f /tmp/p.sql </dev/null >/dev/null 2>&1
  done
  q "VACUUM ANALYZE;" >/dev/null 2>&1
}

run(){
  local label="$1" img="$2" port="$3"; shift 3
  reset
  local nm="b$port"
  docker rm -f "$nm" >/dev/null 2>&1
  docker run -d --name "$nm" --network "$NET" --ulimit nofile=65535:65535 \
    -p "$port":6632 -e PG_HOST="$PG" -e PG_PASSWORD=postgres "$@" "$img" >/dev/null
  for i in $(seq 1 90); do curl -sf "http://localhost:$port/api/v1/status" >/dev/null 2>&1 && break; sleep 1; done
  local pid; pid=$(docker inspect -f '{{.State.Pid}}' "$nm")
  local C0; C0=$(q "select xact_commit from pg_stat_database where datname='postgres';")
  "$GOLOAD" -mode max -url "http://localhost:$port" -queue "x$RANDOM" \
    -partitions 100 -producers 300 -consumers 200 -push-batch 10 -pop-batch 200 \
    -pop-partitions 5 -pop-wait=true -pop-timeout=2000 -payload 256 \
    -duration "$DURATION" -report 100 >/tmp/load.log 2>&1 &
  local loadpid=$!
  sleep "$PROF_AT"
  echo "===== perf stat: $label (pid=$pid, ${PROF_FOR}s steady) ====="
  perf stat -p "$pid" \
    -e task-clock,context-switches,cpu-migrations,cycles,instructions \
    -- sleep "$PROF_FOR" 2>/tmp/perf.txt
  cat /tmp/perf.txt
  local st; st=$(docker stats --no-stream --format '{{.CPUPerc}} / {{.MemUsage}}' "$nm")
  local pg; pg=$(docker stats --no-stream --format '{{.CPUPerc}}' "$PG")
  wait "$loadpid"
  local C1; C1=$(q "select xact_commit from pg_stat_database where datname='postgres';")
  local f; f=$(tail -1 /tmp/load.log)
  local push pop
  push=$(( $(echo "$f"|grep -o 'pushed=[0-9]*'|cut -d= -f2)/DURATION ))
  pop=$(( $(echo "$f"|grep -o 'popped=[0-9]*'|cut -d= -f2)/DURATION ))
  echo "----- $label result -----"
  echo "  throughput: push=${push}/s pop=${pop}/s | commits/s=$(( (C1-C0)/DURATION ))"
  echo "  broker[$st]  pgCPU=$pg"
  echo
  docker rm -f "$nm" >/dev/null 2>&1
}

echo "###### DURATION=$DURATION  profile window: +${PROF_AT}s for ${PROF_FOR}s ######"
run "C++ (0.16.0, all machinery on)" smartnessai/queen-mq:0.16.0 6634 \
  -e DB_POOL_SIZE=100 -e NUM_WORKERS=10 -e QUEEN_CONCURRENCY_MODE=vegas \
  -e QUEEN_POP_PREFERRED_BATCH_SIZE=40 -e QUEEN_PUSH_PREFERRED_BATCH_SIZE=100
sleep 5
run "Rust FULL_FEATURES=1 (attribution+lag+cache+retention+flush)" queen-hotpath-rust:latest 6635 \
  -e QUEEN_FULL_FEATURES=1 -e QUEEN_GLOBAL_CONCURRENCY=72 -e QUEEN_PUSH_MAX_CONCURRENT=24 \
  -e QUEEN_POP_MAX_CONCURRENT=40 -e QUEEN_POP_PREFERRED_BATCH_SIZE=20
echo "###### done ######"
