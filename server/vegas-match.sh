#!/usr/bin/env bash
# vegas-match.sh — set the concurrency vegas stabilizes on in C++ (~25 in-flight
# per active worker: ~25 push, ~25 pop+ack, ~50 total DB ops) into Rust, and see
# if Rust's per-message efficiency edge turns into equal/greater throughput.
# C++ anchor measured in the same session on the same fresh schema.
set -uo pipefail
NET=qbench; PG=qbench-pg; GOLOAD=/root/goload
DURATION="${DURATION:-45}"; SNAP_AT="${SNAP_AT:-28}"

q(){ docker exec "$PG" psql -U postgres -tAc "$1" </dev/null 2>/dev/null; }
reset(){
  q "select pg_terminate_backend(pid) from pg_stat_activity where pid<>pg_backend_pid() and state<>'idle';" >/dev/null 2>&1
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
  local nm="b$port"; docker rm -f "$nm" >/dev/null 2>&1
  docker run -d --name "$nm" --network "$NET" --ulimit nofile=65535:65535 \
    -p "$port":6632 -e PG_HOST="$PG" -e PG_PASSWORD=postgres "$@" "$img" >/dev/null
  for i in $(seq 1 90); do curl -sf "http://localhost:$port/api/v1/status" >/dev/null 2>&1 && break; sleep 1; done
  local C0; C0=$(q "select xact_commit from pg_stat_database where datname='postgres';")
  "$GOLOAD" -mode max -url "http://localhost:$port" -queue "x$RANDOM" \
    -partitions 100 -producers 300 -consumers 200 -push-batch 10 -pop-batch 200 \
    -pop-partitions 5 -pop-wait=true -pop-timeout=2000 -payload 256 \
    -duration "$DURATION" -report 100 >/tmp/load.log 2>&1 &
  local lp=$!
  sleep "$SNAP_AT"
  local cpu; cpu=$(docker stats --no-stream --format '{{.CPUPerc}}' "$nm"|tr -d '%')
  local pgcpu; pgcpu=$(docker stats --no-stream --format '{{.CPUPerc}}' "$PG"|tr -d '%')
  wait "$lp"
  local C1; C1=$(q "select xact_commit from pg_stat_database where datname='postgres';")
  local f; f=$(tail -1 /tmp/load.log)
  local push pop commits
  push=$(( $(echo "$f"|grep -o 'pushed=[0-9]*'|cut -d= -f2)/DURATION ))
  pop=$(( $(echo "$f"|grep -o 'popped=[0-9]*'|cut -d= -f2)/DURATION ))
  commits=$(( (C1-C0)/DURATION ))
  awk -v l="$label" -v push="$push" -v pop="$pop" -v c="$commits" -v cpu="$cpu" -v pg="$pgcpu" 'BEGIN{
    tot=push+pop; cpumsg=(tot>0?(cpu/100.0)/tot*1e6:0);
    printf "%-40s | push=%6d pop=%6d /s | commits/s=%5d | brokerCPU=%6.1f%% CPUus/msg=%5.2f pgCPU=%7.1f%%\n",
      l,push,pop,c,cpu,cpumsg,pg }'
  docker rm -f "$nm" >/dev/null 2>&1
}

RUST=queen-hotpath-rust:latest
echo "###### vegas-match (Rust @ C++ concurrency) DURATION=$DURATION ######"
# V1: mirror vegas concurrency (~25 push / ~25 pop, 50 total) + Rust best fusion (point C)
run "Rust V1 conc25/25 fusionC" "$RUST" 6651 \
  -e QUEEN_FULL_FEATURES=1 -e QUEEN_GLOBAL_CONCURRENCY=50 -e QUEEN_PUSH_MAX_CONCURRENT=25 -e QUEEN_POP_MAX_CONCURRENT=25 -e QUEEN_ACK_MAX_CONCURRENT=16 \
  -e QUEEN_PUSH_PREFERRED_BATCH_SIZE=200 -e QUEEN_PUSH_MAX_HOLD_MS=40 -e QUEEN_POP_PREFERRED_BATCH_SIZE=80 -e QUEEN_POP_MAX_HOLD_MS=15
# V2: mirror C++ regime — low push fusion (like batchEfficiency=10) + conc 25/25
run "Rust V2 conc25/25 pushFusionOff" "$RUST" 6652 \
  -e QUEEN_FULL_FEATURES=1 -e QUEEN_GLOBAL_CONCURRENCY=50 -e QUEEN_PUSH_MAX_CONCURRENT=25 -e QUEEN_POP_MAX_CONCURRENT=25 -e QUEEN_ACK_MAX_CONCURRENT=16 \
  -e QUEEN_PUSH_PREFERRED_BATCH_SIZE=10 -e QUEEN_PUSH_MAX_HOLD_MS=2 -e QUEEN_POP_PREFERRED_BATCH_SIZE=80 -e QUEEN_POP_MAX_HOLD_MS=15
# V3: give push more slots (C++ push worker is queue-bound) + fusion C
run "Rust V3 conc40push/25pop fusionC" "$RUST" 6653 \
  -e QUEEN_FULL_FEATURES=1 -e QUEEN_GLOBAL_CONCURRENCY=72 -e QUEEN_PUSH_MAX_CONCURRENT=40 -e QUEEN_POP_MAX_CONCURRENT=25 -e QUEEN_ACK_MAX_CONCURRENT=16 \
  -e QUEEN_PUSH_PREFERRED_BATCH_SIZE=200 -e QUEEN_PUSH_MAX_HOLD_MS=40 -e QUEEN_POP_PREFERRED_BATCH_SIZE=80 -e QUEEN_POP_MAX_HOLD_MS=15
echo "--- anchor ---"
run "C++ 0.16.0 vegas (anchor)" smartnessai/queen-mq:0.16.0 6634 \
  -e DB_POOL_SIZE=100 -e NUM_WORKERS=10 -e QUEEN_CONCURRENCY_MODE=vegas \
  -e QUEEN_POP_PREFERRED_BATCH_SIZE=40 -e QUEEN_PUSH_PREFERRED_BATCH_SIZE=100
echo "###### done ######"
