#!/usr/bin/env bash
# fusion-sweep.sh — PROVE the throughput shortfall is fusion, not CPU.
# Rust stays FULL_FEATURES=1 with IDENTICAL concurrency/pools; ONLY the fusion
# knobs (preferred batch size + max hold) change across points. If the thesis
# holds, commits/s must fall as fusion strengthens and throughput must rise
# monotonically toward/over C++, while CPU-per-message stays low.
# A C++ anchor is measured in the same session on the same fresh schema.
set -uo pipefail
NET=qbench; PG=qbench-pg; GOLOAD=/root/goload
DURATION="${DURATION:-40}"; SNAP_AT="${SNAP_AT:-25}"

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

# run <label> <img> <port> -- <env...>
run(){
  local label="$1" img="$2" port="$3"; shift 3
  reset
  local nm="b$port"
  docker rm -f "$nm" >/dev/null 2>&1
  docker run -d --name "$nm" --network "$NET" --ulimit nofile=65535:65535 \
    -p "$port":6632 -e PG_HOST="$PG" -e PG_PASSWORD=postgres "$@" "$img" >/dev/null
  for i in $(seq 1 90); do curl -sf "http://localhost:$port/api/v1/status" >/dev/null 2>&1 && break; sleep 1; done
  local C0; C0=$(q "select xact_commit from pg_stat_database where datname='postgres';")
  "$GOLOAD" -mode max -url "http://localhost:$port" -queue "x$RANDOM" \
    -partitions 100 -producers 300 -consumers 200 -push-batch 10 -pop-batch 200 \
    -pop-partitions 5 -pop-wait=true -pop-timeout=2000 -payload 256 \
    -duration "$DURATION" -report 100 >/tmp/load.log 2>&1 &
  local loadpid=$!
  sleep "$SNAP_AT"
  local cpu; cpu=$(docker stats --no-stream --format '{{.CPUPerc}}' "$nm" | tr -d '%')
  local pgcpu; pgcpu=$(docker stats --no-stream --format '{{.CPUPerc}}' "$PG" | tr -d '%')
  wait "$loadpid"
  local C1; C1=$(q "select xact_commit from pg_stat_database where datname='postgres';")
  local f; f=$(tail -1 /tmp/load.log)
  local push pop commits fusion cpumsg
  push=$(( $(echo "$f"|grep -o 'pushed=[0-9]*'|cut -d= -f2)/DURATION ))
  pop=$(( $(echo "$f"|grep -o 'popped=[0-9]*'|cut -d= -f2)/DURATION ))
  commits=$(( (C1-C0)/DURATION ))
  awk -v l="$label" -v push="$push" -v pop="$pop" -v c="$commits" -v cpu="$cpu" -v pg="$pgcpu" 'BEGIN{
    tot=push+pop; fus=(c>0? push/c : 0); cpumsg=(tot>0? (cpu/100.0)/tot*1e6 : 0);
    printf "%-34s | push=%6d pop=%6d /s | commits/s=%5d fusion=%5.1f | brokerCPU=%6.1f%% CPUus/msg=%5.2f pgCPU=%7.1f%%\n",
      l, push, pop, c, fus, cpu, cpumsg, pg;
  }'
  docker rm -f "$nm" >/dev/null 2>&1
}

RUST=queen-hotpath-rust:latest
RBASE="-e QUEEN_FULL_FEATURES=1 -e QUEEN_GLOBAL_CONCURRENCY=72 -e QUEEN_PUSH_MAX_CONCURRENT=24 -e QUEEN_POP_MAX_CONCURRENT=40"

echo "###### fusion sweep (Rust FULL_FEATURES, only fusion knobs change) DURATION=$DURATION ######"
# point A = last run's tuning (weak fusion)
run "Rust A pushPref50/h20 popPref20/h5"  "$RUST" 6641 $RBASE \
  -e QUEEN_PUSH_PREFERRED_BATCH_SIZE=50  -e QUEEN_PUSH_MAX_HOLD_MS=20 \
  -e QUEEN_POP_PREFERRED_BATCH_SIZE=20   -e QUEEN_POP_MAX_HOLD_MS=5
run "Rust B pushPref100/h25 popPref40/h10" "$RUST" 6642 $RBASE \
  -e QUEEN_PUSH_PREFERRED_BATCH_SIZE=100 -e QUEEN_PUSH_MAX_HOLD_MS=25 \
  -e QUEEN_POP_PREFERRED_BATCH_SIZE=40   -e QUEEN_POP_MAX_HOLD_MS=10
run "Rust C pushPref200/h40 popPref80/h15" "$RUST" 6643 $RBASE \
  -e QUEEN_PUSH_PREFERRED_BATCH_SIZE=200 -e QUEEN_PUSH_MAX_HOLD_MS=40 \
  -e QUEEN_POP_PREFERRED_BATCH_SIZE=80   -e QUEEN_POP_MAX_HOLD_MS=15
run "Rust D pushPref300/h50 popPref150/h20" "$RUST" 6644 $RBASE \
  -e QUEEN_PUSH_PREFERRED_BATCH_SIZE=300 -e QUEEN_PUSH_MAX_HOLD_MS=50 \
  -e QUEEN_POP_PREFERRED_BATCH_SIZE=150  -e QUEEN_POP_MAX_HOLD_MS=20
echo "--- anchor ---"
run "C++ 0.16.0 vegas (anchor)" smartnessai/queen-mq:0.16.0 6634 \
  -e DB_POOL_SIZE=100 -e NUM_WORKERS=10 -e QUEEN_CONCURRENCY_MODE=vegas \
  -e QUEEN_POP_PREFERRED_BATCH_SIZE=40 -e QUEEN_PUSH_PREFERRED_BATCH_SIZE=100
echo "###### done ######"
