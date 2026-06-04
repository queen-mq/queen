#!/usr/bin/env bash
#
# Robust head-to-head: queen-mq:simd vs smartnessai/queen-mq:0.15.5, using the
# CLOSED-LOOP client (benchmark-queen/pgmq/queen-bench.js) which exits cleanly
# (the autocannon-cluster examples can hang on exit). Identical resources/PG/
# workload. Reports authoritative push/pop msg/s from /api/v1/status plus the
# client's own msg/s + latency, and mean queen-broker CPU%.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PGMQ="$HERE/../pgmq"
SERVER_URL="http://localhost:6633"

SIMD_IMAGE="${SIMD_IMAGE:-queen-mq:simd}"
REL_IMAGE="${REL_IMAGE:-smartnessai/queen-mq:0.15.5}"

export QUEEN_CPUS="${QUEEN_CPUS:-3.0}"
export QUEEN_NUM_WORKERS="${QUEEN_NUM_WORKERS:-2}"
CONNECTIONS="${CONNECTIONS:-50}"          # per role
DURATION="${DURATION:-40}"
MSGS_PER_PUSH="${MSGS_PER_PUSH:-10}"
READ_QTY="${READ_QTY:-100}"
NUM_PARTITIONS="${NUM_PARTITIONS:-1000}"
QUEUE="cmpq"
ORDER="${ORDER:-0155 simd}"

export NVM_DIR="${NVM_DIR:-$HOME/.nvm}"
load_node() { [ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh" && nvm use 22 >/dev/null 2>&1; return 0; }
# queen-bench.js needs axios; reuse examples/ node_modules.
export NODE_PATH="$HERE/../../examples/node_modules"

STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="$HERE/results/bench-$STAMP"; mkdir -p "$OUT"
echo "== compare-bench (closed-loop) =="
echo "   conns/role=$CONNECTIONS dur=${DURATION}s msgsPerPush=$MSGS_PER_PUSH popBatch=$READ_QTY parts=$NUM_PARTITIONS CPUS=$QUEEN_CPUS workers=$QUEEN_NUM_WORKERS"

run_one() {
  local label="$1" image="$2"
  local cpufile="$OUT/$label-cpu.txt"
  echo; echo "############ $label ($image) ############"
  ( cd "$PGMQ" && QUEEN_IMAGE="$image" QUEEN_CPUS="$QUEEN_CPUS" QUEEN_NUM_WORKERS="$QUEEN_NUM_WORKERS" \
      docker compose -f queen-compose.yml up -d >/dev/null 2>&1 )
  for i in $(seq 1 90); do curl -sf "$SERVER_URL/api/v1/status" >/dev/null 2>&1 && break; sleep 1; done

  ( while true; do docker stats --no-stream --format '{{.CPUPerc}}' queen-broker 2>/dev/null || true; sleep 2; done ) > "$cpufile" &
  local spid=$!

  load_node
  ROLE=producer SERVER_URL="$SERVER_URL" QUEUE="$QUEUE" CONNECTIONS="$CONNECTIONS" DURATION="$DURATION" \
    MSGS_PER_PUSH="$MSGS_PER_PUSH" NUM_PARTITIONS="$NUM_PARTITIONS" \
    node "$PGMQ/queen-bench.js" > "$OUT/$label-producer.json" 2>"$OUT/$label-producer.err" &
  local ppid=$!
  sleep 2
  ROLE=consumer SERVER_URL="$SERVER_URL" QUEUE="$QUEUE" CONNECTIONS="$CONNECTIONS" DURATION="$DURATION" \
    READ_QTY="$READ_QTY" NUM_PARTITIONS="$NUM_PARTITIONS" \
    node "$PGMQ/queen-bench.js" > "$OUT/$label-consumer.json" 2>"$OUT/$label-consumer.err" &
  local cpid=$!
  wait "$ppid"; wait "$cpid"

  sleep 11   # let StatsService aggregate (10s interval) so /status is fresh
  curl -sf "$SERVER_URL/api/v1/status" > "$OUT/$label-status.json" 2>/dev/null || true
  kill "$spid" 2>/dev/null || true
  ( cd "$PGMQ" && docker compose -f queen-compose.yml down -v >/dev/null 2>&1 || true )
}

for label in $ORDER; do
  case "$label" in
    simd) run_one simd "$SIMD_IMAGE" ;;
    0155) run_one 0155 "$REL_IMAGE" ;;
  esac
done

echo; echo "================= compare-bench SUMMARY (CPUS=$QUEEN_CPUS) ================="
printf "%-6s %12s %12s %12s %10s %10s %12s\n" "ver" "push/s(cl)" "pop/s(cl)" "status_push/s" "prod_p99" "cons_p99" "broker_cpu%"
for label in 0155 simd; do
  pf="$OUT/$label-producer.json"; cf="$OUT/$label-consumer.json"; sf="$OUT/$label-status.json"; cpuf="$OUT/$label-cpu.txt"
  [ -f "$pf" ] || continue
  read PUSH_CL PROD_P99 < <(python3 -c "import json;d=json.load(open('$pf'));print(d.get('msgPerSec',0), d.get('latency',{}).get('p99',0))" 2>/dev/null || echo "0 0")
  read POP_CL CONS_P99 < <(python3 -c "import json;d=json.load(open('$cf'));print(d.get('msgPerSec',0), d.get('latency',{}).get('p99',0))" 2>/dev/null || echo "0 0")
  SPUSH=$(python3 -c "import json;d=json.load(open('$sf'));m=d.get('messages',{});print(round((m.get('total',0))/$DURATION))" 2>/dev/null || echo na)
  CPU=$(awk '{v=$1;gsub(/%/,"",v); if(v+0>10){s+=v;n++}} END{if(n)printf "%.0f",s/n; else print "?"}' "$cpuf" 2>/dev/null)
  printf "%-6s %12s %12s %12s %10s %10s %12s\n" "$label" "$PUSH_CL" "$POP_CL" "$SPUSH" "$PROD_P99" "$CONS_P99" "$CPU"
done
echo "artifacts: $OUT/"
echo "==========================================================================="
