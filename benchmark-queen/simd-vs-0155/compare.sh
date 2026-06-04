#!/usr/bin/env bash
#
# Head-to-head: current-branch simdjson broker (queen-mq:simd) vs the released
# smartnessai/queen-mq:0.15.5, on the IDENTICAL clustered producer/consumer
# harness (benchmark-queen/pgmq/run-queen.sh -> examples/long-running) and the
# same Docker resource budget, PG, and workload. Both images are arm64-native.
#
# Per run it samples queen-broker CPU (the attributable signal — the broker is
# usually PG-bound, so throughput tends to parity and CPU/msg is where the
# conversion shows). Run twice for two regimes:
#   - equal resources (default QUEEN_CPUS): CPU/msg signal
#   - broker-capped (QUEEN_CPUS=1): throughput signal (broker is the bottleneck)
#
# Usage:
#   ./compare.sh                       # default workload, QUEEN_CPUS=3
#   QUEEN_CPUS=1 DURATION=60 ./compare.sh
#   ORDER="simd 0155" ./compare.sh     # control run order
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PGMQ="$HERE/../pgmq"

SIMD_IMAGE="${SIMD_IMAGE:-queen-mq:simd}"
REL_IMAGE="${REL_IMAGE:-smartnessai/queen-mq:0.15.5}"

# Shared workload + resources (identical for both images).
export CONNECTIONS="${CONNECTIONS:-100}"
export MSGS_PER_PUSH="${MSGS_PER_PUSH:-10}"
export READ_QTY="${READ_QTY:-100}"
export NUM_PARTITIONS="${NUM_PARTITIONS:-1000}"
export DURATION="${DURATION:-60}"
export QUEEN_CPUS="${QUEEN_CPUS:-3.0}"
export QUEEN_NUM_WORKERS="${QUEEN_NUM_WORKERS:-2}"
ORDER="${ORDER:-0155 simd}"

STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="$HERE/results/$STAMP"
mkdir -p "$OUT"

echo "== simd-vs-0155 =="
echo "   workload: conns=$CONNECTIONS msgsPerPush=$MSGS_PER_PUSH popBatch=$READ_QTY parts=$NUM_PARTITIONS dur=${DURATION}s"
echo "   broker:   QUEEN_CPUS=$QUEEN_CPUS NUM_WORKERS=$QUEEN_NUM_WORKERS"
echo "   images:   simd=$SIMD_IMAGE  release=$REL_IMAGE"
echo "   order:    $ORDER"

run_one() {
  local label="$1" image="$2"
  local cpufile="$OUT/$label-brokercpu.txt"
  local runlog="$OUT/$label-run.log"
  echo
  echo "############ RUN: $label ($image) ############"

  # Sample queen-broker CPU% every 2s for the whole run (container exists only
  # while the run is up; idle samples are filtered out in the summary).
  ( while true; do
      docker stats --no-stream --format '{{.CPUPerc}}' queen-broker 2>/dev/null || true
      sleep 2
    done ) > "$cpufile" &
  local spid=$!

  ( cd "$PGMQ" && QUEEN_IMAGE="$image" QUEEN_CPUS="$QUEEN_CPUS" QUEEN_NUM_WORKERS="$QUEEN_NUM_WORKERS" \
      ./run-queen.sh "cmp-$label-$STAMP" ) 2>&1 | tee "$runlog"

  kill "$spid" 2>/dev/null || true
  ( cd "$PGMQ" && docker compose -f queen-compose.yml down -v >/dev/null 2>&1 || true )
}

for label in $ORDER; do
  case "$label" in
    simd) run_one simd "$SIMD_IMAGE" ;;
    0155) run_one 0155 "$REL_IMAGE" ;;
  esac
done

# ---- summary ----
push_of() { grep -m1 'PRODUCER:' "$1" 2>/dev/null | sed -E 's/.*push msg\/s=([0-9na]+).*/\1/'; }
pop_of()  { grep -m1 'CONSUMER:' "$1" 2>/dev/null | sed -E 's/.*pop  msg\/s=([0-9na]+).*/\1/'; }
pp50_of() { grep -m1 'PRODUCER:' "$1" 2>/dev/null | sed -E 's/.*p50=([0-9.]+)ms.*/\1/'; }
pp99_of() { grep -m1 'PRODUCER:' "$1" 2>/dev/null | sed -E 's/.*p99=([0-9.]+)ms.*/\1/'; }
cp99_of() { grep -m1 'CONSUMER:' "$1" 2>/dev/null | sed -E 's/.*p99=([0-9.]+)ms.*/\1/'; }
cpu_of()  { # mean broker CPU% over "busy" samples (>20% to skip idle setup/teardown)
  awk '{ v=$1; gsub(/%/,"",v); if (v+0>20){s+=v; n++} } END{ if(n) printf "%.0f", s/n; else print "?" }' "$1" 2>/dev/null
}

echo
echo "================= simd-vs-0155 SUMMARY ================="
printf "%-6s %12s %12s %10s %10s %10s %12s\n" "ver" "push_msg/s" "pop_msg/s" "prod_p50" "prod_p99" "cons_p99" "broker_cpu%"
for label in 0155 simd; do
  rl="$OUT/$label-run.log"; cf="$OUT/$label-brokercpu.txt"
  [ -f "$rl" ] || continue
  printf "%-6s %12s %12s %10s %10s %10s %12s\n" "$label" \
    "$(push_of "$rl")" "$(pop_of "$rl")" "$(pp50_of "$rl")" "$(pp99_of "$rl")" "$(cp99_of "$rl")" "$(cpu_of "$cf")"
done
echo "artifacts: $OUT/"
echo "======================================================="
