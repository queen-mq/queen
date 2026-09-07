#!/usr/bin/env bash
# Proof of the 0.15.2 load-adaptive latency bypass.
# Same broker config (PUSH_MAX_HOLD_MS=20, native arm64), producer concurrency
# sweep, with the gate ON (threshold=1) then a concurrency=1 control with the
# gate OFF (threshold=0). Shows: low latency at low load AND batching under load,
# from ONE knob — without lowering the production hold.
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; cd "$SCRIPT_DIR"
IMG="${QUEEN_IMAGE:-queen-mq:arm64-local-0.15.2}"
STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="results/adaptive-$STAMP"; mkdir -p "$OUT"
export NVM_DIR="${NVM_DIR:-$HOME/.nvm}"
load_node() { [ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh" && nvm use 24 >/dev/null 2>&1; return 0; }

# Run a producer-only measurement at a given concurrency; prints "msg/s p50 p99".
run_point() {
  local conns="$1" dur="${2:-25}"
  ( load_node; SERVER_URL=http://localhost:6633 QUEUE=bench CONNECTIONS=$conns MSGS_PER_PUSH=1 \
    NUM_PARTITIONS=1000 DURATION=$dur node queen-bench.js ) 2>/dev/null \
  | ( load_node; node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{const j=JSON.parse(s);console.log(j.msgPerSec, j.latency.p50, j.latency.p99)})" )
}

bring_up() { # $1=push_hold $2=threshold
  docker compose -f queen-compose.yml down -v >/dev/null 2>&1 || true
  QUEEN_IMAGE="$IMG" QUEEN_CPUS=3 QUEEN_NUM_WORKERS=2 QUEEN_PUSH_MAX_HOLD_MS="$1" \
    QUEEN_PUSH_BATCH_INFLIGHT_THRESHOLD="$2" docker compose -f queen-compose.yml up -d >/dev/null 2>&1
  for i in $(seq 1 90); do curl -sf http://localhost:6633/api/v1/status >/dev/null 2>&1 && break; sleep 1; done
}

echo "##### ADAPTIVE BYPASS PROOF (image=$IMG, PUSH_MAX_HOLD_MS=20)"
echo

echo "### Gate ON (QUEEN_PUSH_BATCH_INFLIGHT_THRESHOLD=1, default) — producer concurrency sweep"
bring_up 20 1
printf "%-14s %-14s %-12s %s\n" "connections" "push msg/s" "p50 (ms)" "p99 (ms)"
for C in 1 5 20 100; do
  read MS P50 P99 < <(run_point "$C" 25)
  printf "%-14s %-14s %-12s %s\n" "$C" "$MS" "$P50" "$P99" | tee -a "$OUT/gate-on.txt"
done

echo
echo "### Gate OFF (QUEEN_PUSH_BATCH_INFLIGHT_THRESHOLD=0) — concurrency=1 control (old behavior)"
bring_up 20 0
read MS P50 P99 < <(run_point 1 25)
printf "%-14s %-14s %-12s %s\n" "1 (gate off)" "$MS" "$P50" "$P99" | tee "$OUT/gate-off.txt"

docker compose -f queen-compose.yml down -v >/dev/null 2>&1 || true
echo
echo "Expected: gate ON @ conn=1 ~1.5ms (fires immediately though hold=20); rises"
echo "with concurrency as batching engages. Gate OFF @ conn=1 ~21ms (hold honored)."
echo "artifacts: $OUT/"
