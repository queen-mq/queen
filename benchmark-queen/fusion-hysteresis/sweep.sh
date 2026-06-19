#!/usr/bin/env bash
# Drive the open-loop pacer across a ladder of offered rates, UP then DOWN, so we
# can see (a) the fusion amortization curve and (b) whether the up-sweep and
# down-sweep trace the SAME path or a hysteresis loop. Per step: warmup, snapshot
# PG (pre), measure while sampling PG CPU, snapshot PG (post). analyze.mjs turns
# the raw snapshots into metrics + an HTML report.
#
# Prereqs (start these first, in separate terminals):
#   1. docker compose up -d          # the pinned Postgres
#   2. ./run-broker.sh               # the locally-built queen-server
#
# Then:  ./sweep.sh                   # ~10 min with the default ladder
#
# Env:
#   SERVER_URL    default http://localhost:6632
#   QUEUE_NAME    default fusion-test
#   PARTITIONS    default 8
#   WARMUP        seconds discarded per step (default 8)
#   MEASURE       seconds measured per step  (default 20)
#   SETTLE        idle seconds between steps (default 4)
#   RATES_UP      space-separated offered rates, ascending
#   RATES_DOWN    descending (default: RATES_UP reversed, minus the peak)
#   OUT_DIR       default ./runs/<unset-stamp> (pass one in for a stable name)
#   PG_CONTAINER  default queen-fusion-pg
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVER_URL="${SERVER_URL:-http://localhost:6632}"
QUEUE_NAME="${QUEUE_NAME:-fusion-test}"
PARTITIONS="${PARTITIONS:-8}"
WARMUP="${WARMUP:-8}"
MEASURE="${MEASURE:-20}"
SETTLE="${SETTLE:-4}"
PG_CONTAINER="${PG_CONTAINER:-queen-fusion-pg}"
MAX_INFLIGHT="${MAX_INFLIGHT:-4000}"
PAYLOAD_SIZE_BYTES="${PAYLOAD_SIZE_BYTES:-0}"

RATES_UP="${RATES_UP:-50 100 200 500 1000 2000 5000 10000 20000 40000 80000}"
if [[ -z "${RATES_DOWN:-}" ]]; then
  # reverse RATES_UP and drop the first (peak) so we don't measure it twice
  RATES_DOWN="$(echo "$RATES_UP" | tr ' ' '\n' | tail -r 2>/dev/null | tail -n +2 | tr '\n' ' ' || \
               echo "$RATES_UP" | tr ' ' '\n' | tac | tail -n +2 | tr '\n' ' ')"
fi

OUT_DIR="${OUT_DIR:-$HERE/runs/run-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$OUT_DIR"
MANIFEST="$OUT_DIR/manifest.jsonl"
: > "$MANIFEST"

echo "[sweep] out=$OUT_DIR"
echo "[sweep] up:   $RATES_UP"
echo "[sweep] down: $RATES_DOWN"
echo "[sweep] warmup=${WARMUP}s measure=${MEASURE}s settle=${SETTLE}s partitions=$PARTITIONS"

# sanity: PG container up, broker reachable
if ! docker ps --format '{{.Names}}' | grep -qx "$PG_CONTAINER"; then
  echo "[sweep] ERROR: container '$PG_CONTAINER' not running. Run: docker compose up -d" >&2; exit 1
fi
if ! curl -fsS "$SERVER_URL/health" >/dev/null 2>&1; then
  echo "[sweep] ERROR: broker not reachable at $SERVER_URL. Run: ./run-broker.sh" >&2; exit 1
fi

run_step() {
  local phase="$1" rate="$2" idx="$3"
  local dir="$OUT_DIR/$(printf '%03d' "$idx")-${phase}-${rate}"
  mkdir -p "$dir"
  local dur=$(( WARMUP + MEASURE ))

  echo "[sweep] --- $phase rate=$rate (warmup ${WARMUP}s + measure ${MEASURE}s) ---"

  # start the pacer for the full warmup+measure window
  RATE="$rate" DURATION="$dur" SERVER_URL="$SERVER_URL" QUEUE_NAME="$QUEUE_NAME" \
    PARTITIONS="$PARTITIONS" MAX_INFLIGHT="$MAX_INFLIGHT" PAYLOAD_SIZE_BYTES="$PAYLOAD_SIZE_BYTES" \
    OUTPUT_FILE="$dir/pacer.json" \
    node "$HERE/pacer.mjs" >"$dir/pacer.log" 2>&1 &
  local pacer_pid=$!

  sleep "$WARMUP"

  # pre snapshot + start CPU sampler over the measure window
  OUT="$dir/pre.json" PG_CONTAINER="$PG_CONTAINER" bash "$HERE/snapshot-pg.sh" pre >/dev/null
  curl -fsS "$SERVER_URL/metrics/prometheus" >"$dir/prom-pre.txt" 2>/dev/null || true

  : > "$dir/pg-cpu.txt"
  ( while :; do
      docker stats --no-stream --format '{{.CPUPerc}}' "$PG_CONTAINER" 2>/dev/null \
        | tr -d '%' >> "$dir/pg-cpu.txt"
      sleep 2
    done ) &
  local sampler_pid=$!
  disown "$sampler_pid" 2>/dev/null || true  # keep bash from printing "Terminated" when we kill it

  sleep "$MEASURE"

  kill "$sampler_pid" 2>/dev/null || true
  wait "$sampler_pid" 2>/dev/null || true
  OUT="$dir/post.json" PG_CONTAINER="$PG_CONTAINER" bash "$HERE/snapshot-pg.sh" post >/dev/null
  curl -fsS "$SERVER_URL/metrics/prometheus" >"$dir/prom-post.txt" 2>/dev/null || true

  wait "$pacer_pid" 2>/dev/null || true

  printf '{"idx":%d,"phase":"%s","rate":%d,"dir":"%s"}\n' \
    "$idx" "$phase" "$rate" "$dir" >> "$MANIFEST"

  sleep "$SETTLE"
}

idx=0
for r in $RATES_UP;   do run_step up   "$r" "$idx"; idx=$((idx+1)); done
for r in $RATES_DOWN; do run_step down "$r" "$idx"; idx=$((idx+1)); done

echo "[sweep] done. analyzing -> $OUT_DIR/results.csv + report.html"
node "$HERE/analyze.mjs" "$OUT_DIR"
echo "[sweep] open: $OUT_DIR/report.html"
