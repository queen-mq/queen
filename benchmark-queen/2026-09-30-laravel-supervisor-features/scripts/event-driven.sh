#!/usr/bin/env bash
# One Rust supervisor at the production cadence (3 s poll, 3 s cooldown) with
# fast_scale_up, idle at one worker, then a burst of 600 jobs; with and
# without event_driven. Samples the running workers every quarter second
# from the moment the burst starts.
# Usage: event-driven.sh <poll|event> <run> <app dir> <binary> <csv>
set -u
MODE=$1; RUN=$2; APP=$3; BIN=$4
# Absolute, since the script runs from the application directory.
OUT=$(cd "$(dirname "$5")" && pwd)/$(basename "$5")
Q=eventdriven$(openssl rand -hex 3)
export BENCH_CONNECTION=queen BENCH_PROFILE=auto BENCH_QUEUE=$Q BENCH_GROUP=g$Q BENCH_WORKERS=20 BENCH_MIN_WORKERS=1 BENCH_MAX_WORKERS=20 \
  BENCH_STRATEGY=size BENCH_TARGET_JOBS_PER_PROCESS=10 BENCH_SCALE_DOWN_DELAY=10 BENCH_BALANCE_MAX_SHIFT=1 BENCH_BALANCE_COOLDOWN=3 \
  BENCH_POLL_INTERVAL=3 BENCH_RESULTS_DIRECTORY=/tmp/queen-bench-results QUEEN_URL=${QUEEN_URL:-http://127.0.0.1:6632} \
  BENCH_TIMEOUT=30 BENCH_RETRY_AFTER=60 QUEEN_SUPERVISOR_FAST_SCALE_UP=true \
  QUEEN_SUPERVISOR_EVENT_DRIVEN=$([ "$MODE" = event ] && echo true || echo false) \
  QUEEN_SUPERVISOR_STATE_DIRECTORY=/tmp/queen-event-driven
mkdir -p /tmp/queen-bench-results; rm -rf /tmp/queen-event-driven
cd "$APP"
# The queue exists before the supervisor starts, as a deployed one does.
php artisan bench:dispatch --jobs=1 --sleep-ms=10 --connection=queen --queue=$Q >/dev/null
"$BIN" --php php --artisan artisan >/dev/null 2>&1 & M=$!
sleep 8
start=$(python3 -c 'import time; print(time.time())')
php artisan bench:dispatch --jobs=600 --sleep-ms=5000 --connection=queen --queue=$Q >/dev/null &
for i in $(seq 1 100); do
  sleep 0.25
  python3 - "$MODE" "$RUN" "$start" >> "$OUT" <<'PY'
import json, sys, time
mode, run, start = sys.argv[1], sys.argv[2], float(sys.argv[3])
try:
    running = json.load(open("/tmp/queen-event-driven/status.json"))["pool_status"][0]["running"]
except (OSError, ValueError, KeyError, IndexError):
    running = 0
print(f"{mode},{run},{time.time() - start:.2f},{running}")
PY
done
wait %2 2>/dev/null
kill -TERM $M; wait $M 2>/dev/null
