#!/usr/bin/env bash
# One Rust supervisor facing a sudden backlog, stepping by balance_max_shift=1
# or with fast_scale_up. Samples the running workers every half second.
# Usage: scale-up.sh <step|fast> <app dir> <binary> <csv>
set -u
MODE=$1; APP=$2; BIN=$3
# Absolute, since the script runs from the application directory.
OUT=$(cd "$(dirname "$4")" && pwd)/$(basename "$4")
Q=scaleup$(openssl rand -hex 3)
export BENCH_CONNECTION=queen BENCH_PROFILE=auto BENCH_QUEUE=$Q BENCH_GROUP=g$Q BENCH_WORKERS=20 BENCH_MIN_WORKERS=1 BENCH_MAX_WORKERS=20 \
  BENCH_STRATEGY=size BENCH_TARGET_JOBS_PER_PROCESS=10 BENCH_SCALE_DOWN_DELAY=0 BENCH_BALANCE_MAX_SHIFT=1 BENCH_BALANCE_COOLDOWN=1 \
  BENCH_POLL_INTERVAL=1 BENCH_RESULTS_DIRECTORY=/tmp/queen-bench-results QUEEN_URL=${QUEEN_URL:-http://127.0.0.1:6632} \
  BENCH_TIMEOUT=30 BENCH_RETRY_AFTER=60 QUEEN_SUPERVISOR_FAST_SCALE_UP=$([ "$MODE" = fast ] && echo true || echo false) \
  QUEEN_SUPERVISOR_STATE_DIRECTORY=/tmp/queen-scale-up
mkdir -p /tmp/queen-bench-results; rm -rf /tmp/queen-scale-up
cd "$APP"
"$BIN" --php php --artisan artisan >/dev/null 2>&1 & M=$!
sleep 4
php artisan bench:dispatch --jobs=600 --sleep-ms=5000 --connection=queen --queue=$Q >/dev/null
start=$(python3 -c 'import time; print(time.time())')
for i in $(seq 1 60); do
  sleep 0.5
  python3 - "$MODE" "$start" >> "$OUT" <<'PY'
import json, sys, time
mode, start = sys.argv[1], float(sys.argv[2])
try:
    running = json.load(open("/tmp/queen-scale-up/status.json"))["pool_status"][0]["running"]
except (OSError, ValueError, KeyError, IndexError):
    running = 0
print(f"{mode},{time.time() - start:.1f},{running}")
PY
done
kill -TERM $M; wait $M 2>/dev/null
