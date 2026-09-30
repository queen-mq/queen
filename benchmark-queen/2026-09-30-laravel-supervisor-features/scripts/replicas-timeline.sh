#!/usr/bin/env bash
# Two Rust supervisors on one queue and consumer group, as two pods would run,
# with and without coordination. Samples both status documents every second.
# Usage: replicas-timeline.sh <coordinated|uncoordinated> <app dir> <binary> <csv>
set -u
MODE=$1; APP=$2; BIN=$3
# Absolute, since the script runs from the application directory.
OUT=$(cd "$(dirname "$4")" && pwd)/$(basename "$4")
Q=replicas$(openssl rand -hex 3)
export BENCH_CONNECTION=queen BENCH_PROFILE=auto BENCH_QUEUE=$Q BENCH_GROUP=g$Q BENCH_WORKERS=8 BENCH_MIN_WORKERS=1 BENCH_MAX_WORKERS=8 \
  BENCH_STRATEGY=size BENCH_TARGET_JOBS_PER_PROCESS=10 BENCH_SCALE_DOWN_DELAY=0 BENCH_BALANCE_MAX_SHIFT=8 BENCH_BALANCE_COOLDOWN=1 \
  BENCH_POLL_INTERVAL=1 BENCH_RESULTS_DIRECTORY=/tmp/queen-bench-results QUEEN_URL=${QUEEN_URL:-http://127.0.0.1:6632} \
  BENCH_TIMEOUT=30 BENCH_RETRY_AFTER=60 QUEEN_SUPERVISOR_COORDINATION=$([ "$MODE" = coordinated ] && echo true || echo false)
mkdir -p /tmp/queen-bench-results; rm -rf /tmp/queen-replica-a /tmp/queen-replica-b
cd "$APP"
php artisan bench:dispatch --jobs=150 --sleep-ms=3000 --connection=queen --queue=$Q >/dev/null
QUEEN_SUPERVISOR_STATE_DIRECTORY=/tmp/queen-replica-a "$BIN" --php php --artisan artisan >/dev/null 2>&1 & A=$!
QUEEN_SUPERVISOR_STATE_DIRECTORY=/tmp/queen-replica-b "$BIN" --php php --artisan artisan >/dev/null 2>&1 & B=$!
for t in $(seq 1 30); do
  sleep 1
  python3 - "$MODE" "$t" >> "$OUT" <<'PY'
import json, math, sys
mode, t = sys.argv[1], sys.argv[2]
depth, workers = None, []
for name in ("a", "b"):
    try:
        pool = json.load(open(f"/tmp/queen-replica-{name}/status.json"))["pool_status"][0]
    except (OSError, ValueError, KeyError, IndexError):
        workers.append(0)
        continue
    workers.append(pool["running"])
    if isinstance(pool.get("depth"), int):
        depth = pool["depth"] if depth is None else max(depth, pool["depth"])
target = "" if depth is None else min(16, max(1, math.ceil(depth / 10)))
print(f"{mode},{t},{'' if depth is None else depth},{target},{workers[0]},{workers[1]}")
PY
done
kill -TERM $A $B; wait $A $B 2>/dev/null
