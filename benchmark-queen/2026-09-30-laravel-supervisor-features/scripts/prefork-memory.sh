#!/usr/bin/env bash
# PSS of N `queue:work` workers started one by one against N workers forked
# from one booted Laravel (scripts/prefork.php), with opcache off and on.
# Runs inside a Linux container with the benchmark app mounted at /app;
# appends CSV rows to $OUT. JOBS > 0 dispatches work before measuring.
set -u
cd /app
export BENCH_CONNECTION=queen QUEEN_URL=${QUEEN_URL:-http://host.docker.internal:6632} BENCH_QUEUE=$SPIKE_QUEUE BENCH_GROUP=spike \
  BENCH_RESULTS_DIRECTORY=/tmp/results BENCH_PROFILE=fixed BENCH_WORKERS=4
mkdir -p /tmp/results
N=${N:-4}; JOBS=${JOBS:-0}; RUN=${RUN:-1}
pss() { local total=0 rss=0; for pid in "$@"; do p=$(awk '/^Pss:/{print $2}' /proc/$pid/smaps_rollup 2>/dev/null); r=$(awk '/^Rss:/{print $2}' /proc/$pid/smaps_rollup 2>/dev/null); total=$((total + ${p:-0})); rss=$((rss + ${r:-0})); done; echo "$((total/1024)),$((rss/1024))"; }
for OPC in 0 1; do
  pids=()
  for i in $(seq $N); do php -d opcache.enable_cli=$OPC artisan queue:work queen --queue=$SPIKE_QUEUE --sleep=1 --quiet & pids+=($!); done
  sleep 5; [ "$JOBS" -gt 0 ] && php artisan bench:dispatch --jobs=$JOBS --sleep-ms=20 --connection=queen --queue=$SPIKE_QUEUE >/dev/null 2>&1
  sleep 15
  echo "$RUN,$N,$JOBS,$OPC,spawned,$(pss ${pids[@]})" >> $OUT
  kill -TERM ${pids[@]}; wait ${pids[@]} 2>/dev/null
  php -d opcache.enable_cli=$OPC /scripts/prefork.php $N & parent=$!
  sleep 5; [ "$JOBS" -gt 0 ] && php artisan bench:dispatch --jobs=$JOBS --sleep-ms=20 --connection=queen --queue=$SPIKE_QUEUE >/dev/null 2>&1
  sleep 15
  children=$(cat /tmp/prefork-children)
  echo "$RUN,$N,$JOBS,$OPC,forked,$(pss $parent $children)" >> $OUT
  kill -TERM $children; wait $parent 2>/dev/null
done
