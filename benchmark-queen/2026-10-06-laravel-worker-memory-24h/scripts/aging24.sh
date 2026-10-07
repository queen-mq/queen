#!/usr/bin/env bash
# 24 hours of forked workers on the 2.3.0 tree: does their private memory keep
# growing? The 20-minute aging lane of 2026-10-05, stretched: 64 forked
# workers, jobs that allocate 8 MiB, 50 jobs/s, about 64,000 jobs a worker, for about 23 hours.
set -u
while ! grep -q "^done" /root/results/v230/progress.log 2>/dev/null; do sleep 60; done
BENCH=/opt/queen-aging24/benchmark-queen/laravel-supervisors
OUT=/root/results/aging24
mkdir -p "$OUT"
export BENCH_APP_CPUS=8.0 BENCH_APP_MEMORY=8192m
export BENCH_BROKER_CPUS=4.0 BENCH_BROKER_MEMORY=4096m
export BENCH_REDIS_CPUS=4.0 BENCH_REDIS_MEMORY=4096m
export BENCH_APP_IMAGE=queen-laravel-supervisor-bench:aging24
echo "start $(date -u +%FT%TZ)" > "$OUT/progress.log"
BENCH_JOB_ALLOC_KB=8192 BENCH_HORIZON_OPCACHE_CLI=0 "$BENCH/scripts/run.sh" \
    --queen-storage raft --sample-interval 10 --timeout 86400 --profile fixed --workers 64 \
    --queen-prefetch 4 --redis-appendfsync always \
    --queen-lease-service 1 --queen-ack-async 1 --queen-pop-ahead 1 \
    --runs 1 --dispatch-mode single --dispatch-rate 50 --jobs 4100000 --post-drain 60 \
    --engines queen-rust --queen-prefork 1 --queen-opcache-cli 1 \
    --output "$OUT/run" > "$OUT/run.log" 2>&1
echo "exit $? $(date -u +%FT%TZ)" >> "$OUT/progress.log"
