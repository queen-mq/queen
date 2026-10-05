#!/usr/bin/env bash
# prefetch "auto" against fixed prefetch on the dedicated Linux VM, for the
# three delivery profiles of the Laravel guide and four kinds of job.
# Usage: auto.sh OUT_DIR LANE...
set -o pipefail
BENCH=/opt/queen-auto/benchmark-queen/laravel-supervisors
OUT="${1:?output directory}"; shift
mkdir -p "$OUT"

# The budgets of the 2026-10-01 campaign: 16 vCPUs and 31 GB.
export BENCH_APP_CPUS=8.0 BENCH_APP_MEMORY=8192m
export BENCH_BROKER_CPUS=4.0 BENCH_BROKER_MEMORY=4096m
export BENCH_REDIS_CPUS=4.0 BENCH_REDIS_MEMORY=4096m
export BENCH_APP_IMAGE=queen-laravel-supervisor-bench:auto

# Queen only: the Rust supervisor with prefork, the CLI opcache and lease
# renewal in the master, every write fsynced.
COMMON=(--queen-storage raft --sample-interval 1.0 --timeout 3600 --engines queen-rust --runs 3
    --queen-prefork 1 --queen-opcache-cli 1 --queen-lease-service 1)
# The profiles of the guide.
SAFE=(--queen-prefetch 1 --queen-ack-async 0 --queen-pop-ahead 0)
BALANCED=(--queen-prefetch auto --queen-ack-async 0 --queen-pop-ahead 0)
FAST=(--queen-prefetch auto --queen-ack-async 1 --queen-pop-ahead 1)
# The fast profile before "auto": a fixed prefetch of 4.
FIXED4=(--queen-prefetch 4 --queen-ack-async 1 --queen-pop-ahead 1)
BUILT=0

lane() {
    local name="$1"; shift
    local -a extra=()
    [ "$BUILT" = 1 ] && extra+=(--no-build)
    if docker ps --quiet | grep -q .; then extra+=(--allow-foreign-containers); fi
    echo "== $name: $* ${extra[*]} ($(date +%H:%M:%S))" | tee -a "$OUT/campaign.log"
    "$BENCH/scripts/run.sh" "${COMMON[@]}" "$@" ${extra[@]+"${extra[@]}"} \
        --output "$OUT/$name" >"$OUT/$name.log" 2>&1
    local status=$?
    echo "== $name exit $status ($(date +%H:%M:%S))" | tee -a "$OUT/campaign.log"
    [ "$status" = 0 ] && BUILT=1
    return 0
}

# Workloads: 32 workers draining a burst, or 16 workers at a steady rate.
EMPTY=(--profile fixed --workers 32 --dispatch-mode bulk --jobs 100000 --sleep-ms 0)
TEN=(--profile fixed --workers 32 --dispatch-mode bulk --jobs 50000 --sleep-ms 10)
SLOW=(--profile fixed --workers 32 --dispatch-mode bulk --jobs 6000 --sleep-ms 200)
PACED=(--profile fixed --workers 16 --dispatch-mode single --dispatch-rate 500 --jobs 30000 --sleep-ms 10)

run() {
    local workload profile
    case "$1" in
        empty-*) workload=EMPTY ;;
        ten-*) workload=TEN ;;
        slow-*) workload=SLOW ;;
        paced-*) workload=PACED ;;
        *) echo "unknown lane $1" >&2; return 0 ;;
    esac
    case "$1" in
        *-safe) profile=SAFE ;;
        *-balanced) profile=BALANCED ;;
        *-fast) profile=FAST ;;
        *-fixed4) profile=FIXED4 ;;
        *) echo "unknown lane $1" >&2; return 0 ;;
    esac
    local -n w="$workload" pr="$profile"
    lane "$1" "${w[@]}" "${pr[@]}"
}

for name in "$@"; do run "$name"; done
echo "== done ($(date +%H:%M:%S))" | tee -a "$OUT/campaign.log"
