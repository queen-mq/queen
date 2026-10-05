#!/usr/bin/env bash
# Worker memory on the dedicated Linux VM: which part of the saving comes from
# prefork, which from the CLI opcache, and what is left when every job
# allocates memory of its own.
# Usage: memory.sh OUT_DIR LANE...
set -o pipefail
BENCH=/opt/queen-memory/benchmark-queen/laravel-supervisors
OUT="${1:?output directory}"; shift
mkdir -p "$OUT"

# The budgets of the 2026-10-01 campaign: 16 vCPUs and 31 GB.
export BENCH_APP_CPUS=8.0 BENCH_APP_MEMORY=8192m
export BENCH_BROKER_CPUS=4.0 BENCH_BROKER_MEMORY=4096m
export BENCH_REDIS_CPUS=4.0 BENCH_REDIS_MEMORY=4096m
export BENCH_APP_IMAGE=queen-laravel-supervisor-bench:memory

# 64 fixed workers drain 60,000 jobs; the sampler keeps reading for 20 s after
# the last job, with every worker alive and idle: the window the extractor
# reads. The Queen features other than prefork and opcache match 2026-10-01.
COMMON=(--queen-storage raft --sample-interval 1.0 --timeout 3600 --profile fixed --workers 64
    --queen-prefetch 4 --redis-appendfsync always
    --queen-lease-service 1 --queen-ack-async 1 --queen-pop-ahead 1)
DRAIN=(--runs 3 --dispatch-mode bulk --jobs 60000 --post-drain 20)
# 20 minutes at 500 jobs/s: about 9,400 jobs for each worker.
AGING=(--runs 1 --dispatch-mode single --dispatch-rate 500 --jobs 600000 --post-drain 20)
BUILT=0

lane() {
    local name="$1" alloc="$2" horizon_opcache="$3"; shift 3
    local -a extra=()
    [ "$BUILT" = 1 ] && extra+=(--no-build)
    if docker ps --quiet | grep -q .; then extra+=(--allow-foreign-containers); fi
    echo "== $name: alloc=${alloc} horizon_opcache=${horizon_opcache} $* ${extra[*]} ($(date +%H:%M:%S))" \
        | tee -a "$OUT/campaign.log"
    BENCH_JOB_ALLOC_KB="$alloc" BENCH_HORIZON_OPCACHE_CLI="$horizon_opcache" \
        "$BENCH/scripts/run.sh" "${COMMON[@]}" "$@" ${extra[@]+"${extra[@]}"} \
        --output "$OUT/$name" >"$OUT/$name.log" 2>&1
    local status=$?
    echo "== $name exit $status ($(date +%H:%M:%S))" | tee -a "$OUT/campaign.log"
    [ "$status" = 0 ] && BUILT=1
    return 0
}

# Each lane names the Queen configuration; the Horizon one is in its engines.
#   published:  Horizon (opcache off, as PHP ships) and Queen with prefork and opcache
#   opcache:    both start every worker separately with the opcache on
#   plain:      Queen with neither, the control for the Horizon worker
#   fork-only:  Queen with prefork and the opcache off
run() {
    local alloc
    case "$1" in
        *-tiny) alloc=0 ;;
        *-alloc8) alloc=8192 ;;
        aging) alloc=8192 ;;
        *) echo "unknown lane $1" >&2; return 0 ;;
    esac
    case "$1" in
        published-*) lane "$1" "$alloc" 0 "${DRAIN[@]}" --engines horizon,queen-rust --queen-prefork 1 --queen-opcache-cli 1 ;;
        opcache-*) lane "$1" "$alloc" 1 "${DRAIN[@]}" --engines horizon,queen-rust --queen-prefork 0 --queen-opcache-cli 1 ;;
        plain-*) lane "$1" "$alloc" 0 "${DRAIN[@]}" --engines queen-rust --queen-prefork 0 --queen-opcache-cli 0 ;;
        fork-only-*) lane "$1" "$alloc" 0 "${DRAIN[@]}" --engines queen-rust --queen-prefork 1 --queen-opcache-cli 0 ;;
        aging) lane "$1" "$alloc" 0 "${AGING[@]}" --engines horizon,queen-rust --queen-prefork 1 --queen-opcache-cli 1 ;;
        *) echo "unknown lane $1" >&2 ;;
    esac
}

for name in "$@"; do run "$name"; done
echo "== done ($(date +%H:%M:%S))" | tee -a "$OUT/campaign.log"
