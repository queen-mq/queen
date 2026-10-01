#!/usr/bin/env bash
# Load campaign on a dedicated Linux VM: Horizon against Queen on Raft.
# Usage: load.sh OUT_DIR LANE...
set -o pipefail
BENCH=/opt/queen/benchmark-queen/laravel-supervisors
OUT="${1:?output directory}"; shift
mkdir -p "$OUT"

# Budgets for 16 vCPUs and 31 GB: the rest is for the producer and the OS.
export BENCH_APP_CPUS=8.0 BENCH_APP_MEMORY=8192m
export BENCH_BROKER_CPUS=4.0 BENCH_BROKER_MEMORY=4096m
export BENCH_REDIS_CPUS=4.0 BENCH_REDIS_MEMORY=4096m

QUEEN_ALL=(--queen-prefork 1 --queen-opcache-cli 1 --queen-lease-service 1 --queen-ack-async 1 --queen-pop-ahead 1)
BASE=(--queen-storage raft --sample-interval 1.0 --timeout 3600 --engines horizon,queen-rust)
QUEEN_ONLY=(--queen-storage raft --sample-interval 1.0 --timeout 3600 --engines queen-rust)
STRICT=(--redis-appendfsync always)
BUILT=0

lane() {
    local name="$1"; shift
    local -a extra=()
    [ "$BUILT" = 1 ] && extra+=(--no-build)
    if docker ps --quiet | grep -q .; then extra+=(--allow-foreign-containers); fi
    echo "== $name: $* ${extra[*]} ($(date +%H:%M:%S))" | tee -a "$OUT/campaign.log"
    "$BENCH/scripts/run.sh" "$@" ${extra[@]+"${extra[@]}"} --output "$OUT/$name" >"$OUT/$name.log" 2>&1
    local status=$?
    echo "== $name exit $status ($(date +%H:%M:%S))" | tee -a "$OUT/campaign.log"
    [ "$status" = 0 ] && BUILT=1
    return 0
}

run() {
    case "$1" in
        throughput-32) lane "$1" "${BASE[@]}" --runs 3 --profile fixed --workers 32 --dispatch-mode bulk --queen-prefetch 4 --jobs 50000 "${STRICT[@]}" "${QUEEN_ALL[@]}" ;;
        throughput-16) lane "$1" "${BASE[@]}" --runs 3 --profile fixed --workers 16 --dispatch-mode bulk --queen-prefetch 4 --jobs 30000 "${STRICT[@]}" "${QUEEN_ALL[@]}" ;;
        everysec-32) lane "$1" "${BASE[@]}" --runs 3 --profile fixed --workers 32 --dispatch-mode bulk --queen-prefetch 4 --jobs 50000 --redis-appendfsync everysec "${QUEEN_ALL[@]}" ;;
        noop-32) lane "$1" "${BASE[@]}" --runs 3 --profile fixed --workers 32 --dispatch-mode bulk --queen-prefetch 4 --jobs 100000 --sleep-ms 0 "${STRICT[@]}" "${QUEEN_ALL[@]}" ;;
        burst-64) lane "$1" "${BASE[@]}" --runs 3 --profile auto --min-workers 1 --max-workers 64 --dispatch-mode bulk --queen-prefetch 4 --jobs 30000 "${STRICT[@]}" "${QUEEN_ALL[@]}" --queen-event-driven 1 --queen-fast-scale-up 1 ;;
        paced-500) lane "$1" "${BASE[@]}" --runs 3 --profile fixed --workers 16 --dispatch-mode single --dispatch-rate 500 --jobs 60000 --queen-prefetch 4 "${STRICT[@]}" "${QUEEN_ALL[@]}" ;;
        soak) lane "$1" "${BASE[@]}" --runs 1 --profile fixed --workers 16 --dispatch-mode single --dispatch-rate 400 --jobs 360000 --queen-prefetch 4 "${STRICT[@]}" "${QUEEN_ALL[@]}" ;;
        ablation-none-32) lane "$1" "${QUEEN_ONLY[@]}" --runs 3 --profile fixed --workers 32 --dispatch-mode bulk --queen-prefetch 4 --jobs 50000 "${STRICT[@]}" \
            --queen-prefork 1 --queen-opcache-cli 1 --queen-lease-service 0 --queen-ack-async 0 --queen-pop-ahead 0 ;;
        ablation-lease-32) lane "$1" "${QUEEN_ONLY[@]}" --runs 3 --profile fixed --workers 32 --dispatch-mode bulk --queen-prefetch 4 --jobs 50000 "${STRICT[@]}" \
            --queen-prefork 1 --queen-opcache-cli 1 --queen-lease-service 1 --queen-ack-async 0 --queen-pop-ahead 0 ;;
        ablation-ack-32) lane "$1" "${QUEEN_ONLY[@]}" --runs 3 --profile fixed --workers 32 --dispatch-mode bulk --queen-prefetch 4 --jobs 50000 "${STRICT[@]}" \
            --queen-prefork 1 --queen-opcache-cli 1 --queen-lease-service 1 --queen-ack-async 1 --queen-pop-ahead 0 ;;
        noop-guzzle-32|noop-curl-32) local t="${1#noop-}"; lane "$1" "${QUEEN_ONLY[@]}" --runs 3 --profile fixed --workers 32 --dispatch-mode bulk --queen-prefetch 4 --jobs 100000 --sleep-ms 0 \
            "${STRICT[@]}" "${QUEEN_ALL[@]}" --queen-http-transport "${t%-32}" ;;
        *) echo "unknown lane $1" >&2 ;;
    esac
}

for name in "$@"; do run "$name"; done
echo "== done ($(date +%H:%M:%S))" | tee -a "$OUT/campaign.log"
