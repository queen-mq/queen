#!/usr/bin/env bash
# Horizon against Queen on the Raft broker. Usage: campaign.sh OUT_DIR CAMPAIGN...
set -o pipefail

BENCH="$(cd "$(dirname "$0")/../../laravel-supervisors" && pwd)"
OUT="${1:?output directory}"
shift
mkdir -p "$OUT"

RUNS="${RUNS:-5}"
BUILD_DONE="${BUILD_DONE:-0}"

# Queen with every feature this release ships.
QUEEN_ALL=(--queen-prefork 1 --queen-opcache-cli 1 --queen-lease-service 1 --queen-ack-async 1)
BASE=(--queen-storage raft --sample-interval 1.0 --runs "$RUNS" --engines horizon,queen-rust)
BULK8=(--profile fixed --workers 8 --dispatch-mode bulk --queen-prefetch 4)

campaign() {
    local name="$1"
    shift
    local -a extra=()
    if [ "$BUILD_DONE" = 1 ]; then
        extra+=(--no-build)
    fi
    # Between campaigns no lane runs: any running container is foreign.
    if docker ps --quiet | grep -q .; then
        extra+=(--allow-foreign-containers)
        echo "== $name: foreign containers: $(docker ps --format '{{.Names}}' | tr '\n' ' ')" | tee -a "$OUT/campaign.log"
    fi
    echo "== $name: $* ${extra[*]} ($(date +%H:%M:%S))" | tee -a "$OUT/campaign.log"
    "$BENCH/scripts/run.sh" "$@" ${extra[@]+"${extra[@]}"} --output "$OUT/$name" >"$OUT/$name.log" 2>&1
    local status=$?
    echo "== $name exit $status ($(date +%H:%M:%S))" | tee -a "$OUT/campaign.log"
    [ "$status" = 0 ] && BUILD_DONE=1
    return 0
}

run_named() {
    case "$1" in
        strict) campaign strict "${BASE[@]}" "${BULK8[@]}" --jobs 5000 --redis-appendfsync always "${QUEEN_ALL[@]}" ;;
        everysec) campaign everysec "${BASE[@]}" "${BULK8[@]}" --jobs 5000 --redis-appendfsync everysec "${QUEEN_ALL[@]}" ;;
        ablation-none) campaign ablation-none "${BASE[@]/horizon,queen-rust/queen-rust}" "${BULK8[@]}" --jobs 5000 --redis-appendfsync always \
            --queen-prefork 1 --queen-opcache-cli 1 --queen-lease-service 0 --queen-ack-async 0 ;;
        ablation-lease) campaign ablation-lease "${BASE[@]/horizon,queen-rust/queen-rust}" "${BULK8[@]}" --jobs 5000 --redis-appendfsync always \
            --queen-prefork 1 --queen-opcache-cli 1 --queen-lease-service 1 --queen-ack-async 0 ;;
        ablation-ack) campaign ablation-ack "${BASE[@]/horizon,queen-rust/queen-rust}" "${BULK8[@]}" --jobs 5000 --redis-appendfsync always \
            --queen-prefork 1 --queen-opcache-cli 1 --queen-lease-service 0 --queen-ack-async 1 ;;
        noop) campaign noop "${BASE[@]}" "${BULK8[@]}" --jobs 10000 --sleep-ms 0 --redis-appendfsync always "${QUEEN_ALL[@]}" ;;
        latency-100) campaign latency-100 "${BASE[@]}" --profile fixed --workers 8 --queen-prefetch 1 --dispatch-mode single --dispatch-rate 100 --jobs 2000 \
            --redis-appendfsync always --queen-prefork 1 --queen-opcache-cli 1 --queen-ack-async 1 ;;
        latency-300) campaign latency-300 "${BASE[@]}" --profile fixed --workers 8 --queen-prefetch 1 --dispatch-mode single --dispatch-rate 300 --jobs 4000 \
            --redis-appendfsync always --queen-prefork 1 --queen-opcache-cli 1 --queen-ack-async 1 ;;
        burst) campaign burst "${BASE[@]}" --profile auto --min-workers 1 --max-workers 16 --jobs 3000 --dispatch-mode bulk --queen-prefetch 4 \
            --redis-appendfsync always "${QUEEN_ALL[@]}" --queen-event-driven 1 --queen-fast-scale-up 1 ;;
        lean) campaign lean "${BASE[@]}" --profile fixed --workers 8 --queen-prefetch 1 --dispatch-mode bulk --jobs 3000 \
            --redis-appendfsync always --queen-prefork 1 --queen-opcache-cli 1 --queen-ack-async 1 ;;
        cpu) campaign cpu "${BASE[@]}" "${BULK8[@]}" --jobs 3000 --sleep-ms 0 --cpu-iterations 20000 --redis-appendfsync always "${QUEEN_ALL[@]}" ;;
        # Every feature, pop_ahead included.
        strict-all) campaign strict-all "${BASE[@]}" "${BULK8[@]}" --jobs 5000 --redis-appendfsync always "${QUEEN_ALL[@]}" --queen-pop-ahead 1 ;;
        everysec-all) campaign everysec-all "${BASE[@]}" "${BULK8[@]}" --jobs 5000 --redis-appendfsync everysec "${QUEEN_ALL[@]}" --queen-pop-ahead 1 ;;
        noop-all) campaign noop-all "${BASE[@]}" "${BULK8[@]}" --jobs 10000 --sleep-ms 0 --redis-appendfsync always "${QUEEN_ALL[@]}" --queen-pop-ahead 1 ;;
        burst-all) campaign burst-all "${BASE[@]}" --profile auto --min-workers 1 --max-workers 16 --jobs 3000 --dispatch-mode bulk --queen-prefetch 4 \
            --redis-appendfsync always "${QUEEN_ALL[@]}" --queen-pop-ahead 1 --queen-event-driven 1 --queen-fast-scale-up 1 ;;
        prefetch8-all) campaign prefetch8-all "${BASE[@]/horizon,queen-rust/queen-rust}" --profile fixed --workers 8 --dispatch-mode bulk --queen-prefetch 8 --jobs 5000 \
            --redis-appendfsync always "${QUEEN_ALL[@]}" --queen-pop-ahead 1 ;;
        lean-all) campaign lean-all "${BASE[@]}" --profile fixed --workers 8 --queen-prefetch 1 --dispatch-mode bulk --jobs 3000 \
            --redis-appendfsync always --queen-prefork 1 --queen-opcache-cli 1 --queen-ack-async 1 --queen-pop-ahead 1 ;;
        latency-300-all) campaign latency-300-all "${BASE[@]}" --profile fixed --workers 8 --queen-prefetch 4 --dispatch-mode single --dispatch-rate 300 --jobs 4000 \
            --redis-appendfsync always "${QUEEN_ALL[@]}" --queen-pop-ahead 1 ;;
        latency-300-lean) campaign latency-300-lean "${BASE[@]/horizon,queen-rust/queen-rust}" --profile fixed --workers 8 --queen-prefetch 1 --dispatch-mode single --dispatch-rate 300 --jobs 4000 \
            --redis-appendfsync always --queen-prefork 1 --queen-opcache-cli 1 --queen-ack-async 1 --queen-pop-ahead 1 ;;
        *) echo "unknown campaign $1" >&2 ;;
    esac
}

for name in "$@"; do
    run_named "$name"
done
