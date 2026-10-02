#!/usr/bin/env bash
# More partition stripes than one pop checks out: the Queen Rust supervisor
# with 64, 128 and 256 stripes per queue, a pop always checking out at most 64.
# Usage: stripes.sh OUT_DIR [RUNS]
set -o pipefail
BENCH="$(CDPATH='' cd -- "$(dirname -- "$0")/../../laravel-supervisors" && pwd)"
OUT="${1:?output directory}"
RUNS="${2:-3}"
mkdir -p "$OUT"

# Budgets for 16 vCPUs and 31 GB, as in the 2026-10-01 Linux campaign.
export BENCH_APP_CPUS=8.0 BENCH_APP_MEMORY=8192m
export BENCH_BROKER_CPUS=4.0 BENCH_BROKER_MEMORY=4096m
export BENCH_REDIS_CPUS=4.0 BENCH_REDIS_MEMORY=4096m

QUEEN_ALL=(--queen-prefork 1 --queen-opcache-cli 1 --queen-lease-service 1 --queen-ack-async 1 --queen-pop-ahead 1)
BASE=(--queen-storage raft --sample-interval 1.0 --timeout 3600 --engines queen-rust --runs 1 --queen-prefetch 4 --profile fixed)
BUILT=0

lane() {
    local name="$1"; shift
    local -a extra=()
    [ "$BUILT" = 1 ] && extra+=(--no-build)
    if docker ps --quiet | grep -q .; then extra+=(--allow-foreign-containers); fi
    mkdir -p "$(dirname -- "$OUT/$name")"
    echo "== $name: $* ${extra[*]} ($(date +%H:%M:%S))" | tee -a "$OUT/campaign.log"
    "$BENCH/scripts/run.sh" "$@" ${extra[@]+"${extra[@]}"} --output "$OUT/$name" >"$OUT/$name.log" 2>&1
    local status=$?
    echo "== $name exit $status ($(date +%H:%M:%S))" | tee -a "$OUT/campaign.log"
    [ "$status" = 0 ] && BUILT=1
    return 0
}

# Stripe counts alternate inside each run, so drift over the campaign hits
# every count alike.
for run in $(seq 1 "$RUNS"); do
    for stripes in 64 128 256; do
        S=(--queen-partitions "$stripes")
        lane "low-50/s$stripes-r$run" "${BASE[@]}" "${S[@]}" "${QUEEN_ALL[@]}" --workers 16 --dispatch-mode single --dispatch-rate 50 --jobs 6000
        lane "paced-500/s$stripes-r$run" "${BASE[@]}" "${S[@]}" "${QUEEN_ALL[@]}" --workers 16 --dispatch-mode single --dispatch-rate 500 --jobs 30000
        lane "drain-64/s$stripes-r$run" "${BASE[@]}" "${S[@]}" "${QUEEN_ALL[@]}" --workers 64 --dispatch-mode bulk --jobs 50000 --backlog-first 1
        lane "drain-128/s$stripes-r$run" "${BASE[@]}" "${S[@]}" "${QUEEN_ALL[@]}" --workers 128 --dispatch-mode bulk --jobs 50000 --backlog-first 1
    done
done
echo "== done ($(date +%H:%M:%S))" | tee -a "$OUT/campaign.log"
