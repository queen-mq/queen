#!/usr/bin/env bash
# A/B of the full-batch pop_ahead rule on Queen: the old client (A, 79af91ee)
# against the new one (B, e80e7ea8), lane by lane, each variant building its image.
# Usage: popfix.sh OUT_DIR
set -o pipefail
OUT="${1:?output directory}"
mkdir -p "$OUT"
export BENCH_APP_CPUS=8.0 BENCH_APP_MEMORY=8192m
export BENCH_BROKER_CPUS=4.0 BENCH_BROKER_MEMORY=4096m
export BENCH_REDIS_CPUS=4.0 BENCH_REDIS_MEMORY=4096m
declare -A TREE=([old]=/opt/queen-drain [new]=/opt/queen-popfix)
BASE=(--queen-storage raft --sample-interval 1.0 --timeout 3600 --engines queen-rust --runs 3 --profile fixed
      --queen-prefetch 4 --redis-appendfsync always --queen-prefork 1 --queen-opcache-cli 1
      --queen-lease-service 1 --queen-ack-async 1 --queen-pop-ahead 1)
lane() {
    local name="$1" variant="$2"; shift 2
    local -a extra=()
    if [ "$(docker ps --quiet | grep -c .)" -gt 0 ]; then extra+=(--allow-foreign-containers); fi
    echo "== $name/$variant: $* ($(date +%H:%M:%S))" | tee -a "$OUT/campaign.log"
    "${TREE[$variant]}/benchmark-queen/laravel-supervisors/scripts/run.sh" "${BASE[@]}" "$@" ${extra[@]+"${extra[@]}"} \
        --output "$OUT/$name-$variant" >"$OUT/$name-$variant.log" 2>&1
    echo "== $name/$variant exit $? ($(date +%H:%M:%S))" | tee -a "$OUT/campaign.log"
}
for variant in old new; do lane paced-500 "$variant" --workers 16 --dispatch-mode single --dispatch-rate 500 --jobs 60000; done
for variant in old new; do lane throughput-32 "$variant" --workers 32 --dispatch-mode bulk --jobs 50000; done
for variant in old new; do lane noop-32 "$variant" --workers 32 --dispatch-mode bulk --jobs 100000 --sleep-ms 0; done
echo "== done ($(date +%H:%M:%S))" | tee -a "$OUT/campaign.log"
