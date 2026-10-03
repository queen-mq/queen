#!/usr/bin/env bash
# chain2-0930.sh — after the Kafka grid v2: Kafka 3M + 4M (200 partitions), Kafka 100k-partition re-run with a loader
# memory cap (v2 attempt: 2 kload OOM-killed at 11 GB), then the Pulsar plan. GOMEMLIMIT 9 GiB x 3 per 31 GB loader.
cd "$(dirname "$0")/../.."
log() { echo "[$(date -u +%FT%TZ)] chain: $*"; }
while pgrep -f "kafka/grid.sh" > /dev/null; do sleep 10; done
log "kafka grid v2 finished"
kpt() {  # kpt <tag> <rate> [K=V...]
  local tag=$1 rate=$2; shift 2
  [ -f runs/kafka/$tag/DONE ] && { log "skip $tag (DONE)"; return; }
  log "point $tag"; bash kafka/run.sh $tag $rate GOMEMLIMIT=9GiB "$@"
  [ -f runs/kafka/$tag/DONE ] && log "point $tag DONE" || log "point $tag FAILED"
}
kpt 1x200-3000k 3000000 TOPICS=1 PARTITIONS=200 CONSUMERS=22
kpt 1x200-4000k 4000000 TOPICS=1 PARTITIONS=200 CONSUMERS=22
[ -d runs/kafka/1x100000-1000k ] && [ ! -f runs/kafka/1x100000-1000k/DONE ] && mv runs/kafka/1x100000-1000k runs/kafka/1x100000-1000k-oom
kpt 1x100000-1000k 1000000 TOPICS=1 PARTITIONS=100000 CONSUMERS=33 READY_TIMEOUT=900 GROUP_TIMEOUT=600s
log "pulsar plan"
bash pulsar/plan-0930.sh
log "end"
