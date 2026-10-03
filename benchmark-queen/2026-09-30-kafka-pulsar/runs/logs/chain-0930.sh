#!/usr/bin/env bash
# chain-0930.sh — after the Kafka grid v2 exits: Kafka rate extension (3M, 4M at 200 partitions), then the Pulsar plan.
cd "$(dirname "$0")/../.."
log() { echo "[$(date -u +%FT%TZ)] chain: $*"; }
while pgrep -f "kafka/grid.sh" > /dev/null; do sleep 10; done
log "kafka grid v2 finished; rate extension"
for r in 3000000 4000000; do
  tag=1x200-$((r / 1000))k
  [ -f runs/kafka/$tag/DONE ] && continue
  log "point $tag"; bash kafka/run.sh $tag $r TOPICS=1 PARTITIONS=200 CONSUMERS=22
  [ -f runs/kafka/$tag/DONE ] && log "point $tag DONE" || log "point $tag FAILED"
done
log "pulsar plan"
bash pulsar/plan-0930.sh
log "end"
