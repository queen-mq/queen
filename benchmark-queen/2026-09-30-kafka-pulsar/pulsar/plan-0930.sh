#!/usr/bin/env bash
# plan-0930.sh (Mac) — the Pulsar points of 2026-09-30, in value order, as grid.sh would run them (same tags and
# parameters, see `DRY=1 pulsar/grid.sh`), minus what the loaders cannot carry:
#   - pB-p200 = pA-r1000k (same shape, failover is the default): run once
#   - 100k partitions in total (pB-p100000, pC-*-p100k): pload's Go client runs one 5 ms flush ticker + a 512 KB lz4 table
#     per partition producer (~0.55 idle cores per 1000 producers per process, TESTS.md) = ~18 cores per 16-core loader
#     even with producer sharding. One probe at the very end (pB-p100000, READY_TIMEOUT 900) documents the limit.
# Skips points whose runs/pulsar/<tag>/DONE exists; stops after 2 consecutive failures.
set -u
H=$(cd "$(dirname "$0")/.." && pwd)
RUNS=${RUNS:-$H/runs/pulsar}
log() { echo "[$(date -u +%FT%TZ)] plan: $*"; }
FAILS=0
pt() {  # pt <tag> <rate> [KEY=VAL...]
  local tag=$1 rate=$2; shift 2
  if [ -f "$RUNS/$tag/DONE" ]; then log "skip $tag (DONE)"; return 0; fi
  log "point $tag: $rate $*"
  bash "$H/pulsar/run.sh" "$tag" "$rate" PLOAD_ENV=GOMEMLIMIT=9GiB "$@"   # 3 per 31 GB loader (09-30: kload OOM at 11 GB each, 100k partitions)
  if [ -f "$RUNS/$tag/DONE" ]; then log "point $tag DONE"; FAILS=0
  else FAILS=$((FAILS + 1)); log "point $tag FAILED ($(tail -1 "$RUNS/$tag/FAILED" 2>/dev/null))"
    [ "$FAILS" -ge 2 ] && { log "2 consecutive failures: stop"; exit 1; }
  fi
}
L=(READY_TIMEOUT=1800 TOPIC_TIMEOUT=1800s)
# A: rate, 1 topic x 200 partitions, failover
pt pA-r300k   300000 PARTITIONS=200 CONSUMERS=22
pt pA-r600k   600000 PARTITIONS=200 CONSUMERS=22
pt pA-r1000k 1000000 PARTITIONS=200 CONSUMERS=22
pt pA-r1500k 1500000 PARTITIONS=200 CONSUMERS=22
pt pA-r2000k 2000000 PARTITIONS=200 CONSUMERS=22
# B: partitions at 1M, failover
pt pB-p2000   1000000 PARTITIONS=2000  SUB_TYPE=failover CONSUMERS=33
pt pB-p10000  1000000 PARTITIONS=10000 SUB_TYPE=failover CONSUMERS=33 "${L[@]}"
pt pB-p50000  1000000 PARTITIONS=50000 SUB_TYPE=failover CONSUMERS=33 "${L[@]}"
# C: topics at 1M, 10k partitions in total
pt pC-t10-p10k   1000000 TOPICS=10   PARTITIONS=1000 CONSUMERS=33 "${L[@]}"
pt pC-t100-p10k  1000000 TOPICS=100  PARTITIONS=100  CONSUMERS=33 "${L[@]}"
pt pC-t1000-p10k 1000000 TOPICS=1000 PARTITIONS=10   CONSUMERS=33 "${L[@]}"
# D: keys at 1M, Key_Shared on 48 partitions
pt pD-e1m  1000000 PARTITIONS=48 ENTITIES=1000000  SUB_TYPE=key_shared CONSUMERS=22
pt pD-e10m 1000000 PARTITIONS=48 ENTITIES=10000000 SUB_TYPE=key_shared CONSUMERS=22
# the 100k-partition probe (expected to hit the loaders' client-side limit)
pt pB-p100000 1000000 PARTITIONS=100000 SUB_TYPE=failover CONSUMERS=33 READY_TIMEOUT=900 TOPIC_TIMEOUT=900s
log "end"
