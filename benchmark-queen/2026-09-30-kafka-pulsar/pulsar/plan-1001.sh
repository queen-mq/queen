#!/usr/bin/env bash
# plan-1001.sh (Mac) — the rest of the 2026-09-30 Pulsar plan, reordered on 10-01 for real (failover) partitions first:
# Alice wants Pulsar at 100k and 1M partitions ("Key_Shared gives FIFO, but no isolation"), the shapes Queen ran
# (1x100k, 1x1M) and Kafka could not carry.
#   - first waits for a run.sh still in flight (the plan-0930 point that was running when this one replaced it);
#   - -nb points: the client cannot carry 10k+ partition producers per process with its defaults (a 5 ms flush ticker
#     and a 512 KB lz4 table per partition producer, ~0.55 idle cores per 1000, TESTS.md). At 1M msg/s over 100k
#     partitions a partition sees 10 msg/s: a batch never holds a second message and lz4 on one ~300 B message buys
#     nothing, so -batch-max-msgs 1 (every message flushes at once), -batch-delay 1s (the ticker idles) and
#     -compression none change nothing the brokers do; producers are created before READY (-producer-eager);
#   - the 1M probe gets bigger JVMs inside the 31 GB node (ZK 4 GB for ~3M znodes, broker heap 12 GB for ~1M topics at
#     ~30 KB each, bookie direct 4 GB) and an hour to get READY; it may well fail, and how it fails is the result;
#   - probe failures do not count toward the 2-consecutive-failures stop;
#   - 10-01 07:14: pB-p50000 failed at setup (the partitioned-topic subscription PUT answered 500 at 50k partitions);
#     pload now creates the subscription partition by partition when that fan-out fails. 50k runs -nb too (20 msg/s per
#     partition: batching groups nothing), and the 1M probe skips the warm step (one producer per partition in ONE
#     process: 1M partition producers would not fit a loader).
set -u
H=$(cd "$(dirname "$0")/.." && pwd)
RUNS=${RUNS:-$H/runs/pulsar}
log() { echo "[$(date -u +%FT%TZ)] plan: $*"; }
while pgrep -f "[p]ulsar/run.sh" >/dev/null; do sleep 5; done
log "plan-1001 starts (previous run finished)"
FAILS=0
pt() {  # pt <tag> <rate> [KEY=VAL...]
  local tag=$1 rate=$2; shift 2
  if [ -f "$RUNS/$tag/DONE" ]; then log "skip $tag (DONE)"; return 0; fi
  log "point $tag: $rate $*"
  bash "$H/pulsar/run.sh" "$tag" "$rate" PLOAD_ENV=GOMEMLIMIT=9GiB "$@"
  if [ -f "$RUNS/$tag/DONE" ]; then log "point $tag DONE"; FAILS=0
  else FAILS=$((FAILS + 1)); log "point $tag FAILED ($(tail -1 "$RUNS/$tag/FAILED" 2>/dev/null))"
    [ "$FAILS" -ge 2 ] && { log "2 consecutive failures: stop"; exit 1; }
  fi
}
probe() { pt "$@"; FAILS=0; }
L=(READY_TIMEOUT=1800 TOPIC_TIMEOUT=1800s)
NB=(COMPRESSION=none BATCH_MAX_MSGS=1 BATCH_DELAY=1s PLOAD_EXTRA=-producer-eager)
# B: partitions at 1M, failover (real partitions)
pt pB-p10000  1000000 PARTITIONS=10000 SUB_TYPE=failover CONSUMERS=33 "${L[@]}"
probe pB-p50000-nb   1000000 PARTITIONS=50000   SUB_TYPE=failover CONSUMERS=33 "${L[@]}" "${NB[@]}"
probe pB-p100000-nb  1000000 PARTITIONS=100000  SUB_TYPE=failover CONSUMERS=33 "${L[@]}" "${NB[@]}"
probe pB-p1000000-nb 1000000 PARTITIONS=1000000 SUB_TYPE=failover CONSUMERS=33 READY_TIMEOUT=3600 TOPIC_TIMEOUT=3600s WARM=0 \
  "${NB[@]}" "MEM_ZK=-Xms4g -Xmx4g" "MEM_BROKER=-Xms12g -Xmx12g -XX:MaxDirectMemorySize=4g" \
  "MEM_BOOKIE=-Xms3g -Xmx3g -XX:MaxDirectMemorySize=4g"
# C: topics at 1M, 10k partitions in total
pt pC-t10-p10k   1000000 TOPICS=10   PARTITIONS=1000 CONSUMERS=33 "${L[@]}"
pt pC-t100-p10k  1000000 TOPICS=100  PARTITIONS=100  CONSUMERS=33 "${L[@]}"
pt pC-t1000-p10k 1000000 TOPICS=1000 PARTITIONS=10   CONSUMERS=33 "${L[@]}"
# D: keys at 1M, Key_Shared on 48 partitions (per-key FIFO, no per-partition isolation)
pt pD-e1m  1000000 PARTITIONS=48 ENTITIES=1000000  SUB_TYPE=key_shared CONSUMERS=22
pt pD-e10m 1000000 PARTITIONS=48 ENTITIES=10000000 SUB_TYPE=key_shared CONSUMERS=22
log "end"
