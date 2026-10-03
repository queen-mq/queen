#!/usr/bin/env bash
# grid.sh (Mac) — the Kafka matrix re-run on Redpanda: EXACTLY the 19 points that are DONE in runs/kafka/ (09-30), same
# tags, rates and shapes, in this order, as redpanda/run.sh calls. The per-point arguments are the ones the Kafka points'
# run.env files record (CONSUMERS 22/33, READY_TIMEOUT=900 GROUP_TIMEOUT=600s at the big points, GOMEMLIMIT=9GiB where the
# Kafka loaders needed it: 3M, 4M, 100k). A point whose runs/redpanda/<tag>/DONE exists is skipped, so re-running resumes;
# a FAILED point is simply run again.
#   POINTS="A B C D"   series to run, in this order (default all four)
#   DRY=1              print the run.sh calls, run nothing
# Every other variable (HOSTS_ENV, PROCS_PER_LOADER, TUNE, ...) passes through to run.sh.
#   A  rate, 1 topic x 200 partitions: 300k 600k 1M 1.5M 2M 3M 4M (kafka/grid.sh lists up to 2M; 3M/4M were run by hand)
#   B  partitions, 1 topic at 1M: 2k 10k 50k 100k (B's 200-partition point = A's 1M point, one tag)
#   C  topics at 1M: 10x1000 100x100 1000x10 (10k in all), 10x10000 100x1000 1000x100 (100k in all)
#   D  keys at 1M: 1 topic x 1k partitions, 1M and 10M keys (murmur2), batch mode
# Expected: ~3-4 min per point up to 10k partitions (reset + proof ~1 min, create, groups, settle, 80 s of load, collect);
# 50k/100k points add partition creation + warm + a 297-member classic group over 50k-100k partitions (see README).
set -u
H=$(cd "$(dirname "$0")/.." && pwd)
RUNS=$H/runs/redpanda
log() { echo "[$(date -u +%FT%TZ)] grid: $*"; }
OKS=""; FAILS=""; SKIPS=""; NPTS=0

pt() {  # pt <tag> <total rate> [KEY=VAL ...]
  local tag=$1 rc; shift
  NPTS=$((NPTS + 1))
  if [ -f "$RUNS/$tag/DONE" ]; then log "skip $tag (DONE)"; SKIPS="$SKIPS $tag"; return 0; fi
  if [ "${DRY:-0}" = 1 ]; then echo "  redpanda/run.sh $tag $*"; return 0; fi
  log "point $tag: $*"
  bash "$H/redpanda/run.sh" "$tag" "$@"; rc=$?
  if [ -f "$RUNS/$tag/DONE" ]; then OKS="$OKS $tag"; log "point $tag DONE"
  else FAILS="$FAILS $tag"; log "point $tag FAILED rc=$rc: $(tail -1 "$RUNS/$tag/FAILED" 2>/dev/null)"; fi
}

series_A() {  # rate, 1 topic x 200 partitions
  pt 1x200-300k   300000  TOPICS=1 PARTITIONS=200 CONSUMERS=22
  pt 1x200-600k   600000  TOPICS=1 PARTITIONS=200 CONSUMERS=22
  pt 1x200-1000k  1000000 TOPICS=1 PARTITIONS=200 CONSUMERS=22
  pt 1x200-1500k  1500000 TOPICS=1 PARTITIONS=200 CONSUMERS=22
  pt 1x200-2000k  2000000 TOPICS=1 PARTITIONS=200 CONSUMERS=22
  pt 1x200-3000k  3000000 TOPICS=1 PARTITIONS=200 CONSUMERS=22 GOMEMLIMIT=9GiB
  pt 1x200-4000k  4000000 TOPICS=1 PARTITIONS=200 CONSUMERS=22 GOMEMLIMIT=9GiB
}
series_B() {  # partitions, 1 topic at 1M; warm from 2k (run.sh WARM=auto); 50k/100k: longer create/group budgets
  pt 1x2000-1000k   1000000 TOPICS=1 PARTITIONS=2000   CONSUMERS=33
  pt 1x10000-1000k  1000000 TOPICS=1 PARTITIONS=10000  CONSUMERS=33
  pt 1x50000-1000k  1000000 TOPICS=1 PARTITIONS=50000  CONSUMERS=33 READY_TIMEOUT=900 GROUP_TIMEOUT=600s
  pt 1x100000-1000k 1000000 TOPICS=1 PARTITIONS=100000 CONSUMERS=33 READY_TIMEOUT=900 GROUP_TIMEOUT=600s GOMEMLIMIT=9GiB
}
series_C() {  # topics at 1M: 10 / 100 / 1000 topics x (10k and 100k partitions in all)
  pt 10x1000-1000k   1000000 TOPICS=10   PARTITIONS=1000  CONSUMERS=33
  pt 100x100-1000k   1000000 TOPICS=100  PARTITIONS=100   CONSUMERS=33
  pt 1000x10-1000k   1000000 TOPICS=1000 PARTITIONS=10    CONSUMERS=33
  pt 10x10000-1000k  1000000 TOPICS=10   PARTITIONS=10000 CONSUMERS=33 READY_TIMEOUT=900 GROUP_TIMEOUT=600s
  pt 100x1000-1000k  1000000 TOPICS=100  PARTITIONS=1000  CONSUMERS=33 READY_TIMEOUT=900 GROUP_TIMEOUT=600s
  pt 1000x100-1000k  1000000 TOPICS=1000 PARTITIONS=100   CONSUMERS=33 READY_TIMEOUT=900 GROUP_TIMEOUT=600s
}
series_D() {  # keys at 1M: 1 topic x 1k partitions, keyed by e<id> (murmur2), batch mode
  pt 1x1000-1000k-e1m  1000000 TOPICS=1 PARTITIONS=1000 ENTITIES=1000000  MODE=batch CONSUMERS=33
  pt 1x1000-1000k-e10m 1000000 TOPICS=1 PARTITIONS=1000 ENTITIES=10000000 MODE=batch CONSUMERS=33
}

main() {
  local s T0; T0=$(date +%s)
  mkdir -p "$RUNS"
  log "series ${POINTS:-A B C D} (hosts: ${HOSTS_ENV:-$H/hosts.env})$([ "${DRY:-0}" = 1 ] && echo ' DRY')"
  for s in ${POINTS:-A B C D}; do
    case $s in A|B|C|D) log "== series $s"; "series_$s" ;; *) log "unknown series $s (A B C D)"; return 2;; esac
  done
  log "end after $(( ($(date +%s) - T0) / 60 )) min | $NPTS points | done:${OKS:- none} | skipped:${SKIPS:- none} | FAILED:${FAILS:- none}"
  if [ "${DRY:-0}" != 1 ] && [ -f "$H/report/report.py" ] && ls -d "$RUNS"/*/DONE > /dev/null 2>&1; then
    python3 "$H/report/report.py" $(ls -d "$RUNS"/*/ | while read -r d; do [ -f "$d/DONE" ] && echo "$d"; done)
  fi
  [ -z "$FAILS" ]
}
main "$@"; exit $?
