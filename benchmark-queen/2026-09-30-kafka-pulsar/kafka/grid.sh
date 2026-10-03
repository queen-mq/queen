#!/usr/bin/env bash
# grid.sh (Mac) — the SPEC §7 matrix for Kafka as run.sh calls, one point at a time. A point whose
# runs/kafka/<tag>/DONE exists is skipped, so re-running grid.sh resumes after a failure or an interruption; a FAILED
# point is simply run again. Tags name the shape (<topics>x<partitions per topic>-<rate>k[-extra]), so a shape shared
# by two series (A's 1M point = B's 200-partition point) runs once.
#   POINTS="A B C D F"  series to run, in this order (default; E is the optional "real shape" ramp)
#   BEST_B=200          partitions of the best B point, for the F soak (decide after B; default 200)
#   DRY=1               print the run.sh calls, run nothing
# Every other variable (HOSTS_ENV, PROCS_PER_LOADER, KPROFILE, ...) passes through to run.sh. Consumers per SPEC §7:
# 22 per process at 1 topic x 200 partitions (198 members <= partitions), else 33 (297 in all).
# ~3-4 min per point at <= 10k partitions; 50k/100k add topic creation + warm (minutes); F is 15 min + setup.
set -u
H=$(cd "$(dirname "$0")/.." && pwd)
RUNS=$H/runs/kafka
log() { echo "[$(date -u +%FT%TZ)] grid: $*"; }
OKS=""; FAILS=""; SKIPS=""

pt() {  # pt <tag> <total rate> [KEY=VAL ...]
  local tag=$1 rc; shift
  if [ -f "$RUNS/$tag/DONE" ]; then log "skip $tag (DONE)"; SKIPS="$SKIPS $tag"; return 0; fi
  if [ "${DRY:-0}" = 1 ]; then echo "  kafka/run.sh $tag $*"; return 0; fi
  log "point $tag: $*"
  bash "$H/kafka/run.sh" "$tag" "$@"; rc=$?
  if [ -f "$RUNS/$tag/DONE" ]; then OKS="$OKS $tag"; log "point $tag DONE"
  else FAILS="$FAILS $tag"; log "point $tag FAILED rc=$rc: $(tail -1 "$RUNS/$tag/FAILED" 2>/dev/null)"; fi
}

series_A() {  # rate, 1 topic x 200 partitions
  pt 1x200-300k   300000 TOPICS=1 PARTITIONS=200 CONSUMERS=22
  pt 1x200-600k   600000 TOPICS=1 PARTITIONS=200 CONSUMERS=22
  pt 1x200-1000k 1000000 TOPICS=1 PARTITIONS=200 CONSUMERS=22
  pt 1x200-1500k 1500000 TOPICS=1 PARTITIONS=200 CONSUMERS=22
  pt 1x200-2000k 2000000 TOPICS=1 PARTITIONS=200 CONSUMERS=22
}
series_B() {  # partitions, 1 topic at 1M; warm from 2k (run.sh WARM=auto); 50k/100k: longer create/group budgets
  pt 1x200-1000k    1000000 TOPICS=1 PARTITIONS=200    CONSUMERS=22
  pt 1x2000-1000k   1000000 TOPICS=1 PARTITIONS=2000   CONSUMERS=33
  pt 1x10000-1000k  1000000 TOPICS=1 PARTITIONS=10000  CONSUMERS=33
  pt 1x50000-1000k  1000000 TOPICS=1 PARTITIONS=50000  CONSUMERS=33 READY_TIMEOUT=900 GROUP_TIMEOUT=600s
  pt 1x100000-1000k 1000000 TOPICS=1 PARTITIONS=100000 CONSUMERS=33 READY_TIMEOUT=900 GROUP_TIMEOUT=600s
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
series_E() {  # OPTIONAL real shape: 10 topics (zipf 1.0) x 100 partitions, 1M keys/topic zipf 1.1, keyed 1..20, proc-us 200;
  # the rate steps up until the worst e2e p99 passes 500 ms (partitions per topic are not fixed by the SPEC: 100 = D's 1k in all)
  local r p99
  for r in 200000 400000 600000 800000 1000000 1200000 1500000 2000000; do
    pt "10x100-$((r / 1000))k-real" "$r" TOPICS=10 PARTITIONS=100 TOPIC_DIST=zipf TOPIC_ZIPF_S=1.0 ENTITIES=1000000 \
      DIST=zipf ZIPF_S=1.1 MODE=keyed BATCH=1 BATCH_MAX=20 PROC_US=200 CONSUMERS=33
    [ "${DRY:-0}" = 1 ] && continue
    p99=$(sed -n 's/.*worst e2e p99=\([0-9.]*\) ms.*/\1/p' "$RUNS/10x100-$((r / 1000))k-real/summary.txt" 2>/dev/null | tail -1)
    log "E at $r msg/s: worst e2e p99 ${p99:-?} ms"
    [ -n "$p99" ] && awk -v p="$p99" 'BEGIN {exit !(p > 500)}' && { log "E: p99 > 500 ms at $r msg/s: stop"; break; }
  done
}
series_F() {  # soak: 15 min (+ 10 s ramp) at the best B point
  local p=${BEST_B:-200} c=33; [ "$p" -le 200 ] && c=22
  pt "1x$p-1000k-soak15m" 1000000 TOPICS=1 PARTITIONS="$p" CONSUMERS=$c DURATION=910s READY_TIMEOUT=900 GROUP_TIMEOUT=600s
}

main() {
  local s T0; T0=$(date +%s)
  mkdir -p "$RUNS"
  log "series ${POINTS:-A B C D F} (hosts: ${HOSTS_ENV:-$H/hosts.env})$([ "${DRY:-0}" = 1 ] && echo ' DRY')"
  for s in ${POINTS:-A B C D F}; do
    case $s in A|B|C|D|E|F) log "== series $s"; "series_$s" ;; *) log "unknown series $s (A B C D E F)"; return 2;; esac
  done
  log "end after $(( ($(date +%s) - T0) / 60 )) min | done:${OKS:- none} | skipped:${SKIPS:- none} | FAILED:${FAILS:- none}"
  if [ "${DRY:-0}" != 1 ] && [ -f "$H/report/report.py" ] && ls -d "$RUNS"/*/DONE > /dev/null 2>&1; then
    python3 "$H/report/report.py" $(ls -d "$RUNS"/*/ | while read -r d; do [ -f "$d/DONE" ] && echo "$d"; done)
  fi
  [ -z "$FAILS" ]
}
main "$@"; exit $?
