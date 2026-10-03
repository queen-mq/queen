#!/usr/bin/env bash
# grid.sh [A B C D E F]... (Mac) — the SPEC §7 matrix for Pulsar as run.sh calls, 1M msg/s offered unless stated,
# 256 B, batch 100, 60 s + 10 s ramp (run.sh defaults). Points whose $RUNS/<tag>/DONE exists are skipped, so a
# re-run resumes; 2 consecutive failures stop the grid. Default sets: A B C D F (E is optional: `grid.sh E`).
#   A  rate, 1 topic x 200 partitions: 300k 600k 1M 1.5M 2M                       tags pA-r300k .. pA-r2000k
#   B  partitions, 1 topic at 1M, failover subscription: 200 2k 10k 50k 100k      tags pB-p200 .. pB-p100000
#   C  topics at 1M: 10 / 100 / 1000 topics x (10k and 100k partitions in total)  tags pC-t10-p10k .. pC-t1000-p100k
#   D  keys at 1M: 1 topic x 48 partitions, Key_Shared, entities 1M and 10M       tags pD-e1m pD-e10m
#   E  real shape (optional): 10 topics (topic zipf 1.0) x 48 partitions, 1M keys/topic zipf 1.1, keyed 1..20,
#      proc-us 200; the rate steps up (E_RATES) until the worst process's e2e p99 > 500 ms    tags pE-r<k>k
#   F  soak: 15 min at the best B point (SOAK_PARTITIONS, default 200 — set it after looking at B)  tag pF-soak-p<P>
# Consumers per process (SPEC §7): 22 at <= 200 partitions in total, else 33 (run.sh's default rule, explicit here).
# Env: HOSTS_ENV, RUNS (default $H/runs/pulsar), EXTRA (KEY=VAL added to every point, e.g. "JOURNALS=2"),
#      SOAK_PARTITIONS, SOAK_DURATION (910s = 15 min + ramp), E_RATES, DRY=1 (print the run.sh calls, run nothing).
set -u
. "$(cd "$(dirname "$0")" && pwd)/lib.sh"
RUNS=${RUNS:-$H/runs/pulsar}; export RUNS
FAILS=0
point() {  # point <tag> <rate> [KEY=VAL...]
  local tag=$1; shift
  if [ -f "$RUNS/$tag/DONE" ]; then log "skip $tag (DONE)"; return 0; fi
  if [ "${DRY:-0}" = 1 ]; then echo "run.sh $tag $* ${EXTRA:-}"; return 0; fi
  log "point $tag: $* ${EXTRA:-}"
  if "$PDIR/run.sh" "$tag" "$@" ${EXTRA:-}; then FAILS=0
  else
    FAILS=$((FAILS + 1)); log "FAILED $tag ($FAILS in a row)"
    [ $FAILS -ge 2 ] && die "2 consecutive failures: grid stopped (fix, then re-run: DONE points are skipped)"
  fi
  return 0
}
cons() { if [ "$1" -le 200 ]; then echo 22; else echo 33; fi; }
big() { if [ "$1" -ge 10000 ]; then echo "READY_TIMEOUT=1800 TOPIC_TIMEOUT=1800s"; fi; }   # 10k+ partitions: slow setup
A() { local r; for r in 300000 600000 1000000 1500000 2000000; do point "pA-r$((r / 1000))k" $r PARTITIONS=200 CONSUMERS=22; done; }
B() { local p; for p in 200 2000 10000 50000 100000; do point "pB-p$p" 1000000 PARTITIONS=$p SUB_TYPE=failover CONSUMERS=$(cons $p) $(big $p); done; }
C() {
  local t tot
  for t in 10 100 1000; do for tot in 10000 100000; do
    point "pC-t$t-p$((tot / 1000))k" 1000000 TOPICS=$t PARTITIONS=$((tot / t)) CONSUMERS=33 $(big $tot)
  done; done
}
D() { local e; for e in 1000000 10000000; do point "pD-e$((e / 1000000))m" 1000000 PARTITIONS=48 ENTITIES=$e SUB_TYPE=key_shared CONSUMERS=22; done; }
E() {  # optional real shape: step the rate up until e2e p99 (worst process, report.py) exceeds 500 ms
  local r p99
  for r in ${E_RATES:-200000 400000 600000 800000 1000000 1200000}; do
    point "pE-r$((r / 1000))k" $r TOPICS=10 TOPIC_DIST=zipf TOPIC_ZIPF_S=1.0 PARTITIONS=48 ENTITIES=1000000 DIST=zipf \
      ZIPF_S=1.1 MODE=keyed BATCH=1 BATCH_MAX=20 PROC_US=200 CONSUMERS=33
    [ "${DRY:-0}" = 1 ] && continue
    p99=$(python3 "$H/report/report.py" --csv "$RUNS/pE-r$((r / 1000))k" 2>/dev/null | awk -F, 'NR==2{print $9}')
    log "pE-r$((r / 1000))k: e2e p99 ${p99:-?} ms"
    case ${p99:-x} in ''|x|*[!0-9.]*) ;; *) awk -v p="$p99" 'BEGIN{exit !(p > 500)}' && { log "E: p99 > 500 ms at $r msg/s: stop"; break; } ;; esac
  done
}
F() { local p=${SOAK_PARTITIONS:-200}; point "pF-soak-p$p" 1000000 PARTITIONS=$p DURATION=${SOAK_DURATION:-910s} CONSUMERS=$(cons $p) $(big $p); }

SETS=${*:-A B C D F}
for s in $SETS; do
  case $s in A|B|C|D|E|F) log "=== set $s"; $s ;; *) die "unknown set $s (A B C D E F)";; esac
done
log "grid done: $(ls -d "$RUNS"/*/DONE 2>/dev/null | wc -l | tr -d ' ') points DONE in $RUNS"
