#!/usr/bin/env bash
# abplan-base13.sh (Mac): the baselines that were never measured, run on 2026-11-13 while the homes planner is being
# written, so the rig is not idle. Same rig and settings as 11-12 (hosts-big.env). Every ladder stops at its first step
# not carried.
#   push   281     one-partition pushes ABOVE 3M: the ceiling of 2.8.1 (11-12 stopped at 3M)
#   push   sched7  the same on sched7 (11-12 ran 3M only)
#   spread sched7  multi-push, 10 partitions a request (11-12 measured 2.8.1, sched5 and sched6, not sched7)
#   t10, t100, t10kv, t10w  sched7 above the one rate of 11-12 that each carried: where transactions stop now
# Lines to ../runs/abplan-base13.out as they land.
set -u
main() {
  HERE=$(cd "$(dirname "$0")" && pwd)
  export HOSTS_ENV=$HERE/hosts-big.env
  OUTF=$HERE/../runs/abplan-base13.out
  say "abplan-base13 start"
  one push   281    PARTS=1000 CONS=17 PROCS=8 SPREAD=1  -- push 3000000 3250000 3500000 3750000
  one push   sched7 PARTS=1000 CONS=17 PROCS=8 SPREAD=1  -- push 3000000 3250000 3500000
  one spread sched7 PARTS=1000 CONS=17 PROCS=8 SPREAD=10 -- push 1000000 1500000 2000000
  one t10    sched7 PARTS=200 TXN_SIZE=10 KV=0 PROCS=8 CONS=25  -- txn 240000 280000 320000 360000 400000
  one t100   sched7 PARTS=200 TXN_SIZE=100 KV=0 PROCS=8 CONS=25 -- txn 750000 900000 1050000 1200000
  one t10kv  sched7 PARTS=200 TXN_SIZE=10 KV=1 PROCS=8 CONS=25  -- txn 200000 240000 280000 320000
  one t10w   sched7 PARTS=900 TXN_SIZE=10 KV=0 PROCS=8 CONS=55  -- txn 280000 320000 360000 400000
  say "ABPLAN_BASE13_DONE"
}
say() { echo "[$(date -u +%H:%M:%S)] $*" >> "$OUTF"; }
one() {  # one <workload> <build> KEY=VAL... -- <txn|push> <rates...>
  local w=$1 b=$2 tag=b13-$1-$2; shift 2
  local envs=()
  while [ "$1" != "--" ]; do envs+=("$1"); shift; done
  shift
  local mode=$1; shift
  say "== $w on $b"
  env "${envs[@]}" bash "$HERE/ab.sh" $tag $b $mode 40 "$@" 2>&1 \
    | tee "$HERE/../runs/$tag.console" | grep --line-buffered -E "^\[..:..:..\] +[0-9,]+:|FAILED|stopping" \
    | sed -l "s/^/$tag /" >> "$OUTF"
}
main "$@"; exit $?
