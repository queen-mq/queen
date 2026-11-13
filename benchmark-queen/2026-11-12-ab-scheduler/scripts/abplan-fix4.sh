#!/usr/bin/env bash
# abplan-fix4.sh (Mac): build `sched4` (sched3 plus: the link hint the receiver and the forward decoder set, so the
# cycle thread reads no frame to footprint a push; the group overlays boxed; the planned commands and the merged group
# overlays freed on the planner's free thread) on the bigger rig, the scheduler only: pushes at 2.5M, 3M, 3.5M msg/s,
# then the t10 and t100 checkpoints (t100 again for its last-window spike). Same settings as abplan-fix3.sh. Lines to
# ../runs/abplan-fix4.out.
set -u
main() {
  HERE=$(cd "$(dirname "$0")" && pwd)
  export HOSTS_ENV=$HERE/hosts-big.env
  OUTF=$HERE/../runs/abplan-fix4.out
  say "abplan-fix4 start"
  one push  PARTS=1000 CONS=17 PROCS=8 -- push 2500000 3000000 3500000
  one t10   PARTS=200 TXN_SIZE=10 KV=0 PROCS=8 CONS=25 -- txn 240000
  one t100  PARTS=200 TXN_SIZE=100 KV=0 PROCS=8 CONS=25 -- txn 750000
  say "ABPLAN_FIX4_DONE"
}
say() { echo "[$(date -u +%H:%M:%S)] $*" >> "$OUTF"; }
one() {  # one <workload> KEY=VAL... -- <txn|push> <rates...>
  local w=$1 tag=fix4-$1-sched4; shift
  local envs=()
  while [ "$1" != "--" ]; do envs+=("$1"); shift; done
  shift
  local mode=$1; shift
  say "== $w on sched4"
  env "${envs[@]}" bash "$HERE/ab.sh" $tag sched4 $mode 40 "$@" 2>&1 \
    | tee "$HERE/../runs/$tag.console" | grep --line-buffered -E "^\[..:..:..\] +[0-9,]+:|FAILED|stopping" \
    | sed -l "s/^/$tag /" >> "$OUTF"
}
main "$@"; exit $?
