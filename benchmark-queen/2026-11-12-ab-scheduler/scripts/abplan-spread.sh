#!/usr/bin/env bash
# abplan-spread.sh (Mac): multi-push on the bigger rig, 2.8.1 against `sched5` (sched4 plus the review fixes: the
# dedup front forgets the partitions a group created, the budget never defers a unit ahead of a planned piece, the
# replay restores its overlay, footprints taken again after a command planned alone). Every push request spreads its
# 100 messages over 10 consecutive partitions (qload -push-spread 10): one multi-push of 10 pieces. Then the
# one-partition pushes at 3M and 3.5M on sched5, the final build. Lines to ../runs/abplan-spread.out.
set -u
main() {
  HERE=$(cd "$(dirname "$0")" && pwd)
  export HOSTS_ENV=$HERE/hosts-big.env
  OUTF=$HERE/../runs/abplan-spread.out
  say "abplan-spread start"
  one spread 281    PARTS=1000 CONS=17 PROCS=8 SPREAD=10 -- push 1000000 2000000 3000000
  one spread sched5 PARTS=1000 CONS=17 PROCS=8 SPREAD=10 -- push 1000000 2000000 3000000
  one push   sched5 PARTS=1000 CONS=17 PROCS=8 SPREAD=1  -- push 3000000 3500000
  say "ABPLAN_SPREAD_DONE"
}
say() { echo "[$(date -u +%H:%M:%S)] $*" >> "$OUTF"; }
one() {  # one <workload> <build> KEY=VAL... -- <txn|push> <rates...>
  local w=$1 b=$2 tag=sp-$1-$2; shift 2
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
