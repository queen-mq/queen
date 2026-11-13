#!/usr/bin/env bash
# abplan-sched7.sh (Mac): `sched7` on the bigger rig: sched6 (bins: the route hands each planning thread whole
# shards of the partition state for a run, planned in place; transactions roll back from a savepoint) plus the
# existence check of a cursor or dead-letter row from the committed row, so checkpoints no longer chain shards.
# The one-partition pushes at 3M msg/s only. Same settings as abplan-sched6.sh. Lines to ../runs/abplan-sched7.out.
set -u
main() {
  HERE=$(cd "$(dirname "$0")" && pwd)
  export HOSTS_ENV=$HERE/hosts-big.env
  OUTF=$HERE/../runs/abplan-sched7.out
  say "abplan-sched7 start"
  one push   PARTS=1000 CONS=17 PROCS=8 SPREAD=1 -- push 3000000
  say "ABPLAN_SCHED7_DONE"
}
say() { echo "[$(date -u +%H:%M:%S)] $*" >> "$OUTF"; }
one() {  # one <workload> KEY=VAL... -- <txn|push> <rates...>
  local w=$1 tag=s7-$1-sched7; shift
  local envs=()
  while [ "$1" != "--" ]; do envs+=("$1"); shift; done
  shift
  local mode=$1; shift
  say "== $w on sched7"
  env "${envs[@]}" bash "$HERE/ab.sh" $tag sched7 $mode 40 "$@" 2>&1 \
    | tee "$HERE/../runs/$tag.console" | grep --line-buffered -E "^\[..:..:..\] +[0-9,]+:|FAILED|stopping" \
    | sed -l "s/^/$tag /" >> "$OUTF"
}
main "$@"; exit $?
