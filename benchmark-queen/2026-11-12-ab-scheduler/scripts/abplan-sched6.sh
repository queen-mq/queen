#!/usr/bin/env bash
# abplan-sched6.sh (Mac): `sched6` on the bigger rig: the route hands each planning thread whole shards of the
# partition state for a run (bins, planned in place, no merge of them), transactions roll back from a savepoint
# instead of a copy of the overlay. The one-partition pushes at 3M and 3.5M msg/s, the t100 transactions at 750k,
# then the multi-push (10 partitions a request) at 2M and 2.5M. Same settings as abplan-spread.sh / abplan-fix4.sh.
# Lines to ../runs/abplan-sched6.out.
set -u
main() {
  HERE=$(cd "$(dirname "$0")" && pwd)
  export HOSTS_ENV=$HERE/hosts-big.env
  OUTF=$HERE/../runs/abplan-sched6.out
  say "abplan-sched6 start"
  one push   PARTS=1000 CONS=17 PROCS=8 SPREAD=1 -- push 3000000 3500000
  one t100   PARTS=200 TXN_SIZE=100 KV=0 PROCS=8 CONS=25 -- txn 750000
  one spread PARTS=1000 CONS=17 PROCS=8 SPREAD=10 -- push 2000000 2500000
  say "ABPLAN_SCHED6_DONE"
}
say() { echo "[$(date -u +%H:%M:%S)] $*" >> "$OUTF"; }
one() {  # one <workload> KEY=VAL... -- <txn|push> <rates...>
  local w=$1 tag=s6-$1-sched6; shift
  local envs=()
  while [ "$1" != "--" ]; do envs+=("$1"); shift; done
  shift
  local mode=$1; shift
  say "== $w on sched6"
  env "${envs[@]}" bash "$HERE/ab.sh" $tag sched6 $mode 40 "$@" 2>&1 \
    | tee "$HERE/../runs/$tag.console" | grep --line-buffered -E "^\[..:..:..\] +[0-9,]+:|FAILED|stopping" \
    | sed -l "s/^/$tag /" >> "$OUTF"
}
main "$@"; exit $?
