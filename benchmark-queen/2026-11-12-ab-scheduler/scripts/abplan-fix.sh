#!/usr/bin/env bash
# abplan-fix.sh (Mac): the push regression fix (sharded partition state, build `sched2`) on the bigger rig, the
# scheduler only: first what went bad (the push ladder, 1,000 partitions, 500k to 3M msg/s), then checkpoints of what
# went well (one or two steps of each transaction ladder at rates the first scheduler build carried). Same settings as
# abplan-big.sh / abplan-bigpush.sh, so the lines compare with big-*-281 and big-*-sched. Lines to ../runs/abplan-fix.out.
set -u
main() {
  HERE=$(cd "$(dirname "$0")" && pwd)
  export HOSTS_ENV=$HERE/hosts-big.env
  OUTF=$HERE/../runs/abplan-fix.out
  say "abplan-fix start"
  one push  PARTS=1000 CONS=17 PROCS=8 -- push 500000 1000000 1500000 2000000 2500000 3000000
  one t10   PARTS=200 TXN_SIZE=10 KV=0 PROCS=8 CONS=25 -- txn 200000 240000
  one t10kv PARTS=200 TXN_SIZE=10 KV=1 PROCS=8 CONS=25 -- txn 200000
  one t10w  PARTS=900 TXN_SIZE=10 KV=0 PROCS=8 CONS=55 -- txn 280000
  one t100  PARTS=200 TXN_SIZE=100 KV=0 PROCS=8 CONS=25 -- txn 750000
  say "ABPLAN_FIX_DONE"
}
say() { echo "[$(date -u +%H:%M:%S)] $*" >> "$OUTF"; }
one() {  # one <workload> KEY=VAL... -- <txn|push> <rates...>
  local w=$1 tag=fix-$1-sched2; shift
  local envs=()
  while [ "$1" != "--" ]; do envs+=("$1"); shift; done
  shift
  local mode=$1; shift
  say "== $w on sched2"
  env "${envs[@]}" bash "$HERE/ab.sh" $tag sched2 $mode 40 "$@" 2>&1 \
    | tee "$HERE/../runs/$tag.console" | grep --line-buffered -E "^\[..:..:..\] +[0-9,]+:|FAILED|stopping" \
    | sed -l "s/^/$tag /" >> "$OUTF"
}
main "$@"; exit $?
