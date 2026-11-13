#!/usr/bin/env bash
# abplan-fix3.sh (Mac): build `sched3` (the push fix plus: merge only the shards a group wrote, the landing bucketed
# by shard in one pass and run on the planning threads while the cycle routes, the whole cycle timed) on the bigger
# rig, the scheduler only: first what went bad (pushes at 2M, 2.5M, 3M msg/s), then one checkpoint per transaction
# ladder at a rate the first build carried. Same settings as abplan-fix.sh. Lines to ../runs/abplan-fix3.out.
set -u
main() {
  HERE=$(cd "$(dirname "$0")" && pwd)
  export HOSTS_ENV=$HERE/hosts-big.env
  OUTF=$HERE/../runs/abplan-fix3.out
  say "abplan-fix3 start"
  one push  PARTS=1000 CONS=17 PROCS=8 -- push 2000000 2500000 3000000
  one t10   PARTS=200 TXN_SIZE=10 KV=0 PROCS=8 CONS=25 -- txn 240000
  one t10w  PARTS=900 TXN_SIZE=10 KV=0 PROCS=8 CONS=55 -- txn 280000
  one t10kv PARTS=200 TXN_SIZE=10 KV=1 PROCS=8 CONS=25 -- txn 200000
  one t100  PARTS=200 TXN_SIZE=100 KV=0 PROCS=8 CONS=25 -- txn 750000
  say "ABPLAN_FIX3_DONE"
}
say() { echo "[$(date -u +%H:%M:%S)] $*" >> "$OUTF"; }
one() {  # one <workload> KEY=VAL... -- <txn|push> <rates...>
  local w=$1 tag=fix3-$1-sched3; shift
  local envs=()
  while [ "$1" != "--" ]; do envs+=("$1"); shift; done
  shift
  local mode=$1; shift
  say "== $w on sched3"
  env "${envs[@]}" bash "$HERE/ab.sh" $tag sched3 $mode 40 "$@" 2>&1 \
    | tee "$HERE/../runs/$tag.console" | grep --line-buffered -E "^\[..:..:..\] +[0-9,]+:|FAILED|stopping" \
    | sed -l "s/^/$tag /" >> "$OUTF"
}
main "$@"; exit $?
