#!/usr/bin/env bash
# abplan-homes.sh <build> [ONLY="push spread t10 t100 t10kv t10w"] (Mac): one build of the homes planner on the bigger
# rig, the same ladders as abplan-base13.sh (2.8.1 and sched7 on 2026-11-13, same rig, same settings), so the lines
# compare. Every ladder stops at its first step not carried. Lines to ../runs/abplan-<build>.out as they land.
set -u
main() {
  HERE=$(cd "$(dirname "$0")" && pwd)
  export HOSTS_ENV=$HERE/hosts-big.env
  BUILD=${1:?the build name under /root/p3/bin}
  ONLY=${ONLY:-"push spread t10 t100 t10kv t10w"}
  OUTF=$HERE/../runs/abplan-$BUILD.out
  say "abplan-homes start: $BUILD | $ONLY"
  for w in $ONLY; do
    case $w in
      push)   one push   PARTS=1000 CONS=17 PROCS=8 SPREAD=1  -- push 3000000 3250000 3500000 3750000 4000000 ;;
      spread) one spread PARTS=1000 CONS=17 PROCS=8 SPREAD=10 -- push 1000000 1500000 2000000 2250000 ;;
      t10)    one t10    PARTS=200 TXN_SIZE=10 KV=0 PROCS=8 CONS=25  -- txn 240000 280000 320000 360000 ;;
      t100)   one t100   PARTS=200 TXN_SIZE=100 KV=0 PROCS=8 CONS=25 -- txn 750000 900000 1050000 1200000 1350000 ;;
      t10kv)  one t10kv  PARTS=200 TXN_SIZE=10 KV=1 PROCS=8 CONS=25  -- txn 200000 240000 280000 320000 ;;
      t10w)   one t10w   PARTS=900 TXN_SIZE=10 KV=0 PROCS=8 CONS=55  -- txn 280000 320000 360000 400000 ;;
    esac
  done
  say "ABPLAN_HOMES_DONE $BUILD"
}
say() { echo "[$(date -u +%H:%M:%S)] $*" >> "$OUTF"; }
one() {  # one <workload> KEY=VAL... -- <txn|push> <rates...>
  local w=$1 tag=h-$1-$BUILD; shift
  local envs=()
  while [ "$1" != "--" ]; do envs+=("$1"); shift; done
  shift
  local mode=$1; shift
  say "== $w on $BUILD"
  env "${envs[@]}" bash "$HERE/ab.sh" $tag $BUILD $mode 40 "$@" 2>&1 \
    | tee "$HERE/../runs/$tag.console" | grep --line-buffered -E "^\[..:..:..\] +[0-9,]+:|FAILED|stopping" \
    | sed -l "s/^/$tag /" >> "$OUTF"
}
main "$@"; exit $?
