#!/usr/bin/env bash
# abplan.sh [ONLY="push spread t10 t100 t10kv t10w g1 g3 f1 f3"] (controller qab-l1): the 11-15 A/B, 2.8.1 against the
# build under test ("new", jepsen-all), each workload on both builds back to back so drift hits both alike. The
# 11-13 workloads (abplan-homes.sh) with the load on three loaders, then the consumer-group and filtered-pop ladders:
#   g1: one plain consumer group           (SHARE 1)   both builds
#   g3: three plain consumer groups        (SHARE 3)   both builds: fan-out, every group gets everything
#   f1: one filtered group, data.k < 30    (SHARE 0.3) new only (2.8.1 has no filtered groups)
#   f3: three filtered groups splitting the stream by data.k: < 34, 34..66, >= 67 (SHARE 1) new only
#   single (not in the default ONLY; her ask 11-15): push + pop with ONE message a push request (BATCH=1, so
#           msg/s = push requests/s), 1000 partitions, the same consumers and forms as push: the request ceiling
#   rg3, rg1t (not in the default ONLY; her ask 11-15 "a ceiling test on multiraft with QUEEN_RAFT_GROUPS=3"): the push
#           ladder as three tenants of 333 partitions, one a loader (ab3.sh TEN=3), in three raft groups (rg3: tenant
#           t in group t, group g led by member g) and, on new only, in one group (rg1t: what the tenants alone change)
#   t10x, t10wx (not in the default ONLY; her ask 11-15): t10 and t10w on new only, from the top of the first ladders
#           (new carried 320k and 360k there without failing) up, to find the ceiling
# Every ladder stops at its first step not carried. Lines to runs/abplan.out as they land.
set -u
main() {
  HERE=$(cd "$(dirname "$0")" && pwd)
  ONLY=${ONLY:-"push spread t10 t100 t10kv t10w g1 g3 f1 f3"}
  BUILDS=${BUILDS:-"281 new"}
  OUTF=$HERE/runs/abplan.out; mkdir -p "$HERE/runs"
  say "abplan start: $ONLY | builds $BUILDS"
  for w in $ONLY; do
    for b in $BUILDS; do
      case $w in
        push)   one push   $b PARTS=1000 CONS=15 SPREAD=1  -- push 3000000 3500000 4000000 4500000 5000000 5500000 ;;
        spread) one spread $b PARTS=1000 CONS=15 SPREAD=10 -- push 1000000 1500000 2000000 2500000 3000000 3500000 ;;
        t10)    one t10    $b PARTS=200 TXN_SIZE=10 KV=0 CONS=22  -- txn 120000 160000 200000 240000 280000 320000 ;;
        t100)   one t100   $b PARTS=200 TXN_SIZE=100 KV=0 CONS=22 -- txn 250000 500000 750000 1000000 1250000 1500000 ;;
        t10kv)  one t10kv  $b PARTS=200 TXN_SIZE=10 KV=1 CONS=22  -- txn 100000 150000 200000 250000 300000 ;;
        t10w)   one t10w   $b PARTS=900 TXN_SIZE=10 KV=0 CONS=49  -- txn 80000 160000 240000 280000 320000 360000 ;;
        single) one single $b PARTS=1000 CONS=15 SPREAD=1 BATCH=1 -- push 100000 200000 300000 400000 500000 600000 800000 1000000 1250000 1500000 ;;
        t10x)   [ $b = 281 ] && continue
                one t10x   $b PARTS=200 TXN_SIZE=10 KV=0 CONS=22  -- txn 320000 360000 400000 450000 500000 550000 600000 ;;
        t10wx)  [ $b = 281 ] && continue
                one t10wx  $b PARTS=900 TXN_SIZE=10 KV=0 CONS=49  -- txn 360000 400000 450000 500000 550000 600000 700000 ;;
        rg3)    one rg3    $b PARTS=1000 CONS=15 SPREAD=1 TEN=3 RG=3 -- push 4000000 5000000 6000000 7000000 8000000 9000000 10000000 ;;
        rg1t)   [ $b = 281 ] && continue
                one rg1t   $b PARTS=1000 CONS=15 SPREAD=1 TEN=3 RG=1 -- push 3500000 4000000 4500000 5000000 ;;
        g1)     one g1   $b PARTS=1000 CONS=15 SPREAD=1 GROUPS_ARGS="-groups 1" SHARE=1 -- push 1000000 2000000 2500000 3000000 3500000 4000000 ;;
        g3)     one g3     $b PARTS=1000 CONS=15 SPREAD=1 GROUPS_ARGS="-groups 3" SHARE=3 -- push 250000 500000 1000000 1500000 2000000 2500000 ;;
        f1)     [ $b = 281 ] && continue
                one f1     $b PARTS=1000 CONS=15 SPREAD=1 GROUPS_ARGS="-group-where 'data.k < 30' -stamp-k 100" SHARE=0.3 -- push 500000 1000000 1500000 2000000 2500000 3000000 3500000 ;;
        f3)     [ $b = 281 ] && continue
                one f3     $b PARTS=1000 CONS=15 SPREAD=1 GROUPS_ARGS="-group-where 'data.k < 34;data.k >= 34 and data.k < 67;data.k >= 67' -stamp-k 100" SHARE=1 -- push 500000 1000000 1500000 2000000 2500000 3000000 3500000 ;;
      esac
    done
  done
  say "ABPLAN_DONE"
}
say() { echo "[$(date -u +%H:%M:%S)] $*" >> "$OUTF"; }
one() {  # one <workload> <build> KEY=VAL... -- <txn|push> <rates...>
  local w=$1 b=$2 tag=$1-$2; shift 2
  local envs=()
  while [ "$1" != "--" ]; do envs+=("$1"); shift; done
  shift
  local mode=$1; shift
  say "== $w on $b"
  env "${envs[@]}" bash "$HERE/ab3.sh" $tag $b $mode ${DUR:-40} "$@" 2>&1 \
    | tee "$HERE/runs/$tag.console" | grep --line-buffered -E "^\[..:..:..\] +[0-9,]+:|FAILED|stopping" \
    | sed -u "s/^/$tag /" >> "$OUTF"
}
main "$@"; exit $?
