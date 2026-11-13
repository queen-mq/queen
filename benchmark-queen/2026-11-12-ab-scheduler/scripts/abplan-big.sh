#!/usr/bin/env bash
# abplan-big.sh (Mac): the transaction ladders of abplan.sh on the bigger rig (hosts-big.env: brokers c3-standard-22-lssd,
# loader c4-highcpu-24), every ladder on 2.8.1 first and then on the scheduler's build, 40 s steps, eight qload processes
# with the same worker totals as the first rig. Lines to ../runs/abplan-big.out as they land.
#   t10    transactions of 10 messages, 200 partitions, 200 workers
#   t10kv  the same, each transaction also writing one KV key per entity
#   t10w   transactions of 10 messages, 900 partitions, 440 workers
#   t100   transactions of 100 messages, 200 partitions, 200 workers
# ONLY="t10 t10w" runs a subset; BUILDS="281 sched" the order of the builds.
set -u
main() {
  HERE=$(cd "$(dirname "$0")" && pwd)
  export HOSTS_ENV=$HERE/hosts-big.env
  OUTF=$HERE/../runs/abplan-big.out; mkdir -p "$HERE/../runs"
  ONLY=${ONLY:-"t10 t10kv t10w t100"}; BUILDS=${BUILDS:-"281 sched"}
  say "abplan-big start: $ONLY | builds $BUILDS"
  for w in $ONLY; do
    for b in $BUILDS; do
      say "== $w on $b"
      one $w $b
    done
  done
  say "ABPLAN_BIG_DONE"
}
say() { echo "[$(date -u +%H:%M:%S)] $*" >> "$OUTF"; }
one() {  # one <workload> <build>
  local w=$1 b=$2 tag=big-$1-$2
  case $w in
    t10)   PARTS=200 TXN_SIZE=10 KV=0 PROCS=8 CONS=25 bash "$HERE/ab.sh" $tag $b txn 40 80000 120000 160000 200000 240000 280000 ;;
    t10kv) PARTS=200 TXN_SIZE=10 KV=1 PROCS=8 CONS=25 bash "$HERE/ab.sh" $tag $b txn 40 80000 120000 160000 200000 240000 ;;
    t10w)  PARTS=900 TXN_SIZE=10 KV=0 PROCS=8 CONS=55 bash "$HERE/ab.sh" $tag $b txn 40 120000 160000 200000 240000 280000 320000 ;;
    t100)  PARTS=200 TXN_SIZE=100 KV=0 PROCS=8 CONS=25 bash "$HERE/ab.sh" $tag $b txn 40 150000 300000 450000 600000 750000 ;;
  esac 2>&1 | tee "$HERE/../runs/$tag.console" | grep --line-buffered -E "^\[..:..:..\] +[0-9,]+:|FAILED|stopping" | sed -l "s/^/$tag /" >> "$OUTF"
}
main "$@"; exit $?
