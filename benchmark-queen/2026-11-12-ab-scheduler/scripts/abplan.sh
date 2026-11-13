#!/usr/bin/env bash
# abplan.sh (Mac): the A/B of 2026-11-12, 2.8.1 against the footprint scheduler, every ladder on 2.8.1 first and then
# on the scheduler's build, 40 s steps. Every finished step's line goes to ../runs/abplan.out as it lands.
#   t10     transactions of 10 messages, 200 partitions in and out, 50 workers a process (200, as 10-12)
#   t10kv   the same, each transaction also writing one KV key per entity (a workflow step)
#   t10w    transactions of 10 messages, 900 partitions, 110 workers a process (440)
#   t100    transactions of 100 messages, 200 partitions
#   push    pushes of 100 messages to 1,000 partitions, popped and acked
# ONLY="t10 push" runs a subset; BUILDS="281 sched" the order of the builds.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
OUTF=$HERE/../runs/abplan.out; mkdir -p "$HERE/../runs"
ONLY=${ONLY:-"t10 t10kv t10w t100 push"}; BUILDS=${BUILDS:-"281 sched"}
say() { echo "[$(date -u +%H:%M:%S)] $*" >> "$OUTF"; }
one() {  # one <workload> <build>
  local w=$1 b=$2 tag=ab-$1-$2
  case $w in
    t10)   PARTS=200 TXN_SIZE=10 KV=0 CONS=50 bash "$HERE/ab.sh" $tag $b txn 40 20000 40000 60000 80000 100000 120000 140000 160000 ;;
    t10kv) PARTS=200 TXN_SIZE=10 KV=1 CONS=50 bash "$HERE/ab.sh" $tag $b txn 40 20000 40000 60000 80000 100000 120000 140000 ;;
    t10w)  PARTS=900 TXN_SIZE=10 KV=0 CONS=110 bash "$HERE/ab.sh" $tag $b txn 40 40000 80000 120000 160000 200000 ;;
    t100)  PARTS=200 TXN_SIZE=100 KV=0 CONS=50 bash "$HERE/ab.sh" $tag $b txn 40 50000 100000 150000 200000 250000 300000 ;;
    push)  PARTS=1000 CONS=33 bash "$HERE/ab.sh" $tag $b push 40 200000 400000 600000 800000 1000000 1200000 ;;
  esac 2>&1 | tee "$HERE/../runs/$tag.console" | grep --line-buffered -E "^\[..:..:..\] +[0-9,]+:|FAILED|stopping" | sed -l "s/^/$tag /" >> "$OUTF"
}
say "abplan start: $ONLY | builds $BUILDS"
for w in $ONLY; do
  for b in $BUILDS; do
    say "== $w on $b"
    one $w $b
  done
done
say "ABPLAN_DONE"
