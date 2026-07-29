#!/usr/bin/env bash
# B1 — proxy overhead. Same offered load, same cell shape, two targets.
#
# Protocol:
#   * the DB is dropped/recreated and the broker restarted before EVERY point,
#     so no run inherits another run's retained rows or partition state
#   * broker and proxy points at a given rate run back to back, and the order
#     ALTERNATES between rates, so any slow drift of the rig cannot masquerade
#     as proxy overhead
#   * drain 45s everywhere: a partition can stop being handed out for ~30s
#     (measured), and a drain shorter than that reports a stall as loss
#   * -target broker sends x-queen-tenant, the same UUID the proxy injects, so
#     both targets hit the same broker-side tenant rows
set -uo pipefail
G=/root/queen/benchmark-queen/2026-07-29-vm-campaign
OUT=/root/campaign/B1
SHAPE="cell 4c/8G slice-capped (PG+pxdb+broker+proxy in queencell.slice), PG over docker0 bridge"
COMMON="-mode cloud -tenants-file $OUT/tenants.json -tenants 4 -shared-queue -queue orders -group workers -partitions 4 -push-batch 1 -producers-per-tenant 2 -consumers-per-tenant 4 -pop-batch 50 -pop-wait -payload 256 -fail-on-verify=false -out $OUT"

point() { # runid target rate duration drain
  bash $G/reset-cell-db.sh >/dev/null 2>&1
  bash $G/runpt.sh $OUT "$1" -- $COMMON -target "$2" -rate "$3" -duration "$4" -drain "$5" \
    -run-id "$1" -note "B1 $SHAPE; 4 tenants x 4 partitions; pushBatch=1 popBatch=50 lease+ack; db reset before run" \
    >/dev/null 2>&1
  echo "  [$(date -u +%T)] $1 exit=$?"
}

echo "=== B1a: per-request added latency at low rate (100 msg/s, no queueing) ==="
for i in 1 2 3; do
  if [ $((i % 2)) -eq 1 ]; then a=broker; b=proxy; else a=proxy; b=broker; fi
  point "lat-$a-100-r$i" $a 100 60 15
  point "lat-$b-100-r$i" $b 100 60 15
done

echo "=== B1b: sweep at 25/50/75/95% of the 2400 msg/s direct ceiling ==="
for pair in "600 broker proxy" "1200 proxy broker" "1800 broker proxy" "2300 proxy broker"; do
  set -- $pair; r=$1; a=$2; b=$3
  point "sweep-$a-$r" $a $r 45 45
  point "sweep-$b-$r" $b $r 45 45
done

echo "=== B1c: drift control — repeat the first sweep point last ==="
point "drift-broker-600" broker 600 45 45
echo "=== B1 done ==="
