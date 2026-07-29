#!/usr/bin/env bash
# S1 — FULL-MACHINE CEILING. Uncapped cell (queencell.slice cpu.max=max) on the
# 8 vCPU box. Ramp the offered rate until achieved stops tracking offered, and
# record what saturates first.
#
# Protocol (same as B1/B2 so the numbers are comparable):
#   * DB dropped/recreated + broker restarted before EVERY point
#   * duration 45s load + 45s drain (a partition can go dark for ~30s; a shorter
#     drain reports a stall as loss)
#   * correctness verified IN the run (per-message bitmaps, -verify default on)
#   * -fail-on-verify=false so a FAIL still produces artifacts
#
# The loader runs on the SAME box and is NOT in the slice; runpt.sh bills it
# separately (comp=loader) so "the cell used N cores" stays honest.
set -uo pipefail
G=/root/queen/benchmark-queen/2026-07-29-vm-campaign
OUT=/root/campaign/S/S1
mkdir -p $OUT
cp /root/campaign/S/tenants.json $OUT/tenants.json 2>/dev/null
SHAPE="cell UNCAPPED (cpu.max=max, mem=max) on 8 vCPU/15GiB; PG18 shared_buffers=3GB; broker=M d78709a3 proxy=e0034cb9; PGSS=0"
COMMON="-mode cloud -tenants-file $OUT/tenants.json -tenants 8 -shared-queue -queue orders -group workers -partitions 8 -push-batch 1 -producers-per-tenant 3 -consumers-per-tenant 4 -pop-batch 100 -pop-wait -payload 256 -fail-on-verify=false -out $OUT"

point() { # runid target rate [extra...]
  local id=$1 tgt=$2 rate=$3; shift 3
  bash $G/reset-cell-db.sh >/dev/null 2>&1
  bash $G/runpt.sh $OUT "$id" -- $COMMON -target "$tgt" -rate "$rate" \
    -duration 45 -drain 45 -run-id "$id" \
    -note "S1 $SHAPE; 8 tenants x 8 partitions shared queue; $*" "$@" \
    >/dev/null 2>&1
  echo "  [$(date -u +%T)] $id exit=$? $(grep -h '^exit' $OUT/$id.shape.txt 2>/dev/null)"
}

echo "=== S1a: ladder THROUGH THE PROXY, uncapped cell  $(date -u +%T) ==="
for r in 1000 2000 3000 4000 5000 6500 8000; do
  point "s1-px-$r" proxy $r
done

echo "=== S1b: direct-to-broker control at the same rates (proxy out of path) ==="
for r in 4000 6500 8000; do
  point "s1-br-$r" broker $r
done

echo "=== S1c: is the wall per-REQUEST or per-MESSAGE? same msg/s, batch 10 ==="
for r in 8000 16000; do
  point "s1-px-pb10-$r" proxy $r -push-batch 10
done

echo "=== S1 done $(date -u +%T) ==="
