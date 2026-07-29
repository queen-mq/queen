#!/usr/bin/env bash
# B2 — free-tier replica: 2 cores and 8 GiB covering PG + pxdb + broker + PROXY
# together (July measured broker+PG only, with no proxy in the path).
#
# Ladder through the proxy to the aggregate ceiling, plus direct-to-broker
# control points at the SAME cap so "the proxy costs X" and "2 cores costs Y"
# stay separable. Wait events are sampled every second by runpt.sh, so the
# commit-bound question is answered from the same runs as the throughput.
set -uo pipefail
G=/root/queen/benchmark-queen/2026-07-29-vm-campaign
OUT=/root/campaign/B2
SHAPE="cell 2c/8G slice-capped (PG+pxdb+broker+proxy in queencell.slice), PG over docker0 bridge"
COMMON="-mode cloud -tenants-file $OUT/tenants.json -tenants 4 -shared-queue -queue orders -group workers -partitions 4 -push-batch 1 -producers-per-tenant 2 -consumers-per-tenant 4 -pop-batch 50 -pop-wait -payload 256 -fail-on-verify=false -out $OUT"

point() { # runid target rate duration drain
  bash $G/reset-cell-db.sh >/dev/null 2>&1
  bash $G/runpt.sh $OUT "$1" -- $COMMON -target "$2" -rate "$3" -duration "$4" -drain "$5" \
    -run-id "$1" -note "B2 $SHAPE; 4 tenants x 4 partitions; pushBatch=1 popBatch=50 lease+ack; db reset before run" \
    >/dev/null 2>&1
  echo "  [$(date -u +%T)] $1 exit=$?"
}

echo "=== B2a: ladder THROUGH THE PROXY to the aggregate ceiling ==="
for r in 300 500 700 900 1200; do point "px-$r" proxy $r 60 45; done

echo "=== B2b: direct-to-broker control at the same 2-core cap ==="
for r in 500 900; do point "br-$r" broker $r 60 45; done

echo "=== B2 done ==="
