#!/usr/bin/env bash
# phaseM.sh — TASK M: measure the MINIMUM POP WAIT on the free-tier replica.
#
# Cell shape for every point below (verified, not assumed, in each run's
# .shape.txt): 2 cores / 8 GiB covering PG + pxdb + broker + PROXY together via
# queencell.slice, PGSS=1, load offered THROUGH THE PROXY.
#
# Workload shape is the B3 shape verbatim, so every number here is directly
# comparable with /root/campaign/B3: 4 real tenants on one shared queue + group,
# 4 partitions, pushBatch=1, popBatch=50, long-poll pops, manual ack, 256B
# payload, DB reset + broker restart before every point.
#
# The phase changes the broker binary, so each block states which build it runs:
#   base = pre-feature build (what B1/B2/B3 measured)
#   M    = adds queen.queues.min_pop_wait_time
set -uo pipefail
G=/root/queen/benchmark-queen/2026-07-29-vm-campaign
OUT=/root/campaign/M
SHAPE="cell 2c/8G slice-capped (PG+pxdb+broker+proxy), PGSS=1, through proxy"
BASE="-mode cloud -tenants-file $OUT/tenants.json -tenants 4 -shared-queue -queue orders -group workers -partitions 4 -push-batch 1 -producers-per-tenant 2 -consumers-per-tenant 4 -pop-batch 50 -pop-wait -payload 256 -fail-on-verify=false -target proxy -out $OUT"

point() { # runid build rate window duration drain extra...
  local id=$1 build=$2 rate=$3 w=$4 dur=$5 dr=$6; shift 6
  bash $G/m-usebin.sh "$build" >/dev/null 2>&1
  bash $G/reset-cell-db.sh >/dev/null 2>&1
  bash $G/runpt.sh $OUT "$id" -- $BASE -rate "$rate" -min-pop-wait "$w" \
    -duration "$dur" -drain "$dr" "$@" -run-id "$id" \
    -note "TASK M $SHAPE; build=$build; minPopWaitTime=${w}ms; rate=$rate; B3 workload shape" \
    >/dev/null 2>&1
  local rc=$?
  # the broker-side proof that the lever engaged at all (0 on every OFF lane)
  # The `base` build has no such counter at all (the feature does not exist in
  # it) — that absence is itself the parity evidence, so it is recorded as 0
  # rather than left blank.
  curl -s localhost:6632/metrics/prometheus 2>/dev/null \
    | awk 'BEGIN{n=0;u=0} /^queen_pop_fill_wait_total /{n=$2} /^queen_pop_fill_wait_microseconds_total /{u=$2}
           END{printf "fill_waits=%d fill_ms_total=%.0f\n", n, u/1000}' >"$OUT/$id.fillwait.txt"
  echo "  [$(date -u +%T)] $id build=$build w=${w}ms rate=$rate exit=$rc $(cat $OUT/$id.fillwait.txt)"
}

echo "########## M0 — PARITY: the option OFF must not move anything  $(date -u +%T)"
point m0-base-800 base 800   0 60 45
point m0-off-800  M    800   0 60 45
point m0-base-300 base 300   0 60 45
point m0-off-300  M    300   0 60 45

echo "########## M1 — window sweep at a fixed near-ceiling load (800 msg/s)  $(date -u +%T)"
for w in 5 10 25 50 100; do point "m1-w$w" M 800 "$w" 60 45; done

echo "########## M2 — does the window move the CEILING?  $(date -u +%T)"
point m2-w0-1200  M 1200   0 60 45
point m2-w50-1200 M 1200  50 60 45
point m2-w50-1600 M 1600  50 60 45
point m2-w0-2000  M 2000   0 60 45
point m2-w50-2000 M 2000  50 60 45

echo "########## M3 — LOW load: what a quiet tenant pays for a window it does not need  $(date -u +%T)"
for w in 0 25 50 100; do point "m3-r60-w$w" M 60 "$w" 60 30; done

echo "########## M4 — the checker can still fail on this build  $(date -u +%T)"
point m4-fault-w50 M 800 50 30 30 -fault lose-msg=3

echo "########## phaseM done $(date -u +%T)"
