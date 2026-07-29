#!/usr/bin/env bash
# B3 — what a delivered message costs in COMMITS, and whether fatter pops move it.
#
#   b3-commits.sh <ceiling-rate> <sub-ceiling-rate>
#
# Runs on the 2-core free-tier cell brought up with PGSS=1, so every run also
# carries a pg_stat_statements diff and the commits can be attributed to
# push / pop / ack / lease instead of inferred.
#
#   b3-ceil-pgss   the B2 ceiling point again, with pg_stat_statements on:
#                  proves the instrumentation did not move the ceiling, and
#                  provides the breakdown at the ceiling
#   b3-pb-N        pop-batch curve at a rate every point can actually serve, so
#                  commits/msg is compared at equal delivered throughput
#   b3-ceilpb-N    does a fatter pop move the CEILING, not just the ratio
#   b3-pushonly    push side alone (no consumers) — an independent cross-check
#                  on the push share of the commits
set -uo pipefail
G=/root/queen/benchmark-queen/2026-07-29-vm-campaign
OUT=/root/campaign/B3
CEIL=${1:?ceiling rate}
SUB=${2:?sub-ceiling rate}
SHAPE="cell 2c/8G slice-capped, PGSS=1 (pg_stat_statements preloaded)"
BASE="-mode cloud -tenants-file $OUT/tenants.json -tenants 4 -shared-queue -queue orders -group workers -partitions 4 -push-batch 1 -producers-per-tenant 2 -consumers-per-tenant 4 -pop-wait -payload 256 -fail-on-verify=false -out $OUT"

point() { # runid rate popbatch duration drain extra...
  local id=$1 rate=$2 pb=$3 dur=$4 dr=$5; shift 5
  bash $G/reset-cell-db.sh >/dev/null 2>&1
  bash $G/runpt.sh $OUT "$id" -- $BASE -target proxy -rate "$rate" -pop-batch "$pb" \
    -duration "$dur" -drain "$dr" "$@" -run-id "$id" \
    -note "B3 $SHAPE; through proxy; 4 tenants x 4 partitions; pushBatch=1 popBatch=$pb; db reset before run" \
    >/dev/null 2>&1
  echo "  [$(date -u +%T)] $id exit=$?"
}

echo "=== B3a: ceiling point with pg_stat_statements on (rate=$CEIL) ==="
point b3-ceil-pgss $CEIL 50 60 45

echo "=== B3b: pop-batch curve at $SUB msg/s (every point serves the full rate) ==="
for pb in 1 10 50 200; do point "b3-pb-$pb" $SUB $pb 60 45; done

echo "=== B3c: does a fatter pop move the ceiling? (rate=$CEIL) ==="
for pb in 10 200; do point "b3-ceilpb-$pb" $CEIL $pb 60 45; done

echo "=== B3d: push-only cross-check (no consumers, verify off) ==="
point b3-pushonly $CEIL 50 60 5 -consumers-per-tenant 0 -verify=false

echo "=== B3 done ==="
