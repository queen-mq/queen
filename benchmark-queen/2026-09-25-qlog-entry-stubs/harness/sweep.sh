#!/usr/bin/env bash
# The copies-vs-stub sweep (2026-09-25, laptop). Sequential, each on a fresh broker.
cd "$(dirname "$0")"
for L in copies stub; do
  ./run-local.sh q1p1000-r5k-$L $L 1 5000 40 -partitions 1000
  ./run-local.sh q100-r5k-$L $L 100 5000 40
  ./run-local.sh q1000-r5k-$L $L 1000 5000 40
  ./run-local.sh q1000-r10k-$L $L 1000 10000 40
done
# Preallocation: push-only 1000 queues, and 2000 idle queues with parked consumers.
for M in 0 64; do
  EXTRA="QUEEN_RAFT_QLOG_PREALLOC_MIN_SYNCS=$M" ./run-local.sh q1000-push-r5k-stub-pm$M stub 1000 5000 40 -consume none
  EXTRA="QUEEN_RAFT_QLOG_PREALLOC_MIN_SYNCS=$M" WARM=5 ./run-local.sh q2000-idle-stub-pm$M stub 2000 0 30
done
echo SWEEP-DONE
