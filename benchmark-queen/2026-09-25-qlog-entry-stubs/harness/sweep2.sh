#!/usr/bin/env bash
# Second pass: the combined fix (stub + sustained-traffic prealloc) and headroom.
cd "$(dirname "$0")"
while ! grep -q SWEEP-DONE sweep.log; do sleep 5; done
EXTRA="QUEEN_RAFT_QLOG_PREALLOC_MIN_SYNCS=64" ./run-local.sh q1000-r5k-stub-pm64 stub 1000 5000 40
EXTRA="QUEEN_RAFT_QLOG_PREALLOC_MIN_SYNCS=64" ./run-local.sh q1000-r5k-copies-pm64 copies 1000 5000 40
EXTRA="QUEEN_RAFT_QLOG_PREALLOC_MIN_SYNCS=64" ./run-local.sh q1000-r20k-stub-pm64 stub 1000 20000 40
EXTRA="QUEEN_RAFT_QLOG_PREALLOC_MIN_SYNCS=64" ./run-local.sh q1000-r20k-copies-pm64 copies 1000 20000 40
echo SWEEP2-DONE
