#!/usr/bin/env bash
# Restart cost with 1000 queue logs after a 40 s run (prealloc gated at 64 syncs).
cd "$(dirname "$0")"
while ! grep -q SWEEP3-DONE sweep3.log 2>/dev/null; do sleep 5; done
for L in copies stub; do
  EXTRA="QUEEN_RAFT_QLOG_PREALLOC_MIN_SYNCS=64" KEEP=1 ./run-local.sh q1000-r5k-$L-restart $L 1000 5000 40
  EXTRA="QUEEN_RAFT_QLOG_PREALLOC_MIN_SYNCS=64" ./restart.sh q1000-r5k-$L-restart $L
done
echo SWEEP4-DONE
