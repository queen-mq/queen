#!/usr/bin/env bash
# Third pass: layout A/B with preallocation OFF (APFS is copy-on-write: a zero
# run saves nothing here), so the layout effect is not mixed with prealloc.
cd "$(dirname "$0")"
while ! grep -q SWEEP2-DONE sweep2.log 2>/dev/null; do sleep 5; done
for L in copies stub; do
  EXTRA="QUEEN_RAFT_QLOG_PREALLOC_KB=0" ./run-local.sh q100-r5k-$L-noprealloc $L 100 5000 40
  EXTRA="QUEEN_RAFT_QLOG_PREALLOC_KB=0" ./run-local.sh q1000-r5k-$L-noprealloc $L 1000 5000 40
  EXTRA="QUEEN_RAFT_QLOG_PREALLOC_KB=0" ./run-local.sh q1000-r10k-$L-noprealloc $L 1000 10000 40
done
echo SWEEP3-DONE
