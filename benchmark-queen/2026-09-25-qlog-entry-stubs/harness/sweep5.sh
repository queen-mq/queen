#!/usr/bin/env bash
# ABBA layout A/B with preallocation OFF and a 20 s cooldown before every run,
# so the laptop SSD's drift (F_FULLFSYNC after tens of GB) cancels out.
cd "$(dirname "$0")"
while pgrep -f "run-local.sh q1000-r20k-stub-pm64" > /dev/null; do sleep 2; done
run() { sleep 20; EXTRA="QUEEN_RAFT_QLOG_PREALLOC_KB=0" ./run-local.sh "$@"; }
run q1000-r5k-stub-np-1 stub 1000 5000 40
run q1000-r5k-copies-np-1 copies 1000 5000 40
run q1000-r5k-copies-np-2 copies 1000 5000 40
run q1000-r5k-stub-np-2 stub 1000 5000 40
run q100-r5k-stub-np stub 100 5000 40
run q100-r5k-copies-np copies 100 5000 40
for L in copies stub; do
  EXTRA="QUEEN_RAFT_QLOG_PREALLOC_MIN_SYNCS=64" KEEP=1 ./run-local.sh q1000-r5k-$L-restart $L 1000 5000 40
  sleep 5; EXTRA="QUEEN_RAFT_QLOG_PREALLOC_MIN_SYNCS=64" ./restart.sh q1000-r5k-$L-restart $L
done
echo SWEEP5-DONE
