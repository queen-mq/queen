#!/usr/bin/env bash
# abc.sh: two more rounds of ab.sh's point C (dedup 60 s, 1 KB, 100 partitions, 1,000,000 msg/s), because 2.6's e2e
# p99 came out 1.5-4.6 ms above 2.5.0's in both of the first two. Round 3 runs 260 first, round 4 250 first.
W=$(cd "$(dirname "$0")/../2026-10-12-three-node-new-forms" && pwd)
OUT=$(cd "$(dirname "$0")" && pwd)/ab.out
cd $W/scripts
QC='-push-wire bin -ack-counts -pop-frames -tail 30s'
run() { echo "[$(date -u +%H:%M:%S)] start $1" >> $OUT; shift; "$@" >> $OUT 2>&1; echo "[$(date -u +%H:%M:%S)] done" >> $OUT; }
for pair in "3 260" "3 250" "4 250" "4 260"; do
  set -- $pair; round=$1; bin=$2
  run ab26-c${round}-$bin env TEN=0 PARTS=100 CRET=1800 DEDUP=60 PAYLOAD=1024 QBIN=$bin QEXTRA="$QC" bash mrun.sh ab26-c${round}-$bin 1 sync 5 70 1000000
done
echo ABC26_DONE >> $OUT
