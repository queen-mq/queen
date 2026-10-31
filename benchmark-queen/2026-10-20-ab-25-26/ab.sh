#!/usr/bin/env bash
# ab.sh: no-regression check of the native path, 2.5.0 against 2.6 (next-release), on the GCP rig of 2026-10-20
# (3 x c4-standard-24-lssd brokers, RAID0 NVMe XFS; 3 x c4-highcpu-16 loaders), with qload through mrun.sh of
# ../2026-10-12-three-node-new-forms. Both binaries built the same way on the rig's brokers (cargo 1.99.0, --release
# --locked): QBIN 250 = tag v2.5.0 (8d6fd153d), QBIN 260 = next-release at 68f83bd73. A fresh cluster before every
# run, one 70 s step, the builds alternating (250, 260, 250, 260):
#   A. 3,000,000 msg/s, 256 B, one queue of 1,000 partitions, binary wire forms with client zstd, dedup off,
#      completed retention 1,800 s (the 2.4 -> 2.5 A/B's point A);
#   B. 1,000,000 msg/s, 256 B, 1,000,000 partitions, same wire forms, dedup off;
#   C. dedup on (60 s), 1 KB, 100 partitions, 1,000,000 msg/s, binary push without zstd (the 2.4 -> 2.5 A/B's point B).
W=$(cd "$(dirname "$0")/../2026-10-12-three-node-new-forms" && pwd)
OUT=$(cd "$(dirname "$0")" && pwd)/ab.out
cd $W/scripts
QA='-push-wire bin -push-zstd -ack-counts -pop-frames -tail 30s'
QC='-push-wire bin -ack-counts -pop-frames -tail 30s'
run() { echo "[$(date -u +%H:%M:%S)] start $1" >> $OUT; shift; "$@" >> $OUT 2>&1; echo "[$(date -u +%H:%M:%S)] done" >> $OUT; }
for round in 1 2; do for bin in 250 260; do
  run ab26-a${round}-$bin env TEN=0 PARTS=1000 CRET=1800 DEDUP=0 TXNW= QBIN=$bin QEXTRA="$QA" bash mrun.sh ab26-a${round}-$bin 1 sync 5 70 3000000
done; done
for round in 1 2; do for bin in 250 260; do
  run ab26-b${round}-$bin env TEN=0 PARTS=1000000 CRET=1800 DEDUP=0 TXNW= QBIN=$bin QEXTRA="$QA" bash mrun.sh ab26-b${round}-$bin 1 sync 5 70 1000000
done; done
for round in 1 2; do for bin in 250 260; do
  run ab26-c${round}-$bin env TEN=0 PARTS=100 CRET=1800 DEDUP=60 PAYLOAD=1024 QBIN=$bin QEXTRA="$QC" bash mrun.sh ab26-c${round}-$bin 1 sync 5 70 1000000
done; done
echo AB26_DONE >> $OUT
