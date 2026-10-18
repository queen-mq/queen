#!/usr/bin/env bash
# ab.sh: no-regression check of the native path, released 2.4.0 against 2.5.0 (next-release), on the GCP rig of
# 2026-10-16 (3 x c4-standard-24-lssd, 3 x c4-highcpu-16 loaders), with qload through mrun.sh of
# ../2026-10-12-three-node-new-forms. Two points, two rounds each, the builds alternating (240, 250, 240, 250):
#   A. the docs' headline: 3,000,000 msg/s, 256-byte payloads, one queue of 1,000 partitions, the binary wire forms
#      with client zstd, dedup off, completed retention 1,800 s (the 2.4.0 A/B of 10-15, ../2026-10-15-bytes-per-msg);
#   B. dedup on (60 s), 1 KB payloads, 100 partitions, 1,000,000 msg/s, binary push without zstd.
# QBIN 240 = /root/p3/bin/240/queen (the released 2.4.0 amd64 binary), 250 = /root/p3/bin/250/queen (2.5.0).
W=$(cd "$(dirname "$0")/../2026-10-12-three-node-new-forms" && pwd)
OUT=$(cd "$(dirname "$0")" && pwd)/ab.out
cd $W/scripts
QA='-push-wire bin -push-zstd -ack-counts -pop-frames -tail 30s'
QB='-push-wire bin -ack-counts -pop-frames -tail 30s'
for round in 1 2; do
  for bin in 240 250; do
    tag=ab25-a${round}-$bin
    echo "[$(date -u +%H:%M:%S)] start $tag" >> $OUT
    TEN=0 PARTS=1000 CRET=1800 DEDUP=0 TXNW= QBIN=$bin QEXTRA="$QA" bash mrun.sh $tag 1 sync 5 70 3000000 >> $OUT 2>&1
    echo "[$(date -u +%H:%M:%S)] done $tag" >> $OUT
  done
done
for round in 1 2; do
  for bin in 240 250; do
    tag=ab25-b${round}-$bin
    echo "[$(date -u +%H:%M:%S)] start $tag" >> $OUT
    TEN=0 PARTS=100 CRET=1800 DEDUP=60 PAYLOAD=1024 QBIN=$bin QEXTRA="$QB" bash mrun.sh $tag 1 sync 5 70 1000000 >> $OUT 2>&1
    echo "[$(date -u +%H:%M:%S)] done $tag" >> $OUT
  done
done
echo AB25_DONE >> $OUT
