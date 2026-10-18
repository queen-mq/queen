#!/usr/bin/env bash
# 2.5.0 (next-release) on n1..n5: the main list, matrix-220.txt (213 tests, the list 2.2.0, 2.3.0 and 2.4.0 passed),
# the control on this machine (16 GB, heap -Xmx12g).
cd /root/jepsen-25
export RUNS=/root/runs-25 BIN=/root/bin/queen-25 NODES=n1,n2,n3,n4,n5
mkdir -p $RUNS; md5sum $BIN > $RUNS/binary.md5
./run-campaign.sh "${1:-matrix-220.txt}"
echo "$(date -u +%FT%TZ) ALL DONE ${1:-matrix-220.txt}" >> $RUNS/summary.txt
