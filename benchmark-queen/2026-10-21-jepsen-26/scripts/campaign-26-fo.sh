#!/usr/bin/env bash
# The 31 failover tests of matrix-220b2.txt (two clusters of three: n1..n3 the source, n4..n6 its standby),
# once the main list has ended: they need all six nodes, so they cannot run beside it.
cd /root/jepsen-26
until grep -q "ALL DONE matrix-220.txt" /root/runs-26/summary.txt 2>/dev/null; do sleep 30; done
export RUNS=/root/runs-26-fo BIN=/root/bin/queen-26 NODES=n1,n2,n3,n4,n5,n6
mkdir -p $RUNS; md5sum $BIN > $RUNS/binary.md5
./run-campaign.sh matrix-220b2.txt
echo "$(date -u +%FT%TZ) ALL DONE matrix-220b2.txt" >> $RUNS/summary.txt
