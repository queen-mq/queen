#!/usr/bin/env bash
# Set A (n1..n5), Jepsen control on cores 0-2.
cd /root/jepsen-220a
export RUNS=/root/runs-220a BIN=/root/bin/queen-220rc1 NODES=n1,n2,n3,n4,n5
taskset -c 0-2 ./run-campaign.sh matrix-220a.txt
echo "$(date -u +%FT%TZ) ALL DONE" >> $RUNS/summary.txt
