#!/usr/bin/env bash
# After set A has finished its campaign: the short pass on rc7, in its own runs directory.
until grep -q "ALL DONE" /root/runs-220a/summary.txt; do sleep 30; done
cd /root/jepsen-220a
export RUNS=/root/runs-220rc7a BIN=/root/bin/queen-220rc7 NODES=n1,n2,n3,n4,n5
mkdir -p $RUNS
taskset -c 0-2 ./run-campaign.sh matrix-rc7a.txt
echo "$(date -u +%FT%TZ) ALL DONE" >> $RUNS/summary.txt
