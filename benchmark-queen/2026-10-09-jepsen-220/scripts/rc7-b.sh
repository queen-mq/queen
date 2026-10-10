#!/usr/bin/env bash
# After set B has finished its campaign: the short pass on rc7, in its own runs directory.
until grep -q "ALL DONE" /root/runs-220b/summary.txt; do sleep 30; done
cd /root/jepsen-220b
export RUNS=/root/runs-220rc7b BIN=/root/bin/queen-220rc7
mkdir -p $RUNS
NODES=m1,m2,m3,m4,m5 taskset -c 3-5 ./run-campaign.sh matrix-rc7b.txt
NODES=m1,m2,m3,m4,m5,m6 taskset -c 3-5 ./run-campaign.sh matrix-rc7b-fo.txt
echo "$(date -u +%FT%TZ) ALL DONE" >> $RUNS/summary.txt
