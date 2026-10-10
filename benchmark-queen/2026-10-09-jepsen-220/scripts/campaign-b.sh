#!/usr/bin/env bash
# Set B (m1..m6), Jepsen control on cores 3-5: the suite's first 50 tests, the
# failover tests on six nodes, then the suite's tests 51 to 89.
cd /root/jepsen-220b
export RUNS=/root/runs-220b BIN=/root/bin/queen-220rc1
NODES=m1,m2,m3,m4,m5 taskset -c 3-5 ./run-campaign.sh matrix-220b1.txt
NODES=m1,m2,m3,m4,m5,m6 taskset -c 3-5 ./run-campaign.sh matrix-220b2.txt
NODES=m1,m2,m3,m4,m5 taskset -c 3-5 ./run-campaign.sh matrix-220b3.txt
echo "$(date -u +%FT%TZ) ALL DONE" >> $RUNS/summary.txt
