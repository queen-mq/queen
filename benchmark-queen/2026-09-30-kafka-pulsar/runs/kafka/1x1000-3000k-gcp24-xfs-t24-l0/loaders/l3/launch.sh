#!/usr/bin/env bash
# run.sh 1x1000-3000k-gcp24-xfs-t24-l0: start this host's load processes (generated 2026-10-14T19:11:05Z)
cd /root/bench/runs/kafka/1x1000-3000k-gcp24-xfs-t24-l0 || exit 1
rm -f p6.start p6.rc p6.pid p6.json p6.log; nohup bash p6.sh > /dev/null 2>&1 < /dev/null &
rm -f p7.start p7.rc p7.pid p7.json p7.log; nohup bash p7.sh > /dev/null 2>&1 < /dev/null &
rm -f p8.start p8.rc p8.pid p8.json p8.log; nohup bash p8.sh > /dev/null 2>&1 < /dev/null &
echo launched 6 7 8  on $(hostname)
