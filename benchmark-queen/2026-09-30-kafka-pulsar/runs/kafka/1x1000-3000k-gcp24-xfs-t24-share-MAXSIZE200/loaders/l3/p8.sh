#!/usr/bin/env bash
# run.sh 1x1000-3000k-gcp24-xfs-t24-share: p8 (generated 2026-10-14T18:03:01Z); pid in p8.pid, exit code in p8.rc
cd /root/bench/runs/kafka/1x1000-3000k-gcp24-xfs-t24-share || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/kload-share -brokers 10.250.0.12:9092,10.250.0.11:9092,10.250.0.13:9092 -topics 1 -topic k1x1000-3000k-gcp24-xfs-t24-share -partitions 1000 -entities 0 -create=false -rate 333333 -ramp 10s -duration 70s -report 10s -start-file /root/bench/runs/kafka/1x1000-3000k-gcp24-xfs-t24-share/p8.start -max-inflight 5000 -mode batch -payload 256 -dist rr -consumers 33 -cons-offset 264 -cons-total 297 -poll 1000 -ack async -loader-index 8 -loaders 9 -local-src 6-8 -out /root/bench/runs/kafka/1x1000-3000k-gcp24-xfs-t24-share/p8.json -tag 1x1000-3000k-gcp24-xfs-t24-share-p8 -group-protocol share  > p8.log 2>&1 < /dev/null &
echo $! > p8.pid.tmp && mv p8.pid.tmp p8.pid
wait $!
echo $? > p8.rc.tmp && mv p8.rc.tmp p8.rc
