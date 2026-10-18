#!/usr/bin/env bash
# run.sh 1x1000-3000k-gcp24-xfs-t24-share: p1 (generated 2026-10-14T18:02:54Z); pid in p1.pid, exit code in p1.rc
cd /root/bench/runs/kafka/1x1000-3000k-gcp24-xfs-t24-share || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/kload-share -brokers 10.250.0.12:9092,10.250.0.11:9092,10.250.0.13:9092 -topics 1 -topic k1x1000-3000k-gcp24-xfs-t24-share -partitions 1000 -entities 0 -create=false -rate 333333 -ramp 10s -duration 70s -report 10s -start-file /root/bench/runs/kafka/1x1000-3000k-gcp24-xfs-t24-share/p1.start -max-inflight 5000 -mode batch -payload 256 -dist rr -consumers 33 -cons-offset 33 -cons-total 297 -poll 1000 -ack async -loader-index 1 -loaders 9 -local-src 0-2 -out /root/bench/runs/kafka/1x1000-3000k-gcp24-xfs-t24-share/p1.json -tag 1x1000-3000k-gcp24-xfs-t24-share-p1 -group-protocol share  > p1.log 2>&1 < /dev/null &
echo $! > p1.pid.tmp && mv p1.pid.tmp p1.pid
wait $!
echo $? > p1.rc.tmp && mv p1.rc.tmp p1.rc
