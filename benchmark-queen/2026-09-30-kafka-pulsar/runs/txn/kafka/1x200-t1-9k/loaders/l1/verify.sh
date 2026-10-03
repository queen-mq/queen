#!/usr/bin/env bash
# txn/run.sh kafka/1x200-t1-9k: verify (generated 2026-10-01T16:09:36Z); pid in verify.pid, exit code in verify.rc
cd /root/bench/runs/txn/kafka/1x200-t1-9k || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/txn/kload -brokers 10.114.0.2:9092,10.114.0.3:9092,10.114.0.4:9092 -rf 3 -min-isr 2 -txn-timeout 10s -group-protocol classic -topic x1x200-t1-9k -partitions 200 -txn -txn-size 1 -txn-linger 1s -payload 256 -verify -ids-dir /root/bench/runs/txn/kafka/1x200-t1-9k/verify-ids -verify-idle 10s -out /root/bench/runs/txn/kafka/1x200-t1-9k/verify.json  > verify.log 2>&1 < /dev/null &
echo $! > verify.pid.tmp && mv verify.pid.tmp verify.pid
wait $!
echo $? > verify.rc.tmp && mv verify.rc.tmp verify.rc
