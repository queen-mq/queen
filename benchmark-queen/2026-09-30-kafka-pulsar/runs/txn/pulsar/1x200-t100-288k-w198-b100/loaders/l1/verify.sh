#!/usr/bin/env bash
# txn/run.sh pulsar/1x200-t100-288k-w198-b100: verify (generated 2026-10-01T20:53:54Z); pid in verify.pid, exit code in verify.rc
cd /root/bench/runs/txn/pulsar/1x200-t100-288k-w198-b100 || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/txn/pload -url pulsar://10.114.0.2:6650,10.114.0.3:6650,10.114.0.4:6650 -admin http://10.114.0.2:8080 -tenant bench -namespace ns -bundles 48 -sub sub -sub-type failover -txn-timeout 10s -topic x1x200-t100-288k-w198-b100 -partitions 200 -txn -txn-size 100 -txn-linger 1s -payload 256 -verify -ids-dir /root/bench/runs/txn/pulsar/1x200-t100-288k-w198-b100/verify-ids -verify-idle 10s -out /root/bench/runs/txn/pulsar/1x200-t100-288k-w198-b100/verify.json  > verify.log 2>&1 < /dev/null &
echo $! > verify.pid.tmp && mv verify.pid.tmp verify.pid
wait $!
echo $? > verify.rc.tmp && mv verify.rc.tmp verify.rc
