#!/usr/bin/env bash
# txn/run.sh pulsar/1x10000-t10-9k-v120: verify (generated 2026-10-01T21:05:31Z); pid in verify.pid, exit code in verify.rc
cd /root/bench/runs/txn/pulsar/1x10000-t10-9k-v120 || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/txn/pload -url pulsar://10.114.0.2:6650,10.114.0.3:6650,10.114.0.4:6650 -admin http://10.114.0.2:8080 -tenant bench -namespace ns -bundles 48 -sub sub -sub-type failover -txn-timeout 10s -topic x1x10000-t10-9k-v120 -partitions 10000 -txn -txn-size 10 -txn-linger 1s -payload 256 -verify -ids-dir /root/bench/runs/txn/pulsar/1x10000-t10-9k-v120/verify-ids -verify-idle 120s -out /root/bench/runs/txn/pulsar/1x10000-t10-9k-v120/verify.json  > verify.log 2>&1 < /dev/null &
echo $! > verify.pid.tmp && mv verify.pid.tmp verify.pid
wait $!
echo $? > verify.rc.tmp && mv verify.rc.tmp verify.rc
