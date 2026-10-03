#!/usr/bin/env bash
# txn/run.sh pulsar/1x200-t10-36k: verify (generated 2026-10-01T18:27:13Z); pid in verify.pid, exit code in verify.rc
cd /root/bench/runs/txn/pulsar/1x200-t10-36k || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/txn/pload -url pulsar://10.114.0.2:6650,10.114.0.3:6650,10.114.0.4:6650 -admin http://10.114.0.2:8080 -tenant bench -namespace ns -bundles 48 -sub sub -sub-type failover -txn-timeout 10s -topic x1x200-t10-36k -partitions 200 -txn -txn-size 10 -txn-linger 1s -payload 256 -verify -ids-dir /root/bench/runs/txn/pulsar/1x200-t10-36k/verify-ids -verify-idle 10s -out /root/bench/runs/txn/pulsar/1x200-t10-36k/verify.json  > verify.log 2>&1 < /dev/null &
echo $! > verify.pid.tmp && mv verify.pid.tmp verify.pid
wait $!
echo $? > verify.rc.tmp && mv verify.rc.tmp verify.rc
