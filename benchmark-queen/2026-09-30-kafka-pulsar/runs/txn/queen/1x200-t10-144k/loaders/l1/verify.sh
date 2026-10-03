#!/usr/bin/env bash
# txn/run.sh queen/1x200-t10-144k: verify (generated 2026-10-01T19:13:41Z); pid in verify.pid, exit code in verify.rc
cd /root/bench/runs/txn/queen/1x200-t10-144k || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n $(ulimit -Hn)
export GOGC=400
/root/bench/bin/txn/qload -urls http://10.114.0.2:6632,http://10.114.0.3:6632,http://10.114.0.4:6632 -pop-width 10 -pop-batch 1000 -lease 10 -topic x1x200-t10-144k -partitions 200 -txn -txn-size 10 -txn-linger 1s -payload 256 -verify -ids-dir /root/bench/runs/txn/queen/1x200-t10-144k/verify-ids -verify-idle 10s -out /root/bench/runs/txn/queen/1x200-t10-144k/verify.json  > verify.log 2>&1 < /dev/null &
echo $! > verify.pid.tmp && mv verify.pid.tmp verify.pid
wait $!
echo $? > verify.rc.tmp && mv verify.rc.tmp verify.rc
