#!/usr/bin/env bash
# manyq.sh <tag> <bin> <queues> <seconds> <msg/s> [extra env...]
# Many quiet queues with retention off: how many log files, memory maps and file
# descriptors a node ends up with. The seal age is 10 s (60 times the default), so
# ten minutes here are ten hours of queues that each get a message now and then.
set -u
TAG=$1; BIN=$2; NQ=$3; SEC=$4; RATE=$5; shift 5
R=/root/improve/runs/$TAG; rm -rf $R; mkdir -p $R
B=http://127.0.0.1:6632
Q="taskset -c 6-15 /root/improve/bin/qload"
export BIN DATA_ROOT=/root/improve/data EXTRA="QUEEN_QLOG_SEAL_AGE_S=10 QUEEN_RAFT_TXN_WINDOW_MIN_S=60 RETENTION_INTERVAL=1000 $*"
taskset -c 6-15 /root/kvl/broker.sh single wipe >/dev/null 2>&1
taskset -c 6-15 /root/kvl/broker.sh single start || exit 1
sleep 2
PID=$(cat /root/kvl/run/n1.pid)
snap() {
  M=$(curl -s -m 5 $B/metrics/prometheus)
  FL=$(echo "$M" | grep "^queen_raft_log_storage" | awk "{print \$2}" | tr "\n" "/")
  DX=$(echo "$M" | grep "^queen_raft_qlog_dirx" | awk "{print \$2}" | tr "\n" "/")
  A=$(curl -s -m 5 $B/api/v1/raft/status | python3 -c "import sys,json; h=json.load(sys.stdin)[\"host\"]; print(round(h[\"anonBytes\"]/1e6), round(h[\"rssBytes\"]/1e6))" 2>/dev/null)
  echo "$(date -u +%H:%M:%S) $1 files/bytes=$FL dirx=$DX maps=$(wc -l < /proc/$PID/maps 2>/dev/null) fds=$(ls /proc/$PID/fd 2>/dev/null | wc -l) dirs=$(ls /root/improve/data/single/n1/qlog 2>/dev/null | wc -l) anon/rss_MB=$A health=$(curl -s -m 2 -o /dev/null -w %{http_code} $B/health)" | tee -a $R/snap.log
}
snap start
( while sleep 20; do snap P; done ) & SN=$!
$Q -urls $B -topic mq -topics $NQ -partitions 1 -mode batch -batch 1 -rate $RATE -ramp 5 -duration $SEC -consumers 0 -create=true -configure=false -payload 256 -out $R/p.json > $R/p.log 2>&1
kill $SN 2>/dev/null; snap P-end
grep -E "^\[final\]" $R/p.log | cut -c1-250 >> $R/snap.log
sleep 30; snap P+30s
echo "limit vm.max_map_count=$(cat /proc/sys/vm/max_map_count)" >> $R/snap.log
grep -c "ERROR\|WARN" /root/kvl/logs/single-n1.log >> $R/snap.log
grep "ERROR\|WARN" /root/kvl/logs/single-n1.log | cut -c32-220 | sed -E "s/[0-9]{3,}/N/g" | sort | uniq -c | sort -rn | head -6 >> $R/snap.log
# A restart on all those files: how long until it serves again.
taskset -c 6-15 /root/kvl/broker.sh single stop; sleep 3
T0=$(date +%s.%N); taskset -c 6-15 /root/kvl/broker.sh single start >/dev/null 2>&1; T1=$(date +%s.%N)
echo "restart_s=$(echo "$T1 - $T0" | bc)" >> $R/snap.log; PID=$(cat /root/kvl/run/n1.pid); snap restarted
taskset -c 6-15 /root/kvl/broker.sh single stop
echo MANYQ_DONE >> $R/snap.log
