#!/usr/bin/env bash
# manyq2.sh <tag> <bin> <queues> <seconds> <retention seconds, 0 = off> [extra env...]
# Many quiet queues, one message into each every 5 s. The seal age is 30 s (a
# twentieth of the default), so a minute here is twenty minutes of such queues.
# How many log files, memory maps and how much memory the node ends with, how
# long a restart takes, and whether deleting the queues gives the disk back.
set -u
TAG=$1; BIN=$2; NQ=$3; SEC=$4; RET=$5; shift 5
R=/root/improve/runs/$TAG; rm -rf $R; mkdir -p $R
B=http://127.0.0.1:6632
export BIN DATA_ROOT=/root/improve/data EXTRA="QUEEN_QLOG_SEAL_AGE_S=30 QUEEN_RAFT_TXN_WINDOW_MIN_S=10 RETENTION_INTERVAL=1000 $*"
taskset -c 6-15 /root/kvl/broker.sh single wipe >/dev/null 2>&1
taskset -c 6-15 /root/kvl/broker.sh single start || exit 1
sleep 2
PID=$(cat /root/kvl/run/n1.pid)
snap() {
  M=$(curl -s -m 5 $B/metrics/prometheus)
  FL=$(echo "$M" | grep "^queen_raft_log_storage" | awk "{print \$2}" | tr "\n" "/")
  DX=$(echo "$M" | grep "^queen_raft_qlog_dirx" | awk "{print \$2}" | tr "\n" "/")
  IM=$(echo "$M" | grep "^queen_raft_qlog_index_maps" | awk "{print \$2}" | tr "\n" "/")
  T=$(echo "$M" | grep "^queen_raft_store_ram_rows{ks=\"txns\",kind=\"live\"}" | awk "{print \$2}")
  A=$(curl -s -m 5 $B/api/v1/raft/status | python3 -c "import sys,json; h=json.load(sys.stdin)[\"host\"]; print(round(h[\"anonBytes\"]/1e6), round(h[\"rssBytes\"]/1e6))" 2>/dev/null)
  D=/root/improve/data/single/n1/qlog
  echo "$(date -u +%H:%M:%S) $1 files/bytes=$FL on_disk_qlog=$(find $D -name "*.qlog" 2>/dev/null | wc -l) dirs=$(ls $D 2>/dev/null | wc -l) dirx=$DX index_maps=$IM maps=$(wc -l < /proc/$PID/maps 2>/dev/null) fds=$(ls /proc/$PID/fd 2>/dev/null | wc -l) rows=$T anon/rss_MB=$A du_MB=$(du -sm $D 2>/dev/null | cut -f1) health=$(curl -s -m 2 -o /dev/null -w %{http_code} $B/health)" | tee -a $R/snap.log
}
if [ "$RET" != 0 ]; then
  for i in $(seq 0 $((NQ - 1))); do
    curl -s -m 5 -X POST $B/api/v1/configure -H "content-type: application/json" -d "{\"queue\":\"$(printf 'mq-%05d' $i)\",\"options\":{\"dedupWindowSeconds\":10,\"retentionEnabled\":true,\"retentionSeconds\":$RET,\"completedRetentionSeconds\":$RET}}" >/dev/null
  done
fi
snap start
( while sleep 20; do snap P; done ) & SN=$!
taskset -c 6-15 python3 /root/improve/mqgen.py $B $NQ $SEC 5 > $R/p.log 2>&1
kill $SN 2>/dev/null; snap P-end
tail -3 $R/p.log | cut -c1-250 >> $R/snap.log
sleep 45; snap P+45s
echo "limit vm.max_map_count=$(cat /proc/sys/vm/max_map_count)" >> $R/snap.log
# A restart on those files: how long until it serves again.
taskset -c 6-15 /root/kvl/broker.sh single stop; sleep 2
T0=$(date +%s.%N); taskset -c 6-15 /root/kvl/broker.sh single start >/dev/null 2>&1; T1=$(date +%s.%N)
echo "restart_s=$(echo "$T1 - $T0" | bc)" >> $R/snap.log; PID=$(cat /root/kvl/run/n1.pid); sleep 2; snap restarted
# Every queue is deleted: their logs must drain and their directories go.
for i in $(seq 0 $((NQ - 1))); do curl -s -m 5 -o /dev/null -X DELETE "$B/api/v1/resources/queues/$(printf 'mq-%05d' $i)"; done
snap deleted
for i in 1 2 3 4 5 6; do sleep 20; snap D; done
grep -c "ERROR\|WARN" /root/kvl/logs/single-n1.log >> $R/snap.log
grep "ERROR\|WARN" /root/kvl/logs/single-n1.log | cut -c32-220 | sed -E "s/[0-9]{3,}/N/g" | sort | uniq -c | sort -rn | head -6 >> $R/snap.log
taskset -c 6-15 /root/kvl/broker.sh single stop
echo MANYQ2_DONE >> $R/snap.log
