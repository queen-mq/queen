#!/usr/bin/env bash
# big.sh <tag> <bin> [P seconds] [P msg/s]
# cold.sh with three times the data and a restart on it: more data than RAM
# with retention off, rows gone; the node is stopped and started (how long
# until it serves, with every sealed file's index to open); the page cache is
# dropped and the backlog read back from disk; then retention removes it. Broker and load generators run on cores 6-15 (the
# Jepsen control has 0-5). Results in /root/improve/runs/<tag>/
set -u
TAG=$1; BIN=$2; PSEC=${3:-1000}; PRATE=${4:-150000}
R=/root/improve/runs/$TAG; rm -rf $R; mkdir -p $R
B=http://127.0.0.1:6632
Q="taskset -c 6-15 /root/improve/bin/qload"
export BIN DATA_ROOT=/root/improve/data EXTRA="QUEEN_RAFT_TXN_WINDOW_MIN_S=60 RETENTION_INTERVAL=1000"
taskset -c 6-15 /root/kvl/broker.sh single wipe >/dev/null 2>&1
taskset -c 6-15 /root/kvl/broker.sh single start || exit 1
sleep 2
curl -s $B/health > $R/health.json
DEV=$(df --output=source /root/improve/data | tail -1 | xargs basename)
rd() { awk -v d="$DEV" '$3==d {print $6*512/1e6}' /proc/diskstats; }   # MB read so far
snap() {
  S=$(curl -s -m 5 $B/api/v1/raft/status); M=$(curl -s -m 5 $B/metrics/prometheus)
  A=$(echo "$S" | python3 -c "import sys,json; h=json.load(sys.stdin)[\"host\"]; print(round(h[\"anonBytes\"]/1e6), round(h[\"rssBytes\"]/1e6), round(h[\"cpuPct\"]))" 2>/dev/null)
  T=$(echo "$M" | grep "^queen_raft_store_ram_rows{ks=\"txns\",kind=\"live\"}" | awk "{print \$2}")
  CC=$(echo "$M" | grep "^queen_consume_cold_claims_total" | awk "{print \$2}")
  DX=$(echo "$M" | grep "^queen_raft_qlog_dirx" | awk "{print \$2}" | tr "\n" "/")
  FL=$(echo "$M" | grep "^queen_raft_log_storage" | awk "{print \$2}" | tr "\n" "/")
  CACHE=$(awk "/^Cached:/{print int(\$2/1024)}" /proc/meminfo)
  echo "$(date -u +%H:%M:%S) $1 rows=$T cold=${CC:-na} dirx=$DX files/bytes=$FL anon/rss/cpu=$A pagecache_MB=$CACHE disk_read_MB=$(rd)" | tee -a $R/snap.log
}
cfg() { curl -s -X POST $B/api/v1/configure -H "content-type: application/json" -d "{\"queue\":\"$1\",\"options\":$2}" >/dev/null; }
replay() { $Q -urls $B -topic big -topics 1 -partitions 2000 -rate 0 -consumers 16 -pop-batch 1000 -pop-width 10 -duration 1 -drain $1 -report 2 -create=false -configure=false -out $R/$2.json > $R/$2.log 2>&1; }
insync() { $Q -urls $B -topic $1 -topics 1 -partitions 200 -mode batch -batch 100 -rate ${IRATE:-300000} -ramp 10 -duration $2 -consumers 16 -pop-batch 1000 -pop-width 10 -drain 10 -report 2 -create=true -configure=false -out $R/$3.json > $R/$3.log 2>&1; }
cfg big '{"dedupWindowSeconds":60,"leaseTime":30}'
snap start
( while sleep 15; do snap P; done ) & SN=$!
$Q -urls $B -topic big -topics 1 -partitions 2000 -mode batch -batch 10 -rate $PRATE -ramp 10 -duration $PSEC -consumers 0 -create=true -configure=false -payload 1024 -out $R/p.json > $R/p.log 2>&1
kill $SN 2>/dev/null; snap P-end
sleep 80; snap P+80s
du -sh /root/improve/data/single >> $R/snap.log
# A restart on all of it.
PIDF=/root/kvl/run/n1.pid
echo "maps_before_restart=$(wc -l < /proc/$(cat $PIDF)/maps) index_maps=$(curl -s -m 5 $B/metrics/prometheus | grep "^queen_raft_qlog_index_maps" | awk "{print \$2}" | tr "\n" "/")" >> $R/snap.log
taskset -c 6-15 /root/kvl/broker.sh single stop; sleep 2
sync; echo 3 > /proc/sys/vm/drop_caches; sleep 1
T0=$(date +%s.%N); taskset -c 6-15 /root/kvl/broker.sh single start >/dev/null 2>&1; T1=$(date +%s.%N)
echo "restart_cold_cache_s=$(echo "$T1 - $T0" | bc)" >> $R/snap.log; sleep 2; snap restarted
grep -i "recover\|opened\|qlog" /root/kvl/logs/single-n1.log | head -5 | cut -c1-300 >> $R/snap.log
# The page cache goes: from here every old message comes from the disk.
sync; echo 3 > /proc/sys/vm/drop_caches; sleep 2; snap cache-dropped
# C1: the backlog alone, 30 s.
( while sleep 5; do snap C1; done ) & SN=$!
replay 60 c1; kill $SN 2>/dev/null; snap C1-end
# R: retention on, the rest of the backlog goes.
cfg big '{"dedupWindowSeconds":60,"leaseTime":30,"retentionEnabled":true,"retentionSeconds":1,"completedRetentionSeconds":1}'
for i in $(seq 1 60); do sleep 10; snap R; F=$(curl -s -m 5 $B/metrics/prometheus | grep "^queen_raft_log_storage{kind=\"bytes\"}" | awk "{print \$2}"); [ "${F%.*}" -lt 6000000000 ] 2>/dev/null && break; done
snap R-end; du -sh /root/improve/data/single >> $R/snap.log
grep -c "ERROR\|WARN" /root/kvl/logs/single-n1.log >> $R/snap.log
grep "ERROR\|WARN" /root/kvl/logs/single-n1.log | cut -c32-200 | sort | uniq -c | sort -rn | head -5 >> $R/snap.log
taskset -c 6-15 /root/kvl/broker.sh single stop
echo BIG_DONE >> $R/snap.log
