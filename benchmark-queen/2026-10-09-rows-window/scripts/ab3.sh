#!/usr/bin/env bash
# ab3.sh <tag> <bin> [P seconds] [P rate]: ab2.sh with the broker and the load generators on cores 6-15 (the Jepsen controls have 0-5)
# P: single pushes with retention off (RAM), C1: backlog replay alone for 8 s,
# M: in-sync load with the rest of the backlog replayed in the middle of it,
# I: in-sync load alone. Results in /root/improve/runs/<tag>/
set -u
TAG=$1; BIN=$2; PSEC=${3:-300}; PRATE=${4:-20000}
R=/root/improve/runs/$TAG; rm -rf $R; mkdir -p $R
B=http://127.0.0.1:6632
Q="taskset -c 6-15 /root/improve/bin/qload"
export BIN DATA_ROOT=/root/improve/data EXTRA="QUEEN_RAFT_TXN_WINDOW_MIN_S=60 RETENTION_INTERVAL=1000"
taskset -c 6-15 /root/kvl/broker.sh single wipe >/dev/null 2>&1
taskset -c 6-15 /root/kvl/broker.sh single start || exit 1
sleep 2
curl -s $B/health > $R/health.json
snap() {
  S=$(curl -s -m 5 $B/api/v1/raft/status); M=$(curl -s -m 5 $B/metrics/prometheus)
  A=$(echo "$S" | python3 -c "import sys,json; h=json.load(sys.stdin)[\"host\"]; print(round(h[\"anonBytes\"]/1e6), round(h[\"rssBytes\"]/1e6), round(h[\"cpuPct\"]))")
  T=$(echo "$M" | grep "^queen_raft_store_ram_rows{ks=\"txns\",kind=\"live\"}" | awk "{print \$2}")
  CC=$(echo "$M" | grep "^queen_consume_cold_claims_total" | awk "{print \$2}")
  echo "$(date -u +%H:%M:%S) $1 txns_rows=$T cold_claims=${CC:-na} anon_MB rss_MB cpu% = $A" | tee -a $R/snap.log
}
cfg() { curl -s -X POST $B/api/v1/configure -H "content-type: application/json" -d "{\"queue\":\"$1\",\"options\":{\"dedupWindowSeconds\":60,\"leaseTime\":30}}" >/dev/null; }
replay() { $Q -urls $B -topic ram -topics 1 -partitions 2000 -rate 0 -consumers 16 -pop-batch 1000 -pop-width 10 -duration 1 -drain $1 -report 2 -create=false -configure=false -out $R/$2.json > $R/$2.log 2>&1; }
insync() { $Q -urls $B -topic $1 -topics 1 -partitions 200 -mode batch -batch 100 -rate ${IRATE:-300000} -ramp 10 -duration $2 -consumers 16 -pop-batch 1000 -pop-width 10 -drain 10 -report 2 -create=true -configure=false -out $R/$3.json > $R/$3.log 2>&1; }
cfg ram
snap start
( while sleep 10; do snap P; done ) & SN=$!
$Q -urls $B -topic ram -topics 1 -partitions 2000 -mode batch -batch 1 -rate $PRATE -ramp 5 -duration $PSEC -consumers 0 -create=true -configure=false -payload 256 -out $R/p.json > $R/p.log 2>&1
kill $SN 2>/dev/null; snap P-end
sleep 75; snap P+75s
# C1: the backlog alone, 8 s.
replay 8 c1; snap C1-end
# M: in-sync traffic for 70 s; 25 s in, the rest of the backlog is replayed.
cfg mix
( while sleep 5; do snap M; done ) & SN=$!
insync mix 70 m & IP=$!
sleep 25; echo "$(date -u +%H:%M:%S) replay starts" >> $R/snap.log
replay 60 c2; echo "$(date -u +%H:%M:%S) replay ends" >> $R/snap.log
wait $IP; kill $SN 2>/dev/null; snap M-end
# I: in-sync alone.
cfg sync
insync sync 70 i
snap I-end
grep -c "ERROR\|WARN" /root/kvl/logs/single-n1.log >> $R/snap.log
taskset -c 6-15 /root/kvl/broker.sh single stop
echo AB_DONE >> $R/snap.log
