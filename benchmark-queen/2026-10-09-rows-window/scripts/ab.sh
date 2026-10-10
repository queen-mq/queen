#!/usr/bin/env bash
# ab.sh <tag> <bin> [P seconds] [P rate] : produce-only RAM run, backlog replay, in-sync run.
# Results in /root/improve/runs/<tag>/
set -u
TAG=$1; BIN=$2; PSEC=${3:-300}; PRATE=${4:-20000}
R=/root/improve/runs/$TAG; rm -rf $R; mkdir -p $R
B=http://127.0.0.1:6632
export BIN DATA_ROOT=/root/improve/data EXTRA="QUEEN_RAFT_TXN_WINDOW_MIN_S=60 RETENTION_INTERVAL=1000"
/root/kvl/broker.sh single wipe >/dev/null 2>&1
/root/kvl/broker.sh single start || exit 1
sleep 2
curl -s $B/health > $R/health.json
snap() {
  S=$(curl -s -m 5 $B/api/v1/raft/status); M=$(curl -s -m 5 $B/metrics/prometheus)
  A=$(echo "$S" | python3 -c "import sys,json; h=json.load(sys.stdin)[\"host\"]; print(round(h[\"anonBytes\"]/1e6), round(h[\"rssBytes\"]/1e6), round(h[\"cpuPct\"]))")
  T=$(echo "$M" | grep "^queen_raft_store_ram_rows{ks=\"txns\",kind=\"live\"}" | awk "{print \$2}")
  echo "$(date -u +%H:%M:%S) $1 txns_rows=$T anon_MB rss_MB cpu% = $A" | tee -a $R/snap.log
}
cfg() { curl -s -X POST $B/api/v1/configure -H "content-type: application/json" -d "{\"queue\":\"$1\",\"options\":{\"dedupWindowSeconds\":60,\"leaseTime\":30}}" >/dev/null; }
cfg ram
snap start
( while sleep 10; do snap P; done ) & SN=$!
/root/improve/bin/qload -urls $B -topic ram -topics 1 -partitions 2000 -mode batch -batch 1 -rate $PRATE -ramp 5 -duration $PSEC -consumers 0 -create=true -configure=false -payload 256 -out $R/p.json > $R/p.log 2>&1
kill $SN 2>/dev/null; snap P-end
sleep 75; snap P+75s
( while sleep 5; do snap C; done ) & SN=$!
/root/improve/bin/qload -urls $B -topic ram -topics 1 -partitions 2000 -rate 0 -consumers 16 -pop-batch 1000 -pop-width 10 -duration 1 -drain ${CDRAIN:-120} -report 5 -create=false -configure=false -out $R/c.json > $R/c.log 2>&1
kill $SN 2>/dev/null; snap C-end
cfg sync
/root/improve/bin/qload -urls $B -topic sync -topics 1 -partitions 200 -mode batch -batch 100 -rate ${IRATE:-300000} -ramp 10 -duration ${ISEC:-70} -consumers 16 -pop-batch 1000 -pop-width 10 -drain 10 -create=true -configure=false -out $R/i.json > $R/i.log 2>&1
snap I-end
grep -c "ERROR\|WARN" /root/kvl/logs/single-n1.log >> $R/snap.log
/root/kvl/broker.sh single stop
echo AB_DONE >> $R/snap.log
