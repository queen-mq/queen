#!/usr/bin/env bash
# sparse.sh <tag> <bin> <partitions> <msg/s> <seconds> [extra env...]
# Single pushes spread over many partitions, retention off, rows that live 60 s:
# every push leaves a row that a watermark of its own partition takes away a
# minute later, so the row expiry works at the push rate. What that costs the
# pushes (latency, CPU), what memory settles at, then the backlog read back.
# Broker and load generators on cores 6-15. Results in /root/improve/runs/<tag>/
set -u
TAG=$1; BIN=$2; NP=$3; RATE=$4; SEC=$5; shift 5
R=/root/improve/runs/$TAG; rm -rf $R; mkdir -p $R
B=http://127.0.0.1:6632
Q="taskset -c 6-15 /root/improve/bin/qload"
export BIN DATA_ROOT=/root/improve/data EXTRA="QUEEN_RAFT_TXN_WINDOW_MIN_S=60 $*"
taskset -c 6-15 /root/kvl/broker.sh single wipe >/dev/null 2>&1
taskset -c 6-15 /root/kvl/broker.sh single start || exit 1
sleep 2
PID=$(cat /root/kvl/run/n1.pid)
snap() {
  S=$(curl -s -m 5 $B/api/v1/raft/status); M=$(curl -s -m 5 $B/metrics/prometheus)
  A=$(echo "$S" | python3 -c "import sys,json; h=json.load(sys.stdin)[\"host\"]; print(round(h[\"anonBytes\"]/1e6), round(h[\"rssBytes\"]/1e6), round(h[\"cpuPct\"]))" 2>/dev/null)
  T=$(echo "$M" | grep "^queen_raft_store_ram_rows{ks=\"txns\",kind=\"live\"}" | awk "{print \$2}")
  PT=$(echo "$M" | grep "^queen_raft_store_ram_rows{ks=\"partitions\",kind=\"live\"}" | awk "{print \$2}")
  CC=$(echo "$M" | grep "^queen_consume_cold_claims_total" | awk "{print \$2}")
  SC=$(echo "$M" | grep "^queen_raft_retention_scan" | awk "{print \$2}" | tr "\n" "/")
  FL=$(echo "$M" | grep "^queen_raft_log_storage" | awk "{print \$2}" | tr "\n" "/")
  echo "$(date -u +%H:%M:%S) $1 rows=$T parts=$PT cold=${CC:-na} scan=$SC files/bytes=$FL anon/rss/cpu=$A" | tee -a $R/snap.log
}
curl -s -X POST $B/api/v1/configure -H "content-type: application/json" -d '{"queue":"sp","options":{"dedupWindowSeconds":60,"leaseTime":30}}' >/dev/null
curl -s $B/metrics/prometheus | grep "^# HELP queen_raft_retention_scan" | cut -c1-200 > $R/scan-help.txt
snap start
( while sleep 10; do snap P; done ) & SN=$!
$Q -urls $B -topic sp -topics 1 -partitions $NP -mode batch -batch 1 -rate $RATE -ramp 10 -duration $SEC -consumers 0 -report 10 -create=true -configure=false -payload 256 -out $R/p.json > $R/p.log 2>&1
kill $SN 2>/dev/null; snap P-end
grep -E "^\[final\]" $R/p.log | cut -c1-330 >> $R/snap.log
if [ "${SHORT:-0}" = 1 ]; then grep -c "ERROR\|WARN" /root/kvl/logs/single-n1.log >> $R/snap.log; taskset -c 6-15 /root/kvl/broker.sh single stop; echo SPARSE_DONE >> $R/snap.log; exit 0; fi
( while sleep 10; do snap W; done ) & SN=$!
sleep 100; kill $SN 2>/dev/null; snap P+100s
# The backlog read back: one or a few messages in each of many partitions.
( while sleep 10; do snap C; done ) & SN=$!
$Q -urls $B -topic sp -topics 1 -partitions $NP -rate 0 -consumers 16 -pop-batch 1000 -pop-width 100 -duration 1 -drain 60 -report 10 -create=false -configure=false -out $R/c.json > $R/c.log 2>&1
kill $SN 2>/dev/null; snap C-end
grep -E "^\[final\]" $R/c.log | cut -c1-330 >> $R/snap.log
grep -c "ERROR\|WARN" /root/kvl/logs/single-n1.log >> $R/snap.log
grep "ERROR\|WARN" /root/kvl/logs/single-n1.log | cut -c32-220 | sed -E "s/[0-9]{3,}/N/g" | sort | uniq -c | sort -rn | head -6 >> $R/snap.log
taskset -c 6-15 /root/kvl/broker.sh single stop
echo SPARSE_DONE >> $R/snap.log
