#!/usr/bin/env bash
# pipe.sh <tag> <bin> [msg/s] [seconds]
# The exactly-once pipeline of the Kafka comparison (qload -txn: a feeder writes
# <topic>-in, 16 workers move ten messages a transaction to <topic>-out, the
# transaction acks the inputs and pushes the outputs) on three nodes of <bin> on
# this machine, with rows that leave the store 20 s after a push. Then the
# verifier reads <topic>-out from its first message and the rest of <topic>-in,
# every message without a row by then, and accounts for every id the loaders
# wrote down. The feeder may outrun the workers: after it stops they go on
# until the input is empty (DRAIN seconds at most, 900), because the verifier
# reads the unprocessed rest without acking, one batch a partition, and can
# only account for a small one. Results in /root/improve/runs/<tag>/
set -u
TAG=$1; BIN=$2; RATE=${3:-30000}; SEC=${4:-600}
cd /root/improve
R=/root/improve/runs/$TAG; rm -rf $R; mkdir -p $R/ids
C=/root/improve/c3.sh; Q="taskset -c 6-15 /root/improve/bin/qload"
U=http://127.0.0.1:6632,http://127.0.0.1:6633,http://127.0.0.1:6634
export BIN EXTRA="QUEEN_RAFT_TXN_WINDOW_MIN_S=20 RETENTION_INTERVAL=1000 QUEEN_QLOG_SEAL_AGE_S=600 QUEEN_RAFT_SEGMENT_BYTES=67108864"
h() { curl -s -m 2 http://127.0.0.1:$((6631 + $1))/health; }
taskset -c 6-15 $C stopall >/dev/null 2>&1; $C wipe
for i in 1 2 3; do taskset -c 6-15 $C start $i; done
for i in 1 2 3; do for _ in $(seq 1 150); do h $i | grep -q '"status":"healthy"' && break; sleep 0.2; done; done
$C status | cut -c1-200 | tee $R/status.txt
snap() {
  L=""
  for i in 1 2 3; do
    M=$(curl -s -m 5 http://127.0.0.1:$((6631 + i))/metrics/prometheus)
    L="$L n$i rows=$(echo "$M" | awk '/^queen_raft_store_ram_rows\{ks="txns",kind="live"\}/{print $2}') cold=$(echo "$M" | awk '/^queen_consume_cold_claims_total /{print $2}') files=$(echo "$M" | awk '/^queen_raft_log_storage\{kind="files"\}/{print $2}') anon_MB=$(curl -s -m 5 http://127.0.0.1:$((6631 + i))/api/v1/raft/status | python3 -c "import sys,json; print(round(json.load(sys.stdin)['host']['anonBytes']/1e6))" 2>/dev/null) |"
  done
  echo "$(date -u +%H:%M:%S) $1$L" | tee -a $R/snap.log
}
for q in vx-in vx-out; do curl -s -X POST http://127.0.0.1:6632/api/v1/configure -H 'content-type: application/json' -d "{\"queue\":\"$q\",\"options\":{\"dedupWindowSeconds\":20,\"leaseTime\":10}}" >/dev/null; done
F="-urls $U -pop-width 10 -pop-batch 1000 -lease 10 -topic vx -partitions 200 -txn -txn-size 10 -txn-linger 1s -payload 256"
$Q $F -create-only -warm -configure=false > $R/create.log 2>&1
snap start
( while sleep 30; do snap P; done ) & SN=$!
$Q $F -create=false -configure=false -rate $RATE -ramp 10 -duration $SEC -report 10 -drain ${DRAIN:-900} -idle-exit 5 -max-inflight 5000 -batch 10 -consumers 16 -out $R/p0.json -ids-out $R/ids/p0.ids > $R/p0.log 2>&1
kill $SN 2>/dev/null; snap P-end
grep -E "^\[final\]" $R/p0.log | cut -c1-400 >> $R/snap.log
sleep 30; snap P+30s
$Q $F -verify -ids-dir $R/ids -verify-idle 10 -out $R/verify.json > $R/verify.log 2>&1
snap verified
grep "\[verify\]" $R/verify.log | tail -4 | cut -c1-400 >> $R/snap.log
for i in 1 2 3; do echo "n$i warn/error: $(grep -c -E ' WARN | ERROR ' /root/improve/c3/logs/n$i.log)" >> $R/snap.log; grep -E ' WARN | ERROR ' /root/improve/c3/logs/n$i.log | cut -c32-200 | sed -E 's/[0-9]{3,}/N/g' | sort | uniq -c | sort -rn | head -4 >> $R/snap.log; done
taskset -c 6-15 $C stopall >/dev/null 2>&1
echo PIPE_DONE >> $R/snap.log
