#!/usr/bin/env bash
# run.sh <tag> <kill|stop> [bin]: the Kafka facade's produce across a leader change.
# A fresh 3-node cluster (../scripts/omb.sh queen-reset <bin>, facade in cluster mode), kfail's idempotent
# producers on loader 1 (4 producers, 200,000 msg/s, 1 KB, 90 s, 100 partitions, RF 3), the raft leader
# stopped at 30 s (kill: kill -9; stop: qc.sh stop, the graceful way) and started again at 50 s, then every
# partition read back and checked (kfail verify: lost, duplicates, reorders). Output in ../runs/failover-<tag>/.
set -u
F=$(cd "$(dirname "$0")" && pwd); H=$(cd "$F/.." && pwd); . "$H/hosts.env"
TAG=${1:?tag}; HOW=${2:?kill|stop}; BIN=${3:-nr6}
OUT=$H/runs/failover-$TAG; mkdir -p "$OUT"
log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$OUT/run.log"; }
BROKERS=$(IFS=,; echo "${B_PRIV[*]/%/:9092}")
log "fresh cluster ($BIN)"
bash "$H/scripts/omb.sh" queen-reset "$BIN" < /dev/null | tail -1 | tee -a "$OUT/run.log"
$SCP "$F/kfail" root@${L_PUB[0]}:/root/kfail < /dev/null
leader() {
  for i in 0 1 2; do
    $SSH root@${B_PUB[$i]} "curl -s -m 2 http://${B_PRIV[$i]}:6632/health" < /dev/null | grep -q '"role":"leader"' && { echo $i; return; }
  done
}
log "produce: 4 producers, 200000 msg/s, 90 s; brokers $BROKERS"
$SSH root@${L_PUB[0]} "/root/kfail produce -brokers $BROKERS -topic kfail-$TAG -partitions 100 -rf 3 -producers 4 -rate 200000 -duration 90s -size 1024 -acked /root/acked-$TAG.bin" > "$OUT/produce.log" 2>&1 < /dev/null &
PRODUCER=$!
sleep 30
L=$(leader); log "leader is node $((L+1)) (${B_PRIV[$L]}): $HOW"
if [ "$HOW" = kill ]; then
  $SSH root@${B_PUB[$L]} "pkill -9 -x queen; true" < /dev/null
else
  $SSH root@${B_PUB[$L]} "/root/p3/qc.sh stop" < /dev/null | tail -1 | tee -a "$OUT/run.log"
fi
sleep 3; N=$(leader); log "new leader: node $((${N:-99}+1))"
sleep 17
log "start node $((L+1)) again"
$SSH root@${B_PUB[$L]} "BIN=/root/p3/bin/$BIN/queen EXTRA='QUEEN_KAFKA_EMBEDDED=true QUEEN_KAFKA_NODE_ID=$((L+1)) QUEEN_TOKEN=kafka-facade QUEEN_KAFKA_ADVERTISED_ADDR=${B_PRIV[$L]}:9092' /root/p3/qc.sh start" < /dev/null | tail -1 | tee -a "$OUT/run.log"
wait $PRODUCER
tail -4 "$OUT/produce.log" | tee -a "$OUT/run.log"
log "verify"
$SSH root@${L_PUB[0]} "/root/kfail verify -brokers $BROKERS -topic kfail-$TAG -acked /root/acked-$TAG.bin" > "$OUT/verify.json" 2>&1 < /dev/null
echo "verify rc=$?" >> "$OUT/run.log"
cat "$OUT/verify.json" | tee -a "$OUT/run.log"
