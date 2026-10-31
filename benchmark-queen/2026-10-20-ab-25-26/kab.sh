#!/usr/bin/env bash
# kab.sh: the Kafka facade, 2.5.0 against 2.6, on the same rig as ab.sh. kload (franz-go, mqload) on the three
# loaders, one process each, against the facade of all three brokers (cluster mode: QUEEN_KAFKA_EMBEDDED, a node id,
# QUEEN_TOKEN, each node advertising its VPC address :9092, as ../2026-10-16-omb/scripts/omb.sh queen-reset does;
# 2.6 takes the node map from the raft peers and ignores QUEEN_KAFKA_NODE_ID). One topic of 100 partitions, RF 3,
# 1 KB, lz4, 4 producer clients and 4 consumers (classic group) a process, 70 s steps; a fresh cluster a run.
# Rates 500,000 and 1,000,000 msg/s (the OMB facade rows of 2026-10-16/17), the builds alternating.
H=$(cd "$(dirname "$0")/../2026-10-12-three-node-new-forms/scripts" && pwd); . "$H/hosts.env"
OUT=$(cd "$(dirname "$0")" && pwd); LOG=$OUT/kab.out; mkdir -p $OUT/kruns
SSH="ssh -o ConnectTimeout=20 -o ConnectionAttempts=3 -o BatchMode=yes"
BOOT="${BV[0]}:9092,${BV[1]}:9092,${BV[2]}:9092"
log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a $LOG; }
fresh() {
  for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh stop; pkill -9 -x queen; /root/p3/qc.sh wipe; sysctl -qw vm.dirty_background_ratio=5" & done
  for h in "${LP[@]}"; do $SSH root@$h "pkill -x kload; pkill -x qload; true" & done; wait
  for i in 0 1 2; do $SSH root@${BP[$i]} "BIN=/root/p3/bin/$1/queen EXTRA='QUEEN_KAFKA_EMBEDDED=true QUEEN_KAFKA_NODE_ID=$((i+1)) QUEEN_TOKEN=kafka-facade QUEEN_KAFKA_ADVERTISED_ADDR=${BV[$i]}:9092' /root/p3/qc.sh start" & done; wait
  for t in $(seq 1 90); do ok=0; for i in 0 1 2; do $SSH root@${BP[$i]} "curl -s -m 2 http://${BV[$i]}:6632/health" 2>/dev/null | grep -q '"status":"healthy"' && ok=$((ok+1)); done; [ $ok = 3 ] && return 0; sleep 2; done
  return 1
}
point() {   # point <bin> <rate> <tag>
  local bin=$1 rate=$2 tag=$3 st START
  log "start $tag: bin $bin, $rate msg/s"
  fresh $bin || { log "$tag: cluster not healthy"; return 1; }
  sleep 5
  for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh sample-stop; /root/p3/qc.sh sample-start $tag" & done; wait
  # The topic first, on its own: kload's creator also waits for DescribeLogDirs, which 2.6's facade does not serve
  # (2.5.0's did); the load processes then take the plain readiness check (leaders and ISR), on both builds.
  $SSH root@${LP[0]} "ulimit -n 65536; /root/p3/bin/kload -brokers $BOOT -rf 3 -min-isr 2 -topic ftest -topics 1 -partitions 100 \
    -create=true -create-only -topic-timeout 20s -rate 0 -consumers 0 > /root/p3/runs/$tag-create.log 2>&1; tail -2 /root/p3/runs/$tag-create.log" | cut -c1-200 | tee -a $LOG
  START=$($SSH root@${LP[0]} 'echo $(( $(date +%s%3N) + 15000 ))')
  for l in 0 1 2; do
    $SSH root@${LP[$l]} "mkdir -p /root/p3/runs/$tag; ulimit -n 1048576; nohup /root/p3/bin/kload -brokers $BOOT -rf 3 -min-isr 2 -group-protocol classic \
      -topic ftest -topics 1 -partitions 100 -payload 1024 -rate $((rate / 3)) -ramp 10 -duration ${DUR:-70} -drain 10 -tail ${TAILW:-30s} \
      -producers 4 -consumers 4 -cons-total 12 -cons-offset $((4 * l)) -loaders 3 -loader-index $l -local-src $l -report 10 \
      -create=false -start-at $START -tag $tag -out /root/p3/runs/$tag/k$l.json > /root/p3/runs/$tag/k$l.log 2>&1 </dev/null &" &
  done; wait
  sleep $(( ${DUR:-70} + 30 ))
  for t in $(seq 1 60); do n=0; for l in 0 1 2; do $SSH root@${LP[$l]} "pgrep -x kload >/dev/null" || n=$((n+1)); done; [ $n = 3 ] && break; sleep 5; done
  for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh sample-stop"; done
  mkdir -p $OUT/kruns/$tag
  for l in 0 1 2; do scp -q root@${LP[$l]}:/root/p3/runs/$tag/k$l.* $OUT/kruns/$tag/ & done
  for i in 0 1 2; do scp -q root@${BP[$i]}:/root/p3/runs/$tag/sample-n$((i+1)).log root@${BP[$i]}:/root/p3/runs/$tag/net-n$((i+1)).log $OUT/kruns/$tag/ 2>/dev/null & done; wait
  echo "$tag $(python3 $OUT/ksum.py $OUT/kruns/$tag $rate)" | tee -a $LOG
}
for rate in ${RATES:-500000 1000000}; do
  for bin in ${BINS:-250 260}; do point $bin $rate ${PFX:-kab26}-$bin-r$rate; done
done
for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh stop" & done; wait
log KAB26_DONE
