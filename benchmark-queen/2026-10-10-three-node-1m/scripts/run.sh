#!/usr/bin/env bash
# run.sh <tag> <queen-210|queen-rc7> <completed|off> [duration s] [msg/s per load process] [partitions]
# One run on the three brokers and the three loaders: fresh cluster, leader on n1, queue configured, partitions
# prefilled, nine load processes started on one instant, samplers on the brokers, everything collected under runs/<tag>.
set -u
TAG=$1; BINNAME=$2; RET=$3; DUR=${4:-300}; RATE=${5:-111111}; PARTS=${6:-500000}
HERE=$(cd "$(dirname "$0")" && pwd); OUT=$HERE/../runs/$TAG; rm -rf "$OUT"; mkdir -p "$OUT"
. "$HERE/hosts.env"   # BP: the brokers' public addresses (n1 first), BV: their VPC addresses, LP: the loaders
SSH="ssh -o ConnectTimeout=20 -o ConnectionAttempts=3 -o BatchMode=yes"
EXTRA="QUEEN_RAFT_TXN_WINDOW_MIN_S=60"
log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$OUT/run.log"; }
role() { $SSH root@${BP[$1]} "curl -s -m 2 http://${BV[$1]}:6632/health" 2>/dev/null | sed -n 's/.*"status":"\([a-z]*\)".*"role":"\([a-z]*\)".*/\1 \2/p'; }
start_node() { $SSH root@${BP[$1]} "BIN=/root/p3/bin/$BINNAME EXTRA='$EXTRA' /root/p3/qc.sh start"; }
all_healthy() { for t in $(seq 1 90); do ok=0; for i in 0 1 2; do [ "$(role $i | cut -d' ' -f1)" = healthy ] && ok=$((ok+1)); done; [ $ok = 3 ] && return 0; sleep 1; done; return 1; }

log "run $TAG: $BINNAME, retention $RET, ${DUR}s, 9 x $RATE msg/s, $PARTS partitions, EXTRA=$EXTRA"
for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh sample-stop; /root/p3/qc.sh stop; /root/p3/qc.sh wipe" & done
for h in "${LP[@]}"; do $SSH root@$h "pkill -x qload; rm -rf /root/p3/runs/$TAG; true" & done; wait
for i in 0 1 2; do start_node $i & done; wait
all_healthy || { log "FAILED: the cluster did not become healthy"; exit 1; }
# the leader on n1, the Gold machine
for try in 1 2 3 4 5 6; do
  r0=$(role 0 | cut -d' ' -f2); [ "$r0" = leader ] && break
  for i in 1 2; do if [ "$(role $i | cut -d' ' -f2)" = leader ]; then log "n$((i+1)) leads: handing over"; $SSH root@${BP[$i]} "/root/p3/qc.sh stop"; sleep 2; start_node $i; fi; done
  all_healthy
done
log "roles: n1 $(role 0) | n2 $(role 1) | n3 $(role 2)"
[ "$(role 0 | cut -d' ' -f2)" = leader ] || log "WARNING: n1 does not lead"
$SSH root@${BP[0]} "curl -s -m 5 http://${BV[0]}:6632/health" > "$OUT/health-start.json"

OPTS='"dedupWindowSeconds":60,"leaseTime":30'
[ "$RET" = completed ] && OPTS="$OPTS,\"retentionEnabled\":true,\"completedRetentionSeconds\":60"
$SSH root@${BP[0]} "curl -s -m 10 -X POST http://${BV[0]}:6632/api/v1/configure -H 'content-type: application/json' -d '{\"queue\":\"bench\",\"options\":{$OPTS}}'" | cut -c1-200 | tee -a "$OUT/run.log"; echo >> "$OUT/run.log"
URLS=http://${BV[0]}:6632,http://${BV[1]}:6632,http://${BV[2]}:6632
T0=$(date +%s)
$SSH root@${LP[0]} "ulimit -n 1048576; /root/p3/bin/qload -urls $URLS -topic bench -topics 1 -partitions $PARTS -create=true -configure=false -create-only -warm -warm-conc 256 -consumers 0 -rate 0 2>&1 | tail -3" | cut -c1-220 | tee -a "$OUT/run.log"
log "prefill of $PARTS partitions took $(( $(date +%s) - T0 )) s"

for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh sample-start $TAG" & done; wait
START=$($SSH root@${LP[0]} 'echo $(( $(date +%s%3N) + 20000 ))')
log "load starts at $START (in 20 s)"
for l in 0 1 2; do $SSH root@${LP[$l]} "URLS=$URLS RATE=$RATE PARTS=$PARTS nohup /root/p3/ld.sh $TAG $START $((l*3)) $DUR > /root/p3/runs/ld-$TAG.log 2>&1 </dev/null &" & done; wait
# the warning lines so far are the cluster's formation (nodes stopped to put the leader on n1): counted apart from the load's
for i in 0 1 2; do $SSH root@${BP[$i]} "grep -c -E ' WARN | ERROR ' /root/p3/logs/broker.log" > "$OUT/warn-before-n$((i+1)).txt" & done; wait
sleep $((DUR + 20 + 15))
for t in $(seq 1 60); do n=0; for l in 0 1 2; do $SSH root@${LP[$l]} "test -f /root/p3/runs/$TAG/done-$((l*3))" 2>/dev/null && n=$((n+1)); done; [ $n = 3 ] && break; sleep 5; done
log "load processes done ($n of 3 loaders)"
for i in 0 1 2; do
  $SSH root@${BP[$i]} "/root/p3/qc.sh sample-stop; curl -s -m 5 http://${BV[$i]}:6632/health; echo; curl -s -m 5 http://${BV[$i]}:6632/api/v1/raft/status | cut -c1-1500; echo; echo WARN_ERROR=\$(grep -c -E ' WARN | ERROR ' /root/p3/logs/broker.log); grep -E ' ERROR ' /root/p3/logs/broker.log | cut -c1-200 | sort | uniq -c | sort -rn | head -5; du -sh /root/p3/data | cut -f1" > "$OUT/end-n$((i+1)).txt" 2>&1 &
done; wait
for i in 0 1 2; do scp -q -o ConnectTimeout=20 root@${BP[$i]}:/root/p3/runs/$TAG/sample-n$((i+1)).log "$OUT/" & done
for l in 0 1 2; do scp -q -o ConnectTimeout=20 "root@${LP[$l]}:/root/p3/runs/$TAG/q*" "$OUT/" & done; wait
for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh stop" & done; wait
log "collected: $(ls "$OUT" | wc -l | tr -d ' ') files; brokers stopped"
echo RUN_DONE >> "$OUT/run.log"
