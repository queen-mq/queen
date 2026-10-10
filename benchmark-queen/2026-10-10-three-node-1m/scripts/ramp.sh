#!/usr/bin/env bash
# ramp.sh <tag> <queen-210|queen-rc7> <completed|off> <rate> [rate...] : one fresh cluster, then one 75 s load step per rate
# (total msg/s over the nine processes), ascending. It stops at the first step that sheds, falls behind or errs.
set -u
TAG=$1; BINNAME=$2; RET=$3; shift 3
HERE=$(cd "$(dirname "$0")" && pwd); PARTS=500000; DUR=75
. "$HERE/hosts.env"   # BP: the brokers' public addresses (n1 first), BV: their VPC addresses, LP: the loaders
SSH="ssh -o ConnectTimeout=20 -o ConnectionAttempts=3 -o BatchMode=yes"
EXTRA="QUEEN_RAFT_TXN_WINDOW_MIN_S=60"
mkdir -p "$HERE/../runs/$TAG"; LOG="$HERE/../runs/$TAG/ramp.log"; : > "$LOG"
log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$LOG"; }
role() { $SSH root@${BP[$1]} "curl -s -m 2 http://${BV[$1]}:6632/health" 2>/dev/null | sed -n 's/.*"status":"\([a-z]*\)".*"role":"\([a-z]*\)".*/\1 \2/p'; }
start_node() { $SSH root@${BP[$1]} "BIN=/root/p3/bin/$BINNAME EXTRA='$EXTRA' /root/p3/qc.sh start"; }
all_healthy() { for t in $(seq 1 90); do ok=0; for i in 0 1 2; do [ "$(role $i | cut -d' ' -f1)" = healthy ] && ok=$((ok+1)); done; [ $ok = 3 ] && return 0; sleep 1; done; return 1; }
log "ramp $TAG: $BINNAME, retention $RET, steps of ${DUR}s at: $*"
for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh sample-stop; /root/p3/qc.sh stop; /root/p3/qc.sh wipe" & done
for h in "${LP[@]}"; do $SSH root@$h "pkill -x qload; true" & done; wait
for i in 0 1 2; do start_node $i & done; wait
all_healthy || { log "FAILED: the cluster did not become healthy"; exit 1; }
for try in 1 2 3 4 5 6; do
  [ "$(role 0 | cut -d' ' -f2)" = leader ] && break
  for i in 1 2; do if [ "$(role $i | cut -d' ' -f2)" = leader ]; then $SSH root@${BP[$i]} "/root/p3/qc.sh stop"; sleep 2; start_node $i; fi; done
  all_healthy
done
log "roles: n1 $(role 0) | n2 $(role 1) | n3 $(role 2)"
OPTS='"dedupWindowSeconds":60,"leaseTime":30'
[ "$RET" = completed ] && OPTS="$OPTS,\"retentionEnabled\":true,\"completedRetentionSeconds\":60"
$SSH root@${BP[0]} "curl -s -m 10 -X POST http://${BV[0]}:6632/api/v1/configure -H 'content-type: application/json' -d '{\"queue\":\"bench\",\"options\":{$OPTS}}'" >/dev/null
URLS=http://${BV[0]}:6632,http://${BV[1]}:6632,http://${BV[2]}:6632
$SSH root@${LP[0]} "ulimit -n 1048576; /root/p3/bin/qload -urls $URLS -topic bench -topics 1 -partitions $PARTS -create=true -configure=false -create-only -warm -warm-conc 256 -consumers 0 -rate 0 2>&1 | tail -1" | cut -c1-120 | tee -a "$LOG"
for TOTAL in "$@"; do
  ST=$TAG-$TOTAL; OUT=$HERE/../runs/$TAG/$TOTAL; rm -rf "$OUT"; mkdir -p "$OUT"; RATE=$((TOTAL / 9))
  for h in "${LP[@]}"; do $SSH root@$h "rm -rf /root/p3/runs/$ST" & done
  for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh sample-stop; /root/p3/qc.sh sample-start $ST" & done; wait
  START=$($SSH root@${LP[0]} 'echo $(( $(date +%s%3N) + 12000 ))')
  for l in 0 1 2; do $SSH root@${LP[$l]} "URLS=$URLS RATE=$RATE PARTS=$PARTS nohup /root/p3/ld.sh $ST $START $((l*3)) $DUR > /root/p3/runs/ld-$ST.log 2>&1 </dev/null &" & done; wait
  sleep $((DUR + 12 + 14))
  for t in $(seq 1 40); do n=0; for l in 0 1 2; do $SSH root@${LP[$l]} "test -f /root/p3/runs/$ST/done-$((l*3))" 2>/dev/null && n=$((n+1)); done; [ $n = 3 ] && break; sleep 3; done
  for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh sample-stop" & scp -q -o ConnectTimeout=20 root@${BP[$i]}:/root/p3/runs/$ST/sample-n$((i+1)).log "$OUT/" & done
  for l in 0 1 2; do scp -q -o ConnectTimeout=20 "root@${LP[$l]}:/root/p3/runs/$ST/q*" "$OUT/" & done; wait
  V=$(python3 "$HERE/step.py" "$OUT" $TOTAL)
  log "$V"
  case "$V" in *" OK "*) ;; *) log "stopping at $TOTAL"; break;; esac
done
for i in 0 1 2; do $SSH root@${BP[$i]} "echo n$((i+1)) WARN_ERROR=\$(grep -c -E ' WARN | ERROR ' /root/p3/logs/broker.log); /root/p3/qc.sh stop" & done 2>&1 | tee -a "$LOG"; wait
echo RAMP_DONE >> "$LOG"
