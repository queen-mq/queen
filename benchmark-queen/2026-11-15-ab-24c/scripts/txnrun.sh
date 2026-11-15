#!/usr/bin/env bash
# txnrun.sh <tag> <json|bin> <messages per transaction> <step seconds> <total msg/s> [more totals...]
# The transactional pipeline of ../2026-09-30-kafka-pulsar/txn on this harness: one fresh cluster, then one step per
# rate, each on queues of its own (b<k>-in, b<k>-out), ascending; it stops at the first step not carried. In a step,
# nine qload processes on the three loaders: a feeder fills the input, workers commit ONE transaction per batch (every
# input acked with its lease + the outputs pushed), readers consume the output.
#   json: every request in the JSON forms.   bin: the transactions as the binary body (-txn-wire bin).
#   FORMS: the other requests' forms (default with bin and with QBIN != 220: the new push, ack and pop forms).
# Env: QBIN (220 | txn | ..., default txn), EXTRA_ENV (broker settings), PARTS (200), CONS (22 workers a process),
# FORMS. No verifier here (txncheck.sh is the check of correctness).
set -u
TAG=$1; WIRE=$2; TXN_SIZE=$3; DUR=$4; shift 4
HERE=$(cd "$(dirname "$0")" && pwd); . "$HERE/hosts.env"
QBIN=${QBIN:-txn}; PARTS=${PARTS:-200}; CONS=${CONS:-22}
SSH="ssh -o ConnectTimeout=20 -o ConnectionAttempts=3 -o BatchMode=yes"
EXTRA="QUEEN_RAFT_TXN_WINDOW_MIN_S=60 ${EXTRA_ENV:-}"
if [ -z "${FORMS+x}" ]; then FORMS=""; [ "$QBIN" != 220 ] && FORMS="-push-wire bin -push-zstd -ack-counts -pop-frames"; fi
[ "$WIRE" = bin ] && FORMS="$FORMS -txn-wire bin"
RUN=$HERE/../runs/$TAG; rm -rf "$RUN"; mkdir -p "$RUN"; LOG=$RUN/run.log
log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$LOG"; }
role() { $SSH root@${BP[$1]} "curl -s -m 2 http://${BV[$1]}:6632/health" 2>/dev/null | sed -n 's/.*"status":"\([a-z]*\)".*"role":"\([a-z+]*\)".*/\1 \2/p'; }
start_node() { $SSH root@${BP[$1]} "BIN=/root/p3/bin/$QBIN/queen EXTRA='$EXTRA' WRAP='' /root/p3/qc.sh start"; }
all_healthy() { for t in $(seq 1 40); do ok=0; for i in 0 1 2; do [ "$(role $i | cut -d' ' -f1)" = healthy ] && ok=$((ok+1)); done; [ $ok = 3 ] && return 0; sleep 1; done; return 1; }
for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh sample-stop; /root/p3/qc.sh stop; pkill -9 -x queen; /root/p3/qc.sh wipe" & done
for h in "${LP[@]}"; do $SSH root@$h "pkill -x qload; true" & done; wait
for i in 0 1 2; do start_node $i & done; wait
all_healthy || { log "FAILED: the cluster did not become healthy"; exit 1; }
for try in 1 2 3 4 5 6; do
  [ "$(role 0 | cut -d' ' -f2)" = leader ] && break
  for i in 1 2; do if [ "$(role $i | cut -d' ' -f2)" = leader ]; then $SSH root@${BP[$i]} "/root/p3/qc.sh stop"; sleep 2; start_node $i; fi; done
  all_healthy
done
log "run $TAG: binary $QBIN, transactions as $WIRE of $TXN_SIZE messages, $PARTS partitions, $CONS workers a process, steps of ${DUR}s at: $* | EXTRA=$EXTRA | FORMS=$FORMS | roles: n1 $(role 0) | n2 $(role 1) | n3 $(role 2)"
URLS=http://${BV[0]}:6632,http://${BV[1]}:6632,http://${BV[2]}:6632
K=0
for TOTAL in "$@"; do
  K=$((K + 1)); ST=$TAG-$TOTAL; OUT=$RUN/$TOTAL; mkdir -p "$OUT"; R=/root/p3/runs/$ST
  COMMON="-urls $URLS -pop-width 10 -pop-batch 1000 -lease 10 -topic b$K -partitions $PARTS -txn -txn-size $TXN_SIZE -txn-linger 1s -payload 256 $FORMS"
  for h in "${LP[@]}"; do $SSH root@$h "rm -rf $R; mkdir -p $R" & done; wait
  $SSH root@${LP[0]} "ulimit -n 1048576; /root/p3/bin/qload $COMMON -create-only -warm -tag $ST-create 2>&1 | tail -1" | cut -c1-120 >> "$LOG"
  for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh sample-stop; /root/p3/qc.sh sample-start $ST" & done; wait
  START=$($SSH root@${LP[0]} 'echo $(( $(date +%s%3N) + 8000 ))')
  for l in 0 1 2; do
    $SSH root@${LP[$l]} "ulimit -n 1048576; cd $R; (for i in $((l*3)) $((l*3+1)) $((l*3+2)); do nohup /root/p3/bin/qload $COMMON -create=false -rate $((TOTAL / 9)) -ramp 5s -duration ${DUR}s -report 10s -drain 40s -idle-exit 3s -max-inflight 5000 -batch 100 -consumers $CONS -cons-offset \$((i * $CONS)) -cons-total $((9 * CONS)) -loader-index \$i -loaders 9 -local-src $((l*3))-$((l*3+2)) -start-at $START -out p\$i.json -tag $ST-p\$i > p\$i.log 2>&1 </dev/null & done; wait; echo done > done-$l) > /dev/null 2>&1 </dev/null &" &
  done; wait
  sleep $((DUR + 12))
  for t in $(seq 1 30); do n=0; for l in 0 1 2; do $SSH root@${LP[$l]} "test -f $R/done-$l" 2>/dev/null && n=$((n+1)); done; [ $n = 3 ] && break; sleep 3; done
  for i in 0 1 2; do
    $SSH root@${BP[$i]} "/root/p3/qc.sh sample-stop"
    scp -q -o ConnectTimeout=20 root@${BP[$i]}:/root/p3/runs/$ST/sample-n$((i+1)).log root@${BP[$i]}:/root/p3/runs/$ST/net-n$((i+1)).log "$OUT/" &
  done
  for l in 0 1 2; do scp -q -o ConnectTimeout=20 "root@${LP[$l]}:$R/p*.json" "$OUT/" & done; wait
  V=$(python3 "$HERE/txnstep.py" "$OUT" $TOTAL $TXN_SIZE $DUR)
  log "$V"
  case "$V" in *" OK "*) ;; *) log "stopping at $TOTAL"; break;; esac
done
for i in 0 1 2; do
  $SSH root@${BP[$i]} "curl -s -m 5 http://${BV[$i]}:6632/health | cut -c1-300; echo; echo WARN_ERROR=\$(grep -a -c -E ' WARN | ERROR ' /root/p3/logs/broker.log); grep -a -E 'INFO rsm: (lanes|apply path stages)' /root/p3/logs/broker.log | tail -40" > "$RUN/end-n$((i+1)).txt" 2>&1 &
done; wait
for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh stop" & done; wait
echo RUN_DONE >> "$LOG"
