#!/usr/bin/env bash
# ab.sh <tag> <281|sched> <txn|push> <step seconds> <total msg/s> [more totals...] (Mac): one A/B ladder.
# A fresh cluster of the build (/root/p3/bin/<build>/queen) with n1 leading, then one step per rate, ascending, each on
# queues of its own (b<k>-in/-out or b<k>), stopping at the first step not carried.
#   txn:  the transactional pipeline of txnrun.sh: a feeder fills <topic>-in, workers commit ONE transaction per batch
#         (every input acked with its lease + the outputs pushed to the same partition of <topic>-out), readers pop
#         the output. KV=1 adds one KV put per entity (partition) to every transaction (qload -txn-kv): a workflow step.
#   push: producers push batches of BATCH messages to PARTS partitions, consumers pop and ack (the plain matrix).
# PROCS qload processes on the one loader share the rate (process i talks to broker i % 3).
# Env: PARTS (200), TXN_SIZE (10), CONS (workers or consumers a process: 22), PROCS (4), KV (0), BATCH (100),
# PAYLOAD (256), EXTRA_ENV (broker settings), FORMS (default: the binary forms, transactions included).
# The whole file is parsed before anything runs (main, called on the last line), so editing it while a ladder runs
# cannot corrupt that ladder.
set -u
main() {
TAG=$1; QBIN=$2; MODE=$3; DUR=$4; shift 4
HERE=$(cd "$(dirname "$0")" && pwd); . "${HOSTS_ENV:-$HERE/hosts.env}"
PARTS=${PARTS:-200}; TXN_SIZE=${TXN_SIZE:-10}; CONS=${CONS:-22}; PROCS=${PROCS:-4}; KV=${KV:-0}; BATCH=${BATCH:-100}
PAYLOAD=${PAYLOAD:-256}; SPREAD=${SPREAD:-1}
# The same broker settings for both builds: the transaction window of the 10-12 runs, and KV writes of transactions
# not held back by their rate (QUEEN_KV_TXN_WRITE_RATE, 10,000/s by default since 2.7.0).
EXTRA="QUEEN_RAFT_TXN_WINDOW_MIN_S=60 QUEEN_KV_TXN_WRITE_RATE=10000000 ${EXTRA_ENV:-}"
FORMS=${FORMS--push-wire bin -push-zstd -ack-counts -pop-frames}
[ "$MODE" = txn ] && [ "$KV" = 0 ] && FORMS="$FORMS -txn-wire bin"
[ "$KV" = 1 ] && FORMS="$FORMS -txn-kv"
RUN=$HERE/../runs/$TAG; rm -rf "$RUN"; mkdir -p "$RUN"; LOG=$RUN/run.log
log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$LOG"; }
role() { $SSH root@${BP[$1]} "curl -s -m 2 http://${BV[$1]}:6632/health" 2>/dev/null | sed -n 's/.*"status":"\([a-z]*\)".*"role":"\([a-z+]*\)".*/\1 \2/p'; }
start_node() { $SSH root@${BP[$1]} "BIN=/root/p3/bin/$QBIN/queen EXTRA='$EXTRA' WRAP='' /root/p3/qc.sh start"; }
all_healthy() { for t in $(seq 1 60); do ok=0; for i in 0 1 2; do [ "$(role $i | cut -d' ' -f1)" = healthy ] && ok=$((ok+1)); done; [ $ok = 3 ] && return 0; sleep 1; done; return 1; }
for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh sample-stop; /root/p3/qc.sh stop; pkill -9 -x queen; /root/p3/qc.sh wipe" & done
$SSH root@${LP[0]} "pkill -x qload; true" & wait
for i in 0 1 2; do start_node $i & done; wait
all_healthy || { log "FAILED: the cluster did not become healthy"; exit 1; }
for try in 1 2 3 4 5 6; do
  [ "$(role 0 | cut -d' ' -f2)" = leader ] && break
  for i in 1 2; do if [ "$(role $i | cut -d' ' -f2)" = leader ]; then $SSH root@${BP[$i]} "/root/p3/qc.sh stop"; sleep 2; start_node $i; fi; done
  all_healthy
done
log "run $TAG: build $QBIN, $MODE, $PARTS partitions, steps of ${DUR}s at: $* | TXN_SIZE=$TXN_SIZE KV=$KV CONS=$CONS PROCS=$PROCS BATCH=$BATCH SPREAD=$SPREAD | EXTRA=$EXTRA | FORMS=$FORMS | roles: n1 $(role 0) | n2 $(role 1) | n3 $(role 2)"
URLS=http://${BV[0]}:6632,http://${BV[1]}:6632,http://${BV[2]}:6632
K=0
for TOTAL in "$@"; do
  K=$((K + 1)); ST=$TAG-$TOTAL; OUT=$RUN/$TOTAL; mkdir -p "$OUT"; R=/root/p3/runs/$ST
  $SSH root@${LP[0]} "rm -rf $R; mkdir -p $R"
  if [ "$MODE" = txn ]; then
    COMMON="-urls $URLS -pop-width 10 -pop-batch 1000 -lease 10 -topic b$K -partitions $PARTS -txn -txn-size $TXN_SIZE -txn-linger 1s -payload $PAYLOAD $FORMS"
    $SSH root@${LP[0]} "ulimit -n 1048576; /root/p3/bin/qload $COMMON -create-only -warm -tag $ST-create 2>&1 | tail -1" | cut -c1-120 >> "$LOG"
  else
    COMMON="-urls $URLS -topic b$K -topics 1 -partitions $PARTS -mode batch -batch $BATCH -push-spread $SPREAD -payload $PAYLOAD -pop-batch 1000 -pop-width 10 -pop-timeout 2s -ack async -ack-inflight 256 $FORMS"
    $SSH root@${LP[0]} "ulimit -n 1048576; /root/p3/bin/qload $COMMON -create=true -configure=false -create-only -warm -warm-conc 256 -consumers 0 -rate 0 -tag $ST-create 2>&1 | tail -1" | cut -c1-120 >> "$LOG"
  fi
  for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh sample-stop; /root/p3/qc.sh sample-start $ST" & done; wait
  START=$($SSH root@${LP[0]} 'echo $(( $(date +%s%3N) + 8000 ))')
  if [ "$MODE" = txn ]; then
    PER="-create=false -rate $((TOTAL / PROCS)) -ramp 5s -duration ${DUR}s -report 10s -drain 40s -idle-exit 3s -max-inflight 5000 -batch 100 -consumers $CONS -cons-total $((PROCS * CONS)) -loaders $PROCS -local-src 0-$((PROCS - 1))"
    $SSH root@${LP[0]} "ulimit -n 1048576; cd $R; (for i in \$(seq 0 $((PROCS - 1))); do nohup /root/p3/bin/qload $COMMON $PER -cons-offset \$((i * $CONS)) -loader-index \$i -start-at $START -out p\$i.json -tag $ST-p\$i > p\$i.log 2>&1 </dev/null & done; wait; echo done > done) > /dev/null 2>&1 </dev/null &"
  else
    PER="-create=false -configure=false -rate $((TOTAL / PROCS)) -ramp 10 -duration $DUR -drain 10 -max-inflight 5000 -report 10 -consumers $CONS -cons-total $((PROCS * CONS)) -loaders $PROCS -local-src 0-$((PROCS - 1))"
    $SSH root@${LP[0]} "ulimit -n 1048576; cd $R; (for i in \$(seq 0 $((PROCS - 1))); do nohup /root/p3/bin/qload $COMMON $PER -cons-offset \$((i * $CONS)) -loader-index \$i -start-at $START -out q\$i.json -tag $ST-q\$i > q\$i.log 2>&1 </dev/null & done; wait; echo done > done) > /dev/null 2>&1 </dev/null &"
  fi
  sleep $((DUR + 12))
  for t in $(seq 1 60); do $SSH root@${LP[0]} "test -f $R/done" 2>/dev/null && break; sleep 3; done
  for i in 0 1 2; do
    $SSH root@${BP[$i]} "/root/p3/qc.sh sample-stop"
    $SCP root@${BP[$i]}:/root/p3/runs/$ST/sample-n$((i+1)).log root@${BP[$i]}:/root/p3/runs/$ST/net-n$((i+1)).log "$OUT/" &
  done
  $SCP "root@${LP[0]}:$R/*.json" "root@${LP[0]}:$R/*.log" "$OUT/" & wait
  if [ "$MODE" = txn ]; then V=$(NPROC=$PROCS python3 "$HERE/txnstep.py" "$OUT" $TOTAL $TXN_SIZE $DUR)
  else V=$(NPROC=$PROCS python3 "$HERE/pushstep.py" "$OUT" $TOTAL $DUR); fi
  log "$V"
  case "$V" in *" OK "*) ;; *) log "stopping at $TOTAL"; break;; esac
done
for i in 0 1 2; do
  $SSH root@${BP[$i]} "curl -s -m 5 http://${BV[$i]}:6632/health | cut -c1-300; echo; echo WARN_ERROR=\$(grep -a -c -E ' WARN | ERROR ' /root/p3/logs/broker.log); grep -a -E 'INFO rsm: (planner|lanes)' /root/p3/logs/broker.log | tail -40" > "$RUN/end-n$((i+1)).txt" 2>&1 &
done; wait
for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh stop" & done; wait
echo RUN_DONE >> "$LOG"
}
main "$@"; exit $?
