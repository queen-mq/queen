#!/usr/bin/env bash
# ab3.sh <tag> <281|new> <txn|push> <step seconds> <total msg/s> [more totals...] (controller qab-l1): one A/B ladder
# on the 11-15 rig. ab.sh of 2026-11-12 with the load spread over THREE loaders: PROCS qload processes on each, all
# NP = 3 x PROCS sharing the rate, process p talking to broker p % 3. A fresh cluster of the build
# (/root/p3/bin/<build>/queen) with n1 leading, then one step per rate, ascending, each on queues of its own
# (b<k>-in/-out or b<k>), stopping at the first step not carried.
#   txn:  workers pop <topic>-in and commit ONE transaction per batch (acks with leases + the outputs pushed to the
#         same partition of <topic>-out), readers pop the output. KV=1 adds one KV put per entity a transaction.
#   push: producers push batches of BATCH messages to PARTS partitions, consumers pop and ack.
#         GROUPS_ARGS (push only): qload's group flags for the consumers, e.g. "-groups 1" (one plain consumer group)
#         or "-group-where 'data.k < 30' -stamp-k 100" (a filtered group taking 30%). SHARE: the share of the
#         pushed messages the consumers must consume (the sum over the groups: 1 for queue mode, N for N plain
#         groups, 0.3 for one group taking 30%); the step is carried when consumed is at least 99% and at most 110% of SHARE x offered.
# CONFIGURE=1 (push only): the step's queue is configured at create, deduplication off and completed messages kept
# CRET s (1800), so nothing is deleted while a step runs: the partitions matrix of 2026-10-13 (grid3.sh). Default 0:
# the queue as the first push creates it (the 11-12/13 ladders).
# TEN=3 (push only; her ask 11-15, the layout of 2026-10-12 mrun.sh): three tenants (x-queen-tenant), each with its
# own queue b<k> of PARTS/3 partitions, loader l driving tenant l through all three brokers (its PROCS processes are
# one load of their own); RG = QUEEN_RAFT_GROUPS (1), with RG > 1 tenant t in group t and each group led by the
# member it prefers (group g: member g), so no n1-leader step then.
# Env: PARTS (200), TXN_SIZE (10), CONS (workers or consumers a process: 15), PROCS (a loader: 3), KV (0), BATCH (100),
# PAYLOAD (256), SPREAD (1), EXTRA_ENV (broker settings), FORMS (default: the binary forms, transactions included).
# Results: /root/ctl/runs/<tag>/<total>/ (every process's json and log, the brokers' samples), run.log.
# The whole file is parsed before anything runs (main, called on the last line).
set -u
main() {
TAG=$1; QBIN=$2; MODE=$3; DUR=$4; shift 4
HERE=$(cd "$(dirname "$0")" && pwd); . "${HOSTS_ENV:-$HERE/hosts.env}"
PARTS=${PARTS:-200}; TXN_SIZE=${TXN_SIZE:-10}; CONS=${CONS:-15}; PROCS=${PROCS:-3}; KV=${KV:-0}; BATCH=${BATCH:-100}
PAYLOAD=${PAYLOAD:-256}; SPREAD=${SPREAD:-1}; GROUPS_ARGS=${GROUPS_ARGS:-}; SHARE=${SHARE:-1}
CONFIGURE=${CONFIGURE:-0}; CRET=${CRET:-1800}; WARM_IDLE=${WARM_IDLE:-10s}   # qload -warm-drain-idle (tenm.sh: 300s)
CREATE_CFG="-configure=false"; [ "$CONFIGURE" = 1 ] && CREATE_CFG="-configure=true -dedup-window 0 -completed-retention $CRET"
NL=${#LP[@]}; NP=$((NL * PROCS))
EXTRA="QUEEN_RAFT_TXN_WINDOW_MIN_S=60 QUEEN_KV_TXN_WRITE_RATE=10000000 ${EXTRA_ENV:-}"
TEN=${TEN:-0}; RG=${RG:-1}
T=(00000000-0000-4000-8000-000000000000 00000001-0000-4000-8000-000000000000 00000002-0000-4000-8000-000000000000)
if [ "$TEN" = 3 ]; then
  [ "$MODE" = push ] && [ "$NL" = 3 ] || { echo "TEN=3 needs push mode and three loaders"; exit 1; }
  EXTRA="$EXTRA QUEEN_TENANCY_HEADER=1 QUEEN_KV_TRUSTED_PROXY=1 QUEEN_RAFT_GROUPS=$RG"
  [ "$RG" -gt 1 ] && EXTRA="$EXTRA QUEEN_TENANT_GROUPS=${T[0]}=0,${T[1]}=1,${T[2]}=2"
  TL=("${T[@]}"); PP=$((PARTS / 3))
else
  TL=(""); PP=$PARTS
fi
FORMS=${FORMS--push-wire bin -push-zstd -ack-counts -pop-frames}
[ "$MODE" = txn ] && [ "$KV" = 0 ] && FORMS="$FORMS -txn-wire bin"
[ "$KV" = 1 ] && FORMS="$FORMS -txn-kv"
# the group flags travel base64-encoded: a where holds spaces, quotes and '<'
G64=$(printf '%s' "$GROUPS_ARGS" | base64 -w0)
RUN=$HERE/runs/$TAG; rm -rf "$RUN"; mkdir -p "$RUN"; LOG=$RUN/run.log
log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$LOG"; }
role() { $SSH root@${BP[$1]} "curl -s -m 2 http://${BV[$1]}:6632/health" 2>/dev/null | sed -n 's/.*"status":"\([a-z]*\)".*"role":"\([a-z+]*\)".*/\1 \2/p'; }
start_node() { $SSH root@${BP[$1]} "BIN=/root/p3/bin/$QBIN/queen EXTRA='$EXTRA' WRAP='' /root/p3/qc.sh start"; }
all_healthy() { for t in $(seq 1 60); do ok=0; for i in 0 1 2; do [ "$(role $i | cut -d' ' -f1)" = healthy ] && ok=$((ok+1)); done; [ $ok = 3 ] && return 0; sleep 1; done; return 1; }
on_loaders() { for l in $(seq 0 $((NL - 1))); do $SSH root@${LP[$l]} "$1" & done; wait; }
for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh sample-stop; /root/p3/qc.sh stop; pkill -9 -x queen; /root/p3/qc.sh wipe" & done
on_loaders "pkill -x qload; true"; wait
for i in 0 1 2; do start_node $i & done; wait
all_healthy || { log "FAILED: the cluster did not become healthy"; exit 1; }
[ "$RG" = 1 ] && for try in 1 2 3 4 5 6; do   # several groups: each led by the member it prefers
  [ "$(role 0 | cut -d' ' -f2)" = leader ] && break
  for i in 1 2; do if [ "$(role $i | cut -d' ' -f2)" = leader ]; then $SSH root@${BP[$i]} "/root/p3/qc.sh stop"; sleep 2; start_node $i; fi; done
  all_healthy
done
log "run $TAG: build $QBIN, $MODE, $PARTS partitions, steps of ${DUR}s at: $* | TXN_SIZE=$TXN_SIZE KV=$KV CONS=$CONS PROCS=$PROCS x $NL loaders BATCH=$BATCH SPREAD=$SPREAD | GROUPS_ARGS=$GROUPS_ARGS SHARE=$SHARE CONFIGURE=$CONFIGURE TEN=$TEN RG=$RG | EXTRA=$EXTRA | FORMS=$FORMS | roles: n1 $(role 0) | n2 $(role 1) | n3 $(role 2)"
URLS=http://${BV[0]}:6632,http://${BV[1]}:6632,http://${BV[2]}:6632
K=0
for TOTAL in "$@"; do
  K=$((K + 1)); ST=$TAG-$TOTAL; OUT=$RUN/$TOTAL; mkdir -p "$OUT"; R=/root/p3/runs/$ST
  on_loaders "rm -rf $R; mkdir -p $R"
  if [ "$MODE" = txn ]; then
    COMMON="-urls $URLS -pop-width 10 -pop-batch 1000 -lease 10 -topic b$K -partitions $PARTS -txn -txn-size $TXN_SIZE -txn-linger 1s -payload $PAYLOAD $FORMS"
    CR=$($SSH root@${LP[0]} "ulimit -n 1048576; /root/p3/bin/qload $COMMON -create-only -warm -warm-drain-idle $WARM_IDLE -tag $ST-create 2>&1 | grep -E 'warm|create|FATAL|rror' | tail -3")
  else
    COMMON="-urls $URLS -topic b$K -topics 1 -partitions $PP -mode batch -batch $BATCH -push-spread $SPREAD -payload $PAYLOAD -pop-batch 1000 -pop-width 10 -pop-timeout 2s -ack async -ack-inflight 256 $FORMS"
    CR=""
    for ten in "${TL[@]}"; do
      TA=""; [ -n "$ten" ] && TA="-tenant $ten"
      CR="$CR$($SSH root@${LP[0]} "ulimit -n 1048576; /root/p3/bin/qload $COMMON $TA -create=true $CREATE_CFG -create-only -warm -warm-conc 256 -warm-drain-idle $WARM_IDLE -consumers 0 -rate 0 -tag $ST-create 2>&1 | grep -E 'warm|create|FATAL|rror' | tail -3")
"
    done
  fi
  echo "$CR" | cut -c1-200 >> "$LOG"
  # a warm-up that did not drain leaves its messages in the queue and they leak into the step: no step then
  if echo "$CR" | grep -q -E 'FATAL|rror' || [ "$(echo "$CR" | grep -c -E '^ *\[create\] done')" -lt "${#TL[@]}" ]; then
    log "$(printf '%9s' "$(printf "%'d" $TOTAL)"): warm-up FAILED, no step | stopping at $TOTAL"; break
  fi
  for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh sample-stop; /root/p3/qc.sh sample-start $ST" & done; wait
  START=$(( $(date +%s%3N) + 10000 ))
  for l in $(seq 0 $((NL - 1))); do
    P0=$((l * PROCS)); P1=$((P0 + PROCS - 1))
    if [ "$MODE" = txn ]; then
      PER="-create=false -rate $((TOTAL / NP)) -ramp 5s -duration ${DUR}s -report 10s -drain 40s -idle-exit 3s -max-inflight 5000 -batch 100 -consumers $CONS -cons-total $((NP * CONS)) -loaders $NP -local-src $P0-$P1"
      LAUNCH="for p in \$(seq $P0 $P1); do nohup /root/p3/bin/qload $COMMON $PER -cons-offset \$((p * $CONS)) -loader-index \$p -start-at $START -out p\$p.json -tag $ST-p\$p > p\$p.log 2>&1 </dev/null & done"
    else
      if [ "$TEN" = 3 ]; then IX0=$P0; LDS="-tenant ${T[$l]} -loaders $PROCS -cons-total $((PROCS * CONS)) -local-src 0-$((PROCS - 1))"
      else IX0=0; LDS="-loaders $NP -cons-total $((NP * CONS)) -local-src $P0-$P1"; fi
      PER="-create=false -configure=false -rate $((TOTAL / NP)) -ramp 10 -duration $DUR -drain 10 -max-inflight 5000 -report 10 -consumers $CONS $LDS"
      LAUNCH="G=\$(echo $G64 | base64 -d); for p in \$(seq $P0 $P1); do eval \"nohup /root/p3/bin/qload $COMMON $PER \$G -cons-offset \$(((p - $IX0) * $CONS)) -loader-index \$((p - $IX0)) -start-at $START -out q\$p.json -tag $ST-q\$p\" > q\$p.log 2>&1 </dev/null & done"
    fi
    $SSH root@${LP[$l]} "ulimit -n 1048576; cd $R; ($LAUNCH; wait; echo done > done) > /dev/null 2>&1 </dev/null &" &
  done
  wait
  sleep $((DUR + 14))
  for l in $(seq 0 $((NL - 1))); do
    for t in $(seq 1 60); do $SSH root@${LP[$l]} "test -f $R/done" 2>/dev/null && break; sleep 3; done
  done
  for i in 0 1 2; do
    $SSH root@${BP[$i]} "/root/p3/qc.sh sample-stop"
    $SCP root@${BP[$i]}:/root/p3/runs/$ST/sample-n$((i+1)).log root@${BP[$i]}:/root/p3/runs/$ST/net-n$((i+1)).log "$OUT/" &
  done
  for l in $(seq 0 $((NL - 1))); do $SCP "root@${LP[$l]}:$R/*.json" "root@${LP[$l]}:$R/*.log" "$OUT/" & done
  wait
  if [ "$MODE" = txn ]; then V=$(NPROC=$NP python3 "$HERE/txnstep.py" "$OUT" $TOTAL $TXN_SIZE $DUR)
  else V=$(NPROC=$NP SHARE=$SHARE python3 "$HERE/pushstep.py" "$OUT" $TOTAL $DUR); fi
  log "$V"
  case "$V" in *" OK "*) ;; *) log "stopping at $TOTAL"; break;; esac
done
for i in 0 1 2; do
  $SSH root@${BP[$i]} "curl -s -m 5 http://${BV[$i]}:6632/health | cut -c1-300; echo; echo WARN_ERROR=\$(grep -a -c -E ' WARN | ERROR ' /root/p3/logs/broker.log); grep -a -E ' (WARN|ERROR) ' /root/p3/logs/broker.log | cut -c1-300 | tail -20" > "$RUN/end-n$((i+1)).txt" 2>&1 &
done; wait
for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh stop" & done; wait
echo RUN_DONE >> "$LOG"
}
main "$@"; exit $?
