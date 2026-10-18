#!/usr/bin/env bash
# mrun.sh <tag> <groups: 1|3> <sync|nosync> <checkpoint ms> <step seconds> <total msg/s> [more totals...]
# One fresh cluster, then one load step per rate on it, ascending. With several rates it stops at the first step
# that sheds, falls behind or errs. From run.sh and ramp.sh of ../2026-10-10-three-node-1m, with:
#   - three tenants, each with its own queue and a third of the partitions: loader l drives tenant l through all
#     three brokers. TEN=0 keeps the 10-10 layout: no tenancy, one queue, nine processes on it.
#   - groups: QUEEN_RAFT_GROUPS, tenant t in group t.
#   - nosync: the broker under eatmydata (every sync a no-op), the kernel told to start writing out at 64 MiB.
# 2026-10-12: QBIN picks the broker binary (220 | new, default 220), QEXTRA adds qload flags (the new wire forms),
# EXTRA_ENV adds broker settings. POPB: the consumers' pop batch (1000). STEP_PY: the script that judges a step.
# DEDUP: the queue's dedup window in seconds (60; 0 = off). CRET: seconds completed messages are kept (60).
# TXNW: QUEEN_RAFT_TXN_WINDOW_MIN_S (60); set but empty leaves the code default.
set -u
TAG=$1; G=$2; SYNC=$3; CK=$4; DUR=$5; shift 5
HERE=$(cd "$(dirname "$0")" && pwd); . "$HERE/hosts.env"
TEN=${TEN:-3}; PARTS=${PARTS:-500000}; RET=${RET:-completed}; QBIN=${QBIN:-220}; QEXTRA=${QEXTRA:-}; CONS=${CONS:-33}; STEP_PY=${STEP_PY:-step.py}
DEDUP=${DEDUP:-60}; CRET=${CRET:-60}; TXNW=${TXNW-60}
SSH="ssh -o ConnectTimeout=20 -o ConnectionAttempts=3 -o BatchMode=yes"
T=(00000000-0000-4000-8000-000000000000 00000001-0000-4000-8000-000000000000 00000002-0000-4000-8000-000000000000)
EXTRA=""; [ -n "$TXNW" ] && EXTRA="QUEEN_RAFT_TXN_WINDOW_MIN_S=$TXNW"
[ "$CK" != 5 ] && EXTRA="$EXTRA QUEEN_CONSUME_CHECKPOINT_MS=$CK"
if [ "$TEN" != 0 ]; then
  EXTRA="$EXTRA QUEEN_TENANCY_HEADER=1 QUEEN_KV_TRUSTED_PROXY=1 QUEEN_RAFT_GROUPS=$G"
  [ "$G" -gt 1 ] && EXTRA="$EXTRA QUEEN_TENANT_GROUPS=${T[0]}=0,${T[1]}=1,${T[2]}=2"
fi
[ -n "${EXTRA_ENV:-}" ] && EXTRA="$EXTRA $EXTRA_ENV"
# DBB (bytes, 2026-10-14): the brokers' vm.dirty_background_bytes for a sync run too (0 = dirty_background_ratio 5, as before)
WRAP=""; DBB=${DBB:-0}; [ "$SYNC" = nosync ] && { WRAP=eatmydata; DBB=67108864; }
RUN=$HERE/../runs/$TAG; rm -rf "$RUN"; mkdir -p "$RUN"; LOG=$RUN/run.log
log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$LOG"; }
role() { $SSH root@${BP[$1]} "curl -s -m 2 http://${BV[$1]}:6632/health" 2>/dev/null | sed -n 's/.*"status":"\([a-z]*\)".*"role":"\([a-z+]*\)".*/\1 \2/p'; }
start_node() { $SSH root@${BP[$1]} "if [ $DBB = 0 ]; then sysctl -qw vm.dirty_background_ratio=5; else sysctl -qw vm.dirty_background_bytes=$DBB; fi; BIN=/root/p3/bin/$QBIN/queen EXTRA='$EXTRA' WRAP='$WRAP' /root/p3/qc.sh start"; }
all_healthy() { for t in $(seq 1 ${1:-40}); do ok=0; for i in 0 1 2; do [ "$(role $i | cut -d' ' -f1)" = healthy ] && ok=$((ok+1)); done; [ $ok = 3 ] && return 0; sleep 1; done; return 1; }
fresh() {
  for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh sample-stop; /root/p3/qc.sh stop; pkill -9 -x queen; /root/p3/qc.sh wipe" & done
  for h in "${LP[@]}"; do $SSH root@$h "pkill -x qload; true" & done; wait
  for i in 0 1 2; do start_node $i & done; wait
}
log "run $TAG: binary $QBIN, groups $G, $SYNC, checkpoint $CK ms, tenants $TEN, partitions $PARTS, consumers $CONS per process, retention $RET ${CRET}s, dedup ${DEDUP}s, steps of ${DUR}s at: $* | EXTRA=$EXTRA | QEXTRA=$QEXTRA"
up=1
for attempt in 1 2 3 4; do fresh; if all_healthy 40; then up=0; break; fi; log "start $attempt: the cluster did not become healthy, starting it again"; done
[ $up = 0 ] || { log "FAILED: the cluster did not become healthy in four starts"; exit 1; }
if [ "$G" = 1 ]; then   # the leader on n1, as on 10-10
  for try in 1 2 3 4 5 6; do
    [ "$(role 0 | cut -d' ' -f2)" = leader ] && break
    for i in 1 2; do if [ "$(role $i | cut -d' ' -f2)" = leader ]; then $SSH root@${BP[$i]} "/root/p3/qc.sh stop"; sleep 2; start_node $i; fi; done
    all_healthy 60
  done
fi
log "roles: n1 $(role 0) | n2 $(role 1) | n3 $(role 2)"

OPTS="\"dedupWindowSeconds\":$DEDUP,\"leaseTime\":30"
[ "$RET" = completed ] && OPTS="$OPTS,\"retentionEnabled\":true,\"completedRetentionSeconds\":$CRET"
URLS=http://${BV[0]}:6632,http://${BV[1]}:6632,http://${BV[2]}:6632
if [ "$TEN" = 0 ]; then TL=(""); PP=$PARTS; else TL=("${T[@]}"); PP=$((PARTS / 3)); fi
for ten in "${TL[@]}"; do
  H=""; TA=""; [ -n "$ten" ] && { H="-H 'x-queen-tenant: $ten'"; TA="-tenant $ten"; }
  $SSH root@${BP[0]} "curl -s -m 10 -X POST http://${BV[0]}:6632/api/v1/configure -H 'content-type: application/json' $H -d '{\"queue\":\"bench\",\"options\":{$OPTS}}'" | cut -c1-100 >> "$LOG"; echo >> "$LOG"
  $SSH root@${LP[0]} "ulimit -n 1048576; /root/p3/bin/qload -urls $URLS $TA -topic bench -topics 1 -partitions $PP -create=true -configure=false -create-only -warm -warm-conc 256 -consumers 0 -rate 0 2>&1 | tail -1" | cut -c1-120 | tee -a "$LOG"
done

# PREDRAIN=<seconds>: before the first step, consumers alone for that long with wide pops (64 partitions, 6,400
# messages), so that what the prefill wrote is consumed and the consumption side is warm. With ten million partitions
# the loader's own drain of the prefill gives up and the first pops take over a minute.
if [ -n "${PREDRAIN:-}" ]; then
  ST=$TAG-predrain; OUT=$RUN/predrain; mkdir -p "$OUT"
  for h in "${LP[@]}"; do $SSH root@$h "rm -rf /root/p3/runs/$ST" & done; wait
  START=$($SSH root@${LP[0]} 'echo $(( $(date +%s%3N) + 8000 ))')
  for l in 0 1 2; do
    TE=""; [ "$TEN" != 0 ] && TE="TENANT=${T[$l]}"
    $SSH root@${LP[$l]} "URLS=$URLS BATCH=100 POPW=64 POPB=6400 RATE=0 PARTS=$PP CONS=$CONS QEXTRA='$QEXTRA' PAYLOAD=${PAYLOAD:-256} $TE nohup /root/p3/ld.sh $ST $START $((l*3)) $PREDRAIN > /root/p3/runs/ld-$ST.log 2>&1 </dev/null &" &
  done; wait
  sleep $((PREDRAIN + 8 + 12))
  for t in $(seq 1 40); do n=0; for l in 0 1 2; do $SSH root@${LP[$l]} "test -f /root/p3/runs/$ST/done-$((l*3))" 2>/dev/null && n=$((n+1)); done; [ $n = 3 ] && break; sleep 3; done
  for l in 0 1 2; do scp -q -o ConnectTimeout=20 "root@${LP[$l]}:/root/p3/runs/$ST/q*.json" "$OUT/" & done; wait
  log "predrain ${PREDRAIN}s: $(python3 "$HERE/predrain.py" "$OUT")"
fi

for TOTAL in "$@"; do
  ST=$TAG-$TOTAL; OUT=$RUN/$TOTAL; mkdir -p "$OUT"; RATE=$((TOTAL / 9))
  for h in "${LP[@]}"; do $SSH root@$h "rm -rf /root/p3/runs/$ST" & done
  for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh sample-stop; /root/p3/qc.sh sample-start $ST" & done; wait
  START=$($SSH root@${LP[0]} 'echo $(( $(date +%s%3N) + 12000 ))')
  for l in 0 1 2; do
    TE=""; [ "$TEN" != 0 ] && TE="TENANT=${T[$l]}"
    $SSH root@${LP[$l]} "URLS=$URLS BATCH=${BATCH:-100} POPW=${POPW:-10} POPB=${POPB:-1000} RATE=$RATE PARTS=$PP CONS=$CONS QEXTRA='$QEXTRA' PAYLOAD=${PAYLOAD:-256} $TE nohup /root/p3/ld.sh $ST $START $((l*3)) $DUR > /root/p3/runs/ld-$ST.log 2>&1 </dev/null &" &
  done; wait
  # PROBE=1: on the leader, the apply thread before and after the 70th second of this step (lateprobe.sh).
  # PROBE_NODES (2026-10-14, default "0" = the leader): the brokers (0 1 2) that run PROBE_SH; node 0's files land in
  # probe/, the others' in probe-n<node+1>/.
  [ -n "${PROBE:-}" ] && for pn in ${PROBE_NODES:-0}; do $SSH root@${BP[$pn]} "mkdir -p /root/p3/runs/$ST; nohup env ${PROBE_ENV:-} /root/p3/${PROBE_SH:-lateprobe.sh} $START /root/p3/runs/$ST/probe > /root/p3/runs/$ST/probe.log 2>&1 </dev/null &"; done
  sleep $((DUR + 12 + 14))
  for t in $(seq 1 40); do n=0; for l in 0 1 2; do $SSH root@${LP[$l]} "test -f /root/p3/runs/$ST/done-$((l*3))" 2>/dev/null && n=$((n+1)); done; [ $n = 3 ] && break; sleep 3; done
  for i in 0 1 2; do
    $SSH root@${BP[$i]} "/root/p3/qc.sh sample-stop"
    scp -q -o ConnectTimeout=20 root@${BP[$i]}:/root/p3/runs/$ST/sample-n$((i+1)).log root@${BP[$i]}:/root/p3/runs/$ST/net-n$((i+1)).log "$OUT/" &
  done
  for l in 0 1 2; do scp -q -o ConnectTimeout=20 "root@${LP[$l]}:/root/p3/runs/$ST/q*.json" "$OUT/" & done; wait
  if [ -n "${PROBE:-}" ]; then
    for pn in ${PROBE_NODES:-0}; do
      PD=probe; [ "$pn" != 0 ] && PD=probe-n$((pn+1))
      for t in $(seq 1 60); do $SSH root@${BP[$pn]} "test -f /root/p3/runs/$ST/probe/done" 2>/dev/null && break; sleep 3; done
      mkdir -p "$OUT/$PD"; scp -q -o ConnectTimeout=20 "root@${BP[$pn]}:/root/p3/runs/$ST/probe/*" "$OUT/$PD/" 2>/dev/null
    done
  fi
  V=$(python3 "$HERE/$STEP_PY" "$OUT" $TOTAL)
  log "$V"
  if [ $# -gt 1 ]; then case "$V" in *" OK "*) ;; *) log "stopping at $TOTAL"; break;; esac; fi
done
for i in 0 1 2; do
  $SSH root@${BP[$i]} "curl -s -m 5 http://${BV[$i]}:6632/health | cut -c1-400; echo; echo WARN_ERROR=\$(grep -a -c -E ' WARN | ERROR ' /root/p3/logs/broker.log); grep -a -E 'apply path stages|INFO rsm: lanes|INFO rsm: dedup front' /root/p3/logs/broker.log | tail -150" > "$RUN/end-n$((i+1)).txt" 2>&1 &
done; wait
for i in 0 1 2; do $SSH root@${BP[$i]} "gzip -c /root/p3/logs/broker.log" > "$RUN/broker-n$((i+1)).log.gz" 2>/dev/null & done; wait   # the whole log: what else starts mid-run
for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh stop" & done; wait
echo RUN_DONE >> "$LOG"
