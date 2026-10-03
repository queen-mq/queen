#!/usr/bin/env bash
# run.sh <tag> <total_rate> [KEY=VAL...] (Mac) — one Pulsar point of the matrix (SPEC §6), collected into $RUNS/<tag>/:
#  1. cluster.sh reset (wipe, render with the cluster knobs, start; bench/ns = 48 bundles, E3/Qw3/Qa2, no retention)
#  2. topics + subscription `sub` + warm: pload -create-only -warm on loader 1 (the subscription exists before any
#     producer sends; warm = one message per partition, consumed), then cluster.sh health (E3/Qw3/Qa2 proof on the topic)
#  3. cluster.sh balance -fix: topics + bundles per broker, bundles moved until every broker is within ±10%
#  4. samplers (common/sampler.sh) on brokers + loaders
#  5. PROCS_PER_LOADER pload processes per loader host: loader-index i = 0..N-1, -loaders N, -cons-offset i*C,
#     -cons-total N*C, -rate total/N (remainder spread), -local-src = the indices on that host, -start-file;
#     wait for N READY lines (READY_TIMEOUT s); pc.sh mark + stats (before) on the brokers
#  6. start instant = loader 1's clock + 5 s into every start file; thread snapshot (pc.sh threads) at mid-run
#  7. wait for the loaders; stats (after); collect loader logs + JSON, samples, broker/bookie/zk logs + GC logs
#     (node<i>/logs.tgz), rendered configs (node<i>/node/), balance, health, run.env; DONE only if all N printed [final]
# KEY=VAL [default]:
#   workload  TOPICS[1] TOPIC[load] PARTITIONS[200] ENTITIES[0] MODE[batch] BATCH[100|1 keyed] BATCH_MAX DIST ACTIVE
#             ACTIVE_POLICY ZIPF_S TOPIC_DIST TOPIC_ZIPF_S PAYLOAD[256] CONSUMERS[22 if topics*partitions<=200 else 33]
#             SUB_TYPE[failover] DURATION[70s] RAMP[10s] REPORT[10s] DRAIN MAX_INFLIGHT POLL PROC_US ACK ACK_INFLIGHT SEED
#   client    COMPRESSION BATCH_DELAY BATCH_MAX_MSGS BATCH_MAX_BYTES MAX_PENDING KEY_BATCHING RECEIVER_QUEUE
#             ACK_GROUP_TIME CONNS_PER_BROKER IO_THREADS (unset = pload's SPEC §4 defaults), PLOAD_EXTRA (raw flags),
#             PLOAD_ENV (env for pload, e.g. GOGC=200)
#   cluster   PROFILE JOURNALS ZK_SET BOOKIE_SET BROKER_SET MEM_ZK MEM_BOOKIE MEM_BROKER (cluster.sh reset knobs)
#   harness   PROCS_PER_LOADER[3] READY_TIMEOUT[300] TOPIC_TIMEOUT (pload -topic-timeout: admin setup + warm, its
#             default 300s) BALANCE_FIX[1] RESET[1] WARM[1] FORCE[0] RUNS[$H/runs/pulsar]
# Bash 3.2 (the Mac's /bin/bash). Never edit this file while it runs (write a new one, then mv).
set -u
. "$(cd "$(dirname "$0")" && pwd)/lib.sh"
[ $# -ge 2 ] || { usage; exit 2; }
TAG=$1 RATE=$2; shift 2
case $TAG in ""|*[!A-Za-z0-9._-]*) die "tag must match [A-Za-z0-9._-]+";; esac
case $RATE in ""|*[!0-9]*) die "total rate must be an integer msg/s";; esac
WKEYS="TOPICS TOPIC PARTITIONS ENTITIES MODE BATCH BATCH_MAX DIST ACTIVE ACTIVE_POLICY ZIPF_S TOPIC_DIST TOPIC_ZIPF_S PAYLOAD
  CONSUMERS SUB_TYPE DURATION RAMP REPORT DRAIN MAX_INFLIGHT POLL PROC_US ACK ACK_INFLIGHT SEED"
CKEYS="COMPRESSION BATCH_DELAY BATCH_MAX_MSGS BATCH_MAX_BYTES MAX_PENDING KEY_BATCHING RECEIVER_QUEUE ACK_GROUP_TIME
  CONNS_PER_BROKER IO_THREADS PLOAD_EXTRA PLOAD_ENV"
HKEYS="PROCS_PER_LOADER READY_TIMEOUT TOPIC_TIMEOUT BALANCE_FIX RESET WARM FORCE RUNS"
ALLKEYS=$(echo $WKEYS $CKEYS $HKEYS $KNOBS)
GIVEN=""
for a in "$@"; do
  case $a in
    [A-Z]*=*) k=${a%%=*}
      case " $ALLKEYS " in *" $k "*) ;; *) die "unknown key $k (known: $ALLKEYS)";; esac
      eval "$k=\${a#*=}"; GIVEN="$GIVEN $k" ;;
    *) die "argument '$a' is not KEY=VAL" ;;
  esac
done
TOPICS=${TOPICS:-1} TOPIC=${TOPIC:-load} PARTITIONS=${PARTITIONS:-200} ENTITIES=${ENTITIES:-0} MODE=${MODE:-batch}
SUB_TYPE=${SUB_TYPE:-failover} PAYLOAD=${PAYLOAD:-256} DURATION=${DURATION:-70s} RAMP=${RAMP:-10s} REPORT=${REPORT:-10s}
if [ -z "${BATCH:-}" ]; then if [ "$MODE" = keyed ]; then BATCH=1; else BATCH=100; fi; fi
TOTALP=$((TOPICS * (PARTITIONS > 0 ? PARTITIONS : 1)))
if [ -z "${CONSUMERS:-}" ]; then if [ $TOTALP -le 200 ]; then CONSUMERS=22; else CONSUMERS=33; fi; fi
PROCS_PER_LOADER=${PROCS_PER_LOADER:-3} READY_TIMEOUT=${READY_TIMEOUT:-300} BALANCE_FIX=${BALANCE_FIX:-1}
RESET=${RESET:-1} WARM=${WARM:-1} RUNS=${RUNS:-$H/runs/pulsar}
N=$((NL * PROCS_PER_LOADER))
[ "$N" -ge 1 ] || die "no loaders in $HOSTS_ENV"
RD=$RUNS/$TAG LRD=$R/runs/pulsar/$TAG
URL=""; for ip in "${B_PRIV[@]}"; do URL="${URL:+$URL,}$ip:6650"; done; URL="pulsar://$URL"
ADMIN=http://${B_PRIV[0]}:8080
CKS=""; for k in $KNOBS; do eval "v=\${$k-}"; if [ -n "$v" ]; then export "$k"; CKS="$CKS $k=$v"; fi; done   # cluster.sh reads them
CL="$PDIR/cluster.sh"

dur_s() {  # "70" "70s" "15m" "1h" "500ms" -> whole seconds (rounded up)
  case $1 in
    *ms) echo $(( (${1%ms} + 999) / 1000 )) ;; *s) echo "${1%s}" ;; *m) echo $(( ${1%m} * 60 )) ;;
    *h) echo $(( ${1%h} * 3600 )) ;; *) echo "$1" ;;
  esac
}
F=""
opt() { local v; eval "v=\${$1-}"; [ -n "$v" ] && F="$F $2 $v"; }
client_flags() {  # the pulsar client options, only those that are set (pload carries the SPEC §4 defaults)
  opt COMPRESSION -compression; opt BATCH_DELAY -batch-delay; opt BATCH_MAX_MSGS -batch-max-msgs
  opt BATCH_MAX_BYTES -batch-max-bytes; opt MAX_PENDING -max-pending; opt RECEIVER_QUEUE -receiver-queue
  opt ACK_GROUP_TIME -ack-group-time; opt CONNS_PER_BROKER -conns-per-broker; opt IO_THREADS -io-threads
  if [ -n "${KEY_BATCHING:-}" ]; then F="$F -key-batching=$KEY_BATCHING"; fi
}
layout_flags() {  # the topic layout, identical for the create step and every load process
  F="-url $URL -admin $ADMIN -tenant bench -namespace ns -bundles 48 -topics $TOPICS -topic $TOPIC"
  F="$F -partitions $PARTITIONS -entities $ENTITIES -sub sub -sub-type $SUB_TYPE -payload $PAYLOAD -tag $TAG"
  opt TOPIC_TIMEOUT -topic-timeout
  client_flags
}
load_flags() {  # load_flags <index> <rate>
  local i=$1 lo
  lo=$(( (i / PROCS_PER_LOADER) * PROCS_PER_LOADER ))
  layout_flags
  F="$F -create=false -mode $MODE -batch $BATCH -consumers $CONSUMERS -cons-offset $((i * CONSUMERS))"
  F="$F -cons-total $((N * CONSUMERS)) -rate $2 -ramp $RAMP -duration $DURATION -report $REPORT"
  F="$F -loader-index $i -loaders $N -local-src $lo-$((lo + PROCS_PER_LOADER - 1)) -start-file $LRD/start -out $LRD/pload-$i.json"
  opt BATCH_MAX -batch-max; opt DIST -dist; opt ACTIVE -active; opt ACTIVE_POLICY -active-policy; opt ZIPF_S -zipf-s
  opt TOPIC_DIST -topic-dist; opt TOPIC_ZIPF_S -topic-zipf-s; opt DRAIN -drain; opt MAX_INFLIGHT -max-inflight
  opt POLL -poll; opt PROC_US -proc-us; opt ACK -ack; opt ACK_INFLIGHT -ack-inflight; opt SEED -seed
  if [ -n "${PLOAD_EXTRA:-}" ]; then F="$F $PLOAD_EXTRA"; fi
}
on_loaders() {  # on_loaders <cmd>: every loader host, sequentially, output prefixed
  local j; for j in $(seq 0 $((NL - 1))); do rsh "${L_PUB[$j]}" "$1" 2>&1 | sed "s/^/  l$((j + 1))  /"; done
}
# on a loader: al <pid> = the process exists and is not a zombie (kill -0 is true for zombies, and a container whose
# PID 1 does not reap leaves them behind)
AL='al() { local s; s=$(ps -o stat= -p "$1" 2>/dev/null); [ -n "$s" ] && [ "${s#Z}" = "$s" ]; };'
count_loaders() {  # count_loaders ready|alive -> sum over the loader hosts
  local j s=0 n
  for j in $(seq 0 $((NL - 1))); do
    if [ "$1" = ready ]; then
      n=$(rsh "${L_PUB[$j]}" "cd $LRD 2>/dev/null && grep -l '^READY' pload-*.log 2>/dev/null | wc -l")
    else
      n=$(rsh "${L_PUB[$j]}" "$AL cd $LRD 2>/dev/null && for f in pload-*.pid; do al \$(cat \$f) && echo x; done | wc -l")
    fi
    s=$((s + ${n:-0}))
  done
  echo $s
}
dead_loaders() {  # processes that exited without READY (setup failures)
  on_loaders "$AL cd $LRD && for f in pload-*.pid; do i=\${f#pload-}; i=\${i%.pid}; al \$(cat \$f) || grep -q '^READY' pload-\$i.log || { echo \"pload-\$i exited before READY:\"; tail -5 pload-\$i.log; }; done"
}
stop_loaders() { on_loaders "$AL cd $LRD 2>/dev/null && for f in pload-*.pid; do kill \$(cat \$f) 2>/dev/null; done; sleep 5; for f in pload-*.pid; do al \$(cat \$f) && kill -9 \$(cat \$f); done; true"; }

collect() {  # everything into $RD (also after a failure)
  local i j h d
  log "collect -> $RD"
  "$CL" stats > "$RD/stats-after.txt" 2>&1
  for j in $(seq 0 $((NL - 1))); do
    h=${L_PUB[$j]} d=$RD/loader$((j + 1)); mkdir -p "$d"
    rsh "$h" "$R/common/sampler.sh stop $TAG" > /dev/null 2>&1
    rsh "$h" "cat $R/samples/$TAG.txt 2>/dev/null" > "$d/samples.txt"
    rsh "$h" "tar czf - -C $LRD . 2>/dev/null" | tar xzf - -C "$d" 2>/dev/null
  done
  for i in $(seq 0 $((NB - 1))); do
    h=${B_PUB[$i]} d=$RD/node$((i + 1)); mkdir -p "$d"
    rsh "$h" "$R/common/sampler.sh stop $TAG" > /dev/null 2>&1
    rsh "$h" "cat $R/samples/$TAG.txt 2>/dev/null" > "$d/samples.txt"
    rsh "$h" "tar czf - -C $R/logs pulsar 2>/dev/null" > "$d/logs.tgz"
    rsh "$h" "tar czf - -C $R/pulsar node 2>/dev/null" | tar xzf - -C "$d" 2>/dev/null
  done
  for f in "$RD"/loader*/pload-*.log; do [ -f "$f" ] && grep -E '^\[final\]|^load_cpu=' "$f" | sed "s#^#  $(basename "$f" .log): #"; done
}
fail() { log "FAILED: $*"; echo "$(ts) $*" > "$RD/FAILED"; stop_loaders > /dev/null 2>&1; collect; exit 1; }

main() {
  log "run $TAG: pulsar $PULSAR_VERSION, $RATE msg/s total over $N load processes ($NL loaders x $PROCS_PER_LOADER)," \
      "topics=$TOPICS x partitions=$PARTITIONS entities=$ENTITIES mode=$MODE batch=$BATCH sub=$SUB_TYPE consumers=$CONSUMERS/process"
  { echo "SYSTEM=pulsar"; echo "TAG=$TAG"; echo "RATE=$RATE"
    echo "SHAPE=\"t=$TOPICS p=$PARTITIONS e=$ENTITIES $MODE/$BATCH $SUB_TYPE c=$((N * CONSUMERS))\""
    for k in TOPICS TOPIC PARTITIONS ENTITIES MODE BATCH SUB_TYPE CONSUMERS PAYLOAD DURATION RAMP REPORT; do eval "echo $k=\\\"\$$k\\\""; done
    for k in $GIVEN; do case " TOPICS TOPIC PARTITIONS ENTITIES MODE BATCH SUB_TYPE CONSUMERS PAYLOAD DURATION RAMP REPORT " in
      *" $k "*) ;; *) eval "echo $k=\\\"\${$k}\\\"";; esac; done
    echo "LOADERS=$N"; echo "LOADER_HOSTS=$NL"; echo "PROCS_PER_LOADER=$PROCS_PER_LOADER"; echo "CONS_TOTAL=$((N * CONSUMERS))"
    echo "PULSAR_VERSION=$PULSAR_VERSION"; echo "PROFILE=$PROFILE"; echo "JOURNALS=${JOURNALS:-1}"
    echo "DURABILITY=\"E3/Qw3/Qa2, journalSyncData=true: 3 copies, ack after 2 bookie fsyncs\""
    echo "B_PRIV=\"${B_PRIV[*]}\""; echo "L_PRIV=\"${L_PRIV[*]}\""; echo "STARTED=$(ts)"; } > "$RD/run.env"

  for j in $(seq 0 $((NL - 1))); do
    rsh "${L_PUB[$j]}" "test -x $R/bin/pload" || fail "no $R/bin/pload on loader ${L_PUB[$j]} (build mqload, deploy.sh harness)"
  done
  if [ "$RESET" = 1 ]; then
    log "1. cluster reset:${CKS:- hosts.env defaults}"
    "$CL" reset > "$RD/cluster-reset.txt" 2>&1 || { tail -20 "$RD/cluster-reset.txt"; fail "cluster reset"; }
    grep -E 'reset done|brokers healthy|proof' "$RD/cluster-reset.txt" | sed 's/^/  /' | cut -c1-200
  fi

  log "2. topics + subscription + warm: pload -create-only on loader 1"
  layout_flags; [ "$WARM" = 1 ] && F="$F -warm"
  echo "create: $R/bin/pload -create-only $F" > "$RD/pload-cmd.txt"
  rsh "${L_PUB[0]}" "mkdir -p $LRD && cd $LRD && $R/bin/pload -create-only $F" 2>&1 \
    | sed 's/^\[final\]/create-only [final]/' > "$RD/create.txt"
  rc=${PIPESTATUS[0]}
  tail -3 "$RD/create.txt" | sed 's/^/  /' | cut -c1-200
  [ "$rc" = 0 ] || fail "pload -create-only rc=$rc"
  "$CL" health > "$RD/health.txt" 2>&1
  grep -E 'proof|ensembleSize|namespace' "$RD/health.txt" | sed 's/^/  /' | cut -c1-220

  log "3. balance$([ "$BALANCE_FIX" = 1 ] && echo ' -fix')"
  if [ "$BALANCE_FIX" = 1 ]; then "$CL" balance -fix > "$RD/balance.txt" 2>&1; else "$CL" balance > "$RD/balance.txt" 2>&1; fi
  brc=$?; sed 's/^/  /' "$RD/balance.txt" | cut -c1-200
  [ $brc = 0 ] || log "WARN balance rc=$brc (out of ±10% or failed): the run goes on, balance.txt says how far"

  log "4. samplers on ${NB} brokers + ${NL} loaders"
  for h in "${B_PUB[@]}" "${L_PUB[@]}"; do rsh "$h" "$R/common/sampler.sh start $TAG" | sed 's/^/  /'; done

  log "5. $N load processes"
  BASE=$((RATE / N)) REM=$((RATE % N))
  for j in $(seq 0 $((NL - 1))); do
    cmd="mkdir -p $LRD && cd $LRD && rm -f start start.tmp pload-*.pid pload-*.log pload-*.json; ulimit -n 1048576 2>/dev/null;"
    for p in $(seq 0 $((PROCS_PER_LOADER - 1))); do
      i=$((j * PROCS_PER_LOADER + p)); r=$((BASE + (i < REM ? 1 : 0)))
      load_flags $i $r
      echo "p$i@${L_PRIV[$j]}: ${PLOAD_ENV:+env $PLOAD_ENV }$R/bin/pload $F" >> "$RD/pload-cmd.txt"
      cmd="$cmd setsid nohup env ${PLOAD_ENV:-} $R/bin/pload $F > $LRD/pload-$i.log 2>&1 < /dev/null & echo \$! > $LRD/pload-$i.pid;"
    done
    rsh "${L_PUB[$j]}" "$cmd" || fail "could not start pload on ${L_PUB[$j]}"
  done
  t0=$(date +%s)
  while :; do
    ready=$(count_loaders ready); alive=$(count_loaders alive)
    [ "$ready" -ge "$N" ] && break
    if [ $((alive + 0)) -lt "$N" ]; then dead_loaders; fail "a load process exited before READY ($ready/$N ready, $alive alive)"; fi
    [ $(( $(date +%s) - t0 )) -ge "$READY_TIMEOUT" ] && { dead_loaders; fail "READY timeout ${READY_TIMEOUT}s ($ready/$N)"; }
    sleep 2
  done
  log "  $N/$N READY after $(( $(date +%s) - t0 ))s"
  "$CL" mark > /dev/null 2>&1
  "$CL" stats > "$RD/stats-before.txt" 2>&1

  # 09-30 (Kafka side): one ssh to loader 1 timed out here and its processes never started; retry + read back each write
  now=""; for try in 1 2 3 4 5; do now=$(rsh "${L_PUB[0]}" 'date +%s%3N' 2>/dev/null) && [ -n "$now" ] && break; sleep 1; done
  [ -n "$now" ] || fail "cannot read loader 1's clock over ssh"
  START=$((now + 10000))
  for j in $(seq 0 $((NL - 1))); do
    ok=0
    for try in 1 2 3 4 5 6; do
      [ "$(rsh "${L_PUB[$j]}" "echo $START > $LRD/start.tmp && mv $LRD/start.tmp $LRD/start && cat $LRD/start" 2>/dev/null)" = "$START" ] && { ok=1; break; }
      log "  start file on ${L_PUB[$j]}: attempt $try failed, retrying"; sleep 1
    done
    [ "$ok" = 1 ] || fail "start file could not be written on ${L_PUB[$j]} (ssh)"
  done
  echo "START_MS=$START" >> "$RD/run.env"
  DS=$(dur_s "$DURATION"); DR=$(dur_s "${DRAIN:-0}")
  log "6. start instant $START (loader 1 clock + 10 s); producing ${DS}s (ramp $RAMP) + drain ${DR}s"
  sleep $((10 + DS / 2))
  log "  mid-run thread snapshot"
  "$CL" threads all 3 > "$RD/threads-mid.txt" 2>&1
  grep -E 'total=' "$RD/threads-mid.txt" | sed 's/^/  /'

  deadline=$(( $(date +%s) + DS - DS / 2 + DR + 180 ))
  while :; do
    alive=$(count_loaders alive)
    [ "$alive" = 0 ] && break
    if [ "$(date +%s)" -ge "$deadline" ]; then log "WARN $alive load processes still running 180 s after the end: stopping them"; stop_loaders; break; fi
    sleep 5
  done
  log "7. loaders done"
  echo "ENDED=$(ts)" >> "$RD/run.env"
  collect
  nf=$(grep -l '^\[final\]' "$RD"/loader*/pload-*.log 2>/dev/null | wc -l | tr -d ' ')
  if [ "$nf" = "$N" ]; then
    echo "$(ts) $N/$N load processes finished; tag=$TAG rate=$RATE" > "$RD/DONE"; rm -f "$RD/FAILED"
    log "DONE $RD"
    [ -f "$H/report/report.py" ] && python3 "$H/report/report.py" "$RD" 2>/dev/null | sed 's/^/  /'
    return 0
  fi
  echo "$(ts) only $nf/$N load processes printed [final]" > "$RD/FAILED"; log "FAILED: only $nf/$N [final] lines"; return 1
}

if [ -f "$RD/DONE" ] && [ "${FORCE:-0}" != 1 ]; then log "$TAG already DONE ($RD/DONE): skipped (FORCE=1 reruns)"; exit 0; fi
[ -d "$RD" ] && mv "$RD" "$RD.prev-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RD"
main 2>&1 | tee "$RD/run.log"
exit "${PIPESTATUS[0]}"
