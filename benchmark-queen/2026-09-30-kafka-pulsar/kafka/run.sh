#!/usr/bin/env bash
# run.sh (Mac) <tag> <total_rate> [KEY=VAL ...] — one Kafka point of the matrix, end to end (SPEC.md §6):
#   1. kafka/cluster.sh reset: fresh 3-node KRaft cluster, ONE new cluster id, heap by TOPICS x PARTITIONS
#   2. common/sampler.sh on every broker and loader host (tag kafka-<tag>)
#   3. topics created (+ warmed when TOPICS x PARTITIONS >= 2000) by `kload -create-only` on loader 1
#   4. PROCS_PER_LOADER kload processes per loader host: loader-index i of N, -rate total/N, -cons-offset i*C,
#      -cons-total N*C, one -start-file each; wait until all N print READY (READY_TIMEOUT)
#   5. start instant = loader-1 clock + START_DELAY, written into every start file; per-thread CPU of the 3 brokers
#      mid-window (kc.sh threads); wait for all N to exit
#   6. collect into runs/kafka/<tag>/ (loader logs + JSON, samples, broker server/controller/GC logs, kc.sh stats
#      before/after, rendered server.properties + jvm.env, run.env with every parameter), stop the cluster, DONE
#      (FAILED + reason otherwise: grid.sh re-runs it)
# KEY=VAL (the environment works too; arguments win). Workload = kload flags (SPEC §2/§4):
#   TOPICS=1 PARTITIONS=200 ENTITIES=0 MODE=batch BATCH= (kload: 100 batch / 1 keyed) BATCH_MAX= DIST=rr ACTIVE= ACTIVE_POLICY= ZIPF_S=
#   TOPIC_DIST= TOPIC_ZIPF_S= PAYLOAD=256 CONSUMERS=auto (22 at 1 topic <= 200 partitions, else 33; SPEC §7) POLL=1000
#   PROC_US= ACK=async ACK_INFLIGHT= DRAIN= RAMP=10s DURATION=70s (whole producing time incl. ramp) REPORT=10s
#   MAX_INFLIGHT=5000 (Queen 09-29 grid) SEED=
#   Kafka client knobs, empty = kload's SPEC default: PRODUCERS LINGER COMPRESSION INFLIGHT_PER_BROKER BATCH_MAX_BYTES
#   FETCH_MAX_WAIT FETCH_MIN_BYTES FETCH_MAX_PARTITION_BYTES GROUP_PROTOCOL SESSION_TIMEOUT STABLE_WAIT GROUP_TIMEOUT
#   TOPIC_CONFIG CREATE_CHUNK; TOPIC_TIMEOUT=1800s TOPIC_SETTLE= (30s at >= 50k partitions); KX="any extra kload flags"
# Cluster: KPROFILE=default|fsync  HEAP= (else 6g <= 10k partitions, 10g above)  KPROPS= (opt-in broker k=v,k=v, not SPEC)
#   JVM_EXTRA= (opt-in JVM flags after Kafka's own, e.g. -XX:+UseTransparentHugePages; not SPEC)
# Harness: WARM=auto|0|1 PROCS_PER_LOADER=3 LOADER_HOSTS=<all of hosts.env> READY_TIMEOUT=300 START_DELAY=10
#   EXIT_GRACE=180 CREATE_WAIT=4200 SAMPLE_IV=2 GOGC=400 GOMEMLIMIT= PREFIX=k<tag> KLOAD=$REMOTE_ROOT/bin/kload
#   STOP_AFTER=1 COLLECT_STATE_CHANGE=0 FORCE=0 HOSTS_ENV=<harness>/hosts.env
# The whole file is parsed before anything runs (main at the end), so editing it during a grid cannot corrupt a
# running point; generated files on the hosts are written to a temp name and mv'd.
set -u

KEYS="TOPICS PARTITIONS ENTITIES MODE BATCH BATCH_MAX DIST ACTIVE ACTIVE_POLICY ZIPF_S TOPIC_DIST TOPIC_ZIPF_S PAYLOAD
CONSUMERS POLL PROC_US ACK ACK_INFLIGHT DRAIN RAMP DURATION REPORT MAX_INFLIGHT SEED PRODUCERS LINGER COMPRESSION
INFLIGHT_PER_BROKER BATCH_MAX_BYTES FETCH_MAX_WAIT FETCH_MIN_BYTES FETCH_MAX_PARTITION_BYTES GROUP_PROTOCOL
SESSION_TIMEOUT STABLE_WAIT GROUP_TIMEOUT TOPIC_CONFIG CREATE_CHUNK TOPIC_TIMEOUT TOPIC_SETTLE KX KPROFILE HEAP KPROPS JVM_EXTRA WARM
PROCS_PER_LOADER LOADER_HOSTS READY_TIMEOUT START_DELAY EXIT_GRACE CREATE_WAIT SAMPLE_IV GOGC GOMEMLIMIT PREFIX KLOAD
STOP_AFTER COLLECT_STATE_CHANGE FORCE HOSTS_ENV TARGET"

log() { echo "[$(date -u +%FT%TZ)] run ${TAG:-?}: $*"; }
die() { log "ERROR $*"; [ "${OUT_READY:-0}" = 1 ] && fail "$*"; exit 2; }
rx() { local h=$1; shift; $SSH root@"$h" "$*"; }
secs() {  # Go-style duration (70s, 15m, 1m30s, 500ms, or plain seconds) -> whole seconds, rounded up
  local s=$1 tot=0 n u
  case $s in ''|*[!0-9a-z]*) echo "bad duration '$1'" >&2; return 1;; esac
  if [[ $s =~ ^[0-9]+$ ]]; then echo "$s"; return; fi
  while [[ $s =~ ^([0-9]+)(ms|h|m|s)(.*)$ ]]; do
    n=${BASH_REMATCH[1]}; u=${BASH_REMATCH[2]}; s=${BASH_REMATCH[3]}
    case $u in h) tot=$((tot + n * 3600));; m) tot=$((tot + n * 60));; s) tot=$((tot + n));; ms) tot=$((tot + (n + 999) / 1000));; esac
  done
  [ -z "$s" ] || { echo "bad duration '$1'" >&2; return 1; }
  echo "$tot"
}
opt() { [ -n "$2" ] && printf ' %s %s' "$1" "$2"; return 0; }   # opt <flag> <value>: the flag only when a value is set

main() {
  [ $# -ge 2 ] || { awk 'NR > 1 && /^#/ {print; next} NR > 1 {exit}' "$0"; exit 2; }
  TAG=$1; RATE=$2; shift 2
  [[ $TAG =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || die "tag '$TAG': use [A-Za-z0-9._-]"
  [[ $RATE =~ ^[0-9]+$ ]] && [ "$RATE" -gt 0 ] || die "total_rate '$RATE' must be a positive integer (msg/s)"
  local a k v
  for a in "$@"; do
    case $a in *=*) ;; *) die "argument '$a' is not KEY=VAL";; esac
    k=${a%%=*}; v=${a#*=}
    case " $(echo $KEYS) " in *" $k "*) ;; *) die "unknown key $k (known: $(echo $KEYS))";; esac
    printf -v "$k" '%s' "$v"
  done

  H=$(cd "$(dirname "$0")/.." && pwd); K=$H/kafka
  HOSTS_ENV=${HOSTS_ENV:-$H/hosts.env}
  [ -f "$HOSTS_ENV" ] || die "no $HOSTS_ENV (copy hosts.env.example or set HOSTS_ENV)"
  . "$HOSTS_ENV"
  R=${REMOTE_ROOT:-/root/bench}

  # ---- parameters (defaults = SPEC §7 point shape) --------------------------------------------------------------------
  TOPICS=${TOPICS:-1}; PARTITIONS=${PARTITIONS:-200}; ENTITIES=${ENTITIES:-0}; MODE=${MODE:-batch}; BATCH=${BATCH:-}
  BATCH_MAX=${BATCH_MAX:-}; DIST=${DIST:-rr}; ACTIVE=${ACTIVE:-}; ACTIVE_POLICY=${ACTIVE_POLICY:-}; ZIPF_S=${ZIPF_S:-}
  TOPIC_DIST=${TOPIC_DIST:-}; TOPIC_ZIPF_S=${TOPIC_ZIPF_S:-}; PAYLOAD=${PAYLOAD:-256}; CONSUMERS=${CONSUMERS:-auto}
  POLL=${POLL:-1000}; PROC_US=${PROC_US:-}; ACK=${ACK:-async}; ACK_INFLIGHT=${ACK_INFLIGHT:-}; DRAIN=${DRAIN:-}
  RAMP=${RAMP:-10s}; DURATION=${DURATION:-70s}; REPORT=${REPORT:-10s}; MAX_INFLIGHT=${MAX_INFLIGHT:-5000}; SEED=${SEED:-}
  PRODUCERS=${PRODUCERS:-}; LINGER=${LINGER:-}; COMPRESSION=${COMPRESSION:-}; INFLIGHT_PER_BROKER=${INFLIGHT_PER_BROKER:-}
  BATCH_MAX_BYTES=${BATCH_MAX_BYTES:-}; FETCH_MAX_WAIT=${FETCH_MAX_WAIT:-}; FETCH_MIN_BYTES=${FETCH_MIN_BYTES:-}
  FETCH_MAX_PARTITION_BYTES=${FETCH_MAX_PARTITION_BYTES:-}; GROUP_PROTOCOL=${GROUP_PROTOCOL:-}
  SESSION_TIMEOUT=${SESSION_TIMEOUT:-}; STABLE_WAIT=${STABLE_WAIT:-}; GROUP_TIMEOUT=${GROUP_TIMEOUT:-}
  TOPIC_CONFIG=${TOPIC_CONFIG:-}; CREATE_CHUNK=${CREATE_CHUNK:-}; TOPIC_TIMEOUT=${TOPIC_TIMEOUT:-1800s}; TOPIC_SETTLE=${TOPIC_SETTLE:-}; KX=${KX:-}
  KPROFILE=${KPROFILE:-default}; HEAP=${HEAP:-}; KPROPS=${KPROPS:-}; JVM_EXTRA=${JVM_EXTRA:-}; WARM=${WARM:-auto}
  PROCS_PER_LOADER=${PROCS_PER_LOADER:-3}; LOADER_HOSTS=${LOADER_HOSTS:-${#L_PUB[@]}}; READY_TIMEOUT=${READY_TIMEOUT:-300}
  START_DELAY=${START_DELAY:-10}; EXIT_GRACE=${EXIT_GRACE:-180}; CREATE_WAIT=${CREATE_WAIT:-4200}; SAMPLE_IV=${SAMPLE_IV:-2}
  GOGC=${GOGC:-400}; GOMEMLIMIT=${GOMEMLIMIT:-}; KLOAD=${KLOAD:-$R/bin/kload}; STOP_AFTER=${STOP_AFTER:-1}
  COLLECT_STATE_CHANGE=${COLLECT_STATE_CHANGE:-0}; FORCE=${FORCE:-0}
  case $KPROFILE in default|fsync) ;; *) die "KPROFILE must be default|fsync";; esac
  [ "$LOADER_HOSTS" -ge 1 ] && [ "$LOADER_HOSTS" -le "${#L_PUB[@]}" ] || die "LOADER_HOSTS=$LOADER_HOSTS (hosts.env has ${#L_PUB[@]})"
  NLH=$LOADER_HOSTS; PPL=$PROCS_PER_LOADER; N=$((NLH * PPL))
  PR=$((RATE / N)); PARTS_TOTAL=$((TOPICS * PARTITIONS))
  if [ "$CONSUMERS" = auto ]; then
    if [ "$TOPICS" = 1 ] && [ "$PARTITIONS" -le 200 ]; then C=22; else C=33; fi
  else C=$CONSUMERS; fi
  if [ "$WARM" = auto ]; then [ "$PARTS_TOTAL" -ge 2000 ] && WARM=1 || WARM=0; fi
  [ -n "$TOPIC_SETTLE" ] || { [ "$PARTS_TOTAL" -ge 50000 ] && TOPIC_SETTLE=30s; }   # 09-24: ISR still moving right after create
  case $MODE in keyed) BEFF=${BATCH:-1};; *) BEFF=${BATCH:-100};; esac             # what kload will use (for the record)
  [ -n "${PREFIX:-}" ] || PREFIX=k$(echo "$TAG" | tr -c 'A-Za-z0-9\n' '-')
  RAMP_S=$(secs "$RAMP") && DUR_S=$(secs "$DURATION") && DRAIN_S=$(secs "${DRAIN:-0}") || die "bad RAMP/DURATION/DRAIN"
  BOOT=""; for b in "${B_PRIV[@]}"; do BOOT="$BOOT${BOOT:+,}$b:9092"; done
  TARGET=${TARGET:-kafka}   # kafka | queen: queen = the same kload load against Queen's embedded Kafka facade on
                            # :9092 of the brokers (cluster started and stopped OUTSIDE run.sh; no Kafka steps here)
  case $TARGET in kafka|queen) ;; *) die "TARGET must be kafka|queen";; esac
  RD=$R/runs/$([ "$TARGET" = queen ] && echo queen-kafka || echo kafka)/$TAG     # on the loader hosts
  OUT=$H/runs/$([ "$TARGET" = queen ] && echo queen-kafka || echo kafka)/$TAG    # here

  [ -f "$OUT/DONE" ] && { log "already DONE ($OUT/DONE): nothing to do"; exit 0; }
  if [ -d "$OUT" ]; then mv "$OUT" "$OUT.partial-$(date -u +%Y%m%dT%H%M%SZ)"; fi
  mkdir -p "$OUT/loaders" "$OUT/samples" "$OUT/brokers" || die "cannot create $OUT"
  exec > >(tee -a "$OUT/run.log") 2>&1
  OUT_READY=1
  trap 'on_signal' INT TERM
  T0=$(date +%s)
  log "start: rate=$RATE over N=$N processes ($NLH loader hosts x $PPL) = $PR msg/s each; ${TOPICS} topic(s) x $PARTITIONS partitions" \
      "($PARTS_TOTAL total), entities=$ENTITIES mode=$MODE batch=$BEFF${BATCH_MAX:+-$BATCH_MAX} consumers=$C/process ($((N * C)) total)" \
      "warm=$WARM profile=$KPROFILE PROFILE=${PROFILE:-vm} out=$OUT"
  [ $((PR * N)) -eq "$RATE" ] || log "note: $RATE is not divisible by $N; offering $((PR * N)) msg/s"

  # ---- pre-flight: hosts reachable, kload present, nothing of ours still running ---------------------------------------
  local h i b
  for i in $(seq 0 $((NLH - 1))); do
    h=${L_PUB[$i]}
    rx "$h" "test -x $KLOAD" || die "loader ${L_PRIV[$i]}: no executable $KLOAD (common/deploy.sh harness with mqload/bin built)"
    if rx "$h" "pgrep -x kload > /dev/null"; then
      [ "$FORCE" = 1 ] || die "loader ${L_PRIV[$i]}: a kload is already running (FORCE=1 overrides)"
    fi
  done
  KLOAD_SHA=$(rx "${L_PUB[0]}" "sha256sum $KLOAD" | cut -c1-16)
  write_env

  # ---- 1. fresh cluster --------------------------------------------------------------------------------------------------
  if [ "$TARGET" = queen ]; then
    log "1. TARGET=queen: Queen's Kafka facade on $BOOT (cluster managed outside run.sh; no reset)"
  else
  log "1. cluster reset (profile $KPROFILE, PARTS=$PARTS_TOTAL)"
  if ! PARTS=$PARTS_TOTAL HEAP=$HEAP KPROPS=$KPROPS JVM_EXTRA=$JVM_EXTRA KPROFILE=$KPROFILE CID_FILE=$OUT/cluster.id HOSTS_ENV=$HOSTS_ENV \
       bash "$K/cluster.sh" reset "$KPROFILE" > "$OUT/cluster.log" 2>&1; then
    tail -25 "$OUT/cluster.log"; fail "cluster reset failed (cluster.log)"; return 1
  fi
  grep -E 'healthy in|features:|versions|  n[0-9] node=' "$OUT/cluster.log" | sed 's/^/    /'
  echo "CLUSTER_ID=$(cat "$OUT/cluster.id" 2>/dev/null)" >> "$OUT/run.env"
  fi

  # ---- 2. samplers -------------------------------------------------------------------------------------------------------
  log "2. samplers start (kafka-$TAG, every ${SAMPLE_IV}s) on ${#B_PUB[@]} brokers + $NLH loader hosts"
  for h in "${B_PUB[@]}" $(lhosts); do rx "$h" "bash $R/common/sampler.sh start kafka-$TAG $SAMPLE_IV" > /dev/null 2>&1 & done; wait
  SAMPLERS=1

  # ---- 3. topics (+ warm) ------------------------------------------------------------------------------------------------
  log "3. create $TOPICS topic(s) x $PARTITIONS partitions, RF 3, min.insync 2$([ "$WARM" = 1 ] && echo ", warm") on loader 1"
  local CF="-brokers $BOOT -topics $TOPICS -topic $PREFIX -partitions $PARTITIONS -rf 3 -min-isr $([ "$TARGET" = queen ] && echo 1 || echo 2) -create-only"
  [ "$WARM" = 1 ] && CF="$CF -warm"
  CF="$CF$(opt -topic-config "$TOPIC_CONFIG")$(opt -create-chunk "$CREATE_CHUNK")$(opt -topic-timeout "$TOPIC_TIMEOUT")$(opt -topic-settle "$TOPIC_SETTLE")"
  CF="$CF -tag $TAG-create -out $RD/create.json $KX"
  mkdir -p "$OUT/loaders/l1"
  runner create "$CF" > "$OUT/loaders/l1/create.sh"
  ship 0 "$OUT/loaders/l1/create.sh"
  rx "${L_PUB[0]}" "cd $RD && rm -f create.rc create.pid create.log && nohup bash create.sh > /dev/null 2>&1 < /dev/null &"
  local deadline=$(( $(date +%s) + CREATE_WAIT )) st
  while :; do
    st=$(rx "${L_PUB[0]}" "cd $RD; if [ -f create.rc ]; then echo DONE \$(cat create.rc); elif kill -0 \$(cat create.pid 2>/dev/null) 2>/dev/null; then echo RUN; else echo GONE; fi")
    case $st in DONE*|GONE) break;; esac
    [ "$(date +%s)" -lt "$deadline" ] || { rx "${L_PUB[0]}" "kill \$(cat $RD/create.pid) 2>/dev/null"; st="TIMEOUT"; break; }
    sleep 3
  done
  rx "${L_PUB[0]}" "cat $RD/create.log" > "$OUT/loaders/l1/create.log" 2>/dev/null
  sed 's/^/    /' "$OUT/loaders/l1/create.log" | tail -12
  [ "$st" = "DONE 0" ] || { fail "topic create: $st (loaders/l1/create.log)"; return 1; }
  log "   topics ready after $(( $(date +%s) - T0 ))s since start"
  stats_to "$OUT/stats-before.txt"

  # ---- 4. N load processes behind the start barrier ----------------------------------------------------------------------
  log "4. launch $N kload processes"
  local base lf li
  for li in $(seq 0 $((NLH - 1))); do
    lf=$OUT/loaders/l$((li + 1)); mkdir -p "$lf"
    { echo "#!/usr/bin/env bash"; echo "# run.sh $TAG: start this host's load processes (generated $(date -u +%FT%TZ))"
      echo "cd $RD || exit 1"
      for i in $(seq $((li * PPL)) $((li * PPL + PPL - 1))); do
        echo "rm -f p$i.start p$i.rc p$i.pid p$i.json p$i.log; nohup bash p$i.sh > /dev/null 2>&1 < /dev/null &"
      done
      echo "echo launched $(seq -s ' ' $((li * PPL)) $((li * PPL + PPL - 1))) on \$(hostname)"; } > "$lf/launch.sh"
    for i in $(seq $((li * PPL)) $((li * PPL + PPL - 1))); do
      base="-brokers $BOOT -topics $TOPICS -topic $PREFIX -partitions $PARTITIONS -entities $ENTITIES -create=false"
      base="$base -rate $PR -ramp $RAMP -duration $DURATION -report $REPORT -start-file $RD/p$i.start"
      base="$base -max-inflight $MAX_INFLIGHT -mode $MODE$(opt -batch "$BATCH") -payload $PAYLOAD -dist $DIST"
      base="$base -consumers $C -cons-offset $((i * C)) -cons-total $((N * C)) -poll $POLL -ack $ACK"
      base="$base -loader-index $i -loaders $N -local-src $((li * PPL))-$((li * PPL + PPL - 1)) -out $RD/p$i.json -tag $TAG-p$i"
      base="$base$(opt -batch-max "$BATCH_MAX")$(opt -active "$ACTIVE")$(opt -active-policy "$ACTIVE_POLICY")$(opt -zipf-s "$ZIPF_S")"
      base="$base$(opt -topic-dist "$TOPIC_DIST")$(opt -topic-zipf-s "$TOPIC_ZIPF_S")$(opt -proc-us "$PROC_US")"
      base="$base$(opt -ack-inflight "$ACK_INFLIGHT")$(opt -drain "$DRAIN")$(opt -seed "$SEED")$(opt -producers "$PRODUCERS")"
      base="$base$(opt -linger "$LINGER")$(opt -compression "$COMPRESSION")$(opt -inflight-per-broker "$INFLIGHT_PER_BROKER")"
      base="$base$(opt -batch-max-bytes "$BATCH_MAX_BYTES")$(opt -fetch-max-wait "$FETCH_MAX_WAIT")$(opt -fetch-min-bytes "$FETCH_MIN_BYTES")"
      base="$base$(opt -fetch-max-partition-bytes "$FETCH_MAX_PARTITION_BYTES")$(opt -group-protocol "$GROUP_PROTOCOL")"
      base="$base$(opt -session-timeout "$SESSION_TIMEOUT")$(opt -stable-wait "$STABLE_WAIT")$(opt -group-timeout "$GROUP_TIMEOUT")"
      base="$base $KX"
      runner "p$i" "$base" > "$lf/p$i.sh"
    done
    ship "$li" "$lf"/p*.sh "$lf/launch.sh"
  done
  for li in $(seq 0 $((NLH - 1))); do rx "${L_PUB[$li]}" "bash $RD/launch.sh" | sed 's/^/    /' & done; wait
  LAUNCHED=1
  deadline=$(( $(date +%s) + READY_TIMEOUT ))
  local S nready last=""
  while :; do
    S=$(states)
    nready=$(echo "$S" | grep -c ' READY$')
    [ "$nready" = "$N" ] && break
    if echo "$S" | grep -qE ' (DONE|GONE)'; then
      echo "$S" | grep -E ' (DONE|GONE)' | sed 's/^/    /'; tails 15; fail "a load process exited before READY"; return 1
    fi
    [ "$(date +%s)" -lt "$deadline" ] || { echo "$S" | sed 's/^/    /'; tails 10; fail "not all READY after ${READY_TIMEOUT}s ($nready/$N)"; return 1; }
    [ "$nready" != "$last" ] && { log "   READY $nready/$N"; last=$nready; }
    sleep 2
  done
  log "   all $N READY after $(( $(date +%s) - T0 ))s since start"

  # ---- 5. start instant, mid-window threads, wait -----------------------------------------------------------------------
  for h in "${B_PUB[@]}"; do rx "$h" "bash $R/kafka/kc.sh mark" > /dev/null 2>&1 & done; wait
  local NOW GO
  # 09-30: one ssh to loader 1 timed out here at 2M msg/s and its 3 processes never started (point FAILED after 5 min):
  # every write is retried and read back; a loader that still has no start files fails the point at once
  local try n li bad=0 spids=()
  NOW=""; for try in 1 2 3 4 5; do NOW=$(rx "${L_PUB[0]}" 'date +%s%3N' 2>/dev/null) && [ -n "$NOW" ] && break; sleep 1; done
  [ -n "$NOW" ] || { fail "cannot read loader 1's clock over ssh"; return 1; }
  GO=$((NOW + START_DELAY * 1000))
  for li in $(seq 0 $((NLH - 1))); do
    ( for try in 1 2 3 4 5 6; do
        n=$(rx "${L_PUB[$li]}" "cd $RD && for i in $(seq -s ' ' $((li * PPL)) $((li * PPL + PPL - 1))); do echo $GO > p\$i.start.tmp && mv p\$i.start.tmp p\$i.start; done; ls p*.start 2>/dev/null | wc -l" 2>/dev/null)
        [ "$(echo $n)" = "$PPL" ] && exit 0
        echo "   start files on ${L_PUB[$li]}: attempt $try got '$n', retrying" >&2; sleep 1
      done; exit 1 ) &
    spids+=($!)
  done
  for n in "${spids[@]}"; do wait "$n" || bad=1; done
  [ "$bad" = 0 ] || { fail "start files could not be written on every loader (ssh)"; return 1; }
  log "5. start instant $GO (loader-1 clock + ${START_DELAY}s) written to $N start files; producing ${DURATION} (ramp $RAMP)"
  { echo "GO_MS=$GO"; echo "GO_UTC=$(date -u -r $((GO / 1000)) +%FT%TZ 2>/dev/null || date -u -d @$((GO / 1000)) +%FT%TZ)"; } >> "$OUT/run.env"
  local MID=$((START_DELAY + RAMP_S + (DUR_S - RAMP_S) / 2 - 1))
  ( sleep "$MID"
    for b in $(seq 0 $((${#B_PUB[@]} - 1))); do rx "${B_PUB[$b]}" "bash $R/kafka/kc.sh threads 3" > "$OUT/threads-mid-n$((b + 1)).txt" 2>&1 & done; wait
  ) &
  local SNAP=$!
  deadline=$(( $(date +%s) + START_DELAY + DUR_S + DRAIN_S + EXIT_GRACE ))
  local ndone lastp=0 TIMEDOUT=0
  while :; do
    sleep 5
    S=$(states); ndone=$(echo "$S" | grep -cE ' (DONE|GONE)')
    [ "$ndone" = "$N" ] && break
    if [ "$(date +%s)" -ge "$deadline" ]; then log "   TIMEOUT: $ndone/$N exited; killing the rest"; killall_loads; TIMEDOUT=1; sleep 3; break; fi
    if [ $(( $(date +%s) - lastp )) -ge 30 ]; then
      lastp=$(date +%s)
      log "   $ndone/$N exited | p0: $(rx "${L_PUB[0]}" "grep -E '^\[[0-9:]{8}\] offered=' $RD/p0.log | tail -1" | cut -c1-200)"
    fi
  done
  [ -s "$OUT/threads-mid-n1.txt" ] || kill "$SNAP" 2>/dev/null   # ended before mid-window: no snapshot to wait for
  wait "$SNAP" 2>/dev/null
  log "   load finished after $(( $(date +%s) - T0 ))s since start"

  # ---- 6. collect -----------------------------------------------------------------------------------------------------------
  collect
  local rcs bad
  rcs=$(states); bad=$(echo "$rcs" | grep -vE ' DONE 0$')
  for li in $(seq 0 $((NLH - 1))); do
    for i in $(seq $((li * PPL)) $((li * PPL + PPL - 1))); do
      grep -q '^\[final\]' "$OUT/loaders/l$((li + 1))/p$i.log" 2>/dev/null || bad="$bad
p$i: no [final] line"
    done
  done
  summary
  { echo "END_UTC=$(date -u +%FT%TZ)"; echo "ELAPSED_S=$(( $(date +%s) - T0 ))"; echo "PROC_STATES=\"$(echo $rcs)\""; } >> "$OUT/run.env"
  if [ "$TIMEDOUT" = 1 ] || [ -n "$(echo "$bad" | tr -d '[:space:]')" ]; then
    echo "$bad" | sed '/^$/d; s/^/    /'
    fail "load processes did not all finish cleanly (timeout=$TIMEDOUT)"; return 1
  fi
  finish
  { date -u +%FT%TZ; cat "$OUT/summary.txt"; } > "$OUT/DONE"
  log "DONE in $(( $(date +%s) - T0 ))s -> $OUT"
}

lhosts() { local i; for i in $(seq 0 $((NLH - 1))); do echo "${L_PUB[$i]}"; done; }

runner() {  # runner <name> <kload flags>: the wrapper that runs kload detached, pid + exit code in files
  cat <<EOF
#!/usr/bin/env bash
# run.sh $TAG: $1 (generated $(date -u +%FT%TZ)); pid in $1.pid, exit code in $1.rc
cd $RD || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n \$(ulimit -Hn)
export GOGC=$GOGC${GOMEMLIMIT:+ GOMEMLIMIT=$GOMEMLIMIT}
$KLOAD $2 > $1.log 2>&1 < /dev/null &
echo \$! > $1.pid.tmp && mv $1.pid.tmp $1.pid
wait \$!
echo \$? > $1.rc.tmp && mv $1.rc.tmp $1.rc
EOF
}

ship() {  # ship <loader idx> <files...>: into $RD on that loader (temp name, then mv; never over a running file in place)
  local li=$1 f; shift
  for f in "$@"; do
    rx "${L_PUB[$li]}" "mkdir -p $RD && cat > $RD/.$(basename "$f").tmp && mv $RD/.$(basename "$f").tmp $RD/$(basename "$f")" < "$f" || die "ship $f"
  done
}

states() {  # one line per load process: "p<i> READY|RUN|DONE <rc>|GONE"
  local li
  for li in $(seq 0 $((NLH - 1))); do
    rx "${L_PUB[$li]}" "cd $RD 2>/dev/null && for i in $(seq -s ' ' $((li * PPL)) $((li * PPL + PPL - 1))); do
      if [ -f p\$i.rc ]; then echo \"p\$i DONE \$(cat p\$i.rc)\"
      elif kill -0 \$(cat p\$i.pid 2>/dev/null) 2>/dev/null; then
        if grep -qE 'READY [0-9]{12,}' p\$i.log 2>/dev/null; then echo \"p\$i READY\"; else echo \"p\$i RUN\"; fi
      else echo \"p\$i GONE\"; fi; done"
  done
}

tails() {  # tails <n>: last n lines of every load process log
  local li
  for li in $(seq 0 $((NLH - 1))); do
    rx "${L_PUB[$li]}" "cd $RD && for f in p*.log; do echo \"--- ${L_PRIV[$li]} \$f\"; tail -$1 \$f; done" 2>/dev/null | sed 's/^/    /'
  done
}

killall_loads() {
  local li
  for li in $(seq 0 $((NLH - 1))); do rx "${L_PUB[$li]}" "cd $RD 2>/dev/null && for p in p*.pid create.pid; do [ -f \$p ] && kill \$(cat \$p) 2>/dev/null; done; true" & done; wait
}

stats_to() {  # stats_to <file>: kc.sh stats of every broker
  local b
  for b in $(seq 0 $((${#B_PUB[@]} - 1))); do rx "${B_PUB[$b]}" "bash $R/kafka/kc.sh stats" > "$1.n$b" 2>&1 & done; wait
  for b in $(seq 0 $((${#B_PUB[@]} - 1))); do cat "$1.n$b"; rm -f "$1.n$b"; done > "$1"
  sed 's/^/    /' "$1"
}

stop_samplers() {
  [ "${SAMPLERS:-0}" = 1 ] || return 0
  local h
  for h in "${B_PUB[@]}" $(lhosts); do rx "$h" "bash $R/common/sampler.sh stop kafka-$TAG" > /dev/null 2>&1 & done; wait
  SAMPLERS=0
}

collect() {
  log "6. collect"
  stop_samplers
  stats_to "$OUT/stats-after.txt"
  local b li d f big
  for li in $(seq 0 $((NLH - 1))); do
    mkdir -p "$OUT/loaders/l$((li + 1))"
    rx "${L_PUB[$li]}" "cd $RD && tar cf - --exclude='*.start' --exclude='*.tmp' ." | tar xf - -C "$OUT/loaders/l$((li + 1))" 2>/dev/null
    rx "${L_PUB[$li]}" "cat $R/samples/kafka-$TAG.txt" > "$OUT/samples/l$((li + 1)).txt" 2>/dev/null
  done
  for b in $(seq 0 $((${#B_PUB[@]} - 1))); do
    d=$OUT/brokers/n$((b + 1)); mkdir -p "$d"
    rx "${B_PUB[$b]}" "cat $R/samples/kafka-$TAG.txt" > "$OUT/samples/b$((b + 1)).txt" 2>/dev/null
    rx "${B_PUB[$b]}" "cat $R/kafka/conf/server.properties" > "$d/server.properties" 2>/dev/null
    rx "${B_PUB[$b]}" "cat $R/kafka/conf/jvm.env" > "$d/jvm.env" 2>/dev/null
    rx "${B_PUB[$b]}" "cd $R/logs/kafka 2>/dev/null && tar czf - \$(ls -d server.log* controller.log* kafkaServer-gc.log* kafka-request.log* kafkaServer.out $([ "$COLLECT_STATE_CHANGE" = 1 ] && echo 'state-change.log*') 2>/dev/null)" \
      | tar xzf - -C "$d" 2>/dev/null
    rx "${B_PUB[$b]}" "ls -la $R/logs/kafka; du -sh $R/data/kafka" > "$d/ls.txt" 2>/dev/null
    for f in "$d"/server.log* "$d"/controller.log* "$d"/kafka-request.log* "$d"/kafkaServer.out "$d"/state-change.log*; do
      [ -f "$f" ] && case $f in *.gz) ;; *) gzip -f "$f";; esac
    done
  done
  big=$(du -sh "$OUT" | cut -f1); log "   collected $big into $OUT"
}

summary() {  # per-process [final] numbers + totals -> summary.txt (lines prefixed: report.py must not count them twice)
  python3 - "$OUT" "$N" > "$OUT/summary.txt" <<'PY'
import glob, os, re, sys
out, n = sys.argv[1], int(sys.argv[2])
F = re.compile(r'^\[final\].*?shed=(\d+) \(msgs: offered=(\d+) achieved=(\d+) shed=(\d+)\) pushErr=(\d+) \| pushed=(\d+) popped=(\d+) lag=(-?\d+) \| popErr=(\d+).*?overall p50=([\d.]+) p99=([\d.]+) p999=([\d.]+).*?ackErr=(\d+).*?e2e p50=([\d.]+) p99=([\d.]+) p999=([\d.]+)')
tot = dict(off=0, ach=0, shed=0, pushed=0, popped=0, errs=0); e99 = []; p99 = []; seen = 0
for p in sorted(glob.glob(os.path.join(out, "loaders", "l*", "p*.log")), key=lambda x: int(re.search(r'p(\d+)\.log$', x)[1])):
    name = os.path.basename(p)[:-4]
    txt = open(p, errors="replace").read()
    m = None
    for l in txt.splitlines():
        mm = F.match(l)
        if mm: m = mm
    cpu = re.findall(r'^load_cpu=([\d.]+)%', txt, re.M)
    if not m:
        print(f"{name}: NO FINAL LINE"); continue
    seen += 1
    g = m.groups()
    tot["off"] += int(g[1]); tot["ach"] += int(g[2]); tot["shed"] += int(g[3]); tot["pushed"] += int(g[5]); tot["popped"] += int(g[6])
    tot["errs"] += int(g[4]) + int(g[8]) + int(g[12]); e99.append(float(g[14])); p99.append(float(g[10]))
    print(f"{name}: msgs offered={g[1]} achieved={g[2]} shed={g[3]} pushed={g[5]} popped={g[6]} lag={g[7]} errs push={g[4]} pop={g[8]} ack={g[12]}"
          f" | produce p50/p99/p999={g[9]}/{g[10]}/{g[11]} ms | e2e p50/p99/p999={g[13]}/{g[14]}/{g[15]} ms | cpu={cpu[-1] if cpu else '?'}%")
print(f"total: {seen}/{n} processes with a final line | msgs offered={tot['off']} achieved={tot['ach']} shed={tot['shed']}"
      f" pushed={tot['pushed']} popped={tot['popped']} errors={tot['errs']} | worst produce p99={max(p99) if p99 else -1} ms"
      f" | worst e2e p99={max(e99) if e99 else -1} ms")
PY
  sed 's/^/    /' "$OUT/summary.txt"
}

write_env() {  # every parameter, KEY=VALUE (report.py reads SYSTEM RATE TOPICS PARTITIONS ENTITIES MODE SHAPE)
  local k
  { echo "# run.sh $TAG, written $(date -u +%FT%TZ); every parameter of this point"
    echo "SYSTEM=kafka"; echo "TAG=$TAG"; echo "RATE=$RATE"
    echo "SHAPE=\"${TOPICS}x${PARTITIONS} e=$ENTITIES $MODE b=$BEFF${BATCH_MAX:+-$BATCH_MAX} $DIST c=${C}x$N $KPROFILE\""
    for k in $KEYS; do eval "echo \"$k=\\\"\${$k:-}\\\"\""; done
    echo "N_PROCS=$N"; echo "RATE_PER_PROC=$PR"; echo "CONSUMERS_PER_PROC=$C"; echo "CONSUMERS_TOTAL=$((N * C))"; echo "BATCH_EFFECTIVE=$BEFF"
    echo "PARTS_TOTAL=$PARTS_TOTAL"; echo "BOOTSTRAP=$BOOT"; echo "RAMP_S=$RAMP_S"; echo "DURATION_S=$DUR_S"
    echo "B_PRIV=\"${B_PRIV[*]}\""; echo "L_PRIV_USED=\"$(for i in $(seq 0 $((NLH - 1))); do printf '%s ' "${L_PRIV[$i]}"; done)\""
    echo "PROFILE=${PROFILE:-vm}"; echo "REMOTE_ROOT=$R"; echo "KLOAD_SHA256_16=$KLOAD_SHA"
    echo "KAFKA_VERSION=4.3.1"; echo "START_UTC=$(date -u +%FT%TZ)"
  } > "$OUT/run.env"
}

finish() {  # end of a point, good or bad: the cluster is stopped (only one system may listen at a time)
  if [ "$STOP_AFTER" = 1 ] && [ "${TARGET:-kafka}" != queen ]; then
    log "   stopping the cluster (data kept until the next reset)"
    HOSTS_ENV=$HOSTS_ENV bash "$K/cluster.sh" stop > "$OUT/cluster-stop.log" 2>&1
  fi
}

fail() {
  log "FAILED: $*"
  [ "${LAUNCHED:-0}" = 1 ] && killall_loads
  if [ ! -f "$OUT/stats-after.txt" ] && [ "${SAMPLERS:-0}" = 1 ]; then collect; fi
  stop_samplers
  finish
  { date -u +%FT%TZ; echo "$*"; } > "$OUT/FAILED"
}

on_signal() {
  trap - INT TERM
  fail "interrupted"
  exit 130
}

main "$@"; exit $?
