#!/usr/bin/env bash
# run.sh (Mac) <tag> <total_rate> [KEY=VAL ...] — one Redpanda point of the Kafka matrix, end to end (kafka/run.sh ported):
#   1. redpanda/cluster.sh reset: fresh 3-node cluster (wipe, render by PARTS = TOPICS x PARTITIONS, production tuners,
#      drop the page cache, start, healthy, overrides checked, durability proof)
#   2. common/sampler.sh on every broker and loader host (tag redpanda-<tag>; the redpanda process is its own role)
#   3. topics created (+ warmed when TOPICS x PARTITIONS >= 2000) by `kload -create-only` on loader 1 (RF 3; -min-isr 0:
#      Redpanda has no min.insync.replicas and logs "not supported configuration ... will be ignored")
#   4. PROCS_PER_LOADER kload processes per loader host: loader-index i of N, -rate total/N, -cons-offset i*C, -cons-total
#      N*C, -group-protocol classic (Redpanda 26.2.3 answers ConsumerGroupHeartbeat/ConsumerGroupDescribe UNSUPPORTED: no
#      KIP-848; classic + franz-go's cooperative-sticky, kload's classic balancer), one -start-file each; wait for N READY;
#      then the leader balancer settle (it moves leaders ~30 s after a topic appears: the window must not see that burst)
#   5. start instant = loader-1 clock + START_DELAY into every start file; rc.sh mark; per-thread CPU mid-window
#      (rc.sh threads 3); the shards' busy seconds at the start and the end of the steady window (rc.sh busy); wait
#   6. collect into runs/redpanda/<tag>/ (loader logs + JSON, samples, redpanda.log, rendered redpanda.yaml +
#      bootstrap.yaml + overrides.txt + io-config.yaml + tune.txt per node, the cluster config in force, rc.sh stats
#      before/after, busy.txt, run.env with every parameter), stop the cluster, DONE (FAILED + reason otherwise)
# KEY=VAL (the environment works too; arguments win). Workload = kload flags (SPEC §2/§4), identical to kafka/run.sh:
#   TOPICS=1 PARTITIONS=200 ENTITIES=0 MODE=batch BATCH= (kload: 100 batch / 1 keyed) BATCH_MAX= DIST=rr ACTIVE= ACTIVE_POLICY= ZIPF_S=
#   TOPIC_DIST= TOPIC_ZIPF_S= PAYLOAD=256 CONSUMERS=auto (22 at 1 topic <= 200 partitions, else 33; SPEC §7) POLL=1000
#   PROC_US= ACK=async ACK_INFLIGHT= DRAIN= RAMP=10s DURATION=70s (whole producing time incl. ramp) REPORT=10s
#   MAX_INFLIGHT=5000 SEED=
#   Kafka client knobs, empty = kload's SPEC default (as in the Kafka runs): PRODUCERS LINGER COMPRESSION INFLIGHT_PER_BROKER
#   BATCH_MAX_BYTES FETCH_MAX_WAIT FETCH_MIN_BYTES FETCH_MAX_PARTITION_BYTES SESSION_TIMEOUT STABLE_WAIT GROUP_TIMEOUT
#   TOPIC_CONFIG CREATE_CHUNK; GROUP_PROTOCOL=classic (KIP-848 is not available on Redpanda 26.2.3); MIN_ISR=0;
#   TOPIC_TIMEOUT=1800s TOPIC_SETTLE= (30s at >= 50k partitions); KX="any extra kload flags"
# Cluster: RPROPS= (opt-in cluster props k=v,k=v, not the expert set) START_FLAGS= (opt-in Seastar flags) FALLOC= (bytes,
#   overrides the fallocation rule) MEMPCT= (percent, overrides the partition-memory rule) TUNE=1 (production tuners; 0 = the SPEC os-tune state only) DROP_CACHES=1 PROOF=1
#   LEADER_SETTLE=1 LEADER_MIN=40 LEADER_QUIET=20 LEADER_WAIT=300 (seconds)
# Harness: WARM=auto|0|1 PROCS_PER_LOADER=3 LOADER_HOSTS=<all of hosts.env> READY_TIMEOUT=300 START_DELAY=10
#   EXIT_GRACE=180 CREATE_WAIT=4200 SAMPLE_IV=2 GOGC=400 GOMEMLIMIT= PREFIX=r<tag> KLOAD=$REMOTE_ROOT/bin/kload
#   STOP_AFTER=1 FORCE=0 HOSTS_ENV=<harness>/hosts.env LOG_MAX_MB=256 (bigger redpanda.log: head + tail + WARN/ERROR digest)
# The whole file is parsed before anything runs (main at the end), so editing it during a grid cannot corrupt a
# running point; generated files on the hosts are written to a temp name and mv'd. bash 3.2-clean (the Mac's /bin/bash).
set -u

KEYS="TOPICS PARTITIONS ENTITIES MODE BATCH BATCH_MAX DIST ACTIVE ACTIVE_POLICY ZIPF_S TOPIC_DIST TOPIC_ZIPF_S PAYLOAD
CONSUMERS POLL PROC_US ACK ACK_INFLIGHT DRAIN RAMP DURATION REPORT MAX_INFLIGHT SEED PRODUCERS LINGER COMPRESSION
INFLIGHT_PER_BROKER BATCH_MAX_BYTES FETCH_MAX_WAIT FETCH_MIN_BYTES FETCH_MAX_PARTITION_BYTES GROUP_PROTOCOL MIN_ISR
SESSION_TIMEOUT STABLE_WAIT GROUP_TIMEOUT TOPIC_CONFIG CREATE_CHUNK TOPIC_TIMEOUT TOPIC_SETTLE KX RPROPS START_FLAGS FALLOC MEMPCT
TUNE DROP_CACHES PROOF LEADER_SETTLE LEADER_MIN LEADER_QUIET LEADER_WAIT WARM PROCS_PER_LOADER LOADER_HOSTS READY_TIMEOUT
START_DELAY EXIT_GRACE CREATE_WAIT SAMPLE_IV GOGC GOMEMLIMIT PREFIX KLOAD STOP_AFTER FORCE HOSTS_ENV LOG_MAX_MB"
REDPANDA_VERSION=26.2.3
DURABILITY="RF 3 raft (3 copies), acks=all acknowledged after a majority (2 of 3) fsynced the batch (write_caching=false, Redpanda's default; no min.insync.replicas in Redpanda), idempotent producers"

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

  H=$(cd "$(dirname "$0")/.." && pwd); RPD=$H/redpanda
  HOSTS_ENV=${HOSTS_ENV:-$H/hosts.env}
  [ -f "$HOSTS_ENV" ] || die "no $HOSTS_ENV (copy hosts.env.example or set HOSTS_ENV)"
  . "$HOSTS_ENV"
  R=${REMOTE_ROOT:-/root/bench}

  # ---- parameters (defaults = SPEC §7 point shape, = kafka/run.sh) ---------------------------------------------------
  TOPICS=${TOPICS:-1}; PARTITIONS=${PARTITIONS:-200}; ENTITIES=${ENTITIES:-0}; MODE=${MODE:-batch}; BATCH=${BATCH:-}
  BATCH_MAX=${BATCH_MAX:-}; DIST=${DIST:-rr}; ACTIVE=${ACTIVE:-}; ACTIVE_POLICY=${ACTIVE_POLICY:-}; ZIPF_S=${ZIPF_S:-}
  TOPIC_DIST=${TOPIC_DIST:-}; TOPIC_ZIPF_S=${TOPIC_ZIPF_S:-}; PAYLOAD=${PAYLOAD:-256}; CONSUMERS=${CONSUMERS:-auto}
  POLL=${POLL:-1000}; PROC_US=${PROC_US:-}; ACK=${ACK:-async}; ACK_INFLIGHT=${ACK_INFLIGHT:-}; DRAIN=${DRAIN:-}
  RAMP=${RAMP:-10s}; DURATION=${DURATION:-70s}; REPORT=${REPORT:-10s}; MAX_INFLIGHT=${MAX_INFLIGHT:-5000}; SEED=${SEED:-}
  PRODUCERS=${PRODUCERS:-}; LINGER=${LINGER:-}; COMPRESSION=${COMPRESSION:-}; INFLIGHT_PER_BROKER=${INFLIGHT_PER_BROKER:-}
  BATCH_MAX_BYTES=${BATCH_MAX_BYTES:-}; FETCH_MAX_WAIT=${FETCH_MAX_WAIT:-}; FETCH_MIN_BYTES=${FETCH_MIN_BYTES:-}
  FETCH_MAX_PARTITION_BYTES=${FETCH_MAX_PARTITION_BYTES:-}; GROUP_PROTOCOL=${GROUP_PROTOCOL:-classic}; MIN_ISR=${MIN_ISR:-0}
  SESSION_TIMEOUT=${SESSION_TIMEOUT:-}; STABLE_WAIT=${STABLE_WAIT:-}; GROUP_TIMEOUT=${GROUP_TIMEOUT:-}
  TOPIC_CONFIG=${TOPIC_CONFIG:-}; CREATE_CHUNK=${CREATE_CHUNK:-}; TOPIC_TIMEOUT=${TOPIC_TIMEOUT:-1800s}; TOPIC_SETTLE=${TOPIC_SETTLE:-}; KX=${KX:-}
  RPROPS=${RPROPS:-}; START_FLAGS=${START_FLAGS:-}; FALLOC=${FALLOC:-}; MEMPCT=${MEMPCT:-}; TUNE=${TUNE:-1}; DROP_CACHES=${DROP_CACHES:-1}; PROOF=${PROOF:-1}
  LEADER_SETTLE=${LEADER_SETTLE:-1}; LEADER_MIN=${LEADER_MIN:-40}; LEADER_QUIET=${LEADER_QUIET:-20}; LEADER_WAIT=${LEADER_WAIT:-300}
  WARM=${WARM:-auto}; PROCS_PER_LOADER=${PROCS_PER_LOADER:-3}; LOADER_HOSTS=${LOADER_HOSTS:-${#L_PUB[@]}}
  READY_TIMEOUT=${READY_TIMEOUT:-300}; START_DELAY=${START_DELAY:-10}; EXIT_GRACE=${EXIT_GRACE:-180}; CREATE_WAIT=${CREATE_WAIT:-4200}
  SAMPLE_IV=${SAMPLE_IV:-2}; GOGC=${GOGC:-400}; GOMEMLIMIT=${GOMEMLIMIT:-}; KLOAD=${KLOAD:-$R/bin/kload}; STOP_AFTER=${STOP_AFTER:-1}
  FORCE=${FORCE:-0}; LOG_MAX_MB=${LOG_MAX_MB:-256}
  case $GROUP_PROTOCOL in classic|consumer) ;; *) die "GROUP_PROTOCOL must be classic|consumer";; esac
  [ "$LOADER_HOSTS" -ge 1 ] && [ "$LOADER_HOSTS" -le "${#L_PUB[@]}" ] || die "LOADER_HOSTS=$LOADER_HOSTS (hosts.env has ${#L_PUB[@]})"
  NLH=$LOADER_HOSTS; PPL=$PROCS_PER_LOADER; N=$((NLH * PPL))
  PR=$((RATE / N)); PARTS_TOTAL=$((TOPICS * PARTITIONS))
  if [ "$CONSUMERS" = auto ]; then
    if [ "$TOPICS" = 1 ] && [ "$PARTITIONS" -le 200 ]; then C=22; else C=33; fi
  else C=$CONSUMERS; fi
  if [ "$WARM" = auto ]; then [ "$PARTS_TOTAL" -ge 2000 ] && WARM=1 || WARM=0; fi
  [ -n "$TOPIC_SETTLE" ] || { [ "$PARTS_TOTAL" -ge 50000 ] && TOPIC_SETTLE=30s; }   # as the Kafka runs
  case $MODE in keyed) BEFF=${BATCH:-1};; *) BEFF=${BATCH:-100};; esac             # what kload will use (for the record)
  [ -n "${PREFIX:-}" ] || PREFIX=r$(echo "$TAG" | tr -c 'A-Za-z0-9\n' '-')
  RAMP_S=$(secs "$RAMP") && DUR_S=$(secs "$DURATION") && DRAIN_S=$(secs "${DRAIN:-0}") || die "bad RAMP/DURATION/DRAIN"
  BOOT=""; for b in "${B_PRIV[@]}"; do BOOT="$BOOT${BOOT:+,}$b:9092"; done
  RD=$R/runs/redpanda/$TAG       # on the loader hosts
  OUT=$H/runs/redpanda/$TAG      # here

  [ -f "$OUT/DONE" ] && { log "already DONE ($OUT/DONE): nothing to do"; exit 0; }
  if [ -d "$OUT" ]; then mv "$OUT" "$OUT.partial-$(date -u +%Y%m%dT%H%M%SZ)"; fi
  mkdir -p "$OUT/loaders" "$OUT/samples" "$OUT/brokers" || die "cannot create $OUT"
  exec > >(tee -a "$OUT/run.log") 2>&1
  OUT_READY=1
  trap 'on_signal' INT TERM
  T0=$(date +%s)
  log "start: rate=$RATE over N=$N processes ($NLH loader hosts x $PPL) = $PR msg/s each; ${TOPICS} topic(s) x $PARTITIONS partitions" \
      "($PARTS_TOTAL total), entities=$ENTITIES mode=$MODE batch=$BEFF${BATCH_MAX:+-$BATCH_MAX} consumers=$C/process ($((N * C)) total)" \
      "group-protocol=$GROUP_PROTOCOL warm=$WARM tune=$TUNE PROFILE=${PROFILE:-vm} out=$OUT"
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
  log "1. cluster reset (PARTS=$PARTS_TOTAL, tune=$TUNE)"
  if ! PARTS=$PARTS_TOTAL FALLOC=$FALLOC MEMPCT=$MEMPCT RPROPS=$RPROPS START_FLAGS=$START_FLAGS TUNE=$TUNE DROP_CACHES=$DROP_CACHES PROOF=$PROOF \
       HOSTS_ENV=$HOSTS_ENV bash "$RPD/cluster.sh" reset > "$OUT/cluster.log" 2>&1; then
    tail -25 "$OUT/cluster.log"; fail "cluster reset failed (cluster.log)"; return 1
  fi
  grep -E 'healthy in|mkconf: node|  n[0-9] node=|WANTED|tuners applied|refused by the kernel|produced:' "$OUT/cluster.log" | sed 's/^/    /'
  { echo "CLUSTER_ID=$(rx "${B_PUB[0]}" "rpk cluster info -X admin.hosts=${B_PRIV[0]}:9644 -X brokers=${B_PRIV[0]}:9092 2>/dev/null | awk '/^CLUSTER/{getline; getline; print \$1; exit}'")"
    rx "${B_PUB[0]}" "grep -vE '^\s*#|^\s*\$' $R/redpanda/conf/.bootstrap.yaml" | sed -E 's/^([a-z_]+): (.*)$/RP_\1="\2"/'
  } >> "$OUT/run.env"

  # ---- 2. samplers -------------------------------------------------------------------------------------------------------
  log "2. samplers start (redpanda-$TAG, every ${SAMPLE_IV}s) on ${#B_PUB[@]} brokers + $NLH loader hosts"
  for h in "${B_PUB[@]}" $(lhosts); do rx "$h" "bash $R/common/sampler.sh start redpanda-$TAG $SAMPLE_IV" > /dev/null 2>&1 & done; wait
  SAMPLERS=1

  # ---- 3. topics (+ warm) ------------------------------------------------------------------------------------------------
  log "3. create $TOPICS topic(s) x $PARTITIONS partitions, RF 3$([ "$WARM" = 1 ] && echo ", warm") on loader 1"
  local CF="-brokers $BOOT -topics $TOPICS -topic $PREFIX -partitions $PARTITIONS -rf 3 -min-isr $MIN_ISR -group-protocol $GROUP_PROTOCOL -create-only"
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
  T_TOPICS=$(date +%s)
  log "   topics ready after $(( T_TOPICS - T0 ))s since start"
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
      base="$base -group-protocol $GROUP_PROTOCOL -min-isr $MIN_ISR"
      base="$base$(opt -batch-max "$BATCH_MAX")$(opt -active "$ACTIVE")$(opt -active-policy "$ACTIVE_POLICY")$(opt -zipf-s "$ZIPF_S")"
      base="$base$(opt -topic-dist "$TOPIC_DIST")$(opt -topic-zipf-s "$TOPIC_ZIPF_S")$(opt -proc-us "$PROC_US")"
      base="$base$(opt -ack-inflight "$ACK_INFLIGHT")$(opt -drain "$DRAIN")$(opt -seed "$SEED")$(opt -producers "$PRODUCERS")"
      base="$base$(opt -linger "$LINGER")$(opt -compression "$COMPRESSION")$(opt -inflight-per-broker "$INFLIGHT_PER_BROKER")"
      base="$base$(opt -batch-max-bytes "$BATCH_MAX_BYTES")$(opt -fetch-max-wait "$FETCH_MAX_WAIT")$(opt -fetch-min-bytes "$FETCH_MIN_BYTES")"
      base="$base$(opt -fetch-max-partition-bytes "$FETCH_MAX_PARTITION_BYTES")"
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
  [ "$LEADER_SETTLE" = 1 ] && leader_settle

  # ---- 5. start instant, mid-window threads, busy snapshots, wait -------------------------------------------------------
  for h in "${B_PUB[@]}"; do rx "$h" "bash $R/redpanda/rc.sh mark" > /dev/null 2>&1 & done; wait
  local NOW GO
  # 09-30 (Kafka): one ssh to a loader timed out at 2M msg/s and its 3 processes never started: every write is retried and
  # read back; a loader that still has no start files fails the point at once
  local try n bad=0 spids=()
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
  local MID=$((START_DELAY + RAMP_S + (DUR_S - RAMP_S) / 2 - 1)) BA=$((START_DELAY + RAMP_S)) BB=$((START_DELAY + DUR_S - 1))
  ( sleep "$MID"
    for b in $(seq 0 $((${#B_PUB[@]} - 1))); do rx "${B_PUB[$b]}" "bash $R/redpanda/rc.sh threads 3" > "$OUT/threads-mid-n$((b + 1)).txt" 2>&1 & done; wait
  ) &
  local SNAP=$!
  ( sleep "$BA"; busy_snap start; sleep $((BB - BA)); busy_snap end ) &   # the shards' own busy time over the steady window
  local BSNAP=$!
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
  grep -q '^end ' "$OUT/busy.txt" 2>/dev/null || kill "$BSNAP" 2>/dev/null
  wait "$BSNAP" 2>/dev/null
  log "   load finished after $(( $(date +%s) - T0 ))s since start"

  # ---- 6. collect -----------------------------------------------------------------------------------------------------------
  collect
  local rcs
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

leader_settle() {  # the leader balancer moves leaders ~30 s after topics appear: start the window after it went quiet
  local t0 now waited tot quiet line n last nnow q
  t0=$(date +%s)
  while :; do
    tot=0; quiet=999999
    for b in $(seq 0 $((${#B_PUB[@]} - 1))); do
      line=$(rx "${B_PUB[$b]}" "bash $R/redpanda/rc.sh transfers" 2>/dev/null)
      n=$(echo "$line" | sed -nE 's/.*transfers=([0-9]+).*/\1/p'); last=$(echo "$line" | sed -nE 's/.* last=([0-9]+).*/\1/p')
      nnow=$(echo "$line" | sed -nE 's/.* now=([0-9]+).*/\1/p')
      tot=$((tot + ${n:-0}))
      if [ "${last:-0}" -gt 0 ] && [ -n "$nnow" ]; then q=$((nnow - last)); [ "$q" -lt "$quiet" ] && quiet=$q; fi
    done
    now=$(date +%s); waited=$((now - T_TOPICS))
    if [ "$waited" -ge "$LEADER_MIN" ] && [ "$quiet" -ge "$LEADER_QUIET" ]; then
      log "   leader balancer quiet for $([ "$quiet" = 999999 ] && echo "ever" || echo "${quiet}s") ($tot transfers so far), $waited s after the topics were ready (waited $((now - t0)) s here)"
      break
    fi
    if [ "$waited" -ge "$LEADER_WAIT" ]; then
      log "   WARN leader balancer still moving after ${LEADER_WAIT}s ($tot transfers, last ${quiet}s ago): starting anyway (transfers_since_mark in stats-after.txt)"
      break
    fi
    sleep 5
  done
  { echo "LEADER_SETTLE_WAITED_S=$(( $(date +%s) - t0 ))"; echo "LEADER_TRANSFERS_BEFORE_WINDOW=$tot"; } >> "$OUT/run.env"
}

busy_snap() {  # busy_snap <label>: the shards' busy seconds on every broker, one line each, into busy.txt
  local b
  for b in $(seq 0 $((${#B_PUB[@]} - 1))); do rx "${B_PUB[$b]}" "bash $R/redpanda/rc.sh busy" > "$OUT/.busy.$1.$b" 2>&1 & done; wait
  for b in $(seq 0 $((${#B_PUB[@]} - 1))); do echo "$1 $(cat "$OUT/.busy.$1.$b")"; rm -f "$OUT/.busy.$1.$b"; done >> "$OUT/busy.txt"
}

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

stats_to() {  # stats_to <file>: rc.sh stats of every broker
  local b
  for b in $(seq 0 $((${#B_PUB[@]} - 1))); do rx "${B_PUB[$b]}" "bash $R/redpanda/rc.sh stats" > "$1.n$b" 2>&1 & done; wait
  for b in $(seq 0 $((${#B_PUB[@]} - 1))); do cat "$1.n$b"; rm -f "$1.n$b"; done > "$1"
  sed 's/^/    /' "$1"
}

stop_samplers() {
  [ "${SAMPLERS:-0}" = 1 ] || return 0
  local h
  for h in "${B_PUB[@]}" $(lhosts); do rx "$h" "bash $R/common/sampler.sh stop redpanda-$TAG" > /dev/null 2>&1 & done; wait
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
    rx "${L_PUB[$li]}" "cat $R/samples/redpanda-$TAG.txt" > "$OUT/samples/l$((li + 1)).txt" 2>/dev/null
  done
  rx "${B_PUB[0]}" "curl -s -m 10 'http://${B_PRIV[0]}:9644/v1/cluster_config?include_defaults=true'" > "$OUT/cluster-config.json" 2>/dev/null
  for b in $(seq 0 $((${#B_PUB[@]} - 1))); do
    d=$OUT/brokers/n$((b + 1)); mkdir -p "$d"
    rx "${B_PUB[$b]}" "cat $R/samples/redpanda-$TAG.txt" > "$OUT/samples/b$((b + 1)).txt" 2>/dev/null
    rx "${B_PUB[$b]}" "cd $R/redpanda && tar cf - conf/redpanda.yaml conf/.bootstrap.yaml conf/overrides.txt conf/io-config.yaml state/tune.txt state/installed 2>/dev/null" \
      | tar xf - -C "$d" 2>/dev/null
    [ -f "$d/conf/.bootstrap.yaml" ] && mv "$d/conf/.bootstrap.yaml" "$d/conf/bootstrap.yaml"   # no hidden files in a run dir
    # the whole log up to LOG_MAX_MB; above that (10-01: a 100k-partition create that does not converge writes ~7 MB/s of
    # raft warnings, 4-7 GB per node in 12 min) the first 64 MiB + the last 128 MiB + a digest of every WARN/ERROR kind
    rx "${B_PUB[$b]}" "cd $R/logs/redpanda 2>/dev/null || exit 0; L=redpanda.log; S=\$(stat -c %s \$L 2>/dev/null || echo 0)
      if [ \"\$S\" -gt $((LOG_MAX_MB * 1048576)) ]; then
        head -c 67108864 \$L > \$L.head; tail -c 134217728 \$L > \$L.tail; echo \"\$S bytes, \$(wc -l < \$L) lines: head 64 MiB + tail 128 MiB + digest collected\" > \$L.digest
        grep -E '^(WARN|ERROR)' \$L | sed -E 's/^([A-Z]+) +[0-9-]+ [0-9:,]+ \\[shard +[0-9]+:[a-z]+ *\\] /\\1 /; s/[0-9]+/N/g' | cut -c1-240 | sort | uniq -c | sort -rn | head -200 >> \$L.digest
        tar czf - \$L.head \$L.tail \$L.digest; rm -f \$L.head \$L.tail \$L.digest
      else tar czf - \$L; fi" | tar xzf - -C "$d" 2>/dev/null
    rx "${B_PUB[$b]}" "ls -la $R/logs/redpanda; du -sh $R/data/redpanda; df -h $R/data/redpanda | tail -1" > "$d/ls.txt" 2>/dev/null
    for f in "$d"/redpanda.log "$d"/redpanda.log.head "$d"/redpanda.log.tail; do [ -f "$f" ] && gzip -f "$f"; done
  done
  big=$(du -sh "$OUT" | cut -f1); log "   collected $big into $OUT"
}

summary() {  # per-process [final] numbers + totals + the shards' busy cores -> summary.txt (lines prefixed: report.py must not count them twice)
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
# the shards' own busy time (Seastar reactors poll: the sampler's /proc cores include the polling) and the broker-side
# produce latency (redpanda_kafka_request_latency_seconds, log2 buckets: upper bounds) over the steady window
snap = {}
bf = os.path.join(out, "busy.txt")
for l in (open(bf) if os.path.exists(bf) else []):
    m = re.match(r'^(start|end) node=(\d+) t=([\d.]+) busy_s=([\d.]+|NA)(?: produce_hist=(\S+))?', l)
    if not m: continue
    hist = {}
    for kv in (m[5] or "").split(","):
        if ":" in kv:
            le, c = kv.rsplit(":", 1)
            try: hist[float("inf") if le == "+Inf" else float(le)] = float(c)
            except ValueError: pass
    snap.setdefault(m[2], {})[m[1]] = (float(m[3]), None if m[4] == "NA" else float(m[4]), hist)
parts, lat = [], []
tb = 0.0
for node in sorted(snap):
    s = snap[node]
    if "start" not in s or "end" not in s or s["end"][0] <= s["start"][0]: continue
    if s["end"][1] is not None and s["start"][1] is not None:
        c = (s["end"][1] - s["start"][1]) / (s["end"][0] - s["start"][0]); tb += c; parts.append(f"n{node}={c:.2f}")
    h0, h1 = s["start"][2], s["end"][2]
    d = sorted((le, h1[le] - h0.get(le, 0)) for le in h1)
    tot = d[-1][1] if d else 0
    if tot > 0:
        q = lambda f: next((le for le, c in d if c >= f * tot), float("inf"))
        fmt = lambda v: "inf" if v == float("inf") else f"{v * 1000:.1f}"
        lat.append(f"n{node} p50<={fmt(q(0.5))} p99<={fmt(q(0.99))} p999<={fmt(q(0.999))} ms ({tot:.0f} req)")
if parts:
    print(f"shards busy (redpanda_cpu_busy_seconds_total over the steady window): {' '.join(parts)} sum={tb:.2f} cores")
if lat:
    print(f"broker produce latency (redpanda_kafka_request_latency_seconds, log2 bucket upper bounds, steady window): {' | '.join(lat)}")
PY
  sed 's/^/    /' "$OUT/summary.txt"
}

write_env() {  # every parameter, KEY=VALUE (report.py reads SYSTEM RATE TOPICS PARTITIONS ENTITIES MODE SHAPE)
  local k
  { echo "# run.sh $TAG, written $(date -u +%FT%TZ); every parameter of this point"
    echo "SYSTEM=redpanda"; echo "TAG=$TAG"; echo "RATE=$RATE"
    echo "SHAPE=\"${TOPICS}x${PARTITIONS} e=$ENTITIES $MODE b=$BEFF${BATCH_MAX:+-$BATCH_MAX} $DIST c=${C}x$N $GROUP_PROTOCOL\""
    for k in $KEYS; do eval "echo \"$k=\\\"\${$k:-}\\\"\""; done
    echo "N_PROCS=$N"; echo "RATE_PER_PROC=$PR"; echo "CONSUMERS_PER_PROC=$C"; echo "CONSUMERS_TOTAL=$((N * C))"; echo "BATCH_EFFECTIVE=$BEFF"
    echo "PARTS_TOTAL=$PARTS_TOTAL"; echo "BOOTSTRAP=$BOOT"; echo "RAMP_S=$RAMP_S"; echo "DURATION_S=$DUR_S"
    echo "B_PRIV=\"${B_PRIV[*]}\""; echo "L_PRIV_USED=\"$(for i in $(seq 0 $((NLH - 1))); do printf '%s ' "${L_PRIV[$i]}"; done)\""
    echo "PROFILE=${PROFILE:-vm}"; echo "REMOTE_ROOT=$R"; echo "KLOAD_SHA256_16=$KLOAD_SHA"
    echo "REDPANDA_VERSION=$REDPANDA_VERSION"; echo "DURABILITY=\"$DURABILITY\""
    echo "GROUP_PROTOCOL_WHY=\"Redpanda $REDPANDA_VERSION: ConsumerGroupHeartbeat(68)/ConsumerGroupDescribe(69) UNSUPPORTED (no KIP-848); classic + cooperative-sticky. The Kafka runs used KIP-848 (uniform assignor)\""
    echo "MIN_ISR_WHY=\"Redpanda ignores min.insync.replicas (logs: not supported configuration); acks=all = raft majority\""
    echo "START_UTC=$(date -u +%FT%TZ)"
  } > "$OUT/run.env"
}

finish() {  # end of a point, good or bad: the cluster is stopped (only one system may listen at a time)
  if [ "$STOP_AFTER" = 1 ]; then
    log "   stopping the cluster (data kept until the next reset)"
    HOSTS_ENV=$HOSTS_ENV bash "$RPD/cluster.sh" stop > "$OUT/cluster-stop.log" 2>&1
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
