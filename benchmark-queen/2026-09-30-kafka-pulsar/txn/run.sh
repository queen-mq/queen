#!/usr/bin/env bash
# txn/run.sh (Mac) <system> <tag> <total_rate> [KEY=VAL ...] — one point of the TRANSACTIONAL benchmark (txn/grid.sh = the matrix):
# exactly-once consume-transform-produce on <system> = kafka | redpanda | pulsar | queen, same droplets, same 3 loaders,
# same loader core (mqload: kload / pload / qload -txn), collected into runs/txn/<system>/<tag>/:
#   1. fresh cluster: kafka/cluster.sh reset | redpanda/cluster.sh reset | pulsar/cluster.sh reset with the transaction
#      coordinator on | Queen's qc.sh stop+wipe+start on the 3 brokers (leader on n1 as the 09-30 Queen grid)
#   2. common/sampler.sh on every broker and loader (tag txn-<system>-<tag>)
#   3. <prefix>-in and <prefix>-out (PARTITIONS each: output partition = input partition) created + warmed by
#      `<tool> -txn -create-only -warm` on loader 1 (Pulsar: subscription sub on both, verify on out)
#   4. PROCS_PER_LOADER processes per loader: the feeder (rate/N msg/s into <prefix>-in, units of BATCH messages, open
#      loop), CONSUMERS transactional workers and as many read-committed readers of <prefix>-out each
#   5. start instant = loader-1 clock + START_DELAY; mid-window thread snapshot; Redpanda: shards' busy time over the
#      steady window; wait for every process ([final] + [txn-final] + the id ledger)
#   6. the verifier on loader 1 (`<tool> -verify` over the 9 ledgers): <prefix>-out in full + the unprocessed rest of
#      <prefix>-in -> verify.log ([verify] ... VERDICT PASS|FAIL)
#   7. collect (loader logs + JSON + ledgers, samples, broker logs + configs, run.env, summary.txt), stop the cluster,
#      DONE (FAILED + reason otherwise; a FAIL verdict is a result, not a failed point: it is in DONE and summary.txt)
# KEY=VAL (the environment works too; arguments win):
#   workload  PARTITIONS=200 (each of in and out: 2 x PARTITIONS in the cluster) TXN_SIZE=10 TXN_LINGER=1s TXN_TIMEOUT=10s (Kafka/Redpanda TransactionTimeout, Pulsar NewTransaction timeout)
#             LEASE=10 (Queen: leaseSeconds of every pop = the transaction's fence, Queen's analogue of TXN_TIMEOUT)
#             BATCH=100 (feeder unit, as the matrix) PAYLOAD=256 CONSUMERS=auto (workers per process: 22 at <= 200
#             partitions, else 33; readers = the same) RAMP=10s DURATION=70s REPORT=10s DRAIN=60s (max; ends after
#             IDLE_EXIT=3s of quiet) MAX_INFLIGHT=5000 SEED= POP_WIDTH=10 POP_BATCH=1000 (Queen) LX="extra loader flags"
#   cluster   kafka: KPROFILE=default HEAP= KPROPS= GROUP_PROTOCOL=classic (10-01 smoke at 20k msg/s: KIP-848 commit p50
#             101 ms / e2e p50 7 s, behind; classic 34 ms / 0.2 s: franz-go's GroupTransactSession forces a group heartbeat
#             before every EndTxn, under KIP-848 a ConsumerGroupHeartbeat to the one group coordinator) TOPIC_SETTLE= (30s
#             at >= 50k); kload's workers run -txn-metadata-min-age 250ms (its default): franz-go retries a produce
#             answered CONCURRENT_TRANSACTIONS (transactions v2, previous transaction's markers not yet written after the
#             broker's own 100 ms of retries) through a metadata refresh gated by MetadataMinAge, 5 s by default = 5 s
#             commit stalls (classic smoke p999 5.0 s -> 0.5 s) |
#             redpanda: RPROPS= TUNE=1 MEMPCT= FALLOC= | pulsar: PULSAR_TXN_SET (default: the coordinator on + batched
#             transaction log and pending-ack writes) BROKER_SET= BOOKIE_SET= | queen: QBIN=/root/qr/queen-b2 QLANES=16
#             QEXTRA="<the 09-30 grid's env>" QAVOID="2 3" (nodes that must not lead: n2 is the older CPU)
#   harness   WARM=1 VERIFY=1 VERIFY_IDLE=10s (120s from 2000 partitions in the cluster) PROCS_PER_LOADER=3 LOADER_HOSTS=<hosts.env> READY_TIMEOUT=300 START_DELAY=10
#             EXIT_GRACE=240 CREATE_WAIT=4200 SAMPLE_IV=2 GOGC=400 GOMEMLIMIT= PREFIX=x<tag> STOP_AFTER=1 FORCE=0
#             HOSTS_ENV=<harness>/hosts.env
# Every ssh is retried on a connection failure (exit 255: the Mac's uplink is a phone hotspot); remote steps that must
# not run twice take a mkdir lock first. The whole file is parsed before anything runs (main at the end), so editing it
# during a grid cannot corrupt a running point. Bash 3.2-clean (the Mac's /bin/bash).
set -u

KEYS="PARTITIONS TXN_SIZE TXN_LINGER TXN_TIMEOUT LEASE BATCH PAYLOAD CONSUMERS RAMP DURATION REPORT DRAIN
IDLE_EXIT MAX_INFLIGHT SEED POP_WIDTH POP_BATCH LX KPROFILE HEAP KPROPS GROUP_PROTOCOL TOPIC_SETTLE RPROPS TUNE MEMPCT FALLOC
PULSAR_TXN_SET BROKER_SET BOOKIE_SET QBIN QLANES QEXTRA QAVOID WARM VERIFY VERIFY_IDLE PROCS_PER_LOADER LOADER_HOSTS
READY_TIMEOUT START_DELAY EXIT_GRACE CREATE_WAIT SAMPLE_IV GOGC GOMEMLIMIT PREFIX STOP_AFTER FORCE HOSTS_ENV"

log() { echo "[$(date -u +%FT%TZ)] txn ${SYS:-?}/${TAG:-?}: $*"; }
die() { log "ERROR $*"; [ "${OUT_READY:-0}" = 1 ] && fail "$*"; exit 2; }
rx() {  # rx <host> <cmd>: ssh, a connection failure (255) retried up to 6 times
  local h=$1 t rc=255; shift
  for t in 1 2 3 4 5 6; do
    $SSH root@"$h" "$*" < /dev/null; rc=$?
    [ "$rc" != 255 ] && return "$rc"
    sleep $((t * 2))
  done
  return "$rc"
}
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
  [ $# -ge 3 ] || { awk 'NR > 1 && /^#/ {print; next} NR > 1 {exit}' "$0"; exit 2; }
  SYS=$1; TAG=$2; RATE=$3; shift 3
  case " kafka redpanda pulsar queen " in *" $SYS "*) ;; *) die "system '$SYS': kafka|redpanda|pulsar|queen";; esac
  [[ $TAG =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || die "tag '$TAG': use [A-Za-z0-9._-]"
  [[ $RATE =~ ^[0-9]+$ ]] && [ "$RATE" -gt 0 ] || die "total_rate '$RATE' must be a positive integer (msg/s)"
  local a k v
  for a in "$@"; do
    case $a in *=*) ;; *) die "argument '$a' is not KEY=VAL";; esac
    k=${a%%=*}; v=${a#*=}
    case " $(echo $KEYS) " in *" $k "*) ;; *) die "unknown key $k (known: $(echo $KEYS))";; esac
    printf -v "$k" '%s' "$v"
  done

  H=$(cd "$(dirname "$0")/.." && pwd)
  HOSTS_ENV=${HOSTS_ENV:-$H/hosts.env}
  [ -f "$HOSTS_ENV" ] || die "no $HOSTS_ENV (copy hosts.env.example or set HOSTS_ENV)"
  . "$HOSTS_ENV"
  R=${REMOTE_ROOT:-/root/bench}

  # ---- parameters ---------------------------------------------------------------------------------------------------
  PARTITIONS=${PARTITIONS:-200}; TXN_SIZE=${TXN_SIZE:-10}; TXN_LINGER=${TXN_LINGER:-1s}
  TXN_TIMEOUT=${TXN_TIMEOUT:-10s}; LEASE=${LEASE:-10}; BATCH=${BATCH:-100}; PAYLOAD=${PAYLOAD:-256}; CONSUMERS=${CONSUMERS:-auto}
  RAMP=${RAMP:-10s}; DURATION=${DURATION:-70s}; REPORT=${REPORT:-10s}; DRAIN=${DRAIN:-60s}; IDLE_EXIT=${IDLE_EXIT:-3s}
  MAX_INFLIGHT=${MAX_INFLIGHT:-5000}; SEED=${SEED:-}; POP_WIDTH=${POP_WIDTH:-10}; POP_BATCH=${POP_BATCH:-1000}; LX=${LX:-}
  KPROFILE=${KPROFILE:-default}; HEAP=${HEAP:-}; KPROPS=${KPROPS:-}; GROUP_PROTOCOL=${GROUP_PROTOCOL:-classic}; TOPIC_SETTLE=${TOPIC_SETTLE:-}
  RPROPS=${RPROPS:-}; TUNE=${TUNE:-1}; MEMPCT=${MEMPCT:-}; FALLOC=${FALLOC:-}
  PULSAR_TXN_SET=${PULSAR_TXN_SET:-transactionCoordinatorEnabled=true;transactionLogBatchedWriteEnabled=true;transactionPendingAckBatchedWriteEnabled=true}
  BROKER_SET=${BROKER_SET:-}; BOOKIE_SET=${BOOKIE_SET:-}
  QBIN=${QBIN:-/root/qr/queen-b2}; QLANES=${QLANES:-16}
  QEXTRA=${QEXTRA-QUEEN_RAFT_LOG_CACHE_MB=4096 QUEEN_RAFT_TXN_WINDOW_MIN_S=300 QUEEN_QLOG_SHARDS=4}; QAVOID=${QAVOID-2 3}
  WARM=${WARM:-1}; VERIFY=${VERIFY:-1}; VERIFY_IDLE=${VERIFY_IDLE:-}
  PROCS_PER_LOADER=${PROCS_PER_LOADER:-3}; LOADER_HOSTS=${LOADER_HOSTS:-${#L_PUB[@]}}; READY_TIMEOUT=${READY_TIMEOUT:-300}
  START_DELAY=${START_DELAY:-10}; EXIT_GRACE=${EXIT_GRACE:-240}; CREATE_WAIT=${CREATE_WAIT:-4200}; SAMPLE_IV=${SAMPLE_IV:-2}
  GOGC=${GOGC:-400}; GOMEMLIMIT=${GOMEMLIMIT:-}; STOP_AFTER=${STOP_AFTER:-1}; FORCE=${FORCE:-0}
  [ "$LOADER_HOSTS" -ge 1 ] && [ "$LOADER_HOSTS" -le "${#L_PUB[@]}" ] || die "LOADER_HOSTS=$LOADER_HOSTS (hosts.env has ${#L_PUB[@]})"
  NLH=$LOADER_HOSTS; PPL=$PROCS_PER_LOADER; N=$((NLH * PPL)); PR=$((RATE / N))
  OUTP=$PARTITIONS; PARTS_TOTAL=$((PARTITIONS + OUTP))
  if [ "$CONSUMERS" = auto ]; then if [ "$PARTITIONS" -le 200 ]; then C=22; else C=33; fi; else C=$CONSUMERS; fi
  [ -n "$TOPIC_SETTLE" ] || { [ "$PARTS_TOTAL" -ge 50000 ] && TOPIC_SETTLE=30s; }
  # 10 s of quiet ended the verifier's scan of a 10k-partition Pulsar topic with ~571 partitions still unread (10-01:
  # "missing=34320", Pulsar's own readers had all 585,020; the same point with 120 s: PASS, all 595,040 by 20 s)
  [ -n "$VERIFY_IDLE" ] || { if [ "$PARTS_TOTAL" -ge 2000 ]; then VERIFY_IDLE=120s; else VERIFY_IDLE=10s; fi; }
  [ -n "${PREFIX:-}" ] || PREFIX=x$(echo "$TAG" | tr -c 'A-Za-z0-9\n' '-')
  RAMP_S=$(secs "$RAMP") && DUR_S=$(secs "$DURATION") && DRAIN_S=$(secs "$DRAIN") || die "bad RAMP/DURATION/DRAIN"
  BOOT=""; for b in "${B_PRIV[@]}"; do BOOT="$BOOT${BOOT:+,}$b:9092"; done
  case $SYS in
    kafka|redpanda) TOOL=kload ;;
    pulsar) TOOL=pload ;;
    queen) TOOL=qload ;;
  esac
  BIN=$R/bin/txn/$TOOL
  RD=$R/runs/txn/$SYS/$TAG       # on the loader hosts
  OUT=$H/runs/txn/$SYS/$TAG      # here
  durability

  [ -f "$OUT/DONE" ] && { log "already DONE ($OUT/DONE): nothing to do"; exit 0; }
  local ts0 li0; ts0=$(date -u +%Y%m%dT%H%M%SZ)
  if [ -d "$OUT" ]; then mv "$OUT" "$OUT.partial-$ts0"; fi
  # the loaders' run dir of an earlier attempt holds its locks (create.lock, p<i>.lock) and its results (create.rc,
  # create.log, p<i>.rc): kept, this attempt would start nothing and read those back as its own (10-01: the Pulsar
  # 1x10000 rerun "failed" in 6 s on the 17:04 create.log), so it moves aside there too
  for li0 in $(seq 0 $((NLH - 1))); do rx "${L_PUB[$li0]}" "[ -d $RD ] && mv $RD $RD.partial-$ts0; true" & done; wait
  mkdir -p "$OUT/loaders" "$OUT/samples" "$OUT/brokers" "$OUT/ids" || die "cannot create $OUT"
  exec > >(tee -a "$OUT/run.log") 2>&1
  OUT_READY=1
  trap 'on_signal' INT TERM
  T0=$(date +%s)
  log "start: $SYS, rate=$RATE over N=$N processes ($NLH loader hosts x $PPL) = $PR msg/s each; in ${PARTITIONS} x out ${OUTP}" \
      "partitions; txn-size $TXN_SIZE (linger $TXN_LINGER); feeder units of $BATCH; $C workers + $C readers per process" \
      "($((N * C)) each in total); out=$OUT"
  log "durability: $DURABILITY"
  [ $((PR * N)) -eq "$RATE" ] || log "note: $RATE is not divisible by $N; offering $((PR * N)) msg/s"

  # ---- pre-flight ------------------------------------------------------------------------------------------------------
  local h i b
  for i in $(seq 0 $((NLH - 1))); do
    h=${L_PUB[$i]}
    rx "$h" "test -x $BIN" || die "loader ${L_PRIV[$i]}: no executable $BIN (txn/deploy.sh after building mqload)"
    if rx "$h" "pgrep -x kload > /dev/null || pgrep -x pload > /dev/null || pgrep -x qload > /dev/null"; then
      [ "$FORCE" = 1 ] || die "loader ${L_PRIV[$i]}: a load process is already running (FORCE=1 overrides)"
    fi
  done
  TOOL_SHA=$(rx "${L_PUB[0]}" "sha256sum $BIN" | cut -c1-16)
  if [ "$SYS" = queen ]; then
    QBIN_MD5=$(rx "${B_PUB[0]}" "md5sum $QBIN" | cut -c1-32)   # identity check only: a Queen binary is never run with flags here
    [ -n "$QBIN_MD5" ] || die "no $QBIN on the brokers"
  fi
  write_env

  # ---- 1. fresh cluster ------------------------------------------------------------------------------------------------
  log "1. cluster reset"
  cluster_reset || { fail "cluster reset failed (cluster.log)"; return 1; }

  # ---- 2. samplers -----------------------------------------------------------------------------------------------------
  STAG=txn-$SYS-$TAG
  log "2. samplers start ($STAG, every ${SAMPLE_IV}s) on ${#B_PUB[@]} brokers + $NLH loader hosts"
  for h in "${B_PUB[@]}" $(lhosts); do rx "$h" "bash $R/common/sampler.sh start $STAG $SAMPLE_IV" > /dev/null 2>&1 & done; wait
  SAMPLERS=1

  # ---- 3. topics/queues (+ warm) -------------------------------------------------------------------------------------
  log "3. create $PREFIX-in ($PARTITIONS) + $PREFIX-out ($OUTP)$([ "$WARM" = 1 ] && echo ", warm") on loader 1"
  local CF="$(conn_flags) $(layout_flags) -create-only"
  [ "$WARM" = 1 ] && CF="$CF -warm"
  case $SYS in kafka|redpanda) CF="$CF -topic-timeout 1800s$(opt -topic-settle "$TOPIC_SETTLE")";; pulsar) CF="$CF -topic-timeout 1800s";; esac
  CF="$CF -tag $TAG-create -out $RD/create.json $LX"
  mkdir -p "$OUT/loaders/l1"
  runner create "$CF" > "$OUT/loaders/l1/create.sh"
  ship 0 "$OUT/loaders/l1/create.sh" || { fail "cannot ship create.sh"; return 1; }
  rx "${L_PUB[0]}" "cd $RD && mkdir create.lock 2>/dev/null && { rm -f create.rc create.pid create.log; nohup bash create.sh > /dev/null 2>&1 < /dev/null & }; true"
  local deadline=$(( $(date +%s) + CREATE_WAIT )) st
  while :; do
    sleep 3
    st=$(rx "${L_PUB[0]}" "cd $RD; if [ -f create.rc ]; then echo DONE \$(cat create.rc); elif kill -0 \$(cat create.pid 2>/dev/null) 2>/dev/null; then echo RUN; elif [ -f create.pid ]; then echo GONE; else echo WAIT; fi")
    case $st in DONE*|GONE) break;; esac
    [ "$(date +%s)" -lt "$deadline" ] || { rx "${L_PUB[0]}" "kill \$(cat $RD/create.pid) 2>/dev/null"; st="TIMEOUT"; break; }
  done
  rx "${L_PUB[0]}" "cat $RD/create.log" > "$OUT/loaders/l1/create.log" 2>/dev/null
  sed 's/^/    /' "$OUT/loaders/l1/create.log" | grep -vE '^\s+\[config\]' | tail -10
  [ "$st" = "DONE 0" ] || { fail "create: $st (loaders/l1/create.log)"; return 1; }
  T_TOPICS=$(date +%s)
  log "   topics ready after $(( T_TOPICS - T0 ))s since start"
  stats_to "$OUT/stats-before.txt"

  # ---- 4. N load processes behind the start barrier ------------------------------------------------------------------
  log "4. launch $N $TOOL processes"
  local base lf li
  for li in $(seq 0 $((NLH - 1))); do
    lf=$OUT/loaders/l$((li + 1)); mkdir -p "$lf"
    { echo "#!/usr/bin/env bash"; echo "# txn/run.sh $SYS/$TAG: start this host's load processes (idempotent: a lock per process)"
      echo "cd $RD || exit 1"
      echo "mkdir -p ids"
      for i in $(seq $((li * PPL)) $((li * PPL + PPL - 1))); do
        echo "if mkdir p$i.lock 2>/dev/null; then rm -f p$i.start p$i.rc p$i.pid p$i.json p$i.log ids/p$i.ids; nohup bash p$i.sh > /dev/null 2>&1 < /dev/null & fi"
      done
      echo "echo launched $(seq -s ' ' $((li * PPL)) $((li * PPL + PPL - 1))) on \$(hostname)"; } > "$lf/launch.sh"
    for i in $(seq $((li * PPL)) $((li * PPL + PPL - 1))); do
      base="$(conn_flags) $(layout_flags) -create=false -rate $PR -ramp $RAMP -duration $DURATION -report $REPORT"
      base="$base -drain $DRAIN -idle-exit $IDLE_EXIT -start-file $RD/p$i.start -max-inflight $MAX_INFLIGHT -batch $BATCH"
      base="$base -consumers $C -cons-offset $((i * C)) -cons-total $((N * C)) -loader-index $i -loaders $N"
      base="$base -local-src $((li * PPL))-$((li * PPL + PPL - 1)) -out $RD/p$i.json -ids-out $RD/ids/p$i.ids -tag $TAG-p$i"
      base="$base$(opt -seed "$SEED") $LX"
      runner "p$i" "$base" > "$lf/p$i.sh"
    done
    ship "$li" "$lf"/p*.sh "$lf/launch.sh" || { fail "cannot ship the load scripts to ${L_PUB[$li]}"; return 1; }
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
  [ "$SYS" = redpanda ] && leader_settle

  # ---- 5. start instant, snapshots, wait ------------------------------------------------------------------------------
  mark_brokers
  local NOW GO try n bad=0 spids=()
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
  log "5. start instant $GO (loader-1 clock + ${START_DELAY}s) written to $N start files; producing ${DURATION} (ramp $RAMP), drain <= $DRAIN"
  { echo "GO_MS=$GO"; echo "GO_UTC=$(date -u -r $((GO / 1000)) +%FT%TZ 2>/dev/null || date -u -d @$((GO / 1000)) +%FT%TZ)"; } >> "$OUT/run.env"
  local MID=$((START_DELAY + RAMP_S + (DUR_S - RAMP_S) / 2 - 1)) BA=$((START_DELAY + RAMP_S)) BB=$((START_DELAY + DUR_S - 1))
  ( sleep "$MID"; threads_snapshot ) &
  local SNAP=$!
  local BSNAP=""
  if [ "$SYS" = redpanda ]; then ( sleep "$BA"; busy_snap start; sleep $((BB - BA)); busy_snap end ) & BSNAP=$!; fi
  deadline=$(( $(date +%s) + START_DELAY + DUR_S + DRAIN_S + EXIT_GRACE ))
  local ndone lastp=0 TIMEDOUT=0
  while :; do
    sleep 5
    S=$(states); ndone=$(echo "$S" | grep -cE ' (DONE|GONE)')
    [ "$ndone" = "$N" ] && break
    if [ "$(date +%s)" -ge "$deadline" ]; then log "   TIMEOUT: $ndone/$N exited; killing the rest"; killall_loads; TIMEDOUT=1; sleep 5; break; fi
    if [ $(( $(date +%s) - lastp )) -ge 30 ]; then
      lastp=$(date +%s)
      log "   $ndone/$N exited | p0: $(rx "${L_PUB[0]}" "grep -E '^\[[0-9:]{8}\] offered=|^\[txn\] ' $RD/p0.log | tail -2 | cut -c1-150 | tr '\n' ' '")"
    fi
  done
  [ -s "$OUT/threads-mid-n1.txt" ] || kill "$SNAP" 2>/dev/null
  wait "$SNAP" 2>/dev/null
  if [ -n "$BSNAP" ]; then grep -q '^end ' "$OUT/busy.txt" 2>/dev/null || kill "$BSNAP" 2>/dev/null; wait "$BSNAP" 2>/dev/null; fi
  log "   load finished after $(( $(date +%s) - T0 ))s since start"

  # ---- 6. verifier (the cluster is still up) ---------------------------------------------------------------------------
  stop_samplers
  VERDICT="not run"
  if [ "$VERIFY" = 1 ] && [ "$TIMEDOUT" = 0 ]; then
    verify || { collect; summary; fail "verifier: $VERDICT (verify.log)"; return 1; }
  fi

  # ---- 7. collect --------------------------------------------------------------------------------------------------------
  collect
  local rcs
  rcs=$(states); bad=$(echo "$rcs" | grep -vE ' DONE 0$')
  for li in $(seq 0 $((NLH - 1))); do
    for i in $(seq $((li * PPL)) $((li * PPL + PPL - 1))); do
      grep -q '^\[final\]' "$OUT/loaders/l$((li + 1))/p$i.log" 2>/dev/null || bad="$bad
p$i: no [final] line"
      grep -q '^\[txn-final\]' "$OUT/loaders/l$((li + 1))/p$i.log" 2>/dev/null || bad="$bad
p$i: no [txn-final] line"
    done
  done
  summary
  { echo "END_UTC=$(date -u +%FT%TZ)"; echo "ELAPSED_S=$(( $(date +%s) - T0 ))"; echo "PROC_STATES=\"$(echo $rcs)\""; echo "VERDICT=\"$VERDICT\""; } >> "$OUT/run.env"
  if [ "$TIMEDOUT" = 1 ] || [ -n "$(echo "$bad" | tr -d '[:space:]')" ]; then
    echo "$bad" | sed '/^$/d; s/^/    /'
    fail "load processes did not all finish cleanly (timeout=$TIMEDOUT)"; return 1
  fi
  finish
  { date -u +%FT%TZ; cat "$OUT/summary.txt"; } > "$OUT/DONE"
  log "DONE in $(( $(date +%s) - T0 ))s -> $OUT ($VERDICT)"
}

# ---------------------------------------------------------------------------------------------------------------------
# per system

durability() {  # DURABILITY = the full sentence, DUR = the short class report.py prints on every row (no commas)
  case $SYS in
    kafka)
      if [ "$KPROFILE" = fsync ]; then
        DURABILITY="RF 3, min.insync.replicas 2, acks=all (idempotent + transactional producers), fsync of every append (log.flush.interval.messages=1, the parity profile); transaction state log RF 3 / min ISR 2; transactions v2 (KIP-890); read_committed"
        DUR="3 copies / ack all ISR (min 2) / fsync every append"
      else
        DURABILITY="RF 3, min.insync.replicas 2, acks=all (idempotent + transactional producers), NO fsync (page cache, Kafka's production model); transaction state log RF 3 / min ISR 2; transactions v2 (KIP-890); read_committed"
        DUR="3 copies / ack all ISR (min 2) / no fsync"
      fi ;;
    redpanda) DURABILITY="RF 3 raft, acks=all acknowledged after a majority (2 of 3) fsynced (write_caching off), idempotent + transactional producers; tx coordinator topic RF 3 (internal_topic_replication_factor); read_committed"
      DUR="3 copies / ack after 2 fsyncs" ;;
    pulsar) DURABILITY="E3/Qw3/Qa2 with journal fsync (ack after 2 bookie fsyncs) for data, the transaction log, the pending-ack logs and the transaction buffer snapshots; 16 transaction coordinators"
      DUR="3 copies / ack after 2 fsyncs" ;;
    queen) DURABILITY="raft: 3 copies, ack after 2 fsyncs; one transaction = one raft entry (acks + pushes, all-or-nothing), the pop lease as the fence"
      DUR="3 copies / ack after 2 fsyncs" ;;
  esac
}

conn_flags() {
  case $SYS in
    kafka) printf -- '-brokers %s -rf 3 -min-isr 2 -txn-timeout %s -group-protocol %s' "$BOOT" "$TXN_TIMEOUT" "$GROUP_PROTOCOL" ;;
    redpanda) printf -- '-brokers %s -rf 3 -min-isr 0 -group-protocol classic -txn-timeout %s' "$BOOT" "$TXN_TIMEOUT" ;;
    pulsar)
      local u="" ip; for ip in "${B_PRIV[@]}"; do u="${u:+$u,}$ip:6650"; done
      printf -- '-url pulsar://%s -admin http://%s:8080 -tenant bench -namespace ns -bundles 48 -sub sub -sub-type failover -txn-timeout %s' "$u" "${B_PRIV[0]}" "$TXN_TIMEOUT" ;;
    queen)
      local u="" ip; for ip in "${B_PRIV[@]}"; do u="${u:+$u,}http://$ip:6632"; done
      printf -- '-urls %s -pop-width %s -pop-batch %s -lease %s' "$u" "$POP_WIDTH" "$POP_BATCH" "$LEASE" ;;
  esac
}

layout_flags() {
  printf -- '-topic %s -partitions %s -txn -txn-size %s -txn-linger %s -payload %s' "$PREFIX" "$PARTITIONS" "$TXN_SIZE" "$TXN_LINGER" "$PAYLOAD"
}

cluster_reset() {
  case $SYS in
    kafka)
      PARTS=$((PARTS_TOTAL + 100)) HEAP=$HEAP KPROPS=$KPROPS KPROFILE=$KPROFILE CID_FILE=$OUT/cluster.id HOSTS_ENV=$HOSTS_ENV \
        bash "$H/kafka/cluster.sh" reset "$KPROFILE" > "$OUT/cluster.log" 2>&1 || { tail -25 "$OUT/cluster.log"; return 1; }
      grep -E 'healthy in|features:' "$OUT/cluster.log" | sed 's/^/    /'
      echo "CLUSTER_ID=$(cat "$OUT/cluster.id" 2>/dev/null)" >> "$OUT/run.env" ;;
    redpanda)
      PARTS=$PARTS_TOTAL FALLOC=$FALLOC MEMPCT=$MEMPCT RPROPS=$RPROPS TUNE=$TUNE DROP_CACHES=1 PROOF=1 HOSTS_ENV=$HOSTS_ENV \
        bash "$H/redpanda/cluster.sh" reset > "$OUT/cluster.log" 2>&1 || { tail -25 "$OUT/cluster.log"; return 1; }
      grep -E 'healthy in|tuners applied|refused by the kernel|produced:' "$OUT/cluster.log" | sed 's/^/    /'
      rx "${B_PUB[0]}" "rpk cluster config get enable_transactions -X admin.hosts=${B_PRIV[0]}:9644 2>/dev/null; rpk cluster config get internal_topic_replication_factor -X admin.hosts=${B_PRIV[0]}:9644 2>/dev/null; rpk cluster config get transaction_coordinator_partitions -X admin.hosts=${B_PRIV[0]}:9644 2>/dev/null" \
        2>/dev/null | grep -v 'IMDS' | paste -sd' ' - | sed 's/^/    enable_transactions internal_topic_replication_factor transaction_coordinator_partitions = /' | tee -a "$OUT/cluster.log" ;;
    pulsar)
      local bs="$PULSAR_TXN_SET${BROKER_SET:+;$BROKER_SET}"
      ( export BROKER_SET="$bs"; [ -n "$BOOKIE_SET" ] && export BOOKIE_SET; HOSTS_ENV=$HOSTS_ENV bash "$H/pulsar/cluster.sh" reset ) > "$OUT/cluster.log" 2>&1 \
        || { tail -25 "$OUT/cluster.log"; return 1; }
      grep -E 'reset done|brokers healthy|proof' "$OUT/cluster.log" | sed 's/^/    /' | cut -c1-200
      echo "PULSAR_BROKER_SET=\"$bs\"" >> "$OUT/run.env"
      local tc t
      for t in $(seq 1 30); do
        tc=$(rx "${B_PUB[0]}" "curl -s -m 5 http://${B_PRIV[0]}:8080/admin/v2/persistent/pulsar/system/transaction_coordinator_assign/partitions")
        case $tc in *'"partitions"'*) break;; esac; sleep 2
      done
      log "   transaction coordinator assign topic: $tc"
      case $tc in *'"partitions":'[1-9]*) ;; *) log "   no transaction coordinator metadata"; return 1;; esac
      rx "${B_PUB[0]}" "grep -E '^transaction' $R/pulsar/node/broker.conf" > "$OUT/broker-txn.conf" 2>/dev/null
      sed 's/^/    /' "$OUT/broker-txn.conf" | head -20 ;;
    queen)
      local b o L t i round OLD
      for b in "${B_PUB[@]}"; do
        o=$(rx "$b" "ss -ltnH | awk '{print \$4}' | grep -oE ':(9092|9093|2181|3181|6650|8080|9644|33145)\$' | sort -u | tr '\n' ' '")
        [ -z "$(echo $o)" ] || [ "$FORCE" = 1 ] || { log "   $b: another system listens ($o): refusing"; return 1; }
      done
      for b in "${B_PUB[@]}"; do rx "$b" "/root/qr/qc.sh stop; /root/qr/qc.sh wipe; BIN=$QBIN LANES=$QLANES EXTRA='$QEXTRA' /root/qr/qc.sh start" > /dev/null 2>&1 & done; wait
      L=$(queen_leader 90) || { for i in 0 1 2; do rx "${B_PUB[$i]}" "tail -3 /root/qr/b1.log"; done; return 1; }
      sleep 12
      # QAVOID: these nodes (1-based) must not lead; stop a leader that is one of them until another leads (sz9.sh)
      if [ -n "$QAVOID" ]; then
        for round in 1 2 3 4 5 6; do
          L=$(queen_leader 30) || return 1
          case " $QAVOID " in *" $((L + 1)) "*) ;; *) break;; esac
          OLD=$L
          rx "${B_PUB[$OLD]}" "/root/qr/qc.sh stop" > /dev/null 2>&1
          L=$(QSKIP=$OLD queen_leader 60) || return 1
          rx "${B_PUB[$OLD]}" "BIN=$QBIN LANES=$QLANES EXTRA='$QEXTRA' /root/qr/qc.sh start" > /dev/null 2>&1
          sleep 8
        done
      fi
      L=$(queen_leader 30) || return 1
      for i in 0 1 2; do rx "${B_PUB[$i]}" "/root/qr/qc.sh health" | sed "s/^/    n$((i + 1)) /" | cut -c1-200; done | tee "$OUT/cluster.log"
      { echo "QUEEN_LEADER=n$((L + 1))"; echo "QBIN_MD5=$QBIN_MD5"; } >> "$OUT/run.env"
      log "   Queen leader n$((L + 1)) ($QBIN md5 $QBIN_MD5, LANES=$QLANES, EXTRA='$QEXTRA')" ;;
  esac
}

queen_leader() {  # queen_leader <secs>: index (0-based) of the node whose /health says leader (QSKIP = a node to skip)
  local t i
  for t in $(seq 1 "$1"); do
    for i in 0 1 2; do
      [ "${QSKIP:-x}" = "$i" ] && continue
      rx "${B_PUB[$i]}" "/root/qr/qc.sh health" 2>/dev/null | grep -q '"role":"leader' && { echo "$i"; return 0; }
    done
    sleep 1
  done
  return 1
}

cluster_stop() {
  case $SYS in
    kafka) HOSTS_ENV=$HOSTS_ENV bash "$H/kafka/cluster.sh" stop ;;
    redpanda) HOSTS_ENV=$HOSTS_ENV bash "$H/redpanda/cluster.sh" stop ;;
    pulsar) HOSTS_ENV=$HOSTS_ENV bash "$H/pulsar/cluster.sh" stop ;;
    queen) local b; for b in "${B_PUB[@]}"; do rx "$b" "/root/qr/qc.sh stop; ss -ltnH | grep -cE ':(6632|7400) ' | sed 's/^/listening on 6632|7400: /'" & done; wait ;;
  esac
}

mark_brokers() {
  local h
  case $SYS in
    kafka) for h in "${B_PUB[@]}"; do rx "$h" "bash $R/kafka/kc.sh mark" > /dev/null 2>&1 & done; wait ;;
    redpanda) for h in "${B_PUB[@]}"; do rx "$h" "bash $R/redpanda/rc.sh mark" > /dev/null 2>&1 & done; wait ;;
    pulsar) HOSTS_ENV=$HOSTS_ENV bash "$H/pulsar/cluster.sh" mark > /dev/null 2>&1 ;;
    queen) : ;;
  esac
}

threads_snapshot() {
  local b
  for b in $(seq 0 $((${#B_PUB[@]} - 1))); do
    case $SYS in
      kafka) rx "${B_PUB[$b]}" "bash $R/kafka/kc.sh threads 3" ;;
      redpanda) rx "${B_PUB[$b]}" "bash $R/redpanda/rc.sh threads 3" ;;
      pulsar) rx "${B_PUB[$b]}" "bash $R/pulsar/pc.sh threads all 3" ;;
      queen) rx "${B_PUB[$b]}" "bash /root/qr/thr.sh | head -25" ;;
    esac > "$OUT/threads-mid-n$((b + 1)).txt" 2>&1 &
  done
  wait
}

stats_to() {  # stats_to <file>: the system's node stats of every broker
  local b
  for b in $(seq 0 $((${#B_PUB[@]} - 1))); do
    case $SYS in
      kafka) rx "${B_PUB[$b]}" "bash $R/kafka/kc.sh stats" ;;
      redpanda) rx "${B_PUB[$b]}" "bash $R/redpanda/rc.sh stats" ;;
      pulsar) rx "${B_PUB[$b]}" "bash $R/pulsar/pc.sh stats" ;;
      queen) rx "${B_PUB[$b]}" "P=\$(cat /root/qr/pid1 2>/dev/null); echo node=$((b + 1)) pid=\$P rss_MB=\$(( \$(awk '/^VmRSS/{print \$2}' /proc/\$P/status 2>/dev/null || echo 0) / 1024 )) threads=\$(awk '/^Threads/{print \$2}' /proc/\$P/status 2>/dev/null) data_MB=\$(du -sm /root/qr/d1 2>/dev/null | cut -f1) \$(curl -s -m 3 http://${B_PRIV[$b]}:6632/health | cut -c1-200)" ;;
    esac > "$1.n$b" 2>&1 &
  done
  wait
  for b in $(seq 0 $((${#B_PUB[@]} - 1))); do cat "$1.n$b"; rm -f "$1.n$b"; done > "$1"
  sed 's/^/    /' "$1" | cut -c1-220
}

leader_settle() {  # Redpanda's leader balancer moves leaders ~30 s after topics appear: start the window after it went quiet
  local t0 now waited tot quiet line n last nnow q b
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
    if [ "$waited" -ge 40 ] && [ "$quiet" -ge 20 ]; then
      log "   leader balancer quiet for $([ "$quiet" = 999999 ] && echo "ever" || echo "${quiet}s") ($tot transfers so far), $waited s after the topics were ready"
      break
    fi
    if [ "$waited" -ge 300 ]; then log "   WARN leader balancer still moving after 300 s: starting anyway"; break; fi
    sleep 5
  done
  { echo "LEADER_SETTLE_WAITED_S=$(( $(date +%s) - t0 ))"; echo "LEADER_TRANSFERS_BEFORE_WINDOW=$tot"; } >> "$OUT/run.env"
}

busy_snap() {  # busy_snap <label>: the Redpanda shards' busy seconds on every broker -> busy.txt
  local b
  for b in $(seq 0 $((${#B_PUB[@]} - 1))); do rx "${B_PUB[$b]}" "bash $R/redpanda/rc.sh busy" > "$OUT/.busy.$1.$b" 2>&1 & done; wait
  for b in $(seq 0 $((${#B_PUB[@]} - 1))); do echo "$1 $(cat "$OUT/.busy.$1.$b")"; rm -f "$OUT/.busy.$1.$b"; done >> "$OUT/busy.txt"
}

# ---------------------------------------------------------------------------------------------------------------------
# the verifier: the 9 ledgers to loader 1, then `<tool> -verify` there

verify() {
  local li i got=0 rc
  log "6. verifier: collect the $N id ledgers, run $TOOL -verify on loader 1"
  for li in $(seq 0 $((NLH - 1))); do
    for i in $(seq $((li * PPL)) $((li * PPL + PPL - 1))); do
      rx "${L_PUB[$li]}" "cat $RD/ids/p$i.ids" > "$OUT/ids/p$i.ids" 2>/dev/null && [ -s "$OUT/ids/p$i.ids" ] && got=$((got + 1))
    done
  done
  [ "$got" = "$N" ] || { VERDICT="only $got/$N ledgers"; return 1; }
  rx "${L_PUB[0]}" "rm -rf $RD/verify-ids && mkdir -p $RD/verify-ids" || { VERDICT="cannot prepare loader 1"; return 1; }
  for i in $(seq 0 $((N - 1))); do
    shipto "${L_PUB[0]}" "$OUT/ids/p$i.ids" "$RD/verify-ids/p$i.ids" || { VERDICT="cannot ship ledger p$i"; return 1; }
  done
  local VF="$(conn_flags) $(layout_flags) -verify -ids-dir $RD/verify-ids -verify-idle $VERIFY_IDLE -out $RD/verify.json $LX"
  runner verify "$VF" > "$OUT/loaders/l1/verify.sh"
  ship 0 "$OUT/loaders/l1/verify.sh" || { VERDICT="cannot ship verify.sh"; return 1; }
  rx "${L_PUB[0]}" "cd $RD && mkdir verify.lock 2>/dev/null && { rm -f verify.rc verify.pid verify.log; nohup bash verify.sh > /dev/null 2>&1 < /dev/null & }; true"
  local deadline=$(( $(date +%s) + 3600 )) st
  while :; do
    sleep 5
    st=$(rx "${L_PUB[0]}" "cd $RD; if [ -f verify.rc ]; then echo DONE \$(cat verify.rc); elif kill -0 \$(cat verify.pid 2>/dev/null) 2>/dev/null; then echo RUN; elif [ -f verify.pid ]; then echo GONE; else echo WAIT; fi")
    case $st in DONE*|GONE) break;; esac
    [ "$(date +%s)" -lt "$deadline" ] || { rx "${L_PUB[0]}" "kill \$(cat $RD/verify.pid) 2>/dev/null"; st=TIMEOUT; break; }
  done
  rx "${L_PUB[0]}" "cat $RD/verify.log" > "$OUT/verify.log" 2>/dev/null
  rx "${L_PUB[0]}" "cat $RD/verify.json" > "$OUT/verify.json" 2>/dev/null
  sed 's/^/    /' "$OUT/verify.log" | cut -c1-400
  rc=${st#DONE }
  case $rc in
    0) VERDICT="PASS: $(grep -oE 'duplicates=[0-9]+ dup_records=[0-9]+ missing=[0-9]+ pending_in=[0-9]+ in_and_out=[0-9]+ extra=[0-9]+' "$OUT/verify.log" | tail -1)" ;;
    3) VERDICT="FAIL: $(grep -oE 'duplicates=[0-9]+ dup_records=[0-9]+ missing=[0-9]+ pending_in=[0-9]+ in_and_out=[0-9]+ extra=[0-9]+' "$OUT/verify.log" | tail -1)" ;;
    *) VERDICT="verifier error ($st)"; return 1 ;;
  esac
  log "   verifier: $VERDICT"
  return 0
}

# ---------------------------------------------------------------------------------------------------------------------
# loader plumbing (as kafka/run.sh)

lhosts() { local i; for i in $(seq 0 $((NLH - 1))); do echo "${L_PUB[$i]}"; done; }

runner() {  # runner <name> <flags>: the wrapper that runs the tool detached, pid + exit code in files
  cat <<EOF
#!/usr/bin/env bash
# txn/run.sh $SYS/$TAG: $1 (generated $(date -u +%FT%TZ)); pid in $1.pid, exit code in $1.rc
cd $RD || exit 1
ulimit -n 1048576 2>/dev/null || ulimit -n \$(ulimit -Hn)
export GOGC=$GOGC${GOMEMLIMIT:+ GOMEMLIMIT=$GOMEMLIMIT}
$BIN $2 > $1.log 2>&1 < /dev/null &
echo \$! > $1.pid.tmp && mv $1.pid.tmp $1.pid
wait \$!
echo \$? > $1.rc.tmp && mv $1.rc.tmp $1.rc
EOF
}

shipto() {  # shipto <host> <local file> <remote path>: temp name, then mv; retried on a connection failure
  local t rc=255
  for t in 1 2 3 4 5 6; do
    $SSH root@"$1" "mkdir -p $(dirname "$3") && cat > $3.tmp && mv $3.tmp $3" < "$2"; rc=$?
    [ "$rc" != 255 ] && return "$rc"
    sleep $((t * 2))
  done
  return "$rc"
}

ship() {  # ship <loader idx> <files...>: into $RD on that loader
  local li=$1 f; shift
  for f in "$@"; do shipto "${L_PUB[$li]}" "$f" "$RD/$(basename "$f")" || return 1; done
}

states() {  # one line per load process: "p<i> READY|RUN|DONE <rc>|GONE|WAIT"
  local li
  for li in $(seq 0 $((NLH - 1))); do
    rx "${L_PUB[$li]}" "cd $RD 2>/dev/null && for i in $(seq -s ' ' $((li * PPL)) $((li * PPL + PPL - 1))); do
      if [ -f p\$i.rc ]; then echo \"p\$i DONE \$(cat p\$i.rc)\"
      elif [ ! -f p\$i.pid ]; then echo \"p\$i WAIT\"
      elif kill -0 \$(cat p\$i.pid 2>/dev/null) 2>/dev/null; then
        if grep -qE 'READY [0-9]{12,}' p\$i.log 2>/dev/null; then echo \"p\$i READY\"; else echo \"p\$i RUN\"; fi
      else echo \"p\$i GONE\"; fi; done"
  done
}

tails() {  # tails <n>: last n lines of every load process log
  local li
  for li in $(seq 0 $((NLH - 1))); do
    rx "${L_PUB[$li]}" "cd $RD && for f in p*.log; do echo \"--- ${L_PRIV[$li]} \$f\"; tail -$1 \$f; done" 2>/dev/null | sed 's/^/    /' | cut -c1-300
  done
}

killall_loads() {
  local li
  for li in $(seq 0 $((NLH - 1))); do rx "${L_PUB[$li]}" "cd $RD 2>/dev/null && for p in p*.pid create.pid verify.pid; do [ -f \$p ] && kill \$(cat \$p) 2>/dev/null; done; true" & done; wait
}

stop_samplers() {
  [ "${SAMPLERS:-0}" = 1 ] || return 0
  local h
  for h in "${B_PUB[@]}" $(lhosts); do rx "$h" "bash $R/common/sampler.sh stop $STAG" > /dev/null 2>&1 & done; wait
  SAMPLERS=0
}

collect() {
  [ "${COLLECTED:-0}" = 1 ] && return 0
  log "7. collect"
  stop_samplers
  stats_to "$OUT/stats-after.txt"
  local b li d f
  for li in $(seq 0 $((NLH - 1))); do
    mkdir -p "$OUT/loaders/l$((li + 1))"
    rx "${L_PUB[$li]}" "cd $RD && tar cf - --exclude='*.start' --exclude='*.tmp' --exclude='*.lock' --exclude=verify-ids ." | tar xf - -C "$OUT/loaders/l$((li + 1))" 2>/dev/null
    rx "${L_PUB[$li]}" "cat $R/samples/$STAG.txt" > "$OUT/samples/l$((li + 1)).txt" 2>/dev/null
  done
  for b in $(seq 0 $((${#B_PUB[@]} - 1))); do
    d=$OUT/brokers/n$((b + 1)); mkdir -p "$d"
    rx "${B_PUB[$b]}" "cat $R/samples/$STAG.txt" > "$OUT/samples/b$((b + 1)).txt" 2>/dev/null
    case $SYS in
      kafka)
        rx "${B_PUB[$b]}" "cat $R/kafka/conf/server.properties" > "$d/server.properties" 2>/dev/null
        rx "${B_PUB[$b]}" "cat $R/kafka/conf/jvm.env" > "$d/jvm.env" 2>/dev/null
        rx "${B_PUB[$b]}" "cd $R/logs/kafka 2>/dev/null && tar czf - \$(ls -d server.log* controller.log* kafkaServer-gc.log* kafkaServer.out 2>/dev/null)" | tar xzf - -C "$d" 2>/dev/null
        for f in "$d"/server.log* "$d"/controller.log* "$d"/kafkaServer.out; do [ -f "$f" ] && case $f in *.gz) ;; *) gzip -f "$f";; esac; done ;;
      redpanda)
        rx "${B_PUB[$b]}" "cd $R/redpanda && tar cf - conf/redpanda.yaml conf/.bootstrap.yaml conf/overrides.txt conf/io-config.yaml state/tune.txt 2>/dev/null" | tar xf - -C "$d" 2>/dev/null
        [ -f "$d/conf/.bootstrap.yaml" ] && mv "$d/conf/.bootstrap.yaml" "$d/conf/bootstrap.yaml"
        rx "${B_PUB[$b]}" "cd $R/logs/redpanda 2>/dev/null || exit 0; L=redpanda.log; S=\$(stat -c %s \$L 2>/dev/null || echo 0)
          if [ \"\$S\" -gt 268435456 ]; then head -c 67108864 \$L > \$L.head; tail -c 134217728 \$L > \$L.tail; tar czf - \$L.head \$L.tail; rm -f \$L.head \$L.tail; else tar czf - \$L; fi" | tar xzf - -C "$d" 2>/dev/null
        for f in "$d"/redpanda.log "$d"/redpanda.log.head "$d"/redpanda.log.tail; do [ -f "$f" ] && gzip -f "$f"; done ;;
      pulsar)
        rx "${B_PUB[$b]}" "tar czf - -C $R/logs pulsar 2>/dev/null" > "$d/logs.tgz"
        rx "${B_PUB[$b]}" "tar czf - -C $R/pulsar node 2>/dev/null" | tar xzf - -C "$d" 2>/dev/null ;;
      queen)
        rx "${B_PUB[$b]}" "gzip -c /root/qr/b1.log" > "$d/b1.log.gz" 2>/dev/null
        rx "${B_PUB[$b]}" "cat /root/qr/peers.env /root/qr/qc.sh" > "$d/qc.txt" 2>/dev/null
        rx "${B_PUB[$b]}" "curl -s -m 10 http://${B_PRIV[$b]}:6632/metrics/prometheus" > "$d/metrics.txt" 2>/dev/null ;;
    esac
  done
  [ "$SYS" = redpanda ] && rx "${B_PUB[0]}" "curl -s -m 10 'http://${B_PRIV[0]}:9644/v1/cluster_config?include_defaults=true'" > "$OUT/cluster-config.json" 2>/dev/null
  COLLECTED=1
  log "   collected $(du -sh "$OUT" | cut -f1) into $OUT"
}

summary() {  # per-process [final] + [txn-final] numbers, totals, the verdict, Redpanda's shards busy -> summary.txt
  python3 - "$OUT" "$N" "$SYS" "$DURABILITY" "${VERDICT:-not run}" > "$OUT/summary.txt" <<'PY'
import glob, os, re, sys
out, n, system, dur, verdict = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5]
F = re.compile(r'^\[final\].*?shed=(\d+) \(msgs: offered=(\d+) achieved=(\d+) shed=(\d+)\) pushErr=(\d+) \| pushed=(\d+) popped=(\d+) lag=(-?\d+) \| popErr=(\d+).*?overall p50=([\d.]+) p99=([\d.]+) p999=([\d.]+).*?ackErr=(\d+).*?e2e p50=([\d.]+) p99=([\d.]+) p999=([\d.]+)')
TF = re.compile(r'^\[txn-final\] txns=(\d+) msgs=(\d+) aborts=(\d+) errs=(\d+) in=(\d+) avg=([\d.]+) msg/txn \| commit p50=([\d.]+) p99=([\d.]+) p999=([\d.]+) max=([\d.]+) ms')
tot = dict(off=0, ach=0, shed=0, pushed=0, popped=0, errs=0, txns=0, tmsgs=0, aborts=0, terrs=0); e99 = []; p99 = []; c99 = []; seen = 0
for p in sorted(glob.glob(os.path.join(out, "loaders", "l*", "p*.log")), key=lambda x: int(re.search(r'p(\d+)\.log$', x)[1])):
    name = os.path.basename(p)[:-4]
    txt = open(p, errors="replace").read()
    m = t = None
    for l in txt.splitlines():
        mm = F.match(l); tt = TF.match(l)
        if mm: m = mm
        if tt: t = tt
    cpu = re.findall(r'^load_cpu=([\d.]+)%', txt, re.M)
    if not m:
        print(f"{name}: NO FINAL LINE"); continue
    seen += 1
    g = m.groups()
    tot["off"] += int(g[1]); tot["ach"] += int(g[2]); tot["shed"] += int(g[3]); tot["pushed"] += int(g[5]); tot["popped"] += int(g[6])
    tot["errs"] += int(g[4]) + int(g[8]) + int(g[12]); e99.append(float(g[14])); p99.append(float(g[10]))
    tx = ""
    if t:
        h = t.groups()
        tot["txns"] += int(h[0]); tot["tmsgs"] += int(h[1]); tot["aborts"] += int(h[2]); tot["terrs"] += int(h[3]); c99.append(float(h[7]))
        tx = f" | txns={h[0]} msgs={h[1]} aborts={h[2]} errs={h[3]} avg={h[5]} commit p50/p99/p999={h[6]}/{h[7]}/{h[8]} ms"
    print(f"{name}: in offered={g[1]} achieved={g[2]} shed={g[3]} | out consumed={g[6]} errs push={g[4]} pop={g[8]} ack={g[12]}"
          f" | produce p99={g[10]} ms | e2e p50/p99/p999={g[13]}/{g[14]}/{g[15]} ms{tx} | cpu={cpu[-1] if cpu else '?'}%")
avg = tot["tmsgs"] / tot["txns"] if tot["txns"] else 0
print(f"total: {seen}/{n} processes with a final line | input msgs offered={tot['off']} achieved={tot['ach']} shed={tot['shed']}"
      f" | output consumed={tot['popped']} | transactions={tot['txns']} msgs={tot['tmsgs']} avg={avg:.2f} msg/txn aborts={tot['aborts']} txnErrors={tot['terrs']}"
      f" | errors={tot['errs']} | worst produce p99={max(p99) if p99 else -1} ms | worst e2e p99={max(e99) if e99 else -1} ms"
      f" | worst commit p99={max(c99) if c99 else -1} ms")
print(f"verifier: {verdict}")
print(f"durability ({system}): {dur}")
snap = {}
bf = os.path.join(out, "busy.txt")
for l in (open(bf) if os.path.exists(bf) else []):
    m = re.match(r'^(start|end) node=(\d+) t=([\d.]+) busy_s=([\d.]+|NA)', l)
    if m: snap.setdefault(m[2], {})[m[1]] = (float(m[3]), None if m[4] == "NA" else float(m[4]))
parts, tb = [], 0.0
for node in sorted(snap):
    s = snap[node]
    if "start" in s and "end" in s and s["end"][0] > s["start"][0] and s["end"][1] is not None and s["start"][1] is not None:
        c = (s["end"][1] - s["start"][1]) / (s["end"][0] - s["start"][0]); tb += c; parts.append(f"n{node}={c:.2f}")
if parts:
    print(f"shards busy (redpanda_cpu_busy_seconds_total over the steady window): {' '.join(parts)} sum={tb:.2f} cores")
PY
  sed 's/^/    /' "$OUT/summary.txt" | cut -c1-400
}

write_env() {  # every parameter, KEY=VALUE (report.py reads SYSTEM RATE SHAPE TXN ...)
  local k
  { echo "# txn/run.sh $SYS/$TAG, written $(date -u +%FT%TZ); every parameter of this point"
    echo "SYSTEM=$SYS"; echo "TAG=$TAG"; echo "RATE=$RATE"; echo "TXN=1"
    echo "SHAPE=\"in+out 1x$PARTITIONS txn=$TXN_SIZE b=$BATCH w=${C}x$N\""
    echo "DURABILITY=\"$DURABILITY\""; echo "DUR=\"$DUR\""
    for k in $KEYS; do eval "echo \"$k=\\\"\${$k:-}\\\"\""; done
    echo "N_PROCS=$N"; echo "RATE_PER_PROC=$PR"; echo "WORKERS_PER_PROC=$C"; echo "WORKERS_TOTAL=$((N * C))"; echo "READERS_TOTAL=$((N * C))"
    echo "PARTS_TOTAL=$PARTS_TOTAL"; echo "TOOL=$TOOL"; echo "TOOL_SHA256_16=$TOOL_SHA"
    echo "RAMP_S=$RAMP_S"; echo "DURATION_S=$DUR_S"; echo "B_PRIV=\"${B_PRIV[*]}\""
    echo "L_PRIV_USED=\"$(for i in $(seq 0 $((NLH - 1))); do printf '%s ' "${L_PRIV[$i]}"; done)\""
    echo "PROFILE=${PROFILE:-vm}"; echo "REMOTE_ROOT=$R"; echo "START_UTC=$(date -u +%FT%TZ)"
    case $SYS in
      kafka) echo "VERSION=kafka 4.3.1" ;; redpanda) echo "VERSION=redpanda 26.2.3" ;; pulsar) echo "VERSION=pulsar 4.2.4" ;;
      queen) echo "VERSION=queen 2.0 beta ($QBIN, md5 $QBIN_MD5)" ;;
    esac
  } > "$OUT/run.env"
}

finish() {  # end of a point, good or bad: the cluster is stopped (only one system may listen at a time)
  if [ "$STOP_AFTER" = 1 ]; then
    log "   stopping the cluster (data kept until the next reset)"
    cluster_stop > "$OUT/cluster-stop.log" 2>&1
  fi
}

fail() {
  log "FAILED: $*"
  [ "${LAUNCHED:-0}" = 1 ] && killall_loads
  if [ "${COLLECTED:-0}" != 1 ] && [ "${SAMPLERS:-0}" = 1 -o "${LAUNCHED:-0}" = 1 ]; then collect; fi
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
