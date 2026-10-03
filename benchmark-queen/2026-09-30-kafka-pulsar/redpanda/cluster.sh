#!/usr/bin/env bash
# cluster.sh (Mac) <cmd> — the 3 Redpanda brokers of hosts.env, driven over $SSH (bash 3.2-clean). Every action is logged
# with a UTC timestamp.
#   reset           stop all 3 (STOP_WAIT 15 s then SIGKILL: the data is wiped anyway), wipe, render the config (PARTS
#                   FALLOC RPROPS START_FLAGS), apply the production tuners (TUNE=1; rc.sh tune records before -> after),
#                   rpk iotune where no io-config exists yet (once per node, ~10 min), drop the page cache (DROP_CACHES=1:
#                   Redpanda does O_DIRECT I/O and takes ~29 GiB itself), start all 3, wait healthy (every
#                   node ready + healthy=true, 3 nodes, 0 down, 0 leaderless, 0 under-replicated; HEALTH_TIMEOUT 240 s),
#                   check that every .bootstrap.yaml property is in force, then print brokers, versions, the tuner summary
#                   and the durability proof (PROOF=0 skips it)
#   proof           replication + durability proof on topic rp-proof (RF 3, 1 partition): 10000 records with acks=all,
#                   then every replica's own raft view (committed / flushed / dirty offsets) and its on-disk segment;
#                   write caching off; the topic is deleted again
#   health          rc.sh health on each node            stop      rc.sh stop on each node (clean, STOP_WAIT 60 s)
#   start           rc.sh start on each node (no wipe)   stats     rc.sh stats on each node
#   versions        versions + running command lines + rendered-config hash per node, cluster config overrides
#   tune | untune   rc.sh tune / untune on each node (untune = back to the SPEC os-tune state for the other systems)
#   iotune [dur]    rc.sh iotune on all 3 in parallel (default 10m)          check     rpk redpanda check on each node
#   ports           Redpanda's and the other systems' ports bound on each node
# Env: HOSTS_ENV (default <harness>/hosts.env), PARTS (total partitions of the coming run: segment_fallocation_step),
# FALLOC / MEMPCT (override the two per-point rules), RPROPS (opt-in cluster props k=v,k=v, not the expert set), START_FLAGS (opt-in Seastar flags), TUNE (1),
# HEALTH_TIMEOUT (240), IOTUNE_DURATION (10m), PROOF (1).
set -u
H=$(cd "$(dirname "$0")/.." && pwd)
HOSTS_ENV=${HOSTS_ENV:-$H/hosts.env}
[ -f "$HOSTS_ENV" ] || { echo "no $HOSTS_ENV (copy hosts.env.example, or set HOSTS_ENV)"; exit 2; }
. "$HOSTS_ENV"
R=${REMOTE_ROOT:-/root/bench}
RC="bash $R/redpanda/rc.sh"      # via bash: a copy that lost its exec bit still runs
NB=${#B_PUB[@]}
log() { echo "[$(date -u +%FT%TZ)] cluster: $*"; }
rx() { local h=$1; shift; $SSH root@"$h" "$*" < /dev/null; }
TMPD=$(mktemp -d "${TMPDIR:-/tmp}/rpcluster.XXXXXX"); trap 'rm -rf "$TMPD"' EXIT
each() {  # each <label> <remote command>: on all brokers in parallel; output in $TMPD/<label>.<i>, rc in .rc<i>
  local label=$1; shift; local cmd="$*" i
  for i in $(seq 0 $((NB - 1))); do
    ( rx "${B_PUB[$i]}" "$cmd" > "$TMPD/$label.$i" 2>&1; echo $? > "$TMPD/$label.rc$i" ) &
  done
  wait
}
show() { local i; for i in $(seq 0 $((NB - 1))); do sed "s/^/  n$((i + 1)) /" "$TMPD/$1.$i"; done; }
allok() { # allok <label> [grep pattern]: every rc 0 (and every output matches the pattern)
  local i; for i in $(seq 0 $((NB - 1))); do
    [ "$(cat "$TMPD/$1.rc$i")" = 0 ] || return 1
    [ -z "${2:-}" ] || grep -qE "$2" "$TMPD/$1.$i" || return 1
  done
}
knobs() { printf "PARTS='%s' FALLOC='%s' MEMPCT='%s' RPROPS='%s' START_FLAGS='%s'" "${PARTS:-}" "${FALLOC:-}" "${MEMPCT:-}" "${RPROPS:-}" "${START_FLAGS:-}"; }
healthy() { # every node: alive, ready, healthy, all NB nodes, none down, no leaderless / under-replicated partitions
  each health "$RC health"
  local i; for i in $(seq 0 $((NB - 1))); do
    grep -qE "alive=1 ready=ready healthy=true controller=[0-9]+ nodes=$NB down=0 leaderless=0 under_replicated=0" "$TMPD/health.$i" || return 1
  done
}
rpk1() { rx "${B_PUB[0]}" "rpk $* -X brokers=${B_PRIV[0]}:9092 -X admin.hosts=${B_PRIV[0]}:9644 2>&1 | grep -v 'ec2imds\|IMDSv1'"; }
props_check() { # every property of node 1's .bootstrap.yaml as the cluster reports it
  rx "${B_PUB[0]}" "grep -vE '^\s*#|^\s*\$' $R/redpanda/conf/.bootstrap.yaml" > "$TMPD/boot" 2>/dev/null
  local k v got bad=0
  while IFS=': ' read -r k v; do
    [ -n "$k" ] || continue
    got=$(rpk1 cluster config get "$k" | tail -1 | sed -e 's/^"//' -e 's/"$//')   # string properties come back quoted
    if [ "$got" = "$v" ]; then echo "  $k = $got"; else echo "  $k = $got   <-- WANTED $v"; bad=$((bad + 1)); fi
  done < "$TMPD/boot"
  [ "$bad" = 0 ] || { log "ERROR $bad cluster properties are not what .bootstrap.yaml asked for"; return 1; }
}
versions() {
  local ok=0
  each version "$RC version"; log "versions (conf_hash must be equal: nodes differ only in node_id + listener IPs)"; show version
  local h; h=$(for i in $(seq 0 $((NB - 1))); do grep -oE 'conf_hash=[0-9a-f]+' "$TMPD/version.$i"; done | sort -u | wc -l | tr -d ' ')
  [ "$h" = 1 ] || { log "ERROR the rendered configs differ beyond node_id/listeners ($h distinct hashes)"; ok=1; }
  log "brokers:"; rpk1 cluster info | sed 's/^/  /'
  log "cluster config overrides in force (.bootstrap.yaml):"; props_check || ok=1
  log "cluster config status (restart pending must be false):"; rpk1 cluster config status | sed 's/^/  /'
  return $ok
}
proof() {
  local T=rp-proof B="${B_PRIV[0]}:9092,${B_PRIV[1]}:9092,${B_PRIV[2]}:9092" i
  log "durability proof on topic $T: RF 3, 1 partition, 10000 records acks=all (rpk produce), then each replica's own raft view"
  rx "${B_PUB[0]}" "rpk topic delete $T -X brokers=$B > /dev/null 2>&1; rpk topic create $T -p 1 -r 3 -X brokers=$B 2>&1 | grep -v 'ec2imds\|IMDSv1'" | sed 's/^/  /'
  rx "${B_PUB[0]}" "seq -f 'proof-%06g' 1 10000 | rpk topic produce $T --acks -1 -X brokers=$B 2>&1 | grep -v 'ec2imds\|IMDSv1' | tail -1" | sed 's/^/  produced: /'
  rx "${B_PUB[0]}" "rpk topic describe $T -a -X brokers=$B 2>&1 | grep -v 'ec2imds\|IMDSv1' | grep -E 'NAME|PARTITIONS|REPLICAS|^(write.caching|flush.ms|flush.bytes|replication.factor|min.insync.replicas|cleanup.policy|retention.ms|segment.bytes) |PARTITION|^0 '" | sed 's/^/  /'
  sleep 1
  log "raft state of every replica (admin API /v1/debug/partition, each replica's own report; flushed = fsynced):"
  rx "${B_PUB[0]}" "curl -s -m 5 http://${B_PRIV[0]}:9644/v1/debug/partition/kafka/$T/0 | python3 -c '
import json, sys
d = json.load(sys.stdin)
for r in d.get(\"replicas\", []):
    rs = r.get(\"raft_state\", {})
    if not rs: continue
    keep = {k: rs[k] for k in (\"node_id\", \"term\", \"is_leader\", \"commit_index\", \"flushed_offset\", \"has_pending_flushes\") if k in rs}
    print(\"replica\", keep)
    for f in rs.get(\"followers\", []) or []:
        print(\"   leader sees follower\", {k: f[k] for k in (\"id\", \"last_flushed_log_index\", \"last_dirty_log_index\", \"match_index\", \"is_learner\") if k in f})
' 2>&1 | head -8" | sed 's/^/  /'
  for i in $(seq 0 $((NB - 1))); do
    rx "${B_PUB[$i]}" "du -b $R/data/redpanda/kafka/$T/0_*/ 2>/dev/null | tail -1 | awk '{print \"on disk:\", \$1, \"bytes in\", \$2}'" | sed "s/^/  n$((i + 1)) /"
  done
  log "write_caching_default=$(rpk1 cluster config get write_caching_default | tail -1 | tr -d '"') (false: acks=all is answered once a raft majority has fsynced)"
  rx "${B_PUB[0]}" "rpk topic delete $T -X brokers=$B 2>&1 | grep -v 'ec2imds\|IMDSv1'" | sed 's/^/  /'
}

main() {
  local cmd=${1:-}; [ $# -gt 0 ] && shift
  case $cmd in
  reset)
    local T0 deadline i miss
    T0=$(date +%s)
    log "reset: PROFILE=${PROFILE:-vm} PARTS=${PARTS:-unset} FALLOC=${FALLOC:-by-rule} MEMPCT=${MEMPCT:-by-rule} RPROPS=${RPROPS:-none} START_FLAGS=${START_FLAGS:-none} TUNE=${TUNE:-1} brokers=${B_PRIV[*]}"
    each stop "STOP_WAIT=${STOP_WAIT:-15} $RC stop && $RC wipe"
    show stop; allok stop || { log "stop/wipe failed"; return 1; }
    each render "$(knobs) $RC render"
    show render; allok render '^mkconf: node' || { log "render failed"; return 1; }
    if [ "${TUNE:-1}" = 1 ]; then
      each tune "$RC tune"
      show tune; allok tune || { log "tune failed (state/tune.txt on the nodes)"; return 1; }
    else log "TUNE=0: Redpanda's tuners NOT applied (the OS stays as common/os-tune.sh left it)"; fi
    miss=""; for i in $(seq 0 $((NB - 1))); do grep -q 'io-config=yes' "$TMPD/render.$i" || miss="$miss $i"; done
    if [ -n "$miss" ]; then
      log "no io-config.yaml on node(s)$(for i in $miss; do printf ' n%d' $((i + 1)); done): rpk iotune ${IOTUNE_DURATION:-10m} (once per node)"
      for i in $miss; do ( rx "${B_PUB[$i]}" "$RC iotune ${IOTUNE_DURATION:-10m} && $(knobs) $RC render" > "$TMPD/iotune.$i" 2>&1; echo $? > "$TMPD/iotune.rc$i" ) & done; wait
      for i in $miss; do sed "s/^/  n$((i + 1)) /" "$TMPD/iotune.$i"; [ "$(cat "$TMPD/iotune.rc$i")" = 0 ] || { log "iotune failed on n$((i + 1))"; return 1; }; done
    fi
    if [ "${DROP_CACHES:-1}" = 1 ]; then   # Redpanda does O_DIRECT I/O and wants its ~29 GiB: no page cache to reclaim mid-run
      each drop "a=\$(awk '/^MemFree/{print int(\$2/1048576)}' /proc/meminfo); sync; echo 3 > /proc/sys/vm/drop_caches; echo \"page cache dropped: MemFree \${a} -> \$(awk '/^MemFree/{print int(\$2/1048576)}' /proc/meminfo) GiB\""
      show drop
    fi
    log "start all $NB"
    each start "$RC start"
    show start; allok start 'started pid' || { log "start failed"; each stop "$RC stop"; return 1; }
    deadline=$(( $(date +%s) + ${HEALTH_TIMEOUT:-240} ))
    until healthy; do
      if [ "$(date +%s)" -ge "$deadline" ]; then
        log "NOT healthy after ${HEALTH_TIMEOUT:-240}s:"; show health
        for i in $(seq 0 $((NB - 1))); do echo "  --- n$((i + 1)) redpanda.log tail"; rx "${B_PUB[$i]}" "grep -E '^(ERROR|WARN)' $R/logs/redpanda/redpanda.log | tail -8" | sed 's/^/  /'; done
        return 1
      fi
      sleep 2
    done
    log "healthy in $(( $(date +%s) - T0 ))s"; show health
    versions || return 1
    log "tuners (node 1; all nodes: state/tune.txt, collected per run):"
    rx "${B_PUB[0]}" "cat $R/redpanda/state/tune.txt 2>/dev/null" | sed 's/^/  /'
    [ "${PROOF:-1}" = 1 ] && proof
    return 0 ;;
  proof)  proof ;;
  health) if healthy; then show health; log "HEALTHY"; else show health; log "NOT healthy"; return 1; fi ;;
  stop)   log "stop all $NB"; each stop "$RC stop"; show stop ;;
  start)  log "start all $NB"; each start "$RC start"; show start; allok start 'started pid' ;;
  stats)  each stats "$RC stats"; show stats ;;
  versions) versions ;;
  tune)   each tune "$RC tune"; show tune; allok tune ;;
  untune) log "restore the pre-Redpanda OS state on all $NB (Redpanda must be stopped)"; each untune "$RC untune"; show untune; allok untune ;;
  iotune) log "rpk iotune ${1:-10m} on all $NB in parallel"; each iotune "$RC iotune ${1:-10m}"; show iotune; allok iotune ;;
  check)  each check "$RC check"; show check ;;
  ports)  each ports "$RC ports"; show ports ;;
  *) awk 'NR > 1 && /^#/ {print; next} NR > 1 {exit}' "$0"; return 2 ;;
  esac
}
main "$@"; exit $?
