#!/usr/bin/env bash
# cluster.sh (Mac) <cmd> — the 3 Kafka brokers of hosts.env, driven over $SSH (ssh on the droplets, common/dexec.sh in the
# local rehearsal). Every action is logged with a UTC timestamp.
#   reset [default|fsync]  stop all 3 (STOP_WAIT 15 s then SIGKILL: the data is wiped anyway), wipe, format all 3 with ONE
#                          fresh cluster id, start all 3, wait healthy = 3 voters + 3 unfenced brokers seen from every node
#                          (HEALTH_TIMEOUT, default 240 s), then print versions, feature levels and the effective settings
#   health                 kc.sh health on each node          stop     kc.sh stop on each node (clean, STOP_WAIT 60 s)
#   start                  kc.sh start on each node (no format)  stats  kc.sh stats on each node
#   versions               versions + running JVM flags + feature levels + effective settings
# Env: HOSTS_ENV (default <harness>/hosts.env), KPROFILE (default|fsync; reset's argument wins), PARTS (total partitions
# of the coming run: heap 6g <= 10k, else 10g), HEAP (explicit heap), KPROPS (opt-in broker props k=v,k=v, not SPEC),
# JVM_EXTRA (opt-in JVM flags, not SPEC).
set -u
H=$(cd "$(dirname "$0")/.." && pwd)
HOSTS_ENV=${HOSTS_ENV:-$H/hosts.env}
[ -f "$HOSTS_ENV" ] || { echo "no $HOSTS_ENV (copy hosts.env.example, or set HOSTS_ENV)"; exit 2; }
. "$HOSTS_ENV"
R=${REMOTE_ROOT:-/root/bench}
KC="bash $R/kafka/kc.sh"      # via bash: a copy that lost its exec bit (scp) still runs
NB=${#B_PUB[@]}
log() { echo "[$(date -u +%FT%TZ)] cluster: $*"; }
rx() { local h=$1; shift; $SSH root@"$h" "$*"; }
TMPD=$(mktemp -d "${TMPDIR:-/tmp}/kcluster.XXXXXX"); trap 'rm -rf "$TMPD"' EXIT
each() {  # each <label> <remote command>: on all brokers in parallel; output in $TMPD/<label>.<i>, rc in .rc<i>
  local label=$1; shift; local cmd="$*" i
  for i in $(seq 0 $((NB - 1))); do
    ( rx "${B_PUB[$i]}" "$cmd" > "$TMPD/$label.$i" 2>&1; echo $? > "$TMPD/$label.rc$i" ) &
  done
  wait
}
show() {  # show <label>: every node's output, indented
  local i; for i in $(seq 0 $((NB - 1))); do sed "s/^/  n$((i + 1)) /" "$TMPD/$1.$i"; done
}
allok() { # allok <label> [grep pattern]: every rc 0 (and every output matches the pattern)
  local i; for i in $(seq 0 $((NB - 1))); do
    [ "$(cat "$TMPD/$1.rc$i")" = 0 ] || return 1
    [ -z "${2:-}" ] || grep -qE "$2" "$TMPD/$1.$i" || return 1
  done
}
newcid() {  # 16 random bytes, base64url, no padding: 22 chars, like Kafka's Uuid; never starting with '-' (flag!)
  local c
  while :; do
    c=$(head -c 16 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n')
    [ "${#c}" = 22 ] && [ "${c#-}" = "$c" ] && { echo "$c"; return; }
  done
}
healthy() { # 3 voters + 3 unfenced brokers + a leader, as seen by EVERY node
  each health "$KC health"
  local i; for i in $(seq 0 $((NB - 1))); do
    grep -qE "alive=1 leader=[0-9]+ voters=$NB unfenced=$NB fenced=0" "$TMPD/health.$i" || return 1
  done
}
versions() {
  each version "$KC version"; log "versions (conf_hash must be equal: nodes differ only in node.id + listener IPs)"; show version
  local h; h=$(for i in $(seq 0 $((NB - 1))); do grep -oE 'conf_hash=[0-9a-f]+' "$TMPD/version.$i"; done | sort -u | wc -l | tr -d ' ')
  [ "$h" = 1 ] || log "WARN the rendered configs differ beyond node.id/listeners ($h distinct hashes)"
  rx "${B_PUB[0]}" "$KC features" > "$TMPD/features" 2>&1; log "features: $(cat "$TMPD/features")"
  log "effective settings (node 1; secrets none):"
  rx "${B_PUB[0]}" "$KC conf" | sed 's/^/  /'
}

main() {
  local cmd=${1:-}; [ $# -gt 0 ] && shift
  case $cmd in
  reset)
    local KP=${1:-${KPROFILE:-default}} T0 CID deadline
    case $KP in default|fsync) ;; *) echo "usage: cluster.sh reset [default|fsync]"; return 2;; esac
    T0=$(date +%s)
    log "reset: profile=$KP PROFILE=${PROFILE:-vm} PARTS=${PARTS:-unset} HEAP=${HEAP:-by-rule} KPROPS=${KPROPS:-none} JVM_EXTRA=${JVM_EXTRA:-none} brokers=${B_PRIV[*]}"
    each stop "STOP_WAIT=${STOP_WAIT:-15} $KC stop && $KC wipe"
    show stop; allok stop || { log "stop/wipe failed"; return 1; }
    CID=$(newcid)
    log "format all $NB with cluster id $CID"
    each format "PARTS='${PARTS:-}' HEAP='${HEAP:-}' KPROPS='${KPROPS:-}' JVM_EXTRA='${JVM_EXTRA:-}' $KC format $CID $KP"
    show format | grep -E 'mkconf|Formatting|rror|xception|usage' ; allok format '^Formatting' || { log "format failed:"; show format | tail -20; return 1; }
    log "start all $NB"
    each start "$KC start"
    show start; allok start 'started pid' || { log "start failed"; each stop "$KC stop"; return 1; }
    deadline=$(( $(date +%s) + ${HEALTH_TIMEOUT:-240} ))
    until healthy; do
      if [ "$(date +%s)" -ge "$deadline" ]; then
        log "NOT healthy after ${HEALTH_TIMEOUT:-240}s:"; show health
        for i in $(seq 0 $((NB - 1))); do echo "  --- n$((i + 1)) server.log tail"; rx "${B_PUB[$i]}" "tail -8 $R/logs/kafka/server.log" | sed 's/^/  /'; done
        return 1
      fi
      sleep 2
    done
    log "healthy in $(( $(date +%s) - T0 ))s: cluster id $CID"; show health
    echo "$CID" > "$TMPD/cid"; [ -n "${CID_FILE:-}" ] && echo "$CID" > "$CID_FILE"
    versions ;;
  health) if healthy; then show health; log "HEALTHY"; else show health; log "NOT healthy"; return 1; fi ;;
  stop)   log "stop all $NB"; each stop "$KC stop"; show stop ;;
  start)  log "start all $NB"; each start "$KC start"; show start; allok start 'started pid' ;;
  stats)  each stats "$KC stats"; show stats ;;
  versions) versions ;;
  *) awk 'NR > 1 && /^#/ {print; next} NR > 1 {exit}' "$0"; return 2 ;;
  esac
}
main "$@"; exit $?
