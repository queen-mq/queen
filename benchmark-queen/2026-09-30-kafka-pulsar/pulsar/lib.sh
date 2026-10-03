# lib.sh — sourced by every pulsar/ script, on the Mac (bash 3.2: no associative arrays, no ${x,,}) and on the nodes.
# Loads hosts.env (HOSTS_ENV overrides the path, e.g. pulsar/rehearsal/hosts.env) and sets:
#   PDIR  this directory          H    harness root (the Mac's checkout, or $REMOTE_ROOT on a node)
#   R     $REMOTE_ROOT            NB   number of brokers (B_PRIV)          NL  number of loaders (L_PRIV)
#   ZKS   ip1:2181,ip2:2181,ip3:2181 (private IPs)
PDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
H=$(cd "$PDIR/.." && pwd)
HOSTS_ENV=${HOSTS_ENV:-$H/hosts.env}
_PROFILE_ENV=${PROFILE-}   # an explicit PROFILE (knob, env) wins over the one in hosts.env
if [ -f "$HOSTS_ENV" ]; then . "$HOSTS_ENV"; [ -n "$_PROFILE_ENV" ] && PROFILE=$_PROFILE_ENV
else echo "no $HOSTS_ENV (cp hosts.env.example hosts.env, or set HOSTS_ENV=)" >&2; exit 2; fi
R=${REMOTE_ROOT:-/root/bench}
PROFILE=${PROFILE:-vm}
NB=${#B_PRIV[@]}
NL=${#L_PRIV[@]}
ZKS=""; for _ip in "${B_PRIV[@]}"; do ZKS="${ZKS:+$ZKS,}$_ip:2181"; done; unset _ip
PULSAR_VERSION=${PULSAR_VERSION:-4.2.4}

ts() { date -u +%FT%TZ; }
log() { echo "[$(ts)] $*"; }
die() { echo "[$(ts)] ERROR: $*" >&2; exit 1; }
usage() { awk 'NR == 1 { next } /^#/ { print; next } { exit }' "$0"; }   # the calling script's header comment

# rsh <host> <command...>: run on a host through $SSH (ssh, or common/dexec.sh in the rehearsal); stdin never read
rsh() { local h=$1; shift; $SSH "root@$h" "$*" < /dev/null; }

# par <outdir> <label> <command> <host...>: run <command> on every host in parallel. Output of host i goes to
# <outdir>/<label>.<i> (i = 1-based position in the list), exit code to <outdir>/<label>.<i>.rc. Returns the number of
# hosts that failed. Waits only for its own jobs (callers may have other background jobs).
par() {
  local out=$1 label=$2 cmd=$3 i=0 h pids="" fails=0
  shift 3
  for h in "$@"; do
    i=$((i + 1))
    ( rsh "$h" "$cmd" > "$out/$label.$i" 2>&1; echo $? > "$out/$label.$i.rc" ) &
    pids="$pids $!"
  done
  wait $pids
  i=0
  for h in "$@"; do i=$((i + 1)); [ "$(cat "$out/$label.$i.rc" 2>/dev/null)" = 0 ] || fails=$((fails + 1)); done
  return $fails
}

# show <outdir> <label> <n>: print the par outputs, each line prefixed with the node number
show() { local i; for i in $(seq 1 "$3"); do sed "s/^/  n$i  /" "$1/$2.$i"; done; }

# knob_env: the cluster knobs that are set in this shell, as a quoted "K=V ..." prefix for a remote command
KNOBS="PROFILE JOURNALS ZK_SET BOOKIE_SET BROKER_SET MEM_ZK MEM_BOOKIE MEM_BROKER"
knob_env() {
  local k v s=""
  for k in $KNOBS; do eval "v=\${$k-}"; [ -n "$v" ] && s="$s $k=$(printf %q "$v")"; done
  echo "$s"
}
