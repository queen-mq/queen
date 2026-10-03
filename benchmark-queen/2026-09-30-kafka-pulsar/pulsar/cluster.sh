#!/usr/bin/env bash
# cluster.sh (Mac) — the 3-node Pulsar 4.2.4 cluster over $SSH: every broker box runs ZooKeeper + bookie + broker.
#   cluster.sh reset [KEY=VAL...]  stop all, wipe, render configs (knobs: PROFILE JOURNALS ZK_SET BOOKIE_SET BROKER_SET
#                                  MEM_ZK MEM_BOOKIE MEM_BROKER), ZK x3 (1 leader + 2 synced followers),
#                                  initialize-cluster-metadata once, bookies x3 (3 writable), brokers x3 (all healthy),
#                                  tenant bench + namespace bench/ns (48 bundles, E3/Qw3/Qa2, no retention), then health
#   cluster.sh health              per node zk/bookie/broker; zk quorum; bookies rw/ro (+ bookkeeper shell listbookies
#                                  -rw); brokers; namespace policies; proof that ledgers are E3/Qw3/Qa2 (broker
#                                  internalStats with ledger metadata + bookkeeper shell ledgermetadata)
#   cluster.sh balance [-fix]      topics + bundles per broker for bench/ns; -fix moves bundles until within ±10%
#   cluster.sh start               start zk, bookies, brokers on the existing data (no wipe, no metadata init)
#   cluster.sh stop | stats | mark | threads [role] [secs] | mkconf [KEY=VAL...]
# Env: HOSTS_ENV (default ../hosts.env), WAIT (s per startup phase, default 240), STOP_WAIT (s, reset uses 5).
# Bash 3.2 (the Mac's /bin/bash): no associative arrays, no ${x,,}.
set -u
. "$(cd "$(dirname "$0")" && pwd)/lib.sh"
PC=$R/pulsar/pc.sh
WAIT=${WAIT:-240}
N1=${B_PUB[0]}
TMP=$(mktemp -d "${TMPDIR:-/tmp}/pcluster.XXXXXX"); trap 'rm -rf "$TMP"' EXIT

CMD=${1:-}; [ $# -gt 0 ] && shift
for a in "$@"; do    # KEY=VAL cluster knobs (validated against lib.sh's KNOBS)
  case $a in
    *=*) k=${a%%=*}; case " $KNOBS " in *" $k "*) eval "$k=\${a#*=}"; export "$k";; *) die "unknown knob $k (known: $KNOBS)";; esac ;;
    -fix|--fix) FIX=1 ;;
    *) ARGS="${ARGS:-} $a" ;;
  esac
done

allb() {  # allb <label> <remote cmd>: on every broker in parallel; outputs in $TMP/<label>.<n>
  par "$TMP" "$1" "$2" "${B_PUB[@]}"
}
wait_rc() {  # wait_rc <what> <remote cmd on node 1 that exits 0 when ready>
  local t0 out
  t0=$(date +%s)
  while :; do
    out=$(rsh "$N1" "$2" 2>&1) && { log "$1: $out"; return 0; }
    if [ $(( $(date +%s) - t0 )) -ge "$WAIT" ]; then log "$1: TIMEOUT after ${WAIT}s: $out"; return 1; fi
    sleep 2
  done
}

start_cluster() {  # start_cluster <init: 1|0>
  local i
  allb zk "$PC start zk" || { show "$TMP" zk "$NB"; log "zk start failed"; return 1; }
  show "$TMP" zk "$NB"
  wait_rc "zk quorum" "$PC zkquorum | grep 'leader=1 followers=$((NB - 1)) synced=$((NB - 1))'" || return 1
  if [ "$1" = 1 ]; then
    log "initialize-cluster-metadata (once, node 1)"
    rsh "$N1" "$PC init-metadata" || { log "initialize-cluster-metadata failed"; return 1; }
  fi
  allb bookie "$PC start bookie" || { show "$TMP" bookie "$NB"; log "bookie start failed"; return 1; }
  show "$TMP" bookie "$NB"
  if ! wait_rc "bookies writable" "$PC bookies | grep 'bookies rw=$NB '"; then
    # format as needed: a bookie whose cookie no longer matches the metadata (its disk or ZooKeeper wiped alone) exits
    # with InvalidCookieException; bookieformat -deleteCookie makes it a new, empty bookie (its ledgers' copies are gone)
    for i in $(seq 1 "$NB"); do
      h=${B_PUB[$((i - 1))]}
      if rsh "$h" "$PC health bookie | grep -q down && tail -n 400 $R/logs/pulsar/bookie/bookie.log | grep -q InvalidCookieException"; then
        log "node $i: bookie down with a cookie error: bookieformat -deleteCookie + restart"
        rsh "$h" "$PC stop bookie; $PC format-bookie > /dev/null 2>&1; $PC start bookie"
      fi
    done
    wait_rc "bookies writable (after format)" "$PC bookies | grep 'bookies rw=$NB '" || return 1
  fi
  allb broker "$PC start broker" || { show "$TMP" broker "$NB"; log "broker start failed"; return 1; }
  show "$TMP" broker "$NB"
  wait_rc "brokers healthy" "$PC brokers | grep 'active=$NB ' | grep -v ERR" || return 1
}
health() {
  allb health "$PC health"; show "$TMP" health "$NB"
  rsh "$N1" "$PC zkquorum" | sed 's/^/  /'
  rsh "$N1" "$PC bookies -shell" | sed 's/^/  /'
  rsh "$N1" "$PC brokers" | sed 's/^/  /'
  rsh "$N1" "python3 $R/pulsar/padmin.py ns-show --admin http://${B_PRIV[0]}:8080" | sed 's/^/  /'
  rsh "$N1" "$PC proof" | sed 's/^/  /'
}

case $CMD in
reset)
  log "reset: stop all + wipe on $NB nodes (${B_PRIV[*]})"
  allb stop "STOP_WAIT=${STOP_WAIT:-5} $PC stop all"
  allb wipe "$PC wipe" || { show "$TMP" wipe "$NB"; die "wipe failed"; }
  log "reset: render configs:$(knob_env)"
  allb mkconf "env $(knob_env) $PC mkconf" || { show "$TMP" mkconf "$NB"; die "mkconf failed"; }
  show "$TMP" mkconf "$NB"
  start_cluster 1 || die "reset failed"
  out=$(rsh "$N1" "$PC ns-setup" 2>&1); rc=$?; echo "$out" | sed 's/^/  /'
  [ $rc = 0 ] || die "namespace setup failed (rc $rc)"
  health
  log "reset done" ;;
start)
  start_cluster 0 || die "start failed"
  health ;;
health) health ;;
balance) rsh "$N1" "$PC balance ${FIX:+--fix}"; exit $? ;;
stop) allb stop "$PC stop all"; show "$TMP" stop "$NB" ;;
stats) allb stats "$PC stats"; show "$TMP" stats "$NB" ;;
mark) allb mark "$PC mark"; show "$TMP" mark "$NB" ;;
threads) allb threads "$PC threads ${ARGS:-all}"; show "$TMP" threads "$NB" ;;
mkconf) allb mkconf "env $(knob_env) $PC mkconf"; show "$TMP" mkconf "$NB" ;;
*) usage; exit 2 ;;
esac
