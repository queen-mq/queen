#!/usr/bin/env bash
# The EMBEDDED cluster rig: three raft brokers, each running the Kafka facade
# IN PROCESS (QUEEN_KAFKA_EMBEDDED=true) — the shape helm_v2/broker deploys,
# relays and all, which rig-cluster.sh (standalone facades over HTTP) is not.
#
#   rig-embedded.sh up                 # build nothing; start three brokers, wait for a leader
#   rig-embedded.sh down               # stop them, keep the logs
#   rig-embedded.sh restart <1|2|3> [term|kill]   # stop one node and start it again
#   rig-embedded.sh rolling [term|kill] [pause_s]  # every node in turn, like a rollout
#   rig-embedded.sh leader             # the raft node id that leads now
#   rig-embedded.sh status             # health of every node
#
# The broker binary is server/target/release/queen unless QUEEN_BIN says
# otherwise. State lives in $RIG_DIR (default: a fixed temp dir, so `down` and
# `restart` find what `up` started).
set -uo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
BIN="${QUEEN_BIN:-$REPO_ROOT/server/target/release/queen}"
RIG_DIR="${RIG_DIR:-${TMPDIR:-/tmp}/queen-embedded-rig}"
TOKEN="${QUEEN_RAFT_TOKEN:-embedded-rig-token}"
mkdir -p "$RIG_DIR/pids"

http_port() { echo $((56631 + $1)); }
raft_port() { echo $((57399 + $1)); }
kafka_port() { echo $((59091 + $1)); }

PEERS=""
for n in 1 2 3; do
  PEERS="${PEERS:+$PEERS,}$n=127.0.0.1:$(raft_port $n)/127.0.0.1:$(http_port $n)"
done

start() {
  local n=$1
  mkdir -p "$RIG_DIR/raft-$n"
  # What this node is started with beyond the rig's own settings, for the record.
  env | grep -E '^QUEEN_' | sort > "$RIG_DIR/node-$n.env"
  QUEEN_RAFT_DIR="$RIG_DIR/raft-$n" QUEEN_RAFT_DISK_HIGH_PCT=99.5 QUEEN_RAFT_DISK_LOW_PCT=99 \
  QUEEN_RAFT_REPLICATOR=openraft QUEEN_RAFT_NODE_ID="$n" QUEEN_RAFT_PEERS="$PEERS" \
  QUEEN_RAFT_LISTEN="127.0.0.1:$(raft_port $n)" QUEEN_RAFT_TOKEN="$TOKEN" \
  QUEEN_RAFT_DURABLE_EVERY_MS=1000 QUEEN_QLOG_SHARDS=4 \
  QUEEN_SERVER_ID="node-$n" PORT="$(http_port $n)" QUEEN_BIND_ADDR=127.0.0.1 \
  QUEEN_KAFKA_EMBEDDED=true QUEEN_KAFKA_NODE_ID="$n" QUEEN_TOKEN="${QUEEN_TOKEN:-embedded-rig}" QUEEN_KAFKA_ADDR="127.0.0.1:$(kafka_port $n)" \
  QUEEN_KAFKA_ADVERTISED_ADDR="127.0.0.1:$(kafka_port $n)" \
  QUEEN_KAFKA_DEFAULT_PARTITIONS="${QUEEN_KAFKA_DEFAULT_PARTITIONS:-1}" \
  LOG_LEVEL="${LOG_LEVEL:-info}" QUEEN_LOG_JSON=true NO_COLOR=1 \
    "$BIN" >> "$RIG_DIR/node-$n.log" 2>&1 &
  echo $! > "$RIG_DIR/pids/node-$n.pid"
}

pid_of() { [ -f "$RIG_DIR/pids/node-$1.pid" ] && cat "$RIG_DIR/pids/node-$1.pid"; }

stop() {
  local n=$1 how=${2:-term} pid
  pid="$(pid_of "$n")" || return 0
  [ -n "$pid" ] || return 0
  if [ "$how" = kill ]; then kill -9 "$pid" 2>/dev/null; else kill "$pid" 2>/dev/null; fi
  for _ in $(seq 1 300); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
  kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null
  rm -f "$RIG_DIR/pids/node-$n.pid"
}

# Every port of node $1 is free: a node restarted onto a port something else
# still holds would fail to bind while its health check answered for the other.
free() {
  local port
  for port in "$(http_port "$1")" "$(raft_port "$1")" "$(kafka_port "$1")"; do
    if lsof -nP -iTCP:"$port" -sTCP:LISTEN -t >/dev/null 2>&1; then
      echo "port $port is still taken after node $1 stopped: not starting it" >&2
      return 1
    fi
  done
}

health() { curl -s -m 2 "http://127.0.0.1:$(http_port "$1")/health" 2>/dev/null; }

wait_healthy() {
  local n=$1
  for _ in $(seq 1 600); do
    health "$n" | grep -q '"status":"healthy"' && return 0
    sleep 0.2
  done
  echo "node $n never became healthy" >&2
  tail -20 "$RIG_DIR/node-$n.log" >&2
  return 1
}

leader() {
  for n in 1 2 3; do
    if health "$n" | grep -q '"role":"leader"'; then echo "$n"; return 0; fi
  done
  return 1
}

case "${1:-}" in
  up)
    [ -x "$BIN" ] || { echo "no broker binary at $BIN" >&2; exit 2; }
    # Refuse rather than answer for somebody else: a port already taken means
    # a health check that passes against whatever holds it.
    for n in 1 2 3; do
      for port in "$(http_port "$n")" "$(raft_port "$n")" "$(kafka_port "$n")"; do
        if lsof -nP -iTCP:"$port" -sTCP:LISTEN -t >/dev/null 2>&1; then
          echo "port $port is already taken (another rig, or something else): not starting" >&2
          exit 2
        fi
      done
    done
    for n in 1 2 3; do start "$n"; done
    for n in 1 2 3; do wait_healthy "$n" || exit 1; done
    echo "up: kafka 127.0.0.1:$(kafka_port 1),127.0.0.1:$(kafka_port 2),127.0.0.1:$(kafka_port 3); leader $(leader); logs $RIG_DIR"
    ;;
  down)
    for n in 1 2 3; do stop "$n" term; done
    echo "down; logs in $RIG_DIR"
    ;;
  wipe)
    for n in 1 2 3; do stop "$n" kill; done
    rm -rf "$RIG_DIR"/raft-* "$RIG_DIR"/node-*.log
    echo "wiped $RIG_DIR"
    ;;
  restart)
    n=${2:?node}; how=${3:-term}
    stop "$n" "$how"
    free "$n" || exit 2
    start "$n"
    wait_healthy "$n" || exit 1
    echo "node $n restarted ($how); leader $(leader)"
    ;;
  rolling)
    how=${2:-term}; pause=${3:-10}
    # The order a StatefulSet rolls: the highest ordinal first.
    for n in 3 2 1; do
      stop "$n" "$how"
      free "$n" || exit 2
      start "$n"
      wait_healthy "$n" || exit 1
      echo "node $n restarted ($how); leader $(leader)"
      sleep "$pause"
    done
    ;;
  leader) leader ;;
  status)
    for n in 1 2 3; do echo "node $n: $(health "$n" | head -c 200)"; done
    ;;
  *)
    sed -n '2,16p' "$0"; exit 2 ;;
esac
