#!/usr/bin/env bash
# broker.sh <single|cluster3> <start|stop|wipe|status>  (on the VM)
#
# single   one node, QUEEN_RAFT_REPLICATOR=local (the default): one voter.
# cluster3 three voters on THIS machine: loopback raft, one disk for the three
#          logs. It shows what replication adds; it is not three machines.
#
# The broker runs code defaults, except: bound to 127.0.0.1 (it has no
# authentication), and the per-tenant KV rate limits raised out of the way
# (DEFAULT_RATES=1 keeps the defaults: 200 reads/s, 100 writes/s).
# EXTRA="K=V K=V" adds environment. DATA_ROOT moves the data directories
# (DATA_ROOT=/dev/shm/kvl puts them in RAM: the broker without its disk).
set -u
ROOT=/root/kvl
BIN=${BIN:-$ROOT/bin/queen}
TOPO=${1:?single|cluster3}
ACT=${2:?start|stop|wipe|status}
PEERS="1=127.0.0.1:7401/127.0.0.1:6632,2=127.0.0.1:7402/127.0.0.1:6633,3=127.0.0.1:7403/127.0.0.1:6634"
case $TOPO in single) N=1 ;; cluster3) N=3 ;; *) echo "unknown topology $TOPO" >&2; exit 2 ;; esac
DATA_ROOT=${DATA_ROOT:-$ROOT/data}
mkdir -p $ROOT/logs $ROOT/run "$DATA_ROOT"

start_node() {
  local i=$1 dir=$DATA_ROOT/$TOPO/n$1 port=$((6631 + $1))
  mkdir -p "$dir"
  (
    ulimit -n 1048576
    export QUEEN_RAFT_DIR=$dir PORT=$port QUEEN_BIND_ADDR=127.0.0.1 QUEEN_SERVER_ID=n$i LOG_LEVEL=info
    if [ "${DEFAULT_RATES:-0}" != 1 ]; then
      export QUEEN_KV_READ_RATE=100000000 QUEEN_KV_READ_BURST=200000000
      export QUEEN_KV_WRITE_RATE=100000000 QUEEN_KV_WRITE_BURST=200000000
    fi
    if [ "$TOPO" = cluster3 ]; then
      export QUEEN_RAFT_REPLICATOR=openraft QUEEN_RAFT_NODE_ID=$i QUEEN_RAFT_PEERS="$PEERS"
      export QUEEN_RAFT_LISTEN=127.0.0.1:740$i QUEEN_RAFT_TOKEN=kvl-bench-token
    fi
    for kv in ${EXTRA:-}; do export "$kv"; done
    nohup "$BIN" >"$ROOT/logs/$TOPO-n$i.log" 2>&1 &
    echo $! >"$ROOT/run/n$i.pid"
  )
}

stop_all() {
  for f in $ROOT/run/n*.pid; do
    [ -e "$f" ] || continue
    kill "$(cat "$f")" 2>/dev/null
  done
  for _ in $(seq 1 100); do
    pgrep -x queen >/dev/null || break
    sleep 0.1
  done
  pkill -9 -x queen 2>/dev/null
  rm -f $ROOT/run/n*.pid
}

# Healthy = every node answers /health as leader or follower with a leader known.
wait_ready() {
  for _ in $(seq 1 300); do
    local ok=0 leaders=0
    for i in $(seq 1 $N); do
      h=$(curl -s -m 1 "http://127.0.0.1:$((6631 + i))/health") || continue
      echo "$h" | grep -q '"status":"healthy"' && ok=$((ok + 1))
      echo "$h" | grep -q '"role":"leader"' && leaders=$((leaders + 1))
    done
    if [ $ok = $N ] && [ $leaders = 1 ]; then return 0; fi
    sleep 0.1
  done
  echo "broker.sh: not ready after 30 s" >&2
  return 1
}

case $ACT in
  start)
    stop_all
    for i in $(seq 1 $N); do start_node "$i"; done
    wait_ready || exit 1
    ;;
  stop) stop_all ;;
  wipe)
    stop_all
    rm -rf "$DATA_ROOT/$TOPO"
    ;;
  status)
    for i in $(seq 1 $N); do
      echo "n$i pid=$(cat $ROOT/run/n$i.pid 2>/dev/null) $(curl -s -m 1 "http://127.0.0.1:$((6631 + i))/health")"
    done
    ;;
  *) echo "unknown action $ACT" >&2; exit 2 ;;
esac
