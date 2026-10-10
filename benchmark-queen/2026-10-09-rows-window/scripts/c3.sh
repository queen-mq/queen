#!/usr/bin/env bash
# c3.sh <startall|stopall|wipe|status|start N|kill N|term N>
# Three voters of the new build on this machine (loopback raft), each controlled alone.
set -u
ROOT=/root/improve/c3; BIN=${BIN:-/root/improve/bin/queen-new}
PEERS="1=127.0.0.1:7401/127.0.0.1:6632,2=127.0.0.1:7402/127.0.0.1:6633,3=127.0.0.1:7403/127.0.0.1:6634"
mkdir -p $ROOT/logs $ROOT/run $ROOT/data
start() {
  local i=$1 dir=$ROOT/data/n$1; mkdir -p "$dir"
  (
    ulimit -n 1048576
    export QUEEN_RAFT_DIR=$dir PORT=$((6631 + i)) QUEEN_BIND_ADDR=127.0.0.1 QUEEN_SERVER_ID=n$i LOG_LEVEL=info
    export QUEEN_RAFT_REPLICATOR=openraft QUEEN_RAFT_NODE_ID=$i QUEEN_RAFT_PEERS="$PEERS"
    export QUEEN_RAFT_LISTEN=127.0.0.1:740$i QUEEN_RAFT_TOKEN=c3-check-token
    export QUEEN_RAFT_TXN_WINDOW_MIN_S=5 RETENTION_INTERVAL=1000 QUEEN_QLOG_SEAL_AGE_S=5 QUEEN_RAFT_SEGMENT_BYTES=1048576
    for kv in ${EXTRA:-}; do export "$kv"; done
    nohup "$BIN" >>"$ROOT/logs/n$i.log" 2>&1 </dev/null &
    echo $! >"$ROOT/run/n$i.pid"
  )
}
pid() { cat $ROOT/run/n$1.pid 2>/dev/null; }
case $1 in
  start) start "$2" ;;
  kill) kill -9 "$(pid "$2")" ;;
  term) kill -TERM "$(pid "$2")" ;;
  startall) for i in 1 2 3; do start $i; done ;;
  stopall)
    for i in 1 2 3; do kill -TERM "$(pid $i)" 2>/dev/null; done
    sleep 3
    for i in 1 2 3; do kill -9 "$(pid $i)" 2>/dev/null; done
    rm -f $ROOT/run/n*.pid ;;
  wipe) rm -rf $ROOT/data; rm -f $ROOT/logs/*.log ;;
  status) for i in 1 2 3; do echo "n$i pid=$(pid $i) $(curl -s -m 1 http://127.0.0.1:$((6631 + i))/health | cut -c1-260)"; done ;;
  *) echo "unknown $1" >&2; exit 2 ;;
esac
