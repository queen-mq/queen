#!/usr/bin/env bash
# node.sh <start BIN | term | kill | wipe | status>: the one broker of the roll cluster on this node
# (n1..n3 = raft ids 1..3, private addresses, HTTP 6632, raft 7400, data in /root/roll/data).
# start copies BIN to /root/roll/queen first, so the process is named queen whichever build runs.
set -u
R=/root/roll
# n1..n3 are names in /etc/hosts (their private addresses); this node is the one whose address is bound here.
MINE=$(ip -4 -o addr show | awk '{print $4}' | cut -d/ -f1)
ID=; PEERS=
for i in 1 2 3; do
  A=$(getent hosts n$i | awk '{print $1}')
  [ -n "$A" ] || { echo "node.sh: n$i is not in /etc/hosts"; exit 2; }
  PEERS="${PEERS:+$PEERS,}$i=$A:7400/$A:6632"
  echo "$MINE" | grep -qx "$A" && { ID=$i; IP=$A; }
done
[ -n "$ID" ] || { echo "node.sh: none of n1..n3 is this node"; exit 2; }
mkdir -p $R/data $R/logs $R/buf
case ${1:?} in
start)
  BIN=${2:?}
  pgrep -x queen >/dev/null && { echo "node.sh: a queen already runs"; exit 1; }
  cp -f "$BIN" $R/queen
  echo "=== $(date -u +%FT%T.%3NZ) start $(basename "$BIN") md5 $(md5sum $R/queen | cut -c1-8)" >> $R/logs/queen.log
  (
    ulimit -n 1048576
    export QUEEN_STORAGE=raft QUEEN_RAFT_REPLICATOR=openraft QUEEN_RAFT_NODE_ID=$ID QUEEN_RAFT_PEERS=$PEERS \
      QUEEN_RAFT_LISTEN=$IP:7400 QUEEN_RAFT_TOKEN=roll26-token QUEEN_RAFT_DIR=$R/data QUEEN_BIND_ADDR=$IP PORT=6632 \
      JWT_ENABLED=false QUEEN_TENANCY_HEADER=false FILE_BUFFER_DIR=$R/buf LOG_LEVEL=info
    setsid nohup $R/queen >> $R/logs/queen.log 2>&1 < /dev/null &
    echo $! > $R/queen.pid
  ) ;;
term)
  pkill -TERM -x queen || exit 0
  for _ in $(seq 1 600); do pgrep -x queen >/dev/null || exit 0; sleep 0.1; done
  echo "node.sh: queen did not stop within 60 s"; exit 1 ;;
kill)
  pkill -9 -x queen; sleep 0.5 ;;
wipe)
  pgrep -x queen >/dev/null && { echo "node.sh: stop it first"; exit 1; }
  rm -rf $R/data $R/buf $R/data.26-saved ;;
status)
  pgrep -a -x queen || echo "no queen" ;;
esac
