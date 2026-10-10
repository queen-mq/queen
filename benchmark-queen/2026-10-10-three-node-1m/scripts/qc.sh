#!/usr/bin/env bash
# qc.sh <start|stop|term|kill|wipe|status|sample-start TAG|sample-stop> : this machine's voter of the three.
# BIN=/root/p3/bin/queen-X and EXTRA="K=V ..." are read at start. The HTTP and raft ports listen on the VPC address only.
set -u
ROOT=/root/p3; . $ROOT/node.env
pid() { cat $ROOT/run/pid 2>/dev/null; }
case $1 in
  start)
    mkdir -p $ROOT/data $ROOT/logs $ROOT/run
    ( ulimit -n 1048576
      export QUEEN_RAFT_DIR=$ROOT/data PORT=6632 QUEEN_BIND_ADDR=$PRIV QUEEN_SERVER_ID=n$NODE LOG_LEVEL=info
      export QUEEN_RAFT_REPLICATOR=openraft QUEEN_RAFT_NODE_ID=$NODE QUEEN_RAFT_PEERS="$PEERS"
      export QUEEN_RAFT_LISTEN=$PRIV:7400 QUEEN_RAFT_TOKEN=p3-perf-check-2026-10-10
      for kv in ${EXTRA:-}; do export "$kv"; done
      nohup "${BIN:?}" >>$ROOT/logs/broker.log 2>&1 </dev/null &
      echo $! > $ROOT/run/pid ) ;;
  term) kill -TERM "$(pid)" ;;
  kill) kill -9 "$(pid)" ;;
  stop)
    kill -TERM "$(pid)" 2>/dev/null
    for i in $(seq 1 60); do kill -0 "$(pid)" 2>/dev/null || break; sleep 0.5; done
    kill -9 "$(pid)" 2>/dev/null; rm -f $ROOT/run/pid ;;
  wipe) rm -rf $ROOT/data; : > $ROOT/logs/broker.log ;;
  status) curl -s -m 2 http://$PRIV:6632/health | cut -c1-300 ;;
  sample-start)
    mkdir -p $ROOT/runs/$2
    nohup python3 $ROOT/sampler.py "$(pid)" $PRIV > $ROOT/runs/$2/sample-n$NODE.log 2>&1 </dev/null &
    echo $! > $ROOT/run/sampler.pid ;;
  sample-stop) kill "$(cat $ROOT/run/sampler.pid 2>/dev/null)" 2>/dev/null; rm -f $ROOT/run/sampler.pid ;;
  *) echo "unknown $1" >&2; exit 2 ;;
esac
