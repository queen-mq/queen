#!/usr/bin/env bash
# restart.sh NAME LAYOUT — reopen the kept data dir of run NAME and time boot to /health 200.
set -u
H=$(cd "$(dirname "$0")" && pwd)
NAME=$1 LAYOUT=$2
BIN=${BIN:-$H/../queen-stub}; PORT=${PORT:-6690}
DATA=$H/data-$NAME; D=$H/runs/$NAME
t0=$(python3 -c 'import time;print(time.time())')
env QUEEN_STORAGE=raft QUEEN_RAFT_REPLICATOR=openraft QUEEN_RAFT_DIR=$DATA/d QUEEN_RAFT_DEDUP_INDEX=segment \
  QUEEN_RAFT_POP_FASTPATH_EMPTY=1 QUEEN_BIND_ADDR=127.0.0.1 PORT=$PORT JWT_ENABLED=false QUEEN_TENANCY_HEADER=false \
  QUEEN_LANES=1 FILE_BUFFER_DIR=$DATA/b LOG_LEVEL=info QUEEN_RAFT_DISK_HIGH_PCT=99 QUEEN_EDGE_MAX_CONNS=120000 \
  QUEEN_QLOG_ENTRY_LAYOUT=$LAYOUT ${EXTRA:-} "$BIN" > "$D/restart-broker.log" 2>&1 &
BP=$!
for i in $(seq 1 3000); do
  c=$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 http://127.0.0.1:$PORT/health); [ "$c" = 200 ] && break; sleep 0.1
done
t1=$(python3 -c 'import time;print(time.time())')
python3 -c "print('restart_to_health_s', round($t1-$t0, 2))" | tee "$D/restart.txt"
kill -TERM $BP; for i in $(seq 1 300); do kill -0 $BP 2>/dev/null || break; sleep 0.1; done; kill -9 $BP 2>/dev/null
rm -rf "$DATA"
