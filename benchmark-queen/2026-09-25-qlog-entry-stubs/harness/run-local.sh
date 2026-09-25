#!/usr/bin/env bash
# run-local.sh NAME LAYOUT QUEUES RATE DURATION [goload args...]
# One run on a FRESH single-node openraft broker (QUEEN_LANES=1), N queues x 1
# partition, one long-poll consumer per queue, push batch 10; then the qlog
# byte split. LAYOUT = copies | stub (QUEEN_QLOG_ENTRY_LAYOUT).
set -u
H=$(cd "$(dirname "$0")" && pwd)
NAME=$1 LAYOUT=$2 Q=$3 RATE=$4 DUR=$5; shift 5
BIN=${BIN:-$H/../queen-stub}
PORT=${PORT:-6690}
D=$H/runs/$NAME; rm -rf "$D"; mkdir -p "$D"
DATA=$H/data-$NAME; rm -rf "$DATA"; mkdir -p "$DATA/b"
env QUEEN_STORAGE=raft QUEEN_RAFT_REPLICATOR=openraft QUEEN_RAFT_DIR=$DATA/d QUEEN_RAFT_DEDUP_INDEX=segment \
  QUEEN_RAFT_POP_FASTPATH_EMPTY=1 QUEEN_BIND_ADDR=127.0.0.1 PORT=$PORT JWT_ENABLED=false QUEEN_TENANCY_HEADER=false \
  QUEEN_LANES=1 FILE_BUFFER_DIR=$DATA/b LOG_LEVEL=info QUEEN_RAFT_DISK_HIGH_PCT=99 QUEEN_EDGE_MAX_CONNS=120000 \
  QUEEN_QLOG_ENTRY_LAYOUT=$LAYOUT ${EXTRA:-} "$BIN" > "$D/broker.log" 2>&1 &
BP=$!
for i in $(seq 1 600); do
  c=$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 http://127.0.0.1:$PORT/health); [ "$c" = 200 ] && break; sleep 0.2
done
echo "health=$c layout=$LAYOUT queues=$Q rate=$RATE" | tee "$D/start.txt"
"$H/goload" -mode multiq -url http://127.0.0.1:$PORT -queues "$Q" -rate "$RATE" -partitions 1 -consume perqueue \
  -consumers-per-queue 1 -push-batch 10 -duration "$DUR" -warm-sec ${WARM:-10} -ramp-sec 5 -pop-batch 100 \
  -pop-timeout 5000 "$@" > "$D/goload.log" 2>&1
curl -s --max-time 5 http://127.0.0.1:$PORT/metrics/prometheus > "$D/metrics.txt"
kill -TERM $BP; for i in $(seq 1 300); do kill -0 $BP 2>/dev/null || break; sleep 0.1; done; kill -9 $BP 2>/dev/null
grep '^\[result\]' "$D/goload.log" | sed 's/^\[result\] //' > "$D/result.json"
python3 "$H/qlogbytes.py" "$DATA/d/qlog" > "$D/bytes.json"
echo "qlog_dirs $(ls -d $DATA/d/qlog/q* 2>/dev/null | wc -l) qlog_du_kb $(du -sk $DATA/d/qlog | cut -f1)" > "$D/census.txt"
cat "$D/census.txt"; cat "$D/bytes.json"; cat "$D/result.json" | head -c 1500; echo
[ -n "${KEEP:-}" ] || rm -rf "$DATA"
