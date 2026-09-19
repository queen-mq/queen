#!/bin/bash
set -u
ulimit -n 262144 2>/dev/null || true
BIN=/root/raft/night/queen/server/target/release/queen
D=/root/raft/night/smoke; rm -rf "$D"; mkdir -p "$D/data"
URL=http://127.0.0.1:6698
echo "start $(date -u +%H:%M:%S)"
env QUEEN_STORAGE=raft QUEEN_RAFT_DIR="$D/data" QUEEN_BIND_ADDR=127.0.0.1 PORT=6698 \
  JWT_ENABLED=false QUEEN_TENANCY_HEADER=false QUEEN_RAFT_PIPELINE=4 \
  FILE_BUFFER_DIR="$D/buf" LOG_LEVEL=warn nohup "$BIN" >"$D/broker.log" 2>&1 &
QPID=$!
ready=""
for i in $(seq 1 60); do curl -s $URL/health 2>/dev/null | grep -q '"storageReady":true' && { ready=1; break; }; kill -0 $QPID 2>/dev/null || { echo "broker exited early"; break; }; sleep 0.5; done
[ -n "$ready" ] || { echo "NOT READY"; tail -20 "$D/broker.log"; kill -9 $QPID 2>/dev/null; exit 1; }
echo "storageReady; pipeline=$(curl -s $URL/health | tr ',' '\n' | grep -i pipeline || echo '(no pipeline field)')"
/root/goload -mode openloop -url $URL -queue smoke -rate 20000 -push-batch 10 -partitions 100 -consumers 32 -pop-batch 200 -manual-ack -payload 256 -duration 20 -ramp-sec 3 > "$D/gl.log" 2>&1
echo "--- goload final ---"; grep -E '^\[final\]' "$D/gl.log"
echo "--- health after ---"; curl -s $URL/health | tr ',' '\n' | grep -iE 'role|storageReady|status' | head
echo "--- poison/error lines ---"; grep -icE 'poison|apply thread gone|TimeWentBackwards|panic' "$D/broker.log"; grep -iE 'poison|apply thread gone|TimeWentBackwards|I5|below the last' "$D/broker.log" | head -5
echo "--- broker error count ---"; grep -ciE ' error |panic' "$D/broker.log"
kill $QPID 2>/dev/null; sleep 2; kill -9 $QPID 2>/dev/null
rm -rf "$D/data" "$D/buf"
echo "done $(date -u +%H:%M:%S)"
