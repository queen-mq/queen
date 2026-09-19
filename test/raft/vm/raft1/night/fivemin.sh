#!/bin/bash
set -u
ulimit -n 262144 2>/dev/null || true
BIN=/root/raft/night/queen/server/target/release/queen
D=/root/raft/night/raft1-a20k5m; rm -rf "$D"; mkdir -p "$D/data"
URL=http://127.0.0.1:6698
HZ=$(getconf CLK_TCK); PAGE=$(getconf PAGESIZE)
echo "start $(date -u +%H:%M:%S) pipeline=4 slow_command_ms=5 LOG_LEVEL=info"
env QUEEN_STORAGE=raft QUEEN_RAFT_DIR="$D/data" QUEEN_BIND_ADDR=127.0.0.1 PORT=6698 \
  JWT_ENABLED=false QUEEN_TENANCY_HEADER=false QUEEN_RAFT_PIPELINE=4 QUEEN_RAFT_SLOW_COMMAND_MS=5 \
  FILE_BUFFER_DIR="$D/buf" LOG_LEVEL=info nohup "$BIN" >"$D/broker.log" 2>&1 &
QPID=$!
for i in $(seq 1 90); do curl -s $URL/health 2>/dev/null | grep -q '"storageReady":true' && break; kill -0 $QPID 2>/dev/null || { echo "broker exited"; tail "$D/broker.log"; exit 1; }; sleep 0.5; done
echo "storageReady $(date -u +%H:%M:%S)"
# RSS/CPU sampler every 5s
( echo "ts,cpu_s,rss_mb" > "$D/rss.csv"
  while [ ! -f "$D/stop" ]; do
    if [ -r /proc/$QPID/stat ]; then
      read u s r <<< "$(awk '{sub(/^[0-9]+ \(.*\) /,""); print $12,$13,$22}' /proc/$QPID/stat)"
      awk -v ts=$(date +%s) -v c=$(( ${u:-0}+${s:-0} )) -v hz=$HZ -v rss=${r:-0} -v pg=$PAGE 'BEGIN{printf "%d,%.2f,%.0f\n",ts,c/hz,rss*pg/1024}' >> "$D/rss.csv"
    fi; sleep 5
  done ) & SPID=$!
/root/goload -mode openloop -url $URL -queue a20k5m -rate 20000 -push-batch 10 -partitions 100 -consumers 32 -pop-batch 200 -manual-ack -payload 256 -duration 300 -ramp-sec 5 > "$D/gl.log" 2>&1
touch "$D/stop"; sleep 6; kill $SPID 2>/dev/null
echo "--- goload final ---"; grep -E '^\[final\]' "$D/gl.log"
echo "--- health after ---"; curl -s $URL/health | tr ',' '\n' | grep -iE 'role|storageReady|status' | head -3
echo "--- slow-command / plan lines in broker.log ---"; grep -icE 'slow|plan.*ms|command.*ms|budget' "$D/broker.log"
echo "--- sample broker.log non-startup lines ---"; grep -iE 'slow|WENT|poison|ERROR|WARN' "$D/broker.log" | head -10
echo "--- broker.log line count / level breakdown ---"; wc -l "$D/broker.log"; grep -oiE ' (INFO|WARN|ERROR|DEBUG) ' "$D/broker.log" | sort | uniq -c
echo "--- rss series (every ~30s) ---"; awk 'NR==1||NR%6==0' "$D/rss.csv"
kill $QPID 2>/dev/null; sleep 2; kill -9 $QPID 2>/dev/null
echo "data dir: $(du -sh "$D/data"|cut -f1)"
echo "done $(date -u +%H:%M:%S)"
