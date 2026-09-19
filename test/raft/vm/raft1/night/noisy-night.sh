#!/bin/bash
# NIGHT-A step 3 — O19 noisy neighbour with a REAL active DLQ storm, pipeline=4.
# before/during/after quiet p99, gated at +-15% vs baseline. Raw CSVs kept.
set -u
ulimit -n 262144 2>/dev/null || true
BIN=/root/raft/night/queen/server/target/release/queen
G=/root/goload
PY=/root/raft/night/dlqstorm.py
PORT=6698; URL=http://127.0.0.1:$PORT
D=${OUTDIR:-/root/raft/night/raft1-noisy}; rm -rf "$D"; mkdir -p "$D/data"
NOISY=${NOISY:-2000000}; QSECS=${QSECS:-90}; STORM_TARGET=${STORM_TARGET:-500}
HZ=$(getconf CLK_TCK); PAGE=$(getconf PAGESIZE)
QPID=""
start(){ env QUEEN_STORAGE=raft QUEEN_RAFT_DIR="$D/data" QUEEN_BIND_ADDR=127.0.0.1 PORT=$PORT \
  JWT_ENABLED=false QUEEN_TENANCY_HEADER=false QUEEN_RAFT_PIPELINE=4 FILE_BUFFER_DIR="$D/buf" \
  LOG_LEVEL=warn nohup "$BIN" >>"$D/broker.log" 2>&1 & QPID=$!
  for i in $(seq 1 90); do curl -s $URL/health 2>/dev/null | grep -q '"storageReady":true' && return 0; kill -0 $QPID 2>/dev/null || return 1; sleep 0.3; done; return 1; }
pushed_count(){ awk -F'push=' '/^\[[0-9]/{n=$2+0} END{print n+0}' "$1"; }
# broker RSS/CPU sampler -> $D/broker-rss.csv while $D/.stop absent
( echo "ts,phase,cpu_s,rss_mb" > "$D/broker-rss.csv"
  while [ ! -f "$D/.stop" ]; do
    ph=$(cat "$D/.phase" 2>/dev/null || echo "-")
    if [ -n "$QPID" ] && [ -r /proc/$QPID/stat ]; then
      read u s r <<< "$(awk '{sub(/^[0-9]+ \(.*\) /,""); print $12,$13,$22}' /proc/$QPID/stat 2>/dev/null)"
      awk -v ts=$(date +%s) -v ph="$ph" -v c=$(( ${u:-0}+${s:-0} )) -v hz=$HZ -v rss=${r:-0} -v pg=$PAGE 'BEGIN{printf "%d,%s,%.2f,%.0f\n",ts,ph,c/hz,rss*pg/1024}' >> "$D/broker-rss.csv"
    fi; sleep 3
  done ) & SPID=$!

quiet(){ # $1 phase tag -> writes $D/quiet-$1.gl ; returns "p50=.. p99=.. p999=.."
  echo "$1" > "$D/.phase"
  "$G" -mode openloop -url $URL -queue quiet -rate 3000 -push-batch 1 -partitions 200 -consumers 48 \
       -pop-batch 50 -manual-ack -payload 256 -duration $QSECS -ramp-sec 3 > "$D/quiet-$1.gl" 2>&1
  grep -E '^\[final\]' "$D/quiet-$1.gl" | sed -n 's/.*overall p50=\([0-9.]*\) p99=\([0-9.]*\) p999=\([0-9.]*\).*/p50=\1 p99=\2 p999=\3/p'
}
p99of(){ echo "$1" | sed -n 's/.*p99=\([0-9.]*\).*/\1/p'; }

echo "noisy-night: pipeline=4 NOISY=$NOISY quiet=${QSECS}s storm_target=${STORM_TARGET}/s  start $(date -u +%H:%M:%S)"
start || { echo "broker failed"; tail "$D/broker.log"; exit 1; }

echo "== BEFORE: baseline quiet, empty store =="
BEFORE=$(quiet before); echo "  quiet $BEFORE"

echo "== build hot backlog: $NOISY msgs on hot/partition 0 (never popped by quiet) =="
echo "build" > "$D/.phase"; t0=$(date +%s)
"$G" -mode openloop -url $URL -queue hot -rate 400000 -push-batch 100 -partitions 1 -consumers 0 \
     -payload 256 -duration 0 -max-inflight 4096 -idle-conns 4096 > "$D/hot.gl" 2>&1 &
HP=$!
while kill -0 $HP 2>/dev/null; do [ "$(pushed_count "$D/hot.gl")" -ge "$NOISY" ] && break; sleep 2; done
kill $HP 2>/dev/null; sleep 1; kill -9 $HP 2>/dev/null
echo "  hot backlog push=$(pushed_count "$D/hot.gl") in $(( $(date +%s)-t0 ))s, data $(du -sh "$D/data"|cut -f1)"

echo "== DURING: quiet WHILE an active DLQ storm churns hot/partition 0 =="
CSV="$D/storm.csv" TARGET=$STORM_TARGET BATCH=50 PORT=$PORT python3 "$PY" hot 0 $((QSECS+8)) > "$D/storm.out" 2>&1 &
STPID=$!
sleep 2   # let the storm ramp
DURING=$(quiet during); echo "  quiet $DURING"
wait $STPID 2>/dev/null; echo "  $(cat "$D/storm.out")"

echo "== AFTER: quiet again, storm stopped, backlog at rest (recovery) =="
AFTER=$(quiet after); echo "  quiet $AFTER"

touch "$D/.stop"; sleep 4; kill $SPID 2>/dev/null
kill $QPID 2>/dev/null; sleep 2; kill -9 $QPID 2>/dev/null

bp=$(p99of "$BEFORE"); dp=$(p99of "$DURING"); ap=$(p99of "$AFTER")
echo "== O19 verdict (pipeline=4, active DLQ storm) =="
echo "  before=$BEFORE"; echo "  during=$DURING"; echo "  after=$AFTER"
awk -v b="$bp" -v d="$dp" -v a="$ap" 'BEGIN{
  if(b<=0){print "  baseline p99 unavailable"; exit}
  dd=(d-b)/b*100; da=(a-b)/b*100;
  printf "  quiet p99: before=%.2f during=%.2f (%+.1f%%) after=%.2f (%+.1f%%)  [threshold +-15%%]\n", b,d,dd,a,da
  printf "  DURING vs before: %s ; AFTER vs before: %s\n", (dd<=15&&dd>=-15)?"PASS":"FAIL", (da<=15&&da>=-15)?"PASS":"FAIL"
}'
echo "  broker error lines: $(grep -ciE ' error |panic|poison|TimeWentBackwards' "$D/broker.log")"
echo "  broker alive at end: $(curl -s $URL/health 2>/dev/null | grep -o '\"status\":\"[a-z]*\"' || echo down)"
echo "  DLQ rows filed (approx from storm.out): $(sed -n 's/.*acked_dlq=\([0-9]*\).*/\1/p' "$D/storm.out")"
echo "  data dir end: $(du -sh "$D/data"|cut -f1)"
echo "done $(date -u +%H:%M:%S)"
