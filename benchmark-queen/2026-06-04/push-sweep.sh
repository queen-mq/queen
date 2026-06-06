#!/bin/bash
# Push-connection sweep: fix 200 consumers / pop-batch 300, wipe DB each iteration,
# ramp producer count, and record push vs pop at steady state. Goal: find the
# largest producer count where pop still keeps up (push == pop, no growing backlog).
set -u
BROKER=root@165.232.78.92
LOADER=root@167.99.246.68
SSH="ssh -o BatchMode=yes -o ConnectTimeout=20"
RES=/tmp/push-sweep.txt
WARM="${WARM:-80}"
NS="${NS:-10 50 100 150 200 250}"

: > "$RES"
printf "%-6s %-10s %-10s %-10s %-9s\n" "Nprod" "push/s" "pop/s" "gap/s" "verdict" | tee "$RES"

for N in $NS; do
  echo ">>> [N=$N] wipe DB + restart broker (pushser, 10 workers)..."
  $SSH "$BROKER" "TAG=pushser NUM_WORKERS=10 bash /root/start-broker.sh" >/dev/null 2>&1

  echo ">>> [N=$N] start loader ($N producers / 200 consumers / batch 300)..."
  $SSH "$LOADER" "pkill -9 -x goload-new 2>/dev/null; sleep 1; mv /root/goload.log /root/goload-N$N.log 2>/dev/null; cd /root; setsid /root/goload-new -url http://165.232.78.92:6632 -queue benchq -partitions 300 -producers $N -consumers 200 -push-batch 10 -pop-batch 300 -pop-partitions 10 -pop-wait -pop-timeout 2000 -payload 256 -completed-retention 120 -pending-retention 600 -idle-conns 1600 -report 10 -retries 2 > /root/goload.log 2>&1 < /dev/null &" 2>/dev/null

  echo ">>> [N=$N] warming ${WARM}s..."
  sleep "$WARM"

  LAST=$($SSH "$LOADER" "tail -1 /root/goload.log" 2>/dev/null)
  echo "    sample: $LAST"
  PUSH=$(echo "$LAST" | grep -oE 'push=[[:space:]]*[0-9]+/s' | grep -oE '[0-9]+' | head -1)
  POP=$(echo "$LAST" | grep -oE 'pop=[[:space:]]*[0-9]+/s' | grep -oE '[0-9]+' | head -1)
  PUSH=${PUSH:-0}; POP=${POP:-0}
  GAP=$((PUSH - POP))
  V="balanced"; [ "$GAP" -gt 8000 ] && V="push>pop"
  printf "%-6s %-10s %-10s %-10s %-9s\n" "$N" "$PUSH" "$POP" "$GAP" "$V" | tee -a "$RES"

  $SSH "$LOADER" "pkill -9 -x goload-new 2>/dev/null" 2>/dev/null
  sleep 2
done

echo ">>> SWEEP COMPLETE"
echo "================ RESULTS ================"
cat "$RES"
