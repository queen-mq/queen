#!/bin/bash
set -u

LOGFILE=/root/bench-runs/v12-master.log
exec >> "$LOGFILE" 2>&1

DURATION=900
IMAGE_TAG="0.12.19"

# format: name:maxPart:msgs:prodW:prodC:consW:consC:consBatch:queueCount
TESTS=(
  "v12-bp-10:1000:10:1:50:1:50:100:1"
  "v12-bp-100:1000:100:1:50:1:50:100:1"
  "v12-hi-part-1:1:1:5:100:2:50:100:1"
  "v12-hi-part-10000:10000:1:5:100:2:50:100:1"
  "v12-q-10:1000:10:1:50:1:50:100:10"
)

ts() { date -u +%FT%TZ; }

echo "[$(ts)] ============================================="
echo "[$(ts)] V12 MASTER START — ${#TESTS[@]} tests on image=$IMAGE_TAG, ${DURATION}s each"
echo "[$(ts)] estimated wall time: $(( ${#TESTS[@]} * 16 )) min"
echo "[$(ts)] ============================================="

for t in "${TESTS[@]}"; do
  IFS=':' read -r name maxp msgs pw pc cw cc cb qc <<< "$t"
  echo "[$(ts)] >>> RUN $name (mp=$maxp msgs=$msgs prod=${pw}x${pc} cons=${cw}x${cc} cb=$cb qc=$qc image=$IMAGE_TAG)"
  /root/bench-runs/run_test_v3.sh "$name" "$maxp" "$msgs" "$DURATION" "$pw" "$pc" "$cw" "$cc" "$cb" "$qc" "$IMAGE_TAG" \
    || echo "[$(ts)] FAILED $name"
  echo "[$(ts)] <<< END $name"
  echo "[$(ts)] ---"
done

echo "[$(ts)] V12 MASTER COMPLETE"
