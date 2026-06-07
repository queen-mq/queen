#!/bin/bash
set -u

LOGFILE=/root/bench-runs/cg-master.log
exec >> "$LOGFILE" 2>&1

DURATION=300
IMAGE_TAG="pushser"

# format: name:maxPart:msgs:prodW:prodC:consW:consC:consBatch:queueCount:cgCount
TESTS=(
  "bp-10-cg5:1000:10:1:50:1:50:100:1:5"
  "bp-10-cg10:1000:10:1:50:1:50:100:1:10"
)

ts() { date -u +%FT%TZ; }

echo "[$(ts)] ============================================="
echo "[$(ts)] CG MASTER START — ${#TESTS[@]} tests on image=$IMAGE_TAG, ${DURATION}s each"
echo "[$(ts)] estimated wall time: $(( ${#TESTS[@]} * 16 )) min"
echo "[$(ts)] ============================================="

for t in "${TESTS[@]}"; do
  IFS=':' read -r name maxp msgs pw pc cw cc cb qc cg <<< "$t"
  echo "[$(ts)] >>> RUN $name (mp=$maxp msgs=$msgs prod=${pw}x${pc} cons=${cw}x${cc}*${cg}cg cb=$cb)"
  /root/bench-runs/run_test_v3.sh "$name" "$maxp" "$msgs" "$DURATION" "$pw" "$pc" "$cw" "$cc" "$cb" "$qc" "$IMAGE_TAG" "$cg" \
    || echo "[$(ts)] FAILED $name"
  echo "[$(ts)] <<< END $name"
  echo "[$(ts)] ---"
done

echo "[$(ts)] CG MASTER COMPLETE"
