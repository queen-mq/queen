#!/bin/bash
set -u

LOGFILE=/root/bench-runs/orchestrator.log
exec >> "$LOGFILE" 2>&1

DURATION=900

TESTS=(
  "part-1:1:1"
  "part-10:10:1"
  "part-100:100:1"
  "part-1000:1000:1"
  "part-10000:10000:1"
)

ts() { date -u +%FT%TZ; }

echo "[$(ts)] ============================================="
echo "[$(ts)] PARTITION AXIS START (${#TESTS[@]} tests, ${DURATION}s each)"
echo "[$(ts)] ============================================="

for t in "${TESTS[@]}"; do
  IFS=':' read -r name maxp batch <<< "$t"
  echo "[$(ts)] >>> RUN $name (maxPart=$maxp, batch=$batch)"
  /root/bench-runs/run_test.sh "$name" "$maxp" "$batch" "$DURATION" || echo "[$(ts)] FAILED $name"
  echo "[$(ts)] <<< END $name"
  echo "[$(ts)] ---"
done

echo "[$(ts)] PARTITION AXIS COMPLETE"
