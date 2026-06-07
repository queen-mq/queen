#!/bin/bash
set -u

LOGFILE=/root/bench-runs/master.log
exec >> "$LOGFILE" 2>&1

DURATION=300

# format: name:maxPart:msgs:prodW:prodC:consW:consC:consBatch:queueCount
TESTS=(
  "hi-part-10:10:1:5:100:2:50:100:1"
  "hi-part-100:100:1:5:100:2:50:100:1"
  "hi-part-1000:1000:1:5:100:2:50:100:1"
  "hi-part-10000:10000:1:5:100:2:50:100:1"
  "bp-1:1000:1:1:50:1:50:100:1"
  "bp-10:1000:10:1:50:1:50:100:1"
  "bp-100:1000:100:1:50:1:50:100:1"
  "q-1:1000:10:1:50:1:50:100:1"
  "q-10:1000:10:1:50:1:50:100:10"
  "q-100:1000:10:1:50:1:50:100:100"
)

ts() { date -u +%FT%TZ; }

echo "[$(ts)] ============================================="
echo "[$(ts)] MASTER START — ${#TESTS[@]} tests, ${DURATION}s each"
echo "[$(ts)] estimated wall time: $(( ${#TESTS[@]} * 16 )) min"
echo "[$(ts)] ============================================="

for t in "${TESTS[@]}"; do
  IFS=':' read -r name maxp msgs pw pc cw cc cb qc <<< "$t"
  echo "[$(ts)] >>> RUN $name (mp=$maxp msgs=$msgs prod=${pw}x${pc} cons=${cw}x${cc} cb=$cb qc=$qc)"
  /root/bench-runs/run_test_v3.sh "$name" "$maxp" "$msgs" "$DURATION" "$pw" "$pc" "$cw" "$cc" "$cb" "$qc" \
    || echo "[$(ts)] FAILED $name"
  echo "[$(ts)] <<< END $name"
  echo "[$(ts)] ---"
done

echo "[$(ts)] MASTER COMPLETE"
