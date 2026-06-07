#!/bin/bash
R=/root/bench-runs/run_test_v3.sh
DUR=${DUR:-300}
TAG=${TAG:-0.16.0.beta.1}
TESTS=(
  "hi-part-1:1:1:5:100:2:50:100:1"
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
echo "[$(date -u +%FT%TZ)] APRIL016 START ${#TESTS[@]} tests @ ${DUR}s tag=$TAG"
for t in "${TESTS[@]}"; do
  IFS=: read -r name maxp msgs pw pc cw cc cb qc <<< "$t"
  echo "[$(date -u +%FT%TZ)] >>> RUN $name"
  $R "$name" "$maxp" "$msgs" "$DUR" "$pw" "$pc" "$cw" "$cc" "$cb" "$qc" "$TAG" || echo "[$(date -u +%FT%TZ)] FAILED $name"
done
echo "APRIL016-DONE"
