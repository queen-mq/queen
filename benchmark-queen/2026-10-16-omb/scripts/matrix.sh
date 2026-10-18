#!/usr/bin/env bash
# matrix.sh <points file> — one OMB point per line: <tag> <kafka|queen-facade|queen-native> <driver.yaml> <workload.yaml>
# A fresh cluster before every point (omb.sh kafka-reset <partitions> / queen-reset $QBIN), then omb.sh run.
# The points file is read on fd 3: every ssh below would otherwise eat the rest of it from stdin.
# Each finished point appends its summary row to ../runs/matrix.txt. Lines starting with # are skipped.
set -u
S=$(cd "$(dirname "$0")" && pwd); H=$(cd "$S/.." && pwd)
QBIN=${QBIN:-nr0}
PAUSE_FILE=${PAUSE_FILE:-$H/runs/PAUSE}
while read -r TAG SYS DRV WL <&3; do
  case "$TAG" in ''|\#*) continue ;; esac
  # ../runs/SKIP-<tag> skips that point (decided while the matrix runs).
  if [ -f "$H/runs/SKIP-$TAG" ]; then echo "[$(date -u +%H:%M:%S)] matrix: skipped $TAG"; echo "$TAG: skipped ($(cat "$H/runs/SKIP-$TAG"))" >> "$H/runs/matrix.txt"; continue; fi
  # A pause between points: while $PAUSE_FILE (default ../runs/PAUSE) exists, wait (the point in flight always finishes first).
  if [ -f "$PAUSE_FILE" ]; then echo "[$(date -u +%H:%M:%S)] matrix: paused before $TAG"; while [ -f "$PAUSE_FILE" ]; do sleep 10; done; echo "[$(date -u +%H:%M:%S)] matrix: resumed"; fi
  P=$(grep -E '^partitionsPerTopic:' "$H/workloads/$WL" | awk '{print $2}')
  echo "[$(date -u +%H:%M:%S)] matrix: $TAG ($SYS, $DRV, $WL, $P partitions)"
  if [ "$SYS" = kafka ]; then bash "$S/omb.sh" kafka-reset "$P" < /dev/null || { echo "$TAG: kafka-reset failed" >> "$H/runs/matrix.txt"; continue; }
  else bash "$S/omb.sh" queen-reset "$QBIN" < /dev/null || { echo "$TAG: queen-reset failed" >> "$H/runs/matrix.txt"; continue; }; fi
  SYSTEM=$SYS QBIN=$QBIN bash "$S/omb.sh" run "$TAG" "$DRV" "$WL" > "$H/runs/$TAG.out" 2>&1 < /dev/null
  tail -1 "$H/runs/$TAG.out" >> "$H/runs/matrix.txt"
  echo "[$(date -u +%H:%M:%S)] matrix: done $(tail -1 "$H/runs/$TAG.out")"
done 3< "${1:?points file}"
bash "$S/omb.sh" stop-all
echo "[$(date -u +%H:%M:%S)] matrix: all points done"
