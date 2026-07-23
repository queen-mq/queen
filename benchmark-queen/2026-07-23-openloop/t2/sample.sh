#!/bin/bash
# Usage: sample.sh LABEL DURATION_S
# Samples xact_commit every ~10s into a CSV, and grabs a 5x1s pg_stat_activity
# wait histogram once mid-run.
LABEL="$1"
DUR="${2:-100}"
OUT=/root/t2out
mkdir -p "$OUT"
CSV="$OUT/commit_${LABEL}.csv"
WHIST="$OUT/waits_${LABEL}.txt"
echo "epoch,xact_commit" > "$CSV"
: > "$WHIST"
PSQL="docker exec qbench-pg psql -U postgres -At"
START=$(date +%s)
END=$(( START + DUR ))
WAITDONE=0
while :; do
  NOW=$(date +%s)
  [ "$NOW" -ge "$END" ] && break
  ROW=$($PSQL -c "SELECT xact_commit FROM pg_stat_database WHERE datname='postgres'" 2>/dev/null)
  echo "${NOW},${ROW}" >> "$CSV"
  ELAPSED=$(( NOW - START ))
  if [ "$WAITDONE" -eq 0 ] && [ "$ELAPSED" -ge $(( DUR / 2 )) ]; then
    for i in 1 2 3 4 5; do
      echo "=== sample $i $(date +%s) ===" >> "$WHIST"
      $PSQL -F'|' -c "SELECT wait_event_type, wait_event, count(*) FROM pg_stat_activity WHERE state='active' AND pid<>pg_backend_pid() GROUP BY 1,2 ORDER BY 3 DESC" >> "$WHIST" 2>/dev/null
      sleep 1
    done
    WAITDONE=1
  fi
  sleep 10
done
echo "SAMPLER_DONE ${LABEL}"
