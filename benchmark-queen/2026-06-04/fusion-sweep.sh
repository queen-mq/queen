#!/bin/bash
# LOADER: producer-only coalescing ladder (batch=10), ramp connections hard.
set -u
BROKER="${BROKER:-http://10.114.0.2:6632}"
DUR="${DUR:-150}"
cd /root/loader
STEPS=("s1 4 100" "s2 8 150" "s3 12 200" "s4 14 250")
for st in "${STEPS[@]}"; do
  read -r lbl pw pc <<< "$st"
  echo "[$(date -u +%FT%TZ)] >>> $lbl prod=${pw}x${pc} conns=$((pw*pc)) batch=10"
  SERVER_URL=$BROKER QUEUE_NAMES="bench-fz-$lbl" NUM_WORKERS=$pw CONNECTIONS_PER_WORKER=$pc \
    MAX_PARTITION=1000 MSGS_PER_PUSH=10 DURATION=$DUR node bench-producer.js > /tmp/fz-$lbl-prod.log 2>&1 &
  P=$!
  sleep $((DUR/2))
  echo "[$(date -u +%FT%TZ)] $lbl loadavg=$(cut -d' ' -f1-3 /proc/loadavg) (16 cores)"
  wait $P
  echo "[$(date -u +%FT%TZ)] <<< $lbl push=$(grep -oE '\"msgPerSec\": *[0-9]+' /tmp/fz-$lbl-prod.log | grep -oE '[0-9]+') p99=$(grep -oE '\"p99\": *[0-9]+' /tmp/fz-$lbl-prod.log | grep -oE '[0-9]+') err=$(grep -oE '\"errors\": *[0-9]+' /tmp/fz-$lbl-prod.log | grep -oE '[0-9]+')"
done
echo "FUSION-DONE"
