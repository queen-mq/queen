#!/bin/bash
# Runs on the LOADER vm. Drives producer+consumer against the remote broker.
set -u
BROKER="${BROKER:-http://10.114.0.2:6632}"
DUR="${DUR:-240}"
cd /root/loader
run() {
  local name=$1 maxp=$2 msgs=$3 pw=$4 pc=$5 cw=$6 cc=$7 cb=$8
  echo "[$(date -u +%FT%TZ)] >>> $name batch=$msgs prod=${pw}x${pc} cons=${cw}x${cc} cb=$cb"
  SERVER_URL=$BROKER QUEUE_NAMES="bench-$name" NUM_WORKERS=$pw CONNECTIONS_PER_WORKER=$pc \
    MAX_PARTITION=$maxp MSGS_PER_PUSH=$msgs DURATION=$DUR node bench-producer.js > /tmp/$name-prod.log 2>&1 &
  local P=$!
  SERVER_URL=$BROKER QUEUE_NAMES="bench-$name" NUM_WORKERS=$cw CONNECTIONS_PER_WORKER=$cc \
    CONSUMER_BATCH=$cb DURATION=$DUR node bench-consumer.js > /tmp/$name-cons.log 2>&1 &
  local C=$!
  wait $P $C
  echo "[$(date -u +%FT%TZ)] <<< $name done"
}
run bp10  1000 10  1 50 1 50 100
run bp100 1000 100 1 50 1 50 100
echo "SEP-DONE"
