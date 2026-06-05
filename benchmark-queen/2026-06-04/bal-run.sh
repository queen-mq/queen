#!/bin/bash
# LOADER: balanced producer + consumer against the broker. Args: PW PC CW CC CB [DUR]
BROKER="${BROKER:-http://10.114.0.2:6632}"
PW=$1; PC=$2; CW=$3; CC=$4; CB=$5; DUR=${6:-180}
cd /root/loader
SERVER_URL=$BROKER QUEUE_NAMES=bench-bal NUM_WORKERS="$PW" CONNECTIONS_PER_WORKER="$PC" \
  MAX_PARTITION=1000 MSGS_PER_PUSH=10 DURATION="$DUR" node bench-producer.js > /tmp/bal-prod.log 2>&1 &
P=$!
SERVER_URL=$BROKER QUEUE_NAMES=bench-bal NUM_WORKERS="$CW" CONNECTIONS_PER_WORKER="$CC" \
  CONSUMER_BATCH="$CB" DURATION="$DUR" node bench-consumer.js > /tmp/bal-cons.log 2>&1 &
C=$!
sleep $((DUR/2)); echo "mid loadavg=$(cut -d' ' -f1-3 /proc/loadavg) (16 cores)"
wait $P $C
echo "push/s=$(grep -oE '\"msgPerSec\": *[0-9]+' /tmp/bal-prod.log | grep -oE '[0-9]+') prodErr=$(grep -oE '\"errors\": *[0-9]+' /tmp/bal-prod.log | grep -oE '[0-9]+') prodTo=$(grep -oE '\"timeouts\": *[0-9]+' /tmp/bal-prod.log | grep -oE '[0-9]+')"
echo "popReq/s=$(grep -oE '\"reqPerSec\": *[0-9]+' /tmp/bal-cons.log | grep -oE '[0-9]+' | head -1) x batch=$CB ; consErr=$(grep -oE '\"errors\": *[0-9]+' /tmp/bal-cons.log | grep -oE '[0-9]+')"
