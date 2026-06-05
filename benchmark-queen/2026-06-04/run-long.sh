#!/bin/bash
# LOADER: launch the long-running balanced producer+consumer (detached).
BROKER="${BROKER:-http://10.114.0.2:6632}"
Q="${Q:-bench-long}"
PW="${PW:-10}"; PC="${PC:-300}"; CW="${CW:-7}"; CC="${CC:-250}"; CB="${CB:-200}"; DUR="${DUR:-36000}"; MAXP="${MAXP:-100}"
cd /root/loader
SERVER_URL=$BROKER QUEUE_NAMES=$Q NUM_WORKERS=$PW CONNECTIONS_PER_WORKER=$PC MAX_PARTITION=$MAXP MSGS_PER_PUSH=10 DURATION=$DUR \
  nohup node bench-producer.js > /tmp/long-prod.log 2>&1 &
echo "producer pid=$! (${PW}x${PC} batch=10)"
SERVER_URL=$BROKER QUEUE_NAMES=$Q NUM_WORKERS=$CW CONNECTIONS_PER_WORKER=$CC CONSUMER_BATCH=$CB DURATION=$DUR \
  nohup node bench-consumer.js > /tmp/long-cons.log 2>&1 &
echo "consumer pid=$! (${CW}x${CC} batch=${CB})"
