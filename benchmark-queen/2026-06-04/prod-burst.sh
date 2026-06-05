#!/bin/bash
# LOADER: single producer-only burst against the broker. Args: PW PC [DUR]
PW=$1; PC=$2; DUR=${3:-90}
cd /root/loader
SERVER_URL=http://10.114.0.2:6632 QUEUE_NAMES=bench-push NUM_WORKERS="$PW" CONNECTIONS_PER_WORKER="$PC" \
  MAX_PARTITION=1000 MSGS_PER_PUSH=10 DURATION="$DUR" node bench-producer.js 2>&1 \
  | grep -oE '"msgPerSec": *[0-9]+|"p99": *[0-9]+|"errors": *[0-9]+|"non2xx": *[0-9]+|"timeouts": *[0-9]+' | tr '\n' ' '
echo
