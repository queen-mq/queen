#!/usr/bin/env bash
# ld.sh <tag> <start-at unix ms> <first index> <duration s> : three load processes on this machine (indices i, i+1, i+2 of nine),
# each 111,111 msg/s in units of 100 messages for one partition, 33 consumers each, against the three brokers.
set -u
TAG=$1; START=$2; I0=$3; DUR=${4:-300}; RATE=${RATE:-111111}; PARTS=${PARTS:-500000}
R=/root/p3/runs/$TAG; mkdir -p $R; ulimit -n 1048576
URLS=${URLS:?the three brokers, comma separated: process j talks to the j-th modulo three}
for k in 0 1 2; do
  j=$((I0 + k))
  nohup /root/p3/bin/qload -urls $URLS -topic bench -topics 1 -partitions $PARTS -mode batch -batch 100 -payload 256 \
    -rate $RATE -ramp 10 -duration $DUR -drain 10 -consumers 33 -cons-total 297 -cons-offset $((33 * j)) \
    -loaders 9 -loader-index $j -local-src $I0-$((I0 + 2)) -pop-batch 1000 -pop-width 10 -pop-timeout 2s \
    -ack async -ack-inflight 256 -max-inflight 5000 -report 10 -create=false -configure=false \
    -start-at $START -tag $TAG -out $R/q$j.json > $R/q$j.log 2>&1 </dev/null &
done
wait
echo LD_DONE > $R/done-$I0
