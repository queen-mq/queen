#!/usr/bin/env bash
# run2.sh <tool> <tag> <start-delay-s> -- <common args>: two processes (loader-index 0/1, 3 consumers each, cons-total 6), shared start file
set -u
TOOL=$1; TAG=$2; DELAY=$3; shift 4
rm -f /work/$TAG.start /work/$TAG-*.log /work/$TAG-*.json
for i in 0 1; do
  /bench/bin/$TOOL -loader-index $i -loaders 2 -consumers 3 -cons-offset $((i*3)) -cons-total 6 -local-src 0-1 \
     -start-file /work/$TAG.start -out /work/$TAG-$i.json -tag $TAG "$@" > /work/$TAG-$i.log 2>&1 &
done
# wait for both READY
for s in $(seq 1 600); do
  n=$(grep -l "^READY " /work/$TAG-0.log /work/$TAG-1.log 2>/dev/null | wc -l)
  [ "$n" = 2 ] && break
  sleep 0.1
done
echo "ready after ${s}00 ms polls: $(grep -h '^READY' /work/$TAG-*.log | tr '\n' ' ')"
echo $(( $(date +%s%3N) + DELAY*1000 )) > /work/$TAG.start.tmp && mv /work/$TAG.start.tmp /work/$TAG.start
wait
echo done
