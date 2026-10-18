#!/usr/bin/env bash
# nr5 = nr4 + produce staging sub-timings: build, distribute, the facade at 1M (12 facade threads), profiled.
set -u; cd "$(dirname "$0")/.."
. ./hosts.env
echo "[$(date -u +%H:%M:%S)] build nr5"
$SSH root@${B_PUB[0]} "cd /root/src/queen/server && . /root/.cargo/env && cargo build --release --locked > /root/build-nr5.log 2>&1; echo rc=\$? >> /root/build-nr5.log; tail -2 /root/build-nr5.log" < /dev/null
$SSH root@${B_PUB[0]} "grep -q '^rc=0' /root/build-nr5.log" < /dev/null || { echo "build failed"; exit 1; }
bash ../2026-10-12-three-node-new-forms/scripts/bins.sh nr5 1 /root/src/queen/server/target/release < /dev/null
TAG=facade-nr5-t12-p100-1000k
QEXTRA_ENV='QUEEN_KAFKA_THREADS=12' bash scripts/omb.sh queen-reset nr5 < /dev/null | tail -1
( until $SSH root@${L_PUB[0]} "grep -q 'Starting benchmark traffic' /root/bench/omb-results/$TAG.log 2>/dev/null" < /dev/null; do sleep 5; done; sleep 30; bash scripts/prof.sh fH 20 < /dev/null > /dev/null 2>&1 ) &
QEXTRA_ENV='QUEEN_KAFKA_THREADS=12' SYSTEM=queen-facade QBIN=nr5 bash scripts/omb.sh run $TAG queen-facade.yaml 1-topic-100-partitions-1kb-4p-4c-1000k.yaml > runs/$TAG.out 2>&1 < /dev/null
wait
tail -1 runs/$TAG.out
python3 scripts/pdiff.py runs/fH-prof
bash scripts/omb.sh stop-all < /dev/null
echo "[$(date -u +%H:%M:%S)] chain nr5 done"
