#!/usr/bin/env bash
# nr6 = nr4 + staging sub-timings + the catalog's map behind a plain mutex: build, distribute, the facade at 1M (profiled) and 500k, default facade threads.
set -u; cd "$(dirname "$0")/.."
. ./hosts.env
echo "[$(date -u +%H:%M:%S)] build nr6"
$SSH root@${B_PUB[0]} "cd /root/src/queen/server && . /root/.cargo/env && cargo build --release --locked > /root/build-nr6.log 2>&1; echo rc=\$? >> /root/build-nr6.log; tail -2 /root/build-nr6.log" < /dev/null
$SSH root@${B_PUB[0]} "grep -q '^rc=0' /root/build-nr6.log" < /dev/null || { echo "build failed"; exit 1; }
bash ../2026-10-12-three-node-new-forms/scripts/bins.sh nr6 1 /root/src/queen/server/target/release < /dev/null
TAG=facade-nr6-p100-1000k
bash scripts/omb.sh queen-reset nr6 < /dev/null | tail -1
( until $SSH root@${L_PUB[0]} "grep -q 'Starting benchmark traffic' /root/bench/omb-results/$TAG.log 2>/dev/null" < /dev/null; do sleep 5; done; sleep 30; bash scripts/prof.sh fI 20 < /dev/null > /dev/null 2>&1 ) &
SYSTEM=queen-facade QBIN=nr6 bash scripts/omb.sh run $TAG queen-facade.yaml 1-topic-100-partitions-1kb-4p-4c-1000k.yaml > runs/$TAG.out 2>&1 < /dev/null
wait
tail -1 runs/$TAG.out
python3 scripts/pdiff.py runs/fI-prof
bash scripts/omb.sh stop-all < /dev/null
echo "[$(date -u +%H:%M:%S)] chain nr6 done"
TAG=facade-nr6-p100-500k
bash scripts/omb.sh queen-reset nr6 < /dev/null | tail -1
SYSTEM=queen-facade QBIN=nr6 bash scripts/omb.sh run $TAG queen-facade.yaml 1-topic-100-partitions-1kb-4p-4c-500k.yaml > runs/$TAG.out 2>&1 < /dev/null
tail -1 runs/$TAG.out
bash scripts/omb.sh stop-all < /dev/null
echo "[$(date -u +%H:%M:%S)] chain nr6 500k done"
