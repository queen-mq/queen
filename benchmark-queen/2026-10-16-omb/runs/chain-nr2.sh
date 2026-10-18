#!/usr/bin/env bash
# build check -> bins nr2 -> reset -> run fC (facade, nr2) with a profile in its measured window.
set -u; cd "$(dirname "$0")/.."
. ./hosts.env
until $SSH root@${B_PUB[0]} "grep -q '^rc=' /root/build-nr2.log"; do sleep 10; done
$SSH root@${B_PUB[0]} "tail -2 /root/build-nr2.log"
$SSH root@${B_PUB[0]} "grep -q '^rc=0' /root/build-nr2.log" || { echo "build failed"; exit 1; }
bash ../2026-10-12-three-node-new-forms/scripts/bins.sh nr2 1 /root/src/queen/server/target/release
OMB_HOSTS=$PWD/hosts-l1.env bash scripts/omb.sh queen-reset nr2
( . ./hosts-l1.env; until $SSH root@${L_PUB[0]} "grep -q 'Starting benchmark traffic' /root/bench/omb-results/fC-nr2-l1-p100-500k.log 2>/dev/null"; do sleep 5; done; sleep 20; OMB_HOSTS=$PWD/hosts-l1.env bash scripts/prof.sh fC 20 ) &
OMB_HOSTS=$PWD/hosts-l1.env SYSTEM=queen-facade QBIN=nr2 bash scripts/omb.sh run fC-nr2-l1-p100-500k queen-facade.yaml 1-topic-100-partitions-1kb-4p-4c-500k.yaml > runs/fC-nr2-l1-p100-500k.out 2>&1
wait
tail -1 runs/fC-nr2-l1-p100-500k.out
python3 scripts/pdiff.py runs/fC-prof
