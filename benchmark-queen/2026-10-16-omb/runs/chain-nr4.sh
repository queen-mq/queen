#!/usr/bin/env bash
# nr4 = next-release + the facade's pipelined produce and follower relay: build on broker 1, distribute,
# then the facade at 500k and 1M (all three loaders), a fresh cluster before each.
set -u; cd "$(dirname "$0")/.."
. ./hosts.env
echo "[$(date -u +%H:%M:%S)] build nr4"
$SSH root@${B_PUB[0]} "cd /root/src/queen/server && . /root/.cargo/env && cargo build --release --locked > /root/build-nr4.log 2>&1; echo rc=\$? >> /root/build-nr4.log; tail -2 /root/build-nr4.log" < /dev/null
$SSH root@${B_PUB[0]} "grep -q '^rc=0' /root/build-nr4.log" < /dev/null || { echo "build failed"; exit 1; }
bash ../2026-10-12-three-node-new-forms/scripts/bins.sh nr4 1 /root/src/queen/server/target/release < /dev/null
# omb.sh directly: matrix.sh would wait on runs/PAUSE, the matrix subagent's pause.
for p in "facade-nr4-p100-500k 1-topic-100-partitions-1kb-4p-4c-500k.yaml" "facade-nr4-p100-1000k 1-topic-100-partitions-1kb-4p-4c-1000k.yaml"; do
  set -- $p
  echo "[$(date -u +%H:%M:%S)] point $1"
  bash scripts/omb.sh queen-reset nr4 < /dev/null | tail -1
  SYSTEM=queen-facade QBIN=nr4 bash scripts/omb.sh run "$1" queen-facade.yaml "$2" > "runs/$1.out" 2>&1 < /dev/null
  tail -1 "runs/$1.out" | tee -a runs/matrix-facade-nr4.txt
done
bash scripts/omb.sh stop-all < /dev/null
echo "[$(date -u +%H:%M:%S)] chain nr4 done"
