#!/usr/bin/env bash
# After the max-rate family: pause the main matrix, deploy the popBatch tarball, run the extra points, resume.
cd /Users/alice/Work/queen/benchmark-queen/2026-10-16-omb
until grep -q "matrix: kafka-max-10t1p-1kb (" runs/matrix-rest.log; do sleep 10; done
touch runs/PAUSE; echo "[$(date -u +%T)] extra: PAUSE set while kafka-max-10t1p-1kb runs"
until grep -q "matrix: paused before" runs/matrix-rest.log; do sleep 10; done
echo "[$(date -u +%T)] extra: main matrix paused; deploying the popBatch tarball"
bash scripts/omb.sh stop-all
bash scripts/omb.sh deploy /private/tmp/claude-502/-Users-alice-Work-queen/9a561819-3d85-429b-8a6b-c43fdbb02338/scratchpad/omb-popbatch-898637956bb48782-bin.tar.gz
QBIN=nr2 RUN_TIMEOUT=1800 bash scripts/matrix.sh runs/points-extra.txt
echo "[$(date -u +%T)] extra: done; resuming the main matrix"
rm -f runs/PAUSE
