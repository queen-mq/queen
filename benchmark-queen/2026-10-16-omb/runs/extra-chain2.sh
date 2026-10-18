#!/usr/bin/env bash
# extra-chain.sh, restarted: its matrix.sh waited on the PAUSE the chain itself had set. The extra points
# now pause on runs/PAUSE-extra instead; the main matrix stays paused on runs/PAUSE until they finish.
cd /Users/alice/Work/queen/benchmark-queen/2026-10-16-omb
echo "[$(date -u +%T)] extra: restarted (the extra matrix had waited on its own PAUSE)"
PAUSE_FILE=runs/PAUSE-extra QBIN=nr2 RUN_TIMEOUT=1800 bash scripts/matrix.sh runs/points-extra.txt
echo "[$(date -u +%T)] extra: done; resuming the main matrix"
rm -f runs/PAUSE
