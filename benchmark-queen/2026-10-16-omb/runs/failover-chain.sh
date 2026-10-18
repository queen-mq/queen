#!/usr/bin/env bash
# After the extra (popBatch) points, while the main matrix still waits on runs/PAUSE: the facade's
# failover test on nr6 (kill -9, then a graceful stop), then the main matrix resumes.
cd /Users/alice/Work/queen/benchmark-queen/2026-10-16-omb
while kill -0 15990 2>/dev/null; do sleep 10; done
echo "[$(date -u +%T)] failover: extra points done; failover runs on nr6 while the main matrix stays paused"
bash failover/run.sh kill-nr6 kill nr6
bash failover/run.sh stop-nr6 stop nr6
bash scripts/omb.sh stop-all > /dev/null 2>&1
echo "[$(date -u +%T)] extra: done; resuming the main matrix"
rm -f runs/PAUSE
