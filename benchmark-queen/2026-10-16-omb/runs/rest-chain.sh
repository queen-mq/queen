#!/usr/bin/env bash
# After the main matrix (pid 4901): the coordinator's 1M rechecks first (short), then family 2 (many topics, long).
# Replaces the two waiters started 12:53 local (f2 after 4901, final after f2).
cd /Users/alice/Work/queen/benchmark-queen/2026-10-16-omb
while kill -0 4901 2>/dev/null; do sleep 20; done; sleep 5
bash scripts/omb.sh stop-all > /dev/null 2>&1
QBIN=nr2 RUN_TIMEOUT=1800 bash scripts/matrix.sh runs/points-final.txt > runs/matrix-final.log 2>&1
bash scripts/omb.sh stop-all > /dev/null 2>&1
QEXTRA_ENV='QUEEN_QLOG_SHARDS=16' QBIN=nr2 RUN_TIMEOUT=5400 bash scripts/matrix.sh runs/points-f2.txt > runs/matrix-f2.log 2>&1
