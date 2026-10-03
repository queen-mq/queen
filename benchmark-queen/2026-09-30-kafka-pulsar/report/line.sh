#!/usr/bin/env bash
# line.sh <run dir>... — one compact line per run (for progress updates), from report.py --csv
D=$(cd "$(dirname "$0")" && pwd)
python3 "$D/report.py" --csv "$@" | tail -n +2 | awk -F, '{printf "%-18s in %5s out %5s shed %4s | e2e p50 %4s p99 %5s ms (same-host p99 %5s) | produce p99 %5s ms | %4s broker cores | %s errs\n", $1, $5, $6, $7, $8, $9, $10, $11, $13, $17}'
