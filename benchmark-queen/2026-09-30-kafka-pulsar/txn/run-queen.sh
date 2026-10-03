#!/usr/bin/env bash
# txn/run-queen.sh (Mac) <tag> <total_rate> [KEY=VAL ...] — one transactional point on queen: txn/run.sh queen ... (its header
# lists every key; runs/txn/queen/<tag>/)
exec bash "$(cd "$(dirname "$0")" && pwd)/run.sh" queen "$@"
