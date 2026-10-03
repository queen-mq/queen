#!/usr/bin/env bash
# txn/run-redpanda.sh (Mac) <tag> <total_rate> [KEY=VAL ...] — one transactional point on redpanda: txn/run.sh redpanda ... (its header
# lists every key; runs/txn/redpanda/<tag>/)
exec bash "$(cd "$(dirname "$0")" && pwd)/run.sh" redpanda "$@"
