#!/usr/bin/env bash
# txn/run-kafka.sh (Mac) <tag> <total_rate> [KEY=VAL ...] — one transactional point on kafka: txn/run.sh kafka ... (its header
# lists every key; runs/txn/kafka/<tag>/)
exec bash "$(cd "$(dirname "$0")" && pwd)/run.sh" kafka "$@"
