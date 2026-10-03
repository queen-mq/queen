#!/usr/bin/env bash
# txn/run-pulsar.sh (Mac) <tag> <total_rate> [KEY=VAL ...] — one transactional point on pulsar: txn/run.sh pulsar ... (its header
# lists every key; runs/txn/pulsar/<tag>/)
exec bash "$(cd "$(dirname "$0")" && pwd)/run.sh" pulsar "$@"
