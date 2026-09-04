#!/usr/bin/env bash
# ClickHouse through `clickhouse-local`. The driver runs on the HOST and shells
# out to docker per query: the image has no Python, and the exactness check must
# be the same code for every reader in the lane.
#   compat/readers/clickhouse.sh <samples-dir>
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
exec python3 "$HERE/clickhouse_reader.py" "${1:?usage: clickhouse.sh <samples-dir>}"
