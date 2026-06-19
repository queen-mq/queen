#!/usr/bin/env bash
# Launch the locally-built queen-server pointed at the experiment Postgres, with
# an environment tuned for a CLEAN fusion measurement.
#
# Key choices:
#   * STATS_RECONCILE_INTERVAL_MS / STATS_INTERVAL_MS pushed way up. The 0.16
#     background stats subsystem costs O(total-partitions) and is LOAD-INDEPENDENT
#     -- at low offered rate it otherwise dominates PG CPU and masks the per-commit
#     signal we are trying to isolate. We are not measuring stats here, so silence it.
#   * RETENTION_INTERVAL high so background DELETE churn doesn't perturb mid-sweep.
#   * Fusion knobs left at their defaults but exported so you can sweep them:
#       QUEEN_PUSH_PREFERRED_BATCH_SIZE (50), QUEEN_PUSH_MAX_HOLD_MS (20),
#       QUEEN_PUSH_MAX_BATCH_SIZE (500), QUEEN_PUSH_MAX_CONCURRENT (24).
#   * QUEEN_PUSH_BATCH_INFLIGHT_THRESHOLD=0 (default): even a lone request waits
#     up to MAX_HOLD_MS, so the low-rate end of the curve is genuinely "1 commit
#     per message". Set it to 1 to instead fire immediately when nothing is
#     in-flight (lower latency, NO fusion until a backlog builds) -- this reshapes
#     the low-load knee, so it's worth a second sweep.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
BIN="${QUEEN_BIN:-$REPO_ROOT/server/bin/queen-server}"

if [[ ! -x "$BIN" ]]; then
  echo "[run-broker] queen-server not found at $BIN -- build it: (cd server && make all)" >&2
  exit 1
fi

# The server resolves its schema dir RELATIVE TO CWD (it probes ./schema, ../schema,
# lib/schema, ../lib/schema, ...). Run from the repo root so it finds lib/schema and
# bootstraps queen.* on first start -- otherwise pushes silently fall back to the
# QoS-0 file buffer and never touch Postgres (so the experiment would measure nothing).
if [[ ! -f "$REPO_ROOT/lib/schema/schema.sql" ]]; then
  echo "[run-broker] ERROR: $REPO_ROOT/lib/schema/schema.sql not found; broker cannot bootstrap schema" >&2
  exit 1
fi
cd "$REPO_ROOT"

export PG_HOST="${PG_HOST:-localhost}"
export PG_PORT="${PG_PORT:-5439}"
export PG_USER="${PG_USER:-postgres}"
export PG_PASSWORD="${PG_PASSWORD:-postgres}"
export PG_DB="${PG_DB:-queen}"

export HOST="${HOST:-0.0.0.0}"
export PORT="${PORT:-6632}"
export NUM_WORKERS="${NUM_WORKERS:-4}"
export DB_POOL_SIZE="${DB_POOL_SIZE:-20}"
export SIDECAR_POOL_SIZE="${SIDECAR_POOL_SIZE:-40}"
export LOG_LEVEL="${LOG_LEVEL:-info}"

# Silence load-independent background maintenance so it doesn't swamp low-load PG CPU.
export STATS_INTERVAL_MS="${STATS_INTERVAL_MS:-60000}"
export STATS_RECONCILE_INTERVAL_MS="${STATS_RECONCILE_INTERVAL_MS:-600000}"
export RETENTION_INTERVAL="${RETENTION_INTERVAL:-3600000}"

# Fusion policy (defaults shown; override to sweep the knobs).
export QUEEN_PUSH_PREFERRED_BATCH_SIZE="${QUEEN_PUSH_PREFERRED_BATCH_SIZE:-50}"
export QUEEN_PUSH_MAX_HOLD_MS="${QUEEN_PUSH_MAX_HOLD_MS:-20}"
export QUEEN_PUSH_MAX_BATCH_SIZE="${QUEEN_PUSH_MAX_BATCH_SIZE:-500}"
export QUEEN_PUSH_MAX_CONCURRENT="${QUEEN_PUSH_MAX_CONCURRENT:-24}"
export QUEEN_PUSH_BATCH_INFLIGHT_THRESHOLD="${QUEEN_PUSH_BATCH_INFLIGHT_THRESHOLD:-0}"

echo "[run-broker] PG=$PG_HOST:$PG_PORT/$PG_DB  PORT=$PORT  workers=$NUM_WORKERS  sidecar=$SIDECAR_POOL_SIZE"
echo "[run-broker] fusion: preferred=$QUEEN_PUSH_PREFERRED_BATCH_SIZE hold=${QUEEN_PUSH_MAX_HOLD_MS}ms maxBatch=$QUEEN_PUSH_MAX_BATCH_SIZE maxConc=$QUEEN_PUSH_MAX_CONCURRENT inflightThresh=$QUEEN_PUSH_BATCH_INFLIGHT_THRESHOLD"
echo "[run-broker] stats reconcile=${STATS_RECONCILE_INTERVAL_MS}ms (raised to silence O(partitions) noise)"
exec "$BIN"
