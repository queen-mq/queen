#!/usr/bin/env bash
# queenctl CLI E2E suite.
set -eu

# The CLI suite uses QUEEN_SERVER (not QUEEN_SERVER_URL) and REQUIRES QUEEN_E2E=1
# — without it TestMain exits 0 having run nothing (a false green).
export QUEEN_SERVER="$QUEEN_HTTP_URL"
export QUEEN_E2E=1
# Per-run queue namespace so a shared broker/PG never collides across runs.
export QUEEN_TEST_QUEUE_PREFIX="${QUEEN_TEST_QUEUE_PREFIX:-ct-e2e-$$}"
# PG_* enable the DB-side assertions; without PG_HOST those tests silently skip.
export PG_HOST="$QUEEN_PG_HOST" PG_PORT="$QUEEN_PG_PORT" \
       PG_DB="$QUEEN_PG_DB" PG_USER="$QUEEN_PG_USER" PG_PASSWORD="$QUEEN_PG_PASSWORD"

/usr/local/bin/wait-for-broker

cd /src/clients/client-cli
# Retention tests skip unless QUEEN_RETENTION_INTERVAL_MS is set to match the
# broker's RETENTION_INTERVAL; we leave the broker at its default so it never
# sweeps other suites mid-run, so those few tests skip by design.
exec go test -v ./tests/... -timeout 10m
