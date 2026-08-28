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
# The retention tests need QUEEN_RETENTION_INTERVAL_MS to know the broker's
# sweep cadence; the compose runner env supplies it. The old note here said we
# left it unset "so it never sweeps other suites mid-run" -- that was wrong on
# its own terms: the var configures the TESTS, not the broker, and the broker
# sweeps every RETENTION_INTERVAL (5s default) whether or not it is set. All it
# ever bought was two tests that skipped silently.
exec go test -v ./tests/... -timeout 10m
