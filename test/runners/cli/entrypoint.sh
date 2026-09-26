#!/usr/bin/env bash
# queenctl CLI E2E suite.
set -eu

# The CLI suite uses QUEEN_SERVER (not QUEEN_SERVER_URL) and REQUIRES QUEEN_E2E=1
# — without it TestMain exits 0 having run nothing (a false green).
export QUEEN_SERVER="$QUEEN_HTTP_URL"
export QUEEN_E2E=1
# Per-run queue namespace so a broker reused across runs never collides.
export QUEEN_TEST_QUEUE_PREFIX="${QUEEN_TEST_QUEUE_PREFIX:-ct-e2e-$$}"

/usr/local/bin/wait-for-broker

cd /src/clients/client-cli
# The retention tests need QUEEN_RETENTION_INTERVAL_MS to know the broker's
# sweep cadence; the compose runner env supplies it. The var configures the
# TESTS, not the broker: the broker sweeps every RETENTION_INTERVAL (5s default)
# whether or not it is set, and unset it only makes the two tests skip.
exec go test -v ./tests/... -timeout 10m
