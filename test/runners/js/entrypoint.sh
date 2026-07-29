#!/usr/bin/env bash
# JS integration suite: human + stream buckets in one process.
set -eu

export QUEEN_SERVER_URL="$QUEEN_HTTP_URL"
# The suite reads PG_DB (not PG_DATABASE) for its direct cleanup pool.
export PG_HOST="$QUEEN_PG_HOST" PG_PORT="$QUEEN_PG_PORT" \
       PG_DB="$QUEEN_PG_DB" PG_USER="$QUEEN_PG_USER" PG_PASSWORD="$QUEEN_PG_PASSWORD"

/usr/local/bin/wait-for-broker

cd /suite
# Broker-free unit suites first, including the proxy 429/Retry-After contract
# (test-v2/http-unit). They live behind `npm test`, which the harness does not
# use, so they ran nowhere.
node --test test-v2/http-unit/retry429.test.js

# No argument = human + stream in one process; run.js calls process.exit(fail?1:0).
exec node test-v2/run.js
