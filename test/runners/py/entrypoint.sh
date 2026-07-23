#!/usr/bin/env bash
# Python integration suite: full pytest tree (incl. streams_integration).
set -eu

# conftest reads QUEEN_SERVER_URL; the streams tests read QUEEN_URL. Set both.
export QUEEN_SERVER_URL="$QUEEN_HTTP_URL"
export QUEEN_URL="$QUEEN_HTTP_URL"
export PG_HOST="$QUEEN_PG_HOST" PG_PORT="$QUEEN_PG_PORT" \
       PG_DB="$QUEEN_PG_DB" PG_USER="$QUEEN_PG_USER" PG_PASSWORD="$QUEEN_PG_PASSWORD"

/usr/local/bin/wait-for-broker

cd /src
# Loop scope comes from pytest.ini (asyncio_default_fixture_loop_scope=session).
exec pytest tests/
