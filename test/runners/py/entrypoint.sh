#!/usr/bin/env bash
# Python integration suite: full pytest tree (incl. streams_integration).
set -eu

# conftest reads QUEEN_SERVER_URL; the streams tests read QUEEN_URL. Set both.
export QUEEN_SERVER_URL="$QUEEN_HTTP_URL"
export QUEEN_URL="$QUEEN_HTTP_URL"

/usr/local/bin/wait-for-broker

cd /src
# Loop scope comes from pytest.ini (asyncio_default_fixture_loop_scope=session).
exec pytest tests/
