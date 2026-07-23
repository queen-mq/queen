#!/usr/bin/env bash
# Go integration suite: main API tests + streams_integration.
set -eu

# Main suite reads QUEEN_SERVER_URL; the streams suite reads QUEEN_URL. They are
# not aliases — set both, or one half targets localhost and silently skips.
export QUEEN_SERVER_URL="$QUEEN_HTTP_URL"
export QUEEN_URL="$QUEEN_HTTP_URL"
export PG_HOST="$QUEEN_PG_HOST" PG_PORT="$QUEEN_PG_PORT" \
       PG_DB="$QUEEN_PG_DB" PG_USER="$QUEEN_PG_USER" PG_PASSWORD="$QUEEN_PG_PASSWORD"

/usr/local/bin/wait-for-broker

cd /src
# -count=1 disables the test cache so every run actually hits the live broker.
exec go test -count=1 -v ./tests/ ./tests/streams_integration/
