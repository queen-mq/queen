#!/usr/bin/env bash
# C++ client test: ~40 HTTP assertions against the broker. No Postgres access.
set -eu

/usr/local/bin/wait-for-broker

cd /src/clients/client-cpp
# Broker URL is argv[1] (there is no env override); default would be localhost.
exec ./bin/test_client "$QUEEN_HTTP_URL"
