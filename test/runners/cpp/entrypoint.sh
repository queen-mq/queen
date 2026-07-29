#!/usr/bin/env bash
# C++ client test: ~40 HTTP assertions against the broker. No Postgres access.
set -eu

/usr/local/bin/wait-for-broker

cd /src/clients/client-cpp
# Proxy contract (bearer/429/403) first: self-contained, no broker involved, so
# a failure here is unambiguously a client-side regression.
./bin/test_retry429

# Broker URL is argv[1] (there is no env override); default would be localhost.
exec ./bin/test_client "$QUEEN_HTTP_URL"
