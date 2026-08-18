#!/usr/bin/env bash
# C++ client test: ~40 HTTP assertions against the broker. No Postgres access.
set -eu

/usr/local/bin/wait-for-broker

cd /src/clients/client-cpp
# Proxy contract (bearer/429/403) first: self-contained, no broker involved, so
# a failure here is unambiguously a client-side regression.
./bin/test_retry429

# KV/timer wire contract (PLAN_KV_TIMERS.md §5, §4, §6.3, §8.1/§8.3, §9.6).
# Also broker-free: it asserts the exact JSON body, method and path of every
# operation, which is the half no end-to-end test can see. A bundle whose KV
# riders sit in the wrong place still commits against a live broker -- it merely
# commits without the gate.
./bin/test_kv_timers

# Broker URL is argv[1] (there is no env override); default would be localhost.
# The KV/timer INTEGRATION tests inside this binary run unconditionally: every
# broker carries both surfaces, so a failure there is a failure and not a
# configuration.
exec ./bin/test_client "$QUEEN_HTTP_URL"
