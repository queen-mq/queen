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

# Conflation wire contract (PLAN_CONFLATION.md §4, §3.1). Broker-free for the
# same reason: the two things it pins cannot be produced by a live 1.1.0 broker.
# Degrade-loudly needs a response WITHOUT the "conflation":true echo, which only
# a pre-1.1.0 broker emits, and "warns exactly once per (queue, group)" is not a
# thing the broker side can count -- one warning and a thousand look identical
# from there. The end-to-end half of this feature is test/run.sh --suite
# conflation, which drives raw HTTP with no SDK in the way.
./bin/test_conflation

# Broker URL is argv[1] (there is no env override); default would be localhost.
# The KV/timer INTEGRATION tests inside this binary run unconditionally: every
# broker carries both surfaces, so a failure there is a failure and not a
# configuration.
exec ./bin/test_client "$QUEEN_HTTP_URL"
