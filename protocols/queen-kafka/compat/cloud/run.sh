#!/usr/bin/env bash
# queen-kafka compat: QUEEN CLOUD acceptance
#
# Runs the cloud suite against a cell that is ALREADY UP. Nothing here starts or
# stops a Postgres, a broker, a proxy or a facade -- that is rig-cloud.sh's job,
# or yours. Every address and every credential comes from the environment, so
# this can be pointed at a real staging cell without editing a line of Go.
#
# REQUIRED:
#   QKC_BOOTSTRAP      the facade's Kafka listener, e.g. 127.0.0.1:33044
#   QKC_KEY_A_FULL     tenant A, scopes produce+consume+admin+read
#   QKC_KEY_B_FULL     tenant B, the same scopes on a DIFFERENT cluster
#
# OPTIONAL -- each unset variable SKIPS the scenario that needs it, loudly:
#   QKC_KEY_A_CONSUME  tenant A, {consume, read}   (scope refusal on CreateTopics)
#   QKC_KEY_A_PRODUCE  tenant A, {produce, read}   (a producer cannot consume)
#   QKC_KEY_A_TXN      tenant A, {produce, consume, read} (the txn producer)
#   QKC_KEY_A_NOREAD   tenant A, {consume} ONLY    (cannot even authenticate)
#   QKC_KEY_A_FULL2    a SECOND full key of tenant A (the /auth/me identity fix)
#   QKC_PROXY_URL      the proxy, for the console read and the raw KV batch
#   QKC_PSQL           script taking one SQL string, running it against pxdb
#   QKC_PSQL_CELL      the same, against the cell's Postgres
#   QKC_CLUSTER_A/_B   the two cluster uuids (metering and limit overrides)
#   QKC_FACADE_LOG     the facade's log file (the /auth/me resolution line)
#
# MEASUREMENTS, printed and never asserted as a target:
#   QKC_UPSTREAM_TIMEOUT_MS  the proxy's QUEEN_PROXY_UPSTREAM_TIMEOUT_MS (35000)
#   QKC_PARTITIONS           the facade's QUEEN_KAFKA_DEFAULT_PARTITIONS (4)
#   RUN_ID                   suffix on every topic and group (default: epoch)
#
# Any extra arguments go straight to `go test`:  ./run.sh -run TestMetering -v
set -euo pipefail

cd "$(dirname "$0")"

: "${QKC_BOOTSTRAP:?set QKC_BOOTSTRAP, e.g. 127.0.0.1:33044}"
: "${QKC_KEY_A_FULL:?set QKC_KEY_A_FULL -- tenant A's full-scope Queen api key}"
: "${QKC_KEY_B_FULL:?set QKC_KEY_B_FULL -- tenant B's full-scope Queen api key}"

export QKC_PARTITIONS="${QKC_PARTITIONS:-4}"
export QKC_UPSTREAM_TIMEOUT_MS="${QKC_UPSTREAM_TIMEOUT_MS:-35000}"
export RUN_ID="${RUN_ID:-$(date +%s)}"

# GOWORK=off is mandatory: the repository's root go.work does not list this
# module, so a bare `go test` refuses to build it.
# -count=1 is mandatory: without it Go replays a cached PASS and the suite
# proves nothing about the cell that is running right now.
export GOWORK=off

echo "queen-kafka compat: QUEEN CLOUD"
echo "  bootstrap  $QKC_BOOTSTRAP"
echo "  proxy      ${QKC_PROXY_URL:-(unset: the console and raw-KV checks will SKIP)}"
echo "  pxdb       ${QKC_PSQL:-(unset: metering and the limit overrides will SKIP)}"
echo "  cell PG    ${QKC_PSQL_CELL:-(unset: the offset-store reads will SKIP)}"
echo "  facade log ${QKC_FACADE_LOG:-(unset: the /auth/me line cannot be read)}"
echo "  partitions $QKC_PARTITIONS   upstream timeout ${QKC_UPSTREAM_TIMEOUT_MS}ms"
echo "  runID      $RUN_ID"
echo

# 600s: the long-poll scenario alone budgets 30 seconds of wall clock, and the
# rate-cap one waits out a token bucket.
exec go test -count=1 -timeout 600s -v "$@" .
