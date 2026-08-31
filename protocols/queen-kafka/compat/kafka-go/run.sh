#!/usr/bin/env bash
# queen-kafka compat: segmentio/kafka-go
#
# Runs the kafka-go suite against a stack that is ALREADY UP. Nothing here
# starts or stops a broker, a facade or a Postgres — that is rig.sh's job, or
# yours. Every address comes from the environment so this can be wired into
# rig.sh without editing a line of Go.
#
#   QUEEN_KAFKA_BOOTSTRAP     plaintext facade      (default 127.0.0.1:19092)
#   QUEEN_KAFKA_PARTITIONS    the facade's QUEEN_KAFKA_DEFAULT_PARTITIONS (default 8)
#   RUN_ID                    suffix on every topic and group (default: epoch seconds)
#
# Optional, and the SASL/TLS lane is SKIPPED unless the first two are both set:
#   QUEEN_KAFKA_TLS_BOOTSTRAP TLS+SASL facade, e.g. localhost:19093
#   QUEEN_KAFKA_SASL_TOKEN    the bearer token that is also the SASL password
#   QUEEN_KAFKA_TLS_CA        PEM of the listener's certificate. Set it and the
#                             chain is verified for real; leave it unset and the
#                             suite falls back to InsecureSkipVerify.
#
# Any extra arguments are passed straight to `go test`, so
#   ./run.sh -run TestGroupConsumeAll
# works.
#
# NOTE ON QUEEN_KAFKA_TLS_BOOTSTRAP: point it at a name the certificate covers.
# rig.sh's self-signed cert has SANs kafka.example.com / shared.queenmq.cloud /
# localhost / 127.0.0.1. Prefer `localhost:<port>` over `127.0.0.1:<port>`: Go
# sends no SNI for an IP-literal ServerName, so the IP form authenticates but
# the facade logs sni="" and the SNI-forwarding path is never exercised.
set -euo pipefail

cd "$(dirname "$0")"

export QUEEN_KAFKA_BOOTSTRAP="${QUEEN_KAFKA_BOOTSTRAP:-127.0.0.1:19092}"
export QUEEN_KAFKA_PARTITIONS="${QUEEN_KAFKA_PARTITIONS:-8}"
export RUN_ID="${RUN_ID:-$(date +%s)}"

# GOWORK=off is mandatory: the repository's root go.work does not list this
# module, so a bare `go test` refuses to build it.
# -count=1 is mandatory: without it Go silently replays a cached PASS and the
# suite proves nothing about the stack that is running right now.
export GOWORK=off

echo "queen-kafka compat: segmentio/kafka-go"
echo "  bootstrap  ${QUEEN_KAFKA_BOOTSTRAP}"
echo "  partitions ${QUEEN_KAFKA_PARTITIONS}"
echo "  runID      ${RUN_ID}"
if [ -n "${QUEEN_KAFKA_TLS_BOOTSTRAP:-}" ] && [ -n "${QUEEN_KAFKA_SASL_TOKEN:-}" ]; then
  echo "  tls        ${QUEEN_KAFKA_TLS_BOOTSTRAP} (SASL/PLAIN${QUEEN_KAFKA_TLS_CA:+, chain verified against ${QUEEN_KAFKA_TLS_CA}})"
else
  echo "  tls        skipped (QUEEN_KAFKA_TLS_BOOTSTRAP / QUEEN_KAFKA_SASL_TOKEN unset)"
fi
echo

# 600s covers the group formations: every consumer group costs 3s server-side
# (QUEEN_KAFKA_GROUP_JOIN_DELAY_MS) and this suite forms five of them.
exec go test -count=1 -timeout 600s -v "$@" .
