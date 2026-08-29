#!/usr/bin/env bash
# The IBM/sarama row of the M6 client matrix. This script does NOT start a
# stack: point it at one that is already up (compat/rig.sh --keep, or your own)
# and it runs the suite against it.
#
#   ./run.sh                                   # 127.0.0.1:19092, all scenarios
#   ./run.sh 127.0.0.1:19092 myrun             # explicit bootstrap and run id
#   SCENARIO=group ./run.sh                    # one scenario
#
# Environment, all optional:
#   QUEEN_KAFKA_BOOTSTRAP     plaintext listener      (default 127.0.0.1:19092)
#   RUN_ID                    stamped into every topic and group name
#   QUEEN_KAFKA_PARTITIONS    the facade's QUEEN_KAFKA_DEFAULT_PARTITIONS (8)
#   QUEEN_URL                 the broker's HTTP API, for cross-checks
#   SCENARIO                  one of the names in main.go, or "all"
#   SARAMA_VERBOSE            echo sarama's own log as it happens
#
# The SASL/TLS lane runs only when all three of these are set; otherwise the
# `sasl` scenario prints a skip and the rest of the suite is unaffected:
#   QUEEN_KAFKA_TLS_BOOTSTRAP the TLS + SASL/PLAIN listener
#   QUEEN_KAFKA_SASL_TOKEN    the Queen bearer token (= the SASL password)
#   QUEEN_KAFKA_TLS_CERT      PEM of the listener's certificate; without it the
#                             suite falls back to InsecureSkipVerify and says so
#
# GOWORK=off is mandatory inside this repository: the root go.work lists the two
# client modules and not this one.
set -euo pipefail
cd "$(dirname "$0")"

export GOWORK=off
export QUEEN_KAFKA_BOOTSTRAP="${1:-${QUEEN_KAFKA_BOOTSTRAP:-127.0.0.1:19092}}"
export RUN_ID="${2:-${RUN_ID:-$(date +%s)}}"

exec go run . "$QUEEN_KAFKA_BOOTSTRAP" "$RUN_ID" "${SCENARIO:-all}"
