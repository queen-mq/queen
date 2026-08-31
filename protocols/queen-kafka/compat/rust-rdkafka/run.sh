#!/usr/bin/env bash
#
# queen-kafka compat: rdkafka (Rust), over a vendored librdkafka.
#
#   queen-kafka/compat/rust-rdkafka/run.sh [scenario]
#
# It assumes a stack is ALREADY RUNNING and starts nothing: the stack is
# rig.sh's job, or yours. Everything it needs comes from the environment.
#
#   QUEEN_KAFKA_BOOTSTRAP   plaintext listener       (default 127.0.0.1:19092)
#   KAFKA_BOOTSTRAP         accepted as an alias, for parity with the js and
#                           python rows
#   RUN_ID                  topic/group suffix       (default: a timestamp)
#   QUEEN_KAFKA_PARTITIONS  what the facade was booted with (default 8)
#
# The SASL/TLS lane is SKIPPED unless the first two of these are both set:
#
#   QUEEN_KAFKA_TLS_BOOTSTRAP  SASL_SSL listener, e.g. localhost:19093
#   QUEEN_KAFKA_SASL_TOKEN     the bearer token that is also the SASL password
#   QUEEN_KAFKA_TLS_CA         PEM of the listener's certificate. Set it and
#                              librdkafka verifies the chain AND the hostname
#                              for real; leave it unset and the suite falls back
#                              to enable.ssl.certificate.verification=false.
#
# NOTE ON QUEEN_KAFKA_TLS_BOOTSTRAP: point it at a name the certificate covers.
# rig.sh's self-signed cert has SANs kafka.example.com / shared.queenmq.cloud /
# localhost / 127.0.0.1. Prefer `localhost:<port>`: librdkafka's default
# ssl.endpoint.identification.algorithm=https checks the name it dialled against
# the certificate's DNS SANs, and an IP literal is not one of them.
#
# Scenarios: metadata | roundtrip | codecs | resume | autocreate | offsets |
#            idempotence | sasl | all      (default: all)
#
# Exit status is the suite's: 0 on RESULT: PASS, 1 on RESULT: FAIL.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

SCENARIO="${1:-all}"
BOOTSTRAP="${QUEEN_KAFKA_BOOTSTRAP:-${KAFKA_BOOTSTRAP:-127.0.0.1:19092}}"
RUN="${RUN_ID:-$(date +%s)}"

# ---------------------------------------------------------------- the build
#
# rdkafka is built with the `cmake-build` feature, so the FIRST build compiles
# librdkafka 2.12.1 from the crate's vendored submodule (about 15s on an M-series
# Mac; every build after it is a no-op). Nothing needs to be installed for that:
# it was verified from a cold target directory with an empty environment on
# macOS 15 / arm64, cmake 4.2.1, and it needed neither OPENSSL_ROOT_DIR nor
# CMAKE_POLICY_VERSION_MINIMUM — `openssl-sys` finds Homebrew's openssl@3
# through pkg-config on its own, and librdkafka's CMakeLists already declares a
# policy version CMake 4 accepts.
#
# If a host does need a hand, these are the two knobs, and they are left to the
# caller rather than forced here because CMAKE_POLICY_VERSION_MINIMUM changes
# how CMake treats every project it builds:
#
#   OPENSSL_ROOT_DIR=/opt/homebrew/opt/openssl@3   CMake's FindOpenSSL misses a
#                                                  Homebrew keg-only OpenSSL
#   CMAKE_POLICY_VERSION_MINIMUM=3.5               CMake 4 dropped compatibility
#                                                  with cmake_minimum_required
#                                                  below 3.5

echo "queen-kafka compat: rdkafka (Rust)"
echo "  bootstrap  ${BOOTSTRAP}"
echo "  runId      ${RUN}"
echo "  scenario   ${SCENARIO}"
if [ -n "${QUEEN_KAFKA_TLS_BOOTSTRAP:-}" ] && [ -n "${QUEEN_KAFKA_SASL_TOKEN:-}" ]; then
  echo "  tls        ${QUEEN_KAFKA_TLS_BOOTSTRAP} (SASL/PLAIN${QUEEN_KAFKA_TLS_CA:+, chain verified against ${QUEEN_KAFKA_TLS_CA}})"
else
  echo "  tls        skipped (QUEEN_KAFKA_TLS_BOOTSTRAP / QUEEN_KAFKA_SASL_TOKEN unset)"
fi
echo

cargo build --quiet || { echo "cargo build failed" >&2; exit 2; }

# macOS has no timeout(1), and a suite that hangs a rig run reports nothing.
# Every scenario already carries its own per-call deadlines; this is the outer
# belt, in case the process itself wedges.
limit() {
  local secs="$1"; shift
  "$@" &
  local pid=$! i=0
  while [ "$i" -lt $((secs * 10)) ]; do
    kill -0 "$pid" 2>/dev/null || { wait "$pid"; return $?; }
    sleep 0.1; i=$((i + 1))
  done
  echo "  !!   TIMED OUT after ${secs}s" >&2
  kill -9 "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null
  return 124
}

SCENARIO="$SCENARIO" limit 900 ./target/debug/compat "$BOOTSTRAP" "$RUN"
exit $?
