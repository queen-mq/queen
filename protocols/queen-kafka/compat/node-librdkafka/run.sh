#!/usr/bin/env bash
# Both Node librdkafka bindings against an ALREADY-RUNNING queen-kafka facade.
#
#   ./run.sh                  # both suites + the header probe
#   ./run.sh node-rdkafka     # just node-rdkafka
#   ./run.sh confluent        # just @confluentinc/kafka-javascript
#   ./run.sh probe            # just the cross-binding header probe
#
# This script starts NOTHING. The stack is compat/rig.sh's job, or yours; every
# address comes from the environment so it can slot into the rig later without
# an edit:
#
#   KAFKA_BOOTSTRAP        plaintext facade            (default 127.0.0.1:19092)
#   RUN_ID                 topic/group suffix          (default: epoch seconds)
#   KAFKA_SASL_BOOTSTRAP   SASL listener; unset skips the SASL lane entirely
#   KAFKA_SASL_PROTOCOL    sasl_ssl (default) | sasl_plaintext
#   KAFKA_SASL_TOKEN       the Queen bearer token (this is the SASL *password*)
#   KAFKA_SSL_CA           PEM to verify the listener with; verification stays ON
#   KAFKA_SSL_INSECURE=1   skip verification (an advertised name with no SAN)
#   RUN_IDEMPOTENCE=1      also record how enable.idempotence=true dies
#   TRACE_DIR              where to keep each run's full librdkafka debug stream
#
# NOTE ON THE SASL LANE. node-rdkafka builds librdkafka from source at install
# time and on a stock macOS box that build has NO `ssl` feature, so it cannot
# speak SASL_SSL at all. It CAN speak SASL_PLAINTEXT. If you point both suites at
# one SASL_SSL listener, node-rdkafka's lane will say so and fail honestly rather
# than pretend. To exercise both, run a second facade with QUEEN_KAFKA_SASL=plain
# and no TLS cert, and set KAFKA_SASL_BOOTSTRAP_PLAINTEXT to it.
set -uo pipefail
cd "$(dirname "$0")"

WHICH="${1:-all}"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:19092}"
export RUN_ID="${RUN_ID:-$(date +%s)}"
TRACE_DIR="${TRACE_DIR:-${TMPDIR:-/tmp}/node-librdkafka-compat.$RUN_ID}"
mkdir -p "$TRACE_DIR"

if [ ! -d node_modules ]; then
  echo "==> npm install (node-rdkafka compiles librdkafka from source; this is the slow part)"
  npm install --no-audit --no-fund || exit 1
fi

fail=0
run_one() {
  local label="$1" file="$2"
  shift 2
  echo
  echo "############################################################"
  echo "# $label"
  echo "############################################################"
  # macOS has no timeout(1); node owns its own deadlines (see fixture.mjs) and
  # this is the outer belt in case a native module wedges below JS.
  NEGOTIATED_TRACE_FILE="$TRACE_DIR/$label.trace.log" \
    node "$file" "$KAFKA_BOOTSTRAP" "$RUN_ID" "$@"
  local rc=$?
  [ $rc -ne 0 ] && fail=$((fail + 1))
  echo "# $label exit=$rc"
  return 0
}

case "$WHICH" in
  node-rdkafka) run_one node-rdkafka node_rdkafka.mjs ;;
  confluent)    run_one confluent-kafka-javascript confluent_kafka_js.mjs ;;
  probe)        run_one header-probe probe-headers.mjs ;;
  all)
    # node-rdkafka has no `ssl` in a stock source build, so when a PLAINTEXT SASL
    # listener was supplied it gets that one instead of the TLS listener. Without
    # it, node-rdkafka simply runs against whatever KAFKA_SASL_BOOTSTRAP names and
    # says out loud if its build cannot manage the protocol.
    if [ -n "${KAFKA_SASL_BOOTSTRAP_PLAINTEXT:-}" ]; then
      KAFKA_SASL_BOOTSTRAP="$KAFKA_SASL_BOOTSTRAP_PLAINTEXT" \
        KAFKA_SASL_PROTOCOL=sasl_plaintext \
        run_one node-rdkafka node_rdkafka.mjs
    else
      run_one node-rdkafka node_rdkafka.mjs
    fi
    run_one confluent-kafka-javascript confluent_kafka_js.mjs
    run_one header-probe probe-headers.mjs
    ;;
  *)
    echo "unknown target: $WHICH (want: all | node-rdkafka | confluent | probe)" >&2
    exit 2
    ;;
esac

echo
echo "traces: $TRACE_DIR"
if [ "$fail" -eq 0 ]; then
  echo "RESULT: PASS (node-librdkafka)"
else
  echo "RESULT: FAIL ($fail suite(s)) (node-librdkafka)"
fi
exit $((fail == 0 ? 0 : 1))
