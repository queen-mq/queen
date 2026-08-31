#!/usr/bin/env bash
#
# The kafka-python row of the M6 client matrix.
#
#   protocols/queen-kafka/compat/kafka-python/run.sh [scenario]
#
# It assumes a stack is ALREADY RUNNING and starts nothing: the stack is
# rig.sh's job, or yours. Everything it needs comes from the environment.
#
#   KAFKA_BOOTSTRAP        plaintext listener        (default 127.0.0.1:19092)
#   RUN_ID                 topic/group suffix        (default: a timestamp)
#   QUEEN_KAFKA_PARTITIONS what the facade was booted with (default 8)
#   KAFKA_TLS_BOOTSTRAP    SASL_SSL listener, if you want the M5 lane
#   QUEEN_KAFKA_SASL_TOKEN the bearer token = the SASL password
#   QUEEN_KAFKA_TLS_CA     PEM for that listener
#   KAFKA_PYTHON_SPEC      what to install (default: kafka-python==2.3.2)
#   KAFKA_PYTHON_VENV      where to build it (default: ./.venv-<spec>)
#   QUEEN_KAFKA_PYTHON_API_VERSION  pin an explicit api_version, e.g. 0.11.0
#
# Scenarios: probe | compat | sasl | raw | all   (default: probe + compat)
#
# WHY A VERSION KNOB. kafka-python is four different clients wearing one name,
# and they disagree about the two things this facade is picky about:
#
#   2.0.2            infers a Kafka RELEASE from ApiVersions; sends Fetch v4;
#                    raw v0 SASL framing; DEAD on Python 3.12 (no
#                    kafka.vendor.six.moves), so it needs a <=3.11 interpreter
#   2.3.2            per-API clamping; Fetch v6; SaslAuthenticate v0
#   3.0.11           per-API clamping; Fetch v6; SaslAuthenticate v1
#   kafka-python-ng  a fork of the 2.0.2 line; behaves like it
#
# Run it more than once with different KAFKA_PYTHON_SPEC values if you want the
# matrix; one run tests one client.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCENARIO="${1:-default}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:19092}"
RUN="${RUN_ID:-$(date +%s)}"
SPEC="${KAFKA_PYTHON_SPEC:-kafka-python==2.3.2}"
VENV="${KAFKA_PYTHON_VENV:-$HERE/.venv-${SPEC//[^A-Za-z0-9._-]/_}}"
PY="${KAFKA_PYTHON_INTERPRETER:-python3}"
FAIL=0

say() { printf '\n===== %s\n' "$*"; }

# macOS has no timeout(1) and this suite must never be the thing that hangs a
# rig run. Every scenario goes through this.
limit() {
  local secs="$1"; shift
  "$@" &
  local pid=$! i=0
  while [ $i -lt $((secs * 10)) ]; do
    kill -0 "$pid" 2>/dev/null || { wait "$pid"; return $?; }
    sleep 0.1; i=$((i + 1))
  done
  echo "  !!   TIMED OUT after ${secs}s: $*" >&2
  kill -9 "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null
  return 124
}

# ------------------------------------------------------------------- the venv
if [ ! -x "$VENV/bin/python" ]; then
  say "creating $VENV for $SPEC"
  "$PY" -m venv "$VENV" || exit 1
  # lz4 / zstandard / python-snappy are the codec libraries kafka-python calls
  # out to; without them it refuses those compression_type values at construction
  # and the codec section reports them as unavailable rather than testing them.
  "$VENV/bin/pip" -q install --disable-pip-version-check \
    "$SPEC" lz4 zstandard python-snappy || {
      echo "pip install failed; retrying without python-snappy (it needs libsnappy)" >&2
      "$VENV/bin/pip" -q install --disable-pip-version-check "$SPEC" lz4 zstandard || exit 1
    }
fi
"$VENV/bin/python" -c 'import kafka; print("kafka-python", kafka.__version__)' || exit 1

run() {  # run <seconds> <script> [args...]
  local secs="$1" script="$2"; shift 2
  say "$script $*"
  ( cd "$HERE" && limit "$secs" "$VENV/bin/python" "$script" "$@" )
  local rc=$?
  [ $rc -eq 0 ] || FAIL=1
  return $rc
}

case "$SCENARIO" in
  probe)   run 400 probe_api_version.py "$BOOTSTRAP" "$RUN" ;;
  compat)  run 1000 compat.py "$BOOTSTRAP" "$RUN" ;;
  sasl)    run 500 sasl_tls.py "${KAFKA_TLS_BOOTSTRAP:?set KAFKA_TLS_BOOTSTRAP}" "$RUN" ;;
  raw)     run 200 raw_sasl_probe.py "${KAFKA_TLS_BOOTSTRAP:?set KAFKA_TLS_BOOTSTRAP}" ;;
  all)
    run 400 probe_api_version.py "$BOOTSTRAP" "$RUN"
    run 1000 compat.py "$BOOTSTRAP" "$RUN"
    if [ -n "${KAFKA_TLS_BOOTSTRAP:-}" ]; then
      run 200 raw_sasl_probe.py "$KAFKA_TLS_BOOTSTRAP"
      run 500 sasl_tls.py "$KAFKA_TLS_BOOTSTRAP" "$RUN"
    else
      say "KAFKA_TLS_BOOTSTRAP unset: skipping the SASL/TLS lane"
    fi
    ;;
  default)
    run 400 probe_api_version.py "$BOOTSTRAP" "$RUN"
    run 1000 compat.py "$BOOTSTRAP" "$RUN"
    ;;
  *) echo "unknown scenario: $SCENARIO (probe|compat|sasl|raw|all)" >&2; exit 2 ;;
esac

say "kafka-python: $([ $FAIL -eq 0 ] && echo 'RESULT: PASS' || echo 'RESULT: FAIL')"
exit $FAIL
