#!/usr/bin/env bash
#
# The aiokafka row of the M6 client matrix.
#
#   protocols/queen-kafka/compat/aiokafka/run.sh [scenario]
#
# It assumes a stack is ALREADY RUNNING and starts nothing: the stack is
# rig.sh's job, or yours. Everything it needs comes from the environment.
#
#   KAFKA_BOOTSTRAP        plaintext listener        (default 127.0.0.1:19092)
#   RUN_ID                 topic/group suffix        (default: a timestamp)
#   QUEEN_KAFKA_PARTITIONS what the facade was booted with (default 8)
#   KAFKA_TLS_BOOTSTRAP    SASL_SSL listener, if you want the M5 lane
#   QUEEN_KAFKA_SASL_TOKEN the bearer token = the SASL password
#   QUEEN_KAFKA_TLS_CA     PEM for that listener's certificate
#   AIOKAFKA_SPEC          what to install (default: aiokafka[lz4,zstd,snappy])
#   AIOKAFKA_VENV          where to build it (default: ./.venv-<spec>)
#
# Scenarios: compat | sasl | all   (default: compat)
#
# ONE VERSION KNOB, and it matters less than kafka-python's. Since 0.13.0
# aiokafka's `api_version` parameter is DEPRECATED and a documented no-op:
# `request.prepare(self._versions)` (conn.py:414) clamps every request against
# the per-API window read from ApiVersions, which is exactly what this facade
# wants. On 0.12.x and older `api_version` is live and pinning it switches that
# clamp off, which is the same footgun kafka-python has. Set AIOKAFKA_SPEC to
# test an older line.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCENARIO="${1:-compat}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:19092}"
RUN="${RUN_ID:-$(date +%s)}"
SPEC="${AIOKAFKA_SPEC:-aiokafka[lz4,zstd,snappy]}"
VENV="${AIOKAFKA_VENV:-$HERE/.venv-${SPEC//[^A-Za-z0-9._-]/_}}"
PY="${AIOKAFKA_INTERPRETER:-python3}"
FAIL=0

say() { printf '\n===== %s\n' "$*"; }

# macOS has no timeout(1) and this suite must never be the thing that hangs a
# rig run. Every scenario goes through this. The scripts carry their own
# in-process watchdog too; this is the belt to that pair of braces.
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
  # The codec extras pull in cramjam, which carries gzip, snappy, lz4 AND zstd
  # in one wheel. Without it aiokafka refuses those compression_type values at
  # construction and compat.py reports them as unavailable rather than testing
  # them.
  "$VENV/bin/pip" -q install --disable-pip-version-check "$SPEC" || {
    echo "pip install failed for $SPEC; retrying without codec extras" >&2
    "$VENV/bin/pip" -q install --disable-pip-version-check "${SPEC%%[*}" || exit 1
  }
fi
"$VENV/bin/python" -c 'import aiokafka; print("aiokafka", aiokafka.__version__)' || exit 1

run() {  # run <seconds> <script> [args...]
  local secs="$1" script="$2"; shift 2
  say "$script $*"
  ( cd "$HERE" && limit "$secs" "$VENV/bin/python" "$script" "$@" )
  local rc=$?
  [ $rc -eq 0 ] || FAIL=1
  return $rc
}

case "$SCENARIO" in
  compat) run 900 compat.py "$BOOTSTRAP" "$RUN" ;;
  sasl)   run 600 sasl_tls.py "${KAFKA_TLS_BOOTSTRAP:?set KAFKA_TLS_BOOTSTRAP}" "$RUN" ;;
  all)
    run 900 compat.py "$BOOTSTRAP" "$RUN"
    if [ -n "${KAFKA_TLS_BOOTSTRAP:-}" ]; then
      run 600 sasl_tls.py "$KAFKA_TLS_BOOTSTRAP" "$RUN"
    else
      say "KAFKA_TLS_BOOTSTRAP unset: skipping the SASL/TLS lane"
    fi
    ;;
  *) echo "unknown scenario: $SCENARIO (compat|sasl|all)" >&2; exit 2 ;;
esac

say "aiokafka: $([ $FAIL -eq 0 ] && echo 'RESULT: PASS' || echo 'RESULT: FAIL')"
exit $FAIL
