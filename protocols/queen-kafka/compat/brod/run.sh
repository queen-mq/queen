#!/usr/bin/env bash
#
# The brod (Erlang/OTP) row of the M6 client matrix.
#
#   protocols/queen-kafka/compat/brod/run.sh [scenario]
#
# It assumes a stack is ALREADY RUNNING and starts nothing: the stack is
# rig.sh's job, or yours. Everything it needs comes from the environment.
#
#   KAFKA_BOOTSTRAP        plaintext listener   (default 127.0.0.1:19092)
#   RUN_ID                 topic/group suffix   (default: a timestamp)
#   KAFKA_TLS_BOOTSTRAP    SASL_SSL listener, if you want the M5 lane
#   QUEEN_KAFKA_SASL_TOKEN the bearer token = the SASL password
#   QUEEN_KAFKA_SASL_USER  the SASL username, a free label (default: brod)
#   BROD_RUNNER            docker | host        (default: auto)
#   BROD_IMAGE             the erlang image      (default qkcompat-brod-erl:27)
#
# Scenarios: versions | produce | codecs | offsets | resume | probes | sasl | all
#
# WHY THERE IS A CONTAINER IN HERE AT ALL, when no other suite in compat/ has
# one. The other four clients (franz-go, kafkajs, librdkafka, Java) have host
# toolchains on the machine this was written on; Erlang does not, and "install
# Erlang" is a bigger ask than "docker run erlang:27", which is also closer to
# what an Erlang shop's CI actually does. If you DO have rebar3 on the host,
# set BROD_RUNNER=host and this runs natively with no container at all -- the
# Erlang source is identical either way.
#
# THE ONE THING THE CONTAINER CHANGES is the address it has to dial. A
# container cannot reach a facade advertising 127.0.0.1, and the host cannot
# resolve host.docker.internal. So: point KAFKA_BOOTSTRAP at whatever the
# facade ADVERTISES, and pick the runner that can reach it. With
# BROD_RUNNER=docker and a bootstrap naming 127.0.0.1, this script says so
# rather than letting brod fail with a resolution error that looks like a
# facade bug.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCENARIO="${1:-all}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:19092}"
RUN="${RUN_ID:-$(date +%s)}"
IMAGE="${BROD_IMAGE:-qkcompat-brod-erl:27}"
FAIL=0

if [ "$SCENARIO" = "sasl" ]; then
  BOOTSTRAP="${KAFKA_TLS_BOOTSTRAP:?set KAFKA_TLS_BOOTSTRAP for the sasl scenario}"
  : "${QUEEN_KAFKA_SASL_TOKEN:?set QUEEN_KAFKA_SASL_TOKEN for the sasl scenario}"
fi

# --------------------------------------------------------------- which runner
RUNNER="${BROD_RUNNER:-auto}"
if [ "$RUNNER" = "auto" ]; then
  if command -v rebar3 >/dev/null 2>&1; then RUNNER=host; else RUNNER=docker; fi
fi

case "$BOOTSTRAP" in
  127.0.0.1:*|localhost:*|\[::1\]:*)
    if [ "$RUNNER" = "docker" ]; then
      echo "run.sh: BOOTSTRAP is $BOOTSTRAP but the runner is docker." >&2
      echo "        A container cannot reach a facade advertising loopback." >&2
      echo "        Point KAFKA_BOOTSTRAP at the ADVERTISED address" >&2
      echo "        (host.docker.internal:<port>), or set BROD_RUNNER=host." >&2
      exit 2
    fi ;;
  host.docker.internal:*)
    if [ "$RUNNER" = "host" ]; then
      echo "run.sh: BOOTSTRAP is $BOOTSTRAP but the runner is host." >&2
      echo "        macOS does not resolve host.docker.internal. Use a facade" >&2
      echo "        advertising 127.0.0.1, or set BROD_RUNNER=docker." >&2
      exit 2
    fi ;;
esac

# macOS has no timeout(1) and this suite must never be the thing that hangs a
# rig run.
limit() {
  local secs="$1"; shift
  "$@" &
  local pid=$! i=0
  while [ $i -lt $((secs * 10)) ]; do
    kill -0 "$pid" 2>/dev/null || { wait "$pid"; return $?; }
    sleep 0.1; i=$((i + 1))
  done
  echo "  !!   TIMED OUT after ${secs}s" >&2
  kill -9 "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null
  return 124
}

ERL_ARGS=(-noshell -run qk_brod main "$BOOTSTRAP" "$RUN" "$SCENARIO")

# BROD_PATCH_TXNID=1 applies compat/brod/patch-kpro-txnid.sh, a one-line change
# to the FETCHED kafka_protocol source in _build/ that makes a non-transactional
# Produce send a null transactional_id instead of an empty string. Without it
# stock brod cannot produce to queen-kafka at all. Read that script's header for
# the whole story. Default is OFF: the default run shows what a real Erlang shop
# actually gets.
if [ "${BROD_PATCH_TXNID:-0}" = "1" ]; then
  BUILD='rebar3 compile >/dev/null && ./patch-kpro-txnid.sh'
  echo "==> BROD_PATCH_TXNID=1: kafka_protocol will be patched (see patch-kpro-txnid.sh)"
else
  # _build persists between runs, so an unpatched run after a patched one would
  # quietly reuse the patched beam and report a PASS stock brod cannot get.
  # Revert unconditionally; it is a no-op when nothing was patched.
  BUILD='rebar3 compile >/dev/null && ./patch-kpro-txnid.sh --revert'
fi

run_host() {
  ( cd "$HERE" && eval "$BUILD" ) || return 1
  ( cd "$HERE" && erl -pa _build/default/lib/*/ebin "${ERL_ARGS[@]}" )
}

run_docker() {
  # The image is erlang:27 plus cmake; crc32cer and snappyer build NIFs with it
  # and the stock image has no cmake. Built here if it is missing so a fresh
  # checkout needs no separate step.
  if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "==> building $IMAGE (erlang:27 + cmake)"
    docker build -t "$IMAGE" "$HERE" >/dev/null || return 1
  fi
  mkdir -p "$HERE/.rebar-cache"
  # --user keeps _build/ owned by the invoking user rather than root.
  # HOME must be writable: rebar3 keeps its hex cache there.
  docker run --rm \
    -v "$HERE":/work \
    -v "$HERE/.rebar-cache":/cache \
    -e HOME=/cache \
    -e "QUEEN_KAFKA_SASL_TOKEN=${QUEEN_KAFKA_SASL_TOKEN:-}" \
    -e "QUEEN_KAFKA_SASL_USER=${QUEEN_KAFKA_SASL_USER:-brod}" \
    -w /work \
    --user "$(id -u):$(id -g)" \
    "$IMAGE" \
    sh -c "$BUILD"' && exec erl -pa _build/default/lib/*/ebin '"$(printf '%q ' "${ERL_ARGS[@]}")"
}

printf '===== brod compat: scenario=%s runner=%s bootstrap=%s\n' \
  "$SCENARIO" "$RUNNER" "$BOOTSTRAP"

case "$RUNNER" in
  host)   limit 900 run_host ;;
  docker) limit 900 run_docker ;;
  *) echo "unknown BROD_RUNNER: $RUNNER (host|docker)" >&2; exit 2 ;;
esac
rc=$?
[ $rc -eq 0 ] || FAIL=$rc

printf '\n===== brod: %s\n' \
  "$([ $FAIL -eq 0 ] && echo 'RESULT: PASS' || echo "RESULT: FAIL ($FAIL)")"
exit $FAIL
