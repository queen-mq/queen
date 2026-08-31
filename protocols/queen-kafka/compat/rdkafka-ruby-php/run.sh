#!/usr/bin/env bash
# The two thin librdkafka bindings that are not covered anywhere else in compat/:
# the `rdkafka` gem (karafka's core) and php-rdkafka (pecl), against an
# ALREADY-RUNNING queen-kafka facade.
#
#   ./run.sh                 # both suites
#   ./run.sh ruby            # just the rdkafka gem
#   ./run.sh php             # just php-rdkafka
#   ./run.sh probe           # just the compression probes (both bindings)
#   ./run.sh build           # (re)build the two images and stop
#
# This script starts NO STACK. That is compat/rig.sh's job, or yours; every address
# comes from the environment so it can slot into the rig later without an edit:
#
#   KAFKA_BOOTSTRAP        plaintext facade      (default host.docker.internal:19092)
#   RUN_ID                 topic/group suffix    (default: epoch seconds)
#   KAFKA_PARTITIONS       expected topic width  (default 8)
#   KAFKA_SASL_BOOTSTRAP   SASL listener; unset SKIPS the SASL lane entirely
#   KAFKA_SASL_PROTOCOL    sasl_ssl (default) | sasl_plaintext
#   KAFKA_SASL_TOKEN       the Queen bearer token -- this is the SASL *password*
#   KAFKA_SSL_CA           PEM to verify the listener with (mounted into the container)
#   KAFKA_SSL_INSECURE=1   skip certificate verification entirely
#   TRACE_DIR              where each run's librdkafka protocol trace is kept
#   CONTAINER_PREFIX       docker --name prefix   (default qkcompat-ruby-php)
#   REBUILD=1              force a docker build even if the images exist
#
# ---------------------------------------------------------------------------
# THE ONE THING THAT IS DIFFERENT ABOUT THIS DIRECTORY
#
# Every other suite in compat/ runs its client ON THE HOST. These two cannot: the
# rdkafka gem needs Ruby 3.x (macOS ships 2.6) and php-rdkafka needs a pecl build
# against a system librdkafka. Both therefore run INSIDE A CONTAINER, from the
# official ruby: and php: images, and that changes one thing and only one thing --
# THE ADDRESS THE FACADE ADVERTISES.
#
# A container cannot reach a facade that advertises 127.0.0.1: it completes the
# bootstrap Metadata and then dies re-dialling itself. The facade must be started
# with QUEEN_KAFKA_ADVERTISED_ADDR=host.docker.internal:<port> (its BIND address,
# QUEEN_KAFKA_ADDR, is separate and should be 0.0.0.0:<port>). The preflight below
# reads the advertised address off the wire and refuses to run rather than letting
# every suite time out on it -- that failure looks exactly like a facade bug and
# is not one.
# ---------------------------------------------------------------------------
set -uo pipefail
cd "$(dirname "$0")"

WHICH="${1:-all}"
RUN_ID="${RUN_ID:-$(date +%s)}"
KAFKA_PARTITIONS="${KAFKA_PARTITIONS:-8}"
PREFIX="${CONTAINER_PREFIX:-qkcompat-ruby-php}"
TRACE_DIR="${TRACE_DIR:-${TMPDIR:-/tmp}/rdkafka-ruby-php.$RUN_ID}"
RUBY_IMAGE="$PREFIX-ruby:latest"
PHP_IMAGE="$PREFIX-php:latest"
mkdir -p "$TRACE_DIR"

# A container's "localhost" is the container. Rewrite the loopback names so a
# bootstrap copied from rig.sh still reaches the host -- but see the preflight:
# the ADVERTISED address is the one that actually decides this.
BOOTSTRAP="${KAFKA_BOOTSTRAP:-host.docker.internal:19092}"
case "$BOOTSTRAP" in
  127.0.0.1:*|localhost:*|0.0.0.0:*)
    BOOTSTRAP="host.docker.internal:${BOOTSTRAP##*:}"
    echo "note: KAFKA_BOOTSTRAP pointed at loopback; using $BOOTSTRAP so the container can reach the host"
    ;;
esac
SASL_BOOTSTRAP="${KAFKA_SASL_BOOTSTRAP:-}"
case "$SASL_BOOTSTRAP" in
  127.0.0.1:*|localhost:*|0.0.0.0:*) SASL_BOOTSTRAP="host.docker.internal:${SASL_BOOTSTRAP##*:}" ;;
esac

# --------------------------------------------------------------------- build
build_images() {
  echo "==> docker build (pecl and the gem are the slow part; cached after the first run)"
  docker build -t "$RUBY_IMAGE" ./ruby || return 1
  docker build -t "$PHP_IMAGE"  ./php  || return 1
}
need_build=0
docker image inspect "$RUBY_IMAGE" >/dev/null 2>&1 || need_build=1
docker image inspect "$PHP_IMAGE"  >/dev/null 2>&1 || need_build=1
[ "${REBUILD:-0}" = 1 ] && need_build=1
if [ "$need_build" = 1 ]; then build_images || { echo "RESULT: FAIL (docker build)"; exit 1; }; fi
[ "$WHICH" = build ] && exit 0

# ----------------------------------------------------------------- preflight
# Ask the facade what it ADVERTISES, from inside a container, before spending a
# minute discovering it the slow way. This is the single trap that separates a
# containerised client from every other suite in compat/.
preflight() {
  local out
  out=$(docker run --rm --name "$PREFIX-preflight" --entrypoint php "$PHP_IMAGE" -r '
    $c = new RdKafka\Conf();
    $c->set("bootstrap.servers", $argv[1]);
    $c->set("enable.idempotence", "false");
    $p = new RdKafka\Producer($c);
    try { $md = $p->getMetadata(true, null, 8000); }
    catch (Throwable $e) { fwrite(STDERR, "UNREACHABLE ".$e->getMessage()."\n"); exit(3); }
    foreach ($md->getBrokers() as $b) { echo $b->getHost(), ":", $b->getPort(), "\n"; }
  ' "$BOOTSTRAP" 2>&1)
  local rc=$?
  if [ $rc -ne 0 ]; then
    echo "PREFLIGHT FAILED: could not read metadata from $BOOTSTRAP" >&2
    echo "$out" >&2
    return 1
  fi
  echo "  advertised broker(s): $(echo "$out" | tr '\n' ' ')"
  case "$out" in
    127.0.0.1:*|localhost:*|0.0.0.0:*)
      echo >&2
      echo "PREFLIGHT FAILED: the facade advertises '$out', which inside a container means" >&2
      echo "the CONTAINER, not your host. These suites run in containers, so every client" >&2
      echo "would bootstrap and then die re-dialling that name." >&2
      echo >&2
      echo "Restart the facade with:" >&2
      echo "    QUEEN_KAFKA_ADDR=0.0.0.0:<port>" >&2
      echo "    QUEEN_KAFKA_ADVERTISED_ADDR=host.docker.internal:<port>" >&2
      echo "(one broker can carry a second facade on a second port; that is what rig.sh --m5 does)" >&2
      return 1
      ;;
  esac
  return 0
}
echo "==> preflight against $BOOTSTRAP"
if ! preflight; then echo; echo "RESULT: BLOCKED (advertised address unreachable from a container)"; exit 2; fi

# ------------------------------------------------------------------ run one
fail=0
run_one() {
  local label="$1" image="$2" script="$3"
  echo
  echo "############################################################"
  echo "# $label"
  echo "############################################################"
  local args=(--rm --name "$PREFIX-$label" -v "$TRACE_DIR":/traces
              -e "NEGOTIATED_TRACE_FILE=/traces/$label.trace.log"
              -e "KAFKA_PARTITIONS=$KAFKA_PARTITIONS")
  if [ -n "$SASL_BOOTSTRAP" ]; then
    args+=(-e "KAFKA_SASL_BOOTSTRAP=$SASL_BOOTSTRAP"
           -e "KAFKA_SASL_PROTOCOL=${KAFKA_SASL_PROTOCOL:-sasl_ssl}"
           -e "KAFKA_SASL_TOKEN=${KAFKA_SASL_TOKEN:-}")
    if [ -n "${KAFKA_SSL_CA:-}" ]; then
      args+=(-v "$KAFKA_SSL_CA":/certs/ca.pem:ro -e KAFKA_SSL_CA=/certs/ca.pem)
    elif [ "${KAFKA_SSL_INSECURE:-}" = 1 ]; then
      args+=(-e KAFKA_SSL_INSECURE=1)
    fi
  fi
  docker run "${args[@]}" "$image" "$script" "$BOOTSTRAP" "$RUN_ID"
  local rc=$?
  [ $rc -ne 0 ] && fail=$((fail + 1))
  echo "# $label exit=$rc"
  return 0
}

case "$WHICH" in
  ruby)  run_one ruby "$RUBY_IMAGE" compat.rb ;;
  php)   run_one php  "$PHP_IMAGE"  compat.php ;;
  probe)
    run_one ruby-codec-probe "$RUBY_IMAGE" probe_compression.rb
    run_one php-codec-probe  "$PHP_IMAGE"  probe_compression.php
    ;;
  all)
    run_one ruby "$RUBY_IMAGE" compat.rb
    run_one php  "$PHP_IMAGE"  compat.php
    ;;
  *)
    echo "unknown target: $WHICH (want: all | ruby | php | probe | build)" >&2
    exit 2
    ;;
esac

echo
echo "traces: $TRACE_DIR"
if [ "$fail" -eq 0 ]; then
  echo "RESULT: PASS (rdkafka-ruby-php)"
else
  echo "RESULT: FAIL ($fail suite(s)) (rdkafka-ruby-php)"
fi
exit $((fail == 0 ? 0 : 1))
