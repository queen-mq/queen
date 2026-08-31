#!/usr/bin/env bash
#
# The EMBEDDED MODE rig: a throwaway Postgres and three brokers -- one with the
# facade embedded, one WITHOUT the flag (the default-off regression), and one
# pointed at a configuration the child cannot boot on (the crash-loop). It runs
# run.sh against the first, asserts the other two itself, and tears everything
# down on every exit path including a failure or a Ctrl-C.
#
#   queen-kafka/compat/embedded/rig-embedded.sh
#   queen-kafka/compat/embedded/rig-embedded.sh --keep      # leave the stack up
#
# Ports are deliberately not the defaults and never 5432 (a live stack on a
# developer machine): 32600 Postgres, 32601/32602 the embedded broker and its
# Kafka listener, 32603 the default-off broker, 32604/32605 the crash-loop one.
# Override with PG_HOST_PORT / BROKER_PORT / KAFKA_PORT / OFF_PORT / LOOP_PORT /
# LOOP_KAFKA_PORT.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

PG_HOST_PORT="${PG_HOST_PORT:-32600}"
BROKER_PORT="${BROKER_PORT:-32601}"
KAFKA_PORT="${KAFKA_PORT:-32602}"
OFF_PORT="${OFF_PORT:-32603}"
LOOP_PORT="${LOOP_PORT:-32604}"
LOOP_KAFKA_PORT="${LOOP_KAFKA_PORT:-32605}"
PARTITIONS="${PARTITIONS:-8}"
CONTAINER="${CONTAINER:-qkemb-pg}"

KEEP=0
RUN_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --keep) KEEP=1;;
    -h|--help) sed -n '2,18p' "$0"; exit 0;;
    *) RUN_ARGS+=("$arg");;
  esac
done

LOGDIR="$(mktemp -d -t queen-kafka-embedded.XXXXXX)"
BROKER_LOG="$LOGDIR/broker.log"
OFF_LOG="$LOGDIR/broker-off.log"
LOOP_LOG="$LOGDIR/broker-crashloop.log"
BROKER_PID=""
OFF_PID=""
LOOP_PID=""

say() { printf '\n=== %s\n' "$*"; }

cleanup() {
  local code=$?
  if [ "$KEEP" = 1 ]; then
    echo
    echo "--keep: the stack is still up."
    echo "  postgres : container $CONTAINER on 127.0.0.1:$PG_HOST_PORT"
    echo "  broker   : pid ${BROKER_PID:-none}, http://127.0.0.1:$BROKER_PORT, log $BROKER_LOG"
    echo "  facade   : 127.0.0.1:$KAFKA_PORT (a CHILD of the broker; it has no pid of its own to keep)"
    echo "  tear down: kill ${BROKER_PID:-} ${OFF_PID:-} ${LOOP_PID:-}; docker rm -f $CONTAINER"
    exit $code
  fi
  say "tearing down"
  # Only pids this script recorded at spawn, and only its own container. The
  # brokers take their facade children with them (that is the feature); the kill
  # -9 pass is the backstop for a broker that never got to run its shutdown.
  for p in "$LOOP_PID" "$OFF_PID" "$BROKER_PID"; do
    [ -n "$p" ] && kill "$p" 2>/dev/null
  done
  sleep 2
  for p in "$LOOP_PID" "$OFF_PID" "$BROKER_PID"; do
    [ -n "$p" ] && kill -9 "$p" 2>/dev/null
  done
  docker rm -f "$CONTAINER" >/dev/null 2>&1
  echo "logs kept at $LOGDIR"
  exit $code
}
trap cleanup EXIT INT TERM

command -v docker >/dev/null || { echo "docker not found" >&2; exit 2; }
command -v cargo  >/dev/null || { echo "cargo not found" >&2; exit 2; }
command -v kcat   >/dev/null || { echo "kcat not found (brew install kcat)" >&2; exit 2; }

# --------------------------------------------------------------------- postgres
say "postgres on 127.0.0.1:$PG_HOST_PORT (tmpfs, thrown away at exit)"
docker rm -f "$CONTAINER" >/dev/null 2>&1
docker run -d --name "$CONTAINER" \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DB=postgres \
  -e PGDATA=/var/lib/postgresql/data/pgdata \
  -p "$PG_HOST_PORT":5432 \
  --tmpfs /var/lib/postgresql/data:rw,size=2g \
  postgres:16 -c max_connections=200 >/dev/null || exit 1
for _ in $(seq 1 60); do
  docker exec "$CONTAINER" pg_isready -U postgres >/dev/null 2>&1 && break
  sleep 1
done
docker exec "$CONTAINER" pg_isready -U postgres >/dev/null 2>&1 || {
  echo "postgres never became ready" >&2; exit 1; }

# ----------------------------------------------------------------------- builds
say "building the broker and the facade (debug)"
( cd "$REPO_ROOT/server" && cargo build ) || exit 1
( cd "$REPO_ROOT/queen-kafka" && cargo build ) || exit 1
BROKER_BIN="$REPO_ROOT/server/target/debug/queen"
FACADE_BIN="$REPO_ROOT/queen-kafka/target/debug/queen-kafka"

# QUEEN_KAFKA_BIN is set EXPLICITLY here because the two debug binaries live in
# two target directories. The zero-configuration path -- the child resolved from
# the directory of the broker's own executable -- is what the Docker image
# exercises, where both land in /app/bin.
pg_env() {
  echo "PG_HOST=127.0.0.1 PG_PORT=$PG_HOST_PORT PG_USER=postgres PG_PASSWORD=postgres PG_DATABASE=postgres"
}

wait_health() { # $1 = port, $2 = pid, $3 = log
  for _ in $(seq 1 90); do
    curl -fsS -m 2 "http://127.0.0.1:$1/health" >/dev/null 2>&1 && return 0
    kill -0 "$2" 2>/dev/null || { echo "the broker died at boot:" >&2; tail -30 "$3" >&2; return 1; }
    sleep 1
  done
  echo "the broker never answered /health" >&2; tail -30 "$3" >&2; return 1
}

# ------------------------------------------------- broker WITH the facade embedded
say "broker on 127.0.0.1:$BROKER_PORT with QUEEN_KAFKA_EMBEDDED=true (facade on $KAFKA_PORT)"
env $(pg_env) PORT="$BROKER_PORT" QUEEN_BIND_ADDR=127.0.0.1 \
  QUEEN_APPLY_SCHEMA=true DB_POOL_SIZE=32 LOG_LEVEL=info \
  QUEEN_KAFKA_EMBEDDED=true \
  QUEEN_KAFKA_BIN="$FACADE_BIN" \
  QUEEN_KAFKA_ADDR="127.0.0.1:$KAFKA_PORT" \
  QUEEN_KAFKA_ADVERTISED_ADDR="127.0.0.1:$KAFKA_PORT" \
  QUEEN_KAFKA_DEFAULT_PARTITIONS="$PARTITIONS" \
  "$BROKER_BIN" > "$BROKER_LOG" 2>&1 &
BROKER_PID=$!
wait_health "$BROKER_PORT" "$BROKER_PID" "$BROKER_LOG" || exit 1
for _ in $(seq 1 100); do nc -z 127.0.0.1 "$KAFKA_PORT" >/dev/null 2>&1 && break; sleep 0.2; done

FAIL=0

# ------------------------------------------------------- the default-off regression
# The claim embedded mode has to earn: a broker that did not ask for it is the
# broker that existed before the feature. No child, no kafka lines, and a /status
# body byte-identical to the one it always answered.
say "default-off regression: a broker on $OFF_PORT with no QUEEN_KAFKA_* at all"
env $(pg_env) PORT="$OFF_PORT" QUEEN_BIND_ADDR=127.0.0.1 \
  QUEEN_APPLY_SCHEMA=false DB_POOL_SIZE=8 LOG_LEVEL=info \
  "$BROKER_BIN" > "$OFF_LOG" 2>&1 &
OFF_PID=$!
if wait_health "$OFF_PORT" "$OFF_PID" "$OFF_LOG"; then
  body="$(curl -fsS -m 5 "http://127.0.0.1:$OFF_PORT/status")"
  if [ "$body" = '{"status":"ok","engine":"segments-rust"}' ]; then
    echo "  PASS  /status is byte-identical: $body"
  else
    echo "  FAIL  /status changed with the feature off: $body"; FAIL=1
  fi
  n=$(grep -c 'kafka' "$OFF_LOG" || true)
  kids=$(pgrep -P "$OFF_PID" | tr '\n' ' ')
  if [ "$n" = 0 ]; then echo "  PASS  no kafka line in the boot log"; else echo "  FAIL  $n kafka lines with the feature off"; FAIL=1; fi
  if [ -z "$kids" ]; then echo "  PASS  no child process"; else echo "  FAIL  unexpected children: $kids"; FAIL=1; fi
else
  FAIL=1
fi

# ------------------------------------------------------------- the crash-loop
# A child pointed at a configuration it refuses to boot on. The facade's own
# validation is what fails it (QUEEN_KAFKA_DEFAULT_PARTITIONS=0 is not a
# partition count), which also proves the passthrough: the broker never reads
# that variable, it only forwards it. What is being measured is the LADDER --
# 1s, 2s, 4s ... -- and that the broker is untouched by any of it.
say "crash-loop: a broker on $LOOP_PORT whose child cannot boot"
env $(pg_env) PORT="$LOOP_PORT" QUEEN_BIND_ADDR=127.0.0.1 \
  QUEEN_APPLY_SCHEMA=false DB_POOL_SIZE=8 LOG_LEVEL=info \
  QUEEN_KAFKA_EMBEDDED=true \
  QUEEN_KAFKA_BIN="$FACADE_BIN" \
  QUEEN_KAFKA_ADDR="127.0.0.1:$LOOP_KAFKA_PORT" \
  QUEEN_KAFKA_ADVERTISED_ADDR="127.0.0.1:$LOOP_KAFKA_PORT" \
  QUEEN_KAFKA_DEFAULT_PARTITIONS=0 \
  "$BROKER_BIN" > "$LOOP_LOG" 2>&1 &
LOOP_PID=$!
if wait_health "$LOOP_PORT" "$LOOP_PID" "$LOOP_LOG"; then
  # 20 seconds is 1+2+4+8 with room to spare: enough rungs to read a ladder.
  sleep 20
  # Scoped to the supervisor's own exit lines: `backoff_ms` is also a field of
  # the sweeper's config block, and grepping the file blind read its 1000..60000
  # as a first rung.
  ladder=$(grep 'facade EXITED' "$LOOP_LOG" | grep -o 'backoff_ms=[0-9]*' | sed 's/backoff_ms=//' | tr '\n' ' ')
  exits=$(grep -c 'facade EXITED' "$LOOP_LOG" || true)
  echo "  ladder: $ladder"
  case "$ladder" in
    "1000 2000 4000 8000"*) echo "  PASS  the backoff doubles: $ladder" ;;
    *) echo "  FAIL  unexpected backoff ladder: $ladder"; FAIL=1 ;;
  esac
  # Anti-flood, measured rather than asserted: an unbacked-off loop would have
  # spawned hundreds of times in 20 seconds.
  if [ "$exits" -le 8 ]; then
    echo "  PASS  $exits exits in 20s -- the ladder is what bounds the log"
  else
    echo "  FAIL  $exits exits in 20s: the child is looping unthrottled"; FAIL=1
  fi
  curl -fsS -m 5 "http://127.0.0.1:$LOOP_PORT/health" >/dev/null 2>&1 \
    && echo "  PASS  the broker keeps serving its own HTTP through the loop" \
    || { echo "  FAIL  the broker stopped answering"; FAIL=1; }
  case "$(curl -fsS -m 5 "http://127.0.0.1:$LOOP_PORT/status")" in
    *'"phase":"restarting"'*) echo "  PASS  /status says restarting" ;;
    *) echo "  FAIL  /status does not report the restart state"; FAIL=1 ;;
  esac
else
  FAIL=1
fi
kill "$LOOP_PID" 2>/dev/null; LOOP_PID=""
kill "$OFF_PID" 2>/dev/null; OFF_PID=""

# ------------------------------------------------------------------ the suite
say "run.sh against the embedded broker"
QUEEN_KAFKA_BOOTSTRAP="127.0.0.1:$KAFKA_PORT" \
QUEEN_BROKER_URL="http://127.0.0.1:$BROKER_PORT" \
QUEEN_BROKER_PID="$BROKER_PID" \
QUEEN_BROKER_LOG="$BROKER_LOG" \
QUEEN_KAFKA_PARTITIONS="$PARTITIONS" \
QUEEN_EMBEDDED_SHUTDOWN="${QUEEN_EMBEDDED_SHUTDOWN:-1}" \
  "$SCRIPT_DIR/run.sh" "${RUN_ARGS[@]+"${RUN_ARGS[@]}"}" || FAIL=1
# Scenario 6 stops the broker on purpose; do not let teardown report a corpse.
kill -0 "$BROKER_PID" 2>/dev/null || BROKER_PID=""

say "rig verdict"
[ "$FAIL" = 0 ] && echo "EMBEDDED MODE: rig green" || echo "EMBEDDED MODE: rig RED"
exit "$FAIL"
