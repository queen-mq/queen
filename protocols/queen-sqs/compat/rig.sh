#!/usr/bin/env bash
#
# The queen-sqs live rig: a throwaway Postgres, a debug broker and a debug
# facade, on ports nothing else on a developer machine uses. It is the same
# discipline as protocols/queen-kafka/compat/rig.sh, with one deliberate difference — that
# rig owns the whole run (stack up, suite, stack down, one exit code), while
# this one is a STACK MANAGER, because the SQS suites are a python file and a
# shell file that a person also wants to run one at a time against a stack that
# stays up between them.
#
#   protocols/queen-sqs/compat/rig.sh up       # stand it up (idempotent: an up on a
#                                    #   running stack re-checks health and says so)
#   protocols/queen-sqs/compat/rig.sh down     # tear it down (idempotent: a down on
#                                    #   nothing succeeds quietly)
#   protocols/queen-sqs/compat/rig.sh status   # what is running, and is it answering
#   protocols/queen-sqs/compat/rig.sh logs     # tail the broker's and the facade's logs
#   protocols/queen-sqs/compat/rig.sh logs -f  # ...following
#
# Ports are deliberately not the defaults: 55440 for Postgres (never 5432 —
# that is a live stack on this machine), 26632 for the broker (never 6632),
# 19324 for the SQS listener (never 9324, so an ElasticMQ or a LocalStack a
# developer has running is not shadowed and cannot shadow this). Override with
# PG_HOST_PORT / BROKER_PORT / SQS_PORT.
#
# NOTHING it starts outlives a FAILED `up`: the trap tears a half-built stack
# back down, so a broker that booted before a facade that could not is not left
# holding a port. A SUCCESSFUL `up` is the one path that leaves things running,
# which is the whole point of the command.
#
# The facade runs at QUEEN_SQS_DEFAULT_PARTITIONS=8 rather than the shipped
# default of 64: eight lanes is enough that "messages spread across partitions"
# means something and few enough that a queue's partition rows are cheap on a
# debug broker. The suites read the same number from QUEEN_SQS_PARTITIONS.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

PG_HOST_PORT="${PG_HOST_PORT:-55440}"
BROKER_PORT="${BROKER_PORT:-26632}"
SQS_PORT="${SQS_PORT:-19324}"
PARTITIONS="${PARTITIONS:-8}"
CONTAINER="${CONTAINER:-qsqs-rig-pg}"

# The credential this rig serves, and the only one. It is not a secret — it
# protects a throwaway broker for the length of one suite — but it is FIXED
# rather than generated, because both suites and any `aws` command a person
# types by hand have to spell it, and a value that changed per run would have to
# be read out of a file before every one of them.
SQS_AKID="${SQS_AKID:-QSQSTEST}"
SQS_SECRET="${SQS_SECRET:-qsqssecret}"
SQS_TOKEN="${SQS_TOKEN:-devtoken}"
SQS_REGION="${SQS_REGION:-queen-1}"
SQS_ACCOUNT="${SQS_ACCOUNT:-000000000000}"
# Configured rather than generated, so a receipt handle taken before a facade
# restart is still verifiable after it. Unset, the facade mints a per-process
# key and every handle in flight across a restart becomes
# ReceiptHandleIsInvalid — correct behaviour, and not what a rig wants to be
# testing by accident. 32 bytes, well past the 16-byte floor config.rs enforces.
SQS_HANDLE_SECRET="${SQS_HANDLE_SECRET:-qsqs-rig-handle-secret-0123456789}"

RIGDIR="$SCRIPT_DIR/.rig"
BROKER_LOG="$RIGDIR/broker.log"
FACADE_LOG="$RIGDIR/facade.log"
BROKER_PIDFILE="$RIGDIR/broker.pid"
FACADE_PIDFILE="$RIGDIR/facade.pid"
ENVFILE="$RIGDIR/env.sh"

BROKER_URL="http://127.0.0.1:$BROKER_PORT"
SQS_ENDPOINT="http://127.0.0.1:$SQS_PORT"

# How long `up` will wait for a queen-sqs that does not compile. The rule is the
# campaign's: another workflow is editing protocols/queen-sqs/src and keeps it green
# BETWEEN its steps, so a build failure here is far more likely to be a
# half-written edit than a real break. Wait it out; never patch src from this
# side.
BUILD_ATTEMPTS="${BUILD_ATTEMPTS:-6}"
BUILD_RETRY_SECONDS="${BUILD_RETRY_SECONDS:-300}"

say()  { printf '\n=== %s\n' "$*"; }
info() { printf '    %s\n' "$*"; }
die()  { printf 'rig: %s\n' "$*" >&2; exit 1; }

# ------------------------------------------------------------------ primitives

pidfile_pid() {
  # A pid file whose process is gone is a stale file, not a running service:
  # every reader goes through here so that no caller has to remember the
  # difference.
  local file="$1" pid
  [ -f "$file" ] || return 1
  pid="$(cat "$file" 2>/dev/null)"
  [ -n "$pid" ] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  printf '%s' "$pid"
}

stop_pidfile() {
  local file="$1" name="$2" pid
  if pid="$(pidfile_pid "$file")"; then
    info "stopping $name (pid $pid)"
    kill "$pid" 2>/dev/null
    for _ in $(seq 1 50); do kill -0 "$pid" 2>/dev/null || break; sleep 0.2; done
    kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null
  fi
  rm -f "$file"
}

container_running() {
  [ -n "$(docker ps -q -f "name=^${CONTAINER}$" 2>/dev/null)" ]
}

http_ok() { curl -fsS -m 2 "$1" >/dev/null 2>&1; }

# ------------------------------------------------------------------------ down

do_down() {
  say "tearing the rig down"
  stop_pidfile "$FACADE_PIDFILE" "queen-sqs"
  stop_pidfile "$BROKER_PIDFILE" "the broker"
  if docker ps -a -q -f "name=^${CONTAINER}$" >/dev/null 2>&1 &&
     [ -n "$(docker ps -a -q -f "name=^${CONTAINER}$" 2>/dev/null)" ]; then
    info "removing container $CONTAINER"
    docker rm -f "$CONTAINER" >/dev/null 2>&1
  fi
  rm -f "$ENVFILE"
  info "down. logs kept at $RIGDIR"
}

# --------------------------------------------------------------------- failure

# Only armed during `up`, and disarmed the moment it has succeeded. A partial
# stack is worse than no stack: it holds the ports the next attempt needs while
# answering nothing.
UP_OK=0
up_trap() {
  local code=$?
  [ "$UP_OK" = 1 ] && exit $code
  echo
  echo "rig: up FAILED — tearing the half-built stack back down." >&2
  do_down >&2
  exit $([ $code -eq 0 ] && echo 1 || echo $code)
}

# -------------------------------------------------------------------------- up

do_up() {
  mkdir -p "$RIGDIR"
  trap up_trap EXIT INT TERM

  command -v docker >/dev/null || die "docker not found"
  command -v curl   >/dev/null || die "curl not found"
  command -v cargo  >/dev/null || die "cargo not found"

  # ------------------------------------------------------------------ postgres
  if container_running; then
    say "postgres: container $CONTAINER already up on 127.0.0.1:$PG_HOST_PORT"
  else
    say "postgres on 127.0.0.1:$PG_HOST_PORT (tmpfs, thrown away with the rig)"
    docker rm -f "$CONTAINER" >/dev/null 2>&1
    # --rm as the task asks, AND an explicit `docker rm -f` in teardown: --rm
    # only fires when the container itself exits, which a `docker kill` does and
    # a host that lost the daemon does not.
    docker run -d --rm --name "$CONTAINER" \
      -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DB=postgres \
      -e PGDATA=/var/lib/postgresql/data/pgdata \
      -p "$PG_HOST_PORT":5432 \
      --tmpfs /var/lib/postgresql/data:rw,size=2g \
      postgres:16 -c max_connections=400 >/dev/null || die "could not start postgres"

    for _ in $(seq 1 60); do
      docker exec "$CONTAINER" pg_isready -U postgres >/dev/null 2>&1 && break
      sleep 1
    done
    docker exec "$CONTAINER" pg_isready -U postgres >/dev/null 2>&1 || {
      docker logs "$CONTAINER" 2>&1 | tail -20 >&2
      die "postgres never became ready"
    }
    info "ready"
  fi

  # -------------------------------------------------------------------- builds
  # The broker: built only if it is not there. This rig does not own server/,
  # and rebuilding it would be a surprise for whoever does.
  if [ ! -x "$REPO_ROOT/server/target/debug/queen" ]; then
    say "building the broker (debug) — server/target/debug/queen is missing"
    ( cd "$REPO_ROOT/server" && cargo build --features server ) || die "the broker did not build"
  else
    say "broker binary: $REPO_ROOT/server/target/debug/queen (already built)"
  fi

  # The facade: ALWAYS rebuilt, because the point of this rig is to exercise
  # whatever protocols/queen-sqs/src says right now. A failure is waited out rather than
  # diagnosed — see BUILD_ATTEMPTS.
  say "building queen-sqs (debug)"
  local attempt=1
  while true; do
    if cargo build --manifest-path "$REPO_ROOT/protocols/queen-sqs/Cargo.toml"; then
      break
    fi
    if [ "$attempt" -ge "$BUILD_ATTEMPTS" ]; then
      die "queen-sqs did not build after $BUILD_ATTEMPTS attempts over \
$((BUILD_ATTEMPTS * BUILD_RETRY_SECONDS / 60)) minutes. This rig does not edit src; \
report the compile error to whoever is editing it."
    fi
    info "build failed (attempt $attempt/$BUILD_ATTEMPTS) — another workflow is probably \
mid-edit; waiting ${BUILD_RETRY_SECONDS}s"
    sleep "$BUILD_RETRY_SECONDS"
    attempt=$((attempt + 1))
  done

  # -------------------------------------------------------------------- broker
  if pidfile_pid "$BROKER_PIDFILE" >/dev/null && http_ok "$BROKER_URL/health"; then
    say "broker: already up on $BROKER_URL"
  else
    stop_pidfile "$BROKER_PIDFILE" "a stale broker"
    say "broker on $BROKER_URL"
    # The kafka rig's broker environment, verbatim in what matters: the rig's
    # own Postgres, schema applied at boot, a small pool, and JWT_ENABLED unset
    # so any bearer is accepted — the facade's own credential check is the one
    # under test here, not the broker's.
    PG_HOST=127.0.0.1 PG_PORT="$PG_HOST_PORT" PG_USER=postgres PG_PASSWORD=postgres \
    PG_DATABASE=postgres PORT="$BROKER_PORT" QUEEN_BIND_ADDR=127.0.0.1 \
    QUEEN_APPLY_SCHEMA=true DB_POOL_SIZE=32 LOG_LEVEL=info \
      "$REPO_ROOT/server/target/debug/queen" >> "$BROKER_LOG" 2>&1 &
    echo $! > "$BROKER_PIDFILE"
    local bpid; bpid="$(cat "$BROKER_PIDFILE")"

    for _ in $(seq 1 120); do
      http_ok "$BROKER_URL/health" && break
      kill -0 "$bpid" 2>/dev/null || { tail -30 "$BROKER_LOG" >&2; die "the broker died at boot"; }
      sleep 1
    done
    http_ok "$BROKER_URL/health" || { tail -30 "$BROKER_LOG" >&2; die "the broker never answered /health"; }
    info "healthy"
  fi

  # -------------------------------------------------------------------- facade
  # Restarted unconditionally when it is up, because it was just rebuilt: a
  # running facade from the previous binary is exactly the confusing thing an
  # idempotent `up` must not leave behind.
  stop_pidfile "$FACADE_PIDFILE" "the previous queen-sqs"
  say "queen-sqs on $SQS_ENDPOINT (sigv4, region $SQS_REGION, account $SQS_ACCOUNT, \
$PARTITIONS partitions per queue)"
  QUEEN_URL="$BROKER_URL" \
  QUEEN_SQS_LISTEN="127.0.0.1:$SQS_PORT" \
  QUEEN_SQS_AUTH=sigv4 \
  QUEEN_SQS_CREDENTIALS="$SQS_AKID:$SQS_SECRET:$SQS_TOKEN" \
  QUEEN_SQS_REGION="$SQS_REGION" \
  QUEEN_SQS_ACCOUNT="$SQS_ACCOUNT" \
  QUEEN_SQS_DEFAULT_PARTITIONS="$PARTITIONS" \
  QUEEN_SQS_HANDLE_SECRET="$SQS_HANDLE_SECRET" \
  LOG_LEVEL="${FACADE_LOG_LEVEL:-debug}" \
    "$REPO_ROOT/protocols/queen-sqs/target/debug/queen-sqs" >> "$FACADE_LOG" 2>&1 &
  echo $! > "$FACADE_PIDFILE"
  local fpid; fpid="$(cat "$FACADE_PIDFILE")"

  for _ in $(seq 1 100); do
    http_ok "$SQS_ENDPOINT/healthz" && break
    kill -0 "$fpid" 2>/dev/null || { tail -30 "$FACADE_LOG" >&2; die "queen-sqs died at boot"; }
    sleep 0.3
  done
  http_ok "$SQS_ENDPOINT/healthz" || { tail -30 "$FACADE_LOG" >&2; die "queen-sqs never answered /healthz"; }
  info "healthy"

  # ---------------------------------------------------------------- the handoff
  # Everything a suite needs to reach this stack, in one file it can source.
  # The suites take it from the environment (the CLIENT_MATRIX contract: stack
  # from env, never a hardcoded address), and this is where a person gets that
  # environment without retyping it.
  cat > "$ENVFILE" <<EOF
# Sourced by the queen-sqs compat suites. Written by rig.sh up.
export QUEEN_SQS_ENDPOINT="$SQS_ENDPOINT"
export QUEEN_SQS_REGION="$SQS_REGION"
export QUEEN_SQS_ACCOUNT="$SQS_ACCOUNT"
export QUEEN_SQS_PARTITIONS="$PARTITIONS"
export QUEEN_URL="$BROKER_URL"
export AWS_ACCESS_KEY_ID="$SQS_AKID"
export AWS_SECRET_ACCESS_KEY="$SQS_SECRET"
export AWS_DEFAULT_REGION="$SQS_REGION"
export AWS_REGION="$SQS_REGION"
# The CLI reads a session token only if one is set; this facade's third
# credential field is a QUEEN bearer, not an AWS session token, so it is
# deliberately NOT exported here — signing with it would fail verification.
unset AWS_SESSION_TOKEN
EOF

  UP_OK=1
  do_status
}

# ---------------------------------------------------------------------- status

do_status() {
  say "rig status"
  local pid rc=0

  if container_running; then
    info "postgres : UP    container $CONTAINER, 127.0.0.1:$PG_HOST_PORT"
  else
    info "postgres : DOWN  container $CONTAINER"; rc=1
  fi

  if pid="$(pidfile_pid "$BROKER_PIDFILE")"; then
    if http_ok "$BROKER_URL/health"; then
      info "broker   : UP    pid $pid, $BROKER_URL"
    else
      info "broker   : SICK  pid $pid, $BROKER_URL does not answer /health"; rc=1
    fi
  else
    info "broker   : DOWN  $BROKER_URL"; rc=1
  fi

  if pid="$(pidfile_pid "$FACADE_PIDFILE")"; then
    if http_ok "$SQS_ENDPOINT/healthz"; then
      info "queen-sqs: UP    pid $pid, $SQS_ENDPOINT"
    else
      info "queen-sqs: SICK  pid $pid, $SQS_ENDPOINT does not answer /healthz"; rc=1
    fi
  else
    info "queen-sqs: DOWN  $SQS_ENDPOINT"; rc=1
  fi

  info "logs     : $BROKER_LOG"
  info "           $FACADE_LOG"
  if [ -f "$ENVFILE" ]; then
    info "env      : source $ENVFILE"
    info "suites   : \"\$SCRIPT_DIR\"/../../..  ->  compat/smoke_m0.py, compat/smoke_m0_cli.sh"
  fi
  return $rc
}

# ------------------------------------------------------------------------ logs

do_logs() {
  local follow=()
  [ "${1:-}" = "-f" ] && follow=(-f)
  tail "${follow[@]+"${follow[@]}"}" -n 60 "$BROKER_LOG" "$FACADE_LOG"
}

# ------------------------------------------------------------------------ main

case "${1:-}" in
  up)     shift; do_up "$@";;
  down)   shift; do_down "$@";;
  status) shift; do_status "$@";;
  logs)   shift; do_logs "$@";;
  -h|--help|help) sed -n '2,32p' "$0";;
  "")     sed -n '2,32p' "$0"; exit 2;;
  *)      die "unknown command '${1}'. It is up | down | status | logs.";;
esac
