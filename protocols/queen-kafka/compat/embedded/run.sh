#!/usr/bin/env bash
# queen-kafka compat: EMBEDDED MODE acceptance
#
# Embedded mode is `QUEEN_KAFKA_EMBEDDED=true` on the BROKER: the broker process
# spawns this facade as a supervised child, wired to its own HTTP listener over
# loopback (server/src/kafka_facade.rs). One deployment, two processes.
#
# This script runs against a stack that is ALREADY UP. Nothing here starts or
# stops a Postgres or a broker -- that is rig-embedded.sh's job, or yours. Every
# address comes from the environment so it can be wired into a rig without
# editing a line of it.
#
# REQUIRED:
#   QUEEN_KAFKA_BOOTSTRAP   the embedded facade's host:port, e.g. 127.0.0.1:32602
#   QUEEN_BROKER_URL        the broker that spawned it, e.g. http://127.0.0.1:32601
#
# OPTIONAL -- each unset variable SKIPS the scenario that needs it, loudly:
#   QUEEN_BROKER_PID        the broker's pid. Without it the parentage check and
#                           the shutdown scenario cannot run: a supervisor is a
#                           claim about a process tree, and a pid is the only way
#                           to look at one.
#   QUEEN_BROKER_LOG        the broker's log file; without it the supervisor's
#                           own lines (restart, backoff, anti-flood) cannot be read
#   QUEEN_EMBEDDED_SHUTDOWN=1  opt in to scenario 5, which is DESTRUCTIVE: it
#                           SIGTERMs the broker and asserts the child went with it.
#                           It runs last and leaves the stack down.
#
# TUNING, all with defaults that match the facade's own:
#   QUEEN_KAFKA_PARTITIONS  the facade's QUEEN_KAFKA_DEFAULT_PARTITIONS (8)
#   QUEEN_KAFKA_GRACE_MS    the broker's QUEEN_KAFKA_SHUTDOWN_GRACE_MS (5000)
#   RUN_ID                  suffix on every topic and group (default: epoch)
#
# Exits non-zero on the first failed assertion, and prints what it read.
set -uo pipefail

: "${QUEEN_KAFKA_BOOTSTRAP:?set QUEEN_KAFKA_BOOTSTRAP, e.g. 127.0.0.1:32602}"
: "${QUEEN_BROKER_URL:?set QUEEN_BROKER_URL, e.g. http://127.0.0.1:32601}"
PARTITIONS="${QUEEN_KAFKA_PARTITIONS:-8}"
GRACE_MS="${QUEEN_KAFKA_GRACE_MS:-5000}"
RUN_ID="${RUN_ID:-$(date +%s)}"
TOPIC="embedded-$RUN_ID"
GROUP="embedded-g-$RUN_ID"
KPORT="${QUEEN_KAFKA_BOOTSTRAP##*:}"
KHOST="${QUEEN_KAFKA_BOOTSTRAP%:*}"

command -v kcat >/dev/null || { echo "kcat not found (brew install kcat)" >&2; exit 2; }
command -v curl >/dev/null || { echo "curl not found" >&2; exit 2; }

FAIL=0
pass() { printf '  PASS  %s\n' "$*"; }
fail() { printf '  FAIL  %s\n' "$*"; FAIL=1; }
skip() { printf '  SKIP  %s\n' "$*"; }
say()  { printf '\n=== %s\n' "$*"; }

# A field of GET /status. The broker renders it with serde_json, so the body is
# compact and one `"key":value` per key -- no jq dependency for six numbers.
status_field() {
  curl -fsS -m 5 "$QUEEN_BROKER_URL/status" 2>/dev/null \
    | sed -n "s/.*\"$1\":\([^,}]*\).*/\1/p" | head -1
}

# `cmd ... ` with a deadline, because macOS ships no coreutils `timeout` and a
# suite that can hang is a suite nobody runs in CI.
run_bounded() { # $1 = seconds, rest = command
  local secs="$1"; shift
  "$@" & local pid=$!
  local waited=0
  while kill -0 "$pid" 2>/dev/null; do
    if [ "$waited" -ge "$((secs * 10))" ]; then
      kill -9 "$pid" 2>/dev/null
      wait "$pid" 2>/dev/null
      return 124
    fi
    sleep 0.1
    waited=$((waited + 1))
  done
  wait "$pid"
}

produce() { # $1 = first index, $2 = count
  local i
  for ((i = $1; i < $1 + $2; i++)); do printf 'm%d\n' "$i"; done \
    | kcat -q -b "$QUEEN_KAFKA_BOOTSTRAP" -P -t "$TOPIC" \
        -X enable.idempotence=false 2>/dev/null
}

# Consume with the GROUP, so the offsets are committed in Queen and the next call
# resumes where this one stopped. Three kcat details that each cost an hour to
# find: in -G mode EVERY trailing argument is a topic name, so the flags go
# BEFORE it; -e exits once every assigned partition has reached its end, which is
# what makes a bounded read possible at all; and a fresh group with no stored
# offset starts at the TAIL unless auto.offset.reset says otherwise, so without
# it the first pass reads nothing and proves nothing.
consume_group() { # $1 = out file, $2 = seconds
  run_bounded "$2" kcat -q -b "$QUEEN_KAFKA_BOOTSTRAP" -G "$GROUP" \
    -e -f '%s\n' -X auto.offset.reset=earliest "$TOPIC" \
    > "$1" 2>/dev/null
}

TMP="$(mktemp -d -t queen-kafka-embedded.XXXXXX)"
trap 'rm -rf "$TMP"' EXIT

echo "queen-kafka compat: EMBEDDED MODE"
echo "  bootstrap  $QUEEN_KAFKA_BOOTSTRAP"
echo "  broker     $QUEEN_BROKER_URL"
echo "  broker pid ${QUEEN_BROKER_PID:-(unset: parentage and shutdown will SKIP)}"
echo "  broker log ${QUEEN_BROKER_LOG:-(unset: the log assertions will SKIP)}"
echo "  topic      $TOPIC   group $GROUP   partitions $PARTITIONS"

# --------------------------------------------------------------- 1. the broker
# says it is supervising a child, in the one place an operator already looks.
say "1. GET /status reports the embedded child"
BODY="$(curl -fsS -m 5 "$QUEEN_BROKER_URL/status" 2>/dev/null)"
echo "  $BODY"
case "$BODY" in
  *'"mode":"embedded"'*) pass "/status carries the kafka block" ;;
  *) fail "/status has no kafka block -- is QUEEN_KAFKA_EMBEDDED=true on this broker?" ;;
esac
[ "$(status_field phase)" = '"running"' ] \
  && pass "phase=running" || fail "phase=$(status_field phase), expected \"running\""
CHILD_PID="$(status_field pid)"
case "$CHILD_PID" in
  ''|null) fail "/status reports no child pid" ;;
  *) pass "child pid $CHILD_PID" ;;
esac

# ------------------------------------------------------- 2. it is a real child
# Two processes, one deployment: the point of embedded mode is that the facade is
# a CHILD of the broker, not a sidecar someone remembered to deploy.
say "2. the facade is a child process of the broker"
if [ -z "${QUEEN_BROKER_PID:-}" ]; then
  skip "QUEEN_BROKER_PID unset"
else
  if pgrep -P "$QUEEN_BROKER_PID" 2>/dev/null | grep -qx "$CHILD_PID"; then
    pass "pid $CHILD_PID is a child of broker $QUEEN_BROKER_PID"
  else
    fail "pid $CHILD_PID is not a child of $QUEEN_BROKER_PID (children: $(pgrep -P "$QUEEN_BROKER_PID" | tr '\n' ' '))"
  fi
fi

# ----------------------------------------------- 3. a real client round-trips
say "3. produce and consume through the embedded facade"
produce 1 8 || fail "produce failed"
consume_group "$TMP/first" 90
GOT=$(sort -u "$TMP/first" | grep -c '^m' || true)
if [ "$GOT" -eq 8 ]; then
  pass "8 messages produced and consumed (m1..m8)"
else
  fail "expected 8 distinct messages, got $GOT: $(tr '\n' ' ' < "$TMP/first")"
fi

# ------------------------------------- 4. the supervision claim, measured
# SIGKILL is the whole test: a facade that was asked politely to stop proves
# nothing about a crash. What must survive it is not in the facade -- the group's
# committed offsets live in Queen -- so the second consume must start where the
# first one stopped, with no replay and no gap.
say "4. SIGKILL the child: the supervisor restarts it and offsets resume"
if [ -z "$CHILD_PID" ] || [ "$CHILD_PID" = "null" ]; then
  skip "no child pid to kill"
else
  RESTARTS_BEFORE="$(status_field restarts)"
  produce 9 8 || fail "second produce failed"
  kill -9 "$CHILD_PID" 2>/dev/null || fail "could not SIGKILL $CHILD_PID"
  # The first backoff rung is one second; give the supervisor a generous multiple
  # of it before calling the restart a failure.
  NEW_PID=""
  for _ in $(seq 1 100); do
    sleep 0.2
    p="$(status_field pid)"
    if [ -n "$p" ] && [ "$p" != "null" ] && [ "$p" != "$CHILD_PID" ]; then NEW_PID="$p"; break; fi
  done
  if [ -n "$NEW_PID" ]; then
    pass "restarted: $CHILD_PID -> $NEW_PID"
  else
    fail "no new child within 20s (phase=$(status_field phase) lastExit=$(status_field lastExit))"
  fi
  RESTARTS_AFTER="$(status_field restarts)"
  [ "${RESTARTS_AFTER:-0}" -gt "${RESTARTS_BEFORE:-0}" ] \
    && pass "restarts $RESTARTS_BEFORE -> $RESTARTS_AFTER" \
    || fail "restart counter did not move ($RESTARTS_BEFORE -> $RESTARTS_AFTER)"
  case "$(status_field lastExit)" in
    *'signal 9'*) pass "lastExit names the signal" ;;
    *) fail "lastExit=$(status_field lastExit), expected signal 9" ;;
  esac
  # The broker itself must be untouched by any of it.
  curl -fsS -m 5 "$QUEEN_BROKER_URL/health" >/dev/null 2>&1 \
    && pass "the broker kept serving its own HTTP throughout" \
    || fail "the broker's /health stopped answering"
  # ...and the client picks up where it left off.
  consume_group "$TMP/second" 90
  SECOND="$(sort -u "$TMP/second" | tr '\n' ' ')"
  EXPECT="$(for i in $(seq 9 16); do echo "m$i"; done | sort -u | tr '\n' ' ')"
  if [ "$SECOND" = "$EXPECT" ]; then
    pass "resumed from committed offsets: m9..m16, no replay and no gap"
  else
    fail "resume read [$SECOND], expected [$EXPECT]"
  fi
fi

# --------------------------------------------- 5. what the log has to contain
say "5. the supervisor's log lines"
if [ -z "${QUEEN_BROKER_LOG:-}" ]; then
  skip "QUEEN_BROKER_LOG unset"
else
  grep -q 'queen-kafka facade started (embedded)' "$QUEEN_BROKER_LOG" \
    && pass "the spawn is logged" || fail "no spawn line in $QUEEN_BROKER_LOG"
  if [ -n "$CHILD_PID" ] && [ "$CHILD_PID" != "null" ]; then
    grep -q 'queen-kafka facade EXITED' "$QUEEN_BROKER_LOG" \
      && pass "the exit is logged at ERROR with a backoff" \
      || fail "no exit line in $QUEEN_BROKER_LOG"
  fi
  # Anti-flood: the child's forwarded output must never dominate the broker's own
  # log. The budget is 200 lines per 10s window per stream; a run of this suite is
  # nowhere near it, so what is asserted is that the guard exists and did not fire
  # spuriously, and that the forwarded lines are TAGGED (an untagged forward is
  # indistinguishable from the broker talking).
  FWD=$(grep -c 'stream="stdout"\|stream=stdout' "$QUEEN_BROKER_LOG" || true)
  RATE=$(grep -c 'output rate-limited' "$QUEEN_BROKER_LOG" || true)
  echo "  forwarded child lines: $FWD   rate-limit notices: $RATE"
  [ "$FWD" -gt 0 ] && pass "child output reaches the broker log, tagged" \
                   || fail "no tagged child output in the broker log"
fi

# ------------------------------------------------- 6. shutdown leaves nothing
say "6. SIGTERM the broker: the child goes with it (destructive)"
if [ "${QUEEN_EMBEDDED_SHUTDOWN:-0}" != "1" ] || [ -z "${QUEEN_BROKER_PID:-}" ]; then
  skip "set QUEEN_EMBEDDED_SHUTDOWN=1 and QUEEN_BROKER_PID to run it (it stops the stack)"
else
  LAST_CHILD="$(status_field pid)"
  kill -TERM "$QUEEN_BROKER_PID" 2>/dev/null || fail "could not SIGTERM $QUEEN_BROKER_PID"
  # The broker drains in-flight requests, then signals the child and WAITS for it.
  # The budget is the grace window plus a generous margin for the drain itself.
  DEADLINE=$(( (GRACE_MS / 100) + 100 ))
  gone=0
  for _ in $(seq 1 "$DEADLINE"); do
    sleep 0.1
    kill -0 "$QUEEN_BROKER_PID" 2>/dev/null || { gone=1; break; }
  done
  [ "$gone" = 1 ] && pass "the broker exited" || fail "the broker is still up after SIGTERM"
  if [ -n "$LAST_CHILD" ] && [ "$LAST_CHILD" != "null" ]; then
    kill -0 "$LAST_CHILD" 2>/dev/null \
      && fail "ORPHAN: facade $LAST_CHILD outlived the broker" \
      || pass "no orphan: facade $LAST_CHILD is gone"
  fi
  # The listener is the observable half of the same claim, and the one an
  # operator hits: a port still held is a restart that will fail to bind.
  if nc -z "$KHOST" "$KPORT" >/dev/null 2>&1; then
    fail "something is still listening on $QUEEN_KAFKA_BOOTSTRAP"
  else
    pass "nothing listens on $QUEEN_KAFKA_BOOTSTRAP"
  fi
fi

echo
if [ "$FAIL" = 0 ]; then
  echo "EMBEDDED MODE: all scenarios passed"
else
  echo "EMBEDDED MODE: FAILURES above"
fi
exit "$FAIL"
