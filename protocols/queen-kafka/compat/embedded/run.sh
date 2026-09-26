#!/usr/bin/env bash
# queen-kafka compat: EMBEDDED MODE acceptance
#
# Embedded mode is `QUEEN_KAFKA_EMBEDDED=true` on the BROKER: the broker runs
# this facade IN-PROCESS, on its own tokio runtime, calling the broker through
# its router rather than over a socket (server/src/kafka_inproc.rs). One
# deployment, one process.
#
# This script runs against a stack that is ALREADY UP. Nothing here starts a
# broker -- that is rig-embedded.sh's job, or yours -- except through the
# restart command it is handed. Every address comes from the environment so it
# can be wired into a rig without editing a line of it.
#
# REQUIRED:
#   QUEEN_KAFKA_BOOTSTRAP   the embedded facade's host:port, e.g. 127.0.0.1:32602
#   QUEEN_BROKER_URL        the broker it runs in, e.g. http://127.0.0.1:32601
#
# OPTIONAL -- each unset variable SKIPS the scenario that needs it, loudly:
#   QUEEN_BROKER_PIDFILE    a file holding the broker's CURRENT pid. Without it
#                           the one-process check and the shutdown scenario
#                           cannot run: both are claims about a process, and a
#                           pid is the only way to look at one. A file and not a
#                           pid, because scenario 4 replaces the process.
#   QUEEN_BROKER_RESTART_CMD  an executable that SIGKILLs the broker and starts
#                           it again on the same data directory, writing the new
#                           pid to QUEEN_BROKER_PIDFILE, and returns once /health
#                           and the Kafka port answer (scenario 4)
#   QUEEN_BROKER_LOG        the broker's log file; without it the facade's own
#                           lines cannot be read
#   QUEEN_EMBEDDED_SHUTDOWN=1  opt in to scenario 6, which is DESTRUCTIVE: it
#                           SIGTERMs the broker and asserts the Kafka listener
#                           went with it. It runs last and leaves the stack down.
#
# TUNING, all with defaults that match the facade's own:
#   QUEEN_KAFKA_PARTITIONS  the facade's QUEEN_KAFKA_DEFAULT_PARTITIONS (8)
#   QUEEN_KAFKA_GRACE_MS    the broker's QUEEN_KAFKA_SHUTDOWN_GRACE_MS (5000)
#   RUN_ID                  suffix on every topic and group (default: epoch)
#
# Exits non-zero if any assertion failed, and prints what it read.
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

broker_pid() {
  [ -n "${QUEEN_BROKER_PIDFILE:-}" ] && [ -f "$QUEEN_BROKER_PIDFILE" ] && cat "$QUEEN_BROKER_PIDFILE"
}

# A field of the `kafka` block of GET /status. The broker renders it with
# serde_json, so the body is compact and one `"key":value` per key -- no jq
# dependency for a handful of fields.
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

TMP="$(mktemp -d -t qk-embedded-run.XXXXXX)"
trap 'rm -rf "$TMP"' EXIT

echo "queen-kafka compat: EMBEDDED MODE (in-process)"
echo "  bootstrap  $QUEEN_KAFKA_BOOTSTRAP"
echo "  broker     $QUEEN_BROKER_URL"
echo "  broker pid $(broker_pid || echo '(unset: the one-process check and shutdown will SKIP)')"
echo "  restart    ${QUEEN_BROKER_RESTART_CMD:-(unset: the crash-and-resume scenario will SKIP)}"
echo "  broker log ${QUEEN_BROKER_LOG:-(unset: the log assertions will SKIP)}"
echo "  topic      $TOPIC   group $GROUP   partitions $PARTITIONS"

# --------------------------------------------------------------- 1. the broker
# says it is running the facade, in the one place an operator already looks.
say "1. GET /status reports the in-process facade"
BODY="$(curl -fsS -m 5 "$QUEEN_BROKER_URL/status" 2>/dev/null)"
echo "  $BODY"
case "$BODY" in
  *'"mode":"in-process"'*) pass "/status carries the kafka block, mode in-process" ;;
  *) fail "/status has no in-process kafka block -- is QUEEN_KAFKA_EMBEDDED=true on this broker?" ;;
esac
[ "$(status_field phase)" = '"running"' ] \
  && pass "phase=running" || fail "phase=$(status_field phase), expected \"running\""
[ "$(status_field transport)" = '"in-process"' ] \
  && pass "transport=in-process: calls reach the broker through its router, not a socket" \
  || fail "transport=$(status_field transport), expected \"in-process\" (an explicit QUEEN_URL keeps HTTP)"

# ------------------------------------------------------- 2. one process
# The point of in-process mode: there is no second process to deploy, supervise
# or orphan. The broker has no child, and the Kafka port is the broker's own.
say "2. the facade is inside the broker process"
BPID="$(broker_pid)"
if [ -z "$BPID" ]; then
  skip "QUEEN_BROKER_PIDFILE unset"
else
  kids="$(pgrep -P "$BPID" 2>/dev/null | tr '\n' ' ')"
  [ -z "$kids" ] && pass "broker $BPID has no child process" \
                 || fail "broker $BPID has children: $kids"
  if command -v lsof >/dev/null; then
    holders="$(lsof -nP -iTCP:"$KPORT" -sTCP:LISTEN -t 2>/dev/null | sort -u | tr '\n' ' ')"
    case " $holders " in
      *" $BPID "*) pass "the Kafka port $KPORT is held by the broker itself" ;;
      *) fail "the Kafka port $KPORT is held by [${holders% }], not by broker $BPID" ;;
    esac
  else
    skip "lsof not found: the port holder cannot be read"
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

# ------------------------------------- 4. a crash, and what survives it
# SIGKILL is the whole test: a broker that was asked politely to stop proves
# nothing about a crash. What must survive it is not in the facade -- the
# records and the group's committed offsets are in the broker's data directory
# -- so after the restart the group must read exactly the records produced
# before the kill that it had not consumed yet: no replay and no gap.
say "4. SIGKILL the broker and restart it on its data directory: offsets resume"
if [ -z "${QUEEN_BROKER_RESTART_CMD:-}" ] || [ -z "$(broker_pid)" ]; then
  skip "set QUEEN_BROKER_RESTART_CMD and QUEEN_BROKER_PIDFILE to run it"
else
  OLD_PID="$(broker_pid)"
  produce 9 8 || fail "second produce failed"
  if "$QUEEN_BROKER_RESTART_CMD"; then
    NEW_PID="$(broker_pid)"
    if [ -n "$NEW_PID" ] && [ "$NEW_PID" != "$OLD_PID" ]; then
      pass "restarted: $OLD_PID -> $NEW_PID"
    else
      fail "the restart command left the pid at ${NEW_PID:-none}"
    fi
  else
    fail "the restart command failed"
  fi
  [ "$(status_field phase)" = '"running"' ] \
    && pass "the facade came back with the broker" \
    || fail "phase=$(status_field phase) after the restart, expected \"running\""
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
say "5. the facade's lines in the broker log"
if [ -z "${QUEEN_BROKER_LOG:-}" ]; then
  skip "QUEEN_BROKER_LOG unset"
else
  grep -q 'starting the Kafka facade in-process' "$QUEEN_BROKER_LOG" \
    && pass "the start is logged" || fail "no start line in $QUEEN_BROKER_LOG"
  # The serve loop is restarted only when it ends on its own (a panic in the
  # accept loop, a listener error). Nothing in this suite should cause one.
  RESTARTS=$(grep -c 'in-process Kafka facade stopped; restarting it' "$QUEEN_BROKER_LOG" || true)
  [ "$RESTARTS" = 0 ] && pass "the facade's serve loop never ended on its own" \
                      || fail "$RESTARTS unplanned facade restarts in $QUEEN_BROKER_LOG"
fi

# ------------------------------------------------- 6. shutdown leaves nothing
say "6. SIGTERM the broker: the Kafka listener goes with it (destructive)"
BPID="$(broker_pid)"
if [ "${QUEEN_EMBEDDED_SHUTDOWN:-0}" != "1" ] || [ -z "$BPID" ]; then
  skip "set QUEEN_EMBEDDED_SHUTDOWN=1 and QUEEN_BROKER_PIDFILE to run it (it stops the stack)"
else
  kill -TERM "$BPID" 2>/dev/null || fail "could not SIGTERM $BPID"
  # The broker drains in-flight requests, then stops the facade and WAITS for
  # it. The budget is the facade's grace window plus a generous margin for the
  # drain itself.
  DEADLINE=$(( (GRACE_MS / 100) + 100 ))
  gone=0
  for _ in $(seq 1 "$DEADLINE"); do
    sleep 0.1
    kill -0 "$BPID" 2>/dev/null || { gone=1; break; }
  done
  [ "$gone" = 1 ] && pass "the broker exited" || fail "the broker is still up after SIGTERM"
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
