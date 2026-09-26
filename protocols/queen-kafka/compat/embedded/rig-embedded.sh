#!/usr/bin/env bash
#
# The EMBEDDED MODE rig: three brokers, each one raft node on its own throwaway
# data directory -- one running the Kafka facade IN-PROCESS
# (QUEEN_KAFKA_EMBEDDED=true), one WITHOUT the flag (the default-off
# regression), and one whose facade configuration is invalid (the boot
# refusal). It runs run.sh against the first, asserts the other two itself, and
# tears everything down on every exit path including a failure or a Ctrl-C.
#
#   protocols/queen-kafka/compat/embedded/rig-embedded.sh
#   protocols/queen-kafka/compat/embedded/rig-embedded.sh --keep      # leave the stack up
#
# Ports are deliberately not the defaults (6632 is a live stack on a developer
# machine): 32601/32602 the embedded broker and its Kafka listener, 32603 the
# default-off broker, 32604/32605 the refused one. Override with BROKER_PORT /
# KAFKA_PORT / OFF_PORT / REFUSED_PORT / REFUSED_KAFKA_PORT.
#
# The brokers' disk gate refuses writes (507) once a data directory's
# filesystem is QUEEN_RAFT_DISK_HIGH_PCT used, 85 by default; a throwaway rig on
# a developer disk is not what it protects, so the rig runs it at 99.5 unless
# QUEEN_RAFT_DISK_HIGH_PCT says otherwise.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

BROKER_PORT="${BROKER_PORT:-32601}"
KAFKA_PORT="${KAFKA_PORT:-32602}"
OFF_PORT="${OFF_PORT:-32603}"
REFUSED_PORT="${REFUSED_PORT:-32604}"
REFUSED_KAFKA_PORT="${REFUSED_KAFKA_PORT:-32605}"
PARTITIONS="${PARTITIONS:-8}"
DISK_HIGH_PCT="${QUEEN_RAFT_DISK_HIGH_PCT:-99.5}"

KEEP=0
RUN_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --keep) KEEP=1;;
    -h|--help) sed -n '2,21p' "$0"; exit 0;;
    *) RUN_ARGS+=("$arg");;
  esac
done

# No "kafka" in this path on purpose: the brokers log their data directory, and
# the default-off check below counts every log line that says kafka.
LOGDIR="$(mktemp -d -t qk-embedded.XXXXXX)"
BROKER_LOG="$LOGDIR/broker.log"
OFF_LOG="$LOGDIR/broker-off.log"
REFUSED_LOG="$LOGDIR/broker-refused.log"
# The embedded broker is the one run.sh SIGKILLs and restarts on its own data
# directory, so its pid lives in a file: the start script writes the current
# one there and teardown reads it instead of a pid that stopped existing.
BROKER_PIDFILE="$LOGDIR/broker.pid"
BROKER_START="$LOGDIR/start-broker.sh"
OFF_PID=""
REFUSED_PID=""
broker_pid() { [ -f "$BROKER_PIDFILE" ] && cat "$BROKER_PIDFILE"; }

say() { printf '\n=== %s\n' "$*"; }

cleanup() {
  local code=$?
  local bpid
  bpid="$(broker_pid)"
  if [ "$KEEP" = 1 ]; then
    echo
    echo "--keep: the stack is still up."
    echo "  broker   : pid ${bpid:-none}, http://127.0.0.1:$BROKER_PORT, data $LOGDIR/raft, log $BROKER_LOG"
    echo "  facade   : 127.0.0.1:$KAFKA_PORT (INSIDE the broker; it has no pid of its own)"
    echo "  tear down: kill ${bpid:-} ${OFF_PID:-} ${REFUSED_PID:-}; rm -rf $LOGDIR/raft*"
    exit $code
  fi
  say "tearing down"
  # Only pids this script recorded at spawn. The facade goes with its broker
  # (that is the feature); the kill -9 pass is the backstop for a broker that
  # never got to run its shutdown.
  for p in "$REFUSED_PID" "$OFF_PID" "$bpid"; do
    [ -n "$p" ] && kill "$p" 2>/dev/null
  done
  sleep 2
  for p in "$REFUSED_PID" "$OFF_PID" "$bpid"; do
    [ -n "$p" ] && kill -9 "$p" 2>/dev/null
  done
  rm -rf "$LOGDIR/raft" "$LOGDIR/raft-off" "$LOGDIR/raft-refused"
  echo "logs kept at $LOGDIR"
  exit $code
}
trap cleanup EXIT INT TERM

command -v cargo  >/dev/null || { echo "cargo not found" >&2; exit 2; }
command -v kcat   >/dev/null || { echo "kcat not found (brew install kcat)" >&2; exit 2; }

# ----------------------------------------------------------------------- builds
# The facade is a library linked into the broker (the `kafka` feature, on by
# default): one build, one binary.
say "building the broker (debug, with the Kafka facade linked in)"
( cd "$REPO_ROOT/server" && cargo build ) || exit 1
BROKER_BIN="$REPO_ROOT/server/target/debug/queen"

wait_health() { # $1 = port, $2 = pid, $3 = log
  for _ in $(seq 1 90); do
    curl -fsS -m 2 "http://127.0.0.1:$1/health" >/dev/null 2>&1 && return 0
    kill -0 "$2" 2>/dev/null || { echo "the broker died at boot:" >&2; tail -30 "$3" >&2; return 1; }
    sleep 1
  done
  echo "the broker never answered /health" >&2; tail -30 "$3" >&2; return 1
}

# ------------------------------------------------- broker WITH the facade embedded
# Written out rather than inlined because run.sh runs it AGAIN: scenario 4
# SIGKILLs the broker and calls this script, so "the same broker, restarted on
# its own data directory" is the same command line by construction. It appends
# to the one log, so a restart leaves the whole run readable in BROKER_LOG.
cat > "$BROKER_START" <<SCRIPT
#!/usr/bin/env bash
set -uo pipefail
old=""
if [ -f "$BROKER_PIDFILE" ]; then
  old=\$(cat "$BROKER_PIDFILE")
  # SIGKILL: a restart is meant to look like a crash to every client and to
  # the data directory, so nothing gets to close or flush anything politely.
  kill -9 "\$old" 2>/dev/null
  for _ in \$(seq 1 50); do kill -0 "\$old" 2>/dev/null || break; sleep 0.1; done
  if kill -0 "\$old" 2>/dev/null; then
    echo "the old broker (\$old) would not die" >&2
    exit 1
  fi
fi
mkdir -p "$LOGDIR/raft"
QUEEN_RAFT_DIR="$LOGDIR/raft" QUEEN_RAFT_DISK_HIGH_PCT="$DISK_HIGH_PCT" \\
PORT="$BROKER_PORT" QUEEN_BIND_ADDR=127.0.0.1 LOG_LEVEL=info \\
QUEEN_KAFKA_EMBEDDED=true \\
QUEEN_KAFKA_ADDR="127.0.0.1:$KAFKA_PORT" \\
QUEEN_KAFKA_ADVERTISED_ADDR="127.0.0.1:$KAFKA_PORT" \\
QUEEN_KAFKA_DEFAULT_PARTITIONS="$PARTITIONS" \\
  "$BROKER_BIN" >> "$BROKER_LOG" 2>&1 &
echo \$! > "$BROKER_PIDFILE"
# Printed so the caller can prove a restart HAPPENED.
echo "broker old=\${old:-none} new=\$!"
for _ in \$(seq 1 90); do
  if curl -fsS -m 2 "http://127.0.0.1:$BROKER_PORT/health" >/dev/null 2>&1 \\
     && nc -z 127.0.0.1 "$KAFKA_PORT" >/dev/null 2>&1; then
    exit 0
  fi
  kill -0 "\$(cat "$BROKER_PIDFILE")" 2>/dev/null || { echo "the broker died at boot" >&2; exit 1; }
  sleep 1
done
echo "the broker never answered /health with its Kafka listener up on $KAFKA_PORT" >&2
exit 1
SCRIPT
chmod +x "$BROKER_START"

say "broker on 127.0.0.1:$BROKER_PORT with QUEEN_KAFKA_EMBEDDED=true (facade on $KAFKA_PORT)"
"$BROKER_START" || { echo "the embedded broker did not start:" >&2; tail -30 "$BROKER_LOG" >&2; exit 1; }

FAIL=0

# ------------------------------------------------------- the default-off regression
# The claim embedded mode has to earn: a broker that did not ask for it is the
# broker that existed before the feature. No kafka lines, no child, and a
# /status body byte-identical to the one it always answered.
say "default-off regression: a broker on $OFF_PORT with no QUEEN_KAFKA_* at all"
mkdir -p "$LOGDIR/raft-off"
env QUEEN_RAFT_DIR="$LOGDIR/raft-off" QUEEN_RAFT_DISK_HIGH_PCT="$DISK_HIGH_PCT" \
  PORT="$OFF_PORT" QUEEN_BIND_ADDR=127.0.0.1 LOG_LEVEL=info \
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

# ------------------------------------------------------------ the boot refusal
# A facade configuration the facade refuses (QUEEN_KAFKA_DEFAULT_PARTITIONS=0 is
# not a partition count). In-process there is no child to crash-loop: the
# broker resolves the facade's configuration at boot, before its listener binds,
# and refuses to start with the facade's own sentence. What is measured is that
# it EXITS, non-zero, naming the variable, and holds neither port afterwards.
say "boot refusal: a broker on $REFUSED_PORT whose facade configuration is invalid"
mkdir -p "$LOGDIR/raft-refused"
env QUEEN_RAFT_DIR="$LOGDIR/raft-refused" QUEEN_RAFT_DISK_HIGH_PCT="$DISK_HIGH_PCT" \
  PORT="$REFUSED_PORT" QUEEN_BIND_ADDR=127.0.0.1 LOG_LEVEL=info \
  QUEEN_KAFKA_EMBEDDED=true \
  QUEEN_KAFKA_ADDR="127.0.0.1:$REFUSED_KAFKA_PORT" \
  QUEEN_KAFKA_ADVERTISED_ADDR="127.0.0.1:$REFUSED_KAFKA_PORT" \
  QUEEN_KAFKA_DEFAULT_PARTITIONS=0 \
  "$BROKER_BIN" > "$REFUSED_LOG" 2>&1 &
REFUSED_PID=$!
exited=0
for _ in $(seq 1 300); do
  kill -0 "$REFUSED_PID" 2>/dev/null || { exited=1; break; }
  sleep 0.1
done
if [ "$exited" = 1 ]; then
  wait "$REFUSED_PID"; rc=$?
  REFUSED_PID=""
  if [ "$rc" != 0 ]; then echo "  PASS  the broker refused to boot (exit $rc)"; else echo "  FAIL  the broker exited 0"; FAIL=1; fi
  if grep -q 'QUEEN_KAFKA_DEFAULT_PARTITIONS' "$REFUSED_LOG"; then
    echo "  PASS  the refusal names QUEEN_KAFKA_DEFAULT_PARTITIONS"
  else
    echo "  FAIL  the refusal does not name the variable:"; tail -5 "$REFUSED_LOG"; FAIL=1
  fi
  for p in "$REFUSED_PORT" "$REFUSED_KAFKA_PORT"; do
    if nc -z 127.0.0.1 "$p" >/dev/null 2>&1; then
      echo "  FAIL  something listens on $p after the refusal"; FAIL=1
    else
      echo "  PASS  nothing listens on $p"
    fi
  done
else
  echo "  FAIL  the broker is still running 30s after an invalid facade configuration"; FAIL=1
fi
kill "$OFF_PID" 2>/dev/null; OFF_PID=""

# ------------------------------------------------------------------ the suite
say "run.sh against the embedded broker"
QUEEN_KAFKA_BOOTSTRAP="127.0.0.1:$KAFKA_PORT" \
QUEEN_BROKER_URL="http://127.0.0.1:$BROKER_PORT" \
QUEEN_BROKER_PIDFILE="$BROKER_PIDFILE" \
QUEEN_BROKER_RESTART_CMD="$BROKER_START" \
QUEEN_BROKER_LOG="$BROKER_LOG" \
QUEEN_KAFKA_PARTITIONS="$PARTITIONS" \
QUEEN_EMBEDDED_SHUTDOWN="${QUEEN_EMBEDDED_SHUTDOWN:-1}" \
  "$SCRIPT_DIR/run.sh" "${RUN_ARGS[@]+"${RUN_ARGS[@]}"}" || FAIL=1
# Scenario 6 stops the broker on purpose; teardown must not signal a pid that
# may since belong to something else.
bpid="$(broker_pid)"
if [ -n "$bpid" ] && ! kill -0 "$bpid" 2>/dev/null; then rm -f "$BROKER_PIDFILE"; fi

say "rig verdict"
[ "$FAIL" = 0 ] && echo "EMBEDDED MODE: rig green" || echo "EMBEDDED MODE: rig RED"
exit "$FAIL"
