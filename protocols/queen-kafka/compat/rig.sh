#!/usr/bin/env bash
#
# The queen-kafka compatibility rig: a throwaway Postgres, a debug broker, a
# debug facade, and the franz-go suite in compat/go run against them. Nothing it
# starts outlives it — the container, the broker and the facade are torn down on
# every exit path, including a failure or a Ctrl-C.
#
#   protocols/queen-kafka/compat/rig.sh                 # the whole suite
#   protocols/queen-kafka/compat/rig.sh -run TestLongPoll -v
#   protocols/queen-kafka/compat/rig.sh --keep          # leave the stack up afterwards
#   protocols/queen-kafka/compat/rig.sh --m5            # ...plus a TLS + SASL/PLAIN listener
#                                             #    and the credential gate it needs
#
# Every argument that is not --keep is passed through to `go test`, so the whole
# of its flag surface (-run, -v, -count, -timeout) is available.
#
# Ports are deliberately not the defaults: 55432 for Postgres (never 5432 — that
# is a live stack on a developer machine), 6699 for the broker, 19092 for the
# Kafka listener, and under --m5 19093 for the TLS one and 6698 for the
# credential gate in front of the broker. Override with PG_HOST_PORT /
# BROKER_PORT / KAFKA_PORT / KAFKA_TLS_PORT / GATE_PORT.
#
# The facade runs at QUEEN_KAFKA_DEFAULT_PARTITIONS=8: small enough that a
# partition listing stays readable, wide enough that "keys across partitions"
# means something. The suite reads the same number from QUEEN_KAFKA_PARTITIONS.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

PG_HOST_PORT="${PG_HOST_PORT:-55432}"
BROKER_PORT="${BROKER_PORT:-6699}"
KAFKA_PORT="${KAFKA_PORT:-19092}"
# The M5 listener, off unless --m5: TLS + SASL/PLAIN + SNI forwarding, which is
# the Cloud shape. A SECOND facade rather than a reconfigured first one, so the
# default suite keeps running against exactly the listener it always has.
KAFKA_TLS_PORT="${KAFKA_TLS_PORT:-19093}"
# A NAME and not 127.0.0.1: Go (and every other client) sends no SNI for an IP
# literal, so an address here would test TLS and nothing about routing.
KAFKA_TLS_HOST="${KAFKA_TLS_HOST:-localhost}"
PARTITIONS="${PARTITIONS:-8}"
# Overridable so a stage of a campaign can run this rig inside its own assigned
# container namespace while another one is up. PG_HOST_PORT / BROKER_PORT /
# KAFKA_PORT / KAFKA_TLS_PORT / GATE_PORT already are.
CONTAINER="${CONTAINER:-queen-kafka-compat-pg}"

KEEP=0
M5=0
GO_TEST_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --keep) KEEP=1;;
    --m5) M5=1;;
    -h|--help) sed -n '2,25p' "$0"; exit 0;;
    *) GO_TEST_ARGS+=("$arg");;
  esac
done

LOGDIR="$(mktemp -d -t queen-kafka-compat.XXXXXX)"
BROKER_LOG="$LOGDIR/broker.log"
FACADE_LOG="$LOGDIR/facade.log"
FACADE_TLS_LOG="$LOGDIR/facade-tls.log"
GATE_LOG="$LOGDIR/authgate.log"
BROKER_PID=""
FACADE_TLS_PID=""
GATE_PID=""
# The plaintext facade is the one test that RESTARTS it (offsets must outlive
# the process), so its pid lives in a file rather than a shell variable: the
# restart script below writes the new one there and this script's teardown reads
# whatever is current instead of a pid that stopped existing mid-suite.
FACADE_PIDFILE="$LOGDIR/facade.pid"
FACADE_START="$LOGDIR/start-facade.sh"
GATE_PORT="${GATE_PORT:-6698}"
# The password the M5 tests present and the only one the gate in front of the
# broker accepts. Not a secret — it protects a throwaway broker for the length
# of one suite — but it is generated per run rather than hardcoded so that
# nothing outside this rig can be depending on its value.
SASL_TOKEN="${SASL_TOKEN:-rig-tenant-$$-$(date +%s)}"
facade_pid() { [ -f "$FACADE_PIDFILE" ] && cat "$FACADE_PIDFILE"; }

say() { printf '\n=== %s\n' "$*"; }

cleanup() {
  local code=$?
  local fpid
  fpid="$(facade_pid)"
  if [ "$KEEP" = 1 ]; then
    echo
    echo "--keep: the stack is still up."
    echo "  postgres : container $CONTAINER on 127.0.0.1:$PG_HOST_PORT"
    echo "  broker   : pid ${BROKER_PID:-none}, http://127.0.0.1:$BROKER_PORT, log $BROKER_LOG"
    echo "  facade   : pid ${fpid:-none}, 127.0.0.1:$KAFKA_PORT, log $FACADE_LOG"
    [ -n "$GATE_PID" ] && \
      echo "  authgate : pid $GATE_PID, http://127.0.0.1:$GATE_PORT in front of the broker, log $GATE_LOG"
    [ -n "$FACADE_TLS_PID" ] && \
      echo "  facade/m5: pid $FACADE_TLS_PID, $KAFKA_TLS_HOST:$KAFKA_TLS_PORT (TLS+SASL), log $FACADE_TLS_LOG"
    echo "  tear down: kill ${BROKER_PID:-} ${fpid:-} ${GATE_PID:-}; docker rm -f $CONTAINER"
    exit $code
  fi
  say "tearing down"
  [ -n "$FACADE_TLS_PID" ] && kill "$FACADE_TLS_PID" 2>/dev/null
  [ -n "$GATE_PID" ] && kill "$GATE_PID" 2>/dev/null
  [ -n "$fpid" ] && kill "$fpid" 2>/dev/null
  [ -n "$BROKER_PID" ] && kill "$BROKER_PID" 2>/dev/null
  # A debug broker with a full pool can take a moment to unwind; give both a
  # grace period before insisting.
  sleep 1
  [ -n "$FACADE_TLS_PID" ] && kill -9 "$FACADE_TLS_PID" 2>/dev/null
  [ -n "$GATE_PID" ] && kill -9 "$GATE_PID" 2>/dev/null
  [ -n "$fpid" ] && kill -9 "$fpid" 2>/dev/null
  [ -n "$BROKER_PID" ] && kill -9 "$BROKER_PID" 2>/dev/null
  docker rm -f "$CONTAINER" >/dev/null 2>&1
  echo "logs kept at $LOGDIR"
  exit $code
}
trap cleanup EXIT INT TERM

command -v docker >/dev/null || { echo "docker not found" >&2; exit 2; }
command -v go >/dev/null || { echo "go not found" >&2; exit 2; }
command -v cargo >/dev/null || { echo "cargo not found" >&2; exit 2; }

# --------------------------------------------------------------------- postgres
say "postgres on 127.0.0.1:$PG_HOST_PORT (tmpfs, thrown away at exit)"
docker rm -f "$CONTAINER" >/dev/null 2>&1
docker run -d --name "$CONTAINER" \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DB=postgres \
  -e PGDATA=/var/lib/postgresql/data/pgdata \
  -p "$PG_HOST_PORT":5432 \
  --tmpfs /var/lib/postgresql/data:rw,size=2g \
  postgres:16 -c max_connections=400 >/dev/null || exit 1

for _ in $(seq 1 60); do
  docker exec "$CONTAINER" pg_isready -U postgres >/dev/null 2>&1 && break
  sleep 1
done
docker exec "$CONTAINER" pg_isready -U postgres >/dev/null 2>&1 || {
  echo "postgres never became ready" >&2; docker logs "$CONTAINER" | tail -20; exit 1; }

# ----------------------------------------------------------------------- builds
say "building the broker and the facade (debug)"
( cd "$REPO_ROOT/server" && cargo build ) || exit 1
( cd "$REPO_ROOT/protocols/queen-kafka" && cargo build ) || exit 1

# ----------------------------------------------------------------------- broker
say "broker on 127.0.0.1:$BROKER_PORT"
PG_HOST=127.0.0.1 PG_PORT="$PG_HOST_PORT" PG_USER=postgres PG_PASSWORD=postgres \
PG_DATABASE=postgres PORT="$BROKER_PORT" QUEEN_BIND_ADDR=127.0.0.1 \
QUEEN_APPLY_SCHEMA=true DB_POOL_SIZE=32 LOG_LEVEL=info \
  "$REPO_ROOT/server/target/debug/queen" > "$BROKER_LOG" 2>&1 &
BROKER_PID=$!

for _ in $(seq 1 90); do
  curl -fsS -m 2 "http://127.0.0.1:$BROKER_PORT/health" >/dev/null 2>&1 && break
  kill -0 "$BROKER_PID" 2>/dev/null || { echo "the broker died at boot:" >&2; tail -30 "$BROKER_LOG" >&2; exit 1; }
  sleep 1
done
curl -fsS -m 2 "http://127.0.0.1:$BROKER_PORT/health" >/dev/null 2>&1 || {
  echo "the broker never answered /health" >&2; tail -30 "$BROKER_LOG" >&2; exit 1; }

# ----------------------------------------------------------------------- facade
# Written out rather than inlined because the suite runs it a SECOND time: the
# offsets-survive-a-restart test kills the facade and calls this script, so
# "the same facade, restarted" is the same command line by construction and not
# by two copies of it staying in step. It appends to the one log, so a restart
# leaves the whole run readable in FACADE_LOG.
cat > "$FACADE_START" <<SCRIPT
#!/usr/bin/env bash
set -uo pipefail
old=""
if [ -f "$FACADE_PIDFILE" ]; then
  old=\$(cat "$FACADE_PIDFILE")
  # SIGKILL: a restart is meant to look like a crash to every client, so
  # nothing gets the chance to close a connection politely.
  kill -9 "\$old" 2>/dev/null
  for _ in \$(seq 1 50); do kill -0 "\$old" 2>/dev/null || break; sleep 0.1; done
  if kill -0 "\$old" 2>/dev/null; then
    echo "the old facade (\$old) would not die" >&2
    exit 1
  fi
fi
QUEEN_URL="http://127.0.0.1:$BROKER_PORT" \\
QUEEN_KAFKA_ADDR="127.0.0.1:$KAFKA_PORT" \\
QUEEN_KAFKA_ADVERTISED_ADDR="127.0.0.1:$KAFKA_PORT" \\
QUEEN_KAFKA_DEFAULT_PARTITIONS="$PARTITIONS" \\
LOG_LEVEL="${FACADE_LOG_LEVEL:-debug}" \\
  "$REPO_ROOT/protocols/queen-kafka/target/debug/queen-kafka" >> "$FACADE_LOG" 2>&1 &
echo \$! > "$FACADE_PIDFILE"
# Printed so the caller can prove a restart HAPPENED: a script that quietly
# failed to kill anything would otherwise look exactly like a successful one.
echo "facade old=\${old:-none} new=\$!"
for _ in \$(seq 1 100); do
  nc -z 127.0.0.1 "$KAFKA_PORT" >/dev/null 2>&1 && exit 0
  kill -0 "\$(cat "$FACADE_PIDFILE")" 2>/dev/null || { echo "the facade died at boot" >&2; exit 1; }
  sleep 0.2
done
echo "the facade never listened on $KAFKA_PORT" >&2
exit 1
SCRIPT
chmod +x "$FACADE_START"

say "queen-kafka on 127.0.0.1:$KAFKA_PORT (advertising itself, $PARTITIONS partitions per topic)"
"$FACADE_START" || { echo "the facade did not start:" >&2; tail -30 "$FACADE_LOG" >&2; exit 1; }

# ------------------------------------------------------------------ m5 facade
if [ "$M5" = 1 ]; then
  say "queen-kafka on $KAFKA_TLS_HOST:$KAFKA_TLS_PORT (TLS + SASL/PLAIN + SNI forwarding)"
  # The same self-signed certificate the crate's own TLS tests use
  # (protocols/queen-kafka/src/tls.rs, `tls::testing`): a P-256 key, SANs for localhost
  # and the two example names, valid until 2126. It has never protected
  # anything and is written into a throwaway directory that goes with the rig.
  cat > "$LOGDIR/tls.crt" <<'CERT'
-----BEGIN CERTIFICATE-----
MIIBdjCCARygAwIBAgIJANV41LV1o/gQMAoGCCqGSM49BAMCMBsxGTAXBgNVBAMM
EHF1ZWVuLWthZmthIHRlc3QwIBcNMjYwODI4MTIyNTUwWhgPMjEyNjA4MDQxMjI1
NTBaMBsxGTAXBgNVBAMMEHF1ZWVuLWthZmthIHRlc3QwWTATBgcqhkjOPQIBBggq
hkjOPQMBBwNCAARdlget+89VGHXfC7zA6HlCeeMRV8T2UH1h3o5riGO/933SZktL
1Fl0o838Kzs3ZUnJWgwUkWbCUNrpEKDwcuddo0cwRTBDBgNVHREEPDA6ghFrYWZr
YS5leGFtcGxlLmNvbYIUc2hhcmVkLnF1ZWVubXEuY2xvdWSCCWxvY2FsaG9zdIcE
fwAAATAKBggqhkjOPQQDAgNIADBFAiAzXdkkGP0Am063H8DWaT+sL7sWM1xF0HU6
aTUORp7DYAIhAKif63VkTwMYq6m9kSchU9hZcBU0TsuRZvShR/xhYBsb
-----END CERTIFICATE-----
CERT
  cat > "$LOGDIR/tls.key" <<'KEY'
-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgI098cQnkRLhtJQ7E
VlRmM9Z/lkDwtVnX9JXBqCkBUi6hRANCAARdlget+89VGHXfC7zA6HlCeeMRV8T2
UH1h3o5riGO/933SZktL1Fl0o838Kzs3ZUnJWgwUkWbCUNrpEKDwcudd
-----END PRIVATE KEY-----
KEY
  chmod 600 "$LOGDIR/tls.key"

  # The credential gate. The rig's broker runs with JWT_ENABLED unset, so it
  # answers 200 to any bearer and every SASL password would be admitted — a
  # facade in front of it can be shown to FORWARD a credential and never to
  # have one refused. This little reverse proxy is the auth layer instead: one
  # exact-match check, a 401 when it fails, which is the answer
  # handlers::sasl_authenticate maps to SASL_AUTHENTICATION_FAILED.
  say "authgate on 127.0.0.1:$GATE_PORT (the credential check the rig's broker does not do)"
  ( cd "$SCRIPT_DIR/authgate" && GOWORK=off go build -o "$LOGDIR/authgate" . ) || exit 1
  AUTHGATE_ADDR="127.0.0.1:$GATE_PORT" \
  AUTHGATE_UPSTREAM="http://127.0.0.1:$BROKER_PORT" \
  AUTHGATE_TOKEN="$SASL_TOKEN" \
    "$LOGDIR/authgate" > "$GATE_LOG" 2>&1 &
  GATE_PID=$!
  for _ in $(seq 1 30); do
    curl -fsS -m 2 "http://127.0.0.1:$GATE_PORT/health" >/dev/null 2>&1 && break
    kill -0 "$GATE_PID" 2>/dev/null || { echo "the authgate died at boot:" >&2; tail -20 "$GATE_LOG" >&2; exit 1; }
    sleep 0.5
  done
  curl -fsS -m 2 "http://127.0.0.1:$GATE_PORT/health" >/dev/null 2>&1 || {
    echo "the authgate never answered /health" >&2; tail -20 "$GATE_LOG" >&2; exit 1; }

  QUEEN_URL="http://127.0.0.1:$GATE_PORT" \
  QUEEN_KAFKA_ADDR="127.0.0.1:$KAFKA_TLS_PORT" \
  QUEEN_KAFKA_ADVERTISED_ADDR="$KAFKA_TLS_HOST:$KAFKA_TLS_PORT" \
  QUEEN_KAFKA_DEFAULT_PARTITIONS="$PARTITIONS" \
  QUEEN_KAFKA_TLS_CERT="$LOGDIR/tls.crt" \
  QUEEN_KAFKA_TLS_KEY="$LOGDIR/tls.key" \
  QUEEN_KAFKA_SASL=plain \
  QUEEN_KAFKA_FORWARD_SNI_HOST=true \
  LOG_LEVEL="${FACADE_LOG_LEVEL:-debug}" \
    "$REPO_ROOT/protocols/queen-kafka/target/debug/queen-kafka" > "$FACADE_TLS_LOG" 2>&1 &
  FACADE_TLS_PID=$!

  for _ in $(seq 1 30); do
    nc -z 127.0.0.1 "$KAFKA_TLS_PORT" >/dev/null 2>&1 && break
    kill -0 "$FACADE_TLS_PID" 2>/dev/null || {
      echo "the TLS facade died at boot:" >&2; tail -30 "$FACADE_TLS_LOG" >&2; exit 1; }
    sleep 1
  done
  nc -z 127.0.0.1 "$KAFKA_TLS_PORT" >/dev/null 2>&1 || {
    echo "the TLS facade never listened" >&2; tail -30 "$FACADE_TLS_LOG" >&2; exit 1; }
fi

# ------------------------------------------------------------------------ suite
say "franz-go suite"
# GOWORK=off: the repository's root go.work lists the two client modules and not
# this one, so a bare `go test` here refuses to build a module outside the
# workspace. `go test .` and not `./...`: this package is the suite.
cd "$SCRIPT_DIR/go" || exit 1
# QUEEN_KAFKA_TLS_BOOTSTRAP is what makes the M5 tests run at all; without
# --m5 it is empty and they skip.
GOWORK=off \
QUEEN_KAFKA_BOOTSTRAP="127.0.0.1:$KAFKA_PORT" \
QUEEN_KAFKA_TLS_BOOTSTRAP="$([ "$M5" = 1 ] && echo "$KAFKA_TLS_HOST:$KAFKA_TLS_PORT")" \
QUEEN_KAFKA_SASL_TOKEN="$([ "$M5" = 1 ] && echo "$SASL_TOKEN")" \
QUEEN_KAFKA_RESTART_CMD="$FACADE_START" \
QUEEN_URL="http://127.0.0.1:$BROKER_PORT" \
QUEEN_KAFKA_PARTITIONS="$PARTITIONS" \
  go test -timeout 20m "${GO_TEST_ARGS[@]+"${GO_TEST_ARGS[@]}"}" .
RESULT=$?

# The SNI actually arrived, which no assertion on the client side can see: the
# facade names it on the line where a connection is admitted.
if [ "$M5" = 1 ] && [ $RESULT -eq 0 ]; then
  # Two greps rather than one pattern: tracing's default format colours the
  # field NAMES, so `sni=` is not a literal substring of the line — the message
  # and the value are, which is what these match.
  if ! grep "sasl authenticated this connection" "$FACADE_TLS_LOG" |
       grep -q "$KAFKA_TLS_HOST"; then
    echo "the TLS facade never recorded an SNI of $KAFKA_TLS_HOST:" >&2
    grep -i -m5 sasl "$FACADE_TLS_LOG" >&2
    RESULT=1
  fi
fi

# A panic on either side is a failure even when every assertion passed: the
# facade is meant to survive whatever a client sends it.
for log in "$BROKER_LOG" "$FACADE_LOG" "$FACADE_TLS_LOG"; do
  [ -s "$log" ] || continue
  if grep -qi 'panic' "$log"; then
    echo "PANIC in $log:" >&2
    grep -i -m5 -A5 'panic' "$log" >&2
    RESULT=1
  fi
done

say "result: $([ $RESULT -eq 0 ] && echo PASS || echo FAIL)"
exit $RESULT
