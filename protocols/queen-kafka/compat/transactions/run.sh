#!/usr/bin/env bash
#
# compat/transactions -- the M9 TRANSACTIONS acceptance suite.
#
# It brings up its OWN stack: a throwaway Postgres, a debug broker and THREE
# debug facades, because three of the checks need a facade configured
# differently from the others and reconfiguring one mid-suite would make every
# earlier result unreproducible.
#
#   127.0.0.1:32912   the facade under test, single-node, 4 partitions
#   127.0.0.1:32913   the same binary with QUEEN_KAFKA_NODE_ID set (scenario 7)
#   127.0.0.1:32914   the same binary with the transaction caps at their floor
#                     and 70 partitions per topic (scenario 6)
#
# Nothing it starts outlives it. The container name and every port are its own
# and are overridable, so it can run beside compat/rig.sh.
#
#   queen-kafka/compat/transactions/run.sh              # everything
#   queen-kafka/compat/transactions/run.sh s1 s3        # only those scenarios
#   queen-kafka/compat/transactions/run.sh --keep       # leave the stack up
#
# The scenarios, and the DESIGN section 8.2 check each one settles:
#
#   s2   A1   initTransactions() is fast (the campaign measured a 20 s hang)
#   s1   A2+A3 commit visibility, abort invisibility, read_uncommitted divergence
#   s3   A4   fencing, asserted by reading the partitions
#   s8        the idempotent producer is untouched
#   s6   A9   the stage caps
#   s7        the cluster-mode refusal
#   s4   A5   crash mid-transaction  (restarts the facade)
#   eos  A6   exactly-once consume-transform-produce with an induced crash
#   go        a quick compat/go run, for the regression half of A11
#
# Requires docker, cargo, go and a JDK 17 or newer. The kafka-clients jars are
# fetched from Maven Central on first use and cached OUTSIDE the repository, the
# same way compat/java-matrix does it; set JARS_CACHE to a populated directory
# to run with no network.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../../.." && pwd)"

PG_HOST_PORT="${PG_HOST_PORT:-32910}"
BROKER_PORT="${BROKER_PORT:-32911}"
KAFKA_PORT="${KAFKA_PORT:-32912}"
KAFKA_CLUSTER_PORT="${KAFKA_CLUSTER_PORT:-32913}"
KAFKA_TIGHT_PORT="${KAFKA_TIGHT_PORT:-32914}"
CONTAINER="${CONTAINER:-qkt-acc-pg}"
PARTITIONS="${PARTITIONS:-4}"
# Wider than MAX_TXN_OFFSETS (62) so one sendOffsetsToTransaction can exceed it.
TIGHT_PARTITIONS="${TIGHT_PARTITIONS:-70}"
KAFKA_VERSION="${KAFKA_VERSION:-4.3.1}"
CACHE="${JARS_CACHE:-${TMPDIR:-/tmp}/queen-kafka-clients}"
CENTRAL="${MAVEN_CENTRAL:-https://repo1.maven.org/maven2}"
RUN_ID="${RUN_ID:-$(date +%s)}"

KEEP=0
SCENARIOS=()
for arg in "$@"; do
  case "$arg" in
    --keep) KEEP=1;;
    -h|--help) sed -n '2,40p' "$0"; exit 0;;
    *) SCENARIOS+=("$arg");;
  esac
done
[ ${#SCENARIOS[@]} -eq 0 ] && SCENARIOS=(s2 s1 s3 s8 s6 s7 s4 eos go)

LOGDIR="$(mktemp -d -t queen-kafka-txn.XXXXXX)"
BROKER_LOG="$LOGDIR/broker.log"
FACADE_LOG="$LOGDIR/facade.log"
CLUSTER_LOG="$LOGDIR/facade-cluster.log"
TIGHT_LOG="$LOGDIR/facade-tight.log"
BROKER_PID=""
CLUSTER_PID=""
TIGHT_PID=""
# The main facade is the one that gets SIGKILLed, so its pid lives in a file:
# the restart script writes the new one there and teardown reads whatever is
# current instead of a pid that stopped existing mid-suite.
FACADE_PIDFILE="$LOGDIR/facade.pid"
FACADE_START="$LOGDIR/start-facade.sh"
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
    echo "  cluster  : pid ${CLUSTER_PID:-none}, 127.0.0.1:$KAFKA_CLUSTER_PORT, log $CLUSTER_LOG"
    echo "  tight    : pid ${TIGHT_PID:-none}, 127.0.0.1:$KAFKA_TIGHT_PORT, log $TIGHT_LOG"
    echo "  tear down: kill ${BROKER_PID:-} ${fpid:-} ${CLUSTER_PID:-} ${TIGHT_PID:-}; docker rm -f $CONTAINER"
    exit $code
  fi
  say "tearing down"
  # Only pids this script recorded, and only the container it named.
  for pid in "$TIGHT_PID" "$CLUSTER_PID" "$fpid" "$BROKER_PID"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null
  done
  sleep 1
  for pid in "$TIGHT_PID" "$CLUSTER_PID" "$fpid" "$BROKER_PID"; do
    [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null
  done
  docker rm -f "$CONTAINER" >/dev/null 2>&1
  echo "logs kept at $LOGDIR"
  exit $code
}
trap cleanup EXIT INT TERM

command -v docker >/dev/null || { echo "docker not found" >&2; exit 2; }
command -v cargo  >/dev/null || { echo "cargo not found" >&2; exit 2; }
command -v go     >/dev/null || { echo "go not found" >&2; exit 2; }
command -v java   >/dev/null || { echo "java not found" >&2; exit 2; }

# ------------------------------------------------------------------- the jars
fetch() { # fetch <maven/path/to.jar> <destdir>
  local path="$1" dest="$2" file
  file="$dest/$(basename "$path")"
  [ -s "$file" ] && return 0
  echo "     fetching $(basename "$path")"
  curl -sS -f -m 180 -o "$file" "$CENTRAL/$path" || { rm -f "$file"; return 1; }
}
JARS="$CACHE/$KAFKA_VERSION"
mkdir -p "$JARS" || exit 1
fetch "org/apache/kafka/kafka-clients/$KAFKA_VERSION/kafka-clients-$KAFKA_VERSION.jar" "$JARS" || exit 1
for d in com/github/luben/zstd-jni/1.5.6-10/zstd-jni-1.5.6-10.jar \
         at/yawk/lz4/lz4-java/1.10.2/lz4-java-1.10.2.jar \
         org/xerial/snappy/snappy-java/1.1.10.7/snappy-java-1.1.10.7.jar \
         org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar \
         org/slf4j/slf4j-simple/1.7.36/slf4j-simple-1.7.36.jar; do
  fetch "$d" "$JARS" || exit 1
done

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
( cd "$REPO_ROOT/queen-kafka" && cargo build ) || exit 1

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

# --------------------------------------------------------------- main facade
# Written out rather than inlined because two scenarios run it AGAIN: s4 and the
# EOS loop SIGKILL the facade mid-transaction and call this script, so "the same
# facade, restarted" is the same command line by construction.
cat > "$FACADE_START" <<SCRIPT
#!/usr/bin/env bash
set -uo pipefail
old=""
if [ -f "$FACADE_PIDFILE" ]; then
  old=\$(cat "$FACADE_PIDFILE")
  # SIGKILL: a restart is meant to look like a crash to every client, so
  # nothing gets the chance to close a connection politely or to flush a stage.
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
  "$REPO_ROOT/queen-kafka/target/debug/queen-kafka" >> "$FACADE_LOG" 2>&1 &
echo \$! > "$FACADE_PIDFILE"
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

say "queen-kafka on 127.0.0.1:$KAFKA_PORT ($PARTITIONS partitions per topic)"
"$FACADE_START" || { echo "the facade did not start:" >&2; tail -30 "$FACADE_LOG" >&2; exit 1; }

wait_for_port() { # wait_for_port <port> <pid> <log>
  for _ in $(seq 1 100); do
    nc -z 127.0.0.1 "$1" >/dev/null 2>&1 && return 0
    kill -0 "$2" 2>/dev/null || { echo "the facade on $1 died at boot:" >&2; tail -30 "$3" >&2; return 1; }
    sleep 0.2
  done
  echo "nothing ever listened on $1" >&2; tail -30 "$3" >&2; return 1
}

# ------------------------------------------------------------ cluster facade
# QUEEN_KAFKA_NODE_ID is the ONE switch of cluster mode, and it is what the
# transaction gate reads: the refusal is on CONFIGURATION and not on the live
# view, so a clustered facade that happens to be alone still refuses.
# QUEEN_TOKEN is mandatory beside it (the node registry is written with this
# process's credential); the rig's broker runs with JWT off, so any string does.
say "queen-kafka on 127.0.0.1:$KAFKA_CLUSTER_PORT (QUEEN_KAFKA_NODE_ID=1, the cluster gate)"
QUEEN_URL="http://127.0.0.1:$BROKER_PORT" \
QUEEN_TOKEN="qkt-acc-cluster" \
QUEEN_KAFKA_ADDR="127.0.0.1:$KAFKA_CLUSTER_PORT" \
QUEEN_KAFKA_ADVERTISED_ADDR="127.0.0.1:$KAFKA_CLUSTER_PORT" \
QUEEN_KAFKA_DEFAULT_PARTITIONS="$PARTITIONS" \
QUEEN_KAFKA_NODE_ID=1 \
QUEEN_KAFKA_CLUSTER="qktacc" \
LOG_LEVEL="${FACADE_LOG_LEVEL:-debug}" \
  "$REPO_ROOT/queen-kafka/target/debug/queen-kafka" > "$CLUSTER_LOG" 2>&1 &
CLUSTER_PID=$!
wait_for_port "$KAFKA_CLUSTER_PORT" "$CLUSTER_PID" "$CLUSTER_LOG" || exit 1

# -------------------------------------------------------------- tight facade
# The caps at their floor. QUEEN_KAFKA_TXN_MAX_BYTES=65536 is the minimum the
# binary accepts, which is what makes the byte cap reachable in a second rather
# than in eight megabytes of records.
say "queen-kafka on 127.0.0.1:$KAFKA_TIGHT_PORT (the caps at their floor, $TIGHT_PARTITIONS partitions)"
QUEEN_URL="http://127.0.0.1:$BROKER_PORT" \
QUEEN_KAFKA_ADDR="127.0.0.1:$KAFKA_TIGHT_PORT" \
QUEEN_KAFKA_ADVERTISED_ADDR="127.0.0.1:$KAFKA_TIGHT_PORT" \
QUEEN_KAFKA_DEFAULT_PARTITIONS="$TIGHT_PARTITIONS" \
QUEEN_KAFKA_TXN_MAX_BYTES=65536 \
QUEEN_KAFKA_TXN_MAX_STAGED_BYTES=1048576 \
LOG_LEVEL="${FACADE_LOG_LEVEL:-debug}" \
  "$REPO_ROOT/queen-kafka/target/debug/queen-kafka" > "$TIGHT_LOG" 2>&1 &
TIGHT_PID=$!
wait_for_port "$KAFKA_TIGHT_PORT" "$TIGHT_PID" "$TIGHT_LOG" || exit 1

# -------------------------------------------------------------- the scenarios
RESULT=0
SUMMARY=""
note() { SUMMARY="$SUMMARY$1"$'\n'; }

java_scenario() { # java_scenario <name> <bootstrap>
  local name="$1" bootstrap="$2"
  say "scenario $name against $bootstrap"
  ( QK_BOOTSTRAP="$bootstrap" \
    QK_RUN="$RUN_ID" \
    QK_PARTITIONS="$PARTITIONS" \
    QK_RESTART_CMD="$FACADE_START" \
    java -Dorg.slf4j.simpleLogger.defaultLogLevel="${QK_JAVA_LOG:-warn}" \
         -cp "$JARS/*" "$HERE/QueenKafkaTxn.java" "$name" )
  local code=$?
  [ $code -ne 0 ] && RESULT=1
  note "$(printf '  %-6s %s' "$name" "$([ $code -eq 0 ] && echo PASS || echo FAIL)")"
}

for s in "${SCENARIOS[@]}"; do
  case "$s" in
    s1|s2|s3|s4|s8) java_scenario "$s" "127.0.0.1:$KAFKA_PORT";;
    s6)             java_scenario "$s" "127.0.0.1:$KAFKA_TIGHT_PORT";;
    s7)             java_scenario "$s" "127.0.0.1:$KAFKA_CLUSTER_PORT";;
    eos)
      say "scenario eos against 127.0.0.1:$KAFKA_PORT (franz-go, with an induced crash)"
      ( cd "$HERE/eos" && GOWORK=off GOPROXY=off \
        QK_BOOTSTRAP="127.0.0.1:$KAFKA_PORT" \
        QK_RUN="$RUN_ID" \
        QK_RESTART_CMD="$FACADE_START" \
        go run . )
      code=$?
      [ $code -ne 0 ] && RESULT=1
      note "$(printf '  %-6s %s' eos "$([ $code -eq 0 ] && echo PASS || echo FAIL)")"
      ;;
    go)
      # The regression half of A11: the existing franz-go suite's produce,
      # consume and idempotent lanes, against a facade that now has
      # transactions. -count=1 so nothing is served from the test cache.
      say "compat/go quick run (produce, consume, idempotent, smoke)"
      ( cd "$REPO_ROOT/queen-kafka/compat/go" && GOWORK=off \
        QUEEN_KAFKA_BOOTSTRAP="127.0.0.1:$KAFKA_PORT" \
        QUEEN_URL="http://127.0.0.1:$BROKER_PORT" \
        QUEEN_KAFKA_PARTITIONS="$PARTITIONS" \
        QUEEN_KAFKA_RESTART_CMD="$FACADE_START" \
        go test -count=1 -timeout 10m \
          -run 'TestProduce|TestConsume|TestInitProducerId|TestATransactional|TestATransactionCoordinator|TestAnEmptyTransactional|TestFranzGo|TestADuplicate|TestAGap|TestAProducer|TestAStale|TestAnIdempotent|TestConcurrentProducers|TestListOffsets|TestLongPoll|TestFetchBeyond' . )
      code=$?
      [ $code -ne 0 ] && RESULT=1
      note "$(printf '  %-6s %s' go "$([ $code -eq 0 ] && echo PASS || echo FAIL)")"
      ;;
    *) echo "unknown scenario $s" >&2; RESULT=1;;
  esac
done

# ----------------------------------------------------------------- the logs
# A panic is a failure even when every assertion passed: the facade is meant to
# survive whatever a client sends it. An ERROR is one too -- every
# tracing::error in the transaction path is on a branch its own comment calls
# unreachable, so one appearing means an assumption is wrong.
#
# THE ANSI TRAP, and it silently disarmed this whole gate on its first run:
# tracing's default formatter COLOURS the level, so the bytes on the line are
# ESC[33m WARN ESC[0m and ` ERROR ` is not a literal substring of anything. A
# grep for it matches nothing, in a log full of errors, and the suite goes
# green. Every count below therefore reads through `plain`.
plain() { LC_ALL=C sed $'s/\033\\[[0-9;]*m//g' "$1"; }
say "the facade logs"
for log in "$FACADE_LOG" "$CLUSTER_LOG" "$TIGHT_LOG" "$BROKER_LOG"; do
  [ -s "$log" ] || continue
  if plain "$log" | grep -qi 'panic'; then
    echo "PANIC in $log:" >&2
    plain "$log" | grep -i -m5 -A5 'panic' >&2
    RESULT=1
  fi
  errors=$(plain "$log" | grep -c ' ERROR ')
  warns=$(plain "$log" | grep -c ' WARN ')
  printf '  %-20s %3s ERROR  %3s WARN\n' "$(basename "$log")" "$errors" "$warns"
  if [ "$errors" != 0 ]; then
    plain "$log" | grep ' ERROR ' | sed 's/.* ERROR *//' | cut -c1-110 | sort | uniq -c | sort -rn | head -10 | sed 's/^/      /'
    RESULT=1
  fi
  if [ "$warns" != 0 ]; then
    plain "$log" | grep ' WARN ' | sed 's/.* WARN *//' | cut -c1-110 | sort | uniq -c | sort -rn | head -10 | sed 's/^/      /'
  fi
done

say "results"
printf '%s' "$SUMMARY"
say "result: $([ $RESULT -eq 0 ] && echo PASS || echo FAIL)"
exit $RESULT
