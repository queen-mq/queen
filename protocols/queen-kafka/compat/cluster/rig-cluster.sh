#!/usr/bin/env bash
#
# The queen-kafka CLUSTER acceptance rig.
#
# It stands up the whole shape cluster mode exists for and then runs `run.sh`
# against it:
#
#   one throwaway Postgres                              (container qkx-c2-pg)
#   TWO Queen brokers, mesh-wired, on that ONE Postgres (the HA recipe of
#                                                        test/compose/docker-compose.ha.yml)
#   THREE queen-kafka facades in cluster mode           nodes 1, 2, 3
#       node 1 -> broker A     node 2 -> broker B     node 3 -> broker A
#   ONE queen-kafka facade with the cluster config ABSENT   (the regression lane)
#   TWO independent single-node facades                     (the old split-brain shape)
#
# Node 1 and node 2 are in front of DIFFERENT brokers on purpose: it puts a
# cross-broker read on the critical path of every group assertion, which is what
# proves the design's premise that the data path is stateless over the shared
# Postgres (032_log_fetch.sql:11-19 takes no lease and writes nothing;
# 003_log_push.sql:131-213 allocates the offset under a row lock in the database
# rather than in a broker).
#
#   protocols/queen-kafka/compat/cluster/rig-cluster.sh            # stand up, run, tear down
#   protocols/queen-kafka/compat/cluster/rig-cluster.sh -run TestAcceptance -v
#   protocols/queen-kafka/compat/cluster/rig-cluster.sh --keep     # leave the stack up
#
# Every argument that is not --keep is passed through to `go test`.
#
# PORTS. This rig owns 32400-32419 and binds nothing else. Postgres 32400,
# brokers 32401/32402 (mesh 32403/32404), facades 32410-32415. Every one is
# overridable by environment variable; if you move them, move them as a block.
#
# TEARDOWN. Every host process's pid is written to $LOGDIR/pids/<name>.pid at
# spawn and teardown kills ONLY those pids. Nothing is ever resolved from a
# port. The container is removed by its own name and no other.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

PG_PORT="${PG_PORT:-32400}"
BROKER_A_PORT="${BROKER_A_PORT:-32401}"
BROKER_B_PORT="${BROKER_B_PORT:-32402}"
MESH_A_PORT="${MESH_A_PORT:-32403}"
MESH_B_PORT="${MESH_B_PORT:-32404}"
NODE1_PORT="${NODE1_PORT:-32410}"
NODE2_PORT="${NODE2_PORT:-32411}"
NODE3_PORT="${NODE3_PORT:-32412}"
SINGLE_PORT="${SINGLE_PORT:-32413}"
SPLIT_A_PORT="${SPLIT_A_PORT:-32414}"
SPLIT_B_PORT="${SPLIT_B_PORT:-32415}"
CONTAINER="${CONTAINER:-qkx-c2-pg}"
PARTITIONS="${PARTITIONS:-8}"
CLUSTER_NAME="${CLUSTER_NAME:-qkxc2}"

# The registry cadence. The product defaults are 2000/10000 and they are what
# main.rs validates; this rig runs the FASTEST pair that validation allows
# (TTL >= 3 x HEARTBEAT, TTL >= 3000) because the node-death scenario waits out
# a whole TTL and a 10 s one would add half a minute to the suite without
# testing anything the 3 s one does not. The suite is told the value and sizes
# its own budgets from it, so raising it here needs no edit in Go.
HEARTBEAT_MS="${HEARTBEAT_MS:-1000}"
TTL_MS="${TTL_MS:-3000}"
# Kafka's group.initial.rebalance.delay.ms, and the facade's default. Left at
# the default: a group formation is part of what is being measured.
JOIN_DELAY_MS="${JOIN_DELAY_MS:-3000}"

# All facades of one cluster must present credentials of ONE Queen tenant --
# queen.kv is keyed by tenant, so two tenants would be two registries and each
# facade would see only itself (cluster/registry.rs, the "alone" warning). The
# rig's brokers run with JWT off, so this string authenticates nothing; it is
# generated per run rather than hardcoded so that nothing outside this rig can
# come to depend on its value.
QUEEN_TOKEN_VALUE="${QUEEN_TOKEN_VALUE:-qkx-c2-$$-$(date +%s)}"
MESH_SECRET="qkx-c2-mesh-$$"

KEEP=0
GO_TEST_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --keep) KEEP=1;;
    -h|--help) sed -n '2,36p' "$0"; exit 0;;
    *) GO_TEST_ARGS+=("$arg");;
  esac
done

LOGDIR="${LOGDIR:-$(mktemp -d -t queen-kafka-cluster.XXXXXX)}"
mkdir -p "$LOGDIR/pids"

say() { printf '\n=== %s\n' "$*"; }

# Every pid this script spawns, in the order they must die.
PID_NAMES=(split-b split-a single node-3 node-2 node-1 broker-b broker-a)

pid_of() { [ -f "$LOGDIR/pids/$1.pid" ] && cat "$LOGDIR/pids/$1.pid"; }

cleanup() {
  local code=$?
  if [ "$KEEP" = 1 ]; then
    echo
    echo "--keep: the stack is still up."
    echo "  postgres  : container $CONTAINER on 127.0.0.1:$PG_PORT"
    echo "  broker A  : http://127.0.0.1:$BROKER_A_PORT   broker B : http://127.0.0.1:$BROKER_B_PORT"
    echo "  cluster   : 1@127.0.0.1:$NODE1_PORT 2@127.0.0.1:$NODE2_PORT 3@127.0.0.1:$NODE3_PORT"
    echo "  single    : 127.0.0.1:$SINGLE_PORT"
    echo "  split     : 127.0.0.1:$SPLIT_A_PORT 127.0.0.1:$SPLIT_B_PORT"
    echo "  logs      : $LOGDIR"
    echo "  tear down : for f in $LOGDIR/pids/*.pid; do kill -9 \$(cat \$f); done; docker rm -f $CONTAINER"
    exit $code
  fi
  say "tearing down (only the pids recorded at spawn, only the container named $CONTAINER)"
  local name pid
  for name in "${PID_NAMES[@]}"; do
    pid="$(pid_of "$name")"
    [ -n "$pid" ] && kill "$pid" 2>/dev/null
  done
  sleep 1
  for name in "${PID_NAMES[@]}"; do
    pid="$(pid_of "$name")"
    [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null
  done
  docker rm -f "$CONTAINER" >/dev/null 2>&1
  echo "logs kept at $LOGDIR"
  exit $code
}
trap cleanup EXIT INT TERM

command -v docker >/dev/null || { echo "docker not found" >&2; exit 2; }
command -v go >/dev/null || { echo "go not found" >&2; exit 2; }
command -v cargo >/dev/null || { echo "cargo not found" >&2; exit 2; }
command -v nc >/dev/null || { echo "nc not found" >&2; exit 2; }

# --------------------------------------------------------------------- postgres
say "postgres on 127.0.0.1:$PG_PORT (container $CONTAINER, tmpfs, thrown away at exit)"
docker rm -f "$CONTAINER" >/dev/null 2>&1
docker run -d --name "$CONTAINER" \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DB=postgres \
  -e PGDATA=/var/lib/postgresql/data/pgdata \
  -p "$PG_PORT":5432 \
  --tmpfs /var/lib/postgresql/data:rw,size=2g \
  postgres:16 -c max_connections=400 >/dev/null || exit 1

for _ in $(seq 1 60); do
  docker exec "$CONTAINER" pg_isready -U postgres >/dev/null 2>&1 && break
  sleep 1
done
docker exec "$CONTAINER" pg_isready -U postgres >/dev/null 2>&1 || {
  echo "postgres never became ready" >&2; docker logs "$CONTAINER" | tail -20; exit 1; }

# ----------------------------------------------------------------------- builds
say "building the broker and the facade (debug, each from its own manifest)"
( cd "$REPO_ROOT/server" && cargo build ) || exit 1
( cd "$REPO_ROOT/protocols/queen-kafka" && cargo build ) || exit 1

# ---------------------------------------------------------------------- brokers
# The HA recipe: ONE Postgres, two brokers with distinct QUEEN_SERVER_IDs, a
# byte-identical QUEEN_SYNC_SECRET and each other as QUEEN_MESH_PEERS. The mesh
# carries best-effort wake hints only -- everything durable is in the shared
# database, which is the whole reason two facades in front of two brokers can
# coordinate one group.
start_broker() {
  local name=$1 port=$2 mesh=$3 peer=$4
  mkdir -p "$LOGDIR/buffers-$name"
  PG_HOST=127.0.0.1 PG_PORT="$PG_PORT" PG_USER=postgres PG_PASSWORD=postgres \
  PG_DATABASE=postgres PORT="$port" QUEEN_BIND_ADDR=127.0.0.1 \
  QUEEN_APPLY_SCHEMA=true DB_POOL_SIZE=32 LOG_LEVEL=info \
  QUEEN_SERVER_ID="$name" QUEEN_MESH_PORT="$mesh" QUEEN_MESH_PEERS="127.0.0.1:$peer" \
  QUEEN_SYNC_SECRET="$MESH_SECRET" \
  FILE_BUFFER_DIR="$LOGDIR/buffers-$name" \
    "$REPO_ROOT/server/target/debug/queen" > "$LOGDIR/$name.log" 2>&1 &
  echo $! > "$LOGDIR/pids/$name.pid"
}

wait_http() {
  local url=$1 pidfile=$2 label=$3
  for _ in $(seq 1 120); do
    curl -fsS -m 2 "$url" >/dev/null 2>&1 && return 0
    kill -0 "$(cat "$pidfile")" 2>/dev/null || { echo "$label died at boot" >&2; return 1; }
    sleep 1
  done
  echo "$label never answered $url" >&2
  return 1
}

say "broker A on 127.0.0.1:$BROKER_A_PORT and broker B on 127.0.0.1:$BROKER_B_PORT (one Postgres, meshed)"
# Broker A alone until it is healthy: both run QUEEN_APPLY_SCHEMA, and two
# schema applies racing on a cold database is a recorded boot deadlock
# (queen-schema-apply-per-statement-fix) rather than anything this rig is for.
start_broker broker-a "$BROKER_A_PORT" "$MESH_A_PORT" "$MESH_B_PORT"
wait_http "http://127.0.0.1:$BROKER_A_PORT/health" "$LOGDIR/pids/broker-a.pid" "broker A" || {
  tail -30 "$LOGDIR/broker-a.log" >&2; exit 1; }
start_broker broker-b "$BROKER_B_PORT" "$MESH_B_PORT" "$MESH_A_PORT"
wait_http "http://127.0.0.1:$BROKER_B_PORT/health" "$LOGDIR/pids/broker-b.pid" "broker B" || {
  tail -30 "$LOGDIR/broker-b.log" >&2; exit 1; }

# ---------------------------------------------------------------------- facades
# One start script per facade, written out rather than inlined, for the same
# reason compat/rig.sh writes one: the node-death scenario RESTARTS a facade,
# and "the same facade, restarted" has to be the same command line by
# construction rather than by two copies of it staying in step. Each appends to
# its own log, so a restart leaves the whole run readable in one file.
#
# NO_COLOR=1 because the suite greps these logs for WARN and ERROR, and
# tracing's ANSI escapes would otherwise sit between the level and the message.
write_facade_script() {
  local name=$1 port=$2 broker_port=$3 node_id=$4   # node_id empty = single mode
  local cluster_env=""
  if [ -n "$node_id" ]; then
    cluster_env="QUEEN_KAFKA_NODE_ID=$node_id QUEEN_KAFKA_CLUSTER=$CLUSTER_NAME"
    cluster_env="$cluster_env QUEEN_KAFKA_CLUSTER_HEARTBEAT_MS=$HEARTBEAT_MS"
    cluster_env="$cluster_env QUEEN_KAFKA_CLUSTER_TTL_MS=$TTL_MS"
  fi
  cat > "$LOGDIR/start-$name.sh" <<SCRIPT
#!/usr/bin/env bash
set -uo pipefail
env QUEEN_URL="http://127.0.0.1:$broker_port" \\
    QUEEN_TOKEN="$QUEEN_TOKEN_VALUE" \\
    QUEEN_KAFKA_ADDR="127.0.0.1:$port" \\
    QUEEN_KAFKA_ADVERTISED_ADDR="127.0.0.1:$port" \\
    QUEEN_KAFKA_DEFAULT_PARTITIONS="$PARTITIONS" \\
    QUEEN_KAFKA_GROUP_JOIN_DELAY_MS="$JOIN_DELAY_MS" \\
    NO_COLOR=1 LOG_LEVEL="${FACADE_LOG_LEVEL:-info}" \\
    $cluster_env \\
    "$REPO_ROOT/protocols/queen-kafka/target/debug/queen-kafka" >> "$LOGDIR/$name.log" 2>&1 &
echo \$! > "$LOGDIR/pids/$name.pid"
for _ in \$(seq 1 150); do
  nc -z 127.0.0.1 "$port" >/dev/null 2>&1 && { echo "$name up, pid \$(cat "$LOGDIR/pids/$name.pid")"; exit 0; }
  kill -0 "\$(cat "$LOGDIR/pids/$name.pid")" 2>/dev/null || { echo "$name died at boot" >&2; exit 1; }
  sleep 0.2
done
echo "$name never listened on $port" >&2
exit 1
SCRIPT
  chmod +x "$LOGDIR/start-$name.sh"
}

write_facade_script node-1 "$NODE1_PORT" "$BROKER_A_PORT" 1
write_facade_script node-2 "$NODE2_PORT" "$BROKER_B_PORT" 2
write_facade_script node-3 "$NODE3_PORT" "$BROKER_A_PORT" 3
write_facade_script single "$SINGLE_PORT" "$BROKER_A_PORT" ""
write_facade_script split-a "$SPLIT_A_PORT" "$BROKER_A_PORT" ""
write_facade_script split-b "$SPLIT_B_PORT" "$BROKER_A_PORT" ""

# The two commands the suite drives node death with. They take a NODE ID and
# resolve it to the pid this rig recorded at spawn -- never to a port. A kill
# that cannot find its pidfile fails loudly instead of guessing.
cat > "$LOGDIR/kill-node.sh" <<SCRIPT
#!/usr/bin/env bash
# SIGKILL facade node \$1 by the pid recorded at spawn. A crash, not a shutdown:
# nothing gets to close a connection politely or write a LeaveGroup.
set -uo pipefail
f="$LOGDIR/pids/node-\$1.pid"
[ -f "\$f" ] || { echo "no pidfile for node \$1 at \$f" >&2; exit 1; }
pid=\$(cat "\$f")
kill -9 "\$pid" 2>/dev/null
for _ in \$(seq 1 50); do
  kill -0 "\$pid" 2>/dev/null || { echo "node \$1 (pid \$pid) is dead"; exit 0; }
  sleep 0.1
done
echo "node \$1 (pid \$pid) would not die" >&2
exit 1
SCRIPT
chmod +x "$LOGDIR/kill-node.sh"

cat > "$LOGDIR/start-node.sh" <<SCRIPT
#!/usr/bin/env bash
set -uo pipefail
exec "$LOGDIR/start-node-\$1.sh"
SCRIPT
chmod +x "$LOGDIR/start-node.sh"

# The DEPLOY half of the same pair: SIGTERM, which is what `kubectl delete pod`,
# `systemctl stop` and the broker's own facade supervisor send. A facade that is
# asked to stop hands its registry row back before it exits, so this is the
# command the rolling-restart scenario drives -- kill-node.sh is a crash and
# proves the other half. Same discipline: the pid comes from the file written at
# spawn and never from a port.
cat > "$LOGDIR/stop-node.sh" <<SCRIPT
#!/usr/bin/env bash
set -uo pipefail
f="$LOGDIR/pids/node-\$1.pid"
[ -f "\$f" ] || { echo "no pidfile for node \$1 at \$f" >&2; exit 1; }
pid=\$(cat "\$f")
kill -TERM "\$pid" 2>/dev/null
# 10s: the facade's own deregistration budget is 2s and it runs one KV call.
for _ in \$(seq 1 100); do
  kill -0 "\$pid" 2>/dev/null || { echo "node \$1 (pid \$pid) stopped on SIGTERM"; exit 0; }
  sleep 0.1
done
echo "node \$1 (pid \$pid) did not exit within 10s of SIGTERM" >&2
exit 1
SCRIPT
chmod +x "$LOGDIR/stop-node.sh"

say "three clustered facades: 1@127.0.0.1:$NODE1_PORT (broker A), 2@127.0.0.1:$NODE2_PORT (broker B), 3@127.0.0.1:$NODE3_PORT (broker A)"
for name in node-1 node-2 node-3; do
  "$LOGDIR/start-$name.sh" || { echo "$name did not start:" >&2; tail -30 "$LOGDIR/$name.log" >&2; exit 1; }
done

say "the regression lane: one facade with the cluster config ABSENT on 127.0.0.1:$SINGLE_PORT"
"$LOGDIR/start-single.sh" || { tail -30 "$LOGDIR/single.log" >&2; exit 1; }

say "the old split-brain shape: two INDEPENDENT single-node facades on 127.0.0.1:$SPLIT_A_PORT and 127.0.0.1:$SPLIT_B_PORT"
for name in split-a split-b; do
  "$LOGDIR/start-$name.sh" || { tail -30 "$LOGDIR/$name.log" >&2; exit 1; }
done

# ------------------------------------------------------------------------ suite
# The suite waits for the three-node view itself (TestMain) rather than the rig
# polling here: it needs the same wait after a restart in the node-death
# scenario, and one implementation of "the cluster has converged" cannot
# disagree with itself.
say "cluster acceptance suite"
QUEEN_KAFKA_NODES="1@127.0.0.1:$NODE1_PORT,2@127.0.0.1:$NODE2_PORT,3@127.0.0.1:$NODE3_PORT" \
QUEEN_KAFKA_SINGLE="127.0.0.1:$SINGLE_PORT" \
QUEEN_KAFKA_SPLIT="127.0.0.1:$SPLIT_A_PORT,127.0.0.1:$SPLIT_B_PORT" \
QUEEN_KAFKA_PARTITIONS="$PARTITIONS" \
QUEEN_KAFKA_TTL_MS="$TTL_MS" \
QUEEN_KAFKA_JOIN_DELAY_MS="$JOIN_DELAY_MS" \
QUEEN_KAFKA_KILL_CMD="$LOGDIR/kill-node.sh" \
QUEEN_KAFKA_STOP_CMD="$LOGDIR/stop-node.sh" \
QUEEN_KAFKA_START_CMD="$LOGDIR/start-node.sh" \
QUEEN_KAFKA_LOGDIR="$LOGDIR" \
QUEEN_URL="http://127.0.0.1:$BROKER_A_PORT" \
QUEEN_URL_B="http://127.0.0.1:$BROKER_B_PORT" \
  "$SCRIPT_DIR/run.sh" "${GO_TEST_ARGS[@]+"${GO_TEST_ARGS[@]}"}"
RESULT=$?

# A panic on either side is a failure even when every assertion passed: the
# facade is meant to survive whatever a client sends it.
for log in "$LOGDIR"/*.log; do
  [ -s "$log" ] || continue
  if grep -qi 'panic' "$log"; then
    echo "PANIC in $log:" >&2
    grep -i -m5 -A5 'panic' "$log" >&2
    RESULT=1
  fi
done

say "result: $([ $RESULT -eq 0 ] && echo PASS || echo FAIL)"
exit $RESULT
