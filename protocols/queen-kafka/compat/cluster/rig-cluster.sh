#!/usr/bin/env bash
#
# The queen-kafka CLUSTER acceptance rig.
#
# It stands up the whole shape cluster mode exists for and then runs `run.sh`
# against it:
#
#   ONE Queen cluster of THREE raft brokers A, B, C     (each on its own
#                                                        throwaway data directory)
#   THREE queen-kafka facades in cluster mode           nodes 1, 2, 3
#       node 1 -> broker A     node 2 -> broker B     node 3 -> broker C
#   ONE queen-kafka facade with the cluster config ABSENT   (the regression lane)
#   TWO independent single-node facades                     (the old split-brain shape)
#
# The three clustered facades are in front of DIFFERENT brokers on purpose:
# whichever broker leads the raft cluster, two of them reach it through a
# follower, which puts a cross-node round trip on the critical path of every
# group assertion. That is what proves the design's premise that the facades'
# data path is stateless over one Queen deployment: every broker answers for
# every partition, and a write is ordered by the raft leader, not by the facade
# or the broker it happened to arrive at.
#
#   protocols/queen-kafka/compat/cluster/rig-cluster.sh            # stand up, run, tear down
#   protocols/queen-kafka/compat/cluster/rig-cluster.sh -run TestAcceptance -v
#   protocols/queen-kafka/compat/cluster/rig-cluster.sh --keep     # leave the stack up
#
# Every argument that is not --keep is passed through to `go test`.
#
# PORTS. This rig owns 32400-32419 and binds nothing else. Brokers 32401-32403
# (their raft RPC on 32404-32406, bound to 127.0.0.1), facades 32410-32415.
# Every one is overridable by environment variable; if you move them, move them
# as a block.
#
# DISK. The brokers' disk gate refuses writes (507) once a data directory's
# filesystem is QUEEN_RAFT_DISK_HIGH_PCT used, 85 by default; a throwaway rig on
# a developer disk is not what it protects, so the rig runs it at 99.5 unless
# QUEEN_RAFT_DISK_HIGH_PCT says otherwise.
#
# TEARDOWN. Every host process's pid is written to $LOGDIR/pids/<name>.pid at
# spawn and teardown kills ONLY those pids. Nothing is ever resolved from a
# port. The brokers' data directories ($LOGDIR/raft-*) are removed; the logs
# are kept.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

BROKER_A_PORT="${BROKER_A_PORT:-32401}"
BROKER_B_PORT="${BROKER_B_PORT:-32402}"
BROKER_C_PORT="${BROKER_C_PORT:-32403}"
RAFT_A_PORT="${RAFT_A_PORT:-32404}"
RAFT_B_PORT="${RAFT_B_PORT:-32405}"
RAFT_C_PORT="${RAFT_C_PORT:-32406}"
NODE1_PORT="${NODE1_PORT:-32410}"
NODE2_PORT="${NODE2_PORT:-32411}"
NODE3_PORT="${NODE3_PORT:-32412}"
SINGLE_PORT="${SINGLE_PORT:-32413}"
SPLIT_A_PORT="${SPLIT_A_PORT:-32414}"
SPLIT_B_PORT="${SPLIT_B_PORT:-32415}"
PARTITIONS="${PARTITIONS:-8}"
DISK_HIGH_PCT="${QUEEN_RAFT_DISK_HIGH_PCT:-99.5}"
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
# Queen's KV is keyed by tenant, so two tenants would be two registries and each
# facade would see only itself (cluster/registry.rs, the "alone" warning). The
# rig's brokers run with JWT off, so this string authenticates nothing; it is
# generated per run rather than hardcoded so that nothing outside this rig can
# come to depend on its value.
QUEEN_TOKEN_VALUE="${QUEEN_TOKEN_VALUE:-qkx-c2-$$-$(date +%s)}"
# Every raft RPC between the brokers carries this; generated per run for the
# same reason.
RAFT_TOKEN="qkx-c2-raft-$$-$(date +%s)"

KEEP=0
GO_TEST_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --keep) KEEP=1;;
    -h|--help) sed -n '2,42p' "$0"; exit 0;;
    *) GO_TEST_ARGS+=("$arg");;
  esac
done

LOGDIR="${LOGDIR:-$(mktemp -d -t queen-kafka-cluster.XXXXXX)}"
mkdir -p "$LOGDIR/pids"

say() { printf '\n=== %s\n' "$*"; }

# Every pid this script spawns, in the order they must die.
PID_NAMES=(split-b split-a single node-3 node-2 node-1 broker-c broker-b broker-a)

pid_of() { [ -f "$LOGDIR/pids/$1.pid" ] && cat "$LOGDIR/pids/$1.pid"; }

cleanup() {
  local code=$?
  if [ "$KEEP" = 1 ]; then
    echo
    echo "--keep: the stack is still up."
    echo "  brokers   : A http://127.0.0.1:$BROKER_A_PORT  B http://127.0.0.1:$BROKER_B_PORT  C http://127.0.0.1:$BROKER_C_PORT"
    echo "  cluster   : 1@127.0.0.1:$NODE1_PORT 2@127.0.0.1:$NODE2_PORT 3@127.0.0.1:$NODE3_PORT"
    echo "  single    : 127.0.0.1:$SINGLE_PORT"
    echo "  split     : 127.0.0.1:$SPLIT_A_PORT 127.0.0.1:$SPLIT_B_PORT"
    echo "  logs      : $LOGDIR"
    echo "  tear down : for f in $LOGDIR/pids/*.pid; do kill -9 \$(cat \$f); done; rm -rf $LOGDIR/raft-*"
    exit $code
  fi
  say "tearing down (only the pids recorded at spawn, only this rig's data directories)"
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
  rm -rf "$LOGDIR"/raft-broker-a "$LOGDIR"/raft-broker-b "$LOGDIR"/raft-broker-c
  echo "logs kept at $LOGDIR"
  exit $code
}
trap cleanup EXIT INT TERM

command -v go >/dev/null || { echo "go not found" >&2; exit 2; }
command -v cargo >/dev/null || { echo "cargo not found" >&2; exit 2; }
command -v nc >/dev/null || { echo "nc not found" >&2; exit 2; }

# ----------------------------------------------------------------------- builds
say "building the broker and the facade (debug, each from its own manifest)"
( cd "$REPO_ROOT/server" && cargo build ) || exit 1
( cd "$REPO_ROOT/protocols/queen-kafka" && cargo build ) || exit 1

# ---------------------------------------------------------------------- brokers
# One raft cluster of three voters, the shape of helm_v2/broker: every node
# starts with the SAME QUEEN_RAFT_PEERS (`id=raft_addr/http_addr`) on an empty
# data directory, they elect a leader, and a follower forwards what it cannot
# answer to the leader's HTTP address. Everything durable is in the replicated
# log, which is the whole reason three facades in front of three brokers can
# coordinate one group.
PEERS="1=127.0.0.1:$RAFT_A_PORT/127.0.0.1:$BROKER_A_PORT"
PEERS="$PEERS,2=127.0.0.1:$RAFT_B_PORT/127.0.0.1:$BROKER_B_PORT"
PEERS="$PEERS,3=127.0.0.1:$RAFT_C_PORT/127.0.0.1:$BROKER_C_PORT"

start_broker() {
  local name=$1 id=$2 port=$3 raft=$4
  mkdir -p "$LOGDIR/raft-$name"
  QUEEN_RAFT_DIR="$LOGDIR/raft-$name" QUEEN_RAFT_DISK_HIGH_PCT="$DISK_HIGH_PCT" \
  QUEEN_RAFT_REPLICATOR=openraft QUEEN_RAFT_NODE_ID="$id" QUEEN_RAFT_PEERS="$PEERS" \
  QUEEN_RAFT_LISTEN="127.0.0.1:$raft" QUEEN_RAFT_TOKEN="$RAFT_TOKEN" \
  QUEEN_SERVER_ID="$name" PORT="$port" QUEEN_BIND_ADDR=127.0.0.1 LOG_LEVEL=info \
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

say "one raft cluster: broker A on 127.0.0.1:$BROKER_A_PORT, B on 127.0.0.1:$BROKER_B_PORT, C on 127.0.0.1:$BROKER_C_PORT"
# All three before waiting on any: a broker answers /health 200 only once a
# leader is known, and no leader is elected without a majority up.
start_broker broker-a 1 "$BROKER_A_PORT" "$RAFT_A_PORT"
start_broker broker-b 2 "$BROKER_B_PORT" "$RAFT_B_PORT"
start_broker broker-c 3 "$BROKER_C_PORT" "$RAFT_C_PORT"
for b in a b c; do
  port_var="BROKER_$(printf '%s' "$b" | tr a-z A-Z)_PORT"
  wait_http "http://127.0.0.1:${!port_var}/health" "$LOGDIR/pids/broker-$b.pid" "broker $b" || {
    tail -30 "$LOGDIR/broker-$b.log" >&2; exit 1; }
done

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
write_facade_script node-3 "$NODE3_PORT" "$BROKER_C_PORT" 3
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

# The DEPLOY half of the same pair: SIGTERM, which is what `kubectl delete pod`
# and `systemctl stop` send. A facade that is
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

say "three clustered facades: 1@127.0.0.1:$NODE1_PORT (broker A), 2@127.0.0.1:$NODE2_PORT (broker B), 3@127.0.0.1:$NODE3_PORT (broker C)"
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
QUEEN_URL_C="http://127.0.0.1:$BROKER_C_PORT" \
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
