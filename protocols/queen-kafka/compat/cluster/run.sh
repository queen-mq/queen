#!/usr/bin/env bash
# queen-kafka compat: CLUSTER MODE acceptance
#
# Runs the cluster suite against a stack that is ALREADY UP. Nothing here starts
# or stops a Postgres, a broker or a facade -- that is rig-cluster.sh's job, or
# yours. Every address comes from the environment so this can be wired into
# compat/rig.sh without editing a line of Go.
#
# REQUIRED:
#   QUEEN_KAFKA_NODES        the clustered facades, "<id>@<host>:<port>" comma
#                            separated, e.g. "1@127.0.0.1:32410,2@127.0.0.1:32411"
#
# OPTIONAL -- each unset variable SKIPS the scenario that needs it, loudly:
#   QUEEN_KAFKA_SINGLE       one facade with the cluster config ABSENT
#                            (scenario 4, the single-node regression)
#   QUEEN_KAFKA_SPLIT        two INDEPENDENT single-node facades, comma
#                            separated (scenario 5, the old split-brain shape)
#   QUEEN_KAFKA_KILL_CMD     script taking a node id, SIGKILLs that facade
#   QUEEN_KAFKA_START_CMD    script taking a node id, starts it again
#                            (both together: scenario 3, node death)
#   QUEEN_KAFKA_STOP_CMD     script taking a node id, SIGTERMs that facade and
#                            waits for it to exit -- a deploy rather than a
#                            crash (with START_CMD: scenario 10, rolling
#                            restart)
#   QUEEN_KAFKA_LOGDIR       directory holding node-<id>.log etc; without it the
#                            facade-log WARN/ERROR scan cannot run
#
# TUNING, all with defaults that match the facade's own:
#   QUEEN_KAFKA_PARTITIONS   the facade's QUEEN_KAFKA_DEFAULT_PARTITIONS (8)
#   QUEEN_KAFKA_TTL_MS       its QUEEN_KAFKA_CLUSTER_TTL_MS (10000). Every
#                            takeover budget in the suite is derived from it.
#   QUEEN_KAFKA_JOIN_DELAY_MS  its QUEEN_KAFKA_GROUP_JOIN_DELAY_MS (3000)
#   RUN_ID                   suffix on every topic and group (default: epoch)
#
# Any extra arguments go straight to `go test`, so
#   ./run.sh -run TestAcceptance -v
# works.
set -euo pipefail

cd "$(dirname "$0")"

: "${QUEEN_KAFKA_NODES:?set QUEEN_KAFKA_NODES, e.g. 1@127.0.0.1:32410,2@127.0.0.1:32411,3@127.0.0.1:32412}"
export QUEEN_KAFKA_NODES
export QUEEN_KAFKA_PARTITIONS="${QUEEN_KAFKA_PARTITIONS:-8}"
export QUEEN_KAFKA_TTL_MS="${QUEEN_KAFKA_TTL_MS:-10000}"
export QUEEN_KAFKA_JOIN_DELAY_MS="${QUEEN_KAFKA_JOIN_DELAY_MS:-3000}"
export RUN_ID="${RUN_ID:-$(date +%s)}"

# GOWORK=off is mandatory: the repository's root go.work does not list this
# module, so a bare `go test` refuses to build it.
# -count=1 is mandatory: without it Go replays a cached PASS and the suite
# proves nothing about the stack that is running right now.
export GOWORK=off

echo "queen-kafka compat: CLUSTER MODE"
echo "  nodes      ${QUEEN_KAFKA_NODES}"
echo "  single     ${QUEEN_KAFKA_SINGLE:-(unset: the single-node regression will SKIP)}"
echo "  split      ${QUEEN_KAFKA_SPLIT:-(unset: the split-brain contrast will SKIP)}"
echo "  kill/start ${QUEEN_KAFKA_KILL_CMD:+set}${QUEEN_KAFKA_KILL_CMD:-(unset: node death will SKIP)}"
echo "  stop       ${QUEEN_KAFKA_STOP_CMD:+set}${QUEEN_KAFKA_STOP_CMD:-(unset: the rolling restart will SKIP)}"
echo "  logs       ${QUEEN_KAFKA_LOGDIR:-(unset: the facade log scan will SKIP)}"
echo "  partitions ${QUEEN_KAFKA_PARTITIONS}   ttl ${QUEEN_KAFKA_TTL_MS}ms   join delay ${QUEEN_KAFKA_JOIN_DELAY_MS}ms"
echo "  runID      ${RUN_ID}"
echo

# 900s: this suite forms roughly a dozen consumer groups, each paying the
# facade's join delay, and one scenario waits out a registry TTL twice.
exec go test -count=1 -timeout 900s -v "$@" .
