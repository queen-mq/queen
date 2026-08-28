#!/usr/bin/env bash
#
# The M6 differential rig: one queen-kafka facade and one real single-node KRaft
# Apache Kafka, side by side, so the same scenarios can be run against both and
# the answers diffed.
#
#   queen-kafka/compat/differential/rig-diff.sh up      # stand both stacks up
#   queen-kafka/compat/differential/rig-diff.sh run     # up (if needed) + runner
#   queen-kafka/compat/differential/rig-diff.sh down    # tear everything down
#
# Deliberately NOT rig.sh's ports and NOT rig.sh's container names: that rig may
# be running at the same time and is owned by someone else. Postgres 25543,
# broker 26699, facade 29192, Kafka 29092.
#
# Kafka is pinned to 3.9.1 on purpose. 4.0 (KIP-896) dropped the request
# versions older clients used, and the facade caps Fetch at v6 — against a 4.0
# broker half the scenarios would fail on the ORACLE's side and prove nothing.
#
# Kafka runs with auto.create.topics.enable=true and num.partitions=8, which is
# what the facade's QUEEN_KAFKA_DEFAULT_PARTITIONS is set to below: the two
# brokers then disagree about a topic's width only if one of them is wrong.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

PG_HOST_PORT="${PG_HOST_PORT:-25543}"
BROKER_PORT="${BROKER_PORT:-26699}"
FACADE_PORT="${FACADE_PORT:-29192}"
KAFKA_PORT="${KAFKA_PORT:-29092}"
PARTITIONS="${PARTITIONS:-8}"
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:3.9.1}"

PG_CONTAINER=qk-diff-pg
KAFKA_CONTAINER=qk-diff-kafka
STATE_DIR="${STATE_DIR:-/tmp/qk-diff}"
BROKER_LOG="$STATE_DIR/broker.log"
FACADE_LOG="$STATE_DIR/facade.log"
BROKER_PIDFILE="$STATE_DIR/broker.pid"
FACADE_PIDFILE="$STATE_DIR/facade.pid"

say() { printf '\n=== %s\n' "$*"; }

down() {
  say "tearing down"
  for f in "$FACADE_PIDFILE" "$BROKER_PIDFILE"; do
    [ -f "$f" ] || continue
    pid="$(cat "$f")"
    kill "$pid" 2>/dev/null
  done
  sleep 1
  for f in "$FACADE_PIDFILE" "$BROKER_PIDFILE"; do
    [ -f "$f" ] || continue
    pid="$(cat "$f")"
    kill -9 "$pid" 2>/dev/null
    rm -f "$f"
  done
  docker rm -f "$PG_CONTAINER" "$KAFKA_CONTAINER" >/dev/null 2>&1
  echo "down. logs (if any) under $STATE_DIR"
}

up() {
  mkdir -p "$STATE_DIR"

  say "postgres on 127.0.0.1:$PG_HOST_PORT (tmpfs, thrown away at exit)"
  docker rm -f "$PG_CONTAINER" >/dev/null 2>&1
  docker run -d --name "$PG_CONTAINER" \
    -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DB=postgres \
    -e PGDATA=/var/lib/postgresql/data/pgdata \
    -p "$PG_HOST_PORT":5432 \
    --tmpfs /var/lib/postgresql/data:rw,size=2g \
    postgres:16 -c max_connections=400 >/dev/null || return 1

  say "kafka ($KAFKA_IMAGE, KRaft single node) on 127.0.0.1:$KAFKA_PORT"
  docker rm -f "$KAFKA_CONTAINER" >/dev/null 2>&1
  docker run -d --name "$KAFKA_CONTAINER" \
    -p "$KAFKA_PORT":9092 \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://127.0.0.1:$KAFKA_PORT" \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
    -e KAFKA_NUM_PARTITIONS="$PARTITIONS" \
    -e KAFKA_DEFAULT_REPLICATION_FACTOR=1 \
    -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    -e KAFKA_LOG_DIRS=/tmp/kraft-combined-logs \
    "$KAFKA_IMAGE" >/dev/null || return 1

  for _ in $(seq 1 60); do
    docker exec "$PG_CONTAINER" pg_isready -U postgres >/dev/null 2>&1 && break
    sleep 1
  done
  docker exec "$PG_CONTAINER" pg_isready -U postgres >/dev/null 2>&1 || {
    echo "postgres never became ready" >&2; docker logs "$PG_CONTAINER" 2>&1 | tail -20; return 1; }

  say "building the broker and the facade (debug)"
  ( cd "$REPO_ROOT/server" && cargo build ) || return 1
  ( cd "$REPO_ROOT/queen-kafka" && cargo build ) || return 1

  say "broker on 127.0.0.1:$BROKER_PORT"
  PG_HOST=127.0.0.1 PG_PORT="$PG_HOST_PORT" PG_USER=postgres PG_PASSWORD=postgres \
  PG_DATABASE=postgres PORT="$BROKER_PORT" QUEEN_BIND_ADDR=127.0.0.1 \
  QUEEN_APPLY_SCHEMA=true DB_POOL_SIZE=32 LOG_LEVEL=info \
    "$REPO_ROOT/server/target/debug/queen" > "$BROKER_LOG" 2>&1 &
  echo $! > "$BROKER_PIDFILE"

  for _ in $(seq 1 90); do
    curl -fsS -m 2 "http://127.0.0.1:$BROKER_PORT/health" >/dev/null 2>&1 && break
    kill -0 "$(cat "$BROKER_PIDFILE")" 2>/dev/null || {
      echo "the broker died at boot:" >&2; tail -30 "$BROKER_LOG" >&2; return 1; }
    sleep 1
  done
  curl -fsS -m 2 "http://127.0.0.1:$BROKER_PORT/health" >/dev/null 2>&1 || {
    echo "the broker never answered /health" >&2; tail -30 "$BROKER_LOG" >&2; return 1; }

  say "queen-kafka on 127.0.0.1:$FACADE_PORT ($PARTITIONS partitions per topic)"
  QUEEN_URL="http://127.0.0.1:$BROKER_PORT" \
  QUEEN_KAFKA_ADDR="127.0.0.1:$FACADE_PORT" \
  QUEEN_KAFKA_ADVERTISED_ADDR="127.0.0.1:$FACADE_PORT" \
  QUEEN_KAFKA_DEFAULT_PARTITIONS="$PARTITIONS" \
  LOG_LEVEL="${FACADE_LOG_LEVEL:-debug}" \
    "$REPO_ROOT/queen-kafka/target/debug/queen-kafka" > "$FACADE_LOG" 2>&1 &
  echo $! > "$FACADE_PIDFILE"

  for _ in $(seq 1 100); do
    nc -z 127.0.0.1 "$FACADE_PORT" >/dev/null 2>&1 && break
    kill -0 "$(cat "$FACADE_PIDFILE")" 2>/dev/null || {
      echo "the facade died at boot:" >&2; tail -30 "$FACADE_LOG" >&2; return 1; }
    sleep 0.2
  done
  nc -z 127.0.0.1 "$FACADE_PORT" >/dev/null 2>&1 || {
    echo "the facade never listened on $FACADE_PORT" >&2; tail -30 "$FACADE_LOG" >&2; return 1; }

  say "waiting for kafka to answer ApiVersions on $KAFKA_PORT"
  for _ in $(seq 1 120); do
    nc -z 127.0.0.1 "$KAFKA_PORT" >/dev/null 2>&1 && break
    docker inspect -f '{{.State.Running}}' "$KAFKA_CONTAINER" 2>/dev/null | grep -q true || {
      echo "the kafka container died at boot:" >&2; docker logs "$KAFKA_CONTAINER" 2>&1 | tail -30 >&2; return 1; }
    sleep 1
  done
  nc -z 127.0.0.1 "$KAFKA_PORT" >/dev/null 2>&1 || {
    echo "kafka never listened on $KAFKA_PORT" >&2; docker logs "$KAFKA_CONTAINER" 2>&1 | tail -30 >&2; return 1; }

  say "up. facade 127.0.0.1:$FACADE_PORT, kafka 127.0.0.1:$KAFKA_PORT"
}

run() {
  cd "$SCRIPT_DIR" || return 1
  GOWORK=off \
  QK_FACADE="127.0.0.1:$FACADE_PORT" \
  QK_KAFKA="127.0.0.1:$KAFKA_PORT" \
    go run . "$@"
}

case "${1:-run}" in
  up) up;;
  down) down;;
  run) shift; up && run "$@";;
  runonly) shift; run "$@";;
  *) echo "usage: $0 {up|run|runonly|down}" >&2; exit 2;;
esac
