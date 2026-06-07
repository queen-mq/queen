#!/bin/bash
# RabbitMQ comparison test — single-node, 1000 classic-mirrored queues with
# persistent messages, simultaneous producer + consumer using
# pivotalrabbitmq/perf-test as the harness.

set -u

TEST_NAME="${1:-rabbitmq-1000q}"
DURATION="${2:-900}"
QUEUE_COUNT="${3:-1000}"

OUTDIR="/root/bench-runs/results/${TEST_NAME}"
mkdir -p "$OUTDIR"

ts() { date -u +%FT%TZ; }
log() { echo "[$(ts)] [$TEST_NAME] $*"; }

log "=== START RabbitMQ test=$TEST_NAME queues=$QUEUE_COUNT dur=${DURATION}s ==="

log "cleanup: stopping containers"
docker stop rabbitmq rabbit-perf 2>/dev/null || true
docker rm -v rabbitmq rabbit-perf 2>/dev/null || true
# Hard-clean any leftover RabbitMQ volumes
docker volume ls -qf dangling=true | xargs -r docker volume rm 2>/dev/null || true
docker network create queen >/dev/null 2>&1 || true

# Start RabbitMQ — bypass docker-entrypoint.sh which causes the cookie eacces bug
# on this kernel. Run rabbitmq-server directly. Default guest/guest works at
# localhost; we'll add an external user via rabbitmqctl after startup.
log "starting rabbitmq (bypassing docker-entrypoint)"
docker run -d --name rabbitmq --hostname rmqhost --network queen \
  --ulimit nofile=65535:65535 \
  -p 5672:5672 -p 15672:15672 \
  -e RABBITMQ_NODENAME=rabbit@localhost \
  --entrypoint rabbitmq-server \
  rabbitmq:3.12-management >/dev/null

log "waiting for rabbitmq"
RABBITMQ_READY=0
for i in $(seq 1 120); do
  if docker exec rabbitmq rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
    log "  rabbitmq ready (after ${i}s)"
    RABBITMQ_READY=1
    break
  fi
  sleep 1
done
if [ "$RABBITMQ_READY" -ne 1 ]; then
  log "FATAL: rabbitmq did not become ready in 120s"
  docker logs --tail 30 rabbitmq > "$OUTDIR/rabbitmq-startup-failure.log" 2>&1
  exit 1
fi

# Make sure 'guest' user works from external connections (default is localhost-only)
docker exec rabbitmq rabbitmqctl -n rabbit@localhost set_permissions -p / guest '.*' '.*' '.*' 2>&1 | tail -2 || true
docker exec rabbitmq rabbitmqctl -n rabbit@localhost authenticate_user guest guest 2>&1 | tail -2 || true
# Allow guest from non-localhost (default loopback-only restriction)
docker exec rabbitmq sh -c 'echo "loopback_users.guest = false" > /etc/rabbitmq/conf.d/10-allow-guest.conf' 2>&1 | tail -1 || true

sleep 5
log "settle done"

START_TIME=$(date -u +%FT%T.000Z)
echo "$START_TIME" > "$OUTDIR/start_time.txt"

# Run perf-test in producer+consumer mode with 1000 queues
# --queue-pattern + --queue-pattern-from/to creates and uses N queues
# --producers and --consumers tell it how many of each to run
# --rate -1 = max possible (default)
# --size 28 = 28-byte body (matching Queen)
# --confirm 50 = publisher confirms with 50 outstanding (durable acks)
# --flag persistent = delivery_mode=2
# --autoack false = consumer acks each message
# --queue-args x-queue-type=classic
log "launching perf-test"
docker run -d --name rabbit-perf --network queen \
  --ulimit nofile=65535:65535 \
  pivotalrabbitmq/perf-test:latest \
  --uri amqp://guest:guest@rabbitmq:5672 \
  --queue-pattern 'bench-q-%d' \
  --queue-pattern-from 1 \
  --queue-pattern-to "$QUEUE_COUNT" \
  --producers 1 \
  --consumers 1 \
  --rate -1 \
  --size 28 \
  --confirm 200 \
  --flag persistent \
  --queue-args x-queue-type=classic \
  --multi-ack-every 100 \
  --qos 100 \
  --time "$DURATION" >/dev/null

log "perf-test running, monitoring for ${DURATION}s"
INTERVAL=30
LOOPS=$((DURATION / INTERVAL + 1))
for i in $(seq 1 $LOOPS); do
  sleep $INTERVAL
  R_S=$(docker stats --no-stream --format "{{.CPUPerc}}/{{.MemUsage}}" rabbitmq 2>/dev/null | head -1)
  P_S=$(docker stats --no-stream --format "{{.CPUPerc}}/{{.MemUsage}}" rabbit-perf 2>/dev/null | head -1)
  log "  +$((i * INTERVAL))s rabbitmq=${R_S} perf=${P_S}"
  echo "[$(ts)] rabbitmq=${R_S} perf=${P_S}" >> "$OUTDIR/docker-stats.log"
done

log "duration elapsed, collecting metrics"
sleep 5

END_TIME=$(date -u +%FT%T.000Z)
echo "$END_TIME" > "$OUTDIR/end_time.txt"

docker logs rabbit-perf > "$OUTDIR/perf-test.log" 2>&1
docker logs rabbitmq > "$OUTDIR/rabbitmq.log" 2>&1
docker stats --no-stream rabbitmq rabbit-perf > "$OUTDIR/docker-stats-final.txt" 2>&1

# Get queue stats via management API
docker exec rabbitmq rabbitmqctl list_queues name messages messages_ready messages_unacknowledged --silent 2>/dev/null | head -20 > "$OUTDIR/queue-stats-sample.txt"
docker exec rabbitmq rabbitmqctl list_queues name messages 2>/dev/null | tail -n +1 | awk '{sum+=$2}END{print "total messages across all queues:", sum}' > "$OUTDIR/queue-totals.txt"

# Disk usage
docker exec rabbitmq du -sh /var/lib/rabbitmq/mnesia/ 2>/dev/null > "$OUTDIR/rabbitmq-data-size.txt"

cat > "$OUTDIR/metadata.json" <<JSON
{
  "testName": "$TEST_NAME",
  "system": "rabbitmq",
  "image": "rabbitmq:3.13-management",
  "queueCount": $QUEUE_COUNT,
  "queueType": "classic",
  "producer": {"count": 1, "rate": -1, "confirm": 200, "size": 28, "persistent": true},
  "consumer": {"count": 1, "qos": 100, "multi_ack_every": 100, "autoack": false},
  "durationSec": $DURATION,
  "startTime": "$START_TIME",
  "endTime": "$END_TIME"
}
JSON

# Stop perf-test container if still running
docker stop rabbit-perf >/dev/null 2>&1 || true

log "=== DONE RabbitMQ test=$TEST_NAME ==="
