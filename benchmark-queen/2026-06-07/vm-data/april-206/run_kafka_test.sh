#!/bin/bash
# Run a Kafka comparison test against Queen's bp-10 baseline.
# Single-node Kafka in KRaft mode, 1000-partition topic, 15-min simultaneous
# producer + consumer perf test. Captures throughput, latency, RSS, CPU.

set -u

TEST_NAME="${1:-kafka-1000p}"
DURATION="${2:-900}"
TOPIC_PARTITIONS="${3:-1000}"
TARGET_RATE="${4:--1}"  # -1 = max possible

OUTDIR="/root/bench-runs/results/${TEST_NAME}"
mkdir -p "$OUTDIR"

ts() { date -u +%FT%TZ; }
log() { echo "[$(ts)] [$TEST_NAME] $*"; }

log "=== START Kafka test=$TEST_NAME partitions=$TOPIC_PARTITIONS dur=${DURATION}s rate=$TARGET_RATE ==="

# Cleanup
log "cleanup: stopping containers"
docker stop kafka kafka-perf 2>/dev/null || true
docker rm -v kafka kafka-perf 2>/dev/null || true
docker volume prune -f >/dev/null 2>&1 || true
docker network create queen >/dev/null 2>&1 || true

# Generate cluster ID for KRaft
CLUSTER_ID=$(docker run --rm apache/kafka:3.7.0 /opt/kafka/bin/kafka-storage.sh random-uuid)
log "cluster_id=$CLUSTER_ID"

# Start Kafka in KRaft mode (single-node: combined broker+controller)
log "starting kafka (KRaft single-node)"
docker run -d --name kafka --network queen \
  --ulimit nofile=65535:65535 \
  -p 9092:9092 -p 9093:9093 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS='PLAINTEXT://:9092,CONTROLLER://:9093' \
  -e KAFKA_ADVERTISED_LISTENERS='PLAINTEXT://kafka:9092' \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP='CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT' \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS="1@kafka:9093" \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_NUM_IO_THREADS=16 \
  -e KAFKA_NUM_NETWORK_THREADS=8 \
  -e KAFKA_NUM_REPLICA_FETCHERS=4 \
  -e KAFKA_SOCKET_SEND_BUFFER_BYTES=1048576 \
  -e KAFKA_SOCKET_RECEIVE_BUFFER_BYTES=1048576 \
  -e KAFKA_SOCKET_REQUEST_MAX_BYTES=104857600 \
  -e KAFKA_LOG_FLUSH_INTERVAL_MESSAGES=10000 \
  -e KAFKA_LOG_FLUSH_INTERVAL_MS=1000 \
  -e KAFKA_HEAP_OPTS='-Xms2g -Xmx4g' \
  -e CLUSTER_ID="$CLUSTER_ID" \
  apache/kafka:3.7.0 >/dev/null

# Wait for Kafka ready
log "waiting for kafka"
for i in $(seq 1 60); do
  if docker exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    log "  kafka ready (after ${i}s)"
    break
  fi
  sleep 1
done

# Create topic
log "creating topic with $TOPIC_PARTITIONS partitions"
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic bench-topic \
  --partitions "$TOPIC_PARTITIONS" \
  --replication-factor 1 \
  --config min.insync.replicas=1 \
  --config retention.ms=86400000 \
  >/dev/null 2>&1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic bench-topic | head -3

sleep 5
log "settle done, launching producer + consumer"

START_TIME=$(date -u +%FT%T.000Z)
echo "$START_TIME" > "$OUTDIR/start_time.txt"

# Write run scripts on host, then docker cp into container
PRODSH="$OUTDIR/run-producer.sh"
CONSSH="$OUTDIR/run-consumer.sh"
cat > "$PRODSH" <<EOF
#!/bin/sh
exec /opt/kafka/bin/kafka-producer-perf-test.sh \\
  --topic bench-topic \\
  --num-records 1000000000 \\
  --record-size 28 \\
  --throughput $TARGET_RATE \\
  --producer-props \\
    bootstrap.servers=localhost:9092 \\
    acks=1 \\
    linger.ms=10 \\
    batch.size=16384 \\
    compression.type=none \\
    buffer.memory=67108864 \\
    max.in.flight.requests.per.connection=5
EOF
CONS_CONF="$OUTDIR/consumer.properties"
cat > "$CONS_CONF" <<EOF
auto.offset.reset=earliest
fetch.max.bytes=10485760
max.poll.records=100
EOF
cat > "$CONSSH" <<EOF
#!/bin/sh
exec /opt/kafka/bin/kafka-consumer-perf-test.sh \\
  --bootstrap-server localhost:9092 \\
  --topic bench-topic \\
  --group bench-group \\
  --messages 1000000000 \\
  --timeout 1000000 \\
  --reporting-interval 5000 \\
  --show-detailed-stats \\
  --consumer.config /tmp/consumer.properties
EOF
chmod +x "$PRODSH" "$CONSSH"
docker cp "$PRODSH" kafka:/tmp/run-producer.sh
docker cp "$CONSSH" kafka:/tmp/run-consumer.sh
docker cp "$CONS_CONF" kafka:/tmp/consumer.properties

# Launch in background inside the container
docker exec -d kafka sh -c 'nohup /tmp/run-producer.sh > /tmp/kafka-producer.log 2>&1 &'
docker exec -d kafka sh -c 'nohup /tmp/run-consumer.sh > /tmp/kafka-consumer.log 2>&1 &'
sleep 3
docker exec kafka pgrep -af 'ProducerPerformance|ConsumerPerformance|kafka-producer|kafka-consumer' || log "WARN: no perf processes found"

log "producer + consumer running, waiting ${DURATION}s"

# 30-second status interval, in case DURATION < 60s for smoke testing
INTERVAL=30
LOOPS=$((DURATION / INTERVAL))
[ "$LOOPS" -lt 1 ] && LOOPS=1
for i in $(seq 1 $LOOPS); do
  sleep $INTERVAL
  K_S=$(docker stats --no-stream --format "{{.CPUPerc}}/{{.MemUsage}}" kafka 2>/dev/null | head -1)
  log "  +$((i * INTERVAL))s kafka=${K_S}"
  echo "[$(ts)] kafka=${K_S}" >> "$OUTDIR/docker-stats.log"
done

log "duration elapsed, killing perf-tests"
docker exec kafka /bin/sh -c "pkill -f kafka-producer-perf-test; pkill -f kafka-consumer-perf-test; sleep 2; pkill -9 -f kafka-producer-perf-test; pkill -9 -f kafka-consumer-perf-test" 2>/dev/null || true
sleep 5

END_TIME=$(date -u +%FT%T.000Z)
echo "$END_TIME" > "$OUTDIR/end_time.txt"

log "collecting metrics"
docker exec kafka cat /tmp/kafka-producer.log > "$OUTDIR/producer.log" 2>&1 || true
docker exec kafka cat /tmp/kafka-consumer.log > "$OUTDIR/consumer.log" 2>&1 || true
docker stats --no-stream kafka > "$OUTDIR/docker-stats-final.txt" 2>&1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic bench-topic > "$OUTDIR/topic-describe.txt" 2>&1
docker exec kafka /opt/kafka/bin/kafka-log-dirs.sh --bootstrap-server localhost:9092 --topic-list bench-topic --describe 2>/dev/null | grep -oE '"size":[0-9]+' | awk -F: '{sum+=$2}END{print "total bytes on disk:", sum}' > "$OUTDIR/disk-usage.txt" || true
docker logs kafka > "$OUTDIR/kafka.log" 2>&1
docker exec kafka du -sh /tmp/kraft-combined-logs/ 2>/dev/null > "$OUTDIR/kafka-data-size.txt" || true

cat > "$OUTDIR/metadata.json" <<JSON
{
  "testName": "$TEST_NAME",
  "system": "kafka",
  "image": "apache/kafka:3.7.0",
  "topicPartitions": $TOPIC_PARTITIONS,
  "replicationFactor": 1,
  "producer": {"acks": 1, "linger_ms": 10, "batch_size": 16384, "compression": "none"},
  "consumer": {"fetch_max_bytes": 10485760, "max_poll_records": 100, "group": "bench-group"},
  "messageSize": 28,
  "targetRate": $TARGET_RATE,
  "durationSec": $DURATION,
  "kafkaHeap": "2g-4g",
  "kafkaIoThreads": 16,
  "kafkaNetworkThreads": 8,
  "startTime": "$START_TIME",
  "endTime": "$END_TIME"
}
JSON

log "=== DONE Kafka test=$TEST_NAME ==="
