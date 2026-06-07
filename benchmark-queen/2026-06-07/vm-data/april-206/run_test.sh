#!/bin/bash
set -u

TEST_NAME="$1"
MAX_PARTITION="$2"
MSGS_PER_PUSH="$3"
DURATION="${4:-900}"
NUM_WORKERS="${5:-1}"
CONN="${6:-50}"

OUTDIR="/root/bench-runs/results/$TEST_NAME"
mkdir -p "$OUTDIR"

ts() { date -u +%FT%TZ; }
log() { echo "[$(ts)] [$TEST_NAME] $*"; }

log "=== START test=$TEST_NAME maxPart=$MAX_PARTITION batch=$MSGS_PER_PUSH dur=${DURATION}s ==="

log "cleanup: stopping containers"
docker stop queen postgres >/dev/null 2>&1 || true
docker rm -v queen postgres >/dev/null 2>&1 || true
docker volume prune -f >/dev/null 2>&1 || true
docker network create queen >/dev/null 2>&1 || true

log "starting postgres"
docker run -d --name postgres --network queen \
  --ulimit nofile=65535:65535 --shm-size=1g \
  -e POSTGRES_PASSWORD=postgres -p 5432:5432 \
  postgres \
  -c max_connections=300 \
  -c shared_buffers=24GB \
  -c effective_cache_size=48GB \
  -c maintenance_work_mem=2GB \
  -c work_mem=32MB \
  -c temp_buffers=64MB \
  -c huge_pages=try \
  -c max_worker_processes=20 \
  -c max_parallel_workers=20 \
  -c max_parallel_workers_per_gather=4 \
  -c max_parallel_maintenance_workers=4 \
  -c wal_buffers=16MB \
  -c min_wal_size=2GB \
  -c max_wal_size=16GB \
  -c checkpoint_timeout=15min \
  -c checkpoint_completion_target=0.9 \
  -c synchronous_commit=on \
  -c wal_compression=on \
  -c random_page_cost=1.1 \
  -c effective_io_concurrency=200 \
  -c default_statistics_target=200 \
  -c autovacuum_max_workers=4 \
  -c autovacuum_naptime=10s \
  -c autovacuum_vacuum_scale_factor=0.05 \
  -c autovacuum_analyze_scale_factor=0.02 \
  -c autovacuum_vacuum_cost_limit=2000 \
  -c autovacuum_vacuum_cost_delay=2ms \
  -c log_min_duration_statement=1000 \
  -c log_checkpoints=on \
  -c log_lock_waits=on \
  -c log_temp_files=0 \
  -c log_autovacuum_min_duration=0 \
  >/dev/null

log "waiting for postgres"
for i in $(seq 1 60); do
  if docker exec postgres pg_isready -U postgres >/dev/null 2>&1; then
    log "  postgres ready (after ${i}s)"
    break
  fi
  sleep 1
done

log "starting queen"
docker run -d --ulimit nofile=65535:65535 \
  --name queen -p 6632:6632 --network queen \
  -e PG_HOST=postgres -e PG_PASSWORD=postgres \
  -e NUM_WORKERS=10 -e DB_POOL_SIZE=50 -e SIDECAR_POOL_SIZE=250 \
  smartnessai/queen-mq:0.14.0.alpha.3 >/dev/null

log "waiting for queen"
for i in $(seq 1 120); do
  if curl -sf http://localhost:6632/api/v1/status >/dev/null 2>&1; then
    log "  queen ready (after ${i}s)"
    break
  fi
  sleep 1
done

sleep 5
log "settle done, launching producer/consumer"

QUEUE_NAME="bench-${TEST_NAME}"
START_TIME=$(date -u +%FT%T.000Z)
echo "$START_TIME" > "$OUTDIR/start_time.txt"

cd /home/queen/examples
QUEUE_NAME="$QUEUE_NAME" NUM_WORKERS="$NUM_WORKERS" CONNECTIONS_PER_WORKER="$CONN" \
  MAX_PARTITION="$MAX_PARTITION" MSGS_PER_PUSH="$MSGS_PER_PUSH" DURATION="$DURATION" \
  node /home/queen/examples/bench-producer.js > "$OUTDIR/producer.log" 2>&1 &
PROD_PID=$!

QUEUE_NAME="$QUEUE_NAME" NUM_WORKERS="$NUM_WORKERS" CONNECTIONS_PER_WORKER="$CONN" \
  CONSUMER_BATCH=100 DURATION="$DURATION" \
  node /home/queen/examples/bench-consumer.js > "$OUTDIR/consumer.log" 2>&1 &
CONS_PID=$!

log "producer pid=$PROD_PID consumer pid=$CONS_PID, waiting ${DURATION}s"

MINUTES=$((DURATION / 60))
for i in $(seq 1 $MINUTES); do
  sleep 60
  STATS=$(docker stats --no-stream --format "pg={{.CPUPerc}}/{{.MemUsage}} qn={{.CPUPerc}}/{{.MemUsage}}" postgres queen 2>/dev/null | tr '\n' '|')
  log "  +${i}min ${STATS}"
  echo "[$(ts)] $STATS" >> "$OUTDIR/docker-stats.log"
done

log "duration elapsed, waiting 30s for clients to finalize"
sleep 30

END_TIME=$(date -u +%FT%T.000Z)
echo "$END_TIME" > "$OUTDIR/end_time.txt"

kill -INT $PROD_PID $CONS_PID 2>/dev/null || true
sleep 5
kill -9 $PROD_PID $CONS_PID 2>/dev/null || true
wait $PROD_PID $CONS_PID 2>/dev/null || true

log "collecting metrics"
curl -s "http://localhost:6632/api/v1/status?from=${START_TIME}&to=${END_TIME}" > "$OUTDIR/status.json"
curl -s "http://localhost:6632/api/v1/analytics/retention?from=${START_TIME}&to=${END_TIME}" > "$OUTDIR/retention.json"
curl -s "http://localhost:6632/api/v1/analytics/queue-ops?from=${START_TIME}&to=${END_TIME}" > "$OUTDIR/queue-ops.json"
curl -s "http://localhost:6632/api/v1/analytics/system-metrics?from=${START_TIME}&to=${END_TIME}" > "$OUTDIR/system-metrics.json"
curl -s "http://localhost:6632/api/v1/analytics/postgres-stats" > "$OUTDIR/postgres-stats.json"
curl -s "http://localhost:6632/api/v1/resources/queues/$QUEUE_NAME" > "$OUTDIR/queue-resource.json"
docker logs queen > "$OUTDIR/queen.log" 2>&1
docker stats --no-stream postgres queen > "$OUTDIR/docker-stats-final.txt" 2>&1

PARTITION_COUNT=$((MAX_PARTITION + 1))
cat > "$OUTDIR/metadata.json" <<JSON
{
  "testName": "$TEST_NAME",
  "queueName": "$QUEUE_NAME",
  "maxPartition": $MAX_PARTITION,
  "partitionCount": $PARTITION_COUNT,
  "msgsPerPush": $MSGS_PER_PUSH,
  "numWorkers": $NUM_WORKERS,
  "connectionsPerWorker": $CONN,
  "consumerBatch": 100,
  "durationSec": $DURATION,
  "startTime": "$START_TIME",
  "endTime": "$END_TIME"
}
JSON

log "=== DONE test=$TEST_NAME ==="
