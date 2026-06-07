#!/bin/bash
set -u

TEST_NAME="$1"
MAX_PARTITION="$2"
MSGS_PER_PUSH="$3"
DURATION="${4:-900}"
PROD_WORKERS="${5:-1}"
PROD_CONN="${6:-50}"
CONS_WORKERS="${7:-1}"
CONS_CONN="${8:-50}"
CONS_BATCH="${9:-100}"
QUEUE_COUNT="${10:-1}"
QUEEN_IMAGE_TAG="${11:-0.14.0.alpha.3}"
CG_COUNT="${12:-0}"  # number of consumer groups (0 = single default consumer, no group)

OUTDIR="/root/bench-runs/results/$TEST_NAME"
mkdir -p "$OUTDIR"

ts() { date -u +%FT%TZ; }
log() { echo "[$(ts)] [$TEST_NAME] $*"; }

# Build comma-separated queue list
if [ "$QUEUE_COUNT" -le 1 ]; then
  QUEUE_NAMES="bench-${TEST_NAME}"
else
  QUEUE_NAMES=""
  for i in $(seq 1 "$QUEUE_COUNT"); do
    if [ -z "$QUEUE_NAMES" ]; then
      QUEUE_NAMES="bench-${TEST_NAME}-${i}"
    else
      QUEUE_NAMES="${QUEUE_NAMES},bench-${TEST_NAME}-${i}"
    fi
  done
fi

log "=== START test=$TEST_NAME image=$QUEEN_IMAGE_TAG maxPart=$MAX_PARTITION batch=$MSGS_PER_PUSH dur=${DURATION}s prod=${PROD_WORKERS}x${PROD_CONN} cons=${CONS_WORKERS}x${CONS_CONN} consBatch=$CONS_BATCH queues=$QUEUE_COUNT cgCount=$CG_COUNT ==="

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
  -c max_connections=300 -c shared_buffers=24GB -c effective_cache_size=48GB \
  -c maintenance_work_mem=2GB -c work_mem=32MB -c temp_buffers=64MB -c huge_pages=try \
  -c max_worker_processes=20 -c max_parallel_workers=20 \
  -c max_parallel_workers_per_gather=4 -c max_parallel_maintenance_workers=4 \
  -c wal_buffers=16MB -c min_wal_size=2GB -c max_wal_size=16GB \
  -c checkpoint_timeout=15min -c checkpoint_completion_target=0.9 \
  -c synchronous_commit=on -c wal_compression=on \
  -c random_page_cost=1.1 -c effective_io_concurrency=200 -c default_statistics_target=200 \
  -c autovacuum_max_workers=4 -c autovacuum_naptime=10s \
  -c autovacuum_vacuum_scale_factor=0.05 -c autovacuum_analyze_scale_factor=0.02 \
  -c autovacuum_vacuum_cost_limit=2000 -c autovacuum_vacuum_cost_delay=2ms \
  -c log_min_duration_statement=1000 -c log_checkpoints=on -c log_lock_waits=on \
  -c log_temp_files=0 -c log_autovacuum_min_duration=0 \
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
  "smartnessai/queen-mq:${QUEEN_IMAGE_TAG}" >/dev/null

log "waiting for queen"
for i in $(seq 1 120); do
  if curl -sf http://localhost:6632/api/v1/status >/dev/null 2>&1; then
    log "  queen ready (after ${i}s)"
    break
  fi
  sleep 1
done

sleep 5
log "settle done, launching producer/consumer (queues=$QUEUE_COUNT)"

START_TIME=$(date -u +%FT%T.000Z)
echo "$START_TIME" > "$OUTDIR/start_time.txt"

cd /home/queen/examples
QUEUE_NAMES="$QUEUE_NAMES" NUM_WORKERS="$PROD_WORKERS" CONNECTIONS_PER_WORKER="$PROD_CONN" \
  MAX_PARTITION="$MAX_PARTITION" MSGS_PER_PUSH="$MSGS_PER_PUSH" DURATION="$DURATION" \
  node /home/queen/examples/bench-producer.js > "$OUTDIR/producer.log" 2>&1 &
PROD_PID=$!

CONS_PIDS=()
if [ "$CG_COUNT" -le 0 ]; then
  # Single consumer, no group (default __QUEUE_MODE__)
  QUEUE_NAMES="$QUEUE_NAMES" NUM_WORKERS="$CONS_WORKERS" CONNECTIONS_PER_WORKER="$CONS_CONN" \
    CONSUMER_BATCH="$CONS_BATCH" DURATION="$DURATION" \
    node /home/queen/examples/bench-consumer.js > "$OUTDIR/consumer.log" 2>&1 &
  CONS_PIDS+=($!)
else
  # CG_COUNT consumer processes, one per consumer group
  for cg_idx in $(seq 1 "$CG_COUNT"); do
    QUEUE_NAMES="$QUEUE_NAMES" NUM_WORKERS="$CONS_WORKERS" CONNECTIONS_PER_WORKER="$CONS_CONN" \
      CONSUMER_BATCH="$CONS_BATCH" DURATION="$DURATION" \
      CONSUMER_GROUP="cg-${cg_idx}" \
      node /home/queen/examples/bench-consumer.js > "$OUTDIR/consumer-cg${cg_idx}.log" 2>&1 &
    CONS_PIDS+=($!)
  done
fi

log "producer pid=$PROD_PID consumer pids=${CONS_PIDS[*]} (n=${#CONS_PIDS[@]}), waiting ${DURATION}s"

MINUTES=$((DURATION / 60))
for i in $(seq 1 $MINUTES); do
  sleep 60
  PG_S=$(docker stats --no-stream --format "{{.CPUPerc}}/{{.MemUsage}}" postgres 2>/dev/null | head -1)
  QN_S=$(docker stats --no-stream --format "{{.CPUPerc}}/{{.MemUsage}}" queen 2>/dev/null | head -1)
  log "  +${i}min postgres=${PG_S} queen=${QN_S}"
  echo "[$(ts)] postgres=${PG_S} queen=${QN_S}" >> "$OUTDIR/docker-stats.log"
done

log "duration elapsed, waiting 30s for clients to finalize"
sleep 30

END_TIME=$(date -u +%FT%T.000Z)
echo "$END_TIME" > "$OUTDIR/end_time.txt"

kill -INT "$PROD_PID" "${CONS_PIDS[@]}" 2>/dev/null || true
sleep 5
kill -9 "$PROD_PID" "${CONS_PIDS[@]}" 2>/dev/null || true
wait "$PROD_PID" "${CONS_PIDS[@]}" 2>/dev/null || true

log "collecting metrics"
curl -s "http://localhost:6632/api/v1/status?from=${START_TIME}&to=${END_TIME}" > "$OUTDIR/status.json"
curl -s "http://localhost:6632/api/v1/analytics/retention?from=${START_TIME}&to=${END_TIME}" > "$OUTDIR/retention.json"
curl -s "http://localhost:6632/api/v1/analytics/queue-ops?from=${START_TIME}&to=${END_TIME}" > "$OUTDIR/queue-ops.json"
curl -s "http://localhost:6632/api/v1/analytics/system-metrics?from=${START_TIME}&to=${END_TIME}" > "$OUTDIR/system-metrics.json"
curl -s "http://localhost:6632/api/v1/analytics/postgres-stats" > "$OUTDIR/postgres-stats.json"
# Save resources for first queue, summary for rest
FIRST_QUEUE=$(echo "$QUEUE_NAMES" | cut -d, -f1)
curl -s "http://localhost:6632/api/v1/resources/queues/$FIRST_QUEUE" > "$OUTDIR/queue-resource-first.json"
curl -s "http://localhost:6632/api/v1/resources/queues" > "$OUTDIR/queues-list.json"
docker logs queen > "$OUTDIR/queen.log" 2>&1
docker stats --no-stream postgres queen > "$OUTDIR/docker-stats-final.txt" 2>&1

PARTITION_COUNT=$((MAX_PARTITION + 1))
EFFECTIVE_CG_COUNT=$([ "$CG_COUNT" -le 0 ] && echo 0 || echo "$CG_COUNT")
TOTAL_CONS_CONN=$([ "$EFFECTIVE_CG_COUNT" -le 0 ] && echo $((CONS_WORKERS * CONS_CONN)) || echo $((EFFECTIVE_CG_COUNT * CONS_WORKERS * CONS_CONN)))
cat > "$OUTDIR/metadata.json" <<JSON
{
  "testName": "$TEST_NAME",
  "queenImageTag": "$QUEEN_IMAGE_TAG",
  "queueNames": "$QUEUE_NAMES",
  "queueCount": $QUEUE_COUNT,
  "maxPartition": $MAX_PARTITION,
  "partitionCount": $PARTITION_COUNT,
  "msgsPerPush": $MSGS_PER_PUSH,
  "producer": {"workers": $PROD_WORKERS, "connectionsPerWorker": $PROD_CONN, "totalConnections": $((PROD_WORKERS * PROD_CONN))},
  "consumer": {"processes": ${#CONS_PIDS[@]}, "workersPerProcess": $CONS_WORKERS, "connectionsPerWorker": $CONS_CONN, "totalConnections": $TOTAL_CONS_CONN, "batch": $CONS_BATCH, "consumerGroupCount": $EFFECTIVE_CG_COUNT},
  "durationSec": $DURATION,
  "startTime": "$START_TIME",
  "endTime": "$END_TIME"
}
JSON

log "=== DONE test=$TEST_NAME ==="
