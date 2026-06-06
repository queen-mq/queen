#!/bin/bash
# Bring up PG + Queen broker ONLY (clients run on a separate loader VM).
set -u
TAG="${TAG:-0.16.0.beta.1-ui}"
PARALLEL="${RETENTION_PARALLELISM:-8}"
WORKERS="${NUM_WORKERS:-10}"
log(){ echo "[$(date -u +%FT%TZ)] $*"; }

docker stop queen postgres >/dev/null 2>&1; docker rm -v queen postgres >/dev/null 2>&1
docker volume prune -f >/dev/null 2>&1; docker network create queen >/dev/null 2>&1 || true

log "start postgres (April tuning + max_wal_size=96GB)"
docker run -d --name postgres --network queen --ulimit nofile=65535:65535 --shm-size=1g \
  -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres \
  -c max_connections=800 -c shared_buffers=24GB -c effective_cache_size=48GB \
  -c maintenance_work_mem=2GB -c work_mem=32MB -c temp_buffers=64MB -c huge_pages=try \
  -c max_worker_processes=20 -c max_parallel_workers=20 -c max_parallel_workers_per_gather=4 \
  -c max_parallel_maintenance_workers=4 -c wal_buffers=128MB -c min_wal_size=8GB -c max_wal_size=96GB \
  -c checkpoint_timeout=15min -c checkpoint_completion_target=0.9 -c synchronous_commit=on -c wal_compression=on \
  -c random_page_cost=1.1 -c effective_io_concurrency=200 -c default_statistics_target=200 \
  -c autovacuum_max_workers=4 -c autovacuum_naptime=10s -c autovacuum_vacuum_scale_factor=0.05 \
  -c autovacuum_analyze_scale_factor=0.02 -c autovacuum_vacuum_cost_limit=2000 -c autovacuum_vacuum_cost_delay=2ms \
  -c log_min_duration_statement=1000 -c log_checkpoints=on >/dev/null
for i in $(seq 1 60); do docker exec postgres pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done

log "start queen $TAG (NUM_WORKERS=$WORKERS, RETENTION_PARALLELISM=$PARALLEL), listening on 0.0.0.0:6632"
  log "  PUSH policy: MAX_CONCURRENT=${QUEEN_PUSH_MAX_CONCURRENT:-24} MAX_HOLD_MS=${QUEEN_PUSH_MAX_HOLD_MS:-20} PREFERRED=${QUEEN_PUSH_PREFERRED_BATCH_SIZE:-50} MAX_BATCH=${QUEEN_PUSH_MAX_BATCH_SIZE:-500}"
log "  CONCURRENCY_MODE=${QUEEN_CONCURRENCY_MODE:-vegas} POP_MAX_CONCURRENT=${QUEEN_POP_MAX_CONCURRENT:-16}"
docker run -d --ulimit nofile=65535:65535 --name queen -p 6632:6632 --network queen \
  -e PG_HOST=postgres -e PG_PASSWORD=postgres -e NUM_WORKERS="$WORKERS" -e DB_POOL_SIZE="${DB_POOL_SIZE:-50}" -e SIDECAR_POOL_SIZE=250 \
  -e RETENTION_BATCH_SIZE=50000 -e RETENTION_INTERVAL=5000 -e RETENTION_PARALLELISM="$PARALLEL" \
  -e QUEEN_PUSH_MAX_CONCURRENT="${QUEEN_PUSH_MAX_CONCURRENT:-24}" -e QUEEN_PUSH_MAX_HOLD_MS="${QUEEN_PUSH_MAX_HOLD_MS:-20}" \
  -e QUEEN_PUSH_PREFERRED_BATCH_SIZE="${QUEEN_PUSH_PREFERRED_BATCH_SIZE:-50}" -e QUEEN_PUSH_MAX_BATCH_SIZE="${QUEEN_PUSH_MAX_BATCH_SIZE:-500}" \
  -e POP_WAIT_INITIAL_INTERVAL_MS="${POP_WAIT_INITIAL_INTERVAL_MS:-10}" -e POP_WAIT_BACKOFF_THRESHOLD="${POP_WAIT_BACKOFF_THRESHOLD:-5}" \
  -e POP_WAIT_BACKOFF_MULTIPLIER="${POP_WAIT_BACKOFF_MULTIPLIER:-2}" -e POP_WAIT_MAX_INTERVAL_MS="${POP_WAIT_MAX_INTERVAL_MS:-100}" \
  -e QUEEN_CONCURRENCY_MODE="${QUEEN_CONCURRENCY_MODE:-vegas}" -e QUEEN_POP_MAX_CONCURRENT="${QUEEN_POP_MAX_CONCURRENT:-16}" \
  smartnessai/queen-mq:"$TAG" >/dev/null
for i in $(seq 1 120); do curl -sf http://localhost:6632/api/v1/status >/dev/null 2>&1 && break; sleep 1; done

log "broker up:"
docker ps --format "  {{.Names}}: {{.Status}}"
echo "BROKER-READY"
