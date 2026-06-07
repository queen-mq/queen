#!/bin/bash
set -u
TAG=0.16.0.beta.1
OUT=/root/bench-runs/cal; mkdir -p $OUT; rm -f $OUT/*.log $OUT/*.json $OUT/*.txt 2>/dev/null
DUR=90
log(){ echo "[$(date -u +%FT%TZ)] $*"; }
docker stop queen postgres >/dev/null 2>&1; docker rm -v queen postgres >/dev/null 2>&1
docker volume prune -f >/dev/null 2>&1; docker network create queen >/dev/null 2>&1 || true
log "start postgres (April tuning)"
docker run -d --name postgres --network queen --ulimit nofile=65535:65535 --shm-size=1g \
  -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres \
  -c max_connections=300 -c shared_buffers=24GB -c effective_cache_size=48GB \
  -c maintenance_work_mem=2GB -c work_mem=32MB -c temp_buffers=64MB -c huge_pages=try \
  -c max_worker_processes=20 -c max_parallel_workers=20 -c max_parallel_workers_per_gather=4 \
  -c max_parallel_maintenance_workers=4 -c wal_buffers=16MB -c min_wal_size=2GB -c max_wal_size=16GB \
  -c checkpoint_timeout=15min -c checkpoint_completion_target=0.9 -c synchronous_commit=on -c wal_compression=on \
  -c random_page_cost=1.1 -c effective_io_concurrency=200 -c default_statistics_target=200 \
  -c autovacuum_max_workers=4 -c autovacuum_naptime=10s -c autovacuum_vacuum_scale_factor=0.05 \
  -c autovacuum_analyze_scale_factor=0.02 -c autovacuum_vacuum_cost_limit=2000 -c autovacuum_vacuum_cost_delay=2ms >/dev/null
for i in $(seq 1 60); do docker exec postgres pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done
log "start queen $TAG (RETENTION_BATCH_SIZE=20000 RETENTION_INTERVAL=30000)"
docker run -d --ulimit nofile=65535:65535 --name queen -p 6632:6632 --network queen \
  -e PG_HOST=postgres -e PG_PASSWORD=postgres -e NUM_WORKERS=10 -e DB_POOL_SIZE=50 -e SIDECAR_POOL_SIZE=250 \
  -e RETENTION_BATCH_SIZE=20000 -e RETENTION_INTERVAL=30000 smartnessai/queen-mq:$TAG >/dev/null
for i in $(seq 1 120); do curl -sf localhost:6632/api/v1/status >/dev/null 2>&1 && break; sleep 1; done
sleep 3
COMBOS=("A 2 100 3 100" "B 3 100 3 100" "C 4 100 3 100")
cd /home/queen/examples
for c in "${COMBOS[@]}"; do
  read -r lbl pw pc cw cc <<< "$c"
  Q="bench-cal-$lbl"
  curl -s -X POST localhost:6632/api/v1/configure -H "Content-Type: application/json" \
    -d "{\"queue\":\"$Q\",\"options\":{\"leaseTime\":60,\"retryLimit\":3,\"retentionEnabled\":true,\"retentionSeconds\":7200,\"completedRetentionSeconds\":300}}" >/dev/null
  log "=== combo $lbl prod=${pw}x${pc} cons=${cw}x${cc} on $Q ==="
  QUEUE_NAMES="$Q" NUM_WORKERS="$pw" CONNECTIONS_PER_WORKER="$pc" MAX_PARTITION=1000 MSGS_PER_PUSH=100 DURATION=$DUR node bench-producer.js > $OUT/$lbl-prod.log 2>&1 &
  PP=$!
  QUEUE_NAMES="$Q" NUM_WORKERS="$cw" CONNECTIONS_PER_WORKER="$cc" CONSUMER_BATCH=100 DURATION=$DUR node bench-consumer.js > $OUT/$lbl-cons.log 2>&1 &
  CP=$!
  sleep 40; docker stats --no-stream --format "{{.Name}} {{.CPUPerc}}" queen postgres > $OUT/$lbl-cpu.txt 2>&1
  sleep 25; docker stats --no-stream --format "{{.Name}} {{.CPUPerc}}" queen postgres >> $OUT/$lbl-cpu.txt 2>&1
  wait $PP $CP 2>/dev/null
  sleep 3
  curl -s "localhost:6632/api/v1/resources/queues/$Q" > $OUT/$lbl-queue.json 2>&1
  log "  done $lbl"
done
echo "CAL-DONE"
