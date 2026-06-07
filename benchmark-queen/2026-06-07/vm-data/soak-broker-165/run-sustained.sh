#!/bin/bash
# bp-100-sustained: 2h soak at the calibrated sustainable rate (~131k push, pop-ahead),
# with 5-min completed retention, on the full (dashboard) image. Rich soak monitoring.
set -u
TAG="${TAG:-0.16.0.beta.1-ui}"
Q="bench-bp-100-sustained"
DUR="${DUR:-7200}"
PW="${PW:-1}"; PC="${PC:-50}"      # producer 1x50  (~131k push)
CW="${CW:-1}"; CC="${CC:-100}"     # consumer 1x100, large batch
MAXPART="${MAXPART:-100}"          # 101 partitions (queue-mode; retention-friendly)
POPBATCH="${POPBATCH:-500}"        # large pop batch (fewer round-trips, frees client CPU)
OUT=/root/bench-runs/results/bp-100-sustained
mkdir -p "$OUT"
ts() { date -u +%FT%TZ; }
log() { echo "[$(ts)] $*"; }
PSQL() { docker exec postgres psql -U postgres -d postgres -tAc "$1" 2>/dev/null; }

log "=== bp-100-sustained START tag=$TAG dur=${DUR}s prod=${PW}x${PC} cons=${CW}x${CC} parts=$((MAXPART+1)) popBatch=$POPBATCH retInterval=5s retParallel=${RETENTION_PARALLELISM:-8} ==="

docker stop queen postgres >/dev/null 2>&1; docker rm -v queen postgres >/dev/null 2>&1
docker volume prune -f >/dev/null 2>&1; docker network create queen >/dev/null 2>&1 || true

log "start postgres (April tuning)"
docker run -d --name postgres --network queen --ulimit nofile=65535:65535 --shm-size=1g \
  -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres \
  -c max_connections=300 -c shared_buffers=24GB -c effective_cache_size=48GB \
  -c maintenance_work_mem=2GB -c work_mem=32MB -c temp_buffers=64MB -c huge_pages=try \
  -c max_worker_processes=20 -c max_parallel_workers=20 -c max_parallel_workers_per_gather=4 \
  -c max_parallel_maintenance_workers=4 -c wal_buffers=128MB -c min_wal_size=8GB -c max_wal_size=96GB \
  -c checkpoint_timeout=15min -c checkpoint_completion_target=0.9 -c synchronous_commit=on -c wal_compression=on \
  -c random_page_cost=1.1 -c effective_io_concurrency=200 -c default_statistics_target=200 \
  -c autovacuum_max_workers=4 -c autovacuum_naptime=10s -c autovacuum_vacuum_scale_factor=0.05 \
  -c autovacuum_analyze_scale_factor=0.02 -c autovacuum_vacuum_cost_limit=2000 -c autovacuum_vacuum_cost_delay=2ms \
  -c log_min_duration_statement=1000 -c log_checkpoints=on -c log_lock_waits=on -c log_autovacuum_min_duration=0 >/dev/null
for i in $(seq 1 60); do docker exec postgres pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done

log "start queen $TAG (RETENTION_BATCH_SIZE=20000 RETENTION_INTERVAL=30000)"
docker run -d --ulimit nofile=65535:65535 --name queen -p 6632:6632 --network queen \
  -e PG_HOST=postgres -e PG_PASSWORD=postgres -e NUM_WORKERS=10 -e DB_POOL_SIZE=50 -e SIDECAR_POOL_SIZE=250 \
  -e RETENTION_BATCH_SIZE=50000 -e RETENTION_INTERVAL=5000 -e RETENTION_PARALLELISM="${RETENTION_PARALLELISM:-8}" \
  smartnessai/queen-mq:"$TAG" >/dev/null
for i in $(seq 1 120); do curl -sf http://localhost:6632/api/v1/status >/dev/null 2>&1 && break; sleep 1; done
sleep 3

log "configure $Q (retentionEnabled, completedRetention=300s, retention=7200s, lease=60s)"
curl -s -X POST http://localhost:6632/api/v1/configure -H "Content-Type: application/json" \
  -d "{\"queue\":\"$Q\",\"options\":{\"leaseTime\":60,\"retryLimit\":3,\"retentionEnabled\":true,\"retentionSeconds\":7200,\"completedRetentionSeconds\":300}}" >/dev/null

START_TIME=$(date -u +%FT%T.000Z); echo "$START_TIME" > "$OUT/start_time.txt"
log "launching producer + consumer for ${DUR}s"
cd /home/queen/examples
QUEUE_NAMES="$Q" NUM_WORKERS="$PW" CONNECTIONS_PER_WORKER="$PC" MAX_PARTITION="$MAXPART" MSGS_PER_PUSH=100 DURATION="$DUR" \
  node bench-producer.js > "$OUT/producer.log" 2>&1 &
PROD=$!
QUEUE_NAMES="$Q" NUM_WORKERS="$CW" CONNECTIONS_PER_WORKER="$CC" CONSUMER_BATCH="$POPBATCH" DURATION="$DUR" \
  node bench-consumer.js > "$OUT/consumer.log" 2>&1 &
CONS=$!

sleep 12
log "enforce retention via direct SQL (configure coerces completed_retention to 1800)"
docker exec postgres psql -U postgres -d postgres -c "UPDATE queen.queues SET retention_enabled=true, retention_seconds=7200, completed_retention_seconds=300 WHERE name='$Q'" >/dev/null 2>&1 || true

CSV="$OUT/soak-metrics.csv"
echo "ts,min,push_s,pop_s,pending,q_vcpu,q_mem_mib,pg_vcpu,pg_mem_gib,msgs_total_gb,dead_tup,live_tup,db_size_gb,wal_mb_s,lock_to_60s" > "$CSV"

read -r PREV_TOTAL PREV_COMP < <(curl -s "http://localhost:6632/api/v1/status" | python3 -c "import json,sys
d=json.load(sys.stdin); m=d.get('messages',{}); print(m.get('total',0), m.get('completed',0))" 2>/dev/null || echo "0 0")
PREV_WAL=$(PSQL "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0')"); PREV_WAL=${PREV_WAL:-0}

MIN=$((DUR/60))
for i in $(seq 1 "$MIN"); do
  sleep 60
  read -r TOTAL COMP PENDING < <(curl -s "http://localhost:6632/api/v1/status" | python3 -c "import json,sys
d=json.load(sys.stdin); m=d.get('messages',{}); print(m.get('total',0), m.get('completed',0), m.get('pending', m.get('total',0)-m.get('completed',0)))" 2>/dev/null || echo "0 0 0")
  PUSH_S=$(( (TOTAL - PREV_TOTAL) / 60 )); POP_S=$(( (COMP - PREV_COMP) / 60 ))
  PREV_TOTAL=$TOTAL; PREV_COMP=$COMP
  QSTAT=$(docker stats --no-stream --format "{{.CPUPerc}}|{{.MemUsage}}" queen 2>/dev/null | head -1)
  PSTAT=$(docker stats --no-stream --format "{{.CPUPerc}}|{{.MemUsage}}" postgres 2>/dev/null | head -1)
  QCPU=$(echo "$QSTAT" | cut -d'|' -f1 | tr -d '% '); QMEM=$(echo "$QSTAT" | cut -d'|' -f2 | awk '{print $1}')
  PCPU=$(echo "$PSTAT" | cut -d'|' -f1 | tr -d '% '); PMEM=$(echo "$PSTAT" | cut -d'|' -f2 | awk '{print $1}')
  ROW=$(PSQL "SELECT pg_total_relation_size('queen.messages'), COALESCE((SELECT n_dead_tup FROM pg_stat_user_tables WHERE schemaname='queen' AND relname='messages'),0), COALESCE((SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname='queen' AND relname='messages'),0), pg_database_size('postgres'), pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0')")
  MSZ=$(echo "$ROW" | cut -d'|' -f1); DEAD=$(echo "$ROW" | cut -d'|' -f2); LIVE=$(echo "$ROW" | cut -d'|' -f3); DBSZ=$(echo "$ROW" | cut -d'|' -f4); WAL=$(echo "$ROW" | cut -d'|' -f5)
  MSZ=${MSZ:-0}; DEAD=${DEAD:-0}; LIVE=${LIVE:-0}; DBSZ=${DBSZ:-0}; WAL=${WAL:-$PREV_WAL}
  MSZ_GB=$(awk "BEGIN{printf \"%.2f\", $MSZ/1073741824}"); DB_GB=$(awk "BEGIN{printf \"%.2f\", $DBSZ/1073741824}")
  WAL_MBS=$(awk "BEGIN{printf \"%.1f\", ($WAL-$PREV_WAL)/60/1048576}"); PREV_WAL=$WAL
  LOCKTO=$(docker logs queen --since=65s 2>&1 | grep -ciE "lock timeout|statement timeout|canceling statement" )
  printf "%s,%d,%d,%d,%d,%s,%s,%s,%s,%s,%d,%d,%s,%s,%d\n" "$(ts)" "$i" "$PUSH_S" "$POP_S" "$PENDING" "$QCPU" "$QMEM" "$PCPU" "$PMEM" "$MSZ_GB" "$DEAD" "$LIVE" "$DB_GB" "$WAL_MBS" "$LOCKTO" >> "$CSV"
  log "  +${i}min push=${PUSH_S}/s pop=${POP_S}/s pending=${PENDING} msgsTbl=${MSZ_GB}GB dead=${DEAD} db=${DB_GB}GB wal=${WAL_MBS}MB/s qCPU=${QCPU}% pgCPU=${PCPU}% lockTO=${LOCKTO}"
done

END_TIME=$(date -u +%FT%T.000Z); echo "$END_TIME" > "$OUT/end_time.txt"
log "duration elapsed; collecting final metrics"
sleep 5
kill -INT "$PROD" "$CONS" 2>/dev/null || true; sleep 5; kill -9 "$PROD" "$CONS" 2>/dev/null || true
curl -s "http://localhost:6632/api/v1/status?from=${START_TIME}&to=${END_TIME}" > "$OUT/status.json"
curl -s "http://localhost:6632/api/v1/analytics/retention?from=${START_TIME}&to=${END_TIME}" > "$OUT/retention.json"
PSQL "SELECT relname, pg_size_pretty(pg_total_relation_size('queen.'||relname)), n_live_tup, n_dead_tup FROM pg_stat_user_tables WHERE schemaname='queen' ORDER BY pg_total_relation_size('queen.'||relname) DESC LIMIT 12" > "$OUT/pg-tables-final.txt"
docker logs queen > "$OUT/queen.log" 2>&1
docker stats --no-stream postgres queen > "$OUT/docker-stats-final.txt" 2>&1
log "=== bp-100-sustained DONE ==="
echo "SUSTAINED-DONE"
