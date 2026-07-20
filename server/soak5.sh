#!/usr/bin/env bash
# soak5.sh — replicate the 0.16 24h-soak WORKLOAD (bp-100, 101 partitions, 300s
# completed-retention, April PG tuning w/ max_wal_size=96GB, RETENTION_PARALLELISM=8)
# but for 5 minutes, to see where tuned Rust lands. C++ 0.16 anchor in the same
# conditions. Per-30s trajectory: push/s, pop/s, broker CPU, pg CPU, messages table size.
set -uo pipefail
NET=qbench; PG=qbench-pg; GOLOAD=/root/goload
DUR="${DUR:-300}"; STEP="${STEP:-30}"

log(){ echo "[$(date -u +%FT%TZ)] $*"; }
q(){ docker exec "$PG" psql -U postgres -tAc "$1" </dev/null 2>/dev/null; }

pg_up(){
  log "recreate Postgres with soak/April tuning (max_wal_size=96GB)"
  docker rm -fv "$PG" >/dev/null 2>&1 || true
  docker run -d --name "$PG" --network "$NET" --ulimit nofile=65535:65535 --shm-size=1g \
    -e POSTGRES_PASSWORD=postgres postgres:16 \
    -c max_connections=300 -c shared_buffers=24GB -c effective_cache_size=48GB \
    -c maintenance_work_mem=2GB -c work_mem=32MB -c temp_buffers=64MB \
    -c max_worker_processes=20 -c max_parallel_workers=20 -c max_parallel_workers_per_gather=4 \
    -c max_parallel_maintenance_workers=4 -c wal_buffers=128MB -c min_wal_size=8GB -c max_wal_size=96GB \
    -c checkpoint_timeout=15min -c checkpoint_completion_target=0.9 -c synchronous_commit=on -c wal_compression=on \
    -c random_page_cost=1.1 -c effective_io_concurrency=200 -c default_statistics_target=200 \
    -c autovacuum_max_workers=4 -c autovacuum_naptime=10s -c autovacuum_vacuum_scale_factor=0.05 \
    -c autovacuum_analyze_scale_factor=0.02 -c autovacuum_vacuum_cost_limit=2000 -c autovacuum_vacuum_cost_delay=2ms >/dev/null
  for i in $(seq 1 60); do docker exec "$PG" pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done
}

schema(){
  q "select pg_terminate_backend(pid) from pg_stat_activity where pid<>pg_backend_pid() and state<>'idle';" >/dev/null 2>&1
  q "DROP SCHEMA IF EXISTS queen CASCADE;" >/dev/null
  docker cp /root/queen/lib/schema/schema.sql "$PG":/tmp/s.sql >/dev/null
  docker exec -i "$PG" psql -U postgres -q -f /tmp/s.sql </dev/null >/dev/null 2>&1
  for f in $(ls /root/queen/lib/schema/procedures/*.sql|sort); do
    docker cp "$f" "$PG":/tmp/p.sql >/dev/null
    docker exec -i "$PG" psql -U postgres -q -f /tmp/p.sql </dev/null >/dev/null 2>&1
  done
  q "VACUUM ANALYZE;" >/dev/null 2>&1
}

scrape(){ curl -s "http://localhost:$1/metrics/prometheus" 2>/dev/null \
  | awk -v m="$2" 'index($1,m)==1 {print $2; exit}'; }

soak(){
  local label="$1" img="$2" port="$3"; shift 3
  pg_up; schema
  local nm="b$port"; docker rm -f "$nm" >/dev/null 2>&1
  docker run -d --name "$nm" --network "$NET" --ulimit nofile=65535:65535 -p "$port":6632 \
    -e PG_HOST="$PG" -e PG_PASSWORD=postgres "$@" "$img" >/dev/null
  for i in $(seq 1 120); do curl -sf "http://localhost:$port/api/v1/status" >/dev/null 2>&1 && break; sleep 1; done
  # configure retention like the soak (queue auto-created on first push otherwise)
  local Q="soak$RANDOM"
  curl -s -X POST "http://localhost:$port/api/v1/configure" -H 'Content-Type: application/json' \
    -d "{\"queue\":\"$Q\",\"options\":{\"leaseTime\":60,\"retryLimit\":3,\"retentionEnabled\":true,\"retentionSeconds\":7200,\"completedRetentionSeconds\":300}}" >/dev/null 2>&1
  q "UPDATE queen.queues SET retention_enabled=true, retention_seconds=7200, completed_retention_seconds=300 WHERE name='$Q';" >/dev/null 2>&1 || true

  log "=== $label :$port  bp-100 for ${DUR}s (queue=$Q) ==="
  "$GOLOAD" -mode max -url "http://localhost:$port" -queue "$Q" \
    -partitions 100 -producers 100 -consumers 100 -push-batch 100 -pop-batch 500 \
    -pop-partitions 10 -pop-wait=true -pop-timeout=2000 -payload 256 \
    -duration "$DUR" -report 100 >/tmp/soak.log 2>&1 &
  local lp=$!

  echo "  min | push/s  pop/s | brokerCPU  pgCPU | msgsTbl commits/s"
  local pP=0 pO=0 pC=0 first=1
  local n=$((DUR/STEP))
  for i in $(seq 1 "$n"); do
    sleep "$STEP"
    local P O C
    P=$(scrape "$port" queen_cluster_push_messages_total); P=${P:-0}
    O=$(scrape "$port" queen_cluster_pop_messages_total);  O=${O:-0}
    C=$(q "select xact_commit from pg_stat_database where datname='postgres';"); C=${C:-0}
    local bcpu pgcpu tbl
    bcpu=$(docker stats --no-stream --format '{{.CPUPerc}}' "$nm" 2>/dev/null|tr -d '%')
    pgcpu=$(docker stats --no-stream --format '{{.CPUPerc}}' "$PG" 2>/dev/null|tr -d '%')
    tbl=$(q "select pg_size_pretty(pg_total_relation_size('queen.messages'));")
    if [ "$first" = "1" ]; then first=0; else
      awk -v t="$((i*STEP/60))m$((i*STEP%60))s" -v dp=$((P-pP)) -v do_=$((O-pO)) -v dc=$((C-pC)) -v st="$STEP" \
        -v bc="$bcpu" -v pc="$pgcpu" -v tb="$tbl" 'BEGIN{
        printf "  %-5s | %6.0f %6.0f | %8s%% %7s%% | %7s %.0f\n", t, dp/st, do_/st, bc, pc, tb, dc/st }'
    fi
    pP=$P; pO=$O; pC=$C
  done
  wait "$lp"
  log "final load: $(tail -1 /tmp/soak.log)"
  log "messages table: $(q "select pg_size_pretty(pg_total_relation_size('queen.messages'));")  live=$(q "select n_live_tup from pg_stat_user_tables where schemaname='queen' and relname='messages';")"
  docker rm -f "$nm" >/dev/null 2>&1
}

MODE="${1:-both}"
if [ "$MODE" = "rust" ] || [ "$MODE" = "both" ]; then
  soak "Rust tuned (zero-copy attr, C++-matched fusion)" queen-hotpath-rust:latest 6661 \
    -e QUEEN_FULL_FEATURES=1 -e QUEEN_GLOBAL_CONCURRENCY=72 \
    -e QUEEN_PUSH_MAX_CONCURRENT=24 -e QUEEN_POP_MAX_CONCURRENT=40 -e QUEEN_ACK_MAX_CONCURRENT=16 \
    -e QUEEN_PUSH_PREFERRED_BATCH_SIZE=300 -e QUEEN_PUSH_MAX_HOLD_MS=20 -e QUEEN_PUSH_MAX_BATCH_SIZE=1000 \
    -e QUEEN_POP_PREFERRED_BATCH_SIZE=40 -e QUEEN_POP_MAX_HOLD_MS=10
fi
if [ "$MODE" = "cpp" ] || [ "$MODE" = "both" ]; then
  sleep 5
  soak "C++ 0.16.0 (retParallel=8)" smartnessai/queen-mq:0.16.0 6634 \
    -e NUM_WORKERS=10 -e DB_POOL_SIZE=50 -e SIDECAR_POOL_SIZE=250 \
    -e RETENTION_BATCH_SIZE=50000 -e RETENTION_INTERVAL=5000 -e RETENTION_PARALLELISM=8 \
    -e QUEEN_CONCURRENCY_MODE=vegas -e QUEEN_PUSH_PREFERRED_BATCH_SIZE=100 -e QUEEN_POP_PREFERRED_BATCH_SIZE=40
fi
log "###### soak5 done ######"
