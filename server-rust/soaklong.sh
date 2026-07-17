#!/usr/bin/env bash
# soaklong.sh — 12-min sustained soak for the Rust port with (a) pushed fusion and
# (b) PARALLEL retention (RETENTION_PARALLELISM=8), completedRetention=120s so
# retention engages ~2min in. Goal: does the messages table PLATEAU (delete >= ingest)
# like the 0.16 soak, while holding high throughput? Per-30s trajectory.
set -uo pipefail
NET=qbench; PG=qbench-pg; GOLOAD=/root/goload
DUR="${DUR:-480}"; STEP="${STEP:-30}"; CRET="${CRET:-60}"

log(){ echo "[$(date -u +%FT%TZ)] $*"; }
q(){ docker exec "$PG" psql -U postgres -tAc "$1" </dev/null 2>/dev/null; }

pg_up(){
  log "recreate Postgres (soak/April tuning, max_wal_size=96GB)"
  docker rm -fv "$PG" >/dev/null 2>&1 || true
  docker run -d --name "$PG" --network "$NET" --ulimit nofile=65535:65535 --shm-size=1g \
    -e POSTGRES_PASSWORD=postgres postgres:16 \
    -c max_connections=400 -c shared_buffers=24GB -c effective_cache_size=48GB \
    -c maintenance_work_mem=2GB -c work_mem=32MB -c temp_buffers=64MB \
    -c max_worker_processes=20 -c max_parallel_workers=20 -c max_parallel_workers_per_gather=4 \
    -c max_parallel_maintenance_workers=4 -c wal_buffers=128MB -c min_wal_size=8GB -c max_wal_size=96GB \
    -c checkpoint_timeout=15min -c checkpoint_completion_target=0.9 -c synchronous_commit=on -c wal_compression=on \
    -c random_page_cost=1.1 -c effective_io_concurrency=200 -c default_statistics_target=200 \
    -c autovacuum_max_workers=4 -c autovacuum_naptime=10s -c autovacuum_vacuum_scale_factor=0.05 \
    -c autovacuum_analyze_scale_factor=0.02 -c autovacuum_vacuum_cost_limit=4000 -c autovacuum_vacuum_cost_delay=0 >/dev/null
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

pg_up; schema
PORT=6671; nm="b$PORT"; docker rm -f "$nm" >/dev/null 2>&1
docker run -d --name "$nm" --network "$NET" --ulimit nofile=65535:65535 -p "$PORT":6632 \
  -e PG_HOST="$PG" -e PG_PASSWORD=postgres \
  -e QUEEN_FULL_FEATURES=1 -e QUEEN_GLOBAL_CONCURRENCY=72 \
  -e QUEEN_PUSH_MAX_CONCURRENT=24 -e QUEEN_POP_MAX_CONCURRENT=40 -e QUEEN_ACK_MAX_CONCURRENT=16 \
  -e QUEEN_PUSH_PREFERRED_BATCH_SIZE=500 -e QUEEN_PUSH_MAX_HOLD_MS=25 -e QUEEN_PUSH_MAX_BATCH_SIZE=1500 \
  -e QUEEN_POP_PREFERRED_BATCH_SIZE=40 -e QUEEN_POP_MAX_HOLD_MS=10 \
  -e RETENTION_PARALLELISM=8 -e RETENTION_BATCH_SIZE=50000 -e RETENTION_INTERVAL=5000 \
  queen-hotpath-rust:latest >/dev/null
for i in $(seq 1 120); do curl -sf "http://localhost:$PORT/api/v1/status" >/dev/null 2>&1 && break; sleep 1; done

Q="soaklong$RANDOM"
curl -s -X POST "http://localhost:$PORT/api/v1/configure" -H 'Content-Type: application/json' \
  -d "{\"queue\":\"$Q\",\"options\":{\"leaseTime\":60,\"retryLimit\":3,\"retentionEnabled\":true,\"retentionSeconds\":7200,\"completedRetentionSeconds\":$CRET}}" >/dev/null 2>&1
# ensure queue row exists then force retention config (configure may coerce it)
"$GOLOAD" -mode max -url "http://localhost:$PORT" -queue "$Q" -partitions 100 -producers 4 -consumers 0 \
  -push-batch 10 -payload 64 -duration 2 -report 100 >/dev/null 2>&1 || true
q "UPDATE queen.queues SET retention_enabled=true, retention_seconds=7200, completed_retention_seconds=$CRET WHERE name='$Q';" >/dev/null 2>&1 || true
log "queue $Q retention: $(q "select retention_enabled||' comp='||completed_retention_seconds from queen.queues where name='$Q';")"

log "=== Rust soak (fusion500, retParallel=8, completedRet=${CRET}s) :$PORT bp-100 for ${DUR}s ==="
"$GOLOAD" -mode max -url "http://localhost:$PORT" -queue "$Q" \
  -partitions 100 -producers 100 -consumers 100 -push-batch 100 -pop-batch 500 \
  -pop-partitions 10 -pop-wait=true -pop-timeout=2000 -payload 256 \
  -duration "$DUR" -report 100 >/tmp/soaklong.log 2>&1 &
LP=$!

# goload issues its own full-upsert configure at startup (resets completed_retention
# to 300 and retention_seconds to 0); re-apply the intended per-queue retention a
# few seconds after it fires so it sticks for the rest of the run.
( sleep 12; for r in 1 2 3; do q "UPDATE queen.queues SET retention_enabled=true, retention_seconds=7200, completed_retention_seconds=$CRET WHERE name='$Q';" >/dev/null 2>&1; sleep 5; done; \
  log "retention re-applied: $(q "select retention_enabled||' comp='||completed_retention_seconds from queen.queues where name='$Q';")" ) &

echo "  time  | push/s  pop/s | bCPU   pgCPU | msgsTbl  liveTup   deadTup commits/s"
pP=0; pO=0; pC=0; first=1
n=$((DUR/STEP))
for i in $(seq 1 "$n"); do
  sleep "$STEP"
  P=$(scrape "$PORT" queen_cluster_push_messages_total); P=${P:-0}
  O=$(scrape "$PORT" queen_cluster_pop_messages_total);  O=${O:-0}
  C=$(q "select xact_commit from pg_stat_database where datname='postgres';"); C=${C:-0}
  bcpu=$(docker stats --no-stream --format '{{.CPUPerc}}' "$nm" 2>/dev/null|tr -d '%')
  pgcpu=$(docker stats --no-stream --format '{{.CPUPerc}}' "$PG" 2>/dev/null|tr -d '%')
  read -r TBL LIVE DEAD < <(q "select pg_size_pretty(pg_total_relation_size('queen.messages')), coalesce((select n_live_tup from pg_stat_user_tables where schemaname='queen' and relname='messages'),0), coalesce((select n_dead_tup from pg_stat_user_tables where schemaname='queen' and relname='messages'),0);" | tr '|' ' ')
  if [ "$first" = "1" ]; then first=0; else
    awk -v t="$((i*STEP/60))m$((i*STEP%60))s" -v dp=$((P-pP)) -v do_=$((O-pO)) -v dc=$((C-pC)) -v st="$STEP" \
      -v bc="$bcpu" -v pc="$pgcpu" -v tb="$TBL" -v lv="$LIVE" -v dd="$DEAD" 'BEGIN{
      printf "  %-5s | %6.0f %6.0f | %5s%% %6s%% | %7s %9d %9d %.0f\n", t, dp/st, do_/st, bc, pc, tb, lv, dd, dc/st }'
  fi
  pP=$P; pO=$O; pC=$C
done
wait "$LP"
log "final load: $(tail -1 /tmp/soaklong.log)"
log "final msgs table: $(q "select pg_size_pretty(pg_total_relation_size('queen.messages'));") live=$(q "select n_live_tup from pg_stat_user_tables where schemaname='queen' and relname='messages';")"
docker rm -f "$nm" >/dev/null 2>&1
log "###### soaklong done ######"
