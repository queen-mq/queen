#!/usr/bin/env bash
# soakv2.sh — same bp-100 soak, but against the storage-v2 "segments" engine
# (C++ broker built from storage-v2-slice). Broker auto-inits the q2 schema at
# boot; the queue is configured with storage='segments'. Data lives in
# q2.segments (one zstd blob per K messages); retention via q2.retention_sweep_v1.
set -uo pipefail
NET=qbench; PG=qbench-pg; GOLOAD=/root/goload
DUR="${DUR:-480}"; STEP="${STEP:-30}"; CRET="${CRET:-60}"
IMG="${IMG:-queen-mq:segments}"; STORAGE="${STORAGE:-segments}"

log(){ echo "[$(date -u +%FT%TZ)] $*"; }
q(){ docker exec "$PG" psql -U postgres -tAc "$1" </dev/null 2>/dev/null; }

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

PORT=6681; nm="b$PORT"; docker rm -fv "$nm" >/dev/null 2>&1
log "start C++ broker $IMG (auto-inits schema incl. q2)"
docker run -d --name "$nm" --network "$NET" --ulimit nofile=65535:65535 -p "$PORT":6632 \
  -e PG_HOST="$PG" -e PG_PASSWORD=postgres -e PG_USER=postgres -e PG_DATABASE=postgres \
  -e NUM_WORKERS="${NW:-16}" -e DB_POOL_SIZE=100 -e SIDECAR_POOL_SIZE=80 \
  -e QUEEN_PUSH_SLOTS=6 -e QUEEN_POP_SLOTS=6 -e QUEEN_REST_SLOTS=88 \
  -e QUEEN_CONCURRENCY_MODE=static -e QUEEN_CUSTOM_MAX_CONCURRENT="${CUSTOM_CONC:-64}" \
  -e QUEEN_V2_FUSION_HOLD_MS="${FHOLD:-15}" -e QUEEN_V2_FUSION_FRAMES="${FFRAMES:-500}" \
  -e RETENTION_INTERVAL=5000 -e RETENTION_BATCH_SIZE=50000 -e RETENTION_PARALLELISM=8 \
  -e EVICTION_INTERVAL=30000 \
  "$IMG" >/dev/null
for i in $(seq 1 150); do curl -sf "http://localhost:$PORT/api/v1/status" >/dev/null 2>&1 && break; sleep 1; done
sleep 3

Q="soakv2_$RANDOM"
log "configure $Q storage=$STORAGE + retention(comp=$CRET)"
curl -s -X POST "http://localhost:$PORT/api/v1/configure" -H 'Content-Type: application/json' \
  -d "{\"queue\":\"$Q\",\"options\":{\"storage\":\"$STORAGE\",\"dedupWindowSeconds\":${DEDUP:-0},\"leaseTime\":60,\"retryLimit\":3,\"retentionEnabled\":true,\"retentionSeconds\":7200,\"completedRetentionSeconds\":$CRET}}"; echo
log "queue row: $(q "select name||' storage='||storage||' retEnabled='||retention_enabled||' comp='||completed_retention_seconds from queen.queues where name='$Q';")"

log "=== storage-v2 soak ($STORAGE) :$PORT bp-100 for ${DUR}s ==="
"$GOLOAD" -mode max -url "http://localhost:$PORT" -queue "$Q" \
  -partitions 100 -producers "${PRODUCERS:-200}" -consumers "${CONSUMERS:-200}" -push-batch 100 -pop-batch 500 \
  -pop-partitions 10 -pop-wait=true -pop-timeout=2000 -payload 256 \
  -duration "$DUR" -report 100 >/tmp/soakv2.log 2>&1 &
LP=$!

# goload's configure (omits storage) preserves 'segments'; re-apply small comp so
# retention engages early.
( sleep 12; for r in 1 2 3; do q "UPDATE queen.queues SET retention_enabled=true, retention_seconds=7200, completed_retention_seconds=$CRET WHERE name='$Q';" >/dev/null 2>&1; q "UPDATE q2.queues SET dedup_window_seconds=${DEDUP:-0} WHERE name='$Q';" >/dev/null 2>&1; sleep 5; done; \
  log "retention re-applied: $(q "select storage||' comp='||completed_retention_seconds from queen.queues where name='$Q';") dedup=$(q "select dedup_window_seconds from q2.queues where name='$Q';")" ) &

echo "  time  | push/s  pop/s | bCPU    pgCPU | segTbl   dedupTbl  msgs(sum)   segs commits/s"
pP=0; pO=0; pC=0; first=1; n=$((DUR/STEP))
scrape(){ curl -s "http://localhost:$PORT/metrics/prometheus" 2>/dev/null | awk -v m="$1" 'index($1,m)==1 {print $2; exit}'; }
for i in $(seq 1 "$n"); do
  sleep "$STEP"
  P=$(scrape queen_cluster_push_messages_total); P=${P:-0}
  O=$(scrape queen_cluster_pop_messages_total);  O=${O:-0}
  C=$(q "select xact_commit from pg_stat_database where datname='postgres';"); C=${C:-0}
  bcpu=$(docker stats --no-stream --format '{{.CPUPerc}}' "$nm" 2>/dev/null|tr -d '%')
  pgcpu=$(docker stats --no-stream --format '{{.CPUPerc}}' "$PG" 2>/dev/null|tr -d '%')
  segtbl=$(q "select pg_size_pretty(pg_total_relation_size('q2.segments'));")
  deduptbl=$(q "select pg_size_pretty(pg_total_relation_size('q2.dedup'));")
  msgs=$(q "select coalesce(sum(msg_count),0) from q2.segments;")
  segs=$(q "select count(*) from q2.segments;")
  if [ "$first" = "1" ]; then first=0; else
    awk -v t="$((i*STEP/60))m$((i*STEP%60))s" -v dp=$((P-pP)) -v do_=$((O-pO)) -v dc=$((C-pC)) -v st="$STEP" \
      -v bc="$bcpu" -v pc="$pgcpu" -v st2="$segtbl" -v dt="$deduptbl" -v mg="$msgs" -v sg="$segs" 'BEGIN{
      printf "  %-5s | %6.0f %6.0f | %5s%% %6s%% | %8s %8s %10s %6s %.0f\n", t, dp/st, do_/st, bc, pc, st2, dt, mg, sg, dc/st }'
  fi
  pP=$P; pO=$O; pC=$C
done
wait "$LP"
log "final load: $(tail -1 /tmp/soakv2.log)"
log "final q2.segments: $(q "select pg_size_pretty(pg_total_relation_size('q2.segments'));") msgs=$(q "select coalesce(sum(msg_count),0) from q2.segments;") segs=$(q "select count(*) from q2.segments;")"
log "final q2.dedup: $(q "select pg_size_pretty(pg_total_relation_size('q2.dedup'));") rows=$(q "select count(*) from q2.dedup;")"
docker rm -fv "$nm" >/dev/null 2>&1
log "###### soakv2 done ######"
