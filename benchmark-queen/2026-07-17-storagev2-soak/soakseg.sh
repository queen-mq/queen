#!/usr/bin/env bash
# soakseg.sh — storage-v2 segments soak on the NEW engine architecture:
# dedicated seg_push (SEGMENT_PUSH+SEGMENT_ACK) and seg_pop (SEGMENT_POP) engine
# threads instead of everything on the single rest engine; tables moved to the
# queen schema (queen.seg_*). Same bp-100 workload, 2x load, dedup=0, comp=60.
set -uo pipefail
NET=qbench; PG=qbench-pg; GOLOAD=/root/goload
DUR="${DUR:-480}"; STEP="${STEP:-30}"; CRET="${CRET:-60}"; DEDUP="${DEDUP:-0}"
PRODUCERS="${PRODUCERS:-200}"; CONSUMERS="${CONSUMERS:-200}"
IMG="${IMG:-queen-mq:segments}"

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
log "start broker $IMG (dedicated segment engines; auto-inits queen.seg_* schema)"
docker run -d --name "$nm" --network "$NET" --ulimit nofile=65535:65535 -p "$PORT":6632 \
  -e PG_HOST="$PG" -e PG_PASSWORD=postgres -e PG_USER=postgres -e PG_DATABASE=postgres \
  -e NUM_WORKERS="${NW:-16}" -e SIDECAR_POOL_SIZE=32 \
  -e QUEEN_SEG_PUSH_SLOTS="${SEG_PUSH_SLOTS:-64}" -e QUEEN_SEG_POP_SLOTS="${SEG_POP_SLOTS:-64}" \
  -e QUEEN_CONCURRENCY_MODE=static \
  -e QUEEN_SEGMENT_PUSH_MAX_CONCURRENT="${SEG_PUSH_CONC:-48}" \
  -e QUEEN_SEGMENT_POP_MAX_CONCURRENT="${SEG_POP_CONC:-48}" \
  -e QUEEN_SEGMENT_ACK_MAX_CONCURRENT="${SEG_ACK_CONC:-24}" \
  -e QUEEN_V2_FUSION_HOLD_MS="${FHOLD:-15}" -e QUEEN_V2_FUSION_FRAMES="${FFRAMES:-500}" \
  -e QUEEN_V2_ZSTD_LEVEL="${ZSTD:-3}" \
  -e RETENTION_INTERVAL=5000 -e RETENTION_BATCH_SIZE=50000 -e EVICTION_INTERVAL=30000 \
  "$IMG" >/dev/null
for i in $(seq 1 150); do curl -sf "http://localhost:$PORT/api/v1/status" >/dev/null 2>&1 && break; sleep 1; done
sleep 3

Q="seg_$RANDOM"
log "configure $Q storage=segments dedup=$DEDUP comp=$CRET"
curl -s -X POST "http://localhost:$PORT/api/v1/configure" -H 'Content-Type: application/json' \
  -d "{\"queue\":\"$Q\",\"options\":{\"storage\":\"segments\",\"dedupWindowSeconds\":$DEDUP,\"leaseTime\":60,\"retryLimit\":3,\"retentionEnabled\":true,\"retentionSeconds\":7200,\"completedRetentionSeconds\":$CRET}}"; echo
log "queue row: $(q "select name||' storage='||storage from queen.queues where name='$Q';")  seg_queue: $(q "select 'dedup='||dedup_window_seconds from queen.seg_queues where name='$Q';")"

log "=== storage-v2 segments soak (dedicated engines) :$PORT bp-100 ${DUR}s load=${PRODUCERS}x${CONSUMERS} ==="
"$GOLOAD" -url "http://localhost:$PORT" -queue "$Q" \
  -partitions "${PARTITIONS:-100}" -producers "$PRODUCERS" -consumers "$CONSUMERS" -push-batch 100 -pop-batch 500 \
  -pop-partitions 10 -pop-wait -pop-timeout 2000 -payload 256 \
  -duration "$DUR" -report 100 >/tmp/soakseg.log 2>&1 &
LP=$!

( sleep 12; for r in 1 2 3; do q "UPDATE queen.queues SET retention_enabled=true, retention_seconds=7200, completed_retention_seconds=$CRET WHERE name='$Q';" >/dev/null 2>&1; q "UPDATE queen.seg_queues SET dedup_window_seconds=$DEDUP WHERE name='$Q';" >/dev/null 2>&1; sleep 5; done ) &

echo "  time  | segTbl   msgs(sum)    bCPU     pgCPU  commits/s WAL_MB/s"
pC=0; pW=$(q "select pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0');"); pW=${pW:-0}; first=1; n=$((DUR/STEP))
for i in $(seq 1 "$n"); do
  sleep "$STEP"
  C=$(q "select xact_commit from pg_stat_database where datname='postgres';"); C=${C:-0}
  W=$(q "select pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0');"); W=${W:-$pW}
  bcpu=$(docker stats --no-stream --format '{{.CPUPerc}}' "$nm" 2>/dev/null|tr -d '%')
  pgcpu=$(docker stats --no-stream --format '{{.CPUPerc}}' "$PG" 2>/dev/null|tr -d '%')
  segtbl=$(q "select pg_size_pretty(pg_total_relation_size('queen.seg_segments'));")
  msgs=$(q "select coalesce(sum(msg_count),0) from queen.seg_segments;")
  if [ "$first" = "1" ]; then first=0; else
    awk -v t="$((i*STEP/60))m$((i*STEP%60))s" -v st2="$segtbl" -v mg="$msgs" \
      -v bc="$bcpu" -v pc="$pgcpu" -v dc=$((C-pC)) -v st="$STEP" -v wal=$((W-pW)) 'BEGIN{
      printf "  %-5s | %8s %10s  %7s%% %6s%%  %.0f    %.1f\n", t, st2, mg, bc, pc, dc/st, wal/st/1048576 }'
  fi
  pC=$C; pW=$W
done
wait "$LP"
log "final load: $(tail -1 /tmp/soakseg.log)"
log "final seg_segments: $(q "select pg_size_pretty(pg_total_relation_size('queen.seg_segments'));") msgs=$(q "select coalesce(sum(msg_count),0) from queen.seg_segments;")"
log "final seg_dedup: $(q "select pg_size_pretty(pg_total_relation_size('queen.seg_dedup'));") rows=$(q "select count(*) from queen.seg_dedup;")"
docker rm -fv "$nm" >/dev/null 2>&1
log "###### soakseg done ######"
