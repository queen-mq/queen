#!/usr/bin/env bash
# brokerside.sh — broker+PG on this box only; goload runs on a SEPARATE loader VM.
# Sets up a segments queue and monitors segTbl/CPU/WAL for DUR; no local loader,
# so all 32 cores are available to broker+PG.
set -uo pipefail
NET=qbench; PG=qbench-pg
DUR="${DUR:-360}"; STEP="${STEP:-30}"; CRET="${CRET:-60}"; DEDUP="${DEDUP:-0}"
Q="${Q:-segbench}"; PORT="${PORT:-6681}"; IMG="${IMG:-queen-mq:segments}"
log(){ echo "[$(date -u +%FT%TZ)] $*"; }
q(){ docker exec "$PG" psql -U postgres -tAc "$1" </dev/null 2>/dev/null; }

log "recreate Postgres (soak/April tuning, max_wal_size=96GB)"
docker rm -fv "$PG" >/dev/null 2>&1 || true
docker run -d --name "$PG" --network "$NET" --ulimit nofile=65535:65535 --shm-size=1g \
  -e POSTGRES_PASSWORD=postgres postgres:16 \
  -c max_connections=500 -c shared_buffers=24GB -c effective_cache_size=48GB \
  -c maintenance_work_mem=2GB -c work_mem=32MB -c temp_buffers=64MB \
  -c max_worker_processes=24 -c max_parallel_workers=24 -c max_parallel_workers_per_gather=4 \
  -c max_parallel_maintenance_workers=4 -c wal_buffers=128MB -c min_wal_size=8GB -c max_wal_size=96GB \
  -c checkpoint_timeout=15min -c checkpoint_completion_target=0.9 -c synchronous_commit=on -c wal_compression=on \
  -c random_page_cost=1.1 -c effective_io_concurrency=200 -c default_statistics_target=200 \
  -c autovacuum_max_workers=6 -c autovacuum_naptime=10s -c autovacuum_vacuum_scale_factor=0.05 \
  -c autovacuum_analyze_scale_factor=0.02 -c autovacuum_vacuum_cost_limit=8000 -c autovacuum_vacuum_cost_delay=0 >/dev/null
for i in $(seq 1 60); do docker exec "$PG" pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done

nm="b$PORT"; docker rm -fv "$nm" >/dev/null 2>&1
log "start broker $IMG (goload runs off-box; all cores for broker+PG)"
docker run -d --name "$nm" --network "$NET" --ulimit nofile=65535:65535 -p "$PORT":6632 \
  -e PG_HOST="$PG" -e PG_PASSWORD=postgres -e PG_USER=postgres -e PG_DATABASE=postgres \
  -e NUM_WORKERS="${NW:-24}" -e SIDECAR_POOL_SIZE=32 \
  -e QUEEN_SEG_PUSH_SLOTS="${SEG_PUSH_SLOTS:-80}" -e QUEEN_SEG_POP_SLOTS="${SEG_POP_SLOTS:-80}" \
  -e QUEEN_CONCURRENCY_MODE=static \
  -e QUEEN_SEGMENT_PUSH_MAX_CONCURRENT="${SEG_PUSH_CONC:-64}" \
  -e QUEEN_SEGMENT_POP_MAX_CONCURRENT="${SEG_POP_CONC:-64}" \
  -e QUEEN_SEGMENT_ACK_MAX_CONCURRENT="${SEG_ACK_CONC:-32}" \
  -e QUEEN_V2_FUSION_HOLD_MS="${FHOLD:-15}" -e QUEEN_V2_FUSION_FRAMES="${FFRAMES:-1000}" \
  -e QUEEN_V2_ZSTD_LEVEL="${ZSTD:-3}" \
  -e RETENTION_INTERVAL=5000 -e RETENTION_BATCH_SIZE=50000 -e EVICTION_INTERVAL=30000 \
  "$IMG" >/dev/null
for i in $(seq 1 150); do curl -sf "http://localhost:$PORT/api/v1/status" >/dev/null 2>&1 && break; sleep 1; done
sleep 2

log "configure $Q storage=segments dedup=$DEDUP comp=$CRET"
curl -s -X POST "http://localhost:$PORT/api/v1/configure" -H 'Content-Type: application/json' \
  -d "{\"queue\":\"$Q\",\"options\":{\"storage\":\"segments\",\"dedupWindowSeconds\":$DEDUP,\"leaseTime\":60,\"retryLimit\":3,\"retentionEnabled\":true,\"retentionSeconds\":7200,\"completedRetentionSeconds\":$CRET}}" >/dev/null
log "READY queue=$Q on :$PORT storage=$(q "select storage from queen.queues where name='$Q';")"

# goload's own configure (from the loader) resets comp/dedup; re-apply for a while.
( for r in $(seq 1 20); do q "UPDATE queen.queues SET retention_enabled=true, retention_seconds=7200, completed_retention_seconds=$CRET WHERE name='$Q';" >/dev/null 2>&1; q "UPDATE queen.seg_queues SET dedup_window_seconds=$DEDUP WHERE name='$Q';" >/dev/null 2>&1; sleep 5; done ) &

echo "  time  | segTbl   msgs(sum)    bCPU     pgCPU  hostIdle% commits/s WAL_MB/s"
pC=0; pW=$(q "select pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0');"); pW=${pW:-0}; first=1; n=$((DUR/STEP))
for i in $(seq 1 "$n"); do
  sleep "$STEP"
  C=$(q "select xact_commit from pg_stat_database where datname='postgres';"); C=${C:-0}
  W=$(q "select pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0');"); W=${W:-$pW}
  bcpu=$(docker stats --no-stream --format '{{.CPUPerc}}' "$nm" 2>/dev/null|tr -d '%')
  pgcpu=$(docker stats --no-stream --format '{{.CPUPerc}}' "$PG" 2>/dev/null|tr -d '%')
  idle=$(top -bn1 | awk '/Cpu\(s\)/{print $8}')
  segtbl=$(q "select pg_size_pretty(pg_total_relation_size('queen.seg_segments'));")
  msgs=$(q "select coalesce(sum(msg_count),0) from queen.seg_segments;")
  if [ "$first" = "1" ]; then first=0; else
    awk -v t="$((i*STEP/60))m$((i*STEP%60))s" -v st2="$segtbl" -v mg="$msgs" -v bc="$bcpu" -v pc="$pgcpu" \
      -v idl="$idle" -v dc=$((C-pC)) -v st="$STEP" -v wal=$((W-pW)) 'BEGIN{
      printf "  %-5s | %8s %10s  %7s%% %6s%%  %6s   %.0f    %.1f\n", t, st2, mg, bc, pc, idl, dc/st, wal/st/1048576 }'
  fi
  pC=$C; pW=$W
done
log "monitor done (containers left up for inspection)"
