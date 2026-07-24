#!/usr/bin/env bash
# setup-broker.sh (runs on the BROKER VM) — fresh PG + tuned Rust segments broker.
# Ricostruito 2026-07-24 (l'originale è morto con le VM del 23) con: tunable di
# memoria validati contro i 3 OOM del 23 (SHBUF/WORKMEM/MAINTMEM/DEDUPMB),
# pg_stat_statements track=all (decisivo per profilare le plpgsql annidate),
# e QUEEN_HOTLIST passabile. Leaves PG + broker running.
# goload is driven separately from the LOADER VM against the private IP.
set -uo pipefail
NET=qbench; PG=qbench-pg
RIMG="${RIMG:-queen-seg-rust:latest}"
Q="${QUEUE:-segbench}"; DEDUP="${DEDUP:-0}"
RPORT="${RPORT:-6682}"
COMMIT_DELAY="${COMMIT_DELAY:-500}"; COMMIT_SIBLINGS="${COMMIT_SIBLINGS:-8}"

log(){ echo "[$(date -u +%FT%TZ)] $*"; }

log "recreate Postgres (max_connections=600, shared_buffers=${SHBUF:-16GB})"
docker rm -fv "$PG" >/dev/null 2>&1 || true
docker run -d --name "$PG" --network "$NET" --ulimit nofile=65535:65535 --shm-size=2g \
  -e POSTGRES_PASSWORD=postgres postgres:18 \
  -c max_connections=600 -c shared_buffers="${SHBUF:-16GB}" -c effective_cache_size=48GB \
  -c maintenance_work_mem="${MAINTMEM:-512MB}" -c work_mem="${WORKMEM:-12MB}" -c temp_buffers=64MB \
  -c max_worker_processes=24 -c max_parallel_workers=24 -c max_parallel_workers_per_gather=4 \
  -c max_parallel_maintenance_workers=4 -c wal_buffers=128MB -c min_wal_size=8GB -c max_wal_size=96GB \
  -c checkpoint_timeout="${CKPT:-15min}" -c checkpoint_completion_target=0.9 -c synchronous_commit=on \
  -c track_wal_io_timing=on -c wal_compression=lz4 \
  -c commit_delay="$COMMIT_DELAY" -c commit_siblings="$COMMIT_SIBLINGS" \
  -c random_page_cost=1.1 -c effective_io_concurrency=200 -c default_statistics_target=200 \
  -c autovacuum_max_workers=4 -c autovacuum_naptime=10s -c autovacuum_vacuum_scale_factor=0.05 \
  -c autovacuum_analyze_scale_factor=0.02 -c autovacuum_vacuum_cost_limit=4000 -c autovacuum_vacuum_cost_delay=0 \
  -c shared_preload_libraries=pg_stat_statements -c pg_stat_statements.track=all -c pg_stat_statements.max=1000 >/dev/null
for i in $(seq 1 60); do docker exec "$PG" pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done

rn="r$RPORT"; docker rm -fv "$rn" >/dev/null 2>&1
DASH_PORT="${DASH_PORT:-6632}"
log "start tuned Rust broker :$RPORT (dedup cache ${DEDUPMB:-20480}MB, hotlist=${HOTLIST:-0})"
docker run -d --name "$rn" --network "$NET" --ulimit nofile=65535:65535 -p "$RPORT":6632 -p "$DASH_PORT":6632 \
  -e PG_HOST="$PG" -e PG_PASSWORD=postgres -e PG_USER=postgres -e PG_DATABASE=postgres \
  -e QUEEN_DEDUP_CACHE_MB="${DEDUPMB:-20480}" -e DB_POOL_SIZE="${POOL:-300}" -e QUEEN_V2_ZSTD_LEVEL="${ZSTD:-3}" \
  -e QUEEN_V2_FUSION_SHARDS="${FSHARDS:-24}" -e QUEEN_V2_FUSION_FRAMES="${FFRAMES:-500}" \
  -e QUEEN_V2_FUSION_HOLD_MS="${FHOLD:-30}" -e QUEEN_V2_FUSION_MAX_INFLIGHT="${MAXINFLIGHT:-64}" \
  -e QUEEN_V2_BUNDLE_MAX="${BUNDLEMAX:-32}" \
  -e QUEEN_SEG_PUSH_INIT="${PINIT:-64}" -e QUEEN_SEG_PUSH_MIN="${PMIN:-16}" -e QUEEN_SEG_PUSH_MAX="${PMAX:-256}" \
  -e QUEEN_SEG_POP_INIT="${OINIT:-96}" -e QUEEN_SEG_POP_MIN="${OMIN:-64}" -e QUEEN_SEG_POP_MAX="${OMAX:-256}" \
  -e QUEEN_VEGAS_ALPHA="${VA:-6}" -e QUEEN_VEGAS_BETA="${VB:-12}" \
  -e QUEEN_HOTLIST="${HOTLIST:-0}" \
  "$RIMG" >/dev/null
for i in $(seq 1 90); do curl -sf "http://localhost:$RPORT/status" >/dev/null 2>&1 && break; sleep 1; done
docker logs "$rn" 2>&1 | tail -2

docker exec "$PG" psql -U postgres -qc "CREATE EXTENSION IF NOT EXISTS pg_stat_statements" 2>/dev/null

log "configure $Q dedup=$DEDUP (segments engine)"
curl -sf -X POST "http://localhost:$RPORT/api/v1/configure" -H 'Content-Type: application/json' \
  -d "{\"queue\":\"$Q\",\"options\":{\"storage\":\"segments\",\"dedupWindowSeconds\":${DEDUP},\"leaseTime\":60,\"retryLimit\":3}}" >/dev/null && echo
log "READY queue=$Q broker_priv=$(ip -4 -o addr show eth1 | awk '{print $4}' | cut -d/ -f1):$RPORT"
