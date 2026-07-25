#!/usr/bin/env bash
# setup-broker-8gb.sh — free-tier shape (2 vCPU / 8 GB, no swap during test).
# Derived from goload/setup-broker.sh (2026-07-24 enterprise script) but every
# memory knob rescaled for an 8 GB box: shared_buffers 2G (not 16-24G), broker
# dedup cache 256 MB (not 20 GB!), PG pool 64, wal caps sane for a 157 GB disk.
# PG + broker both in Docker so `docker stats` gives clean per-cgroup CPU while
# goload runs as a host process (its CPU stays out of the broker/PG numbers).
set -uo pipefail
NET=qbench; PG=qbench-pg
RIMG="${RIMG:-queen-seg-rust:latest}"
Q="${QUEUE:-segbench}"; DEDUP="${DEDUP:-0}"
RPORT="${RPORT:-6682}"; DASH_PORT="${DASH_PORT:-6632}"
COMMIT_DELAY="${COMMIT_DELAY:-500}"; COMMIT_SIBLINGS="${COMMIT_SIBLINGS:-8}"

# 8 GB budget: shbuf 2G + up to 3 av workers*192M + ~64 backends*~10M + broker
# (~0.5G, dedup 256M) + goload (~0.2G) + OS ~ 5.5-6G peak, ~2G headroom, no swap.
SHBUF="${SHBUF:-2GB}"; EFFCACHE="${EFFCACHE:-4GB}"
MAINTMEM="${MAINTMEM:-192MB}"; WORKMEM="${WORKMEM:-8MB}"
MAXCONN="${MAXCONN:-200}"; AVWORKERS="${AVWORKERS:-3}"
SHM="${SHM:-1g}"
DEDUPMB="${DEDUPMB:-256}"; POOL="${POOL:-64}"

log(){ echo "[$(date -u +%FT%TZ)] $*"; }

docker network create "$NET" >/dev/null 2>&1 || true

log "recreate Postgres 18 (max_connections=$MAXCONN, shared_buffers=$SHBUF, shm=$SHM)"
docker rm -fv "$PG" >/dev/null 2>&1 || true
docker run -d --name "$PG" --network "$NET" --ulimit nofile=65535:65535 --shm-size="$SHM" \
  -e POSTGRES_PASSWORD=postgres postgres:18 \
  -c max_connections="$MAXCONN" -c shared_buffers="$SHBUF" -c effective_cache_size="$EFFCACHE" \
  -c maintenance_work_mem="$MAINTMEM" -c work_mem="$WORKMEM" -c temp_buffers=32MB \
  -c max_worker_processes=8 -c max_parallel_workers=4 -c max_parallel_workers_per_gather=2 \
  -c max_parallel_maintenance_workers=2 -c wal_buffers=64MB -c min_wal_size=512MB -c max_wal_size=4GB \
  -c checkpoint_timeout="${CKPT:-15min}" -c checkpoint_completion_target=0.9 -c synchronous_commit=on \
  -c track_wal_io_timing=on -c wal_compression=lz4 \
  -c commit_delay="$COMMIT_DELAY" -c commit_siblings="$COMMIT_SIBLINGS" \
  -c random_page_cost=1.1 -c effective_io_concurrency=200 -c default_statistics_target=100 \
  -c autovacuum_max_workers="$AVWORKERS" -c autovacuum_naptime=10s -c autovacuum_vacuum_scale_factor=0.05 \
  -c autovacuum_analyze_scale_factor=0.02 -c autovacuum_vacuum_cost_limit=4000 -c autovacuum_vacuum_cost_delay=0 \
  -c shared_preload_libraries=pg_stat_statements -c pg_stat_statements.track=all -c pg_stat_statements.max=1000 >/dev/null
for i in $(seq 1 60); do docker exec "$PG" pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done

rn="r$RPORT"; docker rm -fv "$rn" >/dev/null 2>&1
log "start Rust broker :$RPORT (+dash :$DASH_PORT) dedup_cache=${DEDUPMB}MB pool=$POOL hotlist=${HOTLIST:-1} ackfusion=${ACKFUSION:-1}"
docker run -d --name "$rn" --network "$NET" --ulimit nofile=65535:65535 -p "$RPORT":6632 -p "$DASH_PORT":6632 \
  -e PG_HOST="$PG" -e PG_PASSWORD=postgres -e PG_USER=postgres -e PG_DATABASE=postgres \
  -e QUEEN_DEDUP_CACHE_MB="$DEDUPMB" -e DB_POOL_SIZE="$POOL" -e QUEEN_V2_ZSTD_LEVEL="${ZSTD:-3}" \
  -e QUEEN_V2_FUSION_SHARDS="${FSHARDS:-16}" -e QUEEN_V2_FUSION_FRAMES="${FFRAMES:-500}" \
  -e QUEEN_V2_FUSION_HOLD_MS="${FHOLD:-30}" -e QUEEN_V2_FUSION_MAX_INFLIGHT="${MAXINFLIGHT:-64}" \
  -e QUEEN_V2_BUNDLE_MAX="${BUNDLEMAX:-32}" \
  -e QUEEN_SEG_PUSH_INIT="${PINIT:-64}" -e QUEEN_SEG_PUSH_MIN="${PMIN:-16}" -e QUEEN_SEG_PUSH_MAX="${PMAX:-256}" \
  -e QUEEN_SEG_POP_INIT="${OINIT:-96}" -e QUEEN_SEG_POP_MIN="${OMIN:-64}" -e QUEEN_SEG_POP_MAX="${OMAX:-256}" \
  -e QUEEN_VEGAS_ALPHA="${VA:-6}" -e QUEEN_VEGAS_BETA="${VB:-12}" \
  -e QUEEN_HOTLIST="${HOTLIST:-1}" \
  -e QUEEN_ACK_FUSION="${ACKFUSION:-1}" -e QUEEN_ACK_FUSION_SHARDS="${ACKSHARDS:-4}" \
  "$RIMG" >/dev/null
for i in $(seq 1 90); do curl -sf "http://localhost:$RPORT/status" >/dev/null 2>&1 && break; sleep 1; done
docker logs "$rn" 2>&1 | tail -3

docker exec "$PG" psql -U postgres -qc "CREATE EXTENSION IF NOT EXISTS pg_stat_statements" 2>/dev/null

log "configure $Q dedup=$DEDUP (segments engine)"
curl -sf -X POST "http://localhost:$RPORT/api/v1/configure" -H 'Content-Type: application/json' \
  -d "{\"queue\":\"$Q\",\"options\":{\"storage\":\"segments\",\"dedupWindowSeconds\":${DEDUP},\"leaseTime\":60,\"retryLimit\":3}}" >/dev/null && echo
log "READY broker=127.0.0.1:$RPORT status=$(curl -s http://localhost:$RPORT/status | head -c 120)"
