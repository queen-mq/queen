#!/usr/bin/env bash
# soakseg-rust.sh — soak the RUST segments broker (queen-seg-rust) on a fresh PG.
# Flow: fresh Postgres -> boot C++ segments broker once (auto-inits queen.seg_*
# schema) + /configure the queue (storage=segments, dedup=0) -> stop C++ ->
# start Rust broker on same PG -> goload bp-100 soak -> monitor -> teardown.
set -uo pipefail
NET=qbench; PG=qbench-pg; GOLOAD=/root/goload
DUR="${DUR:-300}"; STEP="${STEP:-30}"
CIMG="${CIMG:-queen-mq:segments}"; RIMG="${RIMG:-queen-seg-rust:latest}"
DEDUP="${DEDUP:-0}"

log(){ echo "[$(date -u +%FT%TZ)] $*"; }
q(){ docker exec "$PG" psql -U postgres -tAc "$1" </dev/null 2>/dev/null; }

log "recreate Postgres (soak tuning, max_wal_size=96GB)"
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

# --- schema init + queue configure via the C++ broker (transient) ---
CPORT=6681; cn="b$CPORT"; docker rm -fv "$cn" >/dev/null 2>&1
log "boot C++ broker (schema init incl. queen.seg_*)"
docker run -d --name "$cn" --network "$NET" --ulimit nofile=65535:65535 -p "$CPORT":6632 \
  -e PG_HOST="$PG" -e PG_PASSWORD=postgres -e PG_USER=postgres -e PG_DATABASE=postgres \
  -e NUM_WORKERS=4 -e DB_POOL_SIZE=20 -e SIDECAR_POOL_SIZE=10 "$CIMG" >/dev/null
for i in $(seq 1 150); do curl -sf "http://localhost:$CPORT/api/v1/status" >/dev/null 2>&1 && break; sleep 1; done

Q="soakseg_$RANDOM"
log "configure $Q storage=segments dedup=$DEDUP"
curl -s -X POST "http://localhost:$CPORT/api/v1/configure" -H 'Content-Type: application/json' \
  -d "{\"queue\":\"$Q\",\"options\":{\"storage\":\"segments\",\"dedupWindowSeconds\":${DEDUP},\"leaseTime\":60,\"retryLimit\":3}}"; echo
log "queue row: $(q "select name||' storage='||storage from queen.queues where name='$Q';")"
log "seg queue: $(q "select name||' dedup='||dedup_window_seconds from queen.seg_queues where name='$Q';")"
log "stop C++ broker (Rust runs solo)"
docker rm -fv "$cn" >/dev/null 2>&1

# --- Rust segments broker ---
RPORT=6682; rn="r$RPORT"; docker rm -fv "$rn" >/dev/null 2>&1
log "start Rust broker $RIMG :$RPORT"
docker run -d --name "$rn" --network "$NET" --ulimit nofile=65535:65535 -p "$RPORT":6632 \
  -e PG_HOST="$PG" -e PG_PASSWORD=postgres -e PG_USER=postgres -e PG_DATABASE=postgres \
  -e DB_POOL_SIZE="${POOL:-160}" -e QUEEN_V2_ZSTD_LEVEL="${ZSTD:-3}" \
  -e QUEEN_V2_FUSION_SHARDS="${FSHARDS:-8}" -e QUEEN_V2_FUSION_FRAMES="${FFRAMES:-500}" \
  -e QUEEN_V2_FUSION_HOLD_MS="${FHOLD:-15}" \
  -e QUEEN_SEG_PUSH_INIT="${PINIT:-32}" -e QUEEN_SEG_PUSH_MIN=8 -e QUEEN_SEG_PUSH_MAX="${PMAX:-128}" \
  -e QUEEN_SEG_POP_INIT="${OINIT:-32}" -e QUEEN_SEG_POP_MIN=8 -e QUEEN_SEG_POP_MAX="${OMAX:-128}" \
  -e QUEEN_VEGAS_ALPHA="${VA:-4}" -e QUEEN_VEGAS_BETA="${VB:-8}" \
  "$RIMG" >/dev/null
for i in $(seq 1 60); do curl -sf "http://localhost:$RPORT/status" >/dev/null 2>&1 && break; sleep 1; done
log "rust broker: $(curl -s http://localhost:$RPORT/status)"
docker logs "$rn" 2>&1 | tail -2

log "=== RUST storage-v2 soak :$RPORT bp-100 for ${DUR}s (dedup=$DEDUP) ==="
"$GOLOAD" -url "http://localhost:$RPORT" -queue "$Q" \
  -partitions 100 -producers "${PRODUCERS:-200}" -consumers "${CONSUMERS:-200}" -push-batch 100 -pop-batch 500 \
  -pop-partitions 10 -pop-wait=true -pop-timeout=2000 -payload 256 \
  -duration "$DUR" -report 100 >/tmp/soakseg-rust.log 2>&1 &
LP=$!

echo "  time  | push/s  pop/s | bCPU    pgCPU | segTbl    msgs(sum)    segs  commits/s  vegasPop"
pP=0; pO=0; pC=0; first=1; n=$((DUR/STEP))
scrape(){ curl -s "http://localhost:$RPORT/metrics" 2>/dev/null | awk -v m="$1" 'index($1,m)==1 {print $2; exit}'; }
for i in $(seq 1 "$n"); do
  sleep "$STEP"
  P=$(scrape queen_cluster_push_messages_total); P=${P:-0}
  O=$(scrape queen_cluster_pop_messages_total);  O=${O:-0}
  V=$(scrape queen_seg_pop_vegas_limit); V=${V:-0}
  C=$(q "select xact_commit from pg_stat_database where datname='postgres';"); C=${C:-0}
  bcpu=$(docker stats --no-stream --format '{{.CPUPerc}}' "$rn" 2>/dev/null|tr -d '%')
  pgcpu=$(docker stats --no-stream --format '{{.CPUPerc}}' "$PG" 2>/dev/null|tr -d '%')
  segtbl=$(q "select pg_size_pretty(pg_total_relation_size('queen.seg_segments'));")
  msgs=$(q "select coalesce(sum(msg_count),0) from queen.seg_segments;")
  segs=$(q "select count(*) from queen.seg_segments;")
  if [ "$first" = "1" ]; then first=0; else
    awk -v t="$((i*STEP/60))m$((i*STEP%60))s" -v dp=$((P-pP)) -v do_=$((O-pO)) -v dc=$((C-pC)) -v st="$STEP" \
      -v bc="$bcpu" -v pc="$pgcpu" -v st2="$segtbl" -v mg="$msgs" -v sg="$segs" -v vg="$V" 'BEGIN{
      printf "  %-5s | %6.0f %6.0f | %5s%% %6s%% | %8s %10s %6s %8.0f %8s\n", t, dp/st, do_/st, bc, pc, st2, mg, sg, dc/st, vg }'
  fi
  pP=$P; pO=$O; pC=$C
done
wait "$LP"
log "final load: $(tail -3 /tmp/soakseg-rust.log)"
log "final seg_segments: $(q "select pg_size_pretty(pg_total_relation_size('queen.seg_segments'));") msgs=$(q "select coalesce(sum(msg_count),0) from queen.seg_segments;") segs=$(q "select count(*) from queen.seg_segments;")"
log "rust metrics tail:"; curl -s "http://localhost:$RPORT/metrics" 2>/dev/null | grep -E 'fusion_items_per_batch|batch_rtt|requests_total|messages_total|vegas' | head -20
docker rm -fv "$rn" >/dev/null 2>&1
log "###### soakseg-rust done ######"
