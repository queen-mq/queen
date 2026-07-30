#!/usr/bin/env bash
# setup-broker.sh (runs on the BROKER VM) — fresh PG + tuned Rust segments broker.
# Leaves PG + broker running and spawns a background monitor to /tmp/mon.log.
# goload is driven separately from the LOADER VM against the private IP.
set -uo pipefail
NET=qbench; PG=qbench-pg
CIMG="${CIMG:-queen-mq:segments}"; RIMG="${RIMG:-queen:latest}"
Q="${QUEUE:-segbench}"; DEDUP="${DEDUP:-0}"
RPORT="${RPORT:-6682}"
MON_STEP="${MON_STEP:-30}"; MON_N="${MON_N:-14}"
COMMIT_DELAY="${COMMIT_DELAY:-200}"; COMMIT_SIBLINGS="${COMMIT_SIBLINGS:-5}"

log(){ echo "[$(date -u +%FT%TZ)] $*"; }
q(){ docker exec "$PG" psql -U postgres -tAc "$1" </dev/null 2>/dev/null; }

log "recreate Postgres (max_connections=600)"
docker rm -fv "$PG" >/dev/null 2>&1 || true
docker run -d --name "$PG" --network "$NET" --ulimit nofile=65535:65535 --shm-size=2g \
  -e POSTGRES_PASSWORD=postgres postgres:16 \
  -c max_connections=600 -c shared_buffers=24GB -c effective_cache_size=48GB \
  -c maintenance_work_mem=2GB -c work_mem=32MB -c temp_buffers=64MB \
  -c max_worker_processes=24 -c max_parallel_workers=24 -c max_parallel_workers_per_gather=4 \
  -c max_parallel_maintenance_workers=4 -c wal_buffers=128MB -c min_wal_size=8GB -c max_wal_size=96GB \
  -c checkpoint_timeout=15min -c checkpoint_completion_target=0.9 -c synchronous_commit=on -c wal_compression=on \
  -c commit_delay="$COMMIT_DELAY" -c commit_siblings="$COMMIT_SIBLINGS" \
  -c random_page_cost=1.1 -c effective_io_concurrency=200 -c default_statistics_target=200 \
  -c autovacuum_max_workers=4 -c autovacuum_naptime=10s -c autovacuum_vacuum_scale_factor=0.05 \
  -c autovacuum_analyze_scale_factor=0.02 -c autovacuum_vacuum_cost_limit=4000 -c autovacuum_vacuum_cost_delay=0 >/dev/null
for i in $(seq 1 60); do docker exec "$PG" pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done

# Single-binary flow: the Rust broker self-applies schema.sql + procedures at boot
# (advisory-locked, fail-fast). No C++ bootstrap needed.
rn="r$RPORT"; docker rm -fv "$rn" >/dev/null 2>&1
# DASH_PORT publishes the broker on the host for the dashboard (default 6632,
# reachable from the internet at http://<broker-public-ip>:6632).
DASH_PORT="${DASH_PORT:-6632}"
log "start tuned Rust broker :$RPORT (+dashboard :$DASH_PORT) (self-applies schema at boot)"
docker run -d --name "$rn" --network "$NET" --ulimit nofile=65535:65535 -p "$RPORT":6632 -p "$DASH_PORT":6632 \
  -e PG_HOST="$PG" -e PG_PASSWORD=postgres -e PG_USER=postgres -e PG_DATABASE=postgres \
  -e DB_POOL_SIZE="${POOL:-300}" -e QUEEN_V2_ZSTD_LEVEL="${ZSTD:-3}" \
  -e QUEEN_V2_FUSION_SHARDS="${FSHARDS:-16}" -e QUEEN_V2_FUSION_FRAMES="${FFRAMES:-500}" \
  -e QUEEN_V2_FUSION_HOLD_MS="${FHOLD:-30}" -e QUEEN_V2_FUSION_MAX_INFLIGHT="${MAXINFLIGHT:-64}" \
  -e QUEEN_V2_BUNDLE_MAX="${BUNDLEMAX:-32}" \
  -e QUEEN_SEG_PUSH_INIT="${PINIT:-64}" -e QUEEN_SEG_PUSH_MIN="${PMIN:-16}" -e QUEEN_SEG_PUSH_MAX="${PMAX:-256}" \
  -e QUEEN_SEG_POP_INIT="${OINIT:-64}" -e QUEEN_SEG_POP_MIN="${OMIN:-16}" -e QUEEN_SEG_POP_MAX="${OMAX:-256}" \
  -e QUEEN_VEGAS_ALPHA="${VA:-6}" -e QUEEN_VEGAS_BETA="${VB:-12}" \
  "$RIMG" >/dev/null
for i in $(seq 1 90); do curl -sf "http://localhost:$RPORT/status" >/dev/null 2>&1 && break; sleep 1; done
docker logs "$rn" 2>&1 | tail -3

log "configure $Q dedup=$DEDUP (segments engine)"
curl -s -X POST "http://localhost:$RPORT/api/v1/configure" -H 'Content-Type: application/json' \
  -d "{\"queue\":\"$Q\",\"options\":{\"storage\":\"segments\",\"dedupWindowSeconds\":${DEDUP},\"leaseTime\":60,\"retryLimit\":3}}"; echo

# background monitor
cat > /tmp/mon.sh <<'MON'
#!/usr/bin/env bash
PG=qbench-pg; RPORT="__RPORT__"; rn="r$RPORT"; STEP=__STEP__; N=__N__
q(){ docker exec "$PG" psql -U postgres -tAc "$1" </dev/null 2>/dev/null; }
scrape(){ curl -s "http://localhost:$RPORT/metrics" 2>/dev/null | awk -v m="$1" 'index($1,m)==1 {print $2; exit}'; }
echo "t_s,push_req_s,pop_req_s,push_msg_s,pop_msg_s,commit_s,broker_cpu,pg_cpu" > /tmp/mon.csv
echo "  time  | pushReq popReq | push/s  pop/s | bCPU   pgCPU | commit/s | pgWait(active)"
pPR=0;pOR=0;pPM=0;pOM=0;pC=0;first=1
for i in $(seq 1 "$N"); do
  sleep "$STEP"
  PR=$(scrape queen_cluster_push_requests_total);PR=${PR:-0}
  OR=$(scrape queen_cluster_pop_requests_total);OR=${OR:-0}
  PM=$(scrape queen_cluster_push_messages_total);PM=${PM:-0}
  OM=$(scrape queen_cluster_pop_messages_total);OM=${OM:-0}
  C=$(q "select xact_commit from pg_stat_database where datname='postgres';");C=${C:-0}
  bc=$(docker stats --no-stream --format '{{.CPUPerc}}' "$rn" 2>/dev/null|tr -d '%')
  pc=$(docker stats --no-stream --format '{{.CPUPerc}}' "$PG" 2>/dev/null|tr -d '%')
  wait=$(q "select string_agg(wait_event_type||':'||wait_event||'='||c,' ') from (select coalesce(wait_event_type,'CPU') wait_event_type, coalesce(wait_event,'run') wait_event, count(*) c from pg_stat_activity where state='active' and pid<>pg_backend_pid() group by 1,2 order by 3 desc limit 3) t;")
  if [ "$first" = 1 ]; then first=0; else
    awk -v t="$((i*STEP))" -v dpr=$((PR-pPR)) -v dor=$((OR-pOR)) -v dpm=$((PM-pPM)) -v dom=$((OM-pOM)) -v dc=$((C-pC)) -v st="$STEP" \
      -v bc="$bc" -v pc="$pc" -v wt="$wait" 'BEGIN{
      printf "  %-5s | %6.0f %6.0f | %6.0f %6.0f | %4.0f%% %5.0f%% | %7.0f | %s\n", t"s", dpr/st, dor/st, dpm/st, dom/st, bc, pc, dc/st, wt;
      printf "%d,%.0f,%.0f,%.0f,%.0f,%.0f,%s,%s\n", t, dpr/st, dor/st, dpm/st, dom/st, dc/st, bc, pc >> "/tmp/mon.csv" }'
  fi
  pPR=$PR;pOR=$OR;pPM=$PM;pOM=$OM;pC=$C
done
MON
sed -i "s/__RPORT__/$RPORT/; s/__STEP__/$MON_STEP/; s/__N__/$MON_N/" /tmp/mon.sh
chmod +x /tmp/mon.sh
pkill -f /tmp/mon.sh >/dev/null 2>&1 || true   # stop any stale monitor from a prior run
sleep 1
nohup bash /tmp/mon.sh >/tmp/mon.log 2>&1 &
log "monitor started (pid $!) -> /tmp/mon.log"

# dedup is set OFF by goload's own /configure (dedupWindowSeconds=0) — no pinner.
log "READY queue=$Q broker_priv=$(hostname -I | awk '{print $3}'):$RPORT"
