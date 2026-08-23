#!/bin/bash
# Soak cell — 8 vCPU box, full-quota tenants, multi-day.
#
# SECURITY POSTURE (a bench VM was destroyed by ransomware on 2026-08-22, ~30
# minutes after Postgres was published on 0.0.0.0 with the password "postgres"):
#
#   * Postgres has NO host port. It does not need one — the brokers reach it on
#     the container network. Nothing to scan is stronger than a rare port, and
#     costs nothing. It also listens on a non-default port and uses a generated
#     password and a non-default superuser, as requested.
#   * The brokers publish on 127.0.0.1 only. They run with JWT off and
#     QUEEN_KV_TRUSTED_PROXY=1, so anything that can reach them can forge
#     x-queen-tenant and read or write ANY tenant's data. They must never be
#     reachable from outside the box.
#   * The balancer is loopback-only, for the same reason.
#   * RECONCILE_MS is 60 s, the code's own default (registry.rs
#     RECONCILE_INTERVAL). The previous rig forced 10 s -- SIX TIMES more often
#     -- and every tick calls GET /api/v1/resources/queues, behind which
#     log_queue_stats_all_v1 runs a LATERAL count(*) over queen.log_segments
#     ONCE PER PARTITION. (It is tenant-scoped — an earlier note here wrongly
#     said it had no tenant filter; the cost is the per-partition correlated
#     count, not a missing WHERE.) Work per call = the tenant's whole segment
#     count, so it grows with stored messages: 338 -> 2 204 ms as segments went
#     1M -> 9M on 2026-08-23. Check it in pg_stat_statements before trusting any
#     capacity number.
#
#   * DEFAULT_SUBSCRIPTION_MODE=all for the soak. The default is `new` (the
#     group starts at the TAIL), and goload starts producers and consumers
#     together, so the first ~3 messages per tenant land before the group
#     registers and are never delivered to it. That produced a 27/27 "real LOSS"
#     verdict on the gate run that was not loss at all (backlog 0, segments ==
#     sentOk, gaps at seq 1-3). Over a multi-day soak that startup artifact would
#     sit in the totals and mask genuine loss. Existing groups are immune to the
#     setting and groups here are created once against an empty backlog, so
#     there is no replay risk.
#   * STATS_INTERVAL_MS=60000, up from the 10 000 default. log_refresh_all_stats_v1
#     measured 1 391 ms/call at 757k partitions -- ~14% of a core held
#     continuously at 10 s, ~2.3% at 60 s. queen.stats backs only the status/
#     overview read endpoints; nothing on the data path reads it, and since T2.1
#     the cadence comes from a durable claim row, so this IS the cluster cadence.
#
#   * The proxy publishes on the VPC ADDRESS ONLY, so the loader reaches it and
#     the internet does not. The process hardcodes 0.0.0.0 (server/src/main.rs
#     and proxy/src/main.rs have no bind setting), so Docker's host-IP binding is
#     what actually constrains it.
set -euo pipefail

NET=cell
PG=cell-pg; BROKER_A=cell-broker-a; BROKER_B=cell-broker-b; LB=cell-lb; PROXY=cell-proxy
VPC=${VPC:-10.114.0.2}
PGPORT_INTERNAL=${PGPORT_INTERNAL:-54987}   # non-default, though unpublished
PGUSER=${PGUSER:-queenadm}
PGPASS_FILE=/root/soak/pgpass
# Source-built since 2026-08-23: the hot-list full walk now runs
# log_hotlist_reseed_window_v1 with p_cutoff pinned to '-infinity'. The old
# dedicated statement read every partition in the CELL per ring under the
# generic plan prepare_cached converges to. Not on ghcr — see build-broker.sh.
QUEEN_IMAGE=${QUEEN_IMAGE:-queen:soak}
PROXY_IMAGE=${PROXY_IMAGE:-queen-proxy:soak}   # built from source, see build-proxy.sh
# Size Postgres from the ACTUAL box, never a constant. This defaulted to 8 and
# the soak box has 15.6 GiB, so shared_buffers was 2 GB on a machine that can
# hold twice that -- the config was sized for half the hardware.
MEM_MB=${MEM_MB:-$(free -m | awk '/^Mem:/{print $2}')}
SHARED_MB=$(( MEM_MB / 4 ))            # 25% -- the standard starting point
CACHE_MB=$(( MEM_MB * 70 / 100 ))      # what the planner assumes the OS caches
PROXY_IMAGE=${PROXY_IMAGE:-queen-proxy:soak}   # built from source, see build-proxy.sh
MEM_GB=${MEM_GB:-8}

case "${1:-up}" in
down) docker rm -f $PROXY $LB $BROKER_A $BROKER_B $PG >/dev/null 2>&1 || true
      docker network rm $NET >/dev/null 2>&1 || true; echo "cell down"; exit 0 ;;
esac

docker rm -f $PROXY $LB $BROKER_A $BROKER_B $PG >/dev/null 2>&1 || true
docker network create $NET >/dev/null 2>&1 || true

if [ ! -s "$PGPASS_FILE" ]; then
  head -c 32 /dev/urandom | base64 | tr -d '/+=' | head -c 32 > "$PGPASS_FILE"
  chmod 600 "$PGPASS_FILE"
fi
PGPASS=$(cat "$PGPASS_FILE")

# ---- Postgres: no host port, custom user, custom port, generated password ----
docker run -d --name $PG --network $NET --shm-size=2g --ulimit nofile=1048576:1048576 \
  --restart unless-stopped \
  -e POSTGRES_USER="$PGUSER" -e POSTGRES_PASSWORD="$PGPASS" -e POSTGRES_DB=queen \
  -e PGPORT=$PGPORT_INTERNAL \
  postgres:18 \
  -c port=$PGPORT_INTERNAL \
  -c shared_buffers=${SHARED_MB}MB -c effective_cache_size=${CACHE_MB}MB \
  -c work_mem=16MB -c maintenance_work_mem=1GB -c autovacuum_work_mem=1GB \
  -c max_connections=${PG_MAX_CONN:-700} \
  -c wal_buffers=128MB -c wal_compression=on \
  -c min_wal_size=4GB -c max_wal_size=32GB \
  -c checkpoint_timeout=30min -c checkpoint_completion_target=0.9 \
  -c bgwriter_delay=10ms -c bgwriter_lru_maxpages=1000 -c bgwriter_lru_multiplier=4.0 \
  -c synchronous_commit=on -c fsync=on \
  -c autovacuum=on -c autovacuum_max_workers=6 -c autovacuum_naptime=10s \
  -c autovacuum_vacuum_cost_limit=3000 -c autovacuum_vacuum_cost_delay=2ms \
  -c autovacuum_vacuum_scale_factor=0.02 -c autovacuum_analyze_scale_factor=0.01 \
  -c log_autovacuum_min_duration=1000 -c log_checkpoints=on -c log_lock_waits=on \
  -c random_page_cost=1.1 -c effective_io_concurrency=200 -c maintenance_io_concurrency=200 \
  -c jit=off \
  -c track_io_timing=on -c track_counts=on \
  -c shared_preload_libraries=pg_stat_statements >/dev/null

for _ in $(seq 1 120); do
  docker exec $PG psql -U "$PGUSER" -p $PGPORT_INTERNAL -d queen -qtAc 'SELECT 1' >/dev/null 2>&1 && break
  sleep 1
done
docker exec $PG createdb -U "$PGUSER" -p $PGPORT_INTERNAL queen_proxy 2>/dev/null || true
echo "postgres ready (unpublished, port $PGPORT_INTERNAL, user $PGUSER)"
printf '  tuned for %s MB RAM: shared_buffers=%sMB effective_cache_size=%sMB max_conn=%s\n' \
  "$MEM_MB" "$SHARED_MB" "$CACHE_MB" "${PG_MAX_CONN:-700}"

# ---- brokers: loopback only -------------------------------------------------
run_broker() {
  docker run -d --name "$1" --network $NET -p 127.0.0.1:"$3":6632 \
    --ulimit nofile=1048576:1048576 --restart unless-stopped \
    -e PORT=6632 \
    -e PG_HOST=$PG -e PG_PORT=$PGPORT_INTERNAL -e PG_USER="$PGUSER" \
    -e PG_PASSWORD="$PGPASS" -e PG_DATABASE=queen \
    -e QUEEN_TENANCY_HEADER=true -e QUEEN_KV_TRUSTED_PROXY=1 \
    -e DEFAULT_SUBSCRIPTION_MODE="${SUBSCRIPTION_MODE:-all}" \
    -e STATS_INTERVAL_MS="${STATS_INTERVAL_MS:-60000}" \
    -e DB_POOL_SIZE="${DB_POOL_SIZE:-300}" \
    -e QUEEN_SYNC_ENABLED=true -e QUEEN_MESH_PORT=6633 \
    -e QUEEN_MESH_PEERS="$2:6633" -e QUEEN_MESH_ADVERTISE_HOST="$1" \
    -e QUEEN_SYNC_SECRET="$PGPASS" -e LOG_LEVEL=info \
    "$QUEEN_IMAGE" >/dev/null
}
run_broker $BROKER_A $BROKER_B 6632
run_broker $BROKER_B $BROKER_A 6642
for hp in 6632 6642; do
  for _ in $(seq 1 90); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$hp/health)" = "200" ] && break
    sleep 1
  done
done

# ---- balancer: loopback only, tenant affinity, active health checks ---------
docker run -d --name $LB --network $NET -p 127.0.0.1:6630:6630 -p 127.0.0.1:8404:8404 \
  --ulimit nofile=1048576:1048576 --restart unless-stopped \
  -v /root/haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro haproxy:alpine >/dev/null

# ---- proxy: VPC address only ------------------------------------------------
docker run -d --name $PROXY --network $NET -p "$VPC":6711:6711 \
  --ulimit nofile=1048576:1048576 --restart unless-stopped \
  -e QUEEN_PROXY_PORT=6711 \
  -e PXDB_HOST=$PG -e PXDB_PORT=$PGPORT_INTERNAL -e PXDB_USER="$PGUSER" \
  -e PXDB_PASSWORD="$PGPASS" -e PXDB_DB=queen_proxy \
  -e QUEEN_PROXY_ENFORCE=true -e QUEEN_PROXY_SPOOL_DIR=/tmp/spool \
  -e QUEEN_PROXY_JWT_SECRET="$PGPASS" \
  -e QUEEN_PROXY_RECONCILE_MS="${RECONCILE_MS:-60000}" \
  -e PXDB_TIMEOUT_MS="${PXDB_TIMEOUT_MS:-5000}" \
  -e LOG_LEVEL=info "$PROXY_IMAGE" >/dev/null

for _ in $(seq 1 90); do curl -sf "http://$VPC:6711/healthz" >/dev/null 2>&1 && break; sleep 1; done

docker exec -i $PG psql -qtA -U "$PGUSER" -p $PGPORT_INTERNAL -d queen_proxy <<SQL >/dev/null
INSERT INTO queen_proxy.cells (slug, region, base_url, class, capacity_slots, cell_secret, status)
VALUES ('cell-01','soak','http://$LB:6630','shared',100000,'soak-cell-token','active')
ON CONFLICT (slug) DO UPDATE SET base_url = EXCLUDED.base_url, status='active';
SQL

echo
echo "--- cell ---"
docker ps --format '{{.Names}}\t{{.Status}}' | grep cell-
echo
printf 'proxy  : '; curl -sf "http://$VPC:6711/healthz" && echo
printf 'lb     : %s\n' "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:6630/health)"
echo
echo "--- exposure (only the VPC proxy should be non-loopback) ---"
docker ps --format '{{.Names}}\t{{.Ports}}' | grep cell-
