#!/bin/bash
# Queen Cloud cell on one VM: 1 Postgres, 2 brokers, 1 balancer, 1 proxy.
#
# Mirrors the production shape rather than a convenient one:
#   * helm_v1/broker/values.yaml runs `replicas: 2`, so the cell has TWO brokers.
#   * helm_v1/sql/03-bootstrap-proxy.sql points the cell's base_url at a
#     Kubernetes Service, which load-balances across those replicas. On a single
#     VM the faithful equivalent is a small L7 balancer, so nginx stands in for
#     the Service and the proxy addresses the cell through it — exactly one
#     base_url, two brokers behind it, same as prod.
#   * ONE Postgres holds BOTH databases (queen + queen_proxy). Production
#     separates them; a shared cell is the cheap shape and it is the shape whose
#     cost we are trying to measure.
#
# Every container gets nofile=1048576. Docker's default soft limit is 1024, and
# a cell holds ONE PARKED LONG-POLL PER TENANT: at ~900 tenants the proxy died
# with "Too many open files (os error 24)", nginx logged RLIMIT_NOFILE 1024, and
# 130 of 900 queue creations failed with timeouts and 502 upstream-unreachable.
# It reads exactly like a broker capacity ceiling and is nothing of the kind.
#
# Everything joins a user-defined docker network and dials by CONTAINER NAME.
# That is not cosmetic: publishing a port and dialling 127.0.0.1 sends the
# traffic through the userland docker-proxy process, which adds latency and
# bills the CPU outside the container's cgroup — the exact defect vm-cell.sh
# found and fixed on 2026-07-29. Host ports are published for the loader only.
set -euo pipefail

NET=cell
PG=cell-pg
BROKER_A=cell-broker-a
BROKER_B=cell-broker-b
LB=cell-lb
PROXY=cell-proxy

QUEEN_IMAGE=${QUEEN_IMAGE:-ghcr.io/queen-mq/queen:1.1.0}
PROXY_IMAGE=${PROXY_IMAGE:-ghcr.io/queen-mq/queen-proxy:1.1.0}
PG_IMAGE=${PG_IMAGE:-postgres:18}

PGMEM_GB=${PGMEM_GB:-8}
MESH_SECRET=${MESH_SECRET:-cell-mesh-secret}

down() {
  docker rm -f $PROXY $LB $BROKER_A $BROKER_B $PG >/dev/null 2>&1 || true
  docker network rm $NET >/dev/null 2>&1 || true
}

case "${1:-up}" in
down)   down; echo "cell down"; exit 0 ;;
status) ;;
up)     down ;;
esac

if [ "${1:-up}" = "up" ]; then

docker network create $NET >/dev/null 2>&1 || true

# ---- 1. Postgres: one instance, two databases -------------------------------
shared=$(( PGMEM_GB * 1024 / 4 ))
eff=$(( PGMEM_GB * 1024 * 3 / 4 ))
docker run -d --name $PG --network $NET --shm-size=1g -p 5432:5432 \
  --ulimit nofile=1048576:1048576 \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=queen \
  $PG_IMAGE \
  -c shared_buffers=${shared}MB -c effective_cache_size=${eff}MB \
  -c max_connections=500 -c work_mem=12MB -c maintenance_work_mem=512MB \
  -c wal_buffers=64MB -c min_wal_size=2GB -c max_wal_size=8GB \
  -c checkpoint_timeout=15min -c checkpoint_completion_target=0.9 \
  -c synchronous_commit=on -c fsync=on \
  -c autovacuum_naptime=10s -c autovacuum_vacuum_scale_factor=0.02 \
  -c random_page_cost=1.1 -c effective_io_concurrency=200 \
  -c track_io_timing=on -c shared_preload_libraries=pg_stat_statements >/dev/null

for _ in $(seq 1 120); do
  docker exec $PG psql -U postgres -d queen -qtAc 'SELECT 1' >/dev/null 2>&1 && break
  sleep 1
done
has_px=$(docker exec $PG psql -U postgres -qtAc \
  "SELECT count(*) FROM pg_database WHERE datname='queen_proxy'" 2>/dev/null || echo 0)
if [ "${has_px//[[:space:]]/}" != "1" ]; then
  docker exec $PG createdb -U postgres queen_proxy
fi
echo "postgres ready: databases queen + queen_proxy"

# ---- 2. Two brokers on the SAME database, meshed ----------------------------
# QUEEN_KV_TRUSTED_PROXY=1 affirms that a proxy in front sets x-queen-tenant and
# strips the client's. The broker refuses to boot with QUEEN_TENANCY_HEADER=1
# without it, because the header is opaque and validated against nothing. Here
# the affirmation is TRUE: the only route into the brokers from outside this box
# is the proxy on :6711, and the brokers' own host ports are loopback-bound.
#
# The mesh is a wake-up/ownership transport (server/src/config.rs: mesh.rs +
# notify.rs), not replication — the data lives in the shared Postgres. Without
# peers the transport is never bound and the two brokers would not coordinate.
run_broker() {
  local name=$1 peer=$2 hostport=$3
  # Host port bound to LOOPBACK ONLY. The brokers must be reachable from the
  # docker network (the proxy and the balancer) and from the host (health checks
  # and the sampler), but NOT from the VPC — otherwise a client could bypass the
  # proxy and forge x-queen-tenant, and the affirmation below would be a lie.
  docker run -d --name "$name" --network $NET -p 127.0.0.1:"$hostport":6632 \
    --ulimit nofile=1048576:1048576 \
    -e PORT=6632 \
    -e PG_HOST=$PG -e PG_PORT=5432 -e PG_USER=postgres \
    -e PG_PASSWORD=postgres -e PG_DATABASE=queen \
    -e QUEEN_TENANCY_HEADER=true \
    -e QUEEN_KV_TRUSTED_PROXY=1 \
    -e QUEEN_SYNC_ENABLED=true \
    -e QUEEN_MESH_PORT=6633 \
    -e QUEEN_MESH_PEERS="$peer:6633" \
    -e QUEEN_MESH_ADVERTISE_HOST="$name" \
    -e QUEEN_SYNC_SECRET="$MESH_SECRET" \
    -e LOG_LEVEL=info \
    "$QUEEN_IMAGE" >/dev/null
}
run_broker $BROKER_A $BROKER_B 6632
run_broker $BROKER_B $BROKER_A 6642
for hp in 6632 6642; do
  code=000
  for _ in $(seq 1 90); do
    code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$hp/health" 2>/dev/null || echo 000)
    [ "$code" = "200" ] && break
    sleep 1
  done
  echo "broker on :$hp health=$code"
done

# ---- 3. The balancer standing in for the k8s Service -------------------------
cat > /tmp/cell-lb.conf <<NGINX
upstream brokers {
    server $BROKER_A:6632 max_fails=3 fail_timeout=5s;
    server $BROKER_B:6632 max_fails=3 fail_timeout=5s;
    keepalive 256;
}
server {
    listen 6630;
    # Long-poll pops park for up to 30s server-side; a 60s read timeout keeps
    # the balancer from cutting a parked consumer and manufacturing an error
    # the broker never returned.
    proxy_read_timeout 120s;
    proxy_send_timeout 120s;
    location / {
        proxy_pass http://brokers;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_set_header Host \$host;
    }
}
NGINX
docker run -d --name $LB --network $NET -p 6630:6630 \
  --ulimit nofile=1048576:1048576 \
  -v /tmp/cell-lb.conf:/etc/nginx/conf.d/default.conf:ro nginx:alpine >/dev/null
echo "$LB up on :6630 -> $BROKER_A, $BROKER_B"

# ---- 4. The proxy ------------------------------------------------------------
docker run -d --name $PROXY --network $NET -p 6711:6711 \
  --ulimit nofile=1048576:1048576 \
  -e QUEEN_PROXY_PORT=6711 \
  -e PXDB_HOST=$PG -e PXDB_PORT=5432 -e PXDB_USER=postgres \
  -e PXDB_PASSWORD=postgres -e PXDB_DB=queen_proxy \
  -e QUEEN_PROXY_ENFORCE=true \
  -e QUEEN_PROXY_SPOOL_DIR=/tmp/spool \
  -e QUEEN_PROXY_JWT_SECRET=cell-bench-secret \
  -e QUEEN_PROXY_RECONCILE_MS=10000 \
  -e LOG_LEVEL=info \
  "$PROXY_IMAGE" >/dev/null

for _ in $(seq 1 90); do
  curl -sf http://127.0.0.1:6711/healthz >/dev/null 2>&1 && break
  sleep 1
done

# ---- 5. The cell row the proxy forwards through ------------------------------
# base_url points at the BALANCER, not at a broker: one cell, two replicas,
# same indirection production gets from a Service.
docker exec -i $PG psql -qtA -U postgres -d queen_proxy <<SQL >/dev/null
INSERT INTO queen_proxy.cells (slug, region, base_url, class, capacity_slots, cell_secret, status)
VALUES ('cell-01','bench','http://$LB:6630','shared',100000,'cell-bench-token','active')
ON CONFLICT (slug) DO UPDATE SET base_url = EXCLUDED.base_url, status='active';
SQL
echo "cell row registered -> http://$LB:6630"
fi

# ---- status -------------------------------------------------------------------
echo
echo "--- cell components ---"
docker ps --format '{{.Names}}\t{{.Status}}' | grep -E "$PG|$BROKER_A|$BROKER_B|$LB|$PROXY" || true
echo
printf 'proxy  /healthz : '; curl -sf http://127.0.0.1:6711/healthz && echo || echo UNREACHABLE
printf 'lb -> broker    : '; curl -sf -o /dev/null -w '%{http_code}\n' http://127.0.0.1:6630/health || echo UNREACHABLE
echo
echo "--- mesh ---"
for b in $BROKER_A $BROKER_B; do
  printf '%s: ' "$b"
  docker logs "$b" 2>&1 | grep -oE 'mesh_active=[a-z]+|peers=\[[^]]*\]' | tr '\n' ' '
  echo
done
