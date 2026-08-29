#!/bin/bash
# Capacity table: for a cell of N cores, how many tenants of each PLAN fit
# inside the SLO?
#
# Runs on the CELL box. Caps the whole cell (PG + 2 brokers + proxy + balancer)
# with a systemd slice, because Docker's per-container --cpus would hand each of
# the five components its own N cores and "2 cores" would quietly mean ten.
# That is the defect vm-cell.sh found on 2026-07-29 and it invalidates the whole
# table if you get it wrong.
#
# Plans as defined 2026-08-22 (partitions are the binding resource, so they are
# modelled exactly; queue count is not, so each tenant gets ONE queue carrying
# the whole partition budget):
#     free  500 partitions   5 msg/s e2e
#     dev  2000 partitions  50 msg/s e2e
#     pro 10000 partitions  50 msg/s e2e
#
# SECURITY (learned the hard way, 2026-08-22): this script previously published
# Postgres with `-p 5432:5432`, which binds ALL interfaces. On a public IP with
# the password `postgres`, automated ransomware found it ~30 minutes after the
# port opened, dropped the `queen` database and left a ransom note. The VM was
# destroyed. Rules now enforced below:
#   * Postgres gets NO host port. Only containers on the docker network need it;
#     admin access is `docker exec`.
#   * Every other host port binds 127.0.0.1 explicitly, never 0.0.0.0.
#   * The balancer must NEVER be public: the brokers run with JWT off and
#     QUEEN_KV_TRUSTED_PROXY=1, so anything that can reach them can forge
#     x-queen-tenant and read or write any tenant's data.
#   * The password is generated, not a dictionary word.
set -euo pipefail

SLICE=queencell.slice
# Generated per build; never a dictionary word. See SECURITY note in the header.
PGPASS=${PGPASS:-$(head -c 24 /dev/urandom | base64 | tr -d "/+=" | head -c 24)}
CORES=${1:?usage: capacity.sh <cores|0 for uncapped>}

write_slice() {
  local q=""
  [ "$CORES" != "0" ] && q="CPUQuota=$(( CORES * 100 ))%"
  cat >/etc/systemd/system/$SLICE <<EOF
[Unit]
Description=Queen cell CPU budget
[Slice]
$q
EOF
  systemctl daemon-reload
  systemctl start $SLICE
}

run_broker() {
  docker run -d --name "$1" --network cell --cgroup-parent=$SLICE \
    -p 127.0.0.1:"$3":6632 --ulimit nofile=1048576:1048576 \
    -e PORT=6632 -e PG_HOST=cell-pg -e PG_PORT=5432 -e PG_USER=postgres \
    -e PG_PASSWORD="$PGPASS" -e PG_DATABASE=queen \
    -e QUEEN_TENANCY_HEADER=true -e QUEEN_KV_TRUSTED_PROXY=1 \
    -e QUEEN_SYNC_ENABLED=true -e QUEEN_MESH_PORT=6633 \
    -e QUEEN_MESH_PEERS="$2:6633" -e QUEEN_MESH_ADVERTISE_HOST="$1" \
    -e QUEEN_SYNC_SECRET=cell-mesh-secret -e LOG_LEVEL=info \
    ghcr.io/queen-mq/queen:1.1.0 >/dev/null
}

echo "=== rebuilding cell under ${CORES}-core budget ==="
docker rm -f cell-proxy cell-lb cell-broker-a cell-broker-b cell-pg >/dev/null 2>&1 || true
write_slice

# PG memory follows the core budget so the shape stays plausible at each size.
mem=$(( CORES == 0 ? 8 : (CORES < 4 ? 2 : CORES / 2) ))
docker run -d --name cell-pg --network cell --cgroup-parent=$SLICE --shm-size=1g \
  --ulimit nofile=1048576:1048576 \
  -e POSTGRES_PASSWORD="$PGPASS" -e POSTGRES_DB=queen postgres:18 \
  -c shared_buffers=$(( mem * 256 ))MB -c effective_cache_size=$(( mem * 768 ))MB \
  -c max_connections=500 -c work_mem=12MB -c maintenance_work_mem=512MB \
  -c wal_buffers=64MB -c min_wal_size=2GB -c max_wal_size=8GB \
  -c checkpoint_timeout=15min -c checkpoint_completion_target=0.9 \
  -c synchronous_commit=on -c fsync=on -c autovacuum_naptime=10s \
  -c autovacuum_vacuum_scale_factor=0.02 -c random_page_cost=1.1 \
  -c effective_io_concurrency=200 -c track_io_timing=on \
  -c shared_preload_libraries=pg_stat_statements >/dev/null

for _ in $(seq 1 120); do
  docker exec cell-pg psql -U postgres -d queen -qtAc 'SELECT 1' >/dev/null 2>&1 && break
  sleep 1
done
docker exec cell-pg createdb -U postgres queen_proxy 2>/dev/null || true

run_broker cell-broker-a cell-broker-b 6632
run_broker cell-broker-b cell-broker-a 6642
for hp in 6632 6642; do
  for _ in $(seq 1 90); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$hp/health)" = "200" ] && break
    sleep 1
  done
done

docker run -d --name cell-lb --network cell --cgroup-parent=$SLICE \
  -p 127.0.0.1:6630:6630 -p 127.0.0.1:8404:8404 --ulimit nofile=1048576:1048576 \
  -v /root/haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro haproxy:alpine >/dev/null
docker run -d --name cell-proxy --network cell --cgroup-parent=$SLICE \
  -p 127.0.0.1:6711:6711 --ulimit nofile=1048576:1048576 \
  -e QUEEN_PROXY_PORT=6711 -e PXDB_HOST=cell-pg -e PXDB_PORT=5432 \
  -e PXDB_USER=postgres -e PXDB_PASSWORD="$PGPASS" -e PXDB_DB=queen_proxy \
  -e QUEEN_PROXY_ENFORCE=true -e QUEEN_PROXY_SPOOL_DIR=/tmp/spool \
  -e QUEEN_PROXY_JWT_SECRET=cell-bench-secret -e QUEEN_PROXY_RECONCILE_MS=10000 \
  -e LOG_LEVEL=info ghcr.io/queen-mq/queen-proxy:1.1.0 >/dev/null

for _ in $(seq 1 90); do curl -sf http://127.0.0.1:6711/healthz >/dev/null 2>&1 && break; sleep 1; done

# The PG rebuild empties queen_proxy, so the cell row and the tenant fleet must
# be recreated. The cached key files are stale the moment the DB is dropped —
# provision "never rewrites existing indices", so a leftover file would make it
# skip creation and hand the loader keys that no longer authenticate.
docker exec -i cell-pg psql -qtA -U postgres -d queen_proxy <<SQL >/dev/null
INSERT INTO queen_proxy.cells (slug, region, base_url, class, capacity_slots, cell_secret, status)
VALUES ('cell-01','bench','http://cell-lb:6630','shared',100000,'cell-bench-token','active')
ON CONFLICT (slug) DO UPDATE SET base_url = EXCLUDED.base_url, status='active';
SQL

PSQL="docker exec -i cell-pg psql -qtA -v ON_ERROR_STOP=1 -U postgres -d queen_proxy"
rm -f /root/c-free.json /root/c-dev.json /root/c-pro.json
/root/goload -mode provision -tenants ${NFREE:-400} -prefix cf -plan free -cell cell-01 -file /root/c-free.json -psql-cmd "$PSQL" 2>&1 | tail -1
/root/goload -mode provision -tenants ${NDEV:-200}  -prefix cd -plan dev  -cell cell-01 -file /root/c-dev.json  -psql-cmd "$PSQL" 2>&1 | tail -1
/root/goload -mode provision -tenants ${NPRO:-100}  -prefix cp -plan pro  -cell cell-01 -file /root/c-pro.json  -psql-cmd "$PSQL" 2>&1 | tail -1

echo "=== verify the cap actually covers everything ==="
for c in cell-pg cell-broker-a cell-broker-b cell-proxy cell-lb; do
  printf '  %-16s cgroup=%s\n' "$c" "$(docker inspect -f '{{.HostConfig.CgroupParent}}' $c)"
done
printf '  slice quota: %s\n' "$(systemctl show $SLICE -p CPUQuotaPerSecUSec --value 2>/dev/null)"
printf '  proxy  /healthz : '; curl -sf http://127.0.0.1:6711/healthz && echo
printf '  lb -> broker    : '; curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:6630/health
echo "=== cell ready at ${CORES} cores ==="
