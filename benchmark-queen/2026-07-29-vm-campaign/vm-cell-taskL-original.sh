#!/usr/bin/env bash
# Campaign cell for the 8c/16GB bench VM: pxdb + cell PG + broker + proxy.
#
#   vm-cell.sh up|down|status|logs [--cell-cpus N] [--cell-mem G] [--enforce 0|1]
#
# Two shapes come out of the same script, which is the point:
#   * FREE-TIER REPLICA  --cell-cpus 2 --cell-mem 8   (what a shared free cell is)
#   * FULL MACHINE       --cell-cpus 0                (no cap; find the real ceiling)
# The cap covers PG + broker + proxy together, via a systemd slice, so the load
# generator on the same box cannot borrow the cell's cores and flatter the result.
# The July free-tier numbers excluded the loader by cgroup accounting; this keeps
# that discipline.
set -euo pipefail

ROOT=/root/queen
RUN=/root/cell
mkdir -p "$RUN"

PXPG=cell-pxdb
CELLPG=cell-pg
BROKER_PORT=6632
PROXY_PORT=6711
SLICE=queencell.slice

CELL_CPUS=0        # 0 = uncapped
CELL_MEM=0         # 0 = uncapped (GiB)
ENFORCE=1
while [ $# -gt 0 ]; do
  case "$1" in
    --cell-cpus) CELL_CPUS="$2"; shift 2;;
    --cell-mem)  CELL_MEM="$2";  shift 2;;
    --enforce)   ENFORCE="$2";   shift 2;;
    *) ACTION="$1"; shift;;
  esac
done
ACTION="${ACTION:-up}"

# PG tuning tracks the cell's memory budget, not the host's: a 2c/8G free cell
# must be measured with the settings it would really run, or the ceiling is
# fiction. Values mirror the 2026-07-24 free-tier rig.
pg_args() { # mem_gb
  local m=${1:-8}
  local shared=$(( m * 1024 / 4 ))       # 25% shared_buffers
  local eff=$(( m * 1024 * 3 / 4 ))
  echo "-c shared_buffers=${shared}MB -c effective_cache_size=${eff}MB \
        -c max_connections=400 -c work_mem=8MB -c maintenance_work_mem=512MB \
        -c wal_compression=on -c checkpoint_timeout=15min \
        -c max_wal_size=8GB -c min_wal_size=2GB -c random_page_cost=1.1"
}

wait_pg() { # container db
  for _ in $(seq 1 120); do
    docker exec "$1" psql -U postgres -d "$2" -qtAc 'SELECT 1' >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  echo "PG $1 ($2) not ready" >&2; return 1
}

slice_flags() {
  local f=(--slice="$SLICE")
  [ "$CELL_CPUS" != "0" ] && f+=(-p CPUQuota=$(( CELL_CPUS * 100 ))%)
  [ "$CELL_MEM"  != "0" ] && f+=(-p MemoryMax=${CELL_MEM}G)
  printf '%s\n' "${f[@]}"
}

up() {
  down >/dev/null 2>&1 || true
  local mem=${CELL_MEM:-8}; [ "$mem" = "0" ] && mem=12

  docker run -d --name $PXPG  -p 5465:5432 -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_DB=queen_proxy postgres:18 >/dev/null
  docker run -d --name $CELLPG -p 5466:5432 -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_DB=queen --shm-size=1g postgres:18 $(pg_args "$mem") >/dev/null
  wait_pg $PXPG queen_proxy; wait_pg $CELLPG queen

  mapfile -t SF < <(slice_flags)

  systemd-run --unit=queen-broker "${SF[@]}" \
    -p Environment="PORT=$BROKER_PORT" \
    -p Environment=PG_HOST=127.0.0.1 -p Environment=PG_PORT=5466 \
    -p Environment=PG_USER=postgres -p Environment=PG_PASSWORD=postgres \
    -p Environment=PG_DATABASE=queen \
    -p Environment=QUEEN_TENANCY_HEADER=true \
    -p Environment=LOG_LEVEL=info \
    -p StandardOutput=append:$RUN/broker.log -p StandardError=append:$RUN/broker.log \
    $ROOT/server/target/release/queen-seg >/dev/null

  systemd-run --unit=queen-proxy "${SF[@]}" \
    -p Environment="QUEEN_PROXY_PORT=$PROXY_PORT" \
    -p Environment=PXDB_HOST=127.0.0.1 -p Environment=PXDB_PORT=5465 \
    -p Environment=PXDB_USER=postgres -p Environment=PXDB_PASSWORD=postgres \
    -p Environment=PXDB_DB=queen_proxy \
    -p Environment="QUEEN_PROXY_ENFORCE=$([ "$ENFORCE" = 1 ] && echo true || echo false)" \
    -p Environment=QUEEN_PROXY_SPOOL_DIR=$RUN/spool \
    -p Environment=QUEEN_PROXY_JWT_SECRET=bench-only-secret \
    -p Environment=QUEEN_PROXY_RECONCILE_MS=10000 \
    -p Environment=LOG_LEVEL=info \
    -p StandardOutput=append:$RUN/proxy.log -p StandardError=append:$RUN/proxy.log \
    $ROOT/queen_proxy/target/release/queen-proxy >/dev/null

  for _ in $(seq 1 60); do
    curl -sf "http://127.0.0.1:$PROXY_PORT/healthz" >/dev/null 2>&1 && break
    sleep 0.5
  done

  # The cell row the proxy forwards through. cell_secret is the broker bearer;
  # the broker runs with JWT off here, so any value works and it stays a
  # placeholder rather than pretending to be a secret.
  docker exec -i $PXPG psql -qtA -U postgres -d queen_proxy <<SQL >/dev/null
INSERT INTO queen_proxy.cells (slug, region, base_url, class, capacity_slots, cell_secret, status)
VALUES ('bench','local','http://127.0.0.1:$BROKER_PORT','shared',10000,'bench-cell-token','active')
ON CONFLICT (slug) DO UPDATE SET base_url = EXCLUDED.base_url, status='active';
SQL
  status
}

down() {
  systemctl stop queen-broker queen-proxy 2>/dev/null || true
  systemctl reset-failed queen-broker queen-proxy 2>/dev/null || true
  docker rm -f $PXPG $CELLPG >/dev/null 2>&1 || true
}

status() {
  echo "--- cell (cpus=${CELL_CPUS:-uncapped} mem=${CELL_MEM:-uncapped}G enforce=$ENFORCE)"
  docker ps --format '{{.Names}} {{.Status}}' | grep -E "$PXPG|$CELLPG" || true
  for u in queen-broker queen-proxy; do
    printf '%s: %s\n' "$u" "$(systemctl is-active $u 2>/dev/null)"
  done
  printf 'proxy /healthz: '; curl -sf "http://127.0.0.1:$PROXY_PORT/healthz" || echo unreachable; echo
  printf 'broker /health:  '; curl -sf "http://127.0.0.1:$BROKER_PORT/health" >/dev/null && echo ok || echo unreachable
}

case "$ACTION" in
  up) up;;
  down) down; echo "cell down";;
  status) status;;
  logs) tail -n "${2:-60}" $RUN/broker.log $RUN/proxy.log;;
  *) echo "usage: vm-cell.sh up|down|status|logs [--cell-cpus N] [--cell-mem G] [--enforce 0|1]" >&2; exit 1;;
esac
