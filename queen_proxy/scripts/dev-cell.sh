#!/usr/bin/env bash
# Dev cell for queen-proxy: pxdb (:5465) + cell PG (:5466) + broker (:6710) + proxy (:6711).
# Usage: scripts/dev-cell.sh up|down|status|logs
# Reserved elsewhere (do NOT reuse): 5432 5455 5457 5460 5464 6632 6682 6690 6702.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PROXY_DIR="$ROOT/queen_proxy"
RUN_DIR="$PROXY_DIR/.devcell"
mkdir -p "$RUN_DIR"

PXPG=qpx-pg     # proxy state PG  -> host :5465
CELLPG=qcell-pg # cell (broker) PG -> host :5466
BROKER_PORT=6710
PROXY_PORT=6711

wait_pg() { # container
  for _ in $(seq 1 60); do
    if docker exec "$1" pg_isready -U postgres >/dev/null 2>&1; then return 0; fi
    sleep 0.5
  done
  echo "PG $1 not ready" >&2; return 1
}

up() {
  docker rm -f $PXPG $CELLPG >/dev/null 2>&1 || true
  docker run -d --name $PXPG  -p 5465:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=queen_proxy postgres:16 >/dev/null
  docker run -d --name $CELLPG -p 5466:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=queen postgres:16 >/dev/null
  wait_pg $PXPG; wait_pg $CELLPG

  echo "== building broker + proxy"
  (cd "$ROOT/server" && cargo build 2>&1 | tail -2)
  (cd "$PROXY_DIR" && cargo build 2>&1 | tail -2)

  echo "== starting broker :$BROKER_PORT (tenancy header ON)"
  ( PORT=$BROKER_PORT PG_HOST=127.0.0.1 PG_PORT=5466 PG_USER=postgres PG_PASSWORD=postgres \
    PG_DATABASE=queen QUEEN_TENANCY_HEADER=true \
    "$ROOT/server/target/debug/queen-seg" >"$RUN_DIR/broker.log" 2>&1 & echo $! >"$RUN_DIR/broker.pid" )

  echo "== starting proxy :$PROXY_PORT (shadow mode; pxdb-backed)"
  ( QUEEN_PROXY_PORT=$PROXY_PORT PXDB_HOST=127.0.0.1 PXDB_PORT=5465 PXDB_USER=postgres \
    PXDB_PASSWORD=postgres PXDB_DB=queen_proxy \
    QUEEN_PROXY_SPOOL_DIR="$RUN_DIR/spool" \
    "$PROXY_DIR/target/debug/queen-proxy" >"$RUN_DIR/proxy.log" 2>&1 & echo $! >"$RUN_DIR/proxy.pid" )

  sleep 1.5
  if [ -f "$PROXY_DIR/scripts/seed-dev.sql" ]; then
    echo "== seeding pxdb (dev tenant/cluster/key)"
    docker exec -i $PXPG psql -q -U postgres -d queen_proxy <"$PROXY_DIR/scripts/seed-dev.sql" || true
  fi
  status
}

down() {
  for f in broker proxy; do
    [ -f "$RUN_DIR/$f.pid" ] && kill "$(cat "$RUN_DIR/$f.pid")" 2>/dev/null || true
    rm -f "$RUN_DIR/$f.pid"
  done
  docker rm -f $PXPG $CELLPG >/dev/null 2>&1 || true
  echo "dev cell down"
}

status() {
  echo "--- dev cell"
  docker ps --format '{{.Names}} {{.Status}}' | grep -E "$PXPG|$CELLPG" || echo "(no PG containers)"
  for f in broker proxy; do
    if [ -f "$RUN_DIR/$f.pid" ] && kill -0 "$(cat "$RUN_DIR/$f.pid")" 2>/dev/null; then
      echo "$f: up (pid $(cat "$RUN_DIR/$f.pid"))"
    else
      echo "$f: down"
    fi
  done
  curl -sf "http://127.0.0.1:$PROXY_PORT/healthz" >/dev/null 2>&1 && echo "proxy /healthz: ok" || echo "proxy /healthz: no"
  curl -sf "http://127.0.0.1:$BROKER_PORT/health" >/dev/null 2>&1 && echo "broker /health: ok" || echo "broker /health: no"
}

logs() { tail -n 40 "$RUN_DIR/broker.log" "$RUN_DIR/proxy.log"; }

case "${1:-}" in
  up) up ;;
  down) down ;;
  status) status ;;
  logs) logs ;;
  *) echo "usage: $0 up|down|status|logs" >&2; exit 1 ;;
esac
