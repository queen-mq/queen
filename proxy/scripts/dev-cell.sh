#!/usr/bin/env bash
# Dev cell for queen-proxy: pxdb (:5465) + cell PG (:5466) + broker (:6710) + proxy (:6711).
# Usage: scripts/dev-cell.sh up|down|status|logs
# Reserved elsewhere (do NOT reuse): 5432 5455 5457 5460 5464 6632 6682 6690 6702.
#
# `shared.local` is configured as a SHARED host (decision z): on it the cluster
# comes from the credential, not from the Host label, so any cluster's key works
# through it and lands in its own cluster. `QUEEN_PROXY_DEFAULT_CLUSTER=dev` is
# also set below and must NOT absorb it -- that pairing is exactly what
# isolation-smoke.sh section 20 asserts.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PROXY_DIR="$ROOT/proxy"
RUN_DIR="$PROXY_DIR/.devcell"
mkdir -p "$RUN_DIR"

PXPG=qpx-pg     # proxy state PG  -> host :5465
CELLPG=qcell-pg # cell (broker) PG -> host :5466
BROKER_PORT=6710
PROXY_PORT=6711

wait_pg() { # container db
  # pg_isready alone is not enough: the postgres entrypoint runs initdb against a
  # temporary server and then RESTARTS it, and pg_isready answers "ready" during
  # that window. A broker that connects there dies with "error communicating with
  # the server" while applying the schema. Require a real query on the target
  # database instead, which only succeeds once the final server is serving.
  for _ in $(seq 1 60); do
    if docker exec "$1" psql -U postgres -d "$2" -qtAc 'SELECT 1' >/dev/null 2>&1; then return 0; fi
    sleep 0.5
  done
  echo "PG $1 (db $2) not ready" >&2; return 1
}

up() {
  # kill stale processes from a previous run (pid files can drift after
  # manual restarts — kill by binary path too, belt and braces)
  for f in broker proxy; do
    [ -f "$RUN_DIR/$f.pid" ] && kill "$(cat "$RUN_DIR/$f.pid")" 2>/dev/null || true
    rm -f "$RUN_DIR/$f.pid"
  done
  pkill -f "server/target/debug/queen" 2>/dev/null || true
  pkill -f "proxy/target/debug/queen-proxy" 2>/dev/null || true
  sleep 0.5
  docker rm -f $PXPG $CELLPG >/dev/null 2>&1 || true
  docker run -d --name $PXPG  -p 5465:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=queen_proxy postgres:16 >/dev/null
  docker run -d --name $CELLPG -p 5466:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=queen postgres:16 >/dev/null
  wait_pg $PXPG queen_proxy; wait_pg $CELLPG queen

  echo "== building broker + proxy"
  (cd "$ROOT/server" && cargo build 2>&1 | tail -2)
  (cd "$PROXY_DIR" && cargo build 2>&1 | tail -2)

  echo "== starting broker :$BROKER_PORT (tenancy header ON)"
  # RETAINED_BYTES_INTERVAL_MS: the slow lane that fills queen.stats.retained_bytes
  # (028_retained_bytes) runs every TEN MINUTES by default, which is right for a
  # cell and impossible for a smoke run -- the storage quota and every
  # `retainedBytes` assertion read 0 for the whole life of the test and there is
  # no way to tell "not measured yet" from "broken". Five seconds here, and this
  # is a dev cell: the lane is a single aggregate over the tenant's own segments.
  ( PORT=$BROKER_PORT PG_HOST=127.0.0.1 PG_PORT=5466 PG_USER=postgres PG_PASSWORD=postgres \
    PG_DATABASE=queen QUEEN_TENANCY_HEADER=true QUEEN_KV_TRUSTED_PROXY=true \
    RETAINED_BYTES_INTERVAL_MS="${RETAINED_BYTES_INTERVAL_MS:-5000}" \
    "$ROOT/server/target/debug/queen" >"$RUN_DIR/broker.log" 2>&1 & echo $! >"$RUN_DIR/broker.pid" )

  # Shadow by default (proxy default), so the cell mirrors a stock deployment.
  # QUEEN_PROXY_ENFORCE=true dev-cell.sh up flips real 429s on, which is what
  # the rate-limit and SDK-backoff checks need.
  ENFORCE="${QUEEN_PROXY_ENFORCE:-false}"
  # OAuth is off unless you pass credentials, and the sign-in page then shows
  # local login only — that absence is configuration, not a missing feature.
  # Both halves are required (an id without a secret cannot finish the flow):
  #   GOOGLE_CLIENT_ID=… GOOGLE_CLIENT_SECRET=… scripts/dev-cell.sh up
  OAUTH="local-only"
  [ -n "${GOOGLE_CLIENT_ID:-}" ] && [ -n "${GOOGLE_CLIENT_SECRET:-}" ] && OAUTH="google"
  [ -n "${GITHUB_CLIENT_ID:-}" ] && [ -n "${GITHUB_CLIENT_SECRET:-}" ] && OAUTH="$OAUTH+github"
  echo "== starting proxy :$PROXY_PORT (enforce=$ENFORCE; sign-in=$OAUTH; pxdb-backed)"
  ( QUEEN_PROXY_PORT=$PROXY_PORT PXDB_HOST=127.0.0.1 PXDB_PORT=5465 PXDB_USER=postgres \
    PXDB_PASSWORD=postgres PXDB_DB=queen_proxy \
    QUEEN_PROXY_SPOOL_DIR="$RUN_DIR/spool" \
    QUEEN_PROXY_JWT_SECRET="${QUEEN_PROXY_JWT_SECRET:-dev-only-hs256-secret}" \
    QUEEN_PROXY_ENFORCE="$ENFORCE" \
    QUEEN_PROXY_OPERATOR_ENABLED="${QUEEN_PROXY_OPERATOR_ENABLED:-false}" \
    GOOGLE_CLIENT_ID="${GOOGLE_CLIENT_ID:-}" GOOGLE_CLIENT_SECRET="${GOOGLE_CLIENT_SECRET:-}" \
    GITHUB_CLIENT_ID="${GITHUB_CLIENT_ID:-}" GITHUB_CLIENT_SECRET="${GITHUB_CLIENT_SECRET:-}" \
    QUEEN_PROXY_DEFAULT_CLUSTER=dev QUEEN_PROXY_RECONCILE_MS=10000 \
    QUEEN_PROXY_SHARED_HOSTS="${QUEEN_PROXY_SHARED_HOSTS:-shared.local}" \
    "$PROXY_DIR/target/debug/queen-proxy" >"$RUN_DIR/proxy.log" 2>&1 & echo $! >"$RUN_DIR/proxy.pid" )

  sleep 1.5
  if [ -f "$PROXY_DIR/scripts/seed-dev.sql" ]; then
    echo "== seeding pxdb (dev tenant/cluster/key)"
    docker exec -i $PXPG psql -q -U postgres -d queen_proxy <"$PROXY_DIR/scripts/seed-dev.sql" || true
  fi
  grant_kv
  status
}

# The CELL-side grant for the kv / timers / ephemeral families.
#
# Two gates stand in front of those routes and seed-dev.sql only opens the first
# one. The proxy checks the PLAN (queen_proxy.plans.features) and lets the call
# through; the broker then checks its OWN grant, and with QUEEN_TENANCY_HEADER
# on -- which this cell sets -- the ABSENCE of a queen.kv_quota row for the
# tenant is a denial, not a default (quota.rs `require_grant` -> NotGranted ->
# 403 feature_gated, config.rs says the same in full). So a dev cell seeded on a
# plan that grants kv and timers still answered 403 on every KV and timer route,
# and the two dashboard pages rendered "not enabled for this cluster" forever:
# empty by construction, with nothing anywhere saying which of the two gates was
# shut.
#
# The tenant is READ, never hard-coded: queen_proxy.clusters.broker_tenant_uuid
# is assigned by a column default (migrations/002_functions.sql) and is a fresh
# uuid on every pxdb, so a literal here would grant a tenant that does not exist
# on this cell and look exactly like no grant at all.
#
# Generous dev limits -- this is a laptop cell, and a cap that fires during a
# demo teaches nothing about the product. The broker re-reads the table every
# QUEEN_KV_QUOTA_REFRESH_MS (30s), so the grant lands within half a minute even
# though the broker is already up by the time this runs.
grant_kv() {
  local tenant
  tenant="$(docker exec $PXPG psql -U postgres -d queen_proxy -qtAc \
    "SELECT broker_tenant_uuid FROM queen_proxy.clusters WHERE slug = 'dev'" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ -z "$tenant" ]; then
    echo "== no cluster 'dev' in pxdb; skipping the cell-side kv/timers grant" >&2
    return 0
  fi
  echo "== granting kv/timers on the cell for cluster dev (tenant $tenant)"
  docker exec -i $CELLPG psql -q -U postgres -d queen -v tenant="$tenant" <<'SQL' || true
INSERT INTO queen.kv_quota (tenant_id, enabled, max_rows, max_bytes,
                            max_timers, max_timer_horizon_s,
                            max_reads_per_sec, max_writes_per_sec)
VALUES (:'tenant', TRUE, 100000, 104857600, 10000, 7776000, 200, 200)
ON CONFLICT (tenant_id) DO UPDATE
   SET enabled             = EXCLUDED.enabled,
       max_rows            = EXCLUDED.max_rows,
       max_bytes           = EXCLUDED.max_bytes,
       max_timers          = EXCLUDED.max_timers,
       max_timer_horizon_s = EXCLUDED.max_timer_horizon_s,
       max_reads_per_sec   = EXCLUDED.max_reads_per_sec,
       max_writes_per_sec  = EXCLUDED.max_writes_per_sec,
       updated_at          = now();
SQL
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
