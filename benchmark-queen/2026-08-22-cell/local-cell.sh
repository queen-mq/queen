#!/bin/bash
# Native cell on the Mac: Homebrew Postgres + the Rust broker + the Rust proxy,
# no Docker, no virtualised disk.
#
# WHAT THIS RIG IS FOR — plan SHAPE, not capacity:
#   * Do the plan limits express the application a tenant is entitled to run?
#   * Does the limiter bind where it should and stay silent where it shouldn't?
#   * Does correctness hold (0 missing) at the shapes we intend to sell?
#   * Does the active-fraction knob behave like production (hot working set,
#     large cold tail)?
#
# WHAT IT IS NOT FOR — tenants-per-core. Three reasons, all structural:
#   1. fdatasync here is 18 us/op against the bench VM's 90 us. Queen is
#      commit-bound, so local numbers come out OPTIMISTIC by roughly 5x on the
#      one axis that matters most.
#   2. macOS has no cgroups. TOKIO_WORKER_THREADS and QUEEN_PROXY_WORKER_THREADS
#      size the runtimes, but nothing CAPS total CPU, and Postgres backends are
#      processes that will use whatever is free. "4 cores" here is a shape, not
#      a budget.
#   3. The load generator shares the machine with the cell, which voids the
#      SPEC 5.1 rule that a run is invalid unless the loader was demonstrably
#      not the bottleneck.
#
# SECURITY: everything binds 127.0.0.1 and nothing is published. On 2026-08-22 a
# bench VM was destroyed by ransomware within ~30 minutes of Postgres being
# exposed on 0.0.0.0 with a weak password. Local is not an excuse.
set -euo pipefail

# macOS + PG18: without a valid locale in the ENVIRONMENT, the postmaster trips
# "postmaster became multithreaded during startup" and refuses to run — a macOS
# library spawns a thread during locale resolution. This must be exported for
# the SERVER, not just for initdb.
export LANG=C LC_ALL=C

# Generated, not a dictionary word. Host auth is scram even on loopback, so every
# TCP client needs it; the local socket stays trust so we can set it in the first
# place. Exported so psql/createdb never prompt and hang a non-interactive run.
PGPASS=${PGPASS:-$(head -c 24 /dev/urandom | base64 | tr -d '/+=' | head -c 24)}
export PGPASSWORD="$PGPASS"

ROOT=${ROOT:-/private/tmp/claude-502/-Users-alice-Work-queen/c440cf99-b13b-4314-af90-4276616ebefd/scratchpad/localcell}
PGBIN=/opt/homebrew/opt/postgresql@18/bin
PGDATA=$ROOT/pgdata
PGPORT=${PGPORT:-5470}
BROKER_PORT=${BROKER_PORT:-6640}
PROXY_PORT=${PROXY_PORT:-6741}
CELL_THREADS=${CELL_THREADS:-4}     # broker tokio workers ~ the "4 core" shape
PROXY_THREADS=${PROXY_THREADS:-2}
QUEEN_BIN=${QUEEN_BIN:-/Users/alice/Work/queen/server/target/release/queen}
PROXY_BIN=${PROXY_BIN:-/Users/alice/Work/queen/proxy/target/release/queen-proxy}

case "${1:-up}" in
down)
  [ -f "$ROOT/broker.pid" ] && kill "$(cat "$ROOT/broker.pid")" 2>/dev/null || true
  [ -f "$ROOT/proxy.pid" ]  && kill "$(cat "$ROOT/proxy.pid")"  2>/dev/null || true
  "$PGBIN/pg_ctl" -D "$PGDATA" -m fast stop >/dev/null 2>&1 || true
  echo "local cell down"; exit 0 ;;
esac

mkdir -p "$ROOT"

# ---- Postgres ---------------------------------------------------------------
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  echo "=== initdb ==="
  # LANG/LC_* are often unset or non-POSIX under a non-login shell on macOS and
  # initdb refuses rather than guessing; pin the locale explicitly.
  "$PGBIN/initdb" -D "$PGDATA" -U postgres --locale=C --encoding=UTF8 \
    --auth-local=trust --auth-host=scram-sha-256 >/dev/null
  cat >> "$PGDATA/postgresql.conf" <<CONF

# Bench profile. listen_addresses is LOOPBACK ONLY and is not negotiable:
# an exposed Postgres is what cost us a VM on 2026-08-22.
listen_addresses = '127.0.0.1'
port = $PGPORT
shared_buffers = 2GB
effective_cache_size = 6GB
work_mem = 12MB
maintenance_work_mem = 512MB
wal_buffers = 64MB
min_wal_size = 2GB
max_wal_size = 8GB
checkpoint_timeout = 15min
checkpoint_completion_target = 0.9
synchronous_commit = on
fsync = on
max_connections = 500
autovacuum_naptime = 10s
autovacuum_vacuum_scale_factor = 0.02
random_page_cost = 1.1
track_io_timing = on
shared_preload_libraries = 'pg_stat_statements'
CONF
fi

if ! "$PGBIN/pg_ctl" -D "$PGDATA" status >/dev/null 2>&1; then
  "$PGBIN/pg_ctl" -D "$PGDATA" -l "$ROOT/pg.log" -o "-p $PGPORT" start >/dev/null
  sleep 2
fi
# Set/refresh the password over the LOCAL socket (trust), so the TCP path works.
"$PGBIN/psql" -h /tmp -p "$PGPORT" -U postgres -qtAc \
  "ALTER USER postgres PASSWORD '$PGPASS'" >/dev/null 2>&1 || true
for _ in $(seq 1 60); do
  "$PGBIN/psql" -h 127.0.0.1 -p "$PGPORT" -U postgres -qtAc 'SELECT 1' >/dev/null 2>&1 && break
  sleep 1
done
for db in queen queen_proxy; do
  "$PGBIN/psql" -h 127.0.0.1 -p "$PGPORT" -U postgres -qtAc \
    "SELECT 1 FROM pg_database WHERE datname='$db'" | grep -q 1 || \
    "$PGBIN/createdb" -h 127.0.0.1 -p "$PGPORT" -U postgres "$db"
done
echo "postgres up on 127.0.0.1:$PGPORT (queen + queen_proxy)"
echo "$PGPASS" > "$ROOT/pgpass"; chmod 600 "$ROOT/pgpass"

# ---- broker -----------------------------------------------------------------
# TOKIO_WORKER_THREADS is honoured natively by the bare #[tokio::main] in
# server/src/main.rs. It sizes the async runtime; it does not cap CPU.
TOKIO_WORKER_THREADS=$CELL_THREADS \
PORT=$BROKER_PORT \
PG_HOST=127.0.0.1 PG_PORT=$PGPORT PG_USER=postgres PG_PASSWORD="$PGPASS" PG_DATABASE=queen \
QUEEN_TENANCY_HEADER=true QUEEN_KV_TRUSTED_PROXY=1 \
LOG_LEVEL=info \
nohup "$QUEEN_BIN" > "$ROOT/broker.log" 2>&1 &
echo $! > "$ROOT/broker.pid"

for _ in $(seq 1 90); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$BROKER_PORT/health" 2>/dev/null)" = "200" ] && break
  sleep 1
done

# ---- proxy ------------------------------------------------------------------
QUEEN_PROXY_WORKER_THREADS=$PROXY_THREADS \
QUEEN_PROXY_PORT=$PROXY_PORT \
PXDB_HOST=127.0.0.1 PXDB_PORT=$PGPORT PXDB_USER=postgres PXDB_PASSWORD="$PGPASS" PXDB_DB=queen_proxy \
QUEEN_PROXY_ENFORCE=true \
QUEEN_PROXY_SPOOL_DIR=$ROOT/spool \
QUEEN_PROXY_JWT_SECRET=local-bench-secret \
QUEEN_PROXY_RECONCILE_MS=10000 \
LOG_LEVEL=info \
nohup "$PROXY_BIN" > "$ROOT/proxy.log" 2>&1 &
echo $! > "$ROOT/proxy.pid"

for _ in $(seq 1 90); do
  curl -sf "http://127.0.0.1:$PROXY_PORT/healthz" >/dev/null 2>&1 && break
  sleep 1
done

# ---- cell row ---------------------------------------------------------------
"$PGBIN/psql" -h 127.0.0.1 -p "$PGPORT" -U postgres -d queen_proxy -qtA >/dev/null <<SQL
INSERT INTO queen_proxy.cells (slug, region, base_url, class, capacity_slots, cell_secret, status)
VALUES ('cell-local','local','http://127.0.0.1:$BROKER_PORT','shared',100000,'local-bench-token','active')
ON CONFLICT (slug) DO UPDATE SET base_url = EXCLUDED.base_url, status='active';
SQL

echo
echo "--- local cell ---"
printf '  postgres : 127.0.0.1:%s\n' "$PGPORT"
printf '  broker   : 127.0.0.1:%s  health=%s  (tokio workers=%s)\n' "$BROKER_PORT" \
  "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$BROKER_PORT/health)" "$CELL_THREADS"
printf '  proxy    : 127.0.0.1:%s  ' "$PROXY_PORT"; curl -sf "http://127.0.0.1:$PROXY_PORT/healthz" && echo
echo "  nothing bound beyond loopback:"
lsof -nP -iTCP -sTCP:LISTEN 2>/dev/null | awk -v a="$PGPORT" -v b="$BROKER_PORT" -v c="$PROXY_PORT" \
  '$9 ~ ":"a"$" || $9 ~ ":"b"$" || $9 ~ ":"c"$" {printf "    %s %s\n", $1, $9}'
