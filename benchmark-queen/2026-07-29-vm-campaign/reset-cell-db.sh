#!/usr/bin/env bash
# Drop and recreate the broker's database, then restart the broker so it
# re-applies migrations and starts with cold in-memory state.
#
# Every measured point in this campaign starts from this state, so runs are
# independent: no carry-over of retained rows, of a partition's committed
# offset, or of whatever in-memory partition state the previous run left behind.
# Run BEFORE a measured point, never during one.
set -euo pipefail
docker exec cell-pg psql -U postgres -d postgres -qtAX -c "DROP DATABASE IF EXISTS queen WITH (FORCE)" >/dev/null
docker exec cell-pg psql -U postgres -d postgres -qtAX -c "CREATE DATABASE queen" >/dev/null
# the extension lives IN the database, so a drop takes it with it: put it back
# when the cell was built with PGSS=1 (no-op error otherwise, hence 2>/dev/null)
docker exec cell-pg psql -U postgres -d queen -qtAX \
  -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements" >/dev/null 2>&1 || true
systemctl restart queen-broker
for _ in $(seq 1 120); do
  curl -sf http://127.0.0.1:6632/health >/dev/null 2>&1 && break
  sleep 0.5
done
# the proxy caches cluster->cell routing and tenant rows; nothing in pxdb is
# dropped, so it keeps working, but bounce it too when asked for a full cold pair
if [ "${1:-}" = "--with-proxy" ]; then
  systemctl restart queen-proxy
  for _ in $(seq 1 120); do
    curl -sf http://127.0.0.1:6711/healthz >/dev/null 2>&1 && break
    sleep 0.5
  done
fi
echo "[reset] queen db recreated, broker $(systemctl is-active queen-broker), proxy $(systemctl is-active queen-proxy)"
