#!/bin/bash
# Rebuild the soak cell from zero and provision the full-quota tenant fleet.
# Runs ON the cell. Wipes Postgres — the storage curve is a primary deliverable
# of the soak and must start from an empty database, not on top of a gate run.
#
#   ./soak-reset.sh [nfree] [ndev] [npro]
set -euo pipefail
NFREE=${1:-17}; NDEV=${2:-11}; NPRO=${3:-7}
PGU=queenadm; PGP=54987

# rsync from the Mac carries the source file mode, and these scripts have been
# re-synced mid-campaign several times; a non-executable soak-cell.sh has now
# broken the reset twice. Fix it here rather than remembering to chmod.
chmod +x /root/soak-cell.sh /root/dbtrend.sh /root/sampler.sh /root/spstrend.sh 2>/dev/null || true

echo "=== tearing down and rebuilding the cell ==="
/root/soak-cell.sh down >/dev/null 2>&1 || true
/root/soak-cell.sh up

echo
echo "=== applying plan limits ==="
docker exec -i cell-pg psql -qtA -v ON_ERROR_STOP=1 -U $PGU -p $PGP -d queen_proxy < /root/plans.sql >/dev/null
docker exec -i cell-pg psql -U $PGU -p $PGP -d queen_proxy \
  -c "SELECT code, max_msgs_per_sec, max_queues, max_partitions_per_queue, max_retained_bytes FROM queen_proxy.plans ORDER BY code" | sed 's/^/  /'

# The PG rebuild empties queen_proxy, so the cached key files are stale the
# moment the DB is dropped: provision "never rewrites existing indices", so a
# leftover file makes it skip creation and hand the loader keys that no longer
# authenticate. Delete them before provisioning, every time.
echo
echo "=== provisioning $((NFREE+NDEV+NPRO)) full-quota tenants ==="
PSQL="docker exec -i cell-pg psql -qtA -v ON_ERROR_STOP=1 -U $PGU -p $PGP -d queen_proxy"
mkdir -p /root/soak
rm -f /root/soak/t-free.json /root/soak/t-dev.json /root/soak/t-pro.json
/root/goload -mode provision -tenants "$NFREE" -prefix sf -plan free -cell cell-01 -file /root/soak/t-free.json -psql-cmd "$PSQL" 2>&1 | tail -1
/root/goload -mode provision -tenants "$NDEV"  -prefix sd -plan dev  -cell cell-01 -file /root/soak/t-dev.json  -psql-cmd "$PSQL" 2>&1 | tail -1
/root/goload -mode provision -tenants "$NPRO"  -prefix sp -plan pro  -cell cell-01 -file /root/soak/t-pro.json  -psql-cmd "$PSQL" 2>&1 | tail -1

echo
echo "=== broker settings that matter for this soak ==="
for b in cell-broker-a cell-broker-b; do
  printf '  %-16s %s\n' "$b" "$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' $b | grep -E 'DEFAULT_SUBSCRIPTION_MODE|STATS_INTERVAL_MS' | tr '\n' ' ')"
done
printf '  %-16s %s\n' cell-proxy "$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' cell-proxy | grep -E 'RECONCILE_MS|PXDB_TIMEOUT_MS' | tr '\n' ' ')"

echo
echo "=== resetting pg_stat_statements and starting the storage collector ==="
# The wipe drops the extension with the database. shared_preload_libraries makes
# the COLLECTOR available cluster-wide, but the VIEW and the reset function only
# exist where CREATE EXTENSION has run — without this the soak produces no
# statement attribution at all, which is the whole point of the exercise.
docker exec cell-pg psql -U $PGU -p $PGP -d queen -qtAc \
  "CREATE EXTENSION IF NOT EXISTS pg_stat_statements" >/dev/null
docker exec cell-pg psql -U $PGU -p $PGP -d queen -qtAc "SELECT pg_stat_statements_reset()" >/dev/null
echo "  pg_stat_statements ready ($(docker exec cell-pg psql -U $PGU -p $PGP -d queen -qtAc 'SELECT count(*) FROM pg_stat_statements') rows)"
[ -f /root/dbtrend.pid ] && kill "$(cat /root/dbtrend.pid)" 2>/dev/null || true
mkdir -p /root/samples; rm -f /root/samples/soak-db.csv
setsid nohup /root/dbtrend.sh > /root/samples/soak-db.csv 2>/dev/null </dev/null &
echo $! > /root/dbtrend.pid
echo "  dbtrend pid $(cat /root/dbtrend.pid)"
echo
echo "READY — copy tenant files to the loader, then launch soak-run.sh"
