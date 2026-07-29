#!/usr/bin/env bash
# phase3.sh <ceiling-rate> <sub-ceiling-rate> <hotlist-ab-rate>
#
#   1. rebuild the free-tier cell with pg_stat_statements preloaded (PGSS=1) so
#      B3 can ATTRIBUTE commits to push/pop/ack instead of inferring them;
#      re-seed the bench plan and re-provision, because up() recreates both DBs
#   2. B3 — commits per delivered message + the pop-batch curve (mandated)
#   3. HOTLIST A/B — the stall mechanism (bonus, runs last on purpose)
set -uo pipefail
G=/root/queen/benchmark-queen/2026-07-29-vm-campaign
CEIL=${1:?}; SUB=${2:?}; ABRATE=${3:?}

echo "########## 1. rebuild 2c/8G cell with PGSS=1  $(date -u +%T)"
PGSS=1 bash /root/vm-cell.sh up --cell-cpus 2 --cell-mem 8 2>&1 | tail -10
if [ "$(cat /sys/fs/cgroup/queencell.slice/cpu.max)" != "200000 100000" ]; then
  echo "ABORT: slice not capped at 2 cores"; exit 1
fi
bash $G/seed-bench-plan.sh >/dev/null
mkdir -p /root/campaign/B3
rm -f /root/campaign/B3/tenants.json
$G/goload/goload -mode provision -tenants 4 -prefix camp -plan bench -cell bench \
  -file /root/campaign/B3/tenants.json 2>&1 | tail -2
export PGPASSWORD=postgres
psql -h "$(cat /root/cell/cellpg.ip)" -p 5432 -U postgres -d queen -qtAX \
  -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements" 2>&1 | tail -1
psql -h "$(cat /root/cell/cellpg.ip)" -p 5432 -U postgres -d queen -qtAX \
  -c "SELECT 'pg_stat_statements installed: '||count(*) FROM pg_extension WHERE extname='pg_stat_statements'"

echo "########## 2. B3  $(date -u +%T)"
bash $G/b3-commits.sh "$CEIL" "$SUB"

echo "########## 3. HOTLIST A/B  $(date -u +%T)"
cp /root/campaign/B3/tenants.json /root/campaign/STALL/tenants.json
bash $G/hotlist-ab.sh /root/campaign/STALL "$ABRATE"

echo "########## phase3 done $(date -u +%T)"
