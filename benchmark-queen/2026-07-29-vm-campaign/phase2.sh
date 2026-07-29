#!/usr/bin/env bash
# phase2.sh — everything that must run after B1, in order, on an idle box.
#
#   1. STALL DIAG   (still on the 4c cell, because that is where the stall was
#      first measured): reproduce the partition delivery stall while sampling
#      the consumer row once a second, at leaseTime=30s and leaseTime=10s. If
#      the dark period tracks the lease, the lease filter in
#      043_log_pop.sql:558-569 is the mechanism; if it stays ~30s, it is the
#      hardcoded 30s empty-scan re-verification throttle at :608.
#   2. RESHAPE to the free-tier cell (2 cores / 8 GiB covering PG + pxdb +
#      broker + proxy), re-seed the bench plan and re-provision tenants —
#      vm-cell.sh up recreates both databases from scratch.
#   3. B2 ladder.
set -uo pipefail
G=/root/queen/benchmark-queen/2026-07-29-vm-campaign

echo "########## 1. stall diagnosis (4c cell) $(date -u +%T)"
mkdir -p /root/campaign/STALL
cp /root/campaign/B1/tenants.json /root/campaign/STALL/tenants.json
bash $G/stall-diag.sh /root/campaign/STALL stall-lease30 2800 30
bash $G/stall-diag.sh /root/campaign/STALL stall-lease10 2800 10

echo "########## 2. reshape to the free-tier cell $(date -u +%T)"
bash /root/vm-cell.sh up --cell-cpus 2 --cell-mem 8 2>&1 | tail -12
echo "--- cap actually in force:"
cat /sys/fs/cgroup/queencell.slice/cpu.max
if [ "$(cat /sys/fs/cgroup/queencell.slice/cpu.max)" != "200000 100000" ]; then
  echo "ABORT: slice is not capped at 2 cores"; exit 1
fi
bash $G/seed-bench-plan.sh >/dev/null
mkdir -p /root/campaign/B2
rm -f /root/campaign/B2/tenants.json
$G/goload/goload -mode provision -tenants 4 -prefix camp -plan bench -cell bench \
  -file /root/campaign/B2/tenants.json 2>&1 | tail -3
curl -s localhost:6711/healthz; echo

echo "########## 3. B2 ladder $(date -u +%T)"
bash $G/b2-freetier.sh
echo "########## phase2 done $(date -u +%T)"
