#!/usr/bin/env bash
# collect.sh — regenerate every task-B table from the raw artifacts and leave
# them next to the runs, so the final phase can pick them up without re-running
# anything. Pure analysis: reads artifacts, starts no load.
set -uo pipefail
G=/root/queen/benchmark-queen/2026-07-29-vm-campaign
S=/root/campaign/summary-taskB.txt
{
  echo "############################################################"
  echo "# TASK B — proxy overhead, free-tier replica, commit cost"
  echo "# generated $(date -u +%FT%TZ) on $(hostname)"
  echo "# fdatasync on this disk: avg 95.5us p50 93us p99 ~396us"
  echo "############################################################"
  echo
  echo "### RIG NOTE — two defects in the cell script were fixed before any measurement:"
  echo "###  1. the CPU/mem cap was per-unit and the PG containers sat in system.slice,"
  echo "###     so '--cell-cpus N' did not cap Postgres at all. The budget is now a"
  echo "###     property of queencell.slice and the containers join it (--cgroup-parent)."
  echo "###  2. broker->PG went via 127.0.0.1:5466, which docker relays through a"
  echo "###     USERLAND docker-proxy process outside the cap. Both now dial the"
  echo "###     container IP over docker0."
  echo
  echo "===================== B1  proxy overhead (cell 4c/8G) ====================="
  python3 $G/b1report.py /root/campaign/B1 2>&1
  echo
  echo "===================== B1 ladder (direct, ceiling search) ================="
  python3 $G/b23report.py /root/campaign/B1 ladder-broker-400 ladder-broker-800 \
      ladder-broker-1600 ladder-broker-2000 ladder-broker-2400 ladder-broker-2800 \
      ladder-broker-3400 ladder-broker-5000 2>&1
  echo
  echo "===================== STALL diagnosis ===================================="
  for r in stall-lease30 stall-lease10; do
    [ -f /root/campaign/STALL/$r.json ] || continue
    python3 $G/b23report.py /root/campaign/STALL $r 2>&1
    python3 $G/stallsum.py /root/campaign/STALL $r 2>&1
    echo
  done
  echo "===================== B2  free-tier replica (cell 2c/8G) ================="
  python3 $G/b23report.py /root/campaign/B2 2>&1
  echo
  echo "===================== B3  commit cost per delivered message =============="
  python3 $G/b23report.py /root/campaign/B3 2>&1
  echo
  for r in b3-ceil-pgss b3-pb-1 b3-pb-10 b3-pb-50 b3-pb-200 b3-ceilpb-10 b3-ceilpb-200 b3-pushonly; do
    [ -f /root/campaign/B3/$r.pgss-after.csv ] || continue
    python3 $G/pgssdiff.py /root/campaign/B3 $r --top 12 2>&1
    echo
  done
} >"$S" 2>&1
echo "wrote $S ($(wc -l <"$S") lines)"
