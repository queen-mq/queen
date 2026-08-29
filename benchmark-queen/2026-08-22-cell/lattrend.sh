#!/bin/bash
# Aggregate e2e latency across ALL queues per interval — p50 AND p99.
# Single-queue snapshots bounce with the checkpoint cycle; averaging the same
# interval across 27 independent queues removes that. p99 is the tail a
# checkpoint write phase produces; p50 is what a typical message experiences,
# and the two tell different stories — report both.
cd "${1:-/root/soak/soak}" || exit 1
awk -F, 'FNR>1 && $3=="load" {
  n[$1]++; s50[$1]+=$10; s99[$1]+=$11
  if($11>m99[$1]) m99[$1]=$11
  if($10>m50[$1]) m50[$1]=$10
  p[$1]+=$5; w[$1]=$2
} END {
  printf "  %-9s %-9s %8s %8s %8s %8s %8s\n","t_sec","utc","p50","p50_max","p99","p99_max","push/s"
  for (t in n) printf "  %-9d %-9s %8.0f %8.0f %8.0f %8.0f %8.0f\n", t, substr(w[t],12,8), s50[t]/n[t], m50[t], s99[t]/n[t], m99[t], p[t]
}' *-interval.csv | sort -k1 -n
