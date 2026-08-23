#!/bin/bash
# Aggregate e2e p99 across ALL queues per interval.
# Single-queue 5-minute snapshots bounce depending on which phase of the
# 5-minute hot-list full-walk cycle they land in; averaging the same interval
# across 27 independent queues removes that sampling artifact.
cd "${1:-/root/soak/soak}" || exit 1
awk -F, 'FNR>1 && $3=="load" {
  n[$1]++; s[$1]+=$11; if($11>m[$1]) m[$1]=$11; p[$1]+=$5; w[$1]=$2
} END {
  printf "  %-9s %-9s %8s %8s %8s\n","t_sec","utc","mean_p99","max_p99","push/s"
  for (t in n) printf "  %-9d %-9s %8.0f %8.0f %8.0f\n", t, substr(w[t],12,8), s[t]/n[t], m[t], p[t]
}' *-interval.csv | sort -k1 -n
