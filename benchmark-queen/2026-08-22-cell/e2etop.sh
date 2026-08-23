#!/bin/bash
# Per-queue e2e latency snapshot across every loader process.
# Shipped as a FILE, never generated through a nested heredoc over ssh: that
# escaping is what produced header-only output three times in this campaign.
# NOTE the line carries TWO p99 fields (e2e and pushRTT) — anchor on the label.
cd "${1:-/root/soak/gate}" || exit 1
printf '%-10s %7s %8s %8s %8s %6s\n' queue offered e2e_p50 e2e_p99 push_p99 lag
for f in *-q*.log; do
  [ -f "$f" ] || continue
  L=$(grep 'load ]' "$f" | tail -1)
  [ -z "$L" ] && continue
  OF=$(echo "$L" | sed -nE 's/.*offered= *([0-9]+).*/\1/p')
  E50=$(echo "$L" | sed -nE 's/.*e2e p50= *([0-9.]+).*/\1/p')
  E99=$(echo "$L" | sed -nE 's/.*e2e p50= *[0-9.]+ +p99= *([0-9.]+).*/\1/p')
  P99=$(echo "$L" | sed -nE 's/.*pushRTT p50= *[0-9.]+ +p99= *([0-9.]+).*/\1/p')
  LG=$(echo "$L" | sed -nE 's/.*lag=([0-9]+).*/\1/p')
  printf '%-10s %7s %8s %8s %8s %6s\n' "${f%.log}" "$OF" "$E50" "$E99" "$P99" "$LG"
done | sort -k4 -gr
