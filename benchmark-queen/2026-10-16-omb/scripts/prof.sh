#!/usr/bin/env bash
# prof.sh <tag> [seconds] — during a run: per-thread CPU of every queen process (top -H, threads grouped by name),
# /status kafka.consume of every node at the start and the end, and perf on every broker for <seconds>.
# Everything lands in ../runs/<tag>-prof/.
set -u
S=$(cd "$(dirname "$0")" && pwd); H=$(cd "$S/.." && pwd); . "${OMB_HOSTS:-$H/hosts.env}"
TAG=${1:?tag}; D=${2:-20}; OUT=$H/runs/$TAG-prof; mkdir -p "$OUT"
for i in 0 1 2; do $SSH root@${B_PUB[$i]} "curl -s http://${B_PRIV[$i]}:6632/status" > "$OUT/status0-$((i+1)).json" & $SSH root@${B_PUB[$i]} "curl -s http://${B_PRIV[$i]}:6632/metrics/prometheus" > "$OUT/metrics0-$((i+1)).txt" & done; wait
for i in 0 1 2; do
  $SSH root@${B_PUB[$i]} "P=\$(pgrep -x queen); top -H -b -n 2 -d $D -p \$P | awk '/^top -/{n++} n==2 && \$1 ~ /^[0-9]+\$/ {c[\$12]+=\$9; k[\$12]++} END {for (t in c) printf \"%-18s %3d threads %7.1f%%\n\", t, k[t], c[t]}' | sort -k4 -nr | head -25" > "$OUT/threads-$((i+1)).txt" &
  $SSH root@${B_PUB[$i]} "cd /tmp && P=\$(pgrep -x queen) && perf record -F 299 -g -p \$P -o /tmp/perf-$TAG.data -- sleep $D >/dev/null 2>&1; perf report -i /tmp/perf-$TAG.data --no-children --sort sym --stdio 2>/dev/null | grep -E '^ +[0-9]' | head -40" > "$OUT/perf-$((i+1)).txt" &
done; wait
for i in 0 1 2; do $SSH root@${B_PUB[$i]} "curl -s http://${B_PRIV[$i]}:6632/status" > "$OUT/status1-$((i+1)).json" & $SSH root@${B_PUB[$i]} "curl -s http://${B_PRIV[$i]}:6632/metrics/prometheus" > "$OUT/metrics1-$((i+1)).txt" & done; wait
echo "prof $TAG: $(ls "$OUT" | wc -l) files"
