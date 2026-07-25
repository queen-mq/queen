#!/usr/bin/env bash
# runone.sh <label> <goload args...> — co-located run: start vmmon sampler, run
# goload to completion, stop sampler, dump raw. broker/PG CPU come from the
# sampler (per-cgroup), goload CPU is tracked separately so it never pollutes them.
set -uo pipefail
LABEL="$1"; shift
OUT=/root/bench/out; mkdir -p "$OUT"
GL=/root/bench/goload-linux-amd64-fresh
nohup bash /root/bench/vmmon.sh "$OUT/$LABEL.csv" 5 400 > "$OUT/$LABEL.mon" 2>&1 &
MON=$!
sleep 1
echo "### run=$LABEL args: $* ($(date -u +%FT%TZ))"
"$GL" "$@" 2>&1 | tee "$OUT/$LABEL.out"
sleep 2; kill "$MON" 2>/dev/null; pkill -f "vmmon.sh $OUT/$LABEL.csv" 2>/dev/null
echo "=== sampler ($LABEL) — steady-state window ==="
tail -24 "$OUT/$LABEL.mon"
echo "=== broker /status + queue count ==="
curl -s http://127.0.0.1:6682/status 2>/dev/null | head -c 300; echo
