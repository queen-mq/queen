#!/bin/bash
# Gate matrix, corrected: a TERMINAL must always declare a budget (Gate 422s a
# budget-less terminal — "a queue with extra steps"). So the "ceiling" runs use
# a cap far above what the loader can push (10M/10s) rather than no budget.
set -uo pipefail
Q=46.101.193.166; L=209.38.206.19; QPRIV=10.114.0.2
B="$(dirname "$0")"; OUT="$B/out"
SSH="ssh -o BatchMode=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=10"
for cfg in "g1-1hop-ceiling:-hops 1 -cap 500000 -period 10 -lease 1 -pace-batch 50000 -graph gk1" \
           "g2-2hop-ceiling:-hops 2 -cap 500000 -period 10 -lease 1 -pace-batch 50000 -mid-cap 0 -graph gk2" \
           "g3-1hop-cap200s:-hops 1 -cap 2000 -period 10 -lease 5 -pace-batch 1000 -graph gk3" \
           "g4-2hop-cap200s:-hops 2 -cap 2000 -period 10 -lease 5 -pace-batch 1000 -mid-cap 3000 -graph gk4"; do
  name="gate-${cfg%%:*}"; args="${cfg#*:}"
  echo; echo "=== $name ($args) ==="
  $SSH root@$Q "[ -f /root/sampler.pid ] && kill \$(cat /root/sampler.pid) 2>/dev/null
psql -h 127.0.0.1 -U postgres -d queen -qc 'SELECT pg_stat_statements_reset();' >/dev/null
rm -f /root/$name.csv
setsid nohup bash /root/bench-sampler.sh /root/$name.csv 200 </dev/null >/dev/null 2>&1 &
echo \$! > /root/sampler.pid" >/dev/null 2>&1
  $SSH root@$L "/root/gateload -gate http://$QPRIV:8788 -app bench -duration 100 \
      -pushers 32 -consumers 8 -batch 100 $args" 2>&1 | tee "$OUT/$name.load.txt" | grep -E "declared|\[1|final" | tail -10
  $SSH root@$Q "[ -f /root/sampler.pid ] && kill \$(cat /root/sampler.pid) 2>/dev/null
docker stats gate --no-stream --format 'gate: {{.CPUPerc}} cpu {{.MemUsage}}'" 2>/dev/null
  scp -o BatchMode=yes -q root@$Q:/root/$name.csv "$OUT/$name.csv" 2>/dev/null
  python3 "$B/summarize.py" "$OUT/$name.csv" 2>/dev/null | grep -E "PG CPU|Queen CPU|TOTAL"
done
echo; echo "=== GATE MATRIX DONE ==="
