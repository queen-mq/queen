#!/bin/bash
# Pass 3 — Gate 0.1.5 (relay: one runner per source admitted partition).
# Partition sweep at ceiling plus two paced runs near the new relay rate.
# Consumers stay at the original 8 ON PURPOSE: the cons32 rerun (gatec.sh)
# showed heavy pop pressure halves the relay by contending on the same
# partitions, so e2e [final] numbers here are consumer-masked and the relay
# rate is derived from the stage depths printed after each run:
#   admission = pushed - entry.push ; relay = admission - entry.admitted
# Duplicate flags: Go's flag package takes the LAST occurrence.
set -uo pipefail
Q=46.101.193.166; L=209.38.206.19; QPRIV=10.114.0.2
B="$(dirname "$0")"; OUT="$B/out"; mkdir -p "$OUT"
SSH="ssh -o BatchMode=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=10"
for cfg in "p1-1hop-parts1:-hops 1 -cap 500000 -period 10 -lease 1 -pace-batch 50000 -partitions 1 -graph gp1" \
           "p2-1hop-parts4:-hops 1 -cap 500000 -period 10 -lease 1 -pace-batch 50000 -partitions 4 -graph gp2" \
           "p3-1hop-parts32:-hops 1 -cap 500000 -period 10 -lease 1 -pace-batch 50000 -partitions 32 -graph gp3" \
           "p4-2hop-parts32:-hops 2 -cap 500000 -period 10 -lease 1 -pace-batch 50000 -mid-cap 0 -partitions 32 -graph gp4" \
           "p5-1hop-paced2000:-hops 1 -cap 25000 -period 10 -lease 1 -pace-batch 2500 -rate 2000 -graph gp5" \
           "p6-1hop-paced2500:-hops 1 -cap 30000 -period 10 -lease 1 -pace-batch 3000 -rate 2500 -graph gp6"; do
  name="gate-${cfg%%:*}"; args="${cfg#*:}"
  echo; echo "=== $name ($args) ==="
  $SSH root@$Q "[ -f /root/sampler.pid ] && kill \$(cat /root/sampler.pid) 2>/dev/null
psql -h 127.0.0.1 -U postgres -d queen -qc 'SELECT pg_stat_statements_reset();' >/dev/null
rm -f /root/$name.csv
setsid nohup bash /root/bench-sampler.sh /root/$name.csv 200 </dev/null >/dev/null 2>&1 &
echo \$! > /root/sampler.pid" >/dev/null 2>&1
  $SSH root@$L "/root/gateload -gate http://$QPRIV:8788 -app bench -duration 100 \
      -pushers 32 -consumers 8 -batch 100 $args" 2>&1 | tee "$OUT/$name.load.txt" | grep -E "declared|\[final|declare failed" | tail -8
  $SSH root@$Q "[ -f /root/sampler.pid ] && kill \$(cat /root/sampler.pid) 2>/dev/null
docker stats gate --no-stream --format 'gate: {{.CPUPerc}} cpu {{.MemUsage}}'" 2>/dev/null
  g="${args##* }"
  $SSH root@$Q "for q in gate.bench.$g.entry.push gate.bench.$g.entry.admitted.default gate.bench.$g.mid.admitted.default gate.bench.$g.term.admitted.default; do printf '%s ' \$q; curl -s \"http://127.0.0.1:6632/api/v1/resources/queues/\$q/depth\" | python3 -c 'import json,sys; print(json.load(sys.stdin)[\"pending\"])' 2>/dev/null || echo -; done"
  scp -o BatchMode=yes -q root@$Q:/root/$name.csv "$OUT/$name.csv"
  python3 "$B/summarize.py" "$OUT/$name.csv" | grep -E "PG CPU|Queen CPU|TOTAL|top waits" || true
done
echo; echo "=== PASS 3 DONE ==="
