#!/bin/bash
# Ceiling rerun with the consumer side unmasked. The g1/g2 rerun on Gate 0.1.5
# showed the relay moving 2.7-2.8k items/s while gateload's 8 consumers popped
# only ~1.2k/s: the [final] admitted/s was measuring the LOADER, not Gate.
# Same shapes, -consumers 32 (last flag occurrence wins over the fixed prefix).
set -uo pipefail
Q=46.101.193.166; L=209.38.206.19; QPRIV=10.114.0.2
B="$(dirname "$0")"; OUT="$B/out"; mkdir -p "$OUT"
SSH="ssh -o BatchMode=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=10"
for cfg in "c1-1hop-ceiling-cons32:-hops 1 -cap 500000 -period 10 -lease 1 -pace-batch 50000 -consumers 32 -graph gk1c" \
           "c2-2hop-ceiling-cons32:-hops 2 -cap 500000 -period 10 -lease 1 -pace-batch 50000 -mid-cap 0 -consumers 32 -graph gk2c"; do
  name="gate-${cfg%%:*}"; args="${cfg#*:}"
  echo; echo "=== $name ($args) ==="
  $SSH root@$Q "[ -f /root/sampler.pid ] && kill \$(cat /root/sampler.pid) 2>/dev/null
psql -h 127.0.0.1 -U postgres -d queen -qc 'SELECT pg_stat_statements_reset();' >/dev/null
rm -f /root/$name.csv
setsid nohup bash /root/bench-sampler.sh /root/$name.csv 200 </dev/null >/dev/null 2>&1 &
echo \$! > /root/sampler.pid" >/dev/null 2>&1
  $SSH root@$L "/root/gateload -gate http://$QPRIV:8788 -app bench -duration 100 \
      -pushers 32 -consumers 8 -batch 100 $args" 2>&1 | tee "$OUT/$name.load.txt" | grep -E "declared|\[final" | tail -8
  $SSH root@$Q "[ -f /root/sampler.pid ] && kill \$(cat /root/sampler.pid) 2>/dev/null
docker stats gate --no-stream --format 'gate: {{.CPUPerc}} cpu {{.MemUsage}}'" 2>/dev/null
  # stage depths at end of run: where the leftover backlog sits is the
  # bottleneck attribution (push=pre-admission, admitted=pre-relay/pre-consume)
  g="${args##* }"
  $SSH root@$Q "for q in gate.bench.$g.entry.push gate.bench.$g.entry.admitted.default gate.bench.$g.mid.admitted.default gate.bench.$g.term.admitted.default; do printf '%s ' \$q; curl -s \"http://127.0.0.1:6632/api/v1/resources/queues/\$q/depth\" | python3 -c 'import json,sys; print(json.load(sys.stdin)[\"pending\"])' 2>/dev/null || echo -; done"
  scp -o BatchMode=yes -q root@$Q:/root/$name.csv "$OUT/$name.csv"
  python3 "$B/summarize.py" "$OUT/$name.csv" | grep -E "PG CPU|Queen CPU|TOTAL" || true
done
echo; echo "=== CEILING-CONS32 DONE ==="
