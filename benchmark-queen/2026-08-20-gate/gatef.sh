#!/bin/bash
# Clean-state sweep: FULL reset (both DBs dropped, both containers restarted)
# before EVERY run. The first sweep attempt ran back-to-back on accumulated
# backlogs (~4M rows after 10 runs): PG climbed to 12.8 cores of tuple waits,
# push rates collapsed run-over-run, and the partition comparison measured the
# debris, not the relay. Each run here starts from the state g1 started from,
# so rows are comparable to each other AND to g1 (the fresh parts=16 point).
set -uo pipefail
Q=46.101.193.166; L=209.38.206.19; QPRIV=10.114.0.2
B="$(dirname "$0")"; OUT="$B/out"; mkdir -p "$OUT"
SSH="ssh -o BatchMode=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=10"

reset_rig() {
  $SSH root@$Q '
docker stop gate queen >/dev/null 2>&1; docker rm gate queen >/dev/null 2>&1
psql -h 127.0.0.1 -U postgres -qc "DROP DATABASE IF EXISTS queen;" -c "CREATE DATABASE queen;" -c "DROP DATABASE IF EXISTS gate;" -c "CREATE DATABASE gate;"
psql -h 127.0.0.1 -U postgres -d queen -qc "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"
docker run -d --name queen --network host \
  -e LOG_LEVEL=info -e PORT=6632 \
  -e PG_HOST=127.0.0.1 -e PG_PORT=5432 -e PG_USER=postgres -e PG_PASSWORD=postgres -e PG_DATABASE=queen \
  -e QUEEN_DEDUP_CACHE_MB=4096 \
  -e QUEEN_KV_READ_RATE=200000 -e QUEEN_KV_WRITE_RATE=200000 -e QUEEN_KV_READ_BURST=400000 -e QUEEN_KV_WRITE_BURST=400000 -e QUEEN_KV_CELL_RATE=0 \
  ghcr.io/queen-mq/queen:1.0.5 >/dev/null
for i in $(seq 1 30); do code=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:6632/health); [ "$code" = 200 ] && break; sleep 1; done
docker run -d --name gate --network host \
  -e QUEEN_URL=http://127.0.0.1:6632 -e GATE_BIND=0.0.0.0:8788 -e GATE_PUBLIC_BIND=0.0.0.0:8790 \
  -e GATE_ADMIN_EMAILS=bench@example.com -e GATE_DEV_EMAIL=bench@example.com \
  -e PG_HOST=127.0.0.1 -e PG_PORT=5432 -e PG_USER=postgres -e PG_PASSWORD=postgres -e PG_DATABASE=gate \
  ghcr.io/queen-mq/gate:latest >/dev/null
sleep 3'
}

for cfg in "f1-1hop-parts1:-hops 1 -cap 500000 -period 10 -lease 1 -pace-batch 50000 -partitions 1 -graph gf1" \
           "f2-1hop-parts4:-hops 1 -cap 500000 -period 10 -lease 1 -pace-batch 50000 -partitions 4 -graph gf2" \
           "f3-1hop-parts32:-hops 1 -cap 500000 -period 10 -lease 1 -pace-batch 50000 -partitions 32 -graph gf3" \
           "f4-1hop-paced150-fresh:-hops 1 -cap 2000 -period 10 -lease 5 -pace-batch 1000 -rate 150 -graph gf4" \
           "f5-2hop-paced150-fresh:-hops 2 -cap 2000 -period 10 -lease 5 -pace-batch 2000 -mid-cap 3000 -rate 150 -graph gf5" \
           "f6-1hop-paced2000:-hops 1 -cap 25000 -period 10 -lease 1 -pace-batch 2500 -rate 2000 -graph gf6" \
           "f7-1hop-paced2500:-hops 1 -cap 30000 -period 10 -lease 1 -pace-batch 3000 -rate 2500 -graph gf7"; do
  name="gate-${cfg%%:*}"; args="${cfg#*:}"
  echo; echo "=== $name ($args) ==="
  reset_rig
  $SSH root@$Q "rm -f /root/$name.csv
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
echo; echo "=== FRESH SWEEP DONE ==="
