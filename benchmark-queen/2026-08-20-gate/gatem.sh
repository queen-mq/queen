#!/bin/bash
# Multi-target scaling: N independent 1-hop graphs at once. The single-graph
# ceiling is one destination counter partition = one lane (~2.8k items/s on
# this VM). Distinct target nodes have distinct counters, so aggregate relay
# should scale ~linearly with N until PG saturates. Per-graph e2e stays
# consumer-masked; the honest aggregate is derived from the depths:
#   admission_i = pushed_i - entry.push_i ; relay_i = admission_i - entry.admitted_i
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

for N in 10; do
  name="gate-m${N}b-graphs"
  # loader is 8 cores: 6 pushers x 10 graphs = 60 pushing goroutines at batch
  # 100 is well within it (pushers are latency-bound on the broker, not CPU)
  case $N in
    2)  PUSH=16; CONS=8;;
    4)  PUSH=8;  CONS=6;;
    8)  PUSH=4;  CONS=4;;
    10) PUSH=16; CONS=6;;
  esac
  echo; echo "=== $name (N=$N pushers=$PUSH consumers=$CONS each) ==="
  reset_rig
  $SSH root@$Q "rm -f /root/$name.csv
setsid nohup bash /root/bench-sampler.sh /root/$name.csv 200 </dev/null >/dev/null 2>&1 &
echo \$! > /root/sampler.pid" >/dev/null 2>&1
  $SSH root@$L "rm -f /root/m$N-*.log
for i in \$(seq 1 $N); do
  /root/gateload -gate http://$QPRIV:8788 -app bench -duration 120 \
      -pushers $PUSH -consumers $CONS -batch 100 \
      -hops 1 -cap 500000 -period 10 -lease 1 -pace-batch 50000 -graph gm${N}x\$i > /root/m$N-\$i.log 2>&1 &
done
wait
grep -h '\[final\]' /root/m$N-*.log" | tee "$OUT/$name.load.txt"
  $SSH root@$Q "[ -f /root/sampler.pid ] && kill \$(cat /root/sampler.pid) 2>/dev/null
docker stats gate --no-stream --format 'gate: {{.CPUPerc}} cpu {{.MemUsage}}'" 2>/dev/null
  $SSH root@$Q "for i in \$(seq 1 $N); do for q in gate.bench.gm${N}x\$i.entry.push gate.bench.gm${N}x\$i.entry.admitted.default gate.bench.gm${N}x\$i.term.admitted.default; do printf '%s ' \$q; curl -s \"http://127.0.0.1:6632/api/v1/resources/queues/\$q/depth\" | python3 -c 'import json,sys; print(json.load(sys.stdin)[\"pending\"])' 2>/dev/null || echo -; done; done" | tee -a "$OUT/$name.load.txt"
  scp -o BatchMode=yes -q root@$Q:/root/$name.csv "$OUT/$name.csv"
  python3 "$B/summarize.py" "$OUT/$name.csv" | grep -E "PG CPU|Queen CPU|TOTAL|top waits" || true
done
echo; echo "=== MULTI-TARGET DONE ==="
