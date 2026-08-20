#!/bin/bash
# Unattended remainder of the 2026-08-20 campaign: KV load, then the Gate
# throughput/multi-hop matrix. Each stage resets the database first — a stage
# inheriting the previous one's queues is not the same measurement.
set -uo pipefail
Q=46.101.193.166
L=209.38.206.19
QPRIV=10.114.0.2
B="$(dirname "$0")"
OUT="$B/out"; mkdir -p "$OUT"
SSH="ssh -o BatchMode=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=10"

sampler_start() { # $1 name  $2 seconds
  $SSH root@$Q "psql -h 127.0.0.1 -U postgres -d queen -qc 'SELECT pg_stat_statements_reset();' >/dev/null
[ -f /root/sampler.pid ] && kill \$(cat /root/sampler.pid) 2>/dev/null
rm -f /root/$1.csv
setsid nohup bash /root/bench-sampler.sh /root/$1.csv $2 </dev/null > /root/$1.sampler.log 2>&1 &
echo \$! > /root/sampler.pid" >/dev/null 2>&1
}
sampler_stop() { # $1 name
  $SSH root@$Q "[ -f /root/sampler.pid ] && kill \$(cat /root/sampler.pid) 2>/dev/null" >/dev/null 2>&1
  scp -o BatchMode=yes -q root@$Q:/root/$1.csv "$OUT/$1.csv" 2>/dev/null
  python3 "$B/summarize.py" "$OUT/$1.csv" 2>/dev/null
}
pgss() { # $1 name
  $SSH root@$Q "psql -h 127.0.0.1 -U postgres -d queen -qX -c \"
SELECT round(total_exec_time)::bigint AS total_ms, calls, round(mean_exec_time::numeric,3) AS mean_ms,
       round(100*total_exec_time/NULLIF(sum(total_exec_time) OVER (),0))::int AS pct,
       left(regexp_replace(query,'\s+',' ','g'),64) AS query
FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname='queen')
ORDER BY total_exec_time DESC LIMIT 14;\"" > "$OUT/$1.pgss.txt" 2>&1
}

echo "###############################################################"
echo "# STAGE 1 — KV under load"
echo "###############################################################"

for cfg in "mix60_30_10:-incr 60 -get 30 -pia 10 -batch 1 -workers 64" \
           "batch25:-incr 60 -get 30 -pia 10 -batch 25 -workers 32" \
           "incr_only:-incr 100 -get 0 -pia 0 -batch 1 -workers 64"; do
  name="kv-${cfg%%:*}"; args="${cfg#*:}"
  echo; echo "=== $name ($args) ==="
  sampler_start "$name" 200
  $SSH root@$L "/root/kvload -url http://$QPRIV:6632 -duration 120 -namespaces 100 -keys 20000 $args" \
    2>&1 | tee "$OUT/$name.load.txt" | tail -6
  pgss "$name"
  sampler_stop "$name"
done

echo
echo "###############################################################"
echo "# STAGE 2 — Gate throughput + multi-hop"
echo "###############################################################"

# Gate alongside the broker, as it runs in prod (gate pods next to queen pods).
$SSH root@$Q "docker rm -f gate >/dev/null 2>&1
psql -h 127.0.0.1 -U postgres -d postgres -qc 'DROP DATABASE IF EXISTS gate WITH (FORCE);' >/dev/null 2>&1
psql -h 127.0.0.1 -U postgres -d postgres -qc 'CREATE DATABASE gate;' >/dev/null 2>&1
docker run -d --name gate --network host \
  -e QUEEN_URL=http://127.0.0.1:6632 \
  -e GATE_BIND=0.0.0.0:8788 -e GATE_PUBLIC_BIND=0.0.0.0:8790 \
  -e GATE_DEV_EMAIL=bench@example.com -e GATE_ADMIN_EMAILS=bench@example.com \
  -e PG_HOST=127.0.0.1 -e PG_PORT=5432 -e PG_USER=postgres -e PG_PASSWORD=postgres -e PG_DATABASE=gate \
  --ulimit nofile=200000:200000 ghcr.io/queen-mq/gate:latest >/dev/null 2>&1
for i in \$(seq 40); do curl -sf http://127.0.0.1:8788/health >/dev/null 2>&1 && break; sleep 1; done
curl -s http://127.0.0.1:8788/health 2>/dev/null | head -c 200; echo
docker logs gate 2>&1 | tail -5"

# matrix: (name, flags). cap=0 -> no budget -> measures Gate's own ceiling.
for cfg in "g1-1hop-unlimited:-hops 1 -cap 0 -graph g1" \
           "g2-2hop-unlimited:-hops 2 -cap 0 -mid-cap 0 -graph g2" \
           "g3-1hop-cap2000p10:-hops 1 -cap 2000 -period 10 -graph g3" \
           "g4-2hop-capped:-hops 2 -cap 2000 -period 10 -mid-cap 3000 -graph g4"; do
  name="gate-${cfg%%:*}"; args="${cfg#*:}"
  echo; echo "=== $name ($args) ==="
  sampler_start "$name" 220
  $SSH root@$L "/root/gateload -gate http://$QPRIV:8788 -app bench -duration 120 \
      -pushers 32 -consumers 8 -batch 100 $args" 2>&1 | tee "$OUT/$name.load.txt" | tail -8
  pgss "$name"
  sampler_stop "$name"
  $SSH root@$Q "docker stats gate --no-stream --format 'gate container: {{.CPUPerc}} cpu {{.MemUsage}}'" 2>/dev/null
done

echo
echo "=== SUITE DONE ==="
