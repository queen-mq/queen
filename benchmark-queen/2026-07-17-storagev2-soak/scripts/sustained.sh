#!/usr/bin/env bash
set -uo pipefail
DUR=300
PROD=300
CONS=200
q(){ docker exec qbench-pg psql -U postgres -tAc "$1" </dev/null 2>/dev/null; }
reset_schema(){
  q "DROP SCHEMA IF EXISTS queen CASCADE;" >/dev/null
  docker cp /root/queen/lib/schema/schema.sql qbench-pg:/tmp/s.sql >/dev/null 2>&1
  docker exec -i qbench-pg psql -U postgres -q -f /tmp/s.sql </dev/null >/dev/null 2>&1
  for f in $(ls /root/queen/lib/schema/procedures/*.sql | sort); do
    docker cp "$f" qbench-pg:/tmp/p.sql >/dev/null 2>&1
    docker exec -i qbench-pg psql -U postgres -q -f /tmp/p.sql </dev/null >/dev/null 2>&1
  done
  q "VACUUM ANALYZE;" >/dev/null 2>&1
}
run_one(){
  local name="$1" img="$2" port="$3"; shift 3
  echo "===== $name ($img) ====="
  reset_schema
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker run -d --name "$name" --network qbench --ulimit nofile=65535:65535 -p "$port":6632 -e PG_HOST=qbench-pg -e PG_PASSWORD=postgres "$@" "$img" >/dev/null
  for i in $(seq 1 90); do curl -sf "http://localhost:$port/api/v1/status" >/dev/null 2>&1 && break; sleep 1; done
  C0=$(q "select xact_commit from pg_stat_database where datname='postgres';")
  /root/goload -mode max -url "http://localhost:$port" -queue "sus_$name" -partitions 100 -producers $PROD -consumers $CONS -push-batch 10 -pop-batch 200 -pop-partitions 5 -pop-wait=true -pop-timeout=2000 -payload 256 -duration $DUR -report 30 > /root/sus_$name.log 2>&1 &
  GL=$!
  for t in 60 120 180 240 300; do
    sleep 60
    kill -0 $GL 2>/dev/null || break
    st=$(docker stats --no-stream --format '{{.CPUPerc}}/{{.MemUsage}}' "$name" 2>/dev/null)
    pg=$(docker stats --no-stream --format '{{.CPUPerc}}/{{.MemUsage}}' qbench-pg 2>/dev/null)
    rows=$(q "select count(*) from queen.messages;")
    echo "  t=${t}s broker=$st pg=$pg msgs_rows=$rows"
  done
  wait $GL 2>/dev/null
  C1=$(q "select xact_commit from pg_stat_database where datname='postgres';")
  f=$(tail -1 /root/sus_$name.log)
  pu=$(echo "$f"|grep -o pushed=[0-9]*|cut -d= -f2); po=$(echo "$f"|grep -o popped=[0-9]*|cut -d= -f2)
  echo "  FINAL $name: push=$((pu/DUR))/s pop=$((po/DUR))/s totpush=$pu totpop=$po commits/s=$(( (C1-C0)/DUR ))"
  echo "  throughput trend (per 30s report):"; grep -oE 'push= *[0-9]+/s pop= *[0-9]+/s' /root/sus_$name.log | tail -10 | sed 's/^/    /'
  docker rm -f "$name" >/dev/null 2>&1
}
echo "SUSTAINED START $(date -u +%FT%TZ)"
run_one qbench-cpp smartnessai/queen-mq:0.16.0 6634 -e DB_POOL_SIZE=60 -e NUM_WORKERS=10 -e QUEEN_CONCURRENCY_MODE=static
run_one qbench-go go-hotpath-spike:latest 6633 -e QUEEN_ENGINE_SHARDS=1 -e QUEEN_GLOBAL_CONCURRENCY=72 -e QUEEN_PUSH_MAX_CONCURRENT=24 -e QUEEN_POP_MAX_CONCURRENT=40 -e QUEEN_POP_PREFERRED_BATCH_SIZE=20
run_one qbench-rs queen-hotpath-rust:latest 6635 -e QUEEN_GLOBAL_CONCURRENCY=72 -e QUEEN_PUSH_MAX_CONCURRENT=24 -e QUEEN_POP_MAX_CONCURRENT=40 -e QUEEN_POP_PREFERRED_BATCH_SIZE=20
echo "SUSTAINED DONE $(date -u +%FT%TZ)"
