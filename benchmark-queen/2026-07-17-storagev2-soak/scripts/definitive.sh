#!/usr/bin/env bash
set -uo pipefail
KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
SEC=benchsecret
DUR=40
fresh_pg(){
  docker rm -f qbench-pg >/dev/null 2>&1 || true
  docker volume rm qbench-pgdata >/dev/null 2>&1 || true
  docker run -d --name qbench-pg --network qbench --ulimit nofile=65535:65535 --shm-size=1g \
    -v qbench-pgdata:/var/lib/postgresql/data -e POSTGRES_PASSWORD=postgres postgres:16 \
    -c max_connections=400 -c shared_buffers=8GB -c effective_cache_size=32GB \
    -c synchronous_commit=on -c max_wal_size=16GB -c checkpoint_timeout=15min \
    -c work_mem=32MB -c maintenance_work_mem=1GB >/dev/null
  for i in $(seq 1 60); do docker exec qbench-pg pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done
  docker cp /root/queen/lib/schema/schema.sql qbench-pg:/tmp/s.sql >/dev/null
  docker exec -i qbench-pg psql -U postgres -q -f /tmp/s.sql </dev/null >/dev/null 2>&1
  for f in $(ls /root/queen/lib/schema/procedures/*.sql|sort); do docker cp "$f" qbench-pg:/tmp/p.sql >/dev/null; docker exec -i qbench-pg psql -U postgres -q -f /tmp/p.sql </dev/null >/dev/null 2>&1; done
}
q(){ docker exec qbench-pg psql -U postgres -tAc "$1" </dev/null 2>/dev/null; }
run(){
  local label="$1" img="$2" port="$3" enc="$4"; shift 4
  fresh_pg   # PRISTINE Postgres for EVERY run
  local nm="d$port"; docker rm -f "$nm" >/dev/null 2>&1 || true
  docker run -d --name "$nm" --network qbench --ulimit nofile=65535:65535 -p "$port":6632 \
    -e PG_HOST=qbench-pg -e PG_PASSWORD=postgres -e QUEEN_GLOBAL_CONCURRENCY=72 -e QUEEN_PUSH_MAX_CONCURRENT=24 \
    -e QUEEN_POP_MAX_CONCURRENT=40 -e QUEEN_POP_PREFERRED_BATCH_SIZE=20 -e DB_POOL_SIZE=60 -e NUM_WORKERS=10 -e QUEEN_CONCURRENCY_MODE=vegas "$@" "$img" >/dev/null
  for i in $(seq 1 90); do curl -sf http://localhost:$port/api/v1/status >/dev/null 2>&1 && break; sleep 1; done
  C0=$(q "select xact_commit from pg_stat_database where datname='postgres';")
  GOLOAD_ENCRYPT=$enc /root/goload -mode max -url http://localhost:$port -queue "d$port" -partitions 100 -producers 200 -consumers 120 -push-batch 10 -pop-batch 200 -pop-partitions 5 -pop-wait=true -pop-timeout=2000 -payload 256 -duration $DUR -report 100 >/tmp/d.log 2>&1 &
  sleep $((DUR/2)); st=$(docker stats --no-stream --format '{{.CPUPerc}}/{{.MemUsage}}' "$nm"); pg=$(docker stats --no-stream --format '{{.CPUPerc}}' qbench-pg)
  wait; C1=$(q "select xact_commit from pg_stat_database where datname='postgres';")
  f=$(tail -1 /tmp/d.log)
  echo "$label -> push=$(($(echo "$f"|grep -o pushed=[0-9]*|cut -d= -f2)/DUR))/s pop=$(($(echo "$f"|grep -o popped=[0-9]*|cut -d= -f2)/DUR))/s | broker[$st] pgCPU=$pg commits/s=$(( (C1-C0)/DUR ))"
  docker rm -f "$nm" >/dev/null 2>&1
}
echo "DEFINITIVE START $(date -u +%FT%TZ) (fresh PG per run)"
run "C++  baseline    " smartnessai/queen-mq:0.16.0   6634 0
run "C++  full(enc+au)" smartnessai/queen-mq:0.16.0   6634 1 -e QUEEN_ENCRYPTION_KEY=$KEY
run "Go   baseline    " go-hotpath-spike:latest        6633 0
run "Go   full(enc+au)" go-hotpath-spike:latest        6633 1 -e QUEEN_ENCRYPTION_KEY=$KEY -e QUEEN_JWT_SECRET=$SEC
run "Rust baseline    " queen-hotpath-rust:latest      6635 0
run "Rust full(enc+au)" queen-hotpath-rust:latest      6635 1 -e QUEEN_ENCRYPTION_KEY=$KEY -e QUEEN_JWT_SECRET=$SEC
echo "DEFINITIVE DONE $(date -u +%FT%TZ)"
