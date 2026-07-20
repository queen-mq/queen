#!/usr/bin/env bash
# inspect-vegas.sh — run C++ (vegas) under load and dump the concurrency-related
# metrics so we can read the limit vegas converges to, then reuse it in Rust.
set -uo pipefail
NET=qbench; PG=qbench-pg; GOLOAD=/root/goload
DURATION="${DURATION:-60}"

q(){ docker exec "$PG" psql -U postgres -tAc "$1" </dev/null 2>/dev/null; }
reset(){
  # kill any lingering client backends so DROP SCHEMA can't get stuck on a lock
  q "select pg_terminate_backend(pid) from pg_stat_activity where pid<>pg_backend_pid() and state<>'idle';" >/dev/null 2>&1
  q "DROP SCHEMA IF EXISTS queen CASCADE;" >/dev/null
  docker cp /root/queen/lib/schema/schema.sql "$PG":/tmp/s.sql >/dev/null
  docker exec -i "$PG" psql -U postgres -q -f /tmp/s.sql </dev/null >/dev/null 2>&1
  for f in $(ls /root/queen/lib/schema/procedures/*.sql|sort); do
    docker cp "$f" "$PG":/tmp/p.sql >/dev/null
    docker exec -i "$PG" psql -U postgres -q -f /tmp/p.sql </dev/null >/dev/null 2>&1
  done
  q "VACUUM ANALYZE;" >/dev/null 2>&1
}

reset
nm=b6634
docker rm -f "$nm" >/dev/null 2>&1
docker run -d --name "$nm" --network "$NET" --ulimit nofile=65535:65535 -p 6634:6632 \
  -e PG_HOST="$PG" -e PG_PASSWORD=postgres \
  -e DB_POOL_SIZE=100 -e NUM_WORKERS=10 -e QUEEN_CONCURRENCY_MODE=vegas \
  -e QUEEN_POP_PREFERRED_BATCH_SIZE=40 -e QUEEN_PUSH_PREFERRED_BATCH_SIZE=100 \
  smartnessai/queen-mq:0.16.0 >/dev/null
for i in $(seq 1 90); do curl -sf http://localhost:6634/api/v1/status >/dev/null 2>&1 && break; sleep 1; done

"$GOLOAD" -mode max -url http://localhost:6634 -queue "x$RANDOM" \
  -partitions 100 -producers 300 -consumers 200 -push-batch 10 -pop-batch 200 \
  -pop-partitions 5 -pop-wait=true -pop-timeout=2000 -payload 256 \
  -duration "$DURATION" -report 100 >/tmp/load.log 2>&1 &
lp=$!

for t in 20 35 50; do
  sleep_to=$t
  sleep 15
  echo "===== t~${t}s: concurrency-related metrics ====="
  curl -s http://localhost:6634/metrics/prometheus 2>/dev/null \
    | grep -iE 'concurren|in_?flight|limit|vegas|pool|worker|active|permit' \
    | grep -viE '^#' | sort
  echo "--- status ---"
  curl -s http://localhost:6634/api/v1/status 2>/dev/null | head -c 1200; echo
done
wait "$lp"
echo "=== final load ==="; tail -1 /tmp/load.log
docker rm -f "$nm" >/dev/null 2>&1
