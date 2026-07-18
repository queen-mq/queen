#!/usr/bin/env bash
# smoke-rust.sh — correctness check for the Rust segments broker.
# Fresh PG -> C++ schema+configure -> Rust broker -> push 3 known payloads to 2
# partitions -> wildcard autoack pop -> print what went in and what came out.
set -uo pipefail
NET=qbench; PG=qbench-pg
CIMG="${CIMG:-queen-mq:segments}"; RIMG="${RIMG:-queen-seg-rust:latest}"

log(){ echo "[smoke] $*"; }
q(){ docker exec "$PG" psql -U postgres -tAc "$1" </dev/null 2>/dev/null; }

docker rm -fv "$PG" >/dev/null 2>&1 || true
docker run -d --name "$PG" --network "$NET" --shm-size=512m -e POSTGRES_PASSWORD=postgres postgres:16 >/dev/null
for i in $(seq 1 60); do docker exec "$PG" pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done

CPORT=6681; cn="b$CPORT"; docker rm -fv "$cn" >/dev/null 2>&1
docker run -d --name "$cn" --network "$NET" -p "$CPORT":6632 \
  -e PG_HOST="$PG" -e PG_PASSWORD=postgres -e PG_USER=postgres -e PG_DATABASE=postgres \
  -e NUM_WORKERS=4 -e DB_POOL_SIZE=20 -e SIDECAR_POOL_SIZE=10 "$CIMG" >/dev/null
for i in $(seq 1 150); do curl -sf "http://localhost:$CPORT/api/v1/status" >/dev/null 2>&1 && break; sleep 1; done
Q="smoke_$RANDOM"
curl -s -X POST "http://localhost:$CPORT/api/v1/configure" -H 'Content-Type: application/json' \
  -d "{\"queue\":\"$Q\",\"options\":{\"storage\":\"segments\",\"dedupWindowSeconds\":0,\"leaseTime\":60}}" >/dev/null
docker rm -fv "$cn" >/dev/null 2>&1

RPORT=6682; rn="r$RPORT"; docker rm -fv "$rn" >/dev/null 2>&1
docker run -d --name "$rn" --network "$NET" -p "$RPORT":6632 \
  -e PG_HOST="$PG" -e PG_PASSWORD=postgres -e PG_USER=postgres -e PG_DATABASE=postgres \
  -e DB_POOL_SIZE=20 -e QUEEN_V2_FUSION_FRAMES=2 -e QUEEN_V2_FUSION_HOLD_MS=50 "$RIMG" >/dev/null
for i in $(seq 1 60); do curl -sf "http://localhost:$RPORT/status" >/dev/null 2>&1 && break; sleep 1; done
log "rust up: $(curl -s http://localhost:$RPORT/status)"

log "PUSH 3 items (p0:2, p1:1)"
PUSH=$(cat <<JSON
{"items":[
 {"queue":"$Q","partition":"p0","payload":{"n":1,"tag":"alpha"},"transactionId":"tx-aaa"},
 {"queue":"$Q","partition":"p0","payload":{"n":2,"tag":"beta"},"transactionId":"tx-bbb"},
 {"queue":"$Q","partition":"p1","payload":{"n":3,"tag":"gamma"},"transactionId":"tx-ccc"}
]}
JSON
)
echo "$PUSH" | curl -s -X POST "http://localhost:$RPORT/api/v1/push" -H 'Content-Type: application/json' -d @- ; echo
sleep 1
log "DB: seg_segments (partition name, seq, msg_count, blob bytes):"
q "select pt.name, s.seq, s.msg_count, octet_length(s.blob) blob_bytes
   from queen.seg_segments s join queen.seg_partitions pt on pt.id=s.partition_id
   order by pt.name, s.seq;"

log "POP wildcard autoack (batch=10, partitions=10):"
curl -s -w "\n[http_status=%{http_code}]\n" "http://localhost:$RPORT/api/v1/pop/queue/$Q?batch=10&partitions=10&autoAck=true&wait=false&timeout=1000"
log "POP again (should be empty after autoack):"
curl -s -o /dev/null -w "http_status=%{http_code}\n" "http://localhost:$RPORT/api/v1/pop/queue/$Q?batch=10&partitions=10&autoAck=true&wait=false&timeout=500"

log "--- rust broker logs ---"; docker logs "$rn" 2>&1 | tail -15
docker rm -fv "$rn" >/dev/null 2>&1
log "###### smoke done ######"
