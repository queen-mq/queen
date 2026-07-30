#!/usr/bin/env bash
set -uo pipefail
NET=qbench; PG=qbench-pg
CIMG="${CIMG:-queen-mq:segments}"; RIMG="${RIMG:-queen:latest}"
q(){ docker exec "$PG" psql -U postgres -tAc "$1" </dev/null 2>/dev/null; }

docker rm -fv "$PG" >/dev/null 2>&1 || true
docker run -d --name "$PG" --network "$NET" --shm-size=512m -e POSTGRES_PASSWORD=postgres postgres:16 >/dev/null
for i in $(seq 1 60); do docker exec "$PG" pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done

CPORT=6681; cn="b$CPORT"; docker rm -fv "$cn" >/dev/null 2>&1
docker run -d --name "$cn" --network "$NET" -p "$CPORT":6632 \
  -e PG_HOST="$PG" -e PG_PASSWORD=postgres -e PG_USER=postgres -e PG_DATABASE=postgres \
  -e NUM_WORKERS=4 -e DB_POOL_SIZE=20 -e SIDECAR_POOL_SIZE=10 "$CIMG" >/dev/null
for i in $(seq 1 150); do curl -sf "http://localhost:$CPORT/api/v1/status" >/dev/null 2>&1 && break; sleep 1; done
Q="dbg_$RANDOM"
curl -s -X POST "http://localhost:$CPORT/api/v1/configure" -H 'Content-Type: application/json' \
  -d "{\"queue\":\"$Q\",\"options\":{\"storage\":\"segments\",\"dedupWindowSeconds\":0,\"leaseTime\":60}}" >/dev/null
docker rm -fv "$cn" >/dev/null 2>&1

RPORT=6682; rn="r$RPORT"; docker rm -fv "$rn" >/dev/null 2>&1
docker run -d --name "$rn" --network "$NET" -p "$RPORT":6632 \
  -e PG_HOST="$PG" -e PG_PASSWORD=postgres -e PG_USER=postgres -e PG_DATABASE=postgres \
  -e DB_POOL_SIZE=20 -e QUEEN_V2_FUSION_FRAMES=2 -e QUEEN_V2_FUSION_HOLD_MS=50 "$RIMG" >/dev/null
for i in $(seq 1 60); do curl -sf "http://localhost:$RPORT/status" >/dev/null 2>&1 && break; sleep 1; done

curl -s -X POST "http://localhost:$RPORT/api/v1/push" -H 'Content-Type: application/json' -d \
  "{\"items\":[{\"queue\":\"$Q\",\"partition\":\"p0\",\"payload\":{\"n\":1},\"transactionId\":\"tx1\"},{\"queue\":\"$Q\",\"partition\":\"p0\",\"payload\":{\"n\":2},\"transactionId\":\"tx2\"}]}" >/dev/null
sleep 1

echo "=== partitions/segments/consumers state ==="
q "select pt.name, pt.last_seq, pt.last_write_at from queen.seg_partitions pt;"
q "select count(*) segs from queen.seg_segments;"
q "select * from queen.seg_consumer_watermarks;"

echo "=== direct SP: seg_pop_wildcard_wire_v1(Q,__QUEUE_MODE__,10,60,w1,true,10,all,'') ==="
q "select queen.seg_pop_wildcard_wire_v1('$Q','__QUEUE_MODE__',10,60,'w1',true,10,'all','');" | head -c 600; echo

echo "=== rust pop (fresh group to avoid watermark) ==="
curl -s -w "\n[http=%{http_code}]\n" "http://localhost:$RPORT/api/v1/pop/queue/$Q?batch=10&partitions=10&autoAck=true&wait=false&consumerGroup=g2"

echo "=== rust logs ==="; docker logs "$rn" 2>&1 | tail -10
docker rm -fv "$rn" >/dev/null 2>&1
echo "###### debug done ######"
