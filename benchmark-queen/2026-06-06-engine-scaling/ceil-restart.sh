#!/usr/bin/env bash
# Broker-side: (re)start the queen container for one ceiling-sweep cell.
# args: C [SIDE]
# env: TAG, NUM_WORKERS, MODE(static|vegas), MAXP(max partitions/batch), HOLD
set -u
C="${1:?usage: ceil-restart.sh C [SIDE]}"
SIDE="${2:-250}"
TAG="${TAG:-pushser}"
docker stop queen >/dev/null 2>&1; docker rm queen >/dev/null 2>&1
docker run -d --ulimit nofile=65535:65535 --name queen -p 6632:6632 --network queen \
  -e PG_HOST=postgres -e PG_PASSWORD=postgres \
  -e NUM_WORKERS="${NUM_WORKERS:-12}" -e DB_POOL_SIZE=50 -e SIDECAR_POOL_SIZE="$SIDE" \
  -e RETENTION_BATCH_SIZE=50000 -e RETENTION_INTERVAL=5000 -e RETENTION_PARALLELISM=8 \
  -e QUEEN_PUSH_MAX_CONCURRENT="$C" -e QUEEN_PUSH_MAX_HOLD_MS="${HOLD:-40}" \
  -e QUEEN_PUSH_PREFERRED_BATCH_SIZE=50 -e QUEEN_PUSH_MAX_BATCH_SIZE=500 \
  -e QUEEN_PUSH_MAX_PARTITIONS_PER_BATCH="${MAXP:-8}" \
  -e QUEEN_CONCURRENCY_MODE="${MODE:-static}" -e QUEEN_VEGAS_MAX_LIMIT="$C" \
  smartnessai/queen-mq:"$TAG" >/dev/null
ok=0
for i in $(seq 1 60); do curl -sf http://localhost:6632/api/v1/status >/dev/null 2>&1 && { ok=1; break; }; sleep 1; done
sleep 2
docker exec postgres psql -U postgres -d postgres -tAc 'TRUNCATE queen.messages CASCADE;' >/dev/null 2>&1 || true
[ "$ok" = 1 ] && echo "READY C=$C SIDE=$SIDE mode=${MODE:-static}" || { echo "QUEEN-FAILED"; docker logs queen 2>&1 | tail -4; }
