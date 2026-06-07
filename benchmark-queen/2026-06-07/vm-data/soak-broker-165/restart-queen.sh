#!/bin/bash
# Fast queen-only restart (keeps postgres running). Config via env.
set -u
TAG="${TAG:-0.16.0.beta.1-ui}"
W="${NUM_WORKERS:-8}"; POOL="${DB_POOL_SIZE:-96}"; SIDE="${SIDECAR_POOL_SIZE:-128}"
docker stop queen >/dev/null 2>&1; docker rm queen >/dev/null 2>&1
docker run -d --ulimit nofile=65535:65535 --name queen -p 6632:6632 --network queen \
  -e PG_HOST=postgres -e PG_PASSWORD=postgres -e NUM_WORKERS="$W" -e DB_POOL_SIZE="$POOL" -e SIDECAR_POOL_SIZE="$SIDE" \
  -e RETENTION_BATCH_SIZE=50000 -e RETENTION_INTERVAL=5000 -e RETENTION_PARALLELISM="${RETENTION_PARALLELISM:-8}" \
  -e QUEEN_PUSH_MAX_CONCURRENT="${QUEEN_PUSH_MAX_CONCURRENT:-24}" -e QUEEN_PUSH_MAX_HOLD_MS="${QUEEN_PUSH_MAX_HOLD_MS:-20}" \
  -e QUEEN_PUSH_PREFERRED_BATCH_SIZE="${QUEEN_PUSH_PREFERRED_BATCH_SIZE:-50}" -e QUEEN_PUSH_MAX_BATCH_SIZE="${QUEEN_PUSH_MAX_BATCH_SIZE:-500}" \
  -e QUEEN_POP_MAX_CONCURRENT="${QUEEN_POP_MAX_CONCURRENT:-16}" -e QUEEN_POP_MAX_HOLD_MS="${QUEEN_POP_MAX_HOLD_MS:-5}" \
  -e QUEEN_POP_PREFERRED_BATCH_SIZE="${QUEEN_POP_PREFERRED_BATCH_SIZE:-20}" -e QUEEN_POP_MAX_BATCH_SIZE="${QUEEN_POP_MAX_BATCH_SIZE:-500}" \
  -e QUEEN_CONCURRENCY_MODE="${QUEEN_CONCURRENCY_MODE:-vegas}" \
  -e QUEEN_VEGAS_MAX_LIMIT="${QUEEN_VEGAS_MAX_LIMIT:-32}" \
  -e QUEEN_PUSH_BATCH_INFLIGHT_THRESHOLD="${QUEEN_PUSH_BATCH_INFLIGHT_THRESHOLD:-0}" \
  smartnessai/queen-mq:"$TAG" >/dev/null
ok=0
for i in $(seq 1 60); do curl -sf http://localhost:6632/api/v1/status >/dev/null 2>&1 && { ok=1; break; }; sleep 1; done
sleep 2
if docker ps --format '{{.Names}}' | grep -q '^queen$' && [ "$ok" = 1 ]; then
  echo "QUEEN-READY W=$W POOL=$POOL SIDE=$SIDE cc=${QUEEN_CONCURRENCY_MODE:-vegas} push[C=${QUEEN_PUSH_MAX_CONCURRENT:-24} H=${QUEEN_PUSH_MAX_HOLD_MS:-40} P=${QUEEN_PUSH_PREFERRED_BATCH_SIZE:-50} MB=${QUEEN_PUSH_MAX_BATCH_SIZE:-500} IFT=${QUEEN_PUSH_BATCH_INFLIGHT_THRESHOLD:-0}]"
else
  echo "QUEEN-FAILED"; docker logs queen 2>&1 | tail -4
fi
