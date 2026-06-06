#!/usr/bin/env bash
# Heavy push-only drive at a fixed (production-like) worker count, with richer
# sampling than the sweep: standard metrics (mon-engine.sh) PLUS a per-thread
# CPU capture (top -bH inside the container) so we can split libuv ENGINE thread
# CPU from uWS HTTP-worker thread CPU. Answers: at the real push ceiling, is the
# limiter the engine loop (evl high / engine threads pegged), Postgres, or the
# HTTP/loader path?
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
TAG="${TAG:-0.16.0.beta.1-ui}"
GOLOAD="${GOLOAD:-$HERE/../2026-06-04/goload/goload-linux-amd64}"
URL="${URL:-http://localhost:6632}"
W="${W:-8}"; SIDE="${SIDE:-250}"; DB_POOL="${DB_POOL:-50}"
PARTS="${PARTS:-300}"; PROD="${PROD:-1000}"; PB="${PB:-10}"; DUR="${DUR:-75}"
HOLD="${QUEEN_PUSH_MAX_HOLD_MS:-40}"
OUT="$HERE/out/heavy"; mkdir -p "$OUT"
tag="w${W}_p${PROD}_b${PB}"
log(){ echo "[$(date -u +%FT%TZ)] $*"; }

log "restart queen W=$W SIDE=$SIDE hold=$HOLD"
docker stop queen >/dev/null 2>&1; docker rm queen >/dev/null 2>&1
docker run -d --ulimit nofile=65535:65535 --name queen -p 6632:6632 --network queen \
  -e PG_HOST=postgres -e PG_PASSWORD=postgres \
  -e NUM_WORKERS="$W" -e DB_POOL_SIZE="$DB_POOL" -e SIDECAR_POOL_SIZE="$SIDE" \
  -e RETENTION_BATCH_SIZE=50000 -e RETENTION_INTERVAL=5000 -e RETENTION_PARALLELISM=8 \
  -e QUEEN_PUSH_MAX_CONCURRENT="${QUEEN_PUSH_MAX_CONCURRENT:-24}" -e QUEEN_PUSH_MAX_HOLD_MS="$HOLD" \
  -e QUEEN_PUSH_PREFERRED_BATCH_SIZE="${QUEEN_PUSH_PREFERRED_BATCH_SIZE:-50}" -e QUEEN_PUSH_MAX_BATCH_SIZE="${QUEEN_PUSH_MAX_BATCH_SIZE:-500}" \
  -e QUEEN_CONCURRENCY_MODE=vegas -e QUEEN_VEGAS_MAX_LIMIT="${QUEEN_VEGAS_MAX_LIMIT:-32}" \
  smartnessai/queen-mq:"$TAG" >/dev/null
for i in $(seq 1 90); do curl -sf "$URL/api/v1/status" >/dev/null 2>&1 && break; sleep 1; done
sleep 3
docker exec postgres psql -U postgres -d postgres -tAc "TRUNCATE queen.messages CASCADE;" >/dev/null 2>&1 || true

# standard sampler
bash "$HERE/mon-engine.sh" "$OUT/$tag.tsv" 3 queen postgres &
MON=$!
# per-thread CPU snapshots (best effort; top may be absent in minimal images)
( for i in $(seq 1 $(( DUR/6 )) ); do
    echo "=== t=$(date +%s) ==="
    docker exec queen top -bH -n1 2>/dev/null | sed -n '1,5p;/COMMAND/,$p' | head -45
    sleep 6
  done ) > "$OUT/$tag.threads.txt" 2>&1 &
TOP=$!

log "goload heavy: parts=$PARTS prod=$PROD batch=$PB dur=${DUR}s"
"$GOLOAD" -url "$URL" -queue heavyq -partitions "$PARTS" -producers "$PROD" -consumers 0 \
  -push-batch "$PB" -duration "$DUR" -report 5 -completed-retention 100000 -pending-retention 0 \
  > "$OUT/$tag.goload.log" 2>&1

kill "$MON" "$TOP" >/dev/null 2>&1; wait "$MON" "$TOP" 2>/dev/null || true
log "done: $(grep -E '^\[final\]' "$OUT/$tag.goload.log" | tail -1)"
log "host load: $(uptime)"
