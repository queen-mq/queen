#!/usr/bin/env bash
# Engine-scaling discovery sweep.
#
# Holds TOTAL DB concurrency fixed (SIDECAR_POOL_SIZE) and varies the engine
# count via NUM_WORKERS (one Queen engine per HTTP worker on the current build),
# across a high-fusion and a low-fusion push regime. The low-fusion regime is
# where a single event loop is most likely to saturate before Postgres.
#
# Requires: Postgres + queen already running as docker containers (e.g. via
# ../2026-06-04/start-broker.sh). Only the queen container is restarted per cell.
#
# Env knobs (all optional):
#   TAG                 queen image tag        (default 0.16.0.beta.1-ui)
#   GOLOAD              path to goload binary  (default ../2026-06-04/goload/goload-linux-amd64)
#   BROKER_URL          goload target URL      (default http://localhost:6632)
#   QUEUE               queue name             (default benchq-eng)
#   WORKERS             engine-count list      (default "1 2 4 8")
#   SIDE                total slots (constant) (default 240)
#   DB_POOL             DB_POOL_SIZE           (default 50)
#   DURATION            seconds per cell       (default 90)
#   SAMPLE              sampler interval sec   (default 3)
#   PHASE               wsweep | slots | both  (default wsweep)
#   SLOTS               slot list for PHASE=slots at W=1 (default "30 60 120 240")
#   REGIMES             subset of "fusion lowfusion balanced" (default "fusion lowfusion")
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
TAG="${TAG:-0.16.0.beta.1-ui}"
GOLOAD="${GOLOAD:-$HERE/../2026-06-04/goload/goload-linux-amd64}"
BROKER_URL="${BROKER_URL:-http://localhost:6632}"
QUEUE="${QUEUE:-benchq-eng}"
WORKERS="${WORKERS:-1 2 4 8}"
SIDE="${SIDE:-240}"
DB_POOL="${DB_POOL:-50}"
DURATION="${DURATION:-90}"
SAMPLE="${SAMPLE:-3}"
PHASE="${PHASE:-wsweep}"
SLOTS="${SLOTS:-30 60 120 240}"
REGIMES="${REGIMES:-fusion lowfusion}"
QUEEN_CONT="${QUEEN_CONT:-queen}"
PG_CONT="${PG_CONT:-postgres}"

OUT="$HERE/out"
mkdir -p "$OUT"
log(){ echo "[$(date -u +%FT%TZ)] $*"; }

if [ ! -x "$GOLOAD" ]; then
  log "ERROR: goload not found/executable at $GOLOAD (set GOLOAD=...)"; exit 1
fi

# Regime -> goload params: partitions push_batch producers consumers pop_batch
regime_params() {
  case "$1" in
    fusion)    echo "100 100 64 0 200" ;;   # cheap parse, high fusion -> tests if 1 engine holds the PG ceiling
    lowfusion) echo "2 1 200 0 200" ;;       # batch=1 -> poor fusion, heaviest per-batch engine work
    balanced)  echo "100 100 64 32 200" ;;   # push+pop+ack together (ack rides the push/ack engine)
    *) echo "" ;;
  esac
}

restart_queen() {
  local w="$1" side="$2"
  docker stop "$QUEEN_CONT" >/dev/null 2>&1; docker rm "$QUEEN_CONT" >/dev/null 2>&1
  docker run -d --ulimit nofile=65535:65535 --name "$QUEEN_CONT" -p 6632:6632 --network queen \
    -e PG_HOST="$PG_CONT" -e PG_PASSWORD=postgres \
    -e NUM_WORKERS="$w" -e DB_POOL_SIZE="$DB_POOL" -e SIDECAR_POOL_SIZE="$side" \
    -e RETENTION_BATCH_SIZE=50000 -e RETENTION_INTERVAL=5000 -e RETENTION_PARALLELISM="${RETENTION_PARALLELISM:-8}" \
    -e QUEEN_PUSH_MAX_CONCURRENT="${QUEEN_PUSH_MAX_CONCURRENT:-24}" -e QUEEN_PUSH_MAX_HOLD_MS="${QUEEN_PUSH_MAX_HOLD_MS:-40}" \
    -e QUEEN_PUSH_PREFERRED_BATCH_SIZE="${QUEEN_PUSH_PREFERRED_BATCH_SIZE:-50}" -e QUEEN_PUSH_MAX_BATCH_SIZE="${QUEEN_PUSH_MAX_BATCH_SIZE:-500}" \
    -e QUEEN_CONCURRENCY_MODE="${QUEEN_CONCURRENCY_MODE:-vegas}" -e QUEEN_VEGAS_MAX_LIMIT="${QUEEN_VEGAS_MAX_LIMIT:-32}" \
    smartnessai/queen-mq:"$TAG" >/dev/null
  local ok=0 i
  for i in $(seq 1 90); do
    curl -sf "$BROKER_URL/api/v1/status" >/dev/null 2>&1 && { ok=1; break; }
    sleep 1
  done
  if [ "$ok" != 1 ]; then log "  QUEEN-FAILED (W=$w SIDE=$side)"; docker logs "$QUEEN_CONT" 2>&1 | tail -5; return 1; fi
  sleep 3   # let the event loop and pool settle before load
  return 0
}

truncate_messages() {
  docker exec "$PG_CONT" psql -U postgres -d postgres -tAc \
    "TRUNCATE queen.messages CASCADE;" >/dev/null 2>&1 || true
}

run_cell() {
  local regime="$1" w="$2" side="$3"
  local params; params="$(regime_params "$regime")"
  if [ -z "$params" ]; then log "  skip unknown regime $regime"; return; fi
  read -r PARTS PBATCH PRODS CONS POPB <<<"$params"

  local cellout="$OUT/$regime"
  mkdir -p "$cellout"
  local tag="w${w}_s${side}"
  log "CELL regime=$regime W=$w SIDE=$side parts=$PARTS pushBatch=$PBATCH prod=$PRODS cons=$CONS dur=${DURATION}s"

  restart_queen "$w" "$side" || return
  truncate_messages

  # background sampler for this cell
  bash "$HERE/mon-engine.sh" "$cellout/$tag.tsv" "$SAMPLE" "$QUEEN_CONT" "$PG_CONT" &
  local mon_pid=$!

  # push-only when CONS=0 (isolates the push/ack engine); high completed-retention
  # so nothing is deleted and n_tup_ins == ground-truth push rate.
  "$GOLOAD" -url "$BROKER_URL" -queue "$QUEUE" \
    -partitions "$PARTS" -producers "$PRODS" -consumers "$CONS" \
    -push-batch "$PBATCH" -pop-batch "$POPB" \
    -duration "$DURATION" -report 5 \
    -completed-retention 100000 -pending-retention 0 \
    > "$cellout/$tag.goload.log" 2>&1 || log "  goload returned nonzero (see $tag.goload.log)"

  kill "$mon_pid" >/dev/null 2>&1; wait "$mon_pid" 2>/dev/null || true
  local final; final=$(grep -E '^\[final\]' "$cellout/$tag.goload.log" | tail -1)
  log "  done: $final"
}

log "engine-scaling sweep: PHASE=$PHASE regimes=[$REGIMES] WORKERS=[$WORKERS] SIDE=$SIDE dur=${DURATION}s -> $OUT"

case "$PHASE" in
  wsweep|both)
    for regime in $REGIMES; do
      for w in $WORKERS; do
        run_cell "$regime" "$w" "$SIDE"
      done
    done
    ;;
esac
case "$PHASE" in
  slots|both)
    # single engine (W=1), sweep total slots to find the PG-concurrency knee.
    for regime in $REGIMES; do
      for s in $SLOTS; do
        run_cell "$regime" 1 "$s"
      done
    done
    ;;
esac

log "sweep complete. Summarize with: python3 $HERE/summarize-engine.py $OUT"
