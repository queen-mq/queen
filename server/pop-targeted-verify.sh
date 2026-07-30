#!/usr/bin/env bash
# pop-targeted-verify.sh — manual acceptance check for Phase 2 (partition-hinted
# targeted pops, server/src/notify.rs hint mailbox + handlers/data.rs handle_pop).
#
# Single broker against ONE Postgres. Checks:
#
#   1. TARGETED — a consumer parked on a long-poll, then a single push to one
#                 partition, is served via the targeted single-partition pop
#                 (queen_pop_targeted_total increments), not the wildcard scan.
#   2. FALLBACK — with data already present and NO consumer parked, a pop is
#                 served by the wildcard scan (queen_pop_wildcard_total
#                 increments) — no regression on the throughput path.
#
# Requires a reachable Postgres and python3. Usage:
#   PG_HOST=localhost PG_PORT=5432 PG_USER=postgres PG_PASSWORD=postgres \
#   PG_DATABASE=postgres ./pop-targeted-verify.sh
set -uo pipefail
cd "$(dirname "$0")"

export PG_HOST="${PG_HOST:-localhost}"
export PG_PORT="${PG_PORT:-5432}"
export PG_USER="${PG_USER:-postgres}"
export PG_PASSWORD="${PG_PASSWORD:-postgres}"
export PG_DATABASE="${PG_DATABASE:-postgres}"

PORT=6642
Q="pop_targeted_$RANDOM"
BIN=target/debug/queen
PID=""; LOG="$(mktemp)"; CONS_OUT="$(mktemp)"

log(){ echo "[pop-targeted] $*"; }
metric(){ curl -s "http://127.0.0.1:$PORT/metrics/prometheus" | awk -v m="$1" '$1==m{print $2}'; }
fail(){ log "FAIL: $*"; cleanup; exit 1; }
cleanup(){ [ -n "$PID" ] && kill "$PID" 2>/dev/null; wait 2>/dev/null; rm -f "$LOG" "$CONS_OUT"; }
trap cleanup EXIT

log "build (debug)"; cargo build >/dev/null 2>&1 || fail "cargo build failed"

log "start broker on :$PORT (long pop backoff so a wake, not a re-query, must serve)"
PORT="$PORT" QUEEN_SYNC_ENABLED=false \
POP_WAIT_INITIAL_INTERVAL_MS=10000 POP_WAIT_BACKOFF_THRESHOLD=1000 POP_WAIT_MAX_INTERVAL_MS=10000 \
  "$BIN" >>"$LOG" 2>&1 &
PID=$!
for _ in $(seq 1 100); do curl -sf "http://127.0.0.1:$PORT/status" >/dev/null 2>&1 && break; sleep 0.2; done
curl -sf "http://127.0.0.1:$PORT/status" >/dev/null 2>&1 || fail "broker never came up"

curl -s -X POST "http://127.0.0.1:$PORT/api/v1/configure" -H 'Content-Type: application/json' \
  -d "{\"queue\":\"$Q\",\"options\":{\"storage\":\"segments\",\"dedupWindowSeconds\":0,\"leaseTime\":60}}" >/dev/null \
  || fail "configure failed"

# =====================================================================
# 1) TARGETED: park a consumer, push one message, expect the targeted path.
# =====================================================================
T0=$(metric queen_pop_targeted_total); T0=${T0:-0}
log "park a long-poll consumer (wait=true, timeout=30s), then push 1 msg → p0"
( curl -s "http://127.0.0.1:$PORT/api/v1/pop/queue/$Q?wait=true&timeout=30000&autoAck=true&batch=10&partitions=1" >"$CONS_OUT" ) &
CONS_PID=$!
sleep 0.8  # ensure the pop is parked (its first wildcard scan ran + it is waiting)

curl -s -X POST "http://127.0.0.1:$PORT/api/v1/push" -H 'Content-Type: application/json' \
  -d "{\"items\":[{\"queue\":\"$Q\",\"partition\":\"p0\",\"payload\":{\"n\":1,\"tag\":\"hinted\"},\"transactionId\":\"tx-1\"}]}" >/dev/null \
  || fail "push failed"

# Consumer must return quickly (a wake served it, not the 10s backoff).
WAITED=0
while kill -0 "$CONS_PID" 2>/dev/null; do
  sleep 0.1; WAITED=$((WAITED+1))
  [ "$WAITED" -ge 30 ] && fail "consumer did not return within 3s (wake/targeted pop failed)"
done
wait "$CONS_PID" 2>/dev/null
BODY=$(cat "$CONS_OUT")
echo "$BODY" | grep -q '"tag":"hinted"' || fail "consumer did not receive the pushed message: $BODY"

T1=$(metric queen_pop_targeted_total); T1=${T1:-0}
log "queen_pop_targeted_total: $T0 → $T1"
[ "$T1" -gt "$T0" ] || fail "targeted counter did not advance — pop was NOT served via the targeted path"
log "PASS 1/2 TARGETED: parked consumer + single push served via the targeted pop"

# =====================================================================
# 2) FALLBACK: data present, no consumer parked → wildcard scan.
# =====================================================================
W0=$(metric queen_pop_wildcard_total); W0=${W0:-0}
TT0=$(metric queen_pop_targeted_total)
log "push another msg, then do a non-wait pop (no parked consumer → wildcard path)"
curl -s -X POST "http://127.0.0.1:$PORT/api/v1/push" -H 'Content-Type: application/json' \
  -d "{\"items\":[{\"queue\":\"$Q\",\"partition\":\"p1\",\"payload\":{\"n\":2,\"tag\":\"flowing\"},\"transactionId\":\"tx-2\"}]}" >/dev/null
sleep 0.3
RESP=$(curl -s "http://127.0.0.1:$PORT/api/v1/pop/queue/$Q?wait=false&autoAck=true&batch=10&partitions=1")
echo "$RESP" | grep -q '"tag":"flowing"' || fail "wildcard pop did not return the flowing message: $RESP"
W1=$(metric queen_pop_wildcard_total); W1=${W1:-0}
TT1=$(metric queen_pop_targeted_total)
log "queen_pop_wildcard_total: $W0 → $W1 ; targeted stayed $TT0 → $TT1"
[ "$W1" -gt "$W0" ] || fail "wildcard counter did not advance on the flowing path"
[ "$TT1" = "$TT0" ] || fail "a non-parked flowing pop unexpectedly took the targeted path"
log "PASS 2/2 FALLBACK: flowing data with no parked consumer used the wildcard scan"

log "########## pop-targeted-verify: ALL CHECKS PASSED ##########"
cleanup; trap - EXIT
exit 0
