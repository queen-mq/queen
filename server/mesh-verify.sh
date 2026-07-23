#!/usr/bin/env bash
# mesh-verify.sh — manual acceptance check for the framed TCP inter-broker mesh
# (server/src/mesh.rs), Phase 1 of the udp→tcp mesh work.
#
# Runs 2 broker processes on localhost against ONE Postgres, cross-wired as mesh
# peers with an HMAC secret, and checks the three acceptance behaviours:
#
#   1. WAKE  — a push on node A wakes a long-poll pop parked on node B "at once"
#              (via the mesh), not on B's own poll backoff.
#   2. DEAD  — killing A makes B log the dead peer, and B keeps serving.
#   3. RECON — restarting A re-establishes the mesh with no operator action.
#
# Requires: a reachable Postgres and python3 (for millisecond timing). The broker
# self-applies its schema at boot, so a fresh empty database is fine.
#
# Usage:
#   PG_HOST=localhost PG_PORT=5432 PG_USER=postgres PG_PASSWORD=postgres \
#   PG_DATABASE=postgres ./mesh-verify.sh
#
# All PG_* default to the values below. Override QUEEN_SYNC_SECRET to test auth.
set -uo pipefail
cd "$(dirname "$0")"

export PG_HOST="${PG_HOST:-localhost}"
export PG_PORT="${PG_PORT:-5432}"
export PG_USER="${PG_USER:-postgres}"
export PG_PASSWORD="${PG_PASSWORD:-postgres}"
export PG_DATABASE="${PG_DATABASE:-postgres}"
SECRET="${QUEEN_SYNC_SECRET:-meshsecret}"

# Node A: HTTP :6632, mesh :6633. Node B: HTTP :6732, mesh :6733. Cross-peered.
A_HTTP=6632; A_MESH=6633
B_HTTP=6732; B_MESH=6733
Q="mesh_verify_$RANDOM"

log(){ echo "[mesh-verify] $*"; }
now_ms(){ python3 -c 'import time;print(int(time.time()*1000))'; }
fail(){ log "FAIL: $*"; cleanup; exit 1; }

BIN=target/debug/queen-seg
A_PID=""; B_PID=""; CONS_PID=""
A_LOG="$(mktemp)"; B_LOG="$(mktemp)"; CONS_OUT="$(mktemp)"

cleanup(){
  [ -n "$CONS_PID" ] && kill "$CONS_PID" 2>/dev/null
  [ -n "$A_PID" ] && kill "$A_PID" 2>/dev/null
  [ -n "$B_PID" ] && kill "$B_PID" 2>/dev/null
  wait 2>/dev/null
  rm -f "$A_LOG" "$B_LOG" "$CONS_OUT"
}
trap cleanup EXIT

wait_http(){ # port
  for _ in $(seq 1 100); do
    curl -sf "http://127.0.0.1:$1/status" >/dev/null 2>&1 && return 0
    sleep 0.2
  done
  return 1
}

start_a(){
  QUEEN_SYNC_SECRET="$SECRET" QUEEN_SERVER_ID=node-A \
  PORT="$A_HTTP" QUEEN_MESH_PORT="$A_MESH" QUEEN_MESH_PEERS="127.0.0.1:$B_MESH" \
    "$BIN" >>"$A_LOG" 2>&1 &
  A_PID=$!
}

log "build (debug)"; cargo build >/dev/null 2>&1 || fail "cargo build failed"

log "start node B (HTTP :$B_HTTP mesh :$B_MESH, peer 127.0.0.1:$A_MESH)"
# B's parked pops use a deliberately LONG re-query interval so that, without a
# mesh wake, a parked pop would NOT re-query for 10s. A sub-second response then
# proves the wake came from the mesh, not from B's own backoff finding the row.
QUEEN_SYNC_SECRET="$SECRET" QUEEN_SERVER_ID=node-B \
PORT="$B_HTTP" QUEEN_MESH_PORT="$B_MESH" QUEEN_MESH_PEERS="127.0.0.1:$A_MESH" \
POP_WAIT_INITIAL_INTERVAL_MS=10000 POP_WAIT_BACKOFF_THRESHOLD=1000 POP_WAIT_MAX_INTERVAL_MS=10000 \
  "$BIN" >>"$B_LOG" 2>&1 &
B_PID=$!

log "start node A (HTTP :$A_HTTP mesh :$A_MESH, peer 127.0.0.1:$B_MESH)"
start_a

wait_http "$A_HTTP" || fail "node A never came up"
wait_http "$B_HTTP" || fail "node B never came up"
sleep 1  # let the mesh connect + handshake

log "mesh stats on B: $(curl -s http://127.0.0.1:$B_HTTP/internal/api/shared-state/stats)"

# ---- configure a queue (segments, dedup off, short lease) via A ----
curl -s -X POST "http://127.0.0.1:$A_HTTP/api/v1/configure" -H 'Content-Type: application/json' \
  -d "{\"queue\":\"$Q\",\"options\":{\"storage\":\"segments\",\"dedupWindowSeconds\":0,\"leaseTime\":60}}" >/dev/null \
  || fail "configure failed"

# =====================================================================
# 1) WAKE: park a long-poll consumer on B, push on A, measure the latency.
# =====================================================================
log "park a long-poll consumer on B (wait=true, timeout=30s)"
( t0=$(now_ms)
  curl -s "http://127.0.0.1:$B_HTTP/api/v1/pop/queue/$Q?wait=true&timeout=30000&autoAck=true&batch=10&partitions=10" >/dev/null
  t1=$(now_ms)
  echo $((t1 - t0)) >"$CONS_OUT"
) &
CONS_PID=$!
sleep 0.8  # ensure the pop is parked before we push

log "push 1 message on A → partition p0"
PUSH_AT=$(now_ms)
curl -s -X POST "http://127.0.0.1:$A_HTTP/api/v1/push" -H 'Content-Type: application/json' \
  -d "{\"items\":[{\"queue\":\"$Q\",\"partition\":\"p0\",\"payload\":{\"n\":1},\"transactionId\":\"tx-1\"}]}" >/dev/null \
  || fail "push failed"

wait "$CONS_PID" 2>/dev/null; CONS_PID=""
DONE_AT=$(now_ms)
CONS_ELAPSED=$(cat "$CONS_OUT")
WAKE_MS=$((DONE_AT - PUSH_AT))
log "consumer returned; consumer-side elapsed=${CONS_ELAPSED}ms, push→return=${WAKE_MS}ms"
if [ "$WAKE_MS" -lt 2000 ]; then
  log "PASS 1/3 WAKE: B woke within ${WAKE_MS}ms of the push (mesh wake, not the 10s backoff)"
else
  fail "B did not wake promptly (${WAKE_MS}ms ≥ 2s) — mesh wake likely did not fire"
fi

# =====================================================================
# 2) DEAD: kill A, expect B to log the dead peer and keep serving.
# =====================================================================
log "kill node A (pid $A_PID)"
kill "$A_PID" 2>/dev/null; A_PID=""
DEAD_OK=""
for _ in $(seq 1 80); do  # dead_threshold defaults to 5s; monitor ticks ~2.5s
  if grep -q "mesh peer node-A is DOWN" "$B_LOG"; then DEAD_OK=1; break; fi
  sleep 0.25
done
[ -n "$DEAD_OK" ] || fail "B never logged node-A as DOWN"
log "B logged: $(grep 'mesh peer node-A is DOWN' "$B_LOG" | tail -1)"
# B still serves (a fresh non-wait pop returns a well-formed HTTP status).
CODE=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$B_HTTP/api/v1/pop/queue/$Q?wait=false&batch=1")
[ "$CODE" = "200" ] || [ "$CODE" = "204" ] || fail "B stopped serving after A died (HTTP $CODE)"
log "PASS 2/3 DEAD: B logged the dead peer and still serves (pop → HTTP $CODE)"

# =====================================================================
# 3) RECON: restart A, expect the mesh to re-establish with no action.
# =====================================================================
log "restart node A"
start_a
wait_http "$A_HTTP" || fail "node A did not come back up"
RECON_OK=""
for _ in $(seq 1 80); do
  # B recovers A once A's outbound reconnects and heartbeats resume.
  if grep -q "mesh peer node-A recovered" "$B_LOG"; then RECON_OK=1; break; fi
  # Fallback: A's own stats show the outbound peer connected again.
  CONN=$(curl -s "http://127.0.0.1:$A_HTTP/internal/api/shared-state/stats" \
           | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("peers",[{}])[0].get("connected"))' 2>/dev/null)
  [ "$CONN" = "True" ] && { RECON_OK=1; break; }
  sleep 0.25
done
[ -n "$RECON_OK" ] || fail "mesh did not reconnect after A restarted"
log "PASS 3/3 RECON: mesh re-established after A restarted (no operator action)"

log "########## mesh-verify: ALL CHECKS PASSED ##########"
cleanup; trap - EXIT
exit 0
