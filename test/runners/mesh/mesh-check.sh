#!/usr/bin/env bash
# HA mesh assertion: prove the two brokers formed an authenticated TCP mesh over
# the shared Postgres. Uses only public/admin JSON endpoints (open because the
# harness runs with JWT disabled). The deep behavioral gate (cross-replica WAKE,
# DEAD, RECON latencies) remains server/mesh-verify.sh — see test/README.md.
set -uo pipefail

A="${QUEEN_A_URL:?QUEEN_A_URL not set}"
B="${QUEEN_B_URL:?QUEEN_B_URL not set}"

export QUEEN_WAIT_URLS="$A/health $B/health"
/usr/local/bin/wait-for-broker

echo "mesh-check: allowing time for dial + HELLO handshake"
sleep 3

check_node() {
  local name="$1" url="$2" stats connected hsf alive ok=1
  stats="$(curl -fsS "$url/internal/api/shared-state/stats" 2>/dev/null)" \
    || { echo "  $name: /internal/api/shared-state/stats unreachable"; return 1; }
  echo "  $name stats: $(printf '%s' "$stats" | head -c 600)"
  connected="$(printf '%s' "$stats" | jq -r 'try (.peers[0].connected) // false')"
  hsf="$(printf '%s' "$stats" | jq -r 'try (.handshake_failures) // 0')"
  alive="$(printf '%s' "$stats" | jq -r 'try (.servers_alive) // 0')"
  [ "$connected" = "true" ] || { echo "  $name: peer NOT connected (connected=$connected)"; ok=0; }
  [ "$hsf" = "0" ]          || { echo "  $name: handshake_failures=$hsf (sync-secret mismatch?)"; ok=0; }
  { [ "${alive}" -ge 1 ] 2>/dev/null; } || { echo "  $name: servers_alive=$alive"; ok=0; }
  [ "$ok" = 1 ] && echo "  $name: OK (peer connected, 0 handshake failures, servers_alive=$alive)"
  return $((1 - ok))
}

fail=0
echo "== queen-a perspective =="; check_node queen-a "$A" || fail=1
echo "== queen-b perspective =="; check_node queen-b "$B" || fail=1

if [ "$fail" = 0 ]; then echo "MESH: PASS"; else echo "MESH: FAIL"; fi
exit "$fail"
