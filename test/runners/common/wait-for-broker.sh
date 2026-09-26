#!/usr/bin/env bash
# Block until every URL in $QUEEN_WAIT_URLS answers a healthy /health.
#
# A raft broker's /health answers 503 ("settling") until its state machine is
# open and ready — on a cluster node that also means a leader is known and its
# apply has caught up — and 200 ("healthy") from then on. So `curl -f` (fail on
# >=400) is the whole readiness gate, per node. The runtime broker image carries
# no probe tooling of its own, which is why the wait lives here in the runner
# (which has curl) rather than as a compose healthcheck on the broker.
set -u

: "${QUEEN_WAIT_URLS:?QUEEN_WAIT_URLS must be set (space-separated health URLs)}"
timeout="${QUEEN_WAIT_TIMEOUT:-180}"

for url in $QUEEN_WAIT_URLS; do
  printf 'wait-for-broker: %s ' "$url"
  ok=""
  for _ in $(seq 1 "$timeout"); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      ok=1
      break
    fi
    printf '.'
    sleep 1
  done
  if [ -z "$ok" ]; then
    printf '\nwait-for-broker: TIMEOUT after %ss waiting for %s\n' "$timeout" "$url" >&2
    exit 97
  fi
  printf ' up\n'
done
