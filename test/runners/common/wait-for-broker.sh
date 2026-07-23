#!/usr/bin/env bash
# Block until every URL in $QUEEN_WAIT_URLS answers a healthy /health.
#
# The broker's /health returns HTTP 200 only after it has connected to Postgres
# AND applied its schema (it binds the HTTP listener after schema apply), and
# 503 until then — so `curl -f` (fail on >=400) is a combined broker+PG+schema
# readiness gate. The runtime broker image has no shell tools, which is why the
# wait lives here in the runner (which has curl) rather than as a compose
# healthcheck on the broker.
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
