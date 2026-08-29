#!/bin/bash
# Replace ONE broker with the current queen:soak image, preserving its exact
# environment, and leaving Postgres (and the accumulated data) alone.
#
#   ./roll-broker.sh cell-broker-b [EXTRA_ENV=VALUE ...]
#
# Rolls one at a time on purpose: under active/passive the passive side goes
# first, and a mixed old/new pair briefly serving the same cell is exactly the
# compatibility path the autopilot design requires and nothing else tests.
set -euo pipefail
B=${1:?usage: roll-broker.sh <cell-broker-a|cell-broker-b> [ENV=VAL ...]}
shift || true
IMG=${IMG:-queen:soak}

# Carry the existing env forward verbatim — it holds the generated PG password,
# the mesh peer wiring and the tenancy flags. Re-deriving them here would be a
# second source of truth that silently drifts from soak-cell.sh.
mapfile -t ENVV < <(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$B" | grep -E '^[A-Z_]+=' | grep -vE '^(PATH|HOSTNAME)=')
PORT=$(docker inspect -f '{{range $p, $c := .NetworkSettings.Ports}}{{range $c}}{{.HostPort}}{{end}}{{end}}' "$B")
NET=$(docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}' "$B")

ARGS=()
for e in "${ENVV[@]}"; do ARGS+=(-e "$e"); done
for e in "$@"; do ARGS+=(-e "$e"); done

echo "  rolling $B -> $IMG (port $PORT, net $NET, $((${#ENVV[@]})) env vars preserved)"
docker rm -f "$B" >/dev/null
docker run -d --name "$B" --network "$NET" -p 127.0.0.1:"$PORT":6632 \
  --ulimit nofile=1048576:1048576 --restart unless-stopped \
  "${ARGS[@]}" "$IMG" >/dev/null

for _ in $(seq 1 90); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$PORT/health)" = "200" ] && break
  sleep 1
done
printf '  %s health: %s  image=%s\n' "$B" \
  "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$PORT/health)" \
  "$(docker inspect -f '{{.Config.Image}}' $B)"
docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$B" | grep -E 'AUTOPILOT|SUBSCRIPTION|STATS_INTERVAL' | sed 's/^/    /'
