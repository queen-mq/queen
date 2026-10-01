#!/usr/bin/env bash
# qr.sh — a local Queen raft cluster in Docker that mirrors the helm_v2 prod
# StatefulSet (pod names, headless DNS names, env, read-only root, uid 65532,
# restart policy "always" like a pod), for rehearsing recovery procedures.
#
#   qr.sh up [n]            create n nodes (default 3) on empty volumes
#   qr.sh start i [K=V..]   (re)create node i's container on its volume, extra env
#   qr.sh stop i            graceful stop (SIGTERM, like a pod delete without recreate)
#   qr.sh restart i         graceful restart (like kubectl delete pod)
#   qr.sh crash i           kill -9 the process; the restart policy brings it back
#   qr.sh kill i            kill -9 and keep it down (container stays stopped)
#   qr.sh rm i              remove node i's container (volume kept)
#   qr.sh wipe i            remove node i's container AND its volume (disk lost)
#   qr.sh sh i [cmd]        a shell on node i's volume in a helper container (node must be down)
#   qr.sh exec i cmd..      run a command inside node i's running container
#   qr.sh health [i]        /health of every node (or one)
#   qr.sh members           GET /api/v1/raft/members from the first node that answers
#   qr.sh logs i [n]        last n log lines of node i (default 50)
#   qr.sh errs i            WARN/ERROR lines of node i
#   qr.sh down              remove containers, volumes and network
#   qr.sh secrets           create secrets.env (random tokens/keys) if missing
#
# Env: QR_IMAGE (default local/queen:recovery), QR_N (node count for PEERS, default 3)
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
IMAGE="${QR_IMAGE:-local/queen:recovery}"
NET=qr
REL=queen-mq-v2
NS=queen
HEADLESS="$REL-headless.$NS.svc.cluster.local"
SECRETS="$HERE/secrets.env"
N="${QR_N:-$(cat "$HERE/.qr_n" 2>/dev/null || echo 3)}"

name() { echo "$REL-$1"; }
vol() { echo "qr-data-$1"; }
fqdn() { echo "$(name "$1").$HEADLESS"; }
port() { echo "1663$1"; }

secrets() {
  if [ ! -f "$SECRETS" ]; then
    {
      echo "QUEEN_RAFT_TOKEN=$(openssl rand -hex 32)"
      echo "JWT_SECRET=$(openssl rand -hex 32)"
      echo "QUEEN_PROXY_CP_TOKEN=$(openssl rand -hex 24)"
      echo "QUEEN_TOKEN=$(openssl rand -hex 24)"
      echo "QUEEN_PROXY_BOOTSTRAP_API_KEY=qk_live_$(openssl rand -hex 20)"
      echo "QUEEN_ENCRYPTION_KEY=$(openssl rand -hex 32)"
    } > "$SECRETS"
  fi
  # shellcheck disable=SC1090
  set -a; . "$SECRETS"; set +a
}

peers() {
  local s="" i
  for ((i = 0; i < N; i++)); do
    [ -n "$s" ] && s="$s,"
    s="$s$((i + 1))=$(fqdn $i):7400/$(fqdn $i):6632"
  done
  echo "$s"
}

net() { docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET" >/dev/null; }

ensure_vol() {
  local v; v="$(vol "$1")"
  if ! docker volume inspect "$v" >/dev/null 2>&1; then
    docker volume create "$v" >/dev/null
    docker run --rm -v "$v":/d --entrypoint /bin/sh "$IMAGE" -c 'true' >/dev/null 2>&1 || true
    docker run --rm --user 0 -v "$v":/d --entrypoint /bin/sh "$IMAGE" -c 'chown 65532:65532 /d' >/dev/null
  fi
}

start() {
  local i="$1"; shift
  secrets; net; ensure_vol "$i"
  docker rm -f "$(name "$i")" >/dev/null 2>&1 || true
  local extra=()
  for kv in "$@"; do extra+=(-e "$kv"); done
  # shellcheck disable=SC2016
  docker run -d --name "$(name "$i")" --hostname "$(name "$i")" \
    --network "$NET" --network-alias "$(fqdn "$i")" \
    --restart always --stop-timeout 90 \
    --read-only --tmpfs /tmp:rw,size=256m --user 65532:65532 \
    --memory 2g --cpus 2 \
    -p "$(port "$i"):6632" -p "1671$i:6711" \
    -v "$(vol "$i")":/var/lib/queen/raft \
    -e QUEEN_RAFT_REPLICATOR=openraft \
    -e QUEEN_RAFT_DIR=/var/lib/queen/raft \
    -e QUEEN_QLOG_SHARDS=4 \
    -e QUEEN_RAFT_DISK_HIGH_PCT="${QR_DISK_HIGH:-99}" \
    -e QUEEN_RAFT_DISK_LOW_PCT="${QR_DISK_LOW:-98}" \
    -e QUEEN_RAFT_NODE_ID=ordinal \
    -e QUEEN_RAFT_PEERS="$(peers)" \
    -e QUEEN_RAFT_TOKEN="$QUEEN_RAFT_TOKEN" \
    -e PORT=6632 -e JWT_ENABLED=false -e QUEEN_TENANCY_HEADER=false \
    -e LOG_LEVEL=info -e QUEEN_LOG_JSON=true \
    -e QUEEN_ENCRYPTION_KEY="$QUEEN_ENCRYPTION_KEY" \
    -e QUEEN_SERVER_ID="$(name "$i")" \
    -e QUEEN_KAFKA_EMBEDDED=true \
    -e QUEEN_KAFKA_ADVERTISED_ADDR="$(fqdn "$i"):9092" \
    -e QUEEN_TOKEN="$QUEEN_TOKEN" \
    -e QUEEN_PROXY_EMBEDDED=true -e QUEEN_PROXY_PORT=6711 -e QUEEN_PROXY_TENANT_HEADER=false \
    -e QUEEN_PROXY_SPOOL_DIR=/var/lib/queen/raft/proxy-spool \
    -e QUEEN_PROXY_JWT_SECRET="$JWT_SECRET" \
    -e QUEEN_PROXY_CP_TOKEN="$QUEEN_PROXY_CP_TOKEN" \
    -e QUEEN_PROXY_PUBLIC_URL=http://localhost:1671$i \
    -e QUEEN_PROXY_DEFAULT_CLUSTER=smartness \
    -e QUEEN_PROXY_AUTOPROVISION=true -e QUEEN_PROXY_AUTOPROVISION_TENANT=smartness \
    -e QUEEN_PROXY_DEFAULT_ROLE=viewer -e QUEEN_PROXY_ENFORCE=false \
    -e QUEEN_PROXY_OPERATOR_ENABLED=true -e QUEEN_PROXY_OPERATORS=ops@example.com \
    -e QUEEN_PROXY_BOOTSTRAP_TENANT=smartness -e QUEEN_PROXY_BOOTSTRAP_EMAIL=ops@example.com \
    -e QUEEN_PROXY_BOOTSTRAP_PLAN=dedicated-s \
    -e QUEEN_PROXY_BOOTSTRAP_API_KEY="$QUEEN_PROXY_BOOTSTRAP_API_KEY" \
    ${QR_ENV:+$(for kv in $QR_ENV; do printf -- '-e %s ' "$kv"; done)} \
    ${extra[@]+"${extra[@]}"} \
    --entrypoint /bin/sh "$IMAGE" -c '
      ord="${HOSTNAME##*-}"
      case "$ord" in ""|*[!0-9]*) echo "queen: cannot derive QUEEN_KAFKA_NODE_ID from HOSTNAME=$HOSTNAME" >&2; exit 1 ;; esac
      QUEEN_KAFKA_NODE_ID=$(( ord + 1 )); export QUEEN_KAFKA_NODE_ID
      exec /app/bin/queen' >/dev/null
  echo "started $(name "$i") (node $((i + 1))) on localhost:$(port "$i")"
}

health() {
  local i="$1"
  curl -s -m 2 "localhost:$(port "$i")/health" 2>/dev/null || echo "(no answer)"
}

cmd="${1:-}"; shift || true
case "$cmd" in
  up)
    n="${1:-3}"; echo "$n" > "$HERE/.qr_n"; N="$n"
    for ((i = 0; i < n; i++)); do start "$i"; done ;;
  start) start "$@" ;;
  secrets) secrets; echo "secrets in $SECRETS" ;;
  stop) docker stop -t 90 "$(name "$1")" >/dev/null && echo "stopped $(name "$1")" ;;
  restart) docker restart -t 90 "$(name "$1")" >/dev/null && echo "restarted $(name "$1")" ;;
  crash)
    pid=$(docker inspect -f '{{.State.Pid}}' "$(name "$1")")
    docker run --rm --pid=host --privileged alpine kill -9 "$pid"
    echo "kill -9 $(name "$1") (pid $pid); restart policy brings it back" ;;
  kill) docker kill -s KILL "$(name "$1")" >/dev/null && echo "killed $(name "$1") (stays down)" ;;
  rm) docker rm -f "$(name "$1")" >/dev/null && echo "removed container $(name "$1")" ;;
  wipe)
    docker rm -f "$(name "$1")" >/dev/null 2>&1 || true
    docker volume rm "$(vol "$1")" >/dev/null && echo "wiped $(name "$1"): container and volume $(vol "$1") gone" ;;
  sh)
    i="$1"; shift
    if [ $# -gt 0 ]; then
      docker run --rm -i --user 65532:65532 -v "$(vol "$i")":/var/lib/queen/raft -w /var/lib/queen/raft --entrypoint /bin/sh "$IMAGE" -c "$*"
    else
      docker run --rm -it --user 65532:65532 -v "$(vol "$i")":/var/lib/queen/raft -w /var/lib/queen/raft --entrypoint /bin/sh "$IMAGE"
    fi ;;
  exec) i="$1"; shift; docker exec "$(name "$i")" "$@" ;;
  health)
    if [ $# -gt 0 ]; then health "$1"; echo; else
      for ((i = 0; i < N; i++)); do printf '%s: ' "$(name "$i")"; health "$i"; echo; done; fi ;;
  members)
    for ((i = 0; i < N; i++)); do
      out=$(curl -s -m 3 "localhost:$(port "$i")/api/v1/raft/members" 2>/dev/null) && [ -n "$out" ] && { echo "$out"; exit 0; }
    done; echo "(no node answered)" ;;
  logs) docker logs --tail "${2:-50}" "$(name "$1")" 2>&1 ;;
  errs) docker logs "$(name "$1")" 2>&1 | grep -E '"level":"(ERROR|WARN)"|FATAL|panicked' || true ;;
  down)
    for c in $(docker ps -aq --filter "name=$REL-"); do docker rm -f "$c" >/dev/null; done
    for v in $(docker volume ls -q --filter name=qr-data-); do docker volume rm "$v" >/dev/null; done
    docker network rm "$NET" >/dev/null 2>&1 || true
    echo "cluster removed" ;;
  *) sed -n '2,25p' "$0"; exit 2 ;;
esac
