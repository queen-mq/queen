#!/usr/bin/env bash
# rig.sh (Mac) — the local Pulsar rehearsal rig (SPEC §8), nothing else:
#   rig.sh up               docker network pbench (10.202.0.0/24) + pb1..pb3 (.2-.4, --memory 1300m) + pl1 (.5, 700m),
#                           image benchvm:24.04, tarball cache mounted read-only at /cache, --init (a PID 1 that reaps
#                           orphans, like systemd on a droplet: `sleep infinity` leaves exited daemons as zombies)
#   rig.sh deploy <steps>   stage a copy of the harness whose root hosts.env is rehearsal/hosts.env (common/deploy.sh
#                           reads hosts.env from the harness root; the shared one is left alone), then run the staged
#                           common/deploy.sh <steps> with BIN_ARCH=linux-arm64 (e.g. harness tune pulsar check)
#   rig.sh loader-tools     install.sh on pl1 too (pulsar-admin / pulsar-perf from the tarball for the rehearsal)
#   rig.sh ps | down        container status (memory) | docker rm -f + docker network rm (nothing left behind)
# Env: CACHE_DIR (host dir with the tarballs), STAGE (staging dir, default $TMPDIR/pulsar-rehearsal-stage),
#      CPUS (per container, default 2), BIN_ARCH (linux-arm64).
set -u
P=$(cd "$(dirname "$0")/.." && pwd); H=$(cd "$P/.." && pwd)
CACHE_DIR=${CACHE_DIR:-/private/tmp/claude-502/-Users-alice-Work-queen/5b439b2e-f94c-4699-9d71-489a54395651/scratchpad/cache}
STAGE=${STAGE:-${TMPDIR:-/tmp}/pulsar-rehearsal-stage}
CPUS=${CPUS:-2}
log() { echo "[$(date -u +%FT%TZ)] rig: $*"; }
case ${1:-} in
up)
  docker network inspect pbench >/dev/null 2>&1 || docker network create --subnet 10.202.0.0/24 pbench >/dev/null
  for n in 1 2 3; do
    docker rm -f pb$n >/dev/null 2>&1
    docker run -d --init --name pb$n --hostname pb$n --network pbench --ip 10.202.0.$((n + 1)) --memory 1300m --memory-swap 1300m \
      --cpus "$CPUS" --ulimit nofile=1048576:1048576 -v "$CACHE_DIR:/cache:ro" benchvm:24.04 >/dev/null || exit 1
  done
  docker rm -f pl1 >/dev/null 2>&1
  docker run -d --init --name pl1 --hostname pl1 --network pbench --ip 10.202.0.5 --memory 700m --memory-swap 700m \
    --cpus "$CPUS" --ulimit nofile=1048576:1048576 -v "$CACHE_DIR:/cache:ro" benchvm:24.04 >/dev/null || exit 1
  log "up: $(docker ps --filter network=pbench --format '{{.Names}}' | sort | tr '\n' ' ')" ;;
deploy)
  shift
  rm -rf "$STAGE"; mkdir -p "$STAGE"
  rsync -a --exclude 'rehearsal/runs' --exclude '.DS_Store' "$P" "$STAGE/"
  rsync -a --exclude '.DS_Store' "$H/common" "$H/report" "$H/SPEC.md" "$STAGE/"
  mkdir -p "$STAGE/kafka" "$STAGE/mqload"
  [ -d "$H/mqload/bin" ] && rsync -a "$H/mqload/bin" "$STAGE/mqload/"
  cp "$P/rehearsal/hosts.env" "$STAGE/hosts.env"
  log "staged $(du -sh "$STAGE" | cut -f1) in $STAGE; common/deploy.sh $*"
  BIN_ARCH=${BIN_ARCH:-linux-arm64} CACHE=/cache PROFILE=local "$STAGE/common/deploy.sh" "$@" ;;
loader-tools)
  . "$P/rehearsal/hosts.env"
  $SSH root@${L_PUB[0]} "CACHE=/cache $REMOTE_ROOT/pulsar/install.sh" < /dev/null ;;
ps) docker stats --no-stream --format '{{.Name}} {{.MemUsage}} {{.CPUPerc}}' pb1 pb2 pb3 pl1 2>/dev/null ;;
down)
  docker rm -f pb1 pb2 pb3 pl1 >/dev/null 2>&1
  docker network rm pbench >/dev/null 2>&1
  log "down: containers=$(docker ps -a --filter name='^p[bl][0-9]$' -q | wc -l | tr -d ' ') network=$(docker network ls -q --filter name='^pbench$' | wc -l | tr -d ' ')" ;;
*) sed -n '2,12p' "$0"; exit 2 ;;
esac
