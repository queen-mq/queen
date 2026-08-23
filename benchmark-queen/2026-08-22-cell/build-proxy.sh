#!/bin/bash
# Build the proxy image from source on the CELL VM.
#
#   ./build-proxy.sh <cell-ip>
#
# The 2026-08-23 proxy changes (coalesced registry persist, single-flight cache,
# batched last_used_at, PXDB_TIMEOUT_MS) are not on ghcr, so the soak has to
# build them.
#
# The build context is the REPO ROOT, not proxy/ — proxy/Dockerfile embeds TWO
# Vite builds via rust-embed, one of which lives outside its own directory:
#   src/webapp.rs  #[folder = "../server/webapp/dist"]   <- the broker dashboard
#   src/console.rs #[folder = "console/dist"]            <- proxy/console
# rust-embed resolves both against the crate manifest dir and hard-errors at
# compile time if either is missing, so both frontends are build dependencies.
#
# Building ON the VM rather than locally: the Mac is arm64 and the cell is
# amd64, so a local build would need buildx emulation (slow and a different
# binary from the one under test).
set -euo pipefail

CELL=${1:?usage: build-proxy.sh <cell-ip>}
REPO=/Users/alice/Work/queen
TAG=${TAG:-queen-proxy:soak}

echo "=== syncing source to $CELL (excluding build artefacts) ==="
rsync -az --delete \
  --exclude '.git' --exclude 'node_modules' --exclude 'target' \
  --exclude 'benchmark-queen' --exclude '*.log' --exclude 'dist' \
  "$REPO/" "root@$CELL:/root/queen-src/"

echo "=== building $TAG on the VM ==="
ssh -o BatchMode=yes "root@$CELL" "cd /root/queen-src && \
  DOCKER_BUILDKIT=1 docker build -f proxy/Dockerfile -t $TAG . 2>&1 | tail -25"

echo "=== verify ==="
ssh -o BatchMode=yes "root@$CELL" "docker images --format '  {{.Repository}}:{{.Tag}}  {{.Size}}  {{.CreatedSince}}' | grep queen-proxy | head -3"
