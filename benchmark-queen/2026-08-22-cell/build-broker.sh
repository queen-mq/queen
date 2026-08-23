#!/bin/bash
# Build the BROKER image from source on the CELL VM.
#
#   ./build-broker.sh <cell-ip>
#
# The 2026-08-23 hot-list change (full walk routed through
# log_hotlist_reseed_window_v1 with p_cutoff pinned to '-infinity') is not on
# ghcr, so the soak has to build it.
#
# Build context is the REPO ROOT, not server/ — server/Cargo.toml takes
# queen-protocol by the relative path ../crates/queen-protocol.
#
# server/webapp/dist IS included in the sync: handlers/static_files.rs embeds it
# with rust_embed. The Dockerfile mkdir -p's it so a missing dist still compiles,
# but that silently ships an empty dashboard — build what production builds.
#
# The SQL is include_str!-embedded, so an edit to server/sql only takes effect
# through a rebuild. That is the point of this script.
set -euo pipefail

CELL=${1:?usage: build-broker.sh <cell-ip>}
REPO=/Users/alice/Work/queen
TAG=${TAG:-queen:soak}

echo "=== syncing source to $CELL ==="
rsync -az --delete \
  --exclude '.git' --exclude 'node_modules' --exclude 'target' \
  --exclude 'benchmark-queen' --exclude '*.log' \
  "$REPO/" "root@$CELL:/root/queen-src/"

echo "=== building $TAG on the VM (release build, several minutes) ==="
ssh -o BatchMode=yes "root@$CELL" "cd /root/queen-src && \
  DOCKER_BUILDKIT=1 docker build -f server/Dockerfile -t $TAG . 2>&1 | tail -20"

echo "=== verify ==="
ssh -o BatchMode=yes "root@$CELL" "docker images --format '  {{.Repository}}:{{.Tag}}  {{.Size}}  {{.CreatedSince}}' | grep -E '^  queen:' | head -3"
