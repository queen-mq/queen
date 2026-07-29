#!/usr/bin/env bash
# Build goload (campaign edition).
#
# GOWORK=off is REQUIRED, not cosmetic: this module lives inside the repo tree,
# so the repo's go.work (which lists only clients/client-cli and
# clients/client-go) claims the directory and `go build` refuses with
#   "current directory is contained in a module that is not one of the
#    workspace modules listed in go.work".
# With the workspace off, the module's own `replace` directive
# (../../../clients/client-go) resolves the in-tree client, which is exactly
# what we want: the loader is built against the SAME client-go the campaign is
# measuring.
#
#   ./build.sh              -> ./goload for the host platform
#   ./build.sh linux        -> ./goload-linux-amd64 (static, for the bench VM)
set -euo pipefail
cd "$(dirname "$0")"
export GOWORK=off
export PATH="$PATH:/usr/local/go/bin"

if [ "${1:-}" = "linux" ]; then
  GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o goload-linux-amd64 .
  ls -l goload-linux-amd64
else
  go build -o goload .
  ls -l goload
fi
