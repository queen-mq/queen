#!/usr/bin/env bash
# deploy.sh <deploy steps...> — common/deploy.sh for the rehearsal. deploy.sh reads <harness root>/hosts.env, and the
# real hosts.env belongs to VM day, so this stages a copy of the harness (common kafka pulsar report SPEC.md +
# mqload/bin/linux-arm64) under $STAGE with rehearsal/hosts.env as its hosts.env, then runs the STAGED common/deploy.sh.
# Example: kafka/rehearsal/deploy.sh harness tune kafka
set -u
H=$(cd "$(dirname "$0")/../.." && pwd)
STAGE=${STAGE:-/private/tmp/claude-502/-Users-alice-Work-queen/5b439b2e-f94c-4699-9d71-489a54395651/scratchpad/kreh-stage}
rm -rf "$STAGE"; mkdir -p "$STAGE/mqload/bin"
( cd "$H" && COPYFILE_DISABLE=1 tar --no-xattrs -cf - common kafka pulsar report SPEC.md 2>/dev/null ) | ( cd "$STAGE" && tar -xf - )
rm -rf "$STAGE/kafka/rehearsal"
[ -d "$H/mqload/bin/linux-arm64" ] && cp -R "$H/mqload/bin/linux-arm64" "$STAGE/mqload/bin/"
cp "$H/kafka/rehearsal/hosts.env" "$STAGE/hosts.env"
BIN_ARCH=linux-arm64 exec bash "$STAGE/common/deploy.sh" "$@"
