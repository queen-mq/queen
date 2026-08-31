#!/usr/bin/env bash
# @platformatic/kafka against an ALREADY-RUNNING queen-kafka facade.
#
#   ./run.sh                 # every scenario
#   ./run.sh group,resume    # a comma-separated subset
#
# This script starts NOTHING. The stack is compat/rig.sh's job, or yours; every
# address comes from the environment so it can slot into the rig later without
# an edit:
#
#   KAFKA_BOOTSTRAP       plaintext facade                 (default 127.0.0.1:19092)
#   RUN_ID                topic/group suffix               (default: epoch seconds)
#   KAFKA_PARTITIONS      QUEEN_KAFKA_DEFAULT_PARTITIONS   (default 8)
#   KAFKA_SASL_BOOTSTRAP  SASL listener; unset skips the SASL lane entirely
#   KAFKA_SASL_PROTOCOL   sasl_ssl (default) | sasl_plaintext
#   KAFKA_SASL_TOKEN      the Queen bearer token (this is the SASL *password*)
#   KAFKA_SSL_CA          PEM to verify the listener with; verification stays ON
#   KAFKA_SSL_INSECURE=1  skip verification (an advertised name with no SAN)
#   NODE_BIN              node to use; otherwise the newest satisfying one on PATH
#
# NODE VERSION. @platformatic/kafka declares engines ">= 22.22.0 || >= 24.6.0".
# npm only WARNS on an older runtime, and the client's zstd codec goes through
# node:zlib's zstd bindings, which older 22.x lines do not have. This script
# refuses to guess: it looks for a satisfying node (PATH first, then nvm) and
# says which one it picked.
set -uo pipefail
cd "$(dirname "$0")"

export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-127.0.0.1:19092}"
export RUN_ID="${RUN_ID:-$(date +%s)}"
export KAFKA_PARTITIONS="${KAFKA_PARTITIONS:-8}"
SCENARIO="${1:-all}"

satisfies() {
  # $1 = node binary. engines: >= 22.22.0 || >= 24.6.0
  local v
  v=$("$1" -p 'process.versions.node' 2>/dev/null) || return 1
  "$1" -e '
    const [a,b,c] = process.versions.node.split(".").map(Number)
    const okay = (a === 22 && (b > 22 || (b === 22 && c >= 0))) || (a === 23) || (a > 24) ||
                 (a === 24 && (b > 6 || (b === 6 && c >= 0)))
    process.exit(okay ? 0 : 1)
  ' 2>/dev/null || return 1
  echo "$v"
}

NODE="${NODE_BIN:-}"
if [ -z "$NODE" ]; then
  for candidate in node $(ls -d "$HOME"/.nvm/versions/node/*/bin/node 2>/dev/null | sort -Vr); do
    if command -v "$candidate" >/dev/null 2>&1 || [ -x "$candidate" ]; then
      if satisfies "$candidate" >/dev/null 2>&1; then NODE="$candidate"; break; fi
    fi
  done
fi
if [ -z "$NODE" ]; then
  NODE="$(command -v node || true)"
  if [ -z "$NODE" ]; then
    echo "no node on PATH" >&2
    exit 2
  fi
  echo "WARNING: no node satisfying >= 22.22.0 || >= 24.6.0 found; using $($NODE -p process.version) anyway." >&2
  echo "         the zstd lane in particular may fail for that reason and not the facade's." >&2
fi
echo "==> node $("$NODE" -p process.version) ($NODE)"

if [ ! -d node_modules ]; then
  echo "==> npm install (pure JS, no native build)"
  npm install --no-audit --no-fund || exit 1
fi

"$NODE" run.mjs "$KAFKA_BOOTSTRAP" "$RUN_ID" "$SCENARIO"
exit $?
