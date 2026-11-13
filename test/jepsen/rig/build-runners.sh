#!/usr/bin/env bash
# Builds the feed-stream runner bundle W19 (--workload feedstream) uploads to
# every Jepsen node (jepsen.queen.runners): node, the JS SDK and its
# conformance runner, the Python SDK and its dependencies, and the Go and Rust
# conformance runners built for linux/amd64.
#
# Run it on the Jepsen control node (Ubuntu 24.04, as root) after ctl-setup.sh
# has put the repository in $SRC:
#
#   SRC=/root/src OUT=/root/runners.tgz bash test/jepsen/rig/build-runners.sh
#
# The nodes need only python3 (Ubuntu has it): everything else is in the
# bundle. The latest node 24 and the latest Go 1.26 are read from their
# release indexes, so the script does not pin a version that may not exist.
set -euo pipefail

SRC=${SRC:-/root/src}
OUT=${OUT:-/root/runners.tgz}
WORK=$(mktemp -d)
B="$WORK/bundle"
mkdir -p "$B/clients/conformance/feed/runners"

echo "== node 24"
NODE_V=$(curl -fsSL https://nodejs.org/dist/index.json |
  python3 -c 'import json,sys; print(next(v["version"] for v in json.load(sys.stdin) if v["version"].startswith("v24.")))')
curl -fsSL "https://nodejs.org/dist/$NODE_V/node-$NODE_V-linux-x64.tar.xz" | tar -xJ -C "$WORK"
mv "$WORK/node-$NODE_V-linux-x64" "$B/node"
export PATH="$B/node/bin:$PATH"
node --version

echo "== JS SDK and runner"
mkdir -p "$B/clients/client-js"
tar -C "$SRC/clients/client-js" --exclude=node_modules --exclude=test-v2 -cf - . | tar -C "$B/clients/client-js" -xf -
(cd "$B/clients/client-js" && npm ci --omit=dev --ignore-scripts)
cp "$SRC/clients/conformance/feed/runners/js.mjs" "$B/clients/conformance/feed/runners/"

echo "== Python SDK and httpx"
mkdir -p "$B/clients/client-py"
tar -C "$SRC/clients/client-py" --exclude=.venv --exclude=__pycache__ -cf - . | tar -C "$B/clients/client-py" -xf -
python3 -m pip install --quiet --target "$B/py-deps" 'httpx>=0.27' || \
  python3 -m pip install --quiet --break-system-packages --target "$B/py-deps" 'httpx>=0.27'

echo "== Go runner"
if ! command -v go >/dev/null || ! go version | grep -q 'go1\.2[6-9]'; then
  GO_V=$(curl -fsSL 'https://go.dev/dl/?mode=json' |
    python3 -c 'import json,sys; print(next(r["version"] for r in json.load(sys.stdin) if r["version"].startswith("go1.26")))')
  curl -fsSL "https://go.dev/dl/$GO_V.linux-amd64.tar.gz" | tar -xz -C "$WORK"
  export PATH="$WORK/go/bin:$PATH"
fi
go version
(cd "$SRC/clients/client-go" && GOWORK=off go build -o "$B/go-runner" ./tests/feed_conformance)

echo "== Rust runner"
(cd "$SRC/clients/client-rust" && CARGO_INCREMENTAL=0 cargo build --release --quiet --example feed_conformance)
cp "$SRC/clients/client-rust/target/release/examples/feed_conformance" "$B/rust-runner"

echo "== bundle"
tar -czf "$OUT" -C "$B" .
ls -la "$OUT"
md5sum "$OUT"
rm -rf "$WORK"
