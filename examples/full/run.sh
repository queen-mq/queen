#!/usr/bin/env bash
# Run every complete example, in all three languages, against one broker.
#
# These programs are what the documentation's "Full examples" section shows,
# included verbatim through the snippet pipeline. Each one asserts its own
# outcome and exits non-zero on failure, so a green run here is what makes the
# documentation's claim that the examples are tested mean something.
#
#   examples/full/run.sh                          # against http://localhost:6632
#   QUEEN_URL=http://localhost:6699 examples/full/run.sh
#
# Needs: node 22+, python 3.9+ with httpx, go 1.24+, rust 1.75+. The clients are
# taken from this repository, not from the registries.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
export QUEEN_URL="${QUEEN_URL:-http://localhost:6632}"

echo "broker: $QUEEN_URL"
if ! curl -fsS "$QUEEN_URL/health" >/dev/null 2>&1; then
  echo "no broker answering at $QUEEN_URL/health" >&2
  echo "start one with:  docker run --name queen-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:16" >&2
  echo "                 cd $ROOT/server && cargo run" >&2
  exit 1
fi

pass=0
fail=0
failed_names=()

run() {
  local name="$1"
  shift
  printf '  %-40s ' "$name"
  local out
  if out=$("$@" 2>&1); then
    echo "ok"
    pass=$((pass + 1))
  else
    echo "FAILED"
    printf '%s\n' "$out" | tail -20 | sed 's/^/      /'
    fail=$((fail + 1))
    failed_names+=("$name")
  fi
}

# The JavaScript examples resolve `queen-mq` to ../../clients/client-js through
# a file: dependency, so install once before the first run.
if [ ! -d "$HERE/js/node_modules" ]; then
  echo "installing the JavaScript example dependencies"
  (cd "$HERE/js" && npm install --silent) || exit 1
fi

echo
echo "JavaScript"
for f in 01-produce-consume 02-ordering-and-dedup 03-pipeline-transaction; do
  run "$f" env -C "$HERE/js" node "$f.mjs"
done

echo
echo "Python"
for f in 01_produce_consume 02_ordering_and_dedup 03_pipeline_transaction; do
  run "$f" env -C "$ROOT" PYTHONPATH=clients/client-py python3 "examples/full/py/$f.py"
done

# The repository's go.work does not list this module, so the examples carry a
# replace directive instead and run with the workspace off.
echo
echo "Go"
for f in produce-consume ordering-and-dedup pipeline-transaction; do
  run "$f" env -C "$HERE/go" GOWORK=off go run "./$f"
done

# The Rust examples take the client by path, so cargo builds it from this tree.
# Compile once up front: otherwise the first `run` would time its own build.
echo
echo "Rust"
(cd "$HERE/rust" && cargo build --quiet) || exit 1
for f in produce-consume ordering-and-dedup pipeline-transaction; do
  run "$f" env -C "$HERE/rust" cargo run --quiet --bin "$f"
done

echo
if [ "$fail" -gt 0 ]; then
  echo "$pass passed, $fail failed: ${failed_names[*]}"
  exit 1
fi
echo "$pass passed, 0 failed"
