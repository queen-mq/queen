#!/usr/bin/env bash
# Run the complete applications, in every language, against one broker.
#
# These are the programs the documentation's Full examples section shows: whole
# applications rather than fragments, each one asserting the property it exists
# to demonstrate. A green run here is what lets those pages claim they work.
#
#   examples/apps/run.sh                          # against http://localhost:6632
#   QUEEN_URL=http://localhost:6642 examples/apps/run.sh
#   QUEEN_URL=... examples/apps/run.sh js py       # a subset of the languages
#
# The rate limiter exists only where the client has a streaming SDK (js, py, go,
# rust). PHP, C++ and the plain HTTP client carry chat and webhooks.
#
# The exactly-once application needs the key/value surface and the saga needs
# that one plus timers. Every broker serves both, so there is nothing to probe
# for and nothing to skip: a cell that refuses them is an operator's kill switch
# or a quota, and each program says which when it meets one.
#
# Needs, per language: node 22+, python 3.9+, go 1.24+, rust 1.75+, php 8.3+, a
# C++17 compiler, and curl with jq. A language whose toolchain is missing is
# skipped with a note rather than failing the run.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
export QUEEN_URL="${QUEEN_URL:-http://localhost:6632}"

LANGS=("$@")
[ ${#LANGS[@]} -eq 0 ] && LANGS=(js py go rust php cpp http)
wanted() { for l in "${LANGS[@]}"; do [ "$l" = "$1" ] && return 0; done; return 1; }

echo "broker: $QUEEN_URL"
if ! curl -fsS "$QUEEN_URL/health" >/dev/null 2>&1; then
  echo "no broker answering at $QUEEN_URL/health" >&2
  exit 1
fi

# The applications one language carries, in run order. Go has no exactly-once
# program and the plain HTTP client no rate limiter; the other languages are
# literal lists at their own call site, because nothing about them varies.
apps_for() {
  case "$1" in
    js) printf 'chat webhooks rate-limiter exactly-once saga' ;;
    py) printf 'chat webhooks rate_limiter exactly_once saga' ;;
    go) printf 'chat webhooks rate-limiter saga' ;;
    http) printf 'chat webhooks exactly-once saga' ;;
  esac
  return 0
}

pass=0
fail=0
skip=0
failed_names=()

run() {
  local name="$1"
  shift
  printf '  %-30s ' "$name"
  local out
  if out=$("$@" 2>&1); then
    echo "ok"
    pass=$((pass + 1))
  else
    echo "FAILED"
    printf '%s\n' "$out" | tail -25 | sed 's/^/      /'
    fail=$((fail + 1))
    failed_names+=("$name")
  fi
}

skip_lang() {
  echo "  (skipped: $1)"
  skip=$((skip + 1))
}

if wanted js; then
  echo
  echo "JavaScript"
  if command -v node >/dev/null; then
    [ -d "$HERE/js/node_modules" ] || (cd "$HERE/js" && npm install --silent)
    for f in $(apps_for js); do
      run "$f" env -C "$HERE/js" node "$f.mjs"
    done
  else
    skip_lang "no node"
  fi
fi

if wanted py; then
  echo
  echo "Python"
  if command -v python3 >/dev/null; then
    for f in $(apps_for py); do
      [ -f "$HERE/py/$f.py" ] && run "$f" env -C "$ROOT" PYTHONPATH=clients/client-py python3 "examples/apps/py/$f.py"
    done
  else
    skip_lang "no python3"
  fi
fi

if wanted go; then
  echo
  echo "Go"
  if command -v go >/dev/null && [ -f "$HERE/go/go.mod" ]; then
    for f in $(apps_for go); do
      [ -d "$HERE/go/$f" ] && run "$f" env -C "$HERE/go" GOWORK=off go run "./$f"
    done
  else
    skip_lang "no go toolchain or no go module here"
  fi
fi

if wanted rust; then
  echo
  echo "Rust"
  if command -v cargo >/dev/null && [ -f "$HERE/rust/Cargo.toml" ]; then
    (cd "$HERE/rust" && cargo build --quiet) || exit 1
    for f in chat webhooks rate_limiter; do
      [ -f "$HERE/rust/src/bin/$f.rs" ] && run "$f" env -C "$HERE/rust" cargo run --quiet --bin "$f"
    done
  else
    skip_lang "no cargo or no crate here"
  fi
fi

if wanted php; then
  echo
  echo "PHP"
  if command -v php >/dev/null && [ -d "$HERE/php" ]; then
    if [ ! -d "$ROOT/clients/client-laravel/vendor" ]; then
      command -v composer >/dev/null && (cd "$ROOT/clients/client-laravel" && composer install --quiet --no-interaction)
    fi
    [ -e "$HERE/php/vendor" ] || ln -sfn ../../../clients/client-laravel/vendor "$HERE/php/vendor"
    for f in chat webhooks; do
      [ -f "$HERE/php/$f.php" ] && run "$f" env -C "$HERE/php" php "$f.php"
    done
  else
    skip_lang "no php"
  fi
fi

if wanted cpp; then
  echo
  echo "C++"
  # Same header and library set as the tutorials: see examples/tutorials/run.sh
  # for why json.hpp, threadpool.hpp and OpenSSL are all required.
  if ! command -v c++ >/dev/null; then
    skip_lang "no c++ compiler"
  elif [ ! -d "$HERE/cpp" ]; then
    skip_lang "no cpp applications here"
  elif [ ! -f "$ROOT/clients/server/vendor/json.hpp" ] || [ ! -f "$ROOT/clients/server/include/threadpool.hpp" ]; then
    skip_lang "clients/server/{vendor/json.hpp,include/threadpool.hpp} missing (see test/runners/cpp/Dockerfile)"
  else
    cxx=(-std=c++17 -O1 -pthread -I"$ROOT/clients/client-cpp" -I"$ROOT/clients/server/vendor")
    ld=(-lssl -lcrypto -lpthread)
    if brew_prefix="$(brew --prefix 2>/dev/null)" && [ -n "$brew_prefix" ]; then
      cxx+=(-I"$brew_prefix/include")
      if ssl_prefix="$(brew --prefix openssl 2>/dev/null)" && [ -n "$ssl_prefix" ]; then
        cxx+=(-I"$ssl_prefix/include")
        ld+=(-L"$ssl_prefix/lib")
      fi
    fi
    for f in chat webhooks; do
      [ -f "$HERE/cpp/$f.cpp" ] || continue
      bin="$HERE/cpp/build/$f"
      mkdir -p "$HERE/cpp/build"
      if c++ "${cxx[@]}" -o "$bin" "$HERE/cpp/$f.cpp" "${ld[@]}" 2>/dev/null; then
        run "$f" "$bin"
      else
        printf '  %-30s FAILED (compile)\n' "$f"
        fail=$((fail + 1))
        failed_names+=("$f (compile)")
      fi
    done
  fi
fi

if wanted http; then
  echo
  echo "HTTP"
  if command -v curl >/dev/null && [ -d "$HERE/http" ]; then
    for f in $(apps_for http); do
      [ -f "$HERE/http/$f.sh" ] && run "$f" bash "$HERE/http/$f.sh"
    done
  else
    skip_lang "no curl"
  fi
fi

echo
if [ "$fail" -gt 0 ]; then
  echo "$pass passed, $fail failed, $skip language(s) skipped: ${failed_names[*]}"
  exit 1
fi
echo "$pass passed, 0 failed, $skip language(s) skipped"
