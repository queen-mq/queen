#!/usr/bin/env bash
# Run every client tutorial, in every language, against one broker.
#
# These are the programs the documentation shows: the five-minute guide (00-*),
# the client guide and the patterns of the book and the site (book-*), and the
# older per-client tutorials. The site cuts its code out of them by region
# (webdoc/src/lib/book-code.ts). Each one asserts its own
# outcome and exits non-zero on failure, so a green run here is what makes the
# word "verified" on those pages mean something.
#
#   examples/tutorials/run.sh                          # against http://localhost:6632
#   QUEEN_URL=http://localhost:6642 examples/tutorials/run.sh
#   QUEEN_URL=... examples/tutorials/run.sh js py      # a subset of the languages
#
# Needs, per language: node 22+, python 3.9+, go 1.24+, rust 1.75+, php 8.3+,
# a C++17 compiler, and curl with jq. A language whose toolchain is missing is
# skipped with a note rather than failing the run. The clients all come from
# this repository, never from a registry.
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
  echo "start one with:  docker run -d --name queen -p 6632:6632 ghcr.io/queen-mq/queen:2.7.0" >&2
  echo "or from source:  cd $ROOT/server && QUEEN_RAFT_DIR=./queen-data cargo run --release" >&2
  exit 1
fi

pass=0
fail=0
skip=0
failed_names=()

run() {
  local name="$1"
  shift
  printf '  %-34s ' "$name"
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

skip_lang() {
  echo "  (skipped: $1)"
  skip=$((skip + 1))
}

# The five-minute guide (00-*) talks to http://localhost:6632, the node its
# step 1 starts, because the homepage shows its code verbatim. Run a copy
# pointed at $QUEEN_URL instead. The copy sits next to the original, so it
# resolves its dependencies the same way, and is removed after the run.
guide_copy() { # <source> <copy>
  mkdir -p "$(dirname "$2")"
  sed "s#http://localhost:6632#${QUEEN_URL}#g" "$1" > "$2"
}

if wanted js; then
  echo
  echo "JavaScript"
  if command -v node >/dev/null; then
    # The examples resolve `queen-mq` to ../../clients/client-js through a
    # file: dependency, so install once before the first run.
    [ -d "$HERE/js/node_modules" ] || (cd "$HERE/js" && npm install --silent)
    guide_copy "$HERE/js/00-five-minutes.mjs" "$HERE/js/.00-five-minutes.run.mjs"
    run 00-five-minutes env -C "$HERE/js" node .00-five-minutes.run.mjs
    rm -f "$HERE/js/.00-five-minutes.run.mjs"
    # The book's own examples (book/chapters/2-writing-applications), same treatment.
    guide_copy "$HERE/js/book-client-guide.mjs" "$HERE/js/.book-client-guide.run.mjs"
    run book-client-guide env -C "$HERE/js" node .book-client-guide.run.mjs
    rm -f "$HERE/js/.book-client-guide.run.mjs"
    guide_copy "$HERE/js/book-use-cases.mjs" "$HERE/js/.book-use-cases.run.mjs"
    run book-use-cases env -C "$HERE/js" node .book-use-cases.run.mjs
    rm -f "$HERE/js/.book-use-cases.run.mjs"
    for f in 01-hello-world 02-multi-queue-flow 03-transaction-ack-push 04-replay 05-streaming; do
      run "$f" env -C "$HERE/js" node "$f.mjs"
    done
  else
    skip_lang "no node"
  fi
fi

if wanted py; then
  echo
  echo "Python"
  # The client's own virtualenv when there is one: it has httpx, and zstandard for
  # the binary wire forms (Python before 3.14 has no zstd of its own).
  PY=python3
  [ -x "$ROOT/clients/client-py/venv/bin/python" ] && PY="$ROOT/clients/client-py/venv/bin/python"
  if command -v "$PY" >/dev/null; then
    for f in 00_five_minutes book_client_guide book_use_cases; do
      guide_copy "$HERE/py/$f.py" "$HERE/py/_${f}_run.py"
      run "$f" env -C "$ROOT" PYTHONPATH=clients/client-py "$PY" "examples/tutorials/py/_${f}_run.py"
      rm -f "$HERE/py/_${f}_run.py"
    done
    for f in 01_hello_world 02_multi_queue_flow 03_transaction_ack_push 04_replay 05_streaming; do
      [ -f "$HERE/py/$f.py" ] && run "$f" env -C "$ROOT" PYTHONPATH=clients/client-py "$PY" "examples/tutorials/py/$f.py"
    done
  else
    skip_lang "no python3"
  fi
fi

if wanted go; then
  echo
  echo "Go"
  if command -v go >/dev/null && [ -f "$HERE/go/go.mod" ]; then
    for f in five-minutes book-client-guide book-use-cases; do
      guide_copy "$HERE/go/$f/main.go" "$HERE/go/$f-run/main.go"
      run "$f" env -C "$HERE/go" GOWORK=off go run "./$f-run"
      rm -rf "$HERE/go/$f-run"
    done
    for f in hello-world multi-queue-flow transaction-ack-push replay streaming; do
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
    # Compile once up front, or the first run would time its own build.
    (cd "$HERE/rust" && cargo build --quiet) || exit 1
    # Cargo picks up anything under examples/ without a manifest entry.
    for f in 00_five_minutes book_client_guide book_use_cases; do
      guide_copy "$HERE/rust/src/bin/$f.rs" "$HERE/rust/examples/${f}_run.rs"
      run "$f" env -C "$HERE/rust" cargo run --quiet --example "${f}_run"
    done
    rm -rf "$HERE/rust/examples"
    for f in 01_hello_world 02_multi_queue_flow 03_transaction_ack_push 04_replay 05_streaming; do
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
    # The examples require ./vendor/autoload.php, which is a symlink to the
    # client's own Composer install in clients/client-php — the same shape
    # as the JS tutorials' node_modules. Install it once before the first run.
    if [ ! -d "$ROOT/clients/client-php/vendor" ]; then
      command -v composer >/dev/null && (cd "$ROOT/clients/client-php" && composer install --quiet --no-interaction)
    fi
    [ -e "$HERE/php/vendor" ] || ln -sfn ../../../clients/client-php/vendor "$HERE/php/vendor"
    # The book programs need ext-pcntl and ext-posix; wire-forms also needs ext-zstd,
    # and prints a skip line without it.
    for f in 00-five-minutes book-client-guide book-use-cases; do
      guide_copy "$HERE/php/$f.php" "$HERE/php/.$f.run.php"
      run "$f" env -C "$HERE/php" php ".$f.run.php"
      rm -f "$HERE/php/.$f.run.php"
    done
    for f in 01-hello-world 02-multi-queue-flow 03-transaction-ack-push 04-replay; do
      [ -f "$HERE/php/$f.php" ] && run "$f" env -C "$HERE/php" php "$f.php"
    done
  else
    skip_lang "no php"
  fi
fi

if wanted cpp; then
  echo
  echo "C++"
  # queen_client.hpp is header-only, but it includes three headers this
  # repository does not vendor: json.hpp (expected under clients/server/vendor),
  # threadpool.hpp (expected under clients/server/include) and cpp-httplib's
  # httplib.h. It also turns cpp-httplib's OpenSSL support on unconditionally,
  # so -lssl -lcrypto is required even over plain http. These are the flags each
  # tutorial's own header comment prints; keep the two in step.
  # test/runners/cpp/Dockerfile is what puts the three headers in place.
  if ! command -v c++ >/dev/null; then
    skip_lang "no c++ compiler"
  elif [ ! -d "$HERE/cpp" ]; then
    skip_lang "no cpp tutorials here"
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
    # The five-minute guide and the book programs, from a copy pointed at $QUEEN_URL
    # (the includes come from -I, so the copy can live in build/). book-client-guide
    # also links zstd for the binary wire forms when the header is there; without it
    # the program prints a skip line for that step.
    zstd=()
    if [ -f "${brew_prefix:-/usr}/include/zstd.h" ] || [ -f /usr/include/zstd.h ]; then
      zstd=(-DQUEEN_CLIENT_ZSTD -lzstd)
      [ -n "${brew_prefix:-}" ] && zstd+=(-L"$brew_prefix/lib")
    fi
    for f in 00-five-minutes book-client-guide book-use-cases; do
      [ -f "$HERE/cpp/$f.cpp" ] || continue
      mkdir -p "$HERE/cpp/build/src"
      guide_copy "$HERE/cpp/$f.cpp" "$HERE/cpp/build/src/$f.cpp"
      extra=()
      [ "$f" = book-client-guide ] && extra=(${zstd[@]+"${zstd[@]}"})
      # ${a[@]+"${a[@]}"}: an empty array under set -u, which bash 3.2 (macOS) calls unbound.
      if c++ "${cxx[@]}" -o "$HERE/cpp/build/$f" "$HERE/cpp/build/src/$f.cpp" "${ld[@]}" ${extra[@]+"${extra[@]}"} 2>/dev/null; then
        run "$f" "$HERE/cpp/build/$f"
      else
        printf '  %-34s FAILED (compile)\n' "$f"
        fail=$((fail + 1))
        failed_names+=("$f (compile)")
      fi
    done
    for f in 01-hello-world 02-multi-queue-flow 03-transaction-ack-push 04-replay; do
      [ -f "$HERE/cpp/$f.cpp" ] || continue
      bin="$HERE/cpp/build/$f"
      mkdir -p "$HERE/cpp/build"
      if c++ "${cxx[@]}" -o "$bin" "$HERE/cpp/$f.cpp" "${ld[@]}" 2>/dev/null; then
        run "$f" "$bin"
      else
        printf '  %-34s FAILED (compile)\n' "$f"
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
    for f in 01-hello-world 02-multi-queue-flow 03-transaction-ack-push 04-replay; do
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
