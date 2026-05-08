# 06 — Client SDKs

Queen ships five official client SDKs, all from this monorepo. They share concepts (queues, partitions, consumer groups, transactions) but not code — each is idiomatic for its language. This page is the developer-side: how to set up, build, and run each one locally.

For end-user API docs, see each client's `README.md`.

```
clients/
├── client-js/        Node.js 22+         queen-mq          npm
├── client-py/        Python 3.8+         queen-mq          PyPI
├── client-go/        Go 1.24+            client-go         Go modules
├── client-laravel/   PHP 8.3+ + Laravel  smartpricing/queen-mq  Packagist
├── client-cpp/       C++17               header-only       in-repo
└── client-cli/       Go 1.24+            queenctl          GitHub Releases + Homebrew
```

The `client-cli/` package is the operator CLI built on top of `client-go`.
It is not an SDK in itself - see [15-cli.md](15-cli.md) for the architecture.

A running Queen broker on `http://localhost:6632` is the only common runtime dependency.

---

## JavaScript / Node.js (`clients/client-js/`)

### Layout

```
clients/client-js/
├── client-v2/         current implementation (the one published to npm)
├── test-v2/           test runner
├── benchmark/         producer/consumer benchmarks
├── package.json       name: queen-mq, version pinned in here
└── README.md
```

### Setup

```bash
cd clients/client-js
nvm use 22
npm install
```

### Run the smoke test

```bash
# Make sure a broker is running on http://localhost:6632
npm test
```

### Try an example

```bash
cd ../../examples
nvm use 22
node 01-basic-usage.js
node 03-transactional-pipeline.js
```

The examples import from `clients/client-js/client-v2/index.js` directly via the npm workspace, not from the published package, so changes to the client are picked up immediately.

### Benchmarks

```bash
cd clients/client-js
node benchmark/producer.js          # single producer
node benchmark/producer_multi.js    # N producers
node benchmark/consumer.js
node benchmark/consumer_multi.js
```

### Notes for developers

- Module type is `"type": "module"` — write ESM, not CommonJS.
- Public entry is `client-v2/index.js`. Internal helpers are not re-exported.
- Browser is not a target. The client uses `pg` (for transactional outbox) and Node-only APIs.

---

## Python (`clients/client-py/`)

### Layout

```
clients/client-py/
├── queen/                package source
├── tests/                pytest suite
├── example.py
├── pyproject.toml        name: queen-mq, version: 0.14.0
├── run_tests.sh          convenience runner
├── publish.sh            PyPI upload
└── README.md
```

### Setup the venv

This is the canonical way to develop on the Python client:

```bash
cd clients/client-py

# 1. Create a virtual env (one time)
python -m venv venv

# 2. Activate it (every shell)
source venv/bin/activate          # bash/zsh on macOS/Linux
# .\venv\Scripts\activate         # PowerShell on Windows

# 3. Install in editable mode with dev deps
pip install -e ".[dev]"
```

`pip install -e ".[dev]"` installs:

- `httpx>=0.27.0` (runtime)
- `pytest`, `pytest-asyncio`, `asyncpg`, `black`, `mypy`, `ruff` (dev)

After that, `python -c "import queen; print(queen.__file__)"` should print a path inside `clients/client-py/queen/`.

### Run the example

```bash
# venv active, broker running on :6632
python example.py
```

### Run the tests

The wrapper script does dependency + broker checks and then delegates to `pytest`:

```bash
./run_tests.sh                      # everything
./run_tests.sh quick                # smoke set: queue/push/pop
./run_tests.sh pytest-s             # pytest -vs (with stdout)
```

Direct pytest works too:

```bash
pytest tests/ -v
pytest tests/test_push.py -v
```

### Notes for developers

- API is async — every method returns a coroutine; use `async with Queen(...)`.
- Type hints are exported (`queen/py.typed`). Keep `mypy --strict` clean.
- Black + ruff are configured in `pyproject.toml`. Run before committing:
  ```bash
  black queen tests
  ruff check queen tests
  mypy queen
  ```
- When you bump `version` in `pyproject.toml`, also update `version` in `server/server.json` and the other clients' manifests so a release stays in lockstep.

---

## Go (`clients/client-go/`)

### Layout

```
clients/client-go/
├── go.mod                            module: github.com/smartpricing/queen/clients/client-go
├── queen.go, http_client.go, …       package source (top-level)
├── push_builder.go, consume_builder.go, …    fluent builders
├── transaction_builder.go, dlq_builder.go, queue_builder.go
├── load_balancer.go                  multi-server support
├── tests/                            integration tests
└── README.md
```

### Setup

Go modules — nothing to install upfront other than the toolchain (1.24+):

```bash
cd clients/client-go
go mod download
```

### Run the tests

```bash
# Broker on :6632
go test ./...
go test ./tests -run TestPush -v
```

### Use locally from another repo

If you're hacking on the client alongside an application that consumes it, add a `replace` directive to your application's `go.mod`:

```go
replace github.com/smartpricing/queen/clients/client-go => /Users/alice/Work/queen/clients/client-go
```

### Notes for developers

- Public surface is the top-level package only. Helpers are unexported.
- The runtime PG client (`jackc/pgx/v5`) is only required for transactional-outbox features (`Transaction` builder); push-only consumers don't need a PG-reachable network.
- Module path is `github.com/smartpricing/queen/clients/client-go` even though the tag is repo-wide; releases are tagged as `client-go/vX.Y.Z`.

---

## PHP / Laravel (`clients/client-laravel/`)

### Layout

```
clients/client-laravel/
├── composer.json         smartpricing/queen-mq
├── src/
│   ├── Queen.php                    standalone PHP entry
│   └── Laravel/
│       ├── QueenServiceProvider.php  auto-discovered
│       ├── QueenFacade.php
│       └── Console/                  artisan commands
├── config/queen.php       publishable config
├── tests/                 PHPUnit
└── README.md
```

### Setup

```bash
cd clients/client-laravel
composer install
```

PHP 8.3 or newer.

### Run the tests

```bash
# Broker on :6632
./vendor/bin/phpunit
```

### Use in a Laravel project locally

Either `composer require smartpricing/queen-mq` from Packagist, or symlink:

```json
// composer.json of the consuming Laravel app
{
  "repositories": [
    { "type": "path", "url": "/Users/alice/Work/queen/clients/client-laravel" }
  ],
  "require": {
    "smartpricing/queen-mq": "*"
  }
}
```

### Notes for developers

- Standalone usage works without Laravel: `new Queen('http://localhost:6632')`.
- Laravel integration uses **package auto-discovery** — service provider, facade, and artisan commands light up as soon as the package is required. Don't add manual registration steps to docs.
- Two namespaces: `Queen\` (transport) and `Queen\Laravel\` (framework glue).

---

## C++ (`clients/client-cpp/`)

### Layout

```
clients/client-cpp/
├── queen_client.hpp        the whole client (header-only)
├── example_basic.cpp
├── test_client.cpp
├── Makefile
├── README.md
└── QUICK_START.md
```

The C++ client is **not** the same code as `lib/queen.hpp`. `lib/` is libqueen (the broker's internals); `clients/client-cpp/queen_client.hpp` is a small HTTP client built on `cpp-httplib`.

### Setup

```bash
brew install cpp-httplib                          # macOS
# or download to clients/client-cpp/:
# curl -o httplib.h https://raw.githubusercontent.com/yhirose/cpp-httplib/master/httplib.h
```

### Build the test suite + example

```bash
cd clients/client-cpp
make test            # builds bin/test_client
make example         # builds bin/example_basic
```

### Run

```bash
# Broker on :6632
./bin/test_client
./bin/example_basic
```

### Notes for developers

- The whole client is header-only — to use it from a project, `#include "queen_client.hpp"` and pass `-lpthread` (and OpenSSL flags for HTTPS).
- For a "fluent" C++ API see `QUICK_START.md`.
- Fixture in `clients/client-cpp/QUICK_START.md` references absolute paths from the original author's machine; ignore those, the relative paths from `clients/client-cpp/` are what matter.

---

## Sharing test data across clients

If you change something in the broker that affects wire format, run the smoke tests in **at least three** clients (typically JS, Python, Go) before merging. Each client has slightly different parsing assumptions; bugs that only show up on one language are common.

Suggested cross-client smoke:

```bash
# Terminal 1: broker
cd server && ./bin/queen-server

# Terminal 2: JS
cd clients/client-js && nvm use 22 && npm test

# Terminal 3: Python
cd clients/client-py && source venv/bin/activate && pytest tests/ -v

# Terminal 4: Go
cd clients/client-go && go test ./...
```

The end-to-end matrix is captured by `benchmark-queen/` (see [07 — Testing](07-testing.md)).