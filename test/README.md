# Queen test harness

One command that spins up isolated, throwaway stacks in Docker and runs every
client test suite (and the Rust broker unit tests) against a freshly-built
broker — on both a single-node stack and a 2-broker HA mesh — then prints a
pass/fail matrix.

```
test/run.sh
```

The goal: see at a glance whether everything is OK, on a clean environment, in
parallel, without touching your live Postgres on `:5432`.

## What runs

| suite | what it is | single | ha | notes |
|-------|------------|:------:|:--:|-------|
| `js`   | `client-js/test-v2` human + stream | ✓ | ✓ | Node 22 |
| `go`   | `client-go/tests` + `streams_integration` | ✓ | ✓ | Go 1.24, standalone module (`GOWORK=off`) |
| `py`   | `client-py/tests` (pytest, incl. streams) | ✓ | ✓ | Python 3.12 |
| `cli`  | `queenctl` E2E (`client-cli/tests`) | ✓ | ✓ | needs the Go **workspace** (local client-go) + `QUEEN_E2E=1` |
| `cpp`  | `client-cpp/test_client` (~40 HTTP tests) | ✓ | ✓ | no Postgres access |
| `rust` | 50 in-process broker unit tests (`cargo test`) | — | — | `unit` — no stack, no PG |
| `mesh` | asserts the 2 brokers formed an authenticated mesh | — | ✓ | HA only |

- **`single`** = 1 Postgres + 1 broker + the runner.
- **`ha`** = 1 Postgres + `queen-a` + `queen-b` (framed-TCP mesh) + the runner.
  Client suites hit `queen-a`, proving HA is transparent to clients; both
  brokers share the one Postgres (the sole source of truth — dedup lives in
  `queen.log_txns`, offsets in `queen.log_consumers`).

## Isolation model

Each `(suite × topology)` runs as its own `docker compose` **project**, so it
gets a private network and its own Postgres + broker(s). This matters because
the JS/Go/Py suites all wipe the **same** test-queue name patterns
(`test-%`, `edge-%`, `pattern-%`, `workflow-%`) and the CLI suite flips
broker-global maintenance mode — they would clobber each other on a shared
stack. Parallelism is therefore **per stack, not per suite on one stack**.

Postgres runs on `tmpfs` and publishes **no** host ports, so runs are fast,
disposable, and never collide with each other or with your live `:5432`.

## Usage

```
test/run.sh                      # full matrix
test/run.sh --suite js,go        # subset of suites
test/run.sh --suite py --topo single
test/run.sh --suite mesh         # HA mesh assertion only
test/run.sh -j 6                 # more parallelism (default 4)
test/run.sh --no-build-broker    # reuse an existing queen-seg:test image
test/run.sh --keep               # leave stacks up to poke at them
```

Requirements: Docker + Compose v2. The broker image builds from
[`server/Dockerfile`](../server/Dockerfile) (~100 MB, `queen-seg:test`); runner
images build from `test/runners/<suite>/Dockerfile`.

## How readiness works

The runtime broker image is `debian:slim` with only the `queen-seg` binary (no
shell tools), so readiness is gated from the runner side: every runner waits on
the broker's `GET /health`, which returns 200 only after Postgres is connected
**and** the schema is applied (the broker binds its HTTP listener after applying
`schema.sql` + `procedures/*.sql` under an advisory lock). So a healthy
`/health` is a combined broker + PG + schema gate — no init SQL to mount.

## Env-var normalization (handled for you)

The suites diverge on env names; the runners map a single canonical set
(`QUEEN_HTTP_URL`, `QUEEN_PG_*`) to what each suite actually reads:

| suite | broker URL var | PG db var | other |
|-------|----------------|-----------|-------|
| js  | `QUEEN_SERVER_URL` | `PG_DB` | — |
| go  | `QUEEN_SERVER_URL` **and** `QUEEN_URL` | `PG_DB` | streams suite reads `QUEEN_URL` |
| py  | `QUEEN_SERVER_URL` **and** `QUEEN_URL` | `PG_DB` | — |
| cli | `QUEEN_SERVER` (not `_URL`) | `PG_DB` | `QUEEN_E2E=1`, per-run `QUEEN_TEST_QUEUE_PREFIX` |
| cpp | argv[1] (no env) | — | no PG |

## Fixes that landed with this harness

- **Go cleanup was dead code.** `client-go/tests/helpers_test.go` deleted
  unqualified `partitions`/`queues` with a retired `queue_name` column — those
  relations don't exist on the log/segment broker, so cleanup errored on the
  first statement and (being only warned about) never ran. Rewritten to the
  schema-qualified log-engine cleanup the Python/JS suites use.
- **Python `pytest.ini`** now sets `asyncio_default_fixture_loop_scope=session`,
  which the session-scoped async fixtures require on modern pytest-asyncio
  (`pyproject.toml` already intended it, but `pytest.ini` shadowed it).

## The deep mesh gate

The `mesh` suite asserts the mesh is **established and authenticated**
(`/internal/api/shared-state/stats`: peer connected, zero handshake failures).
The deep behavioral gate — cross-replica WAKE latency, DEAD detection, RECON —
remains [`server/mesh-verify.sh`](../server/mesh-verify.sh), which builds its
own debug binary and drives two brokers on localhost. Run it directly for that.

## Baseline on branch `rustserverandstorage` (2026-07-23)

First full run against the current working tree. The harness itself is green —
what's red are pre-existing/WIP issues it surfaced:

| suite | single | ha | unit |
|-------|:------:|:--:|:----:|
| js   | 1 fail | 1 fail | — |
| go   | 1 fail | 1 fail | — |
| py   | 1 fail + 91 teardown errors | same | — |
| cli  | PASS | PASS | — |
| cpp  | PASS | PASS | — |
| rust | — | — | PASS (50 tests) |
| mesh | — | PASS | — |

Core messaging (push/pop/ack/dedup/transactions) passes everywhere; the failures
are all in the **streaming** suites:

- **Sliding-window over-emit** — `slidingEventsAppearInOverlappingWindows` fails
  in **both JS (got 13) and Python (got 16)** against `expected <= 12`. Two
  independent client languages agree the broker emits an extra overlapping
  window emission — a real cross-language streaming finding to triage.
- **Token-bucket gate stall** — `client-go` `TestGateTokenBucketBasic` drains
  only 20 of 60 within its hard 20-second deadline (deterministic, not timing),
  a deny→lease-expiry→redeliver stall.
- **Python teardown errors (91)** — `RuntimeError: Event loop is closed` at
  fixture teardown. The tests PASS (140 passed); the errors are a scope mismatch
  between the session-scoped `db_pool`/`cleanup_test_data` and the
  function-scoped `client` fixture in `conftest.py`, surfaced by modern
  pytest-asyncio. The `pytest.ini` loop-scope line here unblocks the suite (it
  was 100% blocked by ScopeMismatch before); clearing the teardown errors needs
  a `loop_scope` fix on the session fixtures in `conftest.py` — a follow-up.

## Layout

```
test/
  run.sh                     orchestrator (build → parallel matrix → report)
  compose/
    docker-compose.single.yml
    docker-compose.ha.yml
  runners/
    common/wait-for-broker.sh
    <suite>/Dockerfile + entrypoint.sh + Dockerfile.dockerignore
  vendor/cpp/threadpool.hpp  MIT header the C++ client needs (recovered from history)
```
