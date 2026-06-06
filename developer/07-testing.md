# 07 — Testing

There are four layers of tests in this repo. Knowing which one to run when saves a lot of time.

| Layer | What it tests | Needs Postgres? | Where |
|-------|---------------|-----------------|-------|
| 1 | libqueen unit (orchestrator, queue, slot pool) | No | `lib/queen_test.cpp` |
| 2 | libqueen integration (talks to PG via libpq) | Yes | `lib/test_suite.cpp`, `lib/test.cpp` |
| 3 | Per-client tests (JS, Python, Go, PHP, C++) | Yes (via broker) | `clients/*/tests` |
| 4 | End-to-end benchmarks | Yes | `lib/test_contention.cpp`, `clients/client-js/benchmark/`, `benchmark-queen/` |

Most of the time you only need layer 1 (fast, runs anywhere) plus the layer-3 tests for whichever client you're touching.

---

## Layer 1 — libqueen unit tests

No Postgres required. Tests the orchestrator, batch policy, slot pool, per-type queue, and concurrency controllers in isolation.

```bash
# First build broker once so vendor deps are present
cd server && make deps && cd ..

cd lib
make test-unit
```

Source: `lib/queen_test.cpp`. Add new cases here whenever you touch any of:

- `lib/queen/per_type_queue.hpp`
- `lib/queen/batch_policy.hpp`
- `lib/queen/slot_pool.hpp`
- `lib/queen/drain_orchestrator.hpp`
- `lib/queen/concurrency/*.hpp`

Runs in <1 second. CI runs it on every PR.

---

## Layer 2 — libqueen integration tests

These actually open libpq connections to a real Postgres and exercise PUSH/POP/ACK/TRANSACTION/RENEW_LEASE end-to-end.

### Setup

```bash
# Postgres on :5433 (or wherever)
docker run --name qpg --rm -e POSTGRES_PASSWORD=postgres \
  -p 5433:5432 -d postgres:16
```

The test binary will create the schema on first run (same bootstrap path as the broker).

### Run

```bash
cd lib

# Basic smoke (small)
PG_HOST=localhost PG_PORT=5433 PG_USER=postgres PG_PASSWORD=postgres PG_DB=postgres \
  make test

# Full suite (~2.6k lines of test scenarios)
PG_HOST=localhost PG_PORT=5433 PG_USER=postgres PG_PASSWORD=postgres PG_DB=postgres \
  make test-suite
```

The suite covers:

- PUSH: single, batch, duplicate detection, partitions, large payloads
- POP: empty queue, non-empty, batch, partitions, consumer groups
- ACK: single, batch, success/failed status
- TRANSACTIONS: push+ack atomic, multi-op, partitions
- RENEW_LEASE: manual, batch, expired, multiple renewals
- Message ordering (FIFO verification)

### When to run it

Always before merging changes to `lib/schema/procedures/*.sql`. Always before merging changes to libqueen's hot path (`drain_orchestrator.hpp`, `slot_pool.hpp`, `per_type_queue.hpp`).

---

## Layer 3 — per-client tests

Each client has its own test runner that talks to a running broker over HTTP.

### Prereq

```bash
# In one terminal
cd server
./bin/queen-server
# Wait for "Queen server is ready and listening"
```

### JavaScript

```bash
cd clients/client-js
nvm use 22
npm test
```

### Python

```bash
cd clients/client-py
source venv/bin/activate              # if not already
./run_tests.sh                        # or `pytest tests/ -v`
```

### Go

```bash
cd clients/client-go
go test ./...
go test ./tests -run TestPush -v      # specific test
```

### PHP / Laravel

```bash
cd clients/client-laravel
./vendor/bin/phpunit
```

### C++

```bash
cd clients/client-cpp
make test
./bin/test_client
```

### Cross-language smoke matrix

Bug-fixes that touch the **wire format** (route handlers, JSON shapes, error codes) should be validated against at least three clients. The fastest combo:

```bash
( cd clients/client-js && nvm use 22 && npm test ) && \
( cd clients/client-py && source venv/bin/activate && pytest tests/ -v ) && \
( cd clients/client-go && go test ./... )
```

---

## Layer 4 — performance & contention

These don't gate PRs but are the metric we use to evaluate performance changes.

### libqueen contention benchmark

```bash
cd lib
PG_HOST=localhost PG_PORT=5433 PG_PASSWORD=postgres \
  make test-contention
```

Measures four scenarios:

1. PUSH-only throughput (baseline)
2. POP-only throughput (baseline)
3. **Mixed PUSH+POP** (this is the one that matters — see `cdocs/PUSHVSPOP.md`)
4. Wildcard POP + PUSH (worst-case partition contention)

If you change anything in the push or pop SQL, **run this before and after** and compare.

### JS client benchmarks

```bash
cd clients/client-js

# Producer
node benchmark/producer.js
node benchmark/producer_multi.js

# Consumer
node benchmark/consumer.js
node benchmark/consumer_multi.js
```

### Full end-to-end benchmark

```bash
cd benchmark-queen
# See its README for parameters; this is a multi-stage harness
# (producer → worker → fan-out × N) used for the numbers in
# docs/benchmarks.html
```

---

## Useful runtime knobs while testing

Set these on the broker to make problems visible:

```bash
LOG_LEVEL=debug                  # verbose broker logs
LOG_FORMAT=text                  # human-readable
DB_STATEMENT_TIMEOUT=5000        # fail fast instead of hanging
DEFAULT_TIMEOUT=5000             # short pop wait so tests don't stall
QUEUE_POLL_INTERVAL=50           # snappier long-poll for low-latency tests
```

Watch the metrics endpoint while tests run:

```bash
watch -n 1 'curl -s http://localhost:6632/metrics | head -50'
```

Important counters to watch:

- `pending_jobs_total{op="push"}` — should stay near 0 in healthy steady state
- `slot_acquisition_wait_us` — high → DB pool too small
- `vegas_limit{op="*"}` — concurrency cap per lane. The data-path lanes (push/pop/ack) run **static** by default, so this is the fixed limit there; for the aux lanes it's the Vegas-controlled cap (collapsing → overloaded)
- `pushgate(part=… defer=…)` (stats line) — distinct partitions in flight on the push engine and the count of jobs deferred by the per-partition gate; sustained high `defer` means many pushes target the same hot partition
- `push_waits_on_push`, `pop_waits_on_push` — should both be 0 (see `cdocs/PUSHPOPLOOKUPSOL.md`)

---

## CI

GitHub Actions config lives in `.github/`. CI currently runs:

- libqueen unit tests on every PR (no PG)
- A subset of layer-3 tests for the most commonly changed clients

If you add a test that needs a specific Postgres setup or extra services, document it in the test file's header so CI can wire it up.

---

## Common test-time gotchas

- **Stale stored procedures.** If you edit a `procedures/*.sql` file but don't restart the broker, your tests will run against the old version. Restart `queen-server` (or call your changed function via `psql -f` directly) before re-running.
- **Schema drift.** The schema is idempotent, but if you've manually `psql`-edited tables your local DB can drift from `schema.sql`. When in doubt, `DROP SCHEMA queen CASCADE` and let the broker rebuild it.
- **Lease times in tests.** Default lease is 5 minutes; expired-lease tests need to override. Pass `lease_time: 1` (1 second) when creating the test queue.
- **Message ordering tests.** UUIDv7 is monotonic only at millisecond granularity — tests that push 1000 messages in <1 ms and rely on per-message ordering can flake. Use `partition` + sequence in the payload to assert order, not the message UUID.
- **Postgres `max_connections`.** Layer-2 + layer-3 tests in parallel can blow past the default 100. Either run sequentially or bump `max_connections` in your dev Postgres.
