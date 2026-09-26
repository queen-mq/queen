# Queen test harness

One command that spins up isolated, throwaway stacks in Docker and runs every
client test suite (and the Rust broker unit tests) against a freshly-built
broker — the single binary on its only storage class, raft — then prints a
pass/fail matrix.

```
test/run.sh
```

The goal: see at a glance whether everything is OK, on a clean environment, in
parallel, without touching any broker you have running locally. There is no
database anywhere: every stack is broker(s) + a runner.

## What runs

| suite | what it is | single | tenanted | ha | ha-tenanted | notes |
|-------|------------|:------:|:--------:|:--:|:-----------:|-------|
| `js`   | `client-js/test-v2` human + stream | ✓ | ✓ | opt-in | — | Node 24 |
| `go`   | `client-go/tests` + `streams_integration` | ✓ | ✓ | opt-in | — | Go 1.24, standalone module (`GOWORK=off`) |
| `py`   | `client-py/tests` (pytest, incl. streams) | ✓ | ✓ | opt-in | — | Python 3.12 |
| `cli`  | `queenctl` E2E (`client-cli/tests`) | ✓ | ✓ | opt-in | — | needs the Go **workspace** (local client-go) + `QUEEN_E2E=1` |
| `cpp`  | `client-cpp/test_retry429` + `test_kv_timers` + `test_conflation` (broker-free) then `client-cpp/test_client` | ✓ | ✓ | opt-in | — | the kv/timer HTTP tests run unconditionally — a 404 from those routes is a bug |
| `laravel` | `client-php` unit suite, then the integration suite | ✓ | ✓ | opt-in | — | PHP 8.3 |
| `rust-client` | `client-rust` (`cargo test`, strict: a skipped integration test fails) | ✓ | ✓ | opt-in | — | |
| `rust` | in-process broker unit tests (`cargo test` on `server/`) | — | — | — | — | `unit` — no stack |
| `tenancy` | two-tenant isolation with the trusted tenant header | — | ✓ | — | with `--topo ha` | flag-ON only |
| `http` | every kv and timer route, and every form of the wire, with **no SDK** in the way | ✓ | — | — | — | no env of its own — kv and timers are on every broker |
| `conflation` | the `PLAN_CONFLATION.md` §7.3 end-to-end scenarios, raw HTTP with **no SDK** in the way | ✓ | — | — | — | |
| `txnsemantics` | the transactional gate (`PLAN_KV_TIMERS.md` §15): a lost kv precondition rolls back the push AND the timer, commit does not raise on it, an expired lease annuls the kv write, results are index-aligned | ✓ | — | — | — | raw HTTP |

- **`single`** = 1 raft broker (a single voter) + the runner.
- **`tenanted`** = the `single` stack with the broker started
  `QUEEN_TENANCY_HEADER=true` — native tenant scoping ON — while the client
  suites send **no** `x-queen-tenant` header. That is the *default-tenant* path,
  whose whole contract is to be identical to the flag-off path. See
  [Tenancy lanes](#tenancy-lanes) below.
- **`ha`** = a **3-node raft cluster** (`queen-1..3`, openraft, the env set of
  `helm_v2/broker`) + the runner. Client suites hit `queen-1`, which may be the
  leader or a follower (a follower forwards to the leader), proving the cluster
  is transparent to clients. **Opt-in** (`--topo single,ha`) until it has a
  validated baseline.
- **`ha-tenanted`** = the cluster with `QUEEN_TENANCY_HEADER=true` on every
  node. Substrate for the `tenancy` suite when the cluster is requested.

The compose files are shared: `tenanted` reuses `docker-compose.single.yml` and
`ha-tenanted` reuses `docker-compose.ha.yml`, with `run.sh` exporting
`QUEEN_TEST_TENANCY`. Duplicating the stack definitions would let the two lanes
drift, which is precisely what the parity gate exists to catch. `--topo raft1`
(the old opt-in raft lane) is accepted as an alias of `single`.

## Tenancy lanes

Two things are being tested, and they are different:

**1. The flag must change nothing for an untenanted client.** The `tenanted`
lane runs the unmodified client suites against a flag-ON broker with no tenant
header. `run.sh` then compares each suite's `single` and `tenanted` exit codes
(and pass tallies, where the suite prints one) and prints a verdict:

```
TENANCY PARITY: OK (1 suite(s) identical with the flag on and off)
```

A divergence prints `!! TENANCY DIVERGENCE <suite>: single rc=… vs tenanted rc=…`
with both log paths and **fails the run**, even if the flag-ON side is the green
one — a behaviour change either way is a regression. The gate is only as stable
as the suite it compares: re-run before calling a one-test delta a tenancy
regression, and note the direction (a flag-OFF-only failure cannot be caused by
the flag).

**2. Two tenants must not see each other.** The `tenancy` suite
(`test/runners/tenancy/tenancy-check.sh`) drives the brokers directly with the
trusted `x-queen-tenant` header, through two URLs: two nodes of the cluster on
`ha-tenanted`, the single node twice on `tenanted`. Every scenario uses the
*same* queue name, the *same* partition name and the *same* consumer-group name
for both tenants:

| # | asserts |
|---|---------|
| 1 | queue identity + config (`leaseTime`, `retryLimit`, namespace) are per-tenant, read back through **both** URLs after each tenant configured through a **different** one |
| 2 | no message crosses tenants: push through one URL, pop through the other |
| 3 | a foreign `partitionId` cannot advance another tenant's cursor |
| 4 | dedup keys are per-tenant (same `transactionId` on both = two messages) while within-tenant dedup still fires |
| 5 | `resources/queues` is scoped through both URLs |
| 6 | a consumer-group **name** shared by both tenants keeps independent cursors |
| 7 | with every name colliding, one tenant's claim+ack can neither hide nor **delay** the other's pending message (a ≤3 s budget: delivery alone would also pass on shared state served by a periodic rescan) |
| 8 | a tenant-B push delivers nothing to tenant-A's parked long-poll |

It refuses to run vacuously: a probe up front configures the same queue name for
two tenants with different `leaseTime`s and aborts if the broker does not keep
them apart (i.e. if the flag is off), so the lane can never "pass" by comparing
the default tenant with itself.

## Isolation model

Each `(suite × topology)` runs as its own `docker compose` **project**, so it
gets a private network and its own broker(s) with fresh named volumes for the
raft data directories. This matters because the JS/Go/Py suites all use the
**same** test-queue name patterns (`test-%`, `edge-%`, `pattern-%`, `workflow-%`)
— they would clobber each other on a shared stack. Parallelism is therefore **per stack, not per suite on
one stack**.

Nothing publishes host ports, and `run.sh` tears every project down with
`down -v`, so each lane starts on an **empty** broker. The suites rely on that
instead of cleaning up after themselves: fixed-name fixtures (the documentation
snippets' fixed `transactionId`s, a `putIfAbsent` key) are only green on a
broker that has not seen them before. Against a long-lived local broker, expect
those to report duplicates on a second run.

## Usage

```
test/run.sh                             # full matrix (single + tenanted, all suites)
test/run.sh --suite js,go               # subset of suites
test/run.sh --suite py --topo single
test/run.sh --suite js --topo single,tenanted   # the tenancy parity pair
test/run.sh --suite js --topo single,ha         # add the 3-node cluster lane
test/run.sh --suite tenancy             # two-tenant isolation, one node
test/run.sh --suite tenancy --topo ha   # two-tenant isolation over the cluster
test/run.sh --suite conflation          # PLAN_CONFLATION §7.3 e2e
test/run.sh -j 6                        # more parallelism (default 4)
test/run.sh --no-build-broker           # reuse an existing queen:test image
test/run.sh --keep                      # leave stacks up to poke at them
```

`--topo` filters the **client** lanes (`single`, `tenanted`, `ha`); `http`,
`conflation` and `txnsemantics` always run on `single`, and `tenancy` picks its
flag-ON lane from the topologies requested.

Requirements: Docker + Compose v2. The broker image `queen:test` builds from the
product [`Dockerfile`](../Dockerfile) at the repo root (override with
`QUEEN_TEST_BROKER_DOCKERFILE=<path>`); runner images build from
`test/runners/<suite>/Dockerfile`.

## How readiness works

The broker image carries no probe tooling, so readiness is gated from the runner
side: every runner waits on `GET /health` of every broker in `QUEEN_WAIT_URLS`.
A raft broker answers `503 {"status":"settling"}` until its state machine is open
and ready — on a cluster node that also means a leader is known and its apply has
caught up — and `200 {"status":"healthy"}` from then on. There is no schema to
apply and nothing to mount.

## Env-var normalization (handled for you)

The suites diverge on env names; the runners map one canonical set
(`QUEEN_HTTP_URL`, plus `QUEEN_A_URL`/`QUEEN_B_URL` for `tenancy`) to what each
suite actually reads:

| suite | broker URL var | other |
|-------|----------------|-------|
| js  | `QUEEN_SERVER_URL` | — |
| go  | `QUEEN_SERVER_URL` **and** `QUEEN_URL` | streams suite reads `QUEEN_URL` |
| py  | `QUEEN_SERVER_URL` **and** `QUEEN_URL` | — |
| cli | `QUEEN_SERVER` (not `_URL`) | `QUEEN_E2E=1`, per-run `QUEEN_TEST_QUEUE_PREFIX`, `QUEEN_RETENTION_INTERVAL_MS` (must equal the broker's `RETENTION_INTERVAL`) |
| cpp | argv[1] (no env) | — |
| rust-client | `QUEEN_TEST_URL` | `QUEEN_TEST_STRICT=1` |
| http | `QUEEN_HTTP_URL` | `PLAN_PORT` picks the plan server's port |

## The HTTP wire gate (kv + timers)

`PLAN_KV_TIMERS.md` §10.2 lists seven SDK rows and one row that is not an SDK:
HTTP, raw bodies, "a script executed in CI". `test/runners/http` is that row, and
it is written first on purpose: every other suite asserts through a client
library, so a wire that a library gets wrong and re-reads the same wrong way is
green. Here the request is `curl` and the response is `jq`.

It is two halves, and the split is the point:

| file | needs a broker | what it pins |
|------|:--------------:|--------------|
| [`kv-timers-wire.sh`](runners/http/kv-timers-wire.sh) | — | **the client.** Every request body of the surface, built in one place |
| [`http-wire-unit.sh`](runners/http/http-wire-unit.sh) | no | the exact BYTES of each body, against a scripted plan server, plus the §8.3 commit contract (a lost precondition RETURNS, everything else raises) |
| [`http-wire-check.sh`](runners/http/http-wire-check.sh) | yes | all eight routes, every op, every envelope, every documented refusal, and the transaction bundle's three sibling arrays |

If the integration half built its own bodies inline, the unit half would be
pinning bytes nobody sends. And the unit half is where the mistakes that no live
broker can see are caught: a rider that travelled inside `operations` on a broker
that also reads the top level, a `ttl` beside a `ttlSeconds`, a `getPrefix` in a
query string that the handler answers correctly after every access log in between
has already recorded it.

Both halves run from the same entrypoint, unit first: it needs nothing, so a
wrong body is reported in a second with its exact bytes instead of arriving
thirty seconds later as an unexplained 400.

`cleanupTestData` is load-bearing here rather than cosmetic (§10.4): the
namespace and the timer queues are purged through the API at the start AND from
an EXIT trap, and two assertions are built to go red if the purge ever stops
working (a fixed key that must be absent at the start, a fixed counter that must
reach a fixed value). The mirror rule is that anything reaching the message log
carries a per-run id, because no purge can reach the broker's dedup window.

## The conflation gate

`test/runners/conflation` is the `PLAN_CONFLATION.md` §7.3 row, and it is the
same "no SDK in the way" shape as the HTTP gate above, for the same reason: §4
shipped the seven client halves *after* the broker, so a suite that asserted
through a client library could not be written until the thing it gates already
existed. `curl` is the client, `jq` is the assertion. It was written red-first,
so every scenario fails on its *assertions*, never on transport, when the
feature is missing: a red that reads "delivered nothing (HTTP 204)" is the
harness failing, not the feature, and is worth chasing before anything else in
the log.

| scenario | plan | what goes red without the feature |
|---|---|---|
| `E2E-1` | §1.3, §7.3 E2E-1 | the redelivery after the ack is the whole backlog, not the tail; the adversarial producer run processes every message instead of collapsing it |
| `MODES` | §1.5 | `all`+conflation serves 200 of a 1000 backlog, not 1 |
| `DEPTH` | §2.5, §5.3 | `partitionsPending` / `conflation` / `effectivePending` are absent from `/depth` |
| `E2E-4` | §3.1, §3.3 | no `conflation` echo, no `conflationConflict` echo, and neither §3.3 refusal is a 400 |
| `E2E-3` | §7.3 E2E-3 | `workers` reads all 10 000 like `audit` instead of one per partition |
| `E2E-2` | §1.4, §7.3 E2E-2 | deliveries are batches, and the delivered head never supersedes across retries (the M2 pin) |
| `COUNTER` | §6.2, §6.3 | `queen_queue_conflated_per_minute` is not a family |

`E2E-5` (new SDK against an old broker) is **not** here: the behaviour under
test is the SDK's degrade-loudly error (§4), which a suite with no SDK in the
path cannot express. It belongs with the §7.2 client suites.

Hygiene follows the §10.4 rule the HTTP gate established: queue names, consumer
groups and transaction ids are per-run, and the queues are dropped from an EXIT
trap so a run that dies half way leaves nothing behind.

## Layout

```
test/
  run.sh                     orchestrator (build → parallel matrix → report)
  compose/
    docker-compose.single.yml  1 raft broker + runner (single, tenanted)
    docker-compose.ha.yml      3-node raft cluster + runner (ha, ha-tenanted)
  runners/
    common/wait-for-broker.sh
    <suite>/Dockerfile + entrypoint.sh + Dockerfile.dockerignore
    tenancy/tenancy-check.sh   two-tenant isolation (curl + jq)
    conflation/conflation-e2e-check.sh
                               PLAN_CONFLATION §7.3 e2e (curl + jq)
    txnsemantics/txn-semantics-check.sh
                               the transactional gate (curl + jq)
  vendor/cpp/threadpool.hpp  MIT header the C++ client needs (recovered from history)
  raft/                      raft-specific harnesses (checker, crash, kill, flatness, VM)
  jepsen/                    the Jepsen suite for the raft cluster
```
