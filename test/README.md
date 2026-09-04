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

| suite | what it is | single | ha | tenanted | ha-tenanted | notes |
|-------|------------|:------:|:--:|:--------:|:-----------:|-------|
| `js`   | `client-js/test-v2` human + stream | ✓ | ✓ | ✓ | — | Node 24 |
| `go`   | `client-go/tests` + `streams_integration` | ✓ | ✓ | ✓ | — | Go 1.24, standalone module (`GOWORK=off`) |
| `py`   | `client-py/tests` (pytest, incl. streams) | ✓ | ✓ | ✓ | — | Python 3.12 |
| `cli`  | `queenctl` E2E (`client-cli/tests`) | ✓ | ✓ | ✓ | — | needs the Go **workspace** (local client-go) + `QUEEN_E2E=1` |
| `cpp`  | `client-cpp/test_retry429` + `test_kv_timers` (both broker-free) then `client-cpp/test_client` (50 HTTP tests, 13 of them kv/timers) | ✓ | ✓ | ✓ | — | no Postgres access; the kv/timer HTTP tests run unconditionally — a 404 from those routes is a bug, not a cell without the surface |
| `rust` | 50 in-process broker unit tests (`cargo test`) | — | — | — | — | `unit` — no stack, no PG |
| `mesh` | asserts the 2 brokers formed an authenticated mesh | — | ✓ | — | — | HA only |
| `tenancy` | two-tenant isolation over the mesh pair | — | — | — | ✓ | flag-ON only |
| `http` | every kv and timer route, and every form of the wire, with **no SDK** in the way | ✓ | — | — | — | no PG access; no env of its own — kv and timers are on every broker |
| `conflation` | the `PLAN_CONFLATION.md` §7.3 end-to-end scenarios (guarantee, DLQ under a hot producer, mixed groups, conflict echo, mode composition, depth fields, conflated counter), raw HTTP with **no SDK** in the way | ✓ | — | — | — | no PG access; written red-first — stays red until the broker half of the plan lands |

- **`single`** = 1 Postgres + 1 broker + the runner.
- **`ha`** = 1 Postgres + `queen-a` + `queen-b` (framed-TCP mesh) + the runner.
  Client suites hit `queen-a`, proving HA is transparent to clients; both
  brokers share the one Postgres (the sole source of truth — dedup lives in
  `queen.log_txns`, offsets in `queen.log_consumers`).
- **`tenanted`** = the `single` stack with the broker started
  `QUEEN_TENANCY_HEADER=true` — native tenant scoping ON, which is what every
  cloud cell runs — while the client suites send **no** `x-queen-tenant` header.
  That is the *default-tenant* path (`server/src/tenant.rs`), whose whole
  contract is to be byte-identical to the flag-off path. See
  [Tenancy lanes](#tenancy-lanes) below.
- **`ha-tenanted`** = the `ha` pair with `QUEEN_TENANCY_HEADER=true` on both
  brokers. Substrate for the `tenancy` suite only.

The compose files are shared: `tenanted` reuses `docker-compose.single.yml` and
`ha-tenanted` reuses `docker-compose.ha.yml`, with `run.sh` exporting
`QUEEN_TEST_TENANCY`. Duplicating the stack definitions would let the two lanes
drift, which is precisely what the parity gate exists to catch.

## Tenancy lanes

`PLAN_QUEEN_PROXY_CLOUD.md` asks for the suites on **both tenanted and
untenanted lanes**. Two things are being tested, and they are different:

**1. The flag must change nothing for an untenanted client.** The `tenanted`
lane runs the unmodified client suites against a flag-ON broker with no tenant
header. `run.sh` then compares each suite's `single` and `tenanted` exit codes
and prints a verdict:

```
TENANCY PARITY: OK (1 suite(s) identical with the flag on and off)
```

A divergence prints `!! TENANCY DIVERGENCE <suite>: single rc=… vs tenanted rc=…`
with both log paths and **fails the run**, even if the flag-ON side is the green
one — a behaviour change either way is a regression.

**2. Two tenants must not see each other, over the mesh.** The `tenancy` suite
(`test/runners/tenancy/tenancy-check.sh`, 44 assertions) drives the HA pair
directly with the trusted `x-queen-tenant` header. Every scenario uses the *same*
queue name, the *same* partition name and the *same* consumer-group name for both
tenants, with traffic on both brokers:

| # | asserts |
|---|---------|
| 1 | queue identity + config (`leaseTime`, `retryLimit`, namespace) are per-tenant, read back on **both** brokers after each tenant configured on a **different** one |
| 2 | no message crosses tenants: push on one broker, pop on the other |
| 3 | a foreign `partitionId` cannot advance another tenant's cursor (the Track B ownership gate) |
| 4 | dedup keys are per-tenant (same `transactionId` on both = two messages) while within-tenant dedup still fires |
| 5 | `resources/queues` is scoped on both brokers |
| 6 | a consumer-group **name** shared by both tenants keeps independent cursors |
| 7 | the hot-list ring — keyed `(queue, group)` with partition-name interning per queue name, **no tenant** — cannot let one tenant's claim+ack hide the other's pending message. The ring is per broker *process*, so both tenants pop from `queen-a` (shared entry) while both push to `queen-b` (the ring is fed by a name-only mesh hint, as in production) |
| 8 | a tenant-B push delivers nothing to tenant-A's parked long-poll |

It refuses to run vacuously: a probe up front configures the same queue name for
two tenants with different `leaseTime`s and aborts if the broker does not keep
them apart (i.e. if the flag is off), so the lane can never "pass" by comparing
the default tenant with itself.

The crosstalk that *is* expected stays as printed `note` lines rather than
assertions, because it is wasted work and not a correctness property: the mesh
`QUEUE_CONFIG_SET` frame carries only the queue name, so a peer drops the
lease/encryption cache entry of every tenant holding that name
(`server/src/main.rs` `on_queue_config_set`); and the parked-pop wake gate plus
`MESSAGE_AVAILABLE` are queue-name keyed, so one tenant's push wakes every
tenant parked on that name. Postgres is the authority on both paths — each SP
call carries the tenant — so the cost is a re-query, not a leak.

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
test/run.sh                             # full matrix (single + ha + tenanted, all suites)
test/run.sh --suite js,go               # subset of suites
test/run.sh --suite py --topo single
test/run.sh --suite js --topo single,tenanted   # the tenancy parity pair
test/run.sh --suite mesh                # HA mesh assertion only
test/run.sh --suite tenancy             # two-tenant isolation over the HA pair
test/run.sh --suite conflation          # PLAN_CONFLATION §7.3 e2e (red until the feature lands)
test/run.sh -j 6                        # more parallelism (default 4)
test/run.sh --no-build-broker           # reuse an existing queen:test image
test/run.sh --keep                      # leave stacks up to poke at them
```

`--topo` filters the **client** lanes (`single`, `ha`, `tenanted`); `mesh`,
`tenancy`, `http` and `conflation` always bring their own topology, as `mesh`
already did.

Requirements: Docker + Compose v2. The broker image builds from
[`server/Dockerfile`](../server/Dockerfile) (~100 MB, `queen:test`); runner
images build from `test/runners/<suite>/Dockerfile`.

## How readiness works

The runtime broker image is `debian:slim` with only the `queen` binary (no
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
| http | `QUEEN_HTTP_URL` | — | no PG; `PLAN_PORT` picks the plan server's port |

## Fixes that landed with this harness

- **Go cleanup was dead code.** `client-go/tests/helpers_test.go` deleted
  unqualified `partitions`/`queues` with a retired `queue_name` column — those
  relations don't exist on the log/segment broker, so cleanup errored on the
  first statement and (being only warned about) never ran. Rewritten to the
  schema-qualified log-engine cleanup the Python/JS suites use.
- **Python `pytest.ini`** now sets `asyncio_default_fixture_loop_scope=session`,
  which the session-scoped async fixtures require on modern pytest-asyncio
  (`pyproject.toml` already intended it, but `pytest.ini` shadowed it).

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
thirty seconds later as an unexplained 400 from a stored procedure.

`cleanupTestData` is load-bearing here rather than cosmetic (§10.4): the
namespace and the timer queues are purged at the start AND from an EXIT trap, and
two assertions are built to go red if the purge ever stops working (a fixed key
that must be absent at the start, a fixed counter that must reach a fixed value).
The mirror rule is that anything reaching the message log carries a per-run id,
because no purge can reach the broker's dedup window.

## The conflation gate (red-first)

`test/runners/conflation` is the `PLAN_CONFLATION.md` §7.3 row, and it is the
same "no SDK in the way" shape as the HTTP gate above, for the same reason:
§4 ships the seven client halves *after* the broker, so a suite that asserted
through a client library could not be written until the thing it gates already
existed. `curl` is the client, `jq` is the assertion.

**It is expected to be RED until the broker half of the plan lands.** It was
written before the feature — the red half of TDD — so on a broker without
conflation every scenario fails on its *assertions*, never on transport: the
`conflation` query parameter is an unknown key that serde drops, so pops answer
200/204 and simply deliver full batches, which is exactly the behaviour the
suite exists to rule out. `test/run.sh` with no `--suite` therefore reports
`SOME FAILURES` on this branch by design. Baseline measured against the current
tree (`test/run.sh --suite conflation --no-build-broker`, 119 s):
**37 assertions passed, 36 failed, all 7 scenarios red** — and every one of the
36 is an assertion on *content*, with zero transport errors. That distinction is
the suite's own health check: a red that reads "delivered nothing (HTTP 204)"
is the harness failing, not the feature being absent, and is worth chasing
before reading anything else in the log.

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
path cannot express, and the harness has no old-broker topology. It belongs with
the §7.2 client suites. Two clauses of E2E-4 are likewise out of reach at the
wire and say so in the script header — the conflict *counter* only ever reaches
the `rates` log line (§6.1), and "warns exactly once" is SDK behaviour.

Hygiene follows the §10.4 rule the HTTP gate established: queue names, consumer
groups and transaction ids are per-run, because a group carries a cursor and the
dedup ring carries txn ids and no `DELETE` here can reset either; the queues are
dropped from an EXIT trap so a run that dies half way leaves nothing behind.

## The deep mesh gate

The `mesh` suite asserts the mesh is **established and authenticated**
(`/internal/api/shared-state/stats`: peer connected, zero handshake failures).
The deep behavioral gate — cross-replica WAKE latency, DEAD detection, RECON —
remains [`server/mesh-verify.sh`](../server/mesh-verify.sh), which builds its
own debug binary and drives two brokers on localhost. Run it directly for that.

## The targeted-pop gate

Phase 2 partition-hinted pops (`server/src/notify.rs` hint mailbox +
`handlers/data.rs` `handle_pop`) have their own manual acceptance script,
[`server/pop-targeted-verify.sh`](../server/pop-targeted-verify.sh): a parked
long-poll consumer served by a single hinted push must take the targeted path
(`queen_pop_targeted_total`), while flowing data with no parked consumer must
still fall back to the wildcard scan (`queen_pop_wildcard_total`), with no
regression on the throughput path. Run it directly, same as the mesh gate.

## Tenancy baseline on branch `rustproxy` (2026-07-29)

First run of the two new lanes, against a freshly built `queen:test`.

| lane | result |
|------|--------|
| `tenancy` / `ha-tenanted` | **PASS** — 45 assertions, 0 fail, 5 notes (21s). §7 gained a ≤3s latency budget once the hot-list ring / wake gates / mesh frames became (tenant, queue)-keyed: a shared ring still delivers the second tenant eventually (via the reseed floor), so asserting delivery alone would pass on the broken shape. §8's spurious-wake note became a plain statement — a woken pop with nothing to serve keeps looping to its deadline, so the wake is not observable from HTTP in either shape. |
| `cpp` / `single` vs `tenanted` | **PASS** both; parity OK |
| `js` / `single` vs `tenanted` | rc=1 both (the pre-existing streaming failures); parity OK at `120/147` on both lanes, identical set of 27 failing test names |

One caveat worth knowing before you trust a parity failure: the **first** `js`
parity pair came back `119/147` (single) vs `120/147` (tenanted) — the flag-OFF
lane failed `consumerGroupWithPartition` ("Message must have partitionId
property"), the flag-ON lane passed it. A second pair of runs had both lanes at
`120/147` with byte-identical failing sets, so that test is **flaky**, not
flag-sensitive. The count gate is sensitive enough to catch it, which is the
point — but re-run before calling a one-test delta a tenancy regression, and
note the direction (a flag-OFF-only failure cannot be caused by the flag).

### Update, later the same day: the streams bucket is the noise floor

Once `queen.streams_register_query_v1` was fixed (it named `tenant_id` in an
`ON CONFLICT` against a table that has no such column, so every register had
failed since Track B landed) the `js` suite went `120/147` → `147/147` on both
lanes. What is left is **flaky, and it is all in the streams window tests**.
Three consecutive parity pairs:

| run | `single` | `tenanted` | parity verdict |
|-----|----------|------------|----------------|
| 1 | 146/147 (`testLoadConsumerGroup`) | 146/147 (same test) | OK |
| 2 | 145/147 (`slidingEventsAppearInOverlappingWindows`, `tumblingBasicWindowSum`) | — | — |
| 3 | **147/147** | 146/147 (`tumblingBasicWindowSum`, sum 36 vs 37) | **FAILED** |

The failing set moves between runs *and* between lanes, and the arithmetic is a
window **over-emit** (`expected 36, got 37`) — the same class as the
cross-language sliding-window finding recorded in the 2026-07-23 baseline below.
So run 3's `TENANCY PARITY: FAILED` is the gate working correctly and reporting
a divergence that tenancy did not cause.

Practical consequence: **the parity gate is only as stable as the suite it
compares.** Until the streams window failures are fixed, a one-test delta in the
streams bucket is noise — re-run it, check whether the failing test is a
windowing assertion, and only treat it as a tenancy regression if the same test
fails on the flag-ON lane repeatedly while passing on flag-OFF.

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
    mesh/mesh-check.sh         HA mesh assertion (curl + jq)
    tenancy/tenancy-check.sh   two-tenant isolation over the HA pair (curl + jq)
    conflation/conflation-e2e-check.sh
                               PLAN_CONFLATION §7.3 e2e (curl + jq), red-first
  vendor/cpp/threadpool.hpp  MIT header the C++ client needs (recovered from history)
```
