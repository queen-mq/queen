# P5 — Streaming SDK Hardening

## 1. Summary

Version 0.15.0 introduced a fluent streaming SDK shared across JS,
Python, and Go, backed by three new stored procedures
(`streams_register_query_v1`, `streams_cycle_v1`,
`streams_state_get_v1`) and three new HTTP routes
(`/streams/v1/queries`, `/streams/v1/cycle`,
`/streams/v1/state/get`). The semantic claims — exactly-once
cycles, FIFO-preserving rate limiters, identical SHA-256
`config_hash` across runtimes, partial-ack via `release_lease=false`
— are non-trivial and currently rest on documentation alone.

This plan adds four kinds of guarantee-level tests, two
documentation pages, and one observability hook so the streaming
surface can be defended like the older push/pop/ack surface is.

## 2. Motivation

- The README claims "75 Python streams tests, 33 Go subtests, 45 JS
  unit tests pass live", but no public CI run produces those
  results (P3 fixes the runners; this plan fixes what they should
  test).
- `lib/schema/procedures/021_streams_cycle_v1.sql` is the lynchpin
  of exactly-once: state mutations + sink push + source ack commit
  in one PG transaction. There is no end-to-end test that **kills
  the runner mid-cycle** and asserts that no committed sink push
  exists without a corresponding source ack.
- The `release_lease=false` mode (lines 46-66 of `021_*.sql`) is the
  cleverest part of the SDK: it preserves FIFO order for rate
  limiters by partial-acking only the allowed messages and letting
  the lease expire, so the broker re-pops the un-acked tail in
  partition order. There is no test that proves the re-pop order
  matches the original.
- The SHA-256 `config_hash` claim — "a query registered by one
  client can be resumed by a worker written in another" — has no
  cross-language test. A whitespace difference in JSON
  serialisation between Python's `json.dumps` and JavaScript's
  `JSON.stringify` would silently break the contract.
- `cdocs/` does not yet contain a streaming-specific memo. As the
  feature matures, post-mortems and design notes will need a home.

## 3. Out of scope

- The CI shape (workflow files, runner scripts) — that's P3.
- Any new operator added to `Stream` (e.g. `.window_global`,
  `.connect`, `.broadcast`). Hardening covers the operators
  already shipped.
- Backpressure changes. The plan only verifies the existing
  contract; redesign of how the SDK pushes back when a downstream
  PG is slow is a separate effort.

## 4. Current state

### 4.1 Files involved

| Layer       | Path                                                        |
| ----------- | ----------------------------------------------------------- |
| SQL         | `lib/schema/procedures/019_streams_schema.sql`              |
|             | `lib/schema/procedures/020_streams_register_query_v1.sql`   |
|             | `lib/schema/procedures/021_streams_cycle_v1.sql`            |
|             | `lib/schema/procedures/022_streams_state_get_v1.sql`        |
| HTTP routes | `server/src/routes/streams/cycle.cpp`                       |
|             | `server/src/routes/streams/register_query.cpp`              |
|             | `server/src/routes/streams/state_get.cpp`                   |
| JS SDK      | `clients/client-js/client-v2/streams/`                      |
|             | (`Stream.js`, `runtime/Runner.js`, 11 operators)            |
| Python SDK  | `clients/client-py/queen/streams/`                          |
| Go SDK      | `clients/client-go/streams/`                                |
| Examples    | `streams/examples/`, `examples/use-case-rate-limiter.js`,   |
|             | `examples/use-case-aggregations.js`,                        |
|             | `examples/use-case-dedup.js`, `examples/use-case-replay.js` |

### 4.2 Documented invariants (from `021_streams_cycle_v1.sql`)

1. State mutations + sink push + source ack commit atomically.
2. On commit failure the entire cycle rolls back; broker redelivers
   via existing lease/retry path.
3. `release_lease=true` (default): cycle clears `lease_expires_at`,
   `worker_id`, `batch_size`, `acked_count`.
4. `release_lease=false`: `last_consumed_id` advances to the acked
   message; lease fields untouched; un-acked tail re-pops on lease
   expiry in original order.
5. Sink push items get a UUIDv7 stamped server-side, preserving
   per-partition FIFO across batched inserts that share a
   `created_at` timestamp.

## 5. Plan

### 5.1 Test suite 1 — exactly-once under crash

Goal: prove invariant (1) and (2) under crash injection.

Implementation outline (per SDK):

```
1. Create source queue 'src', sink queue 'sink', register query Q.
2. Push N=1000 messages to 'src', partition='p1'.
3. Start a runner that:
     - pops batch=10 from src/p1
     - in user code, sleeps a few ms then THROWS or KILLS the process
       on message 437 (deterministic crash point)
     - on every other message, emits a corresponding sink push and
       a state upsert
4. After crash, restart runner.
5. Read entire 'sink' queue.
6. Assert:
     - count(sink) == count of source messages whose cycle DID commit
     - state matches (cycle.state_ops_applied for committed cycles)
     - No 'sink' message exists for an un-acked source message.
     - Source partition's cursor is positioned at exactly the last
       acked message of the last committed cycle.
```

Crash injection: SIGKILL on Linux, child-process termination on
Node/Go workers, `os._exit(1)` on Python.

Acceptance: the test runs 100 times in CI with **zero** falsely
attributed sink rows.

### 5.2 Test suite 2 — `release_lease=false` FIFO preservation

Goal: prove invariant (4) — the un-acked tail is redelivered in
original partition order.

```
1. Create queue 'q', push 50 messages with payloads {n: 1..50} to
   partition 'p1' in order.
2. Build a Stream with .gate() that DENIES every odd n.
3. Run for one pop+cycle (batch=10): only n=2,4,6,8,10 are emitted
   and acked; lease NOT released.
4. Wait for lease expiry (lease_time = 5 s in the test).
5. Run another pop+cycle: assert returned batch starts with n=1
   (not n=11), then n=3,5,7,9 (the un-acked odds from cycle 1).
6. Continue until queue empties; assert sink contains
   {2,4,6,8,10, 12,14,16,18,20, …, 48,50} in that exact order
   (i.e. all evens, in original partition order).
```

Acceptance: the test runs 100 times with deterministic order.

### 5.3 Test suite 3 — cross-language `config_hash` conformance

Goal: prove the claim that a query registered from one runtime is
resumable from another.

```
For each of (JS, Python, Go):
  - Construct an identical pipeline:
        Stream.from(q)
          .map(double)
          .filter(positive)
          .keyBy('user_id')
          .windowTumbling(60_000)
          .aggregate(sum)
          .to(sink)
  - Compute config_hash via the SDK's public method.
  - Print the hash.

Assert: all three hashes are bit-identical.

Then:
  - Register the query from JS.
  - Push 100 messages.
  - Run the cycle from Python.
  - Verify Python resumes at the cursor JS left off.
```

Implementation note: the test must round-trip through
`/streams/v1/queries`. Hashing client-side and never calling the
server would not catch a Python-side `json.dumps(separators=...)`
divergence.

Acceptance: any change to the canonicalisation procedure (key order,
whitespace, type coercion) breaks the test in CI within one PR.

### 5.4 Test suite 4 — sink push failure behaviour

Goal: clarify and verify what happens when the sink push inside
`streams_cycle_v1` fails (e.g. queue does not exist, payload too
large, deadlock).

The current `021_*.sql` rolls back the entire cycle on any failure
inside the procedure — but the **runner-side** behaviour is not
explicitly tested. Cases:

- Sink queue deleted between register and cycle.
- Sink push transactionId collides (uniqueness violation).
- DB connection drops mid-cycle.

For each case, assert:

- The cycle's HTTP response surfaces `success: false` with a clear
  error.
- Source partition's cursor does **not** advance.
- Source lease is **released** (so the broker re-leases the same
  batch on next pop, rather than holding it for the full lease
  duration).
- The runner retries with the configured backoff and eventually
  surfaces a non-recoverable error to the user callback.

These three assertions become the contract; if any of them needs
to change, it's a deliberate decision recorded in
`cdocs/STREAMS_CYCLE_FAILURE_SEMANTICS.md` (a follow-up memo).

### 5.5 New documentation

#### 5.5.1 `docs/use-case-rate-limiter.html` — SP-level explainer

The page already exists. Add a short section "Why this is FIFO-safe"
that links to and summarises the `release_lease=false` block in
`021_streams_cycle_v1.sql:46-66`. Today operators have to read the
SQL to find this.

#### 5.5.2 `cdocs/STREAMS_DESIGN.md` — engineering memo

A new memo modeled on `cdocs/PUSHPOPLOOKUPSOL.md`. Sections:

1. Problem statement (why a streaming SDK in front of Queen).
2. Why one PG transaction per cycle and not two-phase commit.
3. The three SPs and their responsibilities.
4. Why partition_id is the implicit key for stateful operators.
5. Why UUIDv7 stamping happens server-side, not client-side.
6. Trade-offs (state size cost, replay cost, reset semantics).
7. Open questions (multi-key state, cross-partition aggregations,
   late events past `allowedLateness`).

### 5.6 Observability hook

Surface streaming activity in the Prometheus exposition. New series
in `prometheus.cpp`:

```
queen_streams_cycle_total{result="success|failure"}
queen_streams_cycle_duration_seconds_bucket{le="..."}
queen_streams_state_ops_total{op="upsert|delete"}
queen_streams_active_queries
queen_streams_late_events_total{policy="drop|include"}
```

Source: instrument `cycle.cpp`, `register_query.cpp`, and the
runtime late-event branches in each SDK (the SDK reports counters
back to the broker via existing metrics submission, or via a new
`/streams/v1/metrics` route, depending on which is cheaper).

## 6. Validation

The plan is delivered when:

1. Test suite 1 passes 100/100 in CI for all three SDKs.
2. Test suite 2 passes deterministically; mutating any operator
   that touches `release_lease` breaks it.
3. Test suite 3's three hashes are byte-identical and the
   round-trip Python-resumes-JS test passes.
4. Test suite 4 covers the three failure cases per SDK.
5. `docs/use-case-rate-limiter.html` and `cdocs/STREAMS_DESIGN.md`
   are merged.
6. `curl /metrics/prometheus` returns the new series at non-zero
   values during a streaming workload.

## 7. Risks & rollback

- **Risk: false-positive flaky cycle tests.** Crash-injection tests
  are inherently sensitive to timing. Mitigation: the crash points
  are message-count-based, not time-based; sleep durations are
  bounded by `await` rather than wall time; CI runs each test
  100x and fails on any single failure (so flake is detected, not
  hidden).
- **Risk: `config_hash` test rejects an intentional improvement.**
  Adding a new operator type is a hash-affecting change. Mitigation:
  the test pins the hash for a *fixed* pipeline shape; introducing
  a new operator does not affect existing pinned hashes because
  they don't include that operator.
- **Rollback:** every test file is independent. If suite 1 turns
  out to be flaky, disable just suite 1 in `streams.yml` while a
  fix is iterated.

## 8. Effort estimate

- Test suite 1 (exactly-once): 3 days per SDK to write and
  stabilise — total 9 engineering days for JS+Python+Go.
- Test suite 2 (FIFO under gate): 1 day per SDK.
- Test suite 3 (config_hash conformance): 1 day total (the test is
  small; the canonicalisation it depends on may need polishing).
- Test suite 4 (sink push failures): 2 days per SDK.
- `STREAMS_DESIGN.md`: 1 day.
- Prometheus series: 1 day in C++ + 1 day per SDK for the
  client-side counters that feed back.

Total: **~3 working weeks** for an engineer comfortable across all
three SDKs, or **~5 weeks** if split among per-SDK specialists.

## 9. Follow-ups

- Cross-language **example parity audit**: ensure every example in
  `streams/examples/` has a 1:1 counterpart in `clients/client-py`
  and `clients/client-go`. The README claims this; verify and lock
  it in with a CI script that asserts file-name parity.
- A dedicated **streaming dashboard** view in `app/` showing
  registered queries, cycle rate per query, late events, state size
  per query, and the new `release_lease=false` "tail-redelivery"
  ratio.
