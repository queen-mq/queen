# P2 — `lib/queen.hpp` Decomposition

## 1. Summary

`lib/queen.hpp` is a header-only file of **~2,031 lines / 89 KB**
containing UUIDv7 generation, schema-init helpers, the `Queen`
class itself (slot pool driver, libuv loop owner, cross-thread
intake, drain orchestrator, pop-backoff tracker, reconnect thread,
file-buffer hooks), and a handful of inline utility free functions.

The 0.13 refactor (see `cdocs/LIBQUEEN_IMPROVEMENTS.md` §5) cleanly
extracted the lower three layers — `BatchPolicy`, `PerTypeQueue`,
`SlotPool`, `ConcurrencyController` — into `lib/queen/*.hpp`. This
plan finishes the same job for the top layer (orchestration), so
that:

- Every header in `lib/queen/` compiles standalone.
- Each component is unit-testable without a live PG.
- `lib/queen.hpp` stays as a thin umbrella header for backward
  compatibility (existing `#include "queen.hpp"` keeps working).

## 2. Motivation

- A 2k-line monolith inside a header is recompiled in full by every
  consumer (`server/src/acceptor_server.cpp`, every routes file that
  reaches `submit()`, every test). Even with the precompiled-header
  setup in `server/include/pch.hpp`, an edit to a private member of
  `Queen` triggers a full server rebuild — measured ~45 s on a clean
  ccache, ~12 s warm.
- The orchestrator state (private fields of `Queen`) is not visible
  to unit tests. The closest existing harness is `lib/test_suite.cpp`
  which boots a real PG; there is no PG-free test for the drain
  scheduler logic.
- The reconnect thread, the in-flight deadline watchdog, the
  pop-backoff tracker, and the metrics submission path are
  intermingled inside one class body. Reading any one of them
  requires holding the others in your head.
- `cdocs/LIBQUEEN_IMPROVEMENTS.md` already named the four layers; the
  code now contradicts that document at the top layer (Layer 4 lives
  inside the same file as the Queen public API).

## 3. Out of scope

- No changes to public API surface visible to `server/`. Existing
  `#include "queen.hpp"` and method signatures of `Queen` are
  preserved.
- No change to thread safety invariants stated in `lib/queen.hpp`
  comments (event-loop-only state vs cross-thread state).
- No change to dispatched SQL strings or stored procedure names.
- No change to environment variable names or defaults.
- Performance must be neutral or better. This is a code-organization
  refactor, not an optimization.

## 4. Current state

```
lib/
├── queen.hpp                       2031 lines  ← target of this plan
├── queen/
│   ├── batch_policy.hpp             196 lines  (Layer 2)
│   ├── concurrency/
│   │   ├── concurrency_controller.hpp           (Layer 3 base)
│   │   ├── static_limit.hpp
│   │   └── vegas_limit.hpp
│   ├── drain_orchestrator.hpp        91 lines  (Layer 3 wiring)
│   ├── metrics.hpp                  ~140 lines
│   ├── pending_job.hpp              151 lines
│   ├── per_type_queue.hpp           153 lines  (Layer 1)
│   └── slot_pool.hpp                ~100 lines
└── worker_metrics.hpp               ~600 lines  (PG-targeted submitter)
```

Inside `queen.hpp`, by approximate line range:

| Range          | Concern                                            |
| -------------- | -------------------------------------------------- |
| 1 – 100        | `generate_uuidv7()`                                |
| 100 – 220      | schema init helpers + `initialize_schema`          |
| 220 – 320      | `Queen` ctor / dtor                                |
| 320 – 700      | DB connection slot lifecycle, libpq wiring         |
| 700 – 1000     | submit-side intake, request id, invalidation       |
| 1000 – 1300    | `_drain_orchestrator` (timer / async / kick)       |
| 1300 – 1500    | `_process_slot_result`, response routing           |
| 1500 – 1700    | pop-backoff tracker (`_pop_backoff_tracker`)       |
| 1700 – 1900    | reconnect thread + in-flight deadline watchdog     |
| 1900 – 2031    | metrics submission, stats helpers                  |

## 5. Target structure

```
lib/
├── queen.hpp                       umbrella header (≤ 80 lines)
├── queen/
│   ├── uuidv7.hpp                  generate_uuidv7()
│   ├── schema_init.hpp             initialize_schema() + helpers
│   ├── queen.hpp                   class Queen — public API + ctor/dtor
│   ├── queen.ipp                   class Queen — inline private impls
│   ├── slot.hpp                    DBConnection / slot lifecycle
│   ├── drain_orchestrator.hpp      (already exists)
│   ├── drain_pump.hpp              the actual _drain_orchestrator() body
│   ├── response_router.hpp         _process_slot_result() body
│   ├── pop_backoff_tracker.hpp     stand-alone class, unit-testable
│   ├── reconnect_thread.hpp        watchdog + reconnect logic
│   ├── batch_policy.hpp            (already exists)
│   ├── pending_job.hpp             (already exists)
│   ├── per_type_queue.hpp          (already exists)
│   ├── slot_pool.hpp               (already exists)
│   ├── metrics.hpp                 (already exists)
│   └── concurrency/                (already exists)
└── worker_metrics.hpp              (unchanged)
```

`lib/queen.hpp` after the refactor:

```cpp
#ifndef _QUEEN_HPP_
#define _QUEEN_HPP_

#include "queen/uuidv7.hpp"
#include "queen/schema_init.hpp"
#include "queen/queen.hpp"

#endif
```

i.e. a stable umbrella that any existing `#include "queen.hpp"` in
`server/` continues to satisfy.

## 6. Plan

The refactor is ordered so each commit ships a green build.

### 6.1 Step 1 — extract the leaf utilities

- Move `generate_uuidv7()` to `lib/queen/uuidv7.hpp`.
- Move `initialize_schema`, `detail::read_sql_file`,
  `detail::get_sql_files`, `detail::exec_sql` to
  `lib/queen/schema_init.hpp`.
- Replace those bodies in `lib/queen.hpp` with `#include`s of the new
  headers.

Acceptance for the step: `make all` in `server/` is byte-equivalent
binary diff (no behaviour change).

### 6.2 Step 2 — extract the pop-backoff tracker

It is the most isolated state inside `Queen` (no PG access, no libuv
loop ownership; just a `std::unordered_map` of timers and deadlines).

- New header `lib/queen/pop_backoff_tracker.hpp`. Public surface:
  `register_wait`, `cancel_wait`, `tick`, `notify_arrival`.
- Add `lib/pop_backoff_tracker_test.cpp` with unit tests that exercise
  the threshold/multiplier/cap logic. No PG needed.
- `Queen` holds a `std::unique_ptr<PopBackoffTracker>` and forwards.

### 6.3 Step 3 — extract the reconnect / watchdog thread

Self-contained: a thread, a flag, and a callback to `Queen` for
"reconnect this slot". The current code path
(`_reconnect_running`, `_reconnect_thread`,
`_inflight_deadline_ms`) lifts cleanly.

- New header `lib/queen/reconnect_thread.hpp`. Constructor takes a
  `std::function<void(slot_id)>` that `Queen` provides.
- `Queen` constructs it after slot pool init.

### 6.4 Step 4 — separate declaration / definition of `Queen`

Move the class **declaration** (member layout, public method
signatures, friend declarations) to `lib/queen/queen.hpp` and the
inline method bodies to `lib/queen/queen.ipp`. Import `.ipp` from
`.hpp` at end-of-file.

This is a mechanical split with no semantic change. The C++ rule of
"inline functions defined in headers" still holds because `.ipp` is
just an include.

### 6.5 Step 5 — extract the drain pump body

The big function `_drain_orchestrator()` becomes a free function in
`lib/queen/drain_pump.hpp`:

```cpp
namespace queen {
void drain_pump(Queen& q, std::array<PerTypeState, JobTypeCount>& types,
                SlotPool& slots, /* … */);
}
```

`Queen` calls it from inside the libuv timer callback and from
`submit()` for the submit-kick path. Tests can drive `drain_pump`
directly with hand-built `PerTypeState` and a fake `SlotPool` (mock
that records "fired batch of size N for type T").

### 6.6 Step 6 — extract the response router

Same shape as Step 5: `lib/queen/response_router.hpp` exposes a free
function `process_slot_result(Queen&, slot_id, PGresult*)` that owns
the result-dispatch logic (callbacks, `_free_slot`, kick-drain).

### 6.7 Step 7 — slim the umbrella

After steps 1–6, `lib/queen.hpp` is reduced to the three-line
forward-include shown in §5. Add a brief comment block at top:

```
// Backward-compatible umbrella. Prefer #include <queen/queen.hpp>
// in new code; this header exists so the public API surface keeps
// working for callers that depended on the pre-refactor layout.
```

## 7. Build-system implications

### 7.1 `lib/Makefile`

Today `make test` compiles `test_suite.cpp`, `queen_test.cpp`,
`test_contention.cpp` against `queen.hpp`. After the refactor the
include path stays the same (`-I.`); each new header lives under
`queen/` so existing `#include "queen.hpp"` keeps working.

Add unit-test targets for the newly-extracted units:

```makefile
test-unit: pop_backoff_test reconnect_thread_test drain_pump_test
	./pop_backoff_test
	./reconnect_thread_test
	./drain_pump_test
```

These compile without `libpq` and without a running PG — they fit a
GitHub Actions matrix step that's measured in seconds, not minutes.

### 7.2 `server/Makefile`

`server/include/pch.hpp` precompiles `<queen.hpp>` today. After the
refactor it precompiles the same umbrella, which transitively
includes the same content; PCH stays valid. Adding finer-grained
includes in individual `.cpp` files would let us drop the umbrella
from the PCH and shave incremental rebuild time, but that's a P4
optimization, not a P2 prerequisite.

## 8. Validation

1. **Binary equivalence (Step 1).** `server/bin/queen-server` SHA256
   matches before/after the leaf-utility extraction (compile in a
   reproducible environment with `SOURCE_DATE_EPOCH`).
2. **Performance neutrality (all steps).** Run the
   `benchmark-queen/2026-04-26/bp-10` baseline (1×50 producer,
   batch=10, 10 partitions, 5 min) before and after the full
   refactor. Push msg/s must be within ±2 % of the recorded
   baseline.
3. **New unit tests pass without PG.** `make test-unit` in `lib/`
   completes in < 5 s and exercises pop-backoff, reconnect, and
   drain-pump.
4. **Existing integration tests pass.** `lib/test_suite.cpp`
   unmodified; runs against `postgres:18` in the new
   PR-triggered CI job (P3) and is green.
5. **Public API frozen.** `nm bin/queen-server | grep queen::Queen::`
   identical symbol set before and after.
6. **Rebuild time.** Editing `lib/queen/pop_backoff_tracker.hpp`
   triggers ≤ 5 s incremental rebuild of `server/`, vs ≤ 12 s today
   for any edit anywhere in `queen.hpp`.

## 9. Risks & rollback

- **Risk: a private member is accessed from an unrelated section.**
  Splitting an interlinked class often surfaces hidden coupling.
  Mitigation: do not break the `class Queen` body in steps 5–6;
  only extract the **bodies** of `_drain_orchestrator` and
  `_process_slot_result` into helper free functions that take
  `Queen&` by reference. The class layout is unchanged.
- **Risk: header include order subtly changes preprocessor
  behaviour** (e.g. someone forgot `#include <atomic>` and was
  relying on transitive inclusion). Mitigation: each new header
  declares its own `#include`s; verify with `g++ -H` on the leaf
  headers as part of CI.
- **Rollback:** the refactor lands as **seven commits** in one PR
  (one per step). If a step regresses CI, revert just that commit
  and ship the rest.

## 10. Effort estimate

A focused engineer can land Steps 1–4 in **2 days** (each is ≤ 2 h
of code + verification). Steps 5–6 are the harder ones — they
require thinking about which arguments the extracted functions need
without leaking private state — call it **2 days each**. Total: ~1
working week for the refactor + 2 days for the new unit tests.

## 11. Follow-up

Once `lib/queen/queen.hpp` is well-bounded, the same approach can be
applied to `worker_metrics.hpp` (~600 LOC) and to `lib/queen.hpp`'s
schema-init helpers (which today do their own SQL string handling
that overlaps with the upcoming P4.3 versioned-migration system).
