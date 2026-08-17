# `src/tests_unit/` — the Rust unit tests of PLAN_KV_TIMERS §15 ("Unit Rust", closes F3)

These four files were written **before** the code they test, so they are the contract, not a
report on it. Each one is a `#[cfg(test)] mod` body that belongs to a module which does not exist
yet (or does not yet export the item under test). They are **not compiled** until the owning
module wires them in: a file sitting in `src/` that no `mod` declaration points at is invisible to
cargo, so this directory is inert until F3 and cannot break a build before then.

## Wiring (one three-line block per owner, at the bottom of the owning module)

| file | owner module | wiring block to add |
|---|---|---|
| `classify_sql.rs` | `src/db.rs` | `#[cfg(test)]`<br>`#[path = "tests_unit/classify_sql.rs"]`<br>`mod classify_sql_tests;` |
| `fire_sql_pin.rs` | `src/db.rs` | `#[cfg(test)]`<br>`#[path = "tests_unit/fire_sql_pin.rs"]`<br>`mod fire_sql_pin_tests;` |
| `sweeper_sleep.rs` | `src/sweeper.rs` | `#[cfg(test)]`<br>`#[path = "tests_unit/sweeper_sleep.rs"]`<br>`mod sweeper_sleep_tests;` |
| `pack_segment.rs` | `src/frames.rs` | `#[cfg(test)]`<br>`#[path = "tests_unit/pack_segment.rs"]`<br>`mod pack_segment_tests;` |

`#[path]` on a non-inline `mod` resolves **relative to the directory of the file that declares
it**, i.e. `src/`, and the module is a child of the declaring module, so `use super::*` reaches
the owner's private items. Verified empirically on this toolchain before these files were
written — it is the mechanism that lets a test for a private function live in its own file
without making the function `pub`.

The alternative (paste the body into the owner's existing `mod tests`) is equally correct and
matches the house style of one `mod tests` per module; it just costs a merge every time either
side moves. Pick either; do not do both.

## What each file pins, and the signature it demands

Written first means these files **define** the surface. If an implementation names things
differently, the test does not compile — that is the point, and the fix is the implementation,
not the test, unless the plan says otherwise.

### `classify_sql.rs` → `src/db.rs`

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SqlClass { Transient, Config, Permanent }

/// Pure, testable core. `None` = the error carried no SQLSTATE (connection-level).
pub(crate) fn classify_sqlstate(code: Option<&str>) -> SqlClass;

/// The wrapper every call site uses.
pub(crate) fn classify_sql(e: &tokio_postgres::Error) -> SqlClass;
```

The split is not decoration: `tokio_postgres::DbError` cannot be constructed outside its crate,
so a classifier that only takes `&tokio_postgres::Error` is **untestable**. All the behaviour
lives in `classify_sqlstate`; `classify_sql` is one line
(`classify_sqlstate(e.as_db_error().map(|d| d.code().code()))`) and needs no test.

`file_buffer.rs::classify_push_error` is deleted and its two call sites map the shared verdict:
`Transient => DrainErr::Transient`, `Config | Permanent => DrainErr::Permanent`. The spool keeps
byte-identical behaviour — that parity is the reason the transient set is pinned code by code
here — and `src/file_buffer.rs` must stop containing its own SQLSTATE literals
(`tests/kv_timers_source_pins.rs` fails the build if they survive).

### `sweeper_sleep.rs` → `src/sweeper.rs`

```rust
pub(crate) struct SleepKnobs {
    pub min_ms: u64,             // QUEEN_SWEEPER_MIN_SLEEP_MS        default 5
    pub max_ms: u64,             // QUEEN_SWEEPER_MAX_SLEEP_MS        default 1000
    pub idle_max_ms: u64,        // QUEEN_SWEEPER_IDLE_MAX_SLEEP_MS   default 30_000
    pub idle_after: u32,         // QUEEN_SWEEPER_IDLE_AFTER_CYCLES   default 5
    pub empty_claim_min_ms: u64, // QUEEN_SWEEPER_EMPTY_CLAIM_MIN_MS  default 25
    pub empty_claim_max_ms: u64, // QUEEN_SWEEPER_EMPTY_CLAIM_MAX_MS  default 200
}
impl SleepKnobs { pub(crate) fn defaults() -> Self; }

pub(crate) enum CycleOutcome {
    Due { next_in_ms: i64 }, // the probe returned nextInMs (may be <= 0)
    EmptyClaim,              // work was due, the claim took zero rows (§7.2)
    Idle,                    // nextInMs NULL and nothing pruned (§7.1)
}

/// Pure: no clock, no RNG, no env. `jitter` is the caller's random draw, so the
/// test is deterministic and the RNG stays at the call site.
pub(crate) fn sleep_ms(o: CycleOutcome, idle_cycles: u32, k: &SleepKnobs, jitter: f64) -> u64;

pub(crate) const ABSOLUTE_FLOOR_MS: u64 = 1;
```

`idle_cycles` is the count of consecutive `Idle` outcomes; `hint()` (§7.4) resets it to 0, which
is what makes the 30 s backoff safe. The floor exists because knobs come from the environment: a
`QUEEN_SWEEPER_MIN_SLEEP_MS=0` typo must not turn a background loop into a spin.

### `pack_segment.rs` → `src/frames.rs`

```rust
pub struct PackedSegment { pub hashes: Vec<u8>, pub blob: Vec<u8>, pub count: usize }

/// The fire's phase (b) (§1.3), byte-for-byte the recipe fusion already uses.
pub fn pack_segment(frames: &[FrameIn<'_>], zstd_level: i32) -> PackedSegment;
```

Lives in `frames.rs`, next to `pack_frames`, **not** in `fusion.rs`: the sweeper must not depend
on the fusion module (§1.8 keeps the two paths apart), and `frames.rs` is already the shared
codec. `fusion.rs::build_hashes_and_blob` becomes a thin adapter over it — one call, no second
recipe — which is the whole point of the extraction the plan asks for.

### `fire_sql_pin.rs` → `src/db.rs`

```rust
pub(crate) const TIMERS_FIRE_SQL: &str = "SELECT (queen.log_timers_fire_v1(...))::text";
```

The broker-side statement must be a **named const**, not a string literal inline in an `async fn`
(the retention precedent: `WORK_LIST_SQL`, `Phase::sql()`), or the §1.8 pin has nothing to read.
The file also `include_str!`s `../../sql/procedures/025_log_timers.sql`, so wiring it in before
that file exists is a compile error — wire it at F3, with 025.

## Order of arrival

`classify_sql.rs` and `pack_segment.rs` can be wired as soon as their extractions land (early
F3). `sweeper_sleep.rs` needs `sweeper.rs`. `fire_sql_pin.rs` needs `025_log_timers.sql` **and**
`cargo build` after every edit to it — the SQL is `include_str!`-embedded, so a test run against a
stale binary lies (the repo-wide gotcha, and it bites `include_str!` in tests exactly as it bites
the schema applier).
