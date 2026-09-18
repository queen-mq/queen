# S1 candidates: build dependencies, MSRV, license, maintenance

Checked 2026-09-17 against crates.io and GitHub (`curl https://crates.io/api/v1/crates/<c>`,
`curl https://api.github.com/repos/<r>`). Constraints that decide admissibility:
C-2 (pure Rust, or native code compiled with `cc` only — **no cmake**) and C-3
(`rust-version = "1.88"`, server/Cargo.toml).

## Summary

| | redb | fjall | heed (LMDB) |
|---|---|---|---|
| version used in this spike | **2.6.3** | **2.11.2** | **0.22.1** |
| latest release | 4.3.0 (2026-09-15) | 3.1.10 (2026-08-30) | 0.22.1 (2026-04-07) |
| latest release's MSRV | 1.90 | 1.90 | not declared |
| newest release that fits MSRV 1.88 | 2.6.3 (2025-08-23, MSRV 1.85) | 2.11.2 (2025-07-21, MSRV 1.76) | 0.22.1 |
| build dependencies | none (pure Rust, no build script) | none (pure Rust, no build script) | `cc` + `doxygen-rs` (build-deps of lmdb-master-sys 0.2.6; `doxygen-rs` only rewrites doc comments); `bindgen` only behind its optional feature, off by default |
| cmake | no | no | **no** (`cc::Build` in lmdb-master-sys/build.rs) |
| license | MIT OR Apache-2.0 | MIT OR Apache-2.0 | heed MIT, lmdb-master-sys Apache-2.0 (LMDB itself is OpenLDAP Public License) |
| repository | cberner/redb | fjall-rs/fjall (+ fjall-rs/lsm-tree) | meilisearch/heed |
| stars / open issues | 4789 / 7 | 2331 / 41 | 912 / 51 |
| commits in the last 3 months | 100+ (API cap) | 40 (fjall) + 33 (lsm-tree) | **0** |
| last commit | 2026-09-16 | 2026-08-30 (fjall), 2026-09-10 (lsm-tree) | 2026-05-21 |
| single maintainer risk | yes (cberner) | yes (marvin-j97) | Meilisearch team; LMDB's own C is stable and barely changes |

## What each one costs us

**redb 2.6.3.** Pure Rust, one file, no build script: the cheapest thing to
ship. The price is the MSRV ladder: redb moved to 1.89 with 3.1.3/4.0.0
(2026-04-02) and to 1.90 with 4.2.0 (2026-08-17), so staying on 1.88 pins us to
a release from **2025-08-23** — a year of fixes behind, and the 2.x line gets no
more releases. Choosing redb effectively means raising the server's MSRV to
1.90 (and keeping it moving), or knowingly running an old copy. The project is
the most actively maintained of the three (100+ commits in three months, 7 open
issues), but it is one person.

**fjall 2.11.2.** Pure Rust, no build script, and its MSRV ladder is gentler
(2.x declares 1.76). The 3.x line declares 1.90, so the same choice appears,
one major version later: fjall 2.x is a 2025-07 release and the work has moved
to 3.x. Two crates to track (fjall + lsm-tree), both by the same maintainer.
41 open issues on fjall is normal for an LSM under development, not a red flag
by itself, but it is a young engine (first release 2023-12).

**heed 0.22.1 over LMDB.** The Rust wrapper is thin and the engine underneath is
LMDB 0.9, which has not meaningfully changed in years — the "0 commits in the
last 3 months" line is what a frozen C library looks like, not abandonment; the
last heed release is nonetheless 2026-04. It declares no MSRV, and it builds on
1.88 here (see RESULTS-laptop.md). It is the only candidate with native code:
`cc` compiles two C files (`mdb.c`, `midl.c`), no cmake, no bindgen unless the
optional `bindgen` feature is on (the bindings are checked in),
so it satisfies C-2 — but it does add a C dependency to a project that has none
today, and LMDB brings its own rules (map size fixed at open, long read
transactions pin the file, no in-place compaction, `MDB_NOSYNC` semantics).

## MSRV check

Both `cargo build --release` (Rust 1.94, the laptop default) and
`cargo +1.88 check` are run in RESULTS-laptop.md; the pins above are exactly
the newest releases that pass the 1.88 check.

## Notes that matter for the decision, not for the build

- redb and fjall are pure Rust, so cross-compiling the broker image stays as
  simple as it is today. heed needs a C toolchain in the build image.
  **Verified 2026-09-18: closed in heed's favour.** Both
  `/Users/alice/Work/queen/Dockerfile` (stages `server-builder`,
  `kafka-builder`, `sqs-builder`, `s3-builder`) and
  `/Users/alice/Work/queen/server/Dockerfile` build on `rust:1-bookworm`, a
  glibc image that carries gcc; the runtime stages are `debian:bookworm-slim`
  and `ubuntu:24.04` and need nothing, since LMDB is linked statically into the
  binary. No Dockerfile change is required to adopt heed.
- LMDB's map size must be chosen at open (`--map-size-gb` in this harness). A
  node that grows past it fails writes with `MDB_MAP_FULL` until it reopens
  with a bigger map. This was called "an operational rule, not a defect"; the
  2026-09-18 refutation is right that it is **also a liveness cliff with no
  backpressure in the plan**: §11.8 gates the planner on disk usage percent,
  which is not the map, so a node whose map fills cannot apply a committed entry
  and stops its Raft instance (I16) while the leader keeps accepting. Two such
  nodes lose quorum. `MEMO.md` §4 item 7 asks §11.8 for a map-size rule. redb
  and fjall have no fixed map.
- LMDB's **reader slot table** is the second fixed-size resource, and it was
  never exercised before 2026-09-18: `max_readers` defaults to 126 and the
  127th concurrent read transaction gets `MDB_READERS_FULL`
  (measured, `RESULTS-refutation.md` §4). With heed's default thread-local
  reader mode a *second* read txn on one thread gets `MDB_BAD_RSLOT` outright,
  and in the MDB_NOTLS mode that lifts it the read path stops scaling. See
  `MEMO.md` §4 item 1b.
- fjall is the only candidate whose files are immutable segments, which is what
  an incremental snapshot (I8, §11.6) would be built on.
- This spike depends on heed with `default-features = false`, which drops its
  serde/bincode/json features: the wrapper then pulls in nothing but
  `lmdb-master-sys`, `bitflags`, `byteorder`, `heed-traits`, `heed-types`,
  `libc`, `page_size` and `synchronoise`. The exact resolved set for all three
  engines is in this crate's `Cargo.lock`.
