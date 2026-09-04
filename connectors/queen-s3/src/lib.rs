//! queen-s3 — the S3 / data-lake sink connector for QueenMQ (PLAN_S3_SINK.md).
//!
//! A CLIENT of the broker, out of process: it reads the log through
//! `POST /api/v1/fetch`, discovers moved partitions through
//! `POST /api/v1/partitions/changed`, keeps two small commit-pointer documents
//! per (sink, queue) in Queen's key/value store, and writes open-format objects
//! (JSONL, Parquet) under a Hive layout into any S3-compatible bucket.
//!
//! The one idea everything else hangs off (plan §4): **the commit unit is a
//! time window on PostgreSQL's clock**, `[T_{k-1}, T_k)` over each record's
//! segment timestamp, closed only at or below the broker's `safeTime`. A window
//! is a deterministic set (segment timestamps are monotone per partition in
//! commit order, 003_log_push.sql:219-223), so a retried upload is byte-identical
//! and exactly-once needs no offset-range object names, no conditional PUT and
//! no LIST. Per-partition positions are a cache, never the truth.
//!
//! Module map (the ownership map of the build, kept honest by the tests):
//!
//! | module | what |
//! |---|---|
//! | [`types`]      | the shared vocabulary: [`types::Micros`], [`types::Record`], bounds, KV documents, manifest, checkpoint |
//! | [`config`]     | `QUEEN_S3_*` from the environment, secrets masked |
//! | [`queen`]      | the Queen API client (fetch, partitions/changed, kv) and its test double |
//! | [`s3`]         | SigV4 signing and the object-store client, plus an in-memory double |
//! | [`layout`]     | object keys, Hive partitions, escaping |
//! | [`writer`]     | the record writers: JSONL (+zstd/gzip) and Parquet |
//! | [`window`]     | the window engine — pure, no I/O, no wall clock |
//! | [`checkpoint`] | the position cache written to the bucket |
//! | [`seek`]       | the backwards probe-seek that recovers a position from a timestamp |
//! | [`lease`]      | queue ownership across instances, and the commit fence |
//! | [`driver`]     | the per-queue task that wires engine ↔ Queen ↔ S3 ↔ writers |
//! | [`health`]     | `/healthz` and `/metrics` |
//! | [`obs`]        | the windowed-log `Sampler` the broker and the facades share |

pub mod checkpoint;
pub mod config;
pub mod driver;
pub mod health;
pub mod layout;
pub mod lease;
pub mod obs;
pub mod queen;
pub mod s3;
pub mod seek;
pub mod types;
pub mod window;
pub mod writer;
