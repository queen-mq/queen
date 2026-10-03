//! queen-s3 — the S3 / data-lake sink of QueenMQ, linked into the broker and
//! run in-process on every node of the cluster.
//!
//! The broker reads its node-wide knobs from the environment
//! ([`NodeKnobs::from_env_with`]) into one [`SinkShared`] per node, builds a
//! [`Config`] per tenant — the default tenant's from the `QUEEN_S3_*`
//! environment ([`Config::from_env`]), every other's from its control-plane
//! document ([`Config::from_tenant_doc`]) — and a [`Sink`] per tenant over that
//! tenant's in-process [`queen::QueenApi`] ([`Sink::new_shared`]) — the twins
//! of `POST /api/v1/fetch` and
//! `POST /api/v1/partitions/changed`, served from the node's own applied state,
//! and of `POST /api/v1/kv`, whose reads are linearizable — and runs
//! [`Sink::run`] until it stops. Per queue,
//! the sink keeps three small documents in Queen's key/value store (the window
//! intent, the commit pointer and the ownership lease) and writes open-format
//! objects (JSONL, Parquet) under a Hive layout into any S3-compatible bucket.
//!
//! The one idea everything else hangs off (plan §4): **the commit unit is a
//! time window on the broker's log clock**, `[T_{k-1}, T_k)` over each
//! record's stamp, closed only at or below the `safeTime` of the node that
//! reads it. A record's stamp is the one its append got in the replicated log,
//! stamps strictly increase in log order, and `safeTime` is the greatest stamp
//! the node has applied — so a window is a deterministic set (stamps are
//! co-monotone with offsets in a partition), a retried upload is byte-identical,
//! and exactly-once needs no offset-range object names, no conditional PUT and
//! no LIST. Per-partition positions are a cache, never the truth.
//!
//! Module map (the ownership map of the build, kept honest by the tests):
//!
//! | module | what |
//! |---|---|
//! | [`sink`]       | the entry point: [`Sink`] — the queue set, the leases, the stop, status and metrics |
//! | [`types`]      | the shared vocabulary: [`types::Micros`], [`types::Record`], bounds, KV documents, manifest, checkpoint |
//! | [`config`]     | `QUEEN_S3_*` from the environment, secrets masked |
//! | [`queen`]      | the broker-side trait, the KV wire and answer parsing, and the test double |
//! | [`s3`]         | SigV4 signing and the object-store client, plus an in-memory double |
//! | [`layout`]     | object keys, Hive partitions, escaping |
//! | [`writer`]     | the record writers: JSONL (+zstd/gzip) and Parquet |
//! | [`window`]     | the window engine — pure, no I/O, no wall clock |
//! | [`checkpoint`] | the position cache written to the bucket |
//! | [`seek`]       | the backwards probe-seek that recovers a position from a timestamp |
//! | [`lease`]      | queue ownership across nodes, and the commit fence |
//! | [`placement`]  | presence, fair shares, and giving queues back to spread them over the nodes |
//! | [`driver`]     | the per-queue task that wires engine ↔ Queen ↔ S3 ↔ writers |
//! | [`health`]     | the health verdict |
//! | [`status`]     | the per-queue board [`Sink::status`] reads |
//! | [`obs`]        | the windowed-log `Sampler` the broker and the facades share, and the metrics |

pub mod checkpoint;
pub mod config;
pub mod driver;
pub mod health;
pub mod layout;
pub mod lease;
pub mod obs;
pub mod placement;
pub mod queen;
pub mod s3;
pub mod seek;
pub mod sink;
pub mod status;
pub mod types;
pub mod window;
pub mod writer;

pub use config::{Config, NodeKnobs};
pub use sink::{Sink, SinkShared};
