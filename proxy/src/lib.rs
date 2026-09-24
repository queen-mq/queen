//! queen-proxy — multi-tenant data-plane gateway for QueenMQ cells, as a
//! library: the standalone binary (`src/main.rs`) and the broker's single
//! binary (PLAN_SINGLE_BINARY.md W3/W4) both build it from here.
//! Spec: PLAN_QUEEN_PROXY_CLOUD.md (repo root). Module ownership: CONTRACTS.md.

pub mod acting;
pub mod app;
pub mod auth;
pub mod cache;
pub mod config;
pub mod console;
/// The control-plane API for deployments without a SQL console.
pub mod cp;
pub mod db;
pub mod errors;
pub mod gateway;
pub mod httpget;
pub mod kafka_identity;
pub mod kafka_kv;
pub mod limits;
pub mod meter;
pub mod oauth;
pub mod obs;
pub mod operator;
pub mod pgtls;
pub mod registry;
pub mod routes;
// PLAN_S3_SINK.md §8/D4 — the S3 sink's reserved KV key space, the body-
// conditional twin of `kafka_kv` one namespace over.
pub mod s3_kv;
pub mod spool;
pub mod state;
/// Where the proxy keeps its state: its own Postgres, or the broker's
/// replicated KV (single binary).
pub mod store;
/// Where the data plane sends a request: a cell broker over HTTP, or the
/// broker it runs inside.
pub mod upstream;
pub mod webapp;
