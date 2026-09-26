//! queen-proxy — multi-tenant data-plane gateway for QueenMQ, as a library
//! the broker links and runs in-process (the single binary,
//! PLAN_SINGLE_BINARY.md W3/W4). Its state lives in the broker's replicated
//! KV. Spec: PLAN_QUEEN_PROXY_CLOUD.md (repo root). Module ownership:
//! CONTRACTS.md.

pub mod acting;
pub mod app;
pub mod auth;
pub mod cache;
pub mod config;
pub mod console;
/// The control-plane API (tenants, clusters, keys) behind a token.
pub mod cp;
pub mod errors;
pub mod gateway;
/// W7 edge hardening (PLAN_SINGLE_BINARY.md): per-IP limits, request limits,
/// CSRF/CORS/security headers, login guard, TLS serving, fuzz entry points.
pub mod harden;
pub mod httpget;
pub mod kafka_identity;
pub mod kafka_kv;
pub mod limits;
pub mod meter;
pub mod oauth;
pub mod obs;
pub mod operator;
pub mod registry;
pub mod routes;
pub mod spool;
pub mod state;
/// Where the proxy keeps its state: the broker's replicated KV.
pub mod store;
/// Where the data plane sends a request: a cell broker over HTTP, or the
/// broker it runs inside.
pub mod upstream;
pub mod webapp;
