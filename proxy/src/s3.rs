//! The S3 / data-lake sink's side of the control plane: what the broker
//! hands the proxy so `/api/cp/clusters/:slug/s3` (cp.rs) can configure a
//! cluster's sink without the proxy carrying any S3 code.
//!
//! Every broker tenant (cluster) may mirror its queues to its own bucket with
//! its own credentials, configured only through the control plane. The proxy
//! keeps one row per cluster (`px.s3sinks`, store/schema.rs
//! [`S3SinkDoc`](crate::store::schema::S3SinkDoc)); the broker's sink manager
//! reads every row and runs one sink per broker tenant. The secret is stored
//! only as [`S3Sinks::seal`] returned it, sealed with the cell's
//! `QUEEN_ENCRYPTION_KEY`, so it is never in the raft log or a snapshot in
//! clear; a cell without that key refuses the call instead.

/// The S3 sink's side of the control plane, provided by the broker (the proxy has no S3 code).
pub trait S3Sinks: Send + Sync {
    /// Check a tenant sink config document — the PUT body minus `secretKey` and `enabled` —
    /// exactly as the sink will read it. Err: one line naming the field.
    fn validate(&self, broker_tenant: &str, config: &serde_json::Value) -> Result<(), String>;
    /// Seal a secret with this cell's QUEEN_ENCRYPTION_KEY (an opaque string to store).
    /// Err when the cell has no encryption key.
    fn seal(&self, secret: &str) -> Result<String, String>;
}
