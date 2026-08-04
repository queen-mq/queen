//! Rust client for [Queen MQ](https://queenmq.com) — a message queue that
//! keeps its data in PostgreSQL.
//!
//! A queue is split into **partitions**, one per entity, created on first push.
//! Each partition is a strictly ordered lane that a consumer group drains
//! independently, so a consumer stuck on one lane never blocks another.
//!
//! ```no_run
//! use queen_mq::{Config, Queen};
//!
//! # async fn example() -> queen_mq::Result<()> {
//! let queen = Queen::connect(Config::new("http://localhost:6789"))?;
//!
//! queen.queue("orders")
//!     .partition("customer-42")
//!     .push(serde_json::json!({ "total": 19.99 }))
//!     .await?;
//!
//! queen.queue("orders")
//!     .group("fulfilment")
//!     .batch(10)
//!     .limit(100)
//!     .consume(|msg| async move {
//!         println!("{}", msg.data);
//!         Ok::<_, std::convert::Infallible>(())
//!     })
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Streams
//!
//! The windowing DSL lives in [`streams`]: tumbling, sliding, session and
//! wall-clock windows, event time with watermarks, aggregates, gates and
//! transactional sinks. Its `config_hash` is byte-compatible with the other
//! SDKs, so the same query can be redeployed across languages.
//!
//! # Differences from the other SDKs
//!
//! Deliberate, and each one is a place where matching the others would have
//! meant shipping something worse:
//!
//! * **`pop()` returns `Err` on failure** rather than an empty vector. Turning
//!   a 403 or an exhausted retry budget into "no messages" makes an outage look
//!   like an idle queue.
//! * **Acks read the consumer group from the message**, not from a separate
//!   argument, so a forgotten group cannot ack the wrong cursor.
//! * **No `traceId` on plain pushes.** The broker's push path cannot store one
//!   — see [`queen_protocol::PushItem`]. It works inside a transaction, and
//!   [`transaction::TransactionBuilder::push_item`] exposes it there.
//! * **Signal handlers are opt-in** behind the `signals` feature. A library has
//!   no business installing process-wide handlers by default.
//! * **`limit` counts across all workers**, not per worker.
//! * **No `clear_queue` or `move_message_to_dlq`.** Both routes the JS SDK
//!   calls for these answer 404 on this broker, so they are not offered here.

pub mod admin;
pub mod buffer;
pub mod config;
pub mod consumer;
pub mod error;
mod http;
mod inner;
pub mod lb;
pub mod queue;
pub mod streams;
pub mod transaction;
pub mod uuid;

use std::sync::Arc;

pub use admin::Admin;
pub use buffer::{BufferOptions, BufferStats};
pub use config::{Config, HostHeader, Retry429, RetryKind};
pub use consumer::{Cancel, ConsumeSummary, StopReason};
pub use error::{Error, Result};
pub use lb::Strategy;
pub use queue::QueueBuilder;
pub use transaction::TransactionBuilder;

// Re-exported so callers do not need a direct dependency on the protocol crate
// for the types that appear in this client's signatures.
pub use queen_protocol::{
    AckResult, AckStatus, DlqParams, DlqResponse, Message, PushItem, PushResult, PushStatus,
    QueueOptions, SeekRequest, SubscriptionMode, TraceRequest, TransactionResponse, TxnPushItem,
    TxnResultItem,
};

use crate::buffer::BufferManager;
use crate::http::HttpClient;
use crate::inner::Inner;

/// A connected client.
///
/// Cheap to clone — every clone shares one connection pool, one load balancer
/// and one set of push buffers. Create it once and share it.
#[derive(Clone)]
pub struct Queen {
    inner: Arc<Inner>,
}

impl Queen {
    /// Build a client. Validates the configuration and the TLS setup, but opens
    /// no connection — the first request does that.
    pub fn connect(config: Config) -> Result<Self> {
        let http = Arc::new(HttpClient::new(config)?);
        let buffers = BufferManager::new(Arc::clone(&http));
        Ok(Self {
            inner: Arc::new(Inner { http, buffers }),
        })
    }

    /// Convenience for the common single-broker case.
    pub fn connect_to(url: impl Into<String>) -> Result<Self> {
        Self::connect(Config::new(url))
    }

    pub(crate) fn inner_handle(&self) -> Arc<Inner> {
        Arc::clone(&self.inner)
    }

    /// Start a call chain against a named queue.
    pub fn queue(&self, name: impl Into<String>) -> QueueBuilder {
        QueueBuilder::new(Arc::clone(&self.inner), Some(name.into()))
    }

    /// Start a call chain with no queue name — for discovery pops addressed by
    /// namespace or task.
    pub fn queue_opt(&self, name: Option<String>) -> QueueBuilder {
        QueueBuilder::new(Arc::clone(&self.inner), name)
    }

    /// Administrative and observability endpoints.
    pub fn admin(&self) -> Admin {
        Admin::new(Arc::clone(&self.inner))
    }

    /// Begin an atomic push + ack.
    pub fn transaction(&self) -> TransactionBuilder {
        TransactionBuilder::new(Arc::clone(&self.inner))
    }

    // --------------------------------------------------------------- acking

    /// Mark a message completed, advancing its group's cursor past it.
    pub async fn ack(&self, message: &Message) -> Result<AckResult> {
        self.inner
            .ack_one(message, AckStatus::Completed, None)
            .await
    }

    /// Reject a message. It is redelivered until the queue's retry limit runs
    /// out, then dead-lettered; `reason` is recorded on the DLQ row.
    pub async fn nack(&self, message: &Message, reason: impl Into<String>) -> Result<AckResult> {
        self.inner
            .ack_one(message, AckStatus::Failed, Some(reason.into()))
            .await
    }

    /// Ack with an explicit outcome — `Retry` to redeliver, `Dlq` to
    /// dead-letter immediately.
    pub async fn ack_with(
        &self,
        message: &Message,
        status: AckStatus,
        reason: Option<String>,
    ) -> Result<AckResult> {
        self.inner.ack_one(message, status, reason).await
    }

    /// Ack a batch in one request. Every message must share a consumer group.
    pub async fn ack_all(&self, messages: &[Message]) -> Result<Vec<AckResult>> {
        self.inner
            .ack_batch(messages, AckStatus::Completed, None)
            .await
    }

    /// Nack a batch in one request.
    pub async fn nack_all(
        &self,
        messages: &[Message],
        reason: impl Into<String>,
    ) -> Result<Vec<AckResult>> {
        self.inner
            .ack_batch(messages, AckStatus::Failed, Some(reason.into()))
            .await
    }

    /// Extend the lease held by a claim, so a slow handler does not lose it.
    ///
    /// Every message from one pop shares a lease, so renewing once covers the
    /// whole batch — including a multi-partition claim.
    pub async fn renew(&self, message: &Message, seconds: Option<i32>) -> Result<bool> {
        if message.lease_id.is_empty() {
            return Err(Error::Invalid(
                "this message holds no lease (it came from an autoAck pop), so there is nothing \
                 to renew"
                    .into(),
            ));
        }
        let resp = self
            .admin()
            .renew_lease(&message.lease_id, seconds)
            .await?;
        Ok(resp.success)
    }

    // -------------------------------------------------------------- buffers

    /// Send everything currently buffered, across every queue and partition.
    pub async fn flush_all_buffers(&self) -> Result<Vec<PushResult>> {
        self.inner.buffers.flush_all().await
    }

    pub fn buffer_stats(&self) -> BufferStats {
        self.inner.buffers.stats()
    }

    /// Flush buffers and release resources.
    ///
    /// Call this before exiting when push buffering is in use: buffered
    /// messages live in this process's memory and are lost otherwise.
    pub async fn close(&self) -> Result<()> {
        self.inner.buffers.flush_all().await?;
        Ok(())
    }

    /// Flush buffers when SIGINT or SIGTERM arrives, then resolve.
    ///
    /// Opt-in, and it installs nothing: the caller decides where in its own
    /// shutdown sequence this belongs.
    #[cfg(feature = "signals")]
    pub async fn shutdown_on_signal(&self) -> Result<()> {
        use tokio::signal::unix::{signal, SignalKind};
        let mut int = signal(SignalKind::interrupt())
            .map_err(|e| Error::Config(format!("cannot listen for SIGINT: {e}")))?;
        let mut term = signal(SignalKind::terminate())
            .map_err(|e| Error::Config(format!("cannot listen for SIGTERM: {e}")))?;
        tokio::select! {
            _ = int.recv() => tracing::info!("SIGINT received, flushing buffers"),
            _ = term.recv() => tracing::info!("SIGTERM received, flushing buffers"),
        }
        self.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_rejects_a_bad_configuration_eagerly() {
        assert!(Queen::connect_to("localhost:6789").is_err());
        assert!(Queen::connect(Config::urls(Vec::<String>::new())).is_err());
        assert!(Queen::connect_to("http://localhost:6789").is_ok());
    }

    #[test]
    fn clones_share_one_set_of_buffers() {
        let a = Queen::connect_to("http://127.0.0.1:1").unwrap();
        let b = a.clone();
        assert!(Arc::ptr_eq(&a.inner, &b.inner));
        assert_eq!(b.buffer_stats().active_buffers, 0);
    }

    #[tokio::test]
    async fn renewing_an_unleased_message_is_rejected_locally() {
        let q = Queen::connect_to("http://127.0.0.1:1").unwrap();
        let msg = Message {
            id: "m".into(),
            transaction_id: "t".into(),
            trace_id: None,
            data: serde_json::json!(null),
            producer_sub: None,
            created_at: "2026-08-04T10:00:00Z".into(),
            partition_id: "p".into(),
            partition: "Default".into(),
            lease_id: String::new(),
            consumer_group: "g".into(),
        };
        let err = q.renew(&msg, None).await.unwrap_err();
        assert!(err.to_string().contains("holds no lease"));
    }

    #[tokio::test]
    async fn acking_nothing_is_a_no_op() {
        let q = Queen::connect_to("http://127.0.0.1:1").unwrap();
        assert!(q.ack_all(&[]).await.unwrap().is_empty());
    }
}
