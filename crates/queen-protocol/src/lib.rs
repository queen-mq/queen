//! Canonical wire types for the Queen MQ HTTP API.
//!
//! This crate is the single written-down definition of what goes over
//! `/api/v1/*`. Two consumers share it:
//!
//! * the **broker** (`server/`), as a dev-dependency — its request parsers are
//!   deliberately zero-copy (`PushItem<'a>` borrows out of the request body)
//!   and its responses are rendered by hand into a pre-sized `String`, both for
//!   throughput. Neither can be replaced by these owned types without
//!   regressing the hot path, so instead the broker carries *conformance tests*
//!   that round-trip its own structs and rendered bytes through the types here.
//!   A field that drifts on one side fails to compile or fails a test.
//!
//! * the **Rust client** (`clients/client-rust/`), as a normal dependency —
//!   it serializes and deserializes these types directly.
//!
//! ## Naming is faithful, not tidy
//!
//! The wire is not internally consistent, and this crate reproduces it exactly
//! rather than papering over it. Notably `/api/v1/push` answers with
//! `message_id` and `transaction_id` in snake_case next to `queueName` in
//! camelCase, while every other endpoint is camelCase throughout. Renaming any
//! of it here would break real clients, so the quirks are encoded and
//! commented where they appear.

pub mod ack;
pub mod admin;
pub mod error;
pub mod pop;
pub mod push;
pub mod streams;
pub mod transaction;

pub use ack::{AckBatchItem, AckBatchRequest, AckRequest, AckResult, AckStatus};
pub use admin::{
    ConfigureRequest, ConfigureResponse, DlqMessage, DlqParams, DlqResponse, MaintenanceRequest,
    MaintenanceResponse, QueueOptions, RenewLeaseRequest, RenewLeaseResponse, SeekRequest,
    SubscriptionRequest, TraceRequest, TraceResponse,
};
pub use error::{ErrorBody, ErrorCode};
pub use pop::{Message, PopParams, PopResponse, SubscriptionMode};
pub use streams::{
    parse_state_key, session_state_key, state_key_for, CycleAck, CycleAckResult, CycleRequest,
    CycleResponse, RegisterRequest, RegisterResponse, SinkPushItem, StateGetRequest,
    StateGetResponse, StateKeyParts, StateOp, StateRow, STATE_KEY_SEP, WATERMARK_STATE_KEY,
};
pub use push::{PushItem, PushRequest, PushResult, PushStatus};
pub use transaction::{
    TransactionRequest, TransactionResponse, TxnAckOperation, TxnOperation, TxnPushItem,
    TxnResultItem,
};

/// Default partition name when a push does not name one. The broker applies the
/// same default (`PushItem.partition.unwrap_or("Default")`), so a client that
/// omits the field and one that sends `"Default"` address the same lane.
pub const DEFAULT_PARTITION: &str = "Default";

/// Consumer-group placeholder used for queue-mode (group-less) consumption.
/// It is the group name the broker records for a pop with no `consumerGroup`,
/// and it is what the clients' affinity keys interpolate — see
/// [`grouping_key`].
pub const QUEUE_MODE_GROUP: &str = "__QUEUE_MODE__";

/// Backend-affinity key for a poll, matching the broker's
/// `PollIntention::grouping_key()`.
///
/// Consumers that share a key must reach the same broker replica, otherwise two
/// replicas contend for the same partition's claim. Both forms exist because a
/// pop is addressed either by queue (optionally by partition) or by
/// namespace/task discovery.
pub fn grouping_key(
    queue: Option<&str>,
    partition: Option<&str>,
    namespace: Option<&str>,
    task: Option<&str>,
    group: Option<&str>,
) -> Option<String> {
    let group = group.unwrap_or(QUEUE_MODE_GROUP);
    match queue {
        Some(q) => Some(format!("{}:{}:{}", q, partition.unwrap_or("*"), group)),
        None if namespace.is_some() || task.is_some() => Some(format!(
            "{}:{}:{}",
            namespace.unwrap_or("*"),
            task.unwrap_or("*"),
            group
        )),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grouping_key_queue_forms() {
        assert_eq!(
            grouping_key(Some("orders"), None, None, None, Some("workers")).unwrap(),
            "orders:*:workers"
        );
        assert_eq!(
            grouping_key(Some("orders"), Some("eu"), None, None, None).unwrap(),
            "orders:eu:__QUEUE_MODE__"
        );
    }

    #[test]
    fn grouping_key_discovery_forms() {
        assert_eq!(
            grouping_key(None, None, Some("billing"), Some("invoice"), Some("g")).unwrap(),
            "billing:invoice:g"
        );
        assert_eq!(
            grouping_key(None, None, None, Some("invoice"), None).unwrap(),
            "*:invoice:__QUEUE_MODE__"
        );
        assert!(grouping_key(None, None, None, None, Some("g")).is_none());
    }
}
