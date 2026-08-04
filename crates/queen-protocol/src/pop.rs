//! `GET /api/v1/pop/...` — claim messages.
//!
//! Three routes share one response shape:
//! * `/api/v1/pop/queue/:queue` — any partition of a queue
//! * `/api/v1/pop/queue/:queue/partition/:partition` — one named partition
//! * `/api/v1/pop` — discovery by `namespace`/`task`

use serde::{Deserialize, Serialize};

/// One delivered message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    /// Broker-assigned message id (uuidv7).
    pub id: String,

    #[serde(rename = "transactionId")]
    pub transaction_id: String,

    /// Always `None` for a message that arrived via `/api/v1/push` — that path
    /// cannot store a trace id (see [`crate::PushItem`]). Messages pushed
    /// inside a transaction can carry one.
    #[serde(rename = "traceId")]
    pub trace_id: Option<String>,

    /// The payload as pushed. `null` when the stored payload was empty.
    pub data: serde_json::Value,

    /// `sub` claim of the JWT that pushed this message, when the broker had
    /// authentication enabled at push time.
    #[serde(rename = "producerSub")]
    pub producer_sub: Option<String>,

    /// ISO-8601 timestamp of the *segment* this message was stored in. Every
    /// message written by one push call shares it.
    #[serde(rename = "createdAt")]
    pub created_at: String,

    #[serde(rename = "partitionId")]
    pub partition_id: String,

    pub partition: String,

    /// Empty string on an `autoAck` pop — the broker took no lease, so there is
    /// nothing to renew and nothing to ack.
    #[serde(rename = "leaseId")]
    pub lease_id: String,

    #[serde(rename = "consumerGroup")]
    pub consumer_group: String,
}

impl Message {
    /// Whether this delivery holds a lease the consumer is expected to ack.
    /// False for `autoAck` pops, where the broker already committed the cursor.
    pub fn is_leased(&self) -> bool {
        !self.lease_id.is_empty()
    }
}

/// Response body of every pop route.
///
/// The broker emits three distinct shapes through this one type:
///
/// * a normal claim — `success: true` plus the claimed partition's identity;
/// * a *paused* `204`, `{"messages":[],"paused":true}`, when pop maintenance
///   mode is on. There is no `success` key, hence the default;
/// * a failure, `{"success":false,"error":"...","messages":[]}`, still at
///   HTTP 200.
///
/// The top-level `partition`/`partitionId`/`leaseId` describe the *first*
/// claimed partition only. A multi-partition pop (`partitions=N`) returns
/// messages from several lanes, and each carries its own `partitionId` — so
/// per-message fields are the ones to trust. All partitions in one pop do share
/// the single top-level `leaseId`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PopResponse {
    #[serde(default = "default_true")]
    pub success: bool,

    #[serde(default)]
    pub queue: String,

    #[serde(default)]
    pub partition: String,

    #[serde(rename = "partitionId", default)]
    pub partition_id: String,

    #[serde(rename = "leaseId", default)]
    pub lease_id: String,

    #[serde(rename = "consumerGroup", default)]
    pub consumer_group: String,

    #[serde(default)]
    pub messages: Vec<Message>,

    /// Present and true when pop maintenance mode is on. Consumers should treat
    /// it as "no messages, try again" rather than as an error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

fn default_true() -> bool {
    true
}

impl PopResponse {
    pub fn is_paused(&self) -> bool {
        self.paused.unwrap_or(false)
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }
}

/// How a consumer group's cursor is seeded the first time it meets a partition.
///
/// Only ever applied on first contact; an existing cursor is never re-seeded,
/// so replaying a group means deleting it or seeking it, not re-popping with a
/// different mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SubscriptionMode {
    /// Start at the messages arriving from now on.
    New,
    /// Start at the beginning of the partition. The broker's default.
    All,
}

impl SubscriptionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::New => "new",
            Self::All => "all",
        }
    }
}

/// Query parameters accepted by the pop routes.
///
/// Mirrors the broker's `PopParams`. Fields left `None` are omitted from the
/// query string so the broker applies its own default — which is not always the
/// client default (`batch` server-side defaults to 200, while every SDK sends
/// an explicit small batch).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PopParams {
    pub batch: Option<i32>,
    /// Claim up to N partitions in one round-trip, sharing the `batch` budget
    /// and one lease. 1 (or `None`) is single-partition.
    pub partitions: Option<i32>,
    /// Server-side ack at delivery: the cursor commits immediately and no lease
    /// is taken. A crash after delivery loses the messages.
    pub auto_ack: Option<bool>,
    /// Long-poll instead of returning empty.
    pub wait: Option<bool>,
    /// Long-poll timeout in milliseconds. Sent as `timeout`, not
    /// `timeoutMillis`.
    pub timeout_millis: Option<u64>,
    /// Per-request lease override in seconds, winning over the queue's
    /// configured `leaseTime`.
    pub lease_seconds: Option<i32>,
    pub consumer_group: Option<String>,
    pub subscription_mode: Option<SubscriptionMode>,
    /// `now`, an ISO-8601 timestamp, or empty.
    pub subscription_from: Option<String>,
    /// Discovery pops only (`/api/v1/pop`).
    pub namespace: Option<String>,
    /// Discovery pops only (`/api/v1/pop`).
    pub task: Option<String>,
}

impl PopParams {
    /// Render as `key=value` pairs, ready to be percent-encoded by the caller's
    /// HTTP layer. Order is stable so tests can assert on it.
    pub fn to_pairs(&self) -> Vec<(&'static str, String)> {
        let mut out: Vec<(&'static str, String)> = Vec::new();
        if let Some(v) = self.batch {
            out.push(("batch", v.to_string()));
        }
        if let Some(v) = self.wait {
            out.push(("wait", v.to_string()));
        }
        if let Some(v) = self.timeout_millis {
            out.push(("timeout", v.to_string()));
        }
        if let Some(v) = &self.consumer_group {
            out.push(("consumerGroup", v.clone()));
        }
        if let Some(v) = &self.namespace {
            out.push(("namespace", v.clone()));
        }
        if let Some(v) = &self.task {
            out.push(("task", v.clone()));
        }
        // Only ever sent when true: the broker treats presence as opt-in and
        // every other SDK omits it otherwise.
        if self.auto_ack == Some(true) {
            out.push(("autoAck", "true".to_string()));
        }
        if let Some(v) = self.subscription_mode {
            out.push(("subscriptionMode", v.as_str().to_string()));
        }
        if let Some(v) = &self.subscription_from {
            out.push(("subscriptionFrom", v.clone()));
        }
        if let Some(v) = self.partitions {
            if v > 1 {
                out.push(("partitions", v.to_string()));
            }
        }
        if let Some(v) = self.lease_seconds {
            out.push(("leaseSeconds", v.to_string()));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_normal_claim() {
        let wire = r#"{"success":true,"queue":"orders","partition":"eu","partitionId":"p1","leaseId":"L1","consumerGroup":"g","messages":[
            {"id":"m1","transactionId":"t1","traceId":null,"data":{"a":1},"producerSub":null,
             "createdAt":"2026-08-04T10:00:00Z","partitionId":"p1","partition":"eu","leaseId":"L1","consumerGroup":"g"}]}"#;
        let got: PopResponse = serde_json::from_str(wire).unwrap();
        assert!(got.success);
        assert_eq!(got.messages.len(), 1);
        assert_eq!(got.messages[0].data, serde_json::json!({"a": 1}));
        assert!(got.messages[0].is_leased());
        assert!(got.messages[0].trace_id.is_none());
    }

    #[test]
    fn parses_the_paused_204_shape() {
        // No `success` key at all — the default must not read as a failure.
        let got: PopResponse = serde_json::from_str(r#"{"messages":[],"paused":true}"#).unwrap();
        assert!(got.success);
        assert!(got.is_paused());
        assert!(got.is_empty());
    }

    #[test]
    fn parses_the_error_shape() {
        let got: PopResponse = serde_json::from_str(
            r#"{"success":false,"error":"namespace or task is required","messages":[]}"#,
        )
        .unwrap();
        assert!(!got.success);
        assert_eq!(got.error.as_deref(), Some("namespace or task is required"));
    }

    #[test]
    fn auto_ack_delivery_is_not_leased() {
        let wire = r#"{"success":true,"queue":"q","partition":"p","partitionId":"p1","leaseId":"","consumerGroup":"g","messages":[
            {"id":"m1","transactionId":"t1","traceId":null,"data":null,"producerSub":null,
             "createdAt":"2026-08-04T10:00:00Z","partitionId":"p1","partition":"p","leaseId":"","consumerGroup":"g"}]}"#;
        let got: PopResponse = serde_json::from_str(wire).unwrap();
        assert!(!got.messages[0].is_leased());
    }

    #[test]
    fn params_omit_defaults_and_rename_timeout() {
        let p = PopParams {
            batch: Some(10),
            wait: Some(true),
            timeout_millis: Some(30000),
            consumer_group: Some("g".into()),
            ..Default::default()
        };
        let pairs = p.to_pairs();
        assert_eq!(
            pairs,
            vec![
                ("batch", "10".to_string()),
                ("wait", "true".to_string()),
                ("timeout", "30000".to_string()),
                ("consumerGroup", "g".to_string()),
            ]
        );
    }

    #[test]
    fn params_suppress_auto_ack_false_and_partitions_one() {
        let p = PopParams {
            auto_ack: Some(false),
            partitions: Some(1),
            ..Default::default()
        };
        assert!(p.to_pairs().is_empty());
    }

    #[test]
    fn params_send_multi_partition_and_subscription() {
        let p = PopParams {
            partitions: Some(8),
            subscription_mode: Some(SubscriptionMode::New),
            subscription_from: Some("now".into()),
            ..Default::default()
        };
        let pairs = p.to_pairs();
        assert!(pairs.contains(&("partitions", "8".to_string())));
        assert!(pairs.contains(&("subscriptionMode", "new".to_string())));
        assert!(pairs.contains(&("subscriptionFrom", "now".to_string())));
    }
}
