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

    /// How many partitions this pop actually claimed.
    ///
    /// The renderer always emits it (`server/src/handlers/data.rs:2261`,
    /// `out.push_str("],\"partitionsClaimed\":")`), including `0` on an empty
    /// claim, and it is the only way to tell a `partitions=8` pop that found
    /// one busy lane from one that found eight. Absent from the paused and
    /// failure bodies, which never reach the renderer.
    #[serde(
        rename = "partitionsClaimed",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub partitions_claimed: Option<i32>,
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
    /// Start at the messages arriving from now on. The broker's default when the
    /// pop names a consumer group and sends no mode (`DEFAULT_SUBSCRIPTION_MODE`).
    New,
    /// Start at the beginning of the retained backlog. Always applied to group-less
    /// "queue mode" pops, which ignore the mode entirely.
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

    /// A pop body byte for byte as `render_pop_parts` builds it.
    ///
    /// Transcribed from the renderer in `server/src/handlers/data.rs`: the
    /// top-level keys and their order at :2077-2087, the per-message object at
    /// :2185-2230, the trailing `partitionsClaimed` at :2261. Two partitions
    /// were claimed, so the top-level identity describes `eu` while the third
    /// message comes from `us` — that divergence is the whole reason the
    /// per-message fields exist.
    ///
    /// This is the response every consumer receives on every successful pop and
    /// it had no conformance test at all. A literal invented inside the test
    /// would agree with whatever this struct happens to say, which is exactly
    /// how `DlqMessage.error` spent its life reading a key the broker never
    /// sends.
    const POP_BODY_FROM_THE_RENDERER: &str = concat!(
        r#"{"success":true,"queue":"orders","partition":"eu","#,
        r#""partitionId":"0198f0aa-0000-7000-8000-0000000000e1","#,
        r#""leaseId":"0198f0aa-0000-7000-8000-00000000a001","#,
        r#""consumerGroup":"fulfilment","messages":["#,
        r#"{"id":"0198f0aa-0000-7000-8000-000000000001","#,
        r#""transactionId":"order-1","traceId":null,"data":{"total":19.99},"#,
        r#""producerSub":null,"createdAt":"2026-08-04T09:15:00.123Z","#,
        r#""partitionId":"0198f0aa-0000-7000-8000-0000000000e1","partition":"eu","#,
        r#""leaseId":"0198f0aa-0000-7000-8000-00000000a001","consumerGroup":"fulfilment"},"#,
        r#"{"id":"0198f0aa-0000-7000-8000-000000000002","#,
        r#""transactionId":"order-2","traceId":null,"data":null,"#,
        r#""producerSub":null,"createdAt":"2026-08-04T09:15:00.123Z","#,
        r#""partitionId":"0198f0aa-0000-7000-8000-0000000000e1","partition":"eu","#,
        r#""leaseId":"0198f0aa-0000-7000-8000-00000000a001","consumerGroup":"fulfilment"},"#,
        r#"{"id":"0198f0aa-0000-7000-8000-000000000003","#,
        r#""transactionId":"order-3","#,
        r#""traceId":"0198f0aa-0000-7000-8000-0000000000c3","data":[1,2,3],"#,
        r#""producerSub":"svc-checkout","createdAt":"2026-08-04T09:15:01.000Z","#,
        r#""partitionId":"0198f0aa-0000-7000-8000-0000000000e2","partition":"us","#,
        r#""leaseId":"0198f0aa-0000-7000-8000-00000000a001","consumerGroup":"fulfilment"}"#,
        r#"],"partitionsClaimed":2}"#,
    );

    #[test]
    fn a_rendered_pop_body_parses_with_every_field_populated() {
        let got: PopResponse = serde_json::from_str(POP_BODY_FROM_THE_RENDERER)
            .expect("the body the broker renders for every pop must deserialize");
        assert!(got.success);
        assert_eq!(got.queue, "orders");
        assert_eq!(got.consumer_group, "fulfilment");
        assert_eq!(got.messages.len(), 3, "one message per rendered frame");
        assert_eq!(got.partitions_claimed, Some(2));

        // The payload is spliced into the body verbatim, so every JSON shape a
        // producer can push has to survive the round trip — object, null and
        // array all appear in one real batch.
        assert_eq!(got.messages[0].data, serde_json::json!({"total": 19.99}));
        assert_eq!(
            got.messages[1].data,
            serde_json::Value::Null,
            "an empty stored payload renders as `null`, not as an absent key"
        );
        assert_eq!(got.messages[2].data, serde_json::json!([1, 2, 3]));

        assert_eq!(
            got.messages[2].producer_sub.as_deref(),
            Some("svc-checkout")
        );
        assert!(
            got.messages[0].producer_sub.is_none(),
            "an unauthenticated push renders `producerSub: null`"
        );
        assert!(got.messages.iter().all(|m| m.is_leased()));
    }

    #[test]
    fn the_top_level_identity_describes_only_the_first_claimed_partition() {
        // A `partitions=N` pop returns messages from several lanes but renders
        // ONE top-level partition/partitionId, the first claimed. A consumer
        // that acks with the top-level partitionId therefore acks the wrong
        // lane for every message that came from another one; the per-message
        // fields are the only safe source, and this pins that they differ.
        let got: PopResponse = serde_json::from_str(POP_BODY_FROM_THE_RENDERER).unwrap();
        assert_eq!(got.partition, "eu");
        assert_eq!(got.partition_id, "0198f0aa-0000-7000-8000-0000000000e1");
        assert_eq!(got.messages[2].partition, "us");
        assert_ne!(
            got.messages[2].partition_id, got.partition_id,
            "the third message came from the second claimed lane, so its \
             partitionId must not be the top-level one"
        );
        // The lease, unlike the partition, really is shared by every partition
        // in one pop: one renewal covers the whole claim.
        assert!(got
            .messages
            .iter()
            .all(|m| m.lease_id == got.lease_id && !m.lease_id.is_empty()));
    }

    #[test]
    fn an_empty_claim_renders_blank_identity_rather_than_null() {
        // `render_pop_parts(&[], ...)` (data.rs:2058-2061) falls back to
        // ("", "") for the first partition instead of omitting the keys, so an
        // empty poll arrives as empty STRINGS with success:true. Parsing this
        // as a failure — or choking on it — turns every quiet poll into an
        // error.
        let wire = concat!(
            r#"{"success":true,"queue":"orders","partition":"","partitionId":"","#,
            r#""leaseId":"0198f0aa-0000-7000-8000-00000000a001","#,
            r#""consumerGroup":"fulfilment","messages":[],"partitionsClaimed":0}"#,
        );
        let got: PopResponse = serde_json::from_str(wire).expect("an empty poll must parse");
        assert!(got.success, "an empty poll is not a failure");
        assert!(got.is_empty());
        assert!(!got.is_paused());
        assert_eq!(got.partition, "");
        assert_eq!(got.partitions_claimed, Some(0));
    }

    #[test]
    fn a_pop_failure_omits_every_identity_key_and_still_reads_as_a_failure() {
        // `pop_error_body` (data.rs:1982-1986) writes only three keys. Both
        // defaults have to hold at once: the absent `queue`/`leaseId`/… must
        // fill in, while the EXPLICIT success:false must beat `default_true` —
        // if that default ever leaked over a present key, a failed pop would
        // read as a successful empty one and the consumer would poll forever
        // instead of surfacing the error.
        let wire = r#"{"success":false,"error":"parse","messages":[]}"#;
        let got: PopResponse = serde_json::from_str(wire).unwrap();
        assert!(
            !got.success,
            "an explicit success:false must survive the default"
        );
        assert_eq!(got.error.as_deref(), Some("parse"));
        assert_eq!(got.queue, "");
        assert_eq!(got.lease_id, "");
        assert_eq!(
            got.partitions_claimed, None,
            "the failure body never reaches the renderer, so it carries no count"
        );
    }

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
        // The literal string the pop-maintenance branch writes, from
        // `server/src/handlers/data.rs` — the same constant appears at :561
        // (queue pop), :1683 and :1827 (the partition and discovery routes).
        //
        // It rides an HTTP 204, which is the interesting part: a transport that
        // short-circuits "no content" to an empty body throws this away and
        // maintenance silently reads as "the queue is quiet". The Rust client
        // deliberately reads the 204's body (`src/http.rs:338-348`), so the
        // signal does arrive here and `paused` is a live field, not a dead one.
        // If that branch is ever simplified away, this shape stops reaching
        // `is_paused()` and the only symptom is consumers spinning through a
        // maintenance window.
        let got: PopResponse = serde_json::from_str(r#"{"messages":[],"paused":true}"#).unwrap();
        assert!(
            got.success,
            "there is no `success` key on the paused body, and the default must \
             not turn maintenance into a failure"
        );
        assert!(got.is_paused());
        assert!(got.is_empty());
        assert_eq!(got.partitions_claimed, None);
    }

    #[test]
    fn a_body_from_a_newer_broker_still_parses() {
        // The renderer already sends one key this struct did not model
        // (`partitionsClaimed`, data.rs:2261) and will send more. An unknown
        // key must be dropped, never rejected: a client that fails to decode a
        // pop cannot ack, so its leases lapse and every message is redelivered.
        let wire = concat!(
            r#"{"success":true,"queue":"q","partition":"p","partitionId":"p1","#,
            r#""leaseId":"L1","consumerGroup":"g","partitionsClaimed":1,"#,
            r#""someFutureField":{"nested":true},"messages":["#,
            r#"{"id":"m1","transactionId":"t1","traceId":null,"data":1,"producerSub":null,"#,
            r#""createdAt":"2026-08-04T10:00:00Z","partitionId":"p1","partition":"p","#,
            r#""leaseId":"L1","consumerGroup":"g","deliveryAttempt":2}]}"#,
        );
        let got: PopResponse =
            serde_json::from_str(wire).expect("an unmodelled key must not fail the decode");
        assert_eq!(got.messages.len(), 1);
        assert_eq!(got.partitions_claimed, Some(1));
    }

    #[test]
    fn optional_message_fields_read_the_same_absent_or_null() {
        // The renderer always writes `traceId`/`producerSub` as literal nulls
        // (data.rs:2189-2219), but a fixture, a proxy or a hand-rolled producer
        // may omit them. Both must land on `None` rather than one of them
        // failing to decode.
        let nulls = r#"{"id":"m1","transactionId":"t1","traceId":null,"data":{},"producerSub":null,"createdAt":"2026-08-04T10:00:00Z","partitionId":"p1","partition":"p","leaseId":"","consumerGroup":"g"}"#;
        let absent = r#"{"id":"m1","transactionId":"t1","data":{},"createdAt":"2026-08-04T10:00:00Z","partitionId":"p1","partition":"p","leaseId":"","consumerGroup":"g"}"#;
        let a: Message = serde_json::from_str(nulls).unwrap();
        let b: Message = serde_json::from_str(absent).expect("absent optionals must default");
        assert_eq!(a, b);
        assert!(a.trace_id.is_none() && a.producer_sub.is_none());
        assert!(
            !a.is_leased(),
            "an empty leaseId is the autoAck marker, so this must not read as leased"
        );
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
