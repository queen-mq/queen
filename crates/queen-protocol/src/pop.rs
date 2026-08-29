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

    /// PLAN_CONFLATION §3.1 — present and true when this pop was served under
    /// last-value delivery, on EMPTY responses too. That is the whole
    /// degrade-loudly contract (§4): an SDK that sent `conflation=true` and does
    /// not see this key on the first response is talking to a broker older than
    /// 1.1.0 and must error instead of silently draining the backlog one message
    /// at a time. Absent (not `false`) on every non-conflating response.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflation: Option<bool>,

    /// PLAN_CONFLATION §3.3 — present and true when this pop asked for a
    /// conflation policy the group does not have. The STORED group setting won;
    /// `conflation` above reports what was actually applied.
    #[serde(
        rename = "conflationConflict",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub conflation_conflict: Option<bool>,

    /// POP AUTOPILOT — what the broker chose for this pop, present only when the
    /// request sent `autopilot=true` and the broker resolved at least one knob
    /// for it. Absent (not zeroed) on every other response, which is what keeps
    /// a non-opted-in deployment byte-identical to a pre-1.2 broker's.
    ///
    /// It cannot ride a `204`, which has no body at all, so an empty
    /// non-conflating pop carries no echo even under autopilot. The advice is
    /// stable across pops of a lane, so a client that wants it reads it from a
    /// response that carried messages.
    #[serde(default, deserialize_with = "lenient_autopilot")]
    pub autopilot: Option<AutopilotEcho>,
}

/// The broker's account of how it sized one pop (`server/src/pop_autopilot.rs`,
/// `append_echo`).
///
/// Reading it is optional — the messages are already sized by it — but it is the
/// only view a client has of the controller, and the only source of the pacing
/// advice an empty-poll loop can honour.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutopilotEcho {
    /// Sweep width the claim actually used.
    #[serde(default)]
    pub partitions: i32,
    /// Message budget the claim actually used.
    #[serde(default)]
    pub batch: i32,
    /// How long the broker advises waiting before polling again. Emitted only
    /// when the broker has an opinion; it is advice, not a lease.
    #[serde(rename = "waitMs", default, skip_serializing_if = "Option::is_none")]
    pub wait_ms: Option<u64>,
}

/// Deserialize the echo WITHOUT letting it fail the whole response.
///
/// This field is the broker telling the client what it did. A client that
/// refuses to decode a pop — and therefore stops consuming — because a newer
/// broker grew a fourth number, or spelled one of these as a string, would be a
/// self-inflicted outage over a field nothing depends on. So an absent, null,
/// non-object or wrongly-typed value degrades to "no echo" / zero, exactly as
/// the Go, JS, Python and PHP clients do.
fn lenient_autopilot<'de, D>(de: D) -> Result<Option<AutopilotEcho>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = serde_json::Value::deserialize(de)?;
    let Some(obj) = raw.as_object() else {
        return Ok(None);
    };
    Ok(Some(AutopilotEcho {
        partitions: obj
            .get("partitions")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0) as i32,
        batch: obj
            .get("batch")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0) as i32,
        wait_ms: obj.get("waitMs").and_then(serde_json::Value::as_u64),
    }))
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

    /// PLAN_CONFLATION §4 — did the broker actually apply last-value delivery?
    /// An SDK that requested conflation checks this on its FIRST response and
    /// raises when it is false: an old broker ignores the unknown query parameter
    /// and answers with the whole backlog, which is exactly the silent failure
    /// the feature must not have.
    pub fn conflation_applied(&self) -> bool {
        self.conflation.unwrap_or(false)
    }

    /// PLAN_CONFLATION §3.3 — this consumer disagreed with the group's stored
    /// policy; the stored one is in force. SDKs warn ONCE per (queue, group).
    pub fn has_conflation_conflict(&self) -> bool {
        self.conflation_conflict.unwrap_or(false)
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
    /// Last-value delivery for this consumer GROUP on this queue
    /// (PLAN_CONFLATION §1.1): a pop of a partition delivers exactly the newest
    /// visible message and the ack commits the whole span behind it. Declared in
    /// consume/subscribe options, persisted on the group's FIRST registration,
    /// and from then on the stored value wins for every consumer of that group —
    /// it is not a per-call flag. Requires a `consumer_group`, and cannot be
    /// combined with `auto_ack` (the broker refuses both with 400).
    /// Broker >= 1.1.0; a conflating pop response echoes `"conflation":true`,
    /// empty responses included, which is how an SDK detects an older broker.
    pub conflation: Option<bool>,
    /// Discovery pops only (`/api/v1/pop`).
    pub namespace: Option<String>,
    /// Discovery pops only (`/api/v1/pop`).
    pub task: Option<String>,

    /// POP AUTOPILOT — "broker, choose the knobs I did not send".
    ///
    /// `Some(true)` (never `Some(false)`, the same rule `auto_ack` and
    /// `conflation` follow) makes the broker size every one of `batch` and
    /// `partitions` that is `None` here, from state no client can see. A knob
    /// that IS set stays the caller's and is never overridden — the choice is
    /// per dimension — and setting both leaves nothing to decide, which is why
    /// `to_pairs` then emits the pre-autopilot request unchanged.
    ///
    /// A broker older than 1.2 drops the unknown parameter and applies its OWN
    /// defaults (batch 200, partitions 1) to the omitted knobs. That is a sizing
    /// difference, not a correctness one, so unlike conflation there is no
    /// degrade-loudly check: nothing is lost, misordered or delivered twice.
    pub autopilot: Option<bool>,
}

impl PopParams {
    /// Render as `key=value` pairs, ready to be percent-encoded by the caller's
    /// HTTP layer. Order is stable so tests can assert on it.
    pub fn to_pairs(&self) -> Vec<(&'static str, String)> {
        let mut out: Vec<(&'static str, String)> = Vec::new();
        // Only ever sent when true. First, so that a request which is NOT
        // engaging autopilot is byte-identical — key order included — to the one
        // this crate rendered before the parameter existed.
        let autopilot = self.autopilot == Some(true);
        if autopilot {
            out.push(("autopilot", "true".to_string()));
        }
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
            // The legacy gate: partitions travels only above 1, because 1 IS the
            // server-side default and a v4-era client never sent it. Under
            // autopilot the gate is lifted, and it has to be: a caller who says
            // 1 is pinning a width the controller would otherwise widen, and
            // omitting the key is now how "you choose" is spelled.
            if autopilot || v > 1 {
                out.push(("partitions", v.to_string()));
            }
        }
        if let Some(v) = self.lease_seconds {
            out.push(("leaseSeconds", v.to_string()));
        }
        // Only ever sent when true — the broker treats presence as opt-in, the
        // same rule `autoAck` follows above, and `conflation=false` would read as
        // a DISAGREEMENT with a group whose stored policy is true (the broker
        // answers `"conflationConflict":true` and keeps the stored setting).
        if self.conflation == Some(true) {
            out.push(("conflation", "true".to_string()));
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
            conflation: Some(false),
            ..Default::default()
        };
        assert!(p.to_pairs().is_empty());
    }

    /// PLAN_CONFLATION §3.1/§4: the flag reaches the wire only when opted in, and
    /// it is spelled `conflation` — the same key the broker's `PopParams` reads.
    #[test]
    fn params_send_conflation_only_when_true() {
        let p = PopParams {
            consumer_group: Some("workers".into()),
            conflation: Some(true),
            ..Default::default()
        };
        let pairs = p.to_pairs();
        assert!(pairs.contains(&("conflation", "true".to_string())));

        let off = PopParams {
            consumer_group: Some("workers".into()),
            conflation: None,
            ..Default::default()
        };
        assert!(!off.to_pairs().iter().any(|(k, _)| *k == "conflation"));
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

    // ------------------------------------------------------------ autopilot

    /// POP AUTOPILOT — the opt-in travels only when true, a delegated knob is
    /// simply omitted, and the `partitions > 1` gate is LIFTED while it is on:
    /// under autopilot an omitted `partitions` means "broker, you choose", so a
    /// caller who says 1 is pinning a width the controller would widen.
    #[test]
    fn params_send_autopilot_only_when_true_and_lift_the_partitions_gate() {
        let both_delegated = PopParams {
            consumer_group: Some("workers".into()),
            autopilot: Some(true),
            ..Default::default()
        };
        assert_eq!(
            both_delegated.to_pairs(),
            vec![
                ("autopilot", "true".to_string()),
                ("consumerGroup", "workers".to_string()),
            ],
            "a delegated knob does not travel at all"
        );

        let pinned_to_one = PopParams {
            consumer_group: Some("workers".into()),
            partitions: Some(1),
            autopilot: Some(true),
            ..Default::default()
        };
        assert!(
            pinned_to_one
                .to_pairs()
                .contains(&("partitions", "1".to_string())),
            "partitions=1 is a decision under autopilot, not the absence of one"
        );
    }

    /// The escape hatch has to be exact: with the flag off the rendering is what
    /// this crate produced before the parameter existed, key order included.
    #[test]
    fn params_without_autopilot_are_byte_identical_to_the_old_rendering() {
        let off = PopParams {
            batch: Some(1),
            wait: Some(false),
            timeout_millis: Some(30000),
            consumer_group: Some("workers".into()),
            partitions: Some(1),
            autopilot: None,
            ..Default::default()
        };
        assert_eq!(
            off.to_pairs(),
            vec![
                ("batch", "1".to_string()),
                ("wait", "false".to_string()),
                ("timeout", "30000".to_string()),
                ("consumerGroup", "workers".to_string()),
            ],
            "no autopilot key, and partitions=1 stays off the wire"
        );

        // `Some(false)` is never rendered either, the rule auto_ack and
        // conflation already follow.
        let explicitly_off = PopParams {
            consumer_group: Some("workers".into()),
            autopilot: Some(false),
            ..Default::default()
        };
        assert!(!explicitly_off
            .to_pairs()
            .iter()
            .any(|(k, _)| *k == "autopilot"));
    }

    /// The echo is additive and MUST NOT be load-bearing: a response that says
    /// nothing about autopilot, or says something this crate has never heard of,
    /// still decodes. Refusing to decode a pop over this field would stop a
    /// consumer for a value nothing depends on.
    #[test]
    fn the_autopilot_echo_is_read_leniently() {
        let head = r#"{"success":true,"queue":"q","partition":"p","partitionId":"p1","#;
        let tail = r#""leaseId":"l1","consumerGroup":"g","messages":[]"#;

        let absent: PopResponse = serde_json::from_str(&format!("{head}{tail}}}")).unwrap();
        assert!(absent.autopilot.is_none());

        let full: PopResponse = serde_json::from_str(&format!(
            r#"{head}{tail},"autopilot":{{"partitions":8,"batch":200,"waitMs":25}}}}"#
        ))
        .unwrap();
        let echo = full.autopilot.expect("an object decodes");
        assert_eq!(
            (echo.partitions, echo.batch, echo.wait_ms),
            (8, 200, Some(25))
        );

        // waitMs is optional: the broker sends it only when it has an opinion.
        let no_wait: PopResponse = serde_json::from_str(&format!(
            r#"{head}{tail},"autopilot":{{"partitions":4,"batch":64}}}}"#
        ))
        .unwrap();
        assert_eq!(no_wait.autopilot.unwrap().wait_ms, None);

        // A newer broker growing a field must not cost the fields we understand.
        let grown: PopResponse = serde_json::from_str(&format!(
            r#"{head}{tail},"autopilot":{{"partitions":2,"batch":10,"reason":"ready_age"}}}}"#
        ))
        .unwrap();
        let echo = grown.autopilot.expect("unknown keys are ignored");
        assert_eq!((echo.partitions, echo.batch), (2, 10));

        // A wrongly typed field is dropped, and a wrongly shaped value reads as
        // absent — neither is fatal.
        for body in [
            r#""autopilot":{"partitions":"eight","batch":10}"#,
            r#""autopilot":null"#,
            r#""autopilot":true"#,
            r#""autopilot":[]"#,
        ] {
            let got: PopResponse = serde_json::from_str(&format!("{head}{tail},{body}}}"))
                .unwrap_or_else(|e| panic!("{body} must still decode: {e}"));
            let partitions = got.autopilot.map(|e| e.partitions).unwrap_or(0);
            assert_eq!(partitions, 0, "{body}");
        }
    }
}
