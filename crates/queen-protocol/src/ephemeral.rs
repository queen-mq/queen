//! `/api/v1/ephemeral/*` — the RAM-class queue family (EPHEMERAL_QUEUES.md
//! §1, §3.1).
//!
//! A per-queue storage class whose contents live in broker RAM and survive
//! nothing. The types here are the wire and nothing more, but three properties
//! of that wire only make sense with the contract in view, so they are written
//! down once here rather than repeated on every struct:
//!
//! * **The identity is on the ENVELOPE, not on the item.** The durable push
//!   repeats `{queue, partition}` on every item because a bundle spanning
//!   queues shares one PostgreSQL transaction; there is no transaction here to
//!   share, so one request addresses one queue and
//!   [`EphemeralMessage`] carries a payload and nothing else. No
//!   `transactionId`: there is no dedup index to hold one.
//!
//! * **`stale` is an answer, not an error.** [`EphemeralOutcome::Stale`] means
//!   the id was minted by an incarnation of the ring that is gone — a restart or
//!   an ownership move — which is the loss contract (§1.2) rather than a bug.
//!   It arrives inside a 200 with the other outcomes, and a client that
//!   reconnects after a broker restart and flushes its outstanding acks gets a
//!   row of them. A 4xx per id would be a retry storm.
//!
//! * **An empty pop is an empty ARRAY, not a 204.** The durable pop answers 204
//!   because its empty body carried no information; this one carries the queue
//!   name, so there is one shape for every outcome (§3.1).
//!
//! # What is deliberately not typed here
//!
//! The two status reads — `GET /api/v1/ephemeral/queues` and
//! `.../queues/:queue/depth` — are gauge *documents*, and §5.3 is explicit that
//! their column set is the class's own truth and expected to grow (a dashboard
//! reads owner, tier, drops, per-group skip counters). A struct here would
//! freeze a snapshot of that and make every column a newer broker adds vanish
//! silently on its way to the caller, which is exactly the failure a status read
//! must not have. The Rust client hands those two through as
//! `serde_json::Value`, the same choice its admin surface makes for every other
//! gauge endpoint.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Partition an ephemeral push or pop means when it names none. The broker
/// applies the same default, spelled the same way as the durable engine's, so
/// the two classes are not gratuitously different in the one place a user reads
/// both.
///
/// A client should nonetheless OMIT the field rather than send this: which ring
/// a push without a partition lands on is the broker's rule to make.
pub const EPHEMERAL_DEFAULT_PARTITION: &str = "Default";

/// The queue half's namespace prefix inside the broker's engine keys (§3.2).
///
/// It is here because clients need it too, for a different reason: an ephemeral
/// `orders` and a durable `orders` are unrelated objects (§10 Q8), so an SDK
/// that keys client-side state — a push buffer, say — by queue name must
/// namespace the two apart or one family's messages drain through the other
/// family's route.
pub const EPHEMERAL_KEY_PREFIX: &str = "eph:";

/// The `code` on the ONE 404 this family answers for a reason of its own.
///
/// `GET /api/v1/ephemeral/queues/:queue/depth` is the only verb that can 404
/// because the queue is not there: every other one either creates the queue by
/// naming it (push, pop) or describes a miss inside a 200 (`reset` answers
/// `dropped:0`, `delete` answers `deleted:false`). That matters to an SDK,
/// because every OTHER 404 on this family means the routes do not exist at all —
/// an old broker, or an old proxy answering `route_blocked` — and the two must
/// not be reported as the same thing. Branch on this code, never on the status
/// alone.
pub const EPHEMERAL_QUEUE_NOT_FOUND_CODE: &str = "ephemeral_queue_not_found";

// ===========================================================================
// push
// ===========================================================================

/// One message on the ephemeral push wire.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EphemeralMessage {
    /// Arbitrary JSON, stored raw and returned verbatim: the broker re-emits
    /// the bytes it was given rather than round-tripping them through a value,
    /// so key order and number formatting survive a push/pop.
    pub payload: Value,
}

impl EphemeralMessage {
    pub fn new(payload: Value) -> Self {
        Self { payload }
    }
}

/// Body of `POST /api/v1/ephemeral/push`. All-or-nothing per request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EphemeralPushRequest {
    pub queue: String,

    /// Omit to let the broker choose the ring. FIFO is per `(queue, partition)`
    /// within one ownership incarnation (§1.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<String>,

    pub messages: Vec<EphemeralMessage>,
}

impl EphemeralPushRequest {
    pub fn new(queue: impl Into<String>, messages: Vec<EphemeralMessage>) -> Self {
        Self {
            queue: queue.into(),
            partition: None,
            messages,
        }
    }

    pub fn partition(mut self, partition: impl Into<String>) -> Self {
        self.partition = Some(partition.into());
        self
    }
}

/// `201` answer of a push.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EphemeralPushResponse {
    pub pushed: u64,
}

// ===========================================================================
// pop
// ===========================================================================

/// One delivered message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EphemeralDelivered {
    /// `e:<owner_epoch_hex>:<partition>:<seq>`, and OPAQUE to clients (§3.1).
    /// It encodes the owning broker incarnation, which is what lets an ack
    /// arriving after a restart or an ownership move answer
    /// [`EphemeralOutcome::Stale`] instead of acking somebody else's message.
    /// Do not parse it.
    pub id: String,

    pub partition: String,

    pub payload: Value,

    /// Deliveries of this message so far, starting at 1. It grows on
    /// redelivery after a lease expiry or a `failed`/`retry` ack, until the
    /// queue's `retryLimit`, after which the message is dropped and counted —
    /// there is no DLQ on this class (§9).
    pub attempts: u32,
}

/// Body of a pop answer. `messages` is empty rather than absent when the poll
/// timed out.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EphemeralPopResponse {
    pub queue: String,

    #[serde(default)]
    pub messages: Vec<EphemeralDelivered>,
}

/// Query of `GET /api/v1/ephemeral/pop`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EphemeralPopParams {
    /// Required. Every other field is optional.
    pub queue: String,

    pub partition: Option<String>,

    pub batch: Option<i32>,

    /// A real long poll, parked on a RAM gate with no database behind it and no
    /// polling interval anywhere (§3.4).
    pub wait: Option<bool>,

    /// Long-poll timeout in milliseconds. Sent as `timeout`, not
    /// `timeoutMillis` — the wire's spelling, kept faithfully.
    pub timeout_millis: Option<u64>,

    /// The WHOLE of the consumption semantics (§1.5), exactly as on the durable
    /// engine: the same group competes, its own group fans out, and no group is
    /// the groupless queue mode. There is no queue-level mode to choose.
    pub group: Option<String>,

    /// Commit at delivery: at-most-once, and no lease bookkeeping at all.
    pub auto_ack: Option<bool>,
}

impl EphemeralPopParams {
    pub fn new(queue: impl Into<String>) -> Self {
        Self {
            queue: queue.into(),
            ..Default::default()
        }
    }

    /// Render as `key=value` pairs, ready to be percent-encoded by the caller's
    /// HTTP layer. Order is stable so tests can assert on it, and matches
    /// [`crate::PopParams::to_pairs`]'s discipline: a flag whose absence is the
    /// default is emitted ONLY when true, so a plain pop is the shortest query
    /// this route can receive.
    pub fn to_pairs(&self) -> Vec<(&'static str, String)> {
        let mut out: Vec<(&'static str, String)> = Vec::new();
        out.push(("queue", self.queue.clone()));
        if let Some(v) = &self.partition {
            out.push(("partition", v.clone()));
        }
        if let Some(v) = self.batch {
            out.push(("batch", v.to_string()));
        }
        if self.wait == Some(true) {
            out.push(("wait", "true".to_string()));
            // Always beside `wait`, never on its own: the HTTP deadline a client
            // sets is computed from THIS number, and leaving the window to the
            // broker's default instead is a client that aborts a request the
            // broker was about to answer.
            out.push((
                "timeout",
                self.timeout_millis
                    .unwrap_or(EPHEMERAL_DEFAULT_WAIT_TIMEOUT_MILLIS)
                    .to_string(),
            ));
        }
        if let Some(v) = &self.group {
            out.push(("group", v.clone()));
        }
        if self.auto_ack == Some(true) {
            out.push(("autoAck", "true".to_string()));
        }
        out
    }
}

/// The long-poll default, matching the durable pop's, when `wait` is asked for
/// without a timeout.
pub const EPHEMERAL_DEFAULT_WAIT_TIMEOUT_MILLIS: u64 = 30_000;

// ===========================================================================
// ack
// ===========================================================================

/// What an ack asks for.
///
/// The wire accepts more spellings than this enum has variants, and the broker's
/// table is the durable one restated: `completed`, `success`, `acked` and `ok`
/// all normalize to [`EphemeralStatus::Completed`]; an **absent** status also
/// means completed; and *any* unrecognized string normalizes to
/// [`EphemeralStatus::Failed`] rather than being rejected, so a typo cannot
/// silently retire a message. [`EphemeralStatus::parse`] reproduces it exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EphemeralStatus {
    /// Advance the group's cursor past this message.
    Completed,
    /// Nack. Redelivers with `attempts + 1` until the queue's `retryLimit`.
    Failed,
    /// Nack that explicitly asks for redelivery. Identical to `Failed` on this
    /// class; the two names exist because they mean different things to the
    /// reader.
    Retry,
}

impl EphemeralStatus {
    /// Normalize a wire status exactly as the broker's `AckStatus::parse` does.
    pub fn parse(s: Option<&str>) -> Self {
        match s {
            None => Self::Completed,
            Some("completed") | Some("success") | Some("acked") | Some("ok") => Self::Completed,
            Some("retry") => Self::Retry,
            Some(_) => Self::Failed,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Retry => "retry",
        }
    }

    pub fn from_bool(ok: bool) -> Self {
        if ok {
            Self::Completed
        } else {
            Self::Failed
        }
    }
}

/// One entry of the `acks` array.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EphemeralAck {
    pub id: String,

    /// Absent means [`EphemeralStatus::Completed`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<EphemeralStatus>,

    /// Accepted by the broker and IGNORED on this class: there is no DLQ row
    /// and no trace store to record it on (§9). It exists on the wire so one
    /// ack builder can serve both engines, which is the shape §4 asks for —
    /// refusing the field instead would break every SDK that shares one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl EphemeralAck {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            status: None,
            error: None,
        }
    }

    pub fn status(mut self, status: EphemeralStatus) -> Self {
        self.status = Some(status);
        self
    }

    pub fn error(mut self, error: impl Into<String>) -> Self {
        self.error = Some(error.into());
        self
    }
}

/// Body of `POST /api/v1/ephemeral/ack`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EphemeralAckRequest {
    pub queue: String,

    /// The group the pop used — cursors are per group. Absent is the groupless
    /// queue mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,

    pub acks: Vec<EphemeralAck>,
}

impl EphemeralAckRequest {
    pub fn new(queue: impl Into<String>, acks: Vec<EphemeralAck>) -> Self {
        Self {
            queue: queue.into(),
            group: None,
            acks,
        }
    }

    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }
}

/// The per-id answer of §3.1. A CLOSED vocabulary of four, and every terminal
/// outcome is `Acked` — including a `failed` whose attempts were already spent,
/// because inventing a fifth outcome for it would break every client that
/// matches this enum. That the message was dropped is visible in the
/// `eph_dropped_retry` counter instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EphemeralOutcome {
    /// Applied; the message is retired.
    Acked,
    /// Applied; the message comes back.
    Redelivered,
    /// The id was minted by another incarnation of the ring. NOT an error — see
    /// the module docs.
    Stale,
    /// The lease is no longer this consumer's to release: already acked, or
    /// already expired and redelivered.
    Unknown,
}

impl EphemeralOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Acked => "acked",
            Self::Redelivered => "redelivered",
            Self::Stale => "stale",
            Self::Unknown => "unknown",
        }
    }
}

/// One `{id, outcome}` of an ack answer, in request order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EphemeralAckResult {
    pub id: String,
    pub outcome: EphemeralOutcome,
}

/// Body of an ack answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EphemeralAckResponse {
    #[serde(default)]
    pub results: Vec<EphemeralAckResult>,
}

// ===========================================================================
// configure / reset
// ===========================================================================

/// What breaching a queue's `maxBytes`/`maxLength` does (§1.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EphemeralPolicy {
    /// 429 `queue_full` — backpressure, and the shape every 1.0.6 SDK's bounded
    /// buffer already knows how to drain against.
    Reject,
    /// Feed semantics: the ring keeps accepting and the head falls off,
    /// counted as `eph_dropped_bounds`.
    DropOldest,
}

impl EphemeralPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reject => "reject",
            Self::DropOldest => "dropOldest",
        }
    }
}

/// Let a WAITING pop fatten its batch: it returns when `count` messages are
/// ready or `ms` have passed since the first, bounded by the pop's own timeout
/// (§1.7). Delivery-side batching only — it changes nothing about what is
/// stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct EphemeralWindowBuffer {
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub ms: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub count: u64,
}

fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}

/// The seven knobs of `configure` (§3.1). A CLOSED list, and every one of them
/// bounds something — bytes, length, age, redelivery — so an option the broker
/// does not recognize is refused rather than dropped on the floor: a silently
/// ignored `ttlSeconds` is a ring that grows until a global budget answers 503.
///
/// Every field is optional and an absent one leaves the broker's default in
/// charge, which is why they are `Option` rather than zero-valued: `retryLimit:
/// 0` is a queue that drops on the first nack, and it must be possible to say
/// that on purpose without saying it by accident.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct EphemeralOptions {
    #[serde(rename = "maxBytes", default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<i64>,

    #[serde(rename = "maxLength", default, skip_serializing_if = "Option::is_none")]
    pub max_length: Option<i64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<EphemeralPolicy>,

    /// Drop messages older than this, head first.
    ///
    /// It is NOT the durable queue's `retention`, and the two words are kept
    /// apart on purpose: retention cleans consumed history and never touches
    /// pending, while this drops UNCONSUMED messages. One word per contract.
    #[serde(
        rename = "ttlSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub ttl_seconds: Option<i64>,

    #[serde(
        rename = "leaseSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub lease_seconds: Option<i64>,

    #[serde(
        rename = "retryLimit",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub retry_limit: Option<i64>,

    #[serde(
        rename = "windowBuffer",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub window_buffer: Option<EphemeralWindowBuffer>,
}

/// Body of `POST /api/v1/ephemeral/configure`.
///
/// Declaring a queue persists the OPTIONS in PostgreSQL (§1.1): the
/// configuration survives a restart, the contents never do, and the queue comes
/// back declared and EMPTY.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EphemeralConfigureRequest<'a> {
    pub queue: &'a str,
    pub options: EphemeralOptions,
}

/// Body of `POST /api/v1/ephemeral/reset`, which drops every message, voids
/// every lease and rewinds every group cursor. Legal only because of the loss
/// contract: it destroys nothing the class ever promised to keep.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EphemeralResetRequest<'a> {
    pub queue: &'a str,
}

/// Answer of a reset.
///
/// A queue that is not there answers `dropped: 0` rather than 404: an implicit
/// inbox that has been idle-collected is indistinguishable from one that was
/// never used, and both are correctly described as "there was nothing to drop".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EphemeralResetResponse {
    pub dropped: u64,
}

/// Answer of `DELETE /api/v1/ephemeral/queue/:queue`.
///
/// `deleted` IS THE ANSWER, and the reason this is a struct instead of a bare
/// `()` is an in-house scar: the durable queue delete answers `deleted:false`
/// with a 200, and every client that ignored the field read a miss as a
/// success. The status code describes the call; the field describes the queue.
///
/// `declared` separates the two tiers of §1.1 — it says whether a PostgreSQL
/// declaration row was removed as well as the RAM rings, which is the only part
/// of an ephemeral queue that ever survived anything.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct EphemeralDeleteResponse {
    #[serde(default)]
    pub queue: String,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub declared: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_push_omits_the_partition_it_was_not_given() {
        let req = EphemeralPushRequest::new(
            "presence",
            vec![EphemeralMessage::new(serde_json::json!({"user": "a"}))],
        );
        assert_eq!(
            serde_json::to_string(&req).unwrap(),
            r#"{"queue":"presence","messages":[{"payload":{"user":"a"}}]}"#
        );
    }

    #[test]
    fn a_push_item_is_a_payload_and_nothing_else() {
        // No transactionId: there is no dedup index on this class to hold one,
        // and a field the broker cannot honour is worse than an absent one.
        let m = EphemeralMessage::new(serde_json::json!(null));
        assert_eq!(serde_json::to_string(&m).unwrap(), r#"{"payload":null}"#);
    }

    #[test]
    fn a_push_with_a_partition_hoists_it_to_the_envelope() {
        let req = EphemeralPushRequest::new("presence", vec![EphemeralMessage::new(1.into())])
            .partition("room-7");
        assert_eq!(
            serde_json::to_string(&req).unwrap(),
            r#"{"queue":"presence","partition":"room-7","messages":[{"payload":1}]}"#
        );
    }

    #[test]
    fn a_plain_pop_sends_only_its_queue() {
        let params = EphemeralPopParams::new("inbox");
        assert_eq!(params.to_pairs(), vec![("queue", "inbox".to_string())]);
    }

    #[test]
    fn a_waiting_pop_always_carries_a_timeout() {
        let mut params = EphemeralPopParams::new("inbox");
        params.wait = Some(true);
        assert_eq!(
            params.to_pairs(),
            vec![
                ("queue", "inbox".to_string()),
                ("wait", "true".to_string()),
                ("timeout", "30000".to_string()),
            ]
        );

        params.timeout_millis = Some(1500);
        assert!(params.to_pairs().contains(&("timeout", "1500".to_string())));
    }

    #[test]
    fn pop_flags_are_emitted_only_when_true() {
        let mut params = EphemeralPopParams::new("inbox");
        params.wait = Some(false);
        params.auto_ack = Some(false);
        assert_eq!(
            params.to_pairs(),
            vec![("queue", "inbox".to_string())],
            "a declined flag must be indistinguishable from an unset one"
        );
    }

    #[test]
    fn pop_params_keep_a_stable_order() {
        let params = EphemeralPopParams {
            queue: "inbox".into(),
            partition: Some("room-7".into()),
            batch: Some(10),
            wait: Some(true),
            timeout_millis: Some(1500),
            group: Some("workers".into()),
            auto_ack: Some(true),
        };
        let keys: Vec<&str> = params.to_pairs().into_iter().map(|(k, _)| k).collect();
        assert_eq!(
            keys,
            vec![
                "queue",
                "partition",
                "batch",
                "wait",
                "timeout",
                "group",
                "autoAck"
            ]
        );
    }

    #[test]
    fn an_empty_pop_answer_parses_however_it_is_spelled() {
        for wire in [r#"{"queue":"inbox","messages":[]}"#, r#"{"queue":"inbox"}"#] {
            let got: EphemeralPopResponse = serde_json::from_str(wire).unwrap();
            assert_eq!(got.queue, "inbox");
            assert!(got.messages.is_empty(), "{wire}");
        }
    }

    #[test]
    fn a_delivered_message_parses_the_rendered_wire() {
        // Field order as `render_pop` writes it: id, partition, attempts,
        // payload. Order is not a contract, but parsing what the broker
        // actually emits is.
        let wire =
            r#"{"id":"e:9f1:room-7:12","partition":"room-7","attempts":2,"payload":{"n":1}}"#;
        let got: EphemeralDelivered = serde_json::from_str(wire).unwrap();
        assert_eq!(got.id, "e:9f1:room-7:12");
        assert_eq!(got.attempts, 2);
        assert_eq!(got.payload["n"], 1);
    }

    #[test]
    fn a_pop_answer_from_a_newer_broker_still_parses() {
        // An unmodelled key must never cost a caller the messages it is already
        // holding a lease on.
        let wire = r#"{"queue":"inbox","messages":[{"id":"e:1:Default:1","partition":"Default",
            "attempts":1,"payload":1,"owner":"queen-b"}],"owner":"queen-b"}"#;
        let got: EphemeralPopResponse =
            serde_json::from_str(wire).expect("an unmodelled key must not fail the decode");
        assert_eq!(got.messages.len(), 1);
    }

    #[test]
    fn ack_status_aliases_match_the_broker_table() {
        for alias in ["completed", "success", "acked", "ok"] {
            assert_eq!(
                EphemeralStatus::parse(Some(alias)),
                EphemeralStatus::Completed
            );
        }
        assert_eq!(EphemeralStatus::parse(None), EphemeralStatus::Completed);
        assert_eq!(
            EphemeralStatus::parse(Some("retry")),
            EphemeralStatus::Retry
        );
        // Anything unrecognized is a nack, not a rejection: a typo must not
        // silently retire a message.
        assert_eq!(
            EphemeralStatus::parse(Some("nope")),
            EphemeralStatus::Failed
        );
        assert_eq!(
            EphemeralStatus::parse(Some("COMPLETED")),
            EphemeralStatus::Failed
        );
        assert_eq!(EphemeralStatus::from_bool(false).as_str(), "failed");
    }

    #[test]
    fn an_ack_carries_the_id_and_only_what_it_was_told() {
        let req = EphemeralAckRequest::new("inbox", vec![EphemeralAck::new("e:1:Default:1")]);
        assert_eq!(
            serde_json::to_string(&req).unwrap(),
            r#"{"queue":"inbox","acks":[{"id":"e:1:Default:1"}]}"#
        );

        let req = EphemeralAckRequest::new(
            "inbox",
            vec![EphemeralAck::new("e:1:Default:1")
                .status(EphemeralStatus::Retry)
                .error("boom")],
        )
        .group("workers");
        assert_eq!(
            serde_json::to_string(&req).unwrap(),
            r#"{"queue":"inbox","group":"workers","acks":[{"id":"e:1:Default:1","status":"retry","error":"boom"}]}"#
        );
    }

    #[test]
    fn every_outcome_parses_and_round_trips() {
        for (s, want) in [
            ("acked", EphemeralOutcome::Acked),
            ("redelivered", EphemeralOutcome::Redelivered),
            ("stale", EphemeralOutcome::Stale),
            ("unknown", EphemeralOutcome::Unknown),
        ] {
            let got: EphemeralOutcome = serde_json::from_str(&format!("\"{s}\"")).unwrap();
            assert_eq!(got, want);
            assert_eq!(got.as_str(), s);
        }
    }

    #[test]
    fn an_ack_answer_round_trips_the_rendered_wire() {
        let wire = r#"{"results":[{"id":"e:1:Default:1","outcome":"stale"}]}"#;
        let got: EphemeralAckResponse = serde_json::from_str(wire).unwrap();
        assert_eq!(got.results[0].outcome, EphemeralOutcome::Stale);
        assert_eq!(serde_json::to_string(&got).unwrap(), wire);
    }

    #[test]
    fn configure_omits_every_knob_it_was_not_given() {
        let req = EphemeralConfigureRequest {
            queue: "inbox",
            options: EphemeralOptions::default(),
        };
        assert_eq!(
            serde_json::to_string(&req).unwrap(),
            r#"{"queue":"inbox","options":{}}"#
        );
    }

    #[test]
    fn configure_names_its_knobs_the_way_the_broker_reads_them() {
        let req = EphemeralConfigureRequest {
            queue: "inbox",
            options: EphemeralOptions {
                max_bytes: Some(1_048_576),
                max_length: Some(500),
                policy: Some(EphemeralPolicy::DropOldest),
                ttl_seconds: Some(30),
                lease_seconds: Some(10),
                retry_limit: Some(3),
                window_buffer: Some(EphemeralWindowBuffer { ms: 25, count: 50 }),
            },
        };
        assert_eq!(
            serde_json::to_string(&req).unwrap(),
            r#"{"queue":"inbox","options":{"maxBytes":1048576,"maxLength":500,"policy":"dropOldest","ttlSeconds":30,"leaseSeconds":10,"retryLimit":3,"windowBuffer":{"ms":25,"count":50}}}"#
        );
    }

    #[test]
    fn a_zero_knob_is_declarable_and_an_absent_one_is_not_zero() {
        let opts = EphemeralOptions {
            retry_limit: Some(0),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_string(&opts).unwrap(),
            r#"{"retryLimit":0}"#,
            "a queue that drops on the first nack must be sayable on purpose"
        );
    }

    #[test]
    fn reset_carries_the_queue_and_reads_the_count() {
        assert_eq!(
            serde_json::to_string(&EphemeralResetRequest { queue: "inbox" }).unwrap(),
            r#"{"queue":"inbox"}"#
        );
        // The broker echoes the queue beside the count; an unmodelled key must
        // not cost the caller the number it asked for.
        let got: EphemeralResetResponse =
            serde_json::from_str(r#"{"queue":"inbox","dropped":4211}"#).unwrap();
        assert_eq!(got.dropped, 4211);
    }

    #[test]
    fn a_delete_that_hit_nothing_is_readable_as_a_miss() {
        let got: EphemeralDeleteResponse =
            serde_json::from_str(r#"{"queue":"inbox","deleted":false,"declared":false}"#).unwrap();
        assert!(
            !got.deleted,
            "deleted:false with a 200 is a MISS, not a win"
        );

        let got: EphemeralDeleteResponse =
            serde_json::from_str(r#"{"queue":"inbox","deleted":true,"declared":true}"#).unwrap();
        assert!(got.deleted && got.declared);
    }

    #[test]
    fn the_key_prefix_is_the_one_the_broker_namespaces_with() {
        // A client keying local state by queue name has to apply it, or an
        // ephemeral `orders` and a durable `orders` collide.
        assert_eq!(format!("{EPHEMERAL_KEY_PREFIX}orders"), "eph:orders");
    }
}
