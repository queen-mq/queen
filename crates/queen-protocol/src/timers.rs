//! `POST /api/v1/timers` and the three path routes — scheduled deliveries.
//!
//! A timer is a message that has not been pushed yet. At `deliverAt` the broker
//! deletes the row and pushes the frame in one transaction, so the payload is
//! carried here exactly as the queue will carry it: **base64**, because it is
//! bytes and not necessarily JSON.
//!
//! Four rules of this wire, each one a place where a client gets it wrong
//! quietly:
//!
//! * **Only relative durations, in milliseconds** (`delayMs`). An absolute
//!   instant is not expressible: one clock, Postgres's, so no skew between
//!   brokers or clients can enter. A `delayMs` in the past is **legal** and
//!   fires on the first cycle.
//! * **`deliverAt` is "no earlier than", never "exactly at".**
//! * **There is no tombstone.** A delivered timer has no row, so a cancel that
//!   arrives after delivery answers `absent` — which means *no longer pending*
//!   and **may mean already delivered**. The authority is the log: the response
//!   echoes the `txn` back so that check needs no second API call.
//! * **`ok: false` on `absent` and `too_late`.** Both are HTTP 200 verdicts, and
//!   both are false: the in-house lesson is queue delete, whose `deleted: false`
//!   with a 200 read as success to every client that trusted the status.
//!
//! `producerSub`, `messageId`, `tenant` and `deliverAt` are **server-owned**.
//! Sending one is a `400`, never a silent drop: a client that could post
//! `{"producerSub":"billing-service"}` would get, a second later, a frame whose
//! provenance is attested by the broker and forged by the caller.

use serde::{Deserialize, Serialize};

/// The three operations of the batch route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TimerOpKind {
    Schedule,
    /// The same upsert as `schedule`, so retrying after a crash is safe by
    /// construction. `attempts` goes back to zero: a rescheduled timer is a new
    /// timer under an old name, and a freshly corrected payload must not
    /// inherit the budget spent by the one that was poisoning things.
    Reschedule,
    Cancel,
}

impl TimerOpKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Schedule => "schedule",
            Self::Reschedule => "reschedule",
            Self::Cancel => "cancel",
        }
    }
}

/// The closed taxonomy of a timer verdict. A client that has to tell these
/// apart writes a `match`, not a string comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimerStatus {
    /// A new row.
    Scheduled,
    /// An existing `(queue, timerKey)` was overwritten.
    Rescheduled,
    /// The row was deleted before it could fire.
    Cancelled,
    /// No such pending timer. **May mean already delivered** — there is no
    /// tombstone. Also what a cancel for another tenant's timer answers.
    Absent,
    /// A broker holds the claim: it has already packed that payload and is
    /// about to commit it. Bounded by the sweeper lease. The remedy is a new
    /// key, or waiting for delivery and acting on the message.
    TooLate,
}

impl TimerStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Scheduled => "scheduled",
            Self::Rescheduled => "rescheduled",
            Self::Cancelled => "cancelled",
            Self::Absent => "absent",
            Self::TooLate => "too_late",
        }
    }
}

/// One operation of a timer call.
///
/// `cancel` reads only `queue`, `timerKey` and the echoed `txn`; the schedule
/// fields are absent on it, and the constructors below never set a field the
/// operation does not read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimerOperation {
    pub op: TimerOpKind,

    pub queue: String,

    /// The timer's identity inside the queue, together with the tenant. A
    /// second schedule under the same key overwrites the first.
    #[serde(rename = "timerKey")]
    pub timer_key: String,

    /// The lane the message lands on. Defaults to `Default` server-side; an
    /// explicit empty string is refused rather than treated as absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<String>,

    /// Milliseconds from now. Required for a schedule, refused on a cancel.
    /// **Not** seconds: a 250 ms retry backoff is a real and central use of
    /// timers, which is where the product's rule comes from — durations that
    /// can be sub-second are in milliseconds, the ones that cannot are in
    /// seconds.
    #[serde(rename = "delayMs", default, skip_serializing_if = "Option::is_none")]
    pub delay_ms: Option<i64>,

    /// The transaction id the delivered message will carry, and the value a
    /// cancel echoes back so "was it already delivered?" can be answered
    /// against the destination queue. Mandatory on a schedule, and **overwritten
    /// by every reschedule**.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub txn: Option<String>,

    /// The message body, base64. Use [`TimerOperation::schedule`] rather than
    /// setting this by hand.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,

    /// The payload bytes are zstd-compressed. The broker decompresses at fire,
    /// after decrypting.
    #[serde(
        rename = "payloadZstd",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub payload_zstd: Option<bool>,

    /// The payload is already an encryption envelope. Set it only when the
    /// broker is *not* encrypting for this queue: claiming it while the broker
    /// encrypts is a `400`, because the two possible readings — double
    /// encryption, or a lie to the consumer — are both wrong.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encrypted: Option<bool>,
}

impl TimerOperation {
    /// A schedule (or, with [`TimerOperation::reschedule`], the identical
    /// upsert under an existing key).
    ///
    /// `delay_ms` may be negative or zero: that timer fires on the first sweep
    /// cycle, which is a declared behaviour and not an accident.
    pub fn schedule(
        queue: impl Into<String>,
        timer_key: impl Into<String>,
        delay_ms: i64,
        txn: impl Into<String>,
        payload: &[u8],
    ) -> Self {
        Self {
            op: TimerOpKind::Schedule,
            queue: queue.into(),
            timer_key: timer_key.into(),
            partition: None,
            delay_ms: Some(delay_ms),
            txn: Some(txn.into()),
            payload: Some(base64_encode(payload)),
            payload_zstd: None,
            encrypted: None,
        }
    }

    /// Cancel a pending timer.
    ///
    /// A cancel is never refused by a quota — the fire never switches itself
    /// off, so a tenant that could not cancel would keep producing messages it
    /// cannot stop.
    pub fn cancel(queue: impl Into<String>, timer_key: impl Into<String>) -> Self {
        Self {
            op: TimerOpKind::Cancel,
            queue: queue.into(),
            timer_key: timer_key.into(),
            partition: None,
            delay_ms: None,
            txn: None,
            payload: None,
            payload_zstd: None,
            encrypted: None,
        }
    }

    /// Mark this operation a `reschedule`. The broker treats it identically to
    /// a schedule; the name is what a reader of the call site sees.
    pub fn reschedule(mut self) -> Self {
        self.op = TimerOpKind::Reschedule;
        self
    }

    pub fn partition(mut self, partition: impl Into<String>) -> Self {
        self.partition = Some(partition.into());
        self
    }

    /// The `txn` a cancel expects, echoed back on `absent` so the caller can
    /// look for it in the destination queue.
    pub fn txn(mut self, txn: impl Into<String>) -> Self {
        self.txn = Some(txn.into());
        self
    }

    pub fn payload_zstd(mut self) -> Self {
        self.payload_zstd = Some(true);
        self
    }

    pub fn encrypted(mut self) -> Self {
        self.encrypted = Some(true);
        self
    }
}

/// Body of `POST /api/v1/timers`.
///
/// A cancel sent here inherits this route's authorization: on a cluster over
/// quota a batch carrying even one schedule is refused **whole**. The route
/// that is guaranteed to take a cancel is `DELETE /api/v1/timers/:queue/*key`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimerRequest {
    pub operations: Vec<TimerOperation>,
}

impl TimerRequest {
    pub fn new(operations: Vec<TimerOperation>) -> Self {
        Self { operations }
    }
}

/// One element of a timer `results` array, index-aligned to its operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimerResult {
    /// `false` for `absent` and `too_late`, which are verdicts and not
    /// successes even though both answer HTTP 200.
    #[serde(default)]
    pub ok: bool,

    pub status: TimerStatus,

    #[serde(default)]
    pub queue: String,

    #[serde(rename = "timerKey", default)]
    pub timer_key: String,

    /// The transaction id of the message this timer will become — echoed on
    /// `absent` too, which is what makes the "already delivered?" check
    /// possible without a second API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub txn: Option<String>,

    /// Promised at schedule time, so a caller can correlate the delivered frame
    /// without waiting for it.
    #[serde(rename = "messageId", default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,

    /// When the broker will consider it due. "No earlier than", never
    /// "exactly at".
    #[serde(rename = "deliverAt", default, skip_serializing_if = "Option::is_none")]
    pub deliver_at: Option<String>,
}

impl TimerResult {
    /// Whether this outcome means the timer is now pending as asked.
    pub fn is_pending(&self) -> bool {
        matches!(
            self.status,
            TimerStatus::Scheduled | TimerStatus::Rescheduled
        )
    }
}

/// Body of a timer batch response.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TimerResponse {
    #[serde(default)]
    pub results: Vec<TimerResult>,
}

/// `GET /api/v1/timers/:queue/*timerKey` — one timer, with its payload.
///
/// The payload comes back exactly as it is stored, with `encrypted` telling the
/// truth about it: peek is an inspection surface and does not quietly decrypt
/// what the fire will deliver as an envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimerPeek {
    #[serde(default)]
    pub found: bool,

    #[serde(default)]
    pub queue: String,

    #[serde(rename = "timerKey", default)]
    pub timer_key: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<String>,

    #[serde(rename = "deliverAt", default, skip_serializing_if = "Option::is_none")]
    pub deliver_at: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub txn: Option<String>,

    #[serde(rename = "messageId", default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,

    /// Base64. Read it with [`TimerPeek::payload_bytes`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,

    #[serde(rename = "payloadZstd", default)]
    pub payload_zstd: bool,

    #[serde(default)]
    pub encrypted: bool,

    #[serde(
        rename = "producerSub",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub producer_sub: Option<String>,

    /// Failed fire attempts that consumed budget. Transient database faults do
    /// not count here — only permanent and configuration failures do.
    #[serde(default)]
    pub attempts: i64,

    #[serde(rename = "lastError", default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,

    /// A broker holds this timer right now. A row in **backoff** reads `false`
    /// on purpose: it is still cancellable.
    #[serde(default)]
    pub claimed: bool,

    #[serde(rename = "createdAt", default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,

    #[serde(rename = "updatedAt", default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

impl TimerPeek {
    /// The stored payload, decoded. `Ok(None)` when there is no payload —
    /// which is what a miss looks like.
    pub fn payload_bytes(&self) -> Result<Option<Vec<u8>>, String> {
        match &self.payload {
            None => Ok(None),
            Some(s) => base64_decode(s).map(Some),
        }
    }
}

/// One row of `GET /api/v1/timers/:queue`. No payload: that is what peek is
/// for, and a page of payloads would make a list a bandwidth decision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimerListRow {
    #[serde(default)]
    pub queue: String,

    #[serde(rename = "timerKey", default)]
    pub timer_key: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<String>,

    #[serde(rename = "deliverAt", default, skip_serializing_if = "Option::is_none")]
    pub deliver_at: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub txn: Option<String>,

    #[serde(rename = "messageId", default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,

    #[serde(rename = "payloadZstd", default)]
    pub payload_zstd: bool,

    #[serde(default)]
    pub encrypted: bool,

    #[serde(
        rename = "producerSub",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub producer_sub: Option<String>,

    #[serde(default)]
    pub attempts: i64,

    #[serde(rename = "lastError", default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,

    #[serde(default)]
    pub claimed: bool,

    #[serde(rename = "createdAt", default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,

    #[serde(rename = "updatedAt", default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

/// `GET /api/v1/timers/:queue` — a keyset page. The queue is **mandatory**:
/// there is no tenant-wide list, because that would be a scan an end user of
/// the customer could trigger.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TimerPage {
    #[serde(default)]
    pub rows: Vec<TimerListRow>,

    #[serde(default)]
    pub truncated: bool,

    /// Pass it back as `after`. `None` means this was the last page.
    #[serde(rename = "nextAfter", default, skip_serializing_if = "Option::is_none")]
    pub next_after: Option<String>,
}

// ---------------------------------------------------------------------------
// base64, standard alphabet with padding — what `encode(bytes,'base64')` and
// `decode(text,'base64')` speak on the other side.
//
// Hand-written rather than pulled in: this crate's dependency list is serde and
// serde_json, and the broker's own choice of base64 crate is not part of the
// wire. Forty lines with a test that walks every byte is cheaper than a
// dependency in a published crate.
// ---------------------------------------------------------------------------

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Encode bytes as standard base64 with padding.
pub fn base64_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// Decode standard base64. Whitespace is ignored; anything else outside the
/// alphabet is an error, because a payload silently decoded to the wrong bytes
/// is worse than one that does not decode.
pub fn base64_decode(s: &str) -> Result<Vec<u8>, String> {
    let mut acc: u32 = 0;
    let mut bits = 0u8;
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    for c in s.bytes() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => break,
            b'\n' | b'\r' | b' ' | b'\t' => continue,
            _ => return Err(format!("not base64: byte {c:#04x}")),
        };
        acc = (acc << 6) | u32::from(v);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    // ------------------------------------------------------------------
    // A replica of the stored procedure's validation pass, which reads each op
    // as JSONB key by key. The point is the same as in `kv`: a rename on both
    // sides of an assertion agrees with itself and disagrees with the database.
    // ------------------------------------------------------------------

    #[derive(Debug, PartialEq)]
    enum Verdict {
        Ok,
        Raise(&'static str),
    }

    /// KEEP IN SYNC with the FOREACH list in 025_log_timers.sql.
    const SERVER_OWNED: [&str; 15] = [
        "producerSub",
        "producer_sub",
        "messageId",
        "message_id",
        "tenant",
        "tenantId",
        "tenant_id",
        "deliverAt",
        "deliver_at",
        "delaySeconds",
        "delay_seconds",
        "attempts",
        "claimToken",
        "claim_token",
        "claimedUntil",
    ];

    fn validate_like_the_procedure(op: &Value) -> Verdict {
        let obj = match op.as_object() {
            Some(o) => o,
            None => return Verdict::Raise("not an object"),
        };
        let kind = match obj.get("op").and_then(|v| v.as_str()) {
            Some(k @ ("schedule" | "reschedule" | "cancel")) => k,
            _ => return Verdict::Raise("unknown operation"),
        };
        for k in obj.keys() {
            if k.starts_with('_') || SERVER_OWNED.contains(&k.as_str()) {
                return Verdict::Raise("server-owned field");
            }
        }
        if obj.get("queue").and_then(|v| v.as_str()).unwrap_or("").is_empty() {
            return Verdict::Raise("queue is required");
        }
        if obj
            .get("timerKey")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .is_empty()
        {
            return Verdict::Raise("timerKey is required");
        }
        if obj.contains_key("partition")
            && obj
                .get("partition")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .is_empty()
        {
            return Verdict::Raise("partition must be non-empty");
        }
        if kind != "cancel" {
            if !obj.get("delayMs").map(|v| v.is_number()).unwrap_or(false) {
                return Verdict::Raise("delayMs is required");
            }
            if obj.get("txn").and_then(|v| v.as_str()).unwrap_or("").is_empty() {
                return Verdict::Raise("txn is required");
            }
            if !obj.get("payload").map(|v| v.is_string()).unwrap_or(false) {
                return Verdict::Raise("payload is required");
            }
        }
        Verdict::Ok
    }

    fn body_of(op: &TimerOperation) -> Value {
        serde_json::to_value(op).unwrap()
    }

    #[test]
    fn a_schedule_carries_everything_the_procedure_demands() {
        let op = TimerOperation::schedule("orders", "retry:9137", 250, "t-1", b"{\"n\":1}")
            .partition("customer-42");
        assert_eq!(validate_like_the_procedure(&body_of(&op)), Verdict::Ok);

        let body = serde_json::to_string(&op).unwrap();
        assert!(body.contains(r#""delayMs":250"#), "{body}");
        assert!(body.contains(r#""timerKey":"retry:9137""#), "{body}");
        assert!(
            !body.contains("delaySeconds"),
            "the wire carries milliseconds, and delaySeconds is a server-owned name: {body}"
        );
    }

    #[test]
    fn a_zero_or_past_delay_is_legal_and_fires_on_the_first_cycle() {
        for delay in [0, -60_000] {
            let op = TimerOperation::schedule("q", "k", delay, "t", b"x");
            assert_eq!(
                validate_like_the_procedure(&body_of(&op)),
                Verdict::Ok,
                "a past delayMs must not be rejected client-side"
            );
        }
    }

    #[test]
    fn a_cancel_sends_no_schedule_field() {
        // delayMs and payload on a cancel are meaningless, and `partition:""`
        // would be refused outright. The envelope is shared, so this has to be
        // checked rather than assumed.
        let op = TimerOperation::cancel("orders", "retry:9137");
        let body = serde_json::to_string(&op).unwrap();
        assert_eq!(
            body,
            r#"{"op":"cancel","queue":"orders","timerKey":"retry:9137"}"#
        );
        assert_eq!(validate_like_the_procedure(&body_of(&op)), Verdict::Ok);
    }

    #[test]
    fn a_cancel_may_echo_the_txn_it_expects() {
        let op = TimerOperation::cancel("orders", "k").txn("saga-9137-compensate");
        let body = serde_json::to_string(&op).unwrap();
        assert!(body.contains(r#""txn":"saga-9137-compensate""#), "{body}");
        assert_eq!(validate_like_the_procedure(&body_of(&op)), Verdict::Ok);
    }

    #[test]
    fn no_constructor_can_produce_a_server_owned_field() {
        // The one hole this closes is provenance: `producer_sub` is the only
        // non-repudiable field of a frame, and a client that could set it would
        // get a broker-attested lie one second later.
        for op in [
            TimerOperation::schedule("q", "k", 1, "t", b"x"),
            TimerOperation::cancel("q", "k"),
            TimerOperation::schedule("q", "k", 1, "t", b"x").reschedule(),
        ] {
            let v = body_of(&op);
            for name in SERVER_OWNED {
                assert!(
                    v.get(name).is_none(),
                    "{name} is server-owned and must never be sent"
                );
            }
            assert!(v.as_object().unwrap().keys().all(|k| !k.starts_with('_')));
        }
    }

    #[test]
    fn the_payload_travels_as_base64() {
        let op = TimerOperation::schedule("q", "k", 1, "t", b"{\"order\":9137}");
        assert_eq!(op.payload.as_deref(), Some("eyJvcmRlciI6OTEzN30="));
    }

    #[test]
    fn reschedule_is_the_same_shape_under_a_different_name() {
        let sched = TimerOperation::schedule("q", "k", 60_000, "t", b"x");
        let resched = sched.clone().reschedule();
        assert_eq!(resched.op, TimerOpKind::Reschedule);
        assert_eq!(
            TimerOperation {
                op: TimerOpKind::Schedule,
                ..resched
            },
            sched,
            "reschedule must differ from schedule in the op name and nothing else"
        );
    }

    // ------------------------------------------------------------------
    // Responses, transcribed from what the procedures build.
    // ------------------------------------------------------------------

    #[test]
    fn a_scheduled_timer_promises_its_message_id_and_delivery() {
        let r: TimerResult = serde_json::from_str(
            r#"{"ok":true,"status":"scheduled","queue":"orders","timerKey":"retry:1",
                "txn":"t-1","messageId":"0198f0aa-0000-7000-8000-000000000001",
                "deliverAt":"2026-08-17T10:00:00.000000Z"}"#,
        )
        .unwrap();
        assert!(r.ok && r.is_pending());
        assert_eq!(r.status, TimerStatus::Scheduled);
        assert_eq!(r.deliver_at.as_deref(), Some("2026-08-17T10:00:00.000000Z"));
    }

    #[test]
    fn absent_and_too_late_both_carry_ok_false() {
        // The whole reason `ok` exists: HTTP 200 says the call worked, and a
        // client that read the status alone would take a cancel that changed
        // nothing for a cancel that worked.
        let absent: TimerResult = serde_json::from_str(
            r#"{"ok":false,"status":"absent","queue":"q","timerKey":"k","txn":"t-1"}"#,
        )
        .unwrap();
        assert!(!absent.ok);
        assert_eq!(absent.status, TimerStatus::Absent);
        assert_eq!(
            absent.txn.as_deref(),
            Some("t-1"),
            "absent echoes the txn back: it may mean ALREADY DELIVERED, and the log is the \
             authority"
        );
        assert!(!absent.is_pending());

        let late: TimerResult =
            serde_json::from_str(r#"{"ok":false,"status":"too_late","queue":"q","timerKey":"k"}"#)
                .unwrap();
        assert_eq!(late.status, TimerStatus::TooLate);
        assert!(!late.ok);
    }

    #[test]
    fn every_status_of_the_closed_taxonomy_parses() {
        for (wire, want) in [
            ("scheduled", TimerStatus::Scheduled),
            ("rescheduled", TimerStatus::Rescheduled),
            ("cancelled", TimerStatus::Cancelled),
            ("absent", TimerStatus::Absent),
            ("too_late", TimerStatus::TooLate),
        ] {
            let r: TimerResult = serde_json::from_str(&format!(
                r#"{{"ok":true,"status":"{wire}","queue":"q","timerKey":"k"}}"#
            ))
            .unwrap_or_else(|e| panic!("status {wire} did not parse: {e}"));
            assert_eq!(r.status, want);
            assert_eq!(want.as_str(), wire);
        }
    }

    #[test]
    fn a_peek_decodes_its_payload_and_reports_a_row_in_backoff_as_unclaimed() {
        let p: TimerPeek = serde_json::from_str(
            r#"{"found":true,"queue":"orders","timerKey":"retry:1","partition":"Default",
                "deliverAt":"2026-08-17T10:00:00.000000Z","txn":"t-1",
                "messageId":"0198f0aa-0000-7000-8000-000000000001",
                "payload":"eyJvcmRlciI6OTEzN30=","payloadZstd":false,"encrypted":false,
                "producerSub":null,"attempts":2,"lastError":"boom","claimed":false,
                "createdAt":"2026-08-16T10:00:00.000000Z",
                "updatedAt":"2026-08-16T10:00:05.000000Z"}"#,
        )
        .unwrap();
        assert!(p.found);
        assert_eq!(p.payload_bytes().unwrap().unwrap(), b"{\"order\":9137}");
        assert_eq!(p.attempts, 2);
        assert!(
            !p.claimed,
            "a row in backoff has claim_token NULL and stays cancellable"
        );
        assert_eq!(p.producer_sub, None);
    }

    #[test]
    fn a_peek_miss_is_a_found_false_and_not_an_error() {
        let p: TimerPeek =
            serde_json::from_str(r#"{"found":false,"queue":"q","timerKey":"k"}"#).unwrap();
        assert!(!p.found);
        assert_eq!(p.payload_bytes().unwrap(), None);
    }

    #[test]
    fn a_list_page_hands_back_its_cursor() {
        let page: TimerPage = serde_json::from_str(
            r#"{"rows":[{"queue":"q","timerKey":"a","partition":"Default",
                 "deliverAt":"2026-08-17T10:00:00.000000Z","txn":"t","messageId":"m",
                 "payloadZstd":false,"encrypted":false,"producerSub":null,"attempts":0,
                 "lastError":null,"claimed":false,"createdAt":"c","updatedAt":"u"}],
               "truncated":true,"nextAfter":"a"}"#,
        )
        .unwrap();
        assert_eq!(page.rows.len(), 1);
        assert!(page.truncated);
        assert_eq!(page.next_after.as_deref(), Some("a"));

        let last: TimerPage =
            serde_json::from_str(r#"{"rows":[],"truncated":false,"nextAfter":null}"#).unwrap();
        assert_eq!(last.next_after, None);
    }

    #[test]
    fn base64_round_trips_every_byte_and_every_length() {
        let all: Vec<u8> = (0..=255u8).collect();
        for len in 0..=all.len() {
            let slice = &all[..len];
            let encoded = base64_encode(slice);
            assert_eq!(encoded.len() % 4, 0, "base64 is padded to a multiple of 4");
            assert_eq!(base64_decode(&encoded).unwrap(), slice, "len {len}");
        }
        // Known vectors, against the encoder Postgres uses on the other side.
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(&[0xff, 0xfe, 0xfd]), "//79");
        assert_eq!(base64_decode("//79").unwrap(), vec![0xff, 0xfe, 0xfd]);
    }

    #[test]
    fn base64_refuses_what_it_cannot_decode() {
        assert!(base64_decode("not base64!").is_err());
        // Postgres wraps long base64 in newlines; those are not corruption.
        assert_eq!(base64_decode("Zm9v\nYg==").unwrap(), b"foob");
    }
}
