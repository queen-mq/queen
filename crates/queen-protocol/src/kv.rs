//! `POST /api/v1/kv` and the three path routes — the key/value surface.
//!
//! Seven names, five code paths: `get`, `getMany`, `getPrefix`, `put`,
//! `putIfAbsent` (an alias for `put` with `expect: 0`), `delete`, `incr`.
//!
//! Three rules of this wire are worth stating here, because a client that gets
//! any of them wrong fails silently rather than loudly:
//!
//! * **Every write declares its expiry, exactly once.** One of `ttlSeconds`
//!   (an integer greater than zero) and `forever: true`, never both and never
//!   neither. A `put` does not inherit the previous key's TTL — that is not
//!   expressible, and it is the fastest way to make a marker immortal.
//! * **`applied: false` is a verdict, not a failure.** The broker answers
//!   HTTP 200 for a lost `putIfAbsent`, a missing key and a delete that hit
//!   nothing. It is the single most frequent outcome of this surface, and a
//!   4xx would put it inside every retry policy and error dashboard.
//! * **`found` is separate from `value`.** `'null'::jsonb` is a legal value, so
//!   `{found: true, value: null}` and `{found: false}` are different things and
//!   must not be collapsed.
//!
//! `getPrefix` exists **only** in the POST batch body. It is refused inside the
//! transaction wire, and it is never a query string: `?prefix=quota:acme:` is
//! recorded by the broker's access log, the proxy's, the meter sample and any
//! ingress in front, and a mitigation living in one component out of four is
//! not a mitigation.

use std::time::SystemTime;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

/// Deserialize an optional value where **`null` is a value**.
///
/// `Option<Value>` on its own maps a present `"value": null` to `None`, which
/// is precisely the collapse §5.5 forbids: `{found: true, value: null}` and
/// `{found: false}` are different answers, and a client that cannot tell them
/// apart re-runs an external effect that already happened. With this, `None`
/// means the key was **absent from the JSON** and nothing else.
pub(crate) fn present_value<'de, D>(d: D) -> Result<Option<Value>, D::Error>
where
    D: Deserializer<'de>,
{
    Value::deserialize(d).map(Some)
}

/// How long a written key lives. Exactly one of these reaches the wire, and
/// there is no third option — an absent expiry is a `400 kv_expiry_not_specified`
/// from the stored procedure, which is where the rule lives so that all seven
/// clients inherit it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expiry {
    /// `ttlSeconds`. **Zero is not valid** and neither is a negative: the
    /// broker refuses both, and so does [`Expiry::to_wire`] before the request
    /// leaves the process.
    Seconds(i64),

    /// A deadline, converted to a relative `ttlSeconds` at the moment of
    /// sending. There is no `expiresAt` field on this wire — one clock,
    /// Postgres's, and no client skew enters anywhere.
    Until(SystemTime),

    /// `forever: true`. Legal, and banned from the examples that run in CI: a
    /// test that goes wrong leaves immortal state in a shared database.
    Forever,
}

impl Expiry {
    /// Seconds from now. A convenience so callers do not repeat the variant.
    pub const fn seconds(n: i64) -> Self {
        Self::Seconds(n)
    }

    pub const fn forever() -> Self {
        Self::Forever
    }

    pub const fn until(deadline: SystemTime) -> Self {
        Self::Until(deadline)
    }

    /// `(ttlSeconds, forever)` as the wire carries them, or the reason this
    /// expiry cannot be expressed.
    ///
    /// A deadline is turned into a delta **here**, at send time, which is the
    /// only place where "now" is close enough to the request for the answer to
    /// be right. A deadline that has already passed is an error and not a
    /// one-second TTL: the caller asked for something that cannot happen, and
    /// rounding it into the nearest legal value would write a key nobody
    /// intended.
    pub fn to_wire(self) -> Result<(Option<i64>, Option<bool>), String> {
        match self {
            Self::Forever => Ok((None, Some(true))),
            Self::Seconds(n) if n > 0 => Ok((Some(n), None)),
            Self::Seconds(n) => Err(format!(
                "ttlSeconds must be an integer greater than zero, got {n}; use \
                 Expiry::Forever for a key with no expiry"
            )),
            Self::Until(deadline) => match deadline.duration_since(SystemTime::now()) {
                // Round UP: a 1500 ms deadline is 2 seconds, never 1, because
                // truncating would expire the key before the instant asked for.
                Ok(d) => {
                    let secs = d.as_secs() + u64::from(d.subsec_nanos() > 0);
                    match i64::try_from(secs.max(1)) {
                        Ok(n) => Ok((Some(n), None)),
                        Err(_) => Err("that deadline is further away than i64 seconds".into()),
                    }
                }
                Err(_) => Err(
                    "that deadline has already passed; a KV expiry is a delay from now, so a \
                     past instant is not expressible"
                        .into(),
                ),
            },
        }
    }
}

/// The seven operation names, exactly as they appear in the `op` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvOpKind {
    #[serde(rename = "get")]
    Get,
    #[serde(rename = "getMany")]
    GetMany,
    #[serde(rename = "getPrefix")]
    GetPrefix,
    #[serde(rename = "put")]
    Put,
    /// An alias: the stored procedure desugars it to `put` with `expect: 0` at
    /// entry, so it is one code path. It exists under its own name because that
    /// is the name of the thing, and because "did I win?" is the question most
    /// often asked of this API.
    #[serde(rename = "putIfAbsent")]
    PutIfAbsent,
    #[serde(rename = "delete")]
    Delete,
    #[serde(rename = "incr")]
    Incr,
}

impl KvOpKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::GetMany => "getMany",
            Self::GetPrefix => "getPrefix",
            Self::Put => "put",
            Self::PutIfAbsent => "putIfAbsent",
            Self::Delete => "delete",
            Self::Incr => "incr",
        }
    }

    /// Whether this operation can create or change a row. Reads are never
    /// blocked by an occupancy quota; writes are.
    pub fn is_write(self) -> bool {
        matches!(
            self,
            Self::Put | Self::PutIfAbsent | Self::Delete | Self::Incr
        )
    }
}

/// One operation of a KV call.
///
/// Every field is optional on the wire except `op` and `ns`, because the seven
/// operations share one envelope. The constructors below are the supported way
/// to build one: they set the fields that operation actually reads, and nothing
/// else — an unknown or contradictory field is a `400` from the stored
/// procedure, never a silent drop.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KvOperation {
    pub op: KvOpKind,

    /// Namespace. `^[a-z0-9][a-z0-9._-]{0,63}$`, enforced server-side.
    pub ns: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,

    /// `getMany` only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keys: Option<Vec<String>>,

    /// `getPrefix` only, and mandatory there: a namespace is not a table to
    /// enumerate, so an empty prefix is `400 kv_prefix_required`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,

    /// `getPrefix` only. An **exclusive** keyset cursor, not an offset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,

    /// `getPrefix` only. Clamped server-side, never rejected — `truncated`
    /// tells the truth about what a clamp cost.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,

    #[serde(rename = "keysOnly", default, skip_serializing_if = "Option::is_none")]
    pub keys_only: Option<bool>,

    /// `put` and `putIfAbsent`. `Some(Value::Null)` is a legal value and is
    /// sent as `"value": null`; `None` means the field is absent, which those
    /// two operations reject.
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub value: Option<Value>,

    /// `incr` only. Validated against `min`/`max` before anything is written,
    /// so a first `incr` of a window cannot overshoot the ceiling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta: Option<i64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<i64>,

    /// With `max`, `applied` **is** the admission decision: an increment that
    /// would break the ceiling does not apply and returns the current value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<i64>,

    /// The optimistic lock. `0` means "must not exist" and wins even against an
    /// expired row the sweeper has not pruned yet; `N > 0` is a pure UPDATE and
    /// creates nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expect: Option<i64>,

    /// Escalate a lost precondition from a verdict into a rollback. Only
    /// meaningful inside a transaction, where it is the gate that stops the
    /// bundle; on the standalone routes it turns `applied: false` into an
    /// HTTP 200 body carrying `reason: "kv_precondition"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required: Option<bool>,

    #[serde(
        rename = "ttlSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub ttl_seconds: Option<i64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forever: Option<bool>,
}

impl KvOperation {
    fn bare(op: KvOpKind, ns: impl Into<String>) -> Self {
        Self {
            op,
            ns: ns.into(),
            key: None,
            keys: None,
            prefix: None,
            after: None,
            limit: None,
            keys_only: None,
            value: None,
            delta: None,
            min: None,
            max: None,
            expect: None,
            required: None,
            ttl_seconds: None,
            forever: None,
        }
    }

    pub fn get(ns: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            key: Some(key.into()),
            ..Self::bare(KvOpKind::Get, ns)
        }
    }

    pub fn get_many(ns: impl Into<String>, keys: Vec<String>) -> Self {
        Self {
            keys: Some(keys),
            ..Self::bare(KvOpKind::GetMany, ns)
        }
    }

    pub fn get_prefix(ns: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self {
            prefix: Some(prefix.into()),
            ..Self::bare(KvOpKind::GetPrefix, ns)
        }
    }

    /// A `put` with its expiry already resolved. The `Err` arm is a zero TTL or
    /// a deadline in the past — caught here rather than at the broker, so the
    /// mistake surfaces at the call.
    pub fn put(
        ns: impl Into<String>,
        key: impl Into<String>,
        value: Value,
        expiry: Expiry,
    ) -> Result<Self, String> {
        let (ttl, forever) = expiry.to_wire()?;
        Ok(Self {
            key: Some(key.into()),
            value: Some(value),
            ttl_seconds: ttl,
            forever,
            ..Self::bare(KvOpKind::Put, ns)
        })
    }

    pub fn put_if_absent(
        ns: impl Into<String>,
        key: impl Into<String>,
        value: Value,
        expiry: Expiry,
    ) -> Result<Self, String> {
        Ok(Self {
            op: KvOpKind::PutIfAbsent,
            ..Self::put(ns, key, value, expiry)?
        })
    }

    pub fn delete(ns: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            key: Some(key.into()),
            ..Self::bare(KvOpKind::Delete, ns)
        }
    }

    pub fn incr(
        ns: impl Into<String>,
        key: impl Into<String>,
        delta: i64,
        expiry: Expiry,
    ) -> Result<Self, String> {
        let (ttl, forever) = expiry.to_wire()?;
        Ok(Self {
            key: Some(key.into()),
            delta: Some(delta),
            ttl_seconds: ttl,
            forever,
            ..Self::bare(KvOpKind::Incr, ns)
        })
    }

    /// Fence this write on a version. `0` means "must not exist".
    pub fn expect(mut self, version: i64) -> Self {
        self.expect = Some(version);
        self
    }

    /// Make a lost precondition roll the call back instead of reporting it.
    pub fn required(mut self) -> Self {
        self.required = Some(true);
        self
    }

    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn after(mut self, after: impl Into<String>) -> Self {
        self.after = Some(after.into());
        self
    }

    pub fn keys_only(mut self) -> Self {
        self.keys_only = Some(true);
        self
    }

    pub fn min(mut self, min: i64) -> Self {
        self.min = Some(min);
        self
    }

    pub fn max(mut self, max: i64) -> Self {
        self.max = Some(max);
        self
    }
}

/// Body of `POST /api/v1/kv`.
///
/// The broker also accepts a bare array, but this type always writes the
/// `{"operations": [...]}` envelope — the same key the transaction wire uses,
/// so one shape is learned once.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KvRequest {
    pub operations: Vec<KvOperation>,
}

impl KvRequest {
    pub fn new(operations: Vec<KvOperation>) -> Self {
        Self { operations }
    }
}

/// Body of `PUT /api/v1/kv/:ns/*key`.
///
/// The namespace and the key are in the **path** on this route, and the broker
/// rejects a body that also carries `op`, `ns` or `key` rather than ignoring
/// it — so this type deliberately has no field for them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KvPutBody {
    pub value: Value,

    #[serde(
        rename = "ttlSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub ttl_seconds: Option<i64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forever: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expect: Option<i64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required: Option<bool>,
}

/// Body of `DELETE /api/v1/kv/:ns/*key`. Empty is the common case and is legal.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct KvDeleteBody {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expect: Option<i64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required: Option<bool>,
}

/// Why a write did not apply. A **closed** taxonomy: a client that has to tell
/// these apart writes a `match`, not a string comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KvReason {
    /// `expect: 0` lost — the key was already there and live.
    Exists,
    /// `expect: N > 0` or a fenced delete found nothing. An expired row counts
    /// as nothing, even before the sweeper prunes it.
    Absent,
    /// `expect: N` met a different version.
    Version,
    /// An `incr` would have crossed `min`/`max`, or the delta itself is outside
    /// them. Nothing was written.
    Limit,
    /// `incr` met a live key whose value is not a number.
    Type,
}

/// One row of a multi-key read.
///
/// Reads answer with **rows, never a key→value map**: the shape makes the
/// confusion between "absent" and "present but null" inexpressible.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KvRow {
    pub key: String,

    /// Absent when the read asked for `keysOnly` — and `None` means exactly
    /// that, never "the stored value happens to be null".
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub value: Option<Value>,

    pub version: i64,

    #[serde(rename = "expiresAt", default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,

    #[serde(rename = "updatedAt", default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

/// One element of a KV `results` array, index-aligned to the operation that
/// produced it.
///
/// The seven operations return four different shapes and the broker builds them
/// as free-form objects, so every field here is optional and the accessors
/// below are the safe way to read them.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct KvResult {
    /// The operation's ordinal inside its own array.
    #[serde(default)]
    pub index: usize,

    /// The operation the broker actually ran. Authoritative over what was
    /// asked: `putIfAbsent` comes back as `put`, because the desugaring happens
    /// server-side and there is one code path behind both names.
    #[serde(default)]
    pub op: String,

    /// Writes only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied: Option<bool>,

    /// `get` only. Separate from `value` because `null` is a legal value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub found: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<KvReason>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,

    /// Present on writes **even when they did not apply**, carrying the current
    /// value — the loser of a race must not need a second round trip, which is
    /// the entire point of an idempotency marker.
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub value: Option<Value>,

    /// The current version.
    ///
    /// **An opaque token, not an ordering.** It comes from one sequence shared
    /// by every key, declared with a per-session cache, so consecutive writes to
    /// the same key through a connection pool return values that do not
    /// increase — 6, 3005, 1007, 2003 is a measured sequence. Compare versions
    /// with `==`, never with `<`. The sequence is global rather than per-key on
    /// purpose: a key that expired, was pruned and was recreated cannot reissue
    /// a version an old holder still carries.
    ///
    /// On a lost race it is also **advisory**: it is read outside the row lock,
    /// so it must not be reused blindly as a fencing token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<i64>,

    #[serde(rename = "expiresAt", default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,

    #[serde(rename = "updatedAt", default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,

    /// `getMany` and `getPrefix`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<KvRow>>,

    /// `getMany` only. Absence is a **datum**, not a hole the client computes
    /// by difference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub missing: Option<Vec<String>>,

    /// The page stopped early — at the key limit, or at the byte ceiling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub truncated: Option<bool>,

    /// The cursor to pass as `after` for the next page.
    #[serde(rename = "nextAfter", default, skip_serializing_if = "Option::is_none")]
    pub next_after: Option<String>,
}

impl KvResult {
    /// Did this write happen? `false` for a read, which never applies anything.
    pub fn applied(&self) -> bool {
        self.applied.unwrap_or(false)
    }

    pub fn found(&self) -> bool {
        self.found.unwrap_or(false)
    }

    pub fn truncated(&self) -> bool {
        self.truncated.unwrap_or(false)
    }

    /// The rows of a multi-key read, empty when this was not one.
    pub fn rows(&self) -> &[KvRow] {
        self.rows.as_deref().unwrap_or(&[])
    }

    /// The keys a `getMany` did not find. Empty is a real answer — every key
    /// was there — and so is empty on an operation that reports no absence.
    pub fn missing(&self) -> &[String] {
        self.missing.as_deref().unwrap_or(&[])
    }

    /// The counter after an `incr`, as an integer.
    ///
    /// `Err` when the value does not fit an `i64`: the column is `numeric`
    /// server-side, so a counter can outgrow the type this SDK exposes, and
    /// handing back a rounded number would be worse than saying so.
    pub fn counter(&self) -> Result<i64, String> {
        match self.value.as_ref() {
            Some(Value::Number(n)) => n.as_i64().ok_or_else(|| {
                format!(
                    "counter {n} does not fit an i64; the broker stores it as numeric and this \
                     SDK exposes int64"
                )
            }),
            Some(other) => Err(format!("counter is not a number: {other}")),
            None => Err("no value on this result".into()),
        }
    }
}

/// Body of a KV batch response.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct KvResponse {
    #[serde(default)]
    pub results: Vec<KvResult>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // ------------------------------------------------------------------
    // A replica of the stored procedure's reader.
    //
    // `kv_apply_v1` walks each op as JSONB, key by key, with `v_op->>'key'` and
    // `v_op ? 'expect'`: a key it cannot find is not an error, it is an absent
    // field, and an absent field is either a default or a RAISE. Asserting that
    // `KvOperation` serializes to its own field names would prove nothing
    // against that — a rename on both sides agrees with itself and disagrees
    // with the database. The reader below asks the SAME questions the procedure
    // asks, so a rename on either side shows up as a value that went missing.
    // ------------------------------------------------------------------

    #[derive(Debug, PartialEq)]
    enum Verdict {
        Ok,
        /// The procedure would RAISE, with this message.
        Raise(&'static str),
    }

    /// The validation pass of `kv_apply_v1`, in the order it runs.
    fn validate_like_the_procedure(op: &Value, in_wire: bool) -> Verdict {
        let obj = match op.as_object() {
            Some(o) => o,
            None => return Verdict::Raise("kv_bad_request"),
        };
        let kind = match obj.get("op").and_then(|v| v.as_str()) {
            Some(
                k @ ("get" | "getMany" | "getPrefix" | "put" | "putIfAbsent" | "delete" | "incr"),
            ) => k,
            _ => return Verdict::Raise("kv_unknown_op"),
        };
        if obj.contains_key("tenant") || obj.contains_key("tenantId") || obj.contains_key("_tenant")
        {
            return Verdict::Raise("kv_tenant_not_an_input");
        }
        if kind == "getPrefix" {
            if in_wire {
                return Verdict::Raise("kv_get_prefix_not_allowed_in_transaction");
            }
            match obj.get("prefix").and_then(|v| v.as_str()) {
                Some(p) if !p.is_empty() => {}
                _ => return Verdict::Raise("kv_prefix_required"),
            }
        }
        if kind == "getMany" && !obj.get("keys").map(|k| k.is_array()).unwrap_or(false) {
            return Verdict::Raise("kv_bad_request");
        }
        if matches!(kind, "put" | "putIfAbsent" | "incr") {
            // EXACTLY ONE of ttlSeconds and forever:true. Zero and two are the
            // same error, because both mean the caller did not decide.
            let mut decls = 0;
            if let Some(t) = obj.get("ttlSeconds") {
                decls += 1;
                match t.as_i64() {
                    Some(n) if n > 0 => {}
                    _ => return Verdict::Raise("kv_bad_ttl"),
                }
            }
            if obj.get("forever") == Some(&Value::Bool(true)) {
                decls += 1;
            }
            if decls != 1 {
                return Verdict::Raise("kv_expiry_not_specified");
            }
        }
        if matches!(kind, "put" | "putIfAbsent") && !obj.contains_key("value") {
            return Verdict::Raise("kv_bad_request");
        }
        if matches!(kind, "put" | "putIfAbsent" | "delete") {
            if let Some(e) = obj.get("expect") {
                match e.as_i64() {
                    Some(n) if n >= 0 => {
                        if kind == "putIfAbsent" && n != 0 {
                            return Verdict::Raise("kv_bad_expect");
                        }
                    }
                    _ => return Verdict::Raise("kv_bad_expect"),
                }
            }
        }
        if kind == "incr" {
            if obj.contains_key("expect") {
                return Verdict::Raise("kv_bad_request");
            }
            if !obj.get("delta").map(|d| d.is_number()).unwrap_or(false) {
                return Verdict::Raise("kv_bad_request");
            }
        }
        Verdict::Ok
    }

    fn body_of(op: &KvOperation) -> Value {
        serde_json::to_value(op).unwrap()
    }

    #[test]
    fn every_constructor_produces_an_operation_the_procedure_accepts() {
        let ops = vec![
            KvOperation::get("orders", "saga:9137"),
            KvOperation::get_many("orders", vec!["a".into(), "b".into()]),
            KvOperation::get_prefix("orders", "saga:").limit(50),
            KvOperation::put(
                "orders",
                "saga:9137",
                serde_json::json!({"step": 2}),
                Expiry::seconds(3600),
            )
            .unwrap(),
            KvOperation::put_if_absent(
                "orders",
                "idem:9137",
                serde_json::json!(true),
                Expiry::seconds(86_400),
            )
            .unwrap(),
            KvOperation::delete("orders", "saga:9137"),
            KvOperation::incr("quota", "acme:minute", 1, Expiry::seconds(60))
                .unwrap()
                .max(100),
        ];
        for op in &ops {
            assert_eq!(
                validate_like_the_procedure(&body_of(op), false),
                Verdict::Ok,
                "the procedure would refuse {}",
                serde_json::to_string(op).unwrap()
            );
        }
    }

    #[test]
    fn a_put_carries_exactly_one_expiry_declaration() {
        let ttl = KvOperation::put("ns", "k", Value::Null, Expiry::seconds(60)).unwrap();
        let body = serde_json::to_string(&ttl).unwrap();
        assert!(body.contains(r#""ttlSeconds":60"#), "{body}");
        assert!(
            !body.contains("forever"),
            "sending both declarations is kv_expiry_not_specified: {body}"
        );

        let forever = KvOperation::put("ns", "k", Value::Null, Expiry::Forever).unwrap();
        let body = serde_json::to_string(&forever).unwrap();
        assert!(body.contains(r#""forever":true"#), "{body}");
        assert!(!body.contains("ttlSeconds"), "{body}");
    }

    #[test]
    fn a_zero_ttl_is_refused_before_the_request_leaves() {
        // The procedure would answer 400 kv_bad_ttl; catching it here names the
        // rule at the call site instead.
        let e = KvOperation::put("ns", "k", Value::Null, Expiry::seconds(0)).unwrap_err();
        assert!(e.contains("greater than zero"), "{e}");
        assert!(KvOperation::incr("ns", "k", 1, Expiry::seconds(-1)).is_err());
    }

    #[test]
    fn a_deadline_becomes_a_relative_ttl_and_a_past_one_is_refused() {
        let (ttl, forever) = Expiry::until(SystemTime::now() + std::time::Duration::from_secs(90))
            .to_wire()
            .unwrap();
        assert_eq!(forever, None);
        // Rounded up, so the key is never dropped before the instant asked for.
        assert!(matches!(ttl, Some(n) if (90..=91).contains(&n)), "{ttl:?}");

        let past = Expiry::until(SystemTime::now() - std::time::Duration::from_secs(5)).to_wire();
        assert!(past.is_err(), "a deadline in the past is not a 1s TTL");
    }

    #[test]
    fn a_null_value_reaches_the_wire_as_null_rather_than_vanishing() {
        // `"value": null` is a legal value and an ABSENT value is a 400. If the
        // serializer skipped a null the put would be refused by the procedure
        // for a field the caller did supply.
        let op = KvOperation::put("ns", "k", Value::Null, Expiry::Forever).unwrap();
        let body = serde_json::to_string(&op).unwrap();
        assert!(body.contains(r#""value":null"#), "{body}");
        assert_eq!(
            validate_like_the_procedure(&body_of(&op), false),
            Verdict::Ok
        );
    }

    #[test]
    fn put_if_absent_may_not_carry_a_different_expect() {
        // The alias desugars to expect:0. Sending expect:5 with it is a
        // contradiction the procedure raises on, and the SDK must not build one
        // by accident — so the constructor never sets `expect` itself.
        let op =
            KvOperation::put_if_absent("ns", "k", Value::Bool(true), Expiry::seconds(60)).unwrap();
        assert_eq!(op.expect, None);
        let contradiction = op.clone().expect(5);
        assert_eq!(
            validate_like_the_procedure(&body_of(&contradiction), false),
            Verdict::Raise("kv_bad_expect")
        );
        // ...while expect:0 spelled out is the same thing and is accepted.
        assert_eq!(
            validate_like_the_procedure(&body_of(&op.expect(0)), false),
            Verdict::Ok
        );
    }

    #[test]
    fn incr_never_sends_an_expect() {
        // incr is the way OUT of CAS; a precondition would reintroduce the loop
        // it exists to remove, and the procedure refuses one.
        let op = KvOperation::incr("quota", "acme", 1, Expiry::seconds(60));
        let op = op.unwrap();
        assert_eq!(op.expect, None);
        assert_eq!(
            validate_like_the_procedure(&body_of(&op), false),
            Verdict::Ok
        );
    }

    #[test]
    fn get_prefix_is_refused_inside_the_transaction_wire() {
        // The boundary is COST: get and getMany are allowed there because the
        // caller bounds them, getPrefix is not.
        let op = body_of(&KvOperation::get_prefix("ns", "saga:"));
        assert_eq!(validate_like_the_procedure(&op, false), Verdict::Ok);
        assert_eq!(
            validate_like_the_procedure(&op, true),
            Verdict::Raise("kv_get_prefix_not_allowed_in_transaction")
        );
    }

    #[test]
    fn an_operation_omits_every_field_its_kind_does_not_read() {
        // The envelope is shared by seven operations, so a constructor that
        // leaked a neighbour's field would send `keys` on a get. The procedure
        // ignores extras, which is exactly why this has to be checked here.
        let body = serde_json::to_string(&KvOperation::get("ns", "k")).unwrap();
        assert_eq!(body, r#"{"op":"get","ns":"ns","key":"k"}"#);
    }

    #[test]
    fn the_batch_envelope_is_the_key_the_broker_reads() {
        let req = KvRequest::new(vec![KvOperation::get("ns", "k")]);
        let body = serde_json::to_string(&req).unwrap();
        assert!(body.starts_with(r#"{"operations":["#), "{body}");
    }

    // ------------------------------------------------------------------
    // Responses, transcribed from what the procedure builds.
    // ------------------------------------------------------------------

    #[test]
    fn a_get_that_missed_is_not_a_get_that_found_null() {
        let miss: KvResult =
            serde_json::from_str(r#"{"index":0,"op":"get","found":false,"key":"k"}"#).unwrap();
        assert!(!miss.found());
        assert_eq!(miss.value, None);

        let null: KvResult = serde_json::from_str(
            r#"{"index":0,"op":"get","found":true,"key":"k","value":null,"version":3,
                "expiresAt":"2026-08-17T10:00:00Z","updatedAt":"2026-08-16T10:00:00Z"}"#,
        )
        .unwrap();
        assert!(null.found());
        assert_eq!(
            null.value,
            Some(Value::Null),
            "found:true with value null must not collapse into a miss"
        );
        assert_eq!(null.version, Some(3));
    }

    #[test]
    fn a_lost_race_carries_the_winners_value_and_version() {
        // This is the whole point of the idempotency marker: the loser learns
        // what won without a second round trip.
        let r: KvResult = serde_json::from_str(
            r#"{"index":0,"op":"put","applied":false,"reason":"exists","key":"idem:9137",
                "value":{"by":"worker-2"},"version":90101}"#,
        )
        .unwrap();
        assert!(!r.applied());
        assert_eq!(r.reason, Some(KvReason::Exists));
        assert_eq!(r.value.unwrap()["by"], "worker-2");
        assert_eq!(r.version, Some(90101));
    }

    #[test]
    fn the_reason_taxonomy_is_closed_and_parses_every_member() {
        for (wire, want) in [
            ("exists", KvReason::Exists),
            ("absent", KvReason::Absent),
            ("version", KvReason::Version),
            ("limit", KvReason::Limit),
            ("type", KvReason::Type),
        ] {
            let r: KvResult = serde_json::from_str(&format!(
                r#"{{"index":0,"op":"put","applied":false,"reason":"{wire}"}}"#
            ))
            .unwrap();
            assert_eq!(r.reason, Some(want));
        }
    }

    #[test]
    fn a_get_many_reports_absence_as_a_datum() {
        let r: KvResult = serde_json::from_str(
            r#"{"index":0,"op":"getMany","rows":[{"key":"a","value":1,"version":2,
                "expiresAt":null,"updatedAt":"2026-08-16T10:00:00Z"}],
                "missing":["b"],"truncated":false}"#,
        )
        .unwrap();
        let rows = r.rows.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key, "a");
        assert_eq!(
            rows[0].expires_at, None,
            "a null expiresAt is a key with no expiry, not the string \"null\""
        );
        assert_eq!(r.missing.unwrap(), vec!["b".to_string()]);
    }

    #[test]
    fn a_truncated_prefix_page_hands_back_its_cursor() {
        let r: KvResult = serde_json::from_str(
            r#"{"index":0,"op":"getPrefix","rows":[{"key":"saga:1","version":1}],
                "truncated":true,"nextAfter":"saga:1"}"#,
        )
        .unwrap();
        assert!(r.truncated());
        assert_eq!(r.next_after.as_deref(), Some("saga:1"));
        assert_eq!(
            r.rows.unwrap()[0].value,
            None,
            "keysOnly rows carry no value, and that must not read as value null"
        );
    }

    #[test]
    fn a_counter_that_outgrows_i64_says_so_instead_of_rounding() {
        let ok: KvResult =
            serde_json::from_str(r#"{"index":0,"op":"incr","applied":true,"value":7}"#).unwrap();
        assert_eq!(ok.counter().unwrap(), 7);

        // `numeric` server-side: it can hold what this SDK's int64 cannot.
        let huge: KvResult = serde_json::from_str(
            r#"{"index":0,"op":"incr","applied":true,"value":99999999999999999999}"#,
        )
        .unwrap();
        assert!(
            huge.counter().is_err(),
            "a rounded counter is worse than an error"
        );
    }

    #[test]
    fn a_result_from_a_newer_broker_still_parses() {
        let r: KvResult = serde_json::from_str(
            r#"{"index":0,"op":"put","applied":true,"key":"k","version":1,"somethingNew":42}"#,
        )
        .expect("an unmodelled key must not fail the decode");
        assert!(r.applied());
    }

    #[test]
    fn the_batch_response_is_index_aligned() {
        let resp: KvResponse = serde_json::from_str(
            r#"{"results":[{"index":0,"op":"get","found":false,"key":"a"},
                          {"index":1,"op":"put","applied":true,"key":"b","version":1}]}"#,
        )
        .unwrap();
        for (i, r) in resp.results.iter().enumerate() {
            assert_eq!(r.index, i);
        }
    }
}
