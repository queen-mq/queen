//! Key/value state, alongside the queue and in the same database.
//!
//! The point of this surface is not to be a cache. It is to let a consumer
//! write state **in the same PostgreSQL transaction as its ack**, which is what
//! [`crate::transaction::TransactionBuilder::kv`] does — everything here is the
//! standalone convenience for the times that is not needed.
//!
//! # The one rule that decides how to use it
//!
//! Read-modify-write across two calls is safe **only** when the key derives
//! from the partition key: then the lane serializes and no other writer inside
//! that consumer group can interleave. When it does not derive, use the atomics
//! — [`Kv::incr`], or `expect` on a write.
//!
//! And say it out loud in the code even when you believe the lane serializes
//! you: `expect` costs nothing when it never fails, and the day it does fail
//! you have just discovered that two consumers are serving the same partition —
//! with a verdict instead of a wrong total.
//!
//! # Expiry is mandatory
//!
//! Every write declares exactly one of [`Expiry::Seconds`] and
//! [`Expiry::Forever`]. There is no inherit-the-previous-TTL: a `put` that
//! quietly kept the old expiry is the fastest way to make a marker immortal.
//!
//! # Statuses
//!
//! Nothing here returns an error for a business verdict. A lost `putIfAbsent`,
//! a missing key, a delete that hit nothing: all `Ok`, with `applied` or
//! `found` saying so. An `Err` means the call did not happen.
//!
//! ```no_run
//! use queen_mq::{Expiry, Queen};
//!
//! # async fn example() -> queen_mq::Result<()> {
//! let queen = Queen::connect_to("http://localhost:6789")?;
//!
//! // Did anybody already handle this delivery?
//! let claim = queen
//!     .kv()
//!     .put_if_absent("orders", "idem:9137", serde_json::json!(true), Expiry::seconds(86_400))
//!     .send()
//!     .await?;
//!
//! if !claim.applied() {
//!     // Somebody won. `claim.value` is theirs — no second round trip.
//!     return Ok(());
//! }
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use queen_protocol::kv::{
    Expiry, KvDeleteBody, KvOpKind, KvOperation, KvPutBody, KvRequest, KvResponse, KvResult,
};
use queen_protocol::KvPrecondition;
use serde_json::Value;

use crate::error::{Error, Result};
use crate::http::Opts;
use crate::inner::Inner;
use crate::queue::urlencode;

/// What a call to the KV surface produced.
///
/// The second arm exists because a `required` operation that loses its
/// precondition rolls the **call** back: there are no per-operation results to
/// report, only the verdict, and pretending otherwise would mean inventing
/// results for operations that never ran.
#[derive(Debug, Clone, PartialEq)]
pub enum KvOutcome {
    /// One result per operation, index-aligned to the request.
    Results(Vec<KvResult>),

    /// A `required` operation lost. Nothing in the call was applied.
    Precondition(KvPrecondition),
}

impl KvOutcome {
    /// The results, or an empty slice when the call was rolled back by a
    /// precondition.
    pub fn results(&self) -> &[KvResult] {
        match self {
            Self::Results(r) => r,
            Self::Precondition(_) => &[],
        }
    }

    pub fn precondition(&self) -> Option<&KvPrecondition> {
        match self {
            Self::Precondition(p) => Some(p),
            Self::Results(_) => None,
        }
    }
}

/// The KV surface. Obtained from [`crate::Queen::kv`].
#[derive(Clone)]
pub struct Kv {
    inner: Arc<Inner>,
}

impl Kv {
    pub(crate) fn new(inner: Arc<Inner>) -> Self {
        Self { inner }
    }

    // ------------------------------------------------------------- reads

    /// Read one key.
    ///
    /// A miss is `Ok` with `found() == false`, and an **expired** key is a miss
    /// even before the sweeper has pruned it — the predicate is the truth, not
    /// the presence of the row.
    ///
    /// `found` is separate from `value` because `null` is a legal value.
    pub async fn get(&self, ns: &str, key: &str) -> Result<KvResult> {
        let path = format!("/api/v1/kv/{}/{}", urlencode(ns), urlencode(key));
        let elem: Option<KvResult> = self.inner.http.get_json(&path, &Opts::default()).await?;
        elem.ok_or_else(|| Error::Decode("kv get returned an empty body".into()))
    }

    /// Read several keys of one namespace in a single round trip.
    ///
    /// Absence comes back as a **list of missing keys**, not as a gap the
    /// caller works out by subtraction.
    pub async fn get_many(&self, ns: &str, keys: Vec<String>) -> Result<KvResult> {
        self.one(KvOperation::get_many(ns, keys)).await
    }

    /// Start a prefix scan.
    ///
    /// Only available here, in the POST batch: never as a query string (a
    /// prefix in a URL is recorded by every access log between the client and
    /// the database) and never inside a transaction (its cost is not bounded by
    /// the caller, and a transaction holds the outermost lock space).
    pub fn get_prefix(&self, ns: &str, prefix: &str) -> PrefixQuery {
        PrefixQuery {
            kv: self.clone(),
            op: KvOperation::get_prefix(ns, prefix),
        }
    }

    // ------------------------------------------------------------ writes

    /// Write a key, replacing whatever is there — value **and** expiry.
    pub fn put(&self, ns: &str, key: &str, value: Value, expiry: Expiry) -> WriteBuilder {
        WriteBuilder::new(self.clone(), KvOpKind::Put, ns, key, Some(value), Some(expiry))
    }

    /// Write a key **only if it is not there**.
    ///
    /// This is the idempotency marker, and `applied()` answers "did I win?".
    /// Exactly one concurrent caller wins: the conflict arm takes the row lock
    /// before it evaluates the condition, so the second caller re-reads against
    /// the winner's row rather than racing it. An expired-but-unpruned row
    /// counts as absent, so this also resurrects a dead lineage.
    ///
    /// Do not add an `expect` to it: the alias *is* `expect: 0`, and a
    /// different one is a contradiction the broker refuses.
    pub fn put_if_absent(
        &self,
        ns: &str,
        key: &str,
        value: Value,
        expiry: Expiry,
    ) -> WriteBuilder {
        WriteBuilder::new(
            self.clone(),
            KvOpKind::PutIfAbsent,
            ns,
            key,
            Some(value),
            Some(expiry),
        )
    }

    /// Delete a key. Deleting nothing is `applied() == false`, not an error,
    /// and never an HTTP failure.
    pub fn delete(&self, ns: &str, key: &str) -> WriteBuilder {
        WriteBuilder::new(self.clone(), KvOpKind::Delete, ns, key, None, None)
    }

    /// Add to a counter, atomically.
    ///
    /// This is the way **out** of compare-and-set, so it takes no `expect`. Two
    /// properties are worth knowing before using it as a rate limiter:
    ///
    /// * With [`IncrBuilder::max`], `applied()` **is** the admission decision.
    ///   An increment that would cross the ceiling does not happen and reports
    ///   the current value — so the request that would have broken the budget
    ///   has not already spent it.
    /// * The TTL is **create-only**. A live counter keeps its expiry, or a
    ///   fixed window on a busy caller would never close, which is to say it
    ///   would stop limiting exactly under load. An expired counter counts as
    ///   zero and starts a new window, which is what makes this one call.
    pub fn incr(&self, ns: &str, key: &str, delta: i64, expiry: Expiry) -> IncrBuilder {
        IncrBuilder {
            kv: self.clone(),
            ns: ns.to_string(),
            key: key.to_string(),
            delta,
            expiry,
            min: None,
            max: None,
            required: false,
        }
    }

    // ------------------------------------------------------------ batch

    /// Send a batch of operations built by hand.
    ///
    /// The escape hatch under the typed methods, and the only way to mix reads
    /// and writes in one round trip. Results are index-aligned to `ops`.
    pub async fn batch(&self, ops: Vec<KvOperation>) -> Result<KvOutcome> {
        if ops.is_empty() {
            return Ok(KvOutcome::Results(Vec::new()));
        }
        let expected = ops.len();
        let req = KvRequest::new(ops);
        let raw: Option<Value> = self
            .inner
            .http
            .post_json("/api/v1/kv", &req, &Opts::default())
            .await?;
        let raw = raw.ok_or_else(|| Error::Decode("kv batch returned an empty body".into()))?;

        // A rolled-back `required` operation answers 200 with the verdict
        // envelope instead of a results array. It is not an error — it is the
        // expected outcome of a legitimate redelivery — so it must not go
        // through the error path of any caller.
        if let Some(p) = precondition_from(&raw) {
            return Ok(KvOutcome::Precondition(p));
        }

        let resp: KvResponse = serde_json::from_value(raw)
            .map_err(|e| Error::Decode(format!("kv batch response: {e}")))?;
        // Same guard the ack path carries: a short array silently attributed to
        // the wrong operation is worse than a refused response.
        if resp.results.len() != expected {
            return Err(Error::Decode(format!(
                "kv returned {} results for {expected} operations",
                resp.results.len()
            )));
        }
        Ok(KvOutcome::Results(resp.results))
    }

    /// One operation through the batch route, unwrapped.
    async fn one(&self, op: KvOperation) -> Result<KvResult> {
        let key = op.key.clone();
        let kind = op.op;
        match self.batch(vec![op]).await? {
            KvOutcome::Results(mut r) => Ok(r.remove(0)),
            KvOutcome::Precondition(p) => Ok(synth(kind, key, p)),
        }
    }
}

/// Turn a single-operation precondition verdict into the result that operation
/// would have had.
///
/// Faithful rather than convenient: everything the caller needs — the reason,
/// the winner's version and value — is in the verdict, and for one operation
/// there is no ambiguity about which one lost. A batch keeps the verdict shape,
/// because there the question "which one" has an answer worth reading.
fn synth(kind: KvOpKind, key: Option<String>, p: KvPrecondition) -> KvResult {
    KvResult {
        index: p.failed_index.unwrap_or(0),
        op: kind.as_str().to_string(),
        applied: Some(false),
        reason: p.reason,
        key,
        value: p.value,
        version: p.version,
        ..Default::default()
    }
}

/// Read the `{"ok":false,"reason":"kv_precondition",...}` envelope, if that is
/// what this body is.
fn precondition_from(raw: &Value) -> Option<KvPrecondition> {
    if raw.get("reason").and_then(|r| r.as_str()) != Some("kv_precondition") {
        return None;
    }
    Some(KvPrecondition {
        failed_index: raw
            .get("failedIndex")
            .and_then(|v| v.as_u64())
            .map(|n| n as usize),
        reason: raw
            .get("kvReason")
            .and_then(|v| serde_json::from_value(v.clone()).ok()),
        version: raw.get("version").and_then(|v| v.as_i64()),
        // A present `null` is the winner's value being null, which is legal.
        value: raw.get("value").cloned(),
    })
}

/// A `put`, `putIfAbsent` or `delete` under construction.
pub struct WriteBuilder {
    kv: Kv,
    kind: KvOpKind,
    ns: String,
    key: String,
    value: Option<Value>,
    expiry: Option<Expiry>,
    expect: Option<i64>,
    required: bool,
}

impl WriteBuilder {
    fn new(
        kv: Kv,
        kind: KvOpKind,
        ns: &str,
        key: &str,
        value: Option<Value>,
        expiry: Option<Expiry>,
    ) -> Self {
        Self {
            kv,
            kind,
            ns: ns.to_string(),
            key: key.to_string(),
            value,
            expiry,
            expect: None,
            required: false,
        }
    }

    /// Fence this write on a version, `0` meaning "must not exist".
    ///
    /// A failed `expect` applies nothing and returns the current value and
    /// version. That version is **advisory**: it is read outside the row lock,
    /// so a CAS loop may reuse it, but a fencing scheme must not treat it as a
    /// token.
    pub fn expect(mut self, version: i64) -> Self {
        self.expect = Some(version);
        self
    }

    /// Turn a lost precondition into a rolled-back call.
    ///
    /// Inside a transaction this is the gate that stops the bundle. On this
    /// standalone route there is nothing else in the call to stop, so the only
    /// difference is that the result arrives as the verdict rather than as
    /// `applied: false` — which is why it is rarely what you want here.
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    /// The operation as it would be sent inside a transaction.
    pub fn operation(self) -> Result<KvOperation> {
        let mut op = match self.kind {
            KvOpKind::Delete => KvOperation::delete(&self.ns, &self.key),
            kind => {
                let value = self
                    .value
                    .ok_or_else(|| Error::Invalid("a put needs a value".into()))?;
                let expiry = self.expiry.ok_or_else(|| {
                    Error::Invalid("a put needs an expiry: Expiry::seconds(n) or Expiry::Forever".into())
                })?;
                let built = if kind == KvOpKind::PutIfAbsent {
                    KvOperation::put_if_absent(&self.ns, &self.key, value, expiry)
                } else {
                    KvOperation::put(&self.ns, &self.key, value, expiry)
                };
                built.map_err(Error::Invalid)?
            }
        };
        if let Some(v) = self.expect {
            op.expect = Some(v);
        }
        if self.required {
            op.required = Some(true);
        }
        Ok(op)
    }

    /// Send it.
    ///
    /// `put` and `delete` go through their own path route, which is the shape
    /// somebody reading a log or reaching for `curl` expects; `putIfAbsent`
    /// goes through the batch route, because the alias exists only there — the
    /// path route would need a literal segment under `/api/v1/kv/:ns/`, and any
    /// such segment makes a key of the same name unreachable forever.
    pub async fn send(self) -> Result<KvResult> {
        let (kind, ns, key) = (self.kind, self.ns.clone(), self.key.clone());
        let kv = self.kv.clone();
        match kind {
            KvOpKind::Put | KvOpKind::Delete => {
                let op = self.operation()?;
                let path = format!("/api/v1/kv/{}/{}", urlencode(&ns), urlencode(&key));
                let raw: Option<Value> = if kind == KvOpKind::Put {
                    let body = KvPutBody {
                        value: op.value.clone().unwrap_or(Value::Null),
                        ttl_seconds: op.ttl_seconds,
                        forever: op.forever,
                        expect: op.expect,
                        required: op.required,
                    };
                    kv.inner
                        .http
                        .put_json(&path, &body, &Opts::default())
                        .await?
                } else {
                    let body = KvDeleteBody {
                        expect: op.expect,
                        required: op.required,
                    };
                    kv.inner
                        .http
                        .delete_json_body(&path, &body, &Opts::default())
                        .await?
                };
                let raw =
                    raw.ok_or_else(|| Error::Decode("kv write returned an empty body".into()))?;
                if let Some(p) = precondition_from(&raw) {
                    return Ok(synth(kind, Some(key), p));
                }
                serde_json::from_value(raw)
                    .map_err(|e| Error::Decode(format!("kv write response: {e}")))
            }
            _ => {
                let op = self.operation()?;
                kv.one(op).await
            }
        }
    }
}

/// An `incr` under construction.
pub struct IncrBuilder {
    kv: Kv,
    ns: String,
    key: String,
    delta: i64,
    expiry: Expiry,
    min: Option<i64>,
    max: Option<i64>,
    required: bool,
}

impl IncrBuilder {
    /// Floor. A decrement that would cross it applies nothing.
    pub fn min(mut self, min: i64) -> Self {
        self.min = Some(min);
        self
    }

    /// Ceiling. With it, `applied()` is the admission decision.
    pub fn max(mut self, max: i64) -> Self {
        self.max = Some(max);
        self
    }

    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    pub fn operation(self) -> Result<KvOperation> {
        let mut op = KvOperation::incr(&self.ns, &self.key, self.delta, self.expiry)
            .map_err(Error::Invalid)?;
        op.min = self.min;
        op.max = self.max;
        if self.required {
            op.required = Some(true);
        }
        Ok(op)
    }

    /// Send it. `incr` has no path route: it lives only in the batch body.
    pub async fn send(self) -> Result<KvResult> {
        let kv = self.kv.clone();
        kv.one(self.operation()?).await
    }
}

/// A prefix scan under construction.
pub struct PrefixQuery {
    kv: Kv,
    op: KvOperation,
}

impl PrefixQuery {
    /// Resume after this key. An **exclusive keyset cursor**, not an offset:
    /// stable under byte ordering and aligned with the index.
    ///
    /// Each page is its own snapshot. With a cursor it may miss a key inserted
    /// behind it, which is fine for compacting state and wrong for an exact
    /// count.
    pub fn after(mut self, after: &str) -> Self {
        self.op = self.op.after(after);
        self
    }

    /// How many keys to return. **Clamped** server-side rather than rejected,
    /// with `truncated()` telling the truth about what the clamp cost — and a
    /// byte ceiling applies underneath it, because a thousand keys of 64 KiB is
    /// 64 MB and the real resource is the byte.
    pub fn limit(mut self, limit: u32) -> Self {
        self.op = self.op.limit(limit);
        self
    }

    /// Return keys and versions without values.
    pub fn keys_only(mut self) -> Self {
        self.op = self.op.keys_only();
        self
    }

    pub fn operation(self) -> KvOperation {
        self.op
    }

    pub async fn send(self) -> Result<KvResult> {
        let kv = self.kv.clone();
        kv.one(self.op).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::Queen;

    fn kv() -> Kv {
        Queen::connect(Config::new("http://127.0.0.1:1")).unwrap().kv()
    }

    #[test]
    fn a_put_if_absent_never_carries_an_expect_of_its_own() {
        let op = kv()
            .put_if_absent("ns", "k", Value::Bool(true), Expiry::seconds(60))
            .operation()
            .unwrap();
        assert_eq!(op.op, KvOpKind::PutIfAbsent);
        assert_eq!(op.expect, None, "the alias IS expect:0");
    }

    #[test]
    fn a_zero_ttl_fails_at_the_call_and_not_at_the_broker() {
        let e = kv()
            .put("ns", "k", Value::Null, Expiry::seconds(0))
            .operation()
            .unwrap_err();
        assert!(e.to_string().contains("greater than zero"), "{e}");
    }

    #[test]
    fn a_delete_sends_no_expiry_and_no_value() {
        let op = kv().delete("ns", "k").expect(7).operation().unwrap();
        let body = serde_json::to_string(&op).unwrap();
        assert_eq!(
            body,
            r#"{"op":"delete","ns":"ns","key":"k","expect":7}"#,
            "a delete carries the key, the fence, and nothing else"
        );
    }

    #[test]
    fn incr_carries_its_bounds_and_no_expect() {
        let op = kv()
            .incr("quota", "acme:minute", 1, Expiry::seconds(60))
            .max(100)
            .operation()
            .unwrap();
        assert_eq!(op.delta, Some(1));
        assert_eq!(op.max, Some(100));
        assert_eq!(op.expect, None);
    }

    #[test]
    fn a_precondition_envelope_is_recognised_and_not_mistaken_for_a_result() {
        let raw: Value = serde_json::from_str(
            r#"{"ok":false,"reason":"kv_precondition","failedIndex":0,"kvReason":"exists",
                "version":90101,"value":{"by":"worker-2"}}"#,
        )
        .unwrap();
        let p = precondition_from(&raw).expect("this is the verdict envelope");
        assert_eq!(p.failed_index, Some(0));
        assert_eq!(p.version, Some(90101));

        // ...and an ordinary result is not one.
        let ordinary: Value =
            serde_json::from_str(r#"{"index":0,"op":"put","applied":true,"key":"k"}"#).unwrap();
        assert!(precondition_from(&ordinary).is_none());
    }

    #[test]
    fn a_synthetic_result_keeps_everything_the_verdict_carried() {
        let p = KvPrecondition {
            failed_index: Some(0),
            reason: Some(queen_protocol::KvReason::Exists),
            version: Some(90101),
            value: Some(serde_json::json!({"by": "worker-2"})),
        };
        let r = synth(KvOpKind::Put, Some("idem:1".into()), p);
        assert!(!r.applied());
        assert_eq!(r.reason, Some(queen_protocol::KvReason::Exists));
        assert_eq!(r.version, Some(90101));
        assert_eq!(r.value.unwrap()["by"], "worker-2");
    }

    #[test]
    fn a_prefix_query_builds_the_op_the_batch_route_expects() {
        let op = kv()
            .get_prefix("orders", "saga:")
            .after("saga:100")
            .limit(50)
            .keys_only()
            .operation();
        let body = serde_json::to_string(&op).unwrap();
        assert!(body.contains(r#""prefix":"saga:""#), "{body}");
        assert!(body.contains(r#""after":"saga:100""#), "{body}");
        assert!(body.contains(r#""keysOnly":true"#), "{body}");
    }

    #[tokio::test]
    async fn an_empty_batch_never_reaches_the_network() {
        let out = kv().batch(Vec::new()).await.unwrap();
        assert!(out.results().is_empty());
    }
}
