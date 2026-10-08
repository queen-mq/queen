//! `POST /api/v1/locks` — a lock and a semaphore, as leases with a fencing
//! token.
//!
//! A permit is one KV row in the namespace [`LOCK_NAMESPACE`], written with a
//! lifetime; the broker turns each operation here into the KV write it is
//! (`acquire` a `putIfAbsent`, `renew` a `put` with `expect`, `release` a
//! `delete` with `expect`). A lock is the semaphore of one permit.
//!
//! Four rules of this wire, each a place where a client gets it wrong quietly:
//!
//! * **A verdict is a field, never a status.** A lock somebody else holds, a
//!   token that is no longer the row's, a release of what was not held: all
//!   HTTP 200, with `acquired`, `renewed` or `released` set to `false`.
//! * **A lock always has a finite lifetime.** `ttlSeconds`, an integer above
//!   zero, on every `acquire` and `renew`. There is no `forever`: nobody could
//!   take such a lock back from a holder that died.
//! * **A renew answers a NEW token.** It rewrites the row, so the token before
//!   it stops working — for the guard, for the next renew, for the release. A
//!   holder uses the token of its last answer.
//! * **It is a lease, not a mutex.** It expires and nobody stops the holder
//!   that outlived it. What makes the WORK exclusive is the [`LockResult::guard`]:
//!   a KV `check` of the permit's row at the token, `required`, to put in the
//!   `kv` array of a transaction. Outside the broker the token itself is the
//!   fence: on one lock it only rises.
//!
//! `owner` is the holder's identity, unique per holder. With it an operation
//! is safe to send again when its answer was lost: an acquire by the owner
//! that already holds a permit answers that permit (`already`), and a renew
//! whose token is stale because the owner's own earlier renew landed is
//! carried through.

use serde::{Deserialize, Serialize};

use crate::kv::KvOperation;

/// The KV namespace the permits live in. An ordinary one: the rows can be
/// read and listed through the KV routes, and a stuck lock an operator wants
/// gone is a KV delete.
pub const LOCK_NAMESPACE: &str = "queen-locks";

/// The most permits a semaphore can have.
pub const LOCK_MAX_LIMIT: u32 = 1024;

/// The four operations of the route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LockOpKind {
    Acquire,
    Renew,
    Release,
    Get,
}

impl LockOpKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Acquire => "acquire",
            Self::Renew => "renew",
            Self::Release => "release",
            Self::Get => "get",
        }
    }
}

/// One operation of a locks call. The four share one envelope; the
/// constructors below set the fields each one reads and nothing else.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockOperation {
    pub op: LockOpKind,

    /// The lock: non-empty, at most 256 bytes, no control character and no
    /// `#` (which sits between a name and its slot in the row's key).
    pub name: String,

    /// `acquire` and `renew`: the lifetime, mandatory, above zero.
    #[serde(
        rename = "ttlSeconds",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub ttl_seconds: Option<i64>,

    /// `acquire` and `renew`: the holder's identity. See the module header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,

    /// `acquire`: the permits of the semaphore, 1 to [`LOCK_MAX_LIMIT`].
    /// Absent is 1, a lock. Every caller of one name sends the same limit: it
    /// is the caller's and is stored nowhere.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,

    /// `renew` and `release`: the slot the permit is in. Absent is 0, which
    /// is the only slot a lock has.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot: Option<u32>,

    /// `renew` and `release`: the token of the permit, from the last acquire
    /// or renew.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<u64>,
}

impl LockOperation {
    fn bare(op: LockOpKind, name: impl Into<String>) -> Self {
        Self {
            op,
            name: name.into(),
            ttl_seconds: None,
            owner: None,
            limit: None,
            slot: None,
            token: None,
        }
    }

    /// Take a permit for `ttl_seconds`. A lock, unless [`Self::limit`] makes
    /// it a semaphore.
    pub fn acquire(name: impl Into<String>, ttl_seconds: i64) -> Self {
        Self {
            ttl_seconds: Some(ttl_seconds),
            ..Self::bare(LockOpKind::Acquire, name)
        }
    }

    /// Extend the permit `token` names for `ttl_seconds` more.
    pub fn renew(name: impl Into<String>, token: u64, ttl_seconds: i64) -> Self {
        Self {
            token: Some(token),
            ttl_seconds: Some(ttl_seconds),
            ..Self::bare(LockOpKind::Renew, name)
        }
    }

    /// Give the permit `token` names back.
    pub fn release(name: impl Into<String>, token: u64) -> Self {
        Self {
            token: Some(token),
            ..Self::bare(LockOpKind::Release, name)
        }
    }

    /// Who holds it.
    pub fn get(name: impl Into<String>) -> Self {
        Self::bare(LockOpKind::Get, name)
    }

    pub fn owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = Some(owner.into());
        self
    }

    /// Make an acquire a semaphore's, of `limit` permits.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn slot(mut self, slot: u32) -> Self {
        self.slot = Some(slot);
        self
    }
}

/// Body of `POST /api/v1/locks`. The broker also accepts a bare array; this
/// type always writes the `{"operations": [...]}` envelope, the one the KV
/// and timers routes share.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockRequest {
    pub operations: Vec<LockOperation>,
}

impl LockRequest {
    pub fn new(operations: Vec<LockOperation>) -> Self {
        Self { operations }
    }
}

/// Why an operation did not do what it asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LockReason {
    /// `acquire`: every permit is held.
    Held,
    /// `acquire` of a semaphore: permits were free and every one tried went
    /// to somebody else first. Come back, as for `held`.
    Contended,
    /// `renew`, `release`: the token is no longer the row's. The permit
    /// expired, was released, or is somebody else's (`holders` says which).
    Lost,
}

/// Somebody holding a permit. A refused `acquire` and a lost `renew` or
/// `release` name `slot` and `owner`; a `get` fills every field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockHolder {
    pub slot: u32,
    /// `None` for a permit taken without an owner, and for a row somebody
    /// wrote by hand through the KV routes.
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<u64>,
    /// When the holder took the permit. A renewal does not move it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<String>,
    #[serde(rename = "expiresAt", default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    /// When the permit was last acquired or renewed.
    #[serde(rename = "renewedAt", default, skip_serializing_if = "Option::is_none")]
    pub renewed_at: Option<String>,
}

/// One element of the `results` array, index-aligned to the operation that
/// produced it. The four operations answer different fields, so every one is
/// optional and the accessors below are the safe way to read the verdicts.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LockResult {
    #[serde(default)]
    pub index: usize,
    #[serde(default)]
    pub op: String,
    #[serde(default)]
    pub name: String,

    /// `acquire`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acquired: Option<bool>,
    /// `acquire`: the owner held this permit before the call (the answer to a
    /// retry whose first attempt had won).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub already: Option<bool>,
    /// `renew`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renewed: Option<bool>,
    /// `release`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub released: Option<bool>,
    /// `get`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<LockReason>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot: Option<u32>,

    /// The fencing token of the lease period this answer opens: on one lock a
    /// later one is always higher. Present when a permit was granted or
    /// renewed, and it replaces every token before it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,

    /// The KV `check` that holds while the permit is the caller's, ready for
    /// a transaction's `kv` array or a KV batch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guard: Option<KvOperation>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub holders: Vec<LockHolder>,
}

impl LockResult {
    /// Did the acquire take a permit?
    pub fn acquired(&self) -> bool {
        self.acquired.unwrap_or(false)
    }

    /// Did the renew extend it?
    pub fn renewed(&self) -> bool {
        self.renewed.unwrap_or(false)
    }

    /// Did the release remove it?
    pub fn released(&self) -> bool {
        self.released.unwrap_or(false)
    }

    /// Does anybody hold a permit (`get`)?
    pub fn held(&self) -> bool {
        self.held.unwrap_or(false)
    }
}

/// Body of the answer.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LockResponse {
    #[serde(default)]
    pub results: Vec<LockResult>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::KvOpKind;

    fn body(op: &LockOperation) -> String {
        serde_json::to_string(op).unwrap()
    }

    #[test]
    fn each_operation_sends_exactly_the_fields_it_reads() {
        assert_eq!(
            body(&LockOperation::acquire("daily-report", 30).owner("cron-7")),
            r#"{"op":"acquire","name":"daily-report","ttlSeconds":30,"owner":"cron-7"}"#
        );
        assert_eq!(
            body(&LockOperation::acquire("gpu", 60).limit(4)),
            r#"{"op":"acquire","name":"gpu","ttlSeconds":60,"limit":4}"#
        );
        assert_eq!(
            body(&LockOperation::renew("gpu", 90101, 60).slot(2).owner("w-1")),
            r#"{"op":"renew","name":"gpu","ttlSeconds":60,"owner":"w-1","slot":2,"token":90101}"#
        );
        assert_eq!(
            body(&LockOperation::release("daily-report", 90155)),
            r#"{"op":"release","name":"daily-report","token":90155}"#
        );
        assert_eq!(
            body(&LockOperation::get("daily-report")),
            r#"{"op":"get","name":"daily-report"}"#
        );
        let req = LockRequest::new(vec![LockOperation::get("a")]);
        assert!(serde_json::to_string(&req)
            .unwrap()
            .starts_with(r#"{"operations":["#));
    }

    /// The answers below are a 2.0.3+locks broker's, byte for byte.
    #[test]
    fn a_granted_acquire_carries_its_token_and_its_guard() {
        let r: LockResult = serde_json::from_str(
            r#"{"acquired":true,"guard":{"expect":1,"key":"daily-report#0","ns":"queen-locks",
                "op":"check","required":true},"index":0,"name":"daily-report","op":"acquire",
                "owner":"cron-7","slot":0,"token":1}"#,
        )
        .unwrap();
        assert!(r.acquired());
        assert_eq!(r.already, None, "a first acquire");
        assert_eq!((r.slot, r.token), (Some(0), Some(1)));
        assert_eq!(r.owner.as_deref(), Some("cron-7"));
        let guard = r.guard.expect("a guard");
        assert_eq!(guard.op, KvOpKind::Check);
        assert_eq!(guard.ns, LOCK_NAMESPACE);
        assert_eq!(guard.key.as_deref(), Some("daily-report#0"));
        assert_eq!(guard.expect, Some(1));
        assert_eq!(guard.required, Some(true));
        // It goes back on the wire as the op the KV routes take.
        assert_eq!(
            serde_json::to_string(&guard).unwrap(),
            r#"{"op":"check","ns":"queen-locks","key":"daily-report#0","expect":1,"required":true}"#
        );
    }

    #[test]
    fn a_refused_acquire_says_why_and_who() {
        let r: LockResult = serde_json::from_str(
            r#"{"acquired":false,"holders":[{"owner":"cron-7","slot":0}],"index":0,
                "name":"daily-report","op":"acquire","reason":"held"}"#,
        )
        .unwrap();
        assert!(!r.acquired());
        assert_eq!(r.reason, Some(LockReason::Held));
        assert_eq!(r.token, None, "no token to mistake for one's own");
        assert_eq!(r.holders.len(), 1);
        assert_eq!(r.holders[0].owner.as_deref(), Some("cron-7"));
        assert_eq!(r.holders[0].token, None);
        let contended: LockResult = serde_json::from_str(
            r#"{"acquired":false,"holders":[{"owner":null,"slot":1}],"index":0,"name":"gpu",
                "op":"acquire","reason":"contended"}"#,
        )
        .unwrap();
        assert_eq!(contended.reason, Some(LockReason::Contended));
        assert_eq!(contended.holders[0].owner, None, "a permit with no owner");
    }

    #[test]
    fn renew_release_and_get_answer_their_own_verdicts() {
        let renewed: LockResult = serde_json::from_str(
            r#"{"guard":{"expect":9,"key":"job#0","ns":"queen-locks","op":"check","required":true},
                "index":0,"name":"job","op":"renew","renewed":true,"slot":0,"token":9}"#,
        )
        .unwrap();
        assert!(renewed.renewed());
        assert_eq!(renewed.token, Some(9));
        assert!(
            !renewed.acquired(),
            "another operation's verdict reads false"
        );

        let lost: LockResult = serde_json::from_str(
            r#"{"holders":[{"owner":"b","slot":0}],"index":0,"name":"job","op":"renew",
                "reason":"lost","renewed":false,"slot":0}"#,
        )
        .unwrap();
        assert!(!lost.renewed());
        assert_eq!(lost.reason, Some(LockReason::Lost));

        let released: LockResult = serde_json::from_str(
            r#"{"index":0,"name":"job","op":"release","released":true,"slot":0}"#,
        )
        .unwrap();
        assert!(released.released());
        let gone: LockResult = serde_json::from_str(
            r#"{"holders":[],"index":0,"name":"job","op":"release","reason":"lost",
                "released":false,"slot":0}"#,
        )
        .unwrap();
        assert!(!gone.released() && gone.holders.is_empty());

        let got: LockResult = serde_json::from_str(
            r#"{"held":true,"holders":[{"expiresAt":"2026-10-08T08:50:00.244569+00:00",
                "owner":"cron-7","renewedAt":"2026-10-08T08:49:30.244569+00:00",
                "since":"2026-10-08T08:40:00.118203+00:00","slot":0,
                "token":1}],"index":0,"name":"job","op":"get"}"#,
        )
        .unwrap();
        assert!(got.held());
        let h = &got.holders[0];
        assert_eq!((h.slot, h.token), (0, Some(1)));
        assert!(h.expires_at.is_some() && h.renewed_at.is_some());
        assert_eq!(h.since.as_deref(), Some("2026-10-08T08:40:00.118203+00:00"));
    }

    #[test]
    fn a_result_from_a_newer_broker_still_parses() {
        let r: LockResponse = serde_json::from_str(
            r#"{"results":[{"index":0,"op":"acquire","name":"a","acquired":true,"slot":0,
                "token":7,"somethingNew":42}]}"#,
        )
        .expect("an unmodelled key must not fail the decode");
        assert!(r.results[0].acquired());
    }
}
