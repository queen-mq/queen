//! Locks: a lock and a semaphore, as leases stored in KV.
//!
//! # What a lock is here
//!
//! A permit is ONE KV row: namespace [`NAMESPACE`], key `<name>#<slot>`, a
//! lifetime, and the holder's `owner` as its value. A lock is a semaphore of
//! one permit (slot 0); a semaphore of N has the slots `0..N`. Nothing else is
//! stored anywhere: no table of locks, no list of waiters, no state of this
//! module in the state machine. Every operation of `POST /api/v1/locks` is
//! turned HERE, by the node that received it, into the KV calls any client
//! could send to `POST /api/v1/kv` — the way `putIfAbsent` is `put` with
//! `expect: 0`:
//!
//! | operation | the KV underneath |
//! |---|---|
//! | `acquire` | `putIfAbsent` of a free slot, with the lifetime |
//! | `renew` | a `get` of the slot, then `put` with `expect` and a new lifetime |
//! | `release` | `delete` of the slot with `expect: token` |
//! | `get` | the live rows under `<name>#` |
//!
//! So what makes a lock exclusive is what gives a `putIfAbsent` its one
//! winner: the planner, the one serial point every write passes
//! (`rsm/planner/kv.rs`), which is what the Jepsen runs check. This module
//! adds no atomicity of its own and needs none. A semaphore reads its slots
//! before it picks one, and that read decides only WHICH slot to try, never
//! whether the try wins.
//!
//! # A lease, not a mutex
//!
//! A permit expires. Nobody revokes it and nobody tells its holder, who may be
//! paused, partitioned or slow, and carry on past its lifetime. Keeping two
//! holders from DOING the work is therefore the token's job, not the lock's.
//! The token is the version of the permit's row; a later holder of the slot
//! always has a higher one (the planner's fencing contract); and every answer
//! that grants a permit carries its `guard`: a `check` of that row at that
//! version, `required`. In the `kv` array of a transaction it makes the
//! transaction commit only while the caller still holds the permit. Outside
//! the broker the token itself is the fence: a resource that remembers the
//! highest one it has seen refuses a holder that was replaced.
//!
//! The token names one lease PERIOD. A renew rewrites the row, so it answers
//! a NEW token and the one before stops working — for the guard, for the next
//! renew and for the release. A holder uses the token of its last answer.
//!
//! # The owner
//!
//! `owner` is the holder's identity: unique per holder and chosen by it (the
//! SDKs mint one). It is what makes an operation safe to send again when its
//! answer was lost. An acquire by an owner that already holds a permit
//! answers that permit (`already: true`); a renew whose token is stale
//! because the owner's own earlier renew landed, and only its answer was
//! lost, is carried through at the current token. Two callers with one owner
//! ARE one holder: that is the definition, not a case this module guards
//! against. An operation without an owner works and cannot be recovered: its
//! retry answers `held`, or `lost`. A permit's owner is the one its acquire
//! named: a renew rewrites the row with the value it has.
//!
//! # Since
//!
//! `since` is when the holder took the permit, which a renewal must not move:
//! "held for six hours" is the question somebody looking at a stuck lock
//! asks, and `updatedAt` answers "renewed four seconds ago". The row carries
//! no creation time, so the renew keeps it: an acquire writes the owner alone,
//! and the first renew copies the `updatedAt` it read — the planner's clock at
//! the acquire, never this node's — into the value as `since`, which every
//! later renew carries with the rest of the value. A permit never renewed was
//! taken when it was last written. That is the one reason a renew reads
//! before it writes.
//!
//! # A call
//!
//! One call carries up to [`MAX_OPS`] operations, each on a different lock,
//! and is answered with one result per operation, in order. The operations
//! are independent: nothing here is all-or-nothing. A call that fails (a 503)
//! may have applied some of them; sent again with the same owners it
//! converges. Its KV calls are one read when a semaphore, a renew or a `get`
//! needs one, ONE write carrying every operation's row, and then the rare
//! follow-up: a semaphore slot, or a renewed row, that changed between the
//! read and the write.

use std::collections::{BTreeMap, HashSet};

use serde_json::{json, Map, Value};

use crate::rsm::facade::{Deadline, KvFailure, KvReq, ReqCtx, Rsm};

/// The KV namespace every permit lives in, one per tenant like every
/// namespace. It is an ordinary one: its rows can be read, listed and — a
/// stuck lock an operator wants gone — deleted through the KV routes.
pub const NAMESPACE: &str = "queen-locks";
/// The most operations of one call.
pub const MAX_OPS: usize = 64;
/// The most permits of one semaphore.
pub const MAX_LIMIT: u32 = 1024;
/// The most slot rows one call may name (the planner's key budget of one KV
/// call, `planner::kv::MAX_KEYS_HTTP`).
pub const MAX_KEYS: usize = 4096;
/// A lock's name, in bytes.
pub const MAX_NAME_BYTES: usize = 256;
/// An owner, in bytes.
pub const MAX_OWNER_BYTES: usize = 256;
/// Between a name and its slot. A name may not contain it, which is what
/// makes `<name>#` the prefix of that lock's rows and of no other's.
pub const SLOT_SEP: char = '#';
/// How many slots a semaphore acquire tries before it answers `contended`.
const ACQUIRE_ATTEMPTS: usize = 4;
/// How many times a renew reads its row and writes it before it answers
/// `lost`: the second is for a row that changed under the first.
const RENEW_ATTEMPTS: usize = 2;
/// One page of a `get` (the planner's prefix cap).
const GET_PAGE: usize = 1000;

// ---------------------------------------------------------------------------
// The operations
// ---------------------------------------------------------------------------

/// One validated operation of a call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LockOp {
    Acquire {
        name: String,
        ttl_seconds: i64,
        owner: Option<String>,
        /// The permits of the semaphore: 1 is a lock.
        limit: u32,
    },
    Renew {
        name: String,
        slot: u32,
        token: u64,
        ttl_seconds: i64,
        owner: Option<String>,
    },
    Release {
        name: String,
        slot: u32,
        token: u64,
    },
    Get {
        name: String,
    },
}

impl LockOp {
    /// The wire name, which the answer's `op` echoes.
    pub fn kind(&self) -> &'static str {
        match self {
            LockOp::Acquire { .. } => "acquire",
            LockOp::Renew { .. } => "renew",
            LockOp::Release { .. } => "release",
            LockOp::Get { .. } => "get",
        }
    }

    pub fn name(&self) -> &str {
        match self {
            LockOp::Acquire { name, .. }
            | LockOp::Renew { name, .. }
            | LockOp::Release { name, .. }
            | LockOp::Get { name } => name,
        }
    }

    /// Writes a row, or tries to. A call of `get`s alone is a read.
    pub fn is_write(&self) -> bool {
        !matches!(self, LockOp::Get { .. })
    }

    /// The slot rows the operation may name in one KV call.
    fn keys(&self) -> usize {
        match self {
            LockOp::Acquire { limit, .. } => *limit as usize,
            LockOp::Get { .. } => GET_PAGE,
            _ => 1,
        }
    }
}

/// What a call may add to the tenant's KV occupancy, as an upper bound: one
/// row per `acquire`, and the bytes of its value.
pub fn footprint(ops: &[LockOp]) -> (i64, i64) {
    ops.iter()
        .filter_map(|op| match op {
            LockOp::Acquire { owner, .. } => Some(value_of(owner.as_deref()).to_string().len()),
            _ => None,
        })
        .fold((0, 0), |(rows, bytes), n| (rows + 1, bytes + n as i64))
}

/// A refused call (400): `reason` is the stable identifier a client may
/// branch on, `detail` the human half, which names only what the caller sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Invalid {
    pub reason: &'static str,
    pub detail: String,
}

fn bad(reason: &'static str, detail: impl Into<String>) -> Invalid {
    Invalid {
        reason,
        detail: detail.into(),
    }
}

/// A lock's name: non-empty, at most [`MAX_NAME_BYTES`], no control character
/// and no [`SLOT_SEP`].
pub fn name_ok(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME_BYTES
        && !name.chars().any(|c| c.is_control() || c == SLOT_SEP)
}

fn int_of(v: &Value) -> Option<i128> {
    let n = v.as_number()?;
    if let Some(i) = n.as_i64() {
        return Some(i128::from(i));
    }
    n.as_u64().map(i128::from)
}

/// Validate and type one call's operations, in order, then the two rules of
/// the call as a whole: a lock is named once, and the slot rows fit one KV
/// call.
pub fn parse_ops(ops: &[Value]) -> Result<Vec<LockOp>, Invalid> {
    if ops.len() > MAX_OPS {
        return Err(bad(
            "locks_too_many_ops",
            format!(
                "{} operations in one call, the ceiling is {MAX_OPS}",
                ops.len()
            ),
        ));
    }
    let mut out = Vec::with_capacity(ops.len());
    for (i, op) in ops.iter().enumerate() {
        out.push(parse_one(i, op)?);
    }
    // Every operation's row goes into ONE KV call, and a key is written at
    // most once per call: a lock is named once.
    let mut seen: HashSet<&str> = HashSet::new();
    for op in &out {
        if !seen.insert(op.name()) {
            return Err(bad(
                "locks_duplicate_in_call",
                "a lock may be named by one operation per call",
            ));
        }
    }
    let keys: usize = out.iter().map(LockOp::keys).sum();
    if keys > MAX_KEYS {
        return Err(bad(
            "locks_too_many_keys",
            format!("the call names {keys} permits, the ceiling is {MAX_KEYS}"),
        ));
    }
    Ok(out)
}

fn parse_one(i: usize, v: &Value) -> Result<LockOp, Invalid> {
    let Some(o) = v.as_object() else {
        return Err(bad(
            "locks_bad_request",
            format!("operation at index {i} is not an object"),
        ));
    };
    let kind = match o.get("op").and_then(Value::as_str) {
        Some(k @ ("acquire" | "renew" | "release" | "get")) => k,
        other => {
            return Err(bad(
                "locks_unknown_op",
                format!(
                    "operation at index {i}: unknown operation {}; the four are acquire, renew, \
                     release and get",
                    other.map_or("(none)".to_string(), |k| format!("'{k}'"))
                ),
            ))
        }
    };
    // As on the KV wire: the tenant is the caller's, never a field.
    if o.contains_key("tenant") || o.contains_key("tenantId") || o.contains_key("_tenant") {
        return Err(bad(
            "locks_tenant_not_an_input",
            format!("operation at index {i} carries a tenant field"),
        ));
    }
    let name = match o.get("name") {
        Some(Value::String(n)) if name_ok(n) => n.clone(),
        _ => {
            return Err(bad(
                "locks_bad_name",
                format!(
                    "operation at index {i}: a lock's name is a non-empty string of at most \
                     {MAX_NAME_BYTES} bytes, without control characters and without '{SLOT_SEP}'"
                ),
            ))
        }
    };

    // The lifetime: mandatory, finite, in whole seconds. A lock that never
    // expires is a lock nobody can take back from a holder that died.
    let ttl = |o: &Map<String, Value>| -> Result<i64, Invalid> {
        if o.contains_key("forever") {
            return Err(bad(
                "locks_ttl_required",
                format!(
                    "operation at index {i}: a lock has no `forever`; it takes ttlSeconds, and \
                     a holder that needs longer renews"
                ),
            ));
        }
        match o.get("ttlSeconds") {
            None => Err(bad(
                "locks_ttl_required",
                format!("operation at index {i}: {kind} needs ttlSeconds (an integer above 0)"),
            )),
            Some(t) => int_of(t)
                .filter(|n| *n > 0)
                .map(|n| n.min(i128::from(i64::MAX)) as i64)
                .ok_or_else(|| {
                    bad(
                        "locks_bad_ttl",
                        format!("operation at index {i}: ttlSeconds must be an integer above 0"),
                    )
                }),
        }
    };
    let owner = |o: &Map<String, Value>| -> Result<Option<String>, Invalid> {
        match o.get("owner") {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(s))
                if !s.is_empty()
                    && s.len() <= MAX_OWNER_BYTES
                    && !s.chars().any(char::is_control) =>
            {
                Ok(Some(s.clone()))
            }
            Some(_) => Err(bad(
                "locks_bad_owner",
                format!(
                    "operation at index {i}: owner is a non-empty string of at most \
                     {MAX_OWNER_BYTES} bytes, without control characters"
                ),
            )),
        }
    };
    let token = |o: &Map<String, Value>| -> Result<u64, Invalid> {
        match o.get("token") {
            None => Err(bad(
                "locks_token_required",
                format!(
                    "operation at index {i}: {kind} needs the token of the permit (the one the \
                     last acquire or renew answered)"
                ),
            )),
            Some(t) => int_of(t)
                .filter(|n| *n > 0)
                .and_then(|n| u64::try_from(n).ok())
                .ok_or_else(|| {
                    bad(
                        "locks_bad_token",
                        format!("operation at index {i}: token must be an integer above 0"),
                    )
                }),
        }
    };
    let slot = |o: &Map<String, Value>| -> Result<u32, Invalid> {
        match o.get("slot") {
            None | Some(Value::Null) => Ok(0),
            Some(s) => int_of(s)
                .filter(|n| (0..i128::from(MAX_LIMIT)).contains(n))
                .map(|n| n as u32)
                .ok_or_else(|| {
                    bad(
                        "locks_bad_slot",
                        format!(
                            "operation at index {i}: slot must be an integer from 0 to {}",
                            MAX_LIMIT - 1
                        ),
                    )
                }),
        }
    };

    Ok(match kind {
        "acquire" => {
            let limit = match o.get("limit") {
                None | Some(Value::Null) => 1,
                Some(l) => int_of(l)
                    .filter(|n| (1..=i128::from(MAX_LIMIT)).contains(n))
                    .map(|n| n as u32)
                    .ok_or_else(|| {
                        bad(
                            "locks_bad_limit",
                            format!(
                                "operation at index {i}: limit must be an integer from 1 (a \
                                 lock) to {MAX_LIMIT}"
                            ),
                        )
                    })?,
            };
            LockOp::Acquire {
                name,
                ttl_seconds: ttl(o)?,
                owner: owner(o)?,
                limit,
            }
        }
        "renew" => LockOp::Renew {
            name,
            slot: slot(o)?,
            token: token(o)?,
            ttl_seconds: ttl(o)?,
            owner: owner(o)?,
        },
        "release" => LockOp::Release {
            name,
            slot: slot(o)?,
            token: token(o)?,
        },
        _ => LockOp::Get { name },
    })
}

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

/// The key of one permit.
pub fn slot_key(name: &str, slot: u32) -> String {
    format!("{name}{SLOT_SEP}{slot}")
}

/// The slot a key of `name` names, when it is one of its permits: the decimal
/// this module writes, nothing looser (`#07` and `#+7` are somebody else's).
pub fn slot_of(name: &str, key: &str) -> Option<u32> {
    let digits = key.strip_prefix(name)?.strip_prefix(SLOT_SEP)?;
    let slot: u32 = digits.parse().ok()?;
    (slot.to_string() == digits && slot < MAX_LIMIT).then_some(slot)
}

/// The value of a permit's row.
fn value_of(owner: Option<&str>) -> Value {
    json!({ "owner": owner })
}

/// The owner a row's value names. A row somebody wrote by hand through the
/// KV routes may hold anything: it then has no owner, and holds the slot all
/// the same.
fn owner_of(value: &Value) -> Option<&str> {
    value.get("owner").and_then(Value::as_str)
}

/// The `check` that holds while the permit is still the caller's: the KV op
/// to put in a transaction's `kv` array.
pub fn guard(name: &str, slot: u32, token: u64) -> Value {
    json!({
        "op": "check",
        "ns": NAMESPACE,
        "key": slot_key(name, slot),
        "expect": token,
        "required": true,
    })
}

fn put_if_absent(name: &str, slot: u32, owner: Option<&str>, ttl_seconds: i64) -> Value {
    json!({
        "op": "putIfAbsent",
        "ns": NAMESPACE,
        "key": slot_key(name, slot),
        "value": value_of(owner),
        "ttlSeconds": ttl_seconds,
    })
}

fn put_expect(name: &str, slot: u32, value: &Value, ttl_seconds: i64, expect: u64) -> Value {
    json!({
        "op": "put",
        "ns": NAMESPACE,
        "key": slot_key(name, slot),
        "value": value,
        "ttlSeconds": ttl_seconds,
        "expect": expect,
    })
}

fn delete_expect(name: &str, slot: u32, expect: u64) -> Value {
    json!({
        "op": "delete",
        "ns": NAMESPACE,
        "key": slot_key(name, slot),
        "expect": expect,
    })
}

fn get_slots(name: &str, limit: u32) -> Value {
    json!({
        "op": "getMany",
        "ns": NAMESPACE,
        "keys": (0..limit).map(|s| slot_key(name, s)).collect::<Vec<_>>(),
    })
}

fn get_page(name: &str, after: Option<&str>) -> Value {
    let mut op = json!({
        "op": "getPrefix",
        "ns": NAMESPACE,
        "prefix": format!("{name}{SLOT_SEP}"),
        "limit": GET_PAGE,
    });
    if let Some(a) = after {
        op["after"] = Value::String(a.to_string());
    }
    op
}

fn get_slot(name: &str, slot: u32) -> Value {
    json!({ "op": "get", "ns": NAMESPACE, "key": slot_key(name, slot) })
}

/// When the holder of `row` (a KV read of a permit) took it: the `since` a
/// renew wrote into the value, and for a permit never renewed the time of its
/// one write.
fn since_of(row: &Value) -> Value {
    match row["value"].get("since") {
        Some(Value::String(s)) => Value::String(s.clone()),
        _ => row["updatedAt"].clone(),
    }
}

/// What a renew does, from the row it read.
#[derive(Clone, Debug, PartialEq)]
enum Renewal {
    /// Rewrite the row with this value, expecting this version.
    Put { value: Value, expect: u64 },
    /// The permit is not the caller's any more: who has it, if anybody.
    Lost(Value),
}

/// A renew of the slot `row` was read from, by a caller that sent `token` and
/// maybe its `owner`.
///
/// The token is the row's: the caller's own period, rewritten with the value
/// it has. The token is stale and the row is this owner's: its own earlier
/// renew landed and only the answer was lost, so it is carried through at the
/// version the row has now. Anything else is somebody else's permit, or
/// nobody's.
fn renewal_of(row: &Value, slot: u32, token: u64, owner: Option<&str>) -> Renewal {
    if row["found"] != Value::Bool(true) {
        return Renewal::Lost(json!([]));
    }
    let version = row["version"].as_u64().unwrap_or(0);
    let mine = owner.is_some() && owner_of(&row["value"]) == owner;
    if version == 0 || (version != token && !mine) {
        return Renewal::Lost(json!([{ "slot": slot, "owner": owner_of(&row["value"]) }]));
    }
    let mut value = row["value"].clone();
    // A row somebody wrote by hand may hold anything; it is renewed as it is.
    if let Some(fields) = value.as_object_mut() {
        fields.insert("since".into(), since_of(row));
    }
    Renewal::Put {
        value,
        expect: version,
    }
}

/// A permit somebody holds, as a KV read showed it.
#[derive(Clone, Debug, PartialEq)]
struct Held {
    owner: Option<String>,
    token: u64,
    since: Value,
    expires_at: Value,
    renewed_at: Value,
}

/// The live permits of `name` among `rows` (the rows of a getMany or a
/// getPrefix), by slot.
fn held_of(name: &str, rows: &Value) -> BTreeMap<u32, Held> {
    let mut held = BTreeMap::new();
    for row in rows.as_array().map(Vec::as_slice).unwrap_or_default() {
        let Some(slot) = row["key"].as_str().and_then(|k| slot_of(name, k)) else {
            continue;
        };
        held.insert(
            slot,
            Held {
                owner: owner_of(&row["value"]).map(str::to_string),
                token: row["version"].as_u64().unwrap_or(0),
                since: since_of(row),
                expires_at: row["expiresAt"].clone(),
                renewed_at: row["updatedAt"].clone(),
            },
        );
    }
    held
}

/// What a semaphore acquire does next, from the slots it read.
#[derive(Clone, Debug, PartialEq)]
enum Next {
    /// The owner already holds this permit.
    Already { slot: u32, token: u64 },
    /// Every permit is held.
    Full,
    /// A free slot to try.
    Try(u32),
}

/// `pick` chooses among the free slots: a random one, so that contenders
/// reading the same view do not all try the lowest.
fn next_of(held: &BTreeMap<u32, Held>, limit: u32, owner: Option<&str>, pick: u64) -> Next {
    if let Some(me) = owner {
        if let Some((slot, h)) = held
            .iter()
            .find(|(s, h)| **s < limit && h.owner.as_deref() == Some(me))
        {
            return Next::Already {
                slot: *slot,
                token: h.token,
            };
        }
    }
    let free: Vec<u32> = (0..limit).filter(|s| !held.contains_key(s)).collect();
    match free.len() {
        0 => Next::Full,
        n => Next::Try(free[(pick % n as u64) as usize]),
    }
}

fn pick() -> u64 {
    // The tail of a v7 uuid is random.
    let b = crate::util::uuidv7_bytes();
    u64::from_le_bytes([b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]])
}

// ---------------------------------------------------------------------------
// Answers
// ---------------------------------------------------------------------------

fn holders_brief(held: &BTreeMap<u32, Held>, limit: u32) -> Value {
    Value::Array(
        held.iter()
            .filter(|(s, _)| **s < limit)
            .map(|(slot, h)| json!({ "slot": slot, "owner": h.owner }))
            .collect(),
    )
}

/// The one holder a lost write names: the row the planner saw in its way.
fn holder_of_verdict(slot: u32, verdict: &Value) -> Value {
    if verdict["version"].as_u64().unwrap_or(0) == 0 {
        return json!([]);
    }
    json!([{ "slot": slot, "owner": owner_of(&verdict["value"]) }])
}

fn granted(
    i: usize,
    name: &str,
    slot: u32,
    token: u64,
    owner: Option<&str>,
    already: bool,
) -> Value {
    let mut v = json!({
        "index": i,
        "op": "acquire",
        "name": name,
        "acquired": true,
        "slot": slot,
        "token": token,
        "owner": owner,
        "guard": guard(name, slot, token),
    });
    if already {
        v["already"] = Value::Bool(true);
    }
    v
}

fn refused(i: usize, name: &str, reason: &str, holders: Value) -> Value {
    json!({
        "index": i,
        "op": "acquire",
        "name": name,
        "acquired": false,
        "reason": reason,
        "holders": holders,
    })
}

fn renewed(i: usize, name: &str, slot: u32, token: u64) -> Value {
    json!({
        "index": i,
        "op": "renew",
        "name": name,
        "renewed": true,
        "slot": slot,
        "token": token,
        "guard": guard(name, slot, token),
    })
}

fn released(i: usize, name: &str, slot: u32) -> Value {
    json!({ "index": i, "op": "release", "name": name, "released": true, "slot": slot })
}

/// A renew or a release whose token is no longer the row's: the permit
/// expired, was released, or is somebody else's now (`holders` says which).
fn lost(i: usize, op: &str, name: &str, slot: u32, holders: Value) -> Value {
    let flag = if op == "renew" { "renewed" } else { "released" };
    let mut v = json!({ "index": i, "op": op, "name": name });
    v[flag] = Value::Bool(false);
    v["reason"] = Value::String("lost".into());
    v["slot"] = Value::from(slot);
    v["holders"] = holders;
    v
}

fn got(i: usize, name: &str, held: &BTreeMap<u32, Held>) -> Value {
    json!({
        "index": i,
        "op": "get",
        "name": name,
        "held": !held.is_empty(),
        "holders": held
            .iter()
            .map(|(slot, h)| json!({
                "slot": slot,
                "owner": h.owner,
                "token": h.token,
                "since": h.since,
                "expiresAt": h.expires_at,
                "renewedAt": h.renewed_at,
            }))
            .collect::<Vec<_>>(),
    })
}

// ---------------------------------------------------------------------------
// The call
// ---------------------------------------------------------------------------

/// One KV call of `tenant`, under the lock call's one deadline. Every call
/// mints its own request id: each is a command of its own.
async fn kv(
    rsm: &dyn Rsm,
    tenant: &str,
    deadline: Deadline,
    ops: Vec<Value>,
) -> Result<Vec<Value>, KvFailure> {
    if ops.is_empty() {
        return Ok(Vec::new());
    }
    let n = ops.len();
    let out = rsm
        .kv(ReqCtx::new(tenant, deadline), KvReq { ops })
        .await?
        .results;
    if out.len() != n {
        return Err(KvFailure::Rsm(crate::rsm::facade::RsmError::Internal(
            "locks: the kv answer is not index-aligned".into(),
        )));
    }
    Ok(out)
}

async fn kv_one(
    rsm: &dyn Rsm,
    tenant: &str,
    deadline: Deadline,
    op: Value,
) -> Result<Value, KvFailure> {
    Ok(kv(rsm, tenant, deadline, vec![op]).await?.remove(0))
}

fn applied(verdict: &Value) -> bool {
    verdict["applied"] == Value::Bool(true)
}

/// What an operation is waiting on after the reads.
enum Step {
    /// Answered.
    Done(Value),
    /// Its row is at this index of the one write call.
    Wrote { at: usize, slot: u32 },
}

/// A call that produced no answer: one of its KV calls failed (a timeout, no
/// leader, a store refusal).
#[derive(Debug)]
pub struct Failed {
    pub failure: KvFailure,
    /// A write had been sent when it failed, so some operations may have
    /// applied. `false`: the call wrote nothing, for certain.
    pub wrote: bool,
}

/// Apply one call's operations for `tenant`: the answers, index-aligned.
///
/// On [`Failed`] with `wrote`, some operations may have applied. See the
/// module header for why sending the call again is safe.
pub async fn apply(
    rsm: &dyn Rsm,
    tenant: &str,
    deadline: Deadline,
    ops: &[LockOp],
) -> Result<Vec<Value>, Failed> {
    let mut wrote = false;
    apply_steps(rsm, tenant, deadline, ops, &mut wrote)
        .await
        .map_err(|failure| Failed { failure, wrote })
}

async fn apply_steps(
    rsm: &dyn Rsm,
    tenant: &str,
    deadline: Deadline,
    ops: &[LockOp],
    sent_a_write: &mut bool,
) -> Result<Vec<Value>, KvFailure> {
    // ---- 1. the reads: a semaphore's slots, the row a renew rewrites, the
    //         rows of a get.
    let mut read_at: Vec<Option<usize>> = vec![None; ops.len()];
    let mut reads: Vec<Value> = Vec::new();
    for (i, op) in ops.iter().enumerate() {
        let read = match op {
            LockOp::Acquire { name, limit, .. } if *limit > 1 => get_slots(name, *limit),
            LockOp::Renew { name, slot, .. } => get_slot(name, *slot),
            LockOp::Get { name } => get_page(name, None),
            _ => continue,
        };
        read_at[i] = Some(reads.len());
        reads.push(read);
    }
    let read = kv(rsm, tenant, deadline, reads).await?;

    // ---- 2. the one write: every operation's row.
    let mut steps: Vec<Step> = Vec::with_capacity(ops.len());
    let mut writes: Vec<Value> = Vec::new();
    for (i, op) in ops.iter().enumerate() {
        let seen = read_at[i].map(|at| &read[at]);
        let mut wrote = |row: Value, slot: u32| {
            writes.push(row);
            Step::Wrote {
                at: writes.len() - 1,
                slot,
            }
        };
        steps.push(match op {
            LockOp::Acquire {
                name,
                ttl_seconds,
                owner,
                limit,
            } => {
                let owner = owner.as_deref();
                match seen {
                    // A lock: the one slot, tried at once.
                    None => wrote(put_if_absent(name, 0, owner, *ttl_seconds), 0),
                    Some(slots) => {
                        let held = held_of(name, &slots["rows"]);
                        match next_of(&held, *limit, owner, pick()) {
                            Next::Already { slot, token } => {
                                Step::Done(granted(i, name, slot, token, owner, true))
                            }
                            Next::Full => {
                                Step::Done(refused(i, name, "held", holders_brief(&held, *limit)))
                            }
                            Next::Try(slot) => {
                                wrote(put_if_absent(name, slot, owner, *ttl_seconds), slot)
                            }
                        }
                    }
                }
            }
            LockOp::Renew {
                name,
                slot,
                token,
                ttl_seconds,
                owner,
            } => {
                let row = seen.unwrap_or(&Value::Null);
                match renewal_of(row, *slot, *token, owner.as_deref()) {
                    Renewal::Put { value, expect } => {
                        wrote(put_expect(name, *slot, &value, *ttl_seconds, expect), *slot)
                    }
                    Renewal::Lost(holders) => Step::Done(lost(i, "renew", name, *slot, holders)),
                }
            }
            LockOp::Release { name, slot, token } => {
                wrote(delete_expect(name, *slot, *token), *slot)
            }
            LockOp::Get { name } => {
                let page = seen.cloned().unwrap_or(Value::Null);
                let mut held = held_of(name, &page["rows"]);
                let mut after = page["nextAfter"].as_str().map(str::to_string);
                // A semaphore past one page of permits: the pages that remain.
                while let Some(a) = after {
                    let more = kv_one(rsm, tenant, deadline, get_page(name, Some(&a))).await?;
                    held.extend(held_of(name, &more["rows"]));
                    after = more["nextAfter"].as_str().map(str::to_string);
                }
                Step::Done(got(i, name, &held))
            }
        });
    }
    *sent_a_write = !writes.is_empty();
    let wrote = kv(rsm, tenant, deadline, writes).await?;

    // ---- 3. the verdicts, and what little is left to do after them.
    let mut out = Vec::with_capacity(ops.len());
    for (i, (op, step)) in ops.iter().zip(steps).enumerate() {
        let (verdict, slot) = match step {
            Step::Done(answer) => {
                out.push(answer);
                continue;
            }
            Step::Wrote { at, slot } => (&wrote[at], slot),
        };
        let token = verdict["version"].as_u64().unwrap_or(0);
        out.push(match op {
            LockOp::Acquire { name, owner, .. } if applied(verdict) => {
                granted(i, name, slot, token, owner.as_deref(), false)
            }
            // The slot was taken — by this owner itself, when the answer to
            // an earlier acquire of its own was lost.
            LockOp::Acquire {
                name,
                owner: Some(owner),
                ..
            } if owner_of(&verdict["value"]) == Some(owner.as_str()) => {
                granted(i, name, slot, token, Some(owner), true)
            }
            LockOp::Acquire { name, limit: 1, .. } => {
                refused(i, name, "held", holder_of_verdict(slot, verdict))
            }
            LockOp::Acquire {
                name,
                ttl_seconds,
                owner,
                limit,
            } => {
                acquire_again(
                    rsm,
                    tenant,
                    deadline,
                    i,
                    name,
                    *ttl_seconds,
                    owner.as_deref(),
                    *limit,
                )
                .await?
            }
            LockOp::Renew { name, .. } if applied(verdict) => renewed(i, name, slot, token),
            // The row changed between the read and the write: it expired, it
            // went to somebody else, or this owner's other renew landed first.
            LockOp::Renew {
                name,
                token: sent,
                ttl_seconds,
                owner,
                ..
            } => {
                renew_again(
                    rsm,
                    tenant,
                    deadline,
                    i,
                    name,
                    slot,
                    *sent,
                    *ttl_seconds,
                    owner.as_deref(),
                )
                .await?
            }
            LockOp::Release { name, .. } if applied(verdict) => released(i, name, slot),
            LockOp::Release { name, .. } => {
                lost(i, "release", name, slot, holder_of_verdict(slot, verdict))
            }
            // A get is answered in step 2.
            LockOp::Get { name } => got(i, name, &BTreeMap::new()),
        });
    }
    Ok(out)
}

/// A renew whose row changed between its read and its write: read it again
/// and decide again, once. What the second read shows is the answer — the
/// permit is gone, somebody else's, or this owner's at a newer token (its own
/// other renew landed in between), which is carried through like any stale
/// token of its own.
#[allow(clippy::too_many_arguments)]
async fn renew_again(
    rsm: &dyn Rsm,
    tenant: &str,
    deadline: Deadline,
    i: usize,
    name: &str,
    slot: u32,
    token: u64,
    ttl_seconds: i64,
    owner: Option<&str>,
) -> Result<Value, KvFailure> {
    let mut holders = json!([]);
    for _ in 1..RENEW_ATTEMPTS {
        let row = kv_one(rsm, tenant, deadline, get_slot(name, slot)).await?;
        let (value, expect) = match renewal_of(&row, slot, token, owner) {
            Renewal::Put { value, expect } => (value, expect),
            Renewal::Lost(holders) => return Ok(lost(i, "renew", name, slot, holders)),
        };
        let verdict = kv_one(
            rsm,
            tenant,
            deadline,
            put_expect(name, slot, &value, ttl_seconds, expect),
        )
        .await?;
        if applied(&verdict) {
            let token = verdict["version"].as_u64().unwrap_or(0);
            return Ok(renewed(i, name, slot, token));
        }
        holders = holder_of_verdict(slot, &verdict);
    }
    Ok(lost(i, "renew", name, slot, holders))
}

/// A semaphore acquire whose first slot went to somebody else between the
/// read and the write: read again, try another, a few times. Past that the
/// semaphore is `contended` — free permits, and a crowd on them — and the
/// caller comes back, as it does for `held`.
#[allow(clippy::too_many_arguments)]
async fn acquire_again(
    rsm: &dyn Rsm,
    tenant: &str,
    deadline: Deadline,
    i: usize,
    name: &str,
    ttl_seconds: i64,
    owner: Option<&str>,
    limit: u32,
) -> Result<Value, KvFailure> {
    let mut held = BTreeMap::new();
    for _ in 1..ACQUIRE_ATTEMPTS {
        let slots = kv_one(rsm, tenant, deadline, get_slots(name, limit)).await?;
        held = held_of(name, &slots["rows"]);
        let slot = match next_of(&held, limit, owner, pick()) {
            Next::Already { slot, token } => return Ok(granted(i, name, slot, token, owner, true)),
            Next::Full => return Ok(refused(i, name, "held", holders_brief(&held, limit))),
            Next::Try(slot) => slot,
        };
        let verdict = kv_one(
            rsm,
            tenant,
            deadline,
            put_if_absent(name, slot, owner, ttl_seconds),
        )
        .await?;
        let token = verdict["version"].as_u64().unwrap_or(0);
        if applied(&verdict) {
            return Ok(granted(i, name, slot, token, owner, false));
        }
        if owner.is_some() && owner_of(&verdict["value"]) == owner {
            return Ok(granted(i, name, slot, token, owner, true));
        }
    }
    Ok(refused(i, name, "contended", holders_brief(&held, limit)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ops(v: Value) -> Result<Vec<LockOp>, Invalid> {
        parse_ops(v.as_array().expect("an array"))
    }

    fn reason(v: Value) -> &'static str {
        ops(v).expect_err("the call must be refused").reason
    }

    #[test]
    fn the_four_operations_parse_with_their_defaults() {
        let got = ops(json!([
            {"op":"acquire","name":"daily-report","ttlSeconds":30},
            {"op":"acquire","name":"gpu","ttlSeconds":60,"owner":"w-1","limit":4},
            {"op":"renew","name":"a","token":7,"ttlSeconds":30},
            {"op":"renew","name":"b","token":8,"ttlSeconds":30,"slot":3,"owner":"w-1"},
            {"op":"release","name":"c","token":9},
            {"op":"get","name":"d"},
        ]))
        .expect("valid");
        assert_eq!(
            got[0],
            LockOp::Acquire {
                name: "daily-report".into(),
                ttl_seconds: 30,
                owner: None,
                limit: 1
            }
        );
        assert_eq!(
            got[1],
            LockOp::Acquire {
                name: "gpu".into(),
                ttl_seconds: 60,
                owner: Some("w-1".into()),
                limit: 4
            }
        );
        assert_eq!(
            got[2],
            LockOp::Renew {
                name: "a".into(),
                slot: 0,
                token: 7,
                ttl_seconds: 30,
                owner: None
            }
        );
        assert_eq!(
            got[3],
            LockOp::Renew {
                name: "b".into(),
                slot: 3,
                token: 8,
                ttl_seconds: 30,
                owner: Some("w-1".into())
            }
        );
        assert_eq!(
            got[4],
            LockOp::Release {
                name: "c".into(),
                slot: 0,
                token: 9
            }
        );
        assert_eq!(got[5], LockOp::Get { name: "d".into() });
        assert!(got[..5].iter().all(LockOp::is_write) && !got[5].is_write());
        assert_eq!(ops(json!([])).expect("an empty call"), Vec::new());
    }

    #[test]
    fn a_lock_always_has_a_finite_lifetime() {
        assert_eq!(
            reason(json!([{"op":"acquire","name":"a"}])),
            "locks_ttl_required"
        );
        assert_eq!(
            reason(json!([{"op":"acquire","name":"a","forever":true}])),
            "locks_ttl_required"
        );
        assert_eq!(
            reason(json!([{"op":"acquire","name":"a","ttlSeconds":30,"forever":true}])),
            "locks_ttl_required",
            "forever beside a ttl is still a lock asked to never expire"
        );
        for ttl in [json!(0), json!(-5), json!(1.5), json!("30"), Value::Null] {
            assert_eq!(
                reason(json!([{"op":"acquire","name":"a","ttlSeconds":ttl}])),
                "locks_bad_ttl",
                "{ttl}"
            );
        }
        assert_eq!(
            reason(json!([{"op":"renew","name":"a","token":1}])),
            "locks_ttl_required"
        );
    }

    #[test]
    fn a_call_is_refused_by_name() {
        let cases: Vec<(Value, &str)> = vec![
            (json!(["acquire"]), "locks_bad_request"),
            (json!([{"name":"a","ttlSeconds":1}]), "locks_unknown_op"),
            (json!([{"op":"lock","name":"a"}]), "locks_unknown_op"),
            (json!([{"op":"get"}]), "locks_bad_name"),
            (json!([{"op":"get","name":""}]), "locks_bad_name"),
            (json!([{"op":"get","name":"a#1"}]), "locks_bad_name"),
            (json!([{"op":"get","name":"a\nb"}]), "locks_bad_name"),
            (json!([{"op":"get","name":7}]), "locks_bad_name"),
            (
                json!([{"op":"get","name":"x".repeat(MAX_NAME_BYTES + 1)}]),
                "locks_bad_name",
            ),
            (
                json!([{"op":"get","name":"a","tenant":"other"}]),
                "locks_tenant_not_an_input",
            ),
            (
                json!([{"op":"acquire","name":"a","ttlSeconds":1,"owner":""}]),
                "locks_bad_owner",
            ),
            (
                json!([{"op":"acquire","name":"a","ttlSeconds":1,"owner":7}]),
                "locks_bad_owner",
            ),
            (
                json!([{"op":"acquire","name":"a","ttlSeconds":1,"limit":0}]),
                "locks_bad_limit",
            ),
            (
                json!([{"op":"acquire","name":"a","ttlSeconds":1,"limit":MAX_LIMIT + 1}]),
                "locks_bad_limit",
            ),
            (
                json!([{"op":"renew","name":"a","ttlSeconds":1}]),
                "locks_token_required",
            ),
            (json!([{"op":"release","name":"a"}]), "locks_token_required"),
            (
                json!([{"op":"release","name":"a","token":0}]),
                "locks_bad_token",
            ),
            (
                json!([{"op":"release","name":"a","token":"7"}]),
                "locks_bad_token",
            ),
            (
                json!([{"op":"release","name":"a","token":7,"slot":-1}]),
                "locks_bad_slot",
            ),
            (
                json!([{"op":"release","name":"a","token":7,"slot":MAX_LIMIT}]),
                "locks_bad_slot",
            ),
            (
                json!([{"op":"get","name":"a"},{"op":"release","name":"a","token":7}]),
                "locks_duplicate_in_call",
            ),
        ];
        for (call, want) in cases {
            assert_eq!(reason(call.clone()), want, "{call}");
        }
        let many: Vec<Value> = (0..=MAX_OPS)
            .map(|i| json!({"op":"get","name":format!("l{i}")}))
            .collect();
        assert_eq!(reason(Value::Array(many)), "locks_too_many_ops");
        // The slot rows of one call fit one KV call.
        let wide: Vec<Value> = (0..5)
            .map(
                |i| json!({"op":"acquire","name":format!("s{i}"),"ttlSeconds":1,"limit":MAX_LIMIT}),
            )
            .collect();
        assert_eq!(reason(Value::Array(wide)), "locks_too_many_keys");
    }

    #[test]
    fn a_slot_key_reads_back_and_a_neighbour_is_never_mistaken_for_one() {
        assert_eq!(slot_key("gpu", 0), "gpu#0");
        assert_eq!(slot_of("gpu", "gpu#0"), Some(0));
        assert_eq!(slot_of("gpu", "gpu#1023"), Some(1023));
        for foreign in [
            "gpu", "gpu#", "gpu#x", "gpu#07", "gpu#+7", "gpu#-1", "gpu#1024", "gpus#1", "gp#1",
            "gpu#1#2", "gpu#1 ",
        ] {
            assert_eq!(slot_of("gpu", foreign), None, "{foreign}");
        }
        // A name cannot contain the separator, so `<name>#` is one lock's prefix.
        assert!(name_ok("order/9137:sync") && name_ok("täglich") && !name_ok("a#b"));
    }

    fn view(rows: Value) -> BTreeMap<u32, Held> {
        held_of("gpu", &rows)
    }

    #[test]
    fn a_semaphore_tries_a_free_slot_and_knows_its_own() {
        let rows = json!([
            {"key":"gpu#0","value":{"owner":"a"},"version":11,"expiresAt":"t","updatedAt":"u"},
            {"key":"gpu#2","value":{"owner":"b"},"version":12,"expiresAt":"t","updatedAt":"u"},
            {"key":"gpu#x","value":{"owner":"z"},"version":13},
            {"key":"other#1","value":{"owner":"z"},"version":14},
        ]);
        let held = view(rows);
        assert_eq!(held.keys().copied().collect::<Vec<_>>(), vec![0, 2]);
        // Free slots of 4 are 1 and 3: the pick chooses among them only.
        for p in 0..8u64 {
            match next_of(&held, 4, Some("c"), p) {
                Next::Try(s) => assert!(s == 1 || s == 3, "{s}"),
                other => panic!("{other:?}"),
            }
        }
        assert_eq!(next_of(&held, 4, None, 0), Next::Try(1));
        assert_eq!(next_of(&held, 4, None, 1), Next::Try(3));
        // The owner that already holds one is answered that one.
        assert_eq!(
            next_of(&held, 4, Some("b"), 0),
            Next::Already { slot: 2, token: 12 }
        );
        // A smaller limit sees fewer slots: full at 1, and b's slot is out of it.
        assert_eq!(next_of(&held, 1, Some("b"), 0), Next::Full);
        let all = view(json!([
            {"key":"gpu#0","value":{"owner":"a"},"version":1},
            {"key":"gpu#1","value":"written by hand","version":2},
        ]));
        assert_eq!(next_of(&all, 2, Some("c"), 5), Next::Full);
        assert_eq!(
            holders_brief(&all, 2),
            json!([{"slot":0,"owner":"a"},{"slot":1,"owner":null}]),
            "a row with no owner holds its slot all the same"
        );
    }

    fn row(owner: Value, version: u64, since: Option<&str>) -> Value {
        let mut value = json!({ "owner": owner });
        if let Some(s) = since {
            value["since"] = json!(s);
        }
        json!({"found":true,"key":"job#0","value":value,"version":version,
               "expiresAt":"2026-10-08T10:00:30.000000Z","updatedAt":"2026-10-08T10:00:00.000000Z"})
    }

    /// The whole decision of a renew, from the row it read.
    #[test]
    fn a_renew_rewrites_its_own_row_and_nobody_elses() {
        let acquired = "2026-10-08T10:00:00.000000Z";
        // The caller's token: the row keeps its value, and gains `since`, the
        // time of the acquire's write, at its first renewal.
        assert_eq!(
            renewal_of(&row(json!("a"), 7, None), 0, 7, Some("a")),
            Renewal::Put {
                value: json!({"owner":"a","since":acquired}),
                expect: 7,
            }
        );
        // Every later renewal carries the same `since`, whatever `updatedAt` says.
        let earlier = "2026-10-08T04:00:00.000000Z";
        assert_eq!(
            renewal_of(&row(json!("a"), 9, Some(earlier)), 0, 9, None),
            Renewal::Put {
                value: json!({"owner":"a","since":earlier}),
                expect: 9,
            }
        );
        // A stale token whose row is this owner's is carried through at the
        // version the row has now.
        assert_eq!(
            renewal_of(&row(json!("a"), 9, Some(earlier)), 0, 7, Some("a")),
            Renewal::Put {
                value: json!({"owner":"a","since":earlier}),
                expect: 9,
            }
        );
        // A stale token without an owner, or with another's, is lost, and is
        // told who holds the permit.
        let theirs = json!([{"slot":0,"owner":"a"}]);
        for caller in [None, Some("b")] {
            assert_eq!(
                renewal_of(&row(json!("a"), 9, None), 0, 7, caller),
                Renewal::Lost(theirs.clone()),
                "{caller:?}"
            );
        }
        // The token rules whoever the caller says it is: the row's owner stays.
        assert_eq!(
            renewal_of(&row(json!("a"), 9, None), 0, 9, Some("b")),
            Renewal::Put {
                value: json!({"owner":"a","since":acquired}),
                expect: 9,
            }
        );
        // No owner on either side is nobody, twice: not the same holder.
        assert_eq!(
            renewal_of(&row(Value::Null, 9, None), 0, 7, None),
            Renewal::Lost(json!([{"slot":0,"owner":null}]))
        );
        // Nothing there: expired or released.
        for gone in [json!({"found":false,"key":"job#0"}), Value::Null] {
            assert_eq!(renewal_of(&gone, 0, 7, Some("a")), Renewal::Lost(json!([])));
        }
        // A row written by hand is renewed as it is.
        let hand = json!({"found":true,"value":"by hand","version":4,"updatedAt":"u"});
        assert_eq!(
            renewal_of(&hand, 0, 4, None),
            Renewal::Put {
                value: json!("by hand"),
                expect: 4,
            }
        );
    }

    #[test]
    fn since_is_the_acquire_and_a_get_reports_it() {
        let rows = json!([
            {"key":"gpu#0","value":{"owner":"a"},"version":11,"expiresAt":"e","updatedAt":"u0"},
            {"key":"gpu#1","value":{"owner":"b","since":"s1"},"version":12,"expiresAt":"e",
             "updatedAt":"u1"},
            {"key":"gpu#2","value":{"owner":"c","since":7},"version":13,"expiresAt":"e",
             "updatedAt":"u2"},
        ]);
        let held = view(rows);
        assert_eq!(held[&0].since, json!("u0"), "never renewed: its one write");
        assert_eq!(held[&1].since, json!("s1"), "renewed: what the renew kept");
        assert_eq!(
            held[&2].since,
            json!("u2"),
            "a since that is no time is not one"
        );
        assert_eq!(
            got(3, "gpu", &held)["holders"][1],
            json!({"slot":1,"owner":"b","token":12,"since":"s1","expiresAt":"e","renewedAt":"u1"})
        );
    }

    #[test]
    fn the_guard_is_the_kv_check_of_the_permits_row() {
        assert_eq!(
            guard("daily-report", 0, 90101),
            json!({"op":"check","ns":"queen-locks","key":"daily-report#0","expect":90101,
                   "required":true})
        );
        // It is a valid op of the KV wire and of a transaction's `kv` array.
        for in_wire in [false, true] {
            let parsed = crate::rsm::planner::kv::parse_ops(
                &[guard("daily-report", 3, 7)],
                "t",
                in_wire,
                511,
                65_536,
            )
            .expect("the guard parses");
            assert_eq!(parsed[0].name(), "check");
        }
    }

    #[test]
    fn the_footprint_counts_what_an_acquire_may_add() {
        let call = ops(json!([
            {"op":"acquire","name":"a","ttlSeconds":1,"owner":"w"},
            {"op":"acquire","name":"b","ttlSeconds":1},
            {"op":"renew","name":"c","token":1,"ttlSeconds":1},
            {"op":"release","name":"d","token":1},
            {"op":"get","name":"e"},
        ]))
        .unwrap();
        let (rows, bytes) = footprint(&call);
        assert_eq!(rows, 2);
        assert_eq!(
            bytes as usize,
            r#"{"owner":"w"}"#.len() + r#"{"owner":null}"#.len()
        );
    }
}
