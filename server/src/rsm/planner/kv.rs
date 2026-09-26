//! KV, implemented on the RSM (PLAN_RAFT.md WP-2.2), including the expiry
//! sweep step.
//!
//! # The shape of a KV call in the RSM
//!
//! Validation and apply happen inside ONE transaction, in two passes split
//! along the line D3 draws:
//!
//! 1. **Pass 1 — validation** is pure and runs at the RECEIVER ([`parse_ops`]):
//!    the op taxonomy, the namespace charset, the key and value ceilings, the
//!    mandatory expiry, one write per key per call, the op and key budgets. A
//!    refused call never reaches the planner, so it never writes.
//! 2. **Pass 2 — the writes** are planned here, serially, against committed
//!    state plus the [`Overlay`], in a fixed apply order (phase, namespace,
//!    key, ordinal; byte order, `COLLATE "C"`). The planner is the one serial
//!    point every write passes, so it plays the role a row lock would:
//!    exactly one of N concurrent `putIfAbsent`s wins, an `expect` is judged
//!    against the version the previous writer left, and an `incr` never
//!    loses an update. A verdict carries the loser's current value and
//!    version WITH it, because the planner saw it; a lost `required`
//!    precondition aborts the whole call with nothing logged.
//! 3. **Reads are payload** and never ride in an outcome (D7): get, getMany and
//!    getPrefix are evaluated by the RECEIVER against its own applied state
//!    once the call's entry (if any) has applied ([`KvOpOutcome::Deferred`]),
//!    which sees the call's own writes in the same apply order as pass 2.
//!    The one read that must NOT see them — a `get` of a key the SAME call
//!    writes later in apply order — is answered at plan time
//!    ([`KvOpOutcome::Got`]). A call with no write never enters the planner.
//!
//! # Time (D5, I2) and versions (I18)
//!
//! One instant per call: the planner's stamp. Every expiry, `created_at` and
//! `updated_at` an effect carries is computed from it, so apply has no clock.
//! Liveness is one rule — `expires IS NULL OR expires > now` — the same
//! boundary for readers and for the sweep ([`KvRow::live`]).
//!
//! Versions are `kv_version_base + ordinal` (I18): unique across the store and
//! never re-issued — that uniqueness is the only guarantee. They are also
//! monotone as a side effect of how they are assigned, but nothing may rely
//! on that.
//!
//! # The transaction reuse seam
//!
//! [`Planner::plan_kv_writes`] plans a list of ops into effects and per-op
//! verdicts WITHOUT folding them into the overlay, so the transaction wire
//! (WP-2.1) can plan its KV leg first, and fold the whole command's effects
//! once it has decided to log it.
//!
//! # Design choices worth flagging
//!
//! - **The key ceiling is the store's, when it is tighter.** LMDB keys are at
//!   most 511 bytes (R-108) and the store key is `(tenant, ns, key)`, so a key
//!   is refused `kv_key_too_large` (413) once `tenant + ns + key` would not
//!   fit — which can bind before the nominal 512-byte ceiling when the tenant
//!   and the namespace are long. A key with a NUL byte is a 400 `kv_bad_key`
//!   here rather than a protocol error.
//! - **Bytes are measured on the compact JSON**, not on a padded text form
//!   (one that spaces its separators): the value ceiling and the 4 MiB read
//!   budget are only ever a little more generous that way, never tighter.
//! - **incr arithmetic** is exact decimal while the numbers fit an `i128`
//!   mantissa with at most 38 fractional digits ([`KvNum`]) — every counter
//!   there is — and falls back to `f64` beyond, where NUMERIC would stay exact.

use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value};

use crate::rsm::effect::Effect;
use crate::rsm::entry::{
    KvGot, KvOpOutcome, KvOutcome, KvPrecondition, KvReason, KvWrite, Outcome, RequestId,
};
use crate::rsm::store::rows::KvRow;
use crate::rsm::store::{keys, Reads, TypedReads};

use super::{store_err, Overlay, Plan, Planned, Planner, Refusal};

// ---------------------------------------------------------------------------
// The ceilings, defined once here so the receiver, the planner
// and the read evaluation cannot drift from each other.
// ---------------------------------------------------------------------------

/// `C_MAX_VALUE_BYTES`: the value ceiling, on the compact JSON.
pub const MAX_VALUE_BYTES: usize = 65_536;
/// `C_MAX_READ_BYTES`: the aggregate read budget of one call (4 MiB).
pub const MAX_READ_BYTES: i64 = 4_194_304;
/// `C_PREFIX_DEFAULT` / `C_PREFIX_CAP`: getPrefix's limit, clamped, never refused.
pub const PREFIX_DEFAULT: usize = 100;
pub const PREFIX_CAP: usize = 1000;
/// `C_MAX_OPS_WIRE` / `C_MAX_OPS_HTTP`.
pub const MAX_OPS_WIRE: usize = 64;
pub const MAX_OPS_HTTP: usize = 256;
/// `C_MAX_KEYS_WIRE` / `C_MAX_KEYS_HTTP`: the SUM of the keys every op names.
pub const MAX_KEYS_WIRE: usize = 256;
pub const MAX_KEYS_HTTP: usize = 4096;
/// The name check's key ceiling, in BYTES.
pub const MAX_KEY_BYTES: usize = 512;
/// `C_DETAIL_CAP`: a lost-precondition DETAIL is cut at this many characters.
pub const DETAIL_CAP: usize = 4096;
/// The most rows one leader sweep step deletes.
pub const SWEEP_LIMIT_DEFAULT: usize = 512;

const SEC_US: i64 = 1_000_000;

// ---------------------------------------------------------------------------
// Numbers (exact decimal, for incr)
// ---------------------------------------------------------------------------

/// A number `incr` computes with: exact decimal arithmetic and the shortest
/// rendering (no trailing fractional zero); [`KvNum::Dec`] is exactly
/// that while the value fits an `i128` mantissa with at most 38 fractional
/// digits. Beyond it the arithmetic falls back to `f64` ([`KvNum::Float`]).
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq)]
pub enum KvNum {
    /// `mantissa / 10^scale`, normalized (no trailing fractional zero).
    Dec(i128, u32),
    Float(f64),
}

const MAX_SCALE: u32 = 38;

impl KvNum {
    pub const ZERO: KvNum = KvNum::Dec(0, 0);

    /// Parse a JSON number's text (`-12.5e3`, `0.1`, `7`).
    pub fn parse(text: &str) -> Option<KvNum> {
        Self::parse_dec(text).or_else(|| {
            text.parse::<f64>()
                .ok()
                .filter(|f| f.is_finite())
                .map(KvNum::Float)
        })
    }

    fn parse_dec(text: &str) -> Option<KvNum> {
        let (body, exp) = match text.find(['e', 'E']) {
            Some(at) => (&text[..at], text[at + 1..].parse::<i64>().ok()?),
            None => (text, 0),
        };
        let (neg, body) = match body.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, body.strip_prefix('+').unwrap_or(body)),
        };
        let (int_part, frac_part) = match body.find('.') {
            Some(at) => (&body[..at], &body[at + 1..]),
            None => (body, ""),
        };
        if int_part.is_empty() && frac_part.is_empty() {
            return None;
        }
        let mut m: i128 = 0;
        for c in int_part.bytes().chain(frac_part.bytes()) {
            if !c.is_ascii_digit() {
                return None;
            }
            m = m.checked_mul(10)?.checked_add((c - b'0') as i128)?;
        }
        let mut scale = (frac_part.len() as i64).checked_sub(exp)?;
        if scale < 0 {
            let up = u32::try_from(-scale).ok()?;
            m = m.checked_mul(10i128.checked_pow(up)?)?;
            scale = 0;
        }
        // Trailing fractional zeros never matter; trim before the scale test so
        // `1.000…0` (50 zeros) is still exact.
        let mut scale = scale as u64;
        while scale > 0 && m % 10 == 0 {
            m /= 10;
            scale -= 1;
        }
        if scale > MAX_SCALE as u64 {
            return None;
        }
        Some(KvNum::Dec(if neg { -m } else { m }, scale as u32).normalized())
    }

    /// Read a stored JSON value as a number for a LIVE row: the
    /// number when it is a JSON number, `None` when it is not one.
    pub fn of_json(bytes: &[u8]) -> Option<KvNum> {
        match serde_json::from_slice::<Value>(bytes).ok()? {
            Value::Number(n) => KvNum::parse(&n.to_string()),
            _ => None,
        }
    }

    fn normalized(self) -> KvNum {
        match self {
            KvNum::Dec(mut m, mut s) => {
                while s > 0 && m % 10 == 0 {
                    m /= 10;
                    s -= 1;
                }
                KvNum::Dec(m, s)
            }
            f => f,
        }
    }

    fn to_f64(self) -> f64 {
        match self {
            KvNum::Dec(m, s) => m as f64 / 10f64.powi(s as i32),
            KvNum::Float(f) => f,
        }
    }

    /// Both mantissas at one scale, or `None` when that overflows.
    fn aligned(a: KvNum, b: KvNum) -> Option<(i128, i128, u32)> {
        match (a, b) {
            (KvNum::Dec(ma, sa), KvNum::Dec(mb, sb)) => {
                let s = sa.max(sb);
                let ma = ma.checked_mul(10i128.checked_pow(s - sa)?)?;
                let mb = mb.checked_mul(10i128.checked_pow(s - sb)?)?;
                Some((ma, mb, s))
            }
            _ => None,
        }
    }

    pub fn add(self, other: KvNum) -> KvNum {
        if let Some((a, b, s)) = Self::aligned(self, other) {
            if let Some(sum) = a.checked_add(b) {
                return KvNum::Dec(sum, s).normalized();
            }
        }
        KvNum::Float(self.to_f64() + other.to_f64())
    }

    pub fn cmp_num(self, other: KvNum) -> std::cmp::Ordering {
        match Self::aligned(self, other) {
            Some((a, b, _)) => a.cmp(&b),
            None => self
                .to_f64()
                .partial_cmp(&other.to_f64())
                .unwrap_or(std::cmp::Ordering::Equal),
        }
    }

    /// `to_jsonb(trim_scale(n))::text`: the shortest exact rendering.
    pub fn render(self) -> String {
        match self.normalized() {
            KvNum::Dec(m, 0) => m.to_string(),
            KvNum::Dec(m, s) => {
                let p = 10i128.pow(s);
                let (int, frac) = (m.unsigned_abs() / p as u128, m.unsigned_abs() % p as u128);
                let sign = if m < 0 { "-" } else { "" };
                format!("{sign}{int}.{frac:0width$}", width = s as usize)
            }
            KvNum::Float(f) => serde_json::Number::from_f64(f)
                .map(|n| n.to_string())
                .unwrap_or_else(|| "0".to_string()),
        }
    }
}

// ---------------------------------------------------------------------------
// Ops
// ---------------------------------------------------------------------------

/// The one expiry declaration every write carries (§5.1): EXACTLY ONE of
/// `ttlSeconds` (an integer > 0) and `forever: true`.
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvExpiry {
    TtlSeconds(i64),
    Forever,
}

impl KvExpiry {
    /// The absolute expiry at `now_us` (`v_now + make_interval(secs => ttl)`).
    pub fn at(self, now_us: i64) -> Option<i64> {
        match self {
            KvExpiry::Forever => None,
            KvExpiry::TtlSeconds(s) => Some(now_us.saturating_add(s.saturating_mul(SEC_US))),
        }
    }
}

/// One validated op of a KV call (seven wire names, five code paths:
/// `putIfAbsent` is `Put` with `if_absent`, which desugars to `expect: 0`).
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub enum KvOp {
    Get {
        ns: String,
        key: String,
    },
    GetMany {
        ns: String,
        keys: Vec<String>,
    },
    GetPrefix {
        ns: String,
        prefix: String,
        /// The EXCLUSIVE keyset cursor; `None` for none.
        after: Option<String>,
        /// Clamped to `1..=PREFIX_CAP`, defaulting to `PREFIX_DEFAULT`.
        limit: usize,
        keys_only: bool,
    },
    Put {
        ns: String,
        key: String,
        /// The value JSON, compact.
        #[serde(with = "serde_bytes")]
        value: Vec<u8>,
        expiry: KvExpiry,
        expect: Option<u64>,
        required: bool,
        /// The op was spelled `putIfAbsent` (its answer echoes that name).
        if_absent: bool,
    },
    Delete {
        ns: String,
        key: String,
        expect: Option<u64>,
        required: bool,
    },
    Incr {
        ns: String,
        key: String,
        delta: KvNum,
        min: Option<KvNum>,
        max: Option<KvNum>,
        expiry: KvExpiry,
        required: bool,
    },
}

impl KvOp {
    /// The wire name, which is what the answer's `op` echoes.
    pub fn name(&self) -> &'static str {
        match self {
            KvOp::Get { .. } => "get",
            KvOp::GetMany { .. } => "getMany",
            KvOp::GetPrefix { .. } => "getPrefix",
            KvOp::Put {
                if_absent: true, ..
            } => "putIfAbsent",
            KvOp::Put { .. } => "put",
            KvOp::Delete { .. } => "delete",
            KvOp::Incr { .. } => "incr",
        }
    }

    pub fn is_write(&self) -> bool {
        matches!(
            self,
            KvOp::Put { .. } | KvOp::Delete { .. } | KvOp::Incr { .. }
        )
    }

    pub fn ns(&self) -> &str {
        match self {
            KvOp::Get { ns, .. }
            | KvOp::GetMany { ns, .. }
            | KvOp::GetPrefix { ns, .. }
            | KvOp::Put { ns, .. }
            | KvOp::Delete { ns, .. }
            | KvOp::Incr { ns, .. } => ns,
        }
    }

    /// The one key a single-key op names.
    pub fn key(&self) -> Option<&str> {
        match self {
            KvOp::Get { key, .. }
            | KvOp::Put { key, .. }
            | KvOp::Delete { key, .. }
            | KvOp::Incr { key, .. } => Some(key),
            _ => None,
        }
    }

    fn required(&self) -> bool {
        match self {
            KvOp::Put { required, .. }
            | KvOp::Delete { required, .. }
            | KvOp::Incr { required, .. } => *required,
            _ => false,
        }
    }

    /// The sort key before the ordinal: `(phase, ns, key | prefix)`,
    /// phase 1 being the multi-key reads, which therefore run LAST and see the
    /// call's own writes.
    fn sort_key(&self) -> (u8, &[u8], &[u8]) {
        let phase = u8::from(matches!(
            self,
            KvOp::GetMany { .. } | KvOp::GetPrefix { .. }
        ));
        let second: &str = match self {
            KvOp::GetPrefix { prefix, .. } => prefix,
            KvOp::GetMany { .. } => "",
            other => other.key().unwrap_or(""),
        };
        (phase, self.ns().as_bytes(), second.as_bytes())
    }
}

/// The op indexes of a call in pass-2 APPLY order: `(phase, ns, key |
/// prefix)` in byte order, the input ordinal breaking ties. The planner decides
/// writes in this order and the receiver spends the read budget in it.
pub fn apply_order(ops: &[KvOp]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..ops.len()).collect();
    order.sort_by(|&a, &b| ops[a].sort_key().cmp(&ops[b].sort_key()).then(a.cmp(&b)));
    order
}

/// A KV call for the planner: the receiver has validated every op
/// ([`parse_ops`]) and it carries at least one write (a read-only call never
/// enters the planner).
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct KvCommand {
    pub request_id: RequestId,
    pub tenant: String,
    pub ops: Vec<KvOp>,
}

// ---------------------------------------------------------------------------
// Pass 1: validation
// ---------------------------------------------------------------------------

/// A refused call: `status` is the mapped HTTP code (400 or 413), `reason`
/// is a stable MESSAGE (`kv_bad_namespace`, `kv_expiry_not_specified`, …) —
/// the only part a
/// client may branch on — and `detail` the human half, which names only what
/// the caller itself sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvInvalid {
    pub status: u16,
    pub reason: &'static str,
    pub detail: String,
}

fn bad(reason: &'static str, detail: impl Into<String>) -> KvInvalid {
    KvInvalid {
        status: 400,
        reason,
        detail: detail.into(),
    }
}

fn too_large(reason: &'static str, detail: impl Into<String>) -> KvInvalid {
    KvInvalid {
        status: 413,
        reason,
        detail: detail.into(),
    }
}

/// `v_op->>'field'`: a string's content, any other JSON value's text, `None`
/// for JSON null.
fn text_of(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// A single-quoted literal (quotes doubled), or `NULL` for `None`.
fn lit(s: Option<&str>) -> String {
    match s {
        Some(s) => format!("'{}'", s.replace('\'', "''")),
        None => "NULL".to_string(),
    }
}

/// `^[a-z0-9][a-z0-9._-]{0,63}$`: lowercase because case is the first of the
/// typo classes, and a namespace is registered nowhere, so a typo would mint a
/// phantom namespace that reads empty forever.
pub fn namespace_ok(ns: &str) -> bool {
    let b = ns.as_bytes();
    !b.is_empty()
        && b.len() <= 64
        && (b[0].is_ascii_lowercase() || b[0].is_ascii_digit())
        && b[1..].iter().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, b'.' | b'_' | b'-')
        })
}

/// The namespace check alone — what the list operation checks its
/// namespace with.
pub fn check_namespace(ns: Option<&str>) -> Result<(), KvInvalid> {
    match ns {
        Some(n) if namespace_ok(n) => Ok(()),
        _ => Err(bad(
            "kv_bad_namespace",
            format!(
                "namespace {} does not match ^[a-z0-9][a-z0-9._-]{{0,63}}$",
                lit(ns)
            ),
        )),
    }
}

/// Validates the namespace and key together, plus the store's own key
/// ceiling (see the module header).
fn check_names(
    tenant: &str,
    ns: Option<&str>,
    key: Option<&str>,
    max_store_key: usize,
) -> Result<(), KvInvalid> {
    check_namespace(ns)?;
    let ns_str = ns.unwrap_or_default();
    let key = match key {
        Some(k) if !k.is_empty() => k,
        _ => return Err(bad("kv_bad_key", "the key must be non-empty")),
    };
    if key.contains('\0') {
        return Err(bad("kv_bad_key", "the key must not contain NUL"));
    }
    if key.len() > MAX_KEY_BYTES {
        return Err(too_large(
            "kv_key_too_large",
            format!("key is {} bytes, the ceiling is {MAX_KEY_BYTES}", key.len()),
        ));
    }
    let store_len = keys::kv_len(tenant, ns_str, key);
    if store_len > max_store_key {
        let room = max_store_key.saturating_sub(store_len - key.len());
        return Err(too_large(
            "kv_key_too_large",
            format!(
                "key is {} bytes; with this tenant and namespace the raft storage class \
                 holds keys up to {room} bytes",
                key.len()
            ),
        ));
    }
    Ok(())
}

/// A JSON number that is an integer: its value, or `None`.
fn int_of(v: &Value) -> Option<i128> {
    let n = v.as_number()?;
    if let Some(i) = n.as_i64() {
        return Some(i as i128);
    }
    if let Some(u) = n.as_u64() {
        return Some(u as i128);
    }
    let f = n.as_f64()?;
    (f.is_finite() && f.fract() == 0.0 && f.abs() < 1e38).then_some(f as i128)
}

/// Validate and type one call's ops: pass 1, in its order (each op in
/// input order, then one write per key, then the key budget). `in_wire`
/// marks the transaction wire, which forbids getPrefix and has the
/// smaller budgets. `max_store_key` is the store's key ceiling
/// ([`Reads::max_key_len`]).
pub fn parse_ops(
    ops: &[Value],
    tenant: &str,
    in_wire: bool,
    max_store_key: usize,
) -> Result<Vec<KvOp>, KvInvalid> {
    let n = ops.len();
    if n == 0 {
        return Ok(Vec::new());
    }
    let max_ops = if in_wire { MAX_OPS_WIRE } else { MAX_OPS_HTTP };
    if n > max_ops {
        return Err(bad(
            "kv_too_many_ops",
            format!("{n} ops in one call, the ceiling is {max_ops}"),
        ));
    }
    let mut out = Vec::with_capacity(n);
    for (i, op) in ops.iter().enumerate() {
        out.push(parse_one(i, op, tenant, in_wire, max_store_key)?);
    }

    // §6.1 point 3, LOAD-BEARING here (a key's
    // verdict is decided once per call): at most one WRITE per key.
    let mut seen: HashSet<(&str, &str)> = HashSet::new();
    for op in &out {
        if let (true, Some(k)) = (op.is_write(), op.key()) {
            if !seen.insert((op.ns(), k)) {
                return Err(bad(
                    "kv_duplicate_key_in_call",
                    "a key may be written at most once per kv_apply_v1 call; it is what makes \
                     the intra-space lock order total",
                ));
            }
        }
    }

    // §6.1 point 4: the SUM of the keys every op names; getPrefix counts as
    // its clamped limit.
    let total: usize = out
        .iter()
        .map(|op| match op {
            KvOp::GetMany { keys, .. } => keys.len(),
            KvOp::GetPrefix { limit, .. } => *limit,
            _ => 1,
        })
        .sum();
    let max_keys = if in_wire {
        MAX_KEYS_WIRE
    } else {
        MAX_KEYS_HTTP
    };
    if total > max_keys {
        return Err(bad(
            "kv_too_many_keys",
            format!("{total} keys in one call, the ceiling is {max_keys}"),
        ));
    }
    Ok(out)
}

fn parse_one(
    i: usize,
    v: &Value,
    tenant: &str,
    in_wire: bool,
    max_store_key: usize,
) -> Result<KvOp, KvInvalid> {
    let Some(o) = v.as_object() else {
        return Err(bad(
            "kv_bad_request",
            format!("op at index {i} is not an object"),
        ));
    };
    let kind = text_of(o.get("op"));
    let kind = match kind.as_deref() {
        Some(k @ ("get" | "getMany" | "getPrefix" | "put" | "putIfAbsent" | "delete" | "incr")) => {
            k.to_string()
        }
        other => {
            return Err(bad(
                "kv_unknown_op",
                format!("op at index {i}: unknown operation {}", lit(other)),
            ))
        }
    };
    // §6.1 point 6: the tenant is an argument, never a field of an op.
    if o.contains_key("tenant") || o.contains_key("tenantId") || o.contains_key("_tenant") {
        return Err(bad(
            "kv_tenant_not_an_input",
            format!(
                "op at index {i} carries a tenant field; the tenant is an argument of kv_apply_v1"
            ),
        ));
    }
    let ns = text_of(o.get("ns"));

    // ---- names
    match kind.as_str() {
        "getPrefix" => {
            if in_wire {
                return Err(bad(
                    "kv_get_prefix_not_allowed_in_transaction",
                    format!("op at index {i}: getPrefix is only available on POST /api/v1/kv"),
                ));
            }
            let prefix = text_of(o.get("prefix"));
            if prefix.as_deref().is_none_or(str::is_empty) {
                return Err(bad(
                    "kv_prefix_required",
                    format!("op at index {i}: getPrefix needs a non-empty prefix"),
                ));
            }
            check_names(tenant, ns.as_deref(), prefix.as_deref(), max_store_key)?;
        }
        "getMany" => match o.get("keys") {
            None => {}
            Some(Value::Array(a)) => {
                for k in a {
                    check_names(
                        tenant,
                        ns.as_deref(),
                        text_of(Some(k)).as_deref(),
                        max_store_key,
                    )?;
                }
            }
            Some(_) => {
                return Err(bad(
                    "kv_bad_request",
                    format!("op at index {i}: getMany needs a keys array"),
                ))
            }
        },
        _ => check_names(
            tenant,
            ns.as_deref(),
            text_of(o.get("key")).as_deref(),
            max_store_key,
        )?,
    }
    let ns = ns.unwrap_or_default();

    // ---- the mandatory expiry of every write (§5.1)
    let mut expiry: Option<KvExpiry> = None;
    if matches!(kind.as_str(), "put" | "putIfAbsent" | "incr") {
        let mut decls = 0;
        if let Some(t) = o.get("ttlSeconds") {
            decls += 1;
            let ttl_bad = || {
                bad(
                    "kv_bad_ttl",
                    format!("op at index {i}: ttlSeconds must be an integer greater than zero"),
                )
            };
            let n = int_of(t).ok_or_else(ttl_bad)?;
            if n <= 0 {
                return Err(ttl_bad());
            }
            expiry = Some(KvExpiry::TtlSeconds(n.min(i64::MAX as i128) as i64));
        }
        if o.get("forever") == Some(&Value::Bool(true)) {
            decls += 1;
            expiry = Some(KvExpiry::Forever);
        }
        if decls != 1 {
            return Err(bad(
                "kv_expiry_not_specified",
                format!(
                    "op at index {i}: exactly one of ttlSeconds (integer > 0) and forever:true is \
                     required, got {decls}"
                ),
            ));
        }
    }

    // ---- the value of a put
    let mut value: Vec<u8> = Vec::new();
    if matches!(kind.as_str(), "put" | "putIfAbsent") {
        let Some(v) = o.get("value") else {
            return Err(bad(
                "kv_bad_request",
                format!(
                    "op at index {i}: {kind} needs a value (\"value\": null is legal, an absent \
                     value is not)"
                ),
            ));
        };
        value = serde_json::to_vec(v).unwrap_or_else(|_| b"null".to_vec());
        if value.len() > MAX_VALUE_BYTES {
            return Err(too_large(
                "kv_value_too_large",
                format!(
                    "op at index {i}: value is {} bytes, the ceiling is {MAX_VALUE_BYTES}",
                    value.len()
                ),
            ));
        }
    }

    // ---- expect (§5.3): an explicit null is a client bug, not a downgrade
    let mut expect: Option<u64> = None;
    if matches!(kind.as_str(), "put" | "putIfAbsent" | "delete") {
        if let Some(e) = o.get("expect") {
            let n = int_of(e).filter(|n| *n >= 0).ok_or_else(|| {
                bad(
                    "kv_bad_expect",
                    format!(
                        "op at index {i}: expect must be a non-negative integer version (0 = must \
                         not exist)"
                    ),
                )
            })?;
            if kind == "putIfAbsent" && n != 0 {
                return Err(bad(
                    "kv_bad_expect",
                    format!(
                        "op at index {i}: putIfAbsent desugars to put with expect:0; a different \
                         expect is a contradiction"
                    ),
                ));
            }
            expect = Some(n.min(u64::MAX as i128) as u64);
        }
    }

    // ---- incr (§5.4): no expect, a numeric delta, numeric bounds
    let num = |field: &str| -> Result<Option<KvNum>, KvInvalid> {
        match o.get(field) {
            None => Ok(None),
            Some(Value::Number(n)) => KvNum::parse(&n.to_string()).map(Some).ok_or_else(|| {
                bad(
                    "kv_bad_request",
                    format!("op at index {i}: {field} must be numeric"),
                )
            }),
            Some(_) => Err(bad(
                "kv_bad_request",
                format!("op at index {i}: {field} must be numeric"),
            )),
        }
    };
    let mut delta = KvNum::ZERO;
    let (mut min, mut max) = (None, None);
    if kind == "incr" {
        if o.contains_key("expect") {
            return Err(bad(
                "kv_bad_request",
                format!("op at index {i}: incr takes no expect"),
            ));
        }
        delta = match o.get("delta") {
            Some(Value::Number(_)) => num("delta")?.unwrap_or(KvNum::ZERO),
            _ => {
                return Err(bad(
                    "kv_bad_request",
                    format!("op at index {i}: incr needs a numeric delta"),
                ))
            }
        };
        min = num("min")?;
        max = num("max")?;
    }

    // ---- required
    let required = match o.get("required") {
        None => false,
        Some(Value::Bool(b)) => *b,
        Some(_) => {
            return Err(bad(
                "kv_bad_request",
                format!("op at index {i}: required must be a boolean"),
            ))
        }
    };

    let key = text_of(o.get("key")).unwrap_or_default();
    Ok(match kind.as_str() {
        "get" => KvOp::Get { ns, key },
        "getMany" => KvOp::GetMany {
            ns,
            keys: match o.get("keys") {
                Some(Value::Array(a)) => a
                    .iter()
                    .map(|k| text_of(Some(k)).unwrap_or_default())
                    .collect(),
                _ => Vec::new(),
            },
        },
        "getPrefix" => {
            let limit = match o.get("limit") {
                None | Some(Value::Null) => PREFIX_DEFAULT,
                Some(l) => {
                    let n = int_of(l).ok_or_else(|| {
                        bad(
                            "kv_bad_request",
                            format!("op at index {i}: limit must be an integer"),
                        )
                    })?;
                    n.clamp(1, PREFIX_CAP as i128) as usize
                }
            };
            let keys_only = match o.get("keysOnly") {
                None | Some(Value::Null) => false,
                Some(Value::Bool(b)) => *b,
                Some(_) => {
                    return Err(bad(
                        "kv_bad_request",
                        format!("op at index {i}: keysOnly must be a boolean"),
                    ))
                }
            };
            KvOp::GetPrefix {
                ns,
                prefix: text_of(o.get("prefix")).unwrap_or_default(),
                after: text_of(o.get("after")).filter(|a| !a.is_empty()),
                limit,
                keys_only,
            }
        }
        "put" | "putIfAbsent" => KvOp::Put {
            ns,
            key,
            value,
            expiry: expiry.unwrap_or(KvExpiry::Forever),
            expect,
            required,
            if_absent: kind == "putIfAbsent",
        },
        "delete" => KvOp::Delete {
            ns,
            key,
            expect,
            required,
        },
        _ => KvOp::Incr {
            ns,
            key,
            delta,
            min,
            max,
            expiry: expiry.unwrap_or(KvExpiry::Forever),
            required,
        },
    })
}

// ---------------------------------------------------------------------------
// Pass 2: the planner
// ---------------------------------------------------------------------------

/// What [`Planner::plan_kv_writes`] decided for one KV call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvPlan {
    /// In apply order. The n-th `KvPut` carries the overlay's next KV
    /// version plus n (I18). EMPTY when `failed` is set: the call aborted.
    pub effects: Vec<Effect>,
    /// Index-aligned with the ops; EMPTY when `failed` is set.
    pub results: Vec<KvOpOutcome>,
    /// A write that lost its precondition with `required: true`.
    pub failed: Option<KvPrecondition>,
}

/// One write verdict under construction.
enum Verdict {
    /// Applied: the effect to log (if any) and the answer.
    Applied(Option<Effect>, KvWrite),
    /// Not applied: the answer, and an effect that still has to happen (a
    /// plain delete naming an EXPIRED row still physically removes it, and
    /// answers `absent`).
    Lost(Option<Effect>, KvWrite),
}

fn got_of(row: &KvRow) -> KvGot {
    KvGot {
        value: row.value.clone(),
        version: row.version,
        expires_at_us: row.expires_at_us,
        updated_at_us: row.updated_at_us,
    }
}

impl Overlay {
    /// The overlay's own view of one KV row: `Some(Some)` written in flight,
    /// `Some(None)` deleted in flight, `None` untouched (read committed).
    fn kv_row(&self, tenant: &str, ns: &str, key: &str) -> Option<Option<KvRow>> {
        self.kv
            .get(&(tenant.to_string(), ns.to_string(), key.to_string()))
            .map(|t| t.v.clone())
    }
}

impl<'a, R: Reads + ?Sized> Planner<'a, R> {
    /// The merged row — overlay first (a write or a delete still in flight),
    /// then committed state. Expired or not: liveness is the caller's.
    pub(crate) fn kv_row(
        &self,
        ov: &Overlay,
        tenant: &str,
        ns: &str,
        key: &str,
    ) -> Result<Option<KvRow>, Refusal> {
        if let Some(v) = ov.kv_row(tenant, ns, key) {
            return Ok(v);
        }
        // A purge in flight deletes every row of the tenant when it applies.
        if ov.tenant_purged(tenant) {
            return Ok(None);
        }
        self.reads().kv(tenant, ns, key).map_err(store_err)
    }

    /// THE TRANSACTION REUSE SEAM (WP-2.1): plan a list of already-validated
    /// KV ops ([`parse_ops`]) for `tenant` into effects and per-op verdicts,
    /// against committed state plus `ov`, at this planner's `now_us`.
    ///
    /// It does NOT fold anything into the overlay: the caller folds
    /// [`KvPlan::effects`] (with [`Overlay::apply_effects`], or by logging
    /// them through a `plan_*` that does) when — and only when — it logs them.
    /// The versions it assigns start at the overlay's next KV version, so a
    /// caller that plans KV twice for one command must fold in between.
    ///
    /// Reads are NOT evaluated here except the one case that is ordered before a
    /// write of the same key ([`KvOpOutcome::Got`]); every other read is
    /// [`KvOpOutcome::Deferred`] to the receiver. A `required` write that loses
    /// its precondition aborts the call: [`KvPlan::failed`], no effects.
    pub fn plan_kv_writes(
        &self,
        ov: &Overlay,
        tenant: &str,
        ops: &[KvOp],
    ) -> Result<KvPlan, Refusal> {
        let now = self.now_us;
        let max_key = self.reads().max_key_len();
        // The ordinal of the one write per (ns, key) (pass 1 guaranteed one).
        let writes: HashMap<(&str, &str), usize> = ops
            .iter()
            .enumerate()
            .filter(|(_, op)| op.is_write())
            .filter_map(|(i, op)| Some(((op.ns(), op.key()?), i)))
            .collect();

        let mut results: Vec<KvOpOutcome> = vec![KvOpOutcome::Deferred; ops.len()];
        let mut effects: Vec<Effect> = Vec::new();
        let mut next_version = ov.next_kv_version;

        for i in apply_order(ops) {
            let op = &ops[i];
            // Belt to the receiver's brace: a key the store cannot hold must
            // never reach apply, where `KeyTooLong` would stop the node.
            if let Some(k) = op.key() {
                if keys::kv_len(tenant, op.ns(), k) > max_key {
                    return Err(Refusal::client(
                        "kv_key_too_large",
                        format!("op at index {i}: key is too long for the store"),
                    ));
                }
            }
            let verdict = match op {
                KvOp::Get { ns, key } => {
                    // This must be read BEFORE the later write of the same key; a
                    // read after apply could not see it, so fix the answer now.
                    if writes
                        .get(&(ns.as_str(), key.as_str()))
                        .is_some_and(|&w| w > i)
                    {
                        let row = self.kv_row(ov, tenant, ns, key)?;
                        results[i] =
                            KvOpOutcome::Got(row.filter(|r| r.live(now)).map(|r| got_of(&r)));
                    }
                    continue;
                }
                KvOp::GetMany { .. } | KvOp::GetPrefix { .. } => continue,
                KvOp::Put {
                    ns,
                    key,
                    value,
                    expiry,
                    expect,
                    if_absent,
                    ..
                } => {
                    let cur = self.kv_row(ov, tenant, ns, key)?;
                    let expect = if *if_absent { Some(0) } else { *expect };
                    self.put_verdict(
                        tenant,
                        ns,
                        key,
                        value,
                        *expiry,
                        expect,
                        cur,
                        &mut next_version,
                    )
                }
                KvOp::Delete {
                    ns, key, expect, ..
                } => {
                    let cur = self.kv_row(ov, tenant, ns, key)?;
                    delete_verdict(tenant, ns, key, *expect, cur, now)
                }
                KvOp::Incr {
                    ns,
                    key,
                    delta,
                    min,
                    max,
                    expiry,
                    ..
                } => {
                    let cur = self.kv_row(ov, tenant, ns, key)?;
                    self.incr_verdict(
                        tenant,
                        ns,
                        key,
                        *delta,
                        *min,
                        *max,
                        *expiry,
                        cur,
                        &mut next_version,
                    )
                }
            };
            match verdict {
                Verdict::Applied(eff, w) => {
                    effects.extend(eff);
                    results[i] = KvOpOutcome::Write(w);
                }
                Verdict::Lost(eff, w) => {
                    // §6.1 point 5: escalation is opt-in PER ELEMENT, and it
                    // aborts the whole call.
                    if op.required() {
                        return Ok(KvPlan {
                            effects: Vec::new(),
                            results: Vec::new(),
                            failed: Some(KvPrecondition {
                                index: i as u32,
                                reason: w.reason.unwrap_or(KvReason::Absent),
                                version: w.version,
                                value: w.value,
                            }),
                        });
                    }
                    effects.extend(eff);
                    results[i] = KvOpOutcome::Write(w);
                }
            }
        }
        Ok(KvPlan {
            effects,
            results,
            failed: None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn put_verdict(
        &self,
        tenant: &str,
        ns: &str,
        key: &str,
        value: &[u8],
        expiry: KvExpiry,
        expect: Option<u64>,
        cur: Option<KvRow>,
        next_version: &mut u64,
    ) -> Verdict {
        let now = self.now_us;
        let live = cur.as_ref().filter(|r| r.live(now));
        // `Ok(created_at)` to write, `Err(reason)` to lose.
        let decided: Result<i64, KvReason> = match expect {
            // Unconditional upsert: replaces value AND expiry, never inherits
            // the previous expiry (§5.1). A live row keeps its birthday; an
            // expired one overwritten is a new lineage.
            None => Ok(live.map(|r| r.created_at_us).unwrap_or(now)),
            // "Must not exist", and it WINS against an expired-but-unpruned
            // row (the resurrection of §5.3/§5.7): a new lineage.
            Some(0) => match live {
                Some(_) => Err(KvReason::Exists),
                None => Ok(now),
            },
            // expect: N > 0 — a pure UPDATE: it never creates (§5.3, the repair
            // that matters most) and it keeps `created_at`.
            Some(n) => match live {
                Some(r) if r.version == n => Ok(r.created_at_us),
                Some(_) => Err(KvReason::Version),
                None => Err(KvReason::Absent),
            },
        };
        match decided {
            Ok(created_at_us) => {
                let version = *next_version;
                *next_version += 1;
                Verdict::Applied(
                    Some(Effect::KvPut {
                        tenant: tenant.to_string(),
                        ns: ns.to_string(),
                        key: key.to_string(),
                        value: value.to_vec(),
                        version,
                        expires_at_us: expiry.at(now),
                        created_at_us,
                        updated_at_us: now,
                    }),
                    KvWrite {
                        applied: true,
                        reason: None,
                        value: None, // the op's own value
                        version,
                    },
                )
            }
            Err(reason) => Verdict::Lost(
                None,
                KvWrite {
                    applied: false,
                    reason: Some(reason),
                    // The loser sees what a reader would (§5.7): nothing for an
                    // expired row, and 0 for its version.
                    value: live.map(|r| r.value.clone()),
                    version: live.map(|r| r.version).unwrap_or(0),
                },
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn incr_verdict(
        &self,
        tenant: &str,
        ns: &str,
        key: &str,
        delta: KvNum,
        min: Option<KvNum>,
        max: Option<KvNum>,
        expiry: KvExpiry,
        cur: Option<KvRow>,
        next_version: &mut u64,
    ) -> Verdict {
        use std::cmp::Ordering::{Greater, Less};
        let now = self.now_us;
        let within = |v: KvNum| {
            min.is_none_or(|m| v.cmp_num(m) != Less) && max.is_none_or(|m| v.cmp_num(m) != Greater)
        };
        let live = cur.as_ref().filter(|r| r.live(now));
        // `kv_num_v1`: the number of a LIVE numeric row, else zero.
        let cur_num = live.and_then(|r| KvNum::of_json(&r.value));
        let effective = cur_num.unwrap_or(KvNum::ZERO);
        let type_ok = live.is_none() || cur_num.is_some();
        let next = effective.add(delta);

        // REPAIR 2 (§5.4): with no row at all, the create branch is gated by
        // the PURE comparison of delta against min/max; with a row (expired or
        // live), the update branch by the resulting value — the same thing on
        // an effective zero, so one test covers both.
        let ok = type_ok && within(next);
        if ok {
            let version = *next_version;
            *next_version += 1;
            let rendered = next.render().into_bytes();
            // The TTL is CREATE-ONLY (§5.4): a live row keeps its expiry and
            // its birthday; an expired or absent one starts a new window.
            let (expires_at_us, created_at_us) = match live {
                Some(r) => (r.expires_at_us, r.created_at_us),
                None => (expiry.at(now), now),
            };
            return Verdict::Applied(
                Some(Effect::KvPut {
                    tenant: tenant.to_string(),
                    ns: ns.to_string(),
                    key: key.to_string(),
                    value: rendered.clone(),
                    version,
                    expires_at_us,
                    created_at_us,
                    updated_at_us: now,
                }),
                KvWrite {
                    applied: true,
                    reason: None,
                    value: Some(rendered),
                    version,
                },
            );
        }
        // Refused: `max`/`min` neither saturate nor truncate; the answer is
        // the CURRENT value, never the would-be one — `applied` IS the
        // admission decision (§5.4).
        let reason = if live.is_some() && cur_num.is_none() {
            KvReason::Type
        } else {
            KvReason::Limit
        };
        Verdict::Lost(
            None,
            KvWrite {
                applied: false,
                reason: Some(reason),
                value: Some(effective.render().into_bytes()),
                version: live.map(|r| r.version).unwrap_or(0),
            },
        )
    }

    /// Plan one KV call (`POST /api/v1/kv` and the path routes): the writes
    /// through [`Planner::plan_kv_writes`], folded into the overlay once the
    /// call is logged.
    ///
    /// A call that writes nothing — every write lost a non-required
    /// precondition, or a `required` one aborted it — is [`Plan::Empty`]:
    /// answered from the overlay's view once the entries it read have applied
    /// (§7.2), never logged, its id never recorded (§5.4).
    pub fn plan_kv(&self, ov: &mut Overlay, cmd: &KvCommand) -> Planned {
        let plan = self.plan_kv_writes(ov, &cmd.tenant, &cmd.ops)?;
        if let Some(failed) = plan.failed {
            return Ok(Plan::Empty(Outcome::Kv(KvOutcome {
                results: Vec::new(),
                failed: Some(failed),
            })));
        }
        let outcome = Outcome::Kv(KvOutcome {
            results: plan.results,
            failed: None,
        });
        if plan.effects.is_empty() {
            return Ok(Plan::Empty(outcome));
        }
        // §5.1 413: the planned size of one command.
        let planned: usize = plan
            .effects
            .iter()
            .map(|e| match e {
                Effect::KvPut { value, key, ns, .. } => 96 + value.len() + key.len() + ns.len(),
                _ => 96,
            })
            .sum::<usize>()
            + cmd.tenant.len();
        if planned > self.cfg.entry_max_bytes {
            return Err(Refusal::client(
                "too_large",
                format!(
                    "planned kv call of {planned} B exceeds QUEEN_RAFT_ENTRY_MAX_BYTES ({})",
                    self.cfg.entry_max_bytes
                ),
            ));
        }
        ov.apply_effects(&plan.effects);
        Ok(Plan::logged(plan.effects, outcome))
    }

    /// One bounded step of the leader's expiry sweep: at most `limit`
    /// `KvDelete`s for rows whose expiry
    /// is at or before `now` (the reader's boundary, §5.7), oldest first, read
    /// off the expiry index — O(expired), never a scan of every key.
    ///
    /// A candidate is re-judged against the MERGED row: one that an entry in
    /// flight has rewritten (a new version) or already deleted is skipped, and
    /// its index row is put right when that entry applies. Empty when nothing
    /// is due; the batcher then logs nothing.
    pub fn plan_kv_sweep(&self, ov: &Overlay, limit: usize) -> Result<Vec<Effect>, Refusal> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now = self.now_us;
        let mut due: Vec<(u64, Vec<u8>)> = Vec::new();
        // Read a little past the limit, so a few candidates shadowed by the
        // overlay do not starve the step.
        let cap = limit.saturating_mul(2);
        self.reads()
            .scan_kv_expiry(cap, &mut |at, version, kv_key| {
                if at > now {
                    return false;
                }
                due.push((version, kv_key.to_vec()));
                true
            })
            .map_err(store_err)?;
        let mut effects = Vec::new();
        for (version, kv_key) in due {
            if effects.len() >= limit {
                break;
            }
            let Some((tenant, ns, key)) = keys::kv_parts(&kv_key) else {
                continue;
            };
            match self.kv_row(ov, &tenant, &ns, &key)? {
                Some(row) if row.version == version && !row.live(now) => {
                    effects.push(Effect::KvDelete { tenant, ns, key });
                }
                _ => {}
            }
        }
        Ok(effects)
    }
}

/// The delete verdict (it needs no planner state beyond the row).
fn delete_verdict(
    tenant: &str,
    ns: &str,
    key: &str,
    expect: Option<u64>,
    cur: Option<KvRow>,
    now: i64,
) -> Verdict {
    let del = || Effect::KvDelete {
        tenant: tenant.to_string(),
        ns: ns.to_string(),
        key: key.to_string(),
    };
    let live = cur.as_ref().filter(|r| r.live(now));
    let lost = |reason: KvReason| KvWrite {
        applied: false,
        reason: Some(reason),
        value: live.map(|r| r.value.clone()),
        version: live.map(|r| r.version).unwrap_or(0),
    };
    match expect {
        // A plain delete removes the row physically even when it is expired,
        // but an expired row was never there logically (§5.7): not applied.
        None => match (&cur, live) {
            (_, Some(r)) => Verdict::Applied(
                Some(del()),
                KvWrite {
                    applied: true,
                    reason: None,
                    value: Some(r.value.clone()),
                    version: r.version,
                },
            ),
            (Some(_), None) => Verdict::Lost(Some(del()), lost(KvReason::Absent)),
            (None, None) => Verdict::Lost(None, lost(KvReason::Absent)),
        },
        // "It must not exist": idempotent success when it does not (nothing
        // is written, not even the prune of an expired row), a verdict when it
        // does.
        Some(0) => match live {
            None => Verdict::Applied(
                None,
                KvWrite {
                    applied: true,
                    reason: None,
                    value: None,
                    version: 0,
                },
            ),
            Some(_) => Verdict::Lost(None, lost(KvReason::Exists)),
        },
        Some(n) => match live {
            Some(r) if r.version == n => Verdict::Applied(
                Some(del()),
                KvWrite {
                    applied: true,
                    reason: None,
                    value: Some(r.value.clone()),
                    version: r.version,
                },
            ),
            Some(_) => Verdict::Lost(None, lost(KvReason::Version)),
            None => Verdict::Lost(None, lost(KvReason::Absent)),
        },
    }
}

// ---------------------------------------------------------------------------
// The read side: get / getMany / getPrefix / list, evaluated by the receiver
// against its own applied state (a store read handle), and the answer shapes.
// ---------------------------------------------------------------------------

/// Days since 1970-01-01 → (year, month, day), proleptic Gregorian (Howard
/// Hinnant's `civil_from_days`).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m: i64 = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m as u32, d)
}

fn split_us(us: i64) -> (i64, u32, u32, i64, i64, i64, i64) {
    const US_PER_DAY: i64 = 86_400_000_000;
    let days = us.div_euclid(US_PER_DAY);
    let rem = us.rem_euclid(US_PER_DAY);
    let (y, m, d) = civil_from_days(days);
    let secs = rem / 1_000_000;
    (
        y,
        m,
        d,
        secs / 3600,
        (secs / 60) % 60,
        secs % 60,
        rem % 1_000_000,
    )
}

/// A timestamp rendered `2026-09-22T10:15:30.1234+00:00`, UTC: the
/// fractional seconds trimmed of trailing zeros and absent when zero.
pub fn ts_jsonb(us: i64) -> String {
    let (y, mo, d, h, mi, s, frac) = split_us(us);
    let mut out = format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}");
    if frac != 0 {
        let f = format!("{frac:06}");
        out.push('.');
        out.push_str(f.trim_end_matches('0'));
    }
    out.push_str("+00:00");
    out
}

/// A `timestamptz` the way `kv_list_v1` answers it after converting to UTC:
/// `YYYY-MM-DDTHH:MI:SS.USZ`, always with six fractional digits.
pub fn ts_list(us: i64) -> String {
    let (y, mo, d, h, mi, s, frac) = split_us(us);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{frac:06}Z")
}

/// A stored value, as the JSON the answer embeds.
fn json_value(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).unwrap_or(Value::Null)
}

fn opt_ts(us: Option<i64>, f: fn(i64) -> String) -> Value {
    us.map(|u| Value::String(f(u))).unwrap_or(Value::Null)
}

fn obj(pairs: Vec<(&str, Value)>) -> Value {
    let mut m = Map::new();
    for (k, v) in pairs {
        m.insert(k.to_string(), v);
    }
    Value::Object(m)
}

/// A row as getMany / getPrefix list it.
fn row_json(key: &str, row: &KvRow, with_value: bool) -> Value {
    let mut pairs = vec![("key", Value::String(key.to_string()))];
    if with_value {
        pairs.push(("value", json_value(&row.value)));
    }
    pairs.push(("version", Value::from(row.version)));
    pairs.push(("expiresAt", opt_ts(row.expires_at_us, ts_jsonb)));
    pairs.push(("updatedAt", Value::String(ts_jsonb(row.updated_at_us))));
    obj(pairs)
}

/// The answer element of one write, from its planned verdict (a uniform
/// shape, and incr's own).
pub fn write_json(index: usize, op: &KvOp, w: &KvWrite) -> Value {
    let value = match (&w.value, op) {
        (Some(v), _) => json_value(v),
        (None, KvOp::Put { value, .. }) if w.applied => json_value(value),
        (None, _) => Value::Null,
    };
    let mut pairs = vec![
        ("index", Value::from(index)),
        ("op", Value::String(op.name().to_string())),
        ("applied", Value::Bool(w.applied)),
    ];
    if let (false, Some(r)) = (w.applied, w.reason) {
        pairs.push(("reason", Value::String(r.as_str().to_string())));
    }
    pairs.push(("key", Value::String(op.key().unwrap_or("").to_string())));
    pairs.push(("value", value));
    pairs.push(("version", Value::from(w.version)));
    obj(pairs)
}

/// DETAIL for a lost `required` precondition, cut at [`DETAIL_CAP`]
/// characters: a pathological value then truncates it into invalid JSON,
/// which the HTTP layer degrades to the
/// bare verdict.
pub fn precondition_detail(ops: &[KvOp], f: &KvPrecondition) -> String {
    let op = ops.get(f.index as usize);
    let v = obj(vec![
        ("index", Value::from(f.index)),
        (
            "op",
            Value::String(op.map(|o| o.name()).unwrap_or("").to_string()),
        ),
        (
            "ns",
            Value::String(op.map(|o| o.ns()).unwrap_or("").to_string()),
        ),
        (
            "key",
            Value::String(op.and_then(|o| o.key()).unwrap_or("").to_string()),
        ),
        ("reason", Value::String(f.reason.as_str().to_string())),
        ("version", Value::from(f.version)),
        (
            "value",
            f.value.as_deref().map(json_value).unwrap_or(Value::Null),
        ),
    ]);
    v.to_string().chars().take(DETAIL_CAP).collect()
}

/// Render a whole call's answer, index-aligned (§6.4): the write verdicts
/// and plan-time gets from `pre`, every [`KvOpOutcome::Deferred`] read
/// evaluated NOW against `r` at `now_us`, spending ONE read budget across the
/// call in apply order.
pub fn render_call<R: Reads + ?Sized>(
    r: &R,
    tenant: &str,
    ops: &[KvOp],
    pre: &[KvOpOutcome],
    now_us: i64,
) -> crate::rsm::store::Result<Vec<Value>> {
    let mut out: Vec<Value> = vec![Value::Null; ops.len()];
    let mut read_left: i64 = MAX_READ_BYTES;
    for i in apply_order(ops) {
        let op = &ops[i];
        let pre_i = pre.get(i).unwrap_or(&KvOpOutcome::Deferred);
        out[i] = match (op, pre_i) {
            (_, KvOpOutcome::Write(w)) => write_json(i, op, w),
            (KvOp::Get { ns, key }, pre_i) => {
                let found: Option<KvGot> = match pre_i {
                    KvOpOutcome::Got(g) => g.clone(),
                    _ => r
                        .kv(tenant, ns, key)?
                        .filter(|row| row.live(now_us))
                        .map(|row| got_of(&row)),
                };
                match found {
                    Some(g) => {
                        // A single key is bounded on the way in, so it is always
                        // returned; it still spends from the call's budget.
                        read_left -= g.value.len() as i64;
                        obj(vec![
                            ("index", Value::from(i)),
                            ("op", Value::String("get".into())),
                            ("found", Value::Bool(true)),
                            ("key", Value::String(key.clone())),
                            ("value", json_value(&g.value)),
                            ("version", Value::from(g.version)),
                            ("expiresAt", opt_ts(g.expires_at_us, ts_jsonb)),
                            ("updatedAt", Value::String(ts_jsonb(g.updated_at_us))),
                        ])
                    }
                    // `found` is separate from the value because `null` is a
                    // legal value (§5.5).
                    None => obj(vec![
                        ("index", Value::from(i)),
                        ("op", Value::String("get".into())),
                        ("found", Value::Bool(false)),
                        ("key", Value::String(key.clone())),
                    ]),
                }
            }
            (KvOp::GetMany { ns, keys }, _) => {
                let mut hits: Vec<(String, KvRow)> = Vec::new();
                let mut live_keys: HashSet<String> = HashSet::new();
                for k in keys {
                    if let Some(row) = r.kv(tenant, ns, k)?.filter(|row| row.live(now_us)) {
                        live_keys.insert(k.clone());
                        hits.push((k.clone(), row));
                    }
                }
                hits.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
                let (mut before, mut bytes) = (0i64, 0i64);
                let mut rows = Vec::new();
                for (k, row) in &hits {
                    let blen = row.value.len() as i64;
                    if before < read_left {
                        rows.push(row_json(k, row, true));
                        bytes += blen;
                    }
                    before += blen;
                }
                read_left -= bytes;
                // Absence is a DATUM (§5.5); keys cut by the byte budget are
                // neither in rows nor in missing, and `truncated` says so.
                let missing: Vec<Value> = keys
                    .iter()
                    .filter(|k| !live_keys.contains(*k))
                    .map(|k| Value::String(k.clone()))
                    .collect();
                obj(vec![
                    ("index", Value::from(i)),
                    ("op", Value::String("getMany".into())),
                    ("rows", Value::Array(rows.clone())),
                    ("missing", Value::Array(missing)),
                    ("truncated", Value::Bool(rows.len() < hits.len())),
                ])
            }
            (
                KvOp::GetPrefix {
                    ns,
                    prefix,
                    after,
                    limit,
                    keys_only,
                },
                _,
            ) => {
                let page = page_of(
                    r,
                    tenant,
                    ns,
                    prefix,
                    after.as_deref(),
                    *limit,
                    *keys_only,
                    false,
                    now_us,
                    read_left,
                    ts_jsonb,
                )?;
                read_left -= page.bytes;
                obj(vec![
                    ("index", Value::from(i)),
                    ("op", Value::String("getPrefix".into())),
                    ("rows", Value::Array(page.rows)),
                    ("truncated", Value::Bool(page.truncated)),
                    (
                        "nextAfter",
                        page.next_after.map(Value::String).unwrap_or(Value::Null),
                    ),
                ])
            }
            // A write with no verdict cannot happen (the planner answers every
            // write); render it as not applied rather than panic.
            (op, _) => write_json(
                i,
                op,
                &KvWrite {
                    applied: false,
                    reason: Some(KvReason::Absent),
                    value: None,
                    version: 0,
                },
            ),
        };
    }
    Ok(out)
}

/// One keyset page of a namespace: the getPrefix arm and `kv_list_v1`.
pub struct KvPage {
    pub rows: Vec<Value>,
    pub truncated: bool,
    pub next_after: Option<String>,
    /// The value bytes this page charged against the budget (0 on keysOnly).
    pub bytes: i64,
}

/// The getPrefix / console-list page: live rows only unless `include_expired`
/// (the console's D5, whose rows then carry `expired`), `limit + 1` read so
/// `truncated` needs no second query, and the budget cut after the row that
/// straddles it.
#[allow(clippy::too_many_arguments)]
pub fn page_of<R: Reads + ?Sized>(
    r: &R,
    tenant: &str,
    ns: &str,
    prefix: &str,
    after: Option<&str>,
    limit: usize,
    keys_only: bool,
    include_expired: bool,
    now_us: i64,
    read_left: i64,
    ts: fn(i64) -> String,
) -> crate::rsm::store::Result<KvPage> {
    let limit = limit.clamp(1, PREFIX_CAP);
    let mut page: Vec<(String, KvRow)> = Vec::new();
    r.scan_kv(tenant, ns, prefix, after, usize::MAX, &mut |k, row| {
        if include_expired || row.live(now_us) {
            page.push((k.to_string(), row));
        }
        page.len() <= limit
    })?;
    let total = page.len();
    let (mut before, mut bytes) = (0i64, 0i64);
    let mut rows = Vec::new();
    let mut last: Option<String> = None;
    for (rn, (k, row)) in page.iter().enumerate() {
        let blen = if keys_only { 0 } else { row.value.len() as i64 };
        if rn < limit && before < read_left {
            let mut pairs = vec![("key", Value::String(k.clone()))];
            if !keys_only {
                pairs.push(("value", json_value(&row.value)));
            }
            pairs.push(("version", Value::from(row.version)));
            pairs.push(("expiresAt", opt_ts(row.expires_at_us, ts)));
            pairs.push(("updatedAt", Value::String(ts(row.updated_at_us))));
            if include_expired {
                pairs.push(("expired", Value::Bool(!row.live(now_us))));
            }
            rows.push(obj(pairs));
            bytes += blen;
            last = Some(k.clone());
        }
        before += blen;
    }
    let truncated = rows.len() < total;
    Ok(KvPage {
        rows,
        truncated,
        next_after: if truncated { last } else { None },
        bytes,
    })
}

/// `kv_namespaces_v1`: every namespace of a tenant with its row count —
/// EVERY row, expired ones included (they still occupy the namespace, §2.5
/// D5) — in namespace byte order.
pub fn namespaces_of<R: Reads + ?Sized>(r: &R, tenant: &str) -> crate::rsm::store::Result<Value> {
    let mut out: Vec<(String, u64)> = Vec::new();
    r.scan_kv_tenant(tenant, usize::MAX, &mut |ns, _key, _row| {
        match out.last_mut() {
            Some((last, n)) if last == ns => *n += 1,
            _ => out.push((ns.to_string(), 1)),
        }
        true
    })?;
    Ok(Value::Array(
        out.into_iter()
            .map(|(ns, n)| {
                obj(vec![
                    ("namespace", Value::String(ns)),
                    ("keys", Value::from(n)),
                ])
            })
            .collect(),
    ))
}

#[cfg(test)]
mod num_tests {
    use super::KvNum;

    #[test]
    fn numbers_render_like_trim_scale() {
        let p = |s: &str| KvNum::parse(s).unwrap();
        assert_eq!(p("3").add(p("1")).render(), "4");
        assert_eq!(p("2.50").render(), "2.5");
        assert_eq!(p("0.1").add(p("0.2")).render(), "0.3");
        assert_eq!(p("-0.5").add(p("0.25")).render(), "-0.25");
        assert_eq!(p("1e3").render(), "1000");
        assert_eq!(p("1.5e-3").render(), "0.0015");
        assert_eq!(p("10.0").render(), "10");
        assert_eq!(p("-0").render(), "0");
        assert_eq!(p("5").cmp_num(p("5.0")), std::cmp::Ordering::Equal);
        assert_eq!(p("4.99").cmp_num(p("5")), std::cmp::Ordering::Less);
        // Beyond an i128 decimal: the f64 fallback, still a number.
        assert!(matches!(p("1e300"), KvNum::Float(_)));
        assert!(
            serde_json::from_str::<serde_json::Value>(&p("1e300").add(p("1")).render())
                .unwrap()
                .is_number()
        );
        assert_eq!(KvNum::of_json(b"{\"a\":1}"), None);
        assert_eq!(KvNum::of_json(b"7"), Some(p("7")));
    }
}
