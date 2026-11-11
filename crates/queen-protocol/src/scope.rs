//! Client grants: the names a scoped credential may touch.
//!
//! A scoped API key holds a list of [`Grant`]s: its role. A client token
//! minted from that key (`POST /api/v1/tokens`) adds the entity, its `sub`,
//! and may carry grants of its own that narrow the key's. Neither can be
//! wider than the key: the proxy turns them into one [`Scope`] per request,
//! the key's grants with `{sub}` filled in, then the token's, each a LAYER,
//! and a request passes only when every layer allows it.
//!
//! The broker checks the scope where it resolves names, because only it can:
//! an ack names its partition by number, a binary push names its partitions
//! inside groups the proxy does not open, a pop without a partition lets the
//! broker pick. The places are a push's queue and partition, a pop's queue,
//! partition and consumer group, an ack's partition row, a KV op's namespace
//! and key, a timer's queue, key and partition.
//!
//! Four rules, each a way a grant could leak more than it names:
//!
//! * **Unknown fields are refused**, in a grant and in the scope on the wire:
//!   a misspelt `partiton` must not leave the partition open.
//! * **`{sub}` is filled in from a closed alphabet** ([`is_sub_char`]): no
//!   `*`, `/`, `:`, `{` or `}`, so a sub cannot turn a name into a wildcard.
//! * **A `*` after `{sub}` needs a separator first** (`{sub}/*`, not
//!   `{sub}*`): otherwise `dev-1`'s prefix would also cover `dev-12`.
//! * **A pattern with `{sub}` matches nothing without a sub**: a key used
//!   directly, with no token, gets only its grants that name no entity.

use serde::de::{self, Deserializer};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};

/// The header the proxy hands each scoped request to the broker in. It is
/// the proxy's own: the proxy drops any copy a client sends.
pub const SCOPE_HEADER: &str = "x-queen-scope";

/// The only scope version this crate writes and reads. A broker that meets
/// another refuses the request rather than guess what it allows.
pub const SCOPE_VERSION: u32 = 1;

/// The most grants a key or a token may hold.
pub const MAX_GRANTS: usize = 64;

/// The longest pattern a grant field may hold, in bytes.
pub const MAX_PATTERN_BYTES: usize = 256;

/// The longest `sub`, in bytes.
pub const SUB_MAX_BYTES: usize = 128;

/// The placeholder a pattern names the token's entity with.
pub const SUB_PLACEHOLDER: &str = "{sub}";

/// Can a `sub` hold `c`? ASCII letters and digits, and `.` `_` `-` `@` `+`:
/// enough for ids, UUIDs and e-mail addresses, nothing a pattern reads as
/// syntax and nothing used as a separator after `{sub}`.
pub fn is_sub_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '@' | '+')
}

/// Is `s` a sub a token may carry? 1 to [`SUB_MAX_BYTES`] bytes of
/// [`is_sub_char`].
pub fn valid_sub(s: &str) -> bool {
    !s.is_empty() && s.len() <= SUB_MAX_BYTES && s.chars().all(is_sub_char)
}

/// What a grant lets its holder do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GrantOp {
    /// Queues: push. On the push route and in a transaction.
    Produce,
    /// Queues: pop, ack, renew the lease. On the routes and in a transaction.
    Consume,
    /// KV and timers: read.
    Read,
    /// KV and timers: write (put, delete, incr; schedule, cancel).
    Write,
}

impl GrantOp {
    pub fn as_str(self) -> &'static str {
        match self {
            GrantOp::Produce => "produce",
            GrantOp::Consume => "consume",
            GrantOp::Read => "read",
            GrantOp::Write => "write",
        }
    }
}

/// Which surface a grant names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GrantKind {
    Queue,
    Kv,
    Timers,
}

impl GrantKind {
    pub fn as_str(self) -> &'static str {
        match self {
            GrantKind::Queue => "queue",
            GrantKind::Kv => "kv",
            GrantKind::Timers => "timers",
        }
    }

    /// The operations a grant of this kind may name.
    fn ops(self) -> &'static [GrantOp] {
        match self {
            GrantKind::Queue => &[GrantOp::Produce, GrantOp::Consume],
            GrantKind::Kv | GrantKind::Timers => &[GrantOp::Read, GrantOp::Write],
        }
    }
}

/// One line of a key's role, as it is stored, sent to the control plane and
/// shown in the console. Exactly one of `queue`, `kv` and `timers` names the
/// surface; the other fields narrow it, and an absent one is `*`.
///
/// | Kind | Fields | Ops |
/// |---|---|---|
/// | `queue` | `partition`, `group` | `produce`, `consume` |
/// | `kv` (a namespace) | `key` | `read`, `write` |
/// | `timers` (a queue) | `key` (the timer key), `partition` (where it fires) | `read`, `write` |
///
/// Every field is a pattern: a name, `prefix*`, or `*`, and may hold
/// `{sub}` once (see the module rules).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kv: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timers: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    pub ops: Vec<GrantOp>,
}

impl Grant {
    /// The surface this grant names and its name pattern, or why it names
    /// none or more than one.
    pub fn kind(&self) -> Result<(GrantKind, &str), String> {
        match (&self.queue, &self.kv, &self.timers) {
            (Some(q), None, None) => Ok((GrantKind::Queue, q)),
            (None, Some(k), None) => Ok((GrantKind::Kv, k)),
            (None, None, Some(t)) => Ok((GrantKind::Timers, t)),
            (None, None, None) => Err("a grant names `queue`, `kv` or `timers`".into()),
            _ => Err("a grant names exactly one of `queue`, `kv` and `timers`".into()),
        }
    }

    /// Is this grant well formed? The error names the field and the rule.
    pub fn check(&self) -> Result<(), String> {
        self.templates().map(|_| ())
    }

    fn templates(&self) -> Result<Templates, String> {
        let (kind, name) = self.kind()?;
        let field = |f: &'static str,
                     v: &Option<String>,
                     allowed: bool|
         -> Result<Template, String> {
            match v {
                None => Ok(Template::Any),
                Some(_) if !allowed => Err(format!("a {} grant has no `{f}` field", kind.as_str())),
                Some(p) => Template::parse(f, p),
            }
        };
        let t = Templates {
            kind,
            name: Template::parse(kind.as_str(), name)?,
            partition: field("partition", &self.partition, kind != GrantKind::Kv)?,
            group: field("group", &self.group, kind == GrantKind::Queue)?,
            key: field("key", &self.key, kind != GrantKind::Queue)?,
        };
        if self.ops.is_empty() {
            return Err(format!(
                "a {} grant needs `ops`: {}",
                kind.as_str(),
                ops_list(kind)
            ));
        }
        for op in &self.ops {
            if !kind.ops().contains(op) {
                return Err(format!(
                    "a {} grant takes the ops {}, not `{}`",
                    kind.as_str(),
                    ops_list(kind),
                    op.as_str()
                ));
            }
        }
        Ok(t)
    }

    /// Does any field name `{sub}`?
    pub fn names_sub(&self) -> bool {
        [
            &self.queue,
            &self.kv,
            &self.timers,
            &self.partition,
            &self.group,
            &self.key,
        ]
        .into_iter()
        .flatten()
        .any(|p| p.contains(SUB_PLACEHOLDER))
    }

    /// This grant for the entity `sub`, as the broker matches it. `None` when
    /// it names `{sub}` and there is no sub (a key used directly), or when it
    /// is not well formed: such a grant allows nothing.
    pub fn expand(&self, sub: Option<&str>) -> Option<ScopeGrant> {
        let t = self.templates().ok()?;
        if sub.is_some_and(|s| !valid_sub(s)) {
            return None;
        }
        let mut ops = self.ops.clone();
        ops.sort_by_key(|o| *o as u8);
        ops.dedup();
        Some(ScopeGrant {
            kind: t.kind,
            name: t.name.expand(sub)?,
            partition: t.partition.expand(sub)?,
            group: t.group.expand(sub)?,
            key: t.key.expand(sub)?,
            ops,
        })
    }
}

fn ops_list(kind: GrantKind) -> String {
    kind.ops()
        .iter()
        .map(|o| format!("`{}`", o.as_str()))
        .collect::<Vec<_>>()
        .join(" and ")
}

/// Is every grant well formed, and are there at most [`MAX_GRANTS`]? The
/// error names the grant by its index.
pub fn check_grants(grants: &[Grant]) -> Result<(), String> {
    if grants.len() > MAX_GRANTS {
        return Err(format!("{} grants, at most {MAX_GRANTS}", grants.len()));
    }
    for (i, g) in grants.iter().enumerate() {
        g.check().map_err(|e| format!("grants[{i}]: {e}"))?;
    }
    Ok(())
}

/// `grants` for the entity `sub`: one layer of a [`Scope`]. A grant that
/// cannot be filled in is left out, and allows nothing.
pub fn layer(grants: &[Grant], sub: Option<&str>) -> Vec<ScopeGrant> {
    grants.iter().filter_map(|g| g.expand(sub)).collect()
}

struct Templates {
    kind: GrantKind,
    name: Template,
    partition: Template,
    group: Template,
    key: Template,
}

/// A grant field before `{sub}` is filled in.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Template {
    Any,
    Exact(String),
    Prefix(String),
    /// `before{sub}after`, exactly.
    SubExact {
        before: String,
        after: String,
    },
    /// `before{sub}after*`, where `after` starts with a character no sub
    /// holds, so one entity's prefix never covers another's names.
    SubPrefix {
        before: String,
        after: String,
    },
}

impl Template {
    fn parse(field: &'static str, s: &str) -> Result<Template, String> {
        if s.is_empty() {
            return Err(format!("`{field}` is empty: use a name, `prefix*` or `*`"));
        }
        if s.len() > MAX_PATTERN_BYTES {
            return Err(format!(
                "`{field}` is {} bytes, at most {MAX_PATTERN_BYTES}",
                s.len()
            ));
        }
        if s == "*" {
            return Ok(Template::Any);
        }
        let (body, prefix) = match s.strip_suffix('*') {
            Some(b) => (b, true),
            None => (s, false),
        };
        if body.contains('*') {
            return Err(format!("`{field}` `{s}`: a `*` may only end a pattern"));
        }
        let Some(at) = body.find(SUB_PLACEHOLDER) else {
            if body.contains(['{', '}']) {
                return Err(format!(
                    "`{field}` `{s}`: `{{` and `}}` appear only as `{{sub}}`"
                ));
            }
            return Ok(if prefix {
                Template::Prefix(body.to_string())
            } else {
                Template::Exact(body.to_string())
            });
        };
        let before = &body[..at];
        let after = &body[at + SUB_PLACEHOLDER.len()..];
        if before.contains(['{', '}']) || after.contains(['{', '}']) {
            return Err(format!(
                "`{field}` `{s}`: `{{sub}}` may appear once, and `{{` and `}}` nowhere else"
            ));
        }
        if !prefix {
            return Ok(Template::SubExact {
                before: before.to_string(),
                after: after.to_string(),
            });
        }
        match after.chars().next() {
            Some(c) if !is_sub_char(c) => Ok(Template::SubPrefix {
                before: before.to_string(),
                after: after.to_string(),
            }),
            _ => Err(format!(
                "`{field}` `{s}`: a `*` after `{{sub}}` would let one entity reach another's \
                 names (`dev-1*` covers `dev-12`): put a separator such as `/` or `:` between \
                 them, as in `{{sub}}/*`"
            )),
        }
    }

    fn expand(&self, sub: Option<&str>) -> Option<Pat> {
        Some(match self {
            Template::Any => Pat::Any,
            Template::Exact(s) => Pat::Exact(s.clone()),
            Template::Prefix(s) => Pat::Prefix(s.clone()),
            Template::SubExact { before, after } => Pat::Exact(format!("{before}{}{after}", sub?)),
            Template::SubPrefix { before, after } => {
                Pat::Prefix(format!("{before}{}{after}", sub?))
            }
        })
    }
}

/// A filled-in pattern. On the wire a string: `*`, `=` and the name, or `^`
/// and the prefix, so no name is ever read as syntax.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Pat {
    Any,
    Exact(String),
    Prefix(String),
}

impl Pat {
    pub fn is_any(&self) -> bool {
        matches!(self, Pat::Any)
    }

    pub fn matches(&self, name: &str) -> bool {
        match self {
            Pat::Any => true,
            Pat::Exact(s) => s == name,
            Pat::Prefix(p) => name.starts_with(p.as_str()),
        }
    }

    /// Does this pattern cover every name `keys` can reach?
    pub fn covers(&self, keys: Keys<'_>) -> bool {
        match keys {
            Keys::One(k) => self.matches(k),
            Keys::Prefix(q) => match self {
                Pat::Any => true,
                Pat::Exact(_) => false,
                Pat::Prefix(p) => q.starts_with(p.as_str()),
            },
            Keys::All => self.is_any(),
        }
    }

    fn render(&self) -> String {
        match self {
            Pat::Any => "*".to_string(),
            Pat::Exact(s) => format!("={s}"),
            Pat::Prefix(p) => format!("^{p}"),
        }
    }
}

impl Serialize for Pat {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.render())
    }
}

impl<'de> Deserialize<'de> for Pat {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        if s == "*" {
            return Ok(Pat::Any);
        }
        match s.split_at_checked(1) {
            Some(("=", rest)) => Ok(Pat::Exact(rest.to_string())),
            Some(("^", rest)) => Ok(Pat::Prefix(rest.to_string())),
            _ => Err(de::Error::custom(format!(
                "a scope pattern is `*`, `=name` or `^prefix`, not `{s}`"
            ))),
        }
    }
}

/// The names a key read or a timer read can reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keys<'a> {
    One(&'a str),
    /// Every name that starts with this (a `getPrefix`, a timer count).
    Prefix(&'a str),
    /// Every name (a timer list).
    All,
}

/// One grant as the broker matches it: [`Grant::expand`]'s answer. Every
/// field is on the wire, `*` where it does not apply: a scope that lost a
/// field on its way refuses to decode rather than leave it open.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeGrant {
    pub kind: GrantKind,
    pub name: Pat,
    pub partition: Pat,
    pub group: Pat,
    pub key: Pat,
    pub ops: Vec<GrantOp>,
}

impl ScopeGrant {
    fn has(&self, op: GrantOp) -> bool {
        self.ops.contains(&op)
    }

    pub fn allows(&self, ask: &Ask<'_>) -> bool {
        match (self.kind, ask) {
            (GrantKind::Queue, Ask::Produce { queue, partition }) => {
                self.has(GrantOp::Produce)
                    && self.name.matches(queue)
                    && self.partition.matches(partition)
            }
            (
                GrantKind::Queue,
                Ask::Consume {
                    queue,
                    partition,
                    group,
                },
            ) => {
                self.has(GrantOp::Consume)
                    && self.name.matches(queue)
                    && self.group.matches(group)
                    && match partition {
                        Some(p) => self.partition.matches(p),
                        // The broker picks the partition: only a grant on all
                        // of them covers every one it can pick.
                        None => self.partition.is_any(),
                    }
            }
            (GrantKind::Kv, Ask::Kv { ns, key, write }) => {
                self.has(if *write {
                    GrantOp::Write
                } else {
                    GrantOp::Read
                }) && self.name.matches(ns)
                    && self.key.covers(*key)
            }
            (
                GrantKind::Timers,
                Ask::Timer {
                    queue,
                    key,
                    partition,
                    write,
                },
            ) => {
                self.has(if *write {
                    GrantOp::Write
                } else {
                    GrantOp::Read
                }) && self.name.matches(queue)
                    && self.key.covers(*key)
                    && partition.is_none_or(|p| self.partition.matches(p))
            }
            _ => false,
        }
    }
}

/// What a scoped request asks to touch, as the broker resolved it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ask<'a> {
    Produce {
        queue: &'a str,
        partition: &'a str,
    },
    /// `partition: None` is a pop that lets the broker pick the partition.
    /// `group` is the consumer group, [`crate::QUEUE_MODE_GROUP`] for none.
    Consume {
        queue: &'a str,
        partition: Option<&'a str>,
        group: &'a str,
    },
    Kv {
        ns: &'a str,
        key: Keys<'a>,
        write: bool,
    },
    /// `partition`: where a schedule fires; `None` for an op that names no
    /// partition (a cancel, a read), which the key alone decides.
    Timer {
        queue: &'a str,
        key: Keys<'a>,
        partition: Option<&'a str>,
        write: bool,
    },
}

impl Ask<'_> {
    /// The ask in a few words, for a refusal's message.
    pub fn describe(&self) -> String {
        match self {
            Ask::Produce { queue, partition } => {
                format!("push to queue `{queue}` partition `{partition}`")
            }
            Ask::Consume {
                queue,
                partition: Some(p),
                group,
            } => format!("consume queue `{queue}` partition `{p}` as group `{group}`"),
            Ask::Consume {
                queue,
                partition: None,
                group,
            } => format!("consume any partition of queue `{queue}` as group `{group}`"),
            Ask::Kv { ns, key, write } => format!(
                "{} KV namespace `{ns}` {}",
                if *write { "write" } else { "read" },
                describe_keys(*key)
            ),
            Ask::Timer {
                queue,
                key,
                partition,
                write,
            } => {
                let mut s = format!(
                    "{} timers of queue `{queue}` {}",
                    if *write { "write" } else { "read" },
                    describe_keys(*key)
                );
                if let Some(p) = partition {
                    s.push_str(&format!(" firing into partition `{p}`"));
                }
                s
            }
        }
    }
}

fn describe_keys(k: Keys<'_>) -> String {
    match k {
        Keys::One(k) => format!("key `{k}`"),
        Keys::Prefix(p) => format!("keys starting `{p}`"),
        Keys::All => "every key".to_string(),
    }
}

/// What one scoped request may touch: layers of grants, each of which must
/// allow it (the key's role, then the token's own narrowing, if any).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub v: u32,
    pub layers: Vec<Vec<ScopeGrant>>,
}

impl Scope {
    pub fn new(layers: Vec<Vec<ScopeGrant>>) -> Scope {
        Scope {
            v: SCOPE_VERSION,
            layers,
        }
    }

    /// Does every layer allow `ask`? A scope with no layer allows nothing.
    pub fn allows(&self, ask: &Ask<'_>) -> bool {
        !self.layers.is_empty()
            && self
                .layers
                .iter()
                .all(|layer| layer.iter().any(|g| g.allows(ask)))
    }

    /// The header value.
    pub fn encode(&self) -> String {
        serde_json::to_string(self).expect("a scope always serializes")
    }

    /// A header value back into a scope, or why it is not one. A version
    /// this crate does not know is refused, never read as the nearest one.
    pub fn decode(raw: &[u8]) -> Result<Scope, String> {
        let s: Scope = serde_json::from_slice(raw).map_err(|e| format!("bad scope: {e}"))?;
        if s.v != SCOPE_VERSION {
            return Err(format!(
                "scope version {} is not {SCOPE_VERSION}: a node of another release sent it",
                s.v
            ));
        }
        Ok(s)
    }
}

/// The routes a scoped request may reach, exactly as the broker router spells
/// them: push, the two queue pops, ack, lease renewal, transaction, the KV
/// batch and path routes, the timer routes. Each of them checks the names it
/// touches. Anything else, a near-miss spelling included, is refused, by the
/// proxy before the request travels and by the broker again when it arrives.
/// `method` is the HTTP method, upper case.
pub fn scoped_route(method: &str, path: &str) -> bool {
    let get = method == "GET" || method == "HEAD";
    let post = method == "POST";
    let Some(rest) = path.strip_prefix("/api/v1/") else {
        return false;
    };
    let segs: Vec<&str> = rest.split('/').collect();
    let named = |s: &[&str]| !s.is_empty() && s.iter().all(|x| !x.is_empty());
    match segs.as_slice() {
        ["push"] | ["transaction"] | ["ack"] | ["ack", "batch"] | ["kv"] | ["timers"] => post,
        ["lease", id, "extend"] => post && !id.is_empty(),
        ["pop", "queue", q] => get && !q.is_empty(),
        ["pop", "queue", q, "partition", p] => get && !q.is_empty() && !p.is_empty(),
        ["kv", ns, key @ ..] => {
            (get || method == "PUT" || method == "DELETE") && !ns.is_empty() && named(key)
        }
        ["timers", q] => get && !q.is_empty(),
        ["timers", q, key @ ..] => (get || method == "DELETE") && !q.is_empty() && named(key),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Client tokens (`POST /api/v1/tokens`).
// ---------------------------------------------------------------------------

/// `POST /api/v1/tokens`: a token for one entity, minted with a scoped key
/// (for that key) or with an `admin` key (for the key named in `key`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MintTokenRequest {
    /// The entity, e.g. a device id: [`valid_sub`].
    pub sub: String,
    /// The token's lifetime in seconds. Default 3600, at most the key's own
    /// ceiling.
    #[serde(
        default,
        rename = "ttlSeconds",
        skip_serializing_if = "Option::is_none"
    )]
    pub ttl_seconds: Option<u64>,
    /// The key the token belongs to, by id or name: only when an `admin` key
    /// mints. A scoped key mints for itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Grants that narrow the key's for this token. They never widen it: a
    /// request must pass the key's grants and these.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow: Option<Vec<Grant>>,
}

impl MintTokenRequest {
    /// A token for `sub`, with the key's default lifetime.
    pub fn new(sub: impl Into<String>) -> Self {
        MintTokenRequest {
            sub: sub.into(),
            ttl_seconds: None,
            key: None,
            allow: None,
        }
    }

    pub fn ttl_seconds(mut self, ttl: u64) -> Self {
        self.ttl_seconds = Some(ttl);
        self
    }

    /// The scoped key to mint for, by id or name: only with an `admin` key.
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Grants that narrow the key's for this token.
    pub fn allow(mut self, grants: Vec<Grant>) -> Self {
        self.allow = Some(grants);
        self
    }
}

/// The answer to [`MintTokenRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MintTokenResponse {
    /// The bearer credential.
    pub token: String,
    pub sub: String,
    /// The id of the key the token belongs to.
    pub key: String,
    /// When the token stops working, RFC 3339.
    #[serde(rename = "expiresAt")]
    pub expires_at: String,
    /// Its lifetime in seconds, from now.
    #[serde(rename = "expiresIn")]
    pub expires_in: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grant(json: serde_json::Value) -> Grant {
        serde_json::from_value(json).expect("grant")
    }

    fn scope_of(grants: &[Grant], sub: Option<&str>) -> Scope {
        Scope::new(vec![layer(grants, sub)])
    }

    fn produce<'a>(queue: &'a str, partition: &'a str) -> Ask<'a> {
        Ask::Produce { queue, partition }
    }

    fn consume<'a>(queue: &'a str, partition: Option<&'a str>, group: &'a str) -> Ask<'a> {
        Ask::Consume {
            queue,
            partition,
            group,
        }
    }

    #[test]
    fn a_device_reaches_its_own_partitions_and_no_other() {
        let grants = [
            grant(
                serde_json::json!({"queue": "telemetry", "partition": "{sub}", "ops": ["produce"]}),
            ),
            grant(
                serde_json::json!({"queue": "commands", "partition": "{sub}", "ops": ["consume"]}),
            ),
        ];
        let s = scope_of(&grants, Some("device-8f3a"));
        assert!(s.allows(&produce("telemetry", "device-8f3a")));
        assert!(!s.allows(&produce("telemetry", "device-8f3b")));
        assert!(
            !s.allows(&produce("commands", "device-8f3a")),
            "consume only"
        );
        assert!(s.allows(&consume("commands", Some("device-8f3a"), "__QUEUE_MODE__")));
        assert!(s.allows(&consume("commands", Some("device-8f3a"), "any-group")));
        assert!(!s.allows(&consume("commands", Some("device-8f3b"), "__QUEUE_MODE__")));
        assert!(
            !s.allows(&consume("commands", None, "__QUEUE_MODE__")),
            "a pop that lets the broker pick needs every partition"
        );
        assert!(!s.allows(&consume("telemetry", Some("device-8f3a"), "g")));
    }

    #[test]
    fn a_grant_on_every_partition_covers_a_pop_that_lets_the_broker_pick() {
        let grants = [grant(
            serde_json::json!({"queue": "jobs", "group": "workers", "ops": ["consume"]}),
        )];
        let s = scope_of(&grants, None);
        assert!(s.allows(&consume("jobs", None, "workers")));
        assert!(s.allows(&consume("jobs", Some("p7"), "workers")));
        assert!(
            !s.allows(&consume("jobs", None, "__QUEUE_MODE__")),
            "group named"
        );
        assert!(!s.allows(&consume("jobs", None, "workers-2")));
    }

    #[test]
    fn a_key_used_directly_gets_only_the_grants_that_name_no_entity() {
        let grants = [
            grant(
                serde_json::json!({"queue": "telemetry", "partition": "{sub}", "ops": ["produce"]}),
            ),
            grant(serde_json::json!({"queue": "audit", "ops": ["produce"]})),
        ];
        let s = scope_of(&grants, None);
        assert_eq!(s.layers[0].len(), 1);
        assert!(s.allows(&produce("audit", "x")));
        assert!(!s.allows(&produce("telemetry", "{sub}")));
        assert!(!s.allows(&produce("telemetry", "")));
    }

    #[test]
    fn every_layer_must_allow() {
        let key = [grant(
            serde_json::json!({"queue": "telemetry", "partition": "{sub}/*", "ops": ["produce", "consume"]}),
        )];
        let token = [grant(
            serde_json::json!({"queue": "telemetry", "partition": "{sub}/gps", "ops": ["produce"]}),
        )];
        let s = Scope::new(vec![layer(&key, Some("dev1")), layer(&token, Some("dev1"))]);
        assert!(s.allows(&produce("telemetry", "dev1/gps")));
        assert!(
            !s.allows(&produce("telemetry", "dev1/temp")),
            "the token narrows"
        );
        assert!(
            !s.allows(&consume("telemetry", Some("dev1/gps"), "g")),
            "and never widens"
        );
        // A token grant wider than the key adds nothing.
        let wide = [grant(serde_json::json!({"queue": "*", "ops": ["produce"]}))];
        let s = Scope::new(vec![layer(&key, Some("dev1")), layer(&wide, Some("dev1"))]);
        assert!(!s.allows(&produce("other", "x")));
        assert!(!Scope::new(Vec::new()).allows(&produce("telemetry", "dev1/gps")));
        assert!(
            !Scope::new(vec![Vec::new()]).allows(&produce("telemetry", "dev1/gps")),
            "an empty layer allows nothing"
        );
    }

    #[test]
    fn a_prefix_after_sub_needs_a_separator() {
        let bad =
            grant(serde_json::json!({"queue": "q", "partition": "{sub}*", "ops": ["produce"]}));
        let e = bad.check().unwrap_err();
        assert!(e.contains("separator"), "{e}");
        let bad =
            grant(serde_json::json!({"queue": "q", "partition": "{sub}-x*", "ops": ["produce"]}));
        assert!(bad.check().is_err(), "`-` can be part of a sub");
        let ok =
            grant(serde_json::json!({"queue": "q", "partition": "{sub}/*", "ops": ["produce"]}));
        ok.check().unwrap();
        let s = scope_of(&[ok], Some("dev-1"));
        assert!(s.allows(&produce("q", "dev-1/a")));
        assert!(
            !s.allows(&produce("q", "dev-12/a")),
            "dev-1 must not reach dev-12"
        );
        assert!(!s.allows(&produce("q", "dev-1")));
        // Without a trailing `*`, `{sub}` matches exactly: `dev-1` is not `dev-12`.
        let exact =
            grant(serde_json::json!({"queue": "q", "partition": "p-{sub}", "ops": ["produce"]}));
        let s = scope_of(&[exact], Some("dev-1"));
        assert!(s.allows(&produce("q", "p-dev-1")));
        assert!(!s.allows(&produce("q", "p-dev-12")));
    }

    #[test]
    fn a_sub_that_is_not_one_expands_nothing() {
        let g = grant(serde_json::json!({"queue": "q", "partition": "{sub}", "ops": ["produce"]}));
        let long = "x".repeat(SUB_MAX_BYTES + 1);
        for sub in ["", "*", "a/b", "a:b", "{sub}", "a b", "é", long.as_str()] {
            assert!(g.expand(Some(sub)).is_none(), "sub {sub:?}");
            assert!(!valid_sub(sub), "{sub:?}");
        }
        for sub in [
            "device-8f3a",
            "a.b_c-d@e+f",
            "0b8c2d5e-1f4a-4c1d-9e2b-3a4b5c6d7e8f",
        ] {
            assert!(valid_sub(sub), "{sub:?}");
            assert!(g.expand(Some(sub)).is_some());
        }
    }

    #[test]
    fn malformed_grants_are_refused_with_the_field_named() {
        let cases = [
            (serde_json::json!({"ops": ["produce"]}), "names `queue`"),
            (
                serde_json::json!({"queue": "a", "kv": "b", "ops": ["produce"]}),
                "exactly one",
            ),
            (serde_json::json!({"queue": "a", "ops": []}), "needs `ops`"),
            (
                serde_json::json!({"queue": "a", "ops": ["read"]}),
                "not `read`",
            ),
            (
                serde_json::json!({"kv": "a", "ops": ["produce"]}),
                "not `produce`",
            ),
            (
                serde_json::json!({"kv": "a", "partition": "p", "ops": ["read"]}),
                "no `partition`",
            ),
            (
                serde_json::json!({"queue": "a", "key": "k", "ops": ["produce"]}),
                "no `key`",
            ),
            (
                serde_json::json!({"timers": "a", "group": "g", "ops": ["write"]}),
                "no `group`",
            ),
            (
                serde_json::json!({"queue": "", "ops": ["produce"]}),
                "is empty",
            ),
            (
                serde_json::json!({"queue": "a*b", "ops": ["produce"]}),
                "only end",
            ),
            (
                serde_json::json!({"queue": "**", "ops": ["produce"]}),
                "only end",
            ),
            (
                serde_json::json!({"queue": "{sub}{sub}", "ops": ["produce"]}),
                "once",
            ),
            (
                serde_json::json!({"queue": "{user}", "ops": ["produce"]}),
                "only as",
            ),
        ];
        for (json, want) in cases {
            let g = grant(json.clone());
            let e = g.check().expect_err(&json.to_string());
            assert!(e.contains(want), "{json}: {e}");
            assert!(g.expand(Some("dev")).is_none(), "{json} must allow nothing");
        }
        // A misspelt field is not a missing one.
        let e = serde_json::from_value::<Grant>(
            serde_json::json!({"queue": "a", "partiton": "p", "ops": ["produce"]}),
        )
        .unwrap_err();
        assert!(e.to_string().contains("partiton"), "{e}");
        let e =
            serde_json::from_value::<Grant>(serde_json::json!({"queue": "a", "ops": ["publish"]}))
                .unwrap_err();
        assert!(e.to_string().contains("publish"), "{e}");
        let many: Vec<Grant> = (0..=MAX_GRANTS)
            .map(|i| grant(serde_json::json!({"queue": format!("q{i}"), "ops": ["produce"]})))
            .collect();
        assert!(check_grants(&many).unwrap_err().contains("at most"));
        assert!(check_grants(&many[..MAX_GRANTS]).is_ok());
    }

    #[test]
    fn kv_grants_cover_keys_prefixes_and_writes_apart() {
        let grants = [
            grant(serde_json::json!({"kv": "state", "key": "{sub}/*", "ops": ["read", "write"]})),
            grant(serde_json::json!({"kv": "config", "ops": ["read"]})),
        ];
        let s = scope_of(&grants, Some("dev1"));
        let kv = |ns, key, write| Ask::Kv { ns, key, write };
        assert!(s.allows(&kv("state", Keys::One("dev1/battery"), true)));
        assert!(s.allows(&kv("state", Keys::One("dev1/battery"), false)));
        assert!(!s.allows(&kv("state", Keys::One("dev2/battery"), false)));
        assert!(s.allows(&kv("state", Keys::Prefix("dev1/"), false)));
        assert!(s.allows(&kv("state", Keys::Prefix("dev1/b"), false)));
        assert!(
            !s.allows(&kv("state", Keys::Prefix("dev1"), false)),
            "dev1 also starts dev12/"
        );
        assert!(!s.allows(&kv("state", Keys::Prefix(""), false)));
        assert!(s.allows(&kv("config", Keys::One("anything"), false)));
        assert!(s.allows(&kv("config", Keys::Prefix(""), false)));
        assert!(
            !s.allows(&kv("config", Keys::One("anything"), true)),
            "read only"
        );
        assert!(!s.allows(&kv("other", Keys::One("x"), false)));
    }

    #[test]
    fn timer_grants_name_the_key_and_where_it_fires() {
        let grants = [grant(serde_json::json!({
            "timers": "reminders", "key": "{sub}:*", "partition": "{sub}", "ops": ["write"]
        }))];
        let s = scope_of(&grants, Some("dev1"));
        let timer = |key, partition, write| Ask::Timer {
            queue: "reminders",
            key,
            partition,
            write,
        };
        assert!(s.allows(&timer(Keys::One("dev1:wake"), Some("dev1"), true)));
        assert!(
            !s.allows(&timer(Keys::One("dev1:wake"), Some("dev2"), true)),
            "fires elsewhere"
        );
        assert!(
            !s.allows(&timer(Keys::One("dev2:wake"), Some("dev1"), true)),
            "another's key"
        );
        assert!(
            s.allows(&timer(Keys::One("dev1:wake"), None, true)),
            "a cancel names no partition"
        );
        assert!(
            !s.allows(&timer(Keys::One("dev1:wake"), None, false)),
            "write only"
        );
        let read = [grant(
            serde_json::json!({"timers": "reminders", "ops": ["read"]}),
        )];
        let s = scope_of(&read, Some("dev1"));
        assert!(s.allows(&timer(Keys::All, None, false)));
        let read = [grant(
            serde_json::json!({"timers": "reminders", "key": "dev1:*", "ops": ["read"]}),
        )];
        let s = scope_of(&read, None);
        assert!(
            !s.allows(&timer(Keys::All, None, false)),
            "a list reads every key"
        );
        assert!(s.allows(&timer(Keys::Prefix("dev1:"), None, false)));
    }

    #[test]
    fn the_wire_form_round_trips_and_refuses_what_it_does_not_know() {
        let grants = [
            grant(
                serde_json::json!({"queue": "t*", "partition": "{sub}", "ops": ["consume", "produce", "consume"]}),
            ),
            grant(serde_json::json!({"kv": "*", "key": "=odd^name", "ops": ["read"]})),
        ];
        let s = scope_of(&grants, Some("dev1"));
        assert_eq!(s.layers[0][0].ops, vec![GrantOp::Produce, GrantOp::Consume]);
        let wire = s.encode();
        assert_eq!(Scope::decode(wire.as_bytes()).unwrap(), s);
        // A name that starts with `=` or `^` stays a name.
        assert_eq!(s.layers[0][1].key, Pat::Exact("=odd^name".into()));
        assert!(s.allows(&Ask::Kv {
            ns: "x",
            key: Keys::One("=odd^name"),
            write: false
        }));
        let v2 = wire.replacen("\"v\":1", "\"v\":2", 1);
        assert!(Scope::decode(v2.as_bytes())
            .unwrap_err()
            .contains("version 2"));
        let extra = wire.replacen("\"kind\"", "\"lane\":\"*\",\"kind\"", 1);
        assert!(Scope::decode(extra.as_bytes()).is_err(), "an unknown field");
        let missing = wire.replacen("\"group\":\"*\",", "", 1);
        assert!(Scope::decode(missing.as_bytes()).is_err(), "a lost field");
        assert!(Scope::decode(br#"{"v":1,"layers":[[{"kind":"queue","name":"telemetry","partition":"*","group":"*","key":"*","ops":["produce"]}]]}"#).is_err(), "a bare name");
        assert!(Scope::decode(b"not json").is_err());
    }

    #[test]
    fn a_scoped_request_reaches_only_the_routes_that_check_names() {
        let allowed = [
            ("POST", "/api/v1/push"),
            ("POST", "/api/v1/transaction"),
            ("POST", "/api/v1/ack"),
            ("POST", "/api/v1/ack/batch"),
            ("POST", "/api/v1/lease/0192-abc/extend"),
            ("GET", "/api/v1/pop/queue/commands"),
            ("GET", "/api/v1/pop/queue/commands/partition/dev1"),
            ("HEAD", "/api/v1/pop/queue/commands/partition/dev1"),
            ("POST", "/api/v1/kv"),
            ("GET", "/api/v1/kv/state/dev1/battery"),
            ("PUT", "/api/v1/kv/state/dev1"),
            ("DELETE", "/api/v1/kv/state/dev1"),
            ("POST", "/api/v1/timers"),
            ("GET", "/api/v1/timers/reminders"),
            ("GET", "/api/v1/timers/reminders/dev1:wake"),
            ("DELETE", "/api/v1/timers/reminders/dev1:wake"),
        ];
        for (m, p) in allowed {
            assert!(scoped_route(m, p), "{m} {p}");
        }
        let refused = [
            ("GET", "/api/v1/pop"),
            ("GET", "/api/v1/push"),
            ("POST", "/api/v1/push/"),
            ("POST", "/api/v1/pushes"),
            ("POST", "/api/v1/pop/queue/commands"),
            ("GET", "/api/v1/pop/queue/"),
            ("GET", "/api/v1/pop/queue/commands/partition/"),
            ("POST", "/api/v1/configure"),
            ("GET", "/api/v1/resources/queues"),
            ("POST", "/api/v1/resources/kv/list"),
            ("GET", "/api/v1/resources/kv/namespaces"),
            ("POST", "/api/v1/locks"),
            ("POST", "/api/v1/feed"),
            ("POST", "/api/v1/fetch"),
            ("POST", "/api/v1/ephemeral/push"),
            ("GET", "/api/v1/dlq"),
            ("POST", "/api/v1/tokens"),
            ("POST", "/api/v1/kv/state/dev1"),
            ("GET", "/api/v1/kv/state/"),
            ("GET", "/api/v1/kv/state//x"),
            ("PUT", "/api/v1/timers/reminders/x"),
            ("POST", "/streams/v1/claim"),
            ("GET", "/health"),
            ("GET", "/"),
            ("GET", "/api/v1//push"),
            ("post", "/api/v1/push"),
        ];
        for (m, p) in refused {
            assert!(!scoped_route(m, p), "{m} {p}");
        }
    }

    #[test]
    fn a_mint_request_refuses_unknown_fields() {
        let ok: MintTokenRequest =
            serde_json::from_value(serde_json::json!({"sub": "dev1", "ttlSeconds": 60})).unwrap();
        assert_eq!(ok.ttl_seconds, Some(60));
        assert!(serde_json::from_value::<MintTokenRequest>(
            serde_json::json!({"sub": "dev1", "ttl": 60})
        )
        .is_err());
    }
}
