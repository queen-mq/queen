//! Committed group offsets, kept in Queen's key/value store.
//!
//! This is the durable half of M4, and the only durable thing the facade has:
//! membership is in memory and dies with the process ([`crate::coordinator`]),
//! offsets are here and outlive it. PLAN_QUEEN_KAFKA.md's C3 will graduate them
//! to a native per-(group, queue, partition) cursor — which is what lights up
//! consumer lag in the Queen console for Kafka groups — and until it exists the
//! plan's answer is KV: "OffsetCommit/OffsetFetch use KV".
//!
//! ## One key per (group, topic, partition)
//!
//! ```text
//!   namespace: queen-kafka
//!   key:       qk:group:<group>:<topic>:<partition>
//!   value:     {"offset": 41, "metadata": "…", "ts": 1787824800123}
//! ```
//!
//! A key and not a document per group, because a commit names an arbitrary
//! subset of a group's partitions and a document would make every commit a
//! read-modify-write of the whole group under a lock the KV store does not
//! offer. One key per partition makes a commit a set of independent upserts,
//! which is exactly what Kafka's own `__consumer_offsets` log is.
//!
//! ## The separators are escaped, and that is not decoration
//!
//! A Kafka topic name is `[a-zA-Z0-9._-]`, but a GROUP ID is an arbitrary
//! string: `:` is legal in one, and so are spaces, newlines and NUL. Two things
//! break without escaping. Ambiguity: group `a` with topic `b` and group `a:b`
//! with topic `0` would compose the same key, and the prefix read that answers
//! "all offsets for group `a`" would hand out group `a:b`'s. And storability:
//! the key column is Postgres `TEXT`, which cannot hold a NUL byte at all. So
//! everything outside `[A-Za-z0-9._-]` is percent-encoded ([`escape`]), which
//! leaves every ordinary group and every legal topic name spelled exactly as it
//! is, makes the separator unambiguous, and keeps the byte ordering the prefix
//! cursor pages in.
//!
//! ## One more key per group: EXISTENCE, which is not membership
//!
//! ```text
//!   namespace: queen-kafka                    (the same one)
//!   key:       qk:groups:<esc group>
//!   value:     {"pt": "consumer", "ts": 1787824800123}
//! ```
//!
//! M7 F2's, and it exists because ListGroups asks a question the registry
//! cannot answer. A group's actor is reaped `GroupConfig::empty_reap` after its
//! last member leaves, so a registry-only ListGroups shows nothing for a group
//! whose consumers are merely STOPPED — which is exactly the group an operator
//! opens the tool to look at. Kafka shows it, because `__consumer_offsets` is
//! durable. So existence goes where the offsets already are, and the facade's
//! own split is kept: **offsets and existence are Queen's, liveness is this
//! process's**.
//!
//! Note the prefix, and note that it is deliberate: `qk:groups:x` does not
//! start with `qk:group:` — the ninth character is `s` against `:` — so the
//! index and the offsets cannot see each other's prefix reads, in either
//! direction. [`the_two_group_key_spaces_cannot_see_each_other`] pins it rather
//! than leaving it to be re-derived.
//!
//! `pt` is the group's protocol type as of the commit that wrote the row —
//! `consumer` for a consumer group, `""` for a simple (`assign()`-based) one,
//! which is what Kafka reports for the same thing. It is the one field
//! `KafkaAdminClient.listConsumerGroups` reads to decide whether a group is a
//! consumer group at all.
//!
//! ## Offsets never expire; DeleteGroups is the only thing that removes them
//!
//! Every KV write must declare an expiry and this one's is "forever"
//! ([`crate::queen::KvOp::Put`]). A committed offset that expired would be a
//! consumer group that silently resumes from `auto.offset.reset` — a replay of
//! the whole topic, or a jump to its end past everything it had not read.
//!
//! The consequence is stated rather than hidden: Kafka expires the offsets of a
//! group that has been empty for `offsets.retention.minutes` (7 days by
//! default) and this does not, so a facade that has served a million throwaway
//! group ids holds a million keys until something deletes them. Since M7 F2
//! there is a tool for that and only a tool — [`delete_group`], behind
//! `handlers::delete_groups` — and no policy: nothing here expires anything on
//! its own.
//!
//! ## The fence, in cluster mode only
//!
//! An unconditional upsert is exactly right for one facade and exactly wrong
//! for two: the loser of a race overwrites the winner and nothing says so — the
//! measured "committed 50, read back 16". So in cluster mode
//! ([`crate::cluster`]) a commit carries ONE extra operation at index 0, a
//! conditional write of `qk:fence:<group>` with `"required": true`, and the
//! whole batch is a transaction that either fences or writes nothing
//! (024_kv.sql:1498-1502). It costs no round trip and one operation of the
//! call's budget; in single mode it is not added at all, so the body on the
//! wire is byte-identical to what it has always been.

use kafka_protocol::error::ResponseError;

use crate::cluster::fence::FenceOp;
use crate::queen::{self, KvOp, QueenApi};

/// The KV namespace every offset lives in.
///
/// Fixed, and never anything a client chose: the namespace charset is
/// `^[a-z0-9][a-z0-9._-]{0,63}$` (server/sql/procedures/024_kv.sql) and a group
/// id is an arbitrary string, so a group-derived namespace would be a validation
/// error on the broker for names that are perfectly legal here. Everything
/// variable goes in the key, which has no charset.
pub const NAMESPACE: &str = "queen-kafka";

/// The prefix every offset key starts with. Group offsets share the namespace
/// with whatever else this facade may one day keep there, so the shape is
/// self-describing rather than positional.
const KEY_PREFIX: &str = "qk:group:";

/// The prefix of a group's FENCE key ([`crate::cluster::fence`]). A distinct
/// literal prefix under the same namespace, which is what the `qk:` shape was
/// designed for — it collides with neither `qk:group:` nor the node registry's
/// `qk:node:`.
const FENCE_PREFIX: &str = "qk:fence:";

/// The prefix of the durable GROUP INDEX — group EXISTENCE, which outlives the
/// in-memory registry the way committed offsets already do. See the module
/// header, and note the `s`: this is not a prefix of [`KEY_PREFIX`] and
/// [`KEY_PREFIX`] is not a prefix of it.
const INDEX_PREFIX: &str = "qk:groups:";

/// Ceiling on one key, in bytes — Postgres-side, `queen.kv_check_names_v1`.
/// A group id and a topic name can each be long enough that the composed key
/// passes it; see [`key`].
const MAX_KEY_BYTES: usize = 512;

/// Ceiling on a commit's metadata string, in bytes. Kafka's own
/// `offset.metadata.max.bytes` default, and its own error code for exceeding
/// it, so a client that sets a bigger one on a real broker finds the same
/// number here.
pub const MAX_METADATA_BYTES: usize = 4096;

/// Commits written in one call to Queen.
///
/// The batching rule from the plan is "batch all partitions of one commit into
/// one KV call", and this is where that stops being true: the broker refuses a
/// batch of more than [`queen::MAX_KV_OPS_PER_CALL`] operations outright, and a
/// refusal is the whole commit failing rather than a shorter one succeeding. A
/// consumer assigned more than 256 partitions — ordinary against a topic whose
/// default width here is 1024 — therefore commits in several calls.
const COMMITS_PER_CALL: usize = queen::MAX_KV_OPS_PER_CALL;

/// ...and one fewer when a fence rides along, because the fence IS one of the
/// operations. Every chunk carries its own: `required: true` aborts the
/// transaction it is in, and a chunk is a transaction.
const FENCED_COMMITS_PER_CALL: usize = queen::MAX_KV_OPS_PER_CALL - 1;

/// Keys read in one call to Queen.
///
/// NOT [`queen::MAX_KV_KEYS_PER_CALL`], and the difference is the one ceiling
/// that is invisible from the request: the stored procedure spends a
/// [`queen::MAX_KV_READ_BYTES`] budget (4 MiB) over the rows it returns, and
/// rows past it are simply absent — neither in `rows` nor in `missing`. For an
/// OFFSET, "absent" would mean "never committed", which is a consumer reset.
///
/// So the chunk is sized so the budget cannot bind: one value is an offset, a
/// timestamp and up to [`MAX_METADATA_BYTES`] of metadata, and metadata made
/// entirely of control characters is six JSON bytes per byte — ~24.7 KB in the
/// worst case anyone can construct. 128 of those is ~3.2 MB, inside the budget
/// with room to spare, while an ordinary commit (empty metadata, ~60 bytes)
/// leaves it untouched. The truncation flag is still honoured on top of this —
/// see [`Loaded::Unread`] — because a silent wrong answer here is worse than
/// two round trips.
const KEYS_PER_CALL: usize = 128;

/// Rows one page of the all-topics read asks for. Same derivation as
/// [`KEYS_PER_CALL`], and under the stored procedure's own clamp of
/// [`queen::MAX_KV_PREFIX_LIMIT`].
const PREFIX_PAGE: i64 = KEYS_PER_CALL as i64;

/// Pages one all-topics read will walk before giving up.
///
/// A bound and not a limit anyone should reach: 4096 pages is half a million
/// partitions for one group. It exists because the loop's termination depends
/// on the broker's cursor advancing, and a loop whose exit condition lives in
/// another process needs one.
const MAX_PREFIX_PAGES: usize = 4_096;

/// What one partition's committed offset says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Committed {
    pub offset: i64,
    /// The client's own opaque string. Kafka carries it untouched and so does
    /// this; it is where a consumer stamps a batch id, a schema version, a
    /// deploy marker.
    pub metadata: String,
    /// When the commit was written, epoch milliseconds. Not read back by
    /// anything on the Kafka side — the protocol has no field for it — and kept
    /// because the alternative is a stored record that cannot be dated when
    /// someone is looking at why a group is where it is.
    pub ts: i64,
}

impl Committed {
    fn to_value(&self) -> serde_json::Value {
        serde_json::json!({
            "offset": self.offset,
            "metadata": self.metadata,
            "ts": self.ts,
        })
    }

    /// Read a stored value back. `None` for anything that is not one of ours —
    /// see [`Loaded`] for what the caller does with that.
    fn from_value(v: &serde_json::Value) -> Option<Committed> {
        Some(Committed {
            offset: v.get("offset")?.as_i64()?,
            metadata: v
                .get("metadata")
                .and_then(|m| m.as_str())
                .unwrap_or_default()
                .to_string(),
            ts: v.get("ts").and_then(|t| t.as_i64()).unwrap_or_default(),
        })
    }
}

/// What one requested key resolved to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Loaded {
    /// A commit, as it was written.
    Found(Committed),
    /// No such key. The right answer is offset -1, WITHOUT an error: that is
    /// precisely what makes a consumer apply `auto.offset.reset`, which is what
    /// a group that has never committed is supposed to do.
    Missing,
    /// The read did not cover this key — the byte budget cut it short. It is
    /// NOT `Missing`, and the distinction is the whole reason this variant
    /// exists: answering -1 here would tell a consumer that has committed
    /// nothing exists, and it would reset.
    Unread,
}

/// The key for one (group, topic, partition), or `None` when it cannot be
/// stored.
///
/// A group id has no length limit in the protocol and a topic name may be 249
/// characters, so the composed key can exceed the 512-byte column bound — more
/// easily still once a group id with unusual characters is escaped. `None` is
/// the honest answer, and the caller turns it into INVALID_COMMIT_OFFSET_SIZE:
/// a commit this facade cannot store must not read back as "never committed".
pub fn key(group: &str, topic: &str, partition: i32) -> Option<String> {
    let key = format!(
        "{KEY_PREFIX}{}:{}:{partition}",
        escape(group),
        escape(topic)
    );
    (key.len() <= MAX_KEY_BYTES).then_some(key)
}

/// The prefix under which every one of `group`'s offsets lives.
pub fn group_prefix(group: &str) -> String {
    format!("{KEY_PREFIX}{}:", escape(group))
}

/// The key of `group`'s fence, or `None` when it could not be stored.
///
/// Escaped with the same function and for the same reasons as [`key`]: a group
/// id is an arbitrary string. It is always SHORTER than any offset key of the
/// same group, so a group whose fence key does not fit has no storable offset
/// key either — every partition of that commit is already refused
/// INVALID_COMMIT_OFFSET_SIZE before a fence is ever asked for.
pub fn fence_key(group: &str) -> Option<String> {
    let key = format!("{FENCE_PREFIX}{}", escape(group));
    (key.len() <= MAX_KEY_BYTES).then_some(key)
}

/// The key of `group`'s row in the durable index, or `None` when it could not
/// be stored.
///
/// Always shorter than any offset key of the same group, so a group whose index
/// key does not fit has no storable offset key either — which is the order the
/// commit path checks them in.
pub fn index_key(group: &str) -> Option<String> {
    let key = format!("{INDEX_PREFIX}{}", escape(group));
    (key.len() <= MAX_KEY_BYTES).then_some(key)
}

/// The group id an index key names, or `None` if the key is not one of ours.
pub fn parse_index_key(key: &str) -> Option<String> {
    key.strip_prefix(INDEX_PREFIX).map(unescape)
}

/// What the index remembers about a group. Deliberately almost nothing: this
/// row says a group EXISTS and what kind it is, and everything else about it is
/// either the coordinator's (liveness) or already under [`KEY_PREFIX`]
/// (offsets).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Indexed {
    /// The group's protocol type as of the commit that wrote this row.
    /// `consumer` for a consumer group, empty for a simple one.
    pub protocol_type: String,
}

impl Indexed {
    fn to_value(&self, now: i64) -> serde_json::Value {
        serde_json::json!({ "pt": self.protocol_type, "ts": now })
    }

    /// Read a stored row back. A row that is not one of ours reads as a group
    /// with no protocol type rather than as nothing: the KEY is the existence
    /// claim, and dropping a group from an admin listing because a field was
    /// unreadable would be the worse answer.
    fn from_value(v: &serde_json::Value) -> Indexed {
        Indexed {
            protocol_type: v
                .get("pt")
                .and_then(|p| p.as_str())
                .unwrap_or_default()
                .to_string(),
        }
    }
}

/// The (topic, partition) a key names, or `None` if it is not one of ours.
pub fn parse_key(group: &str, key: &str) -> Option<(String, i32)> {
    let rest = key.strip_prefix(&group_prefix(group))?;
    let (topic, partition) = rest.rsplit_once(':')?;
    Some((unescape(topic), partition.parse().ok()?))
}

/// Percent-encode everything outside `[A-Za-z0-9._-]`.
///
/// The set is the one every legal Kafka topic name is already made of, so a
/// topic is never rewritten and an ordinary group id (`orders-consumer`,
/// `svc.billing`) is not either. What it does catch is the separator, the
/// escape character itself, and the bytes Postgres `TEXT` cannot hold. See the
/// module header for why both matter.
fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// The inverse. An escape that is not one is left as it stands rather than
/// dropped — this reads keys back, and a key that does not round-trip must
/// still be recognisable in a log.
fn unescape(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Some(b) = std::str::from_utf8(&bytes[i + 1..i + 3])
                .ok()
                .and_then(|h| u8::from_str_radix(h, 16).ok())
            {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

// ------------------------------------------------------------------- writing

/// What one commit's worth of writes did.
#[derive(Debug, Default)]
pub struct Stored {
    /// One result per input pair, aligned by index: a chunk that failed fails
    /// exactly its own members, so a wide commit does not report success for
    /// partitions whose call never landed.
    pub results: Vec<queen::Result<()>>,
    /// The version the fence key holds after this call, when a fence was sent
    /// and kept. The next commit `expect`s it.
    pub fence_version: Option<i64>,
    /// Set when the FENCE was lost. Nothing was written — not one offset, not
    /// in this chunk and not in any later one, because a lost `required`
    /// precondition aborts the transaction and the remaining chunks are not
    /// sent. The verdict is [`crate::cluster::fence`]'s to interpret.
    pub lost: Option<Lost>,
}

/// The winner of a lost fence, as the stored procedure handed it back in the
/// same answer (024_kv.sql:1467-1471) — so nothing here needs a second call to
/// find out who it lost to.
#[derive(Debug, Clone)]
pub struct Lost {
    /// `version`, `absent` or `exists`.
    pub reason: String,
    /// ADVISORY. Something to `expect` once, never a token to reuse blindly.
    pub version: i64,
    /// The winner's fence value, or null when the key is simply not there.
    pub value: serde_json::Value,
}

/// Store one commit's worth of offsets, chunked against the broker's op
/// ceiling, optionally behind a `fence`.
pub async fn store(
    api: &dyn QueenApi,
    pairs: &[(String, Committed)],
    token: Option<&str>,
    fence: Option<&FenceOp>,
) -> Stored {
    let per_call = match fence {
        Some(_) => FENCED_COMMITS_PER_CALL,
        None => COMMITS_PER_CALL,
    };
    let mut out = Stored {
        results: Vec::with_capacity(pairs.len()),
        ..Stored::default()
    };
    // The version each chunk's fence expects. It ADVANCES with every chunk,
    // because writing the fence is what gives it a new version and a chunk is
    // its own transaction: a second chunk that still expected the first one's
    // version would fence itself off. This is why a wide commit threads it
    // rather than reusing one operation.
    let mut expect = fence.map(|f| f.expect);
    for (at, chunk) in pairs.chunks(per_call).enumerate() {
        let mut ops: Vec<KvOp> = Vec::with_capacity(chunk.len() + 1);
        // Index 0, always: the answer is read back by position and the fence's
        // own verdict decides whether anything else in the call happened.
        if let (Some(fence), Some(expect)) = (fence, expect) {
            ops.push(fence.at(expect).kv_op());
        }
        ops.extend(
            chunk
                .iter()
                .map(|(key, value)| KvOp::put(NAMESPACE, key, value.to_value())),
        );
        match api.kv(&ops, token).await {
            Ok(mut answers) => {
                if fence.is_some() && !answers.is_empty() {
                    let head = answers.remove(0);
                    match head.applied {
                        Some(true) => {
                            out.fence_version = Some(head.version);
                            expect = Some(head.version);
                        }
                        // Unreachable: `required: true` turns a lost
                        // precondition into an aborted transaction, which
                        // arrives as `Error::Precondition` below. Reported and
                        // never assumed, because a fence that answered
                        // anything else is a broker that changed under us.
                        other => {
                            let e = queen::Error::Body(format!(
                                "kv fence answered applied={other:?} instead of aborting"
                            ));
                            out.results.extend(chunk.iter().map(|_| Err(e.clone())));
                            continue;
                        }
                    }
                }
                // A `put` with no precondition cannot fail to apply, so
                // anything other than `applied` is a broker that changed under
                // us rather than a verdict — reported, never assumed to have
                // written.
                out.results
                    .extend(answers.into_iter().map(|a| match a.applied {
                        Some(true) => Ok(()),
                        other => Err(queen::Error::Body(format!(
                            "kv put answered applied={other:?}"
                        ))),
                    }));
            }
            Err(e @ queen::Error::Precondition { .. }) => {
                if let queen::Error::Precondition {
                    reason,
                    version,
                    value,
                    ..
                } = &e
                {
                    out.lost = Some(Lost {
                        reason: reason.clone(),
                        version: *version,
                        value: value.clone(),
                    });
                }
                // Every pair from here on, not just this chunk's: the fence is
                // gone, so sending the rest would be a second attempt to write
                // as a coordinator this node is not.
                let unsent = pairs.len() - at * per_call;
                out.results.extend((0..unsent).map(|_| Err(e.clone())));
                return out;
            }
            Err(e) => out.results.extend(chunk.iter().map(|_| Err(e.clone()))),
        }
    }
    out
}

// ------------------------------------------------------------------- reading

/// Read the committed offsets for a known key list, chunked against the read
/// budget. One result per input key, in order.
pub async fn load(
    api: &dyn QueenApi,
    keys: &[String],
    token: Option<&str>,
) -> queen::Result<Vec<Loaded>> {
    let mut out = Vec::with_capacity(keys.len());
    for chunk in keys.chunks(KEYS_PER_CALL) {
        let ops = [KvOp::GetMany {
            ns: NAMESPACE.to_string(),
            keys: chunk.to_vec(),
        }];
        let answers = api.kv(&ops, token).await?;
        let answer = answers
            .into_iter()
            .next()
            .ok_or_else(|| queen::Error::Body("kv answered nothing for a getMany".to_string()))?;
        for key in chunk {
            let found = answer.rows.iter().find(|r| &r.key == key);
            out.push(match found {
                Some(row) => match Committed::from_value(&row.value) {
                    Some(c) => Loaded::Found(c),
                    // Something wrote a value we cannot read under one of our
                    // keys. Treated as never committed rather than as a
                    // permanent failure: the next commit overwrites it, and a
                    // retriable error here would be a group that can never
                    // start.
                    None => {
                        tracing::warn!(target: "kafka", key = %key, "unreadable committed offset");
                        Loaded::Missing
                    }
                },
                None if answer.missing.iter().any(|m| m == key) => Loaded::Missing,
                // Neither returned nor reported absent: the byte budget cut it.
                None => Loaded::Unread,
            });
        }
    }
    Ok(out)
}

/// Every committed offset of one group, by prefix, paged.
///
/// This is what OffsetFetch's null-topics form asks for — "everything this
/// group has committed" — and it is answerable exactly because the KV surface
/// has `getPrefix` with a keyset cursor. Returns (topic, partition, commit) in
/// the store's byte order.
pub async fn load_group(
    api: &dyn QueenApi,
    group: &str,
    token: Option<&str>,
) -> queen::Result<Vec<(String, i32, Committed)>> {
    let prefix = group_prefix(group);
    let mut after: Option<String> = None;
    let mut out = Vec::new();
    for _ in 0..MAX_PREFIX_PAGES {
        let ops = [KvOp::GetPrefix {
            ns: NAMESPACE.to_string(),
            prefix: prefix.clone(),
            limit: PREFIX_PAGE,
            after: after.clone(),
        }];
        let answers = api.kv(&ops, token).await?;
        let answer = answers
            .into_iter()
            .next()
            .ok_or_else(|| queen::Error::Body("kv answered nothing for a getPrefix".to_string()))?;
        for row in &answer.rows {
            match (
                parse_key(group, &row.key),
                Committed::from_value(&row.value),
            ) {
                (Some((topic, partition)), Some(c)) => out.push((topic, partition, c)),
                // A key under our prefix that is not one of our keys. Skipped
                // and named, because inventing a (topic, partition) for it
                // would put a partition in a consumer's answer that it never
                // asked about.
                _ => tracing::warn!(target: "kafka", key = %row.key, "unreadable offset key"),
            }
        }
        if !answer.truncated {
            return Ok(out);
        }
        // A truncated page with no cursor cannot be continued — asking again
        // would return the same page for ever.
        let Some(next) = answer.next_after.clone() else {
            return Err(queen::Error::Body(
                "kv truncated a prefix page without a cursor to continue from".to_string(),
            ));
        };
        after = Some(next);
    }
    Err(queen::Error::Body(format!(
        "the offsets of {group} did not fit in {MAX_PREFIX_PAGES} pages"
    )))
}

// ------------------------------------------------------------- the group index

/// Record that `group` exists, with the protocol type it is running under.
///
/// One `put`, on its own call and NOT folded into the commit batch beside it,
/// and the reason is that the commit batch is a fenced TRANSACTION in cluster
/// mode ([`crate::cluster::fence`]): an operation added to it either has to be
/// threaded through the fence's own chunking and answer alignment, or it
/// silently takes one of the [`queen::MAX_KV_OPS_PER_CALL`] slots an offset
/// needed. A separate call can do neither. It costs one extra round trip the
/// FIRST time a group commits in this process's lifetime and none after, which
/// is what `handlers::offset_commit`'s seen-set is for.
pub async fn index(
    api: &dyn QueenApi,
    group: &str,
    protocol_type: &str,
    token: Option<&str>,
) -> queen::Result<()> {
    let Some(key) = index_key(group) else {
        return Err(queen::Error::Body(format!(
            "the index key for {group} is longer than the store's key column"
        )));
    };
    let value = Indexed {
        protocol_type: protocol_type.to_string(),
    }
    .to_value(now_millis());
    let ops = [KvOp::put(NAMESPACE, &key, value)];
    api.kv(&ops, token).await.map(|_| ())
}

/// Wall-clock milliseconds. Not tokio's: this is a timestamp that is STORED and
/// read by a person, and a paused test clock would write 1970 into the database.
fn now_millis() -> i64 {
    std::time::UNIX_EPOCH
        .elapsed()
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

/// Every group of this tenant that the index knows about, paged, up to `max`.
///
/// Returns the rows and whether the walk was CUT SHORT by `max` — ListGroups
/// has no truncation flag on the wire, so the only honest thing to do with that
/// bit is log it, and a caller that could not tell would not be able to.
pub async fn list_index(
    api: &dyn QueenApi,
    token: Option<&str>,
    max: usize,
) -> queen::Result<(Vec<(String, Indexed)>, bool)> {
    let mut after: Option<String> = None;
    let mut out: Vec<(String, Indexed)> = Vec::new();
    for _ in 0..MAX_PREFIX_PAGES {
        let ops = [KvOp::GetPrefix {
            ns: NAMESPACE.to_string(),
            prefix: INDEX_PREFIX.to_string(),
            limit: PREFIX_PAGE,
            after: after.clone(),
        }];
        let answers = api.kv(&ops, token).await?;
        let answer = answers
            .into_iter()
            .next()
            .ok_or_else(|| queen::Error::Body("kv answered nothing for a getPrefix".to_string()))?;
        for row in &answer.rows {
            match parse_index_key(&row.key) {
                Some(group) => out.push((group, Indexed::from_value(&row.value))),
                // A key under our prefix that is not one of our keys. Named and
                // skipped, never invented into a group id an operator would go
                // looking for.
                None => {
                    tracing::warn!(target: "kafka", key = %row.key, "unreadable group index key")
                }
            }
            if out.len() >= max {
                return Ok((out, true));
            }
        }
        if !answer.truncated {
            return Ok((out, false));
        }
        let Some(next) = answer.next_after.clone() else {
            return Err(queen::Error::Body(
                "kv truncated a prefix page without a cursor to continue from".to_string(),
            ));
        };
        after = Some(next);
    }
    Err(queen::Error::Body(format!(
        "the group index did not fit in {MAX_PREFIX_PAGES} pages"
    )))
}

/// What the index says about a known list of groups. One result per input
/// group, in order; `None` for a group the index has never heard of.
pub async fn load_index(
    api: &dyn QueenApi,
    groups: &[String],
    token: Option<&str>,
) -> queen::Result<Vec<Option<Indexed>>> {
    let mut out = Vec::with_capacity(groups.len());
    for chunk in groups.chunks(KEYS_PER_CALL) {
        let keys: Vec<String> = chunk.iter().filter_map(|g| index_key(g)).collect();
        let ops = [KvOp::GetMany {
            ns: NAMESPACE.to_string(),
            keys,
        }];
        let answers = api.kv(&ops, token).await?;
        let answer = answers
            .into_iter()
            .next()
            .ok_or_else(|| queen::Error::Body("kv answered nothing for a getMany".to_string()))?;
        for group in chunk {
            let found = index_key(group)
                .and_then(|key| answer.rows.iter().find(|r| r.key == key))
                .map(|row| Indexed::from_value(&row.value));
            out.push(found);
        }
    }
    Ok(out)
}

/// Delete everything the store holds for `group`: every committed offset under
/// its prefix, and then its index row. Answers how many keys were removed.
///
/// ## Order, and what a failure part-way through leaves behind
///
/// Offsets first, index last, because the index row is the thing that says the
/// group EXISTS: a failure after the offsets and before the index leaves a
/// group that lists as Empty with nothing committed, which is a state Kafka has
/// too and which a re-run finishes. A failure the other way round would leave
/// orphaned offsets under a group nothing lists — invisible, and therefore not
/// re-runnable.
///
/// There is no transaction across a KV batch boundary and this does not pretend
/// otherwise. Every step is idempotent, the caller answers the retriable code
/// the failure maps to ([`kafka_error`]), and running the delete again finishes
/// the job. That is stated in `compat/ERRORS.md` as well as here.
pub async fn delete_group(
    api: &dyn QueenApi,
    group: &str,
    token: Option<&str>,
) -> queen::Result<usize> {
    let prefix = group_prefix(group);
    let mut after: Option<String> = None;
    let mut removed = 0usize;
    for _ in 0..MAX_PREFIX_PAGES {
        let ops = [KvOp::GetPrefix {
            ns: NAMESPACE.to_string(),
            prefix: prefix.clone(),
            limit: PREFIX_PAGE,
            after: after.clone(),
        }];
        let answers = api.kv(&ops, token).await?;
        let answer = answers
            .into_iter()
            .next()
            .ok_or_else(|| queen::Error::Body("kv answered nothing for a getPrefix".to_string()))?;
        let keys: Vec<String> = answer.rows.iter().map(|r| r.key.clone()).collect();
        // The cursor is taken BEFORE the deletes, from the page we just read:
        // `after` is exclusive on the key value, so a page whose keys are gone
        // still advances the walk correctly, and a key the store would not
        // delete cannot make the loop repeat the same page for ever.
        let cursor = answer.next_after.clone();
        for chunk in keys.chunks(queen::MAX_KV_OPS_PER_CALL) {
            let ops: Vec<KvOp> = chunk
                .iter()
                .map(|key| KvOp::delete(NAMESPACE, key, None))
                .collect();
            let applied = api.kv(&ops, token).await?;
            removed += applied.iter().filter(|a| a.applied == Some(true)).count();
        }
        if !answer.truncated {
            break;
        }
        let Some(next) = cursor else {
            return Err(queen::Error::Body(
                "kv truncated a prefix page without a cursor to continue from".to_string(),
            ));
        };
        after = Some(next);
    }

    // The index row last: see the header above.
    if let Some(key) = index_key(group) {
        let ops = [KvOp::delete(NAMESPACE, &key, None)];
        let applied = api.kv(&ops, token).await?;
        removed += applied.iter().filter(|a| a.applied == Some(true)).count();
    }
    Ok(removed)
}

/// The Kafka error for a failed call to the offset store.
///
/// Every code here is one a consumer RETRIES, because that is what the
/// situation is: the offsets exist, Queen was not reachable this second. The
/// one exception is authorization, which no amount of retrying fixes and which
/// a client must be able to report by name.
///
/// GROUP_AUTHORIZATION_FAILED and not TOPIC_AUTHORIZATION_FAILED (which the
/// data path uses for the same status): the credential that failed here is the
/// one reading the group's offsets, and a client that reports the wrong noun
/// sends its operator looking at the wrong grant.
pub fn kafka_error(e: &queen::Error) -> ResponseError {
    match e {
        queen::Error::Transport(_) => ResponseError::CoordinatorNotAvailable,
        queen::Error::Status { code, .. } => match code {
            401 | 403 => ResponseError::GroupAuthorizationFailed,
            // The KV surface's own back-pressure: a rate-limited tenant (429), a
            // cell shedding standalone writes or an exhausted pool (503), a
            // gateway. All "not now", and all answered by the client asking its
            // coordinator again.
            408 | 429 | 502..=504 => ResponseError::CoordinatorNotAvailable,
            _ => ResponseError::UnknownServerError,
        },
        queen::Error::Body(_) => ResponseError::UnknownServerError,
        // A lost FENCE (cluster mode only, [`crate::cluster::fence`]): another
        // facade owns this group, and this call wrote nothing. NOT_COORDINATOR
        // is the code that makes the client re-run FindCoordinator and commit
        // where it should have, which is exactly the repair wanted — the same
        // answer the ownership guard would have given had this node's view been
        // current. The verdict is normally read by the fence itself and mapped
        // there; this arm is what keeps any other path from reporting it as a
        // server fault.
        queen::Error::Precondition { .. } => ResponseError::NotCoordinator,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queen::testing::FakeQueen;
    use crate::queen::Error;

    /// The UNFENCED write, which is what every test in this module is about —
    /// the fence has its own module and its own tests
    /// ([`crate::cluster::fence`]). It shadows [`super::store`] deliberately, so
    /// that these tests keep asserting on the single-mode call unchanged.
    async fn store(
        api: &dyn QueenApi,
        pairs: &[(String, Committed)],
        token: Option<&str>,
    ) -> Vec<queen::Result<()>> {
        super::store(api, pairs, token, None).await.results
    }

    fn commit(offset: i64) -> Committed {
        Committed {
            offset,
            metadata: String::new(),
            ts: 1_787_824_800_123,
        }
    }

    // -------------------------------------------------------------- the keys

    #[test]
    fn a_key_names_its_group_topic_and_partition() {
        assert_eq!(
            key("orders-consumer", "orders", 7).unwrap(),
            "qk:group:orders-consumer:orders:7"
        );
        // Every legal Kafka topic name and every ordinary group id survives
        // unchanged, which is what makes a key readable in a database.
        assert_eq!(
            key("svc.billing_v2", "orders.eu-west_1", 0).unwrap(),
            "qk:group:svc.billing_v2:orders.eu-west_1:0"
        );
    }

    /// THE ambiguity: a group id may contain the separator, a topic name may
    /// not, and without escaping the two would compose the same key.
    #[test]
    fn a_separator_in_a_group_id_cannot_collide_with_a_topic() {
        let colliding = key("a", "b", 0).unwrap();
        let other = key("a:b", "0", 0).unwrap();
        assert_ne!(colliding, other);
        // ...and each still reads back as itself.
        assert_eq!(parse_key("a", &colliding), Some(("b".to_string(), 0)));
        assert_eq!(parse_key("a:b", &other), Some(("0".to_string(), 0)));
        // A group is not a prefix of another group with a longer name.
        assert_eq!(parse_key("a", &other), None);
    }

    #[test]
    fn keys_survive_group_ids_that_postgres_could_not_store_raw() {
        for group in ["with space", "new\nline", "nul\0byte", "unicode-ø", "100%"] {
            let k = key(group, "orders", 3).unwrap();
            assert!(
                k.bytes().all(|b| b.is_ascii_graphic()),
                "{group} produced {k:?}"
            );
            assert_eq!(parse_key(group, &k), Some(("orders".to_string(), 3)));
        }
    }

    /// A key past the column bound is refused rather than truncated: a commit
    /// stored under a shortened key would read back as never committed.
    #[test]
    fn a_key_that_cannot_be_stored_is_refused() {
        let long = "g".repeat(400);
        let topic = "t".repeat(200);
        assert!(key(&long, &topic, 0).is_none());
        assert!(key("short", "orders", 0).is_some());
        // ...and the bound is on the ESCAPED key, because that is what is
        // stored: 250 characters that each escape to three bytes are 750.
        let awkward = " ".repeat(250);
        assert!(key(&awkward, "orders", 0).is_none());
    }

    #[test]
    fn a_key_that_is_not_ours_does_not_parse() {
        for key in [
            "qk:group:other:orders:0",
            "qk:group:g:orders:notanumber",
            "qk:group:g:orders",
            "something:else",
            "",
        ] {
            assert_eq!(parse_key("g", key), None, "{key} parsed");
        }
        assert_eq!(
            parse_key("g", "qk:group:g:orders:0"),
            Some(("orders".to_string(), 0))
        );
    }

    // ------------------------------------------------------------ round trip

    #[tokio::test]
    async fn offsets_round_trip_through_the_store() {
        let api = FakeQueen::with(&[]);
        let pairs: Vec<(String, Committed)> = (0..3)
            .map(|p| {
                (
                    key("g", "orders", p).unwrap(),
                    Committed {
                        offset: 100 + i64::from(p),
                        metadata: format!("batch-{p}"),
                        ts: 1_787_824_800_123,
                    },
                )
            })
            .collect();
        let wrote = store(&*api, &pairs, None).await;
        assert!(wrote.iter().all(|r| r.is_ok()));

        let keys: Vec<String> = pairs
            .iter()
            .map(|(k, _)| k.clone())
            .chain([key("g", "orders", 99).unwrap()])
            .collect();
        let got = load(&*api, &keys, None).await.unwrap();
        assert_eq!(got.len(), 4);
        assert_eq!(got[0], Loaded::Found(pairs[0].1.clone()));
        assert_eq!(got[2], Loaded::Found(pairs[2].1.clone()));
        assert_eq!(
            got[3],
            Loaded::Missing,
            "a partition nobody committed is missing, not an error"
        );

        // A second commit overwrites rather than accumulating.
        store(&*api, &[(pairs[0].0.clone(), commit(500))], None).await;
        let again = load(&*api, &[pairs[0].0.clone()], None).await.unwrap();
        assert_eq!(again[0], Loaded::Found(commit(500)));
    }

    /// The value is written where a human can read it, and read back the same.
    #[tokio::test]
    async fn the_stored_value_is_the_documented_shape() {
        let api = FakeQueen::with(&[]);
        let k = key("g", "orders", 1).unwrap();
        store(
            &*api,
            &[(
                k.clone(),
                Committed {
                    offset: 41,
                    metadata: "m".to_string(),
                    ts: 7,
                },
            )],
            None,
        )
        .await;
        assert_eq!(
            api.kv_get(NAMESPACE, &k).unwrap(),
            serde_json::json!({"offset": 41, "metadata": "m", "ts": 7})
        );
    }

    /// Everything a group has committed, by prefix — and nothing another group
    /// has.
    #[tokio::test]
    async fn the_whole_group_is_readable_by_prefix() {
        let api = FakeQueen::with(&[]);
        let mut pairs = Vec::new();
        for (topic, partitions) in [("orders", 0..3), ("clicks", 0..2)] {
            for p in partitions {
                pairs.push((key("g", topic, p).unwrap(), commit(i64::from(p))));
            }
        }
        pairs.push((key("other-group", "orders", 0).unwrap(), commit(999)));
        store(&*api, &pairs, None).await;

        let mut got = load_group(&*api, "g", None).await.unwrap();
        got.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
        assert_eq!(
            got.iter()
                .map(|(t, p, c)| (t.as_str(), *p, c.offset))
                .collect::<Vec<_>>(),
            [
                ("clicks", 0, 0),
                ("clicks", 1, 1),
                ("orders", 0, 0),
                ("orders", 1, 1),
                ("orders", 2, 2),
            ],
            "another group's offsets leaked into this one"
        );
    }

    /// The prefix read PAGES: a group with more partitions than one page walks
    /// the cursor rather than answering a fifth of them.
    #[tokio::test]
    async fn a_wide_group_is_paged_to_the_end() {
        let api = FakeQueen::with(&[]);
        let pairs: Vec<(String, Committed)> = (0..300)
            .map(|p| (key("g", "orders", p).unwrap(), commit(i64::from(p))))
            .collect();
        store(&*api, &pairs, None).await;

        let got = load_group(&*api, "g", None).await.unwrap();
        assert_eq!(got.len(), 300);
        let calls = api.kv_calls.lock().unwrap().len();
        // 300 keys is two commit calls (256 + 44) and three prefix pages.
        assert_eq!(calls, 2 + 3, "the pages were not walked");
    }

    /// A read the byte budget cut short must not read as "never committed".
    #[tokio::test]
    async fn a_truncated_read_is_unread_and_not_missing() {
        let api = FakeQueen::with(&[]);
        let pairs: Vec<(String, Committed)> = (0..4)
            .map(|p| (key("g", "orders", p).unwrap(), commit(i64::from(p))))
            .collect();
        store(&*api, &pairs, None).await;
        api.kv_truncate_reads_at(2);

        let keys: Vec<String> = pairs.iter().map(|(k, _)| k.clone()).collect();
        let got = load(&*api, &keys, None).await.unwrap();
        let unread = got.iter().filter(|l| **l == Loaded::Unread).count();
        assert_eq!(unread, 2, "{got:?}");
        assert!(
            !got.contains(&Loaded::Missing),
            "a truncated row was reported as never committed: {got:?}"
        );
    }

    // ------------------------------------------------------------- chunking

    /// A commit wider than the broker's op ceiling is several calls, and every
    /// one of them is inside the ceiling — an over-long batch is refused
    /// wholesale, which would fail the entire commit.
    #[tokio::test]
    async fn a_wide_commit_is_chunked_under_the_op_ceiling() {
        let api = FakeQueen::with(&[]);
        let pairs: Vec<(String, Committed)> = (0..600)
            .map(|p| (key("g", "orders", p).unwrap(), commit(i64::from(p))))
            .collect();
        let results = store(&*api, &pairs, None).await;
        assert_eq!(results.len(), 600);
        assert!(results.iter().all(|r| r.is_ok()));

        let calls = api.kv_calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 3);
        for call in &calls {
            assert!(call.len() <= queen::MAX_KV_OPS_PER_CALL);
        }
        assert_eq!(api.kv_keys().len(), 600);
    }

    /// A read wider than one chunk is several calls, each inside the budget the
    /// chunk size was derived from.
    #[tokio::test]
    async fn a_wide_read_is_chunked_and_stays_aligned() {
        let api = FakeQueen::with(&[]);
        let pairs: Vec<(String, Committed)> = (0..300)
            .map(|p| (key("g", "orders", p).unwrap(), commit(i64::from(p))))
            .collect();
        store(&*api, &pairs, None).await;
        api.kv_calls.lock().unwrap().clear();

        let keys: Vec<String> = pairs.iter().map(|(k, _)| k.clone()).collect();
        let got = load(&*api, &keys, None).await.unwrap();
        assert_eq!(got.len(), 300);
        for (i, loaded) in got.iter().enumerate() {
            assert_eq!(
                *loaded,
                Loaded::Found(commit(i as i64)),
                "partition {i} came back as another partition's offset"
            );
        }
        assert_eq!(api.kv_calls.lock().unwrap().len(), 3);
    }

    // --------------------------------------------------------------- failures

    /// A failed chunk fails exactly its own commits, and the others still
    /// report what happened to them.
    #[tokio::test]
    async fn a_failed_chunk_does_not_report_success_for_its_partitions() {
        let api = FakeQueen::with(&[]);
        let pairs: Vec<(String, Committed)> = (0..300)
            .map(|p| (key("g", "orders", p).unwrap(), commit(i64::from(p))))
            .collect();
        api.fail_kv(Error::Transport("connection refused".into()));
        let results = store(&*api, &pairs, None).await;

        assert_eq!(results.len(), 300);
        assert!(results[..COMMITS_PER_CALL].iter().all(|r| r.is_err()));
        assert!(results[COMMITS_PER_CALL..].iter().all(|r| r.is_ok()));
    }

    #[tokio::test]
    async fn a_failed_read_is_an_error_and_not_an_empty_group() {
        let api = FakeQueen::with(&[]);
        api.fail_kv(Error::Transport("reset".into()));
        assert!(load(&*api, &[key("g", "orders", 0).unwrap()], None)
            .await
            .is_err());

        api.fail_kv(Error::Transport("reset".into()));
        assert!(load_group(&*api, "g", None).await.is_err());
    }

    #[test]
    fn a_failure_maps_to_a_code_a_consumer_acts_on() {
        assert_eq!(
            kafka_error(&Error::Transport("reset".into())),
            ResponseError::CoordinatorNotAvailable
        );
        for (code, want) in [
            (401, ResponseError::GroupAuthorizationFailed),
            (403, ResponseError::GroupAuthorizationFailed),
            (429, ResponseError::CoordinatorNotAvailable),
            (503, ResponseError::CoordinatorNotAvailable),
            (500, ResponseError::UnknownServerError),
        ] {
            assert_eq!(kafka_error(&Error::status(code, "")), want, "HTTP {code}");
        }
    }

    // --------------------------------------------------------- the group index

    /// THE property the two key spaces rest on, pinned rather than re-derived:
    /// `qk:groups:` is not a prefix of `qk:group:` and `qk:group:` is not a
    /// prefix of `qk:groups:`, so a prefix read of either cannot see the other.
    /// The ninth character is the whole of it — `s` against `:`.
    #[test]
    fn the_two_group_key_spaces_cannot_see_each_other() {
        let offsets = group_prefix("g");
        let index = index_key("g").unwrap();
        assert_eq!(offsets, "qk:group:g:");
        assert_eq!(index, "qk:groups:g");
        assert!(!index.starts_with(&offsets), "{index} is under {offsets}");
        assert!(
            !index.starts_with(KEY_PREFIX),
            "{index} is under {KEY_PREFIX}"
        );
        assert!(!offsets.starts_with(INDEX_PREFIX));
        // ...and the general case: no offset key of any group is under the
        // index prefix, including the group literally called `s:orders`, which
        // is the name that would compose one if the separator were not escaped.
        for group in ["g", "s", "s:orders", "groups", ""] {
            let offset_key = key(group, "orders", 0).unwrap();
            assert!(
                !offset_key.starts_with(INDEX_PREFIX),
                "{group}: {offset_key}"
            );
            assert!(!index_key(group).unwrap().starts_with(&group_prefix(group)));
        }
    }

    #[test]
    fn an_index_key_names_its_group_and_reads_back() {
        assert_eq!(
            index_key("orders-consumer").unwrap(),
            "qk:groups:orders-consumer"
        );
        for group in ["orders-consumer", "with space", "a:b", "nul\0byte", "100%"] {
            let k = index_key(group).unwrap();
            assert!(k.bytes().all(|b| b.is_ascii_graphic()), "{group} → {k}");
            assert_eq!(parse_index_key(&k).as_deref(), Some(group));
        }
        // A key past the column bound is refused rather than truncated, same
        // rule as an offset key.
        assert!(index_key(&" ".repeat(250)).is_none());
        assert_eq!(parse_index_key("qk:group:g:orders:0"), None);
    }

    /// The index is written, read back by prefix, and read back by key — and a
    /// simple consumer's empty protocol type survives the round trip as an
    /// empty string rather than becoming a plausible `consumer`.
    #[tokio::test]
    async fn the_index_round_trips() {
        let api = FakeQueen::with(&[]);
        index(&*api, "orders-consumer", "consumer", None)
            .await
            .unwrap();
        index(&*api, "simple", "", None).await.unwrap();
        // ...and an offset commit under one of them, which must not appear in
        // the index listing.
        store(
            &*api,
            &[(key("orders-consumer", "orders", 0).unwrap(), commit(1))],
            None,
        )
        .await;

        let (mut listed, truncated) = list_index(&*api, None, 1_000).await.unwrap();
        listed.sort_by(|a, b| a.0.cmp(&b.0));
        assert!(!truncated);
        assert_eq!(
            listed,
            [
                (
                    "orders-consumer".to_string(),
                    Indexed {
                        protocol_type: "consumer".to_string()
                    }
                ),
                (
                    "simple".to_string(),
                    Indexed {
                        protocol_type: String::new()
                    }
                ),
            ]
        );

        let by_key = load_index(
            &*api,
            &[
                "orders-consumer".to_string(),
                "never-existed".to_string(),
                "simple".to_string(),
            ],
            None,
        )
        .await
        .unwrap();
        assert_eq!(by_key[0].as_ref().unwrap().protocol_type, "consumer");
        assert_eq!(by_key[1], None, "a group nobody indexed came back indexed");
        assert_eq!(by_key[2].as_ref().unwrap().protocol_type, "");
    }

    /// The bound is the bound: a listing past `max` stops and SAYS it stopped,
    /// because ListGroups has no truncation flag on the wire.
    #[tokio::test]
    async fn a_listing_past_the_bound_says_it_was_cut() {
        let api = FakeQueen::with(&[]);
        for i in 0..10 {
            index(&*api, &format!("g{i}"), "consumer", None)
                .await
                .unwrap();
        }
        let (listed, truncated) = list_index(&*api, None, 4).await.unwrap();
        assert_eq!(listed.len(), 4);
        assert!(truncated);
    }

    /// A group index wider than one page is walked to the end.
    #[tokio::test]
    async fn a_wide_index_is_paged_to_the_end() {
        let api = FakeQueen::with(&[]);
        for i in 0..300 {
            index(&*api, &format!("g{i:04}"), "consumer", None)
                .await
                .unwrap();
        }
        let (listed, truncated) = list_index(&*api, None, 10_000).await.unwrap();
        assert_eq!(listed.len(), 300);
        assert!(!truncated);
    }

    /// A delete removes every offset of one group and its index row, and
    /// nothing of any other group's.
    #[tokio::test]
    async fn a_delete_removes_one_group_and_only_that_group() {
        let api = FakeQueen::with(&[]);
        let mut pairs = Vec::new();
        for (group, topics) in [("g", ["orders", "clicks"]), ("other", ["orders", "clicks"])] {
            for topic in topics {
                for p in 0..3 {
                    pairs.push((key(group, topic, p).unwrap(), commit(i64::from(p))));
                }
            }
        }
        store(&*api, &pairs, None).await;
        index(&*api, "g", "consumer", None).await.unwrap();
        index(&*api, "other", "consumer", None).await.unwrap();

        let removed = delete_group(&*api, "g", None).await.unwrap();
        assert_eq!(removed, 7, "six offsets and one index row");

        assert!(load_group(&*api, "g", None).await.unwrap().is_empty());
        assert_eq!(
            load_index(&*api, &["g".to_string()], None).await.unwrap()[0],
            None
        );
        // The neighbour is untouched, offsets and index alike.
        assert_eq!(load_group(&*api, "other", None).await.unwrap().len(), 6);
        assert!(load_index(&*api, &["other".to_string()], None)
            .await
            .unwrap()[0]
            .is_some());

        // Idempotent: a second delete removes nothing and does not fail, which
        // is what makes a partially failed delete re-runnable.
        assert_eq!(delete_group(&*api, "g", None).await.unwrap(), 0);
    }

    /// A group with more offsets than one page is deleted to the end, and the
    /// cursor walk terminates.
    #[tokio::test]
    async fn a_wide_group_is_deleted_to_the_end() {
        let api = FakeQueen::with(&[]);
        let pairs: Vec<(String, Committed)> = (0..300)
            .map(|p| (key("g", "orders", p).unwrap(), commit(i64::from(p))))
            .collect();
        store(&*api, &pairs, None).await;
        assert_eq!(delete_group(&*api, "g", None).await.unwrap(), 300);
        assert!(load_group(&*api, "g", None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_failed_index_call_is_an_error_and_not_a_silent_success() {
        let api = FakeQueen::with(&[]);
        api.fail_kv(Error::Transport("reset".into()));
        assert!(index(&*api, "g", "consumer", None).await.is_err());

        api.fail_kv(Error::Transport("reset".into()));
        assert!(list_index(&*api, None, 10).await.is_err());

        api.fail_kv(Error::Transport("reset".into()));
        assert!(delete_group(&*api, "g", None).await.is_err());
    }

    #[tokio::test]
    async fn the_token_reaches_the_store() {
        let api = FakeQueen::with(&[]);
        store(
            &*api,
            &[(key("g", "orders", 0).unwrap(), commit(1))],
            Some("tenant-a"),
        )
        .await;
        load(&*api, &[key("g", "orders", 0).unwrap()], Some("tenant-a"))
            .await
            .unwrap();
        let tokens = api.tokens.lock().unwrap().clone();
        assert!(!tokens.is_empty());
        assert!(tokens.iter().all(|t| t.as_deref() == Some("tenant-a")));
    }
}
