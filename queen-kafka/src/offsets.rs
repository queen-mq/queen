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
//! ## Offsets never expire
//!
//! Every KV write must declare an expiry and this one's is "forever"
//! ([`crate::queen::KvOp::Put`]). A committed offset that expired would be a
//! consumer group that silently resumes from `auto.offset.reset` — a replay of
//! the whole topic, or a jump to its end past everything it had not read.
//!
//! The consequence is stated rather than hidden: Kafka expires the offsets of a
//! group that has been empty for `offsets.retention.minutes` (7 days by
//! default) and this does not, so a facade that has served a million throwaway
//! group ids holds a million keys until something deletes them. There is no
//! DeleteGroups in this milestone to do it with; the key layout is what makes
//! it a one-line prefix delete when there is.

use kafka_protocol::error::ResponseError;

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

/// Store one commit's worth of offsets, chunked against the broker's op
/// ceiling. Answers one result per input pair, aligned by index: a chunk that
/// failed fails exactly its own members, so a wide commit does not report
/// success for partitions whose call never landed.
pub async fn store(
    api: &dyn QueenApi,
    pairs: &[(String, Committed)],
    token: Option<&str>,
) -> Vec<queen::Result<()>> {
    let mut out = Vec::with_capacity(pairs.len());
    for chunk in pairs.chunks(COMMITS_PER_CALL) {
        let ops: Vec<KvOp> = chunk
            .iter()
            .map(|(key, value)| KvOp::put(NAMESPACE, key, value.to_value()))
            .collect();
        match api.kv(&ops, token).await {
            // A `put` with no precondition cannot fail to apply, so anything
            // other than `applied` is a broker that changed under us rather
            // than a verdict — reported, never assumed to have written.
            Ok(answers) => out.extend(answers.into_iter().map(|a| match a.applied {
                Some(true) => Ok(()),
                other => Err(queen::Error::Body(format!(
                    "kv put answered applied={other:?}"
                ))),
            })),
            Err(e) => out.extend(chunk.iter().map(|_| Err(e.clone()))),
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queen::testing::FakeQueen;
    use crate::queen::Error;

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
