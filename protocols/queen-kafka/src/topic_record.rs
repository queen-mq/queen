//! The facade's own record of what it last told Queen about a topic.
//!
//! ## Why this module exists at all
//!
//! `POST /api/v1/configure` is a WHOLE-ROW upsert. `configure_queue_v1` reads
//! nineteen option keys, each through `COALESCE(p_options->>'k', <default>)`,
//! and then rewrites **every** column from `EXCLUDED`
//! (server/sql/procedures/012_configure.sql). So a partial configure on a live
//! queue does not "set one field": it resets the other eighteen to the stored
//! procedure's defaults, including `dedupWindowSeconds` (3600) and
//! `retentionEnabled` (false). Sending "just set `retention.ms`" would silently
//! throw away a tenant's dedup window.
//!
//! And a read-modify-write is not available instead: Queen exposes no HTTP read
//! of those columns. `GET /api/v1/status/queues/:queue` answers six of the
//! nineteen (`leaseTime`, `retryLimit`, `retryDelay`, `ttl`, `maxQueueSize`,
//! `deadLetterQueue`) and the other thirteen — retention among them — cannot be
//! read back through any route.
//!
//! What the facade CAN know is what it itself sent. Every options bag this
//! facade has ever posted for a topic came out of [`crate::topic_config::apply`],
//! so for a queue the FACADE created the invariant "*every column not in the bag
//! the facade sent is at the stored procedure's default*" is true by
//! construction. This module persists that bag, in Queen KV beside the offsets,
//! and two things fall out of it at once:
//!
//!   * an alter is `stored ∪ delta`, posted as one whole bag — lossless, because
//!     the bag is complete by construction rather than by a read;
//!   * a describe can report `retention.ms` from the same record, which is what
//!     closes the round trip DescribeConfigs has been missing
//!     ([`crate::handlers::describe_configs`]).
//!
//! A queue the facade did NOT create has no record, and is answered exactly as
//! it was before: retention omitted, alter refused, with a sentence saying why.
//! That refusal is the point — the alternative, "assume the defaults", is the
//! silent reset above.
//!
//! ## The layout
//!
//! ```text
//!   namespace: queen-kafka                        (offsets::NAMESPACE, shared)
//!   key:       qk:topiccfg:<topic>
//!   value:     {"qid": "<queue uuid>",
//!               "set": {"retentionEnabled": true, "retentionSeconds": 604800},
//!               "at":  1787824800123}
//!   expiry:    forever
//! ```
//!
//! `forever`, for the same reason a committed offset is: a record that expired
//! would silently turn a tracked topic into an untracked one, and a describe
//! would go blank again with nothing to explain it. [`remove`] is what takes a
//! record away, and DeleteTopics is what calls it.
//!
//! ## `qid`, and what it catches
//!
//! `qid` is the queue's `id` as the queue LIST reports it ([`queen::Queue::id`]),
//! taken AFTER the configure that the record describes. It is compared for
//! equality and nothing else. A record whose `qid` does not match the queue that
//! is there now describes a queue that was dropped and recreated under the same
//! name, and reporting its retention would be reporting a value nothing enforces.
//!
//! The comparison is over `Option`s on both sides, and `None == None` is a
//! match. That is deliberate and it is a documented degradation, not an
//! oversight: a Queen that does not report an id in the queue list gives this
//! facade nothing to pin to, and a record written against such a list records
//! that fact rather than inventing one. Where ids are reported — which is every
//! Queen that has `018_stats.sql` — the check is real.
//!
//! ## Absence is the safe direction, everywhere
//!
//! A record that cannot be read (the key is gone, the batch was truncated, the
//! stored JSON is not ours) is treated as ABSENT. That is deliberate and it is
//! why the failure mode is bounded: on the describe side absence omits
//! `retention.ms`, and on the alter side absence refuses the alter. Neither
//! reports nor writes a value from a guess.

use std::collections::HashMap;

use serde_json::{Map, Value};

use crate::offsets::NAMESPACE;
use crate::queen::{self, KvOp, QueenApi};

/// The prefix every config record lives under.
///
/// It shares [`NAMESPACE`] with the offsets (`qk:group:`), the durable group
/// index (`qk:groups:`), the group fences (`qk:fence:`) and the node registry
/// (`qk:node:`). None of those is a prefix of this one and this one is a prefix
/// of none of them, which is the property the `qk:` shape was designed for and
/// which [`tests::the_key_space_cannot_see_the_others`] pins.
const KEY_PREFIX: &str = "qk:topiccfg:";

/// Ceiling on one key, in bytes — Postgres-side, `queen.kv_check_names_v1`.
const MAX_KEY_BYTES: usize = 512;

/// Records read in one call to Queen.
///
/// Sized so the stored procedure's [`queen::MAX_KV_READ_BYTES`] budget cannot
/// bind: a record is a uuid, a handful of small numbers and a timestamp — a few
/// hundred bytes — so 128 of them is well inside 4 MiB, and it is under
/// [`queen::MAX_KV_KEYS_PER_CALL`] with room to spare. An ordinary
/// DescribeConfigs names a handful of topics and takes ONE call; the chunking
/// exists so that a client naming two thousand topics gets a right answer
/// instead of a batch the broker refuses outright.
const KEYS_PER_CALL: usize = 128;

/// The key one topic's record lives under.
///
/// **No escaping, and that is a precondition rather than a shortcut.** Every
/// caller validates the name through [`crate::handlers::metadata::not_a_topic_here`]
/// or `reserved_or_invalid` first, so `topic` is `[A-Za-z0-9._-]{1,249}` — a
/// charset with no separator in it and no byte Postgres `TEXT` cannot hold — and
/// the composed key is at most 261 bytes, inside [`MAX_KEY_BYTES`]. Composing a
/// key from an unvalidated name is a bug in the caller, not an escaping problem
/// here — which is why the precondition is asserted rather than commented: the
/// day a caller skips the name rule, the tests say so instead of the store
/// answering a constraint violation in production.
pub fn key(topic: &str) -> String {
    let key = format!("{KEY_PREFIX}{topic}");
    debug_assert!(
        key.len() <= MAX_KEY_BYTES,
        "a config record key of {} bytes was composed from an unvalidated topic name",
        key.len()
    );
    key
}

/// What the facade last applied to one topic.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Record {
    /// The queue's `id` at the time of the write. See the module header for
    /// what `None` on either side of the comparison means.
    pub qid: Option<String>,
    /// ONLY the keys the facade applied, spelled exactly as
    /// `POST /api/v1/configure` takes them — `retentionEnabled`,
    /// `retentionSeconds`, and `maxSize` rather than `maxQueueSize` if it ever
    /// grows. It is never the stored procedure's echo: reading that back would
    /// need `create_queue_with` to return a body.
    pub set: Map<String, Value>,
    /// When the record was written, epoch milliseconds. Nothing on the Kafka
    /// side reads it; it is here because a stored record that cannot be dated is
    /// one nobody can reason about when they are looking at why a topic reports
    /// the retention it reports.
    pub at: i64,
}

impl Record {
    /// A record for the bag `set`, pinned to `qid`, stamped now.
    pub fn new(qid: Option<String>, set: Map<String, Value>) -> Record {
        Record {
            qid,
            set,
            at: now_millis(),
        }
    }

    fn to_value(&self) -> Value {
        serde_json::json!({
            "qid": self.qid,
            "set": Value::Object(self.set.clone()),
            "at": self.at,
        })
    }

    /// Read a stored value back, or `None` for anything that is not one of
    /// ours. See the module header: absence is the safe direction, so a record
    /// this facade cannot parse is one it does not have.
    fn from_value(v: &Value) -> Option<Record> {
        Some(Record {
            qid: v.get("qid").and_then(|q| q.as_str()).map(str::to_string),
            set: v.get("set")?.as_object()?.clone(),
            at: v.get("at").and_then(|t| t.as_i64()).unwrap_or_default(),
        })
    }

    /// Whether this record describes the queue that is there NOW.
    ///
    /// `live` is [`queen::Queue::id`] from the catalog. Equality of `Option`s,
    /// nothing more: the id is opaque here and is never ordered, parsed or
    /// interpreted.
    pub fn describes(&self, live: Option<&str>) -> bool {
        self.qid.as_deref() == live
    }
}

/// `stored` with `delta` applied: a `Some` value sets the key, a `None` removes
/// it.
///
/// This is IncrementalAlterConfigs' shape, in one place, because it is the step
/// that decides whether a key the caller did not mention survives — and the
/// whole module exists so that it does.
pub fn merge(stored: &Map<String, Value>, delta: &[(String, Option<Value>)]) -> Map<String, Value> {
    let mut out = stored.clone();
    for (name, value) in delta {
        match value {
            Some(v) => {
                out.insert(name.clone(), v.clone());
            }
            None => {
                out.remove(name);
            }
        }
    }
    out
}

/// Write one topic's record, replacing whatever was there.
///
/// Unconditional and `forever`. There is no fence: this is topic-addressed
/// rather than group-addressed, two nodes altering one topic is last-writer-wins
/// — which is exactly what Apache Kafka's AlterConfigs is, having no optimistic
/// concurrency of its own — and a precondition here would refuse a write Kafka
/// would have taken.
pub async fn store(
    api: &dyn QueenApi,
    topic: &str,
    record: &Record,
    token: Option<&str>,
) -> queen::Result<()> {
    store_many(
        api,
        std::slice::from_ref(&(topic.to_string(), record.clone())),
        token,
    )
    .await
}

/// The same, for a whole batch — a CreateTopics naming a hundred topics is ONE
/// call here rather than a hundred, which matters because the Kafka connection
/// that asked is muted until the whole response is written (conn.rs).
pub async fn store_many(
    api: &dyn QueenApi,
    records: &[(String, Record)],
    token: Option<&str>,
) -> queen::Result<()> {
    for chunk in records.chunks(queen::MAX_KV_OPS_PER_CALL) {
        let ops: Vec<KvOp> = chunk
            .iter()
            .map(|(topic, record)| KvOp::put(NAMESPACE, &key(topic), record.to_value()))
            .collect();
        api.kv(&ops, token).await?;
    }
    Ok(())
}

/// Remove one topic's record.
///
/// `applied: false` is not an error: it means the key was not there, which for
/// a record means the topic was never tracked, and the caller's job — a delete,
/// or the roll-back of a failed write — is done either way.
pub async fn remove(api: &dyn QueenApi, topic: &str, token: Option<&str>) -> queen::Result<()> {
    remove_many(api, std::slice::from_ref(&topic.to_string()), token).await
}

/// The same, for a whole batch. See [`store_many`] for why the batch exists.
pub async fn remove_many(
    api: &dyn QueenApi,
    topics: &[String],
    token: Option<&str>,
) -> queen::Result<()> {
    for chunk in topics.chunks(queen::MAX_KV_OPS_PER_CALL) {
        let ops: Vec<KvOp> = chunk
            .iter()
            .map(|topic| KvOp::delete(NAMESPACE, &key(topic), None))
            .collect();
        api.kv(&ops, token).await?;
    }
    Ok(())
}

/// The records for `topics`, keyed by topic name.
///
/// A topic with no record is simply absent from the map — see the module header
/// for why that is the same answer as a record that could not be read.
pub async fn load_many(
    api: &dyn QueenApi,
    topics: &[String],
    token: Option<&str>,
) -> queen::Result<HashMap<String, Record>> {
    let mut out = HashMap::with_capacity(topics.len());
    for chunk in topics.chunks(KEYS_PER_CALL) {
        let keys: Vec<String> = chunk.iter().map(|t| key(t)).collect();
        let ops = [KvOp::GetMany {
            ns: NAMESPACE.to_string(),
            keys,
        }];
        let answers = api.kv(&ops, token).await?;
        let answer = answers
            .into_iter()
            .next()
            .ok_or_else(|| queen::Error::Body("kv answered nothing for a getMany".to_string()))?;
        for row in &answer.rows {
            let Some(topic) = row.key.strip_prefix(KEY_PREFIX) else {
                // A key under our namespace that is not one of ours. Named and
                // skipped rather than guessed at.
                tracing::warn!(target: "kafka", key = %row.key, "unreadable topic config key");
                continue;
            };
            if let Some(record) = Record::from_value(&row.value) {
                out.insert(topic.to_string(), record);
            } else {
                tracing::warn!(
                    target: "kafka",
                    topic = %topic,
                    "a topic config record could not be read; the topic is treated as untracked"
                );
            }
        }
    }
    Ok(out)
}

/// Wall-clock milliseconds. Not tokio's: this is a timestamp that is STORED and
/// read by a person, and a paused test clock would write 1970 into the database.
fn now_millis() -> i64 {
    std::time::UNIX_EPOCH
        .elapsed()
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queen::testing::FakeQueen;
    use serde_json::json;

    fn bag(pairs: &[(&str, Value)]) -> Map<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    /// The four other key spaces under [`NAMESPACE`], and the property that
    /// makes them safe to share it: neither direction of `starts_with` holds
    /// between any pair. A prefix walk over one can therefore never see the
    /// others, which is what `qk:groups:` versus `qk:group:` was already
    /// written to guarantee.
    #[test]
    fn the_key_space_cannot_see_the_others() {
        for other in ["qk:group:", "qk:groups:", "qk:fence:", "qk:node:"] {
            assert!(
                !KEY_PREFIX.starts_with(other),
                "{KEY_PREFIX} lives under {other}"
            );
            assert!(
                !other.starts_with(KEY_PREFIX),
                "{other} lives under {KEY_PREFIX}"
            );
        }
    }

    /// The widest legal topic name still composes a key the store accepts, which
    /// is what makes [`key`]'s "no escaping" precondition safe rather than
    /// merely convenient.
    #[test]
    fn a_maximum_length_topic_name_composes_a_key_the_store_accepts() {
        let widest = "t".repeat(249);
        let composed = key(&widest);
        assert_eq!(composed.len(), KEY_PREFIX.len() + 249);
        assert!(
            composed.len() <= MAX_KEY_BYTES,
            "{} bytes is past the store's column",
            composed.len()
        );
    }

    #[test]
    fn merge_lets_the_delta_win_and_a_delete_removes_the_key() {
        let stored = bag(&[
            ("retentionEnabled", json!(true)),
            ("retentionSeconds", json!(60)),
        ]);

        // SET wins over what is stored, and leaves the rest alone.
        let set = merge(
            &stored,
            &[("retentionSeconds".to_string(), Some(json!(604_800)))],
        );
        assert_eq!(set["retentionEnabled"], json!(true));
        assert_eq!(set["retentionSeconds"], json!(604_800));

        // DELETE removes the key rather than writing a null, so the bag posted
        // to `/configure` does not name it at all and the stored procedure's own
        // default is what takes effect.
        let deleted = merge(
            &stored,
            &[
                ("retentionSeconds".to_string(), None),
                ("retentionEnabled".to_string(), Some(json!(false))),
            ],
        );
        assert!(!deleted.contains_key("retentionSeconds"));
        assert_eq!(deleted["retentionEnabled"], json!(false));

        // A delta naming nothing changes nothing.
        assert_eq!(merge(&stored, &[]), stored);
    }

    /// The staleness check, over both sides being optional. `None == None` is a
    /// match and that is the documented degradation for a Queen that reports no
    /// id; every other pairing is a mismatch.
    #[test]
    fn a_record_describes_only_the_queue_it_was_pinned_to() {
        let pinned = Record::new(Some("uuid-a".to_string()), Map::new());
        assert!(pinned.describes(Some("uuid-a")));
        assert!(!pinned.describes(Some("uuid-b")), "a recreated queue");
        assert!(!pinned.describes(None), "an id that stopped being reported");

        let unpinned = Record::new(None, Map::new());
        assert!(unpinned.describes(None));
        assert!(!unpinned.describes(Some("uuid-a")));
    }

    /// A round trip through the store, in the shape every caller uses it.
    #[tokio::test]
    async fn a_record_round_trips_through_the_store() {
        let api = FakeQueen::with(&[]);
        let record = Record::new(
            Some("uuid-a".to_string()),
            bag(&[
                ("retentionEnabled", json!(true)),
                ("retentionSeconds", json!(604_800)),
            ]),
        );
        store(api.as_ref(), "orders", &record, None).await.unwrap();

        let loaded = load_many(api.as_ref(), &["orders".to_string()], None)
            .await
            .unwrap();
        assert_eq!(loaded["orders"], record);

        remove(api.as_ref(), "orders", None).await.unwrap();
        assert!(load_many(api.as_ref(), &["orders".to_string()], None)
            .await
            .unwrap()
            .is_empty());
    }

    /// Many topics, ONE call — the shape a UI's DescribeConfigs fan-out sends.
    #[tokio::test]
    async fn many_topics_cost_one_call() {
        let api = FakeQueen::with(&[]);
        let topics: Vec<String> = (0..20).map(|i| format!("t{i}")).collect();
        load_many(api.as_ref(), &topics, None).await.unwrap();
        assert_eq!(api.kv_calls.lock().unwrap().len(), 1);
    }

    /// A stored value that is not one of ours reads as ABSENT rather than as a
    /// record with empty contents: absence omits and refuses, and a
    /// half-understood record would write.
    #[tokio::test]
    async fn a_value_that_is_not_ours_reads_as_absent() {
        let api = FakeQueen::with(&[]);
        api.kv_seed(NAMESPACE, &key("orders"), json!({"something": "else"}));
        assert!(load_many(api.as_ref(), &["orders".to_string()], None)
            .await
            .unwrap()
            .is_empty());
    }
}
