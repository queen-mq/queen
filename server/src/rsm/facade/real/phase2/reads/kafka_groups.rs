//! The Kafka facade's consumer groups and topics, as the console's reads
//! report them.
//!
//! A Kafka consumer reads through the facade without a lease: it fetches by
//! offset and commits a POSITION (`positions.rs`), so nothing the planner
//! keeps on a cursor says it is there. `total_consumed` stays 0, and the
//! group listing's state rule called every Kafka group `Dead` while it read
//! with a 2 s lag (stage, 2026-10-19). Membership is the facade's: its group
//! coordinator holds the members that joined and heartbeat. So the facade
//! publishes it, in the tenant's KV beside the rest of its own state, and
//! this module reads it.
//!
//! ```text
//!   namespace  queen-kafka
//!   qk:groups:<esc group>   the group INDEX: the group exists and is a Kafka
//!                           group (queen-kafka offsets.rs), written on its
//!                           first commit
//!   qk:live:<esc group>     PRESENCE (queen-kafka presence.rs), rewritten
//!                           while members heartbeat (every
//!                           QUEEN_KAFKA_GROUP_PRESENCE_MS, at once when the
//!                           membership changes) and by the commits of a group
//!                           with no members (an assign()-based consumer)
//!       {"v":1,"members":3,"topics":{"orders":3},"committing":false,
//!        "freshMs":45000,"state":"Stable","source":"heartbeat","at":…}
//!   qk:topiccfg:<topic>     the facade's TOPIC RECORD (queen-kafka
//!                           topic_record.rs): the queue was created through
//!                           the facade, pinned to its id by `qid`
//! ```
//!
//! A presence row says a queue is being read while it is FRESH — written no
//! more than its `freshMs` ago on the BROKER's clock (the row's own
//! `updated_at_us`, stamped by the planner; the facade's `at` is never read) —
//! and it counts a member subscribed to that queue, or says a group with no
//! members is committing. A facade that dies stops writing and its groups go
//! stale on their own; a clean shutdown (LeaveGroup) writes zero members at
//! once. A row this module cannot read is absent: the group is then reported
//! exactly as it was before presence existed.
//!
//! Nothing here is replicated state. It is read at request time, so the
//! answer changes and the log does not.

use std::collections::{BTreeMap, HashMap};

use serde_json::Value;

use crate::rsm::store::rows::KvRow;
use crate::rsm::store::TypedReads;

/// The facade's KV namespace (queen-kafka offsets.rs `NAMESPACE`).
const NAMESPACE: &str = "queen-kafka";

/// queen-kafka offsets.rs `INDEX_PREFIX`.
const INDEX_PREFIX: &str = "qk:groups:";

/// queen-kafka presence.rs `KEY_PREFIX`.
const PRESENCE_PREFIX: &str = "qk:live:";

/// queen-kafka topic_record.rs `KEY_PREFIX`. A topic name is
/// `[A-Za-z0-9._-]`, so the key is not escaped.
const TOPIC_PREFIX: &str = "qk:topiccfg:";

/// The fresh window a row that names none is read with: three of the
/// facade's default 15 s writes.
const FRESH_DEFAULT_MS: i64 = 45_000;

/// The window a row may name, clamped: under 5 s a write delayed behind the
/// data path would read as a dead group, and past 10 min a group that died
/// would read as live for longer than anyone would call a failover.
const FRESH_MIN_MS: i64 = 5_000;
const FRESH_MAX_MS: i64 = 600_000;

/// The key spelling of a group id, as the facade writes it (queen-kafka
/// offsets.rs `escape`): a group id is an arbitrary string, so every byte
/// outside `[A-Za-z0-9._-]` is percent-encoded.
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

/// `group`'s index key.
pub(super) fn index_key(group: &str) -> String {
    format!("{INDEX_PREFIX}{}", escape(group))
}

/// `group`'s presence key.
pub(super) fn presence_key(group: &str) -> String {
    format!("{PRESENCE_PREFIX}{}", escape(group))
}

/// One group's presence row, as read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Presence {
    /// The group's members when the row was written.
    members: u64,
    /// Members per subscribed topic (a topic is a queue of the same name);
    /// `None` when the facade could not read every member's subscription,
    /// and the group's members then count for every queue it has a cursor on.
    topics: Option<BTreeMap<String, u64>>,
    /// Written by the commit of a group with no members: an assign()-based
    /// consumer, which never joins and is live while it commits.
    committing: bool,
    /// The row's `updated_at_us`: the broker's time of the write.
    seen_at_us: i64,
    fresh_us: i64,
}

/// What a presence row says about one of the group's queues, at one instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Liveness {
    /// The queue is being read for the group: a member subscribed to it, or
    /// a group with no members committing, and the row is fresh.
    pub(super) live: bool,
    /// Members subscribed to the queue while the row is fresh, else 0.
    pub(super) live_members: u64,
    /// When the facade last said the group was alive and reading the queue;
    /// `None` when its row does not name the queue — the group's members no
    /// longer subscribe to it, and nothing dates when they stopped.
    pub(super) last_seen_us: Option<i64>,
}

impl Liveness {
    /// Seconds since the queue was last seen read: 0 while it is, never 0
    /// when it is not (a group that left half a second ago is not live), and
    /// `None` when nothing dates it.
    pub(super) fn inactive_seconds(&self, now_us: i64) -> Option<i64> {
        if self.live {
            return Some(0);
        }
        let seen = self.last_seen_us?;
        let us = now_us.saturating_sub(seen).max(0) as u64;
        Some(us.div_ceil(1_000_000).max(1) as i64)
    }
}

impl Presence {
    /// A row the facade wrote, or `None` when it is not one: not JSON, not
    /// version 1, or without a member count.
    pub(super) fn parse(row: &KvRow) -> Option<Presence> {
        let v: Value = serde_json::from_slice(&row.value).ok()?;
        let o = v.as_object()?;
        if o.get("v").and_then(Value::as_u64) != Some(1) {
            return None;
        }
        let members = o.get("members")?.as_u64()?;
        // A map with one count that is not one is a map nobody can trust for
        // the others either: unknown, so the members count everywhere.
        let topics = match o.get("topics") {
            Some(Value::Object(m)) => m
                .iter()
                .map(|(t, n)| n.as_u64().map(|n| (t.clone(), n)))
                .collect::<Option<BTreeMap<_, _>>>(),
            _ => None,
        };
        let committing = o
            .get("committing")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let fresh_ms = o
            .get("freshMs")
            .and_then(Value::as_i64)
            .unwrap_or(FRESH_DEFAULT_MS)
            .clamp(FRESH_MIN_MS, FRESH_MAX_MS);
        Some(Presence {
            members,
            topics,
            committing,
            seen_at_us: row.updated_at_us,
            fresh_us: fresh_ms * 1_000,
        })
    }

    /// The row's verdict on `queue` at `now_us`. A row stamped after `now_us`
    /// (another node's clock ahead of this one's) is fresh.
    pub(super) fn for_queue(&self, queue: &str, now_us: i64) -> Liveness {
        let fresh = now_us.saturating_sub(self.seen_at_us) <= self.fresh_us;
        let (named, members) = match &self.topics {
            None => (true, self.members),
            Some(t) => match t.get(queue) {
                Some(&n) => (true, n),
                None => (false, 0),
            },
        };
        Liveness {
            live: fresh && named && (members > 0 || self.committing),
            live_members: if fresh { members } else { 0 },
            last_seen_us: named.then_some(self.seen_at_us),
        }
    }
}

/// What the facade's rows say about one group.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct KafkaGroup {
    /// `None` when the facade has written no presence for it: a facade
    /// older than presence, presence turned off, or a group that only ever
    /// committed through an older one.
    pub(super) presence: Option<Presence>,
}

impl KafkaGroup {
    /// The verdict on one of the group's queues, or `None` without presence.
    pub(super) fn on(&self, queue: &str, now_us: i64) -> Option<Liveness> {
        self.presence.as_ref().map(|p| p.for_queue(queue, now_us))
    }
}

/// The Kafka groups among `groups`, by name: a group the facade's index or
/// presence names. Two point reads per group; queue mode is never one.
pub(super) fn kafka_groups<'a, R: TypedReads + ?Sized>(
    r: &R,
    tenant: &str,
    groups: impl IntoIterator<Item = &'a str>,
    now_us: i64,
) -> crate::rsm::store::Result<HashMap<String, KafkaGroup>> {
    let mut out = HashMap::new();
    for group in groups {
        if group == "__QUEUE_MODE__" || out.contains_key(group) {
            continue;
        }
        let live_row = |key: String| -> crate::rsm::store::Result<Option<KvRow>> {
            Ok(r.kv(tenant, NAMESPACE, &key)?
                .filter(|row| row.live(now_us)))
        };
        let presence = live_row(presence_key(group))?;
        let indexed = live_row(index_key(group))?.is_some();
        if presence.is_none() && !indexed {
            continue;
        }
        out.insert(
            group.to_string(),
            KafkaGroup {
                presence: presence.as_ref().and_then(Presence::parse),
            },
        );
    }
    Ok(out)
}

/// The queues the facade created, by name, each with the queue id its record
/// is pinned to (`None` for a record written without one).
pub(in crate::rsm::facade::real::phase2) fn kafka_topics<R: TypedReads + ?Sized>(
    r: &R,
    tenant: &str,
    now_us: i64,
) -> crate::rsm::store::Result<HashMap<String, Option<String>>> {
    let mut out = HashMap::new();
    r.scan_kv(
        tenant,
        NAMESPACE,
        TOPIC_PREFIX,
        None,
        usize::MAX,
        &mut |key, row| {
            if !row.live(now_us) {
                return true;
            }
            let Some(topic) = key.strip_prefix(TOPIC_PREFIX) else {
                return true;
            };
            let Ok(v) = serde_json::from_slice::<Value>(&row.value) else {
                return true;
            };
            let qid = v.get("qid").and_then(Value::as_str).map(str::to_string);
            // A record written AHEAD of its queue's create has no id yet and
            // may belong to a create that died: it marks nothing.
            let pending = v.get("pending").and_then(Value::as_bool).unwrap_or(false);
            if qid.is_some() || !pending {
                out.insert(topic.to_string(), qid);
            }
            true
        },
    )?;
    Ok(out)
}

/// Was the queue `name`, of id `id`, created through the facade? A record
/// pinned to another id describes a queue that was dropped and recreated
/// under the same name.
pub(in crate::rsm::facade::real::phase2) fn is_kafka_topic(
    topics: &HashMap<String, Option<String>>,
    name: &str,
    id: &str,
) -> bool {
    topics
        .get(name)
        .is_some_and(|qid| qid.as_deref().is_none_or(|q| q == id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const S: i64 = 1_000_000;

    fn row(value: Value, updated_at_us: i64) -> KvRow {
        KvRow {
            value: value.to_string().into_bytes(),
            version: 1,
            expires_at_us: None,
            created_at_us: updated_at_us,
            updated_at_us,
        }
    }

    fn presence(value: Value, at: i64) -> Presence {
        Presence::parse(&row(value, at)).expect("a presence row")
    }

    #[test]
    fn the_keys_are_the_facades() {
        assert_eq!(index_key("orders-consumer"), "qk:groups:orders-consumer");
        assert_eq!(presence_key("orders-consumer"), "qk:live:orders-consumer");
        // A group id is any string: the separator and anything outside the
        // topic charset are escaped exactly as the facade escapes them.
        assert_eq!(presence_key("team a:billing"), "qk:live:team%20a%3Abilling");
        assert_eq!(index_key("é"), "qk:groups:%C3%A9");
    }

    /// The server builds without the facade too, so the spelling is kept
    /// here twice. With it, the two must agree.
    #[cfg(feature = "kafka")]
    #[test]
    fn the_keys_agree_with_the_facade_that_writes_them() {
        for g in ["orders", "team a:billing", "lg.stage-24h-001.x", "é/%"] {
            assert_eq!(
                Some(index_key(g)),
                queen_kafka::offsets::index_key(g),
                "{g}"
            );
            assert_eq!(Some(presence_key(g)), queen_kafka::presence::key(g), "{g}");
        }
    }

    #[test]
    fn a_fresh_row_with_members_is_live_on_every_queue_when_topics_are_unknown() {
        let p = presence(json!({"v": 1, "members": 3, "freshMs": 45_000}), 100 * S);
        let l = p.for_queue("orders", 130 * S);
        assert_eq!(
            l,
            Liveness {
                live: true,
                live_members: 3,
                last_seen_us: Some(100 * S)
            }
        );
        assert_eq!(l.inactive_seconds(130 * S), Some(0));
    }

    #[test]
    fn a_row_goes_stale_after_its_window_on_the_brokers_clock() {
        let p = presence(json!({"v": 1, "members": 3, "freshMs": 45_000}), 100 * S);
        assert!(p.for_queue("orders", 145 * S).live, "at the edge");
        let l = p.for_queue("orders", 146 * S);
        assert!(!l.live);
        assert_eq!(l.live_members, 0);
        assert_eq!(l.last_seen_us, Some(100 * S));
        assert_eq!(l.inactive_seconds(146 * S), Some(46));
        // A row stamped ahead of this node's clock is fresh.
        assert!(p.for_queue("orders", 90 * S).live);
    }

    #[test]
    fn a_group_that_left_is_not_live_and_is_never_inactive_for_zero_seconds() {
        let p = presence(json!({"v": 1, "members": 0, "source": "leave"}), 100 * S);
        let l = p.for_queue("orders", 100 * S + 400_000);
        assert!(!l.live);
        assert_eq!(l.inactive_seconds(100 * S + 400_000), Some(1));
    }

    #[test]
    fn members_count_on_the_topics_they_subscribe_to() {
        let p = presence(
            json!({"v": 1, "members": 3, "topics": {"orders": 3, "audit": 1}}),
            100 * S,
        );
        assert_eq!(p.for_queue("orders", 101 * S).live_members, 3);
        assert_eq!(p.for_queue("audit", 101 * S).live_members, 1);
        // A queue the group still has a cursor on and no member reads.
        let gone = p.for_queue("legacy", 101 * S);
        assert!(!gone.live);
        assert_eq!(gone.live_members, 0);
        assert_eq!(gone.last_seen_us, None);
        assert_eq!(gone.inactive_seconds(101 * S), None);
    }

    #[test]
    fn a_group_with_no_members_is_live_while_it_commits() {
        let p = presence(
            json!({"v": 1, "members": 0, "committing": true, "topics": {"orders": 0}}),
            100 * S,
        );
        let l = p.for_queue("orders", 110 * S);
        assert!(l.live);
        assert_eq!(l.live_members, 0);
        assert!(!p.for_queue("orders", 200 * S).live, "stale");
        assert!(
            !p.for_queue("audit", 110 * S).live,
            "a topic it did not commit"
        );
    }

    #[test]
    fn the_window_is_clamped_and_defaulted() {
        let tiny = presence(json!({"v": 1, "members": 1, "freshMs": 1}), 100 * S);
        assert!(tiny.for_queue("q", 104 * S).live, "at least 5 s");
        let huge = presence(
            json!({"v": 1, "members": 1, "freshMs": 86_400_000}),
            100 * S,
        );
        assert!(!huge.for_queue("q", 701 * S).live, "at most 10 min");
        let none = presence(json!({"v": 1, "members": 1}), 100 * S);
        assert!(none.for_queue("q", 145 * S).live);
        assert!(!none.for_queue("q", 146 * S).live);
    }

    #[test]
    fn a_row_that_is_not_one_is_absent() {
        for v in [
            json!("text"),
            json!({"members": 3}),
            json!({"v": 2, "members": 3}),
            json!({"v": 1}),
            json!({"v": 1, "members": -1}),
        ] {
            assert_eq!(Presence::parse(&row(v.clone(), S)), None, "{v}");
        }
        let raw = KvRow {
            value: b"not json".to_vec(),
            ..row(json!(null), S)
        };
        assert_eq!(Presence::parse(&raw), None);
        // A topic map with a count that is not one is unknown, not partial.
        let p = presence(
            json!({"v": 1, "members": 2, "topics": {"orders": 2, "audit": "x"}}),
            S,
        );
        assert_eq!(p.for_queue("legacy", S).live_members, 2);
    }

    #[test]
    fn a_topic_record_marks_the_queue_it_is_pinned_to() {
        let mut topics = HashMap::new();
        topics.insert("orders".to_string(), Some("id-1".to_string()));
        topics.insert("old".to_string(), None);
        assert!(is_kafka_topic(&topics, "orders", "id-1"));
        assert!(
            !is_kafka_topic(&topics, "orders", "id-2"),
            "dropped and recreated under the same name"
        );
        assert!(is_kafka_topic(&topics, "old", "anything"));
        assert!(!is_kafka_topic(&topics, "native", "id-3"));
    }
}
