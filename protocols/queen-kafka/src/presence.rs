//! Group PRESENCE: which consumer groups this facade sees alive, written where
//! the broker's console can read it.
//!
//! ## Why this exists
//!
//! The broker cannot see a Kafka consumer. A Kafka consumer fetches by offset
//! and commits a position; it takes no lease and acknowledges nothing, so the
//! cursor rows the broker keeps never say anyone is there. The console's group
//! listing read that as `Dead` — every Kafka group, while it was reading with
//! a 2 s lag (stage, 2026-10-19) — and the console then reported the topic as
//! having no reader. Membership is the one thing that knows, and it lives here,
//! in memory, in [`crate::coordinator`]: "offsets and existence are Queen's,
//! liveness is this process's" ([`crate::offsets`]). This module copies the
//! liveness out, and nothing else.
//!
//! ## The row
//!
//! ```text
//!   namespace: queen-kafka                       (offsets::NAMESPACE, shared)
//!   key:       qk:live:<esc group>
//!   value:     {"v": 1, "members": 3, "state": "Stable", "generation": 7,
//!               "protocolType": "consumer", "topics": {"orders": 3},
//!               "committing": false, "freshMs": 45000,
//!               "source": "heartbeat", "at": 1787824800123, "node": null}
//!   expiry:    forever; DeleteGroups removes it with the group
//! ```
//!
//! The broker reads it (server/src/rsm/facade/real/phase2/reads/kafka_groups.rs)
//! and judges it by ONE rule: the row is fresh while it was written no more
//! than `freshMs` ago, on the broker's clock — the planner stamps every KV row
//! with the time it applied it, so a facade host whose clock is wrong cannot
//! make a dead group look alive. `at` is this process's clock and is there
//! for a person reading the row, nobody else.
//!
//!   * `members` and `state` are the coordinator's, at the write.
//!   * `topics` counts the members subscribed to each topic, read from the
//!     subscription each member sent at JoinGroup ([`subscribed_topics`]).
//!     `null` when any member's could not be read, which the broker answers by
//!     counting the members on every queue the group has a cursor on.
//!   * `committing` marks the row of a group with NO members that commits: a
//!     simple, `assign()`-based consumer (Flink, Spark, every offset tool),
//!     which never joins and is alive exactly while it commits.
//!
//! ## When it is written
//!
//! On the request path of the member that proves the group alive, with that
//! member's own credential, so the row lands in the tenant the group's offsets
//! are in — and never on the path's critical section: the write is spawned and
//! the response does not wait for it.
//!
//!   * a Heartbeat or an OffsetCommit: once per `QUEEN_KAFKA_GROUP_PRESENCE_MS`
//!     per group, however many members heartbeat;
//!   * a SyncGroup (a generation completed — the membership changed): at
//!     once, at most once a second;
//!   * a LeaveGroup: at once when it leaves the group empty — a clean shutdown
//!     is visible in milliseconds — and otherwise like a sync.
//!
//! A member that dies without leaving writes nothing, and its group goes
//! stale `freshMs` after its last write: three intervals, so a write delayed
//! behind the data path (a KV write is a raft entry like any other) is not a
//! dead group. An empty group, a reaped actor, a restarted facade: nothing is
//! written, nothing needs to be.
//!
//! ## Cost
//!
//! One KV write per live group per interval: 70 groups at the default 15 s are
//! under five writes a second for the whole facade, against a broker whose
//! node registry already renews a row per node every two seconds
//! ([`crate::cluster::registry`]). Nothing is ever read here.
//!
//! ## What can be wrong, and for how long
//!
//! Two writes for one group are spawned tasks and may land out of order; the
//! next one corrects it, so a row is wrong for at most one interval. A write
//! that fails is logged, not retried: the next interval's is the retry, and
//! retrying on every heartbeat would turn a broker that refuses KV writes into
//! one that is asked for more of them.
//!
//! ## Off
//!
//! `QUEEN_KAFKA_GROUP_PRESENCE_MS=0` writes nothing, and the broker then
//! reports Kafka groups exactly as it did before this module existed.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::ConsumerProtocolSubscription;
use kafka_protocol::protocol::{Decodable, Message};
use serde_json::{json, Value};
use tokio::time::Instant;

use crate::coordinator::GroupDescription;
use crate::identity::TenantKey;
use crate::offsets::{self, NAMESPACE};
use crate::queen::KvOp;
use crate::Facade;

/// The prefix every presence row lives under. It shares [`NAMESPACE`] with the
/// offsets (`qk:group:`), the group index (`qk:groups:`), the fences
/// (`qk:fence:`), the node registry (`qk:node:`) and the topic records
/// (`qk:topiccfg:`): none of them is a prefix of this one, and this one is a
/// prefix of none of them.
const KEY_PREFIX: &str = "qk:live:";

/// The row's format. The broker ignores a row of any other.
const VERSION: u64 = 1;

/// `QUEEN_KAFKA_GROUP_PRESENCE_MS` when it is not set.
pub const DEFAULT_INTERVAL: Duration = Duration::from_millis(15_000);

/// The shortest interval the knob takes, other than 0 (off): below a second
/// a group of many members would write on nearly every heartbeat.
pub const MIN_INTERVAL_MS: u64 = 1_000;

/// How many intervals a row stays fresh.
const FRESH_INTERVALS: u32 = 3;

/// The closest two membership-change writes of one group come, so the joins
/// of a 40-member rebalance are one write and not forty.
const MIN_GAP: Duration = Duration::from_secs(1);

/// How many (tenant, group) pairs [`MARKS`] remembers: the coordinator's own
/// group cap, past which this facade is not coordinating the group anyway.
const MAX_MARKS: usize = 10_000;

/// One line per window while presence writes fail: a broker refusing KV
/// writes refuses every group's.
static FAILED: crate::obs::Sampler = crate::obs::Sampler::new(60_000);

/// `group`'s presence key, or `None` when the composed key is past the store's
/// key ceiling — such a group's row is not written, and the broker reports the
/// group as it did before presence existed.
pub fn key(group: &str) -> Option<String> {
    let key = format!("{KEY_PREFIX}{}", offsets::escape(group));
    (key.len() <= offsets::MAX_KEY_BYTES).then_some(key)
}

/// What proved the group alive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Heartbeat,
    Sync,
    Leave,
    Commit,
}

impl Source {
    fn name(self) -> &'static str {
        match self {
            Source::Heartbeat => "heartbeat",
            Source::Sync => "sync",
            Source::Leave => "leave",
            Source::Commit => "commit",
        }
    }

    /// How long after the group's last write this source writes again.
    /// `empty` is whether the group has no members now.
    fn gap(self, every: Duration, empty: bool) -> Duration {
        match self {
            Source::Heartbeat | Source::Commit => every,
            Source::Sync => MIN_GAP,
            Source::Leave if empty => Duration::ZERO,
            Source::Leave => MIN_GAP,
        }
    }
}

/// Note that `group` was proved alive by `source` on a connection holding
/// `token`, and write its row if one is due. Returns at once when none is;
/// never waits for the write.
pub async fn note(facade: &Facade, group: &str, source: Source, token: Option<&str>) {
    let every = facade.coordinator.config().presence;
    if every.is_zero() {
        return;
    }
    let Some(key) = key(group) else { return };
    let tenant = facade.catalog.tenant_key(token);
    // A Leave is decided after the describe: it is due at once when it emptied
    // the group, and that is not known yet.
    if !MARKS.due(&tenant, group, source.gap(every, true), Instant::now()) {
        return;
    }
    let described = facade.coordinator.describe_group(group).await;
    let members = described.as_ref().map_or(0, |d| d.members.len());
    let now = Instant::now();
    if !MARKS.due(&tenant, group, source.gap(every, members == 0), now) {
        return;
    }
    let row = Row::of(
        described.as_ref(),
        source,
        MARKS.topics(&tenant, group),
        every,
    );
    MARKS.mark(&tenant, group, now, row.topics.clone());
    let value = row.value(facade.cluster.state().map(|s| s.me.id));
    let api = Arc::clone(&facade.queen);
    let token = token.map(str::to_string);
    let group = group.to_string();
    tokio::spawn(async move {
        let ops = [KvOp::put(NAMESPACE, &key, value)];
        if let Err(e) = api.kv(&ops, token.as_deref()).await {
            if let Some(suppressed) = FAILED.tick_now() {
                tracing::warn!(
                    target: "kafka",
                    group,
                    error = %e,
                    suppressed,
                    "a consumer group's presence row was not written; the console will show the \
                     group as not live until a later write lands"
                );
            }
        }
    });
}

/// One row, before it is JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Row {
    members: u64,
    state: &'static str,
    generation: i32,
    protocol_type: Option<String>,
    topics: Option<BTreeMap<String, u64>>,
    committing: bool,
    source: Source,
    fresh: Duration,
}

impl Row {
    /// The row for a group as `described` (`None`: no actor holds it — a
    /// simple group), written because of `source`. `previous` is the topic
    /// map of the group's last row from this process, which a row with no
    /// members keeps with its counts at zero: the broker then still dates the
    /// queues the group was reading when its last member left, and not the
    /// ones it had stopped reading before.
    fn of(
        described: Option<&GroupDescription>,
        source: Source,
        previous: Option<BTreeMap<String, u64>>,
        every: Duration,
    ) -> Row {
        let members = described.map_or(0, |d| d.members.len()) as u64;
        let committing = members == 0 && source == Source::Commit;
        let topics = if members > 0 {
            described.and_then(subscriptions)
        } else if committing {
            // What a simple consumer reads is whatever it commits, and one
            // commit need not name all of it: unknown, so it is live on every
            // queue the group has a cursor on while it commits.
            None
        } else {
            previous.map(|t| t.into_keys().map(|k| (k, 0)).collect())
        };
        Row {
            members,
            state: described.map_or("Empty", |d| {
                crate::handlers::list_groups::state_name(d.state)
            }),
            generation: described.map_or(crate::coordinator::NO_GENERATION, |d| d.generation),
            protocol_type: described.and_then(|d| d.protocol_type.clone()),
            topics,
            committing,
            source,
            fresh: every * FRESH_INTERVALS,
        }
    }

    fn value(&self, node: Option<i32>) -> Value {
        json!({
            "v": VERSION,
            "members": self.members,
            "state": self.state,
            "generation": self.generation,
            "protocolType": self.protocol_type,
            "topics": self.topics,
            "committing": self.committing,
            "freshMs": self.fresh.as_millis() as u64,
            "source": self.source.name(),
            "at": now_millis(),
            "node": node,
        })
    }
}

/// Members per subscribed topic, or `None` when any member's subscription
/// cannot be read.
///
/// The ONE place this facade reads inside a member's metadata, and it reads it
/// for the console only: nothing here decides an assignment, a generation or
/// an answer to a client, which is the rule [`crate::coordinator`] is built on.
/// A group of another protocol type (`connect`), or a client whose assignor
/// writes something other than Kafka's consumer subscription, is simply
/// unknown.
fn subscriptions(described: &GroupDescription) -> Option<BTreeMap<String, u64>> {
    if described.protocol_type.as_deref() != Some("consumer") {
        return None;
    }
    let mut topics: BTreeMap<String, u64> = BTreeMap::new();
    for m in &described.members {
        for t in subscribed_topics(&m.metadata)? {
            *topics.entry(t).or_default() += 1;
        }
    }
    Some(topics)
}

/// The topics of one consumer subscription (Kafka's ConsumerProtocol: a
/// big-endian version, then the subscription at that version). A version past
/// the newest this crate knows is read as the newest, as Kafka's own
/// deserializer does: later versions only append fields.
fn subscribed_topics(metadata: &Bytes) -> Option<Vec<String>> {
    let version = i16::from_be_bytes([*metadata.first()?, *metadata.get(1)?]);
    if version < 0 {
        return None;
    }
    let mut body = metadata.slice(2..);
    let version = version.min(ConsumerProtocolSubscription::VERSIONS.max);
    let sub = ConsumerProtocolSubscription::decode(&mut body, version).ok()?;
    Some(sub.topics.iter().map(|t| t.to_string()).collect())
}

/// Wall-clock milliseconds, for `at`: a stamp a person reads, so never tokio's
/// clock (a paused test clock would write 1970).
fn now_millis() -> i64 {
    std::time::UNIX_EPOCH
        .elapsed()
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

/// When this process last wrote each group's row, and the topics it wrote.
/// Keyed by tenant as well as group id, like every process-wide map here:
/// two tenants' groups of one name are two groups.
static MARKS: Marks = Marks::new();

struct Mark {
    at: Instant,
    topics: Option<BTreeMap<String, u64>>,
}

/// A `std::sync::Mutex` around a map; nothing is awaited while it is held.
struct Marks {
    marks: Mutex<Option<HashMap<(TenantKey, String), Mark>>>,
}

impl Marks {
    const fn new() -> Marks {
        Marks {
            marks: Mutex::new(None),
        }
    }

    fn with<T>(&self, f: impl FnOnce(&mut HashMap<(TenantKey, String), Mark>) -> T) -> T {
        let mut held = self
            .marks
            .lock()
            .expect("the presence lock is never held across a panic");
        f(held.get_or_insert_with(Default::default))
    }

    /// Is a write due: no row yet from this process, or the last one at least
    /// `gap` ago?
    fn due(&self, tenant: &TenantKey, group: &str, gap: Duration, now: Instant) -> bool {
        self.with(|m| {
            m.get(&(tenant.clone(), group.to_string()))
                .is_none_or(|mark| now.saturating_duration_since(mark.at) >= gap)
        })
    }

    fn topics(&self, tenant: &TenantKey, group: &str) -> Option<BTreeMap<String, u64>> {
        self.with(|m| {
            m.get(&(tenant.clone(), group.to_string()))
                .and_then(|mark| mark.topics.clone())
        })
    }

    /// Record a write. Past [`MAX_MARKS`] the group written longest ago is
    /// forgotten, which costs it one early write.
    fn mark(
        &self,
        tenant: &TenantKey,
        group: &str,
        at: Instant,
        topics: Option<BTreeMap<String, u64>>,
    ) {
        self.with(|m| {
            let key = (tenant.clone(), group.to_string());
            if !m.contains_key(&key) {
                while m.len() >= MAX_MARKS {
                    let Some(coldest) = m
                        .iter()
                        .min_by_key(|(_, mark)| mark.at)
                        .map(|(k, _)| k.clone())
                    else {
                        break;
                    };
                    m.remove(&coldest);
                }
            }
            m.insert(key, Mark { at, topics });
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::{group::State, MemberDescription};
    use kafka_protocol::protocol::{Encodable, StrBytes};

    /// A member's subscription as a Kafka client writes it.
    pub(crate) fn subscription(version: i16, topics: &[&str]) -> Bytes {
        let sub = ConsumerProtocolSubscription::default().with_topics(
            topics
                .iter()
                .map(|t| StrBytes::from_string(t.to_string()))
                .collect(),
        );
        let mut buf = bytes::BytesMut::new();
        buf.extend_from_slice(&version.to_be_bytes());
        sub.encode(&mut buf, version)
            .expect("encode a subscription");
        buf.freeze()
    }

    fn member(metadata: Bytes) -> MemberDescription {
        MemberDescription {
            id: "m".into(),
            client_id: "c".into(),
            client_host: "/127.0.0.1".into(),
            metadata,
            assignment: Bytes::new(),
        }
    }

    fn group(protocol_type: &str, members: Vec<MemberDescription>) -> GroupDescription {
        GroupDescription {
            state: State::Stable,
            generation: 7,
            protocol_type: Some(protocol_type.into()),
            protocol_name: Some("range".into()),
            members,
        }
    }

    const EVERY: Duration = Duration::from_secs(15);

    #[test]
    fn the_key_is_the_groups_escaped_name_under_its_own_prefix() {
        assert_eq!(key("orders").as_deref(), Some("qk:live:orders"));
        assert_eq!(key("team a:b").as_deref(), Some("qk:live:team%20a%3Ab"));
        assert_eq!(key(&"g".repeat(600)), None, "past the store's key ceiling");
        // Not a prefix of the other key spaces of the namespace, nor they of it.
        for other in [
            "qk:group:",
            "qk:groups:",
            "qk:fence:",
            "qk:node:",
            "qk:topiccfg:",
        ] {
            assert!(!KEY_PREFIX.starts_with(other) && !other.starts_with(KEY_PREFIX));
        }
    }

    #[test]
    fn a_subscription_is_read_at_every_version_a_client_writes() {
        for version in 0..=3 {
            let m = subscription(version, &["orders", "audit"]);
            assert_eq!(
                subscribed_topics(&m),
                Some(vec!["orders".to_string(), "audit".to_string()]),
                "v{version}"
            );
        }
        // A future version is read as the newest known: fields are appended.
        let mut future = subscription(3, &["orders"]).to_vec();
        future[..2].copy_from_slice(&9i16.to_be_bytes());
        assert_eq!(
            subscribed_topics(&Bytes::from(future)),
            Some(vec!["orders".to_string()])
        );
        assert_eq!(subscribed_topics(&Bytes::new()), None);
        assert_eq!(
            subscribed_topics(&Bytes::from_static(&[0xff, 0xff, 1])),
            None
        );
        assert_eq!(
            subscribed_topics(&Bytes::from_static(&[0, 0, 0, 0, 0, 9])),
            None
        );
    }

    #[test]
    fn members_are_counted_per_topic_they_subscribe_to() {
        let g = group(
            "consumer",
            vec![
                member(subscription(1, &["orders", "audit"])),
                member(subscription(3, &["orders"])),
            ],
        );
        let row = Row::of(Some(&g), Source::Heartbeat, None, EVERY);
        assert_eq!(row.members, 2);
        assert_eq!(
            row.topics,
            Some(BTreeMap::from([
                ("audit".to_string(), 1),
                ("orders".to_string(), 2)
            ]))
        );
        assert!(!row.committing);
        let v = row.value(None);
        assert_eq!(v["v"], 1);
        assert_eq!(v["members"], 2);
        assert_eq!(v["state"], "Stable");
        assert_eq!(v["generation"], 7);
        assert_eq!(v["topics"]["orders"], 2);
        assert_eq!(v["freshMs"], 45_000);
        assert_eq!(v["source"], "heartbeat");
    }

    #[test]
    fn one_unreadable_subscription_makes_the_topics_unknown() {
        let g = group(
            "consumer",
            vec![
                member(subscription(1, &["orders"])),
                member(Bytes::from_static(b"not a subscription")),
            ],
        );
        assert_eq!(Row::of(Some(&g), Source::Sync, None, EVERY).topics, None);
        // Another protocol type's metadata is never read.
        let connect = group("connect", vec![member(subscription(1, &["orders"]))]);
        assert_eq!(
            Row::of(Some(&connect), Source::Sync, None, EVERY).topics,
            None
        );
    }

    #[test]
    fn a_group_with_no_members_keeps_the_topics_it_was_reading_at_zero() {
        let previous = Some(BTreeMap::from([("orders".to_string(), 3)]));
        let empty = GroupDescription {
            state: State::Empty,
            protocol_type: None,
            protocol_name: None,
            ..group("consumer", Vec::new())
        };
        let row = Row::of(Some(&empty), Source::Leave, previous.clone(), EVERY);
        assert_eq!(row.members, 0);
        assert_eq!(row.state, "Empty");
        assert_eq!(
            row.topics,
            Some(BTreeMap::from([("orders".to_string(), 0)]))
        );
        assert!(!row.committing);
        // With nothing remembered, unknown.
        assert_eq!(
            Row::of(Some(&empty), Source::Leave, None, EVERY).topics,
            None
        );
    }

    #[test]
    fn a_group_nobody_joined_that_commits_is_committing_on_every_queue() {
        let row = Row::of(None, Source::Commit, None, EVERY);
        assert_eq!(row.members, 0);
        assert_eq!(row.state, "Empty");
        assert!(row.committing);
        assert_eq!(row.topics, None);
        assert_eq!(row.generation, crate::coordinator::NO_GENERATION);
        // A heartbeat for a group with no actor is not a commit.
        assert!(!Row::of(None, Source::Heartbeat, None, EVERY).committing);
    }

    #[test]
    fn writes_are_spaced_by_source() {
        assert_eq!(Source::Heartbeat.gap(EVERY, false), EVERY);
        assert_eq!(Source::Commit.gap(EVERY, true), EVERY);
        assert_eq!(Source::Sync.gap(EVERY, false), MIN_GAP);
        assert_eq!(Source::Leave.gap(EVERY, false), MIN_GAP);
        assert_eq!(
            Source::Leave.gap(EVERY, true),
            Duration::ZERO,
            "the last member left"
        );

        let marks = Marks::new();
        let t = TenantKey::anonymous();
        let start = Instant::now();
        assert!(marks.due(&t, "g", EVERY, start), "never written");
        marks.mark(&t, "g", start, None);
        assert!(!marks.due(&t, "g", EVERY, start + Duration::from_secs(14)));
        assert!(marks.due(&t, "g", EVERY, start + EVERY));
        assert!(marks.due(&t, "g", Duration::ZERO, start));
        // Another tenant's group of the same name is another group.
        assert!(marks.due(&TenantKey::of_credential(Some("other")), "g", EVERY, start));
    }

    // ------------------------------------------- through the handlers, end to end

    use crate::handlers::testing::facade_with_presence;
    use crate::queen::testing::FakeQueen;
    use crate::queen::QueenApi;
    use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
    use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
    use kafka_protocol::messages::{
        GroupId, HeartbeatRequest, JoinGroupRequest, LeaveGroupRequest, SyncGroupRequest,
    };

    fn gid(group: &str) -> GroupId {
        GroupId(StrBytes::from_string(group.to_string()))
    }

    /// One consumer through JoinGroup (the v4 member-id round trip), and
    /// SyncGroup as the group's leader. Answers (member id, generation).
    async fn join_and_sync(f: &Facade, group: &str, metadata: Bytes) -> (String, i32) {
        let req = JoinGroupRequest::default()
            .with_group_id(gid(group))
            .with_protocol_type(StrBytes::from_static_str("consumer"))
            .with_session_timeout_ms(10_000)
            .with_rebalance_timeout_ms(60_000)
            .with_protocols(vec![JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_static_str("range"))
                .with_metadata(metadata)]);
        let minted = crate::handlers::join_group::handle(f, &req, 4, "c", "/127.0.0.1").await;
        let joined = crate::handlers::join_group::handle(
            f,
            &req.clone().with_member_id(minted.member_id),
            4,
            "c",
            "/127.0.0.1",
        )
        .await;
        assert_eq!(joined.error_code, 0);
        let member = joined.member_id.to_string();
        let synced = crate::handlers::sync_group::handle(
            f,
            &SyncGroupRequest::default()
                .with_group_id(gid(group))
                .with_member_id(StrBytes::from_string(member.clone()))
                .with_generation_id(joined.generation_id)
                .with_assignments(vec![SyncGroupRequestAssignment::default()
                    .with_member_id(StrBytes::from_string(member.clone()))
                    .with_assignment(Bytes::new())]),
        )
        .await;
        assert_eq!(synced.error_code, 0);
        (member, joined.generation_id)
    }

    /// The group's row once a write from `source` has landed: the write is a
    /// spawned task, so the test yields to it.
    async fn row_from(api: &FakeQueen, group: &str, source: &str) -> Value {
        let key = key(group).expect("a short group id");
        for _ in 0..400 {
            if let Some(v) = api.kv_get(NAMESPACE, &key) {
                if v["source"] == source {
                    return v;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!(
            "no {source} row for {group}: {:?}",
            api.kv_get(NAMESPACE, &key)
        );
    }

    fn presence_puts(api: &FakeQueen, group: &str) -> usize {
        let key = key(group).expect("a short group id");
        api.kv_ops()
            .iter()
            .filter(|op| matches!(op, KvOp::Put { key: k, .. } if *k == key))
            .count()
    }

    #[tokio::test]
    async fn a_group_is_written_as_its_members_come_and_go() {
        let (f, api) = facade_with_presence(&[("orders", 4)], EVERY);
        let group = "presence-come-and-go";
        let (member, generation) = join_and_sync(&f, group, subscription(1, &["orders"])).await;

        let synced = row_from(&api, group, "sync").await;
        assert_eq!(synced["members"], 1);
        assert_eq!(synced["state"], "Stable");
        assert_eq!(synced["topics"], json!({"orders": 1}));
        assert_eq!(synced["committing"], false);
        assert_eq!(synced["freshMs"], 45_000);

        // A heartbeat inside the interval is one lock and no write.
        let heartbeat = HeartbeatRequest::default()
            .with_group_id(gid(group))
            .with_member_id(StrBytes::from_string(member.clone()))
            .with_generation_id(generation);
        let hb = crate::handlers::heartbeat::handle(&f, &heartbeat).await;
        assert_eq!(hb.error_code, 0);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(presence_puts(&api, group), 1);

        // The last member leaving is written at once, with the topics it read.
        let leave = LeaveGroupRequest::default()
            .with_group_id(gid(group))
            .with_member_id(StrBytes::from_string(member));
        assert_eq!(
            crate::handlers::leave_group::handle(&f, &leave)
                .await
                .error_code,
            0
        );
        let left = row_from(&api, group, "leave").await;
        assert_eq!(left["members"], 0);
        assert_eq!(left["state"], "Empty");
        assert_eq!(left["topics"], json!({"orders": 0}));
    }

    #[tokio::test]
    async fn a_heartbeat_past_the_interval_writes_again() {
        let every = Duration::from_millis(50);
        let (f, api) = facade_with_presence(&[("orders", 4)], every);
        let group = "presence-interval";
        let (member, generation) = join_and_sync(&f, group, subscription(3, &["orders"])).await;
        row_from(&api, group, "sync").await;
        tokio::time::sleep(every).await;
        let heartbeat = HeartbeatRequest::default()
            .with_group_id(gid(group))
            .with_member_id(StrBytes::from_string(member))
            .with_generation_id(generation);
        assert_eq!(
            crate::handlers::heartbeat::handle(&f, &heartbeat)
                .await
                .error_code,
            0
        );
        let beat = row_from(&api, group, "heartbeat").await;
        assert_eq!(beat["members"], 1);
        assert_eq!(beat["topics"], json!({"orders": 1}));
    }

    #[tokio::test]
    async fn a_simple_consumer_is_present_while_it_commits() {
        use kafka_protocol::messages::offset_commit_request::{
            OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        };
        use kafka_protocol::messages::{OffsetCommitRequest, TopicName};
        let (f, api) = facade_with_presence(&[("orders", 4)], EVERY);
        let group = "presence-simple";
        let commit = OffsetCommitRequest::default()
            .with_group_id(gid(group))
            .with_member_id(StrBytes::from_static_str(""))
            .with_generation_id_or_member_epoch(crate::coordinator::NO_GENERATION)
            .with_topics(vec![OffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("orders")))
                .with_partitions(vec![OffsetCommitRequestPartition::default()
                    .with_partition_index(0)
                    .with_committed_offset(41)])]);
        let resp = crate::handlers::offset_commit::handle(&f, &commit, None).await;
        assert_eq!(resp.topics[0].partitions[0].error_code, 0);
        let row = row_from(&api, group, "commit").await;
        assert_eq!(row["members"], 0);
        assert_eq!(row["committing"], true);
        assert_eq!(row["topics"], Value::Null);
        // No actor was conjured to describe it.
        assert_eq!(f.coordinator.live_groups(), 0);
    }

    #[tokio::test]
    async fn nothing_is_written_when_presence_is_off() {
        let (f, api) = facade_with_presence(&[("orders", 4)], Duration::ZERO);
        let group = "presence-off";
        join_and_sync(&f, group, subscription(1, &["orders"])).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(presence_puts(&api, group), 0);
    }

    #[tokio::test]
    async fn a_deleted_group_takes_its_presence_row_with_it() {
        let api = FakeQueen::with(&[]);
        let k = key("presence-deleted").unwrap();
        api.kv(
            &[KvOp::put(NAMESPACE, &k, json!({"v": 1, "members": 0}))],
            None,
        )
        .await
        .unwrap();
        let removed = crate::offsets::delete_group(&*api, "presence-deleted", None)
            .await
            .unwrap();
        assert_eq!(removed, 1, "the presence row");
        assert!(api.kv_get(NAMESPACE, &k).is_none());
    }
}
