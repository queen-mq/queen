//! Metadata — the request that turns `bootstrap.servers` into a cluster.
//!
//! A Kafka client sends this second (right after ApiVersions) and re-sends it
//! whenever its view goes stale. The answer is the whole cluster as far as the
//! client is concerned: which brokers exist, which one leads each partition of
//! each topic, and which topics exist at all. Everything after it — Produce,
//! Fetch, FindCoordinator — is addressed using what this response said.
//!
//! ## The cluster
//!
//! One broker, node 0, at `QUEEN_KAFKA_ADVERTISED_ADDR`; controller 0; leader 0
//! for every partition, with `[0]` as both replicas and ISR. That is
//! PLAN_QUEEN_KAFKA.md's "one logical Kafka broker": the facade is the only
//! address a client ever needs, whatever the Queen cluster behind it looks like.
//! Replication is Postgres's business and is not modelled here — claiming
//! replicas the facade does not arbitrate would be a lie a client could act on.
//!
//! ...unless `QUEEN_KAFKA_NODE_ID` is set, in which case the answer is the
//! whole live set from the node registry, the controller is the lowest live id,
//! and each partition's leader is the rendezvous winner over that set
//! ([`crate::cluster`]). Two things do NOT change with it, and both are
//! deliberate:
//!
//!   * `replica_nodes` and `isr_nodes` stay `[leader]` and not the whole set.
//!     Claiming N replicas would be the same lie in a larger font; the one
//!     client behaviour that reads the ISR list is rack-aware follower fetch,
//!     which arrives at Fetch v11 and is out of reach behind the deliberate v6
//!     cap in [`crate::versions`].
//!   * `leader_epoch` stays −1, for the reason already written beside it: a
//!     synthetic epoch that changed on every membership change would invite
//!     clients to run truncation detection against a value nothing maintains.
//!
//! And the leader is an ADVERTISEMENT: every node serves every partition, so a
//! client using a stale map is not wrong, only unbalanced ([`crate::cluster`]).
//!
//! ## Topics and partitions
//!
//! Topic name = Queen queue name; Kafka partition n = Queen partition n. The
//! partition count is the one number that needs a rule, because Queen does not
//! have the thing Kafka is asking for. A Kafka topic *declares* a width at
//! creation and clients hash keys modulo it; a Queen queue declares nothing and
//! materialises a `queen.log_partitions` row the first time something is pushed
//! to that lane (server/sql/procedures/012_configure.sql). A queue created a
//! second ago therefore has zero partitions, and a topic with zero partitions is
//! one a producer cannot send to.
//!
//! So the advertised width is `max(live, floor)`, where `floor` is the topic's
//! OWN declared width when it has one ([`crate::topic_record::Record::partitions`],
//! set once by CreateTopics) and `QUEEN_KAFKA_DEFAULT_PARTITIONS` when it does
//! not. Queen still declares no width per queue: the floor is the facade's own
//! number, and it is a floor rather than a cap for the second reason below.
//!
//!   * it is never zero, so a fresh topic is usable immediately;
//!   * it never shrinks as lanes materialise, and a Kafka partition count that
//!     shrinks is not a thing clients handle — they would re-hash existing keys
//!     onto different partitions and lose ordering;
//!   * it still covers a queue that is *wider* than the configured default,
//!     which is the case for any queue Kafka did not create — a native Queen
//!     queue with 5000 lanes stays fully readable through the facade.
//!
//! The one thing the mapping cannot express is a Queen partition whose name is
//! not a decimal index (`Default`, `eu-west`, a tenant id). Those lanes are
//! counted but not addressable by a Kafka client, which can only name a
//! partition by number. Producing through the facade always creates numeric
//! lanes, so this only affects queues that Kafka clients share with native
//! producers — documented at M6, not papered over here.
//!
//! ## Size, which is a correctness problem and not a taste one
//!
//! Every advertised partition is written out in full in every Metadata
//! response — `partition_index`, leader, epoch, and three node arrays, ~26
//! bytes on the wire and three heap allocations while it is being built — and
//! clients refresh on their own timer. Two multipliers make that unbounded if
//! nothing here bounds it, and Queen is a system whose recorded production
//! scale is tens of thousands of queues and ~827k partitions:
//!
//!   * one topic's WIDTH, which comes from Queen and can be any number
//!     (`advertised_partitions` clamps it — [`MAX_ADVERTISED_PARTITIONS`]);
//!   * queues TIMES that width, which only the all-topics listing can reach
//!     ([`MAX_LISTING_PARTITIONS`]).
//!
//! Neither is a preference. Past `conn::MAX_FRAME_BYTES` the response cannot be
//! encoded at all: the connection dies AFTER the whole allocation, so a plain
//! `kcat -L` becomes an un-answerable request that costs the broker everything
//! and returns nothing. A truncated listing is strictly better than that, and
//! it is the only case where anything is withheld — a client that NAMES its
//! topics (which is every producer and every consumer) is never truncated.

use std::collections::{HashMap, HashSet};

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::{MetadataRequest, MetadataResponse, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::cluster::Placement;
use crate::queen::Queue;
use crate::throttle;
use crate::topic_record;
use crate::Facade;

/// The facade is the whole cluster, and it is node 0 of it.
///
/// **Reserved**: a clustered node id is `1..=64`
/// ([`crate::cluster::MIN_NODE_ID`]), so node 0 means "this facade is on its
/// own" and can mean nothing else. That is what makes a client's cached broker
/// list from single mode wholly replaced on its next refresh rather than
/// half-overlapping with a cluster's. Apache Kafka is 0-based; the deviation is
/// deliberate, and the boot error for `QUEEN_KAFKA_NODE_ID=0` says so.
pub const SINGLE_NODE_ID: i32 = 0;

/// Reported from Metadata v2 up. Clients use it for logging and for refusing to
/// talk to two different clusters through one connection pool, so it has to be
/// stable across restarts — hence a constant and not something derived from the
/// process or the broker list. In cluster mode it is `QUEEN_KAFKA_CLUSTER`,
/// which DEFAULTS to this same value ([`crate::cluster::Cluster::cluster_id`]).
pub const CLUSTER_ID: &str = "queen";

/// Kafka's own limit on a topic name (org.apache.kafka.common.internals.Topic).
const MAX_TOPIC_NAME_CHARS: usize = 249;

/// Queen's: `queen.queues.name` is `VARCHAR(255)` (server/sql/schema.sql). The
/// two are checked as one bound so neither can drift out of sight; today Kafka's
/// is the tighter of them, which is why no legal Kafka topic name is ever
/// refused for being unstorable.
const MAX_QUEUE_NAME_CHARS: usize = 255;

/// Names Kafka reserves for itself: `__consumer_offsets`, `__transaction_state`,
/// and anything else a client might mistake for one of them.
const INTERNAL_PREFIX: &str = "__";

/// Ceiling on the width advertised for ONE topic, whatever Queen reports.
///
/// It is also the ceiling `QUEEN_KAFKA_DEFAULT_PARTITIONS` is validated against
/// at boot (main.rs), and the same number for the same reason: 100k lanes is a
/// ~2.6 MB partition array in every Metadata response, which is already absurd
/// and is the last order of magnitude before the answer stops being an answer.
/// The configured default cannot exceed it; a queue's LIVE lane count can, and
/// that is the case this clamp is actually for — a native Queen queue with 827k
/// partitions (a real, recorded shape) would otherwise be a 21 MB topic entry
/// built one `Vec`-triple at a time. Lanes above the ceiling stay reachable
/// natively and through `POST /api/v1/fetch`; they are not addressable by a
/// Kafka client, which is the same limitation the module header already states
/// for lanes whose names are not decimal indices.
pub const MAX_ADVERTISED_PARTITIONS: u32 = 100_000;

/// Ceiling on the distinct topic names ONE request is answered about.
///
/// The named form is the only request whose cost is set by the client's own
/// list, and that list is bounded by nothing but the frame: 100 MiB of
/// eight-character names is ~10^7 of them, each of which would be a `String`, a
/// hash-set entry, a plan and a response topic — before any of them reached
/// Queen. Ten thousand is more topics than any Kafka consumer subscribes to
/// (and more queues than a Queen tenant has), and past it the extra names are
/// dropped from the ANSWER rather than allowed to size the work: a client that
/// asked about a topic and is told nothing about it retries, which is the same
/// thing it does when a topic's leader is not available yet.
const MAX_REQUESTED_TOPICS: usize = 10_000;

/// Ceiling on the topics ONE request auto-creates.
///
/// Each creation is a `POST /api/v1/configure` with a ten-second budget, run in
/// sequence, on a connection that is muted until the whole response is written
/// (conn.rs) — so without a bound, one request that names N absent topics is N
/// upstream calls and a connection that says nothing for as long as they take.
/// A hundred is a consumer subscribing to a hundred new topics served in one
/// round trip; the hundred-and-first is answered LEADER_NOT_AVAILABLE, which
/// the client retries, and the next Metadata creates the next hundred. The
/// fleet converges either way — what it cannot do is buy an unbounded fan-out
/// with one frame.
pub(crate) const MAX_AUTO_CREATES_PER_REQUEST: usize = 100;

/// One line per window when either ceiling binds. Both are client-driven, so
/// both are exactly the shape that floods a log.
static TOPIC_CAP: crate::obs::Sampler = crate::obs::Sampler::new(60_000);

/// Ceiling on the partitions materialised for ONE all-topics listing, summed
/// across every topic in it.
///
/// The listing is the only request whose cost is queues × width, and it is the
/// one no client asked to be big: `kcat -L`, an admin `listTopics`, a
/// regex-subscription discovery. At ~26 wire bytes per partition this is a
/// ~5 MB response, past which the answer is not one. Topics beyond the budget
/// are omitted with a loud log line rather than the whole response being made
/// un-encodable, because a listing that arrives short is something a client can
/// act on and a connection reset is not.
const MAX_LISTING_PARTITIONS: usize = 200_000;

/// What one requested topic resolves to, before any call to Queen. Pure: this is
/// the whole auto-create policy, decided from the name, the catalog and the
/// request's flag, and it is what the tests drive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Plan {
    /// The queue exists; advertise this many partitions.
    Serve(i32),
    /// The queue does not exist and the client allowed auto-creation.
    Create,
    /// Answer this error code for this topic, and touch nothing.
    Reject(ResponseError),
}

/// Decide what to do with one requested topic name.
///
/// `live` is the queue's partition count from the catalog, or `None` when there
/// is no such queue. `floor` is the second term of the width — the topic's own
/// stored floor when it declared one, and `QUEEN_KAFKA_DEFAULT_PARTITIONS`
/// otherwise. Prefer [`plan_for`], which reads both off one [`Queue`].
pub fn plan(name: &str, live: Option<i64>, allow_auto_create: bool, floor: u32) -> Plan {
    if let Some(e) = reserved_or_invalid(name) {
        return Plan::Reject(e);
    }
    match live {
        Some(live) => Plan::Serve(advertised_partitions(live, floor)),
        None if allow_auto_create => Plan::Create,
        // The client asked us not to create it, so the honest answer is that it
        // is not here. This is also the code a consumer subscribed to a topic
        // nobody has produced to yet sees, and it retries — as it should.
        None => Plan::Reject(ResponseError::UnknownTopicOrPartition),
    }
}

/// [`plan`], reading both terms of the width off the catalog's own queue.
///
/// The one place that decides which floor applies, so that a call site cannot
/// pass `partitions` from one queue and the default from nowhere. A topic that
/// declared no floor, and one the facade never created, both resolve to
/// `default_partitions` — which is byte-for-byte what this facade answered
/// before per-topic floors existed.
pub fn plan_for(
    name: &str,
    queue: Option<&Queue>,
    allow_auto_create: bool,
    default_partitions: u32,
) -> Plan {
    plan(
        name,
        queue.map(|q| q.partitions),
        allow_auto_create,
        queue.map_or(default_partitions, |q| q.floor_or(default_partitions)),
    )
}

/// The rule for a name, independent of whether the queue exists.
///
/// `__`-prefixed names are refused as UNKNOWN rather than INVALID on purpose: to
/// Kafka they are perfectly valid names that happen to belong to the broker's
/// own bookkeeping topics, and INVALID_TOPIC_EXCEPTION is a permanent client
/// error that would surface as a crash in tooling that lists them. The facade
/// keeps no such topics — offsets live in Queen (PLAN_QUEEN_KAFKA.md M4) — so
/// "there is no such topic" is both the safe answer and the true one, and it is
/// what stops `allow_auto_topic_creation` from ever conjuring a queue called
/// `__consumer_offsets` that a later real implementation would collide with.
/// The rule is by prefix, not by a list of known names, so it also covers the
/// internal topics of Kafka versions this facade has never heard of.
///
/// `pub(crate)` for [`crate::handlers::fetch`] and
/// [`crate::handlers::list_offsets`], which apply the SAME rule without a
/// catalog: a `__` name must be as invisible on the read path as it is here, or
/// a queue Metadata hides would be readable by naming it directly. They apply it
/// through [`not_a_topic_here`], which is the same rule in the one code those
/// APIs may answer.
pub(crate) fn reserved_or_invalid(name: &str) -> Option<ResponseError> {
    if name.starts_with(INTERNAL_PREFIX) {
        return Some(ResponseError::UnknownTopicOrPartition);
    }
    if !is_valid_topic_name(name) {
        return Some(ResponseError::InvalidTopicException);
    }
    None
}

/// The same rule, for every API that is NOT Metadata: one code,
/// UNKNOWN_TOPIC_OR_PARTITION.
///
/// INVALID_TOPIC_EXCEPTION is a METADATA answer. Apache Kafka raises it where a
/// topic name is validated — the metadata path and CreateTopics — and nowhere
/// else: a broker asked to fetch, list the offsets of, or commit against a name
/// it does not have simply does not have it, so it answers
/// UNKNOWN_TOPIC_OR_PARTITION whatever the name looks like. Every Kafka client
/// is written against that shape, and the Java consumer enforces it: the fetch
/// path walks a CLOSED set of per-partition codes and throws
/// `IllegalStateException` out of `poll()` on anything outside it, and
/// INVALID_TOPIC_EXCEPTION is outside it — a consumer that named an illegal
/// topic would die rather than be told there is no such topic. The commit and
/// offset-fetch paths are the same story with a different exception.
///
/// So the read and commit paths narrow the rule to the code they may answer,
/// and Metadata — where a client asked about the NAME and can act on the answer
/// — keeps the precise one. See `compat/ERRORS.md`.
pub(crate) fn not_a_topic_here(name: &str) -> Option<ResponseError> {
    reserved_or_invalid(name).map(|_| ResponseError::UnknownTopicOrPartition)
}

/// Kafka's topic-name rule, and Queen's storage bound, in one place.
pub fn is_valid_topic_name(name: &str) -> bool {
    if name.is_empty() || name == "." || name == ".." {
        return false;
    }
    if name.chars().count() > MAX_TOPIC_NAME_CHARS.min(MAX_QUEUE_NAME_CHARS) {
        return false;
    }
    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
}

/// The width to advertise for a queue with `live` materialised partitions. See
/// the module header for why it is a maximum and not either input alone, and
/// [`MAX_ADVERTISED_PARTITIONS`] for why it has a ceiling at all.
pub fn advertised_partitions(live: i64, default_partitions: u32) -> i32 {
    live.max(default_partitions as i64)
        .clamp(0, MAX_ADVERTISED_PARTITIONS as i64) as i32
}

/// The advertised width of each of `names` that Queen has a queue for.
///
/// The number a READ is checked against, so that a partition index this facade
/// never advertised is answered UNKNOWN_TOPIC_OR_PARTITION instead of as an
/// empty lane that will never fill. Produce has always applied it — through
/// [`plan`], which it needs anyway to auto-create — and the read paths apply
/// the same one from the same cache, because a partition that cannot be
/// written to is not one that can be read from either.
///
/// A name that is absent from the answer is one this facade will not bound: the
/// queue is not in the catalog (it may not exist at all, and Queen is the
/// authority on that — see `handlers::fetch`), or the catalog could not be read
/// right now. The second is the reason this returns a map rather than a result:
/// a blip in the admin API must not fail a fetch of records Queen would have
/// served, so an unreadable catalog costs the bound and nothing else.
///
/// Lanes past [`MAX_ADVERTISED_PARTITIONS`] are outside the width for the same
/// reason they are outside a Metadata answer: a Kafka client cannot address
/// them. They stay readable natively and through `POST /api/v1/fetch`.
pub(crate) async fn advertised_widths<'a>(
    facade: &Facade,
    names: impl Iterator<Item = &'a str>,
    token: Option<&str>,
) -> HashMap<String, i32> {
    let wanted: HashSet<&str> = names.collect();
    if wanted.is_empty() {
        // A request naming no topic at all. There is nothing to bound, so there
        // is nothing to ask Queen either.
        return HashMap::new();
    }
    let queues = match facade.catalog.list(token).await {
        Ok(queues) => queues,
        Err(e) => {
            tracing::debug!(
                target: "kafka",
                error = %e,
                "cannot read the queue list; this read is served without a partition-width check"
            );
            return HashMap::new();
        }
    };
    // One pass over the catalog, and only the names the request asked about
    // kept: a tenant's queue list and a client's topic list are both numbers
    // neither side bounds, and their product is the quadratic this avoids.
    queues
        .iter()
        .filter(|q| wanted.contains(q.name.as_str()))
        .map(|q| {
            (
                q.name.clone(),
                advertised_partitions(q.partitions, q.floor_or(facade.default_partitions)),
            )
        })
        .collect()
}

// ------------------------------------------------------------------- handling

/// Build the Metadata response for one request.
///
/// `token` is the credential to reach Queen with — `QUEEN_TOKEN` at M1, the
/// connection's own tenant token from M5 on.
pub async fn handle(
    facade: &Facade,
    req: &MetadataRequest,
    api_version: i16,
    token: Option<&str>,
) -> MetadataResponse {
    let mut throttle = None;
    let catalog = match facade.catalog.list(token).await {
        Ok(queues) => Some(queues),
        Err(e) => {
            // A capped or frozen tenant is not a broker fault and is answered
            // as a WAIT rather than only as a refusal: the per-topic code stays
            // the retriable LEADER_NOT_AVAILABLE it has always been, and
            // `throttle_time_ms` beside it is what makes the client's retry
            // arrive after the cap has drained rather than into it. See
            // [`crate::throttle`].
            throttle = throttle::for_error(&e);
            if throttle.is_some() {
                tracing::warn!(target: "kafka", error = %e, "metadata: the tenant is throttled");
            } else {
                tracing::error!(target: "kafka", error = %e, "metadata: cannot read the queue list");
            }
            None
        }
    };

    // ONE snapshot for the whole response: every broker entry and every
    // partition's leader is derived from the same live set, so an answer can
    // never name a leader that is missing from its own broker list.
    let placement = facade
        .cluster
        .placement(&facade.advertised_host, facade.advertised_port);

    let topics = match requested_names(req, api_version) {
        None => listing(facade, &placement, catalog.as_deref().map(|q| &q[..])),
        Some(names) => {
            requested(
                facade,
                &placement,
                &names,
                req.allow_auto_topic_creation,
                catalog.as_deref().map(|q| &q[..]),
                token,
            )
            .await
        }
    };

    MetadataResponse::default()
        .with_brokers(
            placement
                .brokers()
                .iter()
                .map(|node| {
                    MetadataResponseBroker::default()
                        .with_node_id(node.id.into())
                        .with_host(StrBytes::from_string(node.host.clone()))
                        .with_port(node.port as i32)
                        // No rack. A rack is what rack-aware follower fetch
                        // reads, and that arrives at Fetch v11.
                        .with_rack(None)
                })
                .collect(),
        )
        .with_cluster_id(Some(StrBytes::from_string(
            facade.cluster.cluster_id().to_string(),
        )))
        .with_controller_id(placement.controller().into())
        .with_topics(topics)
        // v3+. Silently dropped by the encoder below v3, which is the only
        // reason it can be set unconditionally.
        .with_throttle_time_ms(throttle.unwrap_or(0))
}

/// The all-topics form: whatever Queen has, minus the names that are not Kafka
/// topics, minus whatever does not fit in [`MAX_LISTING_PARTITIONS`].
///
/// A queue with a name Kafka cannot express is SKIPPED rather than reported as
/// an error — a client asked for a listing, not for that queue, and an error
/// entry for a topic it never named makes some clients discard the whole
/// response. A queue past the budget is skipped for a different reason and is
/// logged, because that one IS a shortfall.
fn listing(
    facade: &Facade,
    placement: &Placement,
    catalog: Option<&[Queue]>,
) -> Vec<MetadataResponseTopic> {
    // Nothing cached and Queen is unreachable: an empty listing is the only
    // shape available, and it is transient — the client refreshes.
    let Some(queues) = catalog else {
        return Vec::new();
    };
    let mut spent = 0usize;
    let mut omitted = 0usize;
    let mut out = Vec::new();
    for q in queues
        .iter()
        .filter(|q| reserved_or_invalid(&q.name).is_none())
    {
        let width = advertised_partitions(q.partitions, q.floor_or(facade.default_partitions));
        // The first topic always fits, or a single wide queue would empty the
        // listing entirely and tell a client the cluster has no topics.
        if !out.is_empty() && spent + width as usize > MAX_LISTING_PARTITIONS {
            omitted += 1;
            continue;
        }
        spent += width as usize;
        out.push(topic(placement, &q.name, Plan::Serve(width)));
    }
    if omitted > 0 {
        tracing::warn!(
            target: "kafka",
            listed = out.len(),
            omitted,
            partitions = spent,
            budget = MAX_LISTING_PARTITIONS,
            "the all-topics Metadata listing hit its partition budget; \
             topics are missing from it. Clients that NAME their topics are \
             unaffected — lower QUEEN_KAFKA_DEFAULT_PARTITIONS to fit more \
             queues in a listing."
        );
    }
    out
}

/// The named form: decide every topic from the catalog, then run the one
/// auto-create pass the decisions ask for.
async fn requested(
    facade: &Facade,
    placement: &Placement,
    names: &[Option<String>],
    allow_auto_create: bool,
    catalog: Option<&[Queue]>,
    token: Option<&str>,
) -> Vec<MetadataResponseTopic> {
    // The catalog as a lookup and not a scan: both sides of it are client-set
    // (up to [`MAX_REQUESTED_TOPICS`] names against a tenant's whole queue
    // list), and their product is the one quadratic in this handler.
    // The whole `Queue` and not just its lane count: the width's second term is
    // now the queue's own stored floor, and reading it here keeps `plan_for` the
    // only place that decides which floor applies.
    let live: HashMap<&str, &Queue> = catalog
        .map(|queues| queues.iter().map(|q| (q.name.as_str(), q)).collect())
        .unwrap_or_default();
    let mut planned: Vec<(Option<&str>, Plan)> = names
        .iter()
        .map(|name| match (name.as_deref(), catalog) {
            // A null name in a request. Every version this facade advertises
            // addresses topics by name, so there is nothing to look up; the
            // entry is echoed back with its own error rather than dropped,
            // because a client matches the response topics against the ones it
            // asked for.
            (None, _) => (None, Plan::Reject(ResponseError::InvalidTopicException)),
            // Retriable, and the code a client already expects while a topic's
            // leader is being established: it backs off and asks again, which is
            // exactly right for "Queen blipped".
            (Some(n), None) => (Some(n), Plan::Reject(ResponseError::LeaderNotAvailable)),
            (Some(n), Some(_)) => (
                Some(n),
                plan_for(
                    n,
                    live.get(n).copied(),
                    allow_auto_create,
                    facade.default_partitions,
                ),
            ),
        })
        .collect();

    create_absent(facade, &mut planned, token).await;

    planned
        .into_iter()
        .map(|(name, planned)| match name {
            Some(n) => topic(placement, n, planned),
            None => MetadataResponseTopic::default()
                .with_name(None)
                .with_error_code(ResponseError::InvalidTopicException.code()),
        })
        .collect()
}

/// Create every topic the plan pass marked [`Plan::Create`], and rewrite its
/// entry with what to answer.
///
/// ONE catalog re-read for the whole request, and not from cache.
/// `POST /api/v1/configure` is an upsert that rewrites every config column to
/// its default (server/sql/procedures/012_configure.sql), so calling it for a
/// queue that already exists resets that queue's leaseTime, retention and dedup
/// window — and the snapshot the plans were made from can be up to the cache TTL
/// old. The re-read closes that window down to the length of this one request.
///
/// It is shared across the topics because it used to be per topic, and the
/// Kafka connection is MUTED until the whole response is written (conn.rs): a
/// consumer subscribing to 50 new topics was 1 + 2×50 serialised admin calls,
/// each with its own 10 s budget, on a connection that could say nothing in the
/// meantime. It is now 1 + 1 + 50 — the creates are irreducible, the reads are
/// not. Nothing is created without a fresh read behind it either way.
///
/// `pub(crate)` for [`crate::handlers::produce`]: a produce to an absent topic
/// auto-creates it, and it has to be the same policy — the same re-read, the
/// same "it appeared meanwhile, do not re-upsert it", the same retriable error
/// on failure — or the two paths would disagree about when a `/configure` is
/// safe.
pub(crate) async fn create_absent(
    facade: &Facade,
    planned: &mut [(Option<&str>, Plan)],
    token: Option<&str>,
) {
    if !planned.iter().any(|(_, p)| matches!(p, Plan::Create)) {
        return;
    }
    let fresh = match facade.catalog.refresh(token).await {
        Ok(queues) => queues,
        Err(e) => {
            tracing::error!(
                target: "kafka",
                error = %e,
                "cannot confirm the queues are absent; creating none of them"
            );
            for (_, p) in planned
                .iter_mut()
                .filter(|(_, p)| matches!(p, Plan::Create))
            {
                *p = Plan::Reject(ResponseError::LeaderNotAvailable);
            }
            return;
        }
    };

    // Looked up rather than scanned: the names are the client's and the queue
    // list is the tenant's, so a scan per name is their product.
    let live: HashMap<&str, &Queue> = fresh.iter().map(|q| (q.name.as_str(), q)).collect();
    let mut created = 0usize;
    let mut deferred = 0usize;
    // The names that actually landed, for the config records written after the
    // loop. See [`record_auto_creates`].
    let mut recordable: Vec<String> = Vec::new();
    for (name, p) in planned.iter_mut() {
        if !matches!(p, Plan::Create) {
            continue;
        }
        let Some(name) = *name else { continue };
        // It appeared between the plan and the re-read (a native `/configure`,
        // or another connection's auto-create): serve it, do not re-upsert it.
        if let Some(q) = live.get(name) {
            *p = Plan::Serve(advertised_partitions(
                q.partitions,
                q.floor_or(facade.default_partitions),
            ));
            continue;
        }
        // The budget. Retriable on purpose: the client asks again and the next
        // hundred are created, so a fleet subscribing to a thousand new topics
        // converges over a few metadata refreshes instead of holding one
        // connection open for a thousand upstream calls.
        if created >= MAX_AUTO_CREATES_PER_REQUEST {
            deferred += 1;
            *p = Plan::Reject(ResponseError::LeaderNotAvailable);
            continue;
        }
        created += 1;
        *p = match facade.catalog.create(name, token).await {
            Ok(()) => {
                tracing::info!(
                    target: "kafka",
                    topic = name,
                    partitions = facade.default_partitions,
                    "auto-created a queue for a Kafka topic"
                );
                recordable.push(name.to_string());
                // The queue exists but has no partitions yet: Queen materialises
                // them on the first push. The advertised width is the configured
                // default, which is the same number the next refresh will
                // compute through `advertised_partitions`.
                Plan::Serve(advertised_partitions(0, facade.default_partitions))
            }
            Err(e) => {
                tracing::error!(target: "kafka", topic = name, error = %e, "auto-create failed");
                Plan::Reject(ResponseError::LeaderNotAvailable)
            }
        };
    }
    if deferred > 0 {
        if let Some(suppressed) = TOPIC_CAP.tick_now() {
            tracing::warn!(
                target: "kafka",
                created,
                deferred,
                suppressed,
                "one request asked for more topics than are auto-created at a time; the rest \
                 were answered LEADER_NOT_AVAILABLE and will be created as the client retries"
            );
        }
    }
    record_auto_creates(facade, &recordable, token).await;
}

/// Write the facade's own record of what an auto-create applied
/// ([`crate::topic_record`]).
///
/// The bag is EMPTY, because that is literally what the auto-create path sends:
/// `Catalog::create` posts `POST /api/v1/configure` with no options at all, so
/// every one of `configure_queue_v1`'s nineteen columns is at the stored
/// procedure's default — which is the invariant the record exists to state.
/// Writing the empty bag is therefore not bookkeeping about nothing: it is what
/// makes a later `--alter retention.ms` on an auto-created topic land instead of
/// being refused as untracked.
///
/// ONE extra `catalog.refresh()` and ONE KV call, and both only when something
/// was actually created — which is once per topic lifetime. A Metadata that
/// creates nothing, which is every Metadata after the first, pays neither.
///
/// A failure here never fails the Metadata: the queue exists and is served, one
/// line says the record is missing, and the topic behaves as untracked until an
/// alter re-establishes it.
async fn record_auto_creates(facade: &Facade, created: &[String], token: Option<&str>) {
    if created.is_empty() {
        return;
    }
    let fresh = match facade.catalog.refresh(token).await {
        Ok(queues) => queues,
        Err(e) => {
            tracing::warn!(
                target: "kafka",
                error = %e,
                topics = created.len(),
                "the queues were auto-created but their config records were not written"
            );
            return;
        }
    };
    let ids: HashMap<&str, Option<String>> = fresh
        .iter()
        .map(|q| (q.name.as_str(), q.id.clone()))
        .collect();
    let records: Vec<(String, topic_record::Record)> = created
        .iter()
        .map(|name| {
            let qid = ids.get(name.as_str()).cloned().flatten();
            (
                name.clone(),
                topic_record::Record::new(qid, serde_json::Map::new()),
            )
        })
        .collect();
    if let Err(e) = topic_record::store_many(facade.queen.as_ref(), &records, token).await {
        tracing::warn!(
            target: "kafka",
            error = %e,
            topics = records.len(),
            "the queues were auto-created but their config records were not written"
        );
    }
}

/// The topic names a request is asking about, or `None` for "all of them".
///
/// The null-topics form is the one piece of version archaeology in this handler.
/// From v1 on, a *null* array means all topics and an *empty* array means none.
/// At v0 the field is not nullable at all, and an empty array is what means all
/// topics — so the identical wire bytes mean opposite things either side of v1,
/// and a v0 client that got the v1 reading would be told the cluster has no
/// topics.
fn requested_names(req: &MetadataRequest, api_version: i16) -> Option<Vec<Option<String>>> {
    let topics = req.topics.as_ref()?;
    if api_version == 0 && topics.is_empty() {
        return None;
    }
    // Brokers answer a duplicated topic once. Keep first-seen order: clients
    // tolerate any order, but a stable one keeps the logs and the tests readable.
    let mut seen = HashSet::new();
    let mut names: Vec<Option<String>> = Vec::new();
    let mut dropped = 0usize;
    for (at, t) in topics.iter().enumerate() {
        // Counted and abandoned rather than filtered: everything past the
        // ceiling is work this request does not get to buy, including the
        // de-duplication of it. See [`MAX_REQUESTED_TOPICS`].
        if names.len() >= MAX_REQUESTED_TOPICS {
            dropped = topics.len() - at;
            break;
        }
        let name = t.name.as_ref().map(|n| n.0.as_str().to_string());
        match &name {
            Some(n) if !seen.insert(n.clone()) => continue,
            _ => names.push(name),
        }
    }
    if dropped > 0 {
        if let Some(suppressed) = TOPIC_CAP.tick_now() {
            tracing::warn!(
                target: "kafka",
                answered = names.len(),
                dropped,
                suppressed,
                "a Metadata request named more topics than one answer covers; the rest are \
                 absent from it and the client will ask again"
            );
        }
    }
    Some(names)
}

/// One topic entry, from a name and the decision made about it.
///
/// The leader comes from `placement` and not from a constant: in single mode
/// that is node 0 without so much as a hash, and in cluster mode it is the
/// rendezvous winner over the live set.
fn topic(placement: &Placement, name: &str, planned: Plan) -> MetadataResponseTopic {
    let base = MetadataResponseTopic::default()
        .with_name(Some(TopicName(StrBytes::from_string(name.to_string()))))
        // Never true: the facade owns no internal topics, and the names Kafka
        // uses for its own are refused above.
        .with_is_internal(false);
    match planned {
        Plan::Serve(partitions) => base.with_partitions(
            (0..partitions)
                .map(|index| {
                    let leader = placement.leader_of(name, index);
                    MetadataResponsePartition::default()
                        .with_partition_index(index)
                        .with_leader_id(leader.into())
                        // -1 is "unknown epoch", and it is the truth: the facade
                        // has no leader elections to number. Advertising a real
                        // epoch would invite clients to run truncation detection
                        // against a value nothing here maintains.
                        .with_leader_epoch(-1)
                        // The leader alone, and not the live set: see the
                        // module header. Replication is Postgres's business.
                        .with_replica_nodes(vec![leader.into()])
                        .with_isr_nodes(vec![leader.into()])
                        .with_offline_replicas(vec![])
                })
                .collect(),
        ),
        // An errored topic carries no partitions: the error IS the answer, and a
        // partition list beside it is something a client may act on.
        Plan::Reject(e) => base.with_error_code(e.code()),
        // Resolved before rendering, in `handle`.
        Plan::Create => base.with_error_code(ResponseError::LeaderNotAvailable.code()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queen::testing::FakeQueen;
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
    use kafka_protocol::protocol::{Decodable, Encodable, Message};
    use std::sync::Arc;

    // ------------------------------------------------------------ pure policy

    #[test]
    fn kafkas_name_rule_is_the_one_we_enforce() {
        for good in ["orders", "a", "a.b_c-d", "0", "..x", &"x".repeat(249)] {
            assert!(is_valid_topic_name(good), "{good} was refused");
        }
        for bad in [
            "",
            ".",
            "..",
            "my topic",
            "orders/eu",
            "ordini-così",
            "a:b",
            &"x".repeat(250),
        ] {
            assert!(!is_valid_topic_name(bad), "{bad} was accepted");
        }
    }

    /// Every name Kafka calls legal fits `queen.queues.name`, so the storage
    /// bound never rejects something the protocol allows. If Queen's column ever
    /// narrows below 249, this is where it has to be noticed.
    #[test]
    fn the_kafka_limit_is_the_tighter_of_the_two() {
        const _: () = assert!(MAX_TOPIC_NAME_CHARS <= MAX_QUEUE_NAME_CHARS);
        assert!(is_valid_topic_name(&"x".repeat(MAX_TOPIC_NAME_CHARS)));
        assert!(!is_valid_topic_name(&"x".repeat(MAX_QUEUE_NAME_CHARS)));
    }

    #[test]
    fn the_width_never_drops_below_the_configured_default() {
        assert_eq!(advertised_partitions(0, 1024), 1024);
        assert_eq!(advertised_partitions(7, 1024), 1024);
        assert_eq!(advertised_partitions(1024, 1024), 1024);
        // ...and never hides lanes a native Queen queue already has.
        assert_eq!(advertised_partitions(5000, 1024), 5000);
        // Nonsense from the admin API cannot become a negative partition count.
        assert_eq!(advertised_partitions(-3, 16), 16);
    }

    /// The width one topic can reach is bounded, because it is written out in
    /// full in every Metadata response. 827k lanes is a shape this repo has
    /// actually run; unclamped it is a ~21 MB topic entry, built one Vec-triple
    /// at a time, on every refresh of every client.
    #[test]
    fn one_topic_can_never_be_wider_than_the_ceiling() {
        assert_eq!(
            advertised_partitions(827_000, 1024),
            MAX_ADVERTISED_PARTITIONS as i32
        );
        assert_eq!(
            advertised_partitions(i64::MAX, 16),
            MAX_ADVERTISED_PARTITIONS as i32
        );
        // Just under it is untouched: the clamp is a ceiling, not a rounding.
        assert_eq!(
            advertised_partitions(MAX_ADVERTISED_PARTITIONS as i64 - 1, 16),
            MAX_ADVERTISED_PARTITIONS as i32 - 1
        );
    }

    /// `QUEEN_KAFKA_DEFAULT_PARTITIONS` is validated at boot against this same
    /// number (main.rs), so a configured width can never be one the metadata
    /// path would then clamp behind the operator's back.
    #[test]
    fn the_boot_knob_and_the_clamp_are_the_same_ceiling() {
        const MAIN: &str = include_str!("../main.rs");
        assert!(MAIN
            .contains("const MAX_DEFAULT_PARTITIONS: u32 = metadata::MAX_ADVERTISED_PARTITIONS;"));
        assert_eq!(
            advertised_partitions(0, MAX_ADVERTISED_PARTITIONS),
            MAX_ADVERTISED_PARTITIONS as i32
        );
    }

    #[test]
    fn an_unknown_topic_is_created_only_when_the_client_allows_it() {
        assert_eq!(plan("orders", None, true, 8), Plan::Create);
        assert_eq!(
            plan("orders", None, false, 8),
            Plan::Reject(ResponseError::UnknownTopicOrPartition)
        );
        assert_eq!(plan("orders", Some(3), true, 8), Plan::Serve(8));
        assert_eq!(plan("orders", Some(64), true, 8), Plan::Serve(64));
    }

    /// The rule that must hold whatever else changes: a `__` name is never
    /// created, in any combination of inputs.
    #[test]
    fn internal_names_are_unknown_and_never_created() {
        for name in ["__consumer_offsets", "__transaction_state", "__anything"] {
            for live in [None, Some(0), Some(12)] {
                for allow in [true, false] {
                    assert_eq!(
                        plan(name, live, allow, 8),
                        Plan::Reject(ResponseError::UnknownTopicOrPartition),
                        "{name} live={live:?} allow={allow}"
                    );
                }
            }
        }
    }

    #[test]
    fn an_unstorable_name_is_invalid_not_unknown() {
        assert_eq!(
            plan("my topic", None, true, 8),
            Plan::Reject(ResponseError::InvalidTopicException)
        );
        assert_eq!(
            plan(&"x".repeat(300), None, true, 8),
            Plan::Reject(ResponseError::InvalidTopicException)
        );
    }

    // ------------------------------------------------------------- the handler

    fn facade(queues: &[(&str, i64)], default_partitions: u32) -> (Facade, Arc<FakeQueen>) {
        let api = FakeQueen::with(queues);
        let facade = Facade {
            default_partitions,
            ..crate::handlers::testing::over(api.clone(), Default::default())
        };
        (facade, api)
    }

    fn request(names: Option<&[&str]>, allow_auto: bool) -> MetadataRequest {
        MetadataRequest::default()
            .with_topics(names.map(|ns| {
                ns.iter()
                    .map(|n| {
                        MetadataRequestTopic::default()
                            .with_name(Some(TopicName(StrBytes::from_string(n.to_string()))))
                    })
                    .collect()
            }))
            .with_allow_auto_topic_creation(allow_auto)
    }

    fn named<'a>(resp: &'a MetadataResponse, name: &str) -> &'a MetadataResponseTopic {
        resp.topics
            .iter()
            .find(|t| t.name.as_ref().map(|n| n.0.as_str()) == Some(name))
            .unwrap_or_else(|| panic!("{name} is not in the response"))
    }

    #[tokio::test]
    async fn the_cluster_is_one_broker_that_leads_everything() {
        let (f, _) = facade(&[("orders", 3)], 8);
        let resp = handle(&f, &request(Some(&["orders"]), false), 9, None).await;

        assert_eq!(resp.brokers.len(), 1);
        assert_eq!(resp.brokers[0].node_id.0, 0);
        assert_eq!(resp.brokers[0].host.as_str(), "kafka.example.com");
        assert_eq!(resp.brokers[0].port, 9092);
        assert_eq!(resp.controller_id.0, 0);
        assert_eq!(resp.cluster_id.as_ref().unwrap().as_str(), CLUSTER_ID);

        let t = named(&resp, "orders");
        assert_eq!(t.error_code, 0);
        assert!(!t.is_internal);
        assert_eq!(t.partitions.len(), 8, "3 live lanes, 8 configured");
        for (i, p) in t.partitions.iter().enumerate() {
            assert_eq!(p.partition_index, i as i32);
            assert_eq!(p.error_code, 0);
            assert_eq!(p.leader_id.0, 0);
            assert_eq!(p.replica_nodes.iter().map(|b| b.0).collect::<Vec<_>>(), [0]);
            assert_eq!(p.isr_nodes.iter().map(|b| b.0).collect::<Vec<_>>(), [0]);
            assert!(p.offline_replicas.is_empty());
        }
    }

    #[tokio::test]
    async fn a_null_topic_list_is_every_queue() {
        let (f, _) = facade(&[("orders", 3), ("clicks", 0), ("__internal", 2)], 4);
        let resp = handle(&f, &request(None, false), 9, None).await;

        let names: Vec<&str> = resp
            .topics
            .iter()
            .map(|t| t.name.as_ref().unwrap().0.as_str())
            .collect();
        assert_eq!(names, ["orders", "clicks"], "a __ queue is not a topic");
        assert_eq!(named(&resp, "orders").partitions.len(), 4);
        assert_eq!(named(&resp, "clicks").partitions.len(), 4);
    }

    /// The v0 quirk: an empty array there means all topics, not none.
    #[tokio::test]
    async fn an_empty_list_means_all_topics_at_v0_and_none_after() {
        let (f, _) = facade(&[("orders", 3)], 4);
        let all = handle(&f, &request(Some(&[]), true), 0, None).await;
        assert_eq!(all.topics.len(), 1);

        let none = handle(&f, &request(Some(&[]), false), 9, None).await;
        assert!(none.topics.is_empty());
        // The broker list is still there — that is what a client asking for no
        // topics wanted.
        assert_eq!(none.brokers.len(), 1);
    }

    /// The listing is the one request whose size is queues × width, and the one
    /// no client asked to be big. Past `conn::MAX_FRAME_BYTES` it cannot be
    /// encoded at all — the connection dies AFTER the allocation — so it is
    /// bounded, and what does not fit is left out rather than making the whole
    /// answer un-answerable.
    #[tokio::test]
    async fn the_all_topics_listing_is_bounded() {
        // 40 queues that would each advertise 1/8 of the budget: five fit.
        let width = (MAX_LISTING_PARTITIONS / 8) as u32;
        let names: Vec<String> = (0..40).map(|i| format!("q{i}")).collect();
        let queues: Vec<(&str, i64)> = names.iter().map(|n| (n.as_str(), 0i64)).collect();
        let (f, _) = facade(&queues, width);

        let resp = handle(&f, &request(None, false), 9, None).await;
        assert_eq!(resp.topics.len(), 8, "the budget is spent, not exceeded");
        let total: usize = resp.topics.iter().map(|t| t.partitions.len()).sum();
        assert!(total <= MAX_LISTING_PARTITIONS, "{total}");
        // Truncation is from the tail of the catalog order, so what a client
        // does get is stable between refreshes rather than reshuffling.
        let listed: Vec<&str> = resp
            .topics
            .iter()
            .map(|t| t.name.as_ref().unwrap().0.as_str())
            .collect();
        assert_eq!(listed, ["q0", "q1", "q2", "q3", "q4", "q5", "q6", "q7"]);
    }

    /// The NAMED form is bounded too, and for a different reason from the
    /// listing: its size is the client's own list, which is bounded by nothing
    /// but the frame. Everything past the ceiling is left out of the answer
    /// rather than turned into work.
    #[tokio::test]
    async fn a_request_that_names_more_topics_than_the_ceiling_is_answered_short() {
        let (f, api) = facade(&[("orders", 3)], 4);
        let names: Vec<String> = (0..MAX_REQUESTED_TOPICS + 500)
            .map(|i| format!("t{i}"))
            .collect();
        let borrowed: Vec<&str> = names.iter().map(String::as_str).collect();

        // With auto-create ON, so that an unbounded list would also be an
        // unbounded number of calls to Queen.
        let resp = handle(&f, &request(Some(&borrowed), true), 9, None).await;
        assert_eq!(resp.topics.len(), MAX_REQUESTED_TOPICS);
        // First-seen order, so what the client is told about is stable between
        // refreshes rather than a different slice each time.
        assert_eq!(
            resp.topics[0].name.as_ref().unwrap().0.as_str(),
            names[0].as_str()
        );
        assert!(
            api.creates.lock().unwrap().len() <= MAX_AUTO_CREATES_PER_REQUEST,
            "one request created {} queues",
            api.creates.lock().unwrap().len()
        );
    }

    /// ...and the number of topics one request CREATES is bounded below that
    /// again, because each one is a sequential call to Queen on a connection
    /// that is muted until the whole response is written.
    #[tokio::test]
    async fn one_request_creates_at_most_its_budget_and_the_rest_retry() {
        let (f, api) = facade(&[], 4);
        let names: Vec<String> = (0..MAX_AUTO_CREATES_PER_REQUEST + 25)
            .map(|i| format!("new-{i}"))
            .collect();
        let borrowed: Vec<&str> = names.iter().map(String::as_str).collect();

        let resp = handle(&f, &request(Some(&borrowed), true), 9, None).await;
        assert_eq!(
            api.creates.lock().unwrap().len(),
            MAX_AUTO_CREATES_PER_REQUEST
        );
        // Every topic is still ANSWERED — the ones past the budget with the
        // retriable code, which is what makes the client come back for them.
        assert_eq!(resp.topics.len(), names.len());
        let created = named(&resp, "new-0");
        assert_eq!(created.error_code, 0);
        let deferred = named(&resp, &names[names.len() - 1]);
        assert_eq!(
            deferred.error_code,
            ResponseError::LeaderNotAvailable.code()
        );

        // The retry creates the next batch: the fleet converges instead of one
        // request holding a connection open for a thousand upstream calls.
        handle(&f, &request(Some(&borrowed), true), 9, None).await;
        assert_eq!(api.creates.lock().unwrap().len(), names.len());
        let resp = handle(&f, &request(Some(&borrowed), true), 9, None).await;
        for name in &names {
            assert_eq!(named(&resp, name).error_code, 0, "{name}");
        }
    }

    /// The two ceilings compose: no single topic can be wide enough to exhaust
    /// a listing on its own, so the "the first topic always fits" arm of
    /// `listing` is a guard against a future re-tuning of the constants and
    /// never something a client meets. The widest queues Queen can hold are
    /// listed until the budget is spent, and no further.
    #[tokio::test]
    async fn the_widest_possible_queues_still_produce_a_usable_listing() {
        const _: () = assert!(MAX_ADVERTISED_PARTITIONS as usize <= MAX_LISTING_PARTITIONS);
        let (f, _) = facade(&[("a", 0), ("b", 0), ("c", 0)], MAX_ADVERTISED_PARTITIONS);
        let resp = handle(&f, &request(None, false), 9, None).await;
        assert_eq!(
            resp.topics.len(),
            MAX_LISTING_PARTITIONS / MAX_ADVERTISED_PARTITIONS as usize
        );
        assert_eq!(
            resp.topics[0].partitions.len(),
            MAX_ADVERTISED_PARTITIONS as usize
        );
    }

    /// A client that NAMES its topics is never truncated — which is every
    /// producer and every consumer, so the budget above costs them nothing.
    #[tokio::test]
    async fn naming_a_topic_is_never_subject_to_the_listing_budget() {
        let width = (MAX_LISTING_PARTITIONS / 8) as u32;
        let names: Vec<String> = (0..40).map(|i| format!("q{i}")).collect();
        let queues: Vec<(&str, i64)> = names.iter().map(|n| (n.as_str(), 0i64)).collect();
        let (f, _) = facade(&queues, width);

        let resp = handle(&f, &request(Some(&["q39"]), false), 9, None).await;
        assert_eq!(named(&resp, "q39").error_code, 0);
        assert_eq!(named(&resp, "q39").partitions.len(), width as usize);
    }

    #[tokio::test]
    async fn a_queue_kafka_cannot_name_is_left_out_of_the_listing() {
        let (f, _) = facade(&[("orders", 1), ("with space", 1)], 2);
        let resp = handle(&f, &request(None, false), 9, None).await;
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(named(&resp, "orders").error_code, 0);
    }

    #[tokio::test]
    async fn auto_create_creates_once_and_answers_the_default_width() {
        let (f, api) = facade(&[], 16);
        let resp = handle(&f, &request(Some(&["orders"]), true), 9, None).await;

        assert_eq!(api.created(), ["orders"]);
        let t = named(&resp, "orders");
        assert_eq!(t.error_code, 0);
        assert_eq!(t.partitions.len(), 16);

        // The second refresh finds it in the catalog and creates nothing.
        let again = handle(&f, &request(Some(&["orders"]), true), 9, None).await;
        assert_eq!(api.created(), ["orders"], "created twice");
        assert_eq!(named(&again, "orders").partitions.len(), 16);
    }

    #[tokio::test]
    async fn auto_create_is_refused_when_the_client_did_not_ask_for_it() {
        let (f, api) = facade(&[], 16);
        let resp = handle(&f, &request(Some(&["orders"]), false), 9, None).await;
        assert!(api.created().is_empty());
        assert_eq!(
            named(&resp, "orders").error_code,
            ResponseError::UnknownTopicOrPartition.code()
        );
        assert!(named(&resp, "orders").partitions.is_empty());
    }

    #[tokio::test]
    async fn an_internal_topic_is_unknown_and_never_reaches_queen() {
        let (f, api) = facade(&[], 16);
        let resp = handle(&f, &request(Some(&["__consumer_offsets"]), true), 9, None).await;
        assert!(
            api.created().is_empty(),
            "a Kafka internal name was created"
        );
        assert_eq!(
            named(&resp, "__consumer_offsets").error_code,
            ResponseError::UnknownTopicOrPartition.code()
        );
    }

    /// Even when a Queen queue with that name exists, it is not exposed: the
    /// name belongs to Kafka's own bookkeeping and a client that found it there
    /// would treat it as the offsets topic.
    #[tokio::test]
    async fn an_existing_queue_with_an_internal_name_stays_hidden() {
        let (f, _) = facade(&[("__consumer_offsets", 50)], 16);
        let resp = handle(&f, &request(Some(&["__consumer_offsets"]), true), 9, None).await;
        assert_eq!(
            named(&resp, "__consumer_offsets").error_code,
            ResponseError::UnknownTopicOrPartition.code()
        );
    }

    #[tokio::test]
    async fn an_invalid_name_is_rejected_without_touching_queen() {
        let (f, api) = facade(&[], 16);
        let resp = handle(&f, &request(Some(&["not a topic"]), true), 9, None).await;
        assert!(api.created().is_empty());
        assert_eq!(
            named(&resp, "not a topic").error_code,
            ResponseError::InvalidTopicException.code()
        );
    }

    #[tokio::test]
    async fn a_duplicated_topic_is_answered_once() {
        let (f, _) = facade(&[("orders", 1)], 2);
        let resp = handle(&f, &request(Some(&["orders", "orders"]), false), 9, None).await;
        assert_eq!(resp.topics.len(), 1);
    }

    /// Queen unreachable, nothing cached: retriable per topic, and the broker
    /// list still stands so the client knows where to retry.
    #[tokio::test]
    async fn an_unreachable_queen_is_retriable_not_unknown() {
        let (f, api) = facade(&[], 16);
        api.fail_with("connection refused");
        let resp = handle(&f, &request(Some(&["orders"]), true), 9, None).await;
        assert_eq!(resp.brokers.len(), 1);
        assert_eq!(
            named(&resp, "orders").error_code,
            ResponseError::LeaderNotAvailable.code()
        );
        assert!(api.created().is_empty());
    }

    /// A queue created natively inside the cache window is NOT re-configured:
    /// the auto-create path re-reads the catalog first, because `/configure`
    /// would rewrite that queue's options to the defaults.
    #[tokio::test]
    async fn a_queue_that_appeared_since_the_last_refresh_is_not_reconfigured() {
        let (f, api) = facade(&[], 16);
        // A metadata refresh caches an empty world...
        f.catalog.list(None).await.unwrap();
        // ...and then someone creates the queue natively, with 40 lanes.
        api.queues.lock().unwrap().push(crate::queen::Queue {
            name: "orders".into(),
            partitions: 40,
            id: None,
            floor: None,
        });

        let resp = handle(&f, &request(Some(&["orders"]), true), 9, None).await;
        assert!(
            api.created().is_empty(),
            "an existing queue was reconfigured"
        );
        assert_eq!(named(&resp, "orders").error_code, 0);
        assert_eq!(named(&resp, "orders").partitions.len(), 40);
    }

    /// One request naming K new topics is 1 list + 1 re-read + K creates, not
    /// 1 + 2K. The re-read is the thing that stops `/configure` resetting a
    /// queue that already exists, and one of them covers every name in the
    /// request; the Kafka connection is muted for the whole round trip
    /// (conn.rs), so each avoided 10-second call is dead air on the wire.
    #[tokio::test]
    async fn one_request_creating_many_topics_re_reads_once() {
        let (f, api) = facade(&[], 4);
        let resp = handle(&f, &request(Some(&["a", "b", "c"]), true), 9, None).await;

        assert_eq!(api.created(), ["a", "b", "c"]);
        // One plan list, one shared re-read before the creates, and one AFTER
        // them for the queue ids the config records are pinned to
        // (`record_auto_creates`). Three for the request, not three per topic,
        // and none of them on a Metadata that creates nothing.
        assert_eq!(api.list_count(), 3);
        for n in ["a", "b", "c"] {
            assert_eq!(named(&resp, n).error_code, 0);
            assert_eq!(named(&resp, n).partitions.len(), 4);
        }
    }

    // ------------------------------------------------------- the width floor

    /// Seed a topic's stored width floor, the way a CreateTopics that declared
    /// one would have left it. `qid` is `None` because `FakeQueen::with` builds
    /// queues with no id, and `None == None` is the match the record's own gate
    /// documents.
    fn seed_floor(api: &FakeQueen, topic: &str, floor: u32) {
        api.kv_seed(
            crate::offsets::NAMESPACE,
            &topic_record::key(topic),
            serde_json::json!({
                "qid": null,
                "set": {},
                "at": 1_787_824_800_123i64,
                "partitions": floor,
            }),
        );
    }

    fn width_of(r: &MetadataResponse, topic: &str) -> usize {
        r.topics
            .iter()
            .find(|t| t.name.as_ref().is_some_and(|n| n.as_str() == topic))
            .unwrap_or_else(|| panic!("{topic} is not in the response"))
            .partitions
            .len()
    }

    /// The whole feature, end to end: a topic that declared its own floor is
    /// advertised at it, and a topic that declared none still follows
    /// `QUEEN_KAFKA_DEFAULT_PARTITIONS` — in the SAME response, which is what
    /// makes the floor per-topic rather than a second global knob.
    #[tokio::test]
    async fn a_declared_floor_widens_only_the_topic_that_declared_it() {
        let (f, api) = facade(&[("wide", 0), ("plain", 0)], 4);
        seed_floor(&api, "wide", 64);

        let r = handle(&f, &request(Some(&["wide", "plain"]), false), 9, None).await;
        assert_eq!(width_of(&r, "wide"), 64, "the declared floor applies");
        assert_eq!(
            width_of(&r, "plain"),
            4,
            "and only to the topic that declared it"
        );
    }

    /// It is a FLOOR and never a cap. A topic with more live lanes than its
    /// declared floor keeps the lanes: narrowing a Kafka partition count
    /// re-hashes live keys onto different lanes and loses ordering, which is the
    /// one outcome this number may never cause.
    #[tokio::test]
    async fn a_floor_never_narrows_a_topic_that_already_has_more_lanes() {
        let (f, api) = facade(&[("busy", 128)], 4);
        seed_floor(&api, "busy", 16);

        let r = handle(&f, &request(Some(&["busy"]), false), 9, None).await;
        assert_eq!(
            width_of(&r, "busy"),
            128,
            "the lanes win over a lower floor"
        );
    }

    /// A floor pinned to a queue id that is not the one there now describes a
    /// queue that was dropped and recreated under the same name. Advertising its
    /// width would be advertising a number nothing enforces.
    #[tokio::test]
    async fn a_floor_from_a_recreated_queue_is_not_advertised() {
        let (f, api) = facade(&[], 4);
        api.queues.lock().unwrap().push(crate::queen::Queue {
            name: "orders".to_string(),
            partitions: 0,
            id: Some("q-NEW".to_string()),
            floor: None,
        });
        api.kv_seed(
            crate::offsets::NAMESPACE,
            &topic_record::key("orders"),
            serde_json::json!({"qid": "q-OLD", "set": {}, "at": 1, "partitions": 64}),
        );

        let r = handle(&f, &request(Some(&["orders"]), false), 9, None).await;
        assert_eq!(
            width_of(&r, "orders"),
            4,
            "a stale record's width must not be advertised"
        );
    }

    /// A request with nothing to create does not re-read at all, and does not
    /// touch the record store either — which is every Metadata after the first,
    /// on the hottest path this facade has.
    #[tokio::test]
    async fn a_request_that_creates_nothing_costs_one_call() {
        let (f, api) = facade(&[("orders", 2)], 4);
        handle(&f, &request(Some(&["orders"]), true), 9, None).await;
        assert_eq!(api.list_count(), 1);
        // The COLD list also scans for width floors: once for the whole tenant,
        // concurrently with the list, and never per topic.
        let scans = api.kv_calls.lock().unwrap().clone();
        assert_eq!(scans.len(), 1);
        assert!(
            crate::topic_record::is_floor_scan(&scans[0]),
            "the only KV call a plain Metadata may make is the width scan"
        );

        // ...and the claim this test is named for: every Metadata AFTER the
        // first is served from the cache and costs nothing at all — no list, no
        // scan, no record read. This is the property the width floor had to keep.
        api.kv_calls.lock().unwrap().clear();
        handle(&f, &request(Some(&["orders"]), true), 9, None).await;
        assert_eq!(api.list_count(), 1);
        assert!(api.kv_calls.lock().unwrap().is_empty());
    }

    /// An auto-create records an EMPTY bag ([`crate::topic_record`]), which is
    /// literally what it posted: `Catalog::create` sends `/configure` with no
    /// options, so every one of the nineteen columns is at the stored
    /// procedure's default. Writing that down is what makes a later
    /// `kafka-configs.sh --alter` on an auto-created topic land instead of
    /// being refused as untracked.
    #[tokio::test]
    async fn an_auto_create_records_the_empty_bag_it_sent() {
        let (f, api) = facade(&[], 4);
        handle(&f, &request(Some(&["a", "b"]), true), 9, None).await;

        for topic in ["a", "b"] {
            assert_eq!(
                api.kv_get(crate::offsets::NAMESPACE, &topic_record::key(topic))
                    .unwrap_or_else(|| panic!("{topic} was auto-created without a record"))["set"],
                serde_json::json!({})
            );
        }
        // ONE record write for the whole request, not one per topic. The width
        // scans are excluded rather than counted: an auto-create drops the
        // catalog entry, so how many cold refreshes follow is this handler's
        // business and not what this test is about.
        let calls = api.kv_calls.lock().unwrap().clone();
        assert_eq!(
            calls
                .iter()
                .filter(|ops| !crate::topic_record::is_floor_scan(ops))
                .count(),
            1
        );
    }

    /// ...and an auto-create whose record could not be written still serves the
    /// topic. The queue exists; the topic simply behaves as untracked until an
    /// alter re-establishes it.
    #[tokio::test]
    async fn an_auto_create_whose_record_write_fails_still_serves_the_topic() {
        let (f, api) = facade(&[], 4);
        api.fail_kv(crate::queen::Error::Transport("kv is down".into()));
        let resp = handle(&f, &request(Some(&["orders"]), true), 9, None).await;

        assert_eq!(named(&resp, "orders").error_code, 0);
        assert_eq!(api.created(), ["orders"]);
        assert!(api
            .kv_get(crate::offsets::NAMESPACE, &topic_record::key("orders"))
            .is_none());
    }

    #[tokio::test]
    async fn a_failed_auto_create_is_retriable() {
        let (f, api) = facade(&[], 16);
        // The list succeeds and is cached; the create then fails.
        f.catalog.list(None).await.unwrap();
        api.fail_with("500 from configure");
        let resp = handle(&f, &request(Some(&["orders"]), true), 9, None).await;
        assert_eq!(
            named(&resp, "orders").error_code,
            ResponseError::LeaderNotAvailable.code()
        );
    }

    #[tokio::test]
    async fn the_token_reaches_every_call() {
        let (f, api) = facade(&[], 4);
        handle(&f, &request(Some(&["orders"]), true), 9, Some("tenant-a")).await;
        let tokens = api.tokens.lock().unwrap().clone();
        assert!(!tokens.is_empty());
        assert!(tokens.iter().all(|t| t.as_deref() == Some("tenant-a")));
    }

    // -------------------------------------------------------- wire round-trip

    /// Every advertised version encodes and decodes cleanly, with the fields a
    /// client reads surviving the trip. The response is encoded by the broker
    /// half of `kafka-protocol` and decoded by the client half — the same two
    /// halves a real client sits either side of.
    #[tokio::test]
    async fn the_response_round_trips_at_every_advertised_version() {
        let (f, _) = facade(&[("orders", 2)], 3);
        let row = crate::versions::lookup(kafka_protocol::messages::ApiKey::Metadata as i16)
            .expect("Metadata is advertised");

        for version in row.min..=row.max {
            let resp = handle(&f, &request(Some(&["orders"]), false), version, None).await;
            let mut wire = bytes::BytesMut::new();
            resp.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode v{version}: {e}"));

            let mut buf = wire.freeze();
            let back = MetadataResponse::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: {} trailing bytes", buf.len());

            assert_eq!(back.brokers.len(), 1, "v{version}");
            assert_eq!(back.brokers[0].host.as_str(), "kafka.example.com");
            assert_eq!(back.brokers[0].port, 9092);
            let t = named(&back, "orders");
            assert_eq!(t.error_code, 0, "v{version}");
            assert_eq!(t.partitions.len(), 3, "v{version}");
            assert_eq!(t.partitions[2].partition_index, 2);
            assert_eq!(t.partitions[2].leader_id.0, 0);
            // Fields that only exist from a given version up are checked where
            // they exist, and are the defaults below it.
            if version >= 1 {
                assert_eq!(back.controller_id.0, 0, "v{version}");
            }
            if version >= 2 {
                assert_eq!(
                    back.cluster_id.as_ref().map(|c| c.as_str()),
                    Some(CLUSTER_ID),
                    "v{version}"
                );
            }
        }
    }

    /// A request built by a client at every advertised version decodes here into
    /// the same intent. `allow_auto_topic_creation` is the one that matters: it
    /// does not exist below v4, where the broker's own configuration decided, so
    /// the schema pins it to "allowed" there.
    #[test]
    fn the_request_decodes_the_same_intent_at_every_advertised_version() {
        let row = crate::versions::lookup(kafka_protocol::messages::ApiKey::Metadata as i16)
            .expect("Metadata is advertised");
        assert!(
            row.min >= MetadataRequest::VERSIONS.min && row.max <= MetadataRequest::VERSIONS.max
        );

        for version in row.min..=row.max {
            for allow in [true, false] {
                // Below v4 the field cannot be encoded as anything but the
                // schema default (true), so only that half of the matrix exists.
                if version < 4 && !allow {
                    continue;
                }
                let req = request(Some(&["orders"]), allow);
                let mut wire = bytes::BytesMut::new();
                req.encode(&mut wire, version)
                    .unwrap_or_else(|e| panic!("encode v{version}: {e}"));
                let mut buf = wire.freeze();
                let back = MetadataRequest::decode(&mut buf, version)
                    .unwrap_or_else(|e| panic!("decode v{version}: {e}"));
                assert!(buf.is_empty(), "v{version}: {} trailing bytes", buf.len());
                assert_eq!(back.allow_auto_topic_creation, allow, "v{version}");
                assert_eq!(
                    requested_names(&back, version),
                    Some(vec![Some("orders".to_string())]),
                    "v{version}"
                );
            }
        }
    }

    #[test]
    fn a_null_topic_array_reads_as_all_topics_at_every_version() {
        let row =
            crate::versions::lookup(kafka_protocol::messages::ApiKey::Metadata as i16).unwrap();
        for version in row.min..=row.max {
            let req = MetadataRequest::default().with_topics(None);
            // v0 cannot encode a null array; the form starts at v1.
            if version == 0 {
                continue;
            }
            let mut wire = bytes::BytesMut::new();
            req.encode(&mut wire, version).unwrap();
            let mut buf = wire.freeze();
            let back = MetadataRequest::decode(&mut buf, version).unwrap();
            assert_eq!(requested_names(&back, version), None, "v{version}");
        }
    }

    /// A capped tenant asking for metadata is told to wait, and its topics stay
    /// retriable rather than becoming unknown. See [`crate::throttle`].
    #[tokio::test]
    async fn a_throttled_metadata_carries_the_wait_beside_a_retriable_topic() {
        let (f, api) = facade(&[("orders", 4)], 4);
        api.fail_list(crate::queen::Error::Status {
            code: 429,
            body: r#"{"error":"request rate limit exceeded","code":"rate_limited"}"#.into(),
            retry_after_ms: Some(2_000),
        });
        let resp = handle(&f, &request(Some(&["orders"]), false), 9, None).await;

        assert_eq!(resp.throttle_time_ms, 2_000);
        assert_eq!(
            named(&resp, "orders").error_code,
            ResponseError::LeaderNotAvailable.code(),
            "a throttled tenant must not be told its topic is gone"
        );
    }

    /// A queue list that simply failed carries no throttle: the client retries
    /// on its own timer, which is right when nothing said when to come back.
    #[tokio::test]
    async fn an_unreadable_queue_list_carries_no_throttle() {
        let (f, api) = facade(&[("orders", 4)], 4);
        api.fail_list(crate::queen::Error::status(503, "draining"));
        let resp = handle(&f, &request(Some(&["orders"]), false), 9, None).await;
        assert_eq!(resp.throttle_time_ms, 0);
        assert_eq!(
            named(&resp, "orders").error_code,
            ResponseError::LeaderNotAvailable.code()
        );
    }
}

#[cfg(test)]
mod clustered {
    //! Metadata in cluster mode: the whole live set, one deterministic leader
    //! map, and the two fields that deliberately do NOT grow with it.
    use super::*;
    use crate::handlers::testing::clustered;
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
    use kafka_protocol::messages::MetadataRequest;

    const THREE: [(i32, &str, u16); 3] = [
        (1, "kafka-1.example.com", 9092),
        (2, "kafka-2.example.com", 9093),
        (3, "kafka-3.example.com", 9094),
    ];

    fn named(topic: &str) -> MetadataRequest {
        MetadataRequest::default()
            .with_topics(Some(vec![MetadataRequestTopic::default().with_name(Some(
                TopicName(StrBytes::from_string(topic.to_string())),
            ))]))
            .with_allow_auto_topic_creation(false)
    }

    /// THE fix for "both facades are the only broker": every node answers all
    /// three, at their own addresses, with the lowest live id as controller.
    #[tokio::test]
    async fn the_answer_is_the_whole_live_set() {
        for me in [1, 2, 3] {
            let (f, _) = clustered(&[("orders", 2)], &THREE, me);
            let resp = handle(&f, &named("orders"), 9, None).await;

            let brokers: Vec<(i32, String, i32)> = resp
                .brokers
                .iter()
                .map(|b| (b.node_id.0, b.host.as_str().to_string(), b.port))
                .collect();
            assert_eq!(
                brokers,
                vec![
                    (1, "kafka-1.example.com".to_string(), 9092),
                    (2, "kafka-2.example.com".to_string(), 9093),
                    (3, "kafka-3.example.com".to_string(), 9094),
                ],
                "node {me}"
            );
            assert_eq!(
                resp.controller_id.0, 1,
                "the controller is not the lowest id"
            );
            assert_eq!(resp.cluster_id.as_ref().unwrap().as_str(), "rig");
        }
    }

    /// The leader map is a pure function of the live set, so three nodes
    /// answering the same request agree partition by partition — and every
    /// leader is a node that exists.
    #[tokio::test]
    async fn the_leader_map_is_identical_from_every_node() {
        let mut maps = Vec::new();
        for me in [1, 2, 3] {
            let (f, _) = clustered(&[("orders", 2)], &THREE, me);
            let resp = handle(&f, &named("orders"), 9, None).await;
            let topic = &resp.topics[0];
            maps.push(
                topic
                    .partitions
                    .iter()
                    .map(|p| p.leader_id.0)
                    .collect::<Vec<i32>>(),
            );
        }
        assert!(maps.windows(2).all(|w| w[0] == w[1]), "{maps:?}");
        assert!(maps[0].iter().all(|id| (1..=3).contains(id)));
        // The facade's fixture width is 4 lanes, and they are not all on one
        // node — a "deterministic" map that put everything on one node would
        // pass every equality above.
        assert!(
            maps[0]
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                > 1,
            "every partition landed on one node: {:?}",
            maps[0]
        );
    }

    /// What does NOT change with a cluster: the replica list is the leader
    /// alone (replication is Postgres's business) and the epoch is still -1.
    #[tokio::test]
    async fn replicas_isr_and_epoch_are_unchanged() {
        let (f, _) = clustered(&[("orders", 2)], &THREE, 2);
        let resp = handle(&f, &named("orders"), 9, None).await;
        for p in &resp.topics[0].partitions {
            assert_eq!(p.replica_nodes, vec![p.leader_id]);
            assert_eq!(p.isr_nodes, vec![p.leader_id]);
            assert!(p.offline_replicas.is_empty());
            assert_eq!(p.leader_epoch, -1);
        }
    }

    /// A stale view still advertises the cluster it last saw: a Metadata that
    /// suddenly reported one broker would tell every client the cluster shrank,
    /// and they would all reconnect to one node.
    #[tokio::test(start_paused = true)]
    async fn a_stale_view_still_advertises_the_last_known_cluster() {
        let (f, _) = clustered(&[("orders", 2)], &THREE, 2);
        let ttl = f.cluster.state().unwrap().ttl;
        tokio::time::advance(ttl + std::time::Duration::from_secs(60)).await;
        let resp = handle(&f, &named("orders"), 9, None).await;
        assert_eq!(resp.brokers.len(), 3);
    }
}

#[cfg(test)]
mod golden {
    //! THE regression gate for cluster mode: with `QUEEN_KAFKA_NODE_ID` unset,
    //! the two responses a client bootstraps on must be what they were before
    //! cluster mode existed — not equivalent, IDENTICAL, byte for byte, at
    //! every advertised version.
    //!
    //! The buffers below were captured from the pre-cluster code
    //! (`git show HEAD:protocols/queen-kafka/src/handlers/metadata.rs`, node 0 everywhere
    //! and a hard-coded broker list) and are asserted against the code as it is
    //! now. A change that moves a byte of the single-node answer fails here,
    //! which is the only way an operator who set nothing could ever notice.
    use super::*;
    use crate::handlers::testing::facade;
    use bytes::BytesMut;
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
    use kafka_protocol::messages::{FindCoordinatorRequest, MetadataRequest};
    use kafka_protocol::protocol::Encodable;

    /// One captured response per advertised Metadata version, v0..=v9.
    const METADATA: [&str; 10] = [
        // v0
        concat!(
            "000000010000000000116b61666b612e6578616d706c652e636f6d000023840000000100",
            "0000066f7264657273000000040000000000000000000000000001000000000000000100",
            "000000000000000001000000000000000100000000000000010000000000000000000200",
            "000000000000010000000000000001000000000000000000030000000000000001000000",
            "000000000100000000",
        ),
        // v1
        concat!(
            "000000010000000000116b61666b612e6578616d706c652e636f6d00002384ffff000000",
            "0000000001000000066f7264657273000000000400000000000000000000000000010000",
            "000000000001000000000000000000010000000000000001000000000000000100000000",
            "000000000002000000000000000100000000000000010000000000000000000300000000",
            "00000001000000000000000100000000",
        ),
        // v2
        concat!(
            "000000010000000000116b61666b612e6578616d706c652e636f6d00002384ffff000571",
            "7565656e0000000000000001000000066f72646572730000000004000000000000000000",
            "000000000100000000000000010000000000000000000100000000000000010000000000",
            "000001000000000000000000020000000000000001000000000000000100000000000000",
            "0000030000000000000001000000000000000100000000",
        ),
        // v3
        concat!(
            "00000000000000010000000000116b61666b612e6578616d706c652e636f6d00002384ff",
            "ff0005717565656e0000000000000001000000066f726465727300000000040000000000",
            "000000000000000001000000000000000100000000000000000001000000000000000100",
            "000000000000010000000000000000000200000000000000010000000000000001000000",
            "000000000000030000000000000001000000000000000100000000",
        ),
        // v4
        concat!(
            "00000000000000010000000000116b61666b612e6578616d706c652e636f6d00002384ff",
            "ff0005717565656e0000000000000001000000066f726465727300000000040000000000",
            "000000000000000001000000000000000100000000000000000001000000000000000100",
            "000000000000010000000000000000000200000000000000010000000000000001000000",
            "000000000000030000000000000001000000000000000100000000",
        ),
        // v5
        concat!(
            "00000000000000010000000000116b61666b612e6578616d706c652e636f6d00002384ff",
            "ff0005717565656e0000000000000001000000066f726465727300000000040000000000",
            "000000000000000001000000000000000100000000000000000000000000010000000000",
            "000001000000000000000100000000000000000000000000020000000000000001000000",
            "000000000100000000000000000000000000030000000000000001000000000000000100",
            "00000000000000",
        ),
        // v6
        concat!(
            "00000000000000010000000000116b61666b612e6578616d706c652e636f6d00002384ff",
            "ff0005717565656e0000000000000001000000066f726465727300000000040000000000",
            "000000000000000001000000000000000100000000000000000000000000010000000000",
            "000001000000000000000100000000000000000000000000020000000000000001000000",
            "000000000100000000000000000000000000030000000000000001000000000000000100",
            "00000000000000",
        ),
        // v7
        concat!(
            "00000000000000010000000000116b61666b612e6578616d706c652e636f6d00002384ff",
            "ff0005717565656e0000000000000001000000066f726465727300000000040000000000",
            "0000000000ffffffff000000010000000000000001000000000000000000000000000100",
            "000000ffffffff0000000100000000000000010000000000000000000000000002000000",
            "00ffffffff000000010000000000000001000000000000000000000000000300000000ff",
            "ffffff0000000100000000000000010000000000000000",
        ),
        // v8
        concat!(
            "00000000000000010000000000116b61666b612e6578616d706c652e636f6d00002384ff",
            "ff0005717565656e0000000000000001000000066f726465727300000000040000000000",
            "0000000000ffffffff000000010000000000000001000000000000000000000000000100",
            "000000ffffffff0000000100000000000000010000000000000000000000000002000000",
            "00ffffffff000000010000000000000001000000000000000000000000000300000000ff",
            "ffffff00000001000000000000000100000000000000008000000080000000",
        ),
        // v9
        concat!(
            "000000000200000000126b61666b612e6578616d706c652e636f6d000023840000067175",
            "65656e00000000020000076f7264657273000500000000000000000000ffffffff020000",
            "00000200000000010000000000000100000000ffffffff02000000000200000000010000",
            "000000000200000000ffffffff02000000000200000000010000000000000300000000ff",
            "ffffff02000000000200000000010080000000008000000000",
        ),
    ];

    /// The same for FindCoordinator, v0..=v3.
    const FIND_COORDINATOR: [&str; 4] = [
        // v0
        "00000000000000116b61666b612e6578616d706c652e636f6d00002384",
        // v1
        "00000000000000000000000000116b61666b612e6578616d706c652e636f6d00002384",
        // v2
        "00000000000000000000000000116b61666b612e6578616d706c652e636f6d00002384",
        // v3
        "0000000000000100000000126b61666b612e6578616d706c652e636f6d0000238400",
    ];

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// A facade over a queue that exists, so the answer is `Serve` and nothing
    /// is auto-created: the bytes must not depend on a call to Queen.
    #[tokio::test]
    async fn a_single_node_metadata_answer_is_unchanged_at_every_version() {
        let f = facade(&[("orders", 2)]);
        for (version, want) in METADATA.iter().enumerate() {
            let version = version as i16;
            let req = MetadataRequest::default()
                .with_topics(Some(vec![MetadataRequestTopic::default()
                    .with_name(Some(TopicName(StrBytes::from_static_str("orders"))))]))
                .with_allow_auto_topic_creation(false);
            let resp = handle(&f, &req, version, None).await;
            let mut buf = BytesMut::new();
            resp.encode(&mut buf, version)
                .unwrap_or_else(|e| panic!("encode v{version}: {e}"));
            assert_eq!(hex(&buf), *want, "Metadata v{version} changed on the wire");
        }
    }

    #[test]
    fn a_single_node_find_coordinator_answer_is_unchanged_at_every_version() {
        let f = facade(&[]);
        for (version, want) in FIND_COORDINATOR.iter().enumerate() {
            let version = version as i16;
            let req = FindCoordinatorRequest::default()
                .with_key(StrBytes::from_static_str("orders-consumer"))
                .with_key_type(0);
            let resp = crate::handlers::find_coordinator::handle(&f, &req);
            let mut buf = BytesMut::new();
            resp.encode(&mut buf, version)
                .unwrap_or_else(|e| panic!("encode v{version}: {e}"));
            assert_eq!(
                hex(&buf),
                *want,
                "FindCoordinator v{version} changed on the wire"
            );
        }
    }
}
