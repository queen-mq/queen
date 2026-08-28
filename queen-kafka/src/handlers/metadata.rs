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
//! So the advertised width is `max(live, QUEEN_KAFKA_DEFAULT_PARTITIONS)`:
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

use std::collections::HashSet;

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::{MetadataRequest, MetadataResponse, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::Facade;

/// The facade is the whole cluster, and it is node 0 of it.
pub const NODE_ID: i32 = 0;

/// Reported from Metadata v2 up. Clients use it for logging and for refusing to
/// talk to two different clusters through one connection pool, so it has to be
/// stable across restarts — hence a constant and not something derived from the
/// process or the broker list.
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
/// is no such queue.
pub fn plan(
    name: &str,
    live: Option<i64>,
    allow_auto_create: bool,
    default_partitions: u32,
) -> Plan {
    if let Some(e) = reserved_or_invalid(name) {
        return Plan::Reject(e);
    }
    match live {
        Some(live) => Plan::Serve(advertised_partitions(live, default_partitions)),
        None if allow_auto_create => Plan::Create,
        // The client asked us not to create it, so the honest answer is that it
        // is not here. This is also the code a consumer subscribed to a topic
        // nobody has produced to yet sees, and it retries — as it should.
        None => Plan::Reject(ResponseError::UnknownTopicOrPartition),
    }
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
fn reserved_or_invalid(name: &str) -> Option<ResponseError> {
    if name.starts_with(INTERNAL_PREFIX) {
        return Some(ResponseError::UnknownTopicOrPartition);
    }
    if !is_valid_topic_name(name) {
        return Some(ResponseError::InvalidTopicException);
    }
    None
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
/// the module header for why it is a maximum and not either input alone.
pub fn advertised_partitions(live: i64, default_partitions: u32) -> i32 {
    live.max(default_partitions as i64)
        .clamp(0, i32::MAX as i64) as i32
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
    let catalog = match facade.catalog.list(token).await {
        Ok(queues) => Some(queues),
        Err(e) => {
            tracing::error!(target: "kafka", error = %e, "metadata: cannot read the queue list");
            None
        }
    };

    let topics = match requested_names(req, api_version) {
        // The all-topics form: whatever Queen has, minus the names that are not
        // Kafka topics. A queue with a name Kafka cannot express is SKIPPED
        // rather than reported as an error — a client asked for a listing, not
        // for that queue, and an error entry for a topic it never named makes
        // some clients discard the whole response.
        None => catalog
            .as_deref()
            .map(|queues| {
                queues
                    .iter()
                    .filter(|q| reserved_or_invalid(&q.name).is_none())
                    .map(|q| {
                        topic(
                            &q.name,
                            Plan::Serve(advertised_partitions(
                                q.partitions,
                                facade.default_partitions,
                            )),
                        )
                    })
                    .collect()
            })
            // Nothing cached and Queen is unreachable: an empty listing is the
            // only shape available, and it is transient — the client refreshes.
            .unwrap_or_default(),
        Some(names) => {
            let mut out = Vec::with_capacity(names.len());
            for name in names {
                let Some(name) = name else {
                    // A null name in a request. Every version this facade
                    // advertises addresses topics by name, so there is nothing
                    // to look up; the entry is echoed back with its own error
                    // rather than dropped, because a client matches the response
                    // topics against the ones it asked for.
                    out.push(
                        MetadataResponseTopic::default()
                            .with_name(None)
                            .with_error_code(ResponseError::InvalidTopicException.code()),
                    );
                    continue;
                };
                let Some(queues) = catalog.as_deref() else {
                    // Retriable, and the code a client already expects while a
                    // topic's leader is being established: it backs off and asks
                    // again, which is exactly right for "Queen blipped".
                    out.push(topic(
                        &name,
                        Plan::Reject(ResponseError::LeaderNotAvailable),
                    ));
                    continue;
                };
                let live = queues.iter().find(|q| q.name == name).map(|q| q.partitions);
                let planned = plan(
                    &name,
                    live,
                    req.allow_auto_topic_creation,
                    facade.default_partitions,
                );
                let planned = match planned {
                    Plan::Create => create(facade, &name, token).await,
                    other => other,
                };
                out.push(topic(&name, planned));
            }
            out
        }
    };

    MetadataResponse::default()
        .with_brokers(vec![MetadataResponseBroker::default()
            .with_node_id(NODE_ID.into())
            .with_host(StrBytes::from_string(facade.advertised_host.clone()))
            .with_port(facade.advertised_port as i32)
            .with_rack(None)])
        .with_cluster_id(Some(StrBytes::from_static_str(CLUSTER_ID)))
        .with_controller_id(NODE_ID.into())
        .with_topics(topics)
}

/// Create the queue, and report what to answer for it.
///
/// The catalog is re-read first, and not from cache. `POST /api/v1/configure` is
/// an upsert that rewrites every config column to its default
/// (server/sql/procedures/012_configure.sql), so calling it for a queue that
/// already exists resets that queue's leaseTime, retention and dedup window —
/// and the snapshot the plan was made from can be up to the cache TTL old. This
/// is once per topic that genuinely does not exist, not once per refresh.
async fn create(facade: &Facade, name: &str, token: Option<&str>) -> Plan {
    match facade.catalog.refresh(token).await {
        Ok(queues) => {
            if let Some(q) = queues.iter().find(|q| q.name == name) {
                return Plan::Serve(advertised_partitions(
                    q.partitions,
                    facade.default_partitions,
                ));
            }
        }
        Err(e) => {
            tracing::error!(
                target: "kafka",
                topic = name,
                error = %e,
                "cannot confirm the queue is absent; not creating it"
            );
            return Plan::Reject(ResponseError::LeaderNotAvailable);
        }
    }
    match facade.catalog.create(name, token).await {
        Ok(()) => {
            tracing::info!(
                target: "kafka",
                topic = name,
                partitions = facade.default_partitions,
                "auto-created a queue for a Kafka topic"
            );
            // The queue exists but has no partitions yet: Queen materialises
            // them on the first push. The advertised width is the configured
            // default, which is the same number the next refresh will compute
            // through `advertised_partitions`.
            Plan::Serve(facade.default_partitions as i32)
        }
        Err(e) => {
            tracing::error!(target: "kafka", topic = name, error = %e, "auto-create failed");
            Plan::Reject(ResponseError::LeaderNotAvailable)
        }
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
    Some(
        topics
            .iter()
            .map(|t| t.name.as_ref().map(|n| n.0.as_str().to_string()))
            .filter(|n| match n {
                Some(n) => seen.insert(n.clone()),
                None => true,
            })
            .collect(),
    )
}

/// One topic entry, from a name and the decision made about it.
fn topic(name: &str, planned: Plan) -> MetadataResponseTopic {
    let base = MetadataResponseTopic::default()
        .with_name(Some(TopicName(StrBytes::from_string(name.to_string()))))
        // Never true: the facade owns no internal topics, and the names Kafka
        // uses for its own are refused above.
        .with_is_internal(false);
    match planned {
        Plan::Serve(partitions) => base.with_partitions(
            (0..partitions)
                .map(|index| {
                    MetadataResponsePartition::default()
                        .with_partition_index(index)
                        .with_leader_id(NODE_ID.into())
                        // -1 is "unknown epoch", and it is the truth: the facade
                        // has no leader elections to number. Advertising a real
                        // epoch would invite clients to run truncation detection
                        // against a value nothing here maintains.
                        .with_leader_epoch(-1)
                        .with_replica_nodes(vec![NODE_ID.into()])
                        .with_isr_nodes(vec![NODE_ID.into()])
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
    use crate::queen::Catalog;
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
        assert_eq!(advertised_partitions(i64::MAX, 16), i32::MAX);
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
            advertised_host: "kafka.example.com".into(),
            advertised_port: 9092,
            default_partitions,
            queen_token: None,
            catalog: Catalog::new(api.clone()),
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
        });

        let resp = handle(&f, &request(Some(&["orders"]), true), 9, None).await;
        assert!(
            api.created().is_empty(),
            "an existing queue was reconfigured"
        );
        assert_eq!(named(&resp, "orders").error_code, 0);
        assert_eq!(named(&resp, "orders").partitions.len(), 40);
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
}
