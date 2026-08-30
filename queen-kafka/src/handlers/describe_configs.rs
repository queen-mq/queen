//! DescribeConfigs (key 32), v1-v4 — the API without which no admin object
//! works.
//!
//! sarama's `ClusterAdmin.ListTopics` issues one DescribeConfigs per topic after
//! its Metadata, so *nothing* on sarama's admin object works until this is
//! advertised — and every UI reads it for the topic-settings tab. That is the
//! measured red this handler exists to turn green.
//!
//! ## The rule that governs the whole handler
//!
//! **Report a key only where the facade can name the thing that enforces its
//! value; omit every other key.** Omission is protocol-legal — a resource result
//! carries the configs it has — and it is the only option that cannot mislead.
//! The argument, and what it costs, is in [`crate::topic_config`].
//!
//! What that rule makes of the two resource types is very different, and it is
//! worth being blunt about:
//!
//!   * a **TOPIC** answer is SHORT. Queen exposes no HTTP read of a queue's
//!     configuration at all, so two of the three rows are the ones that are true
//!     of every Queen queue by construction. The third, `retention.ms`, is read
//!     from the record this facade keeps of what it last applied to the topic
//!     ([`crate::topic_record`]) — so it round-trips for a topic this facade
//!     created and is OMITTED for one it did not, which is where it used to be
//!     for all of them.
//!   * a **BROKER** answer is the one that earns this API its place. Every value
//!     is a number this process actually enforces, read from the running
//!     configuration — so `kafka-configs.sh --describe --entity-type brokers
//!     --entity-name 0` becomes a real window onto the facade rather than a
//!     recitation of Kafka defaults.
//!
//! ## `message.max.bytes` is deliberately absent
//!
//! `conn::MAX_FRAME_BYTES` exists and it is tempting. It is not offered because
//! it bounds a *request*, not a record, and Queen's own 413 can arrive well
//! below it. A batch-sizing client would act on that number and be wrong.
//!
//! ## The one window the retention round trip leaves open
//!
//! The value reported is the one THIS FACADE last applied. A retention changed
//! outside it — the Queen console, another SDK — between two facade writes is
//! invisible here, and the record is pinned to the queue's `id` so that only a
//! same-name change can hide, never a drop-and-recreate. It is a narrow, named
//! window and it is the same last-writer-wins two admins have against a real
//! Kafka. It is said on the wire too, in the row's `documentation`, and in
//! `compat/ERRORS.md`.

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResult,
};
use kafka_protocol::messages::{DescribeConfigsRequest, DescribeConfigsResponse};
use kafka_protocol::protocol::StrBytes;
use std::collections::{HashMap, HashSet};

use crate::handlers::metadata::{self, SINGLE_NODE_ID};
use crate::topic_config::{self, Kind, Reported, Source};
use crate::topic_record::{self, Record};
use crate::{queen, throttle, Facade};

/// Kafka's `ConfigResource.Type`. Only the two this facade serves are named;
/// everything else is refused by number.
const RESOURCE_TOPIC: i8 = 2;
const RESOURCE_BROKER: i8 = 4;

pub async fn handle(
    facade: &Facade,
    req: &DescribeConfigsRequest,
    token: Option<&str>,
) -> DescribeConfigsResponse {
    let mut throttle_ms: Option<i32> = None;

    // The catalog, at most ONCE for the request and only when a topic resource
    // is in it. A UI fans this call out over every topic it knows, so a read
    // per resource would be a read per topic; the cached list is right here —
    // unlike the create path, this asks "do you have it", not "may I write
    // over it", so a list up to one TTL old costs a client a retry at worst.
    let wants_topics = req
        .resources
        .iter()
        .any(|r| r.resource_type == RESOURCE_TOPIC);
    let catalog = if wants_topics {
        match facade.catalog.list(token).await {
            // The `id` comes along, and it is what makes the staleness check
            // below cost nothing: the list the handler already reads carries it
            // (018_stats.sql:249), so no second call is needed to find out
            // whether a config record belongs to the queue that is there now.
            Ok(queues) => Ok(queues
                .iter()
                .map(|q| (q.name.clone(), q.id.clone()))
                .collect::<HashMap<String, Option<String>>>()),
            Err(e) => {
                tracing::warn!(
                    target: "kafka",
                    error = %e,
                    "DescribeConfigs cannot read the queue list"
                );
                throttle_ms = throttle::longest(throttle_ms, throttle::for_error(&e));
                Err(failed(&e))
            }
        }
    } else {
        Ok(HashMap::new())
    };

    // ONE KV call for the whole request, and none at all for a broker-only one.
    // The keys are the topic resources the catalog actually has, so a request
    // naming topics that do not exist buys no reads.
    let records = match &catalog {
        Ok(live) => {
            let wanted: Vec<String> = req
                .resources
                .iter()
                .filter(|r| r.resource_type == RESOURCE_TOPIC)
                .map(|r| r.resource_name.to_string())
                .filter(|name| live.contains_key(name))
                .collect();
            load_records(facade, &wanted, token).await
        }
        Err(_) => HashMap::new(),
    };

    let broker = broker_configs(facade);
    let results = req
        .resources
        .iter()
        .map(|r| {
            one(
                facade,
                r,
                &catalog,
                &records,
                &broker,
                req.include_documentation,
            )
        })
        .collect();

    DescribeConfigsResponse::default()
        .with_throttle_time_ms(throttle_ms.unwrap_or(0))
        .with_results(results)
}

/// The config records for `topics`, or NONE of them.
///
/// A KV failure costs the retention row and nothing else: the two rows that are
/// true of every Queen queue are still answered, and an omitted `retention.ms`
/// is exactly what this API answered before the record existed. Absence is the
/// safe direction ([`crate::topic_record`]), so there is no code to map here and
/// no reason to fail a resource that has two true things to say.
async fn load_records(
    facade: &Facade,
    topics: &[String],
    token: Option<&str>,
) -> HashMap<String, Record> {
    if topics.is_empty() {
        return HashMap::new();
    }
    match topic_record::load_many(facade.queen.as_ref(), topics, token).await {
        Ok(records) => records,
        Err(e) => {
            tracing::warn!(
                target: "kafka",
                error = %e,
                "DescribeConfigs cannot read the topic config records; retention is omitted"
            );
            HashMap::new()
        }
    }
}

/// The `retention.ms` row for one topic, or `None` when the facade has nothing
/// to say about it.
///
/// The record must both EXIST and describe the queue that is there now: a `qid`
/// that does not match the catalog's `id` is a queue that was dropped and
/// recreated under the same name, and its old record describes a configuration
/// nothing enforces.
///
/// A record whose `retentionEnabled` is true but which carries no
/// `retentionSeconds` is read as retention OFF rather than as some invented
/// window — [`crate::topic_config::alter`] only ever writes the pair together,
/// so such a record did not come from this facade's vocabulary.
fn retention_row(
    records: &HashMap<String, Record>,
    topic: &str,
    live_id: Option<&str>,
) -> Option<Reported> {
    let record = records.get(topic)?;
    if !record.describes(live_id) {
        return None;
    }
    let enabled = record
        .set
        .get(topic_config::RETENTION_ENABLED)
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let seconds = enabled
        .then(|| {
            record
                .set
                .get(topic_config::RETENTION_SECONDS)
                .and_then(|v| v.as_i64())
        })
        .flatten();
    Some(topic_config::reported_retention(seconds))
}

fn one(
    facade: &Facade,
    resource: &DescribeConfigsResource,
    catalog: &Result<HashMap<String, Option<String>>, ResponseError>,
    records: &HashMap<String, Record>,
    broker: &[Reported],
    documented: bool,
) -> DescribeConfigsResult {
    let name = resource.resource_name.clone();
    match resource.resource_type {
        RESOURCE_TOPIC => {
            let topic = name.as_str();
            // The same rule the read paths apply, in the one code they may
            // answer: a `__` name is invisible everywhere and an illegal name
            // is not a topic this facade has. See `metadata::not_a_topic_here`.
            if let Some(code) = metadata::not_a_topic_here(topic) {
                return refused(resource, code, "no such topic here");
            }
            let live = match catalog {
                Ok(live) => live,
                Err(code) => {
                    return refused(
                        resource,
                        *code,
                        "the queue list could not be read, so whether this topic exists is \
                         unknown right now",
                    )
                }
            };
            let Some(live_id) = live.get(topic) else {
                return refused(
                    resource,
                    ResponseError::UnknownTopicOrPartition,
                    "no such topic",
                );
            };
            let mut configs = topic_config::topic_configs();
            configs.extend(retention_row(records, topic, live_id.as_deref()));
            answered(resource, &configs, resource_keys(resource), documented)
        }
        RESOURCE_BROKER => {
            // `""` is "this broker" and the node id is the explicit form. In
            // single-facade mode the node id is 0, which is the pair the design
            // and every tool expect; in cluster mode it is THIS node's id, so
            // `kafka-configs.sh --entity-name <id>` reaches the node it names.
            let me = this_node(facade);
            if !name.is_empty() && name.as_str() != me.to_string() {
                return refused(
                    resource,
                    ResponseError::InvalidRequest,
                    &format!(
                        "this connection is served by node {me}; a broker resource must be named \
                         `` or `{me}`. The facade cannot answer for another node's running \
                         configuration"
                    ),
                );
            }
            answered(resource, broker, resource_keys(resource), documented)
        }
        // BROKER_LOGGER (8) and everything else. The facade runs no log4j
        // hierarchy, has no cluster-level or client-metrics configuration, and
        // answering an empty config set for one of them would read as "this
        // resource exists and is empty", which is a different and wrong thing.
        other => refused(
            resource,
            ResponseError::InvalidRequest,
            &format!(
                "resource type {other} is not one this facade has: it serves topic ({RESOURCE_TOPIC}) \
                 and broker ({RESOURCE_BROKER}) resources only"
            ),
        ),
    }
}

/// The `configuration_keys` filter, as Kafka reads it.
///
/// A NULL list means every key — and so does an EMPTY one. That second half is
/// not a guess: Kafka's own `ConfigHelper.describeConfigs` collapses both to
/// `None` before filtering (`if (resource.configurationKeys == null ||
/// resource.configurationKeys.isEmpty)`), and it matters here because
/// `kafka-protocol`'s `DescribeConfigsResource::default()` is `Some(vec![])` —
/// so a hand-rolled client that builds a resource and never touches the field
/// sends an empty list and would otherwise be answered nothing at all. Measured
/// against apache/kafka:3.9.1 in `compat/differential/admin_topics.go`.
fn resource_keys(resource: &DescribeConfigsResource) -> Option<HashSet<&str>> {
    resource
        .configuration_keys
        .as_ref()
        .filter(|keys| !keys.is_empty())
        .map(|keys| keys.iter().map(|k| k.as_str()).collect())
}

/// One answered resource. A key the filter does not name is simply absent from
/// the result, which is what Kafka does.
fn answered(
    resource: &DescribeConfigsResource,
    configs: &[Reported],
    wanted: Option<HashSet<&str>>,
    documented: bool,
) -> DescribeConfigsResult {
    let configs = configs
        .iter()
        .filter(|c| wanted.as_ref().is_none_or(|w| w.contains(c.name)))
        .map(|c| {
            DescribeConfigsResourceResult::default()
                .with_name(StrBytes::from_string(c.name.to_string()))
                .with_value(Some(StrBytes::from_string(c.value.clone())))
                // Per row since M7 F4 (`topic_config`): the two rows whose only
                // legal value is the one already reported are still read-only,
                // and `retention.ms` on a tracked topic is not — an alter of it
                // really does land. A UI that greys out its edit button on this
                // flag is still being told the truth.
                .with_read_only(c.read_only)
                .with_config_source(c.source as i8)
                .with_is_sensitive(topic_config::IS_SENSITIVE)
                // Empty, and not because it is easier: a synonym is a config
                // whose value this one INHERITED, and nothing here inherits
                // from anything. A fabricated synonym chain would be a
                // fabricated configuration hierarchy.
                .with_synonyms(Vec::new())
                // v3+; dropped by the encoder below it.
                .with_config_type(c.kind as i8)
                .with_documentation(
                    documented.then(|| StrBytes::from_string(c.documentation.to_string())),
                )
        })
        .collect();
    DescribeConfigsResult::default()
        .with_error_code(0)
        .with_error_message(None)
        .with_resource_type(resource.resource_type)
        .with_resource_name(resource.resource_name.clone())
        .with_configs(configs)
}

fn refused(
    resource: &DescribeConfigsResource,
    code: ResponseError,
    why: &str,
) -> DescribeConfigsResult {
    DescribeConfigsResult::default()
        .with_error_code(code.code())
        .with_error_message(Some(StrBytes::from_string(why.to_string())))
        .with_resource_type(resource.resource_type)
        .with_resource_name(resource.resource_name.clone())
        .with_configs(Vec::new())
}

/// Which node this process is: 0 on its own, `QUEEN_KAFKA_NODE_ID` in a
/// cluster.
fn this_node(facade: &Facade) -> i32 {
    facade.cluster.state().map_or(SINGLE_NODE_ID, |s| s.me.id)
}

/// The BROKER configs, read out of the running configuration.
///
/// ## Why the sources are split the way they are
///
/// Kafka's `STATIC_BROKER_CONFIG` means "this came out of the process's
/// start-up configuration" and `DEFAULT_CONFIG` means "nobody set it, this is
/// what it is". The split below is exactly that line drawn through this binary:
/// a value a `QUEEN_KAFKA_*` variable governs is STATIC, and a value that is a
/// constant in the binary with no knob at all is DEFAULT. Whether an operator
/// actually set a given variable is not something [`Facade`] records, and
/// reporting STATIC for a knob left at its default is what Apache Kafka does for
/// a `server.properties` line written at its default value.
///
/// Every one of them is `read_only`, and that is not the old blanket flag: each
/// is a `QUEEN_KAFKA_*` start-up variable or a constant in the binary, and this
/// facade has no dynamic broker configuration to update. AlterConfigs says the
/// same thing in its own words (`handlers::alter_configs`).
fn broker_configs(facade: &Facade) -> Vec<Reported> {
    let groups = facade.coordinator.config();
    vec![
        Reported {
            name: "num.partitions",
            value: facade.default_partitions.to_string(),
            source: Source::StaticBroker,
            kind: Kind::Int,
            read_only: true,
            documentation: "QUEEN_KAFKA_DEFAULT_PARTITIONS. Queen declares no width per queue, \
                            so a topic is advertised at max(live lanes, this).",
        },
        Reported {
            name: "auto.create.topics.enable",
            value: "true".to_string(),
            source: Source::Default,
            kind: Kind::Boolean,
            read_only: true,
            documentation: "Always true at the facade level: a Metadata request that allows \
                            auto-creation creates the queue. There is no knob that turns it off.",
        },
        Reported {
            name: "compression.type",
            value: "producer".to_string(),
            source: Source::Default,
            kind: Kind::String,
            read_only: true,
            documentation: "The facade re-batches on the fetch path without re-compressing, so \
                            the codec on the wire is whatever the producer sent.",
        },
        Reported {
            name: "connections.max.idle.ms",
            value: crate::conn::IDLE_TIMEOUT.as_millis().to_string(),
            source: Source::Default,
            kind: Kind::Long,
            read_only: true,
            documentation: "How long a connection may go quiet before the FIRST byte of a \
                            request; a parked long-poll Fetch is not idle by this measure.",
        },
        Reported {
            name: "group.initial.rebalance.delay.ms",
            value: groups.join_delay.as_millis().to_string(),
            source: Source::StaticBroker,
            kind: Kind::Int,
            read_only: true,
            documentation: "QUEEN_KAFKA_GROUP_JOIN_DELAY_MS. How long the first join of an empty \
                            group waits for company before the join window closes.",
        },
        Reported {
            name: "group.min.session.timeout.ms",
            value: groups.min_session_timeout.as_millis().to_string(),
            source: Source::StaticBroker,
            kind: Kind::Int,
            read_only: true,
            documentation: "QUEEN_KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS. A JoinGroup below it is \
                            answered INVALID_SESSION_TIMEOUT.",
        },
        Reported {
            name: "group.max.session.timeout.ms",
            value: groups.max_session_timeout.as_millis().to_string(),
            source: Source::StaticBroker,
            kind: Kind::Int,
            read_only: true,
            documentation: "QUEEN_KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS. A JoinGroup above it is \
                            answered INVALID_SESSION_TIMEOUT.",
        },
    ]
}

/// The closest Kafka code for a catalog this handler could not read. It is a
/// READ, so it is the read paths' answer: retriable, and the wait rides
/// `throttle_time_ms` beside it.
fn failed(e: &queen::Error) -> ResponseError {
    match e {
        // The credential this connection carries may not list queues. Fatal
        // and named: retrying it for ever would be the wrong answer.
        queen::Error::Status {
            code: 401 | 403, ..
        } => ResponseError::TopicAuthorizationFailed,
        _ => ResponseError::RequestTimedOut,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::alter_configs::tests::track;
    use crate::handlers::testing::{clustered, facade, facade_and_queen};
    use crate::queen::Error;

    fn resource(kind: i8, name: &str) -> DescribeConfigsResource {
        DescribeConfigsResource::default()
            .with_resource_type(kind)
            .with_resource_name(StrBytes::from_string(name.to_string()))
    }

    fn request(resources: Vec<DescribeConfigsResource>) -> DescribeConfigsRequest {
        DescribeConfigsRequest::default().with_resources(resources)
    }

    fn values(r: &DescribeConfigsResult) -> Vec<(String, String)> {
        r.configs
            .iter()
            .map(|c| {
                (
                    c.name.to_string(),
                    c.value.as_ref().map(|v| v.to_string()).unwrap_or_default(),
                )
            })
            .collect()
    }

    /// THE campaign red: sarama's `ListTopics` describes every topic Metadata
    /// named, so a topic the catalog has must answer error 0 with configs.
    #[tokio::test]
    async fn a_topic_the_catalog_has_answers_its_real_configs() {
        let f = facade(&[("orders", 2)]);
        let r = handle(&f, &request(vec![resource(RESOURCE_TOPIC, "orders")]), None).await;

        assert_eq!(r.results[0].error_code, 0);
        assert_eq!(r.results[0].resource_name.as_str(), "orders");
        assert_eq!(
            values(&r.results[0]),
            [
                ("cleanup.policy".to_string(), "delete".to_string()),
                ("min.insync.replicas".to_string(), "1".to_string()),
            ]
        );
        // Nothing here can be altered through this facade, and nothing is a
        // secret.
        assert!(r.results[0].configs.iter().all(|c| c.read_only));
        assert!(r.results[0].configs.iter().all(|c| !c.is_sensitive));
        assert!(r.results[0].configs.iter().all(|c| c.synonyms.is_empty()));
    }

    /// `retention.ms` is NOT reported for a topic this facade did not create.
    /// Queen exposes no read of a queue's config columns and the facade has no
    /// record of this one, so reporting a value would be a guess — which is
    /// what this API answered for EVERY topic before M7 F4.
    #[tokio::test]
    async fn retention_is_omitted_for_an_untracked_topic() {
        let f = facade(&[("orders", 2)]);
        let r = handle(&f, &request(vec![resource(RESOURCE_TOPIC, "orders")]), None).await;
        assert!(
            !r.results[0]
                .configs
                .iter()
                .any(|c| c.name.as_str() == "retention.ms"),
            "retention was reported from somewhere the facade cannot read"
        );
    }

    /// THE round trip, and the hole M7 F4 closes: a topic the facade created
    /// with a retention reports it back, at the resolution it was stored at,
    /// sourced TOPIC and writable.
    #[tokio::test]
    async fn retention_round_trips_for_a_tracked_topic() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        track(
            &api,
            "orders",
            &[
                ("retentionEnabled", serde_json::json!(true)),
                ("retentionSeconds", serde_json::json!(604_800)),
            ],
        );
        let r = handle(
            &f,
            &request(vec![resource(RESOURCE_TOPIC, "orders")]).with_include_documentation(true),
            None,
        )
        .await;

        let row = r.results[0]
            .configs
            .iter()
            .find(|c| c.name.as_str() == "retention.ms")
            .expect("retention was not reported for a tracked topic");
        assert_eq!(row.value.as_ref().unwrap().as_str(), "604800000");
        assert_eq!(row.config_source, Source::Topic as i8);
        assert_eq!(row.config_type, Kind::Int as i8);
        assert!(!row.read_only, "a tracked retention is writable");
        assert!(!row.is_sensitive);
        // The one window the record leaves open is named on the wire, not only
        // in a document nobody reads at three in the morning.
        assert!(row
            .documentation
            .as_ref()
            .is_some_and(|d| d.as_str().contains("outside this facade")));
    }

    /// The case that makes the round trip COMPLETE rather than partial: the
    /// facade created the queue and did not enable retention, so it knows the
    /// column is at the stored procedure's default — and that default IS
    /// Kafka's -1. DEFAULT and not TOPIC, because nobody set it.
    #[tokio::test]
    async fn a_tracked_topic_with_no_retention_reports_minus_one_as_a_default() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        // The auto-create path's own bag: empty.
        track(&api, "orders", &[]);
        let r = handle(&f, &request(vec![resource(RESOURCE_TOPIC, "orders")]), None).await;

        let row = r.results[0]
            .configs
            .iter()
            .find(|c| c.name.as_str() == "retention.ms")
            .expect("retention was not reported for a tracked topic");
        assert_eq!(row.value.as_ref().unwrap().as_str(), "-1");
        assert_eq!(row.config_source, Source::Default as i8);
        assert!(!row.read_only);

        // ...and so does a record that says retention is off explicitly, which
        // is what `--add-config retention.ms=-1` writes.
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        track(
            &api,
            "orders",
            &[("retentionEnabled", serde_json::json!(false))],
        );
        let r = handle(&f, &request(vec![resource(RESOURCE_TOPIC, "orders")]), None).await;
        let row = r.results[0]
            .configs
            .iter()
            .find(|c| c.name.as_str() == "retention.ms")
            .expect("retention was not reported");
        assert_eq!(row.value.as_ref().unwrap().as_str(), "-1");
        assert_eq!(row.config_source, Source::Default as i8);
    }

    /// A queue dropped and recreated under the same name does not report the
    /// old record's retention. The `qid` pin is what catches it, and it is free:
    /// the queue list this handler already reads carries the id.
    #[tokio::test]
    async fn a_recreated_queue_invalidates_the_record() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        api.kv_seed(
            crate::offsets::NAMESPACE,
            &crate::topic_record::key("orders"),
            serde_json::json!({
                "qid": "a-queue-that-is-gone",
                "set": {"retentionEnabled": true, "retentionSeconds": 604_800},
                "at": 1,
            }),
        );
        let r = handle(&f, &request(vec![resource(RESOURCE_TOPIC, "orders")]), None).await;
        assert!(
            !r.results[0]
                .configs
                .iter()
                .any(|c| c.name.as_str() == "retention.ms"),
            "a dead record was reported for the queue that replaced it"
        );
    }

    /// One KV call for the whole request, however many topics it names — the
    /// shape a UI's settings fan-out sends.
    #[tokio::test]
    async fn one_kv_call_for_many_topics() {
        let (f, api) = facade_and_queen(&[("a", 1), ("b", 1), ("c", 1)]);
        for t in ["a", "b", "c"] {
            track(&api, t, &[]);
        }
        handle(
            &f,
            &request(vec![
                resource(RESOURCE_TOPIC, "a"),
                resource(RESOURCE_TOPIC, "b"),
                resource(RESOURCE_TOPIC, "c"),
            ]),
            None,
        )
        .await;
        assert_eq!(api.kv_calls.lock().unwrap().len(), 1);
    }

    /// A KV failure costs the retention row and nothing else: the two rows that
    /// are true of every Queen queue are still answered, error 0, because an
    /// omitted `retention.ms` is exactly what this API answered before the
    /// record existed.
    #[tokio::test]
    async fn an_unreadable_record_costs_only_the_retention_row() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        track(&api, "orders", &[]);
        api.fail_kv(Error::Transport("kv is down".into()));
        let r = handle(&f, &request(vec![resource(RESOURCE_TOPIC, "orders")]), None).await;
        assert_eq!(r.results[0].error_code, 0);
        assert_eq!(values(&r.results[0]).len(), 2);
    }

    #[tokio::test]
    async fn a_topic_the_catalog_does_not_have_is_unknown() {
        let f = facade(&[("orders", 2)]);
        let r = handle(
            &f,
            &request(vec![
                resource(RESOURCE_TOPIC, "nope"),
                resource(RESOURCE_TOPIC, "__consumer_offsets"),
                resource(RESOURCE_TOPIC, "has spaces"),
            ]),
            None,
        )
        .await;

        for result in &r.results {
            assert_eq!(
                result.error_code,
                ResponseError::UnknownTopicOrPartition.code(),
                "{}",
                result.resource_name.as_str()
            );
            assert!(result.configs.is_empty());
        }
    }

    /// The broker resource is where this API earns its place: every value is
    /// one this process enforces.
    #[tokio::test]
    async fn the_broker_resource_reports_the_running_configuration() {
        let f = facade(&[]);
        for named in ["", "0"] {
            let r = handle(&f, &request(vec![resource(RESOURCE_BROKER, named)]), None).await;
            assert_eq!(r.results[0].error_code, 0, "named {named:?}");
            let got = values(&r.results[0]);
            // The fixture's own width, not a Kafka default.
            assert!(
                got.contains(&("num.partitions".into(), "4".into())),
                "{got:?}"
            );
            assert!(got.contains(&("auto.create.topics.enable".into(), "true".into())));
            assert!(got.contains(&("compression.type".into(), "producer".into())));
            assert!(got.contains(&("connections.max.idle.ms".into(), "600000".into())));
            // The coordinator's real knobs, read from the coordinator.
            assert!(got.contains(&("group.min.session.timeout.ms".into(), "6000".into())));
            assert!(got.contains(&("group.max.session.timeout.ms".into(), "300000".into())));
        }
    }

    /// A broker resource naming somebody else is refused rather than answered
    /// with this node's configuration under another node's name.
    #[tokio::test]
    async fn a_broker_resource_naming_another_node_is_refused() {
        let f = facade(&[]);
        let r = handle(&f, &request(vec![resource(RESOURCE_BROKER, "7")]), None).await;
        assert_eq!(
            r.results[0].error_code,
            ResponseError::InvalidRequest.code()
        );
        assert!(r.results[0]
            .error_message
            .as_ref()
            .unwrap()
            .as_str()
            .contains("node 0"));
    }

    /// ...and in cluster mode the name that is accepted is THIS node's id, so
    /// `--entity-name <id>` reaches the node it names. With no cluster settings
    /// the id is 0 and the behaviour above is unchanged.
    #[tokio::test]
    async fn a_clustered_node_answers_for_its_own_id() {
        let (f, _) = clustered(
            &[],
            &[
                (1, "kafka-1.example.com", 9092),
                (2, "kafka-2.example.com", 9092),
            ],
            2,
        );
        let r = handle(
            &f,
            &request(vec![
                resource(RESOURCE_BROKER, "2"),
                resource(RESOURCE_BROKER, ""),
                resource(RESOURCE_BROKER, "1"),
            ]),
            None,
        )
        .await;
        assert_eq!(r.results[0].error_code, 0, "its own id");
        assert_eq!(r.results[1].error_code, 0, "the empty name");
        assert_eq!(
            r.results[2].error_code,
            ResponseError::InvalidRequest.code(),
            "another node's id"
        );
    }

    /// BROKER_LOGGER and every other type: refused by number rather than
    /// answered with an empty set, which would read as "this exists and is
    /// empty".
    #[tokio::test]
    async fn every_other_resource_type_is_invalid() {
        let f = facade(&[]);
        for kind in [0i8, 1, 8, 16, 32] {
            let r = handle(&f, &request(vec![resource(kind, "whatever")]), None).await;
            assert_eq!(
                r.results[0].error_code,
                ResponseError::InvalidRequest.code(),
                "type {kind}"
            );
        }
    }

    /// The filter is Kafka's: a non-empty list is exactly those keys, and a
    /// key we do not report is simply absent from the answer.
    #[tokio::test]
    async fn configuration_keys_filters_the_answer() {
        let f = facade(&[("orders", 2)]);
        let asked = resource(RESOURCE_TOPIC, "orders").with_configuration_keys(Some(vec![
            StrBytes::from_static_str("cleanup.policy"),
            StrBytes::from_static_str("segment.bytes"),
        ]));
        let r = handle(&f, &request(vec![asked]), None).await;

        assert_eq!(r.results[0].error_code, 0);
        assert_eq!(
            values(&r.results[0]),
            [("cleanup.policy".to_string(), "delete".to_string())]
        );
    }

    /// ...and BOTH forms of "no filter" mean every key. The empty list is the
    /// one that would otherwise bite: it is what
    /// `DescribeConfigsResource::default()` carries, so a client that never
    /// touches the field sends one. See [`resource_keys`].
    #[tokio::test]
    async fn an_absent_filter_is_every_key_whichever_way_it_is_spelled() {
        let f = facade(&[("orders", 2)]);
        for keys in [None, Some(Vec::new())] {
            let asked = resource(RESOURCE_TOPIC, "orders").with_configuration_keys(keys.clone());
            let r = handle(&f, &request(vec![asked]), None).await;
            assert_eq!(r.results[0].configs.len(), 2, "{keys:?}");
        }
    }

    /// `include_documentation` is honoured, and it is off by default — the
    /// sentences are real and they are not free bytes on every describe a UI
    /// fans out.
    #[tokio::test]
    async fn documentation_is_answered_only_when_it_was_asked_for() {
        let f = facade(&[("orders", 2)]);
        let quiet = handle(&f, &request(vec![resource(RESOURCE_TOPIC, "orders")]), None).await;
        assert!(quiet.results[0]
            .configs
            .iter()
            .all(|c| c.documentation.is_none()));

        let asked = handle(
            &f,
            &request(vec![resource(RESOURCE_TOPIC, "orders")]).with_include_documentation(true),
            None,
        )
        .await;
        assert!(asked.results[0]
            .configs
            .iter()
            .all(|c| c.documentation.as_ref().is_some_and(|d| !d.is_empty())));
    }

    /// An unreadable catalog costs the TOPIC resources and nothing else: the
    /// broker resource is answered from this process and has no catalog to
    /// fail.
    #[tokio::test]
    async fn an_unreadable_catalog_costs_only_the_topic_resources() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        api.fail_list(Error::Transport("queen is down".into()));
        let r = handle(
            &f,
            &request(vec![
                resource(RESOURCE_TOPIC, "orders"),
                resource(RESOURCE_BROKER, ""),
            ]),
            None,
        )
        .await;

        assert_eq!(
            r.results[0].error_code,
            ResponseError::RequestTimedOut.code()
        );
        assert_eq!(r.results[1].error_code, 0);
        assert!(!r.results[1].configs.is_empty());
    }

    /// A rate cap carries its wait, and a refused credential is named rather
    /// than dressed up as a timeout.
    #[tokio::test]
    async fn queens_failures_map_to_codes_a_client_can_act_on() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        api.fail_list(Error::Status {
            code: 429,
            body: "rate_limited".into(),
            retry_after_ms: Some(2_000),
        });
        let r = handle(&f, &request(vec![resource(RESOURCE_TOPIC, "orders")]), None).await;
        assert_eq!(
            r.results[0].error_code,
            ResponseError::RequestTimedOut.code()
        );
        assert_eq!(r.throttle_time_ms, 2_000);

        let (f, api) = facade_and_queen(&[("orders", 2)]);
        api.fail_list(Error::status(403, "forbidden"));
        let r = handle(&f, &request(vec![resource(RESOURCE_TOPIC, "orders")]), None).await;
        assert_eq!(
            r.results[0].error_code,
            ResponseError::TopicAuthorizationFailed.code()
        );
    }

    /// A broker-only request makes NO call to Queen — neither the catalog nor
    /// the config-record store. A UI polling the broker tab must not cost the
    /// tenant an admin call per refresh.
    #[tokio::test]
    async fn a_broker_only_request_does_not_read_the_catalog() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        handle(&f, &request(vec![resource(RESOURCE_BROKER, "")]), None).await;
        assert_eq!(api.list_count(), 0);
        assert!(api.kv_calls.lock().unwrap().is_empty());
    }

    /// Every broker row is read-only, and each for its own reason rather than
    /// by a blanket flag: it is a `QUEEN_KAFKA_*` start-up variable or a
    /// constant in the binary, and AlterConfigs says exactly that.
    #[tokio::test]
    async fn every_broker_row_is_read_only() {
        let f = facade(&[]);
        let r = handle(&f, &request(vec![resource(RESOURCE_BROKER, "")]), None).await;
        assert!(!r.results[0].configs.is_empty());
        assert!(r.results[0].configs.iter().all(|c| c.read_only));
    }

    /// Several topics, one catalog read: the shape sarama's `ListTopics` and
    /// every UI actually send.
    #[tokio::test]
    async fn many_topics_cost_one_catalog_read() {
        let (f, api) = facade_and_queen(&[("a", 1), ("b", 1), ("c", 1)]);
        let r = handle(
            &f,
            &request(vec![
                resource(RESOURCE_TOPIC, "a"),
                resource(RESOURCE_TOPIC, "b"),
                resource(RESOURCE_TOPIC, "c"),
            ]),
            None,
        )
        .await;
        assert!(r.results.iter().all(|x| x.error_code == 0));
        assert_eq!(api.list_count(), 1);
    }

    /// Every call carries the connection's credential, so one tenant cannot
    /// describe another's topic through this handler.
    #[tokio::test]
    async fn the_connections_credential_scopes_the_answer() {
        let (root, api) = facade_and_queen(&[("orders", 2)]);
        let f = root.for_connection(None).authenticated_as("tenant-key");
        handle(
            &f,
            &request(vec![resource(RESOURCE_TOPIC, "orders")]),
            Some("tenant-key"),
        )
        .await;
        let tokens = api.tokens.lock().unwrap().clone();
        assert!(
            tokens.iter().all(|t| t.as_deref() == Some("tenant-key")),
            "{tokens:?}"
        );
    }
}
