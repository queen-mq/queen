//! AlterConfigs (key 33), v0-v2 — the FULL-REPLACEMENT write half of the config
//! surface, and the machinery its incremental sibling shares.
//!
//! Kafka deprecated key 33 precisely because of what it means: a resource's
//! configuration becomes exactly what the request names, and every other key is
//! reset to its default. Modern tools send IncrementalAlterConfigs (key 44,
//! [`super::incremental_alter_configs`]) instead — `kafka-configs.sh --alter`
//! has used it since Kafka 2.3 and 3.9's `ConfigCommand` has no fallback — so
//! this key exists here to decode the old shape correctly rather than to be the
//! one anybody reaches for. It is honoured LITERALLY, including the part that
//! surprises people: an AlterConfigs naming only `cleanup.policy=delete` turns
//! retention OFF, because retention is a key it did not name.
//!
//! ## The hazard this whole module is arranged around
//!
//! `POST /api/v1/configure` is a whole-row upsert over nineteen columns
//! (012_configure.sql), and thirteen of those columns cannot be read back
//! through any Queen route. A naive "set retention.ms" would therefore post a
//! one-key bag and silently reset a tenant's dedup window, lease time, retry
//! limit and DLQ flag to the stored procedure's defaults.
//!
//! What makes an alter possible at all is [`crate::topic_record`]: the facade
//! keeps its own record of the bag it last posted for a topic, so for a topic it
//! created the complete bag is known by construction and an alter is
//! `stored ∪ delta` posted whole. A topic with no valid record is REFUSED, by
//! name and with the reason, because the only alternative is to guess at
//! thirteen columns.
//!
//! ## Cluster mode: no ownership gate, and that is deliberate
//!
//! These are topic-addressed rather than group-addressed. `/configure` is a
//! Queen write any node may make and the record lives in shared KV, so two nodes
//! altering one topic is last-writer-wins — which is exactly what Apache Kafka's
//! AlterConfigs is, having no optimistic concurrency of its own. A fence here
//! would refuse a write Kafka would have taken.
//!
//! ## Errors
//!
//! See `compat/ERRORS.md`. The one worth naming here is that a transient
//! failure and an untracked topic must not answer the same code: a KV read that
//! failed is `REQUEST_TIMED_OUT` and retriable, and a topic with no record is
//! `INVALID_CONFIG` and final. Collapsing them would make a Queen hiccup look
//! like a permanent refusal to a tool that prints it.

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::alter_configs_response::AlterConfigsResourceResponse;
use kafka_protocol::messages::{AlterConfigsRequest, AlterConfigsResponse};
use kafka_protocol::protocol::StrBytes;
use serde_json::{Map, Value};
use std::collections::HashMap;

use crate::handlers::metadata::{self, SINGLE_NODE_ID};
use crate::topic_config;
use crate::topic_record::{self, Record};
use crate::{queen, throttle, Facade};

/// Kafka's `ConfigResource.Type`. Only the two this facade serves are named;
/// everything else is refused by number, exactly as DescribeConfigs refuses it.
pub(crate) const RESOURCE_TOPIC: i8 = 2;
pub(crate) const RESOURCE_BROKER: i8 = 4;

/// What one resource resolves to before anything is written.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Verdict {
    /// Post this whole bag to `/configure` and store it as the topic's new
    /// record, pinned to `qid`. The bag is complete, never a fragment — that is
    /// the invariant the whole module exists to keep.
    Write {
        qid: Option<String>,
        bag: Map<String, Value>,
    },
    /// The bag the request computes is the bag already stored, so there is
    /// nothing to do. Answered 0 with no call to Queen, which also narrows the
    /// one window the record has: a `/configure` that would rewrite the same
    /// values is still a rewrite, and skipping it means a config changed in the
    /// Queen console survives an alter that changed nothing.
    Unchanged,
    /// Answer this code and this sentence, and touch nothing.
    Refuse(ResponseError, String),
}

/// Everything one request needs to read before it may decide anything: which
/// queues exist, what their ids are, and what the facade last applied to them.
pub(crate) struct Context {
    /// Queue name to the queue's `id`, or the code that read failed with.
    catalog: Result<HashMap<String, Option<String>>, ResponseError>,
    /// The config records, or the code that read failed with. NOT collapsed
    /// into "no record": see the module header.
    records: Result<HashMap<String, Record>, ResponseError>,
    /// The longest `Retry-After` any of those reads carried.
    pub(crate) throttle_ms: Option<i32>,
}

/// Read the catalog and the config records for the topic resources of one
/// request. One catalog call and one KV call for the whole request, and neither
/// at all when no topic resource is in it.
///
/// The catalog read is the TTL-BYPASSING one, unlike DescribeConfigs'. This is a
/// WRITE path: the queue `id` read here is the token the new record is pinned
/// to, and pinning a record to an id that is one cache TTL old would mean a
/// topic recreated in that window describes from a record it does not own. One
/// admin call per alter is the same price `handlers::create_topics` pays for the
/// same reason, and an alter is rare where a describe is not.
pub(crate) async fn context(facade: &Facade, topics: &[String], token: Option<&str>) -> Context {
    if topics.is_empty() {
        return Context {
            catalog: Ok(HashMap::new()),
            records: Ok(HashMap::new()),
            throttle_ms: None,
        };
    }
    let mut throttle_ms: Option<i32> = None;
    let catalog = match facade.catalog.refresh(token).await {
        Ok(queues) => Ok(queues
            .iter()
            .map(|q| (q.name.clone(), q.id.clone()))
            .collect::<HashMap<String, Option<String>>>()),
        Err(e) => {
            tracing::warn!(
                target: "kafka",
                error = %e,
                "an alter cannot read the queue list; nothing is written"
            );
            throttle_ms = throttle::longest(throttle_ms, throttle::for_error(&e));
            Err(failed(&e).0)
        }
    };

    // Only for the topics that are actually there: a request naming a topic
    // that does not exist is answered UNKNOWN_TOPIC_OR_PARTITION and buys no
    // read.
    let records = match &catalog {
        Ok(live) => {
            let wanted: Vec<String> = topics
                .iter()
                .filter(|t| live.contains_key(*t))
                .cloned()
                .collect();
            if wanted.is_empty() {
                Ok(HashMap::new())
            } else {
                match topic_record::load_many(facade.queen.as_ref(), &wanted, token).await {
                    Ok(records) => Ok(records),
                    Err(e) => {
                        tracing::warn!(
                            target: "kafka",
                            error = %e,
                            "an alter cannot read the topic config records; nothing is written"
                        );
                        throttle_ms = throttle::longest(throttle_ms, throttle::for_error(&e));
                        Err(failed(&e).0)
                    }
                }
            }
        }
        // The catalog failed, so nothing was asked of the store either.
        Err(code) => Err(*code),
    };

    Context {
        catalog,
        records,
        throttle_ms,
    }
}

impl Context {
    /// The bag the facade last applied to `topic` and the queue id it is pinned
    /// to, or the reason this topic cannot be altered here.
    ///
    /// The order of the checks IS the error taxonomy: a name that is not a topic
    /// here, then a topic that does not exist, then a read that failed, then a
    /// topic with no valid record. Only the last of those is final on a topic
    /// that exists, and it is the one whose sentence a person reads.
    pub(crate) fn tracked(
        &self,
        topic: &str,
    ) -> Result<(Option<String>, Map<String, Value>), Verdict> {
        // The SAME rule every read path applies, in the code this surface
        // answers it with: a `__` name is invisible here and an illegal name is
        // not a topic this facade has.
        if let Some(code) = metadata::not_a_topic_here(topic) {
            return Err(Verdict::Refuse(
                code,
                "no such topic here: the name is either reserved for a broker's internal topics \
                 (`__`) or not a legal Kafka topic name"
                    .to_string(),
            ));
        }
        let live =
            match &self.catalog {
                Ok(live) => live,
                Err(code) => return Err(Verdict::Refuse(
                    *code,
                    "the queue list could not be read, so whether this topic exists — and which \
                     queue it is — is unknown right now. Nothing was written"
                        .to_string(),
                )),
            };
        let Some(live_id) = live.get(topic) else {
            return Err(Verdict::Refuse(
                ResponseError::UnknownTopicOrPartition,
                "no such topic".to_string(),
            ));
        };
        let records = match &self.records {
            Ok(records) => records,
            Err(code) => {
                return Err(Verdict::Refuse(
                    *code,
                    "the record of what this facade last applied to this topic could not be \
                     read, and altering without it would reset every Queen config column it \
                     cannot see. Nothing was written"
                        .to_string(),
                ))
            }
        };
        match records.get(topic) {
            Some(record) if record.describes(live_id.as_deref()) => {
                Ok((live_id.clone(), record.set.clone()))
            }
            _ => Err(Verdict::Refuse(
                ResponseError::InvalidConfig,
                untracked(topic),
            )),
        }
    }
}

/// The refusal a topic this facade did not create meets, and the sentence a
/// person reads out of `kafka-configs.sh`.
///
/// INVALID_CONFIG and not INVALID_REQUEST: the Java AdminClient turns 40 into a
/// non-retriable `InvalidConfigurationException` whose message the tool prints
/// verbatim, which is the only place a sentence this long is worth writing. It
/// is also the code Kafka itself uses for "this config cannot be changed here".
pub(crate) fn untracked(topic: &str) -> String {
    format!(
        "`{topic}` was not created through this facade (or its Queen queue has been replaced \
         since), so the facade does not know its current Queen configuration. Queen's \
         POST /api/v1/configure rewrites every config column it is not given — lease time, \
         retries, TTL, the DLQ flag, the dedup window — so altering a config here would \
         silently reset settings this facade cannot read back. Recreate the topic through this \
         facade, or set the configuration in the Queen console"
    )
}

/// The BROKER resource, which is always a refusal and never a write.
///
/// The name rule is DescribeConfigs': `""` is "this broker" and the node id is
/// the explicit form, and naming another node is INVALID_REQUEST because this
/// process cannot answer for another node's running configuration. Past that,
/// every broker config here is a `QUEEN_KAFKA_*` start-up variable or a constant
/// in the binary — there is no dynamic broker configuration to update — which is
/// exactly the shape of Apache Kafka's own answer for a non-updatable broker
/// config.
pub(crate) fn broker(facade: &Facade, name: &str) -> Verdict {
    let me = facade.cluster.state().map_or(SINGLE_NODE_ID, |s| s.me.id);
    if !name.is_empty() && name != me.to_string() {
        return Verdict::Refuse(
            ResponseError::InvalidRequest,
            format!(
                "this connection is served by node {me}; a broker resource must be named `` or \
                 `{me}`. The facade cannot alter another node's running configuration"
            ),
        );
    }
    Verdict::Refuse(
        ResponseError::InvalidConfig,
        format!(
            "node {me}'s configuration cannot be updated dynamically: every value \
             DescribeConfigs reports for a broker resource here is a QUEEN_KAFKA_* start-up \
             environment variable or a constant in the binary, so changing one means restarting \
             the process with a different environment. There is no dynamic broker config store \
             behind this facade to write to"
        ),
    )
}

/// BROKER_LOGGER (8) and everything else, refused by number — the same sentence
/// DescribeConfigs already produces, because it is the same fact.
pub(crate) fn other_resource(kind: i8) -> Verdict {
    Verdict::Refuse(
        ResponseError::InvalidRequest,
        format!(
            "resource type {kind} is not one this facade has: it serves topic \
             ({RESOURCE_TOPIC}) and broker ({RESOURCE_BROKER}) resources only"
        ),
    )
}

/// What one landed write cost, whether or not it worked.
pub(crate) struct Landed {
    pub(crate) error: Option<(ResponseError, String)>,
    pub(crate) throttle_ms: Option<i32>,
}

impl Landed {
    fn ok() -> Landed {
        Landed {
            error: None,
            throttle_ms: None,
        }
    }
}

/// Post `bag` to `/configure` and record it, in that order.
///
/// ## The ordering rule, which decides which lie is possible
///
/// 1. `/configure` with the merged bag.
/// 2. On success, write the record.
/// 3. **If the record write fails, delete the record and answer
///    REQUEST_TIMED_OUT.** Absence is the one honest state: a describe then
///    omits `retention.ms` rather than reporting the value from before the
///    alter, and the client's retry re-applies a write that is idempotent. A
///    record left as it was would claim a configuration Queen no longer has.
///
/// Calling `create_with` for a name the catalog HAS is the one place that is
/// deliberately done — its own doc warns against it, and this is the exception
/// it was warning for: the bag is complete by construction
/// ([`crate::topic_record`]), so the upsert rewrites every column to the value
/// the facade intends rather than to a stored-procedure default.
pub(crate) async fn commit(
    facade: &Facade,
    topic: &str,
    qid: Option<String>,
    bag: Map<String, Value>,
    token: Option<&str>,
) -> Landed {
    let options = Value::Object(bag.clone());
    if let Err(e) = facade.catalog.create_with(topic, &options, token).await {
        tracing::error!(
            target: "kafka",
            topic = topic,
            error = %e,
            "an alter could not write the queue configuration"
        );
        let (code, why) = failed(&e);
        return Landed {
            error: Some((code, why)),
            throttle_ms: throttle::for_error(&e),
        };
    }

    let record = Record::new(qid, bag);
    if let Err(e) = topic_record::store(facade.queen.as_ref(), topic, &record, token).await {
        tracing::error!(
            target: "kafka",
            topic = topic,
            error = %e,
            "the queue configuration was written but its record was not; removing the record so \
             the topic reads as untracked rather than as its pre-alter configuration"
        );
        // Best effort, and its own failure changes nothing about the answer: the
        // client is being told to retry either way, and a retry rewrites both.
        if let Err(e) = topic_record::remove(facade.queen.as_ref(), topic, token).await {
            tracing::warn!(
                target: "kafka",
                topic = topic,
                error = %e,
                "the stale config record could not be removed either"
            );
        }
        return Landed {
            error: Some((
                ResponseError::RequestTimedOut,
                "the queue configuration was written but the facade could not record it, so the \
                 record was removed; retry to re-apply it. Until then this topic describes \
                 without `retention.ms`"
                    .to_string(),
            )),
            throttle_ms: throttle::for_error(&e),
        };
    }

    tracing::info!(
        target: "kafka",
        topic = topic,
        configs = record.set.len(),
        "altered a Queen queue's configuration for a Kafka client"
    );
    Landed::ok()
}

/// The closest Kafka code for a Queen call this handler could not make.
///
/// KIP-599's THROTTLING_QUOTA_EXCEEDED is deliberately NOT used, unlike
/// CreateTopics': neither of these APIs has a version at which a client is
/// required to understand that code, and a code outside the closed set the
/// client accepts ends the application instead of making it retry
/// (`handlers/mod.rs`). The wait still rides `throttle_time_ms`.
pub(crate) fn failed(e: &queen::Error) -> (ResponseError, String) {
    match e {
        queen::Error::Status {
            code: 401 | 403, ..
        } => (
            ResponseError::TopicAuthorizationFailed,
            // The refusal's OWN words, not a paraphrase of them. Queen and the
            // proxy both say why ("operation not permitted for this
            // credential", "not in your plan"), and a fixed sentence here threw
            // that away — leaving an operator with a code and no idea which
            // scope or which plan feature to go and change.
            queen::wire_reason_of(&format!("Queen refused this credential for the call: {e}")),
        ),
        _ => (ResponseError::RequestTimedOut, e.wire_reason()),
    }
}

/// The `(error_code, error_message)` pair for one answered resource.
///
/// The two APIs' per-resource results are field-for-field identical and are
/// nonetheless two distinct generated types, so what is shared is the pair
/// rather than the struct. Sharing it is the point: a refusal must read the same
/// whichever key a client used to reach it.
pub(crate) fn outcome_fields(outcome: Option<(ResponseError, String)>) -> (i16, Option<StrBytes>) {
    match outcome {
        Some((code, why)) => (code.code(), Some(StrBytes::from_string(why))),
        None => (0, None),
    }
}

/// The topic names a request's TOPIC resources name, for one round of reads.
pub(crate) fn topic_names<'a>(resources: impl Iterator<Item = (i8, &'a str)>) -> Vec<String> {
    let mut out: Vec<String> = resources
        .filter(|(kind, _)| *kind == RESOURCE_TOPIC)
        .map(|(_, name)| name.to_string())
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

pub async fn handle(
    facade: &Facade,
    req: &AlterConfigsRequest,
    token: Option<&str>,
) -> AlterConfigsResponse {
    let ctx = context(
        facade,
        &topic_names(
            req.resources
                .iter()
                .map(|r| (r.resource_type, r.resource_name.as_str())),
        ),
        token,
    )
    .await;
    let mut throttle_ms = ctx.throttle_ms;

    let mut responses = Vec::with_capacity(req.resources.len());
    for resource in &req.resources {
        let verdict = match resource.resource_type {
            RESOURCE_TOPIC => plan_topic(&ctx, resource.resource_name.as_str(), &resource.configs),
            RESOURCE_BROKER => broker(facade, resource.resource_name.as_str()),
            other => other_resource(other),
        };

        let outcome = match verdict {
            Verdict::Refuse(code, why) => Some((code, why)),
            Verdict::Unchanged => None,
            // `validate_only` (v0-v2): everything above ran, the response is
            // built the same way, and nothing below runs. That is the whole
            // point of the flag — a client uses it to find out what WOULD
            // happen.
            Verdict::Write { .. } if req.validate_only => None,
            Verdict::Write { qid, bag } => {
                let landed = commit(facade, resource.resource_name.as_str(), qid, bag, token).await;
                throttle_ms = throttle::longest(throttle_ms, landed.throttle_ms);
                landed.error
            }
        };
        let (code, why) = outcome_fields(outcome);
        responses.push(
            AlterConfigsResourceResponse::default()
                .with_error_code(code)
                .with_error_message(why)
                .with_resource_type(resource.resource_type)
                .with_resource_name(resource.resource_name.clone()),
        );
    }

    AlterConfigsResponse::default()
        .with_throttle_time_ms(throttle_ms.unwrap_or(0))
        .with_responses(responses)
}

/// FULL REPLACEMENT, honoured literally.
///
/// The desired bag is built from EMPTY and from the request's `configs[]` alone:
/// a key in the facade's vocabulary the request did not name is at its default,
/// which for `cleanup.policy` and `min.insync.replicas` is the value they
/// already have and for `retention.ms` is -1. So an AlterConfigs naming only
/// `cleanup.policy=delete` turns retention off. That is what a real broker does
/// with this key, and it is the reason `compat/ERRORS.md` says to prefer
/// IncrementalAlterConfigs.
///
/// The resulting bag REPLACES the record's `set` outright rather than merging
/// onto it: full replacement on the wire is full replacement in the record.
fn plan_topic(
    ctx: &Context,
    topic: &str,
    configs: &[kafka_protocol::messages::alter_configs_request::AlterableConfig],
) -> Verdict {
    let (qid, stored) = match ctx.tracked(topic) {
        Ok(found) => found,
        Err(refusal) => return refusal,
    };

    let mut desired = Map::new();
    for config in configs {
        let delta = match topic_config::alter(
            config.name.as_str(),
            config.value.as_ref().map(|v| v.as_str()),
        ) {
            Ok(delta) => delta,
            Err(why) => return Verdict::Refuse(ResponseError::InvalidConfig, why),
        };
        topic_config::absorb(&mut desired, &delta);
    }

    if desired == stored {
        Verdict::Unchanged
    } else {
        Verdict::Write { qid, bag: desired }
    }
}

/// `pub(crate)` for [`super::incremental_alter_configs`]'s tests: the two APIs
/// share this module's machinery, so they share the fixture that seeds the
/// record it reads. One `track` means the two cannot come to disagree about what
/// a stored record looks like.
#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::handlers::testing::{clustered, facade_and_queen};
    use crate::queen::testing::FakeQueen;
    use crate::queen::Error;
    use kafka_protocol::messages::alter_configs_request::{AlterConfigsResource, AlterableConfig};
    use serde_json::json;
    use std::sync::Arc;

    /// Seed the record the facade would have written when it created `topic`.
    /// `set` is spelled the way `/configure` takes it, which is the only
    /// spelling the record ever carries.
    pub(crate) fn track(api: &Arc<FakeQueen>, topic: &str, set: &[(&str, Value)]) {
        api.kv_seed(
            crate::offsets::NAMESPACE,
            &topic_record::key(topic),
            json!({
                "qid": null,
                "set": set
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), v.clone()))
                    .collect::<Map<String, Value>>(),
                "at": 1,
            }),
        );
    }

    fn config(name: &str, value: Option<&str>) -> AlterableConfig {
        AlterableConfig::default()
            .with_name(StrBytes::from_string(name.to_string()))
            .with_value(value.map(|v| StrBytes::from_string(v.to_string())))
    }

    fn resource(kind: i8, name: &str, configs: Vec<AlterableConfig>) -> AlterConfigsResource {
        AlterConfigsResource::default()
            .with_resource_type(kind)
            .with_resource_name(StrBytes::from_string(name.to_string()))
            .with_configs(configs)
    }

    fn request(resources: Vec<AlterConfigsResource>) -> AlterConfigsRequest {
        AlterConfigsRequest::default().with_resources(resources)
    }

    fn message(r: &AlterConfigsResourceResponse) -> String {
        r.error_message
            .as_ref()
            .map(|m| m.to_string())
            .unwrap_or_default()
    }

    /// THE test: the bag posted to `/configure` is the WHOLE configuration, and
    /// it names no key the alter did not mean to touch — which is what proves
    /// nothing else was reset.
    #[tokio::test]
    async fn retention_is_written_as_the_whole_merged_bag() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        track(
            &api,
            "orders",
            &[
                ("retentionEnabled", json!(true)),
                ("retentionSeconds", json!(60)),
            ],
        );

        let r = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![config("retention.ms", Some("604800000"))],
            )]),
            None,
        )
        .await;

        assert_eq!(r.responses[0].error_code, 0, "{}", message(&r.responses[0]));
        assert_eq!(
            api.configured(),
            [(
                "orders".to_string(),
                json!({"retentionEnabled": true, "retentionSeconds": 604_800})
            )]
        );
        // ...and the record now says the same thing, so the next describe
        // reports it and the next alter merges onto it.
        let stored = api
            .kv_get(crate::offsets::NAMESPACE, &topic_record::key("orders"))
            .unwrap();
        assert_eq!(
            stored["set"],
            json!({"retentionEnabled": true, "retentionSeconds": 604_800})
        );
    }

    /// FULL REPLACEMENT, and the surprising half of it: a key the request did
    /// not name is reset to its default, so naming only `cleanup.policy` turns
    /// retention off.
    #[tokio::test]
    async fn full_replacement_resets_an_unnamed_key() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        track(
            &api,
            "orders",
            &[
                ("retentionEnabled", json!(true)),
                ("retentionSeconds", json!(604_800)),
            ],
        );

        let r = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![config("cleanup.policy", Some("delete"))],
            )]),
            None,
        )
        .await;

        assert_eq!(r.responses[0].error_code, 0, "{}", message(&r.responses[0]));
        // The bag is EMPTY, which is what leaves `configure_queue_v1`'s own
        // default — retention off — in force.
        assert_eq!(api.configured(), [("orders".to_string(), json!({}))]);
    }

    /// A topic the facade did not create is refused by name, with the sentence
    /// `kafka-configs.sh` prints verbatim.
    #[tokio::test]
    async fn an_untracked_topic_is_refused_by_name() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        let r = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![config("retention.ms", Some("604800000"))],
            )]),
            None,
        )
        .await;

        assert_eq!(
            r.responses[0].error_code,
            ResponseError::InvalidConfig.code()
        );
        let why = message(&r.responses[0]);
        assert!(why.contains("orders"), "{why}");
        assert!(why.contains("rewrites every config column"), "{why}");
        assert!(
            api.configured().is_empty(),
            "an untracked topic was written"
        );
    }

    /// A queue dropped and recreated under the same name does not inherit the
    /// old record: the `qid` pin catches it and the alter is refused rather than
    /// merged onto a configuration nothing enforces.
    #[tokio::test]
    async fn a_recreated_queue_is_untracked_again() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        api.kv_seed(
            crate::offsets::NAMESPACE,
            &topic_record::key("orders"),
            json!({"qid": "a-queue-that-is-gone", "set": {}, "at": 1}),
        );
        let r = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![config("retention.ms", Some("604800000"))],
            )]),
            None,
        )
        .await;
        assert_eq!(
            r.responses[0].error_code,
            ResponseError::InvalidConfig.code()
        );
        assert!(api.configured().is_empty());
    }

    /// `validate_only` runs everything and writes nothing — neither the queue
    /// configuration nor the record.
    #[tokio::test]
    async fn validate_only_writes_nothing() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        track(&api, "orders", &[]);
        let r = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![config("retention.ms", Some("604800000"))],
            )])
            .with_validate_only(true),
            None,
        )
        .await;

        assert_eq!(r.responses[0].error_code, 0, "{}", message(&r.responses[0]));
        assert!(api.configured().is_empty());
        assert_eq!(
            api.kv_get(crate::offsets::NAMESPACE, &topic_record::key("orders"))
                .unwrap()["set"],
            json!({}),
            "the record was rewritten by a validate-only request"
        );

        // ...and a validate_only that would have been REFUSED still says so.
        let refused = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![config("segment.bytes", Some("1073741824"))],
            )])
            .with_validate_only(true),
            None,
        )
        .await;
        assert_eq!(
            refused.responses[0].error_code,
            ResponseError::InvalidConfig.code()
        );
    }

    /// The refusal that stops Kafka Connect at startup instead of losing its
    /// config topic later, reached through the alter path too.
    #[tokio::test]
    async fn compaction_is_still_refused_loudly() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        track(&api, "orders", &[]);
        let r = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![config("cleanup.policy", Some("compact"))],
            )]),
            None,
        )
        .await;
        assert_eq!(
            r.responses[0].error_code,
            ResponseError::InvalidConfig.code()
        );
        assert!(message(&r.responses[0]).contains("compaction"));
        assert!(api.configured().is_empty());
    }

    /// An unknown key is refused by name rather than dropped, which is what
    /// stops a client believing it got a setting it did not get.
    #[tokio::test]
    async fn an_unknown_key_is_refused_by_name() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        track(&api, "orders", &[]);
        let r = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![config("segment.bytes", Some("1073741824"))],
            )]),
            None,
        )
        .await;
        assert_eq!(
            r.responses[0].error_code,
            ResponseError::InvalidConfig.code()
        );
        assert!(message(&r.responses[0]).contains("segment.bytes"));
    }

    /// A broker resource is INVALID_CONFIG and names the environment, which is
    /// the sentence that tells an operator where the knob actually is.
    #[tokio::test]
    async fn a_broker_resource_is_refused_and_names_the_env_var() {
        let (f, api) = facade_and_queen(&[]);
        for named in ["", "0"] {
            let r = handle(
                &f,
                &request(vec![resource(
                    RESOURCE_BROKER,
                    named,
                    vec![config("num.partitions", Some("8"))],
                )]),
                None,
            )
            .await;
            assert_eq!(
                r.responses[0].error_code,
                ResponseError::InvalidConfig.code(),
                "named {named:?}"
            );
            assert!(message(&r.responses[0]).contains("QUEEN_KAFKA_*"));
        }
        // ...and one naming another node is INVALID_REQUEST, exactly as
        // DescribeConfigs answers it.
        let r = handle(
            &f,
            &request(vec![resource(RESOURCE_BROKER, "7", Vec::new())]),
            None,
        )
        .await;
        assert_eq!(
            r.responses[0].error_code,
            ResponseError::InvalidRequest.code()
        );
        assert!(api.configured().is_empty());
    }

    /// A topic that is not there is UNKNOWN, not INVALID_CONFIG: the client can
    /// tell "you cannot change this" from "there is nothing to change".
    #[tokio::test]
    async fn an_unknown_topic_is_unknown_not_invalid_config() {
        let (f, _) = facade_and_queen(&[("orders", 2)]);
        let r = handle(
            &f,
            &request(vec![
                resource(
                    RESOURCE_TOPIC,
                    "nope",
                    vec![config("retention.ms", Some("-1"))],
                ),
                resource(
                    RESOURCE_TOPIC,
                    "__consumer_offsets",
                    vec![config("retention.ms", Some("-1"))],
                ),
                resource(
                    RESOURCE_TOPIC,
                    "has spaces",
                    vec![config("retention.ms", Some("-1"))],
                ),
            ]),
            None,
        )
        .await;
        for response in &r.responses {
            assert_eq!(
                response.error_code,
                ResponseError::UnknownTopicOrPartition.code(),
                "{}",
                response.resource_name.as_str()
            );
        }
    }

    /// BROKER_LOGGER and every other type, refused by number.
    #[tokio::test]
    async fn every_other_resource_type_is_invalid() {
        let (f, _) = facade_and_queen(&[]);
        for kind in [0i8, 1, 8, 16, 32] {
            let r = handle(
                &f,
                &request(vec![resource(kind, "whatever", Vec::new())]),
                None,
            )
            .await;
            assert_eq!(
                r.responses[0].error_code,
                ResponseError::InvalidRequest.code(),
                "type {kind}"
            );
        }
    }

    /// The ordering rule of the module header, driven at [`commit`] — which is
    /// where it lives, and whose first KV call is the record write, so the
    /// double's one-shot failure lands exactly on it.
    ///
    /// A record write that fails after a successful configure DELETES the
    /// record and answers retriable, so a following describe omits retention
    /// rather than reporting the value from before the alter. `handle` puts
    /// `Landed::error` on the wire unchanged, which every other test here
    /// exercises.
    #[tokio::test]
    async fn a_failed_record_write_deletes_the_record_and_answers_retriable() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        track(
            &api,
            "orders",
            &[
                ("retentionEnabled", json!(true)),
                ("retentionSeconds", json!(60)),
            ],
        );
        api.fail_kv(Error::Transport("kv is down".into()));

        let bag: Map<String, Value> = [
            ("retentionEnabled".to_string(), json!(true)),
            ("retentionSeconds".to_string(), json!(604_800)),
        ]
        .into_iter()
        .collect();
        let landed = commit(&f, "orders", None, bag, None).await;

        assert_eq!(
            landed.error.as_ref().map(|(code, _)| *code),
            Some(ResponseError::RequestTimedOut),
            "{:?}",
            landed.error
        );
        // The queue WAS configured — that call succeeded — and the record is
        // gone, which is the one honest state.
        assert_eq!(api.configured().len(), 1);
        let ops = api.kv_ops();
        assert!(
            matches!(ops.last(), Some(crate::queen::KvOp::Delete { key, .. })
                if key == &topic_record::key("orders")),
            "{ops:?}"
        );
        assert!(api
            .kv_get(crate::offsets::NAMESPACE, &topic_record::key("orders"))
            .is_none());
    }

    /// A Queen failure on the configure itself is mapped to a code a client can
    /// act on, and a rate cap carries its wait.
    #[tokio::test]
    async fn queens_failures_map_to_codes_a_client_can_act_on() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        track(&api, "orders", &[]);
        api.fail_create(Error::Status {
            code: 429,
            body: "rate_limited".into(),
            retry_after_ms: Some(2_000),
        });
        let r = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![config("retention.ms", Some("604800000"))],
            )]),
            None,
        )
        .await;
        assert_eq!(
            r.responses[0].error_code,
            ResponseError::RequestTimedOut.code()
        );
        assert_eq!(r.throttle_time_ms, 2_000);

        let (f, api) = facade_and_queen(&[("orders", 2)]);
        track(&api, "orders", &[]);
        api.fail_create(Error::status(403, "forbidden"));
        let r = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![config("retention.ms", Some("604800000"))],
            )]),
            None,
        )
        .await;
        assert_eq!(
            r.responses[0].error_code,
            ResponseError::TopicAuthorizationFailed.code()
        );
    }

    /// A record that could NOT be read is retriable, and an absent one is
    /// final. Collapsing the two would make a Queen hiccup look like a
    /// permanent refusal to a tool that prints it.
    #[tokio::test]
    async fn an_unreadable_record_is_retriable_and_an_absent_one_is_not() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        api.fail_kv(Error::Transport("kv is down".into()));
        let r = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![config("retention.ms", Some("-1"))],
            )]),
            None,
        )
        .await;
        assert_eq!(
            r.responses[0].error_code,
            ResponseError::RequestTimedOut.code(),
            "{}",
            message(&r.responses[0])
        );
        assert!(api.configured().is_empty());
    }

    /// A bag that computes to what is already stored writes nothing at all —
    /// which is also what keeps an alter that changes nothing from rewriting a
    /// configuration somebody changed in the Queen console.
    #[tokio::test]
    async fn an_alter_that_changes_nothing_makes_no_call() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        track(
            &api,
            "orders",
            &[
                ("retentionEnabled", json!(true)),
                ("retentionSeconds", json!(604_800)),
            ],
        );
        let r = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![config("retention.ms", Some("604800000"))],
            )]),
            None,
        )
        .await;
        assert_eq!(r.responses[0].error_code, 0);
        assert!(api.configured().is_empty());
    }

    /// Every node answers a topic-addressed alter: there is no coordinator for
    /// a topic, `/configure` is a write any node may make, and two nodes
    /// altering one topic is the last-writer-wins Apache Kafka's own
    /// AlterConfigs is.
    #[tokio::test]
    async fn there_is_no_ownership_gate() {
        let (f, api) = clustered(
            &[("orders", 2)],
            &[
                (1, "kafka-1.example.com", 9092),
                (2, "kafka-2.example.com", 9092),
            ],
            1,
        );
        track(&api, "orders", &[]);
        let r = handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![config("retention.ms", Some("604800000"))],
            )]),
            None,
        )
        .await;
        assert_eq!(r.responses[0].error_code, 0, "{}", message(&r.responses[0]));
        assert_eq!(api.configured().len(), 1);
    }

    /// One catalog read and one record read for the whole request, however many
    /// topics it names.
    #[tokio::test]
    async fn many_topics_cost_one_round_of_reads() {
        let (f, api) = facade_and_queen(&[("a", 1), ("b", 1), ("c", 1)]);
        for t in ["a", "b", "c"] {
            track(&api, t, &[]);
        }
        let before = api.kv_calls.lock().unwrap().len();
        handle(
            &f,
            &request(vec![
                resource(
                    RESOURCE_TOPIC,
                    "a",
                    vec![config("retention.ms", Some("-1"))],
                ),
                resource(
                    RESOURCE_TOPIC,
                    "b",
                    vec![config("retention.ms", Some("-1"))],
                ),
                resource(
                    RESOURCE_TOPIC,
                    "c",
                    vec![config("retention.ms", Some("-1"))],
                ),
            ]),
            None,
        )
        .await;
        assert_eq!(api.list_count(), 1);
        // One getMany for the three records, then one put per topic that
        // actually changed.
        assert_eq!(api.kv_calls.lock().unwrap().len() - before, 4);
    }

    /// Every call carries the connection's credential, so one tenant cannot
    /// alter another's topic through this handler.
    #[tokio::test]
    async fn the_connections_credential_scopes_the_write() {
        let (root, api) = facade_and_queen(&[("orders", 2)]);
        track(&api, "orders", &[]);
        let f = root.for_connection(None).authenticated_as("tenant-key");
        handle(
            &f,
            &request(vec![resource(
                RESOURCE_TOPIC,
                "orders",
                vec![config("retention.ms", Some("604800000"))],
            )]),
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
