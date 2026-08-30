//! CreateTopics (key 19), v2-v6 — a Kafka client provisioning a Queen queue.
//!
//! Until M7 a topic could only come into existence as a side effect of a
//! Metadata that asked for it ([`super::metadata`]'s auto-create). That is
//! enough for a producer and it is not enough for anything with an
//! `AdminClient` in it: a Spring `NewTopic` bean, a Terraform provider, a
//! provisioner that creates before it produces, and — the one that made this
//! stage worth doing — sarama's whole `ClusterAdmin` object, which cannot be
//! built without the key being advertised.
//!
//! ## The one rule the whole handler is arranged around
//!
//! `POST /api/v1/configure` is an **UPSERT that rewrites every config column**
//! (012_configure.sql): called for a queue that already exists with an options
//! bag that does not mention `leaseTime`, it sets `leaseTime` back to the stored
//! procedure's default, and the same for retention, retries, TTL, the DLQ flag
//! and the dedup window. There is no create-if-absent on the broker to ask for
//! instead. So this handler NEVER calls configure for a name the catalog has:
//! an existing topic is answered TOPIC_ALREADY_EXISTS (which is also Kafka's own
//! answer) and nothing is written.
//!
//! Existence is read through [`crate::queen::Catalog::refresh`] — the
//! TTL-BYPASSING read — and not through the cached list, for the same reason the
//! auto-create path uses it: a list up to one TTL old would let a topic another
//! client created three seconds ago look absent, and the write that followed
//! would silently reset a live queue's configuration. The window is narrowed to
//! the length of one admin call, which is as far as it can be narrowed without
//! a create-if-absent upstream.
//!
//! ## What is honoured, and what is answered honestly instead
//!
//! `num_partitions` is the interesting one. **Queen has no declared width.**
//! `configure_queue_v1` creates the queue row and nothing else; a
//! `queen.log_partitions` row materialises on the first push to that lane. The
//! width a client sees is `max(live lanes, QUEEN_KAFKA_DEFAULT_PARTITIONS)`
//! ([`super::metadata::advertised_partitions`]) and that is a property of the
//! FACADE, not a per-topic number a create can set. So a request for N is
//! accepted, nothing is done with it, and the v5+ response reports the width the
//! topic actually has — which is the number the client's very next Metadata will
//! agree with. Asking for fewer than the default is harmless (the client
//! re-reads Metadata and hashes modulo the real width); asking for more is an
//! unmet request and says so in the log.
//!
//! `replication_factor` is the same shape with a shorter argument: one logical
//! broker, and replication is Postgres's business. Any value including -1 is
//! accepted and the response reports 1. Refusing RF>1 would have been the other
//! defensible choice and is rejected for one measured reason — it breaks every
//! provisioner whose default is 3, which is most of them.
//!
//! Both are deliberate deviations and are listed as such in
//! PLAN_QUEEN_KAFKA.md, beside `timeout_ms`, which is not acted on here for the
//! same reason Produce's is not.
//!
//! ## What is refused
//!
//! See `compat/ERRORS.md` for the table. The one worth naming here is
//! `cleanup.policy=compact` ([`crate::topic_config`]): it is refused
//! INVALID_CONFIG, which is what makes Kafka Connect fail at startup instead of
//! losing its connector configuration on a later restart.

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::create_topics_response::{
    CreatableTopicConfigs, CreatableTopicResult,
};
use kafka_protocol::messages::{CreateTopicsRequest, CreateTopicsResponse, TopicName};
use kafka_protocol::protocol::StrBytes;
use std::collections::HashSet;

use crate::handlers::metadata::{self, advertised_partitions};
use crate::topic_config::{self, Applied};
use crate::topic_record;
use crate::{queen, throttle, Facade};

/// Ceiling on the topics ONE request creates.
///
/// The same number and the same argument as
/// [`metadata::MAX_AUTO_CREATES_PER_REQUEST`]: each create is one
/// `POST /api/v1/configure` under a ten-second budget, run in sequence, on a
/// connection that is muted until the whole response is written (conn.rs). A
/// hundred is a provisioner declaring a hundred topics in one call; the
/// hundred-and-first is answered with a code the AdminClient RETRIES, so a
/// request for a thousand converges over a few calls instead of holding one
/// connection open for a thousand upstream ones.
pub(crate) const MAX_CREATES_PER_REQUEST: usize = metadata::MAX_AUTO_CREATES_PER_REQUEST;

/// The version at which KIP-599's THROTTLING_QUOTA_EXCEEDED becomes a code the
/// client understands. Below it the same situation is answered
/// REQUEST_TIMED_OUT, which is a `RetriableException` in the Java AdminClient —
/// so the call is retried inside the request timeout rather than surfaced.
const THROTTLE_CODE_FROM: i16 = 6;

/// Replication this facade reports, always. One logical broker; every Metadata
/// answer already says `replicas=[0], isr=[0]`.
const REPLICATION_FACTOR: i16 = 1;

/// Kafka's own "this field is not meaningful on an errored result": the
/// AdminClient reads neither when `error_code` is set, and -1 is what a real
/// broker puts there.
const NOT_APPLICABLE: i32 = -1;

/// One line per window when the width a client asked for is not the width it
/// gets, and one when the per-request ceiling binds. Both are client-driven and
/// both are exactly the shape that floods a log.
static UNMET: crate::obs::Sampler = crate::obs::Sampler::new(60_000);

/// What one requested topic resolves to, before anything is written.
#[derive(Debug, Clone, PartialEq)]
enum Plan {
    /// Create it, with this options bag and this echo.
    Create(Box<Applied>),
    /// Answer this code and this sentence, and touch nothing.
    Reject(ResponseError, String),
}

pub async fn handle(
    facade: &Facade,
    req: &CreateTopicsRequest,
    version: i16,
    token: Option<&str>,
) -> CreateTopicsResponse {
    // Duplicates first, and before any I/O: Apache Kafka answers every entry of
    // a repeated name INVALID_REQUEST and creates none of them, and this facade
    // has a second reason to do the same — the second configure for one name
    // would be an upsert over the queue the first one just made, which is the
    // one call this handler exists to never make.
    let mut seen: HashSet<&str> = HashSet::new();
    let mut repeated: HashSet<&str> = HashSet::new();
    for t in &req.topics {
        let name = t.name.as_str();
        if !seen.insert(name) {
            repeated.insert(name);
        }
    }

    // Plan every topic from the name and its configs alone. Pure, and it is
    // what the unit tests drive.
    let mut planned: Vec<(&CreatableTopic, Plan)> = req
        .topics
        .iter()
        .map(|t| {
            let plan = if repeated.contains(t.name.as_str()) {
                Plan::Reject(
                    ResponseError::InvalidRequest,
                    "found multiple entries for this topic in one request; none of them were \
                     created"
                        .to_string(),
                )
            } else {
                plan(t)
            };
            (t, plan)
        })
        .collect();

    let mut throttle_ms: Option<i32> = None;

    // The catalog, ONCE for the request — see the module header for why it is
    // the TTL-bypassing read and not the cached list.
    let wanted = planned.iter().any(|(_, p)| matches!(p, Plan::Create(_)));
    let existing: Option<HashSet<String>> = if wanted {
        match facade.catalog.refresh(token).await {
            Ok(queues) => Some(queues.iter().map(|q| q.name.clone()).collect()),
            Err(e) => {
                tracing::warn!(
                    target: "kafka",
                    error = %e,
                    "cannot confirm the topics are absent; creating none of them"
                );
                throttle_ms = throttle::longest(throttle_ms, throttle::for_error(&e));
                let (code, why) = failed(&e, version);
                for (_, p) in planned.iter_mut() {
                    if matches!(p, Plan::Create(_)) {
                        *p = Plan::Reject(code, format!("cannot read the queue list: {why}"));
                    }
                }
                None
            }
        }
    } else {
        None
    };

    let mut created = 0usize;
    let mut results = Vec::with_capacity(planned.len());
    // The bags that actually landed, for the config records written after the
    // loop. See [`record_what_was_created`].
    let mut recordable: Vec<(String, serde_json::Map<String, serde_json::Value>)> = Vec::new();
    for (topic, plan) in planned {
        let name = topic.name.clone();
        let applied = match plan {
            Plan::Reject(code, why) => {
                results.push(errored(name, code, why));
                continue;
            }
            Plan::Create(applied) => applied,
        };

        if existing
            .as_ref()
            .is_some_and(|live| live.contains(name.as_str()))
        {
            results.push(errored(
                name,
                ResponseError::TopicAlreadyExists,
                // Named rather than generic: the whole reason this branch does
                // not fall through to a configure is that configure would reset
                // the queue's configuration, and a user reading the message
                // should learn that nothing was touched.
                "a Queen queue of this name already exists; its configuration was left exactly \
                 as it is (POST /api/v1/configure is an upsert that would rewrite every column)"
                    .to_string(),
            ));
            continue;
        }

        // The width the topic will actually have, which is the number the
        // client's next Metadata reports. A fresh queue has no lanes yet, so it
        // is the configured default.
        let width = advertised_partitions(0, facade.default_partitions);
        note_unmet_width(&name, topic.num_partitions, width);

        // `validate_only`: everything above ran, nothing below does. The answer
        // says what WOULD have happened, which is the whole point of the flag.
        if req.validate_only {
            results.push(made(name, width, &applied));
            continue;
        }

        if created >= MAX_CREATES_PER_REQUEST {
            let code = throttled(version);
            throttle_ms = throttle::longest(throttle_ms, Some(throttle::DEFAULT_MS));
            results.push(errored(
                name,
                code,
                format!(
                    "this request asked for more than {MAX_CREATES_PER_REQUEST} topics; the rest \
                     are answered with a retriable code and are created as the client retries"
                ),
            ));
            continue;
        }
        created += 1;

        let options = serde_json::Value::Object(applied.options.clone());
        match facade
            .catalog
            .create_with(name.as_str(), &options, token)
            .await
        {
            Ok(()) => {
                tracing::info!(
                    target: "kafka",
                    topic = name.as_str(),
                    partitions = width,
                    configs = applied.options.len(),
                    "created a queue for a Kafka topic (CreateTopics)"
                );
                recordable.push((name.to_string(), applied.options.clone()));
                results.push(made(name, width, &applied));
            }
            Err(e) => {
                tracing::error!(
                    target: "kafka",
                    topic = name.as_str(),
                    error = %e,
                    "CreateTopics could not create the queue"
                );
                throttle_ms = throttle::longest(throttle_ms, throttle::for_error(&e));
                let (code, why) = failed(&e, version);
                results.push(errored(name, code, why));
            }
        }
    }

    record_what_was_created(facade, &recordable, token).await;

    CreateTopicsResponse::default()
        .with_throttle_time_ms(throttle_ms.unwrap_or(0))
        .with_topics(results)
}

/// Write the facade's own record of what each create applied
/// ([`crate::topic_record`]), so that a later DescribeConfigs can report the
/// retention back and a later alter can merge onto it rather than reset
/// eighteen columns it cannot read.
///
/// ONE extra `catalog.refresh()` per REQUEST, not per topic, and only when
/// something was actually created — which on this path is once per topic
/// lifetime. It is taken AFTER the creates rather than reusing the one before
/// them because the queue `id` a record is pinned to only exists once the queue
/// does, and it also makes the widths in the response current, which is a small
/// bonus rather than the reason.
///
/// **A create is never failed by this.** If the refresh or the KV write does
/// not land, the topic exists, the client is told so, one line says the record
/// is missing, and the topic simply behaves as untracked until the next alter
/// re-establishes it. Bookkeeping that can fail a create is worse than
/// bookkeeping that can be absent.
async fn record_what_was_created(
    facade: &Facade,
    created: &[(String, serde_json::Map<String, serde_json::Value>)],
    token: Option<&str>,
) {
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
                "the topics were created but their config records were not written; they will \
                 describe without retention until an alter re-establishes them"
            );
            return;
        }
    };
    let ids: std::collections::HashMap<&str, Option<String>> = fresh
        .iter()
        .map(|q| (q.name.as_str(), q.id.clone()))
        .collect();
    let records: Vec<(String, topic_record::Record)> = created
        .iter()
        .map(|(name, options)| {
            let qid = ids.get(name.as_str()).cloned().flatten();
            (
                name.clone(),
                topic_record::Record::new(qid, options.clone()),
            )
        })
        .collect();
    if let Err(e) = topic_record::store_many(facade.queen.as_ref(), &records, token).await {
        tracing::warn!(
            target: "kafka",
            error = %e,
            topics = records.len(),
            "the topics were created but their config records were not written"
        );
    }
}

/// Everything decidable about one requested topic without touching Queen: the
/// name rule, the replica assignment, and the configs.
fn plan(t: &CreatableTopic) -> Plan {
    let name = t.name.as_str();

    // The SAME rule Metadata applies, in the code THIS surface answers it with.
    //
    // Metadata hides a `__` name as UNKNOWN_TOPIC_OR_PARTITION because to Kafka
    // those are perfectly valid names belonging to the broker's own bookkeeping,
    // and a client listing them must not meet a permanent error. CreateTopics is
    // the other case: the client named it and can act on the answer, and
    // INVALID_TOPIC_EXCEPTION is exactly the code Apache Kafka raises where a
    // topic NAME is validated. Creating one anyway would make a queue this
    // facade then refuses to show anywhere, which is worse than either answer.
    if metadata::reserved_or_invalid(name).is_some() {
        return Plan::Reject(
            ResponseError::InvalidTopicException,
            if name.starts_with("__") {
                "`__` is reserved for a broker's internal topics and this facade hides every \
                 name that begins with it; a queue created here could never be seen again"
                    .to_string()
            } else {
                "not a legal Kafka topic name: 1-249 characters of [A-Za-z0-9._-], and neither \
                 `.` nor `..`"
                    .to_string()
            },
        );
    }

    // A manual replica assignment names broker ids to place partitions on. This
    // facade places nothing anywhere — Metadata already refuses to claim
    // replicas it does not arbitrate — so accepting one would be silently
    // discarding an explicit operator instruction. Refusing is loud and rare:
    // no ordinary client sends assignments.
    if !t.assignments.is_empty() {
        return Plan::Reject(
            ResponseError::InvalidReplicaAssignment,
            "this facade is one logical broker and places no partition on any node, so a manual \
             replica assignment cannot be honoured. Omit `assignments` (or pass -1 for the \
             partition count and replication factor, KIP-464)"
                .to_string(),
        );
    }

    let configs: Vec<(&str, Option<&str>)> = t
        .configs
        .iter()
        .map(|c| (c.name.as_str(), c.value.as_ref().map(|v| v.as_str())))
        .collect();
    match topic_config::apply(&configs) {
        Ok(applied) => Plan::Create(Box::new(applied)),
        Err(why) => Plan::Reject(ResponseError::InvalidConfig, why),
    }
}

/// One created (or validated) topic.
fn made(name: TopicName, width: i32, applied: &Applied) -> CreatableTopicResult {
    CreatableTopicResult::default()
        .with_name(name)
        .with_error_code(0)
        .with_error_message(None)
        // v5+ only; dropped by the encoder below it.
        .with_num_partitions(width)
        .with_replication_factor(REPLICATION_FACTOR)
        .with_configs(Some(
            applied
                .echo
                .iter()
                .map(|r| {
                    CreatableTopicConfigs::default()
                        .with_name(StrBytes::from_string(r.name.to_string()))
                        .with_value(Some(StrBytes::from_string(r.value.clone())))
                        // Per row since M7 F4: the create's own echo of a
                        // retention it just applied is writable, because the
                        // record this create is about to write is what makes an
                        // alter of it land (`topic_record`).
                        .with_read_only(r.read_only)
                        .with_config_source(r.source as i8)
                        .with_is_sensitive(topic_config::IS_SENSITIVE)
                })
                .collect(),
        ))
}

/// One refused topic. `num_partitions` and `replication_factor` are -1 and the
/// configs are null, which is what a real broker answers beside an error and
/// what stops a client rendering a width for a topic that does not exist.
fn errored(name: TopicName, code: ResponseError, why: String) -> CreatableTopicResult {
    CreatableTopicResult::default()
        .with_name(name)
        .with_error_code(code.code())
        .with_error_message(Some(StrBytes::from_string(why)))
        .with_num_partitions(NOT_APPLICABLE)
        .with_replication_factor(NOT_APPLICABLE as i16)
        .with_configs(None)
}

/// The code a rate cap answers at this version, and whether the client knows
/// it. See [`THROTTLE_CODE_FROM`].
fn throttled(version: i16) -> ResponseError {
    if version >= THROTTLE_CODE_FROM {
        ResponseError::ThrottlingQuotaExceeded
    } else {
        ResponseError::RequestTimedOut
    }
}

/// The closest Kafka code for a failed call to Queen, chosen on retriability
/// first — the same axis `produce::kafka_error` is chosen on, because it is what
/// decides between an AdminClient backoff and an exception raised to the caller.
fn failed(e: &queen::Error, version: i16) -> (ResponseError, String) {
    let code = match e {
        // No answer at all, or a Queen that is there and not serving. Both are
        // "we do not know", and REQUEST_TIMED_OUT is the retriable code whose
        // meaning is exactly that.
        queen::Error::Transport(_) => ResponseError::RequestTimedOut,
        queen::Error::Status { code, .. } => match code {
            // The credential this connection carries may not create queues.
            // Fatal and named, rather than a mystery 500.
            401 | 403 => ResponseError::TopicAuthorizationFailed,
            408 => ResponseError::RequestTimedOut,
            // Cloud: a frozen or rate-capped tenant. KIP-599 is the whole
            // reason v6 is in the advertised window.
            429 => throttled(version),
            502..=504 => ResponseError::RequestTimedOut,
            _ => ResponseError::UnknownServerError,
        },
        // A 2xx this client could not read, or a configure that did not
        // confirm. Our bug or the broker's, and it should be loud rather than
        // dressed up as a timeout a client will quietly retry for ever.
        queen::Error::Body(_) | queen::Error::Precondition { .. } => {
            ResponseError::UnknownServerError
        }
    };
    // `wire_reason` and not `to_string`: this string is going into a Kafka
    // `error_message` and out to somebody's terminal, so it is bounded and
    // scrubbed rather than merely clamped for a log line. It is what carries the
    // proxy's own sentence ("operation not permitted for this credential", "not
    // in your plan") to an operator whose CreateTopics was refused for a SCOPE
    // they can go and fix.
    (code, e.wire_reason())
}

/// One INFO line per window when a client asked for a width it did not get.
///
/// Not an error: the client's next Metadata reports the real width and every
/// producer hashes modulo what Metadata said, so nothing breaks. It is logged
/// because an operator who set `--partitions 32` and sees 8 deserves to find out
/// why from the facade rather than by counting lanes.
fn note_unmet_width(name: &TopicName, asked: i32, width: i32) {
    // -1 is KIP-464's "I do not care", which is what a modern AdminClient sends
    // by default. Nothing unmet about it.
    if asked <= 0 || asked <= width {
        return;
    }
    if let Some(suppressed) = UNMET.tick_now() {
        tracing::info!(
            target: "kafka",
            topic = name.as_str(),
            asked,
            width,
            suppressed,
            "CreateTopics asked for a partition count this facade does not declare per topic; \
             the width is max(live lanes, QUEEN_KAFKA_DEFAULT_PARTITIONS) and the response \
             reports the real one"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::testing::{facade, facade_and_queen};
    use crate::queen::Error;
    use kafka_protocol::messages::create_topics_request::{
        CreatableReplicaAssignment, CreatableTopicConfig,
    };

    fn topic(name: &str) -> CreatableTopic {
        CreatableTopic::default()
            .with_name(TopicName(StrBytes::from_string(name.to_string())))
            .with_num_partitions(-1)
            .with_replication_factor(-1)
    }

    fn with_configs(name: &str, configs: &[(&str, Option<&str>)]) -> CreatableTopic {
        topic(name).with_configs(
            configs
                .iter()
                .map(|(k, v)| {
                    CreatableTopicConfig::default()
                        .with_name(StrBytes::from_string(k.to_string()))
                        .with_value(v.map(|v| StrBytes::from_string(v.to_string())))
                })
                .collect(),
        )
    }

    fn request(topics: Vec<CreatableTopic>) -> CreateTopicsRequest {
        CreateTopicsRequest::default()
            .with_topics(topics)
            .with_timeout_ms(30_000)
    }

    fn code(r: &CreateTopicsResponse, name: &str) -> i16 {
        r.topics
            .iter()
            .find(|t| t.name.as_str() == name)
            .unwrap_or_else(|| panic!("{name} is not in the answer"))
            .error_code
    }

    fn message(r: &CreateTopicsResponse, name: &str) -> String {
        r.topics
            .iter()
            .find(|t| t.name.as_str() == name)
            .and_then(|t| t.error_message.as_ref())
            .map(|m| m.to_string())
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn a_new_topic_is_created_and_reports_the_width_it_will_have() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        let r = handle(&f, &request(vec![topic("events")]), 6, None).await;

        assert_eq!(code(&r, "events"), 0);
        let t = &r.topics[0];
        // The FACADE's width (the fixture configures 4), not the -1 asked for.
        assert_eq!(t.num_partitions, 4);
        assert_eq!(t.replication_factor, 1);
        // One configure, with an EMPTY bag — the same body the auto-create
        // path sends, which is what keeps a create from writing defaults it was
        // not asked for.
        assert_eq!(api.configured().len(), 1);
        assert_eq!(api.configured()[0].0, "events");
        assert_eq!(api.configured()[0].1, serde_json::json!({}));
        // ...and NOT through the auto-create method.
        assert!(api.created().is_empty());
    }

    /// THE rule: an existing name is never configured, because configure is an
    /// upsert that would rewrite the queue's whole configuration.
    #[tokio::test]
    async fn an_existing_topic_is_refused_and_never_configured() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        let r = handle(&f, &request(vec![topic("orders")]), 6, None).await;

        assert_eq!(code(&r, "orders"), ResponseError::TopicAlreadyExists.code());
        assert!(message(&r, "orders").contains("upsert"));
        assert!(
            api.configured().is_empty(),
            "an existing queue was configured: its leaseTime, retention and dedup window are gone"
        );
        assert_eq!(r.topics[0].num_partitions, -1);
    }

    /// The configs the mapping refuses take the topic with them, and nothing is
    /// written for it.
    #[tokio::test]
    async fn compaction_is_refused_and_nothing_is_written() {
        let (f, api) = facade_and_queen(&[]);
        let r = handle(
            &f,
            &request(vec![with_configs(
                "connect-configs",
                &[("cleanup.policy", Some("compact"))],
            )]),
            6,
            None,
        )
        .await;

        assert_eq!(
            code(&r, "connect-configs"),
            ResponseError::InvalidConfig.code()
        );
        assert!(message(&r, "connect-configs").contains("compaction"));
        assert!(api.configured().is_empty());
    }

    /// A retention the mapping accepts reaches Queen as Queen's own options,
    /// and comes back in the v5+ echo — which is the only place a client can
    /// read it, because Queen exposes no HTTP read of a queue's config.
    #[tokio::test]
    async fn retention_is_written_as_queen_options_and_echoed_back() {
        let (f, api) = facade_and_queen(&[]);
        let r = handle(
            &f,
            &request(vec![with_configs(
                "sessions",
                &[("retention.ms", Some("604800000"))],
            )]),
            6,
            None,
        )
        .await;

        assert_eq!(code(&r, "sessions"), 0);
        assert_eq!(
            api.configured()[0].1,
            serde_json::json!({"retentionEnabled": true, "retentionSeconds": 604_800})
        );
        let echoed: Vec<(String, String)> = r.topics[0]
            .configs
            .as_ref()
            .unwrap()
            .iter()
            .map(|c| (c.name.to_string(), c.value.as_ref().unwrap().to_string()))
            .collect();
        assert!(echoed.contains(&("retention.ms".into(), "604800000".into())));
        assert!(echoed.contains(&("cleanup.policy".into(), "delete".into())));
        assert!(echoed.contains(&("min.insync.replicas".into(), "1".into())));
        // `read_only` is per row since M7 F4: the two rows whose only legal
        // value is the one already reported cannot be changed, and the
        // retention this create just applied can — the record written below is
        // what makes an alter of it land.
        let flags: Vec<(String, bool)> = r.topics[0]
            .configs
            .as_ref()
            .unwrap()
            .iter()
            .map(|c| (c.name.to_string(), c.read_only))
            .collect();
        assert!(
            flags.contains(&("cleanup.policy".into(), true)),
            "{flags:?}"
        );
        assert!(
            flags.contains(&("min.insync.replicas".into(), true)),
            "{flags:?}"
        );
        assert!(flags.contains(&("retention.ms".into(), false)), "{flags:?}");
    }

    /// The create writes the facade's own record of the bag it sent
    /// ([`crate::topic_record`]) — which is what a later describe reports the
    /// retention from and what a later alter merges onto.
    #[tokio::test]
    async fn a_create_records_the_bag_it_sent() {
        let (f, api) = facade_and_queen(&[]);
        handle(
            &f,
            &request(vec![
                with_configs("sessions", &[("retention.ms", Some("604800000"))]),
                // ...and one with no configs at all, whose bag is EMPTY. That
                // record is not bookkeeping about nothing: it is what makes a
                // later `--alter` on this topic land instead of being refused
                // as untracked.
                with_configs("plain", &[]),
            ]),
            6,
            None,
        )
        .await;

        assert_eq!(
            api.kv_get(crate::offsets::NAMESPACE, &topic_record::key("sessions"))
                .unwrap()["set"],
            serde_json::json!({"retentionEnabled": true, "retentionSeconds": 604_800})
        );
        assert_eq!(
            api.kv_get(crate::offsets::NAMESPACE, &topic_record::key("plain"))
                .unwrap()["set"],
            serde_json::json!({})
        );
        // One post-create refresh for the whole request, not one per topic:
        // the plan read, and the read that carries the queue ids.
        assert_eq!(api.list_count(), 2);
        // ...and one KV call for both records.
        assert_eq!(api.kv_calls.lock().unwrap().len(), 1);
    }

    /// Bookkeeping that can fail a create is worse than bookkeeping that can be
    /// absent. The topic exists, the client is told so, and the topic simply
    /// behaves as untracked until an alter re-establishes it.
    #[tokio::test]
    async fn a_create_whose_record_write_fails_still_succeeds() {
        let (f, api) = facade_and_queen(&[]);
        api.fail_kv(Error::Transport("kv is down".into()));
        let r = handle(
            &f,
            &request(vec![with_configs(
                "sessions",
                &[("retention.ms", Some("604800000"))],
            )]),
            6,
            None,
        )
        .await;

        assert_eq!(code(&r, "sessions"), 0, "a create failed on bookkeeping");
        assert_eq!(api.configured().len(), 1, "the queue was not created");
        assert!(api
            .kv_get(crate::offsets::NAMESPACE, &topic_record::key("sessions"))
            .is_none());
    }

    /// A create that was refused leaves NO record. It is the other half of the
    /// rule above and it is what keeps the record from ever describing a queue
    /// that was not made: `record_what_was_created` is given only the names
    /// whose `/configure` actually returned, so a failed plan read — which
    /// refuses every create in the request — writes nothing at all.
    #[tokio::test]
    async fn a_refused_create_leaves_no_record() {
        let (f, api) = facade_and_queen(&[]);
        api.fail_list(Error::Transport("queen is down".into()));
        let r = handle(&f, &request(vec![with_configs("later", &[])]), 6, None).await;

        assert_ne!(code(&r, "later"), 0);
        assert!(api.configured().is_empty());
        assert!(api
            .kv_get(crate::offsets::NAMESPACE, &topic_record::key("later"))
            .is_none());
        assert!(
            api.kv_calls.lock().unwrap().is_empty(),
            "a refused create still reached the record store"
        );
    }

    /// The name rule is Metadata's, in the code this surface answers it with.
    #[tokio::test]
    async fn a_reserved_or_illegal_name_is_invalid_here_and_never_created() {
        let (f, api) = facade_and_queen(&[]);
        let names = [
            "__consumer_offsets",
            "__evil",
            "has spaces",
            "",
            "..",
            "a/b",
        ];
        let r = handle(
            &f,
            &request(names.iter().map(|n| topic(n)).collect()),
            6,
            None,
        )
        .await;

        for name in names {
            assert_eq!(
                code(&r, name),
                ResponseError::InvalidTopicException.code(),
                "{name}"
            );
        }
        assert!(api.configured().is_empty());
    }

    #[tokio::test]
    async fn a_manual_replica_assignment_is_refused() {
        let (f, api) = facade_and_queen(&[]);
        let t = topic("placed").with_assignments(vec![CreatableReplicaAssignment::default()
            .with_partition_index(0)
            .with_broker_ids(vec![1.into(), 2.into()])]);
        let r = handle(&f, &request(vec![t]), 6, None).await;

        assert_eq!(
            code(&r, "placed"),
            ResponseError::InvalidReplicaAssignment.code()
        );
        assert!(api.configured().is_empty());
    }

    /// Apache Kafka's own answer to a name repeated in one request, and the
    /// second reason to give it: a second configure for one name would be an
    /// upsert over the queue the first one just created.
    #[tokio::test]
    async fn a_repeated_name_refuses_every_entry_and_creates_nothing() {
        let (f, api) = facade_and_queen(&[]);
        let r = handle(&f, &request(vec![topic("twice"), topic("twice")]), 6, None).await;

        assert_eq!(r.topics.len(), 2);
        for t in &r.topics {
            assert_eq!(t.error_code, ResponseError::InvalidRequest.code());
        }
        assert!(api.configured().is_empty());
    }

    /// `validate_only` answers what WOULD have happened and writes nothing —
    /// including the width, which is the number a client is checking for.
    #[tokio::test]
    async fn validate_only_answers_without_writing() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        let r = handle(
            &f,
            &request(vec![topic("events"), topic("orders")]).with_validate_only(true),
            6,
            None,
        )
        .await;

        assert_eq!(code(&r, "events"), 0);
        assert_eq!(r.topics[0].num_partitions, 4);
        // ...and the refusals are the real ones, which is the point of a dry run.
        assert_eq!(code(&r, "orders"), ResponseError::TopicAlreadyExists.code());
        assert!(api.configured().is_empty());
    }

    /// The per-request ceiling: the first hundred are created, the rest are
    /// answered a code the AdminClient retries.
    #[tokio::test]
    async fn one_request_creates_at_most_the_bound() {
        let (f, api) = facade_and_queen(&[]);
        let names: Vec<String> = (0..MAX_CREATES_PER_REQUEST + 5)
            .map(|i| format!("t{i}"))
            .collect();
        let r = handle(
            &f,
            &request(names.iter().map(|n| topic(n)).collect()),
            6,
            None,
        )
        .await;

        assert_eq!(api.configured().len(), MAX_CREATES_PER_REQUEST);
        let refused: Vec<i16> = r
            .topics
            .iter()
            .skip(MAX_CREATES_PER_REQUEST)
            .map(|t| t.error_code)
            .collect();
        assert_eq!(refused.len(), 5);
        assert!(refused
            .iter()
            .all(|c| *c == ResponseError::ThrottlingQuotaExceeded.code()));
        assert!(r.throttle_time_ms > 0);
    }

    /// Queen's status codes, on the axis that matters: retriable or not.
    #[tokio::test]
    async fn queens_failures_map_to_codes_a_client_can_act_on() {
        for (e, want) in [
            (
                Error::status(401, "no"),
                ResponseError::TopicAuthorizationFailed,
            ),
            (
                Error::status(403, "no"),
                ResponseError::TopicAuthorizationFailed,
            ),
            (
                Error::status(500, "boom"),
                ResponseError::UnknownServerError,
            ),
            (
                Error::status(503, "draining"),
                ResponseError::RequestTimedOut,
            ),
            (
                Error::Transport("refused".into()),
                ResponseError::RequestTimedOut,
            ),
            (
                Error::Body("not json".into()),
                ResponseError::UnknownServerError,
            ),
        ] {
            let (f, api) = facade_and_queen(&[]);
            api.fail_create(e.clone());
            let r = handle(&f, &request(vec![topic("events")]), 6, None).await;
            assert_eq!(code(&r, "events"), want.code(), "{e}");
        }
    }

    /// A rate cap: THROTTLING_QUOTA_EXCEEDED where the client knows the code,
    /// REQUEST_TIMED_OUT below it — and the wait is carried either way.
    #[tokio::test]
    async fn a_rate_cap_is_the_code_this_version_understands() {
        for (version, want) in [
            (6i16, ResponseError::ThrottlingQuotaExceeded),
            (5, ResponseError::RequestTimedOut),
            (2, ResponseError::RequestTimedOut),
        ] {
            let (f, api) = facade_and_queen(&[]);
            api.fail_create(Error::Status {
                code: 429,
                body: "rate_limited".into(),
                retry_after_ms: Some(5_000),
            });
            let r = handle(&f, &request(vec![topic("events")]), version, None).await;
            assert_eq!(code(&r, "events"), want.code(), "v{version}");
            assert_eq!(r.throttle_time_ms, 5_000, "v{version}");
        }
    }

    /// An unreadable catalog creates NOTHING: the whole reason existence is
    /// re-read is that a create over an existing queue resets its config, so a
    /// read that failed must not be treated as "absent".
    #[tokio::test]
    async fn an_unreadable_catalog_creates_nothing() {
        let (f, api) = facade_and_queen(&[]);
        api.fail_list(Error::Transport("queen is down".into()));
        let r = handle(&f, &request(vec![topic("events")]), 6, None).await;

        assert_eq!(code(&r, "events"), ResponseError::RequestTimedOut.code());
        assert!(api.configured().is_empty());
    }

    /// Every call carries the CONNECTION's credential, which is what scopes the
    /// queue to a tenant on the broker side.
    #[tokio::test]
    async fn the_connections_credential_is_what_creates() {
        let (root, api) = facade_and_queen(&[]);
        let f = root.for_connection(None).authenticated_as("tenant-key");
        handle(&f, &request(vec![topic("events")]), 6, Some("tenant-key")).await;

        let tokens = api.tokens.lock().unwrap().clone();
        assert!(
            tokens.iter().all(|t| t.as_deref() == Some("tenant-key")),
            "a call went out under the wrong credential: {tokens:?}"
        );
    }

    /// An empty request is an empty answer and no call to Queen — the shape a
    /// client's periodic "create if missing" reduces to once everything exists.
    #[tokio::test]
    async fn an_empty_request_touches_nothing() {
        let (f, api) = facade_and_queen(&[("orders", 2)]);
        let r = handle(&f, &request(vec![]), 6, None).await;
        assert!(r.topics.is_empty());
        assert_eq!(api.list_count(), 0);
    }

    /// The width is reported at every advertised version; below v5 the encoder
    /// drops the field, which is the version boundary and not this handler's
    /// business — so the handler answers the same thing throughout.
    #[tokio::test]
    async fn every_advertised_version_answers_the_same_facts() {
        for version in 2i16..=6 {
            let f = facade(&[]);
            let r = handle(&f, &request(vec![topic("events")]), version, None).await;
            assert_eq!(r.topics[0].error_code, 0, "v{version}");
            assert_eq!(r.topics[0].num_partitions, 4, "v{version}");
            assert_eq!(r.topics[0].replication_factor, 1, "v{version}");
        }
    }
}
