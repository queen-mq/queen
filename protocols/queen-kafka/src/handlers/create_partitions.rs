//! CreatePartitions (key 37), v0-v3 — `kafka-topics.sh --alter --partitions N`.
//!
//! ## An increase raises the topic's declared width
//!
//! Queen declares no width per queue: a lane comes into existence only when
//! something is pushed to it, and the width this facade advertises is
//! `max(live lanes, the topic's declared floor or QUEEN_KAFKA_DEFAULT_PARTITIONS)`
//! ([`metadata::advertised_partitions`]). CreateTopics declares the floor; an
//! INCREASE here raises it, in the same record ([`crate::topic_record`]), which
//! is Apache Kafka's own answer to `kafka-topics.sh --alter --partitions`: the
//! topic is wider from the next Metadata on, and keys hash onto the new width.
//! (It was an advertised refusal until 2026-09-24; tools that grow a topic in
//! chunks — kload creates 100k partitions 5000 at a time — could not run.)
//!
//! Only a topic this facade TRACKS can be widened: one with a record pinned to
//! the queue that is there now. Any other topic is refused with a sentence
//! naming the fix, because a floor written for a queue the facade did not create
//! would be a second record of a width nothing else declared.
//!
//! The other two answers are the oracle's own, byte for byte:
//!
//!   * `count == current` — *"Topic already has N partition(s)."*
//!   * `count < current` — a DECREASE, which a real broker refuses too:
//!     *"The topic X currently has N partition(s); M would not be an
//!     increase."*
//!
//! Both strings were recorded off `apache/kafka:3.9.1` in KRaft mode
//! (`ReplicationControlManager`), not copied out of a document. The KRaft
//! wording is NOT the ZooKeeper-era wording that the same case produced in
//! older brokers, which is why they were measured rather than recalled.
//!
//! ## There is no separate "below 1" case, and that is measured too
//!
//! `--partitions 0` against a topic of width 4 answers the DECREASE sentence on
//! the oracle: KRaft's own `count == current` / `count < current` comparison
//! catches every non-positive count before any lower bound could, because the
//! advertised width is never negative. A separate branch here would answer a
//! sentence the oracle never sends.
//!
//! ## The order of the checks is the oracle's
//!
//! The width comparison runs BEFORE the replica-assignment check, because that
//! is the order `ReplicationControlManager` applies them: a decrease that also
//! carries an assignment is INVALID_PARTITIONS on a real broker, not
//! INVALID_REPLICA_ASSIGNMENT, and the byte-identity above is worth more than
//! the more specific complaint in a case no client produces.
//!
//! ## What is written
//!
//! One `put` per widened topic, all in one KV batch, and nothing for any other
//! answer. `validate_only` runs every check and writes nothing. `timeout_ms` is
//! not acted on: there is no asynchronous work to bound.
//!
//! ## Cluster mode: no gate
//!
//! The answer is computed from the shared catalog and THIS node's
//! `QUEEN_KAFKA_DEFAULT_PARTITIONS`. Two nodes started with different values
//! for it answer different widths — which is already true of every Metadata
//! response this fleet serves, so it is a deployment error and not a new hazard
//! introduced here.

use std::collections::HashMap;

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic;
use kafka_protocol::messages::create_partitions_response::CreatePartitionsTopicResult;
use kafka_protocol::messages::{CreatePartitionsRequest, CreatePartitionsResponse, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::handlers::metadata;
use crate::{queen, throttle, topic_record, Facade};

pub async fn handle(
    facade: &Facade,
    req: &CreatePartitionsRequest,
    token: Option<&str>,
) -> CreatePartitionsResponse {
    let mut throttle_ms: Option<i32> = None;

    // The catalog, at most ONCE for the request, and not at all for a request
    // that names no topic. The live lane count is half of the width every
    // answer below is computed against; the queue id pins a widened record.
    let catalog = if req.topics.is_empty() {
        Ok(HashMap::new())
    } else {
        match facade.catalog.list(token).await {
            Ok(queues) => Ok(queues
                .iter()
                .map(|q| (q.name.clone(), (q.partitions, q.floor, q.id.clone())))
                .collect::<HashMap<String, (i64, Option<u32>, Option<String>)>>()),
            Err(e) => {
                tracing::warn!(
                    target: "kafka",
                    error = %e,
                    "CreatePartitions cannot read the queue list"
                );
                throttle_ms = throttle::longest(throttle_ms, throttle::for_error(&e));
                Err(failed(&e))
            }
        }
    };

    let decisions: Vec<Decision> = req
        .topics
        .iter()
        .map(|t| one(facade, t, &catalog))
        .collect();

    // The increases: each topic's record, widened, in ONE batch.
    let wanted: Vec<String> = decisions
        .iter()
        .filter_map(|d| match d {
            Decision::Increase { name, .. } => Some(name.clone()),
            Decision::Answer(_) => None,
        })
        .collect();
    let records = if wanted.is_empty() {
        Ok(HashMap::new())
    } else {
        topic_record::load_many(facade.queen.as_ref(), &wanted, token).await
    };
    let mut writes: Vec<(String, topic_record::Record)> = Vec::new();
    let mut results: Vec<Option<CreatePartitionsTopicResult>> = Vec::new();
    for d in decisions {
        results.push(match d {
            Decision::Answer(a) => Some(a),
            Decision::Increase { name, qid, count } => match &records {
                Err(e) => {
                    throttle_ms = throttle::longest(throttle_ms, throttle::for_error(e));
                    let (code, why) = failed(e);
                    Some(answer(&name, Some(code), Some(why)))
                }
                Ok(records) => match records.get(&name).filter(|r| r.describes(qid.as_deref())) {
                    Some(record) => {
                        if !req.validate_only {
                            writes.push((
                                name.clone(),
                                record.clone().with_partitions(Some(count as u32)),
                            ));
                        }
                        // Answered after the write lands (below).
                        None
                    }
                    None => Some(answer(
                        &name,
                        Some(ResponseError::InvalidPartitions),
                        Some(format!(
                            "{name} was not created through this facade, so it has no declared \
                             width to raise: its width is max(live lanes, \
                             QUEEN_KAFKA_DEFAULT_PARTITIONS). Recreate it with CreateTopics \
                             `numPartitions`, or produce to the higher lanes directly."
                        )),
                    )),
                },
            },
        });
    }
    let written = if writes.is_empty() {
        Ok(())
    } else {
        topic_record::store_many(facade.queen.as_ref(), &writes, token).await
    };
    if written.is_ok() && !writes.is_empty() {
        // The next Metadata from THIS node reports the new width at once; the
        // other nodes of a cluster within one catalog TTL.
        facade.catalog.invalidate(token).await;
    }
    let results = req
        .topics
        .iter()
        .zip(results)
        .map(|(t, r)| match r {
            Some(r) => r,
            None => match &written {
                Ok(()) => answer(t.name.0.as_str(), None, None),
                Err(e) => {
                    let (code, why) = failed(e);
                    answer(t.name.0.as_str(), Some(code), Some(why))
                }
            },
        })
        .collect();
    CreatePartitionsResponse::default()
        .with_throttle_time_ms(throttle_ms.unwrap_or(0))
        .with_results(results)
}

/// The live width inputs for every topic this request names — lane count,
/// declared floor and queue id — or the one refusal every topic gets when the
/// list failed.
type Catalog = Result<HashMap<String, (i64, Option<u32>, Option<String>)>, (ResponseError, String)>;

/// One topic's answer, or the increase it asks for.
enum Decision {
    Answer(CreatePartitionsTopicResult),
    Increase {
        name: String,
        qid: Option<String>,
        count: i32,
    },
}

/// One requested topic's decision.
fn one(facade: &Facade, t: &CreatePartitionsTopic, catalog: &Catalog) -> Decision {
    let name = t.name.0.as_str();
    let refuse = |e, m| Decision::Answer(answer(name, Some(e), m));

    // The name rule every non-Metadata API applies, in the one code they may
    // answer: a `__`-prefixed or illegal name is a topic this facade does not
    // have. No message, because the oracle sends none for this code either.
    if let Some(e) = metadata::not_a_topic_here(name) {
        return refuse(e, None);
    }
    let live = match catalog {
        Ok(live) => live,
        Err((e, why)) => return refuse(*e, Some(why.clone())),
    };
    let Some((lanes, floor, qid)) = live.get(name) else {
        return refuse(ResponseError::UnknownTopicOrPartition, None);
    };

    let current =
        metadata::advertised_partitions(*lanes, floor.unwrap_or(facade.default_partitions));
    let wanted = t.count;

    // The oracle's own two sentences, in the oracle's own order. Recorded off
    // apache/kafka:3.9.1; see the module header.
    if wanted == current {
        return refuse(
            ResponseError::InvalidPartitions,
            Some(format!("Topic already has {current} partition(s).")),
        );
    }
    if wanted < current {
        return refuse(
            ResponseError::InvalidPartitions,
            Some(format!(
                "The topic {name} currently has {current} partition(s); {wanted} would not be an \
                 increase."
            )),
        );
    }

    // An increase. A manual replica assignment is refused by name first, and it
    // is the same sentence `handlers::create_topics` gives the same field:
    // there is one logical broker here and it places no partition on any node,
    // so an explicit placement is an operator instruction this facade would
    // otherwise discard in silence.
    if t.assignments.as_ref().is_some_and(|a| !a.is_empty()) {
        return refuse(
            ResponseError::InvalidReplicaAssignment,
            Some(
                "this facade is one logical broker and places no partition on any node, so a \
                 manual replica assignment cannot be honoured. Omit `assignments`"
                    .to_string(),
            ),
        );
    }
    if wanted as i64 > i64::from(metadata::MAX_ADVERTISED_PARTITIONS) {
        return refuse(
            ResponseError::InvalidPartitions,
            Some(format!(
                "{wanted} partitions is past this facade's ceiling of {} per topic",
                metadata::MAX_ADVERTISED_PARTITIONS
            )),
        );
    }
    Decision::Increase {
        name: name.to_string(),
        qid: qid.clone(),
        count: wanted,
    }
}

fn answer(
    name: &str,
    error: Option<ResponseError>,
    message: Option<String>,
) -> CreatePartitionsTopicResult {
    CreatePartitionsTopicResult::default()
        .with_name(TopicName(StrBytes::from_string(name.to_string())))
        .with_error_code(error.map_or(0, |e| e.code()))
        .with_error_message(message.map(StrBytes::from_string))
}

/// What a Queen failure on the catalog read becomes.
///
/// The same mapping the rest of the admin surface uses: authorization is not
/// retriable and must be reportable by name, and everything else — unreachable,
/// rate-limited, a gateway — is "not now" and REQUEST_TIMED_OUT, which is on the
/// closed set an AdminClient retries for this API.
fn failed(e: &queen::Error) -> (ResponseError, String) {
    match e {
        queen::Error::Status {
            code: 401 | 403, ..
        } => (
            ResponseError::TopicAuthorizationFailed,
            // The refusal's own words, bounded and scrubbed for the wire
            // (queen.rs `wire_reason_of`): a fixed sentence here would name the
            // problem without naming the scope that fixes it.
            queen::wire_reason_of(&format!("Queen refused this credential: {e}")),
        ),
        other => (
            ResponseError::RequestTimedOut,
            queen::wire_reason_of(&format!("the queue list could not be read: {other}")),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::testing::{clustered, facade_and_queen};
    use crate::queen::Error;
    use kafka_protocol::messages::create_partitions_request::CreatePartitionsAssignment;
    use kafka_protocol::protocol::{Decodable, Encodable, Message};

    /// The fixture's `orders` has four live lanes and the test facade's
    /// `default_partitions` is four, so its advertised width is four.
    const WIDTH: i32 = 4;

    fn request(topics: &[(&str, i32)]) -> CreatePartitionsRequest {
        CreatePartitionsRequest::default().with_topics(
            topics
                .iter()
                .map(|(name, count)| {
                    CreatePartitionsTopic::default()
                        .with_name(TopicName(StrBytes::from_string((*name).to_string())))
                        .with_count(*count)
                })
                .collect(),
        )
    }

    fn only(resp: &CreatePartitionsResponse) -> &CreatePartitionsTopicResult {
        assert_eq!(resp.results.len(), 1, "{:?}", resp.results);
        &resp.results[0]
    }

    fn message(r: &CreatePartitionsTopicResult) -> String {
        r.error_message
            .as_ref()
            .map(|m| m.as_str().to_string())
            .unwrap_or_default()
    }

    /// The oracle's sentence, byte for byte. Recorded off apache/kafka:3.9.1:
    /// `kafka-topics.sh --alter --topic cp-probe --partitions 2` on a
    /// four-partition topic prints exactly this.
    #[tokio::test]
    async fn a_decrease_is_invalid_partitions_with_the_oracles_sentence() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        let r = handle(&f, &request(&[("orders", 2)]), None).await;
        let result = only(&r);
        assert_eq!(result.error_code, ResponseError::InvalidPartitions.code());
        assert_eq!(
            message(result),
            "The topic orders currently has 4 partition(s); 2 would not be an increase."
        );
        assert!(api.configured().is_empty(), "a refusal wrote to Queen");
    }

    /// ...and the other one. Same recording, same run.
    #[tokio::test]
    async fn equal_to_the_current_width_is_the_oracles_other_sentence() {
        let (f, _) = facade_and_queen(&[("orders", 4)]);
        let result = &handle(&f, &request(&[("orders", WIDTH)]), None)
            .await
            .results[0];
        assert_eq!(result.error_code, ResponseError::InvalidPartitions.code());
        assert_eq!(message(result), "Topic already has 4 partition(s).");
    }

    /// A non-positive count is NOT a case of its own: the oracle answers the
    /// decrease sentence for `--partitions 0` (measured), because the
    /// comparison catches it before any lower bound could.
    #[tokio::test]
    async fn a_count_below_one_takes_the_decrease_branch_exactly_as_the_oracle_does() {
        let (f, _) = facade_and_queen(&[("orders", 4)]);
        for count in [0, -1] {
            let result = &handle(&f, &request(&[("orders", count)]), None)
                .await
                .results[0];
            assert_eq!(
                result.error_code,
                ResponseError::InvalidPartitions.code(),
                "count {count}"
            );
            assert_eq!(
                message(result),
                format!(
                    "The topic orders currently has 4 partition(s); {count} would not be an \
                     increase."
                )
            );
        }
    }

    /// An increase of a topic this facade created raises its declared width:
    /// the record is rewritten, and the next Metadata reports the new count.
    #[tokio::test]
    async fn an_increase_of_a_tracked_topic_raises_its_width() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        let record =
            topic_record::Record::new(None, serde_json::Map::new()).with_partitions(Some(4));
        topic_record::store(api.as_ref(), "orders", &record, None)
            .await
            .unwrap();
        let result = &handle(&f, &request(&[("orders", 12)]), None).await.results[0];
        assert_eq!(result.error_code, 0, "{:?}", message(result));
        let stored = api
            .kv_get(crate::offsets::NAMESPACE, &topic_record::key("orders"))
            .unwrap();
        assert_eq!(stored["partitions"], 12);
        assert!(
            api.configured().is_empty(),
            "a width is not a /configure option"
        );
    }

    /// A topic this facade did not create has no declared width to raise: the
    /// refusal names the knob and the two things that do work.
    #[tokio::test]
    async fn an_increase_of_an_untracked_topic_names_the_way_out() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        let result = &handle(&f, &request(&[("orders", 8)]), None).await.results[0];
        assert_eq!(result.error_code, ResponseError::InvalidPartitions.code());
        let m = message(result);
        assert!(m.contains("QUEEN_KAFKA_DEFAULT_PARTITIONS"), "{m}");
        assert!(m.contains("produce to the higher lanes directly"), "{m}");
        assert!(api.configured().is_empty());
    }

    /// Past the per-topic ceiling Metadata could not advertise is refused
    /// before anything is read.
    #[tokio::test]
    async fn an_increase_past_the_ceiling_is_refused() {
        let (f, _api) = facade_and_queen(&[("orders", 4)]);
        let result = &handle(&f, &request(&[("orders", 999_999)]), None)
            .await
            .results[0];
        assert_eq!(result.error_code, ResponseError::InvalidPartitions.code());
        assert!(message(result).contains("ceiling"), "{}", message(result));
    }

    /// An explicit placement on an increase is refused by name — the same
    /// sentence CreateTopics gives the same field.
    #[tokio::test]
    async fn assignments_on_an_increase_are_invalid_replica_assignment() {
        let (f, _) = facade_and_queen(&[("orders", 4)]);
        let req =
            CreatePartitionsRequest::default().with_topics(vec![CreatePartitionsTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("orders")))
                .with_count(8)
                .with_assignments(Some(vec![
                    CreatePartitionsAssignment::default().with_broker_ids(vec![0.into()])
                ]))]);
        let result = &handle(&f, &req, None).await.results[0];
        assert_eq!(
            result.error_code,
            ResponseError::InvalidReplicaAssignment.code()
        );
        assert!(message(result).contains("one logical broker"));
    }

    /// ...but a DECREASE that carries one is still INVALID_PARTITIONS, because
    /// that is the order the oracle applies the two checks in.
    #[tokio::test]
    async fn a_decrease_that_carries_an_assignment_is_still_invalid_partitions() {
        let (f, _) = facade_and_queen(&[("orders", 4)]);
        let req =
            CreatePartitionsRequest::default().with_topics(vec![CreatePartitionsTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("orders")))
                .with_count(2)
                .with_assignments(Some(vec![
                    CreatePartitionsAssignment::default().with_broker_ids(vec![0.into()])
                ]))]);
        assert_eq!(
            handle(&f, &req, None).await.results[0].error_code,
            ResponseError::InvalidPartitions.code()
        );
    }

    /// A topic nobody has, a reserved name and an illegal one are all the same
    /// one code — the rule `metadata::not_a_topic_here` exists to keep.
    #[tokio::test]
    async fn an_unknown_reserved_or_illegal_name_is_unknown_topic() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        let r = handle(
            &f,
            &request(&[("nope", 8), ("__consumer_offsets", 8), ("bad name", 8)]),
            None,
        )
        .await;
        for result in &r.results {
            assert_eq!(
                result.error_code,
                ResponseError::UnknownTopicOrPartition.code(),
                "{:?}",
                result.name
            );
            assert_eq!(result.error_message, None, "the oracle sends no message");
        }
        // Only the catalog was read: a reserved name buys no second call.
        assert_eq!(api.list_count(), 1);
    }

    /// `validate_only` writes nothing — here the topic is untracked, so it is
    /// the same refusal — and
    /// the response is still fully formed, which is what a client reads.
    #[tokio::test]
    async fn validate_only_changes_nothing_and_still_answers() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        let req = request(&[("orders", 8)]).with_validate_only(true);
        let result = &handle(&f, &req, None).await.results[0];
        assert_eq!(result.error_code, ResponseError::InvalidPartitions.code());
        assert!(!message(result).is_empty());
        assert!(api.configured().is_empty());
        assert!(api.deleted().is_empty());
    }

    /// A request naming no topic asks Queen nothing at all.
    #[tokio::test]
    async fn an_empty_request_reads_no_catalog() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        let r = handle(&f, &request(&[]), None).await;
        assert!(r.results.is_empty());
        assert_eq!(api.list_count(), 0);
    }

    /// A catalog that cannot be read is retriable and named — never a silent
    /// "no such topic", which would send an operator looking for a typo.
    #[tokio::test]
    async fn an_unreadable_catalog_is_retriable_and_authorization_is_not() {
        for (queen_error, kafka) in [
            (
                Error::Transport("connection refused".into()),
                ResponseError::RequestTimedOut,
            ),
            (
                Error::status(503, "shedding"),
                ResponseError::RequestTimedOut,
            ),
            (
                Error::status(403, "forbidden"),
                ResponseError::TopicAuthorizationFailed,
            ),
        ] {
            let (f, api) = facade_and_queen(&[("orders", 4)]);
            api.fail_list(queen_error.clone());
            let result = &handle(&f, &request(&[("orders", 8)]), None).await.results[0];
            assert_eq!(result.error_code, kafka.code(), "{queen_error}");
            assert!(!message(result).is_empty(), "{queen_error}");
        }
    }

    /// Every node answers the same: this is topic-addressed, not
    /// group-addressed, so there is no ownership question to gate on.
    #[tokio::test]
    async fn every_node_answers_the_same() {
        const THREE: [(i32, &str, u16); 3] = [
            (1, "kafka-1.example.com", 9092),
            (2, "kafka-2.example.com", 9092),
            (3, "kafka-3.example.com", 9092),
        ];
        let (one_node, _) = clustered(&[("orders", 4)], &THREE, 1);
        let (another, _) = clustered(&[("orders", 4)], &THREE, 2);
        let a = handle(&one_node, &request(&[("orders", 2)]), None).await;
        let b = handle(&another, &request(&[("orders", 2)]), None).await;
        assert_eq!(a.results, b.results);
    }

    /// The whole advertised window, encoded and decoded with the client half of
    /// the crate. Nothing in `0..=3` changes a field or a code, so the answer is
    /// the same at every one of them — which is the fact the table's ceiling
    /// rests on.
    #[tokio::test]
    async fn the_exchange_round_trips_at_every_advertised_version() {
        use bytes::BytesMut;

        let row =
            crate::versions::lookup(kafka_protocol::messages::ApiKey::CreatePartitions as i16)
                .expect("CreatePartitions is advertised");
        assert!(
            row.min >= CreatePartitionsRequest::VERSIONS.min
                && row.max <= CreatePartitionsRequest::VERSIONS.max
        );

        for version in row.min..=row.max {
            let (f, _) = facade_and_queen(&[("orders", 4)]);
            let mut wire = BytesMut::new();
            request(&[("orders", 2)])
                .encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode request v{version}: {e}"));
            let mut buf = wire.freeze();
            let decoded = CreatePartitionsRequest::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode request v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing request bytes");

            let resp = handle(&f, &decoded, None).await;
            let mut wire = BytesMut::new();
            resp.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode response v{version}: {e}"));
            let mut buf = wire.freeze();
            let back = CreatePartitionsResponse::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode response v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing response bytes");
            assert_eq!(
                back.results[0].error_code,
                ResponseError::InvalidPartitions.code(),
                "v{version}"
            );
            assert_eq!(
                message(&back.results[0]),
                "The topic orders currently has 4 partition(s); 2 would not be an increase.",
                "v{version}"
            );
        }
    }
}
