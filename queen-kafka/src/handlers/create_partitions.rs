//! CreatePartitions (key 37), v0-v3 — `kafka-topics.sh --alter --partitions N`.
//!
//! ## This is an advertised REFUSAL, and two thirds of it are Apache Kafka's
//!
//! Queen declares no width per queue. `POST /api/v1/configure` takes nineteen
//! option keys and `partitions` is not one of them, `queen.queues` has no such
//! column, and a lane comes into existence only when something is pushed to it
//! (`003_log_push.sql`). The width this facade advertises is
//! `max(live lanes, QUEEN_KAFKA_DEFAULT_PARTITIONS)`
//! ([`metadata::advertised_partitions`]) and the second half of that is a
//! BROKER START-UP knob, not a per-topic number. So there is no write that
//! widens one topic, and this API cannot be implemented — only answered.
//!
//! Advertising a refusal is normally against `versions::ADVERTISED`'s own rule.
//! It is right here because two of the three answers are not refusals of a
//! capability at all, they are the oracle's own answers, byte for byte:
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
//! Only the third case — an increase — is a genuine capability gap, and it gets
//! a sentence of the facade's own that says what to do instead. The alternative,
//! no row in the table at all, would give `kafka-topics.sh --alter --partitions`
//! an `UnsupportedVersionException`, which reads as *"upgrade your broker"* —
//! the wrong diagnosis in all three cases, and wrong for the commonest one of
//! all: a provisioner declaring 12 partitions against a facade whose default is
//! 1024 is a DECREASE, where this answer is indistinguishable from a real
//! broker's.
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
//! ## Nothing is written, ever
//!
//! No Queen call but the catalog LIST, which asks "do you have this topic",
//! not "may I write over it" — so a cached list up to one TTL old costs a
//! client a retry at worst, the same argument `handlers::describe_configs`
//! makes. `validate_only` therefore changes nothing about the answer, and is
//! honoured by construction rather than by a branch.
//!
//! `timeout_ms` is not acted on, for the same reason `handlers::create_topics`'
//! is not: there is no asynchronous work here to bound.
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
use crate::{queen, throttle, Facade};

pub async fn handle(
    facade: &Facade,
    req: &CreatePartitionsRequest,
    token: Option<&str>,
) -> CreatePartitionsResponse {
    let mut throttle_ms: Option<i32> = None;

    // The catalog, at most ONCE for the request, and not at all for a request
    // that names no topic. The live lane count is half of the width every
    // answer below is computed against.
    let catalog = if req.topics.is_empty() {
        Ok(HashMap::new())
    } else {
        match facade.catalog.list(token).await {
            Ok(queues) => Ok(queues
                .iter()
                .map(|q| (q.name.clone(), q.partitions))
                .collect::<HashMap<String, i64>>()),
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

    let results = req
        .topics
        .iter()
        .map(|t| one(facade, t, &catalog))
        .collect();
    CreatePartitionsResponse::default()
        .with_throttle_time_ms(throttle_ms.unwrap_or(0))
        .with_results(results)
}

/// One requested topic's answer.
fn one(
    facade: &Facade,
    t: &CreatePartitionsTopic,
    catalog: &Result<HashMap<String, i64>, (ResponseError, String)>,
) -> CreatePartitionsTopicResult {
    let name = t.name.0.as_str();

    // The name rule every non-Metadata API applies, in the one code they may
    // answer: a `__`-prefixed or illegal name is a topic this facade does not
    // have. No message, because the oracle sends none for this code either.
    if let Some(e) = metadata::not_a_topic_here(name) {
        return answer(name, Some(e), None);
    }
    let live = match catalog {
        Ok(live) => live,
        Err((e, why)) => return answer(name, Some(*e), Some(why.clone())),
    };
    let Some(lanes) = live.get(name) else {
        return answer(name, Some(ResponseError::UnknownTopicOrPartition), None);
    };

    let current = metadata::advertised_partitions(*lanes, facade.default_partitions);
    let wanted = t.count;

    // The oracle's own two sentences, in the oracle's own order. Recorded off
    // apache/kafka:3.9.1; see the module header.
    if wanted == current {
        return answer(
            name,
            Some(ResponseError::InvalidPartitions),
            Some(format!("Topic already has {current} partition(s).")),
        );
    }
    if wanted < current {
        return answer(
            name,
            Some(ResponseError::InvalidPartitions),
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
        return answer(
            name,
            Some(ResponseError::InvalidReplicaAssignment),
            Some(
                "this facade is one logical broker and places no partition on any node, so a \
                 manual replica assignment cannot be honoured. Omit `assignments`"
                    .to_string(),
            ),
        );
    }

    answer(
        name,
        Some(ResponseError::InvalidPartitions),
        Some(format!(
            "Queen declares no width per queue: a partition exists once something has been \
             written to it, and the width this facade advertises is max(live lanes, \
             QUEEN_KAFKA_DEFAULT_PARTITIONS), which is {current} for {name}. That second number \
             is a broker start-up setting, not a per-topic one, so a topic cannot be widened \
             through this API. Raise QUEEN_KAFKA_DEFAULT_PARTITIONS (it applies to every topic), \
             or produce to the higher lanes directly."
        )),
    )
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
            "Queen refused this credential".to_string(),
        ),
        other => (
            ResponseError::RequestTimedOut,
            format!("the queue list could not be read: {other}"),
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

    /// The one genuine capability gap, and the one sentence that is the
    /// facade's own: it must name the broker knob, because that is the only
    /// thing the operator can actually do.
    #[tokio::test]
    async fn an_increase_names_the_broker_knob() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        let result = &handle(&f, &request(&[("orders", 999_999)]), None)
            .await
            .results[0];
        assert_eq!(result.error_code, ResponseError::InvalidPartitions.code());
        let m = message(result);
        assert!(m.contains("QUEEN_KAFKA_DEFAULT_PARTITIONS"), "{m}");
        assert!(m.contains("produce to the higher lanes directly"), "{m}");
        assert!(api.configured().is_empty());
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

    /// `validate_only` changes nothing, because nothing is ever written — and
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
