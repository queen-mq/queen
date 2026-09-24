//! DescribeLogDirs (key 35), v1-v4 — which partitions a broker holds on disk,
//! and how big they are. `kafka-log-dirs.sh`, a UI's size column, and a
//! readiness check that waits until every replica's log exists (kload's).
//!
//! ## Only a facade inside a raft broker has log directories
//!
//! Running in-process in a raft broker ([`crate::Facade::raft`]), this node's
//! raft data directory holds a copy of every partition — every voter applies
//! every entry — so the answer is ONE directory, that one, listing every
//! partition of every topic asked about, each with the bytes this node counts
//! for it (`retainedBytes` of `GET /api/v1/resources/queues/{queue}`). A
//! partition nothing has been written to yet holds 0 bytes, which is true.
//!
//! Over HTTP the facade knows nothing of the broker's storage — a Postgres
//! broker has segments, not log directories — and answers with no directory
//! at all: a broker with no log dirs, rather than an invented path.
//!
//! `offset_lag` is 0 and `is_future_key` false: there is no replica movement
//! between directories to report.

use std::collections::HashMap;

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::describe_log_dirs_response::{
    DescribeLogDirsPartition, DescribeLogDirsResult, DescribeLogDirsTopic,
};
use kafka_protocol::messages::{DescribeLogDirsRequest, DescribeLogDirsResponse, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::handlers::metadata;
use crate::{queen, throttle, Facade};

/// Handle one DescribeLogDirs.
pub async fn handle(
    facade: &Facade,
    req: &DescribeLogDirsRequest,
    token: Option<&str>,
) -> DescribeLogDirsResponse {
    let Some(raft) = facade.raft.as_ref() else {
        return DescribeLogDirsResponse::default().with_throttle_time_ms(0);
    };
    let queues = match facade.catalog.list(token).await {
        Ok(q) => q,
        Err(e) => {
            tracing::warn!(target: "kafka", error = %e, "DescribeLogDirs cannot read the queue list");
            return DescribeLogDirsResponse::default()
                .with_throttle_time_ms(throttle::for_error(&e).unwrap_or(0))
                .with_error_code(failed(&e).code());
        }
    };

    // Every topic's advertised width — the partitions Metadata says exist.
    let widths: HashMap<&str, i32> = queues
        .iter()
        .filter(|q| metadata::not_a_topic_here(&q.name).is_none())
        .map(|q| {
            (
                q.name.as_str(),
                metadata::advertised_partitions(
                    q.partitions,
                    q.floor.unwrap_or(facade.default_partitions),
                    facade.max_partitions,
                ),
            )
        })
        .collect();
    // Null topics is "every topic"; otherwise the named partitions that exist.
    let mut wanted: Vec<(String, Vec<i32>)> = match &req.topics {
        None => widths
            .iter()
            .map(|(name, width)| (name.to_string(), (0..*width).collect()))
            .collect(),
        Some(topics) => topics
            .iter()
            .filter_map(|t| {
                let width = *widths.get(t.topic.0.as_str())?;
                Some((
                    t.topic.0.to_string(),
                    t.partitions
                        .iter()
                        .copied()
                        .filter(|p| (0..width).contains(p))
                        .collect(),
                ))
            })
            .collect(),
    };
    wanted.sort();

    let mut topics = Vec::with_capacity(wanted.len());
    for (name, partitions) in wanted {
        // One queue read per topic. A read that fails reports sizes of 0 for
        // that topic rather than dropping it: its partitions ARE on this
        // node's disk, and a listing without them would read as missing
        // replicas.
        // Keyed by the Kafka partition index, parsed once per answer: a lookup
        // per partition by a formatted `String` was an allocation per partition
        // of a topic that can be a million wide.
        let sizes: HashMap<i32, i64> = match facade.queen.partition_bytes(&name, token).await {
            Ok(v) => v
                .into_iter()
                .filter_map(|(lane, bytes)| Some((kafka_index(&lane)?, bytes)))
                .collect(),
            Err(e) => {
                tracing::debug!(target: "kafka", topic = %name, error = %e, "partition sizes unavailable");
                HashMap::new()
            }
        };
        topics.push(
            DescribeLogDirsTopic::default()
                .with_name(TopicName(StrBytes::from_string(name)))
                .with_partitions(
                    partitions
                        .into_iter()
                        .map(|p| {
                            DescribeLogDirsPartition::default()
                                .with_partition_index(p)
                                .with_partition_size(sizes.get(&p).copied().unwrap_or(0))
                                .with_offset_lag(0)
                                .with_is_future_key(false)
                        })
                        .collect(),
                ),
        );
    }
    DescribeLogDirsResponse::default()
        .with_throttle_time_ms(0)
        .with_results(vec![DescribeLogDirsResult::default()
            .with_error_code(0)
            .with_log_dir(StrBytes::from_string(raft.log_dir.clone()))
            .with_topics(topics)
            // v4: the volume's size is not measured here.
            .with_total_bytes(-1)
            .with_usable_bytes(-1)])
}

/// The Kafka partition index a Queen lane NAME stands for, or `None` for a
/// lane Kafka cannot address. Canonical decimal only — `7`, never `07` or
/// `+7` — because partition n is the lane named `n.to_string()` and nothing
/// else ([`crate::handlers::metadata`]).
fn kafka_index(lane: &str) -> Option<i32> {
    let canonical = !lane.is_empty()
        && lane.bytes().all(|b| b.is_ascii_digit())
        && (lane == "0" || !lane.starts_with('0'));
    if canonical {
        lane.parse().ok()
    } else {
        None
    }
}

/// What a Queen failure on the catalog read becomes: authorization by name,
/// everything else a retriable server error.
fn failed(e: &queen::Error) -> ResponseError {
    match e {
        queen::Error::Status {
            code: 401 | 403, ..
        } => ResponseError::ClusterAuthorizationFailed,
        _ => ResponseError::UnknownServerError,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::testing::facade_and_queen;
    use kafka_protocol::messages::describe_log_dirs_request::DescribableLogDirTopic;

    fn raft(f: crate::Facade) -> crate::Facade {
        f.with_raft(Some(crate::RaftBroker {
            voters: 3,
            log_dir: "/var/lib/queen/raft".into(),
        }))
    }

    /// Inside a raft broker: one directory, every partition of every topic.
    #[tokio::test]
    async fn a_raft_node_lists_every_partition_in_its_one_directory() {
        let (f, _api) = facade_and_queen(&[("orders", 4), ("clicks", 2)]);
        let f = raft(f);
        // Null topics: every topic, which is what kadm's DescribeAllLogDirs sends.
        let req = DescribeLogDirsRequest::default().with_topics(None);
        let resp = handle(&f, &req, None).await;
        assert_eq!(resp.results.len(), 1);
        let dir = &resp.results[0];
        assert_eq!(dir.error_code, 0);
        assert_eq!(dir.log_dir.as_str(), "/var/lib/queen/raft");
        let count: usize = dir.topics.iter().map(|t| t.partitions.len()).sum();
        // Both fixtures are at the test facade's default width of four.
        assert_eq!(count, 4 + 4, "{:?}", dir.topics);
    }

    /// A named topic answers only the partitions asked for that exist.
    #[tokio::test]
    async fn named_partitions_are_filtered_to_the_ones_that_exist() {
        let (f, _api) = facade_and_queen(&[("orders", 4)]);
        let f = raft(f);
        let req = DescribeLogDirsRequest::default().with_topics(Some(vec![
            DescribableLogDirTopic::default()
                .with_topic(TopicName(StrBytes::from_static_str("orders")))
                .with_partitions(vec![0, 3, 7]),
            DescribableLogDirTopic::default()
                .with_topic(TopicName(StrBytes::from_static_str("nope")))
                .with_partitions(vec![0]),
        ]));
        let resp = handle(&f, &req, None).await;
        let topics = &resp.results[0].topics;
        assert_eq!(topics.len(), 1);
        let got: Vec<i32> = topics[0]
            .partitions
            .iter()
            .map(|p| p.partition_index)
            .collect();
        assert_eq!(got, [0, 3]);
    }

    /// Each partition carries the bytes its lane holds, found by INDEX: the
    /// lane named `3` is partition 3, and a lane Kafka cannot address — `07`,
    /// `eu-west` — sizes nothing, rather than `07` being read as 7.
    #[tokio::test]
    async fn each_partition_reports_the_bytes_of_the_lane_of_its_index() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        let f = raft(f);
        api.partition_sizes.lock().unwrap().insert(
            "orders".into(),
            vec![
                ("0".into(), 10),
                ("3".into(), 30),
                ("07".into(), 99),
                ("eu-west".into(), 5),
            ],
        );
        let resp = handle(
            &f,
            &DescribeLogDirsRequest::default().with_topics(None),
            None,
        )
        .await;
        let sizes: Vec<(i32, i64)> = resp.results[0].topics[0]
            .partitions
            .iter()
            .map(|p| (p.partition_index, p.partition_size))
            .collect();
        assert_eq!(sizes, [(0, 10), (1, 0), (2, 0), (3, 30)]);
    }

    #[test]
    fn only_a_canonical_decimal_lane_is_a_kafka_partition() {
        assert_eq!(kafka_index("0"), Some(0));
        assert_eq!(kafka_index("499999"), Some(499_999));
        for not in ["", "00", "07", "+7", "-1", "7a", "eu-west", "2147483648"] {
            assert_eq!(kafka_index(not), None, "{not}");
        }
    }

    /// A topic at the benchmark's width is described in one pass: every
    /// partition, each once, without one queue read per partition.
    #[tokio::test]
    async fn a_half_million_partition_topic_is_described_in_one_pass() {
        let (f, api) = facade_and_queen(&[("wide", 500_000)]);
        let f = raft(f);
        let resp = handle(
            &f,
            &DescribeLogDirsRequest::default().with_topics(None),
            None,
        )
        .await;
        let partitions = &resp.results[0].topics[0].partitions;
        assert_eq!(partitions.len(), 500_000);
        assert!(partitions
            .iter()
            .enumerate()
            .all(|(i, p)| p.partition_index == i as i32));
        assert_eq!(api.fetches.lock().unwrap().len(), 0);
    }

    /// Over HTTP the facade has no log directory to describe.
    #[tokio::test]
    async fn a_facade_outside_raft_describes_no_directory() {
        let (f, _api) = facade_and_queen(&[("orders", 4)]);
        let resp = handle(&f, &DescribeLogDirsRequest::default(), None).await;
        assert!(resp.results.is_empty());
        assert_eq!(resp.error_code, 0);
    }
}
