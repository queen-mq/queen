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
        let sizes: HashMap<String, i64> = match facade.queen.partition_bytes(&name, token).await {
            Ok(v) => v.into_iter().collect(),
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
                                .with_partition_size(
                                    sizes.get(&p.to_string()).copied().unwrap_or(0),
                                )
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

    /// Over HTTP the facade has no log directory to describe.
    #[tokio::test]
    async fn a_facade_outside_raft_describes_no_directory() {
        let (f, _api) = facade_and_queen(&[("orders", 4)]);
        let resp = handle(&f, &DescribeLogDirsRequest::default(), None).await;
        assert!(resp.results.is_empty());
        assert_eq!(resp.error_code, 0);
    }
}
