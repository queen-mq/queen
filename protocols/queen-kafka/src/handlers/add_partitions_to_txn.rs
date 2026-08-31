//! AddPartitionsToTxn (24) — the partitions a transaction is allowed to write.
//!
//! A transactional producer sends this before its first record to each
//! `(topic, partition)`, and Apache Kafka's coordinator writes the set into
//! `__transaction_state` so that it knows where to put markers at the end. This
//! facade writes no markers, so the set is worth something else here, and it is
//! worth exactly one thing: **it is what makes a produce's partition
//! meaningful**. A `Produce` for a partition that was never added is refused
//! INVALID_TXN_STATE, which is Kafka's own rule, and without a registered set
//! there would be nothing to check it against.
//!
//! It takes NO I/O. The set lives in [`crate::txn`] beside the stage, and the
//! only checks are the binding's `(producer_id, epoch)` and the topic NAME.
//!
//! ## Every refusal is per partition
//!
//! v0-v3 has per-partition error codes and no top-level one — the top-level
//! field arrives with the v4 batched form, which is KIP-890's
//! broker-to-broker verification request and is outside the advertised window
//! ([`crate::versions`]). So a whole-request refusal is replicated across every
//! partition, the same shape `handlers::offset_commit` uses for the cluster
//! guard and the same thing Apache Kafka does with a request-wide fault on an
//! API with no top-level code.
//!
//! ## The topic check is a NAME check, deliberately
//!
//! A topic that Metadata would not show — `__`-prefixed, or not a legal Kafka
//! name — is refused here with the same code and from the same helper as on
//! every other path ([`metadata::not_a_topic_here`]), so a topic that is
//! invisible on the read path cannot be written inside a transaction either.
//!
//! A topic that merely does not EXIST yet is not refused, and that is a
//! decision rather than an omission: the produce path auto-creates
//! (`auto.create.topics.enable` is what a Kafka broker does with a produce to
//! an absent topic), so refusing here would break a transactional producer
//! writing to a topic it is about to create — and it would cost a catalog read
//! on a request whose whole point is that it costs nothing.

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::add_partitions_to_txn_response::{
    AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
};
use kafka_protocol::messages::{AddPartitionsToTxnRequest, AddPartitionsToTxnResponse};

use crate::handlers::metadata;
use crate::idempotent;
use crate::txn::{self, Full};
use crate::Facade;

/// Handle one AddPartitionsToTxn. No I/O; see the module header.
pub fn handle(
    facade: &Facade,
    req: &AddPartitionsToTxnRequest,
    token: Option<&str>,
) -> AddPartitionsToTxnResponse {
    let Some(id) = idempotent::transactional_id(Some(req.v3_and_below_transactional_id.0.as_str()))
    else {
        // An empty id is not a transactional id anywhere in this facade
        // ([`crate::idempotent::transactional_id`]), and no binding was ever
        // made for one, so it is the same answer an unknown id gets.
        return refuse_all(req, txn::Fault::Unknown.code());
    };
    let tenant = facade.catalog.tenant_key(token);

    // The name check first, so a request that names one bad topic does not
    // register the others and then refuse: the whole set is decided before the
    // registry is touched.
    let mut wanted: Vec<(String, i32)> = Vec::new();
    let mut refusals: Vec<Vec<Option<ResponseError>>> = Vec::new();
    for topic in &req.v3_and_below_topics {
        let name = topic.name.0.as_str();
        let bad_name = metadata::not_a_topic_here(name);
        refusals.push(
            topic
                .partitions
                .iter()
                .map(|p| match bad_name {
                    Some(e) => Some(e),
                    // A negative index is not a lane; the width check itself is
                    // the produce path's, because the width is a catalog read.
                    None if *p < 0 => Some(ResponseError::UnknownTopicOrPartition),
                    None => {
                        wanted.push((name.to_string(), *p));
                        None
                    }
                })
                .collect(),
        );
    }

    let verdicts = match facade.txns.add_partitions(
        &tenant,
        id,
        req.v3_and_below_producer_id.0,
        req.v3_and_below_producer_epoch,
        &wanted,
    ) {
        Ok(v) => v,
        Err(fault) => {
            tracing::debug!(
                target: "kafka",
                transactional_id = %id,
                fault = ?fault,
                "AddPartitionsToTxn refused"
            );
            return refuse_all(req, fault.code());
        }
    };

    // Walk the two shapes back together: `verdicts` covers only the partitions
    // that passed the name check, in the order they were collected.
    let mut next = verdicts.into_iter();
    let topics = req
        .v3_and_below_topics
        .iter()
        .zip(refusals)
        .map(|(topic, row)| {
            let partitions = topic
                .partitions
                .iter()
                .zip(row)
                .map(|(p, refused)| {
                    let error = match refused {
                        Some(e) => Some(e),
                        None => match next.next() {
                            Some(Ok(())) => None,
                            // The partition cap. It has no `error_message`
                            // field to say so in below v4, so the cap names
                            // itself in a sampled log line instead.
                            Some(Err(Full::Partitions)) => {
                                tracing::warn!(
                                    target: "kafka",
                                    transactional_id = %id,
                                    max = txn::MAX_TXN_PARTITIONS,
                                    "a transaction asked for more partitions than \
                                     MAX_TXN_PARTITIONS; the rest are refused"
                                );
                                Some(ResponseError::InvalidTxnState)
                            }
                            // Unreachable: `add_partitions` answers only
                            // `Full::Partitions` per partition.
                            Some(Err(_)) | None => Some(ResponseError::UnknownServerError),
                        },
                    };
                    answer(*p, error)
                })
                .collect();
            AddPartitionsToTxnTopicResult::default()
                .with_name(topic.name.clone())
                .with_results_by_partition(partitions)
        })
        .collect();

    AddPartitionsToTxnResponse::default()
        .with_throttle_time_ms(0)
        .with_results_by_topic_v3_and_below(topics)
}

/// Answer every partition of the request with one error. See the module header.
fn refuse_all(req: &AddPartitionsToTxnRequest, error: ResponseError) -> AddPartitionsToTxnResponse {
    AddPartitionsToTxnResponse::default()
        .with_throttle_time_ms(0)
        .with_results_by_topic_v3_and_below(
            req.v3_and_below_topics
                .iter()
                .map(|topic| {
                    AddPartitionsToTxnTopicResult::default()
                        .with_name(topic.name.clone())
                        .with_results_by_partition(
                            topic
                                .partitions
                                .iter()
                                .map(|p| answer(*p, Some(error)))
                                .collect(),
                        )
                })
                .collect(),
        )
}

fn answer(partition: i32, error: Option<ResponseError>) -> AddPartitionsToTxnPartitionResult {
    AddPartitionsToTxnPartitionResult::default()
        .with_partition_index(partition)
        .with_partition_error_code(error.map_or(0, |e| e.code()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::testing::facade;
    use crate::txn::TxnState;
    use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
    use kafka_protocol::messages::{ProducerId, TopicName, TransactionalId};
    use kafka_protocol::protocol::StrBytes;
    use std::time::Duration;

    const PID: i64 = 7;

    fn request(
        id: &str,
        pid: i64,
        epoch: i16,
        topics: &[(&str, &[i32])],
    ) -> AddPartitionsToTxnRequest {
        AddPartitionsToTxnRequest::default()
            .with_v3_and_below_transactional_id(TransactionalId(StrBytes::from_string(
                id.to_string(),
            )))
            .with_v3_and_below_producer_id(ProducerId(pid))
            .with_v3_and_below_producer_epoch(epoch)
            .with_v3_and_below_topics(
                topics
                    .iter()
                    .map(|(name, partitions)| {
                        AddPartitionsToTxnTopic::default()
                            .with_name(TopicName(StrBytes::from_string(name.to_string())))
                            .with_partitions(partitions.to_vec())
                    })
                    .collect(),
            )
    }

    fn bound() -> Facade {
        let f = facade(&[("orders", 4)]);
        let tenant = f.catalog.tenant_key(f.token());
        f.txns
            .bind(&tenant, "tx", PID, 0, 100, 1, Duration::from_secs(60))
            .unwrap();
        f
    }

    fn codes(resp: &AddPartitionsToTxnResponse) -> Vec<i16> {
        resp.results_by_topic_v3_and_below
            .iter()
            .flat_map(|t| {
                t.results_by_partition
                    .iter()
                    .map(|p| p.partition_error_code)
            })
            .collect()
    }

    #[test]
    fn the_first_partitions_open_the_transaction() {
        let f = bound();
        let resp = handle(&f, &request("tx", PID, 0, &[("orders", &[0, 1])]), None);
        assert_eq!(codes(&resp), vec![0, 0]);
        let tenant = f.catalog.tenant_key(f.token());
        assert_eq!(
            f.txns
                .with(&tenant, "tx", PID, 0, |t| (t.state, t.partitions.len()))
                .unwrap(),
            (TxnState::Open, 2)
        );
    }

    /// The whole point of the set: a produce may only write a partition that is
    /// in it, so registering the same one twice must not grow it.
    #[test]
    fn a_partition_added_twice_is_registered_once() {
        let f = bound();
        handle(&f, &request("tx", PID, 0, &[("orders", &[0])]), None);
        let resp = handle(&f, &request("tx", PID, 0, &[("orders", &[0])]), None);
        assert_eq!(codes(&resp), vec![0]);
        let tenant = f.catalog.tenant_key(f.token());
        assert_eq!(
            f.txns
                .with(&tenant, "tx", PID, 0, |t| t.partitions.len())
                .unwrap(),
            1
        );
    }

    /// No binding on this facade — a restart, or a commit that reached a
    /// process which never held the stage. Fatal in the Java producer, and it
    /// has to be: the transaction genuinely cannot continue.
    #[test]
    fn an_unknown_transactional_id_refuses_every_partition() {
        let f = facade(&[("orders", 4)]);
        let resp = handle(&f, &request("tx", PID, 0, &[("orders", &[0, 1])]), None);
        assert_eq!(codes(&resp), vec![ResponseError::InvalidTxnState.code(); 2]);
    }

    /// THE zombie path, at this API: a second producer took the id, so the
    /// first one's epoch is below the bound one and every partition is fenced.
    #[test]
    fn a_fenced_producer_is_refused_on_every_partition() {
        let f = bound();
        let tenant = f.catalog.tenant_key(f.token());
        f.txns
            .bind(&tenant, "tx", PID, 1, 101, 2, Duration::from_secs(60))
            .unwrap();
        let resp = handle(&f, &request("tx", PID, 0, &[("orders", &[0, 1])]), None);
        assert_eq!(codes(&resp), vec![ResponseError::ProducerFenced.code(); 2]);
        // ...and an epoch this facade never granted is a different answer,
        // because it is a different mistake.
        let resp = handle(&f, &request("tx", PID, 9, &[("orders", &[0])]), None);
        assert_eq!(
            codes(&resp),
            vec![ResponseError::InvalidProducerEpoch.code()]
        );
        let resp = handle(&f, &request("tx", 999, 1, &[("orders", &[0])]), None);
        assert_eq!(
            codes(&resp),
            vec![ResponseError::InvalidProducerIdMapping.code()]
        );
    }

    #[test]
    fn a_topic_metadata_would_not_show_is_refused_per_partition() {
        let f = bound();
        let resp = handle(
            &f,
            &request(
                "tx",
                PID,
                0,
                &[("__consumer_offsets", &[0]), ("orders", &[0])],
            ),
            None,
        );
        assert_eq!(
            codes(&resp),
            vec![ResponseError::UnknownTopicOrPartition.code(), 0]
        );
        // ...and the good partition really was registered, so one bad topic
        // does not cost the others.
        let tenant = f.catalog.tenant_key(f.token());
        assert_eq!(
            f.txns
                .with(&tenant, "tx", PID, 0, |t| t.partitions.clone())
                .unwrap(),
            vec![("orders".to_string(), 0)]
        );
    }

    #[test]
    fn a_negative_partition_is_not_a_lane() {
        let f = bound();
        let resp = handle(&f, &request("tx", PID, 0, &[("orders", &[-1])]), None);
        assert_eq!(
            codes(&resp),
            vec![ResponseError::UnknownTopicOrPartition.code()]
        );
    }

    /// The cap refuses only the partitions past it, and names itself in a log
    /// line because this API has no `error_message` field below v4.
    #[test]
    fn the_partition_cap_refuses_only_what_is_past_it() {
        let f = bound();
        let all: Vec<i32> = (0..txn::MAX_TXN_PARTITIONS as i32 + 2).collect();
        let resp = handle(&f, &request("tx", PID, 0, &[("orders", &all)]), None);
        let codes = codes(&resp);
        assert!(codes[..txn::MAX_TXN_PARTITIONS].iter().all(|c| *c == 0));
        assert_eq!(
            codes[txn::MAX_TXN_PARTITIONS],
            ResponseError::InvalidTxnState.code()
        );
    }

    /// Every advertised version encodes and decodes cleanly, both ways. The
    /// per-version fixture uses its OWN transactional id, because a fixture
    /// reusing one across versions would meet the stage the previous version
    /// left and answer a different error.
    #[test]
    fn the_exchange_round_trips_at_every_advertised_version() {
        use bytes::BytesMut;
        use kafka_protocol::protocol::{Decodable, Encodable, Message};

        let f = facade(&[("orders", 4)]);
        let row =
            crate::versions::lookup(kafka_protocol::messages::ApiKey::AddPartitionsToTxn as i16)
                .expect("AddPartitionsToTxn is advertised");
        assert!(
            row.min >= AddPartitionsToTxnRequest::VERSIONS.min
                && row.max <= AddPartitionsToTxnRequest::VERSIONS.max
        );
        let tenant = f.catalog.tenant_key(f.token());

        for version in row.min..=row.max {
            let id = format!("tx-v{version}");
            f.txns
                .bind(&tenant, &id, PID, 0, 100, 1, Duration::from_secs(60))
                .unwrap();
            let mut wire = BytesMut::new();
            request(&id, PID, 0, &[("orders", &[0])])
                .encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode request v{version}: {e}"));
            let mut buf = wire.freeze();
            let decoded = AddPartitionsToTxnRequest::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode request v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing request bytes");

            let resp = handle(&f, &decoded, None);
            let mut wire = BytesMut::new();
            resp.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode response v{version}: {e}"));
            let mut buf = wire.freeze();
            let back = AddPartitionsToTxnResponse::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode response v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing response bytes");
            assert_eq!(codes(&back), vec![0], "v{version}");
        }
    }
}
