//! The API versions this facade answers. The windows are the first facade's,
//! measured against librdkafka, Java 3.9/4.3, franz-go, sarama and kafkajs; the
//! transaction, config-alter, ACL and log-dir keys are not here (phase 2).
//!
//! Capped where a later version needs state this layer does not keep: Fetch at
//! 6 (no fetch sessions), JoinGroup at 4, Sync/Heartbeat/Leave at 2 and
//! OffsetCommit at 6 (no static membership), FindCoordinator at 3 (one key),
//! Metadata at 9 (topics by name, not id), SaslHandshake at 1 (framed tokens).

use kafka_protocol::messages::ApiKey;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Api {
    pub key: ApiKey,
    pub min: i16,
    pub max: i16,
}

const fn api(key: ApiKey, min: i16, max: i16) -> Api {
    Api { key, min, max }
}

pub const ADVERTISED: &[Api] = &[
    api(ApiKey::Produce, 3, 9),
    api(ApiKey::Fetch, 4, 6),
    api(ApiKey::ListOffsets, 1, 5),
    api(ApiKey::Metadata, 0, 9),
    api(ApiKey::OffsetCommit, 2, 6),
    api(ApiKey::OffsetFetch, 1, 7),
    api(ApiKey::FindCoordinator, 0, 3),
    api(ApiKey::JoinGroup, 0, 4),
    api(ApiKey::Heartbeat, 0, 2),
    api(ApiKey::LeaveGroup, 0, 2),
    api(ApiKey::SyncGroup, 0, 2),
    api(ApiKey::DescribeGroups, 0, 3),
    api(ApiKey::ListGroups, 0, 4),
    api(ApiKey::SaslHandshake, 1, 1),
    api(ApiKey::ApiVersions, 0, 3),
    api(ApiKey::CreateTopics, 2, 6),
    api(ApiKey::DeleteTopics, 1, 5),
    api(ApiKey::DeleteGroups, 0, 2),
    api(ApiKey::InitProducerId, 0, 4),
    api(ApiKey::DescribeConfigs, 1, 4),
    api(ApiKey::CreatePartitions, 0, 3),
    api(ApiKey::SaslAuthenticate, 0, 1),
];

pub fn lookup(api_key: i16) -> Option<&'static Api> {
    ADVERTISED.iter().find(|a| a.key as i16 == api_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_window_is_inside_the_schema_and_listed_once() {
        for (i, a) in ADVERTISED.iter().enumerate() {
            let schema = a.key.valid_versions();
            assert!(
                a.min <= a.max && a.min >= schema.min && a.max <= schema.max,
                "{:?}",
                a.key
            );
            assert!(
                !ADVERTISED[..i].iter().any(|b| b.key == a.key),
                "{:?} twice",
                a.key
            );
        }
    }
}
