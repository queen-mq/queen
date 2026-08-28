//! THE advertised-versions table — the compatibility contract in one place.
//!
//! Every Kafka client starts a connection with ApiVersions and then speaks only
//! what the broker answered with, so this table is simultaneously:
//!
//!   * the body of the ApiVersions response (`handlers::api_versions`), and
//!   * the gate every incoming request passes through (`conn::dispatch`).
//!
//! Those two must never drift: a version advertised but not handled answers
//! garbage, and a version handled but not advertised is dead code no client will
//! ever reach. They cannot drift here because there is one table and both read
//! it. Each later milestone of PLAN_QUEEN_KAFKA.md adds its rows to `ADVERTISED`
//! and its arm to the dispatch match — nothing else changes.

use kafka_protocol::messages::ApiKey;

/// One advertised API: the key and the inclusive version window we implement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Api {
    pub key: ApiKey,
    pub min: i16,
    pub max: i16,
}

/// The table. M0 and M1: negotiate, then describe the cluster. A client can
/// bootstrap and list topics; the data APIs arrive with M2 and M3, and until
/// then a produce or a fetch is a clean close and a log line, not a hang.
///
/// `max = 3` for ApiVersions, not the 4 the `kafka-protocol` schema knows: 3 is
/// what every client in the M6 matrix (franz-go, librdkafka, kafkajs, Java)
/// negotiates against a 3.x broker, and it is also what keeps the v0-fallback
/// quirk in `handlers::api_versions` on a path a real client can reach — a
/// client that asks for 4 exercises it.
///
/// `max = 9` for Metadata, where the schema goes to 13. v9 is the last version a
/// topic can only be named by NAME; v10 adds topic ids, and from there a client
/// may address a topic by a UUID this facade has no registry to resolve — it
/// would have to answer every such request "unknown" while the topic sits right
/// there. Stopping at 9 is not a downgrade for anyone: v9 is already the
/// flexible encoding, it carries every field a client needs (cluster id,
/// controller, leader epoch, offline replicas), and ApiVersions makes every
/// client negotiate down to it without noticing.
pub const ADVERTISED: &[Api] = &[
    Api {
        key: ApiKey::ApiVersions,
        min: 0,
        max: 3,
    },
    Api {
        key: ApiKey::Metadata,
        min: 0,
        max: 9,
    },
];

/// Where an incoming `(api_key, api_version)` pair lands against the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Support {
    /// Advertised, and the version is inside the window: dispatch it.
    Advertised(ApiKey),
    /// Advertised key, version outside the window. The client either ignored
    /// the ApiVersions answer or is bootstrapping and has not asked yet.
    UnsupportedVersion(ApiKey),
    /// Not in the table — an API this build does not offer, or not a Kafka API
    /// key at all.
    UnknownApi,
}

/// The advertised row for `api_key`, if there is one.
pub fn lookup(api_key: i16) -> Option<&'static Api> {
    ADVERTISED.iter().find(|a| a.key as i16 == api_key)
}

/// Classify one request's key and version against the table.
pub fn classify(api_key: i16, api_version: i16) -> Support {
    match lookup(api_key) {
        Some(a) if api_version >= a.min && api_version <= a.max => Support::Advertised(a.key),
        Some(a) => Support::UnsupportedVersion(a.key),
        None => Support::UnknownApi,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::protocol::Message;

    #[test]
    fn table_has_no_duplicate_keys_and_no_empty_windows() {
        for (i, a) in ADVERTISED.iter().enumerate() {
            assert!(a.min <= a.max, "{:?} advertises an empty window", a.key);
            assert!(a.min >= 0, "{:?} advertises a negative min", a.key);
            assert!(
                !ADVERTISED[..i].iter().any(|b| b.key == a.key),
                "{:?} appears twice in the table",
                a.key
            );
        }
    }

    /// The window we advertise has to be a window `kafka-protocol` can actually
    /// encode and decode, or we promise a client a version that panics or bails
    /// the moment it is used. Checked for the whole table, against the crate's
    /// own schema range for each key, so a row added later cannot skip it.
    #[test]
    fn advertised_windows_are_inside_the_schema() {
        for a in ADVERTISED {
            let schema = a.key.valid_versions();
            assert!(
                a.min >= schema.min && a.max <= schema.max,
                "{:?} advertises {}..={} but the schema is {}..={}",
                a.key,
                a.min,
                a.max,
                schema.min,
                schema.max
            );
        }
        // The pin the loop above cannot make: ApiVersions stops one below the
        // schema on purpose (see ADVERTISED), and that is what keeps the
        // v0-fallback quirk reachable.
        let api_versions = lookup(ApiKey::ApiVersions as i16).unwrap();
        assert!(api_versions.max < kafka_protocol::messages::ApiVersionsRequest::VERSIONS.max);
    }

    #[test]
    fn classify_api_versions() {
        let k = ApiKey::ApiVersions as i16;
        assert_eq!(classify(k, 0), Support::Advertised(ApiKey::ApiVersions));
        assert_eq!(classify(k, 3), Support::Advertised(ApiKey::ApiVersions));
        assert_eq!(
            classify(k, 4),
            Support::UnsupportedVersion(ApiKey::ApiVersions)
        );
        assert_eq!(
            classify(k, -1),
            Support::UnsupportedVersion(ApiKey::ApiVersions)
        );
    }

    #[test]
    fn classify_metadata() {
        let k = ApiKey::Metadata as i16;
        assert_eq!(classify(k, 0), Support::Advertised(ApiKey::Metadata));
        assert_eq!(classify(k, 9), Support::Advertised(ApiKey::Metadata));
        // Above the window: a client that ignored ApiVersions, or one built
        // against a broker that speaks topic ids.
        assert_eq!(
            classify(k, 10),
            Support::UnsupportedVersion(ApiKey::Metadata)
        );
    }

    #[test]
    fn classify_rejects_everything_else() {
        // A real Kafka API this build does not offer yet (M2)...
        assert_eq!(classify(ApiKey::Produce as i16, 0), Support::UnknownApi);
        assert_eq!(classify(ApiKey::Fetch as i16, 0), Support::UnknownApi);
        // ...and something that is not a Kafka API key at all.
        assert_eq!(classify(31_000, 0), Support::UnknownApi);
        assert_eq!(classify(-7, 0), Support::UnknownApi);
    }
}
