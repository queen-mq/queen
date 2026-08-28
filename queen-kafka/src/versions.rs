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

/// The table. M0 and M1: negotiate, then describe the cluster. M2 adds the
/// write path, M3 the read path. A client can bootstrap, list topics, produce,
/// find where a topic begins and ends, and consume it — everything short of
/// doing it as a GROUP, which is M4.
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
///
/// `3..=9` for Produce, where the schema goes to 13. The floor is not a choice:
/// v3 is the first version whose records are RecordBatch v2, and v0-v2 carry the
/// legacy message sets `kafka-protocol` does not decode and no client from the
/// last decade sends. The ceiling is v9, the flexible encoding, and it stops
/// exactly where the semantics stop being ones this facade can honour — v10
/// adds the `current_leader` / `node_endpoints` pair that hands a client a
/// different broker to retry against after a leader change, which is a
/// conversation with no meaning here (one broker, no elections), and v13
/// addresses topics by UUID, which the facade has no registry to resolve. Every
/// field of v3..=v9 is either answered or deliberately -1; nothing in the window
/// is ignored silently.
///
/// `4..=6` for Fetch, where the schema goes to 18 and its own floor is 4. The
/// ceiling is the load-bearing one and it is PLAN_QUEEN_KAFKA.md's: v7
/// introduces fetch SESSIONS (KIP-227), where a client registers an incremental
/// session and thereafter sends only the partitions that CHANGED, with the
/// broker holding the rest of the assignment as per-connection state. This
/// facade keeps no durable state by design and is explicitly allowed to restart
/// like a broker, so capping at v6 is what makes that whole class of state — and
/// the session-epoch bookkeeping around it — not exist: `session_id`,
/// `session_epoch` and `forgotten_topics_data` are v7 fields, and a client that
/// cannot ask for a session cannot be handed a broken one.
///
/// THE GROUP APIS (M4) share one ceiling and it is a deliberate one: every row
/// below stops ONE VERSION BELOW where `group_instance_id` appears, because that
/// field is STATIC MEMBERSHIP (KIP-345) and static membership is out of scope
/// (`crate::coordinator`). A static member is one that survives a restart
/// WITHOUT triggering a rebalance, which means the coordinator must hold its
/// identity across the session timeout and fence a second instance claiming it —
/// a second liveness model layered on the one this facade implements. Ignoring
/// the field instead of capping the version would be the worst of both: a client
/// configured for static membership would negotiate a version that carries it,
/// send it, and get ordinary dynamic behaviour back, which shows up as
/// unexplained rebalances on exactly the deployments that configured static
/// membership to avoid them. So:
///
///   * `0..=4` for JoinGroup, where `group_instance_id` is v5. v4 is also where
///     MEMBER_ID_REQUIRED lands (KIP-394), and that IS implemented — it is the
///     round trip that stops a client which gives up between join and sync from
///     leaving a member behind on every retry.
///   * `0..=2` for SyncGroup, Heartbeat and LeaveGroup, where it is v3. For
///     LeaveGroup v3 is also where one request may remove SEVERAL members at
///     once; below it the request is one member, which is the shape the
///     coordinator has.
///   * `2..=6` for OffsetCommit, where it is v7. The floor is the schema's own:
///     v0 and v1 are the ZooKeeper-era offset store and no client from the last
///     decade sends them.
///   * `1..=7` for OffsetFetch, which has no `group_instance_id` at all — its
///     ceiling is the OTHER boundary, v8, where one request fetches offsets for
///     SEVERAL GROUPS and the response changes shape to match. v7's
///     `require_stable` is answered honestly rather than ignored: it asks the
///     broker to withhold offsets belonging to an open transaction, and there
///     are none here (`handlers::produce` refuses every shape of transaction),
///     so every offset this facade returns is stable by construction.
///
/// THE SASL APIS (M5) are advertised whether or not `QUEEN_KAFKA_SASL` is on,
/// and that is deliberate rather than an oversight. Apache Kafka advertises
/// them on every listener too, and the reason is diagnostic: a client
/// configured for `SASL_PLAINTEXT` against a listener with no SASL sends a
/// SaslHandshake, and a broker that did not advertise the API would close the
/// connection with no explanation, while one that does answers
/// ILLEGAL_SASL_STATE and says which side is misconfigured
/// (`handlers::sasl_handshake`). The gate that matters is not the table, it is
/// `conn::dispatch`, which refuses everything ELSE until a connection has
/// authenticated.
///
///   * `0..=1` for SaslHandshake. The two versions are the two SASL FLOWS, and
///     both are implemented: after v0 the tokens are raw bytes in the ordinary
///     frames, after v1 they are SaslAuthenticate requests.
///   * `0..=1` for SaslAuthenticate, where the schema goes to 2. v1 is where
///     `session_lifetime_ms` appears (KIP-368, re-authentication) and it is
///     answered honestly — 0, "this credential does not expire on me" — which
///     is what keeps every client from re-authenticating on a timer this facade
///     does not run. v2 is the flexible encoding and adds no field; a client
///     negotiates down to v1 without noticing.
///
/// `0..=3` for FindCoordinator, where the schema goes to 6. v4 is the batched
/// form — a list of coordinator keys, answered with a list — which exists so a
/// client can resolve many groups in one call against a cluster where they live
/// on different brokers. Here they never do: the answer is this process, for
/// every key, always (`handlers::find_coordinator`). v3 is the flexible encoding
/// and the last of the single-key form, so it is exactly the surface that is
/// true.
///
/// `1..=5` for ListOffsets, where the schema goes to 10 and its own floor is 1.
/// v6 is the flexible encoding and v7 adds the MAX_TIMESTAMP sentinel (-3),
/// which asks for the offset of the record with the highest timestamp — a
/// time-index question this facade answers for no version (see
/// `handlers::list_offsets`), so advertising the version that makes it askable
/// would be advertising a refusal. v5 is the last version whose whole surface is
/// the two watermark sentinels, which is exactly the surface Queen answers
/// exactly.
pub const ADVERTISED: &[Api] = &[
    Api {
        key: ApiKey::Produce,
        min: 3,
        max: 9,
    },
    Api {
        key: ApiKey::Fetch,
        min: 4,
        max: 6,
    },
    Api {
        key: ApiKey::ListOffsets,
        min: 1,
        max: 5,
    },
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
    Api {
        key: ApiKey::OffsetCommit,
        min: 2,
        max: 6,
    },
    Api {
        key: ApiKey::OffsetFetch,
        min: 1,
        max: 7,
    },
    Api {
        key: ApiKey::FindCoordinator,
        min: 0,
        max: 3,
    },
    Api {
        key: ApiKey::JoinGroup,
        min: 0,
        max: 4,
    },
    Api {
        key: ApiKey::Heartbeat,
        min: 0,
        max: 2,
    },
    Api {
        key: ApiKey::LeaveGroup,
        min: 0,
        max: 2,
    },
    Api {
        key: ApiKey::SyncGroup,
        min: 0,
        max: 2,
    },
    Api {
        key: ApiKey::SaslHandshake,
        min: 0,
        max: 1,
    },
    Api {
        key: ApiKey::SaslAuthenticate,
        min: 0,
        max: 1,
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
    fn classify_produce() {
        let k = ApiKey::Produce as i16;
        assert_eq!(classify(k, 3), Support::Advertised(ApiKey::Produce));
        assert_eq!(classify(k, 9), Support::Advertised(ApiKey::Produce));
        // Below the floor: the legacy message sets, which v3 replaced.
        assert_eq!(classify(k, 2), Support::UnsupportedVersion(ApiKey::Produce));
        assert_eq!(classify(k, 0), Support::UnsupportedVersion(ApiKey::Produce));
        // Above it: leader-change hints and topic ids.
        assert_eq!(
            classify(k, 10),
            Support::UnsupportedVersion(ApiKey::Produce)
        );
    }

    #[test]
    fn classify_fetch() {
        let k = ApiKey::Fetch as i16;
        assert_eq!(classify(k, 4), Support::Advertised(ApiKey::Fetch));
        assert_eq!(classify(k, 6), Support::Advertised(ApiKey::Fetch));
        // Below the schema's own floor.
        assert_eq!(classify(k, 3), Support::UnsupportedVersion(ApiKey::Fetch));
        // v7 is fetch sessions, and the whole point of the ceiling is that a
        // client can never negotiate one.
        assert_eq!(classify(k, 7), Support::UnsupportedVersion(ApiKey::Fetch));
        assert_eq!(classify(k, 13), Support::UnsupportedVersion(ApiKey::Fetch));
    }

    #[test]
    fn classify_list_offsets() {
        let k = ApiKey::ListOffsets as i16;
        assert_eq!(classify(k, 1), Support::Advertised(ApiKey::ListOffsets));
        assert_eq!(classify(k, 5), Support::Advertised(ApiKey::ListOffsets));
        assert_eq!(
            classify(k, 0),
            Support::UnsupportedVersion(ApiKey::ListOffsets)
        );
        // v7 is where MAX_TIMESTAMP becomes askable.
        assert_eq!(
            classify(k, 7),
            Support::UnsupportedVersion(ApiKey::ListOffsets)
        );
    }

    /// The group APIs, and the one property their whole window exists for:
    /// the version that introduces `group_instance_id` — static membership —
    /// is outside it, for every one of them. A row raised past that line is a
    /// client silently getting dynamic behaviour for a static configuration.
    #[test]
    fn classify_the_group_apis() {
        for (key, min, max, static_membership) in [
            (ApiKey::JoinGroup, 0, 4, 5),
            (ApiKey::SyncGroup, 0, 2, 3),
            (ApiKey::Heartbeat, 0, 2, 3),
            (ApiKey::LeaveGroup, 0, 2, 3),
            (ApiKey::OffsetCommit, 2, 6, 7),
        ] {
            let k = key as i16;
            assert_eq!(classify(k, min), Support::Advertised(key), "{key:?} v{min}");
            assert_eq!(classify(k, max), Support::Advertised(key), "{key:?} v{max}");
            assert_eq!(
                classify(k, static_membership),
                Support::UnsupportedVersion(key),
                "{key:?} v{static_membership} carries group_instance_id"
            );
            if min > 0 {
                assert_eq!(classify(k, min - 1), Support::UnsupportedVersion(key));
            }
        }

        // OffsetFetch has no group_instance_id; v8 is where one request asks
        // for SEVERAL groups and the response changes shape.
        let k = ApiKey::OffsetFetch as i16;
        assert_eq!(classify(k, 1), Support::Advertised(ApiKey::OffsetFetch));
        assert_eq!(classify(k, 7), Support::Advertised(ApiKey::OffsetFetch));
        assert_eq!(
            classify(k, 8),
            Support::UnsupportedVersion(ApiKey::OffsetFetch)
        );

        // FindCoordinator stops below the batched form.
        let k = ApiKey::FindCoordinator as i16;
        assert_eq!(classify(k, 0), Support::Advertised(ApiKey::FindCoordinator));
        assert_eq!(classify(k, 3), Support::Advertised(ApiKey::FindCoordinator));
        assert_eq!(
            classify(k, 4),
            Support::UnsupportedVersion(ApiKey::FindCoordinator)
        );
    }

    /// The SASL pair, and the one version boundary that carries meaning:
    /// SaslHandshake v0 and v1 are two different flows, so BOTH have to be in
    /// the window or a client that negotiates the older one is answered a
    /// protocol it is not speaking.
    #[test]
    fn classify_the_sasl_apis() {
        let k = ApiKey::SaslHandshake as i16;
        assert_eq!(classify(k, 0), Support::Advertised(ApiKey::SaslHandshake));
        assert_eq!(classify(k, 1), Support::Advertised(ApiKey::SaslHandshake));
        assert_eq!(
            classify(k, 2),
            Support::UnsupportedVersion(ApiKey::SaslHandshake)
        );

        let k = ApiKey::SaslAuthenticate as i16;
        assert_eq!(
            classify(k, 0),
            Support::Advertised(ApiKey::SaslAuthenticate)
        );
        assert_eq!(
            classify(k, 1),
            Support::Advertised(ApiKey::SaslAuthenticate)
        );
        // v2 is the flexible encoding; nothing in it is a field this facade
        // would answer differently.
        assert_eq!(
            classify(k, 2),
            Support::UnsupportedVersion(ApiKey::SaslAuthenticate)
        );
    }

    #[test]
    fn classify_rejects_everything_else() {
        // A real Kafka API this build does not offer: the KIP-848 group
        // protocol, which PLAN_QUEEN_KAFKA.md excludes by name.
        assert_eq!(
            classify(ApiKey::ConsumerGroupHeartbeat as i16, 0),
            Support::UnknownApi
        );
        // Transactions, excluded by the same paragraph.
        assert_eq!(
            classify(ApiKey::InitProducerId as i16, 0),
            Support::UnknownApi
        );
        assert_eq!(
            classify(ApiKey::TxnOffsetCommit as i16, 0),
            Support::UnknownApi
        );
        // ...and something that is not a Kafka API key at all.
        assert_eq!(classify(31_000, 0), Support::UnknownApi);
        assert_eq!(classify(-7, 0), Support::UnknownApi);
    }
}
