//! ApiVersions — the first request on every Kafka connection, and the only one
//! this build answers at M0.

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::{ApiKey, ApiVersionsResponse};

use crate::versions::ADVERTISED;

/// A response body and the version it must be encoded at. They are the same
/// number for every API except this one, which is why the pair is returned
/// rather than assumed by the caller.
pub struct Rendered {
    pub body: ApiVersionsResponse,
    pub encode_version: i16,
}

/// The normal answer: the whole advertised table, verbatim.
pub fn handle(request_version: i16) -> Rendered {
    let api_keys = ADVERTISED
        .iter()
        .map(|a| {
            ApiVersion::default()
                .with_api_key(a.key as i16)
                .with_min_version(a.min)
                .with_max_version(a.max)
        })
        .collect();
    Rendered {
        body: ApiVersionsResponse::default()
            .with_error_code(0)
            .with_api_keys(api_keys),
        encode_version: request_version,
    }
}

/// The bootstrap quirk, and the reason ApiVersions cannot be handled like any
/// other API.
///
/// A client that does not yet know what the broker speaks may open with an
/// ApiVersions version above ours. Answering at that version is impossible (we
/// do not have the schema) and answering at ours is worse (the client would
/// parse our newer/older layout as the one it asked for). Apache Kafka's rule,
/// which every client is written against: reply UNSUPPORTED_VERSION with the
/// body encoded at **version 0** — the one layout that has never changed — and
/// the client downgrades and asks again. The response header for ApiVersions is
/// v0 (non-flexible) at every version for the same reason, which
/// `ApiKey::response_header_version` already knows.
///
/// The body carries the error code and ONE entry: this API's own window. That
/// is what Apache Kafka sends (`ApiVersionsRequest.getErrorResponse`, which
/// adds exactly the ApiVersions row when the error is UNSUPPORTED_VERSION), and
/// it is the fact the client came for — `NetworkClient.handleApiVersionsResponse`
/// looks the entry up to choose which version to retry at, and falls back to 0
/// when the array is empty. Answering empty is not fatal, because 0 is always a
/// version this API speaks; it is simply silent about something this facade
/// knows ([`ADVERTISED`]), and it leaves a client with a newer floor than 0
/// nothing to negotiate against.
///
/// Everything else stays out of it: a client that could not parse our
/// ApiVersions request cannot be assumed to parse a whole table, and the one
/// row it is defined to read is the one row it gets.
pub fn unsupported_version() -> Rendered {
    let api_keys = ADVERTISED
        .iter()
        .filter(|a| a.key == ApiKey::ApiVersions)
        .map(|a| {
            ApiVersion::default()
                .with_api_key(a.key as i16)
                .with_min_version(a.min)
                .with_max_version(a.max)
        })
        .collect();
    Rendered {
        body: ApiVersionsResponse::default()
            .with_error_code(ResponseError::UnsupportedVersion.code())
            .with_api_keys(api_keys),
        encode_version: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::versions;

    #[test]
    fn the_response_is_the_table() {
        let r = handle(3);
        assert_eq!(r.encode_version, 3);
        assert_eq!(r.body.error_code, 0);
        assert_eq!(r.body.api_keys.len(), versions::ADVERTISED.len());
        for (got, want) in r.body.api_keys.iter().zip(versions::ADVERTISED) {
            assert_eq!(got.api_key, want.key as i16);
            assert_eq!(got.min_version, want.min);
            assert_eq!(got.max_version, want.max);
        }
    }

    /// The fallback is encoded at v0 — the layout that has never changed — and
    /// carries the one row the client came for: which ApiVersions versions this
    /// broker speaks. Without it a client is told to downgrade and told nothing
    /// about what to downgrade TO.
    #[test]
    fn the_fallback_names_the_version_to_retry_at() {
        let r = unsupported_version();
        assert_eq!(r.encode_version, 0);
        assert_eq!(r.body.error_code, 35);

        let advertised =
            versions::lookup(ApiKey::ApiVersions as i16).expect("ApiVersions is in the table");
        assert_eq!(r.body.api_keys.len(), 1, "the fallback is not one entry");
        assert_eq!(r.body.api_keys[0].api_key, ApiKey::ApiVersions as i16);
        assert_eq!(r.body.api_keys[0].min_version, advertised.min);
        assert_eq!(r.body.api_keys[0].max_version, advertised.max);
    }

    /// ...and the whole body survives the v0 encoding, which is the only one a
    /// client that asked at an unknown version can read.
    #[test]
    fn the_fallback_round_trips_at_version_zero() {
        use bytes::BytesMut;
        use kafka_protocol::protocol::{Decodable, Encodable};

        let r = unsupported_version();
        let mut wire = BytesMut::new();
        r.body.encode(&mut wire, r.encode_version).expect("encodes");
        let mut buf = wire.freeze();
        let back = ApiVersionsResponse::decode(&mut buf, r.encode_version).expect("decodes");
        assert!(buf.is_empty(), "{} trailing bytes", buf.len());
        assert_eq!(back.error_code, 35);
        assert_eq!(back.api_keys.len(), 1);
        assert_eq!(back.api_keys[0].api_key, ApiKey::ApiVersions as i16);
    }
}
