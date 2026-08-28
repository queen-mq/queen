//! ApiVersions — the first request on every Kafka connection, and the only one
//! this build answers at M0.

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::ApiVersionsResponse;

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
/// The body carries the error code alone, with no api_keys: that is what the
/// broker sends (`ApiVersionsRequest.getErrorResponse`), and clients treat
/// UNSUPPORTED_VERSION here as "retry at v0" regardless of the contents.
pub fn unsupported_version() -> Rendered {
    Rendered {
        body: ApiVersionsResponse::default()
            .with_error_code(ResponseError::UnsupportedVersion.code()),
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

    #[test]
    fn the_fallback_is_error_only_at_v0() {
        let r = unsupported_version();
        assert_eq!(r.encode_version, 0);
        assert_eq!(r.body.error_code, 35);
        assert!(r.body.api_keys.is_empty());
    }
}
