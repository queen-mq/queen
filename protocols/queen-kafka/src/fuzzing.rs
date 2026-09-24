//! W7 fuzz entry points (PLAN_SINGLE_BINARY.md): the facade's pre-handler
//! decoders as `fn(&[u8])` that must never panic. `fuzz/` (cargo-fuzz,
//! nightly) drives them; the seed-corpus tests below run them on stable.
//! Compiled for tests and with the `fuzzing` feature only.

use bytes::Bytes;
use kafka_protocol::messages::{ApiKey, RequestHeader, RequestKind};
use kafka_protocol::protocol::Decodable;

use crate::versions::{self, Support};

/// One request frame (the bytes after the 4-byte length prefix), decoded the
/// way `conn::dispatch` does before any handler runs: api key and version,
/// the request header at its version, then the body of that API.
pub fn request_frame(frame: &[u8]) {
    if frame.len() < 4 {
        return;
    }
    let api_key = i16::from_be_bytes([frame[0], frame[1]]);
    let api_version = i16::from_be_bytes([frame[2], frame[3]]);
    let (key, body) = match versions::classify(api_key, api_version) {
        Support::Advertised(k) => (k, true),
        // Answered from the header alone (an ApiVersions the client asked at
        // a version this facade does not speak).
        Support::UnsupportedVersion(ApiKey::ApiVersions) => (ApiKey::ApiVersions, false),
        _ => return,
    };
    let mut buf = Bytes::copy_from_slice(frame);
    if RequestHeader::decode(&mut buf, key.request_header_version(api_version)).is_err() {
        return;
    }
    if body {
        let _ = RequestKind::decode(key, &mut buf, api_version);
    }
}

/// A SASL/PLAIN initial response: parsed BEFORE authentication.
pub fn sasl_plain(data: &[u8]) {
    let _ = crate::sasl::parse_plain(data);
}

/// A produce request's record section (every batch, every codec), under a
/// small decompression budget.
pub fn record_batches(data: &[u8]) {
    let mut raw = Bytes::copy_from_slice(data);
    let budget = crate::decompress::Budget::new(1 << 20, 10_000);
    let _ = crate::decompress::decode_all(&mut raw, &budget);
}

#[cfg(test)]
mod tests {
    use super::*;

    const API_VERSIONS_V3: &[u8] =
        b"\x00\x12\x00\x03\x00\x00\x00\x01\x00\x07rdkafka\x00\x0blibrdkafka\x062.3.0\x00";
    const API_VERSIONS_V0: &[u8] = b"\x00\x12\x00\x00\x00\x00\x00\x01\x00\x07rdkafka";
    const METADATA_V1: &[u8] =
        b"\x00\x03\x00\x01\x00\x00\x00\x02\x00\x07rdkafka\x00\x00\x00\x01\x00\x06orders";
    const SASL_HANDSHAKE_V1: &[u8] =
        b"\x00\x11\x00\x01\x00\x00\x00\x03\x00\x07rdkafka\x00\x05PLAIN";

    #[test]
    fn fuzz_seed_corpus_request_frame() {
        // The seeds are real frames: they decode.
        let mut b = Bytes::from_static(API_VERSIONS_V3);
        let h =
            RequestHeader::decode(&mut b, ApiKey::ApiVersions.request_header_version(3)).unwrap();
        assert_eq!(h.correlation_id, 1);
        assert!(RequestKind::decode(ApiKey::ApiVersions, &mut b, 3).is_ok());
        for seed in [
            API_VERSIONS_V3,
            API_VERSIONS_V0,
            METADATA_V1,
            SASL_HANDSHAKE_V1,
        ] {
            request_frame(seed);
            // Every truncation, and a lying length in the body.
            for cut in 0..seed.len() {
                request_frame(&seed[..cut]);
            }
            let mut lie = seed.to_vec();
            if lie.len() > 12 {
                lie[10] = 0x7f;
                lie[11] = 0xff;
            }
            request_frame(&lie);
        }
        request_frame(b"\x7f\xff\x00\x00");
        request_frame(b"\x00\x00\x7f\xff\x00\x00\x00\x00");
    }

    #[test]
    fn fuzz_seed_corpus_sasl_and_records() {
        for seed in [
            &b"alice\x00alice\x00tok"[..],
            b"\x00alice\x00tok",
            b"\x00\x00",
            b"",
            b"\xff\x00\xfe\x00x",
        ] {
            sasl_plain(seed);
        }
        record_batches(b"");
        record_batches(&[0u8; 61]);
        let mut hdr = vec![0u8; 12];
        hdr[8..12].copy_from_slice(&i32::MAX.to_be_bytes());
        record_batches(&hdr);
        record_batches(&[0xff; 128]);
    }
}
