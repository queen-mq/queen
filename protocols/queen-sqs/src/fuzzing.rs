//! W7 fuzz entry points (PLAN_SINGLE_BINARY.md): the facade's pre-handler
//! decoders — protocol sniffing (AWS JSON and Query) and SigV4 — as
//! `fn(&[u8])` that must never panic. `fuzz/` (cargo-fuzz, nightly) drives
//! them; the seed-corpus tests below run them on stable. Compiled for tests
//! and with the `fuzzing` feature only.
//!
//! Input shape for both: `name: value` header lines, a blank line, the body.
//! The pseudo-headers `:method`, `:path` and `:query` carry the request line.

use std::sync::OnceLock;

use crate::credentials::Directory;
use crate::sigv4::SignedRequest;

const AKID: &str = "AKIDEXAMPLE";
const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
/// 2026-08-30T12:00:00Z, the clock of the botocore seed vector.
const NOW_MS: i64 = 1_788_091_200_000;

fn split(data: &[u8]) -> (Vec<(String, String)>, &[u8]) {
    let (head, body) = match data.windows(2).position(|w| w == b"\n\n") {
        Some(i) => (&data[..i], &data[i + 2..]),
        None => (data, &[][..]),
    };
    let headers = String::from_utf8_lossy(head)
        .lines()
        .filter_map(|l| {
            let (k, v) = l.split_once(": ")?;
            Some((k.to_ascii_lowercase(), v.to_string()))
        })
        .collect();
    (headers, body)
}

fn get<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
}

/// Protocol detection and decoding of one request (`proto::sniff`).
pub fn request(data: &[u8]) {
    let (headers, body) = split(data);
    let _ = crate::proto::sniff(&headers, body);
}

/// SigV4: the `Authorization` header parser, the presigned-query parser, and
/// a full verification against a fixed credential directory.
pub fn sigv4(data: &[u8]) {
    static DIR: OnceLock<Directory> = OnceLock::new();
    let dir = DIR.get_or_init(|| {
        Directory::from_spec(&format!("{AKID}:{SECRET}:tok-1")).expect("static spec")
    });
    let (headers, body) = split(data);
    if let Some(a) = get(&headers, "authorization") {
        let _ = crate::sigv4::parse_authorization(a);
    }
    let query = get(&headers, ":query").unwrap_or("");
    let _ = crate::sigv4::parse_presigned(query);
    let req = SignedRequest {
        method: get(&headers, ":method").unwrap_or("POST"),
        path: get(&headers, ":path").unwrap_or("/"),
        query,
        headers: &headers,
        body,
    };
    let _ = crate::sigv4::verify(&req, dir, "queen-1", NOW_MS);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The botocore-signed Query vector from `sigv4::tests`.
    const SIGNED: &str = "authorization: AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260830/us-east-1/sqs/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=02c85e759cc5e2ce840146c82043bf13598030989142aceb4e9627da8ac45d39\ncontent-type: application/x-www-form-urlencoded; charset=utf-8\nhost: localhost:9324\nx-amz-date: 20260830T120000Z\n\nAction=ListQueues&Version=2012-11-05";

    #[test]
    fn fuzz_seed_corpus_sigv4() {
        // The seed really verifies through the entry point's plumbing.
        let (headers, body) = split(SIGNED.as_bytes());
        let dir = Directory::from_spec(&format!("{AKID}:{SECRET}:tok-1")).unwrap();
        let req = SignedRequest {
            method: "POST",
            path: "/",
            query: "",
            headers: &headers,
            body,
        };
        assert!(crate::sigv4::verify(&req, &dir, "queen-1", NOW_MS).is_ok());
        let seeds = [
            SIGNED.to_string(),
            SIGNED.replace("Signature=02c8", "Signature=ffff"),
            SIGNED.replace("Credential=AKIDEXAMPLE/20260830/us-east-1/sqs/aws4_request", "Credential=a/b/c"),
            "authorization: AWS4-HMAC-SHA256 Credential=, SignedHeaders=, Signature=\n\n".to_string(),
            ":query: X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIDEXAMPLE%2F20260830%2Fus-east-1%2Fsqs%2Faws4_request&X-Amz-Date=20260830T120000Z&X-Amz-Expires=-1&X-Amz-SignedHeaders=host&X-Amz-Signature=00\nhost: localhost:9324\n\n".to_string(),
            ":query: X-Amz-Expires=99999999999999999999999&X-Amz-Algorithm=AWS4-HMAC-SHA256\n\n".to_string(),
            String::new(),
        ];
        for s in &seeds {
            sigv4(s.as_bytes());
            for cut in (0..s.len()).step_by(7) {
                sigv4(&s.as_bytes()[..cut]);
            }
        }
    }

    #[test]
    fn fuzz_seed_corpus_request() {
        for seed in [
            "x-amz-target: AmazonSQS.SendMessage\ncontent-type: application/x-amz-json-1.0\n\n{\"QueueUrl\":\"http://localhost:9324/000000000000/orders\",\"MessageBody\":\"hi\"}",
            "x-amz-target: AmazonSQS.ListQueues\n\n",
            "x-amz-target: AmazonSQS.Nope\n\n[1,2",
            "content-type: application/x-www-form-urlencoded\n\nAction=CreateQueue&Version=2012-11-05&QueueName=q.fifo&Attribute.1.Name=FifoQueue&Attribute.1.Value=true",
            "\n\nAction=SendMessageBatch&Version=2012-11-05&SendMessageBatchRequestEntry.1.Id=a&SendMessageBatchRequestEntry.1.MessageBody=x&SendMessageBatchRequestEntry.1.MessageAttribute.1.Name=k&SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.DataType=String",
            "\n\nAction=Publish&Version=2010-03-31&TopicArn=arn%3Aaws%3Asns%3Aus-east-1%3A0%3At&Message=hi",
            ":query: Action=ListQueues&Version=2012-11-05\n\n",
            "\n\nAction=SendMessage&A.1.2.3.4.5.6.7.8.9.10.11.12.13.14.15.16.17.18.19.20.21.22.23.24.25.26.27.28.29.30.31.32.33.34=x",
            "\n\n%zz=%&&==",
            "",
        ] {
            request(seed.as_bytes());
        }
        request(&[0xff, b'\n', b'\n', 0xfe]);
    }
}
