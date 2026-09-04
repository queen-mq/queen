//! The S3 client against a REAL S3-compatible gateway.
//!
//! Skipped unless the four variables below are set, so `cargo test` on a laptop
//! and in CI stays hermetic; the e2e suite of plan §9 sets them and runs it.
//! versitygw (Apache-2.0) is the gateway the plan names — MinIO is dead — and
//! it is also the closest thing to the awkward end of the compatibility range:
//! path-style addressing, no virtual-host DNS, and a multipart implementation
//! that is not AWS's.
//!
//! ```text
//! docker run -d --name queen-s3-infra-gw -p 17070:7070 \
//!     -v queen-s3-infra-gw-data:/data \
//!     -e ROOT_ACCESS_KEY=queens3test -e ROOT_SECRET_KEY=queens3secret \
//!     versity/versitygw:latest posix /data
//! # then create the bucket with one signed PUT (CreateBucket), and:
//! QUEEN_S3_TEST_ENDPOINT=http://127.0.0.1:17070 \
//! QUEEN_S3_TEST_ACCESS_KEY=queens3test \
//! QUEEN_S3_TEST_SECRET_KEY=queens3secret \
//! QUEEN_S3_TEST_BUCKET=queen-s3-test \
//!     cargo test --test infra_s3_versitygw -- --nocapture
//! ```
//!
//! `QUEEN_S3_TEST_PATH_STYLE` (default `true`) and `QUEEN_S3_TEST_REGION`
//! (default `us-east-1`) are the two optional knobs, for pointing the same test
//! at AWS or at Spaces.

use std::time::Duration;

use bytes::Bytes;
use queen_s3::config::Sse;
use queen_s3::s3::{ObjectStore, S3Client, S3Config};

/// The client, or `None` when the gate is closed.
fn gated_client(multipart_threshold: usize, part_size: usize) -> Option<(S3Client, String)> {
    let var = |name: &str| {
        std::env::var(name)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    };
    let endpoint = var("QUEEN_S3_TEST_ENDPOINT")?;
    let access_key = var("QUEEN_S3_TEST_ACCESS_KEY")?;
    let secret_key = var("QUEEN_S3_TEST_SECRET_KEY")?;
    let bucket = var("QUEEN_S3_TEST_BUCKET")?;
    let path_style = var("QUEEN_S3_TEST_PATH_STYLE")
        .map(|v| !matches!(v.to_ascii_lowercase().as_str(), "false" | "0" | "no"))
        .unwrap_or(true);
    let region = var("QUEEN_S3_TEST_REGION").unwrap_or_else(|| "us-east-1".to_string());

    // A prefix per run, so a re-run against a bucket somebody kept is clean and
    // two runs in parallel cannot collide.
    let run = format!(
        "queen-s3-it/{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let client = S3Client::new(S3Config {
        endpoint,
        region,
        bucket,
        access_key,
        secret_key,
        path_style,
        sse: Sse::Off,
        multipart_threshold,
        part_size,
        retry_base_ms: 100,
        request_timeout: Duration::from_secs(60),
        ..S3Config::default()
    })
    .expect("the gated configuration must build a client");
    Some((client, run))
}

macro_rules! gated {
    ($threshold:expr, $part:expr) => {
        match gated_client($threshold, $part) {
            Some(pair) => pair,
            None => {
                eprintln!(
                    "skipped: set QUEEN_S3_TEST_ENDPOINT, _ACCESS_KEY, _SECRET_KEY and _BUCKET \
                     to run the gateway integration test"
                );
                return;
            }
        }
    };
}

#[tokio::test]
async fn the_five_verbs_against_a_real_gateway() {
    let (s3, run) = gated!(64 * 1024 * 1024, queen_s3::s3::PART_SIZE);

    // A key with the layout's own shape: Hive components, and the `=` that the
    // canonical URI percent-encodes.
    let key = format!("{run}/queue=orders/dt=2026-09-04/hour=10/w-0000000001-1-2.jsonl");
    let body = Bytes::from(
        (0..500)
            .map(|i| format!("{{\"queue\":\"orders\",\"offset\":{i}}}\n"))
            .collect::<String>(),
    );

    let put = s3
        .put(&key, body.clone(), "application/x-ndjson")
        .await
        .unwrap();
    assert!(!put.etag.is_empty(), "a PUT must answer an ETag");

    let meta = s3.head(&key).await.unwrap().expect("HEAD after PUT");
    assert_eq!(meta.size, body.len() as u64);
    assert_eq!(meta.etag, put.etag);

    let got = s3.get(&key).await.unwrap().expect("GET after PUT");
    assert_eq!(got, body, "the bytes must come back exactly");

    let listing = s3.list(&format!("{run}/"), None, 1000).await.unwrap();
    assert!(
        listing.keys.contains(&key),
        "LIST did not name {key}: {listing:?}"
    );

    // The idempotency property the whole design rests on: a retried upload of
    // one window is byte-identical, so the second PUT is an overwrite with the
    // same content and the same ETag, and no reader ever sees two versions.
    let again = s3
        .put(&key, body.clone(), "application/x-ndjson")
        .await
        .unwrap();
    assert_eq!(again.etag, put.etag);
    assert_eq!(
        s3.list(&format!("{run}/"), None, 1000)
            .await
            .unwrap()
            .keys
            .len(),
        1
    );

    s3.delete(&key).await.unwrap();
    assert_eq!(s3.head(&key).await.unwrap(), None, "HEAD after DELETE");
    assert_eq!(s3.get(&key).await.unwrap(), None);
    // Idempotent: deleting what is not there is a success.
    s3.delete(&key).await.unwrap();
}

#[tokio::test]
async fn a_body_over_the_threshold_uploads_in_parts_and_reads_back_whole() {
    // 5 MiB parts, because that is S3's minimum for every part but the last.
    let five_mib = 5 * 1024 * 1024;
    let (s3, run) = gated!(five_mib, five_mib);

    let key = format!("{run}/queue=orders/dt=2026-09-04/hour=10/w-0000000002-3-4.jsonl");
    // Twelve MiB: three parts, the last one short.
    let mut body = Vec::with_capacity(12 * 1024 * 1024);
    let mut i: u64 = 0;
    while body.len() < 12 * 1024 * 1024 {
        body.extend_from_slice(
            format!("{{\"offset\":{i},\"pad\":\"{}\"}}\n", "x".repeat(64)).as_bytes(),
        );
        i += 1;
    }
    let body = Bytes::from(body);

    let put = s3
        .put(&key, body.clone(), "application/x-ndjson")
        .await
        .unwrap();
    assert!(!put.etag.is_empty());

    let meta = s3
        .head(&key)
        .await
        .unwrap()
        .expect("HEAD after a multipart PUT");
    assert_eq!(meta.size, body.len() as u64);

    let got = s3
        .get(&key)
        .await
        .unwrap()
        .expect("GET after a multipart PUT");
    assert_eq!(got.len(), body.len());
    assert_eq!(got, body, "every part must land in order and whole");

    s3.delete(&key).await.unwrap();
    assert_eq!(s3.head(&key).await.unwrap(), None);
}

#[tokio::test]
async fn listing_pages_through_a_prefix() {
    let (s3, run) = gated!(64 * 1024 * 1024, queen_s3::s3::PART_SIZE);
    let prefix = format!("{run}/_queen/orders/checkpoint/");
    let keys: Vec<String> = (0..5)
        .map(|k| format!("{prefix}{k:010}.json.zst"))
        .collect();
    for key in &keys {
        s3.put(key, Bytes::from_static(b"{}"), "application/json")
            .await
            .unwrap();
    }

    let mut seen = Vec::new();
    let mut after = None;
    loop {
        let page = s3.list(&prefix, after, 2).await.unwrap();
        seen.extend(page.keys.clone());
        match page.next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    assert_eq!(seen, keys, "the pages must tile the prefix, in key order");

    for key in &keys {
        s3.delete(key).await.unwrap();
    }
}
