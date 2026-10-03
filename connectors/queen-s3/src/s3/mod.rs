//! The S3 side: SigV4 signing and the object store the sink writes through.
//!
//! Two modules, and the split is the one the plan drew (§6.1): [`sigv4`] is
//! arithmetic over strings with no I/O and no configuration — it is pinned by
//! the AWS documentation's own test vectors and nothing else — and [`client`] is
//! everything that knows about a bucket, an endpoint, a retry and a part.
//!
//! The whole surface is five verbs ([`ObjectStore`]), because that is the whole
//! bucket policy the deploy page asks an operator for (plan §6.9). There is no
//! conditional PUT and no correctness use of LIST: the window protocol makes a
//! retried upload byte-identical, which is what buys exactly-once without either
//! (plan §4.2).

pub mod client;
pub mod sigv4;

pub use client::{
    Listing, MemoryStore, ObjectMeta, ObjectStore, PutOutcome, PutRecord, S3Client, S3Config,
    MAX_BACKOFF_MS, PART_SIZE,
};
pub use sigv4::{Credentials, Signed, SigningRequest, EMPTY_PAYLOAD_SHA256};
