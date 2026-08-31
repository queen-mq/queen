//! The corpus, and what "byte-exact" is checked against.
//!
//! Every record this suite produces is a pure function of its sequence number,
//! so the consumer side never has to remember anything the producer sent — it
//! recomputes the expected key, value, headers and timestamp from the sequence
//! number it reads back out of the value. That is what makes a resumed
//! consumer, a seek, or a second run verifiable at all.
//!
//! Three things in here are deliberate and each one is testing the envelope
//! (`protocols/queen-kafka/src/records.rs`) rather than the transport:
//!
//! * **The value is not UTF-8.** It carries a NUL, a 0xFF, a 0xFE and a lone
//!   0x80 — a byte sequence that no JSON string can hold. Queen payloads are
//!   JSON, so a value that survives this proves the base64 envelope and not
//!   just that bytes moved.
//! * **The headers include an empty value AND a null value**, which Kafka
//!   distinguishes and the envelope writes as `""` and `null` respectively.
//!   A facade that collapsed the two would pass every ordinary header test.
//! * **One header name appears twice.** Kafka's header list is a list, not a
//!   map; the crate the facade decodes with keeps an `IndexMap`, and
//!   `wire::header_lists` exists to recover the repeat. This is the record that
//!   exercises it.

use rdkafka::message::{Header, Headers, OwnedHeaders};

/// Producer CreateTime base, so timestamps are checkable rather than "recent".
/// 2026-08-29T00:00:00Z in ms.
pub const TS_BASE: i64 = 1_787_961_600_000;

pub struct Corpus {
    pub topic: String,
    pub partitions: i32,
    pub count: usize,
    keys: Vec<Vec<u8>>,
    values: Vec<Vec<u8>>,
}

impl Corpus {
    pub fn new(topic: &str, partitions: i32, count: usize) -> Corpus {
        let keys = (0..count).map(key_for).collect();
        let values = (0..count).map(value_for).collect();
        Corpus {
            topic: topic.to_string(),
            partitions,
            count,
            keys,
            values,
        }
    }

    pub fn key(&self, seq: usize) -> &Vec<u8> {
        &self.keys[seq]
    }

    pub fn value(&self, seq: usize) -> &Vec<u8> {
        &self.values[seq]
    }

    /// Round-robin across the whole width, so "at least 4 partitions" is
    /// "exactly `partitions`, evenly".
    pub fn partition(&self, seq: usize) -> i32 {
        (seq % self.partitions as usize) as i32
    }

    pub fn timestamp(&self, seq: usize) -> i64 {
        TS_BASE + seq as i64
    }

    /// The n-th record produced to `partition`, i.e. what a per-partition
    /// order check compares against.
    pub fn seq_at(&self, partition: i32, nth: usize) -> usize {
        nth * self.partitions as usize + partition as usize
    }
}

pub fn key_for(seq: usize) -> Vec<u8> {
    format!("k-{seq:05}").into_bytes()
}

pub fn value_for(seq: usize) -> Vec<u8> {
    let mut v = format!("seq={seq:05};").into_bytes();
    // The part JSON cannot carry.
    v.extend_from_slice(&[0x00, 0xff, 0xfe, 0x80, 0x7f]);
    v.push((seq % 251) as u8);
    v.extend_from_slice(b";end");
    v
}

/// Recover the sequence number a value was built from. Returns None for
/// anything this suite did not write.
pub fn seq_of(value: &[u8]) -> Option<usize> {
    let text = value.strip_prefix(b"seq=")?;
    let digits = text.get(..5)?;
    std::str::from_utf8(digits).ok()?.parse().ok()
}

/// The six headers every corpus record carries, in order.
///
/// `expected_headers` below is the same list as plain data; `owned` is that
/// list as librdkafka's `OwnedHeaders`. They are written out twice on purpose:
/// building the expectation from the same `OwnedHeaders` the producer sent
/// would only prove `OwnedHeaders` round-trips through itself.
pub fn expected_headers(seq: usize) -> Vec<(&'static str, Option<Vec<u8>>)> {
    vec![
        ("seq", Some(format!("{seq}").into_bytes())),
        ("bin", Some(vec![0x00, 0x01, 0xff, 0x80])),
        ("empty", Some(Vec::new())),
        ("nul", None),
        ("dup", Some(b"first".to_vec())),
        ("dup", Some(b"second".to_vec())),
    ]
}

pub fn owned_headers(seq: usize) -> OwnedHeaders {
    let seq_val = format!("{seq}").into_bytes();
    OwnedHeaders::new_with_capacity(6)
        .insert(Header {
            key: "seq",
            value: Some(&seq_val),
        })
        .insert(Header {
            key: "bin",
            value: Some(&[0x00u8, 0x01, 0xff, 0x80][..]),
        })
        .insert(Header {
            key: "empty",
            value: Some(&[][..]),
        })
        .insert(Header {
            key: "nul",
            value: None::<&[u8]>,
        })
        .insert(Header {
            key: "dup",
            value: Some(&b"first"[..]),
        })
        .insert(Header {
            key: "dup",
            value: Some(&b"second"[..]),
        })
}

/// Flatten what came back off the wire into the same shape as
/// [`expected_headers`], preserving order and repeats.
pub fn read_headers<H: Headers>(h: Option<&H>) -> Vec<(String, Option<Vec<u8>>)> {
    match h {
        None => Vec::new(),
        Some(h) => (0..h.count())
            .map(|i| {
                let e = h.get(i);
                (e.key.to_string(), e.value.map(|v| v.to_vec()))
            })
            .collect(),
    }
}

pub fn headers_match(got: &[(String, Option<Vec<u8>>)], seq: usize) -> Result<(), String> {
    let want = expected_headers(seq);
    if got.len() != want.len() {
        return Err(format!(
            "header count {} != {} (got {})",
            got.len(),
            want.len(),
            describe(got)
        ));
    }
    for (i, ((gk, gv), (wk, wv))) in got.iter().zip(want.iter()).enumerate() {
        if gk != wk {
            return Err(format!("header {i} name {gk:?} != {wk:?}"));
        }
        if gv != wv {
            return Err(format!(
                "header {i} ({gk}) value {:?} != {:?}",
                gv.as_ref().map(|v| hex(v)),
                wv.as_ref().map(|v| hex(v))
            ));
        }
    }
    Ok(())
}

pub fn describe(h: &[(String, Option<Vec<u8>>)]) -> String {
    let parts: Vec<String> = h
        .iter()
        .map(|(k, v)| match v {
            None => format!("{k}=<null>"),
            Some(v) => format!("{k}={}", hex(v)),
        })
        .collect();
    format!("[{}]", parts.join(" "))
}

pub fn hex(b: &[u8]) -> String {
    if b.is_empty() {
        return "<empty>".to_string();
    }
    b.iter().map(|c| format!("{c:02x}")).collect()
}
