//! The JSONL writer's object bytes: determinism, the round trip a reader makes,
//! and a size sanity check at a hundred thousand records.
//!
//! The determinism tests are the ones that matter. Exactly-once here rests on
//! "the same records produce the same object" (plan §4.2): if a retried upload
//! wrote different bytes, S3 would still replace the object atomically, but two
//! readers of the same window at different times could disagree, and the
//! manifest's `sha256` would be a lie.

use std::io::Read;

use serde_json::value::RawValue;

use queen_s3::types::{Compression, Format, Micros, Record};
use queen_s3::writer::{factory, WriterConfig};

fn rec(partition: &str, offset: i64, payload: Option<&str>) -> Record {
    Record {
        partition: partition.into(),
        offset,
        transaction_id: format!("txn-{partition}-{offset}"),
        ts: Micros(1_788_512_621_918_204 + offset),
        payload: payload.map(|p| RawValue::from_string(p.to_string()).unwrap()),
    }
}

fn corpus() -> Vec<Record> {
    let mut out = Vec::new();
    for p in ["cust-0001", "cust-0002", "a/b space", "üñî"] {
        for o in 0..25i64 {
            let payload = match o % 4 {
                0 => Some("{\"type\":\"paid\",\"amount\":1290}".to_string()),
                1 => Some(format!("{{\"n\":{o},\"s\":\"quote\\\" back\\\\ tab\\t\"}}")),
                2 => None,
                _ => Some(format!("[{o},null,true,\"\\u0000\"]")),
            };
            out.push(rec(p, o, payload.as_deref()));
        }
    }
    // The engine hands the writer records in (partition, offset) order, and the
    // writer refuses anything else; sort so the fixture obeys the contract.
    out.sort_by(|a, b| (a.partition.as_ref(), a.offset).cmp(&(b.partition.as_ref(), b.offset)));
    out
}

fn write(cfg: &WriterConfig, records: &[Record]) -> Vec<u8> {
    let f = factory(cfg);
    let mut w = f.open();
    for r in records {
        w.push("orders", r).unwrap();
    }
    w.finish().unwrap()
}

fn jsonl(compression: Compression) -> WriterConfig {
    WriterConfig {
        format: Format::Jsonl,
        compression,
        ..WriterConfig::default()
    }
}

const CODECS: [Compression; 3] = [Compression::Zstd, Compression::Gzip, Compression::None];

#[test]
fn the_same_records_produce_the_same_bytes_for_every_codec() {
    for c in CODECS {
        let cfg = jsonl(c);
        let a = write(&cfg, &corpus());
        let b = write(&cfg, &corpus());
        assert_eq!(a, b, "{c:?} is not deterministic");
        assert!(!a.is_empty());
    }
}

#[test]
fn gzip_carries_no_timestamp_and_no_filename() {
    // Bytes 4..8 of a gzip member are MTIME, byte 9 is the OS. A wall clock
    // there would put a timestamp inside the object, which plan §4.2 forbids —
    // and `GzBuilder`'s default is exactly that wall clock.
    let out = write(&jsonl(Compression::Gzip), &corpus());
    assert_eq!(&out[0..2], &[0x1f, 0x8b], "gzip magic");
    assert_eq!(&out[4..8], &[0, 0, 0, 0], "MTIME must be zero");
    assert_eq!(out[9], 255, "OS must be 'unknown'");
    assert_eq!(out[3] & 0b0000_1000, 0, "FNAME flag must be clear");
    assert_eq!(out[3] & 0b0001_0000, 0, "FCOMMENT flag must be clear");
}

#[test]
fn every_codec_decompresses_back_to_the_same_lines() {
    let plain = write(&jsonl(Compression::None), &corpus());
    for c in [Compression::Zstd, Compression::Gzip] {
        let out = write(&jsonl(c), &corpus());
        let back = match c {
            Compression::Zstd => zstd::stream::decode_all(&out[..]).unwrap(),
            Compression::Gzip => {
                let mut d = flate2::read::GzDecoder::new(&out[..]);
                let mut v = Vec::new();
                d.read_to_end(&mut v).unwrap();
                v
            }
            Compression::None => out.clone(),
        };
        assert_eq!(back, plain, "{c:?} did not round trip");
        assert!(out.len() < plain.len(), "{c:?} did not compress");
    }
}

#[test]
fn every_line_is_valid_json_with_the_envelope_in_order() {
    let records = corpus();
    let out = write(&jsonl(Compression::None), &records);
    let text = String::from_utf8(out).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), records.len());
    assert!(text.ends_with('\n'), "every line is terminated");

    for (line, r) in lines.iter().zip(&records) {
        // Field order and spelling are part of the format, so check the text,
        // not only the parse.
        let head = format!(
            "{{\"partition\":{},\"offset\":{},\"transactionId\":{},\"ts\":\"{}\",\"payload\":",
            serde_json::to_string(r.partition.as_ref()).unwrap(),
            r.offset,
            serde_json::to_string(&r.transaction_id).unwrap(),
            r.ts.to_iso()
        );
        assert!(
            line.starts_with(&head),
            "\n{line}\ndoes not start with\n{head}"
        );

        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(
            v.get("queue").is_none(),
            "the queue is the `queue=` path key, never a field: {line}"
        );
        assert_eq!(v["partition"], r.partition.as_ref());
        assert_eq!(v["offset"], r.offset);
        assert_eq!(v["transactionId"], r.transaction_id);
        assert_eq!(v["ts"], r.ts.to_iso());
        match &r.payload {
            None => assert!(v["payload"].is_null(), "{line}"),
            Some(p) => {
                let want: serde_json::Value = serde_json::from_str(p.get()).unwrap();
                assert_eq!(v["payload"], want, "{line}");
            }
        }
    }
}

#[test]
fn a_payload_with_every_escape_survives_verbatim() {
    // Built by serde_json, so it contains exactly the escapes serde_json emits —
    // and the writer must copy the text, not re-encode it.
    let nasty = serde_json::json!({
        "quote": "he said \"hi\"",
        "backslash": "C:\\temp\\x",
        "controls": "\u{0}\u{1}\u{7}\u{8}\u{9}\u{a}\u{b}\u{c}\u{d}\u{1f}",
        "del": "\u{7f}",
        "unicode": "é ü 日本語 🦆 \u{2028}\u{2029}",
        "slash": "a/b",
        "empty": "",
        "nested": {"a": [1, 2, {"b": null}]},
        "numbers": [0, -0.0, 1e308, -1e-308, 12345678901234567890i128 as f64],
    });
    let text = serde_json::to_string(&nasty).unwrap();
    let r = rec("p", 0, Some(&text));
    let out = write(&jsonl(Compression::None), &[r]);
    let line = String::from_utf8(out).unwrap();
    assert!(
        line.contains(&text),
        "the payload was rewritten:\n{line}\nexpected to contain\n{text}"
    );
    // And the line is still one valid JSON document.
    let v: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
    assert_eq!(v["payload"], nasty);
}

#[test]
fn a_partition_name_that_needs_escaping_does_not_break_the_line() {
    let r = Record {
        partition: "quote\"and\\slash\nnewline".into(),
        offset: 7,
        transaction_id: "tx\"n".into(),
        ts: Micros(1_788_512_621_918_204),
        payload: None,
    };
    let out = write(&jsonl(Compression::None), std::slice::from_ref(&r));
    let line = String::from_utf8(out).unwrap();
    let v: serde_json::Value = serde_json::from_str(line.trim_end()).unwrap();
    assert_eq!(v["partition"], r.partition.as_ref());
    assert_eq!(v["transactionId"], r.transaction_id);
}

#[test]
fn a_hundred_thousand_records_are_the_right_size() {
    const N: i64 = 100_000;
    let mut records = Vec::with_capacity(N as usize);
    for o in 0..N {
        records.push(rec(
            "cust-000042",
            o,
            Some("{\"type\":\"paid\",\"amount\":1290,\"currency\":\"EUR\"}"),
        ));
    }

    let plain = write(&jsonl(Compression::None), &records);
    let per_record = plain.len() as f64 / N as f64;
    assert!(
        (168.0..183.0).contains(&per_record),
        "a record line is {per_record:.1} bytes; the envelope moved"
    );

    let zstd = write(&jsonl(Compression::Zstd), &records);
    let gzip = write(&jsonl(Compression::Gzip), &records);
    // Highly repetitive by construction, so both codecs should be far under a
    // tenth; the point is that the object is compressed at all and that the two
    // codecs are in the same league.
    assert!(zstd.len() < plain.len() / 10, "zstd: {} bytes", zstd.len());
    assert!(gzip.len() < plain.len() / 10, "gzip: {} bytes", gzip.len());

    // The size trigger reads uncompressed bytes (plan §6.2), so bytes_so_far
    // must track `plain`, not the object.
    let f = factory(&jsonl(Compression::Zstd));
    let mut w = f.open();
    for r in &records {
        w.push("orders", r).unwrap();
    }
    assert_eq!(w.bytes_so_far(), plain.len());
    assert_eq!(w.records(), N as u64);

    eprintln!(
        "100k records: plain {} B, zstd {} B, gzip {} B",
        plain.len(),
        zstd.len(),
        gzip.len()
    );
}

#[test]
fn the_factory_names_the_object_the_way_the_layout_expects() {
    for (c, ext) in [
        (Compression::Zstd, "jsonl.zst"),
        (Compression::Gzip, "jsonl.gz"),
        (Compression::None, "jsonl"),
    ] {
        let f = factory(&jsonl(c));
        assert_eq!(f.extension(), ext);
        assert_eq!(f.content_type(), "application/x-ndjson");
        assert_eq!(f.format(), Format::Jsonl);
        assert_eq!(f.compression(), c);
        assert!(f.describe().starts_with("queen-s3/"), "{}", f.describe());
    }
}

#[test]
fn an_empty_window_still_produces_a_valid_object() {
    for c in CODECS {
        let out = write(&jsonl(c), &[]);
        let back = match c {
            Compression::Zstd => zstd::stream::decode_all(&out[..]).unwrap(),
            Compression::Gzip => {
                let mut d = flate2::read::GzDecoder::new(&out[..]);
                let mut v = Vec::new();
                d.read_to_end(&mut v).unwrap();
                v
            }
            Compression::None => out.clone(),
        };
        assert!(back.is_empty(), "{c:?}");
    }
}
