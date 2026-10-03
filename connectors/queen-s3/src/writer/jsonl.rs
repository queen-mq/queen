//! jsonl — the JSONL (+zstd/gzip) writer (plan §6.4), and the default format
//! (plan D3: zero schema risk, and every reader in §7 takes it).
//!
//! One record, one line, five fields, always in this order and with these
//! spellings:
//!
//! ```text
//! {"partition":"cust-0420","offset":1811,"transactionId":"ord-9f21","ts":"2026-09-04T10:03:41.918204Z","payload":{"type":"paid","amount":1290}}
//! ```
//!
//! The queue name and the tenant are **not** fields: they are the
//! `tenant=<esc>/queue=<esc>` keys of the path
//! (plan §6.3), which is where Hive, Spark and Athena put a partition key and
//! the only place a table may have it — a column that repeats a partition key
//! is a table a Hive metastore refuses and a schema PyArrow will not merge.
//!
//! `(partition, offset)` is unique within one incarnation of a partition. A
//! partition deleted and created again under the same name restarts at offset
//! 0, so across the lake a record's unique key is `(partition, offset, ts)`:
//! `ts` strictly increases from one append to the next across the whole
//! broker, and every record of a later incarnation is stamped after every
//! record of the earlier one.
//!
//! `payload` is **spliced** from the record's `RawValue` — the text the broker
//! handed over, which on the raft broker is the producer's own — and is `null`
//! exactly when the stored payload was empty. Nothing here parses a payload
//! into a tree: that parse was the loader's ceiling at 1M msg/s (plan §6.5),
//! and it would also mean this writer, not the producer, decided how a float or
//! a large integer is spelled.
//!
//! The one byte the splice changes is a line break. A pretty-printed payload
//! spans lines, and one record must be one line or no NDJSON reader takes the
//! object. In valid JSON a raw `\n` or `\r` can only be whitespace between two
//! tokens — inside a string a control character must be escaped — so each one
//! is written as a space: the value, every string and every number spelling
//! are exactly the producer's ([`splice_payload`]).
//!
//! # Determinism (plan §4.2, the basis of exactly-once)
//!
//! The bytes are a pure function of the records:
//!
//! * field order and spelling are fixed above, not derived from a serde struct
//!   whose field order could be reordered by a refactor;
//! * `ts` is rendered by [`Micros::to_iso`], the broker's own spelling;
//! * string escaping is the one this module implements, which is
//!   `serde_json`'s (`"`, `\`, and the C0 controls; nothing else) — pinned by a
//!   test that compares the two on a corpus rather than by a comment;
//! * gzip headers carry **mtime 0** and no filename or comment, because
//!   `GzBuilder`'s default mtime is the wall clock and would put a timestamp in
//!   the object — the one thing plan §4.2 forbids;
//! * the zstd level and the gzip level are pinned constants: a level is part of
//!   the bytes.
//!
//! A library upgrade may still move the bytes; the manifest's `writer` field
//! records which code wrote the object and `Cargo.lock` is bumped deliberately
//! (plan §6.4, §12).

use std::io::Write;
use std::sync::Arc;

use super::{RecordWriter, WriteError, WriterFactory};
use crate::types::{Compression, Format, Micros, Record};

/// The gzip level, pinned. 6 is `flate2`'s default; naming it here means a
/// change of that default is a test failure rather than a silent reformat of
/// every object.
const GZIP_LEVEL: u32 = 6;

/// The gzip OS byte. 255 = "unknown", which is what a deterministic archive
/// wants: the alternative is a byte that says which machine compressed it.
const GZIP_OS_UNKNOWN: u8 = 255;

/// Opens JSONL writers for one fixed (compression, level).
pub struct JsonlFactory {
    compression: Compression,
    zstd_level: i32,
}

impl JsonlFactory {
    pub fn new(compression: Compression, zstd_level: i32) -> JsonlFactory {
        JsonlFactory {
            compression,
            zstd_level,
        }
    }
}

/// One JSONL object in the making: the uncompressed lines, compressed once at
/// [`RecordWriter::finish`].
///
/// The buffer is the uncompressed text rather than a streaming compressor
/// because [`RecordWriter::bytes_so_far`] must answer *uncompressed* bytes — it
/// feeds the engine's size close rule, which is stated in uncompressed bytes
/// (plan §6.2 `TARGET_MB`) so that the window size does not depend on how well
/// a particular window happens to compress.
struct JsonlWriter {
    buf: Vec<u8>,
    records: u64,
    compression: Compression,
    zstd_level: i32,
    /// The last `(partition, offset, ts)` pushed — the sort-order assertion the
    /// writer trait sanctions ("the engine guarantees it, the writer may assert
    /// it"). Out-of-order records would still *write*, but the object would no
    /// longer be a function of the window's record set.
    last: Option<(Arc<str>, i64, Micros)>,
    finished: bool,
}

impl JsonlWriter {
    fn new(compression: Compression, zstd_level: i32) -> JsonlWriter {
        JsonlWriter {
            buf: Vec::new(),
            records: 0,
            compression,
            zstd_level,
            last: None,
            finished: false,
        }
    }
}

impl RecordWriter for JsonlWriter {
    /// `_queue`: the queue name is in the object's PATH, never in a line
    /// (module docs). The trait passes it because the Parquet writer records it
    /// in the file footer.
    fn push(&mut self, _queue: &str, rec: &Record) -> Result<(), WriteError> {
        if self.finished {
            return Err(WriteError("jsonl: push after finish".into()));
        }
        if let Some((p, o, t)) = &self.last {
            let forward = (&*rec.partition, rec.offset) > (&**p, *o);
            // A partition deleted and created again under the same name restarts
            // its offsets at 0, and a window can hold both incarnations: the
            // newer one's records come after, and every one of them is stamped
            // after the older one's.
            let newer_incarnation = *rec.partition == **p && rec.ts > *t;
            if !forward && !newer_incarnation {
                return Err(WriteError(format!(
                    "jsonl: records must be sorted by (partition, offset): {:?}/{} after {:?}/{}",
                    rec.partition, rec.offset, p, o
                )));
            }
        }
        let out = &mut self.buf;
        out.extend_from_slice(b"{\"partition\":");
        write_json_string(out, &rec.partition);
        out.extend_from_slice(b",\"offset\":");
        write_i64(out, rec.offset);
        out.extend_from_slice(b",\"transactionId\":");
        write_json_string(out, &rec.transaction_id);
        out.extend_from_slice(b",\"ts\":");
        write_json_string(out, &rec.ts.to_iso());
        out.extend_from_slice(b",\"payload\":");
        match &rec.payload {
            // `RawValue::get` is the exact text the broker sent: spliced, not
            // re-serialised, with its line breaks made spaces.
            Some(p) => splice_payload(out, p.get()),
            None => out.extend_from_slice(b"null"),
        }
        out.extend_from_slice(b"}\n");
        self.records += 1;
        self.last = Some((rec.partition.clone(), rec.offset, rec.ts));
        Ok(())
    }

    fn records(&self) -> u64 {
        self.records
    }

    fn bytes_so_far(&self) -> usize {
        self.buf.len()
    }

    fn finish(&mut self) -> Result<Vec<u8>, WriteError> {
        if self.finished {
            return Err(WriteError("jsonl: finish called twice".into()));
        }
        self.finished = true;
        let plain = std::mem::take(&mut self.buf);
        match self.compression {
            Compression::None => Ok(plain),
            // A Parquet page codec. The configuration never pairs it with JSONL
            // (`QUEEN_S3_COMPRESSION` does not take it), so this is a writer
            // built by hand; it refuses rather than write a codec no JSONL
            // reader expects under a name that says otherwise.
            Compression::Snappy => Err(WriteError(
                "jsonl: snappy is a Parquet codec; a JSONL object is zstd, gzip or plain".into(),
            )),
            Compression::Zstd => zstd::stream::encode_all(&plain[..], self.zstd_level)
                .map_err(|e| WriteError(format!("jsonl: zstd: {e}"))),
            Compression::Gzip => {
                let mut enc = flate2::GzBuilder::new()
                    // Not the wall clock: plan §4.2 forbids a timestamp inside
                    // the object.
                    .mtime(0)
                    .operating_system(GZIP_OS_UNKNOWN)
                    .write(Vec::new(), flate2::Compression::new(GZIP_LEVEL));
                enc.write_all(&plain)
                    .map_err(|e| WriteError(format!("jsonl: gzip: {e}")))?;
                enc.finish()
                    .map_err(|e| WriteError(format!("jsonl: gzip: {e}")))
            }
        }
    }
}

impl WriterFactory for JsonlFactory {
    fn open(&self) -> Box<dyn RecordWriter> {
        Box::new(JsonlWriter::new(self.compression, self.zstd_level))
    }
    fn extension(&self) -> &'static str {
        match self.compression {
            Compression::Zstd => "jsonl.zst",
            Compression::Gzip => "jsonl.gz",
            Compression::None => "jsonl",
            // Never written: `finish` refuses the pairing.
            Compression::Snappy => "jsonl.snappy",
        }
    }
    fn content_type(&self) -> &'static str {
        "application/x-ndjson"
    }
    fn describe(&self) -> String {
        match self.compression {
            Compression::Zstd => format!("queen-s3/{} jsonl+zstd", super::CRATE_VERSION),
            Compression::Gzip => format!("queen-s3/{} jsonl+gzip", super::CRATE_VERSION),
            Compression::None => format!("queen-s3/{} jsonl", super::CRATE_VERSION),
            Compression::Snappy => format!("queen-s3/{} jsonl+snappy", super::CRATE_VERSION),
        }
    }
    fn format(&self) -> Format {
        Format::Jsonl
    }
    fn compression(&self) -> Compression {
        self.compression
    }
}

/// Append a payload's JSON text with every raw `\n` and `\r` written as a
/// space, so the record stays one line.
///
/// Exact for valid JSON, which a `RawValue` is: a raw line break can only be
/// insignificant whitespace between tokens (RFC 8259 §2 and §7 — a string must
/// escape every control character), so the parsed value is the producer's, to
/// the last digit. Bytewise is safe in UTF-8: no byte of a multi-byte sequence
/// is `0x0A` or `0x0D`. A payload with no line break, which is nearly all of
/// them, is one copy.
pub fn splice_payload(out: &mut Vec<u8>, text: &str) {
    let bytes = text.as_bytes();
    if !bytes.iter().any(|b| matches!(b, b'\n' | b'\r')) {
        out.extend_from_slice(bytes);
        return;
    }
    out.extend(
        bytes
            .iter()
            .map(|b| if matches!(b, b'\n' | b'\r') { b' ' } else { *b }),
    );
}

// ---------------------------------------------------------------------------
// Hand-rolled JSON scalars
// ---------------------------------------------------------------------------

/// Append `s` as a JSON string, escaped exactly as `serde_json` escapes it:
/// `"` and `\`, the five short forms `\b \f \n \r \t`, every other C0 control as
/// `\u00xx` in lowercase hex — and nothing else. `/` is not escaped, non-ASCII
/// is passed through as UTF-8, `DEL` is passed through.
///
/// Hand-rolled because a `serde_json::to_writer` per field would allocate and
/// re-validate for every one of a million records, and because the escaping is
/// part of the object's byte identity and therefore belongs where the test can
/// see it. `jsonl_escaping_matches_serde_json` pins the equivalence.
fn write_json_string(out: &mut Vec<u8>, s: &str) {
    out.push(b'"');
    let bytes = s.as_bytes();
    let mut start = 0usize;
    for (i, &b) in bytes.iter().enumerate() {
        let short: &[u8] = match b {
            b'"' => b"\\\"",
            b'\\' => b"\\\\",
            0x08 => b"\\b",
            0x0c => b"\\f",
            b'\n' => b"\\n",
            b'\r' => b"\\r",
            b'\t' => b"\\t",
            0x00..=0x1f => b"",
            _ => continue,
        };
        out.extend_from_slice(&bytes[start..i]);
        if short.is_empty() {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            out.extend_from_slice(b"\\u00");
            out.push(HEX[(b >> 4) as usize]);
            out.push(HEX[(b & 0x0f) as usize]);
        } else {
            out.extend_from_slice(short);
        }
        start = i + 1;
    }
    out.extend_from_slice(&bytes[start..]);
    out.push(b'"');
}

/// Append `v` in decimal. Allocation-free: at a million records a `to_string`
/// per offset is a million heap allocations for twenty bytes of digits.
fn write_i64(out: &mut Vec<u8>, v: i64) {
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    let neg = v < 0;
    // Build from the low digit up in u64 space so i64::MIN has no special case.
    let mut mag = if neg {
        (v as i128).unsigned_abs() as u64
    } else {
        v as u64
    };
    loop {
        i -= 1;
        buf[i] = b'0' + (mag % 10) as u8;
        mag /= 10;
        if mag == 0 {
            break;
        }
    }
    if neg {
        out.push(b'-');
    }
    out.extend_from_slice(&buf[i..]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::value::RawValue;

    fn rec(partition: &str, offset: i64, payload: Option<&str>) -> Record {
        Record {
            partition: partition.into(),
            offset,
            transaction_id: format!("txn-{offset}"),
            ts: Micros(1_788_512_621_918_204),
            payload: payload.map(|p| RawValue::from_string(p.to_string()).unwrap()),
        }
    }

    #[test]
    fn line_format_is_exact() {
        let f = JsonlFactory::new(Compression::None, 3);
        let mut w = f.open();
        w.push(
            "orders",
            &Record {
                partition: "cust-0420".into(),
                offset: 1811,
                transaction_id: "ord-9f21".into(),
                ts: Micros::parse_iso("2026-09-04T10:03:41.918204Z").unwrap(),
                payload: Some(
                    RawValue::from_string("{\"type\":\"paid\",\"amount\":1290}".to_string())
                        .unwrap(),
                ),
            },
        )
        .unwrap();
        let out = String::from_utf8(w.finish().unwrap()).unwrap();
        assert_eq!(
            out,
            "{\"partition\":\"cust-0420\",\"offset\":1811,\
             \"transactionId\":\"ord-9f21\",\"ts\":\"2026-09-04T10:03:41.918204Z\",\
             \"payload\":{\"type\":\"paid\",\"amount\":1290}}\n"
        );
        assert!(
            !out.contains("\"queue\""),
            "the queue is the path key: {out}"
        );
    }

    #[test]
    fn null_payload_and_negative_offset() {
        let f = JsonlFactory::new(Compression::None, 3);
        let mut w = f.open();
        w.push("q", &rec("p", -1, None)).unwrap();
        let out = String::from_utf8(w.finish().unwrap()).unwrap();
        assert!(out.contains("\"offset\":-1,"), "{out}");
        assert!(out.ends_with("\"payload\":null}\n"), "{out}");
    }

    #[test]
    fn payload_is_spliced_byte_for_byte_but_for_its_line_breaks() {
        // Whitespace, key order, number spelling: all preserved, because the
        // text is copied and never re-serialised — except a raw line break,
        // which is whitespace between tokens and is written as a space so the
        // record stays one line.
        let odd = "{ \"b\" : 1.500, \"a\":\t[1,2,\n3] , \"big\": 12345678901234567890 }";
        let f = JsonlFactory::new(Compression::None, 3);
        let mut w = f.open();
        w.push("q", &rec("p", 0, Some(odd))).unwrap();
        let out = String::from_utf8(w.finish().unwrap()).unwrap();
        assert!(out.contains(&odd.replace('\n', " ")), "{out}");
        assert_eq!(
            out.matches('\n').count(),
            1,
            "one record, one line: {out:?}"
        );
    }

    /// A pretty-printed payload, LF and CRLF: one line per record, and the
    /// value a reader parses back is the producer's — strings that hold an
    /// ESCAPED newline keep it, numbers keep their spelling.
    #[test]
    fn a_pretty_printed_payload_is_one_line_with_the_same_value() {
        let lf = "{\n  \"text\": \"two\\nlines\",\n  \"n\": 1.500,\n  \"big\": 123456789012345678901234567890,\n  \"list\": [\n    1,\n    2\n  ]\n}";
        let crlf = lf.replace('\n', "\r\n");
        let payloads = [
            lf.to_string(),
            crlf.clone(),
            "[\r\n]".to_string(),
            "\"plain\"".to_string(),
        ];
        let f = JsonlFactory::new(Compression::None, 3);
        let mut w = f.open();
        for (i, p) in payloads.iter().enumerate() {
            w.push("q", &rec("p", i as i64, Some(p))).unwrap();
        }
        let out = String::from_utf8(w.finish().unwrap()).unwrap();
        assert!(!out.contains('\r'), "{out:?}");
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), payloads.len(), "one line per record: {out:?}");
        for (line, p) in lines.iter().zip(&payloads) {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            let want: serde_json::Value = serde_json::from_str(p).unwrap();
            assert_eq!(row["payload"], want, "{line}");
        }
        // The escaped newline inside the string is still an escape, and the
        // number spellings are the producer's.
        assert!(lines[0].contains(r#""two\nlines""#), "{}", lines[0]);
        assert!(lines[0].contains("1.500"), "{}", lines[0]);
        assert!(
            lines[0].contains("123456789012345678901234567890"),
            "{}",
            lines[0]
        );
    }

    #[test]
    fn jsonl_escaping_matches_serde_json() {
        let mut corpus: Vec<String> = vec![
            String::new(),
            "plain".into(),
            "quote\"inside".into(),
            "back\\slash".into(),
            "new\nline\ttab\rcr".into(),
            "bell\u{7}vert\u{b}".into(),
            "slash/not/escaped".into(),
            "unicode: é ü 日本語 🦆".into(),
            "del\u{7f}kept".into(),
            "a/b space".into(),
        ];
        // Every C0 control on its own, plus DEL.
        for c in 0u8..=0x20 {
            corpus.push(format!("x{}y", c as char));
        }
        for s in &corpus {
            let mut mine = Vec::new();
            write_json_string(&mut mine, s);
            let theirs = serde_json::to_string(s).unwrap();
            assert_eq!(
                String::from_utf8(mine).unwrap(),
                theirs,
                "escaping of {s:?} must match serde_json"
            );
        }
    }

    #[test]
    fn write_i64_covers_the_range() {
        for v in [0i64, 1, -1, 9, 10, -10, i64::MAX, i64::MIN, 1811, -1811] {
            let mut out = Vec::new();
            write_i64(&mut out, v);
            assert_eq!(String::from_utf8(out).unwrap(), v.to_string());
        }
    }

    #[test]
    fn out_of_order_push_is_refused() {
        let f = JsonlFactory::new(Compression::None, 3);
        let mut w = f.open();
        w.push("q", &rec("b", 5, None)).unwrap();
        assert!(w.push("q", &rec("b", 5, None)).is_err(), "duplicate");
        assert!(
            w.push("q", &rec("a", 9, None)).is_err(),
            "partition went back"
        );
        let mut w2 = f.open();
        w2.push("q", &rec("a", 1, None)).unwrap();
        w2.push("q", &rec("a", 2, None)).unwrap();
        w2.push("q", &rec("b", 0, None)).unwrap();
    }

    #[test]
    fn a_newer_incarnation_may_start_its_offsets_again() {
        // `b` was deleted and created again: offsets from 0, stamped later.
        let f = JsonlFactory::new(Compression::None, 3);
        let mut w = f.open();
        w.push("q", &rec("b", 7, None)).unwrap();
        let mut newer = rec("b", 0, None);
        newer.ts = Micros(newer.ts.0 + 1);
        w.push("q", &newer).unwrap();
        // ...but an offset going back at the SAME stamp is still a disorder.
        let mut w = f.open();
        w.push("q", &rec("b", 7, None)).unwrap();
        assert!(w.push("q", &rec("b", 3, None)).is_err());
    }

    #[test]
    fn bytes_so_far_is_uncompressed_and_grows() {
        let f = JsonlFactory::new(Compression::Zstd, 3);
        let mut w = f.open();
        assert_eq!(w.bytes_so_far(), 0);
        w.push("q", &rec("p", 0, Some("{\"a\":1}"))).unwrap();
        let one = w.bytes_so_far();
        assert!(one > 40, "{one}");
        w.push("q", &rec("p", 1, Some("{\"a\":1}"))).unwrap();
        assert_eq!(w.bytes_so_far(), 2 * one, "one line each, same shape");
        assert_eq!(w.records(), 2);
    }

    #[test]
    fn describe_and_extension_name_the_codec() {
        for (c, ext, tail) in [
            (Compression::Zstd, "jsonl.zst", "jsonl+zstd"),
            (Compression::Gzip, "jsonl.gz", "jsonl+gzip"),
            (Compression::None, "jsonl", "jsonl"),
        ] {
            let f = JsonlFactory::new(c, 3);
            assert_eq!(f.extension(), ext);
            assert!(f.describe().ends_with(tail), "{}", f.describe());
            assert_eq!(f.format(), Format::Jsonl);
            assert_eq!(f.compression(), c);
        }
    }

    /// `snappy` is a Parquet page codec. A JSONL writer built with it by hand
    /// (the configuration cannot) refuses to finish an object rather than
    /// write plain or mislabelled bytes.
    #[test]
    fn snappy_is_refused_for_jsonl() {
        let f = JsonlFactory::new(Compression::Snappy, 3);
        let mut w = f.open();
        w.push("q", &rec("p", 0, Some("{\"a\":1}"))).unwrap();
        let err = w.finish().unwrap_err();
        assert!(err.0.contains("snappy is a Parquet codec"), "{err}");
    }
}
