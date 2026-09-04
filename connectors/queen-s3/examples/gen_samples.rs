//! `gen_samples` — write a small sample lake with the sink's OWN writers, for
//! the reader compatibility lane (`compat/`, PLAN_S3_SINK.md §9).
//!
//! ```text
//! cargo run --example gen_samples -- /tmp/queen-s3-samples
//! ```
//!
//! The point of this binary is that **nothing here re-implements the object
//! format**. It builds [`Record`]s, hands them to `writer::factory` and puts the
//! bytes at the key `layout::data_key` names — the same two functions the driver
//! calls — so what the readers in `compat/readers/` open is what the sink
//! produces, not a plausible imitation of it.
//!
//! # What it writes
//!
//! Five prefixes, one per (format, compression) the sink can emit, each holding
//! the **same 5 000 records**:
//!
//! ```text
//! <dir>/jsonl-zstd/queue=orders/dt=2026-09-04/hour=10/w-0000001842-…-….jsonl.zst
//! <dir>/jsonl-gzip/…/….jsonl.gz
//! <dir>/jsonl-none/…/….jsonl
//! <dir>/parquet-zstd/…/….parquet
//! <dir>/parquet-snappy/…/….parquet
//! <dir>/expected.json
//! ```
//!
//! Two queues, three partitions each, two aligned hour buckets, [`Layout::Merged`]
//! — so every prefix holds 2 queues × 2 windows = 4 objects. The partition names
//! are deliberately awkward (`cust 42/eu` has a space and a slash, `région-eu` is
//! not ASCII): in the merged layout a partition name is a *column value*, never
//! part of a key, so what these two names test is a reader's string handling, and
//! `expected.json` says so. The QUEUE, by contrast, is only ever a key
//! (`queue=orders`), so a reader gets it from Hive partition discovery and never
//! from a column (plan §6.3, §6.4).
//!
//! # `expected.json`
//!
//! Everything a reader can be checked against for **exactness** rather than for
//! "it opened": the record count per queue and per partition, the number of NULL
//! payloads, a per-queue sha256 of the `partition|offset` list in
//! `(partition, offset)` order, one across all queues, a spot check of decoded
//! payload fields, and the sha256 of every object's bytes. The digest rule is
//! written into the file (`digestSpec`) so a reader script cannot get it subtly
//! wrong and still pass.
//!
//! Offsets are dense from 0 within each partition and span both windows, which is
//! what the log looks like: a reader that loses a window loses a contiguous run
//! and every check fails at once.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

use queen_s3::layout::{data_key, DataKey};
use queen_s3::types::{Align, Compression, Format, Layout, Micros, ParquetCodec, Record};
use queen_s3::writer::{factory, WriterConfig};

// ---------------------------------------------------------------------------
// The shape of the sample lake
// ---------------------------------------------------------------------------

/// `(queue, [(partition, records)])`. Counts are uneven on purpose: a reader
/// that silently reads one object twice, or drops one, cannot land on the right
/// per-partition numbers by accident.
const LANES: &[(&str, &[(&str, i64)])] = &[
    (
        "orders",
        &[
            ("cust-0420", 900),
            ("cust-0421", 720),
            // A space and a slash: illegal in a key without escaping, ordinary
            // as a column value. In the merged layout it is only ever the
            // latter.
            ("cust 42/eu", 660),
        ],
    ),
    (
        "audit",
        &[
            ("svc.billing", 1100),
            ("svc.mail", 840),
            // Not ASCII: JSON must escape or emit it as UTF-8, Parquet stores
            // UTF-8, and a reader must give it back byte for byte.
            ("région-eu", 780),
        ],
    ),
];

/// The first window's start. The second is the hour after it. Both are aligned,
/// so `dt=`/`hour=` are exact for every record in an object (plan §4.4).
const FIRST_HOUR: &str = "2026-09-04T10:00:00Z";
/// The window number of the first window. Arbitrary, but it is what the keys
/// sort on, so it is written down.
const FIRST_K: u64 = 1842;
/// Fraction of a lane's records that fall in the first hour, in tenths.
const FIRST_HOUR_TENTHS: i64 = 6;
/// Records per Parquet row group. Small on purpose: a 1 000-record object then
/// has several row groups, which is what a reader's row-group pruning and
/// statistics handling actually meet in production.
const ROW_GROUP_RECORDS: usize = 500;
const ZSTD_LEVEL: i32 = 3;
/// Records whose payload is JSON `null` — `"payload":null` on the wire, a NULL
/// column in Parquet. One in every 419 offsets.
const NULL_EVERY: i64 = 419;
const NULL_PHASE: i64 = 17;

/// Roughly how many records share one segment timestamp.
const RECORDS_PER_SEGMENT: i64 = 50;

const TYPES: [&str; 4] = ["paid", "refunded", "authorized", "cancelled"];
const CURRENCIES: [&str; 3] = ["EUR", "USD", "JPY"];
const NOTES: [&str; 4] = [
    "café ☕ ordinaria",
    "日本語のメモ",
    "duck 🦆 emoji, and a \"quote\"",
    "backslash \\ and tab\tinside",
];

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() {
    let dir = match std::env::args().nth(1) {
        Some(d) => PathBuf::from(d),
        None => {
            eprintln!("usage: cargo run --example gen_samples -- <dir>");
            std::process::exit(2);
        }
    };
    fs::create_dir_all(&dir).expect("create sample dir");

    let t0 = Micros::parse_iso(FIRST_HOUR).expect("FIRST_HOUR is a broker timestamp");
    let windows: [(u64, Micros, Micros); 2] = [
        (FIRST_K, t0, t0.saturating_add(Micros::HOUR)),
        (
            FIRST_K + 1,
            t0.saturating_add(Micros::HOUR),
            t0.saturating_add(Micros(2 * Micros::HOUR.0)),
        ),
    ];

    // queue -> window index -> records, sorted by (partition, offset) exactly as
    // the engine hands them to a writer.
    let mut by_queue: Vec<(&str, [Vec<Record>; 2])> = Vec::new();
    for (queue, lanes) in LANES {
        let mut buckets: [Vec<Record>; 2] = [Vec::new(), Vec::new()];
        for (partition, count) in *lanes {
            let in_first = count * FIRST_HOUR_TENTHS / 10;
            for offset in 0..*count {
                let w = if offset < in_first { 0 } else { 1 };
                let within = if w == 0 { offset } else { offset - in_first };
                let of_window = if w == 0 { in_first } else { count - in_first };
                let rec = make_record(
                    queue,
                    partition,
                    offset,
                    segment_ts(windows[w].1, within, of_window, partition),
                );
                buckets[w].push(rec);
            }
        }
        for b in buckets.iter_mut() {
            b.sort_by(|a, c| (&*a.partition, a.offset).cmp(&(&*c.partition, c.offset)));
        }
        by_queue.push((queue, buckets));
    }

    // -- the five (format, compression) prefixes ----------------------------
    let configs: &[(&str, Format, Compression, ParquetCodec)] = &[
        (
            "jsonl-zstd",
            Format::Jsonl,
            Compression::Zstd,
            ParquetCodec::Zstd,
        ),
        (
            "jsonl-gzip",
            Format::Jsonl,
            Compression::Gzip,
            ParquetCodec::Zstd,
        ),
        (
            "jsonl-none",
            Format::Jsonl,
            Compression::None,
            ParquetCodec::Zstd,
        ),
        (
            "parquet-zstd",
            Format::Parquet,
            Compression::Zstd,
            ParquetCodec::Zstd,
        ),
        (
            "parquet-snappy",
            Format::Parquet,
            Compression::None,
            ParquetCodec::Snappy,
        ),
    ];

    let mut prefixes = serde_json::Map::new();
    for (prefix, format, compression, codec) in configs {
        let cfg = WriterConfig {
            format: *format,
            compression: *compression,
            parquet_codec: *codec,
            zstd_level: ZSTD_LEVEL,
            parquet_row_group_records: ROW_GROUP_RECORDS,
        };
        let fac = factory(&cfg);
        let mut objects = Vec::new();
        for (queue, buckets) in &by_queue {
            for (w, recs) in buckets.iter().enumerate() {
                let (k, t_start, t_end) = windows[w];
                let key = data_key(&DataKey {
                    prefix,
                    queue,
                    layout: Layout::Merged,
                    align: Align::Hour,
                    k,
                    t_start,
                    t_end,
                    bucket_ts: t_start,
                    ext: fac.extension(),
                    partition: None,
                    offsets: None,
                });
                let mut w8 = fac.open();
                for r in recs {
                    w8.push(queue, r).expect("writer accepts sorted records");
                }
                let bytes = w8.finish().expect("writer finishes");
                write_object(&dir, &key, &bytes);
                objects.push(serde_json::json!({
                    "key": key,
                    "queue": queue,
                    "k": k,
                    "records": recs.len(),
                    "bytes": bytes.len(),
                    "sha256": sha256_hex(&bytes),
                }));
            }
        }
        prefixes.insert(
            prefix.to_string(),
            serde_json::json!({
                "format": match format { Format::Jsonl => "jsonl", Format::Parquet => "parquet" },
                "compression": match (format, compression, codec) {
                    (Format::Parquet, _, ParquetCodec::Zstd) => "zstd",
                    (Format::Parquet, _, ParquetCodec::Snappy) => "snappy",
                    (_, Compression::Zstd, _) => "zstd",
                    (_, Compression::Gzip, _) => "gzip",
                    (_, Compression::None, _) => "none",
                },
                "ext": fac.extension(),
                "contentType": fac.content_type(),
                "writer": fac.describe(),
                "objects": objects,
            }),
        );
    }

    // -- expected.json -------------------------------------------------------
    let expected = build_expected(&by_queue, &windows, prefixes);
    let path = dir.join("expected.json");
    fs::write(
        &path,
        serde_json::to_string_pretty(&expected).expect("expected.json serialises"),
    )
    .expect("write expected.json");

    println!("wrote {} prefixes under {}", configs.len(), dir.display());
    println!("expected: {}", path.display());
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

/// A 64-bit FNV-1a over the record's coordinates: the only source of variation
/// in the payloads, so the whole sample lake is a pure function of the constants
/// above and re-running this binary rewrites byte-identical objects.
fn hash64(parts: &[&str], n: i64) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let mut mix = |bytes: &[u8]| {
        for &b in bytes {
            h ^= b as u64;
            h = h.wrapping_mul(0x1000_0000_01b3);
        }
    };
    for p in parts {
        mix(p.as_bytes());
        mix(b"\x1f");
    }
    mix(&n.to_le_bytes());
    h
}

/// The segment timestamp for the `within`-th record of a lane inside a window.
///
/// Records are grouped into segments of [`RECORDS_PER_SEGMENT`] that share one
/// timestamp — which is what the broker does, one `created_at` per segment — and
/// the segments are spread across the hour so that `ts` is non-decreasing with
/// `offset` and never reaches the window's end.
fn segment_ts(t_start: Micros, within: i64, of_window: i64, partition: &str) -> Micros {
    let segments = (of_window + RECORDS_PER_SEGMENT - 1) / RECORDS_PER_SEGMENT;
    let step = Micros::HOUR.0 / (segments + 2);
    let seg = within / RECORDS_PER_SEGMENT;
    // A per-partition skew under a second, so two partitions do not share every
    // timestamp: realistic, and it keeps a reader from passing a `ts` check by
    // reading a single column correctly.
    let skew = (hash64(&[partition], 0) % 900_000) as i64;
    Micros(t_start.0 + (seg + 1) * step + skew)
}

fn make_record(queue: &str, partition: &str, offset: i64, ts: Micros) -> Record {
    let h = hash64(&[queue, partition], offset);
    let payload = if offset % NULL_EVERY == NULL_PHASE {
        None
    } else {
        Some(
            RawValue::from_string(payload_json(queue, partition, offset, h))
                .expect("the payload is valid JSON"),
        )
    };
    Record {
        partition: Arc::from(partition),
        offset,
        transaction_id: format!("{}-{:012x}", txn_prefix(queue), h & 0xffff_ffff_ffff),
        ts,
        payload,
    }
}

fn txn_prefix(queue: &str) -> &'static str {
    match queue {
        "orders" => "ord",
        _ => "aud",
    }
}

/// A payload that looks like something: nested objects, an array of objects,
/// unicode and escapes in strings, integers, a fractional number that is exact
/// in binary (so no reader can lose it to float formatting), booleans, and an
/// explicit inner `null`.
fn payload_json(queue: &str, partition: &str, offset: i64, h: u64) -> String {
    let ty = TYPES[(h % 4) as usize];
    let cur = CURRENCIES[((h >> 8) % 3) as usize];
    let note = NOTES[((h >> 16) % 4) as usize];
    let amount = 100 + (h % 900_000) as i64;
    let ok = h & 1 == 0;
    let ratio = ((h >> 24) % 64) as f64 / 64.0;
    let qty1 = 1 + (h % 5) as i64;
    let qty2 = 1 + ((h >> 5) % 3) as i64;
    let mut s = String::with_capacity(384);
    s.push('{');
    push_kv_str(&mut s, "type", ty);
    s.push(',');
    push_kv_str(&mut s, "queue", queue);
    s.push(',');
    s.push_str(&format!("\"amount\":{amount},"));
    push_kv_str(&mut s, "currency", cur);
    s.push(',');
    s.push_str(&format!("\"ok\":{ok},"));
    s.push_str(&format!("\"ratio\":{ratio},"));
    s.push_str(&format!("\"seq\":{offset},"));
    s.push_str("\"customer\":{");
    push_kv_str(&mut s, "id", partition);
    s.push(',');
    push_kv_str(&mut s, "name", "Zoë Müller");
    s.push_str(",\"vip\":");
    s.push_str(if ok { "true" } else { "false" });
    s.push_str(",\"score\":");
    s.push_str(&format!("{}", (h >> 32) % 100));
    s.push_str("},\"items\":[{\"sku\":\"QM-A\",\"qty\":");
    s.push_str(&qty1.to_string());
    s.push_str("},{\"sku\":\"QM-B\",\"qty\":");
    s.push_str(&qty2.to_string());
    s.push_str("}],");
    push_kv_str(&mut s, "note", note);
    s.push_str(",\"meta\":{\"retries\":null,\"source\":\"queen\"}}");
    s
}

fn push_kv_str(out: &mut String, key: &str, value: &str) {
    out.push('"');
    out.push_str(key);
    out.push_str("\":");
    out.push_str(&serde_json::to_string(value).expect("a string serialises"));
}

// ---------------------------------------------------------------------------
// expected.json
// ---------------------------------------------------------------------------

fn build_expected(
    by_queue: &[(&str, [Vec<Record>; 2])],
    windows: &[(u64, Micros, Micros); 2],
    prefixes: serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    let mut queues = serde_json::Map::new();
    let mut all_rows: Vec<(String, String, i64)> = Vec::new();
    let mut total = 0usize;

    for (queue, buckets) in by_queue {
        let mut rows: Vec<(String, i64)> = Vec::new();
        let mut per_partition: BTreeMap<String, i64> = BTreeMap::new();
        let mut nulls = 0i64;
        for b in buckets {
            for r in b {
                rows.push((r.partition.to_string(), r.offset));
                *per_partition.entry(r.partition.to_string()).or_insert(0) += 1;
                if r.payload.is_none() {
                    nulls += 1;
                }
                all_rows.push((queue.to_string(), r.partition.to_string(), r.offset));
            }
        }
        rows.sort();
        total += rows.len();
        let mut hasher = Sha256::new();
        for (p, o) in &rows {
            hasher.update(format!("{p}|{o}\n").as_bytes());
        }
        queues.insert(
            queue.to_string(),
            serde_json::json!({
                "records": rows.len(),
                "nullPayloads": nulls,
                "partitions": per_partition,
                "digest": hex::encode(hasher.finalize()),
                "spot": spot_check(buckets),
            }),
        );
    }

    all_rows.sort();
    let mut hasher = Sha256::new();
    for (q, p, o) in &all_rows {
        hasher.update(format!("{q}|{p}|{o}\n").as_bytes());
    }

    serde_json::json!({
        "generator": concat!("queen-s3 gen_samples ", env!("CARGO_PKG_VERSION")),
        "layout": "merged",
        "align": "hour",
        "records": total,
        "queues": queues,
        "digestAll": hex::encode(hasher.finalize()),
        "digestSpec": {
            "perQueue": "sha256 over the concatenation of \"<partition>|<offset>\\n\" for every record \
                         of the queue, UTF-8, in ascending (partition, offset) order; partition names \
                         compare as UTF-8 bytes",
            "all": "the same over \"<queue>|<partition>|<offset>\\n\" in ascending (queue, partition, offset) order"
        },
        "columns": {
            "jsonl": ["partition", "offset", "transactionId", "ts", "payload"],
            "parquet": ["partition", "offset", "transaction_id", "ts", "payload"],
            "note": "the JSONL envelope is camelCase and the Parquet envelope is snake_case \
                     (plan §6.4); `ts` is an ISO-8601 string in JSONL and INT64 TIMESTAMP(MICROS,UTC) \
                     in Parquet; `payload` is inline JSON in JSONL and a UTF-8 JSON *string* in Parquet; \
                     there is NO `queue` column in either — it is the `queue=` path key, and Parquet \
                     objects also carry it in the file metadata as `queen.queue`"
        },
        "hive": {
            "queue": LANES.iter().map(|(q, _)| *q).collect::<Vec<_>>(),
            "dt": [FIRST_HOUR.get(0..10).unwrap()],
            "hour": ["10", "11"],
            "note": "`queue` is ONLY a hive key: no row repeats it, so a reader needs hive \
                     partition discovery (or a per-queue prefix) to know which queue a row is from"
        },
        "windows": windows.iter().map(|(k, s, e)| serde_json::json!({
            "k": k, "tStart": s.0, "tEnd": e.0, "tStartIso": s.to_iso(), "tEndIso": e.to_iso(),
        })).collect::<Vec<_>>(),
        "writerPins": {
            "zstdLevel": ZSTD_LEVEL,
            "parquetRowGroupRecords": ROW_GROUP_RECORDS,
        },
        "prefixes": prefixes,
    })
}

/// The first three records of one awkward partition, decoded: what a reader that
/// really parsed the payload must be able to show. `cust 42/eu` because its name
/// is the one most likely to be mangled on the way through a query engine.
fn spot_check(buckets: &[Vec<Record>; 2]) -> serde_json::Value {
    let mut out = Vec::new();
    let want = ["cust 42/eu", "région-eu"];
    for b in buckets {
        for r in b {
            if !want.contains(&&*r.partition) || r.offset >= 3 {
                continue;
            }
            let p: serde_json::Value = match &r.payload {
                Some(p) => serde_json::from_str(p.get()).expect("payload parses"),
                None => serde_json::Value::Null,
            };
            out.push(serde_json::json!({
                "partition": r.partition.to_string(),
                "offset": r.offset,
                "transactionId": r.transaction_id,
                "tsIso": r.ts.to_iso(),
                "tsMicros": r.ts.0,
                "type": p.get("type").and_then(|v| v.as_str()),
                "amount": p.get("amount").and_then(|v| v.as_i64()),
                "customerName": p.pointer("/customer/name").and_then(|v| v.as_str()),
                "note": p.get("note").and_then(|v| v.as_str()),
            }));
        }
    }
    serde_json::Value::Array(out)
}

// ---------------------------------------------------------------------------
// Files
// ---------------------------------------------------------------------------

fn write_object(dir: &Path, key: &str, bytes: &[u8]) {
    let path = dir.join(key);
    fs::create_dir_all(path.parent().expect("a key has a directory")).expect("create key dir");
    fs::write(&path, bytes).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}
