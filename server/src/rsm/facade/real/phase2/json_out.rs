//! JSON answers that carry stored documents as they are stored.
//!
//! `state/get` and `fetch` used to parse every state and payload into a
//! `serde_json::Value`, wrap it with `json!` (which copies what it embeds:
//! `to_value(&expr)`) and print the tree. That held two or three trees of
//! every document at once, 19 to 25 times its size at the peak, and on
//! 2026-10-29 a 65 MB stream state cost the leader about 1.6 GB per read: it
//! ran out of memory. Here each document is checked with the rules `Value`
//! parses by and copied into the answer once. An answer is the JSON the tree
//! printed, its fields in the same order, each document in its stored form.

use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::Value;

use crate::rsm::facade::{RecordFetch, RecordsFetched};
use crate::rsm::planner::timers::iso_us;

/// A JSON document that `serde_json::Value` would parse: numbers in `f64`
/// range, nesting within serde_json's limit, valid escapes in keys and
/// strings. Nothing is kept. `RawValue` alone accepts more than `Value` does
/// (`1e400`, 128 levels, a lone surrogate), so it is not enough to tell
/// whether a stored document goes out as it is.
pub(super) struct Check;

impl<'de> Deserialize<'de> for Check {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(CheckVisitor)
    }
}

struct CheckVisitor;

impl<'de> Visitor<'de> for CheckVisitor {
    type Value = Check;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a JSON value")
    }
    fn visit_bool<E>(self, _: bool) -> Result<Check, E> {
        Ok(Check)
    }
    fn visit_i64<E>(self, _: i64) -> Result<Check, E> {
        Ok(Check)
    }
    fn visit_u64<E>(self, _: u64) -> Result<Check, E> {
        Ok(Check)
    }
    fn visit_f64<E>(self, _: f64) -> Result<Check, E> {
        Ok(Check)
    }
    fn visit_str<E>(self, _: &str) -> Result<Check, E> {
        Ok(Check)
    }
    fn visit_unit<E>(self) -> Result<Check, E> {
        Ok(Check)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Check, A::Error> {
        while seq.next_element::<Check>()?.is_some() {}
        Ok(Check)
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Check, A::Error> {
        while map.next_key::<Check>()?.is_some() {
            map.next_value::<Check>()?;
        }
        Ok(Check)
    }
}

/// `bytes` as one JSON document, without the whitespace around it, when
/// `serde_json::Value` would parse them.
pub(super) fn checked(bytes: &[u8]) -> Option<&str> {
    let text = std::str::from_utf8(bytes).ok()?;
    serde_json::from_str::<Check>(text).ok()?;
    Some(text.trim_matches([' ', '\t', '\n', '\r']))
}

/// A string as serde_json prints one.
pub(super) fn push_str(out: &mut String, s: &str) {
    out.push_str(&serde_json::to_string(s).expect("a str serializes"));
}

/// A stored message payload as `fetch` renders it: `null` when empty, the
/// document when it is one, else a JSON string of its lossy UTF-8 (a payload
/// the broker could not decrypt).
pub(super) fn push_payload(out: &mut String, bytes: &[u8]) {
    if bytes.is_empty() {
        out.push_str("null");
    } else if let Some(doc) = checked(bytes) {
        out.push_str(doc);
    } else {
        push_str(out, &String::from_utf8_lossy(bytes));
    }
}

/// One field of an object [`push_object`] writes.
pub(super) enum Field<'a> {
    /// A small value, printed as the tree prints it.
    Json(Value),
    /// A stored message payload, as [`push_payload`] writes it.
    Payload(&'a [u8]),
    /// A stored document, `null` when it is not one (trace `data`).
    Doc(&'a [u8]),
}

/// An object as `json!` printed it, its keys sorted (serde_json's map is
/// sorted without `preserve_order`) and a repeated key keeping its last
/// value, with every stored payload copied in once instead of parsed into a
/// tree first.
pub(super) fn push_object(out: &mut String, mut fields: Vec<(&str, Field<'_>)>) {
    // Stable: of two equal keys the later one stays last.
    fields.sort_by(|a, b| a.0.cmp(b.0));
    out.push('{');
    let mut first = true;
    for i in 0..fields.len() {
        if fields.get(i + 1).is_some_and(|n| n.0 == fields[i].0) {
            continue;
        }
        if !first {
            out.push(',');
        }
        first = false;
        push_str(out, fields[i].0);
        out.push(':');
        match &fields[i].1 {
            Field::Json(v) => out.push_str(&v.to_string()),
            Field::Payload(b) => push_payload(out, b),
            Field::Doc(b) => out.push_str(checked(b).unwrap_or("null")),
        }
    }
    out.push('}');
}

/// A checked state document's top-level `windowEnd`, read as
/// `Value::get("windowEnd").and_then(Value::as_i64)` reads it, without the
/// tree.
pub(super) fn window_end(doc: &str) -> Option<i64> {
    #[derive(serde::Deserialize)]
    struct Head {
        #[serde(rename = "windowEnd", default)]
        window_end: Option<Value>,
    }
    // A struct also reads from an array, by position; the tree finds no
    // `windowEnd` in anything but an object.
    if !doc.starts_with('{') {
        return None;
    }
    match serde_json::from_str::<Head>(doc) {
        Ok(head) => head.window_end.and_then(|v| v.as_i64()),
        // A repeated key, which the struct refuses: the tree keeps the last.
        Err(_) => serde_json::from_str::<Value>(doc)
            .ok()?
            .get("windowEnd")
            .and_then(Value::as_i64),
    }
}

/// The `state/get` answer, `{"rows":[…],"success":true}`, written row by row
/// as the scan reads them.
pub(super) struct StateAnswer {
    out: String,
    rows: usize,
}

impl StateAnswer {
    pub(super) fn new() -> StateAnswer {
        StateAnswer {
            out: String::from("{\"rows\":["),
            rows: 0,
        }
    }

    /// One stored row, `{"key":…,"updated_at":…,"value":…}` (the tree's
    /// objects were sorted maps). With `ripe_at_or_before`, only a value whose
    /// top-level `windowEnd` is at or before it. A value that is not a
    /// document goes out as `null`, as the tree read it.
    pub(super) fn row(
        &mut self,
        key: &str,
        value: &[u8],
        updated_at_us: i64,
        ripe_at_or_before: Option<i64>,
    ) {
        let doc = checked(value);
        if let Some(cutoff) = ripe_at_or_before {
            if !doc.and_then(window_end).is_some_and(|end| end <= cutoff) {
                return;
            }
        }
        let doc = doc.unwrap_or("null");
        let out = &mut self.out;
        if self.rows > 0 {
            out.push(',');
        }
        self.rows += 1;
        // The row's own fields and the answer's closing `],"success":true}`
        // fit in what is reserved: one byte short and the buffer doubles.
        out.reserve(doc.len() + 2 * key.len() + 128);
        out.push_str("{\"key\":");
        push_str(out, key);
        out.push_str(",\"updated_at\":");
        push_str(out, &iso_us(updated_at_us));
        out.push_str(",\"value\":");
        out.push_str(doc);
        out.push('}');
    }

    pub(super) fn finish(mut self) -> String {
        self.out.push_str("],\"success\":true}");
        self.out
    }
}

/// The `POST /api/v1/fetch` answer, `{"entries":[…]}`, one entry per ask in
/// order, with each entry's and each record's fields in the order the tree
/// printed them.
pub(super) fn fetch_answer(asks: &[RecordFetch], answers: &[RecordsFetched]) -> String {
    // Sized for the whole answer up front (every record's fixed fields are
    // under 96 bytes): a buffer that outgrows its capacity doubles.
    let records: usize = answers
        .iter()
        .flat_map(|a| a.records.iter())
        .map(|r| r.payload.len() + r.txn.as_ref().map_or(0, String::len) + 96)
        .sum();
    let entries: usize = asks
        .iter()
        .map(|a| a.queue.len() + a.partition.len() + 192)
        .sum();
    let mut out = String::with_capacity(records + entries + 16);
    out.push_str("{\"entries\":[");
    for (i, (ask, a)) in asks.iter().zip(answers).enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('{');
        if let Some(error) = a.error {
            out.push_str("\"error\":");
            push_str(&mut out, error);
            out.push(',');
        }
        out.push_str("\"highWatermark\":");
        out.push_str(&a.high_watermark.to_string());
        out.push_str(",\"logStartOffset\":");
        out.push_str(&a.log_start_offset.to_string());
        out.push_str(",\"partition\":");
        push_str(&mut out, &ask.partition);
        out.push_str(",\"queue\":");
        push_str(&mut out, &ask.queue);
        out.push_str(",\"records\":[");
        for (j, rec) in a.records.iter().enumerate() {
            if j > 0 {
                out.push(',');
            }
            out.push_str("{\"offset\":");
            out.push_str(&rec.offset.to_string());
            out.push_str(",\"payload\":");
            push_payload(&mut out, &rec.payload);
            out.push_str(",\"transactionId\":");
            push_str(&mut out, rec.txn.as_deref().unwrap_or_default());
            out.push_str(",\"ts\":");
            push_str(&mut out, &iso_us(rec.created_at_us));
            out.push('}');
        }
        out.push_str("]}");
    }
    out.push_str("]}");
    out
}

/// A session window as a tracing app keeps it: `{"acc": trace}` with
/// `steps` small objects, each a few hundred bytes.
#[cfg(test)]
pub(super) fn trace_state(steps: usize) -> Value {
    let mut all = serde_json::Map::new();
    for i in 0..steps {
        let id = format!("{:08x}-0000-4000-8000-{:012x}", i, i * 7919);
        all.insert(
            id.clone(),
            serde_json::json!({
                "step_id": id, "first_at": "2026-10-29T17:34:01.137Z",
                "last_at": "2026-10-29T17:34:02.137Z", "step_number": i,
                "name": format!("step {}", i % 50), "status": "completed",
                "input": {"account": 42, "sku": "A3", "amount": 129.5, "currency": "EUR"},
                "output": {"ok": true, "http_status": 200, "latency_ms": 42},
            }),
        );
    }
    serde_json::json!({"acc": {"trace_id": "t", "status": "running", "steps": Value::Object(all)}})
}

#[cfg(test)]
mod tests {
    #[test]
    fn push_object_prints_what_the_tree_printed() {
        let payload = br#"{"b":1,"a":[true,null]}"#;
        let mut out = String::new();
        super::push_object(
            &mut out,
            vec![
                ("status", super::Field::Json(serde_json::json!("pending"))),
                ("data", super::Field::Payload(payload)),
                ("id", super::Field::Json(serde_json::json!("x"))),
                ("status", super::Field::Json(serde_json::json!("completed"))),
                ("busStatus", super::Field::Json(serde_json::json!({"z":1,"a":2}))),
            ],
        );
        let tree = serde_json::json!({
            "status": "completed",
            "data": serde_json::from_slice::<serde_json::Value>(payload).unwrap(),
            "id": "x",
            "busStatus": {"z":1,"a":2},
        });
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&out).unwrap(),
            tree
        );
        assert!(out.starts_with("{\"busStatus\":{\"a\":2,\"z\":1},\"data\":{\"b\":1"));
    }

    use super::*;
    use serde_json::json;

    /// `payload_json` as `fetch` used it before json_out.
    fn old_payload_json(b: &[u8]) -> Value {
        if b.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(b)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(b).into_owned()))
        }
    }

    /// The `fetch` answer as the route printed it before json_out.
    fn old_fetch_answer(asks: &[RecordFetch], answers: &[RecordsFetched]) -> String {
        let entries: Vec<Value> = asks
            .iter()
            .cloned()
            .zip(answers.iter().cloned())
            .map(|(ask, a)| {
                let mut entry = json!({
                    "queue": ask.queue,
                    "partition": ask.partition,
                    "records": a.records.into_iter().map(|rec| json!({
                        "offset": rec.offset,
                        "transactionId": rec.txn.unwrap_or_default(),
                        "payload": old_payload_json(&rec.payload),
                        "ts": iso_us(rec.created_at_us),
                    })).collect::<Vec<_>>(),
                    "highWatermark": a.high_watermark,
                    "logStartOffset": a.log_start_offset,
                });
                if let Some(error) = a.error {
                    entry["error"] = Value::from(error);
                }
                entry
            })
            .collect();
        json!({ "entries": entries }).to_string()
    }

    /// The `state/get` answer as the route printed it before json_out.
    fn old_state_answer(rows: &[(&str, &[u8], i64)], ripe_at_or_before: Option<i64>) -> String {
        let mut out = Vec::new();
        for (key, value, at) in rows {
            let value = serde_json::from_slice::<Value>(value).unwrap_or(Value::Null);
            let ripe = ripe_at_or_before.is_none_or(|cutoff| {
                value
                    .get("windowEnd")
                    .and_then(Value::as_i64)
                    .is_some_and(|end| end <= cutoff)
            });
            if ripe {
                out.push(json!({"key":key,"value":value,"updated_at":iso_us(*at)}));
            }
        }
        json!({"success":true,"rows":out}).to_string()
    }

    fn new_state_answer(rows: &[(&str, &[u8], i64)], ripe_at_or_before: Option<i64>) -> String {
        let mut answer = StateAnswer::new();
        for (key, value, at) in rows {
            answer.row(key, value, *at, ripe_at_or_before);
        }
        answer.finish()
    }

    /// Stored documents of every kind the tests know of, the strange ones too.
    fn docs() -> Vec<Vec<u8>> {
        let mut d: Vec<Vec<u8>> = [
            &br#"{"a":1,"b":[1,2,{"c":"d"}],"e":null,"f":true,"g":-1.5}"#[..],
            br#"{"z":1,"a":2}"#,
            br#"{ "a" : 1 ,  "b":[ 1 , 2 ] }"#,
            b"  {\"a\":1}\n",
            br#""a string""#,
            br#"[]"#,
            br#"{}"#,
            br#"null"#,
            br#"0"#,
            br#"12345678901234567890123"#,
            br#"{"e":1e3,"f":1.50,"g":-0.0}"#,
            br#"{"a":1,"a":2}"#,
            br#"{"windowEnd":5,"acc":{"n":1}}"#,
            br#"{"windowEnd":5.0}"#,
            br#"{"windowEnd":"5"}"#,
            br#"{"windowEnd":null}"#,
            br#"{"windowEnd":1,"windowEnd":9}"#,
            br#"{"inner":{"windowEnd":3}}"#,
            br#"[5]"#,
            br#"{"windowEnd":18446744073709551615}"#,
            br#"{"windowEnd":-3}"#,
            // What `Value` refuses, and `RawValue` alone would take.
            br#"1e400"#,
            br#"{"x":-1e400}"#,
            br#""\ud800""#,
            br#"{"\ud800":1}"#,
            // Not JSON at all.
            br#"{"a":1,}"#,
            b"NaN",
            b"",
            b"\xff\xfe binary",
            b"\"a\x01b\"",
            br#"{"a":1} trailing"#,
        ]
        .iter()
        .map(|b| b.to_vec())
        .collect();
        // Escapes the tree printed as characters: \u00e9, a surrogate pair, and
        // the escapes it keeps.
        let bs = '\\';
        d.push(
            format!(
                "{{\"a\":\"\u{e9}\",\"b\":\"{bs}u00e9\",\"c\":\"{bs}ud83d{bs}ude00\",\"d\":\"{bs}\"{bs}{bs}{bs}/{bs}n\"}}"
            )
            .into_bytes(),
        );
        d.push(format!("{}{}", "[".repeat(127), "]".repeat(127)).into_bytes());
        d.push(format!("{}{}", "[".repeat(128), "]".repeat(128)).into_bytes());
        d.push(format!("{}1{}", "{\"a\":".repeat(200), "}".repeat(200)).into_bytes());
        d
    }

    /// Printed by the tree exactly as stored: compact, keys sorted, each
    /// number and string in serde_json's spelling (every state a cycle stored
    /// before json_out), or not a document at all.
    fn canonical(doc: &[u8]) -> bool {
        match serde_json::from_slice::<Value>(doc) {
            Ok(v) => serde_json::to_vec(&v).unwrap() == doc,
            Err(_) => true,
        }
    }

    fn parsed(body: &str) -> Value {
        serde_json::from_str(body).unwrap_or_else(|e| panic!("not JSON ({e}): {body}"))
    }

    /// Nested deeper than an answer can wrap and serde_json still parse
    /// (128 levels): such a document is compared byte for byte only.
    fn deep(doc: &[u8]) -> bool {
        doc.iter().filter(|c| matches!(c, b'[' | b'{')).count() > 100
    }

    #[test]
    fn checked_takes_exactly_what_value_takes() {
        for doc in docs() {
            assert_eq!(
                checked(&doc).is_some(),
                serde_json::from_slice::<Value>(&doc).is_ok(),
                "{:?}",
                String::from_utf8_lossy(&doc)
            );
        }
    }

    #[test]
    fn the_fetch_answer_is_the_tree_answer() {
        let mut docs = docs();
        let all = docs.clone();
        docs.retain(|d| !deep(d));
        let asks = vec![
            RecordFetch {
                queue: "events".into(),
                partition: "trace \"1\"".into(),
                offset: 0,
                max_bytes: 1 << 20,
            },
            RecordFetch {
                queue: "q".into(),
                partition: "gone".into(),
                offset: 9,
                max_bytes: 1 << 20,
            },
        ];
        let answers = vec![
            RecordsFetched {
                records: docs
                    .iter()
                    .enumerate()
                    .map(|(i, d)| crate::rsm::facade::RecordRead {
                        offset: i as u64,
                        created_at_us: 1_761_759_241_000_000 + i as i64,
                        payload: bytes::Bytes::from(d.clone()),
                        txn: (i % 3 != 0).then(|| format!("step:{i}:\u{e9}")),
                    })
                    .collect(),
                high_watermark: docs.len() as u64,
                log_start_offset: 0,
                error: None,
            },
            RecordsFetched {
                records: Vec::new(),
                high_watermark: 4,
                log_start_offset: 2,
                error: Some("OFFSET_OUT_OF_RANGE"),
            },
        ];
        let new = fetch_answer(&asks, &answers);
        let old = old_fetch_answer(&asks, &answers);
        assert_eq!(parsed(&new), parsed(&old));

        // Byte for byte, whenever the tree printed a payload as it is stored.
        let only = |keep: &dyn Fn(&[u8]) -> bool| {
            let mut a = answers.clone();
            a[0].records.retain(|r| keep(&r.payload));
            a
        };
        let same = only(&|d| canonical(d));
        assert_eq!(fetch_answer(&asks, &same), old_fetch_answer(&asks, &same));
        // Otherwise the payload goes out as the producer sent it.
        let sent = only(&|d| d == br#"{"z":1,"a":2}"#);
        assert!(fetch_answer(&asks, &sent).contains(r#""payload":{"z":1,"a":2}"#));
        // The deep ones, as stored or as the tree's fallback, byte for byte.
        let mut deep_answers = answers.clone();
        deep_answers[0].records = all
            .iter()
            .filter(|d| deep(d))
            .enumerate()
            .map(|(i, d)| crate::rsm::facade::RecordRead {
                offset: i as u64,
                created_at_us: 1_761_759_241_000_000,
                payload: bytes::Bytes::from(d.clone()),
                txn: None,
            })
            .collect();
        assert_eq!(deep_answers[0].records.len(), 3);
        assert_eq!(
            fetch_answer(&asks, &deep_answers),
            old_fetch_answer(&asks, &deep_answers)
        );
    }

    #[test]
    fn the_state_answer_is_the_tree_answer() {
        let all = docs();
        let docs: Vec<Vec<u8>> = all.iter().filter(|d| !deep(d)).cloned().collect();
        let rows: Vec<(&str, &[u8], i64)> = docs
            .iter()
            .enumerate()
            .map(|(i, d)| {
                let key: &str = ["count", "sess:60\u{1f}open\u{1f}7", "k\"q"][i % 3];
                (key, d.as_slice(), 1_761_759_241_000_000 + i as i64)
            })
            .collect();
        for cutoff in [
            None,
            Some(-4),
            Some(-3),
            Some(4),
            Some(5),
            Some(9),
            Some(i64::MAX),
        ] {
            let new = new_state_answer(&rows, cutoff);
            let old = old_state_answer(&rows, cutoff);
            assert_eq!(parsed(&new), parsed(&old), "cutoff {cutoff:?}");
            let same: Vec<_> = rows.iter().copied().filter(|r| canonical(r.1)).collect();
            assert_eq!(
                new_state_answer(&same, cutoff),
                old_state_answer(&same, cutoff),
                "cutoff {cutoff:?}"
            );
        }
        assert_eq!(new_state_answer(&[], None), old_state_answer(&[], None));
        for d in all.iter().filter(|d| deep(d)) {
            let rows = [("deep", d.as_slice(), 1)];
            assert_eq!(new_state_answer(&rows, None), old_state_answer(&rows, None));
        }
    }

    #[test]
    fn window_end_reads_as_the_tree_did() {
        for doc in docs() {
            let Some(text) = checked(&doc) else { continue };
            let tree = serde_json::from_slice::<Value>(&doc).unwrap();
            assert_eq!(
                window_end(text),
                tree.get("windowEnd").and_then(Value::as_i64),
                "{text}"
            );
        }
    }

    #[test]
    fn a_large_state_is_answered_in_about_its_size() {
        let state = serde_json::to_vec(&super::trace_state(20_000)).unwrap();
        assert!(state.len() > 6 << 20, "{}", state.len());
        let (body, peak) = crate::test_alloc::peak_during(|| {
            new_state_answer(&[("sess:60\u{1f}open\u{1f}7", &state, 1)], None)
        });
        assert!(
            body.ends_with(r#"}}],"success":true}"#),
            "{}",
            &body[body.len() - 40..]
        );
        // The tree took 25 times the state (2026-10-29, 65 MB -> 1.6 GB).
        if let Some(peak) = peak {
            eprintln!(
                "state/get: {peak} bytes at the peak for a {} byte state",
                state.len()
            );
            assert!(
                peak < state.len() * 11 / 10,
                "{peak} bytes at the peak for a {} byte state",
                state.len()
            );
        }
    }

    #[test]
    fn a_full_fetch_answer_is_written_in_about_its_size() {
        let mut records = Vec::new();
        let mut bytes = 0;
        while bytes < 8 << 20 {
            let i = records.len();
            let payload = serde_json::to_vec(&json!({
                "kind": "step", "trace_id": "t", "at": "2026-10-29T17:34:01.137Z",
                "step_id": format!("{i:08x}"),
                "body": {"step_number": i, "status": "completed", "input": {"amount": 129.5}},
            }))
            .unwrap();
            bytes += payload.len();
            records.push(crate::rsm::facade::RecordRead {
                offset: i as u64,
                created_at_us: 1_761_759_241_000_000,
                payload: bytes::Bytes::from(payload),
                txn: Some(format!("step:{i}:start")),
            });
        }
        let asks = [RecordFetch {
            queue: "events".into(),
            partition: "t".into(),
            offset: 0,
            max_bytes: 8 << 20,
        }];
        let answers = [RecordsFetched {
            records,
            high_watermark: 0,
            log_start_offset: 0,
            error: None,
        }];
        let (body, peak) = crate::test_alloc::peak_during(|| fetch_answer(&asks, &answers));
        assert!(body.len() > bytes);
        // The tree took 19 times the payloads.
        if let Some(peak) = peak {
            eprintln!("fetch: {peak} bytes at the peak for {bytes} payload bytes");
            assert!(
                peak < bytes * 2,
                "{peak} bytes at the peak for {bytes} payload bytes"
            );
        }
    }
}
