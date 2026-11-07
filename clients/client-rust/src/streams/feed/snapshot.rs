//! Snapshots of a feed stream's state, in KV, in chunks (feed/snapshot.js).
//!
//! A snapshot is the state of every stateful step at one position, as
//! LAYERS: a full one (every entry), then deltas (the entries set or deleted
//! since the layer before), applied in order. A layer is written in PIECES:
//! a few thousand entries of one step as JSON (`{"v":2,"s","k","set","del"}`,
//! about 256 KB), deflated (raw, RFC 1951), base64, cut into KV values of at
//! most 48,000 characters (a value with `m: 1` goes on in the next).
//!
//! ```text
//! queen-feed / <id>                    the position record: { snapshot: { layers: [{ sid, chunks, bytes, full? }], position, clock, metas, units } }
//! queen-feed / <id>#snap#<sid>#<n>     chunk n of layer sid: { d: '<base64>', m?: 1 }
//! ```
//!
//! Format 1 (one JSON of the whole state across the chunks of one id) is
//! still read; the next snapshot after it is a full layer.

use std::io::{Read, Write};

use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use flate2::Compression;
use serde_json::{json, Map, Value};

use crate::js;
use crate::locks::Lock;
use crate::Queen;
use queen_protocol::kv::{Expiry, KvOperation};

use super::runner::FeedErr;

pub(crate) const SNAPSHOT_FORMAT: i64 = 1;
pub(crate) const LAYER_FORMAT: i64 = 2;
pub(crate) const DEFAULT_CHUNK_CHARS: usize = 48_000;
/// JSON of one piece before deflate.
pub(crate) const DEFAULT_PIECE_BYTES: usize = 256 * 1024;
/// At most this many layers: a full one and deltas.
pub(crate) const MAX_LAYERS: usize = 16;
// A transaction carries at most 64 KV operations; the lock's guard is one.
const OPS_PER_TX: usize = 60;
// Keys asked per read: about 3 MB of chunks, under the 4 MiB read budget.
const KEYS_PER_READ: usize = 64;

// ------------------------------------------------------------------ base64

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Standard base64 with padding, as `Buffer.toString('base64')`.
pub(crate) fn b64_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = match chunk.len() {
            3 => (u32::from(chunk[0]) << 16) | (u32::from(chunk[1]) << 8) | u32::from(chunk[2]),
            2 => (u32::from(chunk[0]) << 16) | (u32::from(chunk[1]) << 8),
            _ => u32::from(chunk[0]) << 16,
        };
        out.push(B64[(n >> 18) as usize & 63] as char);
        out.push(B64[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            B64[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            B64[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// Base64 read as node reads it: the standard and the URL alphabet, padding
/// or none, anything else skipped.
pub(crate) fn b64_decode(text: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(text.len() / 4 * 3);
    let mut acc: u32 = 0;
    let mut bits = 0;
    for c in text.bytes() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' | b'-' => 62,
            b'/' | b'_' => 63,
            b'=' => break,
            _ => continue,
        };
        acc = (acc << 6) | u32::from(v);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    out
}

pub(crate) fn deflate_raw(bytes: &[u8]) -> Vec<u8> {
    let mut e = DeflateEncoder::new(Vec::new(), Compression::default());
    // Writing to a Vec does not fail.
    let _ = e.write_all(bytes);
    e.finish().unwrap_or_default()
}

pub(crate) fn inflate_raw(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let mut d = DeflateDecoder::new(bytes);
    let mut out = Vec::new();
    d.read_to_end(&mut out)
        .map_err(|e| format!("feed snapshot: inflate: {e}"))?;
    Ok(out)
}

// ------------------------------------------------------------------ layers

/// One layer a pointer names.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Layer {
    pub sid: String,
    pub chunks: usize,
    pub bytes: u64,
    pub full: bool,
    /// Format 1 (`v: 1`): one JSON of the whole state.
    pub v1: bool,
}

impl Layer {
    pub fn to_value(&self) -> Value {
        let mut m = Map::new();
        m.insert("sid".into(), Value::String(self.sid.clone()));
        m.insert("chunks".into(), Value::from(self.chunks));
        m.insert("bytes".into(), Value::from(self.bytes));
        if self.full {
            m.insert("full".into(), Value::Bool(true));
        }
        if self.v1 {
            m.insert("v".into(), Value::from(1));
        }
        Value::Object(m)
    }
}

fn int(v: Option<&Value>) -> usize {
    js::as_num(v)
        .filter(|x| *x > 0.0)
        .map(|x| x as usize)
        .unwrap_or(0)
}

/// The layers a snapshot pointer names, oldest first: format 2 lists them;
/// format 1 is one full layer of its own (`v: 1`); an empty state names none.
pub(crate) fn layers_of(snap: Option<&Value>) -> Vec<Layer> {
    let Some(snap) = snap.filter(|s| s.is_object()) else {
        return Vec::new();
    };
    if let Some(Value::Array(list)) = snap.get("layers") {
        return list
            .iter()
            .map(|l| Layer {
                sid: js::string_of(l.get("sid")),
                chunks: int(l.get("chunks")),
                bytes: int(l.get("bytes")) as u64,
                full: js::truthy(l.get("full")),
                v1: js::as_num(l.get("v")) == Some(1.0),
            })
            .collect();
    }
    if js::truthy(snap.get("id")) && int(snap.get("chunks")) > 0 {
        return vec![Layer {
            sid: js::string_of(snap.get("id")),
            chunks: int(snap.get("chunks")),
            bytes: int(snap.get("bytes")) as u64,
            full: true,
            v1: true,
        }];
    }
    Vec::new()
}

/// A pointer that names `layers` (sums kept for the metrics and for the old
/// readers of `id` / `chunks`). `units`: how many closes the stream had made
/// when the state was as the layers hold it.
pub(crate) fn pointer_of(
    layers: &[Layer],
    position: Value,
    clock: f64,
    metas: Value,
    units: Option<u64>,
) -> Value {
    let chunks: usize = layers.iter().map(|l| l.chunks).sum();
    let bytes: u64 = layers.iter().map(|l| l.bytes).sum();
    let mut p = Map::new();
    p.insert(
        "id".into(),
        layers
            .last()
            .map(|l| Value::String(l.sid.clone()))
            .unwrap_or(Value::Null),
    );
    p.insert("chunks".into(), Value::from(chunks));
    p.insert("bytes".into(), Value::from(bytes));
    p.insert(
        "layers".into(),
        Value::Array(layers.iter().map(Layer::to_value).collect()),
    );
    p.insert("position".into(), position);
    p.insert("clock".into(), js::num(clock));
    p.insert("metas".into(), metas);
    if let Some(u) = units {
        p.insert("units".into(), Value::from(u));
    }
    Value::Object(p)
}

pub(crate) fn chunk_prefix(id: &str) -> String {
    format!("{id}#snap#")
}

pub(crate) fn chunk_key(id: &str, sid: &str, n: usize) -> String {
    format!("{}{sid}#{n}", chunk_prefix(id))
}

/// Every chunk key of the layers.
pub(crate) fn layer_keys(id: &str, layers: &[Layer]) -> Vec<String> {
    let mut out = Vec::new();
    for l in layers {
        for n in 0..l.chunks {
            out.push(chunk_key(id, &l.sid, n));
        }
    }
    out
}

fn base36(mut n: u64) -> String {
    if n == 0 {
        return "0".into();
    }
    let digits = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut out = Vec::new();
    while n > 0 {
        out.push(digits[(n % 36) as usize]);
        n /= 36;
    }
    out.reverse();
    String::from_utf8(out).unwrap_or_default()
}

/// A new snapshot id: never one an earlier attempt or another process used.
pub(crate) fn new_snapshot_id() -> String {
    format!(
        "{}{:08x}",
        base36(js::now_ms() as u64),
        rand::random::<u32>()
    )
}

/// A dump (format 1) as chunks: `(chunks, bytes)` (bytes: the JSON before deflate).
#[cfg(test)]
pub(crate) fn encode_snapshot(dump: &Value, chunk_chars: usize) -> (Vec<String>, usize) {
    let mut m = Map::new();
    m.insert("v".into(), Value::from(SNAPSHOT_FORMAT));
    if let Some(o) = dump.as_object() {
        for (k, v) in o {
            m.insert(k.clone(), v.clone());
        }
    }
    let json = js::stringify(&Value::Object(m));
    let b64 = b64_encode(&deflate_raw(json.as_bytes()));
    let size = chunk_chars.max(1024);
    (split_chars(&b64, size), json.len())
}

fn split_chars(s: &str, size: usize) -> Vec<String> {
    // Base64 is ASCII: bytes are characters.
    s.as_bytes()
        .chunks(size)
        .map(|c| String::from_utf8_lossy(c).into_owned())
        .collect()
}

pub(crate) fn decode_snapshot(chunks: &[String]) -> Result<Value, FeedErr> {
    let raw = inflate_raw(&b64_decode(&chunks.concat())).map_err(FeedErr::msg)?;
    let dump: Value =
        serde_json::from_slice(&raw).map_err(|e| FeedErr::msg(format!("feed snapshot: {e}")))?;
    if js::as_num(dump.get("v")) != Some(SNAPSHOT_FORMAT as f64) {
        return Err(FeedErr::msg(format!(
            "feed snapshot: format {} is not one this client reads ({SNAPSHOT_FORMAT})",
            js::string_of(dump.get("v"))
        )));
    }
    Ok(dump)
}

async fn commit_kv(
    queen: &Queen,
    lock: Option<&Lock>,
    ops: Vec<KvOperation>,
) -> Result<(), FeedErr> {
    let mut tx = queen.transaction();
    if let Some(l) = lock {
        tx = tx.guard(l);
    }
    for op in ops {
        tx = tx.kv(op);
    }
    match tx.commit_outcome().await.map_err(FeedErr::from)? {
        crate::transaction::TxOutcome::LockNotHeld(name) => Err(FeedErr::lock_not_held(&name)),
        crate::transaction::TxOutcome::Answered(r) if !r.success => {
            let reason = r.reason.clone().unwrap_or_else(|| "unknown".into());
            let mut e = FeedErr::msg(format!(
                "feed snapshot: a KV write was refused ({reason}): {}",
                r.error.as_deref().unwrap_or("")
            ));
            e.reason = Some(reason);
            Err(e)
        }
        _ => Ok(()),
    }
}

/// Writes one layer `sid`, piece by piece; nothing points to it until the
/// position does.
pub(crate) struct LayerWriter<'a> {
    queen: &'a Queen,
    ns: &'a str,
    id: &'a str,
    sid: &'a str,
    lock: Option<&'a Lock>,
    size: usize,
    pub room: usize,
    pub chunks: usize,
    pub bytes: usize,
    pub entries: usize,
    batch: Vec<(usize, Value)>,
}

impl<'a> LayerWriter<'a> {
    pub fn new(
        queen: &'a Queen,
        ns: &'a str,
        id: &'a str,
        sid: &'a str,
        lock: Option<&'a Lock>,
        chunk_chars: usize,
        piece_bytes: usize,
    ) -> Self {
        Self {
            queen,
            ns,
            id,
            sid,
            lock,
            size: chunk_chars.max(1024),
            room: piece_bytes.max(1024),
            chunks: 0,
            bytes: 0,
            entries: 0,
            batch: Vec::new(),
        }
    }

    async fn send(&mut self) -> Result<(), FeedErr> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let part = std::mem::take(&mut self.batch);
        let mut ops = Vec::with_capacity(part.len());
        for (n, value) in part {
            ops.push(
                KvOperation::put(
                    self.ns,
                    chunk_key(self.id, self.sid, n),
                    value,
                    Expiry::Forever,
                )
                .map_err(FeedErr::msg)?,
            );
        }
        commit_kv(self.queen, self.lock, ops).await
    }

    /// One piece: `set` holds `JSON.stringify([key, value])` texts, `del` `JSON.stringify(key)`.
    pub async fn piece(
        &mut self,
        s: usize,
        kind: &str,
        set: &[String],
        del: &[String],
    ) -> Result<(), FeedErr> {
        let json = format!(
            "{{\"v\":{LAYER_FORMAT},\"s\":{s},\"k\":{},\"set\":[{}],\"del\":[{}]}}",
            js::string_json(kind),
            set.join(","),
            del.join(",")
        );
        self.bytes += json.len();
        let b64 = b64_encode(&deflate_raw(json.as_bytes()));
        let parts = split_chars(&b64, self.size);
        let last = parts.len().saturating_sub(1);
        for (i, d) in parts.into_iter().enumerate() {
            let value = if i < last {
                json!({ "d": d, "m": 1 })
            } else {
                json!({ "d": d })
            };
            self.batch.push((self.chunks, value));
            self.chunks += 1;
            if self.batch.len() >= OPS_PER_TX {
                self.send().await?;
            }
        }
        tokio::task::yield_now().await;
        Ok(())
    }

    pub async fn finish(mut self) -> Result<(usize, usize, usize), FeedErr> {
        self.send().await?;
        Ok((self.chunks, self.bytes, self.entries))
    }
}

/// Builds the pieces of one step's entries: `set` / `del` texts until `room`.
#[derive(Default)]
pub(crate) struct PieceBuf {
    pub set: Vec<String>,
    pub del: Vec<String>,
    pub used: usize,
}

impl PieceBuf {
    /// Add one entry (`None`: a delete); true when the piece is full. The
    /// limit is in UTF-8 bytes (`Buffer.byteLength`), as the JSON goes out.
    pub fn add(&mut self, key: &str, value: Option<&Value>, room: usize) -> bool {
        match value {
            None => {
                let j = js::string_json(key);
                self.used += j.len();
                self.del.push(j);
            }
            Some(v) => {
                let mut j = String::from("[");
                js::write_string(&mut j, key);
                j.push(',');
                js::write_value(&mut j, v);
                j.push(']');
                self.used += j.len();
                self.set.push(j);
            }
        }
        self.used >= room
    }

    pub fn is_empty(&self) -> bool {
        self.set.is_empty() && self.del.is_empty()
    }
}

/// A piece as read: `{ s, k, set, del }`.
pub(crate) struct Piece {
    pub s: usize,
    pub k: String,
    pub set: Vec<Value>,
    pub del: Vec<Value>,
}

/// Read layer `sid` (`count` chunks), piece by piece, in order, handing each to `on_piece`.
pub(crate) async fn read_layer(
    queen: &Queen,
    ns: &str,
    id: &str,
    sid: &str,
    count: usize,
    on_piece: &mut (dyn FnMut(Piece) + Send),
) -> Result<(), FeedErr> {
    let mut parts: Vec<String> = Vec::new();
    let mut i = 0;
    while i < count {
        let to = count.min(i + KEYS_PER_READ);
        let values = read_values(queen, ns, id, sid, i, to, count).await?;
        for v in values {
            parts.push(
                v.get("d")
                    .and_then(|d| d.as_str())
                    .unwrap_or("")
                    .to_string(),
            );
            if js::truthy(v.get("m")) {
                continue;
            }
            let raw = inflate_raw(&b64_decode(&parts.concat())).map_err(FeedErr::msg)?;
            parts.clear();
            let p: Value = serde_json::from_slice(&raw)
                .map_err(|e| FeedErr::msg(format!("feed snapshot: {e}")))?;
            if js::as_num(p.get("v")) != Some(LAYER_FORMAT as f64) {
                return Err(FeedErr::fatal(
                    format!(
                        "feed snapshot {sid} of {id}: a piece of format {} (this client reads {LAYER_FORMAT})",
                        js::string_of(p.get("v"))
                    ),
                    "feed_snapshot_format",
                ));
            }
            on_piece(Piece {
                s: js::as_num(p.get("s"))
                    .map(|x| x as usize)
                    .unwrap_or(usize::MAX),
                k: js::string_of(p.get("k")),
                set: p
                    .get("set")
                    .and_then(|x| x.as_array())
                    .cloned()
                    .unwrap_or_default(),
                del: p
                    .get("del")
                    .and_then(|x| x.as_array())
                    .cloned()
                    .unwrap_or_default(),
            });
            tokio::task::yield_now().await;
        }
        i = to;
    }
    if !parts.is_empty() {
        return Err(FeedErr::fatal(
            format!("feed snapshot {sid} of {id}: its last chunk says more follows"),
            "feed_snapshot_missing",
        ));
    }
    Ok(())
}

/// Chunks `from` to `to` (exclusive) of layer `sid`, as their values, in order.
async fn read_values(
    queen: &Queen,
    ns: &str,
    id: &str,
    sid: &str,
    from: usize,
    to: usize,
    count: usize,
) -> Result<Vec<Value>, FeedErr> {
    let mut out: Vec<Option<Value>> = vec![None; to - from];
    let mut ask: Vec<String> = (from..to).map(|n| chunk_key(id, sid, n)).collect();
    let mut round = 0;
    while !ask.is_empty() {
        let r = queen
            .kv()
            .get_many(ns, ask.clone())
            .await
            .map_err(FeedErr::from)?;
        let mut got = std::collections::HashSet::new();
        for row in r.rows() {
            let n: usize = row.key[row.key.rfind('#').map_or(0, |i| i + 1)..]
                .parse()
                .unwrap_or(usize::MAX);
            if n >= from && n < to {
                out[n - from] = row
                    .value
                    .as_ref()
                    .filter(|v| v.get("d").is_some_and(|d| d.is_string()))
                    .cloned();
            }
            got.insert(row.key.clone());
        }
        let missing = r.missing();
        if !missing.is_empty() {
            return Err(FeedErr::fatal(
                format!(
                    "feed snapshot {sid} of {id} lacks {} of its {count} chunks (first: {}): the state cannot be loaded; \
                     start the stream over with {{ reset: true }}",
                    missing.len(),
                    missing[0]
                ),
                "feed_snapshot_missing",
            ));
        }
        let left: Vec<String> = ask.iter().filter(|k| !got.contains(*k)).cloned().collect();
        if left.len() == ask.len() || round > 64 {
            return Err(FeedErr::msg(format!(
                "feed snapshot: the KV read of {id}'s chunks came back empty and cut"
            )));
        }
        ask = left;
        round += 1;
    }
    let mut values = Vec::with_capacity(out.len());
    for (n, v) in out.into_iter().enumerate() {
        match v {
            Some(v) => values.push(v),
            None => {
                return Err(FeedErr::fatal(
                    format!(
                        "feed snapshot {sid} of {id}: chunk {} is not a chunk",
                        from + n
                    ),
                    "feed_snapshot_missing",
                ))
            }
        }
    }
    Ok(values)
}

/// The chunks of a format-1 snapshot `sid`, in order.
pub(crate) async fn read_chunks(
    queen: &Queen,
    ns: &str,
    id: &str,
    sid: &str,
    count: usize,
) -> Result<Vec<String>, FeedErr> {
    let mut out = Vec::with_capacity(count);
    let mut i = 0;
    while i < count {
        let to = count.min(i + KEYS_PER_READ);
        for v in read_values(queen, ns, id, sid, i, to, count).await? {
            out.push(
                v.get("d")
                    .and_then(|d| d.as_str())
                    .unwrap_or("")
                    .to_string(),
            );
        }
        i = to;
    }
    Ok(out)
}

/// Delete keys, in transactions of at most 60 deletes.
pub(crate) async fn delete_keys(
    queen: &Queen,
    ns: &str,
    keys: &[String],
    lock: Option<&Lock>,
) -> Result<(), FeedErr> {
    for part in keys.chunks(OPS_PER_TX) {
        let ops = part
            .iter()
            .map(|k| KvOperation::delete(ns, k.clone()))
            .collect();
        commit_kv(queen, lock, ops).await?;
    }
    Ok(())
}

/// The chunk keys of `id` in KV whose layer is not one of `keep`.
pub(crate) async fn stray_chunk_keys(
    queen: &Queen,
    ns: &str,
    id: &str,
    keep: &std::collections::HashSet<String>,
) -> Result<Vec<String>, FeedErr> {
    let prefix = chunk_prefix(id);
    let mut out = Vec::new();
    let mut after: Option<String> = None;
    for _ in 0..1000 {
        let mut q = queen.kv().get_prefix(ns, &prefix).keys_only().limit(1000);
        if let Some(a) = &after {
            q = q.after(a);
        }
        let r = q.send().await.map_err(FeedErr::from)?;
        for row in r.rows() {
            let key = &row.key;
            let sid = key
                .get(prefix.len()..key.rfind('#').unwrap_or(prefix.len()).max(prefix.len()))
                .unwrap_or("");
            if !keep.contains(sid) {
                out.push(key.clone());
            }
        }
        match (r.truncated(), r.next_after.clone()) {
            (true, Some(n)) if !n.is_empty() => after = Some(n),
            _ => break,
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_round_trips_and_reads_leniently() {
        for len in 0..40 {
            let bytes: Vec<u8> = (0..len).map(|i| (i * 37 % 256) as u8).collect();
            let e = b64_encode(&bytes);
            assert_eq!(b64_decode(&e), bytes);
            assert_eq!(
                b64_decode(e.trim_end_matches('=')),
                bytes,
                "without padding"
            );
        }
        assert_eq!(b64_encode(b"hello"), "aGVsbG8=");
        assert_eq!(b64_decode("aGVs\nbG8"), b"hello");
    }

    #[test]
    fn snapshots_encode_and_decode() {
        let dump = json!({ "steps": [{ "name": "s", "kind": "window", "entries": [["a", { "x": "é".repeat(3000) }], ["b", [1, 2, null]]] }] });
        let (chunks, bytes) = encode_snapshot(&dump, 1024);
        assert!(!chunks.is_empty());
        assert!(bytes > 3000);
        assert_eq!(decode_snapshot(&chunks).unwrap()["steps"], dump["steps"]);
    }

    #[test]
    fn a_piece_is_deflate_raw_that_node_reads() {
        // Written by node's `zlib.deflateRawSync(...).toString('base64')`.
        let node = "PYsxCgIxEAD/MnWKPQuL/cqZ4kjWQyIJeIlBQv4uKljNFDODJ3pyHKg4Ekq/5Vg6jsMquq70SxNZrvJDwg2SvVDSJ6rbo35fyxE9i4g4YrO/byGgg1Barugyp/eOaHd0ZS/Z8PMN";
        let raw = inflate_raw(&b64_decode(node)).unwrap();
        let text = r#"{"v":2,"s":0,"k":"window","set":[["w\u001f0\u001fk",{"key":"k","start":0,"end":60000,"due":60000,"acc":{"count":1}}]],"del":["gone"]}"#;
        assert_eq!(std::str::from_utf8(&raw).unwrap(), text);
        // And what this writes, node reads (the same text, deflated here).
        let mine = b64_encode(&deflate_raw(text.as_bytes()));
        assert_eq!(inflate_raw(&b64_decode(&mine)).unwrap(), text.as_bytes());
        let mut buf = PieceBuf::default();
        buf.add("w\u{1f}0\u{1f}k", Some(&json!({ "key": "k", "start": 0, "end": 60000, "due": 60000, "acc": { "count": 1 } })), 1 << 20);
        assert_eq!(
            buf.set[0],
            r#"["w\u001f0\u001fk",{"acc":{"count":1},"due":60000,"end":60000,"key":"k","start":0}]"#
        );
    }

    #[test]
    fn pointers_and_layers() {
        let l = vec![
            Layer {
                sid: "a".into(),
                chunks: 2,
                bytes: 100,
                full: true,
                v1: false,
            },
            Layer {
                sid: "b".into(),
                chunks: 1,
                bytes: 10,
                full: false,
                v1: false,
            },
        ];
        let p = pointer_of(&l, json!(9), 1000.0, json!([{}]), Some(3));
        assert_eq!(p["id"], json!("b"));
        assert_eq!(p["chunks"], json!(3));
        assert_eq!(p["units"], json!(3));
        assert_eq!(layers_of(Some(&p)), l);
        let v1 = json!({ "id": "x", "chunks": 2, "bytes": 5, "position": 1 });
        assert!(layers_of(Some(&v1))[0].v1);
        assert!(layers_of(Some(&json!({ "id": null, "chunks": 0 }))).is_empty());
        assert_eq!(
            layer_keys("q", &l),
            ["q#snap#a#0", "q#snap#a#1", "q#snap#b#0"]
        );
        assert!(new_snapshot_id().len() >= 12);
    }

    #[test]
    fn the_piece_limit_counts_utf8_bytes_not_characters() {
        // 200 characters, 600 bytes in UTF-8: counted in characters, a piece would hold three times the limit.
        let text = "漢字".repeat(100);
        let room = 2048;
        let mut pieces: Vec<PieceBuf> = Vec::new();
        let mut buf = PieceBuf::default();
        for i in 0..12 {
            if buf.add(&format!("k{i}"), Some(&json!({ "text": text })), room) {
                pieces.push(std::mem::take(&mut buf));
            }
        }
        if !buf.is_empty() {
            pieces.push(buf);
        }
        assert_eq!(pieces.iter().map(|p| p.set.len()).sum::<usize>(), 12);
        assert!(pieces.len() >= 3, "{} pieces", pieces.len());
        for p in &pieces {
            // A piece ends at the entry that takes it to the limit: the entries before that one stay under it.
            let before: usize = p.set[..p.set.len() - 1].iter().map(|j| j.len()).sum();
            assert!(
                before < room,
                "{before} bytes before the last entry of a piece (limit {room})"
            );
        }
    }
}
