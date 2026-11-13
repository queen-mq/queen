//! The binary push body: an alternative to the JSON body of `POST /api/v1/push`.
//!
//! ```text
//! "QPB1" | u8 flags | [u32 raw_len, when zstd] | body
//! body:  u32 groups | per group: u16 len, queue | u16 len, partition | u32 n
//!        | n x ([u16 len, transactionId, when flag 4] | u32 len, payload)
//! ```
//!
//! All integers little-endian. flags: 1 = the body is one zstd frame of
//! `raw_len` bytes, 2 = answer with the counts only, 4 = every item carries its
//! transaction id (empty = the broker mints it). No trace id. Each payload is
//! one JSON document.
//!
//! With flag 8 ([`encode_push_frames`]) a group is the messages as the broker
//! stores them, compressed here once:
//!
//! ```text
//! body:  u32 groups | per group: u16 len, queue | u16 len, partition | u32 n | u32 z_len | z
//! z:     one zstd frame of n frames: u32 body_len | u8 flags | u8[16] message id
//!        | u16 len, transactionId | [u16 len, headers block, flag 8] | payload
//! ```
//!
//! The message id is a version-7 UUID minted by the client; a message without
//! its own transaction id uses the id's text. A frame's flags are 0, or 8 when
//! it carries headers: `u16 count | (u8 len, name | u16 len, value) x count`,
//! in name order (broker catalogue version 8). The broker checks every frame
//! and keeps the compressed bytes as they are, so it does not compress them
//! again. The plain body (without flag 8) has no place for headers: a push
//! whose messages carry some goes as the framed body or as JSON.
//!
//! A broker that does not know this body answers 400; a client sends it only
//! when [`Config::binary_push`](crate::Config::binary_push) is set, so by
//! default a push is the JSON request every broker knows.
//!
//! The binary transaction body ([`encode_transaction_frames`]), an alternative
//! to the JSON body of `POST /api/v1/transaction`, carries the pushes the same
//! way:
//!
//! ```text
//! "QTB1" | u8 flags | u32 len, head | u32 groups | the groups, as with flag 8 above
//! ```
//!
//! `head` is the JSON transaction body without its push operations: the acks
//! in `operations`, `requiredLeases`, `kv`, `timers`. flags: 2 = the answer
//! leaves out the pushes' and the acks' results and counts them (`pushed`,
//! `duplicate`, `acked`); the riders' results stay. In the answer the head's
//! operations come first and the groups' messages after them, in the order
//! written; the riders follow, as in the JSON body.

use std::cell::RefCell;

use queen_protocol::{
    PushItem, TransactionRequest, TransactionResponse, TxnOperation, DEFAULT_PARTITION,
};
use serde::Deserialize;

use crate::error::{Error, Result};
use crate::routing::Headers;

/// `Content-Type` of a binary push body.
pub const CONTENT_TYPE: &str = "application/x-queen-push";

const MAGIC: &[u8; 4] = b"QPB1";
const TXN_MAGIC: &[u8; 4] = b"QTB1";
const FLAG_ZSTD: u8 = 1;
const FLAG_COUNTS: u8 = 2;
const FLAG_TXN: u8 = 4;
const FLAG_FRAMES: u8 = 8;
/// A frame's flag: a headers block follows its transaction id.
const FRAME_HEADERS: u8 = 8;

/// One partition's messages for [`encode_push_binary`] and
/// [`encode_push_frames`].
#[derive(Debug, Clone)]
pub struct PushBatch<'a> {
    pub queue: &'a str,
    pub partition: &'a str,
    /// Each one JSON document.
    pub payloads: Vec<&'a [u8]>,
    /// `None`, or one transaction id per payload (`""` = the broker mints it).
    pub transaction_ids: Option<Vec<&'a str>>,
}

/// The short answer of a binary push that asked for the counts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
pub struct PushCounts {
    #[serde(default)]
    pub count: u64,
    #[serde(default)]
    pub queued: u64,
    #[serde(default)]
    pub duplicate: u64,
    #[serde(default)]
    pub error: u64,
}

thread_local! {
    static CCTX: RefCell<Option<zstd::bulk::Compressor<'static>>> = const { RefCell::new(None) };
}

/// One zstd frame of `raw`, at the fastest level. The bulk compressor writes
/// the content size, which the broker asks of a frame it stores as it came.
pub(crate) fn zstd_compress(raw: &[u8]) -> Result<Vec<u8>> {
    CCTX.with(|c| {
        let mut c = c.borrow_mut();
        if c.is_none() {
            *c = Some(zstd::bulk::Compressor::new(1)?);
        }
        c.as_mut().expect("cctx").compress(raw)
    })
    .map_err(|e| Error::Invalid(format!("zstd: {e}")))
}

fn text16(out: &mut Vec<u8>, text: &str, what: &str) -> Result<()> {
    let len = u16::try_from(text.len())
        .map_err(|_| Error::Invalid(format!("{what} longer than 65535 bytes")))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(text.as_bytes());
    Ok(())
}

fn len32(len: usize, what: &str) -> Result<[u8; 4]> {
    u32::try_from(len)
        .map(u32::to_le_bytes)
        .map_err(|_| Error::Invalid(format!("{what} does not fit a binary push body")))
}

fn check(batch: &PushBatch<'_>) -> Result<()> {
    match &batch.transaction_ids {
        Some(ids) if ids.len() != batch.payloads.len() => Err(Error::Invalid(format!(
            "{} transaction ids for {} payloads",
            ids.len(),
            batch.payloads.len()
        ))),
        _ => Ok(()),
    }
}

/// Encode batches as a binary push body.
///
/// `compress`: the body is zstd-compressed. `counts`: the broker answers with
/// [`PushCounts`] instead of one object per message.
pub fn encode_push_binary(
    batches: &[PushBatch<'_>],
    compress: bool,
    counts: bool,
) -> Result<Vec<u8>> {
    let with_txn = batches.iter().any(|b| b.transaction_ids.is_some());
    let mut flags = 0u8;
    if counts {
        flags |= FLAG_COUNTS;
    }
    if with_txn {
        flags |= FLAG_TXN;
    }
    let size: usize = batches
        .iter()
        .map(|b| {
            12 + b.queue.len()
                + b.partition.len()
                + b.payloads.iter().map(|p| 6 + p.len()).sum::<usize>()
                + b.transaction_ids
                    .as_ref()
                    .map_or(0, |ids| ids.iter().map(|t| t.len()).sum())
        })
        .sum();
    let head = MAGIC.len() + 1;
    let mut out = Vec::with_capacity(head + 4 + size);
    out.extend_from_slice(MAGIC);
    out.push(flags);
    out.extend_from_slice(&len32(batches.len(), "the number of groups")?);
    for batch in batches {
        check(batch)?;
        text16(&mut out, batch.queue, "queue name")?;
        text16(&mut out, batch.partition, "partition name")?;
        out.extend_from_slice(&len32(batch.payloads.len(), "the number of messages")?);
        for (i, payload) in batch.payloads.iter().enumerate() {
            if with_txn {
                let txn = batch.transaction_ids.as_ref().map_or("", |ids| ids[i]);
                text16(&mut out, txn, "transaction id")?;
            }
            out.extend_from_slice(&len32(payload.len(), "a payload")?);
            out.extend_from_slice(payload);
        }
    }
    if !compress {
        return Ok(out);
    }
    let raw = &out[head..];
    let mut packed = Vec::with_capacity(head + 4 + raw.len() / 2);
    packed.extend_from_slice(MAGIC);
    packed.push(flags | FLAG_ZSTD);
    packed.extend_from_slice(&len32(raw.len(), "the body")?);
    packed.extend_from_slice(&zstd_compress(raw)?);
    Ok(packed)
}

/// Encode batches as a binary push body whose groups are the messages framed
/// as the broker stores them and compressed once, here. `counts`: the broker
/// answers with [`PushCounts`] instead of one object per message.
pub fn encode_push_frames(batches: &[PushBatch<'_>], counts: bool) -> Result<Vec<u8>> {
    encode_push_frames_with(batches, &[], counts)
}

/// [`encode_push_frames`], message `i` of `batches[b]` with the headers
/// `headers[b][i]` (none past the end of either).
fn encode_push_frames_with(
    batches: &[PushBatch<'_>],
    headers: &[Vec<Option<&Headers>>],
    counts: bool,
) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(1024);
    out.extend_from_slice(MAGIC);
    out.push(if counts {
        FLAG_FRAMES | FLAG_COUNTS
    } else {
        FLAG_FRAMES
    });
    append_frame_groups(out, batches, headers)
}

/// Encode a binary transaction body: `head`, the JSON transaction body without
/// its push operations (empty for none), then `batches` as groups of messages
/// framed as the broker stores them and compressed once, here. `counts`: the
/// broker counts the pushes and the acks in its answer instead of rendering
/// one result for each.
pub fn encode_transaction_frames(
    head: &[u8],
    batches: &[PushBatch<'_>],
    counts: bool,
) -> Result<Vec<u8>> {
    encode_transaction_frames_with(head, batches, &[], counts)
}

/// [`encode_transaction_frames`], with headers as [`encode_push_frames_with`]
/// takes them.
fn encode_transaction_frames_with(
    head: &[u8],
    batches: &[PushBatch<'_>],
    headers: &[Vec<Option<&Headers>>],
    counts: bool,
) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(1024 + head.len());
    out.extend_from_slice(TXN_MAGIC);
    out.push(if counts { FLAG_COUNTS } else { 0 });
    out.extend_from_slice(&len32(head.len(), "the head")?);
    out.extend_from_slice(head);
    append_frame_groups(out, batches, headers)
}

/// `headers` as a frame's block: `u16 count | (u8 len, name | u16 len, value)
/// x count`, in name order, as the broker stores it. `None` for no headers: an
/// empty block is written as none. An error when a name, a value or the whole
/// block does not fit its length field; the caller then sends the JSON
/// request.
fn headers_block(headers: &Headers) -> Result<Option<Vec<u8>>> {
    if headers.is_empty() {
        return Ok(None);
    }
    let count = u16::try_from(headers.len())
        .map_err(|_| Error::Invalid("more than 65535 headers on one message".into()))?;
    let size: usize = headers.iter().map(|(k, v)| 3 + k.len() + v.len()).sum();
    let mut out = Vec::with_capacity(2 + size);
    out.extend_from_slice(&count.to_le_bytes());
    // A BTreeMap of Strings iterates in byte order: the broker's name order.
    for (name, value) in headers {
        let len = u8::try_from(name.len())
            .map_err(|_| Error::Invalid(format!("header name {name:?} is over 255 bytes")))?;
        out.push(len);
        out.extend_from_slice(name.as_bytes());
        text16(&mut out, value, "a header value")?;
    }
    if out.len() > u16::MAX as usize {
        return Err(Error::Invalid(
            "a message's headers do not fit a frame (65535 bytes)".into(),
        ));
    }
    Ok(Some(out))
}

/// Appends `u32 groups | per group: u16 len, queue | u16 len, partition | u32 n
/// | u32 z_len | z` to `out`: each batch's messages framed as the broker stores
/// them, with a version-7 id minted here, as one zstd frame. Message `i` of
/// `batches[b]` carries the headers `headers[b][i]`, when there are some.
fn append_frame_groups(
    mut out: Vec<u8>,
    batches: &[PushBatch<'_>],
    headers: &[Vec<Option<&Headers>>],
) -> Result<Vec<u8>> {
    out.extend_from_slice(&len32(batches.len(), "the number of groups")?);
    let mut frames: Vec<u8> = Vec::new();
    for (b, batch) in batches.iter().enumerate() {
        check(batch)?;
        frames.clear();
        frames.reserve(
            batch
                .payloads
                .iter()
                .map(|p| 4 + 1 + 16 + 2 + 36 + p.len())
                .sum(),
        );
        for (i, payload) in batch.payloads.iter().enumerate() {
            let id = crate::uuid::uuidv7_bytes();
            let own = batch.transaction_ids.as_ref().map_or("", |ids| ids[i]);
            let minted;
            let txn = if own.is_empty() {
                minted = crate::uuid::format_uuid(&id);
                minted.as_str()
            } else {
                own
            };
            let block = match headers.get(b).and_then(|h| h.get(i)).copied().flatten() {
                Some(h) => headers_block(h)?,
                None => None,
            };
            let mut body_len = 1 + 16 + 2 + txn.len() + payload.len();
            if let Some(block) = &block {
                body_len += 2 + block.len();
            }
            frames.extend_from_slice(&len32(body_len, "a message")?);
            frames.push(if block.is_some() { FRAME_HEADERS } else { 0 });
            frames.extend_from_slice(&id);
            text16(&mut frames, txn, "transaction id")?;
            if let Some(block) = &block {
                frames.extend_from_slice(&(block.len() as u16).to_le_bytes());
                frames.extend_from_slice(block);
            }
            frames.extend_from_slice(payload);
        }
        text16(&mut out, batch.queue, "queue name")?;
        text16(&mut out, batch.partition, "partition name")?;
        out.extend_from_slice(&len32(batch.payloads.len(), "the number of messages")?);
        let compressed = zstd_compress(&frames)?;
        out.extend_from_slice(&len32(compressed.len(), "a group")?);
        out.extend_from_slice(&compressed);
    }
    Ok(out)
}

/// `items` as a binary push body. Consecutive items of one partition share a
/// group, so the order of the items is the order on the wire. `compress`: the
/// messages framed and compressed here, kept by the broker as they are.
///
/// An error means an item does not fit the body (a name or a transaction id
/// over 65535 bytes, or headers on the plain body, which has no place for
/// them): the caller then sends the JSON request, and the broker answers that
/// as it always did.
pub(crate) fn body_of(items: &[PushItem], compress: bool) -> Result<Vec<u8>> {
    // Every payload once, into one buffer the batches borrow from.
    let mut payloads: Vec<u8> = Vec::new();
    let mut pushed: Vec<Pushed<'_>> = Vec::with_capacity(items.len());
    for item in items {
        let headers = item.headers.as_ref().filter(|h| !h.is_empty());
        if headers.is_some() && !compress {
            return Err(Error::Invalid(
                "the plain binary push body carries no headers".into(),
            ));
        }
        let start = payloads.len();
        serde_json::to_writer(&mut payloads, &item.payload)?;
        pushed.push(Pushed {
            queue: &item.queue,
            partition: item
                .partition
                .as_deref()
                .filter(|p| !p.is_empty())
                .unwrap_or(DEFAULT_PARTITION),
            transaction_id: item.transaction_id.as_deref().unwrap_or(""),
            payload: start..payloads.len(),
            headers,
        });
    }
    let (batches, headers) = batches_of(&pushed, &payloads);
    if compress {
        encode_push_frames_with(&batches, &headers, false)
    } else {
        encode_push_binary(&batches, false, false)
    }
}

/// One pushed message on its way into a batch: its names borrowed, its
/// payload a range of the buffer every payload was written to.
struct Pushed<'a> {
    queue: &'a str,
    partition: &'a str,
    transaction_id: &'a str,
    payload: std::ops::Range<usize>,
    headers: Option<&'a Headers>,
}

/// One headers entry per message of each batch: `None` for a message without.
type BatchHeaders<'a> = Vec<Vec<Option<&'a Headers>>>;

/// `pushed` as batches, consecutive messages of one partition sharing one,
/// and each batch's headers, message by message.
fn batches_of<'a>(
    pushed: &[Pushed<'a>],
    payloads: &'a [u8],
) -> (Vec<PushBatch<'a>>, BatchHeaders<'a>) {
    let mut batches: Vec<PushBatch<'a>> = Vec::new();
    let mut headers: BatchHeaders<'a> = Vec::new();
    for p in pushed {
        let same = batches
            .last()
            .is_some_and(|b| b.queue == p.queue && b.partition == p.partition);
        if !same {
            batches.push(PushBatch {
                queue: p.queue,
                partition: p.partition,
                payloads: Vec::new(),
                transaction_ids: Some(Vec::new()),
            });
            headers.push(Vec::new());
        }
        let batch = batches.last_mut().expect("a batch");
        batch.payloads.push(&payloads[p.payload.clone()]);
        if let Some(ids) = batch.transaction_ids.as_mut() {
            ids.push(p.transaction_id);
        }
        headers.last_mut().expect("a batch").push(p.headers);
    }
    (batches, headers)
}

/// A transaction as a binary body, when its pushes can travel in one: there is
/// at least one, and none carries a trace id. The acks stay in the JSON head;
/// consecutive pushed items of one partition share a group, so the pushes keep
/// their order. `None` otherwise: the caller then sends the JSON body.
///
/// The second value is `None` when the broker's answer lists the operations as
/// the caller added them (every ack before every push). Otherwise `order[k]`
/// is the place, among the caller's operations, of the k-th result the broker
/// sends: it lists the head's acks first, then the pushes.
pub(crate) fn transaction_frames(
    req: &TransactionRequest,
) -> Option<(Vec<u8>, Option<Vec<usize>>)> {
    let mut head_operations: Vec<TxnOperation> = Vec::new();
    let mut payloads: Vec<u8> = Vec::new();
    let mut pushed: Vec<Pushed<'_>> = Vec::new();
    let (mut head_at, mut push_at) = (Vec::new(), Vec::new());
    let mut flat = 0usize;
    for op in &req.operations {
        let TxnOperation::Push { items } = op else {
            head_operations.push(op.clone());
            head_at.push(flat);
            flat += 1;
            continue;
        };
        for item in items {
            if item.trace_id.is_some() {
                return None;
            }
            let start = payloads.len();
            serde_json::to_writer(&mut payloads, &item.payload).ok()?;
            pushed.push(Pushed {
                queue: &item.queue,
                partition: item
                    .partition
                    .as_deref()
                    .filter(|p| !p.is_empty())
                    .unwrap_or(DEFAULT_PARTITION),
                transaction_id: item.transaction_id.as_deref().unwrap_or(""),
                payload: start..payloads.len(),
                headers: item.headers.as_ref().filter(|h| !h.is_empty()),
            });
            push_at.push(flat);
            flat += 1;
        }
    }
    if pushed.is_empty() {
        return None;
    }
    // The head is the request's own type, so it is written as the JSON body is.
    let head = TransactionRequest::new(head_operations)
        .with_required_leases(req.required_leases.clone())
        .with_kv(req.kv.clone())
        .with_timers(req.timers.clone());
    let head = serde_json::to_vec(&head).ok()?;
    let (batches, headers) = batches_of(&pushed, &payloads);
    let body = encode_transaction_frames_with(&head, &batches, &headers, false).ok()?;
    head_at.append(&mut push_at);
    let order = head_at;
    let in_order = order.iter().enumerate().all(|(k, &at)| at == k);
    Some((body, (!in_order).then_some(order)))
}

/// Puts the results of a binary transaction back in the order the caller added
/// its operations in, and gives each its own index again. The riders' results,
/// after the operations', are already in place.
pub(crate) fn restore_result_order(resp: &mut TransactionResponse, order: &[usize]) {
    if resp.results.len() < order.len() {
        return;
    }
    let riders = resp.results.split_off(order.len());
    let mut slots: Vec<_> = order.iter().map(|_| None).collect();
    for (mut result, &at) in std::mem::take(&mut resp.results).into_iter().zip(order) {
        result.index = at;
        slots[at] = Some(result);
    }
    resp.results = slots.into_iter().flatten().collect();
    resp.results.extend(riders);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch<'a>(payloads: &[&'a [u8]], ids: Option<Vec<&'a str>>) -> PushBatch<'a> {
        PushBatch {
            queue: "q",
            partition: "p",
            payloads: payloads.to_vec(),
            transaction_ids: ids,
        }
    }

    #[test]
    fn the_counts_flag() {
        let b = [batch(&[b"1"], None)];
        assert!(encode_push_binary(&b, false, true)
            .unwrap()
            .starts_with(b"QPB1\x02"));
        assert_eq!(encode_push_frames(&b, true).unwrap()[4], 8 | 2);
        let c: PushCounts =
            serde_json::from_str(r#"{"count":3,"queued":2,"duplicate":1,"error":0}"#).unwrap();
        assert_eq!(
            c,
            PushCounts {
                count: 3,
                queued: 2,
                duplicate: 1,
                error: 0
            }
        );
    }

    #[test]
    fn the_zstd_flag_compresses_the_whole_body_with_its_raw_length_in_front() {
        let b = [batch(&[b"1"], None)];
        let plain = encode_push_binary(&b, false, false).unwrap();
        let packed = encode_push_binary(&b, true, false).unwrap();
        assert!(packed.starts_with(b"QPB1\x01"));
        assert_eq!(
            u32::from_le_bytes(packed[5..9].try_into().unwrap()) as usize,
            plain.len() - 5
        );
        assert_eq!(zstd::stream::decode_all(&packed[9..]).unwrap(), &plain[5..]);
    }

    #[test]
    fn transaction_ids_must_match_the_payloads() {
        let b = [batch(&[b"1", b"2"], Some(vec!["t"]))];
        let err = encode_push_binary(&b, false, false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("1 transaction ids for 2 payloads"), "{err}");
        assert!(encode_push_frames(&b, false).is_err());
    }

    #[test]
    fn a_name_over_65535_bytes_does_not_fit() {
        let long = "p".repeat(70_000);
        let b = [PushBatch {
            queue: "q",
            partition: &long,
            payloads: vec![b"1"],
            transaction_ids: None,
        }];
        assert!(encode_push_binary(&b, false, false).is_err());
        assert!(encode_push_frames(&b, false).is_err());
    }
}
