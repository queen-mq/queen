//! The framed pop answer: what a broker sends for a pop with `frames=true`.
//! The messages come as the stored records, each still the zstd frame the
//! producer (or the broker) made, and are unpacked here.
//!
//! ```text
//! "QPF1" | u8 1 | u32 len, the answer's own keys as one JSON object
//! u16 len, queue | u16 len, leaseId | u16 len, consumerGroup | u32 claims
//! per claim:  u64 partitionId | u16 len, partition | u64 start | u64 end | u32 deliveryAttempt | u32 records
//! per record: u64 base_offset | i64 created_at_us | u32 count | u8 codec (0 raw, 1 zstd) | u32 len | bytes
//! ```
//!
//! All integers little-endian. A record's bytes are `count` frames:
//!
//! ```text
//! u32 body_len | u8 flags | u8[16] message id | [u8[16] trace id, flag 1]
//! | u16 len, transactionId | [u16 len, producer, flag 2]
//! | [u16 len, headers block, flag 8] | payload
//! ```
//!
//! Message `i` of a record has offset `base_offset + i` and belongs to the
//! delivery when that lies in the claim's `start..=end`. The JSON object in
//! front holds what the JSON answer has beside its messages: the conflation
//! echo and the autopilot's choice.
//!
//! A headers block is `u16 count | (u8 len, name | u16 len, value) x count`.
//! The broker answers JSON for a claim that holds headers or links, as for an
//! encrypted one, so the block is read here for completeness; a link frame
//! (flag 16), which only the broker can resolve, is refused as damage.

use std::cell::RefCell;

use queen_protocol::{Message, PopResponse};

use crate::error::{Error, Result};
use crate::routing::Headers;
use crate::uuid::format_uuid;

const MAGIC: &[u8; 4] = b"QPF1";
const FLAG_TRACE: u8 = 1;
const FLAG_PRODUCER: u8 = 2;
const FLAG_ENCRYPTED: u8 = 4;
const FLAG_HEADERS: u8 = 8;
const FLAG_LINK: u8 = 16;

/// Largest record the decoder inflates: the broker's own bound on a stored one.
const MAX_RECORD_BYTES: u64 = 1 << 30;

fn damaged(what: impl std::fmt::Display) -> Error {
    Error::Decode(format!("framed pop answer: {what}"))
}

/// Reads little-endian fields off a byte slice; a read past the end fails with
/// `short`.
struct In<'a> {
    bytes: &'a [u8],
    short: &'static str,
}

impl<'a> In<'a> {
    fn new(bytes: &'a [u8], short: &'static str) -> Self {
        Self { bytes, short }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.bytes.len() < n {
            return Err(damaged(self.short));
        }
        let (head, rest) = self.bytes.split_at(n);
        self.bytes = rest;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self.take(N)?.try_into().expect("N bytes"))
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<usize> {
        Ok(u16::from_le_bytes(self.array()?) as usize)
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn text(&mut self) -> Result<String> {
        let n = self.u16()?;
        String::from_utf8(self.take(n)?.to_vec()).map_err(|_| damaged("a name is not UTF-8"))
    }
}

/// A frame's headers block: `u16 count | (u8 len, name | u16 len, value) x count`.
fn headers_of(block: &[u8]) -> Result<Headers> {
    let mut b = In::new(block, "a headers block is malformed");
    let mut out = Headers::new();
    for _ in 0..b.u16()? {
        let n = b.u8()? as usize;
        let name = String::from_utf8(b.take(n)?.to_vec())
            .map_err(|_| damaged("a header name is not UTF-8"))?;
        let value = b.text()?;
        out.insert(name, value);
    }
    if !b.bytes.is_empty() {
        return Err(damaged("a headers block is malformed"));
    }
    Ok(out)
}

/// Whether a pop answer's bytes are the framed one.
pub fn is_pop_frames(bytes: &[u8]) -> bool {
    bytes.len() > MAGIC.len() && bytes.starts_with(MAGIC)
}

thread_local! {
    static DCTX: RefCell<Option<zstd::bulk::Decompressor<'static>>> = const { RefCell::new(None) };
}

fn zstd_decompress(frame: &[u8]) -> Result<Vec<u8>> {
    // A stored frame carries its content size; one that does not is read as a stream.
    let size = match zstd::zstd_safe::get_frame_content_size(frame) {
        Ok(Some(n)) if n <= MAX_RECORD_BYTES => n as usize,
        Ok(Some(_)) => return Err(damaged("a record is implausibly large")),
        _ => return zstd::stream::decode_all(frame).map_err(|e| damaged(format!("zstd: {e}"))),
    };
    DCTX.with(|d| {
        let mut d = d.borrow_mut();
        if d.is_none() {
            *d = Some(zstd::bulk::Decompressor::new()?);
        }
        d.as_mut().expect("dctx").decompress(frame, size)
    })
    .map_err(|e| damaged(format!("zstd: {e}")))
}

/// Microseconds since the epoch as the broker writes `createdAt`.
fn iso_from_us(us: i64) -> String {
    const US_PER_DAY: i64 = 86_400_000_000;
    let days = us.div_euclid(US_PER_DAY);
    let rem = us.rem_euclid(US_PER_DAY);
    // Howard Hinnant's `civil_from_days`: days since 1970-01-01 to a date in
    // the proleptic Gregorian calendar.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    let secs = rem / 1_000_000;
    let ms = (rem / 1000) % 1000;
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{ms:03}Z",
        secs / 3600,
        (secs / 60) % 60,
        secs % 60
    )
}

/// Unpack a framed pop answer into the [`PopResponse`] the JSON answer parses
/// to: the same fields, the messages in delivery order, and the answer's own
/// keys (the conflation echo, the autopilot's choice). Everything that reads a
/// pop answer reads this as it reads the JSON one.
pub fn decode_pop_frames(bytes: &[u8]) -> Result<PopResponse> {
    if !is_pop_frames(bytes) {
        return Err(damaged("not a framed pop answer"));
    }
    let mut answer = In::new(&bytes[MAGIC.len()..], "truncated");
    let own: &[u8] = match answer.u8()? {
        0 => b"",
        1 => {
            let n = answer.u32()? as usize;
            answer.take(n)?
        }
        version => return Err(damaged(format!("unknown version {version}"))),
    };
    // The answer's own keys set what the JSON answer's would; the rest starts
    // from that type's defaults.
    let mut out: PopResponse = serde_json::from_slice(if own.is_empty() { b"{}" } else { own })
        .map_err(|e| damaged(format!("its own keys: {e}")))?;

    out.queue = answer.text()?;
    out.lease_id = answer.text()?;
    out.consumer_group = answer.text()?;
    let claims = answer.u32()?;
    out.partitions_claimed = Some(claims as i32);

    for claim in 0..claims {
        let partition_id = answer.u64()?.to_string();
        let partition = answer.text()?;
        let (start, end) = (answer.u64()?, answer.u64()?);
        let _delivery_attempt = answer.u32()?;
        let records = answer.u32()?;
        if claim == 0 {
            out.partition = partition.clone();
            out.partition_id = partition_id.clone();
        }
        for _ in 0..records {
            let base = answer.u64()?;
            let created_at = iso_from_us(answer.u64()? as i64);
            let count = answer.u32()?;
            let codec = answer.u8()?;
            let len = answer.u32()? as usize;
            let stored = answer.take(len)?;
            let inflated;
            let raw: &[u8] = match codec {
                0 => stored,
                1 => {
                    inflated = zstd_decompress(stored)?;
                    &inflated
                }
                other => return Err(damaged(format!("unknown codec {other}"))),
            };
            let mut frames = In::new(raw, "a record is shorter than its frames");
            for i in 0..count {
                let len = frames.u32()? as usize;
                let mut frame = In::new(frames.take(len)?, "a frame is malformed");
                let flags = frame.u8()?;
                let id: [u8; 16] = frame.array()?;
                let trace: Option<[u8; 16]> = if flags & FLAG_TRACE != 0 {
                    Some(frame.array()?)
                } else {
                    None
                };
                let transaction_id = frame.text()?;
                let producer_sub = if flags & FLAG_PRODUCER != 0 {
                    Some(frame.text()?)
                } else {
                    None
                };
                let headers = if flags & FLAG_HEADERS != 0 {
                    let n = frame.u16()?;
                    Some(frame.take(n)?)
                } else {
                    None
                };
                if flags & FLAG_ENCRYPTED != 0 {
                    return Err(damaged("a frame is encrypted"));
                }
                if flags & FLAG_LINK != 0 {
                    return Err(damaged("a frame is a link"));
                }
                let offset = base + u64::from(i);
                if offset < start || offset > end {
                    continue;
                }
                let payload = frame.bytes;
                out.messages.push(Message {
                    id: format_uuid(&id),
                    transaction_id,
                    trace_id: trace.as_ref().map(format_uuid),
                    data: if payload.is_empty() {
                        serde_json::Value::Null
                    } else {
                        serde_json::from_slice(payload)
                            .map_err(|e| damaged(format!("a payload: {e}")))?
                    },
                    producer_sub,
                    headers: headers.map(headers_of).transpose()?,
                    created_at: created_at.clone(),
                    partition_id: partition_id.clone(),
                    partition: partition.clone(),
                    lease_id: out.lease_id.clone(),
                    consumer_group: out.consumer_group.clone(),
                    offset: i64::try_from(offset).ok(),
                });
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_headers_block_reads_as_the_broker_writes_it() {
        // `frames::encode_headers([("source", "web"), ("type", "order.paid")])`.
        let mut block = 2u16.to_le_bytes().to_vec();
        for (k, v) in [("source", "web"), ("type", "order.paid")] {
            block.push(k.len() as u8);
            block.extend_from_slice(k.as_bytes());
            block.extend_from_slice(&(v.len() as u16).to_le_bytes());
            block.extend_from_slice(v.as_bytes());
        }
        let h = headers_of(&block).unwrap();
        assert_eq!(h.len(), 2);
        assert_eq!(h["type"], "order.paid");
        assert!(headers_of(&block[..block.len() - 1]).is_err(), "truncated");
        let mut long = block.clone();
        long.push(0);
        assert!(headers_of(&long).is_err(), "bytes after the last header");
    }

    #[test]
    fn created_at_is_written_as_the_broker_writes_it() {
        assert_eq!(iso_from_us(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            iso_from_us(1_700_000_000_000_000),
            "2023-11-14T22:13:20.000Z"
        );
        assert_eq!(
            iso_from_us(1_709_164_799_999_999),
            "2024-02-28T23:59:59.999Z"
        );
        assert_eq!(
            iso_from_us(1_709_251_199_123_000),
            "2024-02-29T23:59:59.123Z"
        );
    }
}
