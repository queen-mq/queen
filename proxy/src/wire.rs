//! What a body weighs UNCOMPRESSED, read from its headers alone.
//!
//! The byte rate (limits.rs, `check_bytes` / `debit_bytes`) counts uncompressed
//! bytes: what the broker has to hold and work on, whatever a client did to the
//! bytes on the wire. Nothing is inflated here to learn it. Every form the
//! broker reads declares it, and the broker holds a body to what it declares
//! (measured against a broker of this tree on 2026-10-12):
//!
//!   - a JSON body, and a binary push with neither flag 1 nor flag 8, are not
//!     compressed: they weigh what they measure;
//!   - a binary push whose whole body is one zstd frame (flag 1) carries
//!     `raw_len` in front of it. The broker inflates into exactly that many
//!     bytes, so a body that holds more is refused (400), and one that
//!     declares more than it holds is only counted for more;
//!   - a framed push (flag 8) and a binary transaction (`QTB1`) carry groups,
//!     each ONE zstd frame whose own header declares its content size. The
//!     broker refuses a group whose frame declares nothing, less or more than
//!     it holds, and a group of two frames;
//!   - flags 1 and 8 together would hide the groups, and their sizes, inside
//!     the outer frame: the broker refuses that body, and so does this reader.
//!
//! A framed pop answer (`QPF1`) is the same thing in the other direction: each
//! record says whether it is compressed, and the frame of one that is declares
//! its size.
//!
//! FAIL CLOSED. A binary body this reader cannot follow (flags it does not
//! know, a header cut short, a frame that declares nothing, a kind of frame it
//! has not been told about) has no size, and the gateway refuses it instead of
//! forwarding a body it could not count. That is the lesson of 2026-10-12: the
//! binary bodies reached the broker uncounted for as long as "cannot read it"
//! meant "let the broker answer". The layouts are the broker's own,
//! server/src/rsm/facade/real.rs: `push_binary`, `frame_groups`, `txn_impl`
//! and `render_pop_frames`.

/// First bytes of a binary push body, a binary transaction body and a framed
/// pop answer.
const PUSH_MAGIC: &[u8] = b"QPB1";
const TXN_MAGIC: &[u8] = b"QTB1";
const POP_MAGIC: &[u8] = b"QPF1";

/// Binary push flags: the whole body is one zstd frame; answer with the counts;
/// every item carries a transaction id; every group is the client's frames.
const FLAG_ZSTD: u8 = 1;
const FLAG_COUNTS: u8 = 2;
const FLAG_TXN: u8 = 4;
const FLAG_FRAMES: u8 = 8;
/// The one binary transaction flag: answer with the counts.
const TXN_FLAG_COUNTS: u8 = 2;

/// Why a binary body has no size. The text goes into the 400.
#[derive(Debug, PartialEq, Eq)]
pub struct Unreadable(pub &'static str);

/// Is `body` the binary body of this produce route? The broker decides the
/// same way: `QPB1` on the push route, `QTB1` on the transaction route, and
/// anything else is read as JSON.
pub fn is_binary(path: &str, body: &[u8]) -> bool {
    match path {
        "/api/v1/push" => body.starts_with(PUSH_MAGIC),
        "/api/v1/transaction" => body.starts_with(TXN_MAGIC),
        _ => false,
    }
}

/// The uncompressed bytes of a produce body, headers included.
pub fn request_bytes(path: &str, body: &[u8]) -> Result<u64, Unreadable> {
    if !is_binary(path, body) {
        return Ok(body.len() as u64);
    }
    let flags = *body.get(4).ok_or(Unreadable("truncated"))?;
    if path == "/api/v1/transaction" {
        if flags & !TXN_FLAG_COUNTS != 0 {
            return Err(Unreadable("unknown flags"));
        }
        let mut p = 5;
        let head = u32le(body, &mut p)?;
        take(body, &mut p, head)?;
        return with_groups_at_their_size(body, p);
    }
    if flags & !(FLAG_ZSTD | FLAG_COUNTS | FLAG_TXN | FLAG_FRAMES) != 0 {
        return Err(Unreadable("unknown flags"));
    }
    match (flags & FLAG_ZSTD != 0, flags & FLAG_FRAMES != 0) {
        (true, true) => Err(Unreadable(
            "flags 1 and 8 together: a framed body is already compressed, group by group",
        )),
        (true, false) => {
            let mut p = 5;
            let raw_len = u32le(body, &mut p)?;
            Ok(p as u64 + raw_len as u64)
        }
        (false, true) => with_groups_at_their_size(body, 5),
        (false, false) => Ok(body.len() as u64),
    }
}

/// The uncompressed bytes of a pop answer. A JSON answer weighs what it
/// measures; a framed one (`QPF1`) is counted with each compressed record at
/// the size its frame declares. An answer is the broker's and is never
/// refused: one this reader cannot follow weighs what it measures.
pub fn pop_answer_bytes(body: &[u8]) -> u64 {
    framed_pop_bytes(body).unwrap_or(body.len() as u64)
}

/// `"QPF1" | u8 1 | u32 len, the answer's own keys`, three names, then per
/// claim its header and per record `.. | u8 codec (0 raw, 1 zstd) | u32 len |
/// bytes`. `None`: not a framed answer, or not one this reader follows.
fn framed_pop_bytes(body: &[u8]) -> Option<u64> {
    if !body.starts_with(POP_MAGIC) || body.get(4) != Some(&1) {
        return None;
    }
    let mut p = 5;
    let keys = u32le(body, &mut p).ok()?;
    take(body, &mut p, keys).ok()?;
    for _ in 0..3 {
        let n = u16le(body, &mut p).ok()?;
        take(body, &mut p, n).ok()?;
    }
    let claims = u32le(body, &mut p).ok()?;
    let (mut stored, mut declared) = (0u64, 0u64);
    for _ in 0..claims {
        take(body, &mut p, 8).ok()?; // partition id
        let n = u16le(body, &mut p).ok()?;
        take(body, &mut p, n).ok()?; // partition
        take(body, &mut p, 8 + 8 + 4).ok()?; // start, end, delivery attempt
        let records = u32le(body, &mut p).ok()?;
        for _ in 0..records {
            take(body, &mut p, 8 + 8 + 4).ok()?; // base offset, created at, count
            let codec = take(body, &mut p, 1).ok()?[0];
            let len = u32le(body, &mut p).ok()?;
            let bytes = take(body, &mut p, len).ok()?;
            match codec {
                0 => {}
                1 => {
                    stored += len as u64;
                    declared = declared.checked_add(frame_content_size(bytes)?)?;
                }
                _ => return None,
            }
        }
    }
    (p == body.len()).then(|| (body.len() as u64 - stored).saturating_add(declared))
}

/// `body`, whose groups start at `p`, with each group's frames counted at the
/// size their zstd frame declares instead of the bytes it takes:
///
/// ```text
/// u32 groups | per group: u16 len, queue | u16 len, partition | u32 n | u32 z_len | z
/// ```
fn with_groups_at_their_size(body: &[u8], mut p: usize) -> Result<u64, Unreadable> {
    let groups = u32le(body, &mut p)?;
    let (mut stored, mut declared) = (0u64, 0u64);
    for _ in 0..groups {
        let n = u16le(body, &mut p)?;
        take(body, &mut p, n)?; // queue
        let n = u16le(body, &mut p)?;
        take(body, &mut p, n)?; // partition
        take(body, &mut p, 4)?; // n
        let z_len = u32le(body, &mut p)?;
        let z = take(body, &mut p, z_len)?;
        let size = frame_content_size(z).ok_or(Unreadable(
            "a group's zstd frame does not declare its content size",
        ))?;
        stored += z_len as u64;
        declared = declared
            .checked_add(size)
            .ok_or(Unreadable("the declared sizes do not add up"))?;
    }
    Ok((body.len() as u64 - stored).saturating_add(declared))
}

/// What a zstd frame's header declares as its content size (RFC 8878, 3.1.1.1):
/// `None` when it declares none, or is not a frame this reader follows. The
/// broker asks libzstd the same question (`get_frame_content_size`) and
/// inflates into exactly the answer.
fn frame_content_size(z: &[u8]) -> Option<u64> {
    const MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];
    if z.get(..4)? != MAGIC {
        return None;
    }
    let descriptor = *z.get(4)?;
    if descriptor & 0x08 != 0 {
        return None; // the reserved bit: no decoder takes the frame
    }
    let single_segment = descriptor & 0x20 != 0;
    // The window descriptor (absent in a single segment) and the dictionary id
    // sit between the descriptor and the size.
    let p = 5 + usize::from(!single_segment) + [0, 1, 2, 4][usize::from(descriptor & 3)];
    match descriptor >> 6 {
        0 if single_segment => Some(u64::from(*z.get(p)?)),
        0 => None,
        1 => Some(u64::from(u16::from_le_bytes(z.get(p..p + 2)?.try_into().ok()?)) + 256),
        2 => Some(u64::from(u32::from_le_bytes(
            z.get(p..p + 4)?.try_into().ok()?,
        ))),
        _ => Some(u64::from_le_bytes(z.get(p..p + 8)?.try_into().ok()?)),
    }
}

fn take<'a>(b: &'a [u8], p: &mut usize, n: usize) -> Result<&'a [u8], Unreadable> {
    let end = p.checked_add(n).ok_or(Unreadable("truncated"))?;
    let s = b.get(*p..end).ok_or(Unreadable("truncated"))?;
    *p = end;
    Ok(s)
}

fn u16le(b: &[u8], p: &mut usize) -> Result<usize, Unreadable> {
    Ok(u16::from_le_bytes(take(b, p, 2)?.try_into().expect("2 bytes")) as usize)
}

fn u32le(b: &[u8], p: &mut usize) -> Result<usize, Unreadable> {
    Ok(u32::from_le_bytes(take(b, p, 4)?.try_into().expect("4 bytes")) as usize)
}

/// Bodies for tests, here and in gateway.rs: the same three messages in every
/// form the broker reads.
#[cfg(test)]
pub(crate) mod fixtures {
    /// The three messages as a JSON body lists them: two queues, and payloads
    /// of 7, 136 and 200 bytes.
    pub(crate) fn three_items() -> String {
        format!(
            concat!(
                r#"{{"queue":"orders","partition":"p1","payload":{{"n":1}}}},"#,
                r#"{{"queue":"orders","partition":"p1","payload":{{"n":2,"pad":"{}"}}}},"#,
                r#"{{"queue":"audit","partition":"p1","payload":{{"n":3,"pad":"{}"}}}}"#
            ),
            "x".repeat(120),
            "x".repeat(184)
        )
    }

    // The same three messages as the Go client's reference encoder writes them
    // (clients/client-go/push_binary.go), in hex; the message ids are the ones
    // it minted on that run. The broker took each of the four on 2026-10-12.
    // The pads are there for that: the encoder of that morning wrote no
    // content size into a zstd frame of under 256 bytes, and the broker
    // refuses a group whose frame does not declare it.

    /// `EncodePushBinary(batches, false, false)`: what `binaryPush` sends (flag 4).
    pub(crate) const QPB1_PLAIN: &str = "51504231040200000006006f7264657273020070310200000000\
        00070000007b226e223a317d0000880000007b226e223a322c22706164223a2278787878787878787878\
        787878787878787878787878787878787878787878787878787878787878787878787878787878787878\
        787878787878787878787878787878787878787878787878787878787878787878787878787878787878\
        7878787878787878787878787878787878787878787878787878227d0500617564697402007031010000\
        000000c80000007b226e223a332c22706164223a22787878787878787878787878787878787878787878\
        787878787878787878787878787878787878787878787878787878787878787878787878787878787878\
        787878787878787878787878787878787878787878787878787878787878787878787878787878787878\
        787878787878787878787878787878787878787878787878787878787878787878787878787878787878\
        78787878787878787878787878787878787878787878787878787878787878787878787878227d";

    /// `EncodePushBinary(batches, true, true)`: the body above as one zstd
    /// frame, asking for the counts answer (flags 1, 2 and 4).
    pub(crate) const QPB1_ZSTD_COUNTS: &str = "51504231078c01000028b52ffd44008c008d020052440f\
        19a00900303310f45119f9f31d30c08769b84dcb624419e9250fad46f43f80eb63906cd441abd3d454ab\
        44017dc3d4a4cf3ffc7fb8be851dc99485ff020500b55812744817c8bc8ce21222bcf309a9813300";

    /// `EncodePushFrames(batches, false)`: what `pushCompression` sends (flag 8),
    /// each group's frames as one zstd frame.
    pub(crate) const QPB1_FRAMES: &str = "51504231080200000006006f72646572730200703102000000\
        8f00000028b52ffd4400050005040024073e0000000001a130a88e54731c892bec67ff431aed24003031\
        6131333061382d386535342d373331632d383932622d6563363766663433316165647b226e223a317dbf\
        2492747110c6bdf19832342d393237342d3731313063366264663139387b226e223a322c22706164223a\
        227878227d03009c79fa8af03714463003ee28ee720500617564697402007031010000006600000028b5\
        2ffd44000300bd0200e404ff0000000001a130a88e54743db5aee30e9e3ad6be24003031613133306138\
        2d386535342d373433642d623561652d6533306539653361643662657b226e223a332c22706164223a22\
        787878227d015419022b8ccc0a9feb25";

    /// `EncodeTransactionFrames({"operations":[]}, batches, false)`.
    pub(crate) const QTB1: &str = "5154423100110000007b226f7065726174696f6e73223a5b5d7d020000\
        0006006f726465727302007031020000008e00000028b52ffd44000500fd030014073e0000000001a130\
        a88e547664b379f97589aa8b95240030316131333061382d386535342d373636342d623337392d663937\
        3538396161386239357b226e223a317dbf6894c24cc9429e84da382d393463322d346363393432396538\
        3464617b226e223a322c22706164223a227878227d03009b79fa8a4837144630038d5d2e550500617564\
        697402007031010000006600000028b52ffd44000300bd0200e404ff0000000001a130a88e547795956a\
        58f10f8a1476240030316131333061382d386535342d373739352d393536612d35386631306638613134\
        37367b226e223a332c22706164223a22787878227d015419022b8ccc90b84a2b";

    /// The six produce bodies of the three messages: (name, route, bytes).
    pub(crate) fn produce_bodies() -> Vec<(&'static str, &'static str, Vec<u8>)> {
        let bin = |fixture: &str| hex::decode(fixture).expect("fixture");
        let items = three_items();
        vec![
            (
                "json push",
                "/api/v1/push",
                format!(r#"{{"items":[{items}]}}"#).into_bytes(),
            ),
            ("QPB1", "/api/v1/push", bin(QPB1_PLAIN)),
            ("QPB1 zstd, counts", "/api/v1/push", bin(QPB1_ZSTD_COUNTS)),
            ("QPB1 frames", "/api/v1/push", bin(QPB1_FRAMES)),
            (
                "json transaction",
                "/api/v1/transaction",
                format!(r#"{{"operations":[{{"type":"push","items":[{items}]}}]}}"#).into_bytes(),
            ),
            ("QTB1", "/api/v1/transaction", bin(QTB1)),
        ]
    }

    /// One zstd frame holding `data` in raw blocks, its header declaring
    /// `declared` bytes of content (`None`: it declares nothing). A frame any
    /// decoder reads when `declared` is the truth, and no compressor is needed
    /// to make it.
    pub(crate) fn zstd_frame(data: &[u8], declared: Option<u64>) -> Vec<u8> {
        let mut z = vec![0x28, 0xb5, 0x2f, 0xfd];
        match declared {
            None => z.extend_from_slice(&[0x00, 0x38]), // a 128 KiB window, no size
            Some(n) if n < 256 => z.extend_from_slice(&[0x20, n as u8]),
            Some(n) if n < 65_536 + 256 => {
                z.push(0x60);
                z.extend_from_slice(&((n - 256) as u16).to_le_bytes());
            }
            Some(n) if n <= u64::from(u32::MAX) => {
                z.push(0xa0);
                z.extend_from_slice(&(n as u32).to_le_bytes());
            }
            Some(n) => {
                z.push(0xe0);
                z.extend_from_slice(&n.to_le_bytes());
            }
        }
        // One raw block, the last: `u24 (size << 3 | last)`, then the bytes.
        z.extend_from_slice(&(((data.len() as u32) << 3) | 1).to_le_bytes()[..3]);
        z.extend_from_slice(data);
        z
    }

    /// One group of a framed body: its names, `n`, and `z` as its frames.
    pub(crate) fn group(queue: &str, partition: &str, n: u32, z: &[u8]) -> Vec<u8> {
        let mut g = (queue.len() as u16).to_le_bytes().to_vec();
        g.extend_from_slice(queue.as_bytes());
        g.extend_from_slice(&(partition.len() as u16).to_le_bytes());
        g.extend_from_slice(partition.as_bytes());
        g.extend_from_slice(&n.to_le_bytes());
        g.extend_from_slice(&(z.len() as u32).to_le_bytes());
        g.extend_from_slice(z);
        g
    }

    /// A binary push of `groups` with `flags`.
    pub(crate) fn framed_push(flags: u8, groups: &[Vec<u8>]) -> Vec<u8> {
        let mut b = b"QPB1".to_vec();
        b.push(flags);
        b.extend_from_slice(&(groups.len() as u32).to_le_bytes());
        b.extend(groups.iter().flatten());
        b
    }

    /// One stored frame (`crate::frames` in the broker):
    /// `u32 body_len | u8 flags | u8[16] message id | u16 len, transactionId | payload`.
    pub(crate) fn stored_frame(i: u8) -> Vec<u8> {
        let (txn, payload) = (format!("t-{i}"), format!(r#"{{"n":{i}}}"#));
        let mut id = [0u8; 16];
        (id[0], id[6], id[8]) = (i, 0x70, 0x80);
        let mut body = vec![0u8];
        body.extend_from_slice(&id);
        body.extend_from_slice(&(txn.len() as u16).to_le_bytes());
        body.extend_from_slice(txn.as_bytes());
        body.extend_from_slice(payload.as_bytes());
        let mut frame = (body.len() as u32).to_le_bytes().to_vec();
        frame.extend_from_slice(&body);
        frame
    }

    /// A framed pop answer of four messages (`render_pop_frames`): one claim on
    /// partition 7 ("p1") of "orders", offsets 10..=13, lease "lease-1", in one
    /// record. `compressed`: the record is a zstd frame of the four frames
    /// (codec 1), as the queue log stores a batch; otherwise the frames as
    /// they are (codec 0).
    pub(crate) fn framed_pop_answer(compressed: bool) -> Vec<u8> {
        fn text(b: &mut Vec<u8>, s: &str) {
            b.extend_from_slice(&(s.len() as u16).to_le_bytes());
            b.extend_from_slice(s.as_bytes());
        }
        let frames: Vec<u8> = (10..14).flat_map(stored_frame).collect();
        let record = if compressed {
            zstd_frame(&frames, Some(frames.len() as u64))
        } else {
            frames
        };
        let own_keys = r#"{"frames":true}"#;
        let mut b = b"QPF1\x01".to_vec();
        b.extend_from_slice(&(own_keys.len() as u32).to_le_bytes());
        b.extend_from_slice(own_keys.as_bytes());
        text(&mut b, "orders");
        text(&mut b, "lease-1");
        text(&mut b, "__QUEUE_MODE__");
        b.extend_from_slice(&1u32.to_le_bytes()); // claims
        b.extend_from_slice(&7u64.to_le_bytes()); // partition id
        text(&mut b, "p1");
        b.extend_from_slice(&10u64.to_le_bytes()); // start
        b.extend_from_slice(&13u64.to_le_bytes()); // end
        b.extend_from_slice(&1u32.to_le_bytes()); // delivery attempt
        b.extend_from_slice(&1u32.to_le_bytes()); // records
        b.extend_from_slice(&10u64.to_le_bytes()); // base offset
        b.extend_from_slice(&1_700_000_000_000_000i64.to_le_bytes()); // created at, microseconds
        b.extend_from_slice(&4u32.to_le_bytes()); // count
        b.push(u8::from(compressed)); // codec
        b.extend_from_slice(&(record.len() as u32).to_le_bytes());
        b.extend_from_slice(&record);
        b
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::*;
    use super::*;

    /// The Go encoder's bodies, at the sizes a real inflate gave on 2026-10-12
    /// (`zstd -d` on each compressed part): what is on the wire, and what the
    /// headers say.
    #[test]
    fn the_reference_bodies_weigh_what_their_headers_say() {
        let sizes: Vec<(&str, usize, u64)> = produce_bodies()
            .into_iter()
            .map(|(name, path, body)| (name, body.len(), request_bytes(path, &body).expect(name)))
            .collect();
        let json_push = sizes[0].1;
        let json_txn = sizes[4].1;
        assert_eq!(
            sizes,
            vec![
                ("json push", json_push, json_push as u64),
                ("QPB1", 401, 401),
                // The same body as one zstd frame: 396 bytes of it behind 9 of header.
                ("QPB1 zstd, counts", 105, 405),
                // Two groups, whose frames declare 261 and 259 bytes.
                ("QPB1 frames", 293, 568),
                ("json transaction", json_txn, json_txn as u64),
                ("QTB1", 313, 589),
            ]
        );
    }

    #[test]
    fn a_group_weighs_what_its_frame_declares_in_every_width_of_the_field() {
        // One, two, four and eight bytes of content size in the frame header.
        for size in [0u64, 255, 256, 65_791, 65_792, u64::from(u32::MAX), 1 << 40] {
            let z = zstd_frame(b"", Some(size));
            assert_eq!(frame_content_size(&z), Some(size), "{size}");
            let body = framed_push(FLAG_FRAMES, &[group("q", "p", 1, &z)]);
            let around = (body.len() - z.len()) as u64;
            assert_eq!(
                request_bytes("/api/v1/push", &body),
                Ok(around + size),
                "{size}"
            );
        }
        // A dictionary id and a window descriptor sit in front of the size.
        let mut z = vec![0x28, 0xb5, 0x2f, 0xfd, 0x40 | 0x02, 0x38, 0xaa, 0xbb];
        z.extend_from_slice(&700u16.to_le_bytes());
        assert_eq!(frame_content_size(&z), Some(700 + 256));
    }

    #[test]
    fn two_groups_and_what_follows_them_are_counted_once_each() {
        let (a, b) = (
            zstd_frame(b"aaaa", Some(1_000)),
            zstd_frame(b"bb", Some(50)),
        );
        let groups = [group("q", "p", 3, &a), group("r", "", 1, &b)];
        let mut body = framed_push(FLAG_FRAMES | FLAG_COUNTS, &groups);
        let around = (body.len() - a.len() - b.len()) as u64;
        assert_eq!(request_bytes("/api/v1/push", &body), Ok(around + 1_050));
        // The broker does not read past the last group of a push; the bytes
        // are still on the request, at what they measure.
        body.extend_from_slice(b"tail");
        assert_eq!(request_bytes("/api/v1/push", &body), Ok(around + 1_050 + 4));
    }

    #[test]
    fn a_whole_body_frame_weighs_its_raw_len() {
        let mut body = b"QPB1".to_vec();
        body.push(FLAG_ZSTD | FLAG_TXN);
        body.extend_from_slice(&5_000u32.to_le_bytes());
        body.extend_from_slice(&zstd_frame(b"whatever is in it", None));
        assert_eq!(request_bytes("/api/v1/push", &body), Ok(9 + 5_000));
    }

    #[test]
    fn a_binary_transaction_weighs_its_head_and_its_groups() {
        let head = br#"{"operations":[{"type":"ack","transactionId":"t","partitionId":"7"}]}"#;
        let z = zstd_frame(b"frames", Some(4_096));
        let mut body = b"QTB1".to_vec();
        body.push(TXN_FLAG_COUNTS);
        body.extend_from_slice(&(head.len() as u32).to_le_bytes());
        body.extend_from_slice(head);
        body.extend_from_slice(&1u32.to_le_bytes());
        body.extend_from_slice(&group("q", "p", 2, &z));
        let around = (body.len() - z.len()) as u64;
        assert_eq!(
            request_bytes("/api/v1/transaction", &body),
            Ok(around + 4_096)
        );
    }

    #[test]
    fn a_body_that_is_not_this_routes_binary_body_weighs_what_it_measures() {
        let json = br#"{"items":[{"queue":"q","payload":1}]}"#;
        assert_eq!(request_bytes("/api/v1/push", json), Ok(json.len() as u64));
        assert_eq!(request_bytes("/api/v1/push", b"not json"), Ok(8));
        assert_eq!(request_bytes("/api/v1/push", b""), Ok(0));
        // The broker reads these as JSON too, and answers its own 400.
        let push = framed_push(FLAG_FRAMES, &[]);
        assert!(!is_binary("/api/v1/transaction", &push));
        assert_eq!(
            request_bytes("/api/v1/transaction", &push),
            Ok(push.len() as u64)
        );
        assert_eq!(request_bytes("/api/v1/push", b"QTB1\x00"), Ok(5));
        assert_eq!(
            request_bytes("/streams/v1/cycle", &push),
            Ok(push.len() as u64)
        );
    }

    /// Fail closed: every one of these is a binary body with no size, and the
    /// gateway refuses it.
    #[test]
    fn a_binary_body_the_reader_cannot_follow_has_no_size() {
        let z = zstd_frame(b"x", Some(1));
        let good = framed_push(FLAG_FRAMES, &[group("q", "p", 1, &z)]);
        assert!(request_bytes("/api/v1/push", &good).is_ok());
        let unreadable = |body: &[u8]| request_bytes("/api/v1/push", body).expect_err("no size").0;

        // Compressed twice: the groups are inside the outer frame.
        let mut twice = b"QPB1".to_vec();
        twice.push(FLAG_ZSTD | FLAG_FRAMES);
        twice.extend_from_slice(&64u32.to_le_bytes());
        twice.extend_from_slice(&zstd_frame(&good[5..], Some((good.len() - 5) as u64)));
        assert!(unreadable(&twice).starts_with("flags 1 and 8 together"));

        assert_eq!(unreadable(&framed_push(16, &[])), "unknown flags");
        assert_eq!(unreadable(b"QPB1"), "truncated");
        assert_eq!(unreadable(b"QPB1\x01\x00\x00"), "truncated");
        assert_eq!(unreadable(&good[..good.len() - 1]), "truncated");
        // More groups announced than sent.
        let mut short = good.clone();
        short[5] = 2;
        assert_eq!(unreadable(&short), "truncated");

        let no_size = "a group's zstd frame does not declare its content size";
        for frame in [
            zstd_frame(b"x", None),
            b"not a zstd frame".to_vec(),
            vec![0x28, 0xb5, 0x2f, 0xfd],
            vec![0x28, 0xb5, 0x2f, 0xfd, 0x28, 0x01], // the reserved bit
            vec![0x28, 0xb5, 0x2f, 0xfd, 0x60, 0x01], // a size cut short
        ] {
            let body = framed_push(FLAG_FRAMES, &[group("q", "p", 1, &frame)]);
            assert_eq!(unreadable(&body), no_size, "{frame:02x?}");
        }

        // The same on the transaction route.
        let txn = |flags: u8, head_len: u32| {
            let mut b = b"QTB1".to_vec();
            b.push(flags);
            b.extend_from_slice(&head_len.to_le_bytes());
            b.extend_from_slice(b"{}");
            b.extend_from_slice(&1u32.to_le_bytes());
            b.extend_from_slice(&group("q", "p", 1, &z));
            b
        };
        assert!(request_bytes("/api/v1/transaction", &txn(0, 2)).is_ok());
        let unreadable = |body: &[u8]| {
            request_bytes("/api/v1/transaction", body)
                .expect_err("no size")
                .0
        };
        assert_eq!(unreadable(&txn(1, 2)), "unknown flags");
        assert_eq!(unreadable(&txn(0, 900)), "truncated");
    }

    #[test]
    fn a_pop_answer_weighs_its_records_at_their_declared_size() {
        let json = br#"{"success":true,"messages":[{"data":{"n":1}}]}"#;
        assert_eq!(pop_answer_bytes(json), json.len() as u64);
        assert_eq!(pop_answer_bytes(b""), 0);

        // The four frames as they are, and as one zstd frame.
        let raw = framed_pop_answer(false);
        assert_eq!(pop_answer_bytes(&raw), raw.len() as u64);
        let compressed = framed_pop_answer(true);
        assert_eq!(pop_answer_bytes(&compressed), raw.len() as u64);
        assert!(
            compressed.len() > raw.len(),
            "raw blocks: the frame only adds its header"
        );

        // An answer this reader cannot follow weighs what it measures: cut
        // short, bytes after the last record, another version, another codec.
        let cut = &compressed[..compressed.len() - 1];
        assert_eq!(pop_answer_bytes(cut), cut.len() as u64);
        let mut more = compressed.clone();
        more.push(0);
        assert_eq!(pop_answer_bytes(&more), more.len() as u64);
        let mut version = compressed.clone();
        version[4] = 2;
        assert_eq!(pop_answer_bytes(&version), version.len() as u64);
        // `.. | u8 codec | u32 len | the record`, and the record is the last thing.
        let record = 4 * stored_frame(10).len() + (compressed.len() - raw.len());
        let codec_at = compressed.len() - record - 5;
        let mut codec = compressed.clone();
        assert_eq!(codec[codec_at], 1, "the codec byte");
        codec[codec_at] = 9;
        assert_eq!(pop_answer_bytes(&codec), codec.len() as u64);
    }
}
