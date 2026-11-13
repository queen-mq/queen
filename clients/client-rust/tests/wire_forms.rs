//! The binary push body and the framed pop answer, as they cross the socket.
//!
//! These run against [`support::FakeBroker`] and need no stack. What is at
//! stake: a client that sets none of the three options must send the JSON
//! requests every released broker knows, and a consumer that sets `framed_pop`
//! must be handed exactly the messages the JSON answer gives it.

mod support;

use std::sync::{Arc, Mutex};

use queen_mq::pop_frames::{decode_pop_frames, is_pop_frames};
use queen_mq::{BufferOptions, Config, PopResponse, PushItem, Queen};
use support::{FakeBroker, Hit, Reply};

const TXN: &str = "0190aaaa-0000-7000-8000-000000000001";
const POP_FRAMES: &str = "application/x-queen-pop";

fn client(broker: &FakeBroker, config: impl FnOnce(Config) -> Config) -> Queen {
    Queen::connect(config(Config::new(broker.url()).retry_attempts(1))).expect("fake broker url")
}

/// A push answer of `n` queued items, shaped as the broker renders it.
fn queued(n: usize) -> Reply {
    let items: Vec<String> = (0..n)
        .map(|i| {
            format!(
                r#"{{"index":{i},"message_id":"0190aaaa-0000-7000-8000-00000000000{i}","transaction_id":"t{i}","queueName":"orders","status":"queued","offset":{i}}}"#
            )
        })
        .collect();
    Reply::ok(format!("[{}]", items.join(",")))
}

fn pushes(broker: &FakeBroker) -> Vec<Hit> {
    let mut hits = broker.hits();
    hits.retain(|h| h.method == "POST" && h.route() == "/api/v1/push");
    hits
}

fn pops(broker: &FakeBroker) -> Vec<Hit> {
    let mut hits = broker.hits();
    hits.retain(|h| h.method == "GET" && h.route().starts_with("/api/v1/pop"));
    hits
}

// ===========================================================================
// Reading a binary push body
// ===========================================================================

/// Reads the fields of a binary push body in order.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> &'a [u8] {
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        head
    }

    fn u16(&mut self) -> usize {
        u16::from_le_bytes(self.take(2).try_into().unwrap()) as usize
    }

    fn u32(&mut self) -> usize {
        u32::from_le_bytes(self.take(4).try_into().unwrap()) as usize
    }

    fn text16(&mut self) -> String {
        let n = self.u16();
        String::from_utf8(self.take(n).to_vec()).unwrap()
    }
}

/// `(partition, [(transaction id, payload)])` per group of a plain binary push
/// body (after magic and flags), every item with its transaction id.
fn plain_groups(body: &[u8]) -> Vec<(String, Vec<(String, String)>)> {
    let mut r = Reader(body);
    let mut groups = Vec::new();
    for _ in 0..r.u32() {
        let (_queue, partition) = (r.text16(), r.text16());
        let mut items = Vec::new();
        for _ in 0..r.u32() {
            let txn = r.text16();
            let n = r.u32();
            items.push((txn, String::from_utf8(r.take(n).to_vec()).unwrap()));
        }
        groups.push((partition, items));
    }
    assert!(r.0.is_empty(), "bytes left after the last payload");
    groups
}

struct StoredFrame {
    flags: u8,
    id: [u8; 16],
    txn: String,
    payload: String,
}

/// One group of client-built frames: `(queue, partition, count, frames)`.
type FramedGroup = (String, String, usize, Vec<StoredFrame>);

/// The groups of a framed binary push body.
fn framed_groups(body: &[u8]) -> Vec<FramedGroup> {
    let mut r = Reader(body);
    let mut groups = Vec::new();
    for _ in 0..r.u32() {
        let (queue, partition, count) = (r.text16(), r.text16(), r.u32());
        let n = r.u32();
        let raw = zstd::stream::decode_all(r.take(n)).expect("one zstd frame");
        let mut f = Reader(&raw);
        let mut frames = Vec::new();
        while !f.0.is_empty() {
            let n = f.u32();
            let mut body = Reader(f.take(n));
            let flags = body.take(1)[0];
            let id: [u8; 16] = body.take(16).try_into().unwrap();
            let txn = body.text16();
            frames.push(StoredFrame {
                flags,
                id,
                txn,
                payload: String::from_utf8(body.0.to_vec()).unwrap(),
            });
        }
        groups.push((queue, partition, count, frames));
    }
    assert!(r.0.is_empty(), "bytes left after the last group");
    groups
}

// ===========================================================================
// Push
// ===========================================================================

#[tokio::test]
async fn a_push_is_json_by_default() {
    let broker = FakeBroker::start(vec![queued(1)]).await;
    let res = client(&broker, |c| c)
        .queue("orders")
        .partition("p1")
        .push(serde_json::json!({"a": 1}))
        .await
        .expect("push");

    assert_eq!(res.len(), 1);
    let hit = pushes(&broker).remove(0);
    assert_eq!(hit.header("content-type"), Some("application/json"));
    let body = hit.json();
    assert_eq!(body["items"][0]["payload"], serde_json::json!({"a": 1}));
    assert_eq!(body["items"][0]["partition"], "p1");
}

#[tokio::test]
async fn binary_push_sends_the_binary_body_with_each_transaction_id() {
    let broker = FakeBroker::start(vec![queued(1)]).await;
    let res = client(&broker, |c| c.binary_push(true))
        .queue("orders")
        .push_items(vec![PushItem::new("orders", serde_json::json!({"a": 1}))
            .partition("p1")
            .transaction_id(TXN)])
        .await
        .expect("push");

    assert_eq!(res.len(), 1);
    let hit = pushes(&broker).remove(0);
    assert_eq!(hit.header("content-type"), Some("application/x-queen-push"));
    assert!(hit.raw.starts_with(b"QPB1\x04"), "{:?}", &hit.raw[..8]);
    assert_eq!(
        plain_groups(&hit.raw[5..]),
        vec![(
            "p1".to_string(),
            vec![(TXN.to_string(), r#"{"a":1}"#.to_string())]
        )]
    );
}

#[tokio::test]
async fn binary_push_keeps_the_order_and_groups_consecutive_items_of_a_partition() {
    let broker = FakeBroker::start(vec![queued(4)]).await;
    let item = |partition: &str, n: u64| {
        PushItem::new("orders", serde_json::json!(n))
            .partition(partition)
            .transaction_id(format!("t{n}"))
    };
    client(&broker, |c| c.binary_push(true))
        .queue("orders")
        .push_items(vec![item("a", 1), item("a", 2), item("b", 3), item("a", 4)])
        .await
        .expect("push");

    let groups = plain_groups(&pushes(&broker)[0].raw[5..]);
    let shape: Vec<(&str, Vec<&str>)> = groups
        .iter()
        .map(|(p, items)| (p.as_str(), items.iter().map(|(t, _)| t.as_str()).collect()))
        .collect();
    assert_eq!(
        shape,
        vec![
            ("a", vec!["t1", "t2"]),
            ("b", vec!["t3"]),
            ("a", vec!["t4"])
        ]
    );
}

#[tokio::test]
async fn binary_push_carries_payloads_that_are_not_objects_and_non_ascii_text() {
    let broker = FakeBroker::start(vec![queued(5)]).await;
    let payloads = vec![
        serde_json::json!("testo \"con\" virgolette\n"),
        serde_json::json!([1, 2]),
        serde_json::json!(7),
        serde_json::Value::Null,
        serde_json::json!({"è": "€"}),
    ];
    client(&broker, |c| c.binary_push(true))
        .queue("ordini")
        .partition("città")
        .push_many(payloads.clone())
        .await
        .expect("push");

    let groups = plain_groups(&pushes(&broker)[0].raw[5..]);
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].0, "città");
    let sent: Vec<serde_json::Value> = groups[0]
        .1
        .iter()
        .map(|(txn, payload)| {
            assert_eq!(txn.len(), 36, "the client mints a transaction id");
            serde_json::from_str(payload).unwrap()
        })
        .collect();
    assert_eq!(sent, payloads);
}

#[tokio::test]
async fn a_name_the_binary_body_does_not_carry_stays_json() {
    let broker = FakeBroker::start(vec![queued(1)]).await;
    client(&broker, |c| c.binary_push(true))
        .queue("orders")
        .partition("p".repeat(70_000))
        .push(serde_json::json!(1))
        .await
        .expect("push");

    assert_eq!(
        pushes(&broker)[0].header("content-type"),
        Some("application/json")
    );
}

#[tokio::test]
async fn a_buffered_push_is_binary_too() {
    let broker = FakeBroker::start(vec![queued(1)]).await;
    let queen = client(&broker, |c| c.binary_push(true));
    let queue = queen
        .queue("orders")
        .partition("p1")
        .buffer(BufferOptions::default());
    queue.push(serde_json::json!({"a": 1})).await.expect("push");
    queue.flush_buffer().await.expect("flush");

    let hit = pushes(&broker).remove(0);
    assert!(hit.raw.starts_with(b"QPB1\x04"));
    assert_eq!(plain_groups(&hit.raw[5..])[0].1[0].1, r#"{"a":1}"#);
}

#[tokio::test]
async fn push_compression_sends_each_partition_as_one_zstd_frame_of_stored_frames() {
    let broker = FakeBroker::start(vec![queued(3)]).await;
    client(&broker, |c| c.binary_push(true).push_compression(true))
        .queue("orders")
        .push_items(vec![
            PushItem::new("orders", serde_json::json!({"a": 1}))
                .partition("p1")
                .transaction_id(TXN),
            PushItem::new("orders", serde_json::json!({"a": 2})).partition("p1"),
            PushItem::new("orders", serde_json::json!("x")).partition("p1"),
        ])
        .await
        .expect("push");

    let hit = pushes(&broker).remove(0);
    assert_eq!(hit.header("content-type"), Some("application/x-queen-push"));
    assert!(hit.raw.starts_with(b"QPB1\x08"), "{:?}", &hit.raw[..8]);
    let groups = framed_groups(&hit.raw[5..]);
    assert_eq!(groups.len(), 1);
    let (queue, partition, count, frames) = &groups[0];
    assert_eq!(
        (queue.as_str(), partition.as_str(), *count),
        ("orders", "p1", 3)
    );
    let payloads: Vec<&str> = frames.iter().map(|f| f.payload.as_str()).collect();
    assert_eq!(payloads, vec![r#"{"a":1}"#, r#"{"a":2}"#, r#""x""#]);
    assert_eq!(frames[0].txn, TXN);
    for frame in frames {
        assert_eq!(frame.flags, 0);
        assert_eq!(frame.id[6] >> 4, 7, "the message id is a version-7 UUID");
        assert_eq!(frame.id[8] & 0xc0, 0x80);
        assert_eq!(frame.txn.len(), 36);
    }
    // A message without a transaction id of its own gets the text of its id.
    let hex: String = frames[1].id.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(frames[1].txn.replace('-', ""), hex);
    assert_ne!(frames[1].id, frames[2].id);
}

#[tokio::test]
async fn each_zstd_frame_carries_its_content_size() {
    // The broker asks for it of a frame it stores as it came.
    let broker = FakeBroker::start(vec![queued(1)]).await;
    client(&broker, |c| c.binary_push(true).push_compression(true))
        .queue("orders")
        .push(serde_json::json!({"a": 1}))
        .await
        .expect("push");

    let hit = pushes(&broker).remove(0);
    let mut r = Reader(&hit.raw[5..]);
    let (_groups, _queue, _partition, _count) = (r.u32(), r.text16(), r.text16(), r.u32());
    let n = r.u32();
    let frame = r.take(n);
    assert_eq!(
        zstd::zstd_safe::get_frame_content_size(frame)
            .ok()
            .flatten(),
        Some(
            zstd::stream::decode_all(frame)
                .expect("one zstd frame")
                .len() as u64
        )
    );
}

// ===========================================================================
// Building a framed pop answer
// ===========================================================================

fn text(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u16).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

/// One stored frame: u32 body_len | flags | id | [trace] | txn | [producer] | payload.
fn stored_frame(
    id: u8,
    txn: &str,
    payload: &str,
    producer: Option<&str>,
    trace: Option<[u8; 16]>,
) -> Vec<u8> {
    let mut body = vec![u8::from(trace.is_some()) | (u8::from(producer.is_some()) << 1)];
    let mut id_bytes = [0u8; 16];
    (id_bytes[0], id_bytes[6], id_bytes[8]) = (id, 0x70, 0x80);
    body.extend_from_slice(&id_bytes);
    if let Some(trace) = trace {
        body.extend_from_slice(&trace);
    }
    text(&mut body, txn);
    if let Some(producer) = producer {
        text(&mut body, producer);
    }
    body.extend_from_slice(payload.as_bytes());
    let mut frame = (body.len() as u32).to_le_bytes().to_vec();
    frame.extend_from_slice(&body);
    frame
}

/// A framed pop answer of one claim on partition 7 ("p7") of "orders", lease
/// "lease-1": one record at base offset 10 holding `frames`, created at
/// 2023-11-14T22:13:20Z, of which the claim covers start..=end.
fn framed(own: &str, start: u64, end: u64, compress: bool, frames: &[Vec<u8>]) -> Vec<u8> {
    let mut raw = frames.concat();
    if compress {
        raw = zstd::bulk::compress(&raw, 1).unwrap();
    }
    let mut b = b"QPF1\x01".to_vec();
    b.extend_from_slice(&(own.len() as u32).to_le_bytes());
    b.extend_from_slice(own.as_bytes());
    text(&mut b, "orders");
    text(&mut b, "lease-1");
    text(&mut b, "__QUEUE_MODE__");
    b.extend_from_slice(&1u32.to_le_bytes()); // claims
    b.extend_from_slice(&7u64.to_le_bytes()); // partition id
    text(&mut b, "p7");
    b.extend_from_slice(&start.to_le_bytes());
    b.extend_from_slice(&end.to_le_bytes());
    b.extend_from_slice(&3u32.to_le_bytes()); // delivery attempt
    b.extend_from_slice(&1u32.to_le_bytes()); // records
    b.extend_from_slice(&10u64.to_le_bytes()); // base offset
    b.extend_from_slice(&1_700_000_000_000_000i64.to_le_bytes()); // created at, microseconds
    b.extend_from_slice(&(frames.len() as u32).to_le_bytes());
    b.push(u8::from(compress));
    b.extend_from_slice(&(raw.len() as u32).to_le_bytes());
    b.extend_from_slice(&raw);
    b
}

fn two_frames() -> Vec<Vec<u8>> {
    vec![
        stored_frame(1, "t-10", r#"{"n":10}"#, None, None),
        stored_frame(2, "t-11", r#"{"n":11,"s":"x"}"#, Some("svc"), None),
    ]
}

/// The JSON answer of the same two messages, as the broker renders it.
const TWO_MESSAGES_JSON: &str = concat!(
    r#"{"success":true,"queue":"orders","partition":"p7","partitionId":"7","leaseId":"lease-1","consumerGroup":"__QUEUE_MODE__","messages":["#,
    r#"{"id":"01000000-0000-7000-8000-000000000000","transactionId":"t-10","traceId":null,"data":{"n":10},"producerSub":null,"createdAt":"2023-11-14T22:13:20.000Z","partitionId":"7","partition":"p7","leaseId":"lease-1","consumerGroup":"__QUEUE_MODE__","deliveryAttempt":3,"offset":10},"#,
    r#"{"id":"02000000-0000-7000-8000-000000000000","transactionId":"t-11","traceId":null,"data":{"n":11,"s":"x"},"producerSub":"svc","createdAt":"2023-11-14T22:13:20.000Z","partitionId":"7","partition":"p7","leaseId":"lease-1","consumerGroup":"__QUEUE_MODE__","deliveryAttempt":3,"offset":11}"#,
    r#"],"partitionsClaimed":1}"#
);

// ===========================================================================
// The decoder
// ===========================================================================

#[test]
fn decode_unpacks_a_record_into_the_json_answers_response() {
    let want: PopResponse = serde_json::from_str(TWO_MESSAGES_JSON).unwrap();
    for compress in [false, true] {
        let got = decode_pop_frames(&framed(
            r#"{"frames":true}"#,
            10,
            11,
            compress,
            &two_frames(),
        ))
        .unwrap_or_else(|e| panic!("compress={compress}: {e}"));
        assert_eq!(got, want, "compress={compress}");
    }
}

#[test]
fn decode_keeps_only_the_messages_inside_the_claim() {
    for compress in [false, true] {
        let got = decode_pop_frames(&framed(
            r#"{"frames":true}"#,
            11,
            11,
            compress,
            &two_frames(),
        ))
        .unwrap();
        assert_eq!(got.messages.len(), 1, "compress={compress}");
        let m = &got.messages[0];
        assert_eq!(m.transaction_id, "t-11");
        assert_eq!(m.producer_sub.as_deref(), Some("svc"));
        assert_eq!(m.id, "02000000-0000-7000-8000-000000000000");
        assert_eq!((m.partition_id.as_str(), m.partition.as_str()), ("7", "p7"));
    }
}

#[test]
fn decode_reads_a_trace_id_an_empty_payload_and_an_answer_without_own_keys() {
    let mut trace = [0u8; 16];
    (trace[0], trace[6], trace[8], trace[15]) = (0x01, 0x40, 0x80, 0x01);
    let frames = vec![
        stored_frame(1, "t-10", "", None, Some(trace)),
        stored_frame(2, "t-11", r#"[1,"due"]"#, None, None),
    ];
    let answer = framed("", 10, 11, false, &frames);
    let got = decode_pop_frames(&answer).unwrap();

    assert_eq!(
        got.messages[0].trace_id.as_deref(),
        Some("01000000-0000-4000-8000-000000000001")
    );
    assert_eq!(got.messages[0].data, serde_json::Value::Null);
    assert_eq!(got.messages[1].data, serde_json::json!([1, "due"]));

    // Version 0 has no JSON object in front of the names.
    let mut v0 = b"QPF1\x00".to_vec();
    v0.extend_from_slice(&answer[9..]);
    assert_eq!(decode_pop_frames(&v0).unwrap(), got);
}

#[test]
fn decode_reads_the_answers_own_keys() {
    let own = r#"{"frames":true,"conflation":true,"autopilot":{"partitions":4,"batch":50}}"#;
    let got = decode_pop_frames(&framed(own, 10, 11, false, &two_frames())).unwrap();

    assert_eq!(got.conflation, Some(true));
    let autopilot = got.autopilot.expect("the autopilot's choice");
    assert_eq!((autopilot.partitions, autopilot.batch), (4, 50));
}

#[test]
fn decode_rejects_damage() {
    let frames = two_frames();
    let answer = framed(r#"{"frames":true}"#, 10, 11, false, &frames);
    let tail = frames[0].len() + frames[1].len();

    assert!(is_pop_frames(&answer));
    assert!(!is_pop_frames(TWO_MESSAGES_JSON.as_bytes()));
    assert!(
        decode_pop_frames(&answer[..answer.len() - 3]).is_err(),
        "truncated"
    );
    assert!(
        decode_pop_frames(TWO_MESSAGES_JSON.as_bytes()).is_err(),
        "JSON"
    );

    let mut future = answer.clone();
    future[4] = 9;
    let err = decode_pop_frames(&future).unwrap_err().to_string();
    assert!(err.contains("unknown version 9"), "{err}");

    // A record that declares more frames than it holds.
    let mut short = answer.clone();
    let at = short.len() - tail - 9;
    short[at..at + 4].copy_from_slice(&3u32.to_le_bytes());
    let err = decode_pop_frames(&short).unwrap_err().to_string();
    assert!(err.contains("a record is shorter than its frames"), "{err}");

    let mut bad_codec = answer.clone();
    let at = bad_codec.len() - tail - 5;
    bad_codec[at] = 7;
    let err = decode_pop_frames(&bad_codec).unwrap_err().to_string();
    assert!(err.contains("unknown codec 7"), "{err}");
}

// ===========================================================================
// Pop
// ===========================================================================

#[tokio::test]
async fn a_pop_sends_no_frames_parameter_by_default() {
    let broker = FakeBroker::start(vec![Reply::ok(TWO_MESSAGES_JSON)]).await;
    let messages = client(&broker, |c| c)
        .queue("orders")
        .batch(2)
        .wait(false)
        .pop()
        .await
        .expect("pop");

    assert_eq!(messages.len(), 2);
    assert_eq!(pops(&broker)[0].query("frames"), None);
}

#[tokio::test]
async fn a_framed_pop_gives_the_messages_of_the_json_answer() {
    let json = FakeBroker::start(vec![Reply::ok(TWO_MESSAGES_JSON)]).await;
    let want = client(&json, |c| c)
        .queue("orders")
        .batch(2)
        .wait(false)
        .pop()
        .await
        .expect("JSON pop");

    for compress in [false, true] {
        let answer = framed(r#"{"frames":true}"#, 10, 11, compress, &two_frames());
        let broker = FakeBroker::start(vec![Reply::bytes(POP_FRAMES, answer)]).await;
        let got = client(&broker, |c| c.framed_pop(true))
            .queue("orders")
            .batch(2)
            .wait(false)
            .pop()
            .await
            .expect("framed pop");

        assert_eq!(pops(&broker)[0].query("frames"), Some("true"));
        assert_eq!(got.len(), 2);
        assert_eq!(got, want, "compress={compress}");
    }
}

#[tokio::test]
async fn a_framed_pop_asks_for_frames_on_the_partition_and_namespace_routes_too() {
    let broker = FakeBroker::start(vec![Reply::status(204)]).await;
    let queen = client(&broker, |c| c.framed_pop(true));
    queen
        .queue("orders")
        .partition("p7")
        .wait(false)
        .pop()
        .await
        .expect("pop");
    queen
        .queue_opt(None)
        .namespace("ns")
        .wait(false)
        .pop()
        .await
        .expect("pop");

    let hits = pops(&broker);
    assert_eq!(hits[0].route(), "/api/v1/pop/queue/orders/partition/p7");
    assert_eq!(hits[0].query("frames"), Some("true"));
    assert_eq!(hits[1].route(), "/api/v1/pop");
    assert_eq!(hits[1].query("frames"), Some("true"));
}

/// A broker that does not know the parameter answers JSON: read as always.
#[tokio::test]
async fn a_framed_pop_reads_a_json_answer() {
    let broker = FakeBroker::start(vec![Reply::ok(TWO_MESSAGES_JSON)]).await;
    let got = client(&broker, |c| c.framed_pop(true))
        .queue("orders")
        .batch(2)
        .wait(false)
        .pop()
        .await
        .expect("pop");

    assert_eq!(got.len(), 2);
    assert_eq!(got[1].transaction_id, "t-11");
    assert_eq!(got[1].data["s"], "x");
}

#[tokio::test]
async fn a_framed_pop_of_nothing() {
    let broker = FakeBroker::start(vec![Reply::status(204)]).await;
    let got = client(&broker, |c| c.framed_pop(true))
        .queue("orders")
        .wait(false)
        .pop()
        .await
        .expect("pop");

    assert!(got.is_empty());
}

/// The autopilot's choice travels in the framed answer's own keys.
#[tokio::test]
async fn a_framed_pop_carries_the_autopilots_choice() {
    let own = r#"{"frames":true,"autopilot":{"partitions":4,"batch":50}}"#;
    let answer = framed(own, 10, 11, false, &two_frames());
    let broker = FakeBroker::start(vec![Reply::bytes(POP_FRAMES, answer)]).await;
    let out = client(&broker, |c| c.framed_pop(true))
        .queue("orders")
        .wait(false)
        .pop_result()
        .await
        .expect("pop");

    assert_eq!(out.messages.len(), 2);
    let autopilot = out.autopilot.expect("the autopilot's choice");
    assert_eq!((autopilot.partitions, autopilot.batch), (4, 50));
}

#[tokio::test]
async fn a_framed_pop_carries_the_conflation_echo_and_still_fails_without_it() {
    let echoed = framed(
        r#"{"frames":true,"conflation":true}"#,
        10,
        11,
        false,
        &two_frames(),
    );
    let broker = FakeBroker::start(vec![Reply::bytes(POP_FRAMES, echoed)]).await;
    let got = client(&broker, |c| c.framed_pop(true))
        .queue("orders")
        .group("g")
        .conflation(true)
        .wait(false)
        .pop()
        .await
        .expect("a conflating pop whose answer echoes it");
    assert_eq!(got.len(), 2);

    let silent = framed(r#"{"frames":true}"#, 10, 11, false, &two_frames());
    let broker = FakeBroker::start(vec![Reply::bytes(POP_FRAMES, silent)]).await;
    let err = client(&broker, |c| c.framed_pop(true))
        .queue("orders")
        .group("g2")
        .conflation(true)
        .wait(false)
        .pop()
        .await
        .expect_err("no echo, no conflation");
    assert!(
        err.to_string().to_lowercase().contains("conflation"),
        "{err}"
    );
}

/// The consumer loop on framed answers: the handler gets every message of the
/// claim, in order, and the loop stops at its limit.
#[tokio::test]
async fn consume_with_framed_pops_hands_the_handler_the_claim_in_order() {
    let answer = framed(r#"{"frames":true}"#, 10, 11, true, &two_frames());
    let broker = FakeBroker::start_with(move |_n, hit| {
        if hit.route().starts_with("/api/v1/pop") {
            Reply::bytes(POP_FRAMES, answer.clone())
        } else {
            Reply::ok(r#"[{"success":true},{"success":true}]"#)
        }
    })
    .await;
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&seen);

    client(&broker, |c| c.framed_pop(true))
        .queue("orders")
        .batch(2)
        .wait(false)
        .limit(2)
        .consume(move |msg| {
            let log = Arc::clone(&log);
            async move {
                log.lock().unwrap().push(format!(
                    "{}@{}/{}",
                    msg.transaction_id, msg.partition_id, msg.lease_id
                ));
                Ok::<(), queen_mq::Error>(())
            }
        })
        .await
        .expect("consume");

    assert_eq!(
        *seen.lock().unwrap(),
        vec!["t-10@7/lease-1".to_string(), "t-11@7/lease-1".to_string()]
    );
    let hits = pops(&broker);
    assert!(!hits.is_empty());
    assert!(hits.iter().all(|h| h.query("frames") == Some("true")));
}

// ===========================================================================
// Transaction
// ===========================================================================
//
// With binary_push and push_compression a bundle that pushes goes out as the
// binary transaction body. What is at stake is the default (a client that does
// not set both sends the JSON bundle every released broker knows) and, with
// the options, the order of the results: the broker lists the head's acks
// before the pushes, and the caller must get them back in the order of its own
// operations.

fn transactions(broker: &FakeBroker) -> Vec<Hit> {
    let mut hits = broker.hits();
    hits.retain(|h| h.method == "POST" && h.route() == "/api/v1/transaction");
    hits
}

fn framed_client(broker: &FakeBroker) -> Queen {
    client(broker, |c| c.binary_push(true).push_compression(true))
}

fn popped() -> queen_mq::Message {
    queen_mq::Message {
        id: "m".into(),
        transaction_id: "in-1".into(),
        trace_id: None,
        data: serde_json::Value::Null,
        producer_sub: None,
        headers: None,
        created_at: "2026-08-04T10:00:00Z".into(),
        partition_id: "7".into(),
        partition: "Default".into(),
        lease_id: "lease-1".into(),
        consumer_group: String::new(),
        offset: None,
    }
}

fn committed(results: &str) -> Reply {
    Reply::ok(format!(
        r#"{{"transactionId":"tx","success":true,"results":[{results}]}}"#
    ))
}

/// The head and the groups of a binary transaction body.
fn transaction_body(raw: &[u8]) -> (u8, serde_json::Value, Vec<FramedGroup>) {
    assert!(raw.starts_with(b"QTB1"), "{:?}", &raw[..8.min(raw.len())]);
    let head_len = u32::from_le_bytes(raw[5..9].try_into().unwrap()) as usize;
    let head = &raw[9..9 + head_len];
    let head = if head.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(head).expect("the head is JSON")
    };
    (raw[4], head, framed_groups(&raw[9 + head_len..]))
}

#[tokio::test]
async fn a_transaction_is_the_json_bundle_by_default() {
    for configure in [
        (|c| c) as fn(Config) -> Config,
        |c| c.binary_push(true),
        |c| c.push_compression(true),
    ] {
        let broker = FakeBroker::start(vec![committed(
            r#"{"index":0,"type":"ack","success":true},{"index":1,"type":"push","success":true}"#,
        )])
        .await;
        client(&broker, configure)
            .transaction()
            .ack(&popped())
            .push_to("out", "p1", serde_json::json!({"n": 1}))
            .unwrap()
            .commit()
            .await
            .expect("commit");

        let hit = transactions(&broker).remove(0);
        assert_eq!(hit.header("content-type"), Some("application/json"));
        assert_eq!(hit.json()["operations"][1]["type"], "push");
    }
}

#[tokio::test]
async fn the_pushes_travel_as_frames_and_the_rest_as_the_json_head() {
    let broker = FakeBroker::start(vec![committed(concat!(
        r#"{"index":0,"type":"ack","success":true,"transactionId":"in-1"},"#,
        r#"{"index":1,"type":"push","success":true,"transactionId":"a"},"#,
        r#"{"index":2,"type":"push","success":true,"transactionId":"tx-b"},"#,
        r#"{"index":3,"type":"push","success":true,"transactionId":"c"}"#
    ))])
    .await;
    let res = framed_client(&broker)
        .transaction()
        .ack(&popped())
        .push_to("out", "p1", serde_json::json!({"n": 1}))
        .unwrap()
        .push_item(
            queen_mq::TxnPushItem::new("out", serde_json::json!({"n": 2}))
                .partition("p1")
                .transaction_id("tx-b"),
        )
        .unwrap()
        .push_to("out", "p2", "text")
        .unwrap()
        .commit()
        .await
        .expect("commit");

    assert!(res.success);
    assert_eq!(res.results.len(), 4);
    let hit = transactions(&broker).remove(0);
    assert_eq!(hit.header("content-type"), Some("application/x-queen-push"));
    let (flags, head, groups) = transaction_body(&hit.raw);
    assert_eq!(flags, 0);
    // The head: the ack and the lease, and no push.
    assert_eq!(
        head,
        serde_json::json!({
            "operations": [{"type": "ack", "transactionId": "in-1", "partitionId": "7",
                            "status": "completed", "leaseId": "lease-1"}],
            "requiredLeases": ["lease-1"]
        })
    );
    // The pushes: consecutive items of one partition share a group.
    assert_eq!(groups.len(), 2);
    let (queue, partition, count, frames) = &groups[0];
    assert_eq!(
        (queue.as_str(), partition.as_str(), *count),
        ("out", "p1", 2)
    );
    assert_eq!(frames[0].payload, r#"{"n":1}"#);
    assert_eq!(frames[0].txn.len(), 36);
    assert_eq!(
        (frames[1].payload.as_str(), frames[1].txn.as_str()),
        (r#"{"n":2}"#, "tx-b")
    );
    let (queue, partition, count, frames) = &groups[1];
    assert_eq!(
        (queue.as_str(), partition.as_str(), *count),
        ("out", "p2", 1)
    );
    assert_eq!(frames[0].payload, r#""text""#);
}

#[tokio::test]
async fn the_results_come_back_in_the_order_of_the_callers_operations() {
    // The caller pushes, acks, pushes. The broker answers the ack first (it is
    // in the head), then the two pushes, then the rider.
    let broker = FakeBroker::start(vec![committed(concat!(
        r#"{"index":0,"type":"ack","success":true,"transactionId":"in-1"},"#,
        r#"{"index":1,"type":"push","success":true,"transactionId":"first"},"#,
        r#"{"index":2,"type":"push","success":true,"transactionId":"second"},"#,
        r#"{"index":3,"type":"kv","opIndex":0,"applied":true}"#
    ))])
    .await;
    let item = |n: u64, txn: &str| {
        queen_mq::TxnPushItem::new("out", serde_json::json!(n)).transaction_id(txn)
    };
    let res = framed_client(&broker)
        .transaction()
        .push_item(item(1, "first"))
        .unwrap()
        .ack(&popped())
        .push_item(item(2, "second"))
        .unwrap()
        .kv_put(
            "saga",
            "k1",
            serde_json::json!({"step": 1}),
            queen_mq::Expiry::Seconds(86_400),
        )
        .unwrap()
        .commit()
        .await
        .expect("commit");

    let got: Vec<(usize, &str, &str)> = res
        .results
        .iter()
        .map(|r| (r.index, r.op_type.as_str(), r.transaction_id.as_str()))
        .collect();
    assert_eq!(
        got,
        vec![
            (0, "push", "first"),
            (1, "ack", "in-1"),
            (2, "push", "second"),
            (3, "kv", "")
        ]
    );
    // On the wire the two pushes are one group, in the caller's order, and the
    // rider travels in the head.
    let (_, head, groups) = transaction_body(&transactions(&broker)[0].raw);
    assert_eq!(head["operations"].as_array().map(Vec::len), Some(1));
    assert_eq!(head["operations"][0]["type"], "ack");
    assert_eq!(head["kv"].as_array().map(Vec::len), Some(1));
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].1, "Default");
    let txns: Vec<&str> = groups[0].3.iter().map(|f| f.txn.as_str()).collect();
    assert_eq!(txns, vec!["first", "second"]);
}

#[tokio::test]
async fn a_rolled_back_bundle_comes_back_as_the_broker_answered_it() {
    let broker = FakeBroker::start(vec![Reply::ok(concat!(
        r#"{"transactionId":"tx","success":false,"reason":"kv_precondition","results":[],"#,
        r#""ok":false,"failedIndex":2,"kvReason":"exists"}"#
    ))])
    .await;
    let res = framed_client(&broker)
        .transaction()
        .push("out", serde_json::json!(1))
        .unwrap()
        .ack(&popped())
        .kv_put_if_absent(
            "idem",
            "order-1",
            serde_json::json!(true),
            queen_mq::Expiry::Seconds(86_400),
        )
        .unwrap()
        .commit()
        .await
        .expect("a lost precondition is a verdict, not an error");

    assert!(!res.success);
    assert!(res.results.is_empty());
    assert_eq!(res.failed_index, Some(2));
    assert!(transactions(&broker)[0].raw.starts_with(b"QTB1"));
}

#[tokio::test]
async fn a_bundle_whose_push_carries_a_trace_id_stays_json() {
    let trace = "0190a0a0-0000-4000-8000-000000000001";
    let broker = FakeBroker::start(vec![committed(
        r#"{"index":0,"type":"push","success":true}"#,
    )])
    .await;
    framed_client(&broker)
        .transaction()
        .push_item(queen_mq::TxnPushItem::new("out", serde_json::json!(1)).trace_id(trace))
        .unwrap()
        .commit()
        .await
        .expect("commit");

    let hit = transactions(&broker).remove(0);
    assert_eq!(hit.header("content-type"), Some("application/json"));
    assert_eq!(hit.json()["operations"][0]["items"][0]["traceId"], trace);
}

/// There is nothing to frame.
#[tokio::test]
async fn a_bundle_without_pushes_stays_json() {
    let broker = FakeBroker::start(vec![committed(
        r#"{"index":0,"type":"ack","success":true}"#,
    )])
    .await;
    framed_client(&broker)
        .transaction()
        .ack(&popped())
        .commit()
        .await
        .expect("commit");

    let hit = transactions(&broker).remove(0);
    assert_eq!(hit.header("content-type"), Some("application/json"));
    assert!(hit.raw.starts_with(b"{"));
}

#[test]
fn the_transaction_counts_flag_and_an_empty_head() {
    use queen_mq::push_binary::{encode_transaction_frames, PushBatch};
    let body = encode_transaction_frames(
        b"",
        &[PushBatch {
            queue: "q",
            partition: "p",
            payloads: vec![b"1"],
            transaction_ids: None,
        }],
        true,
    )
    .unwrap();

    assert!(
        body.starts_with(b"QTB1\x02\x00\x00\x00\x00"),
        "{:?}",
        &body[..9]
    );
    let (flags, head, groups) = transaction_body(&body);
    assert_eq!((flags, head), (2, serde_json::Value::Null));
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].3[0].payload, "1");
}
