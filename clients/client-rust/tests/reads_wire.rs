//! `fetch` and `changed_since` against a fake broker that answers
//! `POST /api/v1/fetch` and `POST /api/v1/partitions/changed` the way
//! `server/src/rsm/facade/real/phase2/reads.rs` does: entries answered in
//! order, each entry's byte budget and the whole call's ceiling (the first
//! record of an entry read whatever it weighs), a record cap per entry,
//! `OFFSET_OUT_OF_RANGE` below `logStartOffset` and above the high watermark,
//! `UNKNOWN_TOPIC_OR_PARTITION` for a queue that does not exist, discovery
//! pages with a `next` cursor exactly when a page is full, `BAD_CURSOR` for a
//! cursor it did not issue. The requests are recorded as they arrived on the
//! socket.

mod support;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use queen_mq::{Config, FetchOptions, Queen, Since};
use serde_json::{json, Value};
use support::{FakeBroker, Hit, Reply};

const TS: &str = "2026-10-20T10:00:00.000000Z";
const MIB: i64 = 1024 * 1024;

#[derive(Clone)]
struct Part {
    records: Vec<Value>,
    log_start: i64,
    created: usize,
    last_write_at: String,
}

#[derive(Default)]
struct State {
    queues: BTreeMap<String, BTreeMap<String, Part>>,
    records_per_entry: usize,
    safe_times: Vec<String>,
    created: usize,
    /// Appends one record to q/p after every fetch answer.
    grow_after_fetch: Option<(String, String)>,
    fetch_override: Option<String>,
    changed_override: Option<String>,
}

impl State {
    fn new() -> Self {
        Self {
            records_per_entry: 10_000,
            ..Self::default()
        }
    }

    fn write(&mut self, queue: &str, partition: &str, payloads: Vec<Value>, last_write_at: &str) {
        let created = self.created;
        let q = self.queues.entry(queue.into()).or_default();
        let p = q.entry(partition.into()).or_insert_with(|| Part {
            records: Vec::new(),
            log_start: 0,
            created,
            last_write_at: last_write_at.into(),
        });
        if p.created == created {
            self.created += 1;
        }
        for payload in payloads {
            let offset = p
                .records
                .last()
                .map_or(p.log_start, |r| r["offset"].as_i64().unwrap() + 1);
            p.records.push(json!({"offset": offset, "transactionId": format!("{partition}-{offset}"), "payload": payload, "ts": TS}));
        }
        p.last_write_at = last_write_at.into();
    }

    fn trim(&mut self, queue: &str, partition: &str, offset: i64) {
        let p = self
            .queues
            .get_mut(queue)
            .unwrap()
            .get_mut(partition)
            .unwrap();
        p.records
            .retain(|r| r["offset"].as_i64().unwrap() >= offset);
        p.log_start = offset;
    }

    fn fetch(&mut self, body: &Value) -> Reply {
        if let Some(raw) = &self.fetch_override {
            return Reply::ok(raw.clone());
        }
        let asks = body["entries"].as_array().unwrap();
        if asks.len() > 1024 {
            return Reply::json(400, r#"{"error":"too many entries"}"#);
        }
        let ceiling = body["maxBytes"].as_i64();
        let mut used = 0i64;
        let mut entries = Vec::new();
        for e in asks {
            let queue = e["queue"].as_str().unwrap();
            let partition = e["partition"].as_str().unwrap_or("Default");
            let mut out = json!({"queue": queue, "partition": partition, "records": [], "highWatermark": 0, "logStartOffset": 0});
            let Some(q) = self.queues.get(queue) else {
                out["error"] = "UNKNOWN_TOPIC_OR_PARTITION".into();
                entries.push(out);
                continue;
            };
            let Some(p) = q.get(partition) else {
                entries.push(out);
                continue;
            };
            let high = p
                .records
                .last()
                .map_or(p.log_start, |r| r["offset"].as_i64().unwrap() + 1);
            out["highWatermark"] = high.into();
            out["logStartOffset"] = p.log_start.into();
            let offset = e["offset"].as_i64().unwrap();
            if offset < p.log_start || offset > high {
                out["error"] = "OFFSET_OUT_OF_RANGE".into();
                entries.push(out);
                continue;
            }
            let mut budget = e["maxBytes"].as_i64().unwrap_or(MIB);
            if let Some(c) = ceiling {
                if used >= c {
                    entries.push(out);
                    continue;
                }
                budget = budget.min(c - used);
            }
            let mut spent = 0i64;
            let mut records = Vec::new();
            for r in &p.records {
                if r["offset"].as_i64().unwrap() < offset {
                    continue;
                }
                if records.len() >= self.records_per_entry {
                    break;
                }
                let weight = (r["payload"].to_string().len() as i64).max(1);
                if !records.is_empty() && spent + weight > budget {
                    break;
                }
                spent += weight;
                records.push(r.clone());
            }
            used += spent;
            out["records"] = Value::Array(records);
            entries.push(out);
        }
        if let Some((q, p)) = self.grow_after_fetch.clone() {
            self.write(&q, &p, vec![json!("late")], TS);
        }
        Reply::ok(json!({ "entries": entries }).to_string())
    }

    fn changed(&mut self, body: &Value) -> Reply {
        if let Some(raw) = &self.changed_override {
            return Reply::ok(raw.clone());
        }
        let safe_time = if self.safe_times.len() > 1 {
            self.safe_times.remove(0)
        } else {
            self.safe_times
                .first()
                .cloned()
                .unwrap_or_else(|| "2026-10-20T12:00:00.000000Z".into())
        };
        let mut entries = Vec::new();
        for e in body["entries"].as_array().unwrap() {
            let queue = e["queue"].as_str().unwrap();
            let Some(q) = self.queues.get(queue) else {
                entries.push(json!({"queue": queue, "error": "UNKNOWN_TOPIC_OR_PARTITION"}));
                continue;
            };
            let mut from = 0usize;
            if let Some(after) = e["after"].as_str() {
                match after
                    .strip_prefix("p|")
                    .and_then(|n| n.parse::<usize>().ok())
                {
                    Some(n) => from = n + 1,
                    None => {
                        entries.push(json!({"queue": queue, "error": "BAD_CURSOR"}));
                        continue;
                    }
                }
            }
            let limit = e["limit"].as_i64().unwrap_or(1000).clamp(1, 1000) as usize;
            let mut all: Vec<(&String, &Part)> = q.iter().collect();
            all.sort_by_key(|(_, p)| p.created);
            let since = e["since"].as_str();
            let mut partitions = Vec::new();
            let mut last = 0usize;
            for (name, p) in all {
                if p.created < from || since.is_some_and(|s| p.last_write_at.as_str() < s) {
                    continue;
                }
                let last_offset = p
                    .records
                    .last()
                    .map_or(p.log_start - 1, |r| r["offset"].as_i64().unwrap());
                partitions.push(json!({"name": name, "id": format!("id-{name}"), "lastOffset": last_offset, "logStart": p.log_start, "lastWriteAt": p.last_write_at}));
                last = p.created;
                if partitions.len() >= limit {
                    break;
                }
            }
            let next = (partitions.len() >= limit).then(|| format!("p|{last}"));
            entries.push(json!({"queue": queue, "partitions": partitions, "next": next}));
        }
        Reply::ok(
            json!({"safeTime": safe_time, "safeTimeDegraded": false, "entries": entries})
                .to_string(),
        )
    }
}

async fn start(state: State) -> (FakeBroker, Queen, Arc<Mutex<State>>) {
    let state = Arc::new(Mutex::new(state));
    let shared = Arc::clone(&state);
    let broker = FakeBroker::start_with(move |_, hit: &Hit| {
        let mut s = shared.lock().unwrap();
        match (hit.method.as_str(), hit.route()) {
            ("POST", "/api/v1/fetch") => s.fetch(&hit.json()),
            ("POST", "/api/v1/partitions/changed") => s.changed(&hit.json()),
            _ => Reply::json(404, r#"{"error":"not found"}"#),
        }
    })
    .await;
    let queen =
        Queen::connect(Config::new(broker.url()).retry_attempts(1)).expect("fake broker url");
    (broker, queen, state)
}

fn payloads(n: i64) -> Vec<Value> {
    (0..n).map(|i| json!({ "n": i })).collect()
}

fn fetch_bodies(broker: &FakeBroker) -> Vec<Value> {
    broker
        .hits()
        .iter()
        .filter(|h| h.route() == "/api/v1/fetch")
        .map(Hit::json)
        .collect()
}

fn first_offsets(broker: &FakeBroker) -> Vec<i64> {
    fetch_bodies(broker)
        .iter()
        .map(|b| b["entries"][0]["offset"].as_i64().unwrap())
        .collect()
}

fn offsets_of(p: &queen_mq::FetchedPartition) -> Vec<i64> {
    p.records.iter().map(|r| r.offset).collect()
}

#[tokio::test]
async fn fetch_reads_every_record_from_offset_zero_in_one_call_when_they_fit() {
    let mut s = State::new();
    s.write("tf.events", "trace-1", payloads(3), TS);
    s.write("tf.events", "trace-2", vec![json!({"a": 1})], TS);
    let (broker, queen, _) = start(s).await;

    let read = queen
        .queue("tf.events")
        .fetch(["trace-1", "trace-2"])
        .await
        .unwrap();
    let t1 = &read["trace-1"];
    assert_eq!(offsets_of(t1), vec![0, 1, 2]);
    assert_eq!(t1.records[1].payload, json!({"n": 1}));
    assert_eq!(t1.records[0].transaction_id, "trace-1-0");
    assert_eq!(t1.records[0].ts, TS);
    assert_eq!(
        (
            t1.partition.as_str(),
            t1.high_watermark,
            t1.log_start_offset,
            t1.next_offset
        ),
        ("trace-1", 3, 0, 3)
    );
    assert_eq!(read["trace-2"].records[0].payload, json!({"a": 1}));

    let bodies = fetch_bodies(&broker);
    assert_eq!(bodies.len(), 1);
    assert_eq!(
        bodies[0],
        json!({
            "entries": [
                {"queue": "tf.events", "partition": "trace-1", "offset": 0, "maxBytes": 8 * MIB},
                {"queue": "tf.events", "partition": "trace-2", "offset": 0, "maxBytes": 8 * MIB},
            ],
            "maxBytes": 8 * MIB,
        })
    );
}

#[tokio::test]
async fn fetch_asks_again_until_the_high_watermark_of_the_first_answer() {
    let mut s = State::new();
    s.records_per_entry = 3;
    s.write("q", "p", payloads(10), TS);
    let (broker, queen, _) = start(s).await;
    let read = queen.queue("q").fetch(["p"]).await.unwrap();
    assert_eq!(offsets_of(&read["p"]), (0..10).collect::<Vec<_>>());
    assert_eq!(first_offsets(&broker), vec![0, 3, 6, 9]);
    assert_eq!(read["p"].next_offset, 10);
}

#[tokio::test]
async fn fetch_does_not_chase_a_partition_written_during_the_read() {
    let mut s = State::new();
    s.records_per_entry = 2;
    s.write("q", "p", payloads(4), TS);
    s.grow_after_fetch = Some(("q".into(), "p".into()));
    let (broker, queen, _) = start(s).await;
    let read = queen.queue("q").fetch(["p"]).await.unwrap();
    assert_eq!(offsets_of(&read["p"]), vec![0, 1, 2, 3]);
    assert_eq!(fetch_bodies(&broker).len(), 2);
    assert_eq!(read["p"].next_offset, 4);
}

#[tokio::test]
async fn fetch_asks_for_at_most_1000_partitions_per_call() {
    let mut s = State::new();
    let names: Vec<String> = (0..2500).map(|i| format!("p-{i}")).collect();
    for name in &names {
        s.write("q", name, vec![json!(name)], TS);
    }
    let (broker, queen, _) = start(s).await;
    let read = queen.queue("q").fetch(names.clone()).await.unwrap();
    let sizes: Vec<usize> = fetch_bodies(&broker)
        .iter()
        .map(|b| b["entries"].as_array().unwrap().len())
        .collect();
    assert_eq!(sizes, vec![1000, 1000, 500]);
    assert_eq!(read.len(), 2500);
    for name in &names {
        assert_eq!(read[name].records[0].payload, json!(name));
    }
}

#[tokio::test]
async fn fetch_asks_again_for_the_partitions_the_call_ceiling_left_empty() {
    let mut s = State::new();
    let fat = json!("x".repeat(100));
    for name in ["a", "b", "c"] {
        s.write("q", name, vec![fat.clone(), fat.clone()], TS);
    }
    let (broker, queen, _) = start(s).await;
    let read = queen
        .queue("q")
        .fetch_with(["a", "b", "c"], FetchOptions::new().max_bytes(250))
        .await
        .unwrap();
    for name in ["a", "b", "c"] {
        assert_eq!(offsets_of(&read[name]), vec![0, 1], "{name}");
    }
    let bodies = fetch_bodies(&broker);
    assert!(bodies.len() > 1);
    assert!(bodies.iter().all(|b| b["maxBytes"] == json!(250)));
}

#[tokio::test]
async fn fetch_reads_from_log_start_when_retention_deleted_the_start() {
    let mut s = State::new();
    s.write("q", "p", payloads(10), TS);
    s.trim("q", "p", 6);
    let (broker, queen, _) = start(s).await;
    let read = queen.queue("q").fetch(["p"]).await.unwrap();
    assert_eq!(offsets_of(&read["p"]), vec![6, 7, 8, 9]);
    assert_eq!(read["p"].log_start_offset, 6);
    assert_eq!(first_offsets(&broker), vec![0, 6]);
}

#[tokio::test]
async fn fetch_starts_each_partition_at_its_offset() {
    let mut s = State::new();
    s.write("q", "a", payloads(5), TS);
    s.write("q", "b", payloads(5), TS);
    let (_broker, queen, _) = start(s).await;
    let read = queen
        .queue("q")
        .fetch_with(["a", "b"], FetchOptions::new().offset("a", 3))
        .await
        .unwrap();
    assert_eq!(offsets_of(&read["a"]), vec![3, 4]);
    assert_eq!(offsets_of(&read["b"]), vec![0, 1, 2, 3, 4]);
    let again = queen
        .queue("q")
        .fetch_with(
            ["a"],
            FetchOptions::new().offsets([("a", read["a"].next_offset)]),
        )
        .await
        .unwrap();
    assert!(again["a"].records.is_empty());
    assert_eq!(again["a"].next_offset, 5);
}

#[tokio::test]
async fn fetch_answers_no_records_for_a_missing_queue_an_unwritten_partition_and_an_offset_past_the_end(
) {
    let mut s = State::new();
    s.write("q", "p", payloads(2), TS);
    let (_broker, queen, _) = start(s).await;
    let missing = queen.queue("nope").fetch(["p"]).await.unwrap();
    assert!(missing["p"].records.is_empty());
    let read = queen
        .queue("q")
        .fetch_with(["never", "p"], FetchOptions::new().offset("p", 7))
        .await
        .unwrap();
    assert!(read["never"].records.is_empty());
    assert!(read["p"].records.is_empty());
    assert_eq!((read["p"].next_offset, read["p"].high_watermark), (7, 2));
}

#[tokio::test]
async fn fetch_fails_on_an_error_marker_it_does_not_know() {
    let mut s = State::new();
    s.fetch_override = Some(r#"{"entries":[{"queue":"q","partition":"p","records":[],"highWatermark":1,"logStartOffset":0,"error":"SOMETHING_NEW"}]}"#.into());
    let (_broker, queen, _) = start(s).await;
    let err = queen.queue("q").fetch(["p"]).await.unwrap_err();
    assert!(err.to_string().contains("SOMETHING_NEW"), "{err}");
}

#[tokio::test]
async fn fetch_stops_when_a_call_reads_nothing_although_the_partition_is_not_at_its_end() {
    let mut s = State::new();
    s.fetch_override = Some(r#"{"entries":[{"queue":"q","partition":"p","records":[],"highWatermark":5,"logStartOffset":0}]}"#.into());
    let (broker, queen, _) = start(s).await;
    let read = queen.queue("q").fetch(["p"]).await.unwrap();
    assert!(read["p"].records.is_empty());
    assert_eq!(fetch_bodies(&broker).len(), 1);
}

#[tokio::test]
async fn fetch_asks_once_for_a_name_given_twice_and_refuses_bad_input_before_any_call() {
    let mut s = State::new();
    s.write("q", "p", payloads(1), TS);
    let (broker, queen, _) = start(s).await;
    let read = queen.queue("q").fetch(["p", "p"]).await.unwrap();
    assert_eq!(read.len(), 1);
    assert_eq!(
        fetch_bodies(&broker)[0]["entries"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(queen.queue("q").fetch([""]).await.is_err());
    assert!(queen
        .queue("q")
        .fetch_with(["p"], FetchOptions::new().offset("p", -1))
        .await
        .is_err());
    assert!(queen
        .queue("q")
        .fetch_with(["p"], FetchOptions::new().max_bytes(0))
        .await
        .is_err());
    assert_eq!(fetch_bodies(&broker).len(), 1);
}

#[tokio::test]
async fn changed_since_follows_the_page_cursor_to_the_end_of_the_pass() {
    let mut s = State::new();
    for i in 0..2300 {
        s.write("tf.events", &format!("t-{i}"), vec![json!(i)], TS);
    }
    let (broker, queen, _) = start(s).await;
    let changed = queen
        .queue("tf.events")
        .changed_since("2026-10-20T09:00:00Z")
        .await
        .unwrap();
    assert_eq!(changed.partitions.len(), 2300);
    let p0 = &changed.partitions[0];
    assert_eq!(
        (
            p0.name.as_str(),
            p0.id.as_deref(),
            p0.last_offset,
            p0.log_start,
            p0.last_write_at.as_str()
        ),
        ("t-0", Some("id-t-0"), 0, 0, TS)
    );
    assert_eq!(changed.partitions[2299].name, "t-2299");
    assert_eq!(changed.safe_time, "2026-10-20T12:00:00.000000Z");
    let entries: Vec<Value> = broker
        .hits()
        .iter()
        .map(|h| h.json()["entries"][0].clone())
        .collect();
    let afters: Vec<Value> = entries.iter().map(|e| e["after"].clone()).collect();
    assert_eq!(afters, vec![Value::Null, json!("p|999"), json!("p|1999")]);
    assert!(entries
        .iter()
        .all(|e| e["since"] == json!("2026-10-20T09:00:00Z")
            && e["limit"] == json!(1000)
            && e["queue"] == json!("tf.events")));
}

#[tokio::test]
async fn changed_since_lists_only_what_was_written_at_or_after_since() {
    let mut s = State::new();
    s.write("q", "old", vec![json!(1)], "2026-10-20T08:00:00.000000Z");
    s.write("q", "new", vec![json!(1)], "2026-10-20T10:30:00.000000Z");
    let (broker, queen, _) = start(s).await;
    let since = UNIX_EPOCH + Duration::from_secs(1_792_490_400); // 2026-10-20T10:00:00Z
    let changed = queen.queue("q").changed_since(since).await.unwrap();
    let names: Vec<&str> = changed.partitions.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(names, vec!["new"]);
    assert_eq!(
        broker.hits()[0].json()["entries"][0]["since"],
        json!("2026-10-20T10:00:00.000000Z")
    );
}

#[tokio::test]
async fn changed_since_all_lists_every_partition_and_sends_no_since() {
    let mut s = State::new();
    s.write("q", "a", vec![json!(1)], "2020-01-01T00:00:00.000000Z");
    let (broker, queen, _) = start(s).await;
    let changed = queen.queue("q").changed_since(Since::All).await.unwrap();
    assert_eq!(changed.partitions.len(), 1);
    assert!(broker.hits()[0].json()["entries"][0].get("since").is_none());
    let none: Option<String> = None;
    queen.queue("q").changed_since(none).await.unwrap();
    assert!(broker.hits()[1].json()["entries"][0].get("since").is_none());
}

#[tokio::test]
async fn changed_since_answers_the_smallest_safe_time_of_its_pages() {
    let mut s = State::new();
    for i in 0..1500 {
        s.write("q", &format!("p-{i}"), vec![json!(1)], TS);
    }
    s.safe_times = vec![
        "2026-10-20T12:00:05.000000Z".into(),
        "2026-10-20T12:00:01.000000Z".into(),
    ];
    let (_broker, queen, _) = start(s).await;
    let changed = queen.queue("q").changed_since(Since::All).await.unwrap();
    assert_eq!(changed.safe_time, "2026-10-20T12:00:01.000000Z");
}

#[tokio::test]
async fn changed_since_answers_nothing_for_a_missing_queue_and_fails_on_bad_cursor() {
    let (_broker, queen, _) = start(State::new()).await;
    let changed = queen
        .queue("ghost")
        .changed_since("2026-10-20T00:00:00Z")
        .await
        .unwrap();
    assert!(changed.partitions.is_empty());
    assert_eq!(changed.safe_time, "2026-10-20T12:00:00.000000Z");

    let mut bad = State::new();
    bad.changed_override = Some(r#"{"safeTime":"2026-10-20T12:00:00.000000Z","safeTimeDegraded":false,"entries":[{"queue":"q","error":"BAD_CURSOR"}]}"#.into());
    let (_broker, queen, _) = start(bad).await;
    let err = queen
        .queue("q")
        .changed_since(Since::All)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("BAD_CURSOR"), "{err}");
    assert!(queen.queue("q").changed_since("").await.is_err());
}
