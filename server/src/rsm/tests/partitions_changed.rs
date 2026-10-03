//! `POST /api/v1/partitions/changed` and the typed twins of the S3 sink's two
//! reads ([`Rsm::partitions_changed`], [`Rsm::fetch_log`]) end to end through
//! the real [`RaftFacade`] (receiver, batcher, planner, apply, reads), over a
//! throwaway data directory per test, like `fetch_offsets.rs`.
//!
//! What they prove:
//! - `safeTime` is the applied clock: the greatest stamp apply wrote, at or
//!   above every record a read can see, and still while nothing applies;
//! - a reader that closes a window at a pass's `safeTime` misses no record
//!   while writers race it (the property the sink rests on);
//! - a pass pages in creation order, lists each partition once, meets the
//!   partitions created under it and filters by `since`;
//! - the refusals: a cursor the broker never issued, an unknown queue, a
//!   `since` that is not a timestamp, too many entries;
//! - `id` is the partition's uuid, stable across calls and new for a new
//!   partition of the same name;
//! - the typed twins answer exactly what the routes render.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::{
    ApiReq, ChangedAsk, ChangedPartition, Deadline, PartitionsChanged, PushReq, RecordFetch,
    RecordsFetched, ReqCtx, Rsm, RsmBuildCtx, RsmError,
};
use crate::rsm::planner::timers::iso_us;

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-partitions-changed-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn build_ctx(dir: &Path) -> RsmBuildCtx {
    RsmBuildCtx {
        data_dir: dir.display().to_string(),
        notifier: crate::notify::Notifier::new(false),
        disk_high_pct: 85.0,
        disk_low_pct: 80.0,
    }
}

fn ctx() -> ReqCtx {
    ReqCtx::new(
        crate::config::DEFAULT_TENANT,
        Deadline::after(Duration::from_secs(30)),
    )
}

async fn push(f: &RaftFacade, items: Vec<Value>) {
    let out = f
        .push(
            ctx(),
            PushReq {
                raw: json!({ "items": items }).to_string().into_bytes(),
            },
        )
        .await
        .expect("push");
    let v: Value = serde_json::from_str(&out.body).expect("push body");
    for it in v.as_array().expect("push array") {
        assert_eq!(it["status"], "queued", "{it}");
    }
}

/// One record into each of `partitions` of `queue`, in that order.
async fn touch(f: &RaftFacade, queue: &str, partitions: &[String]) {
    let items: Vec<Value> = partitions
        .iter()
        .map(|p| json!({"queue": queue, "partition": p, "payload": {"p": p}}))
        .collect();
    for chunk in items.chunks(500) {
        push(f, chunk.to_vec()).await;
    }
}

async fn api(
    f: &RaftFacade,
    method: &str,
    path: &str,
    query: Option<&str>,
    body: Value,
) -> (u16, Value) {
    let out = f
        .api(
            ctx(),
            ApiReq {
                method: method.to_string(),
                path: path.to_string(),
                query: query.map(str::to_string),
                body: body.to_string().into_bytes(),
            },
        )
        .await
        .expect("api call");
    let v = serde_json::from_str(&out.body)
        .unwrap_or_else(|e| panic!("bad JSON body: {e}\n{}", out.body));
    (out.status, v)
}

/// The route, answered as its status and JSON body.
async fn route(f: &RaftFacade, entries: Value) -> (u16, Value) {
    api(
        f,
        "POST",
        "/api/v1/partitions/changed",
        None,
        json!({ "entries": entries }),
    )
    .await
}

fn ask(queue: &str, since_us: Option<i64>, after: Option<&str>, limit: usize) -> ChangedAsk {
    ChangedAsk {
        queue: queue.to_string(),
        since_us,
        after: after.map(str::to_string),
        limit,
    }
}

async fn changed(f: &RaftFacade, asks: Vec<ChangedAsk>) -> PartitionsChanged {
    f.partitions_changed(ctx(), asks)
        .await
        .expect("partitions_changed")
}

async fn safe_time(f: &RaftFacade) -> i64 {
    changed(f, Vec::new()).await.safe_time_us
}

/// A COMPLETE pass over `queue`, typed: every page until `next` is `None`.
/// The partitions in the order listed, the minimum `safeTime` of the pages,
/// and the page count. `between` runs after every page but the last.
async fn pass_with<F, Fut>(
    f: &RaftFacade,
    queue: &str,
    since_us: Option<i64>,
    limit: usize,
    mut between: F,
) -> (Vec<ChangedPartition>, i64, usize)
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut out = Vec::new();
    let mut safe = i64::MAX;
    let mut after: Option<String> = None;
    let mut pages = 0usize;
    loop {
        let answer = changed(f, vec![ask(queue, since_us, after.as_deref(), limit)]).await;
        pages += 1;
        safe = safe.min(answer.safe_time_us);
        let page = answer.entries.into_iter().next().expect("one entry");
        assert_eq!(page.error, None, "{queue}");
        assert!(page.partitions.len() <= limit, "a page holds at most limit");
        assert_eq!(
            page.next.is_some(),
            page.partitions.len() == limit,
            "next exactly when the page is full"
        );
        out.extend(page.partitions);
        match page.next {
            Some(next) => after = Some(next),
            None => break,
        }
        between(pages).await;
        assert!(pages < 1_000_000, "a pass that does not end");
    }
    (out, safe, pages)
}

async fn pass(
    f: &RaftFacade,
    queue: &str,
    since_us: Option<i64>,
    limit: usize,
) -> (Vec<ChangedPartition>, i64, usize) {
    pass_with(f, queue, since_us, limit, |_| async {}).await
}

fn names(parts: &[ChangedPartition]) -> Vec<String> {
    parts.iter().map(|p| p.name.clone()).collect()
}

/// Every record of the named partitions of `queue` with an offset at or below
/// its bound, as `name -> [(offset, ts)]`, read with [`Rsm::fetch_log`].
async fn read_upto(
    f: &RaftFacade,
    queue: &str,
    bounds: &BTreeMap<String, i64>,
) -> BTreeMap<String, Vec<(u64, i64)>> {
    let mut out: BTreeMap<String, Vec<(u64, i64)>> = BTreeMap::new();
    let mut next: BTreeMap<String, u64> = bounds
        .iter()
        .filter(|(_, last)| **last >= 0)
        .map(|(p, _)| (p.clone(), 0))
        .collect();
    while !next.is_empty() {
        let want: Vec<(String, u64)> = next
            .iter()
            .take(1024)
            .map(|(p, o)| (p.clone(), *o))
            .collect();
        let asks = want
            .iter()
            .map(|(p, o)| RecordFetch {
                queue: queue.to_string(),
                partition: p.clone(),
                offset: *o,
                max_bytes: 8 << 20,
            })
            .collect();
        let got = f.fetch_log(ctx(), asks, 0, 1).await.expect("fetch_log");
        for ((p, _), entry) in want.into_iter().zip(got) {
            assert_eq!(entry.error, None, "{p}");
            let bound = bounds[&p] as u64;
            let mut done = entry.records.is_empty();
            for r in &entry.records {
                if r.offset <= bound {
                    out.entry(p.clone())
                        .or_default()
                        .push((r.offset, r.created_at_us));
                }
                done |= r.offset >= bound;
            }
            match entry.records.last() {
                Some(last) if !done => {
                    next.insert(p, last.offset + 1);
                }
                _ => {
                    next.remove(&p);
                }
            }
        }
    }
    out
}

/// `RecordFetch` for offset 0 of each partition.
fn from_zero(queue: &str, partitions: &[&str]) -> Vec<RecordFetch> {
    partitions
        .iter()
        .map(|p| RecordFetch {
            queue: queue.to_string(),
            partition: p.to_string(),
            offset: 0,
            max_bytes: 1 << 20,
        })
        .collect()
}

fn lcg(s: u64) -> u64 {
    s.wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407)
}

async fn close(f: RaftFacade, dir: PathBuf) {
    f.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// safeTime
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn safe_time_is_the_greatest_applied_stamp() {
    let dir = scratch("clock");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    let parts: Vec<String> = (0..5).map(|p| format!("p{p}")).collect();
    for round in 0..3 {
        touch(&f, "orders", &parts).await;
        // Several segments in one partition: stamps run a µs apart there.
        push(
            &f,
            (0..4)
                .map(|i| json!({"queue":"orders","partition":"p0","payload":{"r":round,"i":i}}))
                .collect(),
        )
        .await;
    }
    let names: Vec<&str> = parts.iter().map(String::as_str).collect();
    let fetched = f
        .fetch_log(ctx(), from_zero("orders", &names), 0, 1)
        .await
        .expect("fetch");
    let stamps: Vec<i64> = fetched
        .iter()
        .flat_map(|e| e.records.iter().map(|r| r.created_at_us))
        .collect();
    assert_eq!(stamps.len(), 3 * (5 + 4));
    let newest = *stamps.iter().max().unwrap();

    // Twice, 50 ms apart, with nothing applied in between (checked, not
    // assumed: the stamps are monotone, so equal before and after means equal
    // throughout; a boot-time entry landing in between only asks again): the
    // greatest applied stamp, the same both times, on both surfaces. The wall
    // clock this replaced moves 50 ms here.
    let mut conclusive = false;
    for _ in 0..50 {
        let before = f.applied_stamps_for_test();
        let first = safe_time(&f).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (status, body) = route(&f, json!([])).await;
        let second = safe_time(&f).await;
        if f.applied_stamps_for_test() != before {
            continue;
        }
        assert_eq!(first, before.0.max(before.1), "the greatest applied stamp");
        assert_eq!(second, first, "nothing applied: the same safeTime");
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["safeTime"], json!(iso_us(first)), "{body}");
        assert_eq!(body["safeTimeDegraded"], json!(false));
        assert_eq!(body["entries"], json!([]));
        conclusive = true;
        break;
    }
    assert!(conclusive, "something applied under every attempt");

    // At or above every record a read can see; the newest record is the
    // applied max created_at.
    let safe = safe_time(&f).await;
    assert!(
        safe >= newest,
        "safeTime {safe} below a visible record {newest}"
    );
    assert_eq!(f.applied_stamps_for_test().1, newest);

    // A write moves it, past the stamp of what it wrote.
    push(
        &f,
        vec![json!({"queue":"orders","partition":"p1","payload":1})],
    )
    .await;
    let moved = safe_time(&f).await;
    let tail = f
        .fetch_log(ctx(), from_zero("orders", &["p1"]), 0, 1)
        .await
        .expect("fetch")[0]
        .records
        .last()
        .expect("p1's records")
        .created_at_us;
    assert!(
        tail > safe,
        "a record applied after the read is stamped above it"
    );
    assert!(moved >= tail, "the record is at or below the new safeTime");
    close(f, dir).await;
}

/// The property the sink rests on, under writers racing the reader: a
/// COMPLETE pass with a small page, closed at the minimum `safeTime` of its
/// pages, then every listed partition read up to the bound the pass listed.
/// Every record the final log holds stamped at or below that `safeTime` was in
/// what the pass read. (A partition born after a pass's first page may be
/// missing from it: its records are stamped above, which this allows.)
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_pass_closed_at_its_safe_time_misses_no_record() {
    let dir = scratch("race");
    let f = Arc::new(RaftFacade::open(&build_ctx(&dir)).expect("open facade"));
    push(
        &f,
        vec![json!({"queue":"race","partition":"seed","payload":0})],
    )
    .await;

    const WRITERS: u64 = 4;
    let stop = Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    for w in 0..WRITERS {
        let (f, stop) = (Arc::clone(&f), Arc::clone(&stop));
        writers.push(tokio::spawn(async move {
            let mut seed = 0x9E37_79B9_7F4A_7C15u64 ^ (w + 1);
            let mut pushes = 0u64;
            while !stop.load(Ordering::Relaxed) {
                // New partitions keep being born: the range grows.
                let span = 8 + pushes / 2;
                let items: Vec<Value> = (0..12)
                    .map(|i| {
                        seed = lcg(seed);
                        let p = (seed >> 33) % span;
                        json!({"queue":"race","partition":format!("w{w}-{p}"),
                               "payload":{"w":w,"n":pushes,"i":i}})
                    })
                    .collect();
                push(&f, items).await;
                pushes += 1;
            }
            pushes
        }));
    }

    struct Pass {
        safe: i64,
        pages: usize,
        seen: BTreeSet<(String, u64)>,
    }
    let mut passes: Vec<Pass> = Vec::new();
    let until = Instant::now() + Duration::from_millis(2_500);
    while Instant::now() < until {
        let (listed, safe, pages) = pass(&f, "race", None, 7).await;
        let bounds: BTreeMap<String, i64> = listed
            .iter()
            .map(|p| (p.name.clone(), p.last_offset))
            .collect();
        assert_eq!(
            bounds.len(),
            listed.len(),
            "a partition listed twice in a pass"
        );
        let seen = read_upto(&f, "race", &bounds)
            .await
            .into_iter()
            .flat_map(|(p, recs)| recs.into_iter().map(move |(o, _)| (p.clone(), o)))
            .collect();
        passes.push(Pass { safe, pages, seen });
    }
    stop.store(true, Ordering::Relaxed);
    let mut pushed = 0;
    for w in writers {
        pushed += w.await.expect("writer");
    }

    // Every push was answered, so every record is applied: the final log.
    let (listed, _, _) = pass(&f, "race", None, 1000).await;
    let bounds: BTreeMap<String, i64> = listed
        .iter()
        .map(|p| (p.name.clone(), p.last_offset))
        .collect();
    let log = read_upto(&f, "race", &bounds).await;
    let total: usize = log.values().map(Vec::len).sum();
    assert_eq!(
        total as u64,
        1 + pushed * 12,
        "the final log holds every push"
    );

    let mut checked = 0usize;
    let mut above = 0usize;
    for (i, p) in passes.iter().enumerate() {
        for (name, recs) in &log {
            for (offset, ts) in recs {
                if *ts <= p.safe {
                    checked += 1;
                    assert!(
                        p.seen.contains(&(name.clone(), *offset)),
                        "pass {i} (safeTime {}) missed {name}@{offset} stamped {ts}",
                        p.safe
                    );
                } else {
                    above += 1;
                }
            }
        }
    }
    // The run exercised what it claims to: several multi-page passes while
    // writes were landing (the first ones may still fit one page).
    let paged = passes.iter().filter(|p| p.pages > 2).count();
    assert!(
        paged >= 3,
        "{paged} of {} passes had more than two pages",
        passes.len()
    );
    assert!(checked > 0 && above > 0, "checked {checked}, above {above}");
    let f = Arc::try_unwrap(f).ok().expect("the writers are done");
    close(f, dir).await;
}

// ---------------------------------------------------------------------------
// Paging
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_pass_pages_in_creation_order_and_lists_each_partition_once() {
    let dir = scratch("paging");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");

    // Creation order is not name order: twelve partitions born one push at a
    // time, named against the grain.
    let born: Vec<String> = [7, 3, 11, 0, 9, 1, 10, 4, 8, 2, 6, 5]
        .iter()
        .map(|i| format!("k{i:02}"))
        .collect();
    for p in &born {
        touch(&f, "small", std::slice::from_ref(p)).await;
    }
    let (listed, _, pages) = pass(&f, "small", None, 5).await;
    assert_eq!(names(&listed), born, "creation order");
    assert_eq!(pages, 3);

    // 2,500 partitions, a page of 1,000: three pages, each partition once.
    let all: Vec<String> = (0..2_500)
        .map(|i| format!("cust-{:05}", (i * 7_919) % 2_500))
        .collect();
    touch(&f, "orders", &all).await;
    let mut sizes = Vec::new();
    let mut after: Option<String> = None;
    let mut listed = Vec::new();
    loop {
        let a = changed(&f, vec![ask("orders", None, after.as_deref(), 1000)]).await;
        let e = a.entries.into_iter().next().unwrap();
        sizes.push((e.partitions.len(), e.next.is_some()));
        listed.extend(e.partitions);
        match e.next {
            Some(n) => after = Some(n),
            None => break,
        }
    }
    assert_eq!(sizes, [(1000, true), (1000, true), (500, false)]);
    let once: BTreeSet<String> = names(&listed).into_iter().collect();
    assert_eq!(once.len(), 2_500, "each partition exactly once");
    assert_eq!(once, all.iter().cloned().collect::<BTreeSet<_>>());

    // The same pass through the route: the same partitions, the same order,
    // the same ids, `next` null on the last page.
    let mut via_route = Vec::new();
    let mut after = Value::Null;
    let mut route_pages = 0;
    loop {
        let (status, v) = route(&f, json!([{"queue":"orders","limit":1000,"after":after}])).await;
        assert_eq!(status, 200, "{v}");
        route_pages += 1;
        let e = &v["entries"][0];
        for p in e["partitions"].as_array().unwrap() {
            via_route.push((
                p["name"].as_str().unwrap().to_string(),
                p["id"].as_str().unwrap().to_string(),
            ));
        }
        if e["next"].is_null() {
            break;
        }
        after = e["next"].clone();
    }
    assert_eq!(route_pages, 3);
    assert_eq!(
        via_route,
        listed
            .iter()
            .map(|p| (p.name.clone(), p.id.clone()))
            .collect::<Vec<_>>()
    );

    // A pass whose size is a multiple of the page: full pages, then an empty
    // one that ends it.
    let (exact, _, pages) = pass(&f, "orders", None, 500).await;
    assert_eq!(pages, 6);
    assert_eq!(names(&exact), names(&listed), "the order is stable");

    // `since`: what was written after a pass's safeTime, in the same order.
    let (_, safe, _) = pass(&f, "orders", None, 1000).await;
    let moved: Vec<String> = listed.iter().step_by(83).map(|p| p.name.clone()).collect();
    for p in moved.iter().rev() {
        touch(&f, "orders", std::slice::from_ref(p)).await;
    }
    let (since, _, _) = pass(&f, "orders", Some(safe + 1), 7).await;
    assert_eq!(
        names(&since),
        moved,
        "exactly what moved, in creation order"
    );
    assert!(since.iter().all(|p| p.last_write_at_us > safe));
    let (status, v) = route(
        &f,
        json!([{"queue":"orders","since":iso_us(safe + 1),"limit":1000}]),
    )
    .await;
    assert_eq!(status, 200, "{v}");
    let routed: Vec<&str> = v["entries"][0]["partitions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["name"].as_str().unwrap())
        .collect();
    assert_eq!(routed, moved.iter().map(String::as_str).collect::<Vec<_>>());
    assert!(v["entries"][0]["next"].is_null());
    // `since` is inclusive, and an old one lists everything; a future one
    // nothing.
    let newest = since.iter().map(|p| p.last_write_at_us).max().unwrap();
    let (at, _, _) = pass(&f, "orders", Some(newest), 1000).await;
    assert!(at.iter().any(|p| p.last_write_at_us == newest));
    assert_eq!(pass(&f, "orders", Some(0), 1000).await.0.len(), 2_500);
    assert!(pass(&f, "orders", Some(newest + 1), 1000)
        .await
        .0
        .is_empty());

    // Partitions born during a paged pass: past the cursor, so the pass still
    // meets every one of them, once; and so does the next complete pass.
    let late: Vec<String> = (0..50).map(|i| format!("late-{i:02}")).collect();
    let f_ref = &f;
    let late_ref = &late;
    let (during, _, _) = pass_with(f_ref, "orders", None, 100, |page| async move {
        if page == 2 {
            touch(f_ref, "orders", late_ref).await;
        }
    })
    .await;
    let during: Vec<String> = names(&during);
    assert_eq!(during.len(), 2_550);
    assert_eq!(
        during[2_500..].iter().collect::<BTreeSet<_>>(),
        late.iter().collect::<BTreeSet<_>>(),
        "born late, listed last, each once"
    );
    let (next_pass, _, _) = pass(&f, "orders", None, 333).await;
    assert_eq!(names(&next_pass), during);
    close(f, dir).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn cursors_queues_and_timestamps_the_broker_refuses() {
    let dir = scratch("refuse");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    let parts: Vec<String> = (0..4).map(|i| format!("p{i}")).collect();
    touch(&f, "orders", &parts).await;

    // A cursor this broker never issued, the 1.x ones included.
    for bad in [
        "garbage",
        "p|",
        "p|abc",
        "p|-1",
        "p|+1",
        "p| 1",
        "p|1x",
        "p|1|2",
        "P|1",
        "p|99999999999999999999999",
        "n|p1",
        "t|1788516004000000|p1",
    ] {
        let a = changed(&f, vec![ask("orders", None, Some(bad), 10)]).await;
        assert_eq!(a.entries[0].error, Some("BAD_CURSOR"), "{bad}");
        assert!(a.entries[0].partitions.is_empty() && a.entries[0].next.is_none());
        let (status, v) = route(&f, json!([{"queue":"orders","after":bad}])).await;
        assert_eq!(status, 200, "{v}");
        assert_eq!(
            v["entries"][0],
            json!({"queue":"orders","error":"BAD_CURSOR"}),
            "{bad}"
        );
    }
    // A cursor that is not even a string.
    for bad in [json!(5), json!({"p": 1}), json!(["p|1"])] {
        let (_, v) = route(&f, json!([{"queue":"orders","after":bad}])).await;
        assert_eq!(v["entries"][0]["error"], "BAD_CURSOR", "{bad}");
    }
    // No cursor: absent, null or empty starts the pass.
    let (status, v) = route(
        &f,
        json!([{"queue":"orders"},{"queue":"orders","after":null},{"queue":"orders","after":""}]),
    )
    .await;
    assert_eq!(status, 200, "{v}");
    for e in v["entries"].as_array().unwrap() {
        assert_eq!(e["partitions"].as_array().unwrap().len(), 4, "{e}");
    }
    // The last pid there is: nothing after it.
    let end = changed(
        &f,
        vec![ask("orders", None, Some(&format!("p|{}", u64::MAX)), 10)],
    )
    .await;
    assert_eq!(end.entries[0], Default::default());

    // An unknown queue, checked before the cursor; a missing name is one.
    let (status, v) = route(
        &f,
        json!([
            {"queue":"ghost"},
            {"queue":"ghost","after":"garbage"},
            {"limit":3},
            {"queue":"orders","limit":2}
        ]),
    )
    .await;
    assert_eq!(status, 200, "{v}");
    assert_eq!(
        v["entries"][0],
        json!({"queue":"ghost","error":"UNKNOWN_TOPIC_OR_PARTITION"})
    );
    assert_eq!(
        v["entries"][1],
        json!({"queue":"ghost","error":"UNKNOWN_TOPIC_OR_PARTITION"})
    );
    assert_eq!(
        v["entries"][2],
        json!({"queue":"","error":"UNKNOWN_TOPIC_OR_PARTITION"})
    );
    // ... and does not spoil the entries around it.
    assert_eq!(v["entries"][3]["partitions"].as_array().unwrap().len(), 2);
    assert!(v["entries"][3]["next"].is_string());
    let typed = changed(&f, vec![ask("ghost", None, None, 10)]).await;
    assert_eq!(typed.entries[0].error, Some("UNKNOWN_TOPIC_OR_PARTITION"));

    // A `since` that is not a timestamp refuses the request; null is none.
    for bad in [
        json!("yesterday"),
        json!(""),
        json!("2026-13-01T00:00:00Z"),
        json!(12345),
        json!(true),
        json!({"t":1}),
    ] {
        let (status, v) = route(
            &f,
            json!([{"queue":"orders"},{"queue":"orders","since":bad}]),
        )
        .await;
        assert_eq!(status, 400, "{bad}: {v}");
        assert!(v["error"].as_str().unwrap().contains("since"), "{v}");
    }
    for good in [
        json!(null),
        json!("2001-09-04T10:00:00Z"),
        json!("2001-09-04T10:00:00.123456Z"),
        json!("2001-09-04T12:00:00+02:00"),
        json!("2001-09-04T10:00:00"),
        json!("2001-09-04"),
    ] {
        let (status, v) = route(&f, json!([{"queue":"orders","since":good}])).await;
        assert_eq!(status, 200, "{good}: {v}");
        assert_eq!(
            v["entries"][0]["partitions"].as_array().unwrap().len(),
            4,
            "{good}"
        );
    }
    // Read to the microsecond: one µs past a partition's last write excludes it.
    let (all, _, _) = pass(&f, "orders", None, 10).await;
    let p0 = &all[0];
    let (status, v) = route(
        &f,
        json!([
            {"queue":"orders","since":iso_us(p0.last_write_at_us)},
            {"queue":"orders","since":iso_us(p0.last_write_at_us + 1)}
        ]),
    )
    .await;
    assert_eq!(status, 200);
    let has_p0 = |e: &Value| {
        e["partitions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["name"] == json!(p0.name))
    };
    assert!(has_p0(&v["entries"][0]));
    assert!(!has_p0(&v["entries"][1]));

    // `limit` is clamped, never refused.
    let (_, v) = route(
        &f,
        json!([
            {"queue":"orders","limit":0},
            {"queue":"orders","limit":-5},
            {"queue":"orders","limit":5000},
            {"queue":"orders","limit":null}
        ]),
    )
    .await;
    let lens: Vec<usize> = v["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["partitions"].as_array().unwrap().len())
        .collect();
    assert_eq!(lens, [1, 1, 4, 4]);
    let one = changed(&f, vec![ask("orders", None, None, 0)]).await;
    assert_eq!(one.entries[0].partitions.len(), 1);

    // At most 1024 entries a call, refused whole above.
    let entries: Vec<Value> = (0..1024)
        .map(|_| json!({"queue":"orders","limit":1}))
        .collect();
    assert_eq!(route(&f, Value::Array(entries.clone())).await.0, 200);
    let mut over = entries;
    over.push(json!({"queue":"orders"}));
    let (status, v) = route(&f, Value::Array(over)).await;
    assert_eq!(status, 400, "{v}");
    let asks: Vec<ChangedAsk> = (0..1025).map(|_| ask("orders", None, None, 1)).collect();
    match f.partitions_changed(ctx(), asks).await {
        Err(RsmError::Rejected { code, .. }) => assert_eq!(code, "bad_body"),
        other => panic!("1025 asks: {other:?}"),
    }
    close(f, dir).await;
}

// ---------------------------------------------------------------------------
// id
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn id_is_the_partition_uuid_and_does_not_change() {
    let dir = scratch("id");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    let parts: Vec<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
    touch(&f, "ids", &parts).await;
    let (listed, _, _) = pass(&f, "ids", None, 10).await;
    let ids: BTreeMap<String, String> = listed
        .iter()
        .map(|p| (p.name.clone(), p.id.clone()))
        .collect();
    assert_eq!(ids.len(), 3);
    for id in ids.values() {
        assert!(
            crate::frames::uuid_string_to_bytes(id).is_some(),
            "{id} is a uuid"
        );
    }
    assert_eq!(
        ids.values().collect::<BTreeSet<_>>().len(),
        3,
        "one id per partition"
    );

    // The uuid every other read reports for the partition.
    let (status, v) = api(
        &f,
        "GET",
        "/api/v1/messages",
        Some("queue=ids"),
        Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{v}");
    let messages = v["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 3, "{v}");
    for m in messages {
        let name = m["partition"].as_str().unwrap();
        assert_eq!(m["partitionId"].as_str().unwrap(), ids[name], "{m}");
    }

    // The same on every call and surface, writes in between.
    touch(&f, "ids", &parts).await;
    let (again, _, _) = pass(&f, "ids", None, 2).await;
    assert_eq!(
        again
            .iter()
            .map(|p| (p.name.clone(), p.id.clone()))
            .collect::<BTreeMap<_, _>>(),
        ids
    );
    let (_, v) = route(&f, json!([{"queue":"ids"}])).await;
    for p in v["entries"][0]["partitions"].as_array().unwrap() {
        assert_eq!(p["id"].as_str().unwrap(), ids[p["name"].as_str().unwrap()]);
    }

    // A partition deleted and created again under the same name is a new one.
    let (status, v) = api(
        &f,
        "DELETE",
        "/api/v1/resources/queues/ids",
        None,
        Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{v}");
    touch(&f, "ids", &parts[..1]).await;
    let (reborn, _, _) = pass(&f, "ids", None, 10).await;
    assert_eq!(names(&reborn), ["a"]);
    assert_ne!(reborn[0].id, ids["a"], "a new partition, a new id");
    close(f, dir).await;
}

// ---------------------------------------------------------------------------
// The typed twins are the routes
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn the_typed_discovery_answers_what_the_route_renders() {
    let dir = scratch("twin");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    // Two writes, two stamps: the second six partitions are newer.
    let q1: Vec<String> = (0..12).map(|i| format!("q1-{i}")).collect();
    touch(&f, "q1", &q1[..6]).await;
    touch(&f, "q1", &q1[6..]).await;
    touch(
        &f,
        "q2",
        &["x".to_string(), "y".to_string(), "z".to_string()],
    )
    .await;
    let first = changed(&f, vec![ask("q1", None, None, 5)]).await;
    let cursor = first.entries[0].next.clone().expect("a full page");
    let (all_q1, _, _) = pass(&f, "q1", None, 100).await;
    let since = all_q1[6].last_write_at_us;
    assert!(all_q1[5].last_write_at_us < since);

    let asks = vec![
        ask("q1", None, None, 5),
        ask("q1", None, Some(&cursor), 5),
        ask("q1", Some(since), None, 1000),
        ask("q2", None, None, 1000),
        ask("ghost", None, None, 10),
        ask("q2", None, Some("n|x"), 10),
    ];
    let body = json!([
        {"queue":"q1","limit":5},
        {"queue":"q1","limit":5,"after":cursor},
        {"queue":"q1","since":iso_us(since)},
        {"queue":"q2"},
        {"queue":"ghost","limit":10},
        {"queue":"q2","after":"n|x","limit":10},
    ]);
    // Read the two with nothing applied in between (checked as above).
    let mut compared = false;
    for _ in 0..50 {
        let before = f.applied_stamps_for_test();
        let typed = changed(&f, asks.clone()).await;
        let (status, v) = route(&f, body.clone()).await;
        if f.applied_stamps_for_test() != before {
            continue;
        }
        assert_eq!(status, 200, "{v}");
        assert_eq!(v["safeTime"], json!(iso_us(typed.safe_time_us)));
        assert_eq!(v["safeTimeDegraded"], json!(false));
        let rendered = v["entries"].as_array().unwrap();
        assert_eq!(rendered.len(), typed.entries.len());
        for ((a, t), r) in asks.iter().zip(&typed.entries).zip(rendered) {
            let want = match t.error {
                Some(e) => json!({"queue": a.queue, "error": e}),
                None => json!({
                    "queue": a.queue,
                    "partitions": t.partitions.iter().map(|p| json!({
                        "name": p.name,
                        "id": p.id,
                        "lastOffset": p.last_offset,
                        "logStart": p.log_start,
                        "lastWriteAt": iso_us(p.last_write_at_us),
                    })).collect::<Vec<_>>(),
                    "next": t.next,
                }),
            };
            assert_eq!(r, &want);
        }
        // Pinned to the wire types, `id` included.
        let wire: queen_protocol::ChangedResponse =
            serde_json::from_value(v.clone()).expect("the route's body is the protocol's");
        assert_eq!(wire.entries.len(), typed.entries.len());
        for (w, t) in wire.entries.iter().zip(&typed.entries) {
            assert_eq!(w.error.as_deref(), t.error);
            assert_eq!(w.next, t.next);
            let ids: Vec<Option<String>> =
                t.partitions.iter().map(|p| Some(p.id.clone())).collect();
            assert_eq!(
                w.partitions
                    .iter()
                    .map(|p| p.id.clone())
                    .collect::<Vec<_>>(),
                ids
            );
        }
        assert_eq!(names(&typed.entries[0].partitions), names(&all_q1[..5]));
        assert_eq!(names(&typed.entries[1].partitions), names(&all_q1[5..10]));
        assert_eq!(
            names(&typed.entries[2].partitions),
            names(&all_q1[6..]),
            "since is inclusive"
        );
        assert_eq!(
            names(&all_q1[6..]).into_iter().collect::<BTreeSet<_>>(),
            q1[6..].iter().cloned().collect::<BTreeSet<_>>()
        );
        assert_eq!(typed.entries[4].error, Some("UNKNOWN_TOPIC_OR_PARTITION"));
        assert_eq!(typed.entries[5].error, Some("BAD_CURSOR"));
        compared = true;
        break;
    }
    assert!(compared, "something applied under every attempt");
    close(f, dir).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn fetch_log_carries_the_routes_transaction_ids() {
    let dir = scratch("txn");
    let f = RaftFacade::open(&build_ctx(&dir)).expect("open facade");
    // Named transaction ids, and items without one (the broker mints the
    // message id and uses it as the transaction id).
    let mut items: Vec<Value> = (0..5)
        .map(|i| json!({"queue":"log","partition":"0","transactionId":format!("tx-{i}"),"payload":{"i":i}}))
        .collect();
    items.extend((5..9).map(|i| json!({"queue":"log","partition":"0","payload":{"i":i}})));
    items.push(json!({"queue":"log","partition":"1","transactionId":"metric|x","payload":"plain"}));
    push(&f, items).await;

    let (status, v) = api(
        &f,
        "POST",
        "/api/v1/fetch",
        None,
        json!({"entries":[{"queue":"log","partition":"0","offset":0},{"queue":"log","partition":"1","offset":0}]}),
    )
    .await;
    assert_eq!(status, 200, "{v}");
    let typed: Vec<RecordsFetched> = f
        .fetch_log(ctx(), from_zero("log", &["0", "1"]), 0, 1)
        .await
        .expect("fetch_log");
    let mut txns = BTreeSet::new();
    for (t, r) in typed.iter().zip(v["entries"].as_array().unwrap()) {
        let rendered = r["records"].as_array().unwrap();
        assert_eq!(t.records.len(), rendered.len());
        assert_eq!(json!(t.high_watermark), r["highWatermark"]);
        assert_eq!(json!(t.log_start_offset), r["logStartOffset"]);
        for (rec, json_rec) in t.records.iter().zip(rendered) {
            let txn = rec
                .txn
                .as_deref()
                .expect("fetch_log reads the transaction id");
            assert_eq!(json_rec["transactionId"], json!(txn));
            assert_eq!(json_rec["offset"], json!(rec.offset));
            assert_eq!(json_rec["ts"], json!(iso_us(rec.created_at_us)));
            assert_eq!(
                json_rec["payload"],
                serde_json::from_slice::<Value>(&rec.payload).unwrap()
            );
            txns.insert(txn.to_string());
        }
    }
    assert_eq!(txns.len(), 10, "every record its own transaction id");
    for i in 0..5 {
        assert!(txns.contains(&format!("tx-{i}")));
    }
    assert!(txns.contains("metric|x"));

    // fetch_records is unchanged: no transaction ids.
    let plain = f
        .fetch_records(ctx(), from_zero("log", &["0", "1"]), 0, 1)
        .await
        .expect("fetch_records");
    assert!(plain
        .iter()
        .flat_map(|e| &e.records)
        .all(|r| r.txn.is_none()));
    assert_eq!(
        plain.iter().map(|e| e.records.len()).sum::<usize>(),
        typed.iter().map(|e| e.records.len()).sum::<usize>()
    );
    close(f, dir).await;
}
