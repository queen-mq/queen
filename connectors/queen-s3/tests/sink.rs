//! The entry point the broker calls — [`Sink`] — over a [`FakeQueen`] and an
//! in-memory bucket: the queue set, the lease, the bucket probe, the stop, and
//! what `status()` and `prometheus()` report.
//!
//! These run the real tasks on a paused clock, as the broker runs them on its
//! own runtime: nothing is ticked by hand here, which is what the `driver_*`
//! tests are for.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use queen_s3::config::DEFAULT_TENANT;
use queen_s3::lease::{Acquired, Lease};
use queen_s3::queen::{BoxFuture, FakeQueen, KvOp, KvResult, QueenApi};
use queen_s3::s3::MemoryStore;
use queen_s3::sink::MISSING_RETRY;
use queen_s3::types::{
    ChangedRequestEntry, ChangedResponse, FetchRequestEntry, FetchedEntry, Micros, SinkError,
};
use queen_s3::{Config, NodeKnobs, Sink, SinkShared};

#[path = "driver_support.rs"]
mod support;
use support::*;

/// The smallest configuration that runs, plus `extra`, as node `node-1`.
fn config(extra: &[(&'static str, &'static str)]) -> Config {
    config_for("node-1", extra)
}

/// [`config`] as node `instance`.
fn config_for(instance: &str, extra: &[(&str, &str)]) -> Config {
    let mut pairs: Vec<(&str, &str)> = vec![
        ("QUEEN_S3_QUEUES", "orders"),
        ("QUEEN_S3_ENDPOINT", "http://gw:7070"),
        ("QUEEN_S3_REGION", "us-east-1"),
        ("QUEEN_S3_BUCKET", "lake"),
        ("QUEEN_S3_ACCESS_KEY", "ak"),
        ("QUEEN_S3_SECRET_KEY", "sk"),
        ("QUEEN_S3_START", "earliest"),
        // FakeQueen's safeTime is one microsecond past its newest record: the
        // product's 5 s guard would hold every record of a test back.
        ("QUEEN_S3_SAFE_GUARD_MS", "0"),
        ("QUEEN_S3_DISCOVERY_INTERVAL_MS", "50"),
    ];
    for (k, v) in extra {
        pairs.retain(|(existing, _)| existing != k);
        pairs.push((k, v));
    }
    Config::from_pairs_with(&pairs, instance).expect("the test configuration parses")
}

/// The queues a sink's status says it runs here.
fn owned(sink: &Sink) -> Vec<String> {
    sink.status()["queues"]
        .as_array()
        .map(|qs| {
            qs.iter()
                .filter(|q| q["ownedHere"] == true)
                .map(|q| q["name"].as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// Move the broker's clock forward by `by` — on a real broker the sink's own
/// lease refreshes are the entries that move it when nothing else is written.
fn advance_safe_time(queen: &FakeQueen, by: Duration) {
    let now = queen.safe_time();
    queen.set_safe_time(Micros(now.0 + by.as_micros() as i64));
}

/// Wait, on the paused clock, until `done` — or panic naming `what`.
async fn until(what: &str, done: impl Fn() -> bool) {
    for _ in 0..20_000 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for {what}");
}

/// One queue's row of a status document, or `null`.
fn row(status: &Value, queue: &str) -> Value {
    status["queues"]
        .as_array()
        .and_then(|qs| qs.iter().find(|q| q["name"] == queue).cloned())
        .unwrap_or(Value::Null)
}

/// Run `sink` on a task until the returned sender fires.
fn start(
    sink: &Arc<Sink>,
) -> (
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let sink = sink.clone();
    let handle = tokio::spawn(async move {
        sink.run(async {
            let _ = rx.await;
        })
        .await
    });
    (tx, handle)
}

async fn stop(tx: tokio::sync::oneshot::Sender<()>, handle: tokio::task::JoinHandle<()>) {
    tx.send(()).expect("the sink is still running");
    tokio::time::timeout(Duration::from_secs(600), handle)
        .await
        .expect("run returns once every queue has drained")
        .expect("the run task does not panic");
}

#[tokio::test(start_paused = true)]
async fn the_sink_ships_its_queue_reports_it_and_stops_when_told() {
    let queen = Arc::new(FakeQueen::new());
    let store = Arc::new(MemoryStore::new());
    let expected = seed_two_hours(&queen, "orders", &["cust-1", "cust-2"]);
    let sink = Arc::new(Sink::new(config(&[]), queen.clone(), Some(store.clone())).unwrap());

    let idle = sink.status();
    assert_eq!(idle["running"], false);
    assert_eq!(
        idle["version"], "2.0.0",
        "the sink in-process on the raft broker; 1.5.0 named the standalone one"
    );
    assert_eq!(idle["instance"], "node-1");
    assert_eq!(idle["sink"], "default");
    assert_eq!(idle["bucket"]["reachable"], Value::Null, "not probed yet");
    assert!(idle["queues"].as_array().unwrap().is_empty());

    let (tx, handle) = start(&sink);
    until("two committed windows", || {
        row(&sink.status(), "orders")["k"] == 2
    })
    .await;

    let st = sink.status();
    assert_eq!(st["running"], true);
    assert_eq!(st["ok"], true);
    assert_eq!(st["bucket"]["reachable"], true);
    assert_eq!(st["bucket"]["name"], "lake");
    assert_eq!(st["bucket"]["prefix"], "queen");
    assert_eq!(st["format"], "jsonl");
    assert_eq!(st["compression"], "zstd");
    assert_eq!(st["layout"], "merged");
    assert_eq!(st["align"], "hour");
    assert_eq!(st["health"]["ok"], true);
    assert_eq!(st["health"]["queues"], 1, "one queue owned here");
    let q = row(&st, "orders");
    assert_eq!(q["ownedHere"], true);
    assert_eq!(q["heldBy"], Value::Null);
    assert_eq!(q["state"], "filling");
    assert_eq!(q["tEnd"], "2026-09-04T11:20:00.000001Z");
    assert_eq!(q["windowsCommitted"], 2);
    assert_eq!(q["records"], 30);
    assert_eq!(q["recordsLost"], 0);
    assert!(q["bytes"].as_u64().unwrap() > 0);
    assert_eq!(
        q["lagSeconds"], 0.0,
        "safeTime - tEnd, on the broker's clock: nothing is behind"
    );
    assert_eq!(
        queen.kv_get("s3:default:orders:lease").unwrap()["instance"],
        "node-1",
        "the lease names this node"
    );
    let text = sink.prometheus();
    assert!(
        text.contains("queen_s3_windows_committed_total{queue=\"orders\"} 2"),
        "{text}"
    );
    assert!(text.contains("# TYPE queen_s3_lag_seconds gauge"), "{text}");

    stop(tx, handle).await;
    let st = sink.status();
    assert_eq!(st["running"], false);
    assert_eq!(row(&st, "orders")["state"], "drained");
    assert_eq!(
        queen.kv_get("s3:default:orders:lease"),
        None,
        "a stopped sink gives its leases back"
    );
    assert_exactly_once(&store, &expected);
    assert_manifests_match_objects(&store);
}

#[tokio::test(start_paused = true)]
async fn an_unreachable_bucket_delays_the_queues_is_reported_and_is_never_fatal() {
    let queen = Arc::new(FakeQueen::new());
    let store = Arc::new(MemoryStore::new());
    let expected = seed_two_hours(&queen, "orders", &["a"]);
    store.fail_next(4);
    let sink = Arc::new(Sink::new(config(&[]), queen.clone(), Some(store.clone())).unwrap());
    let (tx, handle) = start(&sink);

    until("a failed probe", || {
        sink.status()["bucket"]["reachable"] == false
    })
    .await;
    let st = sink.status();
    assert_eq!(st["ok"], false, "a bucket that does not answer is not ok");
    assert_eq!(st["running"], true);
    let error = st["bucket"]["error"].as_str().unwrap().to_string();
    assert!(error.contains("cannot reach the bucket lake"), "{error}");
    assert!(
        st["queues"].as_array().unwrap().is_empty(),
        "no queue starts before the bucket answers"
    );
    assert_eq!(queen.kv_calls(), 0, "and no lease is claimed");

    until("the bucket answers and the queue ships", || {
        row(&sink.status(), "orders")["k"] == 2
    })
    .await;
    let st = sink.status();
    assert_eq!(st["bucket"]["reachable"], true);
    assert_eq!(st["bucket"]["error"], Value::Null);
    assert_eq!(st["ok"], true);
    stop(tx, handle).await;
    assert_exactly_once(&store, &expected);
}

#[tokio::test(start_paused = true)]
async fn a_queue_another_node_holds_is_reported_with_its_owner_and_left_alone() {
    let queen = Arc::new(FakeQueen::new());
    let store = Arc::new(MemoryStore::new());
    seed_two_hours(&queen, "orders", &["a"]);
    let other = Lease::new(queen.clone(), "default", "orders", "node-2", 30_000);
    assert_eq!(other.acquire().await.unwrap(), Acquired::Taken);

    let sink = Arc::new(Sink::new(config(&[]), queen.clone(), Some(store.clone())).unwrap());
    let (tx, handle) = start(&sink);
    until("the claim is answered", || {
        row(&sink.status(), "orders")["state"] == "held"
    })
    .await;
    let q = row(&sink.status(), "orders");
    assert_eq!(q["ownedHere"], false);
    assert_eq!(q["heldBy"], "node-2");
    assert_eq!(q["k"], 0);
    assert_eq!(
        sink.status()["health"]["queues"],
        0,
        "a queue another node owns is not this node's health"
    );
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(
        data_keys(&store).is_empty(),
        "this node writes nothing for it"
    );
    stop(tx, handle).await;
    assert_eq!(
        queen.kv_get("s3:default:orders:lease").unwrap()["instance"],
        "node-2",
        "stopping does not touch a lease this node never held"
    );
}

#[tokio::test(start_paused = true)]
async fn with_a_star_every_queue_is_sinked_including_one_created_later() {
    let queen = Arc::new(FakeQueen::new());
    let store = Arc::new(MemoryStore::new());
    seed_two_hours(&queen, "orders", &["a"]);
    let sink = Arc::new(
        Sink::new(
            config(&[("QUEEN_S3_QUEUES", "*")]),
            queen.clone(),
            Some(store.clone()),
        )
        .unwrap(),
    );
    let (tx, handle) = start(&sink);
    until("orders ships", || row(&sink.status(), "orders")["k"] == 2).await;

    seed_two_hours(&queen, "clicks", &["x"]);
    until("the new queue is listed and ships", || {
        row(&sink.status(), "clicks")["k"] == 2
    })
    .await;
    stop(tx, handle).await;
    assert!(data_keys(&store).iter().any(|k| k.starts_with(&format!(
        "queen/tenant={}/queue=clicks/",
        queen_s3::config::DEFAULT_TENANT
    ))));
}

/// An idle queue — everything shipped, nothing new — stays green, with a lag
/// of about one discovery pass, for as long as it stays idle: here thirty
/// windows' worth of the broker's clock, which a verdict counting from the
/// last commit turned red after three. `completeThrough` follows the clock;
/// `tEnd` stays where the last window ended.
#[tokio::test(start_paused = true)]
async fn an_idle_queue_stays_green_with_no_lag_however_long_it_is_idle() {
    let queen = Arc::new(FakeQueen::new());
    let store = Arc::new(MemoryStore::new());
    seed_two_hours(&queen, "orders", &["a", "b"]);
    let sink = Arc::new(
        Sink::new(
            config(&[("QUEEN_S3_MAX_WINDOW_MS", "10000")]),
            queen.clone(),
            Some(store.clone()),
        )
        .unwrap(),
    );
    let (tx, handle) = start(&sink);
    until("two committed windows", || {
        row(&sink.status(), "orders")["k"] == 2
    })
    .await;
    let t_end = row(&sink.status(), "orders")["tEnd"].clone();

    for second in 0..300 {
        advance_safe_time(&queen, Duration::from_secs(1));
        tokio::time::sleep(Duration::from_secs(1)).await;
        let st = sink.status();
        assert_eq!(st["health"]["ok"], true, "second {second}: {st}");
        let q = row(&st, "orders");
        let lag = q["lagSeconds"].as_f64().expect("the lag is known");
        assert!(lag <= 1.5, "second {second}: {q}");
    }
    let q = row(&sink.status(), "orders");
    assert_eq!(q["k"], 2, "nothing was committed while it was idle");
    assert_eq!(q["tEnd"], t_end);
    let through = Micros::parse_iso(q["completeThrough"].as_str().unwrap()).unwrap();
    let end = Micros::parse_iso(t_end.as_str().unwrap()).unwrap();
    assert!(
        through.0 - end.0 >= 290 * 1_000_000,
        "complete through the clock, not the last commit: {q}"
    );
    stop(tx, handle).await;
}

/// A bucket that refuses PUTs while records are pending: the queue cannot
/// ship, its lag grows with the clock, and the verdict turns red once the lag
/// passes the budget (3 × 10 s, floored at 30 s) — whether the refusal is
/// retried in place (503) or stops the queue until it is fixed (403). When the
/// bucket takes writes again the queue ships, and the verdict is green again.
#[tokio::test(start_paused = true)]
async fn a_bucket_that_refuses_writes_turns_the_verdict_red_until_it_takes_them() {
    for status in [503u16, 403] {
        let queen = Arc::new(FakeQueen::new());
        let store = Arc::new(MemoryStore::new());
        let expected = seed_two_hours(&queen, "orders", &["a"]);
        store.refuse_puts(Some(status));
        let sink = Arc::new(
            Sink::new(
                config(&[("QUEEN_S3_MAX_WINDOW_MS", "10000")]),
                queen.clone(),
                Some(store.clone()),
            )
            .unwrap(),
        );
        let (tx, handle) = start(&sink);

        let mut red = None;
        for second in 0..120 {
            advance_safe_time(&queen, Duration::from_secs(1));
            tokio::time::sleep(Duration::from_secs(1)).await;
            let st = sink.status();
            if st["health"]["ok"] == false {
                red = Some((second, st));
                break;
            }
        }
        let (second, st) = red.unwrap_or_else(|| panic!("{status}: never went red"));
        assert!(
            (28..=40).contains(&second),
            "{status}: red at {second} s, the budget is 30 s"
        );
        assert_eq!(st["ok"], false);
        assert_eq!(st["health"]["queue"], "orders", "{st}");
        assert!(st["health"]["staleMs"].as_i64().unwrap() > 30_000, "{st}");
        assert!(
            data_keys(&store).is_empty(),
            "{status}: nothing was written"
        );

        store.refuse_puts(None);
        until("the queue ships once the bucket takes writes", || {
            row(&sink.status(), "orders")["k"] == 2
        })
        .await;
        until("and the verdict is green again", || {
            sink.status()["health"]["ok"] == true
        })
        .await;
        stop(tx, handle).await;
        assert_exactly_once(&store, &expected);
    }
}

/// A queue this node stops running is no longer in its gauges: once another
/// node takes the lease over, `queen_s3_lag_seconds` and the other per-queue
/// gauges of the queue are gone from the exposition — an alert taking the max
/// over nodes must not keep this node's last lag for ever — while its counters
/// stay, as totals do. With no queue running here, the node's safe-lag gauge
/// goes too.
#[tokio::test(start_paused = true)]
async fn a_queue_this_node_stops_running_leaves_no_gauge_behind() {
    let queen = Arc::new(FakeQueen::new());
    let store = Arc::new(MemoryStore::new());
    seed_two_hours(&queen, "orders", &["a"]);
    let sink = Arc::new(Sink::new(config(&[]), queen.clone(), Some(store.clone())).unwrap());
    let (tx, handle) = start(&sink);
    until("two committed windows", || {
        row(&sink.status(), "orders")["k"] == 2
    })
    .await;
    let gauges = [
        "queen_s3_lag_seconds{queue=\"orders\"}",
        "queen_s3_discovery_partitions{queue=\"orders\"}",
        "queen_s3_checkpoint_age_windows{queue=\"orders\"}",
        "\nqueen_s3_safe_lag_seconds ",
    ];
    let text = sink.prometheus();
    for g in gauges {
        assert!(
            text.contains(g),
            "{g} is exported while the queue runs:\n{text}"
        );
    }

    queen.kv_seed(
        "s3:default:orders:lease",
        json!({"instance": "node-2", "incarnation": "x", "sinceMs": 0}),
    );
    until("another node holds the queue", || {
        row(&sink.status(), "orders")["heldBy"] == "node-2"
    })
    .await;
    let text = sink.prometheus();
    for g in gauges {
        assert!(!text.contains(g), "{g} outlived the queue here:\n{text}");
    }
    assert!(
        text.contains("queen_s3_windows_committed_total{queue=\"orders\"} 2"),
        "counters stay:\n{text}"
    );
    assert!(text.contains("# TYPE queen_s3_lag_seconds gauge"), "{text}");
    assert_eq!(sink.status()["health"]["queues"], 0);
    stop(tx, handle).await;
}

/// A queue named in `QUEEN_S3_QUEUES` that does not exist yet is reported
/// `missing` and looked for again every `MISSING_RETRY`: created a moment
/// after boot, it ships seconds later, not a minute later. A missing queue is
/// not a broken sink.
#[tokio::test(start_paused = true)]
async fn a_queue_created_after_boot_ships_seconds_later() {
    let queen = Arc::new(FakeQueen::new());
    let store = Arc::new(MemoryStore::new());
    let sink = Arc::new(Sink::new(config(&[]), queen.clone(), Some(store.clone())).unwrap());
    let (tx, handle) = start(&sink);
    until("the queue is reported missing", || {
        row(&sink.status(), "orders")["state"] == "missing"
    })
    .await;
    assert_eq!(sink.status()["health"]["ok"], true);

    let created = tokio::time::Instant::now();
    let expected = seed_two_hours(&queen, "orders", &["a"]);
    until("the queue ships", || {
        row(&sink.status(), "orders")["k"] == 2
    })
    .await;
    assert!(
        created.elapsed() <= MISSING_RETRY + Duration::from_secs(2),
        "picked up {:?} after it was created",
        created.elapsed()
    );
    stop(tx, handle).await;
    assert_exactly_once(&store, &expected);
}

/// Three nodes boot together over seven queues. Each claims a FREE queue only
/// after a wait that grows with what it already runs, so the queues spread —
/// no node ends with more than ceil(7/3) + 1 — instead of all landing on the
/// node that asked first. When a node stops, it gives its queues back and the
/// other two take them within about one lease TTL, still spread.
#[tokio::test(start_paused = true)]
async fn queues_spread_over_the_nodes_and_fail_over_within_a_ttl() {
    let queen = Arc::new(FakeQueen::new());
    let store = Arc::new(MemoryStore::new());
    let queues: Vec<String> = (1..=7).map(|i| format!("q{i}")).collect();
    let mut expected = Vec::new();
    for q in &queues {
        // One partition per queue, named after it: the lake is checked by
        // (partition, offset).
        let partition = format!("{q}-a");
        expected.extend(seed_two_hours(&queen, q, &[partition.as_str()]));
    }
    let list = queues.join(",");
    let sinks: Vec<Arc<Sink>> = (0..3u64)
        .map(|n| {
            let cfg = config_for(&format!("node-{n}"), &[("QUEEN_S3_QUEUES", list.as_str())]);
            // A fixed jitter per (node, queue): the order of the claims is the
            // same on every run.
            let jitter = move |q: &str| {
                let qi: u64 = q.trim_start_matches('q').parse().unwrap_or(0);
                Duration::from_millis((61 * n + 17 * qi) % 197)
            };
            Arc::new(
                Sink::new(cfg, queen.clone(), Some(store.clone()))
                    .unwrap()
                    .with_claim_jitter(jitter),
            )
        })
        .collect();
    let mut runs: Vec<_> = sinks.iter().map(start).collect();

    let all_owned = |nodes: &[Arc<Sink>]| {
        let mut held: Vec<String> = nodes.iter().flat_map(|s| owned(s)).collect();
        held.sort();
        held == queues
    };
    let all_shipped = |nodes: &[Arc<Sink>]| {
        nodes
            .iter()
            .flat_map(|s| s.status()["queues"].as_array().cloned().unwrap_or_default())
            .filter(|q| q["k"] == 2 && q["ownedHere"] == true)
            .count()
            == queues.len()
    };
    until("every queue owned by exactly one node", || {
        all_owned(&sinks)
    })
    .await;
    let counts: Vec<usize> = sinks.iter().map(|s| owned(s).len()).collect();
    assert!(
        counts.iter().all(|c| *c <= 4),
        "spread over three: {counts:?}"
    );
    assert_eq!(counts, [2, 2, 3], "the claims of this jitter, every run");
    until("every queue shipped", || all_shipped(&sinks)).await;

    // Settled: every claim of the boot has been answered. Node 0 stops and
    // gives its queues back; the others find them free at their next look,
    // one TTL after their last, and the less loaded of the two claims first.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let (tx, handle) = runs.remove(0);
    let stopped = tokio::time::Instant::now();
    stop(tx, handle).await;
    until("the other two own all seven", || all_owned(&sinks[1..])).await;
    assert!(
        stopped.elapsed() <= Duration::from_secs(30 + 2),
        "failed over in {:?}, the TTL is 30 s",
        stopped.elapsed()
    );
    let counts: Vec<usize> = sinks[1..].iter().map(|s| owned(s).len()).collect();
    assert!(
        counts.iter().all(|c| *c <= 5),
        "spread over two: {counts:?}"
    );

    until("every queue shipped by the two", || {
        all_shipped(&sinks[1..])
    })
    .await;
    for (tx, handle) in runs {
        stop(tx, handle).await;
    }
    assert_exactly_once(&store, &expected);
}

/// Two tenants' sinks on one node, one `SinkShared`, ONE bucket and prefix,
/// each tenant with a queue called `orders` read from its own Queen: the
/// objects never collide (the tenant is the first key), the exposition renders
/// every family once with each tenant's series told apart by its label — the
/// default tenant's carrying none — and the two sinks share the node's one
/// memory budget. A tenant whose sink stops loses its gauges; one whose
/// configuration is deleted loses every series.
#[tokio::test(start_paused = true)]
async fn two_tenants_share_a_node_and_a_bucket_without_touching() {
    let node = NodeKnobs::from_pairs_with(
        &[
            ("QUEEN_S3_SAFE_GUARD_MS", "0"),
            ("QUEEN_S3_DISCOVERY_INTERVAL_MS", "50"),
            ("QUEEN_S3_MEMORY_MB", "64"),
        ],
        "node-1",
    )
    .unwrap();
    let shared = Arc::new(SinkShared::new(&node));
    let store = Arc::new(MemoryStore::new());

    // The default tenant: the environment's sink, no tenant label.
    let queen_d = Arc::new(FakeQueen::new());
    let expected_d = seed_two_hours(&queen_d, "orders", &["d-1", "d-2"]);
    let env: Vec<(&str, &str)> = vec![
        ("QUEEN_S3_QUEUES", "orders"),
        ("QUEEN_S3_ENDPOINT", "http://gw:7070"),
        ("QUEEN_S3_REGION", "us-east-1"),
        ("QUEEN_S3_BUCKET", "lake"),
        ("QUEEN_S3_ACCESS_KEY", "ak"),
        ("QUEEN_S3_SECRET_KEY", "sk"),
        ("QUEEN_S3_START", "earliest"),
    ];
    let cfg_d = Config::from_env_pairs(&node, DEFAULT_TENANT, &env)
        .unwrap()
        .unwrap()
        .with_tenant_label(None);
    let sink_d =
        Arc::new(Sink::new_shared(cfg_d, queen_d.clone(), Some(store.clone()), &shared).unwrap());

    // Tenant A: a control-plane document, labelled with its id.
    let queen_a = Arc::new(FakeQueen::new());
    let expected_a = seed_two_hours(&queen_a, "orders", &["a-1"]);
    let doc = json!({
        "endpoint": "http://gw:7070",
        "region": "us-east-1",
        "bucket": "lake",
        "accessKey": "ak",
        "queues": ["orders"],
        "start": "earliest",
    });
    let cfg_a = Config::from_tenant_doc(&node, "tenant-a", &doc, "sk")
        .unwrap()
        .with_tenant_label(Some("tenant-a".into()));
    let sink_a =
        Arc::new(Sink::new_shared(cfg_a, queen_a.clone(), Some(store.clone()), &shared).unwrap());

    let (tx_d, run_d) = start(&sink_d);
    let (tx_a, run_a) = start(&sink_a);
    until("both tenants commit their two windows", || {
        row(&sink_d.status(), "orders")["k"] == 2 && row(&sink_a.status(), "orders")["k"] == 2
    })
    .await;
    assert_eq!(sink_d.status()["tenant"], DEFAULT_TENANT);
    assert_eq!(sink_a.status()["tenant"], "tenant-a");
    assert_eq!(
        sink_a.status()["memory"]["limitBytes"],
        64 * 1024 * 1024,
        "the node's one budget"
    );
    assert_eq!(shared.memory_limit_bytes(), 64 * 1024 * 1024);

    // One bucket, one prefix, two tenants: every key under its own tenant.
    let keys = store.keys();
    let under = |t: &str| {
        let data = format!("queen/tenant={t}/queue=orders/");
        let side = format!("queen/_queen/tenant={t}/queue=orders/");
        keys.iter()
            .filter(|k| k.starts_with(&data) || k.starts_with(&side))
            .count()
    };
    assert_eq!(
        under(DEFAULT_TENANT) + under("tenant-a"),
        keys.len(),
        "{keys:?}"
    );
    assert!(
        under(DEFAULT_TENANT) >= 4 && under("tenant-a") >= 4,
        "{keys:?}"
    );
    for m in manifests(&store) {
        assert!(
            m.objects
                .iter()
                .all(|o| o.key.contains(&format!("tenant={}/", m.tenant))),
            "a manifest names its own tenant's objects: {m:?}"
        );
    }
    assert_eq!(
        expected_d.len() + expected_a.len(),
        all_rows(&store).len(),
        "each record once, across both tenants"
    );

    // One exposition, every family once, the tenants told apart.
    let text = shared.prometheus();
    assert_eq!(
        text.matches("# TYPE queen_s3_windows_committed_total counter")
            .count(),
        1,
        "{text}"
    );
    for line in [
        "queen_s3_windows_committed_total{queue=\"orders\"} 2",
        "queen_s3_windows_committed_total{tenant=\"tenant-a\",queue=\"orders\"} 2",
        "queen_s3_lag_seconds{tenant=\"tenant-a\",queue=\"orders\"}",
        "queen_s3_lag_seconds{queue=\"orders\"}",
    ] {
        assert!(text.contains(line), "{line} missing:\n{text}");
    }

    // Tenant A's sink stops: its gauges go, the default tenant's stay.
    stop(tx_a, run_a).await;
    let text = shared.prometheus();
    assert!(
        !text.contains("queen_s3_lag_seconds{tenant=\"tenant-a\""),
        "a stopped sink leaves no gauge:\n{text}"
    );
    assert!(
        text.contains("queen_s3_lag_seconds{queue=\"orders\"}"),
        "{text}"
    );
    assert!(
        text.contains("queen_s3_windows_committed_total{tenant=\"tenant-a\",queue=\"orders\"} 2"),
        "its counters stay while the tenant may come back:\n{text}"
    );
    // Its configuration is deleted: every series of the tenant goes.
    shared.forget_tenant("tenant-a");
    let text = shared.prometheus();
    assert!(!text.contains("tenant=\"tenant-a\""), "{text}");
    assert!(
        text.contains("queen_s3_windows_committed_total{queue=\"orders\"} 2"),
        "{text}"
    );

    stop(tx_d, run_d).await;
}

/// One node's view of a shared [`FakeQueen`] whose KV store has no leader yet
/// for this node: every KV call answers 503 until `until` (on the tokio
/// clock), as right after a cold start; reads of the log are answered.
struct Leaderless {
    inner: Arc<FakeQueen>,
    until: tokio::time::Instant,
}

impl QueenApi for Leaderless {
    fn fetch(
        &self,
        entries: Vec<FetchRequestEntry>,
        max_wait_ms: u64,
        min_bytes: i64,
    ) -> BoxFuture<'_, queen_s3::queen::Result<Vec<FetchedEntry>>> {
        self.inner.fetch(entries, max_wait_ms, min_bytes)
    }

    fn partitions_changed(
        &self,
        entries: Vec<ChangedRequestEntry>,
    ) -> BoxFuture<'_, queen_s3::queen::Result<ChangedResponse>> {
        self.inner.partitions_changed(entries)
    }

    fn kv(&self, ops: Vec<KvOp>) -> BoxFuture<'_, queen_s3::queen::Result<Vec<KvResult>>> {
        if tokio::time::Instant::now() < self.until {
            return Box::pin(async {
                Err(SinkError::Status {
                    code: 503,
                    body: "{\"error\":\"no leader\"}".into(),
                    retry_after_ms: None,
                })
            });
        }
        self.inner.kv(ops)
    }

    fn list_queues(&self) -> BoxFuture<'_, queen_s3::queen::Result<Vec<String>>> {
        self.inner.list_queues()
    }
}

/// A staggered cold start: three nodes over six queues, and two of them meet
/// no leader for their first 400 ms. They are back within about a second — a
/// transient failure is retried soon, not after a lease TTL — while the node
/// that could read has taken only what its load-aware wait allows, so every
/// node ends with a share: nobody empty, nobody past ceil(6/3) + 1.
#[tokio::test(start_paused = true)]
async fn nodes_whose_first_reads_fail_still_end_spread() {
    let queen = Arc::new(FakeQueen::new());
    let store = Arc::new(MemoryStore::new());
    let queues: Vec<String> = (1..=6).map(|i| format!("q{i}")).collect();
    for q in &queues {
        let partition = format!("{q}-a");
        seed_two_hours(&queen, q, &[partition.as_str()]);
    }
    let list = queues.join(",");
    let t0 = tokio::time::Instant::now();
    let sinks: Vec<Arc<Sink>> = (0..3u64)
        .map(|n| {
            let cfg = config_for(&format!("node-{n}"), &[("QUEEN_S3_QUEUES", list.as_str())]);
            let api: Arc<dyn QueenApi> = match n {
                0 => queen.clone(),
                _ => Arc::new(Leaderless {
                    inner: queen.clone(),
                    until: t0 + Duration::from_millis(400),
                }),
            };
            let jitter = move |q: &str| {
                let qi: u64 = q.trim_start_matches('q').parse().unwrap_or(0);
                Duration::from_millis((61 * n + 17 * qi) % 197)
            };
            Arc::new(
                Sink::new(cfg, api, Some(store.clone()))
                    .unwrap()
                    .with_claim_jitter(jitter),
            )
        })
        .collect();
    let runs: Vec<_> = sinks.iter().map(start).collect();

    until("every queue owned by exactly one node", || {
        let mut held: Vec<String> = sinks.iter().flat_map(|s| owned(s)).collect();
        held.sort();
        held == queues
    })
    .await;
    let counts: Vec<usize> = sinks.iter().map(|s| owned(s).len()).collect();
    assert!(
        counts.iter().all(|c| (1..=3).contains(c)),
        "every node a share, none past ceil(6/3)+1: {counts:?}"
    );
    assert!(
        t0.elapsed() < Duration::from_secs(10),
        "spread long before a lease TTL: {:?}",
        t0.elapsed()
    );
    for (tx, handle) in runs {
        stop(tx, handle).await;
    }
}

/// A stopping sink gives its lease back even when its first release calls
/// meet no leader — a SIGTERMed leader releases during its own leadership
/// hand-off. The release is retried for a few seconds, and the next node
/// does not wait out a TTL.
#[tokio::test(start_paused = true)]
async fn a_stopping_sink_gives_its_lease_back_through_a_leaderless_moment() {
    let queen = Arc::new(FakeQueen::new());
    let store = Arc::new(MemoryStore::new());
    seed_two_hours(&queen, "orders", &["a"]);
    let sink = Arc::new(Sink::new(config(&[]), queen.clone(), Some(store.clone())).unwrap());
    let (tx, handle) = start(&sink);
    until("two committed windows", || {
        row(&sink.status(), "orders")["k"] == 2
    })
    .await;
    assert!(queen.kv_get("s3:default:orders:lease").is_some());

    queen.fail_kv_next(5);
    stop(tx, handle).await;
    assert_eq!(
        queen.kv_get("s3:default:orders:lease"),
        None,
        "given back through five failed calls"
    );
}

#[test]
fn a_configuration_error_fails_the_boot_with_the_variable_named() {
    let queen = Arc::new(FakeQueen::new());
    let err = Sink::new(
        config(&[("QUEEN_S3_PARTITIONS", "orders:0..3")]),
        queen.clone(),
        None,
    )
    .err()
    .expect("a static partition list is refused");
    assert!(err.contains("QUEEN_S3_PARTITIONS"), "{err}");

    // The real S3 client is built without touching the network.
    let sink = Sink::new(config(&[]), queen, None).expect("no I/O at construction");
    assert_eq!(sink.status()["bucket"]["reachable"], Value::Null);
}

#[test]
fn the_sink_can_be_shared_and_run_from_any_runtime_thread() {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<Sink>();
    fn send<T: Send>(_: &T) {}
    let sink = Sink::new(
        config(&[]),
        Arc::new(FakeQueen::new()),
        Some(Arc::new(MemoryStore::new())),
    )
    .unwrap();
    let run = sink.run(std::future::pending());
    send(&run);
}
