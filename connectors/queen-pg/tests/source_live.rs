//! Live tests of the source (PLAN §4) against a real PostgreSQL 17/18 and the
//! crate's `FakeQueen`.
//!
//! Run only when `QUEEN_PG_TEST_DSN` is set (libpq key=value form, e.g.
//! `host=127.0.0.1 port=55432 user=queen_test password=queen_test_pw
//! dbname=queen_test`); otherwise every test prints a skip note and passes.
//! The role needs LOGIN REPLICATION and must own its database (it creates
//! schemas, tables and publications). Every test works in its own schema,
//! slot and publication (random suffix) and drops all three when it ends —
//! on a panic too (a leaked slot pins WAL on a shared server).
//!
//! The oracle of the end-to-end checks: replaying a queue per partition in
//! offset order (`c`/`r` put `after`, `u` merges `after` keeping the
//! `unchanged` columns, `d` removes the key) gives exactly the table; every
//! transaction id is unique; within a partition the events' `lsn` never goes
//! backwards; and every change the writer committed after the slot existed
//! is in the queue exactly once (each write carries a unique value).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rand::{Rng, SeedableRng};
use serde_json::{json, Map, Value};

use queen_pg::config::{ConnectionSpec, ConnectorDoc, DeleteRequest, NodeKnobs, SslMode};
use queen_pg::fake::{FakeCall, FakeMessage, FakeQueen, Fault};
use queen_pg::pg::connect::{connect, EgressPolicy};
use queen_pg::repl::Lsn;
use queen_pg::source::Source;
use queen_pg::{stop_pair, Context, Metrics, RunEnd, StopHandle};

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

/// The DSN as a `ConnectionSpec`, built through the document's own serde
/// (defaults included), so a field added to the spec does not break the
/// test.
fn dsn() -> Option<ConnectionSpec> {
    let dsn = std::env::var("QUEEN_PG_TEST_DSN").ok()?;
    let mut spec = json!({
        "host": "127.0.0.1",
        "port": 5432,
        "sslMode": "disable",
        "connectTimeoutMs": 5000,
    });
    for kv in dsn.split_whitespace() {
        let (k, v) = kv.split_once('=')?;
        match k {
            "host" => spec["host"] = json!(v),
            "port" => spec["port"] = json!(v.parse::<u16>().ok()?),
            "user" => spec["user"] = json!(v),
            "password" => spec["password"] = json!(v),
            "dbname" => spec["database"] = json!(v),
            _ => {}
        }
    }
    let spec: ConnectionSpec = serde_json::from_value(spec).ok()?;
    assert_eq!(spec.ssl_mode, SslMode::Disable);
    Some(spec)
}

async fn open(spec: &ConnectionSpec) -> tokio_postgres::Client {
    connect(
        spec,
        spec.password.as_deref(),
        &EgressPolicy::allow_all(),
        "queen-pg-source-live-test",
    )
    .await
    .unwrap_or_else(|e| panic!("connect: {e}"))
}

/// One test's database objects: a schema, a slot and a publication with the
/// same random name, dropped on `Drop` (on a separate thread with its own
/// runtime, so a panicking test cleans up too).
struct Env {
    spec: ConnectionSpec,
    db: tokio_postgres::Client,
    schema: String,
    name: String,
}

macro_rules! live {
    ($tag:expr) => {
        match Env::new($tag).await {
            Some(e) => e,
            None => {
                eprintln!("skipped: QUEEN_PG_TEST_DSN is not set");
                return;
            }
        }
    };
}

impl Env {
    async fn new(tag: &str) -> Option<Env> {
        let spec = dsn()?;
        let schema = format!(
            "qpgs_{tag}_{}_{:08x}",
            std::process::id(),
            rand::random::<u32>()
        );
        let db = open(&spec).await;
        db.batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        Some(Env {
            spec,
            db,
            schema,
            name: format!("src-{tag}"),
        })
    }

    /// `schema.table`.
    fn t(&self, table: &str) -> String {
        format!("{}.{table}", self.schema)
    }

    async fn exec(&self, sql: &str) {
        self.db
            .batch_execute(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    /// `None` when there is no row (no slot) or the value is NULL.
    async fn lsn(&self, sql: &str) -> Option<Lsn> {
        let r = self.db.query_opt(sql, &[&self.schema]).await.unwrap()?;
        r.get::<_, Option<String>>(0).map(|s| s.parse().unwrap())
    }

    async fn confirmed_flush(&self) -> Option<Lsn> {
        self.lsn("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1")
            .await
    }

    async fn drop_slot(&self) {
        drop_slot(&self.db, &self.schema).await;
    }

    fn doc(&self, tables: Value, extra: Value) -> ConnectorDoc {
        let mut source = json!({
            "tables": tables,
            "slot": self.schema,
            "publication": self.schema,
            "snapshot": "never",
            "lingerMs": 5,
            "heartbeatSeconds": 2,
        });
        for (k, v) in extra.as_object().cloned().unwrap_or_default() {
            source[k] = v;
        }
        ConnectorDoc::from_json(&json!({
            "kind": "source",
            "enabled": true,
            "connection": {
                "host": self.spec.host,
                "port": self.spec.port,
                "database": self.spec.database,
                "user": self.spec.user,
                "sslMode": "disable",
                "connectTimeoutMs": 5000,
            },
            "source": source,
        }))
        .unwrap()
    }

    fn source(&self, fake: &Arc<FakeQueen>, node: &str, doc: ConnectorDoc) -> Arc<Source> {
        let mut knobs = NodeKnobs::defaults(node);
        knobs.lease_ttl_ms = 3_000;
        knobs.shutdown_grace_ms = 10_000;
        let ctx = Context {
            tenant: "t1".into(),
            tenant_label: None,
            name: self.name.clone(),
            api: fake.clone(),
            knobs: Arc::new(knobs),
            metrics: Metrics::new(),
        };
        Source::new(ctx, doc, self.spec.password.clone()).unwrap()
    }

    fn pointer(&self, fake: &FakeQueen) -> Option<Value> {
        fake.kv_value(&format!("src:{}:pointer", self.name))
            .map(|(v, _)| v)
    }

    /// Every row of `table` as the events render it (simple types only:
    /// bigint, int and text columns).
    async fn rows(&self, table: &str, key: &str) -> BTreeMap<String, Value> {
        let rows = self
            .db
            .query(
                &format!("SELECT to_jsonb(t)::text FROM {} t", self.t(table)),
                &[],
            )
            .await
            .unwrap();
        rows.iter()
            .map(|r| {
                let v: Value = serde_json::from_str(&r.get::<_, String>(0)).unwrap();
                (canon(&json!({ key: v[key].clone() })), v)
            })
            .collect()
    }
}

async fn drop_slot(c: &tokio_postgres::Client, slot: &str) {
    for _ in 0..80 {
        let r = c
            .execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
                 WHERE slot_name = $1",
                &[&slot],
            )
            .await;
        if r.is_ok() {
            return;
        }
        let _ = c
            .execute(
                "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
                 WHERE slot_name = $1 AND active_pid IS NOT NULL",
                &[&slot],
            )
            .await;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("could not drop the slot {slot}");
}

impl Drop for Env {
    fn drop(&mut self) {
        let spec = self.spec.clone();
        let schema = self.schema.clone();
        let _ = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let c = open(&spec).await;
                drop_slot(&c, &schema).await;
                let _ = c
                    .batch_execute(&format!(
                        "DROP PUBLICATION IF EXISTS {schema}; DROP SCHEMA IF EXISTS {schema} CASCADE"
                    ))
                    .await;
            });
        })
        .join();
    }
}

// ---------------------------------------------------------------------------
// Running sources
// ---------------------------------------------------------------------------

struct Running {
    stop: StopHandle,
    task: tokio::task::JoinHandle<queen_pg::Result<RunEnd>>,
}

fn start(src: &Arc<Source>) -> Running {
    let (stop, s) = stop_pair();
    Running {
        stop,
        task: tokio::spawn(src.clone().run(s)),
    }
}

impl Running {
    async fn stop(self) -> queen_pg::Result<RunEnd> {
        self.stop.stop();
        tokio::time::timeout(Duration::from_secs(30), self.task)
            .await
            .expect("the source stops within 30 s")
            .expect("the source task does not panic")
    }

    /// A crash: the task is dropped where it is (a bundle in flight keeps
    /// running detached and may still commit, like a broker that applied a
    /// request whose caller died).
    fn crash(self) {
        self.task.abort();
    }
}

async fn wait_until(what: &str, timeout: Duration, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn phase(src: &Source) -> String {
    src.status()["phase"].as_str().unwrap_or("").to_string()
}

fn streaming(src: &Source) -> bool {
    matches!(phase(src).as_str(), "streaming" | "snapshot")
}

// ---------------------------------------------------------------------------
// The oracle
// ---------------------------------------------------------------------------

/// JSON with sorted object keys.
fn canon(v: &Value) -> String {
    match v {
        Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            let inner: Vec<String> = keys
                .iter()
                .map(|k| format!("{}:{}", Value::String((*k).clone()), canon(&m[*k])))
                .collect();
            format!("{{{}}}", inner.join(","))
        }
        Value::Array(a) => format!("[{}]", a.iter().map(canon).collect::<Vec<_>>().join(",")),
        other => other.to_string(),
    }
}

fn events(msgs: &[FakeMessage]) -> Vec<(FakeMessage, Value)> {
    msgs.iter()
        .map(|m| (m.clone(), serde_json::from_str(&m.payload).unwrap()))
        .collect()
}

/// Replay per partition in offset order (`messages()` is ordered by
/// partition id, then offset).
fn replay(evs: &[(FakeMessage, Value)]) -> BTreeMap<String, Value> {
    let mut state: BTreeMap<String, Map<String, Value>> = BTreeMap::new();
    for (_, e) in evs {
        let key = canon(&e["key"]);
        match e["op"].as_str().unwrap() {
            "c" | "r" => {
                state.insert(key, e["after"].as_object().unwrap().clone());
            }
            "u" => {
                let mut row = e["after"].as_object().unwrap().clone();
                if let Some(old) = state.get(&key) {
                    for c in e["unchanged"].as_array().cloned().unwrap_or_default() {
                        let c = c.as_str().unwrap();
                        if let Some(v) = old.get(c) {
                            row.insert(c.to_string(), v.clone());
                        }
                    }
                }
                state.insert(key, row);
            }
            "d" => {
                state.remove(&key);
            }
            "t" => state.clear(),
            other => panic!("unknown op {other}"),
        }
    }
    state
        .into_iter()
        .map(|(k, v)| (k, Value::Object(v)))
        .collect()
}

/// Unique transaction ids, and within each partition the events' LSN never
/// goes backwards (commit order), `seq` rising inside one source
/// transaction.
fn check_ids_and_order(evs: &[(FakeMessage, Value)]) {
    let mut ids = HashSet::new();
    for (m, _) in evs {
        assert!(
            ids.insert(m.transaction_id.clone()),
            "duplicate id {}",
            m.transaction_id
        );
    }
    let mut last: HashMap<i64, (Lsn, i64, String)> = HashMap::new();
    for (m, e) in evs {
        let lsn: Lsn = e["lsn"].as_str().unwrap().parse().unwrap();
        let seq = e["seq"].as_i64().unwrap();
        let op = e["op"].as_str().unwrap().to_string();
        if let Some((l, s, o)) = last.get(&m.partition_id) {
            assert!(
                lsn > *l || (lsn == *l && (seq > *s || op == "r" || o == "r")),
                "partition {} goes back: {l}/{s} then {lsn}/{seq} ({})",
                m.partition,
                m.payload
            );
        }
        last.insert(m.partition_id, (lsn, seq, op));
    }
}

/// What a writer committed: per unique value the key it was written to, and
/// the deletes that removed a row, per key.
#[derive(Default, Debug)]
struct Log {
    upserts: Vec<(i64, String)>,
    deletes: HashMap<i64, u32>,
    txns: u64,
}

/// Every committed write exactly once among the stream events (`c`/`u` by
/// its unique value; `d` by count per key).
fn check_log(evs: &[(FakeMessage, Value)], log: &Log) {
    let mut seen: HashMap<String, u32> = HashMap::new();
    let mut deletes: HashMap<i64, u32> = HashMap::new();
    for (_, e) in evs {
        match e["op"].as_str().unwrap() {
            "c" | "u" => {
                if let Some(v) = e["after"]["v"].as_str() {
                    *seen.entry(v.to_string()).or_default() += 1;
                }
            }
            "d" => *deletes.entry(e["key"]["id"].as_i64().unwrap()).or_default() += 1,
            _ => {}
        }
    }
    for (k, v) in &log.upserts {
        assert_eq!(
            seen.get(v).copied().unwrap_or(0),
            1,
            "the write {v} of key {k} must be in the queue exactly once"
        );
    }
    for (k, n) in &log.deletes {
        assert_eq!(
            deletes.get(k).copied().unwrap_or(0),
            *n,
            "deletes of key {k}"
        );
    }
}

/// Random transactions of 1–4 upserts/deletes over `keys`, each write a
/// unique value, until `stop` (or `max` transactions).
async fn writer(
    spec: ConnectionSpec,
    table: String,
    keys: std::ops::Range<i64>,
    seed: u64,
    max: u64,
    pace: Duration,
    stop: Arc<AtomicBool>,
) -> Log {
    let mut c = open(&spec).await;
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let mut log = Log::default();
    let mut n = 0u64;
    let upsert = format!(
        "INSERT INTO {table} (id, v, n) VALUES ($1, $2, $3) \
         ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v, n = EXCLUDED.n"
    );
    let delete = format!("DELETE FROM {table} WHERE id = $1");
    while !stop.load(Ordering::SeqCst) && log.txns < max {
        let tx = c.transaction().await.unwrap();
        let mut ups = Vec::new();
        let mut dels: Vec<i64> = Vec::new();
        for _ in 0..rng.gen_range(1..=4) {
            let k = rng.gen_range(keys.clone());
            if rng.gen_bool(0.75) {
                n += 1;
                let v = format!("w{seed}-{n}");
                let num = rng.gen_range(0..1000i32);
                tx.execute(&upsert, &[&k, &v, &num]).await.unwrap();
                ups.push((k, v));
            } else if tx.execute(&delete, &[&k]).await.unwrap() == 1 {
                dels.push(k);
            }
        }
        tx.commit().await.unwrap();
        log.txns += 1;
        log.upserts.extend(ups);
        for k in dels {
            *log.deletes.entry(k).or_default() += 1;
        }
        if !pace.is_zero() {
            tokio::time::sleep(pace).await;
        }
    }
    let _ = &mut c;
    log
}

/// Insert a sentinel row and wait until its event is in the queue and the
/// snapshot is over: everything committed before it is then in Queen.
async fn catch_up(env: &Env, fake: &FakeQueen, table: &str, queue: &str, tag: &str) {
    env.exec(&format!(
        "INSERT INTO {} (id, v, n) VALUES (-1, '{tag}', 0) \
         ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
        env.t(table)
    ))
    .await;
    wait_until(
        "the sentinel and the end of the snapshot",
        Duration::from_secs(120),
        || {
            let p = env.pointer(fake);
            let snap_done = p.as_ref().is_some_and(|p| p["snapshot"].is_null());
            snap_done
                && fake
                    .messages(queue)
                    .iter()
                    .any(|m| m.payload.contains(&format!("\"v\":\"{tag}\"")))
        },
    )
    .await;
}

fn check_all(evs: &[(FakeMessage, Value)], table: &BTreeMap<String, Value>, log: &Log) {
    check_ids_and_order(evs);
    let got = replay(evs);
    let want: BTreeMap<String, String> = table.iter().map(|(k, v)| (k.clone(), canon(v))).collect();
    let got: BTreeMap<String, String> = got.iter().map(|(k, v)| (k.clone(), canon(v))).collect();
    if got != want {
        let missing: Vec<&String> = want
            .keys()
            .filter(|k| !got.contains_key(*k))
            .take(5)
            .collect();
        let extra: Vec<&String> = got
            .keys()
            .filter(|k| !want.contains_key(*k))
            .take(5)
            .collect();
        let differ: Vec<(&String, &String, &String)> = want
            .iter()
            .filter_map(|(k, w)| got.get(k).filter(|g| *g != w).map(|g| (k, w, g)))
            .take(5)
            .collect();
        panic!(
            "replay != table ({} vs {} keys): missing {missing:?}, extra {extra:?}, differ (key, table, replay) {differ:?}",
            got.len(),
            want.len()
        );
    }
    check_log(evs, log);
}

async fn create_kv_table(env: &Env, table: &str, rows: i64) {
    env.exec(&format!(
        "CREATE TABLE {t} (id bigint PRIMARY KEY, v text NOT NULL, n int); \
         INSERT INTO {t} SELECT g, 'init-' || g, (g % 7)::int FROM generate_series(1, {rows}) g",
        t = env.t(table)
    ))
    .await;
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

/// (1) A snapshot of 10k rows while a writer inserts, updates and deletes
/// concurrently: the queue replays to the table exactly, every write once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_under_concurrent_writes_replays_to_the_table() {
    let env = live!("snap");
    create_kv_table(&env, "t", 10_000).await;
    let fake = FakeQueen::new();
    // Big chunks and four unpaced writers: a stale chunk row (a change
    // committed after the chunk's snapshot, before its high watermark) is
    // then near-certain somewhere, so a broken chunk filter fails this test
    // (checked by disabling the filter).
    let doc = env.doc(
        json!([{"table": env.t("t"), "queue": "q"}]),
        json!({"snapshot": "initial", "snapshotChunkRows": 2500, "maxBundleMessages": 500}),
    );
    let src = env.source(&fake, "node-a", doc);
    let run = start(&src);
    wait_until("the pointer", Duration::from_secs(30), || {
        env.pointer(&fake).is_some()
    })
    .await;
    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (1..=4)
        .map(|seed| {
            tokio::spawn(writer(
                env.spec.clone(),
                env.t("t"),
                1..12_001,
                seed,
                700,
                Duration::ZERO,
                stop.clone(),
            ))
        })
        .collect();
    let mut log = Log::default();
    for w in writers {
        let l = w.await.unwrap();
        log.txns += l.txns;
        log.upserts.extend(l.upserts);
        for (k, n) in l.deletes {
            *log.deletes.entry(k).or_default() += n;
        }
    }
    catch_up(&env, &fake, "t", "q", "END1").await;
    let st = src.status();
    assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);
    let evs = events(&fake.messages("q"));
    let r_events = evs.iter().filter(|(_, e)| e["op"] == "r").count();
    eprintln!(
        "snapshot: {} events ({r_events} r), {} writer txns, status {st}",
        evs.len(),
        log.txns
    );
    assert!(r_events > 1_000, "the snapshot pushed rows ({r_events})");
    check_all(&evs, &env.rows("t", "id").await, &log);
    // Confirmed to PostgreSQL: never past the pointer (I2).
    let p = env.pointer(&fake).unwrap();
    let plsn: Lsn = p["lsn"].as_str().unwrap().parse().unwrap();
    let cf = env.confirmed_flush().await.unwrap();
    assert!(cf <= plsn, "confirmed_flush {cf} > pointer {plsn}");
    assert_eq!(src.status()["phase"], "stopped");
}

/// (2) Stops and crashes at random moments, with lost answers of committed
/// bundles, under concurrent writes: nothing lost, nothing twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crashes_and_lost_answers_lose_and_repeat_nothing() {
    let env = live!("crash");
    create_kv_table(&env, "t", 3_000).await;
    let fake = FakeQueen::new();
    let doc = env.doc(
        json!([{"table": env.t("t"), "queue": "q"}]),
        json!({"snapshot": "initial", "snapshotChunkRows": 400, "maxBundleMessages": 150}),
    );
    // The slot exists before the writer starts: one short run creates it.
    let first = env.source(&fake, "node-a", doc.clone());
    let run = start(&first);
    wait_until("the pointer", Duration::from_secs(30), || {
        env.pointer(&fake).is_some()
    })
    .await;
    let stop = Arc::new(AtomicBool::new(false));
    let w = tokio::spawn(writer(
        env.spec.clone(),
        env.t("t"),
        1..4_001,
        2,
        u64::MAX,
        Duration::from_millis(2),
        stop.clone(),
    ));
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let mut run = Some(run);
    for cycle in 0..5 {
        tokio::time::sleep(Duration::from_millis(rng.gen_range(400..1_800))).await;
        let r = run.take().unwrap();
        if cycle % 2 == 0 {
            r.crash();
        } else {
            assert_eq!(r.stop().await.unwrap(), RunEnd::Stopped);
        }
        if cycle >= 1 {
            // The next bundle commits, and its answer is lost.
            fake.inject(FakeCall::Transaction, Fault::LoseAnswer);
        }
        let src = env.source(&fake, "node-a", doc.clone());
        run = Some(start(&src));
    }
    tokio::time::sleep(Duration::from_millis(800)).await;
    stop.store(true, Ordering::SeqCst);
    let log = w.await.unwrap();
    let last = env.source(&fake, "node-a", doc.clone());
    // The last life started above; this one replaces it after a crash too.
    run.take().unwrap().crash();
    let run = start(&last);
    catch_up(&env, &fake, "t", "q", "END2").await;
    assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);
    let evs = events(&fake.messages("q"));
    eprintln!(
        "crashes: {} events, {} writer txns, {} transaction calls",
        evs.len(),
        log.txns,
        fake.calls(FakeCall::Transaction)
    );
    check_all(&evs, &env.rows("t", "id").await, &log);
}

/// (3) A 50k-row transaction with maxBundleMessages 1000 is pushed in chunks
/// (`inTxn`), and a stop in the middle resumes after the chunks already in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_big_transaction_is_split_and_resumes_after_a_stop() {
    let env = live!("split");
    create_kv_table(&env, "t", 0).await;
    let fake = FakeQueen::new();
    let doc = env.doc(
        json!([{"table": env.t("t"), "queue": "q"}]),
        json!({"maxBundleMessages": 1000}),
    );
    let src = env.source(&fake, "node-a", doc.clone());
    let run = start(&src);
    wait_until("streaming", Duration::from_secs(30), || {
        phase(&src) == "streaming"
    })
    .await;
    env.exec(&format!(
        "INSERT INTO {} SELECT g, 'big-' || g, 0 FROM generate_series(1, 50000) g",
        env.t("t")
    ))
    .await;
    wait_until("the first chunks", Duration::from_secs(60), || {
        fake.messages("q").len() >= 5_000
    })
    .await;
    assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);
    let p = env.pointer(&fake).unwrap();
    let done = p["inTxn"]["done"]
        .as_u64()
        .expect("stopped inside the transaction");
    let pushed = fake.messages("q").len() as u64;
    assert_eq!(
        done, pushed,
        "every chunk in Queen is counted, no more: {p}"
    );
    assert!(pushed < 50_000, "stopped before the end ({pushed})");
    assert_eq!(done % 1_000, 0, "chunks of maxBundleMessages");
    let before: Lsn = p["lsn"].as_str().unwrap().parse().unwrap();

    let again = env.source(&fake, "node-a", doc);
    let run = start(&again);
    wait_until("all 50k rows", Duration::from_secs(120), || {
        fake.messages("q").len() >= 50_000
    })
    .await;
    wait_until(
        "the end of the transaction",
        Duration::from_secs(30),
        || env.pointer(&fake).is_some_and(|p| p["inTxn"].is_null()),
    )
    .await;
    assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);
    let evs = events(&fake.messages("q"));
    assert_eq!(evs.len(), 50_000);
    check_ids_and_order(&evs);
    let seqs: HashSet<i64> = evs
        .iter()
        .map(|(_, e)| e["seq"].as_i64().unwrap())
        .collect();
    assert_eq!(seqs.len(), 50_000);
    assert_eq!(*seqs.iter().max().unwrap(), 49_999);
    let commit: HashSet<&str> = evs
        .iter()
        .map(|(_, e)| e["lsn"].as_str().unwrap())
        .collect();
    assert_eq!(commit.len(), 1, "one source transaction");
    let p = env.pointer(&fake).unwrap();
    let after: Lsn = p["lsn"].as_str().unwrap().parse().unwrap();
    assert!(after > before);
    check_all(&evs, &env.rows("t", "id").await, &Log::default());
}

/// (4) A slot dropped behind the source's back is `slot_lost` (an error,
/// never a silent recreate); a resync recreates it and snapshots again under
/// a new epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_slot_stops_and_a_resync_snapshots_again() {
    let env = live!("resync");
    create_kv_table(&env, "t", 100).await;
    let fake = FakeQueen::new();
    let doc = env.doc(
        json!([{"table": env.t("t"), "queue": "q"}]),
        json!({"snapshot": "initial"}),
    );
    let src = env.source(&fake, "node-a", doc.clone());
    let run = start(&src);
    wait_until("the snapshot", Duration::from_secs(60), || {
        env.pointer(&fake).is_some_and(|p| p["snapshot"].is_null())
    })
    .await;
    assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);
    let epoch1 = env.pointer(&fake).unwrap()["epoch"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(fake.messages("q").len(), 100);

    env.drop_slot().await;
    let lost = env.source(&fake, "node-a", doc.clone());
    let e = tokio::time::timeout(
        Duration::from_secs(30),
        lost.clone().run(queen_pg::Stop::never()),
    )
    .await
    .expect("a fatal error ends the run")
    .unwrap_err();
    assert_eq!(e.code(), "slot_lost", "{e}");
    let st = lost.status();
    assert_eq!(st["phase"], "error");
    assert_eq!(st["error"]["code"], "slot_lost");
    assert!(st["error"]["message"].as_str().unwrap().contains("resync"));

    env.exec(&format!(
        "INSERT INTO {} VALUES (101, 'after-loss', 0)",
        env.t("t")
    ))
    .await;
    let mut doc2 = doc.clone();
    doc2.resync_requested_at = Some("2026-10-02T12:00:00.000000Z".into());
    let src = env.source(&fake, "node-a", doc2.clone());
    let run = start(&src);
    wait_until("the resync snapshot", Duration::from_secs(60), || {
        env.pointer(&fake).is_some_and(|p| {
            p["resyncedAt"] == "2026-10-02T12:00:00.000000Z" && p["snapshot"].is_null()
        })
    })
    .await;
    assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);
    let p = env.pointer(&fake).unwrap();
    let epoch2 = p["epoch"].as_str().unwrap();
    assert_ne!(epoch2, epoch1, "a new epoch");
    let evs = events(&fake.messages("q"));
    let resnap = evs
        .iter()
        .filter(|(m, _)| m.transaction_id.starts_with(&format!("pg:{epoch2}:s:")))
        .count();
    assert_eq!(
        resnap, 101,
        "every row again, the one written while lost included"
    );
    check_all(&evs, &env.rows("t", "id").await, &Log::default());

    // Once done, the same request is not carried out again.
    let src = env.source(&fake, "node-a", doc2);
    let run = start(&src);
    wait_until("streaming", Duration::from_secs(30), || {
        phase(&src) == "streaming"
    })
    .await;
    assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);
    assert_eq!(env.pointer(&fake).unwrap()["epoch"], epoch2);
}

/// (5) A busy table outside the publication while the published one is
/// idle: keepalives (and heartbeats) still advance the pointer and the
/// slot, so the database does not keep WAL forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_published_table_still_advances_the_slot() {
    let env = live!("idle");
    create_kv_table(&env, "quiet", 1).await;
    create_kv_table(&env, "busy", 1).await;
    let fake = FakeQueen::new();
    let doc = env.doc(json!([{"table": env.t("quiet"), "queue": "q"}]), json!({}));
    let src = env.source(&fake, "node-a", doc);
    let run = start(&src);
    wait_until("streaming", Duration::from_secs(30), || {
        phase(&src) == "streaming"
    })
    .await;
    let p0: Lsn = env.pointer(&fake).unwrap()["lsn"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let c0 = env.confirmed_flush().await.unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let busy = {
        let (spec, table, stop) = (env.spec.clone(), env.t("busy"), stop.clone());
        tokio::spawn(async move {
            let c = open(&spec).await;
            let mut i = 0i64;
            while !stop.load(Ordering::SeqCst) {
                i += 1;
                c.execute(
                    &format!("UPDATE {table} SET n = $1 WHERE id = 1"),
                    &[&((i % 1000) as i32)],
                )
                .await
                .unwrap();
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };
    let deadline = Instant::now() + Duration::from_secs(40);
    let mut advanced = None;
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let c = env.confirmed_flush().await.unwrap();
        let p: Lsn = env.pointer(&fake).unwrap()["lsn"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        assert!(c <= p, "confirmed {c} beyond the pointer {p}");
        if c > c0 && p > p0 {
            advanced = Some((p, c));
            break;
        }
    }
    stop.store(true, Ordering::SeqCst);
    busy.await.unwrap();
    let (p, c) = advanced.expect("the slot advanced while the published table was idle");
    eprintln!("idle: pointer {p0} -> {p}, confirmed {c0} -> {c}");
    assert!(
        fake.messages("q").is_empty(),
        "nothing of the busy table is pushed"
    );
    assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);
}

/// (6) An update of a row whose big column is TOASTed out of line: the
/// event lists it in `unchanged` and leaves it out of `after`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unchanged_toast_column_is_listed_not_sent() {
    let env = live!("toast");
    env.exec(&format!(
        "CREATE TABLE {t} (id bigint PRIMARY KEY, n int, big text); \
         ALTER TABLE {t} ALTER COLUMN big SET STORAGE EXTERNAL",
        t = env.t("t")
    ))
    .await;
    let fake = FakeQueen::new();
    let doc = env.doc(json!([{"table": env.t("t"), "queue": "q"}]), json!({}));
    let src = env.source(&fake, "node-a", doc);
    let run = start(&src);
    wait_until("streaming", Duration::from_secs(30), || {
        phase(&src) == "streaming"
    })
    .await;
    env.exec(&format!(
        "INSERT INTO {t} VALUES (1, 0, repeat('x', 100000)); UPDATE {t} SET n = 1 WHERE id = 1",
        t = env.t("t")
    ))
    .await;
    wait_until("two events", Duration::from_secs(30), || {
        fake.messages("q").len() >= 2
    })
    .await;
    assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);
    let evs = events(&fake.messages("q"));
    let (c, u) = (&evs[0].1, &evs[1].1);
    assert_eq!(c["op"], "c");
    assert_eq!(c["after"]["big"].as_str().unwrap().len(), 100_000);
    assert_eq!(u["op"], "u");
    assert_eq!(u["unchanged"], json!(["big"]));
    assert!(u["after"].get("big").is_none(), "{u}");
    assert_eq!(u["after"]["n"], 1);
    assert_eq!(u["key"], json!({"id": 1}));
}

/// (7) TRUNCATE with `onTruncate: skip`: counted, not emitted; the rows
/// after it still come through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_truncate_is_skipped_and_counted() {
    let env = live!("trunc");
    create_kv_table(&env, "t", 0).await;
    let fake = FakeQueen::new();
    let doc = env.doc(json!([{"table": env.t("t"), "queue": "q"}]), json!({}));
    let src = env.source(&fake, "node-a", doc);
    let run = start(&src);
    wait_until("streaming", Duration::from_secs(30), || {
        phase(&src) == "streaming"
    })
    .await;
    env.exec(&format!(
        "INSERT INTO {t} VALUES (1,'a',0),(2,'b',0),(3,'c',0); TRUNCATE {t}; \
         INSERT INTO {t} VALUES (4,'d',0),(5,'e',0)",
        t = env.t("t")
    ))
    .await;
    wait_until("five inserts", Duration::from_secs(30), || {
        fake.messages("q").len() >= 5
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let st = src.status();
    assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);
    let evs = events(&fake.messages("q"));
    assert_eq!(evs.len(), 5);
    assert!(evs.iter().all(|(_, e)| e["op"] == "c"));
    assert_eq!(st["truncatesSkipped"], 1, "{st}");
}

/// (8) Two nodes run the same source on one broker: only the lease holder
/// streams; stopping it hands the source over within about a TTL, and the
/// queue holds every write exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_one_streams_and_a_stop_hands_over() {
    let env = live!("nodes");
    create_kv_table(&env, "t", 2_000).await;
    let fake = FakeQueen::new();
    let doc = env.doc(
        json!([{"table": env.t("t"), "queue": "q"}]),
        json!({"snapshot": "initial", "snapshotChunkRows": 300, "maxBundleMessages": 100}),
    );
    let a = env.source(&fake, "node-a", doc.clone());
    let b = env.source(&fake, "node-b", doc.clone());
    let run_a = start(&a);
    wait_until("a streams", Duration::from_secs(30), || streaming(&a)).await;
    let run_b = start(&b);
    let stop = Arc::new(AtomicBool::new(false));
    let w = tokio::spawn(writer(
        env.spec.clone(),
        env.t("t"),
        1..2_501,
        8,
        u64::MAX,
        Duration::from_millis(2),
        stop.clone(),
    ));
    let seen = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        seen.lock().unwrap().push((phase(&a), phase(&b)));
        assert!(
            !(streaming(&a) && streaming(&b)),
            "both nodes stream: {:?}",
            seen.lock().unwrap()
        );
    }
    assert_eq!(phase(&b), "standby");
    assert_eq!(b.status()["owner"], "node-a");
    let handed = Instant::now();
    assert_eq!(run_a.stop().await.unwrap(), RunEnd::Stopped);
    wait_until("b takes over", Duration::from_secs(15), || streaming(&b)).await;
    let took = handed.elapsed();
    eprintln!("hand-over in {took:?}");
    assert!(
        took < Duration::from_secs(8),
        "within about a TTL: {took:?}"
    );
    tokio::time::sleep(Duration::from_millis(800)).await;
    stop.store(true, Ordering::SeqCst);
    let log = w.await.unwrap();
    catch_up(&env, &fake, "t", "q", "END8").await;
    assert_eq!(run_b.stop().await.unwrap(), RunEnd::Stopped);
    let evs = events(&fake.messages("q"));
    check_all(&evs, &env.rows("t", "id").await, &log);
}

/// (9) `DELETE …` on a source: the owner deletes its pointer (and with
/// `dropSlot`, the slot and the managed publication) and the run ends
/// `TornDown` — also when the document is disabled: a disabled source must
/// not pin WAL forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_tears_down_even_a_disabled_source() {
    let env = live!("teardown");
    create_kv_table(&env, "t", 10).await;
    let fake = FakeQueen::new();
    let doc = env.doc(
        json!([{"table": env.t("t"), "queue": "q"}]),
        json!({"snapshot": "initial"}),
    );
    let src = env.source(&fake, "node-a", doc.clone());
    let run = start(&src);
    wait_until("the snapshot", Duration::from_secs(60), || {
        env.pointer(&fake).is_some_and(|p| p["snapshot"].is_null())
    })
    .await;
    assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);
    let exists = |sql: &'static str| {
        let (db, name) = (&env.db, env.schema.clone());
        async move { db.query_one(sql, &[&name]).await.unwrap().get::<_, i64>(0) == 1 }
    };
    let slot = "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1";
    let publication = "SELECT count(*) FROM pg_publication WHERE pubname = $1";
    assert!(exists(slot).await && exists(publication).await);

    // Without dropSlot: the runtime state goes, the slot stays.
    let mut keep = doc.clone();
    keep.deleting = Some(DeleteRequest {
        drop_slot: false,
        requested_at: "2026-10-02T13:00:00.000000Z".into(),
    });
    let src = env.source(&fake, "node-a", keep);
    let end = tokio::time::timeout(
        Duration::from_secs(30),
        src.clone().run(queen_pg::Stop::never()),
    )
    .await
    .expect("a teardown ends the run")
    .unwrap();
    assert_eq!(end, RunEnd::TornDown);
    assert!(env.pointer(&fake).is_none(), "the pointer is deleted");
    assert!(
        fake.kv_value(&format!("src:{}:lease", env.name)).is_none(),
        "and the lease"
    );
    assert!(exists(slot).await, "the slot is kept without dropSlot");

    // Disabled AND deleting with dropSlot: torn down all the same.
    let mut gone = doc;
    gone.enabled = false;
    gone.deleting = Some(DeleteRequest {
        drop_slot: true,
        requested_at: "2026-10-02T13:01:00.000000Z".into(),
    });
    let src = env.source(&fake, "node-a", gone);
    assert_eq!(
        src.status()["phase"],
        "connecting",
        "not parked as disabled"
    );
    let end = tokio::time::timeout(
        Duration::from_secs(30),
        src.clone().run(queen_pg::Stop::never()),
    )
    .await
    .expect("a teardown ends the run")
    .unwrap();
    assert_eq!(end, RunEnd::TornDown);
    assert!(!exists(slot).await, "the slot is dropped");
    assert!(
        !exists(publication).await,
        "the managed publication is dropped"
    );
    assert!(fake.kv_value(&format!("src:{}:lease", env.name)).is_none());
    assert_eq!(src.status()["tornDown"], true);
}

async fn run_to_error(src: &Arc<Source>) -> queen_pg::Error {
    tokio::time::timeout(
        Duration::from_secs(30),
        src.clone().run(queen_pg::Stop::never()),
    )
    .await
    .expect("a fatal error ends the run")
    .expect_err("the run must fail")
}

/// (10) PLAN §4.3 step 5, the slot cases: a slot with no pointer is ADOPTED
/// (after waiting while another consumer holds it); a pointer from another
/// system is `system_changed`; a slot consumed past the pointer is
/// `slot_ahead`. Each error names the fix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slot_is_waited_for_and_adopted_and_foreign_states_are_refused() {
    let env = live!("adopt");
    create_kv_table(&env, "t", 0).await;
    let slot = env.schema.clone();
    // Two statements, two transactions: a slot cannot be created in a
    // transaction that has written.
    env.exec(&format!(
        "CREATE PUBLICATION {slot} FOR TABLE {}",
        env.t("t")
    ))
    .await;
    env.exec(&format!(
        "SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput', false, false, true)"
    ))
    .await;
    let created = env.confirmed_flush().await.unwrap();
    // Another consumer streams from the slot.
    let other = queen_pg::repl::ReplicationClient::connect(
        &env.spec,
        env.spec.password.as_deref(),
        &EgressPolicy::allow_all(),
        "another-consumer",
    )
    .await
    .unwrap()
    .start_logical(&slot, Lsn(0), &slot)
    .await
    .unwrap();
    // Committed after the slot's consistent point, never confirmed by the
    // other consumer: an adoption at the confirmed position streams it.
    env.exec(&format!(
        "INSERT INTO {} VALUES (1, 'before-adoption', 0)",
        env.t("t")
    ))
    .await;
    let fake = FakeQueen::new();
    let doc = env.doc(json!([{"table": env.t("t"), "queue": "q"}]), json!({}));
    let src = env.source(&fake, "node-a", doc.clone());
    let run = start(&src);
    wait_until("waiting_for_slot", Duration::from_secs(30), || {
        phase(&src) == "waiting_for_slot"
    })
    .await;
    assert!(
        env.pointer(&fake).is_none(),
        "nothing written while waiting"
    );
    other.close(Duration::from_secs(5)).await;
    wait_until("streaming", Duration::from_secs(30), || {
        phase(&src) == "streaming"
    })
    .await;
    let p = env.pointer(&fake).unwrap();
    assert!(p["lsn"].as_str().unwrap().parse::<Lsn>().unwrap() >= created);
    env.exec(&format!("INSERT INTO {} VALUES (2, 'x', 0)", env.t("t")))
        .await;
    wait_until("both inserts", Duration::from_secs(30), || {
        fake.messages("q").len() == 2
    })
    .await;
    assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);

    // The pointer says another system: refused, nothing streamed.
    use queen_pg::queen::{KvOp, QueenApi};
    let key = format!("src:{}:pointer", env.name);
    let (good, v) = fake.kv_value(&key).unwrap();
    let mut alien = good.clone();
    alien["systemId"] = json!("1");
    let a = fake
        .kv(vec![KvOp::fence(key.clone(), alien, v)])
        .await
        .unwrap();
    assert!(a.ok);
    let e = run_to_error(&env.source(&fake, "node-a", doc.clone())).await;
    assert_eq!(e.code(), "system_changed", "{e}");
    assert!(e.to_string().contains("resync"), "{e}");
    let (_, v) = fake.kv_value(&key).unwrap();
    let a = fake
        .kv(vec![KvOp::fence(key.clone(), good, v)])
        .await
        .unwrap();
    assert!(a.ok);

    // Someone consumed the slot past the pointer: the changes between are
    // not in Queen. Refused, never skipped.
    env.exec(&format!("INSERT INTO {} VALUES (3, 'y', 0)", env.t("t")))
        .await;
    env.exec(&format!(
        "SELECT pg_replication_slot_advance('{slot}', pg_current_wal_lsn())"
    ))
    .await;
    let src = env.source(&fake, "node-a", doc);
    let e = run_to_error(&src).await;
    assert_eq!(e.code(), "slot_ahead", "{e}");
    assert_eq!(src.status()["error"]["code"], "slot_ahead");
    let evs = events(&fake.messages("q"));
    assert_eq!(evs.len(), 2, "nothing streamed by the refused starts");
    assert_eq!(evs[0].1["after"]["v"], "before-adoption");
}

/// (11) Tables the source cannot stream are refused with the fix in the
/// message — and BEFORE the publication is touched (a table without a usable
/// replica identity in a publication breaks the application's own UPDATEs).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unusable_tables_are_refused_before_the_publication_changes() {
    let env = live!("refuse");
    env.exec(&format!(
        "CREATE TABLE {nopk} (id int, v text); \
         CREATE TABLE {nothing} (id int PRIMARY KEY, v text); \
         ALTER TABLE {nothing} REPLICA IDENTITY NOTHING; \
         CREATE TABLE {ok} (id int PRIMARY KEY, v text)",
        nopk = env.t("nopk"),
        nothing = env.t("nothing"),
        ok = env.t("ok"),
    ))
    .await;
    let fake = FakeQueen::new();
    let cases = [
        (
            json!([{"table": env.t("nopk"), "queue": "q"}]),
            "replica_identity",
        ),
        (
            json!([{"table": env.t("nothing"), "queue": "q"}]),
            "replica_identity",
        ),
        (
            json!([{"table": env.t("ok"), "queue": "q", "partitionBy": ["v"]}]),
            "partition_by",
        ),
        (
            json!([{"table": env.t("missing"), "queue": "q"}]),
            "table_missing",
        ),
    ];
    for (tables, code) in cases {
        let src = env.source(&fake, "node-a", env.doc(tables.clone(), json!({})));
        let e = run_to_error(&src).await;
        assert_eq!(e.code(), code, "{tables}: {e}");
        assert_eq!(src.status()["phase"], "error");
        let pubs: i64 = env
            .db
            .query_one(
                "SELECT count(*) FROM pg_publication WHERE pubname = $1",
                &[&env.schema],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(pubs, 0, "{code}: the publication was not created");
    }
    // The application can still update the table without a key.
    env.exec(&format!(
        "INSERT INTO {t} VALUES (1, 'a'); UPDATE {t} SET v = 'b'",
        t = env.t("nopk")
    ))
    .await;
    assert!(env.pointer(&fake).is_none());
}

/// (12) An unmanaged publication that narrows a table — a row filter or a
/// column list — is refused before any slot exists: the stream would leave
/// out what the snapshot reads. A managed publication found in that state is
/// reset to the whole table.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_narrowed_publication_is_refused_unmanaged_and_reset_managed() {
    let env = live!("pubflt");
    create_kv_table(&env, "t", 5).await;
    let fake = FakeQueen::new();
    let p = env.schema.clone();
    let t = env.t("t");
    env.exec(&format!(
        "CREATE PUBLICATION {p} FOR TABLE {t} WHERE (id > 2)"
    ))
    .await;
    let tables = json!([{"table": t, "queue": "q"}]);
    let unmanaged = env.doc(
        tables.clone(),
        json!({"managePublication": false, "snapshot": "initial"}),
    );
    let e = run_to_error(&env.source(&fake, "node-a", unmanaged.clone())).await;
    assert_eq!(e.code(), "publication", "{e}");
    assert!(
        e.to_string().contains("row filter") && e.to_string().contains(&t),
        "{e}"
    );
    env.exec(&format!("ALTER PUBLICATION {p} SET TABLE {t} (id, v)"))
        .await;
    let e = run_to_error(&env.source(&fake, "node-a", unmanaged)).await;
    assert_eq!(e.code(), "publication", "{e}");
    assert!(e.to_string().contains("column list"), "{e}");
    assert!(env.confirmed_flush().await.is_none(), "no slot was created");
    assert!(env.pointer(&fake).is_none());

    let managed = env.doc(tables, json!({"snapshot": "initial"}));
    let src = env.source(&fake, "node-a", managed);
    let run = start(&src);
    wait_until("the snapshot", Duration::from_secs(60), || {
        env.pointer(&fake).is_some_and(|p| p["snapshot"].is_null())
    })
    .await;
    assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);
    let r = env
        .db
        .query_one(
            "SELECT rowfilter IS NULL, array_length(attnames, 1) \
               FROM pg_publication_tables WHERE pubname = $1",
            &[&p],
        )
        .await
        .unwrap();
    assert!(r.get::<_, bool>(0), "the row filter is gone");
    assert_eq!(r.get::<_, i32>(1), 3, "every column is published again");
    check_all(
        &events(&fake.messages("q")),
        &env.rows("t", "id").await,
        &Log::default(),
    );
}

async fn create_toast_table(env: &Env, table: &str, rows: i64) {
    // STORAGE EXTERNAL: every value over ~2 kB is stored out of line,
    // uncompressed, so an update that does not touch it sends it unchanged.
    env.exec(&format!(
        "CREATE TABLE {t} (id bigint PRIMARY KEY, n int NOT NULL, big text); \
         ALTER TABLE {t} ALTER COLUMN big SET STORAGE EXTERNAL; \
         INSERT INTO {t} SELECT g, 0, repeat(md5(g::text), 625 + (g % 625)::int) \
           FROM generate_series(1, {rows}) g",
        t = env.t(table)
    ))
    .await;
}

/// Wait until `pred` holds for some event of `queue` (and the snapshot is
/// over), after inserting a sentinel row with `big = tag`.
async fn catch_up_toast(env: &Env, fake: &FakeQueen, table: &str, tag: &str) {
    env.exec(&format!(
        "INSERT INTO {} VALUES (-1, 0, '{tag}') ON CONFLICT (id) DO UPDATE SET big = EXCLUDED.big",
        env.t(table)
    ))
    .await;
    wait_until("the sentinel", Duration::from_secs(120), || {
        env.pointer(fake).is_some_and(|p| p["snapshot"].is_null())
            && fake
                .messages("q")
                .iter()
                .any(|m| m.payload.contains(&format!("\"big\":\"{tag}\"")))
    })
    .await;
}

/// (13) The window-drop + unchanged-TOAST loss (E's scenario (i)): rows
/// with 20–40 kB out-of-line values, read in 500-row chunks while writers
/// update a NON-TOAST column of them. A dropped chunk row whose window
/// events all left `big` unchanged gets a fill; the queue replays to the
/// table, `big` included.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn toast_rows_changed_during_the_snapshot_keep_their_toast() {
    let env = live!("toastsnap");
    create_toast_table(&env, "t", 1_500).await;
    let fake = FakeQueen::new();
    let doc = env.doc(
        json!([{"table": env.t("t"), "queue": "q"}]),
        json!({"snapshot": "initial", "snapshotChunkRows": 500, "maxBundleMessages": 200}),
    );
    let src = env.source(&fake, "node-a", doc);
    let run = start(&src);
    wait_until("the pointer", Duration::from_secs(30), || {
        env.pointer(&fake).is_some()
    })
    .await;
    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..2)
        .map(|w| {
            let (spec, table, stop) = (env.spec.clone(), env.t("t"), stop.clone());
            tokio::spawn(async move {
                let c = open(&spec).await;
                let mut rng = rand::rngs::StdRng::seed_from_u64(1300 + w);
                let sql = format!("UPDATE {table} SET n = n + 1 WHERE id = $1");
                let mut done = 0u64;
                while !stop.load(Ordering::SeqCst) {
                    let id: i64 = rng.gen_range(1..=1_500);
                    c.execute(&sql, &[&id]).await.unwrap();
                    done += 1;
                }
                done
            })
        })
        .collect();
    wait_until("the end of the snapshot", Duration::from_secs(120), || {
        env.pointer(&fake).is_some_and(|p| p["snapshot"].is_null())
    })
    .await;
    stop.store(true, Ordering::SeqCst);
    let mut updates = 0;
    for w in writers {
        updates += w.await.unwrap();
    }
    catch_up_toast(&env, &fake, "t", "END13").await;
    assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);
    let evs = events(&fake.messages("q"));
    let fills = evs
        .iter()
        .filter(|(m, _)| m.transaction_id.contains(":f"))
        .count();
    let r = evs.iter().filter(|(_, e)| e["op"] == "r").count();
    eprintln!(
        "toast snapshot: {} events ({r} r, {fills} fills), {updates} updates",
        evs.len()
    );
    assert!(fills > 0, "the window dropped TOAST rows and filled them");
    for (m, e) in evs.iter().filter(|(m, _)| m.transaction_id.contains(":f")) {
        assert_eq!(e["op"], "u", "{}", m.payload);
        assert!(e["after"]["big"].is_string(), "a fill carries big");
        assert_eq!(e["unchanged"], json!(["n"]));
        assert!(e.get("xid").is_none());
    }
    check_ids_and_order(&evs);
    let table = env.rows("t", "id").await;
    let got = replay(&evs);
    let lost: Vec<&String> = table
        .iter()
        .filter(|(k, v)| got.get(*k).map(canon) != Some(canon(v)))
        .map(|(k, _)| k)
        .take(10)
        .collect();
    assert!(lost.is_empty(), "rows that differ (big lost?): {lost:?}");
    assert_eq!(got.len(), table.len());
}

/// (14) A key change of a row whose big column is TOASTed (no FULL): the
/// `c` for the new key carries `big`, read back for the new key; when the
/// row is already gone again the column stays `unchanged` and the delete
/// that follows removes the row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_move_keeps_its_toast_column() {
    let env = live!("keymove");
    create_toast_table(&env, "t", 0).await;
    let fake = FakeQueen::new();
    let doc = env.doc(json!([{"table": env.t("t"), "queue": "q"}]), json!({}));
    let src = env.source(&fake, "node-a", doc);
    let run = start(&src);
    wait_until("streaming", Duration::from_secs(30), || {
        phase(&src) == "streaming"
    })
    .await;
    let t = env.t("t");
    env.exec(&format!(
        "INSERT INTO {t} VALUES (2, 0, repeat('y', 30000)), (4, 0, repeat('z', 30000)), \
                                (6, 0, repeat('w', 30000))"
    ))
    .await;
    env.exec(&format!("UPDATE {t} SET id = 3 WHERE id = 2"))
        .await;
    env.exec(&format!(
        "BEGIN; UPDATE {t} SET id = 5 WHERE id = 4; DELETE FROM {t} WHERE id = 5; COMMIT"
    ))
    .await;
    env.exec(&format!("UPDATE {t} SET id = 7, n = 1 WHERE id = 6"))
        .await;
    catch_up_toast(&env, &fake, "t", "END14").await;
    assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);
    let evs = events(&fake.messages("q"));
    let c_of = |id: i64| {
        evs.iter()
            .find(|(_, e)| e["op"] == "c" && e["key"]["id"] == id)
            .map(|(_, e)| e.clone())
            .unwrap_or_else(|| panic!("no c for {id}"))
    };
    let c3 = c_of(3);
    assert_eq!(c3["after"]["big"].as_str().unwrap().len(), 30_000, "{c3}");
    assert!(c3.get("unchanged").is_none(), "{c3}");
    let c7 = c_of(7);
    assert_eq!(c7["after"]["big"].as_str().unwrap().len(), 30_000);
    assert_eq!(c7["after"]["n"], 1);
    let c5 = c_of(5);
    assert_eq!(
        c5["unchanged"],
        json!(["big"]),
        "row 5 was gone when read back"
    );
    check_ids_and_order(&evs);
    let table = env.rows("t", "id").await;
    let got = replay(&evs);
    let want: BTreeMap<String, String> = table.iter().map(|(k, v)| (k.clone(), canon(v))).collect();
    let got: BTreeMap<String, String> = got.iter().map(|(k, v)| (k.clone(), canon(v))).collect();
    assert_eq!(got, want, "the replay keeps big through the key moves");
}
