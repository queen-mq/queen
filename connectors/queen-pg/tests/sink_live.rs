//! Live tests of the sink (PLAN §5): a real PostgreSQL, the in-memory broker
//! double ([`FakeQueen`]) as the queue.
//!
//! Run only when `QUEEN_PG_TEST_DSN` is set (libpq key=value form, e.g.
//! `host=127.0.0.1 port=55432 user=queen_test password=queen_test_pw
//! dbname=queen_test`); otherwise every test prints a skip note and passes.
//! The role must be able to create a schema. Each test works in its own
//! schema (random suffix) — target tables AND its progress table — dropped at
//! the end, so the tests run in parallel with each other and with the other
//! suites sharing the database.
//!
//! The exactly-once tests share one oracle: a NON-idempotent effect (sql mode,
//! `n = n + 1`, `balance = balance + amount`), so a message applied twice — or
//! dropped — shows in the table, and at every quiescent point the table must
//! equal exactly what the progress rows say was applied.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use queen_pg::config::{ConnectionSpec, ConnectorDoc, NodeKnobs, SslMode};
use queen_pg::fake::{FakeCall, FakeQueen, Fault};
use queen_pg::pg::connect::{connect, EgressPolicy};
use queen_pg::queen::QueenError;
use queen_pg::sink::Sink;
use queen_pg::{stop_pair, Context, Metrics, RunEnd, StopHandle};
use serde_json::{json, Value};
use tokio::task::JoinHandle;
use tokio_postgres::Client;

const TENANT: &str = "tenant-live";

fn dsn() -> Option<ConnectionSpec> {
    let dsn = std::env::var("QUEEN_PG_TEST_DSN").ok()?;
    let mut spec = ConnectionSpec {
        url: None,
        host: "127.0.0.1".into(),
        port: 5432,
        database: String::new(),
        user: String::new(),
        password: None,
        password_sealed: None,
        ssl_mode: SslMode::Disable,
        ssl_root_cert: None,
        connect_timeout_ms: 5_000,
    };
    for kv in dsn.split_whitespace() {
        let (k, v) = kv.split_once('=')?;
        match k {
            "host" => spec.host = v.into(),
            "port" => spec.port = v.parse().ok()?,
            "user" => spec.user = v.into(),
            "password" => spec.password = Some(v.into()),
            "dbname" => spec.database = v.into(),
            _ => {}
        }
    }
    Some(spec)
}

macro_rules! live {
    () => {
        match dsn() {
            Some(d) => d,
            None => {
                eprintln!("skipped: QUEEN_PG_TEST_DSN is not set");
                return;
            }
        }
    };
}

/// Poll `$cond` (an expression that may `.await`) every 50 ms for up to
/// `$secs` seconds.
macro_rules! wait_until {
    ($what:expr, $secs:expr, $cond:expr) => {{
        let deadline = Instant::now() + Duration::from_secs($secs);
        loop {
            if $cond {
                break;
            }
            assert!(Instant::now() < deadline, "timed out waiting for {}", $what);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }};
}

/// The test's own connection and its schema.
struct Db {
    spec: ConnectionSpec,
    schema: String,
    c: Client,
}

impl Db {
    async fn new(spec: ConnectionSpec) -> Db {
        let c = connect(
            &spec,
            spec.password.as_deref(),
            &EgressPolicy::allow_all(),
            "sink_live",
        )
        .await
        .expect("the test connects");
        let schema = format!("k_{:08x}", rand::random::<u32>());
        c.batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        Db { spec, schema, c }
    }

    async fn drop_schema(self) {
        self.c
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await
            .unwrap();
    }

    /// `schema.name` as the sink document names it.
    fn t(&self, name: &str) -> String {
        format!("{}.{}", self.schema, name)
    }

    /// `"schema"."name"` for SQL.
    fn q(&self, name: &str) -> String {
        format!("\"{}\".\"{}\"", self.schema, name)
    }

    async fn exec(&self, sql: &str) {
        self.c
            .batch_execute(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    async fn i64(&self, sql: &str) -> i64 {
        self.c
            .query_one(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .get(0)
    }

    async fn text(&self, sql: &str) -> Option<String> {
        self.c
            .query_one(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .get(0)
    }

    /// partition_id → last_offset of sink `name`.
    async fn progress(&self, name: &str) -> BTreeMap<i64, i64> {
        self.c
            .query(
                &format!(
                    "SELECT partition_id, last_offset FROM {} WHERE sink = $1",
                    self.q("sink_progress")
                ),
                &[&format!("{TENANT}/{name}")],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect()
    }

    /// WORKER sessions of sink `name` (not the setup connection) waiting on
    /// a lock.
    async fn waiting(&self, name: &str) -> i64 {
        self.c
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE $1 \
                 AND application_name NOT LIKE '%:setup' AND wait_event_type = 'Lock'",
                &[&format!("queen-pg/% sink:{name}:%")],
            )
            .await
            .unwrap()
            .get(0)
    }

    /// Worker sessions of sink `name` connected and idle (statements
    /// prepared, polling the queue).
    async fn idle_workers(&self, name: &str) -> i64 {
        self.c
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE $1 \
                 AND application_name NOT LIKE '%:setup' AND state = 'idle'",
                &[&format!("queen-pg/% sink:{name}:%")],
            )
            .await
            .unwrap()
            .get(0)
    }

    /// A connection holding every row of `table` FOR UPDATE: a sink's
    /// statements on those rows wait, mid-batch. (Row locks, not LOCK
    /// TABLE: preparing an UPDATE takes RowExclusiveLock while it parses,
    /// so a table lock would stall the sink's setup instead of its batch.)
    async fn row_blocker(&self, table: &str) -> Client {
        let c = connect(
            &self.spec,
            self.spec.password.as_deref(),
            &EgressPolicy::allow_all(),
            "sink_live blocker",
        )
        .await
        .unwrap();
        c.batch_execute(&format!("BEGIN; SELECT * FROM {table} FOR UPDATE"))
            .await
            .unwrap();
        c
    }

    /// Terminate sessions of sink `name` (`only_waiting`: the workers
    /// waiting on a lock); how many.
    async fn terminate(&self, name: &str, only_waiting: bool) -> i64 {
        self.c
            .query_one(
                "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity \
                 WHERE application_name LIKE $1 AND (NOT $2 OR \
                 (wait_event_type = 'Lock' AND application_name NOT LIKE '%:setup'))",
                &[&format!("queen-pg/% sink:{name}:%"), &only_waiting],
            )
            .await
            .unwrap()
            .get(0)
    }
}

fn sink_doc(db: &Db, sink: Value) -> ConnectorDoc {
    let mut sink = sink;
    sink["progressTable"] = json!(db.t("sink_progress"));
    ConnectorDoc::from_json(&json!({
        "kind": "sink",
        "connection": {
            "host": db.spec.host, "port": db.spec.port, "database": db.spec.database,
            "user": db.spec.user, "sslMode": "disable", "connectTimeoutMs": 5000,
        },
        "sink": sink,
    }))
    .expect("a valid sink document")
}

fn context(name: &str, fake: &Arc<FakeQueen>, node: &str) -> Context {
    Context {
        tenant: TENANT.into(),
        tenant_label: None,
        name: name.into(),
        api: fake.clone(),
        knobs: Arc::new(NodeKnobs::defaults(node)),
        metrics: Metrics::new(),
    }
}

fn new_sink(db: &Db, name: &str, fake: &Arc<FakeQueen>, node: &str, sink: Value) -> Arc<Sink> {
    Sink::new(
        context(name, fake, node),
        sink_doc(db, sink),
        db.spec.password.clone(),
    )
    .expect("the sink builds")
}

/// A running sink.
struct Running {
    handle: StopHandle,
    task: JoinHandle<queen_pg::Result<RunEnd>>,
}

fn start(sink: &Arc<Sink>) -> Running {
    let (handle, stop) = stop_pair();
    Running {
        handle,
        task: tokio::spawn(Arc::clone(sink).run(stop)),
    }
}

impl Running {
    async fn stop(self) {
        self.handle.stop();
        let end = tokio::time::timeout(Duration::from_secs(60), self.task)
            .await
            .expect("the sink stops within a minute")
            .expect("the sink task does not panic");
        assert_eq!(end.expect("the sink stops cleanly"), RunEnd::Stopped);
    }
}

/// Every partition's cursor of `group` at its last offset.
fn drained(fake: &FakeQueen, queue: &str, group: &str) -> bool {
    let mut last: BTreeMap<String, i64> = BTreeMap::new();
    for m in fake.messages(queue) {
        last.insert(m.partition.clone(), m.offset);
    }
    last.iter()
        .all(|(p, off)| fake.cursor(queue, group, p) == Some(*off))
}

/// partition_id → highest offset in the queue.
fn max_offsets(fake: &FakeQueen, queue: &str) -> BTreeMap<i64, i64> {
    let mut m = BTreeMap::new();
    for msg in fake.messages(queue) {
        m.insert(msg.partition_id, msg.offset);
    }
    m
}

/// Wait until `group` has acked every partition of `queue` to its end.
///
/// FakeQueen's clock moves only when told, so the leases a stopped node, a
/// refused ack or an abandoned batch left behind must be run out by the test
/// — but only once NOTHING moved for a second. A batch in flight keeps its
/// lease (its keeper extends it in real time), as against the broker; a test
/// that jumps the clock on every poll expires leases under live batches, and
/// a batch slower than the poll (a poison message's isolation) then never
/// gets its ack in.
async fn drain(fake: &FakeQueen, queue: &str, group: &str) {
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut seen = positions(fake, queue, group);
    let mut still = Instant::now();
    while !drained(fake, queue, group) {
        assert!(Instant::now() < deadline, "{queue} never drained");
        tokio::time::sleep(Duration::from_millis(100)).await;
        let now = positions(fake, queue, group);
        if now != seen {
            seen = now;
            still = Instant::now();
        } else if still.elapsed() >= Duration::from_secs(1) {
            fake.advance(Duration::from_secs(6));
            still = Instant::now();
        }
    }
}

/// Every partition's cursor of `group`, and the DLQ's length.
fn positions(fake: &FakeQueen, queue: &str, group: &str) -> (Vec<Option<i64>>, usize) {
    let parts: std::collections::BTreeSet<String> = fake
        .messages(queue)
        .into_iter()
        .map(|m| m.partition)
        .collect();
    (
        parts.iter().map(|p| fake.cursor(queue, group, p)).collect(),
        fake.dlq(queue).len(),
    )
}

fn status_u64(s: &Value, k: &str) -> u64 {
    s[k].as_u64()
        .unwrap_or_else(|| panic!("status has no {k}: {s}"))
}

// ---------------------------------------------------------------------------
// The modes
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn append_writes_rows_in_queue_order_with_metadata_columns() {
    let spec = live!();
    let db = Db::new(spec).await;
    let t = db.q("events");
    db.exec(&format!(
        r#"CREATE TABLE {t} (id bigserial PRIMARY KEY, "Kind" text, amount numeric, tags jsonb,
           seen timestamptz NOT NULL DEFAULT now(), q_off bigint, q_part text, q_pid bigint,
           q_tx text, q_at timestamptz, doc jsonb,
           twice bigint GENERATED ALWAYS AS (q_off * 2) STORED)"#
    ))
    .await;
    let fake = FakeQueen::new();
    for p in 0..3 {
        for i in 0..40 {
            // Every tenth row names fewer columns: its own statement, in
            // place, and the absent columns take their defaults.
            let payload = if i % 10 == 9 {
                format!(r#"{{"Kind":"k{p}"}}"#)
            } else {
                format!(
                    r#"{{"Kind":"k{p}","amount":123456789012345678901234567890.{i:03},"tags":{{"i":{i},"a":[1,2]}},"ignored":true,"twice":99}}"#
                )
            };
            fake.push_raw(
                "events",
                &format!("p{p}"),
                &payload,
                Some(&format!("tx-{p}-{i}")),
            );
        }
    }
    // Not an object: with metadata.payload mapped, a metadata-only row.
    fake.push_raw("events", "p0", r#""just text""#, Some("tx-text"));

    let sink = new_sink(
        &db,
        "append",
        &fake,
        "n1",
        json!({
            "queue": "events", "table": db.t("events"), "mode": "append",
            "batch": 25, "workers": 2, "leaseSeconds": 30,
            "metadata": {"offset": "q_off", "partition": "q_part", "partitionId": "q_pid",
                         "transactionId": "q_tx", "createdAt": "q_at", "payload": "doc"},
        }),
    );
    let run = start(&sink);
    wait_until!(
        "121 rows",
        60,
        db.i64(&format!("SELECT count(*) FROM {t}")).await == 121
    );
    wait_until!("the acks", 30, drained(&fake, "events", "pg-append"));
    run.stop().await;

    for p in 0..3 {
        let part = format!("p{p}");
        let rows = db
            .c
            .query(
                &format!(
                    r#"SELECT q_off, q_pid, q_tx, amount::text, tags, doc, "Kind", q_at IS NOT NULL, twice
                       FROM {t} WHERE q_part = $1 ORDER BY id"#
                ),
                &[&part],
            )
            .await
            .unwrap();
        let n = if p == 0 { 41 } else { 40 };
        assert_eq!(rows.len(), n, "{part}");
        let pid = fake.partition_id("events", &part).unwrap();
        for (i, r) in rows.iter().enumerate() {
            let off: i64 = r.get(0);
            assert_eq!(off, i as i64, "{part}: rows went in in offset order");
            assert_eq!(r.get::<_, i64>(1), pid);
            assert!(r.get::<_, bool>(7), "createdAt filled");
            assert_eq!(
                r.get::<_, i64>(8),
                off * 2,
                "a generated column is never written"
            );
            if p == 0 && i == 40 {
                assert_eq!(r.get::<_, String>(2), "tx-text");
                assert_eq!(r.get::<_, Option<String>>(6), None);
                assert!(r.get::<_, Option<String>>(3).is_none());
                continue;
            }
            assert_eq!(r.get::<_, String>(2), format!("tx-{p}-{i}"));
            assert_eq!(
                r.get::<_, Option<String>>(6).as_deref(),
                Some(part.replace('p', "k").as_str())
            );
            if i % 10 == 9 {
                assert_eq!(
                    r.get::<_, Option<String>>(3),
                    None,
                    "absent column: its default"
                );
            } else {
                assert_eq!(
                    r.get::<_, Option<String>>(3).as_deref(),
                    Some(format!("123456789012345678901234567890.{i:03}").as_str()),
                    "every digit of a numeric survives"
                );
            }
        }
    }
    assert_eq!(
        db.text(&format!("SELECT doc::text FROM {t} WHERE q_tx = 'tx-text'"))
            .await
            .as_deref(),
        Some(r#""just text""#)
    );
    assert_eq!(
        db.text(&format!("SELECT doc::text FROM {t} WHERE q_tx = 'tx-1-3'"))
            .await
            .as_deref(),
        Some(
            r#"{"Kind": "k1", "tags": {"a": [1, 2], "i": 3}, "twice": 99, "amount": 123456789012345678901234567890.003, "ignored": true}"#
        )
    );
    assert_eq!(db.progress("append").await, max_offsets(&fake, "events"));
    let st = sink.status();
    assert_eq!(st["phase"], "stopped");
    assert_eq!(status_u64(&st, "applied"), 121);
    assert_eq!(status_u64(&st, "dlq"), 0);
    assert!(st["lastAppliedAt"].is_string(), "{st}");
    db.drop_schema().await;
}

/// `(id, "Name", balance, note, big, q_off)`.
type AccountRow = (
    i64,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upsert_partial_payloads_never_null_out_absent_columns() {
    let spec = live!();
    let db = Db::new(spec).await;
    let t = db.q("Accounts");
    db.exec(&format!(
        r#"CREATE TABLE {t} (id bigint PRIMARY KEY, "Name" text, balance numeric,
           note text DEFAULT 'none', big numeric, q_off bigint)"#
    ))
    .await;
    let fake = FakeQueen::new();
    for p in [
        r#"{"id":1,"Name":"ann","balance":10,"big":98765432109876543210.123456789}"#,
        r#"{"id":1,"note":"vip"}"#,
        r#"{"id":1,"balance":20.5}"#,
        r#"{"id":2,"Name":"bob"}"#,
        r#"{"id":2,"Name":"bob2","note":null}"#,
        r#"{"id":3}"#,
        r#"{"id":3,"unknown":1}"#,
    ] {
        fake.push_raw("accts", "one", p, None);
    }
    let sink = new_sink(
        &db,
        "upsert",
        &fake,
        "n1",
        json!({"queue": "accts", "table": db.t("Accounts"), "mode": "upsert",
               "batch": 3, "leaseSeconds": 30, "metadata": {"offset": "q_off"}}),
    );
    let run = start(&sink);
    wait_until!("the acks", 60, drained(&fake, "accts", "pg-upsert"));
    run.stop().await;
    let rows =
        db.c.query(
            &format!(
                r#"SELECT id, "Name", balance::text, note, big::text, q_off FROM {t} ORDER BY id"#
            ),
            &[],
        )
        .await
        .unwrap();
    let got: Vec<AccountRow> = rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5)))
        .collect();
    let s = |v: &str| Some(v.to_string());
    assert_eq!(
        got,
        vec![
            (
                1,
                s("ann"),
                s("20.5"),
                s("vip"),
                s("98765432109876543210.123456789"),
                Some(2)
            ),
            (2, s("bob2"), None, None, None, Some(4)),
            (3, None, None, s("none"), None, Some(6)),
        ]
    );
    assert_eq!(db.progress("upsert").await, max_offsets(&fake, "accts"));
    db.drop_schema().await;
}

fn ev(v: Value) -> String {
    v.to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cdc_applies_the_source_events() {
    let spec = live!();
    let db = Db::new(spec).await;
    let t = db.q("orders");
    db.exec(&format!(
        "CREATE TABLE {t} (id int PRIMARY KEY, status text, doc text, total numeric, \
         note text DEFAULT 'sink-only')"
    ))
    .await;
    let fake = FakeQueen::new();
    let push = |v: Value| {
        fake.push_raw("orders", "all", &ev(v), None);
    };
    let meta = |seq: u32| json!({"table": "public.orders", "lsn": "0/16B3748", "xid": 7, "ts": "2026-10-02T10:00:00.000000Z", "seq": seq});
    let with = |mut base: Value, seq: u32| {
        for (k, v) in meta(seq).as_object().unwrap() {
            base[k] = v.clone();
        }
        base
    };
    push(with(
        json!({"op":"c","key":{"id":1},"after":{"id":1,"status":"new","doc":"D1","total":10.50},"before":null}),
        0,
    ));
    push(with(
        json!({"op":"c","key":{"id":2},"after":{"id":2,"status":"new","doc":"D2","total":20},"before":null}),
        1,
    ));
    push(with(
        json!({"op":"r","key":{"id":4},"after":{"id":4,"status":"snap","doc":"D4","total":40},"before":null}),
        0,
    ));
    // An unchanged TOAST column: kept.
    push(with(
        json!({"op":"u","key":{"id":1},"after":{"id":1,"status":"paid","total":11},"before":null,"unchanged":["doc"]}),
        0,
    ));
    // REPLICA IDENTITY FULL (the old row in before), same key: an upsert.
    push(with(
        json!({"op":"u","key":{"id":1},"after":{"id":1,"status":"done","doc":"D1","total":11},"before":{"id":1,"status":"paid","doc":"D1","total":11}}),
        0,
    ));
    // The key changes 2 → 3 with an unchanged TOAST column: the row moves and
    // keeps it (and the sink-only column).
    push(with(
        json!({"op":"u","key":{"id":3},"after":{"id":3,"status":"moved","total":21},"before":{"id":2},"unchanged":["doc"]}),
        0,
    ));
    push(with(
        json!({"op":"d","key":{"id":4},"after":null,"before":{"id":4}}),
        0,
    ));
    // A key change whose old row the sink never saw: the new row is written.
    push(with(
        json!({"op":"u","key":{"id":5},"after":{"id":5,"status":"late","doc":"D5","total":50},"before":{"id":6}}),
        0,
    ));
    let sink = new_sink(
        &db,
        "cdc",
        &fake,
        "n1",
        json!({"queue": "orders", "table": db.t("orders"), "mode": "cdc", "batch": 4, "leaseSeconds": 30}),
    );
    let run = start(&sink);
    wait_until!("the events", 60, drained(&fake, "orders", "pg-cdc"));
    let rows: Vec<(i32, String, Option<String>, String, String)> =
        db.c.query(
            &format!("SELECT id, status, doc, total::text, note FROM {t} ORDER BY id"),
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4)))
        .collect();
    let row = |id: i32, st: &str, doc: &str, total: &str| {
        (
            id,
            st.to_string(),
            Some(doc.to_string()),
            total.to_string(),
            "sink-only".to_string(),
        )
    };
    assert_eq!(
        rows,
        vec![
            row(1, "done", "D1", "11"),
            row(3, "moved", "D2", "21"),
            row(5, "late", "D5", "50")
        ]
    );
    // A truncate, then life goes on.
    push(
        json!({"op":"t","tables":["public.orders"],"cascade":false,"restartIdentity":false,"lsn":"0/2","xid":8,"ts":"2026-10-02T10:00:01.000000Z","seq":0}),
    );
    push(with(
        json!({"op":"c","key":{"id":7},"after":{"id":7,"status":"new","doc":"D7","total":70},"before":null}),
        1,
    ));
    wait_until!("the truncate", 60, drained(&fake, "orders", "pg-cdc"));
    run.stop().await;
    assert_eq!(db.i64(&format!("SELECT count(*) FROM {t}")).await, 1);
    assert_eq!(db.i64(&format!("SELECT id::bigint FROM {t}")).await, 7);
    assert_eq!(db.progress("cdc").await, max_offsets(&fake, "orders"));
    db.drop_schema().await;
}

// ---------------------------------------------------------------------------
// Exactly-once under faults
// ---------------------------------------------------------------------------

/// `counters (id int PRIMARY KEY, n bigint)` with `ids` rows at 0, and the
/// sql-mode document that increments one per message.
async fn counters(db: &Db, ids: i32) -> String {
    let t = db.q("counters");
    db.exec(&format!(
        "CREATE TABLE {t} (id int PRIMARY KEY, n bigint NOT NULL DEFAULT 0); \
         INSERT INTO {t} (id) SELECT generate_series(0, {})",
        ids - 1
    ))
    .await;
    format!("UPDATE {t} SET n = n + 1 WHERE id = $1::int")
}

/// The table holds exactly what the progress rows cover: Σ n = Σ (last+1).
async fn effects_match_progress(db: &Db, name: &str) {
    let applied = db
        .i64(&format!(
            "SELECT coalesce(sum(n), 0)::bigint FROM {}",
            db.q("counters")
        ))
        .await;
    let covered: i64 = db.progress(name).await.values().map(|o| o + 1).sum();
    assert_eq!(applied, covered, "the effects equal the progress");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_or_refused_ack_redelivers_without_applying_twice() {
    let spec = live!();
    let db = Db::new(spec).await;
    let statement = counters(&db, 10).await;
    let fake = FakeQueen::new();
    let push = |from: u32, to: u32| {
        for i in from..to {
            fake.push_raw(
                "hits",
                &format!("p{}", i % 4),
                &format!(r#"{{"id":{}}}"#, i % 10),
                None,
            );
        }
    };
    let sink = new_sink(
        &db,
        "acks",
        &fake,
        "n1",
        json!({"queue": "hits", "table": db.t("counters"), "mode": "sql", "statement": statement,
               "params": ["$.id"], "batch": 10, "leaseSeconds": 5}),
    );
    // Phase 1: an ack lands but its answer is lost. The sink sends it again
    // and the broker answers the repeat as it answered the first.
    fake.inject(FakeCall::Ack, Fault::LoseAnswer);
    push(0, 40);
    let run = start(&sink);
    wait_until!("phase 1 acked", 60, drained(&fake, "hits", "pg-acks"));
    assert_eq!(
        db.i64(&format!("SELECT sum(n)::bigint FROM {}", db.q("counters")))
            .await,
        40
    );
    assert_eq!(
        fake.calls(FakeCall::Ack),
        5,
        "four batches, one ack sent twice"
    );
    assert_eq!(
        status_u64(&sink.status(), "skipped"),
        0,
        "a resent ack redelivers nothing"
    );
    // Phase 2: an ack is refused outright. Its batch — applied — stays leased
    // until the lease runs out, comes back, and the progress rows drop it.
    fake.inject(
        FakeCall::Ack,
        Fault::Fail(QueenError::Status {
            code: 400,
            body: r#"{"error":"refused by the test"}"#.into(),
            retry_after_ms: None,
        }),
    );
    push(40, 100);
    drain(&fake, "hits", "pg-acks").await;
    run.stop().await;
    assert_eq!(
        db.i64(&format!(
            "SELECT count(*) FROM {} WHERE n <> 10",
            db.q("counters")
        ))
        .await,
        0,
        "every counter exactly 10"
    );
    effects_match_progress(&db, "acks").await;
    let st = sink.status();
    assert_eq!(status_u64(&st, "applied"), 100);
    assert!(
        status_u64(&st, "skipped") >= 10,
        "the refused batch came back and was dropped: {st}"
    );
    db.drop_schema().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_holding_one_partition_serialize_on_the_progress_row() {
    let spec = live!();
    let db = Db::new(spec).await;
    let statement = counters(&db, 5).await;
    let fake = FakeQueen::new();
    let doc = json!({"queue": "twin", "table": db.t("counters"), "mode": "sql", "statement": statement,
                     "params": ["$.id"], "batch": 100, "leaseSeconds": 5});
    let a = new_sink(&db, "twin", &fake, "node-a", doc.clone());
    let b = new_sink(&db, "twin", &fake, "node-b", doc);
    let run_a = start(&a);
    wait_until!(
        "node A's worker ready",
        60,
        db.idle_workers("twin").await == 1
    );
    // Node A pops the batch, takes the progress row, and waits on its first
    // UPDATE.
    let blocker = db.row_blocker(&db.q("counters")).await;
    for i in 0..50 {
        fake.push_raw("twin", "only", &format!(r#"{{"id":{}}}"#, i % 5), None);
    }
    wait_until!(
        "node A blocked mid-batch",
        60,
        db.waiting("twin").await == 1
    );
    // A's lease runs out under it; node B is handed the same messages and
    // queues behind the progress row A holds.
    fake.expire_leases();
    let run_b = start(&b);
    wait_until!(
        "node B blocked behind A's progress row",
        60,
        db.waiting("twin").await == 2
    );
    blocker.batch_execute("COMMIT").await.unwrap();
    drain(&fake, "twin", "pg-twin").await;
    run_a.stop().await;
    run_b.stop().await;
    assert_eq!(
        db.i64(&format!(
            "SELECT count(*) FROM {} WHERE n <> 10",
            db.q("counters")
        ))
        .await,
        0,
        "applied once although two nodes held the batch"
    );
    effects_match_progress(&db, "twin").await;
    let (sa, sb) = (a.status(), b.status());
    assert_eq!(status_u64(&sa, "applied"), 50, "A applied: {sa}");
    assert_eq!(status_u64(&sb, "applied"), 0, "B applied nothing: {sb}");
    assert_eq!(
        status_u64(&sb, "skipped"),
        50,
        "B saw A's progress and dropped all: {sb}"
    );
    db.drop_schema().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopping_and_restarting_at_random_moments_applies_everything_once() {
    let spec = live!();
    let db = Db::new(spec).await;
    let statement = counters(&db, 50).await;
    let fake = FakeQueen::new();
    for i in 0..6_000 {
        fake.push_raw(
            "stream",
            &format!("p{}", i % 12),
            &format!(r#"{{"id":{}}}"#, i % 50),
            None,
        );
    }
    let sink = new_sink(
        &db,
        "restart",
        &fake,
        "n1",
        json!({"queue": "stream", "table": db.t("counters"), "mode": "sql", "statement": statement,
               "params": ["$.id"], "batch": 20, "workers": 2, "leaseSeconds": 5}),
    );
    for round in 0..5 {
        let run = start(&sink);
        tokio::time::sleep(Duration::from_millis(30 + rand::random::<u64>() % 300)).await;
        run.stop().await;
        // Whatever the stopped node left leased comes back now.
        fake.expire_leases();
        effects_match_progress(&db, "restart").await;
        eprintln!(
            "round {round}: {} applied",
            db.i64(&format!("SELECT sum(n)::bigint FROM {}", db.q("counters")))
                .await
        );
    }
    let run = start(&sink);
    drain(&fake, "stream", "pg-restart").await;
    run.stop().await;
    assert_eq!(
        db.i64(&format!(
            "SELECT count(*) FROM {} WHERE n <> 120",
            db.q("counters")
        ))
        .await,
        0,
        "every counter exactly 120"
    );
    effects_match_progress(&db, "restart").await;
    assert_eq!(db.progress("restart").await, max_offsets(&fake, "stream"));
    db.drop_schema().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_terminated_connection_mid_batch_is_retried() {
    let spec = live!();
    let db = Db::new(spec).await;
    let statement = counters(&db, 5).await;
    let fake = FakeQueen::new();
    let sink = new_sink(
        &db,
        "term",
        &fake,
        "n1",
        json!({"queue": "term", "table": db.t("counters"), "mode": "sql", "statement": statement,
               "params": ["$.id"], "batch": 100, "leaseSeconds": 30}),
    );
    let run = start(&sink);
    wait_until!("the worker ready", 60, db.idle_workers("term").await == 1);
    let blocker = db.row_blocker(&db.q("counters")).await;
    for i in 0..40 {
        fake.push_raw(
            "term",
            &format!("p{}", i % 2),
            &format!(r#"{{"id":{}}}"#, i % 5),
            None,
        );
    }
    wait_until!(
        "the sink blocked mid-batch",
        60,
        db.waiting("term").await == 1
    );
    assert_eq!(
        db.terminate("term", true).await,
        1,
        "the worker's session, mid-statement"
    );
    wait_until!(
        "the failure reported",
        30,
        sink.status()["phase"] == "error"
    );
    blocker.batch_execute("COMMIT").await.unwrap();
    wait_until!("the retry", 60, drained(&fake, "term", "pg-term"));
    wait_until!("recovered", 30, sink.status()["phase"] == "running");
    run.stop().await;
    assert_eq!(
        db.i64(&format!(
            "SELECT count(*) FROM {} WHERE n <> 8",
            db.q("counters")
        ))
        .await,
        0
    );
    effects_match_progress(&db, "term").await;
    let st = sink.status();
    assert_eq!(st["lastError"]["code"], "postgres", "{st}");
    assert!(
        st["lastError"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("57P01"),
        "{st}"
    );
    assert_eq!(status_u64(&st, "applied"), 40);
    db.drop_schema().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_poison_message_is_dead_lettered_and_its_partition_continues() {
    let spec = live!();
    let db = Db::new(spec).await;
    let t = db.q("items");
    db.exec(&format!(
        "CREATE TABLE {t} (part text NOT NULL, n int NOT NULL)"
    ))
    .await;
    let fake = FakeQueen::new();
    let row = |p: &str, n: &str| format!(r#"{{"part":"{p}","n":{n}}}"#);
    for n in ["1", "2", r#""abc""#, "4", "5"] {
        fake.push_raw("items", "p1", &row("p1", n), None);
    }
    for n in 11..=15 {
        fake.push_raw("items", "p2", &row("p2", &n.to_string()), None);
    }
    // The poison last: the partition's progress stays below it.
    fake.push_raw("items", "p3", &row("p3", "21"), None);
    fake.push_raw("items", "p3", &row("p3", r#""x""#), None);
    // A payload append cannot read (not an object), first.
    fake.push_raw("items", "p4", "[1,2]", None);
    fake.push_raw("items", "p4", &row("p4", "41"), None);
    let sink = new_sink(
        &db,
        "poison",
        &fake,
        "n1",
        json!({"queue": "items", "table": db.t("items"), "mode": "append",
               "batch": 50, "leaseSeconds": 5, "maxAttempts": 2}),
    );
    let run = start(&sink);
    wait_until!("three dead letters", 60, fake.dlq("items").len() == 3);
    // FakeQueen keeps the lease of a partition whose tail was left unacked
    // after its dead letter (the broker releases it at the dlq ack): run it
    // out so the tail comes back.
    drain(&fake, "items", "pg-poison").await;
    run.stop().await;
    let mut got: Vec<(String, i32)> =
        db.c.query(&format!("SELECT part, n FROM {t} ORDER BY part, n"), &[])
            .await
            .unwrap()
            .iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect();
    got.sort();
    let want: Vec<(String, i32)> = [
        ("p1", 1),
        ("p1", 2),
        ("p1", 4),
        ("p1", 5),
        ("p2", 11),
        ("p2", 12),
        ("p2", 13),
        ("p2", 14),
        ("p2", 15),
        ("p3", 21),
        ("p4", 41),
    ]
    .iter()
    .map(|(p, n)| (p.to_string(), *n))
    .collect();
    assert_eq!(got, want);
    let mut dead: Vec<(String, i64)> = fake
        .dlq("items")
        .iter()
        .map(|m| (m.partition.clone(), m.offset))
        .collect();
    dead.sort();
    assert_eq!(
        dead,
        vec![("p1".into(), 2), ("p3".into(), 1), ("p4".into(), 0)]
    );
    let pid = |p: &str| fake.partition_id("items", p).unwrap();
    let want_progress: BTreeMap<i64, i64> = [
        (pid("p1"), 4),
        (pid("p2"), 4),
        (pid("p3"), 0),
        (pid("p4"), 1),
    ]
    .into_iter()
    .collect();
    assert_eq!(
        db.progress("poison").await,
        want_progress,
        "a dead letter's offset is never written"
    );
    let st = sink.status();
    assert_eq!(status_u64(&st, "dlq"), 3);
    assert_eq!(status_u64(&st, "applied"), 11);
    let last = st["lastError"]["message"].as_str().unwrap_or_default();
    assert!(last.contains("22P02") || last.contains("payload"), "{st}");
    db.drop_schema().await;
}

/// 10k increments of exact decimal amounts (a few of them 20 digits wide)
/// over 100 accounts, two nodes of two workers each, while every fault fires
/// at random: answers lost, acks and pops refused, leases run out, sessions
/// terminated mid-statement, nodes stopped and restarted. A poison amount
/// goes to the DLQ. The balances must be EXACT.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn sql_increments_are_exact_under_every_fault() {
    let spec = live!();
    let db = Db::new(spec).await;
    let t = db.q("accounts");
    db.exec(&format!(
        "CREATE TABLE {t} (id bigint PRIMARY KEY, balance numeric NOT NULL DEFAULT 0); \
         INSERT INTO {t} (id) SELECT generate_series(0, 99)"
    ))
    .await;
    let fake = FakeQueen::new();
    let name = "ledger";
    let doc = json!({"queue": "ledger", "table": db.t("accounts"), "mode": "sql",
                     "statement": format!("UPDATE {t} SET balance = balance + $1::numeric WHERE id = $2::bigint"),
                     "params": ["$.amount", "$.account_id"],
                     "batch": 50, "workers": 2, "leaseSeconds": 5, "maxAttempts": 2});
    let sinks = [
        new_sink(&db, name, &fake, "node-a", doc.clone()),
        new_sink(&db, name, &fake, "node-b", doc),
    ];
    let mut runs = [Some(start(&sinks[0])), Some(start(&sinks[1]))];

    let mut expected = [0i128; 100];
    let mut pushed = 0u64;
    let mut faults = BTreeMap::<&str, u32>::new();
    for wave in 0..50u64 {
        for k in 0..200u64 {
            let i = wave * 200 + k;
            let acct = ((i * 7919) % 100) as usize;
            let cents: i128 = if i % 1000 == 7 {
                1_234_567_890_123_456_789_012 // 20 digits before the point
            } else {
                1 + ((i * 31) % 997) as i128
            };
            let amount = format!("{}.{:02}", cents / 100, cents % 100);
            fake.push_raw(
                "ledger",
                &format!("acct-{}", acct % 20),
                &format!(r#"{{"account_id":{acct},"amount":{amount},"i":{i}}}"#),
                Some(&format!("tx-{i}")),
            );
            expected[acct] += cents;
            pushed += 1;
        }
        if wave == 20 {
            fake.push_raw(
                "ledger",
                "acct-3",
                r#"{"account_id":3,"amount":"abc"}"#,
                Some("tx-poison"),
            );
        }
        tokio::time::sleep(Duration::from_millis(20 + rand::random::<u64>() % 80)).await;
        let fault = match rand::random::<u32>() % 8 {
            0 => {
                fake.inject(FakeCall::Ack, Fault::LoseAnswer);
                "ack answer lost"
            }
            1 => {
                fake.inject(
                    FakeCall::Ack,
                    Fault::Fail(QueenError::Status {
                        code: 503,
                        body: "{}".into(),
                        retry_after_ms: None,
                    }),
                );
                "ack refused 503"
            }
            2 => {
                fake.inject(
                    FakeCall::Pop,
                    Fault::Fail(QueenError::Status {
                        code: 503,
                        body: "{}".into(),
                        retry_after_ms: Some(50),
                    }),
                );
                "pop refused 503"
            }
            3 => {
                fake.expire_leases();
                "leases expired"
            }
            4 => {
                fake.advance(Duration::from_secs(6));
                "clock past the lease"
            }
            5 | 6 => {
                db.terminate(name, false).await;
                "sessions terminated"
            }
            _ => {
                let k = (rand::random::<u32>() % 2) as usize;
                runs[k].take().expect("running").stop().await;
                runs[k] = Some(start(&sinks[k]));
                "node restarted"
            }
        };
        *faults.entry(fault).or_default() += 1;
    }
    eprintln!("pushed {pushed} (+1 poison), faults: {faults:?}");

    drain(&fake, "ledger", "pg-ledger").await;
    for r in runs.iter_mut() {
        r.take().unwrap().stop().await;
    }
    let total: i128 = expected.iter().sum();
    let sum: i128 = db
        .text(&format!("SELECT (sum(balance) * 100)::text FROM {t}"))
        .await
        .unwrap()
        .split('.')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(sum, total, "every amount exactly once, in total");
    for (acct, cents) in expected.iter().enumerate() {
        let got = db
            .text(&format!("SELECT balance::text FROM {t} WHERE id = {acct}"))
            .await
            .unwrap();
        assert_eq!(
            got,
            format!("{}.{:02}", cents / 100, cents % 100),
            "account {acct}"
        );
    }
    let dlq = fake.dlq("ledger");
    assert_eq!(dlq.len(), 1, "the poison is filed once");
    assert_eq!(dlq[0].transaction_id, "tx-poison");
    assert_eq!(
        db.progress(name).await,
        max_offsets(&fake, "ledger"),
        "progress == highest offsets"
    );
    db.drop_schema().await;
}

// ---------------------------------------------------------------------------
// What only an operator can fix
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn setup_names_what_an_operator_must_fix() {
    let spec = live!();
    let db = Db::new(spec).await;
    db.exec(&format!(
        "CREATE TABLE {} (id int, v text); CREATE TABLE {} (id int PRIMARY KEY, name text)",
        db.q("nokey"),
        db.q("keyed")
    ))
    .await;
    let fake = FakeQueen::new();
    let code = |sink: Arc<Sink>| async move {
        let (_h, stop) = stop_pair();
        let r = match tokio::time::timeout(Duration::from_secs(30), sink.clone().run(stop)).await {
            Ok(r) => r,
            Err(_) => panic!("setup did not fail fast; status: {}", sink.status()),
        };
        let e = r.expect_err("setup refuses");
        assert_eq!(sink.status()["phase"], "error");
        e.code()
    };
    assert_eq!(
        code(new_sink(
            &db,
            "s1",
            &fake,
            "n",
            json!({"queue": "q", "table": db.t("nokey"), "mode": "upsert"})
        ))
        .await,
        "no_key"
    );
    assert_eq!(
        code(new_sink(
            &db,
            "s2",
            &fake,
            "n",
            json!({"queue": "q", "table": db.t("keyed"), "mode": "upsert", "key": ["name"]})
        ))
        .await,
        "no_key",
        "a key without a unique index"
    );
    assert_eq!(
        code(new_sink(
            &db,
            "s3",
            &fake,
            "n",
            json!({"queue": "q", "table": db.t("missing"), "mode": "append"})
        ))
        .await,
        "table_missing"
    );
    assert_eq!(
        code(new_sink(
            &db,
            "s4",
            &fake,
            "n",
            json!({"queue": "q", "table": db.t("keyed"), "mode": "sql",
            "statement": "UPDATE nope SET x = $1", "params": ["$.a"]})
        ))
        .await,
        "statement"
    );
    assert_eq!(
        code(new_sink(&db, "s5", &fake, "n", json!({"queue": "q", "table": db.t("keyed"), "mode": "sql",
            "statement": format!("UPDATE {} SET name = $1 WHERE id = $2::int", db.q("keyed")), "params": ["$.a"]}))).await,
        "statement",
        "two placeholders, one param"
    );
    let mut no_create = sink_doc(
        &db,
        json!({"queue": "q", "table": db.t("keyed"), "mode": "append"}),
    );
    no_create.sink.as_mut().unwrap().progress_table = db.t("other_progress");
    no_create.sink.as_mut().unwrap().create_progress_table = false;
    let s6 = Sink::new(
        context("s6", &fake, "n"),
        no_create,
        db.spec.password.clone(),
    )
    .unwrap();
    assert_eq!(code(s6).await, "progress_table");
    db.drop_schema().await;
}
