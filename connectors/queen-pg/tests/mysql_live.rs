//! Live tests of the MySQL source against a real MySQL 8.0+ and the crate's
//! `FakeQueen`.
//!
//! Run only when `QUEEN_MYSQL_TEST_URL` is set (`mysql://root:pw@127.0.0.1:
//! 53306/queen_test`); otherwise every test prints a skip note and passes.
//! The server needs `gtid_mode=ON`, `enforce_gtid_consistency=ON` and
//! `binlog_row_metadata=FULL`; the user creates and drops databases. Every
//! test works in its own database (random name), dropped when it ends.
//!
//! The oracle: replaying a queue per partition in offset order (`c`/`r`/`u`
//! put `after`, `d` removes the key) gives exactly the table; every
//! transaction id is unique; and every change the writer committed after
//! the source started is in the queue exactly once (each write carries a
//! unique value).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, Row};
use rand::{Rng, SeedableRng};
use serde_json::{json, Value};

use queen_pg::config::{ConnectorDoc, NodeKnobs};
use queen_pg::fake::{FakeCall, FakeMessage, FakeQueen, Fault};
use queen_pg::mysql::MySource;
use queen_pg::queen::QueenError;
use queen_pg::{stop_pair, Context, Metrics, RunEnd, StopHandle};

/// Tests that change the server for everyone (a purge) take this for
/// writing; every other test holds it for reading.
static SERVER: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());

fn base_url() -> Option<String> {
    std::env::var("QUEEN_MYSQL_TEST_URL").ok()
}

macro_rules! need_mysql {
    () => {
        match base_url() {
            Some(u) => u,
            None => {
                eprintln!("skipped: QUEEN_MYSQL_TEST_URL is not set");
                return;
            }
        }
    };
}

/// `mysql://u:p@h:port/db` with the database replaced.
fn url_for(base: &str, db: &str) -> String {
    let (head, _) = base.rsplit_once('/').expect("a database in the URL");
    format!("{head}/{db}")
}

async fn admin(base: &str) -> Conn {
    let u = format!("{}?prefer_socket=false", url_for(base, "mysql"));
    Conn::new(Opts::from_url(&u).unwrap()).await.unwrap()
}

/// One test's database, dropped on `Drop`.
struct Env {
    base: String,
    db: String,
    name: String,
}

impl Env {
    async fn new(base: &str) -> Env {
        let tag: u32 = rand::random();
        let db = format!("qt_{tag:08x}");
        let mut c = admin(base).await;
        c.query_drop(format!("CREATE DATABASE `{db}`"))
            .await
            .unwrap();
        Env {
            base: base.to_string(),
            name: format!("src-{tag:08x}"),
            db,
        }
    }

    async fn conn(&self) -> Conn {
        let u = format!("{}?prefer_socket=false", url_for(&self.base, &self.db));
        Conn::new(Opts::from_url(&u).unwrap()).await.unwrap()
    }

    async fn exec(&self, sql: &str) {
        let mut c = self.conn().await;
        c.query_drop(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    fn doc(&self, source: Value) -> ConnectorDoc {
        let mut d = ConnectorDoc::from_json(&json!({
            "kind": "source",
            "engine": "mysql",
            "connection": {"url": url_for(&self.base, &self.db), "sslMode": "require"},
            "source": source,
        }))
        .unwrap();
        d.normalize().unwrap();
        d.validate(&self.name).unwrap();
        d
    }

    fn source(&self, fake: &Arc<FakeQueen>, doc: ConnectorDoc, node: &str) -> Arc<MySource> {
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
        let pw = doc.connection.password.clone();
        MySource::new(ctx, doc, pw).unwrap()
    }

    fn pointer(&self, fake: &FakeQueen) -> Option<Value> {
        fake.kv_value(&format!("src:{}:pointer", self.name))
            .map(|(v, _)| v)
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        let (base, db) = (self.base.clone(), self.db.clone());
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let mut c = admin(&base).await;
                let _ = c
                    .query_drop(format!("DROP DATABASE IF EXISTS `{db}`"))
                    .await;
            });
        })
        .join()
        .ok();
    }
}

struct Running {
    stop: StopHandle,
    task: tokio::task::JoinHandle<queen_pg::Result<RunEnd>>,
}

fn start(src: &Arc<MySource>) -> Running {
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
            .expect("the task does not panic")
    }

    /// A crash: the task is dropped where it is.
    fn kill(self) {
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

fn phase(src: &MySource) -> String {
    src.status()["phase"].as_str().unwrap_or("").to_string()
}

fn events(fake: &FakeQueen, queue: &str) -> Vec<(FakeMessage, Value)> {
    fake.messages(queue)
        .into_iter()
        .map(|m| {
            let v: Value = serde_json::from_str(&m.payload).unwrap();
            (m, v)
        })
        .collect()
}

fn canon(v: &Value) -> String {
    v.to_string()
}

/// Per partition in offset order: the table the events describe.
fn replay(evs: &[(FakeMessage, Value)]) -> BTreeMap<String, Value> {
    let mut state = BTreeMap::new();
    for (_, e) in evs {
        let key = canon(&e["key"]);
        match e["op"].as_str().unwrap() {
            "c" | "r" | "u" => {
                state.insert(key, e["after"].clone());
            }
            "d" => {
                state.remove(&key);
            }
            other => panic!("unexpected op {other}"),
        }
    }
    state
}

fn assert_unique_ids(evs: &[(FakeMessage, Value)]) {
    let mut seen = HashSet::new();
    for (m, _) in evs {
        assert!(
            seen.insert(m.transaction_id.clone()),
            "duplicate id {}",
            m.transaction_id
        );
    }
}

/// `SELECT id, v, s FROM t` as the events render it.
async fn table_state(env: &Env, table: &str) -> BTreeMap<String, Value> {
    let mut c = env.conn().await;
    let rows: Vec<(i64, i64, String)> = c
        .query(format!("SELECT id, v, s FROM `{table}` ORDER BY id"))
        .await
        .unwrap();
    rows.into_iter()
        .map(|(id, v, s)| (canon(&json!({"id": id})), json!({"id": id, "v": v, "s": s})))
        .collect()
}

#[tokio::test]
async fn streams_changes_one_transaction_at_a_time() {
    let base = need_mysql!();
    let _g = SERVER.read().await;
    let env = Env::new(&base).await;
    env.exec(
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, status VARCHAR(20) NOT NULL, \
         total DECIMAL(12,2) NOT NULL)",
    )
    .await;
    let fake = FakeQueen::new();
    let q = format!("{}-orders", env.db);
    let src = env.source(
        &fake,
        env.doc(json!({
            "tables": [{"table": "orders", "queue": q}],
            "snapshot": "never",
            "heartbeatSeconds": 1
        })),
        "n1",
    );
    let run = start(&src);
    wait_until("streaming", Duration::from_secs(30), || {
        phase(&src) == "streaming"
    })
    .await;

    env.exec(
        "INSERT INTO orders VALUES (1, 'new', 42.00), (2, 'new', 17.50); \
         UPDATE orders SET status = 'paid' WHERE id = 1; DELETE FROM orders WHERE id = 2",
    )
    .await;
    env.exec(
        "START TRANSACTION; INSERT INTO orders VALUES (3, 'new', 99.90); \
         UPDATE orders SET total = 100.00 WHERE id = 3; COMMIT",
    )
    .await;
    wait_until("6 events", Duration::from_secs(30), || {
        fake.messages(&q).len() == 6
    })
    .await;
    let evs = events(&fake, &q);
    assert_unique_ids(&evs);
    let by_part: HashMap<String, Vec<&Value>> =
        evs.iter().fold(HashMap::new(), |mut m, (fm, v)| {
            m.entry(fm.partition.clone()).or_default().push(v);
            m
        });
    let ops = |p: &str| -> Vec<String> {
        by_part[p]
            .iter()
            .map(|v| v["op"].as_str().unwrap().to_string())
            .collect()
    };
    assert_eq!(ops("1"), vec!["c", "u"]);
    assert_eq!(ops("2"), vec!["c", "d"]);
    assert_eq!(ops("3"), vec!["c", "u"]);
    let u1 = by_part["1"][1];
    assert_eq!(
        u1["after"],
        json!({"id": 1, "status": "paid", "total": 42.00})
    );
    assert_eq!(
        u1["before"],
        json!({"id": 1, "status": "new", "total": 42.00})
    );
    assert_eq!(u1["table"], format!("{}.orders", env.db));
    assert!(u1["gtid"].as_str().unwrap().contains(':'), "{u1}");
    // The total keeps its digits.
    let raw = evs
        .iter()
        .find(|(m, _)| m.partition == "1")
        .unwrap()
        .0
        .payload
        .clone();
    assert!(raw.contains("\"total\":42.00"), "{raw}");
    // One MySQL transaction, one GTID.
    assert_eq!(by_part["3"][0]["gtid"], by_part["3"][1]["gtid"]);
    assert_ne!(by_part["1"][0]["gtid"], by_part["1"][1]["gtid"]);
    // The pointer holds every GTID the events carry.
    let ptr = env.pointer(&fake).unwrap();
    let set = ptr["gtids"].as_str().unwrap().to_string();
    for (_, e) in &evs {
        let g = e["gtid"].as_str().unwrap();
        let (uuid, gno) = g.rsplit_once(':').unwrap();
        assert!(set.contains(uuid), "{set} lacks {g}");
        let _: u64 = gno.parse().unwrap();
    }
    assert_eq!(replay(&evs).len(), 2);
    assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);
}

/// One row of every type, before the source starts (an `r`) and after (a
/// `c`): the two renderings must be the same.
#[tokio::test]
async fn every_type_renders_alike_in_the_snapshot_and_the_stream() {
    let base = need_mysql!();
    let _g = SERVER.read().await;
    let env = Env::new(&base).await;
    env.exec(
        "CREATE TABLE ty (\
         id INT PRIMARY KEY, \
         i8 TINYINT, u8 TINYINT UNSIGNED, i16 SMALLINT, u24 MEDIUMINT UNSIGNED, i32 INT, \
         u64 BIGINT UNSIGNED, i64 BIGINT, \
         dec1 DECIMAL(20,6), dec2 DECIMAL(5,0), f FLOAT, d DOUBLE, \
         b BIT(10), y YEAR, dt DATE, dtm DATETIME(3), dtm0 DATETIME, ts TIMESTAMP(6) NULL, \
         tm TIME(2), \
         c CHAR(5), vc VARCHAR(40), l1 VARCHAR(20) CHARACTER SET latin1, \
         bn BINARY(4), vb VARBINARY(8), bl BLOB, tx TEXT, \
         e ENUM('new','paid','gone'), st SET('a','b','c'), j JSON, g POINT, \
         nul VARCHAR(5))",
    )
    .await;
    let row = |id: i32| {
        format!(
            "INSERT INTO ty VALUES ({id}, -128, 255, -32768, 16777215, -2147483648, \
             18446744073709551615, -9223372036854775808, \
             -12345678901234.123456, 99999, 3.14159, 2.718281828459045, \
             b'1010101010', 2026, '2026-11-12', '2026-11-12 10:20:30.456', \
             '1999-12-31 23:59:59', '2026-11-12 10:20:30.123456', '-838:59:58.50', \
             'ab', 'héllo wörld 🐝', 'é€Ÿ', x'00ff', x'0001ff', x'deadbeef', 'some text', \
             'paid', 'a,c', '{{\"b\": [1, 2.5, \"x\"], \"a\": null, \"n\": {{\"t\": true}}}}', \
             ST_GeomFromText('POINT(1 2)'), NULL)"
        )
    };
    env.exec(&format!("SET time_zone = '+00:00'; {}", row(1)))
        .await;
    let fake = FakeQueen::new();
    let q = format!("{}-ty", env.db);
    let src = env.source(
        &fake,
        env.doc(json!({
            "tables": [{"table": "ty", "queue": q}],
            "heartbeatSeconds": 1
        })),
        "n1",
    );
    let run = start(&src);
    wait_until("the snapshot row", Duration::from_secs(30), || {
        !fake.messages(&q).is_empty()
    })
    .await;
    env.exec(&format!("SET time_zone = '+00:00'; {}", row(2)))
        .await;
    wait_until("the stream row", Duration::from_secs(30), || {
        fake.messages(&q).len() == 2
    })
    .await;
    let evs = events(&fake, &q);
    let (r, c) = (&evs[0].1, &evs[1].1);
    assert_eq!(r["op"], "r");
    assert_eq!(c["op"], "c");
    let strip = |v: &Value| {
        let mut o = v["after"].as_object().unwrap().clone();
        o.remove("id");
        o
    };
    assert_eq!(strip(r), strip(c), "\nr: {r}\nc: {c}");
    // The raw text too (numbers as printed).
    let text = |p: &str| {
        p.split("\"after\":")
            .nth(1)
            .unwrap()
            .split(",\"before\"")
            .next()
            .unwrap()
            .replacen("\"id\":1", "\"id\":X", 1)
            .replacen("\"id\":2", "\"id\":X", 1)
    };
    assert_eq!(text(&evs[0].0.payload), text(&evs[1].0.payload));
    let a = &c["after"];
    assert_eq!(a["u64"], json!(18446744073709551615u64));
    assert_eq!(a["i64"], json!(-9223372036854775808i64));
    assert_eq!(a["b"], json!(0b1010101010));
    assert_eq!(a["y"], json!(2026));
    assert_eq!(a["dt"], "2026-11-12");
    assert_eq!(a["dtm"], "2026-11-12T10:20:30.456");
    assert_eq!(a["dtm0"], "1999-12-31T23:59:59");
    assert_eq!(a["ts"], "2026-11-12T10:20:30.123456+00:00");
    assert_eq!(a["tm"], "-838:59:58.50");
    assert_eq!(a["c"], "ab");
    assert_eq!(a["vc"], "héllo wörld 🐝");
    assert_eq!(a["l1"], "é€Ÿ");
    assert_eq!(a["bn"], "\\x00ff0000");
    assert_eq!(a["vb"], "\\x0001ff");
    assert_eq!(a["e"], "paid");
    assert_eq!(a["st"], "a,c");
    assert_eq!(
        a["j"],
        json!({"a": null, "b": [1, 2.5, "x"], "n": {"t": true}})
    );
    assert_eq!(a["nul"], Value::Null);
    let raw = &evs[1].0.payload;
    assert!(raw.contains("\"dec1\":-12345678901234.123456"), "{raw}");
    assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);
}

/// The snapshot reads a table while a writer changes it; the queue ends as
/// the table, and every write is in it exactly once.
#[tokio::test]
async fn the_snapshot_and_the_stream_agree_under_concurrent_writes() {
    let base = need_mysql!();
    let _g = SERVER.read().await;
    let env = Env::new(&base).await;
    env.exec("CREATE TABLE t (id BIGINT PRIMARY KEY, v INT NOT NULL, s VARCHAR(40) NOT NULL)")
        .await;
    // 3000 rows to read in chunks of 100, one writer (more with
    // QUEEN_MYSQL_TEST_ROWS and QUEEN_MYSQL_TEST_WRITERS).
    let env_n = |k: &str, d: usize| {
        std::env::var(k)
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(d)
    };
    let (rows, writers) = (
        env_n("QUEEN_MYSQL_TEST_ROWS", 3000),
        env_n("QUEEN_MYSQL_TEST_WRITERS", 1),
    );
    let id_space = (rows as i64) * 6 / 5;
    let mut c = env.conn().await;
    for chunk in 0..(rows / 100) as i64 {
        let vals: Vec<String> = (1..=100)
            .map(|i| {
                let id = chunk * 100 + i;
                format!("({id}, 0, 'init{id}')")
            })
            .collect();
        c.query_drop(format!("INSERT INTO t VALUES {}", vals.join(",")))
            .await
            .unwrap();
    }
    let fake = FakeQueen::new();
    let q = format!("{}-t", env.db);
    let src = env.source(
        &fake,
        env.doc(json!({
            "tables": [{"table": "t", "queue": q}],
            "snapshotChunkRows": 100,
            "maxBundleMessages": 200,
            "heartbeatSeconds": 1
        })),
        "n1",
    );
    let run = start(&src);
    wait_until("the pointer", Duration::from_secs(30), || {
        env.pointer(&fake).is_some()
    })
    .await;

    // The writer: random updates, inserts and deletes, one to four per
    // transaction, each write a unique token.
    let stop = Arc::new(AtomicBool::new(false));
    let written = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for w in 0..writers {
        let stop = stop.clone();
        let written = written.clone();
        let mut c = env.conn().await;
        handles.push(tokio::spawn(async move {
            let mut rng = rand::rngs::StdRng::seed_from_u64(4242 + w as u64);
            let mut tokens = Vec::new();
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let k = rng.gen_range(1..=4);
                let mut sql = String::from("START TRANSACTION;");
                let mut mine = Vec::new();
                for _ in 0..k {
                    n += 1;
                    let tok = format!("w{w}x{n}");
                    let id: i64 = rng.gen_range(1..=id_space);
                    match rng.gen_range(0..10) {
                        0..=6 => {
                            sql.push_str(&format!(
                                "UPDATE t SET v = v + 1, s = '{tok}' WHERE id = {id};"
                            ));
                            mine.push((tok, id, 'u'));
                        }
                        7..=8 => {
                            sql.push_str(&format!(
                                "INSERT INTO t VALUES ({id}, 0, '{tok}') \
                                 ON DUPLICATE KEY UPDATE v = v + 1, s = '{tok}';"
                            ));
                            mine.push((tok, id, 'i'));
                        }
                        _ => {
                            sql.push_str(&format!("DELETE FROM t WHERE id = {id};"));
                        }
                    }
                }
                sql.push_str("COMMIT");
                // Two writers on one row: a deadlock is MySQL's, retried.
                match c.query_drop(&sql).await {
                    Ok(()) => tokens.extend(mine),
                    Err(mysql_async::Error::Server(e)) if e.code == 1213 || e.code == 1205 => {
                        let _ = c.query_drop("ROLLBACK").await;
                        continue;
                    }
                    Err(e) => panic!("{e}"),
                }
                written.fetch_add(k as u64, Ordering::Relaxed);
            }
            tokens
        }));
    }
    wait_until("the snapshot to finish", Duration::from_secs(120), || {
        phase(&src) == "streaming"
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, Ordering::Relaxed);
    let mut tokens = Vec::new();
    for h in handles {
        tokens.extend(h.await.unwrap());
    }
    assert!(
        written.load(Ordering::Relaxed) > 100,
        "the writer wrote little"
    );

    let want = table_state(&env, "t").await;
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let got = replay(&events(&fake, &q));
        if got == want {
            break;
        }
        if Instant::now() > deadline {
            let missing: Vec<_> = want
                .keys()
                .filter(|k| !got.contains_key(*k))
                .take(5)
                .collect();
            let extra: Vec<_> = got
                .keys()
                .filter(|k| !want.contains_key(*k))
                .take(5)
                .collect();
            let differ: Vec<_> = want
                .iter()
                .filter(|(k, v)| got.get(*k).is_some_and(|g| g != *v))
                .take(5)
                .collect();
            panic!(
                "the queue is not the table: missing {missing:?} extra {extra:?} differ {differ:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let evs = events(&fake, &q);
    assert_unique_ids(&evs);
    // Every write that changed a row is in the stream once (a token of an
    // update of a row that did not exist changed nothing).
    let mut count: HashMap<String, usize> = HashMap::new();
    for (_, e) in &evs {
        if e["op"] == "r" {
            continue;
        }
        if let Some(s) = e["after"]["s"].as_str() {
            *count.entry(s.to_string()).or_default() += 1;
        }
    }
    for (tok, _, _) in &tokens {
        assert!(count.get(tok).copied().unwrap_or(0) <= 1, "{tok} twice");
    }
    let inserted = tokens.iter().filter(|(_, _, k)| *k == 'i').count();
    let seen_inserts = tokens
        .iter()
        .filter(|(t, _, k)| *k == 'i' && count.contains_key(t))
        .count();
    assert_eq!(
        inserted, seen_inserts,
        "an insert-or-update is always a change"
    );
    let st = src.status();
    assert_eq!(st["snapshot"], Value::Null, "{st}");
    eprintln!(
        "writes {} chunk rows dropped by them {}",
        written.load(Ordering::Relaxed),
        st["snapshotRowsDropped"]
    );
    assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);
}

/// `n` single-row inserts in one round trip, ids after `next`.
async fn insert_rows(c: &mut Conn, next: &mut i64, n: usize) {
    let mut sql = String::new();
    for _ in 0..n {
        *next += 1;
        sql.push_str(&format!("INSERT INTO t VALUES ({0}, 0, 's{0}');", *next));
    }
    c.query_drop(sql).await.unwrap();
}

/// Stops, crashes and lost answers: the queue ends with every change once.
#[tokio::test]
async fn restarts_lost_answers_and_crashes_lose_and_repeat_nothing() {
    let base = need_mysql!();
    let _g = SERVER.read().await;
    let env = Env::new(&base).await;
    env.exec("CREATE TABLE t (id BIGINT PRIMARY KEY, v INT NOT NULL, s VARCHAR(40) NOT NULL)")
        .await;
    let fake = FakeQueen::new();
    let q = format!("{}-t", env.db);
    let doc = env.doc(json!({
        "tables": [{"table": "t", "queue": q}],
        "snapshot": "never",
        "maxBundleMessages": 50,
        "heartbeatSeconds": 1
    }));
    let mut c = env.conn().await;
    let mut next = 0i64;
    for round in 0..6 {
        let src = env.source(&fake, doc.clone(), &format!("n{round}"));
        let run = start(&src);
        wait_until("streaming", Duration::from_secs(30), || {
            phase(&src) == "streaming"
        })
        .await;
        if round % 2 == 1 {
            // The broker commits a bundle and the answer is lost.
            fake.inject(FakeCall::Transaction, Fault::LoseAnswer);
        }
        if round == 4 {
            fake.inject(
                FakeCall::Transaction,
                Fault::Fail(QueenError::Status {
                    code: 503,
                    body: r#"{"error":"leaderless"}"#.into(),
                    retry_after_ms: Some(50),
                }),
            );
        }
        insert_rows(&mut c, &mut next, 40).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        if round % 3 == 2 {
            run.kill();
            // A crashed owner's lease runs out before another takes over.
            fake.advance(Duration::from_secs(4));
        } else {
            assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);
        }
        // Written while no source runs.
        insert_rows(&mut c, &mut next, 15).await;
    }
    let src = env.source(&fake, doc.clone(), "last");
    let run = start(&src);
    let want = next as usize;
    wait_until("every row", Duration::from_secs(60), || {
        fake.messages(&q).len() >= want
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let evs = events(&fake, &q);
    assert_unique_ids(&evs);
    assert_eq!(evs.len(), want, "each insert exactly once");
    let ids: HashSet<i64> = evs
        .iter()
        .map(|(_, e)| e["key"]["id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids.len(), want);
    assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);
}

/// A transaction larger than a bundle goes in parts; a crash between parts
/// resumes after the last part in Queen.
#[tokio::test]
async fn a_big_transaction_goes_in_parts_and_resumes() {
    let base = need_mysql!();
    let _g = SERVER.read().await;
    let env = Env::new(&base).await;
    env.exec("CREATE TABLE t (id BIGINT PRIMARY KEY, v INT NOT NULL, s VARCHAR(40) NOT NULL)")
        .await;
    let fake = FakeQueen::new();
    let q = format!("{}-t", env.db);
    let doc = env.doc(json!({
        "tables": [{"table": "t", "queue": q}],
        "snapshot": "never",
        "maxBundleMessages": 100,
        "heartbeatSeconds": 1
    }));
    let src = env.source(&fake, doc.clone(), "n1");
    let run = start(&src);
    wait_until("streaming", Duration::from_secs(30), || {
        phase(&src) == "streaming"
    })
    .await;
    const N: usize = 20_000;
    let vals: Vec<String> = (1..=N).map(|i| format!("({i}, 0, 'big{i}')")).collect();
    env.exec(&format!("INSERT INTO t VALUES {}", vals.join(",")))
        .await;
    // Crash as soon as a part is in Queen.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let n = fake.messages(&q).len();
        if n > 0 {
            assert!(n < N, "the transaction went in before the crash could land");
            break;
        }
        assert!(Instant::now() < deadline, "no part in Queen");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    run.kill();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mid = env.pointer(&fake).unwrap();
    let parts_in = fake.messages(&q).len();
    assert!(mid["inTxn"]["done"].as_u64().unwrap() > 0, "{mid}");
    fake.advance(Duration::from_secs(4));
    let src = env.source(&fake, doc, "n2");
    let run = start(&src);
    wait_until("every row", Duration::from_secs(60), || {
        fake.messages(&q).len() >= N
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let evs = events(&fake, &q);
    assert_eq!(evs.len(), N, "{parts_in} were in before the crash");
    assert_unique_ids(&evs);
    let g0 = evs[0].1["gtid"].clone();
    assert!(
        evs.iter().all(|(_, e)| e["gtid"] == g0),
        "one transaction, one GTID"
    );
    let seqs: HashSet<u64> = evs
        .iter()
        .map(|(_, e)| e["seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs.len(), N);
    let ptr = env.pointer(&fake).unwrap();
    assert_eq!(ptr["inTxn"], Value::Null, "{ptr}");
    assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);
}

/// `TRUNCATE` is skipped by default and emitted with `onTruncate: emit`.
#[tokio::test]
async fn truncates_are_skipped_or_emitted() {
    let base = need_mysql!();
    let _g = SERVER.read().await;
    let env = Env::new(&base).await;
    env.exec("CREATE TABLE a (id INT PRIMARY KEY); CREATE TABLE b (id INT PRIMARY KEY)")
        .await;
    let fake = FakeQueen::new();
    let (qa, qb) = (format!("{}-a", env.db), format!("{}-b", env.db));
    let src = env.source(
        &fake,
        env.doc(json!({
            "tables": [
                {"table": "a", "queue": qa, "partitionBy": "single"},
                {"table": "b", "queue": qb, "partitionBy": "single"}
            ],
            "snapshot": "never",
            "onTruncate": "emit",
            "heartbeatSeconds": 1
        })),
        "n1",
    );
    let run = start(&src);
    wait_until("streaming", Duration::from_secs(30), || {
        phase(&src) == "streaming"
    })
    .await;
    env.exec("INSERT INTO a VALUES (1); TRUNCATE TABLE a; INSERT INTO a VALUES (2); TRUNCATE b")
        .await;
    wait_until("4 events", Duration::from_secs(30), || {
        fake.messages(&qa).len() == 3 && fake.messages(&qb).len() == 1
    })
    .await;
    let ops: Vec<String> = events(&fake, &qa)
        .iter()
        .map(|(_, e)| e["op"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ops, vec!["c", "t", "c"]);
    let t = &events(&fake, &qb)[0].1;
    assert_eq!(t["tables"], json!([format!("{}.b", env.db)]));
    assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);
}

/// A source stopped while the binlog it needs is purged stops with
/// `binlog_purged`; a resync snapshots again under a new epoch.
#[tokio::test]
async fn a_purged_binlog_stops_the_source_until_a_resync() {
    let base = need_mysql!();
    let _g = SERVER.write().await;
    let env = Env::new(&base).await;
    env.exec("CREATE TABLE t (id BIGINT PRIMARY KEY, v INT NOT NULL, s VARCHAR(40) NOT NULL)")
        .await;
    let fake = FakeQueen::new();
    let q = format!("{}-t", env.db);
    let mut doc = env.doc(json!({
        "tables": [{"table": "t", "queue": q}],
        "snapshot": "never",
        "heartbeatSeconds": 1
    }));
    let src = env.source(&fake, doc.clone(), "n1");
    let run = start(&src);
    wait_until("streaming", Duration::from_secs(30), || {
        phase(&src) == "streaming"
    })
    .await;
    assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);
    env.exec("INSERT INTO t VALUES (1, 0, 'lost')").await;
    let mut a = admin(&base).await;
    a.query_drop("FLUSH BINARY LOGS").await.unwrap();
    let files: Vec<Row> = a.query("SHOW BINARY LOGS").await.unwrap();
    let last: String = files.last().unwrap().get(0).unwrap();
    a.query_drop(format!("PURGE BINARY LOGS TO '{last}'"))
        .await
        .unwrap();

    let src = env.source(&fake, doc.clone(), "n2");
    let run = start(&src);
    let e = tokio::time::timeout(Duration::from_secs(30), run.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(e.code(), "binlog_purged", "{e}");
    assert!(e.to_string().contains("resync"), "{e}");

    doc.resync_requested_at = Some("2099-01-01T00:00:00Z".into());
    let epoch0 = env.pointer(&fake).unwrap()["epoch"].clone();
    let src = env.source(&fake, doc, "n3");
    let run = start(&src);
    wait_until("the resync snapshot", Duration::from_secs(30), || {
        fake.messages(&q)
            .iter()
            .any(|m| m.payload.contains("\"op\":\"r\""))
    })
    .await;
    let ptr = env.pointer(&fake).unwrap();
    assert_ne!(ptr["epoch"], epoch0);
    assert_eq!(ptr["resyncedAt"], "2099-01-01T00:00:00Z");
    assert_eq!(run.stop().await.unwrap(), RunEnd::Stopped);
}
