//! Live tests of the replication client and the pgoutput decoder against a
//! real PostgreSQL 17+ (`wal_level = logical`, a role with LOGIN REPLICATION
//! that may create tables and publications), named by `QUEEN_PG_TEST_DSN`
//! (libpq key=value or URL). Without it every test prints a skip note and
//! passes.
//!
//! Every test makes its own table, publication and slot (random suffix) and
//! drops them however it ends — a leaked slot pins WAL on the server. The
//! database may be shared with other suites: their logical messages and
//! transactions reach our slots too (messages are database-wide), so every
//! assertion looks only at our relation and our message prefix.

use std::str::FromStr;
use std::time::{Duration, Instant};

use queen_pg::config::{ConnectionSpec, SslMode};
use queen_pg::pg::connect::{connect, EgressPolicy};
use queen_pg::repl::pgoutput::{Datum, Message, OldTuple, Relation, Tuple};
use queen_pg::repl::{Lsn, ReplicationClient, ReplicationStream, StreamEvent};
use queen_pg::Error;

struct Env {
    spec: ConnectionSpec,
    password: Option<String>,
}

fn env() -> Option<Env> {
    let dsn = std::env::var("QUEEN_PG_TEST_DSN").ok()?;
    if dsn.trim().is_empty() {
        return None;
    }
    let cfg = tokio_postgres::Config::from_str(&dsn).expect("QUEEN_PG_TEST_DSN does not parse");
    let host = match cfg.get_hosts().first() {
        Some(tokio_postgres::config::Host::Tcp(h)) => h.clone(),
        _ => "localhost".to_string(),
    };
    let ssl_mode = match cfg.get_ssl_mode() {
        tokio_postgres::config::SslMode::Disable => SslMode::Disable,
        tokio_postgres::config::SslMode::Require => SslMode::Require,
        _ => SslMode::Prefer,
    };
    Some(Env {
        spec: ConnectionSpec {
            url: None,
            host,
            port: cfg.get_ports().first().copied().unwrap_or(5432),
            database: cfg.get_dbname().unwrap_or_default().to_string(),
            user: cfg.get_user().unwrap_or_default().to_string(),
            password: None,
            password_sealed: None,
            ssl_mode,
            ssl_root_cert: None,
            connect_timeout_ms: 10_000,
        },
        password: cfg
            .get_password()
            .map(|p| String::from_utf8_lossy(p).into_owned()),
    })
}

macro_rules! live_env {
    () => {
        match env() {
            Some(e) => e,
            None => {
                eprintln!("skipped: QUEEN_PG_TEST_DSN is not set");
                return;
            }
        }
    };
}

fn suffix() -> String {
    format!("{:08x}", rand::random::<u32>())
}

/// A regular connection with the crate's session settings.
async fn regular(env: &Env) -> tokio_postgres::Client {
    connect(
        &env.spec,
        env.password.as_deref(),
        &EgressPolicy::allow_all(),
        "queen-pg/repl-live",
    )
    .await
    .unwrap_or_else(|e| panic!("regular connection: {e}"))
}

async fn replication(env: &Env) -> ReplicationClient {
    ReplicationClient::connect(
        &env.spec,
        env.password.as_deref(),
        &EgressPolicy::allow_all(),
        "queen-pg/repl-live",
    )
    .await
    .unwrap_or_else(|e| panic!("replication connection: {e}"))
}

/// Drops the slots (terminating a walsender that still holds one) and runs
/// the cleanup statements when the test ends, passed or panicked. Drop
/// cannot await, so it runs on a thread of its own with its own runtime.
struct Cleanup {
    spec: ConnectionSpec,
    password: Option<String>,
    slots: Vec<String>,
    statements: Vec<String>,
}

impl Cleanup {
    fn new(env: &Env) -> Cleanup {
        Cleanup {
            spec: env.spec.clone(),
            password: env.password.clone(),
            slots: Vec::new(),
            statements: Vec::new(),
        }
    }
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        let spec = self.spec.clone();
        let password = self.password.clone();
        let slots = std::mem::take(&mut self.slots);
        let statements = std::mem::take(&mut self.statements);
        let _ = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("cleanup runtime");
            rt.block_on(async move {
                let c = match connect(
                    &spec,
                    password.as_deref(),
                    &EgressPolicy::allow_all(),
                    "queen-pg/cleanup",
                )
                .await
                {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("CLEANUP FAILED ({e}): drop slots {slots:?} by hand");
                        return;
                    }
                };
                for slot in &slots {
                    drop_slot(&c, slot).await;
                }
                for s in &statements {
                    if let Err(e) = c.batch_execute(s).await {
                        eprintln!("cleanup: {s}: {e}");
                    }
                }
            })
        })
        .join();
    }
}

async fn drop_slot(c: &tokio_postgres::Client, slot: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let _ = c
            .execute(
                "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
                 WHERE slot_name = $1 AND active_pid IS NOT NULL",
                &[&slot],
            )
            .await;
        match c
            .execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
                 WHERE slot_name = $1",
                &[&slot],
            )
            .await
        {
            Ok(_) => return,
            Err(e) if Instant::now() < deadline => {
                let _ = e;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(e) => {
                eprintln!("CLEANUP FAILED: slot {slot} not dropped: {e}");
                return;
            }
        }
    }
}

async fn create_slot(c: &tokio_postgres::Client, slot: &str) -> Lsn {
    let lsn: String = c
        .query_one(
            "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot],
        )
        .await
        .unwrap_or_else(|e| panic!("create slot: {e}"))
        .get(0);
    lsn.parse().unwrap()
}

async fn slot_state(c: &tokio_postgres::Client, slot: &str) -> (bool, Option<i32>, Lsn) {
    let r = c
        .query_one(
            "SELECT active, active_pid, confirmed_flush_lsn::text FROM pg_replication_slots \
             WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .unwrap();
    let lsn: String = r.get(2);
    (r.get(0), r.get(1), lsn.parse().unwrap())
}

/// Everything the stream delivered, and the subset that is ours.
#[derive(Default)]
struct Seen {
    /// Our transactions (BEGIN … COMMIT holding a change of our relation or
    /// a message with our prefix) and our non-transactional messages, in
    /// stream order; Relation and Type messages left out (see `relations`).
    ours: Vec<Message>,
    /// Every Relation message for our relation, in order.
    relations: Vec<Relation>,
    /// Every Type message.
    types: Vec<Message>,
    keepalives: usize,
    pending: Option<Vec<Message>>,
    pending_ours: bool,
}

impl Seen {
    fn take(&mut self, m: Message, relid: u32, prefix: &str) {
        let mine = |m: &Message| match m {
            Message::Insert { relid: r, .. }
            | Message::Update { relid: r, .. }
            | Message::Delete { relid: r, .. } => *r == relid,
            Message::Truncate { relids, .. } => relids.contains(&relid),
            Message::LogicalMessage { prefix: p, .. } => p == prefix,
            _ => false,
        };
        match m {
            Message::Begin { .. } => {
                assert!(self.pending.is_none(), "BEGIN inside a transaction");
                self.pending = Some(vec![m]);
                self.pending_ours = false;
            }
            Message::Commit { .. } => {
                let mut txn = self.pending.take().expect("COMMIT outside a transaction");
                if self.pending_ours {
                    txn.push(m);
                    self.ours.extend(txn);
                }
            }
            Message::Relation(r) => {
                if r.id == relid {
                    self.relations.push(r);
                }
            }
            Message::Type { .. } => self.types.push(m),
            Message::LogicalMessage {
                transactional: false,
                ..
            } => {
                if mine(&m) {
                    self.ours.push(m);
                }
            }
            m => {
                let is_mine = mine(&m);
                let txn = self
                    .pending
                    .as_mut()
                    .unwrap_or_else(|| panic!("{m:?} outside a transaction"));
                if is_mine {
                    self.pending_ours = true;
                    txn.push(m);
                }
            }
        }
    }

    fn done(&self, marker: &str) -> bool {
        self.ours.iter().any(|m| {
            matches!(m, Message::LogicalMessage { transactional: false, content, .. }
                if content.as_ref() == marker.as_bytes())
        })
    }
}

/// Read until our non-transactional `marker` message arrives.
async fn read_until(st: &mut ReplicationStream, relid: u32, prefix: &str, marker: &str) -> Seen {
    let mut seen = Seen::default();
    let deadline = Instant::now() + Duration::from_secs(60);
    while !seen.done(marker) {
        let left = deadline.saturating_duration_since(Instant::now());
        let ev = tokio::time::timeout(left, st.next())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "no {marker:?} within 60 s; ours so far: {:?}",
                    render_all(&seen.ours)
                )
            })
            .unwrap_or_else(|e| panic!("stream: {e}"))
            .expect("the server ended the stream");
        match ev {
            StreamEvent::XLogData { message, .. } => seen.take(message, relid, prefix),
            StreamEvent::Keepalive { .. } => seen.keepalives += 1,
        }
    }
    seen
}

fn datum(d: &Datum) -> String {
    match d {
        Datum::Null => "NULL".into(),
        Datum::Unchanged => "UNCHANGED".into(),
        Datum::Text(b) if b.len() > 64 => format!("<{} bytes>", b.len()),
        Datum::Text(b) => String::from_utf8_lossy(b).into_owned(),
        Datum::Binary(b) => format!("<binary {} bytes>", b.len()),
    }
}

fn tuple(t: &Tuple) -> String {
    let parts: Vec<String> = t.0.iter().map(datum).collect();
    format!("[{}]", parts.join(", "))
}

fn old(o: &OldTuple) -> String {
    match o {
        OldTuple::Key(t) => format!("K{}", tuple(t)),
        OldTuple::Full(t) => format!("O{}", tuple(t)),
    }
}

fn render(m: &Message) -> String {
    match m {
        Message::Begin { .. } => "BEGIN".into(),
        Message::Commit { .. } => "COMMIT".into(),
        Message::Insert { new, .. } => format!("INSERT {}", tuple(new)),
        Message::Update { old: o, new, .. } => format!(
            "UPDATE {} {}",
            o.as_ref().map(old).unwrap_or_else(|| "-".into()),
            tuple(new)
        ),
        Message::Delete { old: o, .. } => format!("DELETE {}", old(o)),
        Message::Truncate { options, relids } => {
            format!("TRUNCATE {options} {}", relids.len())
        }
        Message::LogicalMessage {
            transactional,
            content,
            ..
        } => format!(
            "MESSAGE {} {}",
            if *transactional { "tx" } else { "nontx" },
            String::from_utf8_lossy(content)
        ),
        other => format!("{other:?}"),
    }
}

fn render_all(ms: &[Message]) -> Vec<String> {
    ms.iter().map(render).collect()
}

/// (begin final LSN, commit LSN, end LSN, commit time) of every transaction.
fn commits(ms: &[Message]) -> Vec<(Lsn, Lsn, Lsn, i64)> {
    let mut out = Vec::new();
    let mut begin = None;
    for m in ms {
        match m {
            Message::Begin {
                final_lsn,
                commit_time_us,
                ..
            } => begin = Some((*final_lsn, *commit_time_us)),
            Message::Commit {
                commit_lsn,
                end_lsn,
                commit_time_us,
                ..
            } => {
                let (final_lsn, begin_time) = begin.take().unwrap();
                assert_eq!(final_lsn, *commit_lsn, "BEGIN announces the commit LSN");
                assert_eq!(
                    begin_time, *commit_time_us,
                    "BEGIN and COMMIT agree on the time"
                );
                assert!(end_lsn > commit_lsn);
                out.push((final_lsn, *commit_lsn, *end_lsn, *commit_time_us));
            }
            _ => {}
        }
    }
    out
}

/// Every message kind from a real server, decoded exactly; confirming moves
/// `confirmed_flush_lsn`; `close` frees the slot; a later start LSN skips
/// what committed before it; a start below the confirmed position is moved
/// up to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_message_decodes_and_the_slot_follows_confirmations() {
    let env = live_env!();
    let sfx = suffix();
    let table = format!("rl_{sfx}");
    let publication = format!("rl_pub_{sfx}");
    let slot = format!("rl_slot_{sfx}");
    let prefix = format!("queen-test-{sfx}");
    let mut cleanup = Cleanup::new(&env);
    cleanup.statements.push(format!(
        "DROP PUBLICATION IF EXISTS {publication}; DROP TABLE IF EXISTS {table}"
    ));
    let db = regular(&env).await;
    db.batch_execute(&format!(
        "CREATE TABLE {table} (id int PRIMARY KEY, name text, big text); \
         ALTER TABLE {table} ALTER COLUMN big SET STORAGE EXTERNAL; \
         CREATE PUBLICATION {publication} FOR TABLE {table}"
    ))
    .await
    .unwrap();
    let relid: u32 = db
        .query_one("SELECT $1::text::regclass::oid", &[&table])
        .await
        .unwrap()
        .get(0);
    cleanup.slots.push(slot.clone());
    let start = create_slot(&db, &slot).await;

    // IDENTIFY_SYSTEM against what the server says about itself.
    let mut rc = replication(&env).await;
    let id = rc.identify_system().await.unwrap();
    let sysid: String = db
        .query_one(
            "SELECT system_identifier::text FROM pg_control_system()",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let current: String = db
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(id.system_id, sysid);
    assert!(id.timeline >= 1);
    assert!(id.xlogpos <= current.parse::<Lsn>().unwrap());
    assert_eq!(id.dbname.as_deref(), Some(env.spec.database.as_str()));
    assert!(rc.parameter("server_version").is_some());

    // The workload: 2 KB+ of incompressible text in an EXTERNAL column is
    // stored out of line, so an UPDATE that leaves it alone sends 'u'.
    let big: String = (0..400u32)
        .map(|i| format!("{:032x}", u128::from(i) * 0x9E37_79B9_7F4A_7C15))
        .collect();
    db.execute(
        &format!("INSERT INTO {table} VALUES (1, 'a', $1), (2, 'b', NULL)"),
        &[&big],
    )
    .await
    .unwrap();
    for sql in [
        format!("UPDATE {table} SET name = 'a2' WHERE id = 1"),
        format!("UPDATE {table} SET id = 3 WHERE id = 2"),
        format!("DELETE FROM {table} WHERE id = 3"),
        format!("SELECT pg_logical_emit_message(false, '{prefix}', 'non-transactional')"),
        format!(
            "BEGIN; SELECT pg_logical_emit_message(true, '{prefix}', 'transactional'); \
             INSERT INTO {table} VALUES (10, 'x', NULL); COMMIT"
        ),
        format!("TRUNCATE {table}"),
        format!("INSERT INTO {table} VALUES (20, 'f', NULL)"),
        format!("ALTER TABLE {table} REPLICA IDENTITY FULL"),
        format!("UPDATE {table} SET name = 'f2' WHERE id = 20"),
        format!("DELETE FROM {table} WHERE id = 20"),
        format!("SELECT pg_logical_emit_message(false, '{prefix}', 'done')"),
    ] {
        db.batch_execute(&sql).await.unwrap();
    }
    let expected = [
        "BEGIN",
        "INSERT [1, a, <12800 bytes>]",
        "INSERT [2, b, NULL]",
        "COMMIT",
        "BEGIN",
        "UPDATE - [1, a2, UNCHANGED]",
        "COMMIT",
        "BEGIN",
        "UPDATE K[2, NULL, NULL] [3, b, NULL]",
        "COMMIT",
        "BEGIN",
        "DELETE K[3, NULL, NULL]",
        "COMMIT",
        "MESSAGE nontx non-transactional",
        "BEGIN",
        "MESSAGE tx transactional",
        "INSERT [10, x, NULL]",
        "COMMIT",
        "BEGIN",
        "TRUNCATE 0 1",
        "COMMIT",
        "BEGIN",
        "INSERT [20, f, NULL]",
        "COMMIT",
        "BEGIN",
        "UPDATE O[20, f, NULL] [20, f2, NULL]",
        "COMMIT",
        "BEGIN",
        "DELETE O[20, f2, NULL]",
        "COMMIT",
        "MESSAGE nontx done",
    ];

    let mut st = rc.start_logical(&slot, start, &publication).await.unwrap();
    let (active, pid, _) = slot_state(&db, &slot).await;
    assert!(active);
    assert_eq!(pid, Some(st.backend_pid()));
    let t0 = queen_pg::status::now_us();
    let seen = read_until(&mut st, relid, &prefix, "done").await;
    assert_eq!(render_all(&seen.ours), expected);

    // The out-of-line value arrived whole.
    let Message::Insert { new, .. } = &seen.ours[1] else {
        panic!()
    };
    assert_eq!(new.0[2], Datum::Text(big.clone().into()));

    // Relation messages: before the first change, with the key flags of
    // the identity in force (FULL flags every column).
    assert!(!seen.relations.is_empty());
    for r in &seen.relations {
        assert_eq!(
            (r.namespace.as_str(), r.name.as_str()),
            ("public", table.as_str())
        );
        let cols: Vec<(bool, &str, u32, i32)> = r
            .columns
            .iter()
            .map(|c| (c.key, c.name.as_str(), c.type_oid, c.type_mod))
            .collect();
        let full = r.replica_identity == b'f';
        assert!(full || r.replica_identity == b'd', "{}", r.replica_identity);
        assert_eq!(
            cols,
            vec![
                (true, "id", 23, -1),
                (full, "name", 25, -1),
                (full, "big", 25, -1)
            ]
        );
    }
    assert_eq!(seen.relations.first().unwrap().replica_identity, b'd');
    assert_eq!(seen.relations.last().unwrap().replica_identity, b'f');

    // LSNs and times: BEGIN/COMMIT agree, commits ascend, times are now.
    let txns = commits(&seen.ours);
    assert_eq!(txns.len(), 9);
    for w in txns.windows(2) {
        assert!(w[0].2 <= w[1].1, "commits in WAL order");
    }
    for t in &txns {
        assert!(
            (t.3 - t0).abs() < 600_000_000,
            "commit time {} vs now {t0}",
            t.3
        );
    }
    let msg_lsn = |content: &str| {
        seen.ours
            .iter()
            .find_map(|m| match m {
                Message::LogicalMessage {
                    lsn, content: c, ..
                } if c.as_ref() == content.as_bytes() => Some(*lsn),
                _ => None,
            })
            .unwrap()
    };
    let nontx = msg_lsn("non-transactional");
    assert!(
        txns[3].2 <= nontx && nontx < txns[4].1,
        "a non-transactional message sits between the commits around it"
    );
    if let Message::Truncate { relids, options } = &seen.ours[19] {
        assert_eq!((relids.as_slice(), *options), (&[relid][..], 0));
    } else {
        panic!("{:?}", seen.ours[19]);
    }

    // Confirm the first transaction only: the slot moves exactly there.
    let t1_end = txns[0].2;
    st.send_status(txns[8].2, t1_end, t1_end, false)
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (_, _, confirmed) = slot_state(&db, &slot).await;
        if confirmed == t1_end {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "confirmed_flush_lsn {confirmed} never reached {t1_end}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // close() returns once the walsender released the slot.
    st.close(Duration::from_secs(10)).await;
    let (active, pid, _) = slot_state(&db, &slot).await;
    assert!(
        !active && pid.is_none(),
        "the slot is still held by {pid:?}"
    );

    // From T2's end: T1 and T2 are skipped.
    let t2_end = txns[1].2;
    let mut st = replication(&env)
        .await
        .start_logical(&slot, t2_end, &publication)
        .await
        .unwrap();
    let again = read_until(&mut st, relid, &prefix, "done").await;
    assert_eq!(render_all(&again.ours), expected[7..]);
    assert_eq!(commits(&again.ours)[0].1, txns[2].1);
    assert_eq!(
        again.relations.first().unwrap().replica_identity,
        b'd',
        "a new session sends the Relation again"
    );
    st.close(Duration::from_secs(10)).await;

    // From 0/0: moved up to confirmed_flush_lsn (T1's end), so from T2 on.
    let mut st = replication(&env)
        .await
        .start_logical(&slot, Lsn::ZERO, &publication)
        .await
        .unwrap();
    let from_zero = read_until(&mut st, relid, &prefix, "done").await;
    assert_eq!(render_all(&from_zero.ours), expected[4..]);
    st.close(Duration::from_secs(10)).await;
}

/// The walsender asks for a reply after `wal_sender_timeout / 2` of
/// silence; answering keeps the connection, not answering loses it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keepalives_asking_for_a_reply_must_be_answered() {
    let env = live_env!();
    let sfx = suffix();
    let table = format!("rk_{sfx}");
    let publication = format!("rk_pub_{sfx}");
    let slot = format!("rk_slot_{sfx}");
    let mut cleanup = Cleanup::new(&env);
    cleanup.statements.push(format!(
        "DROP PUBLICATION IF EXISTS {publication}; DROP TABLE IF EXISTS {table}"
    ));
    let db = regular(&env).await;
    db.batch_execute(&format!(
        "CREATE TABLE {table} (id int PRIMARY KEY); CREATE PUBLICATION {publication} FOR TABLE {table}"
    ))
    .await
    .unwrap();
    cleanup.slots.push(slot.clone());
    let start = create_slot(&db, &slot).await;

    let mut rc = replication(&env).await;
    // A logical replication connection runs plain SQL, SET included.
    rc.simple_query("SET wal_sender_timeout = '2s'")
        .await
        .unwrap();
    assert_eq!(
        rc.simple_query("SHOW wal_sender_timeout").await.unwrap(),
        vec![vec![Some("2s".to_string())]]
    );
    let e = rc
        .simple_query("SELECT * FROM no_such_table_here")
        .await
        .unwrap_err();
    assert!(
        matches!(&e, Error::Pg { sqlstate: Some(c), transient: false, .. } if c == "42P01"),
        "{e:?}"
    );
    let mut st = rc.start_logical(&slot, start, &publication).await.unwrap();

    // Answering: six seconds, three timeouts' worth, and the stream lives.
    let t0 = Instant::now();
    let mut asked = 0;
    while t0.elapsed() < Duration::from_secs(6) {
        match tokio::time::timeout(Duration::from_millis(500), st.next()).await {
            Err(_) => {}
            Ok(Ok(Some(StreamEvent::Keepalive {
                reply_requested: true,
                wal_end,
                ..
            }))) => {
                asked += 1;
                st.send_status(wal_end, start, start, false).await.unwrap();
            }
            Ok(Ok(Some(_))) => {}
            Ok(other) => panic!("the stream ended while every keepalive was answered: {other:?}"),
        }
    }
    assert!(asked >= 2, "only {asked} keepalives asked for a reply");

    // Silence: the server gives up after wal_sender_timeout, by closing.
    let t1 = Instant::now();
    let end = loop {
        match tokio::time::timeout(Duration::from_secs(15), st.next()).await {
            Ok(Ok(Some(_))) => continue,
            Ok(other) => break other,
            Err(_) => panic!("the server kept a silent client for 15 s"),
        }
    };
    assert!(matches!(end, Err(Error::Io(_))), "{end:?}");
    assert!(t1.elapsed() < Duration::from_secs(10), "{:?}", t1.elapsed());
}

/// `next` inside a `select!` that cancels it whenever it would wait (a
/// yield, a 50 µs timer), while a 20 000-row transaction streams: every
/// frame arrives, whole and in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn next_survives_cancellation_mid_frame() {
    let env = live_env!();
    let sfx = suffix();
    let table = format!("rc_{sfx}");
    let publication = format!("rc_pub_{sfx}");
    let slot = format!("rc_slot_{sfx}");
    let prefix = format!("queen-test-{sfx}");
    let mut cleanup = Cleanup::new(&env);
    cleanup.statements.push(format!(
        "DROP PUBLICATION IF EXISTS {publication}; DROP TABLE IF EXISTS {table}"
    ));
    let db = regular(&env).await;
    db.batch_execute(&format!(
        "CREATE TABLE {table} (id int PRIMARY KEY, pad text); CREATE PUBLICATION {publication} FOR TABLE {table}"
    ))
    .await
    .unwrap();
    let relid: u32 = db
        .query_one("SELECT $1::text::regclass::oid", &[&table])
        .await
        .unwrap()
        .get(0);
    cleanup.slots.push(slot.clone());
    let start = create_slot(&db, &slot).await;
    const ROWS: i64 = 20_000;
    // Two statements, two transactions: a non-transactional message sent
    // inside the INSERT's transaction would arrive BEFORE it (it is
    // delivered at its WAL position, the transaction at its commit).
    db.batch_execute(&format!(
        "INSERT INTO {table} SELECT g, repeat(g::text || '-', 100) FROM generate_series(1, {ROWS}) g"
    ))
    .await
    .unwrap();
    db.batch_execute(&format!(
        "SELECT pg_logical_emit_message(false, '{prefix}', 'done')"
    ))
    .await
    .unwrap();

    let mut st = replication(&env)
        .await
        .start_logical(&slot, start, &publication)
        .await
        .unwrap();
    let mut seen = Seen::default();
    let mut cancelled = 0u64;
    let mut flip = false;
    let deadline = Instant::now() + Duration::from_secs(120);
    while !seen.done("done") {
        assert!(
            Instant::now() < deadline,
            "the transaction did not arrive in 120 s"
        );
        flip = !flip;
        tokio::select! {
            biased;
            ev = st.next() => match ev.unwrap().unwrap() {
                StreamEvent::XLogData { message, .. } => seen.take(message, relid, &prefix),
                StreamEvent::Keepalive { .. } => seen.keepalives += 1,
            },
            _ = async {
                if flip {
                    tokio::task::yield_now().await
                } else {
                    tokio::time::sleep(Duration::from_micros(50)).await
                }
            } => cancelled += 1,
        }
    }
    let ours = &seen.ours;
    assert_eq!(
        ours.len() as i64,
        ROWS + 3,
        "BEGIN, {ROWS} rows, COMMIT, the marker"
    );
    assert!(matches!(ours[0], Message::Begin { .. }));
    for (i, m) in ours[1..=ROWS as usize].iter().enumerate() {
        let id = i as i64 + 1;
        let want = Tuple(vec![
            Datum::Text(id.to_string().into()),
            Datum::Text(format!("{id}-").repeat(100).into()),
        ]);
        match m {
            Message::Insert { relid: r, new } if *r == relid && *new == want => {}
            other => panic!("row {id}: {other:?}"),
        }
    }
    assert!(matches!(ours[ROWS as usize + 1], Message::Commit { .. }));
    // The scripted unit test cuts frames at every byte; here the count only
    // shows the cancellations happened with a real server's traffic (they
    // occur when the client drains the socket faster than the walsender
    // fills it, which depends on the machine).
    assert!(
        cancelled > 10,
        "only {cancelled} cancellations: the test did not exercise them"
    );
    eprintln!("{ROWS} rows through {cancelled} cancelled next() calls");
    st.close(Duration::from_secs(10)).await;
}

/// A start the server refuses arrives as its error, classified: an unknown
/// slot is not transient; a slot held by another walsender (55006) is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refusals_carry_the_server_error() {
    let env = live_env!();
    let sfx = suffix();
    let publication = format!("rr_pub_{sfx}");
    let slot = format!("rr_slot_{sfx}");
    let mut cleanup = Cleanup::new(&env);
    cleanup
        .statements
        .push(format!("DROP PUBLICATION IF EXISTS {publication}"));
    let db = regular(&env).await;
    db.batch_execute(&format!("CREATE PUBLICATION {publication}"))
        .await
        .unwrap();

    let e = replication(&env)
        .await
        .start_logical(&format!("no_such_slot_{sfx}"), Lsn::ZERO, &publication)
        .await
        .err()
        .unwrap();
    assert!(
        matches!(&e, Error::Pg { sqlstate: Some(c), transient: false, .. } if c == "42704"),
        "{e:?}"
    );

    cleanup.slots.push(slot.clone());
    let start = create_slot(&db, &slot).await;
    let holder = replication(&env)
        .await
        .start_logical(&slot, start, &publication)
        .await
        .unwrap();
    let e = replication(&env)
        .await
        .start_logical(&slot, start, &publication)
        .await
        .err()
        .unwrap();
    assert!(
        matches!(&e, Error::Pg { sqlstate: Some(c), transient: true, .. } if c == "55006"),
        "{e:?}"
    );
    holder.close(Duration::from_secs(10)).await;

    // A wrong password: the server's 28P01, not retryable.
    let e = ReplicationClient::connect(
        &env.spec,
        Some("certainly-not-the-password"),
        &EgressPolicy::allow_all(),
        "queen-pg/repl-live",
    )
    .await
    .err()
    .unwrap();
    assert!(!e.is_retryable(), "{e:?}");
}
