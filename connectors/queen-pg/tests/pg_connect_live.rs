//! Live tests of `pg::connect` and `pg::catalog` against a real PostgreSQL.
//!
//! Run only when `QUEEN_PG_TEST_DSN` is set (libpq key=value form, e.g.
//! `host=127.0.0.1 port=55432 user=queen_test password=queen_test_pw
//! dbname=queen_test`); otherwise every test prints a skip note and passes.
//! The server is expected WITHOUT TLS (the `prefer` fallback and the
//! `require` refusal are asserted), and the role must be able to create a
//! schema in the database. Each test works in its own schema, dropped at the
//! end, so the tests run in parallel with each other and with other suites
//! sharing the database.

use queen_pg::config::{ConnectionSpec, SslMode};
use queen_pg::pg::catalog::{server_info, table_info, TableName};
use queen_pg::pg::connect::{classify, connect, EgressPolicy};

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
            Some(s) => s,
            None => {
                eprintln!("skipped: QUEEN_PG_TEST_DSN is not set");
                return;
            }
        }
    };
}

async fn open(spec: &ConnectionSpec) -> tokio_postgres::Client {
    connect(
        spec,
        spec.password.as_deref(),
        &EgressPolicy::allow_all(),
        "queen-pg-live-test",
    )
    .await
    .unwrap_or_else(|e| panic!("connect: {e}"))
}

/// A fresh schema for one test; dropped by [`Scratch::drop_now`].
struct Scratch {
    schema: String,
}

impl Scratch {
    async fn new(c: &tokio_postgres::Client, tag: &str) -> Scratch {
        let schema = format!(
            "qpgc_{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        );
        c.batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        Scratch { schema }
    }

    async fn drop_now(self, c: &tokio_postgres::Client) {
        c.batch_execute(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await
            .unwrap();
    }
}

async fn show(c: &tokio_postgres::Client, setting: &str) -> String {
    c.query_one(&format!("SHOW {setting}"), &[])
        .await
        .unwrap()
        .get(0)
}

#[tokio::test]
async fn disable_connects_with_the_session_settings_in_effect() {
    let spec = live!();
    let c = open(&spec).await;
    assert_eq!(show(&c, "TimeZone").await, "UTC");
    assert!(show(&c, "DateStyle").await.starts_with("ISO"));
    assert_eq!(show(&c, "IntervalStyle").await, "postgres");
    assert_eq!(show(&c, "extra_float_digits").await, "3");
    assert_eq!(show(&c, "bytea_output").await, "hex");
    assert_eq!(show(&c, "application_name").await, "queen-pg-live-test");
    // The settings are what the text output is built from.
    let row = c
        .query_one(
            "SELECT '2026-10-02 12:00:00+02'::timestamptz::text, '\\x0a0b'::bytea::text, \
                    0.1::float8::text, '1 day 2 hours'::interval::text",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "2026-10-02 10:00:00+00");
    assert_eq!(row.get::<_, String>(1), "\\x0a0b");
    assert_eq!(row.get::<_, String>(2), "0.1");
    assert_eq!(row.get::<_, String>(3), "1 day 02:00:00");
}

#[tokio::test]
async fn prefer_falls_back_to_plain_when_the_server_has_no_tls() {
    let mut spec = live!();
    spec.ssl_mode = SslMode::Prefer;
    let c = open(&spec).await;
    let ssl: bool = c
        .query_one(
            "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(!ssl);
}

#[tokio::test]
async fn require_and_verify_full_fail_cleanly_against_a_server_without_tls() {
    let base = live!();
    for mode in [SslMode::Require, SslMode::VerifyFull] {
        let mut spec = base.clone();
        spec.ssl_mode = mode;
        let e = connect(
            &spec,
            spec.password.as_deref(),
            &EgressPolicy::allow_all(),
            "t",
        )
        .await
        .err()
        .unwrap_or_else(|| panic!("{mode:?} connected to a server without TLS"));
        assert_eq!(e.code(), "postgres", "{mode:?}: {e}");
        assert!(!e.is_retryable(), "{mode:?}: {e}");
        assert!(e.to_string().contains("TLS"), "{mode:?}: {e}");
    }
}

#[tokio::test]
async fn the_egress_policy_refuses_a_loopback_server() {
    let spec = live!();
    let e = connect(
        &spec,
        spec.password.as_deref(),
        &EgressPolicy {
            allow_private: false,
        },
        "t",
    )
    .await
    .expect_err("a private address was dialled");
    assert_eq!(e.code(), "egress", "{e}");
    assert!(
        e.to_string().contains("QUEEN_PG_ALLOW_PRIVATE_NETWORKS"),
        "{e}"
    );
}

#[tokio::test]
async fn a_wrong_password_is_not_transient() {
    let spec = live!();
    let e = connect(
        &spec,
        Some("not-the-password"),
        &EgressPolicy::allow_all(),
        "t",
    )
    .await
    .expect_err("logged in with a wrong password");
    match &e {
        queen_pg::Error::Pg {
            sqlstate,
            transient,
            ..
        } => {
            assert_eq!(sqlstate.as_deref(), Some("28P01"), "{e}");
            assert!(!transient);
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn classify_a_unique_violation_and_a_terminated_backend() {
    let spec = live!();
    let c = open(&spec).await;
    let s = Scratch::new(&c, "cls").await;
    c.batch_execute(&format!(
        "CREATE TABLE {0}.u (id int PRIMARY KEY); INSERT INTO {0}.u VALUES (1)",
        s.schema
    ))
    .await
    .unwrap();
    let e = c
        .execute(&format!("INSERT INTO {}.u VALUES (1)", s.schema), &[])
        .await
        .unwrap_err();
    match classify(&e) {
        queen_pg::Error::Pg {
            sqlstate,
            transient,
            message,
        } => {
            assert_eq!(sqlstate.as_deref(), Some("23505"));
            assert!(!transient);
            assert!(message.contains("duplicate key"), "{message}");
        }
        other => panic!("{other:?}"),
    }

    // A second session, killed from the first: transient, whichever way the
    // client learns it (57P01 from the server, or a closed socket).
    let victim = open(&spec).await;
    let pid: i32 = victim
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let killed: bool = c
        .query_one("SELECT pg_terminate_backend($1)", &[&pid])
        .await
        .unwrap()
        .get(0);
    assert!(killed);
    let mut seen = None;
    for _ in 0..50 {
        match victim.query_one("SELECT 1", &[]).await {
            Ok(_) => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
            Err(e) => {
                seen = Some(classify(&e));
                break;
            }
        }
    }
    let e = seen.expect("the terminated session kept answering");
    assert_eq!(e.code(), "postgres", "{e}");
    assert!(e.is_retryable(), "{e:?}");
    s.drop_now(&c).await;
}

#[tokio::test]
async fn server_info_reads_version_and_wal_level() {
    let spec = live!();
    let c = open(&spec).await;
    let info = server_info(&c).await.unwrap();
    assert!(info.version_num >= 170_000, "{info:?}");
    assert!(!info.version.is_empty());
    assert_eq!(
        info.wal_level, "logical",
        "the test server needs wal_level=logical"
    );
}

#[tokio::test]
async fn table_info_describes_keys_identities_and_columns() {
    let spec = live!();
    let c = open(&spec).await;
    let s = Scratch::new(&c, "cat").await;
    let sc = &s.schema;
    c.batch_execute(&format!(
        "CREATE TABLE {sc}.pk (id bigint PRIMARY KEY, name text NOT NULL, amount numeric(10,2));
         CREATE TABLE {sc}.composite (b int, a text, c int, note varchar(20),
                                       PRIMARY KEY (c, a) INCLUDE (note));
         CREATE TABLE {sc}.idx (x int NOT NULL, y int NOT NULL, z text);
         CREATE UNIQUE INDEX idx_yx ON {sc}.idx (y, x);
         ALTER TABLE {sc}.idx REPLICA IDENTITY USING INDEX idx_yx;
         CREATE TABLE {sc}.full_t (id int PRIMARY KEY, v jsonb);
         ALTER TABLE {sc}.full_t REPLICA IDENTITY FULL;
         CREATE TABLE {sc}.nothing (id int PRIMARY KEY);
         ALTER TABLE {sc}.nothing REPLICA IDENTITY NOTHING;
         CREATE TABLE {sc}.dropped (a int, b int, c int, d int[]);
         ALTER TABLE {sc}.dropped DROP COLUMN b;
         CREATE TABLE {sc}.gen (id int PRIMARY KEY, price int,
                                 doubled int GENERATED ALWAYS AS (price * 2) STORED);
         CREATE TABLE {sc}.\"MixedCase\" (\"Id\" int PRIMARY KEY, \"Value\" text);
         CREATE TABLE {sc}.mixedcase (id int);
         CREATE TABLE {sc}.pk_is_identity (id int PRIMARY KEY, v text);
         ALTER TABLE {sc}.pk_is_identity REPLICA IDENTITY USING INDEX pk_is_identity_pkey;
         CREATE VIEW {sc}.a_view AS SELECT 1 AS one;"
    ))
    .await
    .unwrap();
    let t = |n: &str| TableName {
        schema: sc.clone(),
        name: n.to_string(),
    };

    let pk = table_info(&c, &t("pk")).await.unwrap().unwrap();
    assert_eq!(pk.name, t("pk"));
    assert!(pk.oid > 0);
    assert_eq!(pk.primary_key, ["id"]);
    assert_eq!(pk.replica_identity, b'd');
    assert!(pk.identity_index.is_empty());
    let names: Vec<&str> = pk.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["id", "name", "amount"]);
    assert_eq!(pk.columns[0].type_oid, 20);
    assert_eq!(pk.columns[0].type_name, "bigint");
    assert!(pk.columns[0].not_null);
    assert!(pk.columns[1].not_null);
    assert_eq!(pk.columns[2].type_oid, 1700);
    assert_eq!(pk.columns[2].type_name, "numeric(10,2)");
    assert!(!pk.columns[2].not_null);
    assert!(pk.columns.iter().all(|c| !c.generated));
    assert_eq!(pk.key_columns().unwrap(), ["id"]);

    let comp = table_info(&c, &t("composite")).await.unwrap().unwrap();
    assert_eq!(comp.primary_key, ["c", "a"], "key order, INCLUDE excluded");
    assert_eq!(
        comp.column("note").unwrap().type_name,
        "character varying(20)"
    );

    let idx = table_info(&c, &t("idx")).await.unwrap().unwrap();
    assert_eq!(idx.replica_identity, b'i');
    assert!(idx.primary_key.is_empty());
    assert_eq!(idx.identity_index, ["y", "x"]);
    assert_eq!(idx.key_columns().unwrap(), ["y", "x"]);

    let full = table_info(&c, &t("full_t")).await.unwrap().unwrap();
    assert_eq!(full.replica_identity, b'f');
    assert_eq!(full.primary_key, ["id"]);
    assert_eq!(full.column("v").unwrap().type_oid, 3802);

    let nothing = table_info(&c, &t("nothing")).await.unwrap().unwrap();
    assert_eq!(nothing.replica_identity, b'n');
    assert_eq!(nothing.primary_key, ["id"]);

    let dropped = table_info(&c, &t("dropped")).await.unwrap().unwrap();
    let cols: Vec<(&str, i16)> = dropped
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.attnum))
        .collect();
    assert_eq!(
        cols,
        [("a", 1), ("c", 3), ("d", 4)],
        "dropped b is gone, attnums kept"
    );
    assert_eq!(dropped.column("d").unwrap().type_name, "integer[]");
    assert_eq!(dropped.column("d").unwrap().type_oid, 1007);
    assert!(dropped.key_columns().is_none());

    let gen = table_info(&c, &t("gen")).await.unwrap().unwrap();
    assert!(gen.column("doubled").unwrap().generated);
    assert!(!gen.column("price").unwrap().generated);

    let mixed = table_info(&c, &t("MixedCase")).await.unwrap().unwrap();
    assert_eq!(mixed.primary_key, ["Id"]);
    assert_eq!(mixed.columns[1].name, "Value");
    let lower = table_info(&c, &t("mixedcase")).await.unwrap().unwrap();
    assert_ne!(lower.oid, mixed.oid, "names are exact, not case-folded");
    assert!(lower.primary_key.is_empty());

    let both = table_info(&c, &t("pk_is_identity")).await.unwrap().unwrap();
    assert_eq!(both.replica_identity, b'i');
    assert_eq!(both.primary_key, ["id"]);
    assert_eq!(both.identity_index, ["id"]);

    assert!(table_info(&c, &t("no_such_table")).await.unwrap().is_none());
    assert!(table_info(&c, &t("a_view")).await.unwrap().is_none());
    assert!(table_info(
        &c,
        &TableName {
            schema: "no_such_schema_qpgc".into(),
            name: "pk".into()
        }
    )
    .await
    .unwrap()
    .is_none());

    // Parsed names resolve to the same tables.
    let parsed = TableName::parse(&format!("{sc}.MixedCase")).unwrap();
    assert_eq!(
        table_info(&c, &parsed).await.unwrap().unwrap().oid,
        mixed.oid
    );
    s.drop_now(&c).await;
}
