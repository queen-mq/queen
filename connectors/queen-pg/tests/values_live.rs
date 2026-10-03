//! Live test of `values` (PLAN §4.6 "Values") against a real PostgreSQL 17+
//! named by `QUEEN_PG_TEST_DSN` (see tests/repl_live.rs); skipped without it.
//!
//! One table with a column of every builtin type this crate converts, their
//! arrays, an enum, an enum array, domains, a composite and its array; rows
//! of NULLs, ordinary values and edge values. Each value is read twice: as
//! pgoutput streams it, and as a snapshot reads it. Both go through the same
//! converter and must give the same bytes — that is what makes an `r` event
//! and a `c` event for the same row identical.
//!
//! The snapshot read that matches is the type's OUTPUT FUNCTION (the simple
//! query protocol's text, what the source's snapshot uses). `column::text` is
//! checked too, to pin down exactly where it is NOT the output function.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::str::FromStr;
use std::time::{Duration, Instant};

use queen_pg::config::{ConnectionSpec, SslMode};
use queen_pg::pg::connect::{connect, EgressPolicy};
use queen_pg::repl::pgoutput::{Datum, Message, Relation};
use queen_pg::repl::{Lsn, ReplicationClient, StreamEvent};
use queen_pg::values::append_json_with_element;
use tokio_postgres::SimpleQueryMessage;

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

/// Drops the slot (terminating a walsender that still holds it) and runs
/// the cleanup statements however the test ends.
struct Cleanup {
    spec: ConnectionSpec,
    password: Option<String>,
    slot: Option<String>,
    statements: Vec<String>,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        let spec = self.spec.clone();
        let password = self.password.clone();
        let slot = self.slot.take();
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
                        eprintln!("CLEANUP FAILED ({e}): drop slot {slot:?} by hand");
                        return;
                    }
                };
                if let Some(slot) = slot {
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
                                "SELECT pg_drop_replication_slot(slot_name) \
                                 FROM pg_replication_slots WHERE slot_name = $1",
                                &[&slot],
                            )
                            .await
                        {
                            Ok(_) => break,
                            Err(_) if Instant::now() < deadline => {
                                tokio::time::sleep(Duration::from_millis(200)).await
                            }
                            Err(e) => {
                                eprintln!("CLEANUP FAILED: slot {slot} not dropped: {e}");
                                break;
                            }
                        }
                    }
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

/// (column, type, values of rows 2..=7 as SQL; missing ones are NULL).
/// Row 1 is all NULL.
fn columns(sfx: &str) -> Vec<(String, String, Vec<String>)> {
    let huge = format!("1{}.5", "0".repeat(400));
    let raw: Vec<(&str, String, Vec<String>)> = vec![
        (
            "c_bool",
            "bool".into(),
            v(&["true", "false", "true", "false", "NULL", "true"]),
        ),
        (
            "c_bytea",
            "bytea".into(),
            v(&[
                r"'\x0a0b'",
                r"'\x'",
                r"'\xdeadbeef'",
                r"'\x5c22'",
                "NULL",
                r"'\x00'",
            ]),
        ),
        (
            "c_char",
            "\"char\"".into(),
            v(&["'x'", r#"'"'"#, r"'\'", r"'\377'", "' '"]),
        ),
        (
            "c_name",
            "name".into(),
            v(&["'orders'", "'Mixed Case'", "''", r#"'a"b'"#, "NULL", "'ü'"]),
        ),
        (
            "c_int8",
            "int8".into(),
            v(&[
                "9223372036854775807",
                "-9223372036854775808",
                "0",
                "NULL",
                "1",
                "-1",
            ]),
        ),
        (
            "c_int2",
            "int2".into(),
            v(&["-32768", "32767", "0", "NULL", "5", "-5"]),
        ),
        (
            "c_int4",
            "int4".into(),
            v(&["2147483647", "-2147483648", "0", "42", "NULL", "7"]),
        ),
        (
            "c_text",
            "text".into(),
            v(&[
                "'héllo ✓ 𝄞'",
                r#"E'quote " back \\ tab \t nl \n cr \r ctl \x01 del \x7f'"#,
                "''",
                "'NULL'",
                "NULL",
                "'{a,b}'",
            ]),
        ),
        (
            "c_oid",
            "oid".into(),
            v(&["4294967295", "0", "12345", "NULL", "1", "2"]),
        ),
        (
            "c_json",
            "json".into(),
            v(&[
                r#"'{"a": [1, 2.5, "x"], "b": null}'"#,
                "'  [1, 2]  '",
                r#"'"str"'"#,
                "'null'",
                "NULL",
                r#"'{"big": 1e400, "u": "é"}'"#,
            ]),
        ),
        (
            "c_xml",
            "xml".into(),
            v(&[
                r#"'<a x="1">t</a>'"#,
                "'<b/>'",
                "NULL",
                "'text node'",
                "'<c>&amp;</c>'",
                r#"'<?xml version="1.0"?><d/>'"#,
            ]),
        ),
        (
            "c_point",
            "point".into(),
            v(&["'(1.5,2)'", "NULL", "'(0,0)'", "'(-1e+300,1e-300)'"]),
        ),
        ("c_lseg", "lseg".into(), v(&["'[(0,0),(1,1)]'"])),
        (
            "c_path",
            "path".into(),
            v(&["'((0,0),(1,1),(2,0))'", "'[(0,0),(1,1)]'"]),
        ),
        ("c_box", "box".into(), v(&["'(1,1),(0,0)'"])),
        ("c_polygon", "polygon".into(), v(&["'((0,0),(1,1),(1,0))'"])),
        ("c_line", "line".into(), v(&["'{1,2,3}'"])),
        ("c_circle", "circle".into(), v(&["'<(0,0),5>'"])),
        (
            "c_cidr",
            "cidr".into(),
            v(&["'10.0.0.0/8'", "'::1/128'", "'192.168.1.0/24'"]),
        ),
        (
            "c_float4",
            "float4".into(),
            v(&[
                "1.1",
                "'NaN'",
                "'Infinity'",
                "'-Infinity'",
                "'-0'",
                "3.4028235e38",
            ]),
        ),
        (
            "c_float8",
            "float8".into(),
            v(&[
                "0.1",
                "'NaN'",
                "'-Infinity'",
                "'-0'",
                "1e300",
                "2.2250738585072014e-308",
            ]),
        ),
        (
            "c_macaddr8",
            "macaddr8".into(),
            v(&["'08:00:2b:01:02:03:04:05'"]),
        ),
        ("c_money", "money".into(), v(&["1234.56", "-1", "0"])),
        ("c_macaddr", "macaddr".into(), v(&["'08:00:2b:01:02:03'"])),
        (
            "c_inet",
            "inet".into(),
            v(&["'192.168.1.5'", "'::1'", "'10.1.2.3/8'"]),
        ),
        (
            "c_bpchar",
            "char(5)".into(),
            v(&["'ab'", "''", "'abcde'", "' a'"]),
        ),
        ("c_varchar", "varchar(20)".into(), v(&["'v'", "''"])),
        (
            "c_date",
            "date".into(),
            v(&[
                "'2026-10-02'",
                "'infinity'",
                "'-infinity'",
                "'0044-03-15 BC'",
                "'5874897-12-31'",
            ]),
        ),
        (
            "c_time",
            "time".into(),
            v(&["'10:00:00.5'", "'24:00:00'", "'00:00:00'"]),
        ),
        (
            "c_timestamp",
            "timestamp".into(),
            v(&[
                "'2026-10-02 10:00:00.123'",
                "'infinity'",
                "'-infinity'",
                "'0044-03-15 12:00:00 BC'",
                "'294276-12-31 23:59:59.999999'",
            ]),
        ),
        (
            "c_timestamptz",
            "timestamptz".into(),
            v(&[
                "'2026-10-02 10:00:00.123456+00'",
                "'infinity'",
                "'-infinity'",
                "'0044-03-15 12:00:00+00 BC'",
                "'2026-10-02 15:30:00+05:30'",
                "'1900-01-01 00:00:00+00'",
            ]),
        ),
        (
            "c_interval",
            "interval".into(),
            v(&[
                "'1 year 2 mons 3 days 04:05:06.7'",
                "'-1 days'",
                "'0'",
                "'infinity'",
            ]),
        ),
        (
            "c_timetz",
            "timetz".into(),
            v(&["'10:00:00+02'", "'23:59:59.999999-12'", "'00:00:00+05:30'"]),
        ),
        ("c_bit", "bit(4)".into(), v(&["B'1010'", "B'0000'"])),
        ("c_varbit", "varbit".into(), v(&["B'101'", "B''"])),
        (
            "c_numeric",
            "numeric".into(),
            vec![
                "123.4500".into(),
                "'NaN'".into(),
                "'Infinity'".into(),
                "'-Infinity'".into(),
                huge,
                "-0.000001".into(),
            ],
        ),
        (
            "c_uuid",
            "uuid".into(),
            v(&["'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"]),
        ),
        ("c_pg_lsn", "pg_lsn".into(), v(&["'16/B374D848'"])),
        (
            "c_tsvector",
            "tsvector".into(),
            v(&["'a fat cat'", r"$$'it''s' 'x':1A$$"]),
        ),
        ("c_tsquery", "tsquery".into(), v(&["'fat & cat'"])),
        (
            "c_jsonb",
            "jsonb".into(),
            v(&[
                r#"'{"b": [1, {"c": "d"}], "a": 1.50}'"#,
                "'[]'",
                r#"'"s"'"#,
                "'null'",
                r#"'{"big": 1e400}'"#,
            ]),
        ),
        (
            "c_int4range",
            "int4range".into(),
            v(&["'[1,10)'", "'empty'"]),
        ),
        ("c_jsonpath", "jsonpath".into(), v(&["'$.a[*] ? (@ > 1)'"])),
        ("c_xid8", "xid8".into(), v(&["'12345'"])),
        (
            "c_int4multirange",
            "int4multirange".into(),
            v(&["'{[1,3), [5,7)}'"]),
        ),
        ("c_bool_a", "bool[]".into(), v(&["'{t,f,NULL}'", "'{}'"])),
        ("c_bytea_a", "bytea[]".into(), v(&[r#"'{"\\x0a0b",NULL}'"#])),
        ("c_int2_a", "int2[]".into(), v(&["'{1,-2}'"])),
        (
            "c_int4_a",
            "int4[]".into(),
            v(&["'{1,2,NULL,3}'", "'{}'", "'[0:1]={5,6}'", "'{{1,2},{3,4}}'"]),
        ),
        (
            "c_int8_a",
            "int8[]".into(),
            v(&["'{9223372036854775807,-9223372036854775808}'"]),
        ),
        ("c_oid_a", "oid[]".into(), v(&["'{1,4294967295}'"])),
        (
            "c_text_a",
            "text[]".into(),
            v(&[
                r#"'{"a\"b","NULL",NULL,"","c\\d","{}","a,b"," lead","trail "}'"#,
                "'{}'",
                "'{{a,b},{c,d}}'",
                r#"'{héllo,"✓ 𝄞"}'"#,
            ]),
        ),
        ("c_varchar_a", "varchar[]".into(), v(&[r#"'{x,"y z"}'"#])),
        ("c_bpchar_a", "char(3)[]".into(), v(&["'{a,bc}'"])),
        ("c_name_a", "name[]".into(), v(&[r#"'{a,"b c"}'"#])),
        (
            "c_float4_a",
            "float4[]".into(),
            v(&["'{NaN,Infinity,-Infinity,1.5,-0}'"]),
        ),
        ("c_float8_a", "float8[]".into(), v(&["'{0.1,1e300,NaN}'"])),
        (
            "c_numeric_a",
            "numeric[]".into(),
            v(&["'{1.50,NaN,-Infinity}'"]),
        ),
        (
            "c_uuid_a",
            "uuid[]".into(),
            v(&["'{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}'"]),
        ),
        (
            "c_json_a",
            "json[]".into(),
            v(&[r#"ARRAY['{"a": 1}', '[1, 2]', 'null', NULL]::json[]"#]),
        ),
        (
            "c_jsonb_a",
            "jsonb[]".into(),
            v(&[r#"ARRAY['{"a": 1}', '"s"', NULL]::jsonb[]"#]),
        ),
        ("c_date_a", "date[]".into(), v(&["'{2026-10-02,infinity}'"])),
        (
            "c_ts_a",
            "timestamp[]".into(),
            v(&[r#"'{"2026-10-02 10:00:00.5",infinity}'"#]),
        ),
        (
            "c_tstz_a",
            "timestamptz[]".into(),
            v(&[r#"'{"2026-10-02 10:00:00+00",-infinity}'"#]),
        ),
        ("c_time_a", "time[]".into(), v(&["'{10:00:00}'"])),
        ("c_timetz_a", "timetz[]".into(), v(&["'{10:00:00+02}'"])),
        (
            "c_interval_a",
            "interval[]".into(),
            v(&[r#"'{"1 day","-02:00:00"}'"#]),
        ),
        ("c_inet_a", "inet[]".into(), v(&["'{192.168.1.5,::1/128}'"])),
        ("c_cidr_a", "cidr[]".into(), v(&["'{10.0.0.0/8}'"])),
        (
            "c_macaddr_a",
            "macaddr[]".into(),
            v(&["'{08:00:2b:01:02:03}'"]),
        ),
        ("c_bit_a", "bit(2)[]".into(), v(&["'{10,01}'"])),
        ("c_varbit_a", "varbit[]".into(), v(&["'{1,101}'"])),
        ("c_char_a", "\"char\"[]".into(), v(&[r#"'{a,"\\"}'"#])),
        (
            "c_xml_a",
            "xml[]".into(),
            v(&["ARRAY['<a/>', '<b>x</b>']::xml[]"]),
        ),
        ("c_money_a", "money[]".into(), v(&[r#"'{1.00,"-2.50"}'"#])),
        (
            "c_box_a",
            "box[]".into(),
            v(&["'{(1,1),(0,0);(2,2),(1,1)}'"]),
        ),
        (
            "c_point_a",
            "point[]".into(),
            v(&[r#"'{"(1,2)","(3,4)"}'"#]),
        ),
        ("c_enum", format!("mood_{sfx}"), v(&["'happy'", "'sad'"])),
        (
            "c_enum_a",
            format!("mood_{sfx}[]"),
            v(&["'{sad,happy,NULL}'", "'{}'"]),
        ),
        ("c_domain", format!("posint_{sfx}"), v(&["5", "1"])),
        ("c_intlist", format!("intlist_{sfx}"), v(&["'{1,2}'"])),
        (
            "c_composite",
            format!("pair_{sfx}"),
            v(&["ROW(1, 'abc')", r#"ROW(2, 'x y "q"')"#, "ROW(NULL, NULL)"]),
        ),
        (
            "c_composite_a",
            format!("pair_{sfx}[]"),
            vec![format!("ARRAY[ROW(1,'abc'), ROW(2,'x y')]::pair_{sfx}[]")],
        ),
    ];
    raw.into_iter()
        .map(|(c, t, mut vals)| {
            vals.resize(6, "NULL".into());
            (c.to_string(), t, vals)
        })
        .collect()
}

fn v(xs: &[&str]) -> Vec<String> {
    xs.iter().map(|s| s.to_string()).collect()
}

/// The (type, array element) a source would convert a column with:
/// domains resolve to their base type, a non-builtin array (`typlen = -1`,
/// `typelem <> 0`) passes its element, everything else is itself.
async fn conversion(db: &tokio_postgres::Client, oid: u32) -> (u32, Option<u32>) {
    let mut t = oid;
    for _ in 0..8 {
        if t < 10_000 {
            return (t, None);
        }
        let r = db
            .query_one(
                "SELECT typtype::text, typbasetype, typelem, typlen FROM pg_type WHERE oid = $1",
                &[&t],
            )
            .await
            .unwrap();
        let kind: String = r.get(0);
        let base: u32 = r.get(1);
        let elem: u32 = r.get(2);
        let len: i16 = r.get(3);
        if kind == "d" {
            t = base;
            continue;
        }
        if len == -1 && elem != 0 {
            return (t, Some(elem));
        }
        return (t, None);
    }
    (t, None)
}

fn json(conv: (u32, Option<u32>), text: Option<&str>) -> String {
    let mut out = String::new();
    match text {
        Some(t) => append_json_with_element(conv.0, conv.1, t, &mut out),
        None => out.push_str("null"),
    }
    serde_json::from_str::<&serde_json::value::RawValue>(&out)
        .unwrap_or_else(|e| panic!("{conv:?} {text:?} -> invalid JSON {out:?}: {e}"));
    out
}

fn rows_of(msgs: Vec<SimpleQueryMessage>) -> Vec<Vec<Option<String>>> {
    msgs.into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).map(str::to_string)).collect())
            }
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_and_snapshot_values_are_byte_identical() {
    let Some(env) = env() else {
        eprintln!("skipped: QUEEN_PG_TEST_DSN is not set");
        return;
    };
    let sfx = format!("{:08x}", rand::random::<u32>());
    let table = format!("vl_{sfx}");
    let publication = format!("vl_pub_{sfx}");
    let slot = format!("vl_slot_{sfx}");
    let prefix = format!("queen-test-{sfx}");
    let mut cleanup = Cleanup {
        spec: env.spec.clone(),
        password: env.password.clone(),
        slot: None,
        statements: vec![format!(
            "DROP PUBLICATION IF EXISTS {publication}; DROP TABLE IF EXISTS {table}; \
             DROP TYPE IF EXISTS pair_{sfx}; DROP DOMAIN IF EXISTS intlist_{sfx}; \
             DROP DOMAIN IF EXISTS posint_{sfx}; DROP TYPE IF EXISTS mood_{sfx}"
        )],
    };
    let db = connect(
        &env.spec,
        env.password.as_deref(),
        &EgressPolicy::allow_all(),
        "queen-pg/values-live",
    )
    .await
    .unwrap_or_else(|e| panic!("connect: {e}"));
    let cols = columns(&sfx);
    let defs: Vec<String> = cols.iter().map(|(c, t, _)| format!("{c} {t}")).collect();
    db.batch_execute(&format!(
        "CREATE TYPE mood_{sfx} AS ENUM ('sad', 'ok', 'happy'); \
         CREATE DOMAIN posint_{sfx} AS int CHECK (VALUE > 0); \
         CREATE DOMAIN intlist_{sfx} AS int[]; \
         CREATE TYPE pair_{sfx} AS (a int, b text); \
         CREATE TABLE {table} (id int PRIMARY KEY, {}); \
         CREATE PUBLICATION {publication} FOR TABLE {table}",
        defs.join(", ")
    ))
    .await
    .unwrap();
    let relid: u32 = db
        .query_one("SELECT $1::text::regclass::oid", &[&table])
        .await
        .unwrap()
        .get(0);
    cleanup.slot = Some(slot.clone());
    let start: Lsn = db
        .query_one(
            "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot],
        )
        .await
        .unwrap()
        .get::<_, String>(0)
        .parse()
        .unwrap();

    let names: Vec<&str> = cols.iter().map(|(c, _, _)| c.as_str()).collect();
    let mut rows = vec![format!("(1{})", ", NULL".repeat(cols.len()))];
    for r in 0..6 {
        let vals: Vec<&str> = cols.iter().map(|(_, _, vals)| vals[r].as_str()).collect();
        rows.push(format!("({}, {})", r + 2, vals.join(", ")));
    }
    db.batch_execute(&format!(
        "INSERT INTO {table} (id, {}) VALUES {}",
        names.join(", "),
        rows.join(", ")
    ))
    .await
    .unwrap();
    db.batch_execute(&format!(
        "SELECT pg_logical_emit_message(false, '{prefix}', 'done')"
    ))
    .await
    .unwrap();

    // The stream: Type messages, the Relation, the seven rows.
    let mut st = ReplicationClient::connect(
        &env.spec,
        env.password.as_deref(),
        &EgressPolicy::allow_all(),
        "queen-pg/values-live",
    )
    .await
    .unwrap()
    .start_logical(&slot, start, &publication)
    .await
    .unwrap();
    let mut types: HashMap<u32, (String, String)> = HashMap::new();
    let mut relation: Option<Relation> = None;
    let mut streamed: Vec<Vec<Datum>> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let ev = tokio::time::timeout(left, st.next())
            .await
            .expect("the rows did not arrive within 60 s")
            .unwrap()
            .unwrap();
        let StreamEvent::XLogData { message, .. } = ev else {
            continue;
        };
        match message {
            Message::Type {
                oid,
                namespace,
                name,
            } => {
                types.insert(oid, (namespace, name));
            }
            Message::Relation(r) if r.id == relid => relation = Some(r),
            Message::Insert { relid: r, new } if r == relid => streamed.push(new.0),
            Message::LogicalMessage {
                transactional: false,
                prefix: p,
                content,
                ..
            } if p == prefix && content.as_ref() == b"done" => break,
            _ => {}
        }
    }
    st.close(Duration::from_secs(10)).await;
    let relation = relation.expect("no Relation message");
    assert_eq!(streamed.len(), 7);
    assert_eq!(relation.columns.len(), cols.len() + 1);
    for (c, (name, _, _)) in relation.columns[1..].iter().zip(&cols) {
        assert_eq!(&c.name, name);
    }
    let typmod = |name: &str| {
        relation
            .columns
            .iter()
            .find(|c| c.name == name)
            .unwrap()
            .type_mod
    };
    assert_eq!(
        (
            typmod("c_bpchar"),
            typmod("c_varchar"),
            typmod("c_bit"),
            typmod("c_numeric")
        ),
        (9, 24, 4, -1)
    );

    // Every non-builtin column type was described by a Type message first;
    // a domain is named by its BASE type.
    let mut want_types: BTreeMap<String, (String, String)> = BTreeMap::new();
    for (col, ns, name) in [
        ("c_enum", "public", format!("mood_{sfx}")),
        ("c_enum_a", "public", format!("_mood_{sfx}")),
        ("c_domain", "pg_catalog", "int4".to_string()),
        ("c_intlist", "pg_catalog", "_int4".to_string()),
        ("c_composite", "public", format!("pair_{sfx}")),
        ("c_composite_a", "public", format!("_pair_{sfx}")),
    ] {
        want_types.insert(col.to_string(), (ns.to_string(), name));
    }
    for c in &relation.columns {
        if c.type_oid >= 10_000 {
            let got = types
                .get(&c.type_oid)
                .unwrap_or_else(|| panic!("no Type message for {}", c.name));
            assert_eq!(Some(got), want_types.get(&c.name), "{}", c.name);
        }
    }

    // The snapshot reads: the output functions (simple query protocol), and
    // `::text`.
    let snapshot = rows_of(
        db.simple_query(&format!("SELECT * FROM {table} ORDER BY id"))
            .await
            .unwrap(),
    );
    let casts: Vec<String> = names.iter().map(|c| format!("{c}::text")).collect();
    let as_text = rows_of(
        db.simple_query(&format!(
            "SELECT id, {} FROM {table} ORDER BY id",
            casts.join(", ")
        ))
        .await
        .unwrap(),
    );
    assert_eq!((snapshot.len(), as_text.len()), (7, 7));

    let mut convs = Vec::new();
    for c in &relation.columns {
        convs.push(conversion(&db, c.type_oid).await);
    }
    let mut cells: BTreeMap<(String, usize), String> = BTreeMap::new();
    let mut text_cast_differs: BTreeSet<String> = BTreeSet::new();
    for (r, row) in streamed.iter().enumerate() {
        for (i, d) in row.iter().enumerate() {
            let name = &relation.columns[i].name;
            let text = match d {
                Datum::Null => None,
                Datum::Text(b) => Some(std::str::from_utf8(b).expect("text datums are UTF-8")),
                other => panic!("{name} row {}: {other:?}", r + 1),
            };
            // Byte-identical text, hence byte-identical JSON.
            assert_eq!(text, snapshot[r][i].as_deref(), "{name} row {}", r + 1);
            let streamed_json = json(convs[i], text);
            assert_eq!(streamed_json, json(convs[i], snapshot[r][i].as_deref()));
            if json(convs[i], as_text[r][i].as_deref()) != streamed_json {
                text_cast_differs.insert(name.clone());
            }
            cells.insert((name.clone(), r + 1), streamed_json);
        }
    }
    for ((c, r), j) in &cells {
        if *r >= 2 && j != "null" {
            eprintln!(
                "{c:>16} row {r}: {}",
                if j.len() > 120 { &j[..120] } else { j }
            );
        }
    }

    // `::text` is not the output function for these (see values.rs).
    let want: BTreeSet<String> = ["c_bpchar", "c_inet", "c_xml"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(text_cast_differs, want);

    // What the converter made of the server's text.
    let huge = format!("1{}.5", "0".repeat(400));
    let checks: Vec<(&str, usize, String)> = vec![
        ("c_bool", 2, "true".into()),
        ("c_bool", 3, "false".into()),
        ("c_bytea", 2, r#""\\x0a0b""#.into()),
        ("c_char", 5, r#""\\377""#.into()),
        ("c_int8", 3, "-9223372036854775808".into()),
        ("c_int2", 2, "-32768".into()),
        ("c_oid", 2, "4294967295".into()),
        (
            "c_text",
            3,
            r#""quote \" back \\ tab \t nl \n cr \r ctl \u0001 del "#.to_string() + "\u{7f}\"",
        ),
        ("c_json", 2, r#"{"a": [1, 2.5, "x"], "b": null}"#.into()),
        ("c_json", 3, "[1, 2]".into()),
        ("c_json", 5, "null".into()),
        ("c_float4", 2, "1.1".into()),
        ("c_float4", 3, r#""NaN""#.into()),
        ("c_float4", 4, r#""Infinity""#.into()),
        ("c_float4", 5, r#""-Infinity""#.into()),
        ("c_float4", 6, "-0".into()),
        ("c_float4", 7, "3.4028235e+38".into()),
        ("c_float8", 2, "0.1".into()),
        ("c_float8", 6, "1e+300".into()),
        ("c_float8", 7, "2.2250738585072014e-308".into()),
        ("c_numeric", 2, "123.4500".into()),
        ("c_numeric", 3, r#""NaN""#.into()),
        ("c_numeric", 5, r#""-Infinity""#.into()),
        ("c_numeric", 6, huge),
        ("c_numeric", 7, "-0.000001".into()),
        ("c_bpchar", 2, r#""ab   ""#.into()),
        ("c_inet", 2, r#""192.168.1.5""#.into()),
        (
            "c_timestamptz",
            2,
            r#""2026-10-02T10:00:00.123456+00:00""#.into(),
        ),
        ("c_timestamptz", 3, r#""infinity""#.into()),
        ("c_timestamptz", 5, r#""0044-03-15 12:00:00+00 BC""#.into()),
        ("c_timestamptz", 6, r#""2026-10-02T10:00:00+00:00""#.into()),
        ("c_timestamp", 2, r#""2026-10-02T10:00:00.123""#.into()),
        ("c_timestamp", 6, r#""294276-12-31T23:59:59.999999""#.into()),
        ("c_date", 5, r#""0044-03-15 BC""#.into()),
        (
            "c_interval",
            2,
            r#""1 year 2 mons 3 days 04:05:06.7""#.into(),
        ),
        ("c_bool_a", 2, "[true,false,null]".into()),
        ("c_bool_a", 3, "[]".into()),
        ("c_bytea_a", 2, r#"["\\x0a0b",null]"#.into()),
        ("c_int4_a", 2, "[1,2,null,3]".into()),
        ("c_int4_a", 4, "[5,6]".into()),
        ("c_int4_a", 5, "[[1,2],[3,4]]".into()),
        (
            "c_int8_a",
            2,
            "[9223372036854775807,-9223372036854775808]".into(),
        ),
        (
            "c_text_a",
            2,
            r#"["a\"b","NULL",null,"","c\\d","{}","a,b"," lead","trail "]"#.into(),
        ),
        ("c_text_a", 4, r#"[["a","b"],["c","d"]]"#.into()),
        ("c_text_a", 5, r#"["héllo","✓ 𝄞"]"#.into()),
        ("c_bpchar_a", 2, r#"["a  ","bc "]"#.into()),
        (
            "c_float4_a",
            2,
            r#"["NaN","Infinity","-Infinity",1.5,-0]"#.into(),
        ),
        ("c_numeric_a", 2, r#"[1.50,"NaN","-Infinity"]"#.into()),
        ("c_json_a", 2, r#"[{"a": 1},[1, 2],null,null]"#.into()),
        ("c_jsonb_a", 2, r#"[{"a": 1},"s",null]"#.into()),
        (
            "c_tstz_a",
            2,
            r#"["2026-10-02T10:00:00+00:00","-infinity"]"#.into(),
        ),
        (
            "c_ts_a",
            2,
            r#"["2026-10-02T10:00:00.5","infinity"]"#.into(),
        ),
        // inet prints a full-length mask as nothing.
        ("c_inet_a", 2, r#"["192.168.1.5","::1"]"#.into()),
        ("c_char_a", 2, r#"["a","\\"]"#.into()),
        ("c_money_a", 2, r#"["$1.00","-$2.50"]"#.into()),
        ("c_box_a", 2, r#"["(1,1),(0,0)","(2,2),(1,1)"]"#.into()),
        ("c_point_a", 2, r#"["(1,2)","(3,4)"]"#.into()),
        ("c_enum", 2, r#""happy""#.into()),
        ("c_enum_a", 2, r#"["sad","happy",null]"#.into()),
        ("c_enum_a", 3, "[]".into()),
        ("c_domain", 2, "5".into()),
        ("c_intlist", 2, "[1,2]".into()),
        ("c_composite", 2, r#""(1,abc)""#.into()),
        ("c_composite", 3, r#""(2,\"x y \"\"q\"\"\")""#.into()),
        ("c_composite_a", 2, r#"["(1,abc)","(2,\"x y\")"]"#.into()),
    ];
    for (c, r, want) in checks {
        assert_eq!(cells[&(c.to_string(), r)], want, "{c} row {r}");
    }
    // Row 1: every column NULL.
    for (c, _, _) in &cols {
        assert_eq!(cells[&(c.clone(), 1)], "null", "{c}");
    }
    // jsonb normalizes 1e400 to its 401 digits: still a JSON number.
    assert!(cells[&("c_jsonb".to_string(), 6)].starts_with(r#"{"big": 1000000"#));
}
