//! Every procedure that renders a timestamp renders it in UTC, on ANY Postgres.
//!
//! THE BUG THIS PINS. `to_char(timestamptz, fmt)` converts to the SESSION's
//! `TimeZone` before formatting. Every rendered timestamp in this repo uses a
//! format string that ENDS IN A LITERAL "Z" — `'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'`
//! and its `.MS` sibling — so on a session that is not UTC the value is local
//! time wearing a UTC label. Nothing in the broker pins the session timezone:
//! it comes from `PGTZ`, from `postgresql.conf`, or from
//! `ALTER ROLE ... SET timezone` on the role the pool connects as, and a managed
//! Postgres set to the account's region is an ordinary deployment. The fix is
//! `AT TIME ZONE 'UTC'` before `to_char`, which yields a plain `TIMESTAMP`
//! already in UTC that `to_char` formats verbatim.
//!
//! It was found on one site (`032_log_fetch.sql`, the Kafka fetch path) and the
//! same pattern held at 82 further sites across 13 procedure files plus one
//! inline statement in `src/db.rs`. Not all of them are cosmetic:
//!
//!   * `states[].until` (`004_log_pop.sql`) is parsed back by
//!     `crate::util::parse_iso_ms`, which reads the civil fields and computes
//!     epoch-ms AS IF UTC — it never inspects the trailing "Z". A mislabelled
//!     local time therefore shifts the hot-list revisit deadline by the whole
//!     UTC offset, parking partitions long past their real lease expiry.
//!   * `expiresAt` (`005_log_ack.sql`) is what a worker schedules its next
//!     lease renewal against.
//!   * `createdAt` on the pop and fetch paths becomes the record timestamp a
//!     consumer reads, hours out with nothing on the wire to say so.
//!
//! TWO LAYERS, AND WHY BOTH. `source_pin` reads the repository text and needs no
//! database, so it runs in an ordinary `cargo test` and covers all 86 sites at
//! once — including the 87th that someone adds next year. It cannot, however,
//! prove that what it approves actually EVALUATES, and that gap is not
//! hypothetical: `AT TIME ZONE` binds tighter than `+`, so the four
//! `v_now + interval '250 milliseconds'` sites need parentheses that the
//! unparenthesised form silently omits — and plpgsql does not plan expressions
//! inside a function body at `CREATE FUNCTION` time, so a broker boot accepts
//! the broken form and fails only when that branch is first reached in
//! production. Hence the second layer: `renders_utc_whatever_the_session_is`
//! drives the real procedures under three session timezones. The source pin
//! guards coverage; the behavioural pin guards meaning.
//!
//! Needs a throwaway Postgres for the behavioural half, so that half is
//! `#[ignore]` — the convention `kv_sql_helpers.rs`, `hotlist_repairs.rs` and
//! `log_fetch_timezone.rs` already follow, including booting the real broker
//! rather than running psql: the SQL is `include_str!`-embedded, so a broker
//! boot is the only thing that proves the file on disk is the one the binary
//! carries.
//!
//! ```bash
//! cargo test --test procedures_timezone                      # source pin only
//! docker run --rm -d --name queen-tzpins-pg -e POSTGRES_PASSWORD=postgres -p 5475:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5475 cargo test --test procedures_timezone -- --ignored --nocapture
//! ```

use queen::{Broker, BrokerConfig};

/// The one instant every seeded fixture is stamped with, written with an
/// explicit offset so the literal itself cannot depend on the session.
const PINNED: &str = "TIMESTAMPTZ '2026-03-14 09:26:53.589+00'";
/// What that instant must render as, at each of the two precisions in use.
const PINNED_MS: &str = "2026-03-14T09:26:53.589Z";
const PINNED_US: &str = "2026-03-14T09:26:53.589000Z";

/// UTC (where the bug is invisible), one zone well east of it and one well west,
/// so a fault in either direction fails rather than only one.
const TIMEZONES: [&str; 3] = ["UTC", "Asia/Tokyo", "America/Los_Angeles"];

/// The default tenant every other test in this repo uses.
const TENANT: &str = "00000000-0000-0000-0000-000000000001";

/// How far a `now()`-derived render may sit from the instant we measure it at.
/// Generous enough for a loaded CI box, and two orders of magnitude below the
/// smallest offset any of the three timezones would introduce.
const TOL_SECS: f64 = 30.0;

/// `tokio_postgres::Error`'s own Display is just "db error"; the SQLSTATE and
/// the server's message live in its source. A dropped `AT TIME ZONE` paren
/// surfaces here as `42883 function pg_catalog.timezone(unknown, interval) does
/// not exist`, and that sentence is the whole diagnosis — so print it.
fn detail(e: &tokio_postgres::Error) -> String {
    use std::error::Error;
    match e.source() {
        Some(s) => format!("{e}: {s}"),
        None => e.to_string(),
    }
}

/// What a probe's rendered string has to satisfy.
enum Expect {
    /// Byte-for-byte, in every timezone: the value came from a seeded fixture.
    Exact(&'static str),
    /// Within `TOL_SECS` of `now() + this many seconds`: the value is
    /// `now()`-derived, so it cannot be pinned to a literal — but a session-
    /// timezone leak moves it by hours, which no tolerance hides.
    NearNow(f64),
}

struct Probe {
    /// Where the rendered value is produced, for the failure report.
    site: &'static str,
    /// SQL returning the rendered string as TEXT.
    sql: String,
    expect: Expect,
}

/// The probes, built fresh per timezone because several of them consume state:
/// a consumer group only reads a partition from the beginning once, so each pass
/// needs its own group and worker names. `i` is the pass index.
fn probes(i: usize) -> Vec<Probe> {
    let t = TENANT;
    vec![
        // ---- seeded fixtures: exact, in every timezone -------------------
        Probe {
            site: "011_log_stats.sql:564 get_queue_v2 -> createdAt",
            sql: format!("SELECT queen.get_queue_v2('tzpin','{t}'::uuid)->>'createdAt'"),
            expect: Expect::Exact(PINNED_MS),
        },
        Probe {
            site: "011_log_stats.sql:636 get_queue_v2 -> partitions[].createdAt",
            sql: format!(
                "SELECT queen.get_queue_v2('tzpin','{t}'::uuid)->'partitions'->0->>'createdAt'"
            ),
            expect: Expect::Exact(PINNED_MS),
        },
        Probe {
            site: "018_stats.sql:253 get_queues_v2 -> queues[].createdAt",
            sql: format!(
                "SELECT x->>'createdAt' FROM jsonb_array_elements(
                     queen.get_queues_v2('{t}'::uuid)->'queues') x WHERE x->>'name'='tzpin'"
            ),
            expect: Expect::Exact(PINNED_MS),
        },
        Probe {
            // The window is explicit because list_messages_v1 defaults `from` to
            // NOW() - 1 hour, which the pinned instant is far outside.
            site: "010_log_admin.sql:665 list_messages_v1 -> messages[].createdAt",
            sql: "SELECT queen.list_messages_v1(
                      '{\"queue\":\"tzpin\",\"from\":\"2026-03-14T00:00:00Z\",\
                        \"to\":\"2026-03-15T00:00:00Z\"}'::jsonb)
                  ->'messages'->0->>'createdAt'"
                .to_string(),
            expect: Expect::Exact(PINNED_MS),
        },
        Probe {
            site: "010_log_admin.sql:823 get_dlq_messages_v1 -> messages[].createdAt",
            sql: "SELECT queen.get_dlq_messages_v1('{\"queue\":\"tzpin\"}'::jsonb)
                  ->'messages'->0->>'createdAt'"
                .to_string(),
            expect: Expect::Exact(PINNED_MS),
        },
        Probe {
            site: "010_log_admin.sql:824 get_dlq_messages_v1 -> messages[].failedAt",
            sql: "SELECT queen.get_dlq_messages_v1('{\"queue\":\"tzpin\"}'::jsonb)
                  ->'messages'->0->>'failedAt'"
                .to_string(),
            expect: Expect::Exact(PINNED_MS),
        },
        Probe {
            site: "017_traces.sql:31 get_message_traces_v1 -> traces[].created_at",
            sql: "SELECT queen.get_message_traces_v1(
                      (SELECT p.id FROM queen.log_partitions p
                         JOIN queen.queues q ON q.id = p.queue_id WHERE q.name='tzpin'),
                      'txn-pin')->'traces'->0->>'created_at'"
                .to_string(),
            expect: Expect::Exact(PINNED_MS),
        },
        Probe {
            site: "025_log_timers.sql:1440 log_timers_peek_v1 -> deliverAt",
            sql: format!("SELECT queen.log_timers_peek_v1('{t}'::uuid,'tzpin','k1')->>'deliverAt'"),
            expect: Expect::Exact(PINNED_US),
        },
        Probe {
            site: "025_log_timers.sql:1453 log_timers_peek_v1 -> createdAt",
            sql: format!("SELECT queen.log_timers_peek_v1('{t}'::uuid,'tzpin','k1')->>'createdAt'"),
            expect: Expect::Exact(PINNED_US),
        },
        Probe {
            site: "025_log_timers.sql:1454 log_timers_peek_v1 -> updatedAt",
            sql: format!("SELECT queen.log_timers_peek_v1('{t}'::uuid,'tzpin','k1')->>'updatedAt'"),
            expect: Expect::Exact(PINNED_US),
        },
        // A function PARAMETER rather than a column: the broker binds `now()`
        // here, but the render is the same to_char and the same literal "Z".
        Probe {
            site: "025_log_timers.sql:632 log_timers_due_v1 -> now",
            sql: format!(
                "SELECT queen.log_timers_due_v1(ARRAY[0]::smallint[], {PINNED}, 10)->>'now'"
            ),
            expect: Expect::Exact(PINNED_US),
        },
        Probe {
            site: "010_log_admin.sql:381 log_seek_partition_v1 -> targetTimestamp",
            sql: format!(
                "SELECT queen.log_seek_partition_v1(
                     'seek_{i}','tzpin','Default',false,{PINNED},'{t}'::uuid)->>'targetTimestamp'"
            ),
            expect: Expect::Exact(PINNED_MS),
        },
        // The pop path's segment stamp — a fresh group each pass, because a
        // group reads a partition from the beginning exactly once.
        Probe {
            site: "004_log_pop.sql:2229 log_pop_list_v1 -> segments[].createdAt",
            sql: format!(
                "SELECT m->'partitions'->0->'segments'->0->>'createdAt'
                   FROM (SELECT meta AS m FROM queen.log_pop_list_v1(
                             'tzpin','tzpin_g_{i}',ARRAY['Default'],1048576,60,
                             'tzpin_w_{i}',false,10)) s"
            ),
            expect: Expect::Exact(PINNED_US),
        },
        // ---- now()-derived: near, in every timezone ----------------------
        Probe {
            site: "003_log_push.sql:235 log_push_one_v1 -> createdAt",
            sql: format!(
                "SELECT queen.log_push_one_v1('tzlive','Default',1,
                     decode(md5('push{i}'),'hex'),0,'\\x00'::bytea)->>'createdAt'"
            ),
            expect: Expect::NearNow(0.0),
        },
        Probe {
            site: "028_retained_bytes.sql:102 log_refresh_retained_bytes_v1 -> refreshedAt",
            sql: "SELECT queen.log_refresh_retained_bytes_v1()->>'refreshedAt'".to_string(),
            expect: Expect::NearNow(0.0),
        },
        Probe {
            site: "011_log_stats.sql:361 log_refresh_all_stats_v1 -> refreshedAt",
            sql: "SELECT queen.log_refresh_all_stats_v1()->>'refreshedAt'".to_string(),
            expect: Expect::NearNow(0.0),
        },
        Probe {
            site: "021_postgres_stats.sql:204 get_postgres_stats_v1 -> timestamp",
            sql: "SELECT queen.get_postgres_stats_v1()->>'timestamp'".to_string(),
            expect: Expect::NearNow(0.0),
        },
    ]
}

/// The `states[].until` render of `004_log_pop.sql`, which is the ONLY shape in
/// the tree where the fix needed parentheses — `AT TIME ZONE` binds tighter than
/// `+`, so `to_char(v_now + interval '250 milliseconds' AT TIME ZONE 'UTC', …)`
/// parses as `interval AT TIME ZONE`, a function that does not exist, and blows
/// up the first time the branch runs. All four sites carry the identical
/// expression, so reaching one proves the form for all of them.
///
/// Reaching it takes a specific state: a worker holding a live lease on the
/// partition, and backlog arriving behind that lease. The partition is then
/// `leased` with `until = now + 250ms` rather than being served.
async fn until_probe(c: &tokio_postgres::Client, i: usize) -> Probe {
    let seed = format!(
        "SELECT queen.log_push_one_v1('tzlive','Default',1,decode(md5('u1{i}'),'hex'),0,'\\x00'::bytea);
         SELECT (queen.log_pop_list_v1('tzlive','tzlive_g_{i}',ARRAY['Default'],1048576,60,
                     'tzlive_w_{i}',false,10)).states;
         SELECT queen.log_push_one_v1('tzlive','Default',1,decode(md5('u2{i}'),'hex'),0,'\\x01'::bytea);"
    );
    c.batch_execute(&seed)
        .await
        .expect("seed the leased-with-backlog state");
    Probe {
        site: "004_log_pop.sql:2301 log_pop_list_v1 -> states[].until (parenthesised)",
        sql: format!(
            "SELECT s->0->>'until'
               FROM (SELECT (queen.log_pop_list_v1('tzlive','tzlive_g_{i}',ARRAY['Default'],
                                 1048576,60,'tzlive_w_{i}',false,10)).states AS s) x"
        ),
        expect: Expect::NearNow(0.25),
    }
}

/// Seed one queue whose every timestamp is the pinned instant, and one live
/// queue for the `now()`-derived probes.
async fn seed(c: &tokio_postgres::Client) {
    // One statement per `batch_execute` group would be tidier, but psql-style
    // multi-statement batches run in ONE transaction: a failure anywhere rolls
    // the whole fixture back, which is what we want — a half-seeded fixture
    // would produce NULL probes that look like a passing timezone.
    c.batch_execute(&format!(
        "
        DELETE FROM queen.log_timers WHERE queue = 'tzpin';
        DELETE FROM queen.queues WHERE name IN ('tzpin','tzlive');
        INSERT INTO queen.queues (name, tenant_id) VALUES ('tzpin','{TENANT}'),('tzlive','{TENANT}');

        -- A real push, so the partition, segment and txn rows are the shapes the
        -- procedures actually read; their stamps are then pinned.
        SELECT queen.log_push_one_v1('tzpin','Default',2,
                   decode(repeat('11',32),'hex'),0,'\\x00'::bytea);

        UPDATE queen.queues SET created_at = {PINNED} WHERE name = 'tzpin';
        UPDATE queen.log_partitions SET created_at = {PINNED}
            WHERE queue_id = (SELECT id FROM queen.queues WHERE name='tzpin');
        UPDATE queen.log_segments SET created_at = {PINNED}
            WHERE partition_id IN (SELECT p.id FROM queen.log_partitions p
                JOIN queen.queues q ON q.id=p.queue_id WHERE q.name='tzpin');
        UPDATE queen.log_txns SET created_at = {PINNED}
            WHERE partition_id IN (SELECT p.id FROM queen.log_partitions p
                JOIN queen.queues q ON q.id=p.queue_id WHERE q.name='tzpin');

        INSERT INTO queen.log_dlq (partition_id, consumer_group, \"offset\",
                                   transaction_id, error, failed_at)
            SELECT p.id, 'g1', 0, 'txn-pin', 'pinned', {PINNED}
            FROM queen.log_partitions p JOIN queen.queues q ON q.id=p.queue_id
            WHERE q.name='tzpin';

        INSERT INTO queen.message_traces (partition_id, transaction_id, consumer_group,
                                          event_type, data, created_at, worker_id)
            SELECT p.id, 'txn-pin', 'g1', 'pinned', '{{}}'::jsonb, {PINNED}, 'w1'
            FROM queen.log_partitions p JOIN queen.queues q ON q.id=p.queue_id
            WHERE q.name='tzpin';

        -- `shard` and `visible_at` are GENERATED: naming them is an error, not a
        -- no-op, and it would abort this whole transaction.
        INSERT INTO queen.log_timers (tenant_id, queue, timer_key, deliver_at, txn,
                                      message_id, payload, created_at, updated_at)
            VALUES ('{TENANT}','tzpin','k1',{PINNED},'txn-pin',
                    gen_random_uuid(),'\\x00'::bytea,{PINNED},{PINNED});
        "
    ))
    .await
    .expect("seed fixtures");
}

#[tokio::test]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn renders_utc_whatever_the_session_is() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    // The real broker, purely to apply the real (embedded) schema. Background
    // loops off: they would move the rows these probes read.
    let _broker = Broker::start(
        BrokerConfig::new()
            .pg(host.clone(), port, "postgres", "postgres", "postgres")
            .pool_size(4)
            .retention(false)
            .stats_refresh(false)
            .system_metrics(false)
            .log_reports(false),
    )
    .await
    .expect("broker start");

    let (c, conn) = tokio_postgres::connect(
        &format!("host={host} port={port} user=postgres password=postgres dbname=postgres"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    seed(&c).await;

    let mut wrong: Vec<String> = Vec::new();
    for (i, tz) in TIMEZONES.iter().enumerate() {
        // SET, not `SET LOCAL`: this is the shape a deployment has it in — a
        // session that was already in that timezone before the call ran.
        c.batch_execute(&format!("SET TIME ZONE '{tz}'"))
            .await
            .unwrap_or_else(|e| panic!("SET TIME ZONE '{tz}': {e}"));

        let mut all = probes(i);
        all.push(until_probe(&c, i).await);

        for p in &all {
            let got: Option<String> = c
                .query_one(&p.sql, &[])
                .await
                .unwrap_or_else(|e| panic!("probe {} under {tz}: {}", p.site, detail(&e)))
                .get(0);
            let Some(got) = got else {
                wrong.push(format!(
                    "  ✗ [{tz}] {}: rendered NULL (fixture missing?)",
                    p.site
                ));
                continue;
            };

            match p.expect {
                Expect::Exact(want) => {
                    if got != want {
                        wrong.push(format!("  ✗ [{tz}] {}: was {got:?}, want {want:?}", p.site));
                    }
                }
                Expect::NearNow(offset) => {
                    // Parsed by Postgres, not by hand: the string ends in "Z", so
                    // `::timestamptz` reads it as UTC whatever the session is —
                    // which is precisely the claim under test. A leak shows up
                    // here as a skew of whole hours.
                    let skew: f64 = c
                        .query_one(
                            "SELECT EXTRACT(EPOCH FROM ($1::text::timestamptz - now()))::float8",
                            &[&got],
                        )
                        .await
                        .unwrap_or_else(|e| panic!("skew of {} under {tz}: {}", p.site, detail(&e)))
                        .get(0);
                    if (skew - offset).abs() > TOL_SECS {
                        wrong.push(format!(
                            "  ✗ [{tz}] {}: was {got:?}, {skew:.1}s from now — expected \
                             within {TOL_SECS:.0}s of now{offset:+.2}s",
                            p.site
                        ));
                    }
                }
            }
        }
    }

    assert!(
        wrong.is_empty(),
        "a procedure renders a timestamp in the SESSION timezone and labels it Z, so a broker \
         on a non-UTC Postgres serves times hours out with nothing on the wire to say so. \
         Fix: AT TIME ZONE 'UTC' before to_char.\n{}",
        wrong.join("\n")
    );
}

// ---------------------------------------------------------------- source pin

/// Split a `to_char(` call into its first argument and its format string.
/// Tracks paren depth and single-quoted strings (with `''` escapes) rather than
/// matching a regex, because several calls nest function calls and array
/// subscripts, and four put the format string on the following line.
fn split_to_char(src: &str, open: usize) -> Option<(&str, &str)> {
    let b = src.as_bytes();
    let (mut i, mut depth, mut in_str, mut comma) = (open + 1, 1usize, false, None);
    while i < b.len() {
        let c = b[i];
        if in_str {
            if c == b'\'' {
                if b.get(i + 1) == Some(&b'\'') {
                    i += 2;
                    continue;
                }
                in_str = false;
            }
        } else if c == b'\'' {
            in_str = true;
        } else if c == b'(' {
            depth += 1;
        } else if c == b')' {
            depth -= 1;
            if depth == 0 {
                return comma.map(|k| (&src[open + 1..k], &src[k + 1..i]));
            }
        } else if c == b',' && depth == 1 && comma.is_none() {
            comma = Some(i);
        }
        i += 1;
    }
    None
}

/// True when the expression has a top-level operator, so `AT TIME ZONE` — which
/// binds TIGHTER than `+` — would attach to the wrong operand without parens.
fn has_top_level_operator(arg: &str) -> bool {
    let b = arg.as_bytes();
    let (mut i, mut depth, mut in_str) = (0usize, 0i32, false);
    while i < b.len() {
        let c = b[i];
        if in_str {
            if c == b'\'' {
                if b.get(i + 1) == Some(&b'\'') {
                    i += 2;
                    continue;
                }
                in_str = false;
            }
        } else if c == b'\'' {
            in_str = true;
        } else if c == b'(' {
            depth += 1;
        } else if c == b')' {
            depth -= 1;
        } else if depth == 0 && matches!(c, b'+' | b'-' | b'*' | b'/' | b'|') {
            return true;
        }
        i += 1;
    }
    false
}

/// Every file that can carry a `to_char`: the procedure SQL, and the Rust that
/// embeds SQL of its own (`src/db.rs` renders a segment stamp inline).
fn sql_bearing_files() -> Vec<(String, String)> {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut out = Vec::new();
    let mut stack = vec![root.join("sql"), root.join("src")];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue;
        };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if matches!(
                p.extension().and_then(|x| x.to_str()),
                Some("sql") | Some("rs")
            ) {
                if let Ok(s) = std::fs::read_to_string(&p) {
                    out.push((p.display().to_string(), s));
                }
            }
        }
    }
    out.sort();
    out
}

/// No `to_char` may render a timestamp under a format string ending in a literal
/// `"Z"` without first converting it with `AT TIME ZONE 'UTC'` — and where the
/// argument has a top-level operator, that conversion must be parenthesised.
///
/// File text rather than behaviour, because behaviour can only reach the sites a
/// test manages to drive: this covers every site in the tree, including the ones
/// behind branches no fixture reaches, and the ones added after today.
#[test]
fn source_pin_every_z_render_converts_to_utc() {
    let mut bad: Vec<String> = Vec::new();
    let mut checked = 0usize;

    for (path, src) in sql_bearing_files() {
        let lower = src.to_ascii_lowercase();
        let mut from = 0usize;
        while let Some(rel) = lower[from..].find("to_char(") {
            let open = from + rel + "to_char".len();
            from = open + 1;
            let Some((arg, fmt)) = split_to_char(&src, open) else {
                continue;
            };
            // A format without a literal "Z" makes no UTC claim, so it is not
            // this pin's business (`to_char(x,'YYYY-MM')` and friends).
            if !fmt.contains("\"Z\"") {
                continue;
            }
            checked += 1;
            let line = src[..open].matches('\n').count() + 1;
            let has_attz = arg.to_ascii_lowercase().contains("at time zone");
            if !has_attz {
                bad.push(format!(
                    "  ✗ {path}:{line}\n      to_char({}, …\"Z\") renders a timestamptz in the \
                     SESSION timezone and labels it UTC",
                    arg.trim()
                ));
            } else if has_top_level_operator(arg) && !arg.trim().starts_with('(') {
                bad.push(format!(
                    "  ✗ {path}:{line}\n      to_char({}, …) — AT TIME ZONE binds tighter than \
                     `+`, so this attaches to the wrong operand; parenthesise the expression",
                    arg.trim()
                ));
            }
        }
    }

    assert!(
        checked > 0,
        "found no to_char with a literal \"Z\" format — this pin has stopped looking at anything, \
         which is a broken test, not a clean tree"
    );
    assert!(
        bad.is_empty(),
        "{} of {checked} \"Z\"-format to_char renders do not convert to UTC first. \
         `to_char(timestamptz, …)` formats in the SESSION's TimeZone, so on any Postgres that \
         is not UTC these emit local time wearing a UTC label.\n{}",
        bad.len(),
        bad.join("\n")
    );
    println!("source pin: {checked} \"Z\"-format to_char renders, all convert to UTC first");
}
