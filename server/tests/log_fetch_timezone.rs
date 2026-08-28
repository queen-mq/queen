//! `queen.log_fetch_bin_v1` renders segment timestamps in UTC on ANY Postgres
//! (PLAN_QUEEN_KAFKA.md core change C2).
//!
//! `queen.log_segments.created_at` is `TIMESTAMPTZ` and the meta JSON renders it
//! with `to_char(..., 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')` — a format string that
//! ENDS IN A LITERAL Z. `to_char` on a timestamptz first converts to the
//! SESSION's `TimeZone`, so on any connection that is not UTC the value is local
//! time wearing a UTC label. The session timezone is not this broker's to choose:
//! it comes from `PGTZ`, from `postgresql.conf`, or from
//! `ALTER ROLE ... SET timezone` on the role the pool connects as, and a managed
//! Postgres set to the account's region is an ordinary deployment.
//!
//! What that costs is not cosmetic. `server/src/handlers/fetch.rs` parses the
//! string straight back into epoch milliseconds, and that number becomes the
//! record timestamp a Kafka consumer reads (`queen-kafka`'s `records.rs`), so
//! every record served from such a broker would carry a timestamp hours out with
//! nothing on the wire to say so — silently wrong, in the one field a consumer
//! cannot cross-check.
//!
//! The fix is `AT TIME ZONE 'UTC'` before `to_char`, which yields a plain
//! `TIMESTAMP` already in UTC. This file is the pin: one segment at a known
//! instant, read back under three session timezones, and the three answers must
//! be the same string.
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test` —
//! the convention `kv_sql_helpers.rs` and `hotlist_repairs.rs` already follow,
//! including booting the real broker rather than running psql: the SQL is
//! `include_str!`-embedded, so a broker boot is the only thing that proves the
//! file on disk is the one the binary carries.
//!
//! ```bash
//! docker run --rm -d --name queen-fetchtz-pg -e POSTGRES_PASSWORD=postgres -p 5473:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5473 cargo test --test log_fetch_timezone -- --ignored --nocapture
//! ```

use queen::{Broker, BrokerConfig};

/// The instant the one segment is stamped with, written with an explicit offset
/// so the literal itself cannot depend on the session.
const CREATED_AT: &str = "TIMESTAMPTZ '2026-08-28 10:00:00+00'";

/// What the meta JSON must say, whatever the session timezone is.
const RENDERED: &str = "2026-08-28T10:00:00.000000Z";

/// Three sessions: UTC (where the bug is invisible), one well east of it and one
/// well west, so a fault in either direction fails rather than only one.
const TIMEZONES: [&str; 3] = ["UTC", "Asia/Tokyo", "America/Los_Angeles"];

/// The default tenant every other test in this repo uses.
const TENANT: &str = "00000000-0000-0000-0000-000000000001";

#[tokio::test]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn segment_timestamps_are_utc_whatever_the_session_timezone_is() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    // The real broker, purely to apply the real (embedded) schema. Background
    // loops off: this test reads nothing they write.
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

    // One queue, one lane, one segment, seeded directly: the push path is not
    // what is under test and going through it would make the timestamp `now()`.
    c.batch_execute(&format!(
        "
        DELETE FROM queen.queues WHERE name = 'fetch-tz';
        INSERT INTO queen.queues (name, tenant_id) VALUES ('fetch-tz', '{TENANT}');
        INSERT INTO queen.log_partitions (queue_id, name, last_offset, log_start)
            SELECT id, '0', 0, 0 FROM queen.queues WHERE name = 'fetch-tz';
        INSERT INTO queen.log_segments (partition_id, base_offset, end_offset, created_at, blob)
            SELECT p.id, 0, 0, {CREATED_AT}, '\\x00'::bytea
            FROM queen.log_partitions p
            JOIN queen.queues q ON q.id = p.queue_id
            WHERE q.name = 'fetch-tz';
        "
    ))
    .await
    .expect("seed one segment");

    let mut wrong: Vec<String> = Vec::new();
    for tz in TIMEZONES {
        // SET, not `SET LOCAL`: this is the shape a deployment has it in — a
        // session that was already in that timezone before the fetch ran.
        c.batch_execute(&format!("SET TIME ZONE '{tz}'"))
            .await
            .unwrap_or_else(|e| panic!("SET TIME ZONE '{tz}': {e}"));

        // The one field, dug out in SQL and returned as text: the tenant is a
        // literal because this crate has no `uuid` dependency to bind one with,
        // and `->>` avoids needing a JSON `FromSql` for the same reason.
        let got: Option<String> = c
            .query_one(
                &format!(
                    "SELECT (queen.log_fetch_bin_v1(
                         ARRAY['fetch-tz'], ARRAY['0'], ARRAY[0::bigint], ARRAY[1048576],
                         67108864::bigint, 10000, '{TENANT}'::uuid)).meta
                     -> 'entries' -> 0 -> 'segments' -> 0 ->> 'createdAt'"
                ),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("log_fetch_bin_v1 under {tz}: {e}"))
            .get(0);

        let got = got.unwrap_or_else(|| "<absent>".to_string());
        if got != RENDERED {
            wrong.push(format!(
                "  ✗ TimeZone={tz}: createdAt was {got:?}, not {RENDERED:?}"
            ));
        }
    }

    assert!(
        wrong.is_empty(),
        "log_fetch_bin_v1 renders a segment's created_at in the SESSION timezone and \
         labels it Z, so a broker on a non-UTC Postgres serves every Kafka record with \
         a timestamp hours out. Fix: AT TIME ZONE 'UTC' before to_char.\n{}",
        wrong.join("\n")
    );
}
