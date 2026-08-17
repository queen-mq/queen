//! The 42P22 verification (PLAN_KV_TIMERS.md §15 row 3, §2.4 C3) — an exit
//! criterion of F1, and the one item in that phase the plan explicitly says must
//! be MEASURED and not assumed: *"Zero precedente in casa significa zero
//! certezza: si prova, non si assume."*
//!
//! `grep -rn COLLATE server/sql/` returns nothing today, so `queen.kv` and
//! `queen.log_timers` are the first tables in this schema whose name columns
//! carry an explicit `COLLATE "C"`. C3 pins that collation because the ordering
//! of `(namespace, key)` IS the intra-space lock order of §2.3: under the
//! database default, two pods with different `lc_collate` — or two ICU minors,
//! which is the real case in a rolling base-image upgrade — order `saga:A_1` and
//! `saga:A-1` oppositely, two overlapping bundles take the rows in inverted
//! order, and the result is a 40P01 that reproduces only on some installations
//! and only for some keys.
//!
//! The open question C3 leaves is whether a `COLLATE "C"` column can be compared
//! against a plain `text` parameter at all, or whether every comparison has to
//! spell the collation out (`k.key >= p_prefix COLLATE "C"`, inside
//! `kv_prefix_end_v1` included). This file answers it by running every shape the
//! SPs of §6.1 and §6.2 actually write.
//!
//! COLLATION IS RESOLVED AT PARSE TIME, so `prepare` is the whole test: it needs
//! no rows, writes nothing, and cannot be fooled by an empty table.
//!
//! TWO SQLSTATES, NOT ONE. The plan names 42P22 (`indeterminate_collation`,
//! "could not determine which collation to use"). PostgreSQL 16 also raises
//! 42P21 (`collation_mismatch`, "collation mismatch between implicit collations")
//! for the neighbouring case, and the fix is identical. Both are failures here.
//!
//! Needs a throwaway Postgres, so it is `#[ignore]` for a plain `cargo test`:
//!
//! ```bash
//! docker run --rm -d --name queen-kvt-pg -e POSTGRES_PASSWORD=postgres -p 5471:5432 postgres:16-alpine
//! QUEEN_EMBEDDED_TEST_PG=localhost:5471 cargo test --test kv_collation_42p22 -- --ignored --nocapture
//! ```

use queen::{Broker, BrokerConfig};

fn sqlstate(e: &tokio_postgres::Error) -> String {
    e.code()
        .map(|c| c.code().to_string())
        .unwrap_or_else(|| "<no sqlstate>".to_string())
}

/// `Display` for a tokio_postgres error is the useless string "db error"; the
/// server's own message lives on the DbError, and here it is the whole verdict.
fn detail(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(d) => format!("{}: {}", sqlstate(e), d.message()),
        None => format!("{}: {e}", sqlstate(e)),
    }
}

fn is_collation_error(code: &str) -> bool {
    code == "42P22" || code == "42P21"
}

/// One statement shape taken from the SP surface, with the section that writes it.
struct Form {
    id: &'static str,
    sql: &'static str,
    why: &'static str,
}

const FORMS: &[Form] = &[
    // ------------------------------------------------- queen.kv, read path (§5.5)
    Form {
        id: "prefix lower bound: column >= text parameter",
        sql: "SELECT 1 FROM queen.kv k WHERE k.key >= $1::text",
        why: "§5.5: the range clause `AND k.key >= p_prefix` — the exact comparison C3 says to \
              verify, column COLLATE \"C\" against a text parameter",
    },
    Form {
        id: "prefix upper bound: column < text parameter",
        sql: "SELECT 1 FROM queen.kv k WHERE k.key < $1::text",
        why: "§5.5: `AND (v_end IS NULL OR k.key < v_end)`, where v_end is the text result of \
              kv_prefix_end_v1 — a local, not a column, so it carries no collation of its own",
    },
    Form {
        id: "prefix bound against kv_prefix_end_v1 directly",
        sql: "SELECT 1 FROM queen.kv k WHERE k.key < queen.kv_prefix_end_v1($1::text)",
        why: "§2.4 C3: the helper's own result compared against the column — the form C3 singles \
              out (\"incluso dentro kv_prefix_end_v1\")",
    },
    Form {
        id: "starts_with(column, text parameter)",
        sql: "SELECT 1 FROM queen.kv k WHERE starts_with(k.key, $1::text)",
        why: "§5.5: starts_with IS the semantics; the range is only the index driver. It must \
              compile against a COLLATE \"C\" column or the whole prefix predicate has no home",
    },
    Form {
        id: "the full §5.5 prefix triple, ordered and limited",
        sql: "SELECT k.key, k.value FROM queen.kv k \
              WHERE k.tenant_id = $1::uuid AND k.namespace = $2::text \
                AND k.key >= $3::text \
                AND (queen.kv_prefix_end_v1($3::text) IS NULL OR k.key < queen.kv_prefix_end_v1($3::text)) \
                AND starts_with(k.key, $3::text) \
              ORDER BY k.namespace, k.key LIMIT $4::int",
        why: "§5.5: the predicate written out as the plan writes it, with the ORDER BY that must \
              match the index so there is no Sort node",
    },
    Form {
        id: "keyset cursor: column > text parameter",
        sql: "SELECT 1 FROM queen.kv k WHERE k.namespace = $1::text AND k.key > $2::text \
              ORDER BY k.key LIMIT 1",
        why: "§5.5: `after` is an exclusive keyset cursor, stable under COLLATE \"C\" and aligned \
              with the index order",
    },
    Form {
        id: "get: equality against text parameters",
        sql: "SELECT 1 FROM queen.kv k WHERE k.tenant_id = $1::uuid AND k.namespace = $2::text \
              AND k.key = $3::text",
        why: "§5.5 get. Equality on text does not consult a collation for deterministic \
              collations, so this is the shape most likely to pass while a neighbouring \
              inequality fails — which is why both are here",
    },
    Form {
        id: "getMany: column = ANY(text[])",
        sql: "SELECT 1 FROM queen.kv k WHERE k.namespace = $1::text AND k.key = ANY($2::text[])",
        why: "§5.5 getMany, the explicit-keys form — the one the pop rider will accept when it \
              returns (§18.1)",
    },
    Form {
        id: "getMany: column = ANY(array built from jsonb)",
        sql: "SELECT 1 FROM queen.kv k WHERE k.namespace = $1::text \
              AND k.key = ANY(ARRAY(SELECT jsonb_array_elements_text($2::jsonb)))",
        why: "§6.1: the op arrives as JSONB, so the keys are text derived from jsonb rather than \
              a bound text[] — a different collation derivation for the same comparison",
    },
    // ------------------------------------- queen.kv, wire path (§6.1, §2.4 C3)
    Form {
        id: "column = jsonb extraction",
        sql: "SELECT 1 FROM queen.kv k, jsonb_array_elements($1::jsonb) o \
              WHERE k.namespace = (o.value->>'ns') AND k.key = (o.value->>'key')",
        why: "§2.4 C3 note: `op->>'key'` does NOT inherit the column's collation. This is the \
              join the wire's step 0 writes for every op in the batch",
    },
    Form {
        id: "column < jsonb extraction (inequality, not equality)",
        sql: "SELECT 1 FROM queen.kv k, jsonb_array_elements($1::jsonb) o \
              WHERE k.key < (o.value->>'key')",
        why: "the inequality version of the same pair — equality can dodge collation resolution \
              for deterministic collations, an ordering comparison cannot",
    },
    Form {
        id: "ORDER BY on jsonb extraction with explicit COLLATE \"C\"",
        sql: "SELECT o.value->>'ns', o.value->>'key' FROM jsonb_array_elements($1::jsonb) o \
              ORDER BY (o.value->>'ns') COLLATE \"C\", (o.value->>'key') COLLATE \"C\"",
        why: "§2.4 C3 piece 2, verbatim: the apply order of §6.1 point 2. The explicit COLLATE is \
              the correction — this form must compile, because it is the ONLY thing making the \
              intra-space lock order of §2.3 the same on every pod",
    },
    Form {
        id: "apply order: CTE of jsonb ops joined to the table, ordered COLLATE \"C\"",
        sql: "WITH ops AS (
                  SELECT (o.value->>'ns')  COLLATE \"C\" AS ns,
                         (o.value->>'key') COLLATE \"C\" AS key,
                         o.ordinality       AS ord
                    FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY o
              )
              SELECT ops.ord, k.version
                FROM ops LEFT JOIN queen.kv k
                  ON k.tenant_id = $2::uuid AND k.namespace = ops.ns AND k.key = ops.key
               ORDER BY ops.ns, ops.key",
        why: "§6.1 points 2 and 6.4: apply in ascending (namespace, key), report by input \
              ordinal. The CTE columns carry the collation forward — if that derivation \
              conflicts with the table's, this is where it shows",
    },
    Form {
        id: "ON CONFLICT on the full PK with a conditional DO UPDATE",
        sql: "INSERT INTO queen.kv AS k (tenant_id, namespace, key, value, version, expires_at) \
              VALUES ($1::uuid, $2::text, $3::text, $4::jsonb, nextval('queen.kv_version_seq'), $5::timestamptz) \
              ON CONFLICT (tenant_id, namespace, key) DO UPDATE \
                 SET value = EXCLUDED.value, version = EXCLUDED.version, \
                     expires_at = EXCLUDED.expires_at, updated_at = $6::timestamptz \
               WHERE NOT queen.kv_live_v1(k.expires_at, $6::timestamptz)",
        why: "§5.3 expect:0. The conflict target is the COMPLETE PK (§3.4: a unique index on \
              (namespace, key) alone would make a conflict cross-tenant), and index inference \
              has to resolve against COLLATE \"C\" columns",
    },
    Form {
        id: "UPDATE ... WHERE with a locking row-lock companion",
        sql: "SELECT 1 FROM queen.kv k \
              WHERE k.tenant_id = $1::uuid AND k.namespace = $2::text AND k.key = $3::text \
              ORDER BY k.namespace, k.key FOR UPDATE",
        why: "§2.3: the intra-space order of queen.kv is ascending (namespace, key) with an \
              explicit COLLATE \"C\"; this is the shape that takes the locks in it",
    },
    Form {
        id: "sweeper prune: shard range scan with SKIP LOCKED",
        sql: "SELECT k.tenant_id, k.namespace, k.key FROM queen.kv k \
              WHERE k.shard = $1::smallint AND k.expires_at IS NOT NULL AND k.expires_at <= $2::timestamptz \
              ORDER BY k.namespace, k.key LIMIT $3::int FOR UPDATE SKIP LOCKED",
        why: "§2.3 actor 7 and §7: the pruner never waits, and it still visits rows in the \
              declared intra-space order",
    },
    // ------------------------------------------------- queen.log_timers (§6.2)
    Form {
        id: "timers: PK equality against text parameters",
        sql: "SELECT 1 FROM queen.log_timers t \
              WHERE t.tenant_id = $1::uuid AND t.queue = $2::text AND t.timer_key = $3::text",
        why: "§6.2: cancel and peek address by the full PK, keyed by NAMES (§1.2)",
    },
    Form {
        id: "timers: ORDER BY (queue, timer_key) FOR UPDATE",
        sql: "SELECT 1 FROM queen.log_timers t WHERE t.tenant_id = $1::uuid \
              ORDER BY t.queue, t.timer_key FOR UPDATE",
        why: "§2.3: intra-space order for the wire, ascending (queue, timer_key) at fixed tenant",
    },
    Form {
        id: "timers: ORDER BY (tenant_id, queue, timer_key) — the sweeper's multi-tenant order",
        sql: "SELECT 1 FROM queen.log_timers t WHERE t.shard = $1::smallint \
              ORDER BY t.tenant_id, t.queue, t.timer_key FOR UPDATE SKIP LOCKED",
        why: "§2.3: the asymmetry the plan says to write down — the sweeper's batch is \
              multi-tenant so it orders by three columns, the wire by two at fixed tenant. \
              Restricted to the rows they can contend for, the two orders coincide",
    },
    Form {
        id: "timers: upsert on the full PK with the claim guard",
        sql: "INSERT INTO queen.log_timers AS t \
                  (tenant_id, queue, timer_key, partition, deliver_at, txn, message_id, payload) \
              VALUES ($1::uuid, $2::text, $3::text, $4::text, $5::timestamptz, $6::text, $7::uuid, $8::bytea) \
              ON CONFLICT (tenant_id, queue, timer_key) DO UPDATE \
                 SET deliver_at = EXCLUDED.deliver_at, txn = EXCLUDED.txn, \
                     message_id = EXCLUDED.message_id, payload = EXCLUDED.payload, \
                     attempts = 0, last_error = NULL, claimed_until = NULL, claim_token = NULL, \
                     updated_at = $9::timestamptz \
               WHERE t.claim_token IS NULL OR t.claimed_until <= $9::timestamptz \
              RETURNING (xmax = 0) AS inserted",
        why: "§6.2: schedule and reschedule are the SAME upsert; `xmax = 0` is what separates \
              scheduled from rescheduled under one lock",
    },
    Form {
        id: "timers: claim ordered by the generated visible_at",
        sql: "SELECT t.tenant_id::text, t.queue, t.timer_key FROM queen.log_timers t \
              WHERE t.shard = $1::smallint AND t.visible_at <= $2::timestamptz \
              ORDER BY t.visible_at LIMIT $3::int FOR UPDATE SKIP LOCKED",
        why: "§6.2 claim: a range scan in expiry order per shard with ZERO rows filtered — the \
              reason visible_at is GENERATED instead of a runtime predicate",
    },
    Form {
        id: "timers joined to kv (one statement touching both new tables)",
        sql: "SELECT 1 FROM queen.log_timers t JOIN queen.kv k ON k.tenant_id = t.tenant_id \
              WHERE t.queue = $1::text AND k.namespace = $2::text ORDER BY k.namespace, k.key",
        why: "the two tables were given COLLATE \"C\" independently; a statement that names both \
              is where a divergence between them would surface",
    },
];

/// The fallback C3 prescribes if any form above raises: spell the collation out.
/// Pinned so that when the answer is "it raises", the correction is known to
/// compile rather than being the next thing to discover.
const EXPLICIT_FALLBACKS: &[Form] = &[
    Form {
        id: "fallback: column >= parameter COLLATE \"C\"",
        sql: "SELECT 1 FROM queen.kv k WHERE k.key >= $1::text COLLATE \"C\"",
        why: "§2.4 C3: \"ogni confronto va scritto k.key >= p_prefix COLLATE \\\"C\\\"\"",
    },
    Form {
        id: "fallback: column = jsonb extraction COLLATE \"C\"",
        sql: "SELECT 1 FROM queen.kv k, jsonb_array_elements($1::jsonb) o \
              WHERE k.key = ((o.value->>'key') COLLATE \"C\")",
        why: "§2.4 C3: the jsonb-extraction comparison with the collation pinned on the \
              non-column side",
    },
];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Postgres; set QUEEN_EMBEDDED_TEST_PG and run with --ignored"]
async fn collate_c_columns_compare_against_text_without_42p22() {
    let target = std::env::var("QUEEN_EMBEDDED_TEST_PG")
        .expect("QUEEN_EMBEDDED_TEST_PG must be set (host:port)");
    let (host, port) = target
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
        .unwrap_or((target.clone(), 5432));

    let broker = Broker::start(
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

    let mut failures: Vec<String> = Vec::new();
    let mut notes: Vec<String> = Vec::new();

    positive_control(&c, &mut failures, &mut notes).await;
    columns_carry_collate_c(&c, &mut failures).await;

    for f in FORMS.iter().chain(EXPLICIT_FALLBACKS.iter()) {
        match c.prepare(f.sql).await {
            Ok(_) => {}
            Err(e) => {
                let code = sqlstate(&e);
                let verdict = if is_collation_error(&code) {
                    "COLLATION CONFLICT — this form needs an explicit COLLATE \"C\" (§2.4 C3)"
                } else {
                    "did not compile"
                };
                failures.push(format!(
                    "  ✗ {}\n      plan: {}\n      {verdict}: {}",
                    f.id, f.why, detail(&e)
                ));
            }
        }
    }

    broker.shutdown().await;

    if !failures.is_empty() {
        panic!(
            "\nPLAN_KV_TIMERS §15 row 3 / §2.4 C3 — 42P22 verification on the rig\n{}\n\n{} \
             failing form(s).{}\n\nBefore F1 every form fails with 42P01/42883 (queen.kv, \
             queen.log_timers and the helpers do not exist yet) — that is the expected state. \
             After F1, any 42P21/42P22 above is C3's answer being \"yes, it raises\": rewrite \
             THAT form with an explicit COLLATE \"C\", kv_prefix_end_v1 included, and re-run.\n",
            failures.join("\n"),
            failures.len(),
            if notes.is_empty() {
                String::new()
            } else {
                format!("\n\nnotes:\n{}", notes.join("\n"))
            }
        );
    }
}

/// A form that MUST raise a collation error, so "nothing raised" above is a
/// result and not a vacuum. Without it, a check that only ever asserts absence
/// would still pass on a server where collation resolution had been defeated
/// some other way.
async fn positive_control(
    c: &tokio_postgres::Client,
    failures: &mut Vec<String>,
    notes: &mut Vec<String>,
) {
    // "unicode" is the ICU root collation, present in any PG >= 15 built with
    // ICU. If it is absent the control cannot run; that is reported, not
    // silently skipped.
    let have: bool = c
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_collation WHERE collname = 'unicode')",
            &[],
        )
        .await
        .expect("collation probe")
        .get(0);
    if !have {
        notes.push(
            "  ! positive control skipped: no \"unicode\" collation on this server, so the \
             \"no conflict\" verdict below is unwitnessed"
                .to_string(),
        );
        return;
    }

    c.batch_execute(
        "CREATE TEMP TABLE kvt_collation_control (a text COLLATE \"C\", b text COLLATE \"unicode\")",
    )
    .await
    .expect("temp control table");

    match c
        .prepare("SELECT a FROM kvt_collation_control ORDER BY a || b")
        .await
    {
        Ok(_) => failures.push(
            "  ✗ positive control did not raise\n      plan: §15 row 3 is only meaningful if \
             this server DOES reject conflicting implicit collations; it accepted two, so every \
             \"no conflict\" verdict in this file is vacuous"
                .to_string(),
        ),
        Err(e) => {
            let code = sqlstate(&e);
            if !is_collation_error(&code) {
                failures.push(format!(
                    "  ✗ positive control raised the wrong error\n      plan: expected 42P21 or \
                     42P22 from two conflicting implicit collations\n      got: {}",
                    detail(&e)
                ));
            } else {
                notes.push(format!(
                    "  · positive control OK: conflicting implicit collations raise {code} on \
                     this server, so the verdicts above are witnessed"
                ));
            }
        }
    }
}

/// C3 piece 1: the collation is on all FOUR name columns, or the ordering
/// argument the whole of §2 rests on is only true for some of them.
async fn columns_carry_collate_c(c: &tokio_postgres::Client, failures: &mut Vec<String>) {
    let want = [
        ("kv", "namespace"),
        ("kv", "key"),
        ("log_timers", "queue"),
        ("log_timers", "timer_key"),
    ];
    for (table, column) in want {
        let row = c
            .query_opt(
                "SELECT coalesce(co.collname, '<none>')
                   FROM pg_attribute a
                   JOIN pg_class    t ON t.oid = a.attrelid
                   JOIN pg_namespace n ON n.oid = t.relnamespace
              LEFT JOIN pg_collation co ON co.oid = a.attcollation
                  WHERE n.nspname = 'queen' AND t.relname = $1 AND a.attname = $2
                    AND a.attnum > 0 AND NOT a.attisdropped",
                &[&table, &column],
            )
            .await
            .expect("attcollation lookup");

        match row {
            None => failures.push(format!(
                "  ✗ queen.{table}.{column}: column does not exist\n      plan: §3.2/§3.3 define \
                 it with COLLATE \"C\""
            )),
            Some(r) => {
                let coll: String = r.get(0);
                if coll != "C" {
                    failures.push(format!(
                        "  ✗ queen.{table}.{column}: collation is {coll:?}, want \"C\"\n      \
                         plan: §2.4 C3 piece 1 — all FOUR name columns. Under the database \
                         default, two pods with different lc_collate order two keys oppositely, \
                         two overlapping bundles take the rows in inverted order, and the 40P01 \
                         reproduces only on some installations and only for some keys"
                    ));
                }
            }
        }
    }
}
