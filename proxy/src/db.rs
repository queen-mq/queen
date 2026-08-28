//! pxdb pool + migrations. OWNER: Agent B (schema task). The skeleton only
//! builds the pool; migrations list is filled by Agent B (embedded include_str!,
//! applied in order, recorded in queen_proxy.schema_migrations).

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use tokio_postgres::NoTls;

use crate::config::PxdbConfig;

/// tokio_postgres::Error's own Display is unhelpfully terse for server-side
/// failures: `Kind::Db => "db error"`, full stop -- the actual severity,
/// message, DETAIL/HINT live on the nested `DbError` (via `.as_db_error()`),
/// whose Display carries them. Every `{e}`/`e.to_string()` in this file goes
/// through one of these instead so a migration failure is actually
/// diagnosable.
fn describe_pg_error(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db_err) => db_err.to_string(),
        None => e.to_string(),
    }
}

/// Same, for errors from `pool.get()` (deadpool wraps the backend error in
/// `PoolError::Backend`; the other variants -- Timeout/Closed/... -- have no
/// nested DbError and just use their own Display).
fn describe_pool_error(e: &deadpool_postgres::PoolError) -> String {
    match e {
        deadpool_postgres::PoolError::Backend(db_e) => describe_pg_error(db_e),
        other => other.to_string(),
    }
}

pub async fn create_pool(cfg: &PxdbConfig) -> Result<Pool, String> {
    let timeout = cfg.timeout();
    let mut pg = tokio_postgres::Config::new();
    pg.host(&cfg.host)
        .port(cfg.port)
        .user(&cfg.user)
        .password(&cfg.password)
        .dbname(&cfg.dbname)
        .application_name("queen-proxy")
        .connect_timeout(timeout);
    let mgr_cfg = ManagerConfig { recycling_method: RecyclingMethod::Fast };
    let mgr = if cfg.use_ssl {
        // `PXDB_SSL_ROOT_CERT` was already validated in `Config::load`, so the
        // error arm is unreachable here — and it is an error rather than a
        // fall-back for the same reason it is fatal there: a CA that will not
        // parse must never quietly become "trust the Mozilla set", which would
        // accept a different certificate than the operator authorised.
        let connector =
            crate::pgtls::make_connector(cfg.ssl_reject_unauthorized, cfg.ssl_root_cert.as_deref())
                .map_err(|e| format!("PXDB_SSL_ROOT_CERT: {e}"))?;
        Manager::from_config(pg, connector, mgr_cfg)
    } else {
        Manager::from_config(pg, NoTls, mgr_cfg)
    };
    // Every wait is bounded (PxdbConfig::timeout). `wait` is the one that
    // matters under load: with the pool saturated a caller now gets a
    // PoolError::Timeout it already knows how to degrade on -- stale-serve,
    // spool, skip the cycle -- instead of queueing behind the saturation.
    // deadpool drives its timeouts off a runtime, hence `Runtime::Tokio1`
    // (BuildError::NoRuntimeSpecified otherwise).
    let pool = Pool::builder(mgr)
        .max_size(cfg.pool_size)
        .runtime(Runtime::Tokio1)
        .wait_timeout(Some(timeout))
        .create_timeout(Some(timeout))
        .recycle_timeout(Some(timeout))
        .build()
        .map_err(|e| e.to_string())?;
    // fail fast on unreachable pxdb at boot
    let client = pool.get().await.map_err(|e| format!("pxdb connect: {}", describe_pool_error(&e)))?;
    client
        .simple_query("SELECT 1")
        .await
        .map_err(|e| format!("pxdb ping: {}", describe_pg_error(&e)))?;
    Ok(pool)
}

/// Ordered embedded migrations, applied in order and recorded by name in
/// queen_proxy.schema_migrations (apply_migrations below skips ones already
/// applied). SQL is include_str!-embedded, so `cargo build` after editing a
/// migration file before runtime-testing it (same gotcha as server/).
#[rustfmt::skip]
pub fn migrations() -> Vec<(&'static str, &'static str)> {
    vec![
        ("001_init", include_str!("../migrations/001_init.sql")),
        ("002_functions", include_str!("../migrations/002_functions.sql")),
        ("003_limit_override", include_str!("../migrations/003_limit_override.sql")),
        ("004_lifecycle", include_str!("../migrations/004_lifecycle.sql")),
        ("005_usage_prune", include_str!("../migrations/005_usage_prune.sql")),
        ("006_operator", include_str!("../migrations/006_operator.sql")),
        ("007_tenant_delete", include_str!("../migrations/007_tenant_delete.sql")),
        ("008_bootstrap_password", include_str!("../migrations/008_bootstrap_password.sql")),
    ]
}

pub async fn apply_migrations(pool: &Pool) -> Result<(), String> {
    let client = pool.get().await.map_err(|e| describe_pool_error(&e))?;
    client
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS queen_proxy;
             CREATE TABLE IF NOT EXISTS queen_proxy.schema_migrations (
               name TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())",
        )
        .await
        .map_err(|e| describe_pg_error(&e))?;
    for (name, sql) in migrations() {
        let done = client
            .query_opt("SELECT 1 FROM queen_proxy.schema_migrations WHERE name=$1", &[&name])
            .await
            .map_err(|e| describe_pg_error(&e))?
            .is_some();
        if done {
            continue;
        }
        client
            .batch_execute(sql)
            .await
            .map_err(|e| format!("migration {name}: {}", describe_pg_error(&e)))?;
        client
            .execute("INSERT INTO queen_proxy.schema_migrations(name) VALUES ($1)", &[&name])
            .await
            .map_err(|e| describe_pg_error(&e))?;
        tracing::info!(migration = name, "applied");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The migration runner applies by NAME and skips any name already in
    /// `queen_proxy.schema_migrations`, so a duplicate name silently swallows a
    /// whole file and an out-of-order one applies a migration before the objects
    /// it depends on. Neither fails loudly at runtime -- the first looks like
    /// "already applied", the second like a missing table on a database nobody
    /// has re-created in months. Both are caught here instead.
    #[test]
    fn migration_names_are_unique_ordered_and_numbered() {
        let ms = migrations();
        let names: Vec<&str> = ms.iter().map(|(n, _)| *n).collect();

        let mut sorted = names.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), names.len(), "duplicate migration name in {names:?}");

        let mut last = 0u32;
        for name in &names {
            let (num, _) = name.split_once('_').unwrap_or_else(|| {
                panic!(
                    "migration {name} is not NNN_name -- the runner orders by this list \
                     while humans order by the filename, so the two must agree"
                )
            });
            let n: u32 = num
                .parse()
                .unwrap_or_else(|_| panic!("migration {name} has no numeric prefix"));
            assert!(n > last, "migration {name} is out of order after {last:03}");
            last = n;
        }

        for (name, sql) in &ms {
            assert!(!sql.trim().is_empty(), "migration {name} is empty -- a bad include_str! path");
        }
    }

    // ---------------------------------------------------------------- live
    //
    // UUIDs are bound as text and cast in SQL (`$1::text::uuid`), and JSONB is
    // read back as `::text`: tokio-postgres is built here without the `uuid` and
    // `serde_json` type features, which is the same reason auth.rs and cache.rs
    // spell their casts out.

    fn uuid_str(v: &serde_json::Value, field: &str) -> String {
        v[field]
            .as_str()
            .unwrap_or_else(|| panic!("{field} missing from {v}"))
            .to_string()
    }

    async fn json1(
        c: &deadpool_postgres::Client,
        sql: &str,
        args: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<serde_json::Value, tokio_postgres::Error> {
        let row = c.query_one(sql, args).await?;
        let txt: String = row.get(0);
        Ok(serde_json::from_str(&txt).expect("result is JSON"))
    }

    /// Every table under a tenant, counted through whatever FK chain reaches it.
    /// Returned as an ordered `(label, count)` list so two snapshots compare
    /// directly -- "B changed" is one assertion, not ten.
    const TENANT_TABLES: &[(&str, &str)] = &[
        ("tenants", "SELECT count(*) FROM queen_proxy.tenants WHERE id = $1::text::uuid"),
        ("users", "SELECT count(*) FROM queen_proxy.users WHERE tenant_id = $1::text::uuid"),
        (
            "identities",
            "SELECT count(*) FROM queen_proxy.identities i
               JOIN queen_proxy.users u ON u.id = i.user_id
              WHERE u.tenant_id = $1::text::uuid",
        ),
        ("clusters", "SELECT count(*) FROM queen_proxy.clusters WHERE tenant_id = $1::text::uuid"),
        (
            "cluster_roles",
            "SELECT count(*) FROM queen_proxy.cluster_roles r
               JOIN queen_proxy.clusters cl ON cl.id = r.cluster_id
              WHERE cl.tenant_id = $1::text::uuid",
        ),
        (
            "api_keys",
            "SELECT count(*) FROM queen_proxy.api_keys k
               JOIN queen_proxy.clusters cl ON cl.id = k.cluster_id
              WHERE cl.tenant_id = $1::text::uuid",
        ),
        (
            "queues",
            "SELECT count(*) FROM queen_proxy.queues q
               JOIN queen_proxy.clusters cl ON cl.id = q.cluster_id
              WHERE cl.tenant_id = $1::text::uuid",
        ),
        (
            "usage_minutes",
            "SELECT count(*) FROM queen_proxy.usage_minutes um
               JOIN queen_proxy.clusters cl ON cl.id = um.cluster_id
              WHERE cl.tenant_id = $1::text::uuid",
        ),
        (
            "usage_days",
            "SELECT count(*) FROM queen_proxy.usage_days ud
               JOIN queen_proxy.clusters cl ON cl.id = ud.cluster_id
              WHERE cl.tenant_id = $1::text::uuid",
        ),
        (
            "operations",
            "SELECT count(*) FROM queen_proxy.operations WHERE tenant_id = $1::text::uuid",
        ),
    ];

    async fn counts(c: &deadpool_postgres::Client, tenant: &String) -> Vec<(&'static str, i64)> {
        let mut out = Vec::with_capacity(TENANT_TABLES.len());
        for (label, sql) in TENANT_TABLES {
            let n: i64 = c.query_one(*sql, &[tenant]).await.expect(label).get(0);
            out.push((*label, n));
        }
        out
    }

    /// End-to-end for `queen_proxy.delete_tenant` (007_tenant_delete.sql).
    ///
    /// Needs a throwaway Postgres of its own -- it DROPs and re-applies the whole
    /// `queen_proxy` schema, so it must never be pointed at the dev cell's pxdb
    /// (:5465) or anything else that matters:
    ///
    /// ```bash
    /// docker run --rm -d --name queen-proxy-wipe-pg -e POSTGRES_PASSWORD=postgres \
    ///     -p 5489:5432 postgres:16-alpine
    /// QUEEN_PROXY_TEST_PG=127.0.0.1:5489 cargo test --bin queen-proxy \
    ///     db::tests::live_delete_tenant_against_real_postgres -- --ignored --nocapture
    /// ```
    ///
    /// (`--bin queen-proxy`, not `--lib`: this crate has no library target, so
    /// every unit test lives in the binary.)
    ///
    /// What it pins, in order:
    ///   * the two-step ordering is ENFORCED, not documented: a tenant that was
    ///     never marked `deleting` is refused, because a tenant whose traffic was
    ///     never stopped is a tenant whose cell data was purged while it was
    ///     still being written to;
    ///   * every table under the tenant is empty afterwards -- including
    ///     `operations`, whose RESTRICT foreign key is what made a hard delete
    ///     impossible before this migration, and whose `meta` carries email
    ///     addresses;
    ///   * a SECOND tenant on the same cell keeps every row;
    ///   * one `pg_notify('queen_proxy_inval')` per deleted cluster actually
    ///     reaches a listener. Not decoration: a missed invalidation is invisible
    ///     in SQL and lets a proxy keep serving a deleted tenant out of cache,
    ///     and this repository has already paid for that class of bug once
    ///     (cache.rs's `run_listen_session` header);
    ///   * the surviving audit is an `outbox` row carrying the
    ///     `broker_tenant_uuid`s, which exist nowhere else after the commit and
    ///     are the only thing that can name the cell-side data;
    ///   * a re-run answers `{"deleted": false, "existed": false}` instead of
    ///     raising, so a wipe loop is safe to retry after a partial failure.
    #[tokio::test]
    #[ignore = "requires a THROWAWAY postgres -- see doc comment (it drops the queen_proxy schema)"]
    async fn live_delete_tenant_against_real_postgres() {
        let target =
            std::env::var("QUEEN_PROXY_TEST_PG").unwrap_or_else(|_| "127.0.0.1:5489".to_string());
        let (host, port) = target
            .split_once(':')
            .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
            .unwrap_or((target.clone(), 5432));

        let pxcfg = crate::config::PxdbConfig {
            host: host.clone(),
            port,
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            dbname: "postgres".to_string(),
            use_ssl: false,
            ssl_reject_unauthorized: false,
            ssl_root_cert: None,
            pool_size: 4,
            timeout_ms: 5_000,
        };
        let pool = create_pool(&pxcfg).await.expect("connect to the throwaway postgres");

        {
            let client = pool.get().await.unwrap();
            client
                .batch_execute("DROP SCHEMA IF EXISTS queen_proxy CASCADE")
                .await
                .expect("clean slate");
        }
        // Applying the migrations here is also the only proof that the .sql on
        // disk is the one the binary carries -- it is include_str!-embedded, so a
        // psql script would happily pass against a stale `cargo build`.
        apply_migrations(&pool).await.expect("apply migrations");

        let c = pool.get().await.unwrap();

        // A listener on its own connection, wired exactly like cache.rs's:
        // batch_execute("LISTEN ...") only completes while something polls the
        // connection, so the polling task is spawned FIRST.
        let (ntx, mut nrx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let (nclient, mut nconn) = tokio_postgres::connect(
            &format!("host={host} port={port} user=postgres password=postgres dbname=postgres"),
            NoTls,
        )
        .await
        .expect("listener connect");
        tokio::spawn(async move {
            while let Some(msg) = std::future::poll_fn(|cx| nconn.poll_message(cx)).await {
                match msg {
                    Ok(tokio_postgres::AsyncMessage::Notification(n)) => {
                        let _ = ntx.send(n.payload().to_string());
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });
        nclient.batch_execute("LISTEN queen_proxy_inval").await.expect("LISTEN");

        // No SQL function creates a cell -- cells are fleet infrastructure,
        // provisioned by ops (001_init). A hand INSERT is the documented way.
        let cell: String = c
            .query_one(
                "INSERT INTO queen_proxy.cells(slug, region, base_url, class, cell_secret)
                 VALUES ('wipe-cell', 'local', 'http://127.0.0.1:6710', 'shared', 'secret')
                 RETURNING id::text",
                &[],
            )
            .await
            .expect("cell")
            .get(0);

        // Two tenants on one cell. B exists to prove the delete is scoped.
        let mut ids: Vec<(String, String)> = Vec::new();
        for slug in ["wipe-a", "wipe-b"] {
            let v = json1(
                &c,
                "SELECT (queen_proxy.bootstrap_tenant($1, $1, $1 || '-c', 'free',
                                                      $2::text::uuid,
                                                      $1 || '@example.test', 'pw'))::text",
                &[&slug, &cell],
            )
            .await
            .expect("bootstrap_tenant");
            assert!(v["api_key"].is_string(), "bootstrap returns the plaintext key once: {v}");
            ids.push((uuid_str(&v, "tenant_id"), uuid_str(&v, "cluster_id")));
        }
        let (tenant_a, cluster_a) = ids[0].clone();
        let (tenant_b, cluster_b) = ids[1].clone();

        // A and B are seeded IDENTICALLY -- a second key, an identity row, a
        // registry queue and two minutes of usage each. B being a mirror of A is
        // what makes "B is untouched" a real claim: a delete scoped by anything
        // other than the tenant would take a matching row out of B too.
        for (tenant, cluster) in [&ids[0], &ids[1]] {
            c.execute(
                "SELECT queen_proxy.issue_api_key($1::text::uuid, 'ci',
                                                  encode(public.digest($1, 'sha256'), 'hex'),
                                                  ARRAY['produce'])",
                &[cluster],
            )
            .await
            .expect("second key");
            c.execute(
                "INSERT INTO queen_proxy.queues(cluster_id, name, partitions_count)
                 VALUES ($1::text::uuid, 'orders', 4)",
                &[cluster],
            )
            .await
            .expect("registry queue");
            // Two minutes, one of them YESTERDAY: rollup_usage_days only folds
            // days that are already CLOSED, so a row dated now() would leave
            // usage_days empty and the cascade through it untested.
            c.execute(
                "INSERT INTO queen_proxy.usage_minutes(cluster_id, minute, op_class, msgs, reqs)
                 VALUES ($1::text::uuid, date_trunc('minute', now()), 'push', 10, 3),
                        ($1::text::uuid, date_trunc('minute', now() - interval '1 day'),
                         'push', 7, 2)",
                &[cluster],
            )
            .await
            .expect("usage");
            // Nothing in the control-plane contract writes `identities`
            // (001_init says so of create_user), so the cascade from `users` is
            // untested unless the test writes one itself.
            c.execute(
                "INSERT INTO queen_proxy.identities(user_id, provider, provider_id, email,
                                                    verified)
                 SELECT id, 'github', 'gh-' || id::text, email, true
                   FROM queen_proxy.users WHERE tenant_id = $1::text::uuid",
                &[tenant],
            )
            .await
            .expect("identity");
        }
        let rolled: i32 =
            c.query_one("SELECT queen_proxy.rollup_usage_days()", &[]).await.expect("rollup").get(0);
        assert!(rolled > 0, "the rollup must have produced usage_days rows to cascade");

        let a_before = counts(&c, &tenant_a).await;
        let b_before = counts(&c, &tenant_b).await;
        assert!(a_before.iter().all(|(_, n)| *n > 0), "A seeded everywhere: {a_before:?}");
        assert!(b_before.iter().all(|(_, n)| *n > 0), "B seeded everywhere: {b_before:?}");

        // --- the ordering is enforced, not merely documented ---------------
        let err = json1(
            &c,
            "SELECT (queen_proxy.delete_tenant($1::text::uuid))::text",
            &[&tenant_a],
        )
        .await
        .expect_err("an active tenant must be refused");
        let msg = err.as_db_error().map(|d| d.message().to_string()).unwrap_or_default();
        assert!(
            msg.contains("not deleting") && msg.contains("set_tenant_status"),
            "the refusal must name the step that was skipped, got: {msg}"
        );

        // --- step 1: stop the traffic --------------------------------------
        c.execute("SELECT queen_proxy.set_tenant_status($1::text::uuid, 'deleting')", &[&tenant_a])
            .await
            .expect("set_tenant_status");
        // Drain the invalidations set_tenant_status itself emits, so the ones
        // asserted below can only have come from delete_tenant.
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        while nrx.try_recv().is_ok() {}

        // --- step 3 (step 2 is the broker's, in the other database) --------
        let res = json1(&c, "SELECT (queen_proxy.delete_tenant($1::text::uuid))::text", &[&tenant_a])
            .await
            .expect("delete_tenant");
        println!("{res}");
        assert_eq!(res["deleted"], serde_json::json!(true), "{res}");
        assert_eq!(res["existed"], serde_json::json!(true));
        assert_eq!(res["status_was"], serde_json::json!("deleting"));
        assert_eq!(res["counts"]["clusters"], serde_json::json!(1));
        assert_eq!(res["counts"]["api_keys"], serde_json::json!(2), "bootstrap key + the ci one");
        assert_eq!(res["counts"]["api_keys_revoked"], serde_json::json!(2));
        assert!(res["counts"]["operations"].as_i64().unwrap_or(0) > 0, "{res}");

        let clusters = res["clusters"].as_array().expect("clusters array").clone();
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0]["cluster_id"], serde_json::json!(cluster_a));
        // The broker_tenant_uuid is the ONLY thing that can name the cell-side
        // data, and after this commit it exists nowhere else.
        assert_ne!(
            clusters[0]["broker_tenant_uuid"].as_str().unwrap(),
            tenant_a,
            "broker_tenant_uuid is NOT tenants.id -- a wipe that hands the wrong one to \
             queen.delete_tenant_data_v1 purges nothing and reports success"
        );

        // --- nothing of A's is left ----------------------------------------
        for (label, n) in counts(&c, &tenant_a).await {
            assert_eq!(n, 0, "queen_proxy.{label} still holds rows for the deleted tenant");
        }
        // --- and nothing of B's moved --------------------------------------
        assert_eq!(counts(&c, &tenant_b).await, b_before, "B changed");
        let b_cluster_live: i64 = c
            .query_one(
                "SELECT count(*) FROM queen_proxy.clusters WHERE id = $1::text::uuid",
                &[&cluster_b],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(b_cluster_live, 1);

        // --- the surviving audit -------------------------------------------
        let outbox = json1(
            &c,
            "SELECT payload::text FROM queen_proxy.outbox
              WHERE kind = 'tenant_deleted' AND payload->>'tenant_id' = $1
              ORDER BY created_at DESC LIMIT 1",
            &[&tenant_a],
        )
        .await
        .expect("the outbox is where the audit survives -- operations goes with the tenant");
        assert_eq!(outbox["tenant_slug"], serde_json::json!("wipe-a"));
        assert_eq!(
            outbox["clusters"][0]["broker_tenant_uuid"],
            clusters[0]["broker_tenant_uuid"],
            "the outbox event must carry the same uuids as the result: it is the copy an \
             operator who ran the steps out of order still has"
        );

        // --- the erasure reaches the outbox's PII, without eating the queue --
        // `bootstrap_tenant` emits a `tenant_bootstrapped` event whose payload
        // carries admin_email. Nothing in the OSS drains the outbox (meter.rs
        // writes it; no reader exists), so before the redaction below the
        // address of a fully deleted tenant simply stayed in this table
        // forever, in the same function that deletes `operations` because "an
        // address left in an audit table is not a deletion".
        let boot = json1(
            &c,
            "SELECT payload::text FROM queen_proxy.outbox
              WHERE kind = 'tenant_bootstrapped' AND payload->>'tenant_id' = $1
              ORDER BY created_at DESC LIMIT 1",
            &[&tenant_a],
        )
        .await
        .expect("the bootstrap event must still be there: the outbox is a queue, not an audit");
        assert!(
            boot.get("admin_email").is_none(),
            "the deleted tenant's address survives in queen_proxy.outbox: {boot}"
        );
        assert_eq!(
            boot["tenant_slug"],
            serde_json::json!("wipe-a"),
            "only the PII keys go; the event itself must still be drainable: {boot}"
        );
        assert!(
            res["counts"]["outbox_redacted"].as_i64().unwrap_or(0) >= 1,
            "the redaction must be reported in the result, or an operator cannot audit it: {res}"
        );
        // B's own bootstrap event is untouched -- the redaction is scoped like
        // every other statement in this function.
        let boot_b = json1(
            &c,
            "SELECT payload::text FROM queen_proxy.outbox
              WHERE kind = 'tenant_bootstrapped' AND payload->>'tenant_id' = $1
              ORDER BY created_at DESC LIMIT 1",
            &[&tenant_b],
        )
        .await
        .expect("B's signup event");
        assert!(
            boot_b.get("admin_email").is_some(),
            "B's address was redacted while A was deleted: {boot_b}"
        );

        // --- the invalidation actually reached a listener -------------------
        let mut seen: Vec<String> = Vec::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        while std::time::Instant::now() < deadline && !seen.contains(&cluster_a) {
            match nrx.try_recv() {
                Ok(p) => seen.push(p),
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
            }
        }
        assert!(
            seen.contains(&cluster_a),
            "no queen_proxy_inval for the deleted cluster: every proxy would keep serving it \
             out of cache until its own TTL expired. Saw {seen:?}"
        );

        // --- idempotent ------------------------------------------------------
        let again =
            json1(&c, "SELECT (queen_proxy.delete_tenant($1::text::uuid))::text", &[&tenant_a])
                .await
                .expect("a second delete must not raise");
        assert_eq!(again["deleted"], serde_json::json!(false), "{again}");
        assert_eq!(again["existed"], serde_json::json!(false), "{again}");

        // --- p_force is the operator's escape hatch, and nothing more --------
        let v = json1(
            &c,
            "SELECT (queen_proxy.bootstrap_tenant('wipe-c', 'wipe-c', 'wipe-c-c', 'free',
                                                  $1::text::uuid, 'c@example.test'))::text",
            &[&cell],
        )
        .await
        .expect("bootstrap c");
        let tenant_c = uuid_str(&v, "tenant_id");
        let forced = json1(
            &c,
            "SELECT (queen_proxy.delete_tenant($1::text::uuid, true))::text",
            &[&tenant_c],
        )
        .await
        .expect("force must skip only the status precondition");
        assert_eq!(forced["deleted"], serde_json::json!(true), "{forced}");
        assert_eq!(forced["status_was"], serde_json::json!("active"));
        assert_eq!(forced["forced"], serde_json::json!(true));

        // --- a tenant with NO cluster ---------------------------------------
        // The other shape of a botched provision: create_tenant succeeded and
        // create_cluster did not. Both aggregates over `clusters` return SQL
        // NULL here, and the whole function walks a zero-length notify loop --
        // if either COALESCE were missing this raises instead of answering.
        let lonely: String = c
            .query_one("SELECT (queen_proxy.create_tenant('lonely', 'lonely'))::text", &[])
            .await
            .expect("create_tenant")
            .get(0);
        let res = json1(
            &c,
            "SELECT (queen_proxy.delete_tenant($1::text::uuid, true))::text",
            &[&lonely],
        )
        .await
        .expect("a tenant with no cluster must delete, not raise");
        assert_eq!(res["deleted"], serde_json::json!(true), "{res}");
        assert_eq!(res["clusters"], serde_json::json!([]), "{res}");
        assert_eq!(res["counts"]["clusters"], serde_json::json!(0), "{res}");

        // B is STILL there, after four deletes around it.
        assert_eq!(counts(&c, &tenant_b).await, b_before, "B changed");
        println!(
            "\nOK -- delete_tenant removes one tenant completely and leaves the others intact."
        );
    }

    /// `queen_proxy.bootstrap_tenant` with a NULL password (008_bootstrap_password.sql).
    ///
    /// Needs a Postgres to talk to, but unlike the test above it does NOT drop
    /// anything: it applies the migrations (a no-op on a database that has them)
    /// and names every object with a per-run random suffix, so it is safe to
    /// point at any throwaway instance, including one the wipe test has used.
    ///
    /// ```bash
    /// docker run --rm -d --name queen-proxy-bootstrap-pg -e POSTGRES_PASSWORD=postgres \
    ///     -p 5490:5432 postgres:16-alpine
    /// QUEEN_PROXY_TEST_PG=127.0.0.1:5490 cargo test --bin queen-proxy \
    ///     db::tests::live_bootstrap_password_flags_against_real_postgres -- --ignored --nocapture
    /// ```
    ///
    /// (It shares `QUEEN_PROXY_TEST_PG` with `live_delete_tenant_against_real_postgres`,
    /// which DOES drop the schema -- so give the two separate containers, or run
    /// them one at a time, rather than `-- --ignored` across both.)
    ///
    /// What it pins:
    ///   * `p_password => NULL` is NOT an error -- the tenant, cluster, admin
    ///     role and API key are all created exactly as with a password;
    ///   * it answers `password_set:false, can_login:false`, and the same two
    ///     flags reach the `tenant_bootstrapped` audit row, so the fact survives
    ///     the session that created it;
    ///   * a RAISE WARNING with a HINT actually reaches a client connection --
    ///     the loud half, and the only half a human running psql will see;
    ///   * a password makes all of it quiet;
    ///   * `can_login` is read back from the TABLES, not inferred from the
    ///     argument: link an OAuth identity to the password-less admin and the
    ///     next bootstrap of the same tenant is quiet too. That is the shape the
    ///     warning must not cry wolf on.
    #[tokio::test]
    #[ignore = "requires a postgres (applies migrations; creates suffixed rows) -- see doc comment"]
    async fn live_bootstrap_password_flags_against_real_postgres() {
        let target =
            std::env::var("QUEEN_PROXY_TEST_PG").unwrap_or_else(|_| "127.0.0.1:5490".to_string());
        let (host, port) = target
            .split_once(':')
            .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
            .unwrap_or((target.clone(), 5432));

        let pxcfg = crate::config::PxdbConfig {
            host: host.clone(),
            port,
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            dbname: "postgres".to_string(),
            use_ssl: false,
            ssl_reject_unauthorized: false,
            ssl_root_cert: None,
            pool_size: 4,
            timeout_ms: 5_000,
        };
        let pool = create_pool(&pxcfg).await.expect("connect to the test postgres");
        // Also the only proof that the .sql on disk is the one the binary
        // carries: it is include_str!-embedded, so a psql run would happily pass
        // against a stale `cargo build`.
        apply_migrations(&pool).await.expect("apply migrations");

        // A raw connection of our own, because a WARNING is delivered as an
        // asynchronous NOTICE on the connection object and a pooled client
        // gives no way to see one. Same shape as cache.rs's listener: the poll
        // loop is spawned BEFORE anything is executed, or nothing is delivered.
        let (raw, mut conn) = tokio_postgres::connect(
            &format!("host={host} port={port} user=postgres password=postgres dbname=postgres"),
            NoTls,
        )
        .await
        .expect("raw connect");
        let (ntx, mut nrx) = tokio::sync::mpsc::unbounded_channel::<(String, String, String)>();
        tokio::spawn(async move {
            while let Some(msg) = std::future::poll_fn(|cx| conn.poll_message(cx)).await {
                match msg {
                    Ok(tokio_postgres::AsyncMessage::Notice(n)) => {
                        let _ = ntx.send((
                            n.severity().to_string(),
                            n.message().to_string(),
                            n.hint().unwrap_or_default().to_string(),
                        ));
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });
        // Everything CREATE-related is already applied, so drain the notices the
        // migration runner produced before asserting on ours.
        while nrx.try_recv().is_ok() {}

        let tag = uuid::Uuid::new_v4().simple().to_string()[..10].to_string();
        let cell: String = raw
            .query_one(
                "INSERT INTO queen_proxy.cells(slug, region, base_url, class, cell_secret)
                 VALUES ('bp-' || $1, 'local', 'http://127.0.0.1:6710', 'shared', 'secret')
                 RETURNING id::text",
                &[&tag],
            )
            .await
            .expect("cell")
            .get(0);

        async fn boot(
            c: &tokio_postgres::Client,
            slug: &String,
            cell: &String,
            password: Option<&str>,
        ) -> serde_json::Value {
            let row = c
                .query_one(
                    "SELECT (queen_proxy.bootstrap_tenant($1, $1, $1 || '-c', 'free',
                                                          $2::text::uuid,
                                                          $1 || '@example.test', $3))::text",
                    &[slug, cell, &password],
                )
                .await
                .expect("bootstrap_tenant must not raise on a NULL password");
            let txt: String = row.get(0);
            serde_json::from_str(&txt).expect("result is JSON")
        }

        // --- NULL password: legal, complete, and LOUD ------------------------
        let quiet_slug = format!("bpnull{tag}");
        let v = boot(&raw, &quiet_slug, &cell, None).await;
        assert!(v["api_key"].is_string(), "the API key is still issued: {v}");
        assert!(v["tenant_id"].is_string() && v["cluster_id"].is_string(), "{v}");
        assert_eq!(v["password_set"], serde_json::json!(false), "{v}");
        assert_eq!(v["can_login"], serde_json::json!(false), "{v}");

        let (sev, msg, hint) = nrx.try_recv().expect("a NULL password must warn on the wire");
        assert_eq!(sev, "WARNING", "{sev}: {msg}");
        assert!(msg.contains("cannot sign in"), "{msg}");
        assert!(msg.contains(&quiet_slug), "the warning must name the admin: {msg}");
        assert!(hint.contains("p_password"), "the hint must name the way out: {hint}");

        // The audit row carries it, because stderr will not be there later.
        let meta: String = raw
            .query_one(
                "SELECT meta::text FROM queen_proxy.operations
                  WHERE tenant_id = $1::text::uuid AND action = 'tenant_bootstrapped'",
                &[&v["tenant_id"].as_str().unwrap().to_string()],
            )
            .await
            .expect("audit row")
            .get(0);
        let meta: serde_json::Value = serde_json::from_str(&meta).unwrap();
        assert_eq!(meta["password_set"], serde_json::json!(false), "{meta}");
        assert_eq!(meta["can_login"], serde_json::json!(false), "{meta}");

        // --- a password: quiet -----------------------------------------------
        let pw_slug = format!("bppw{tag}");
        let v = boot(&raw, &pw_slug, &cell, Some("hunter2-hunter2")).await;
        assert_eq!(v["password_set"], serde_json::json!(true), "{v}");
        assert_eq!(v["can_login"], serde_json::json!(true), "{v}");
        assert!(nrx.try_recv().is_err(), "a usable admin must not warn");

        // --- can_login is read from the tables, not from the argument --------
        // Link an OAuth identity to the password-less admin: it can sign in now,
        // and a re-run must stop crying wolf even though p_password is still NULL.
        raw.execute(
            "INSERT INTO queen_proxy.identities(user_id, provider, provider_id, email, verified)
             SELECT id, 'google', 'g-' || $1, email, true
               FROM queen_proxy.users WHERE email = $2",
            &[&tag, &format!("{quiet_slug}@example.test")],
        )
        .await
        .expect("link identity");
        let v = boot(&raw, &quiet_slug, &cell, None).await;
        assert_eq!(v["password_set"], serde_json::json!(false), "{v}");
        assert_eq!(v["can_login"], serde_json::json!(true), "an OAuth admin can log in: {v}");
        assert!(v["api_key"].is_null(), "a re-run cannot re-issue the plaintext key: {v}");
        assert!(nrx.try_recv().is_err(), "an OAuth-linked admin must not warn");

        println!("\nOK -- a password-less bootstrap is legal, flagged, audited and announced.");
    }

    /// END TO END for `PXDB_SSL_ROOT_CERT`, against a real PostgreSQL serving a
    /// certificate signed by a PRIVATE CA -- the shape every managed provider
    /// presents (Scaleway RDB, Cloud SQL, Aiven) and the one this variable
    /// exists for. Unit tests can prove the PEM parses; only this can prove the
    /// parsed anchors are what rustls actually validates the chain against.
    ///
    /// Three connections, one server:
    ///   * no CA, `reject_unauthorized = true`  -> FAILS (unknown issuer). This
    ///     is the state of the product before this change, and it is why every
    ///     cell shipped with the escape hatch;
    ///   * the CA supplied                      -> SUCCEEDS, verified;
    ///   * a DIFFERENT, unrelated CA supplied   -> FAILS. Without this one the
    ///     test would also pass against an implementation that ignored the
    ///     variable and quietly turned verification off.
    ///
    /// And a fourth, which is the decision this feature rests on: CA supplied
    /// AND `reject_unauthorized = false`. The CA wins, so the WRONG CA still
    /// fails -- the escape hatch does not resurrect itself as a fallback.
    ///
    /// Setup (any throwaway PostgreSQL; nothing is written, so it is safe
    /// anywhere -- unlike `live_delete_tenant_against_real_postgres`, which
    /// drops the schema):
    ///
    /// ```sh
    /// # a CA, and a server cert for `localhost` signed by it
    /// openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
    ///     -keyout ca.key -out ca.pem -days 3650 -subj "/CN=Test CA" \
    ///     -addext "basicConstraints=critical,CA:TRUE"
    /// printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' > s.ext
    /// openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
    ///     -keyout server.key -out server.csr -subj "/CN=localhost"
    /// # -sha256 is NOT optional: rustls rejects a SHA-1 signature outright.
    /// openssl x509 -req -sha256 -in server.csr -CA ca.pem -CAkey ca.key \
    ///     -CAcreateserial -out server.crt -days 3650 -extfile s.ext
    /// # the key must be 0600 and owned by the postgres user INSIDE the image,
    /// # which a bind mount cannot do without root -- so bake it in
    /// printf 'FROM postgres:16-alpine\nCOPY server.crt /certs/server.crt\nCOPY server.key /certs/server.key\nRUN chown postgres:postgres /certs/* && chmod 600 /certs/server.key\n' > Dockerfile
    /// docker build -t queen-tls-pg:test .
    /// docker run --rm -d --name queen-tls-pg -e POSTGRES_PASSWORD=postgres -p 5488:5432 \
    ///     queen-tls-pg:test -c ssl=on -c ssl_cert_file=/certs/server.crt -c ssl_key_file=/certs/server.key
    ///
    /// QUEEN_PROXY_TLS_TEST_PG=localhost:5488 QUEEN_PROXY_TLS_TEST_CA=$PWD/ca.pem \
    ///     cargo test --bin queen-proxy db::tests::live_pxdb_ssl_root_cert -- --ignored --nocapture
    /// ```
    ///
    /// `QUEEN_PROXY_TLS_TEST_CA` is a PATH here on purpose: it is test rigging,
    /// not the product contract. The product variable takes the PEM CONTENT --
    /// which is exactly what this test then puts in `ssl_root_cert`.
    ///
    /// The host must be `localhost`, not `127.0.0.1`: the certificate above
    /// carries both, but a hostname is what a real cell uses and what exercises
    /// rustls's DNS-name matching.
    #[tokio::test]
    #[ignore = "requires a TLS-serving postgres and its CA -- see doc comment"]
    async fn live_pxdb_ssl_root_cert_verifies_a_private_ca() {
        let target = std::env::var("QUEEN_PROXY_TLS_TEST_PG")
            .unwrap_or_else(|_| "localhost:5488".to_string());
        let (host, port) = target
            .split_once(':')
            .map(|(h, p)| (h.to_string(), p.parse::<u16>().expect("port")))
            .unwrap_or((target.clone(), 5432));
        let ca_path = std::env::var("QUEEN_PROXY_TLS_TEST_CA")
            .expect("QUEEN_PROXY_TLS_TEST_CA=<path to the CA pem> -- see the doc comment");
        let ca = std::fs::read_to_string(&ca_path).expect("read the CA pem");
        // An unrelated CA, generated here so the test carries its own negative
        // control rather than trusting a second file to be the wrong one.
        let other_ca = OTHER_CA.to_string();

        let cfg = |use_ssl: bool, reject: bool, root: Option<String>| crate::config::PxdbConfig {
            host: host.clone(),
            port,
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            dbname: "postgres".to_string(),
            use_ssl,
            ssl_reject_unauthorized: reject,
            ssl_root_cert: root,
            pool_size: 2,
            timeout_ms: 5_000,
        };

        // 1. The status quo ante: a private chain against the Mozilla set.
        let e = create_pool(&cfg(true, true, None))
            .await
            .expect_err("webpki-roots must NOT accept a private CA");
        println!("no CA          -> {e}");

        // 2. The fix.
        let pool = create_pool(&cfg(true, true, Some(ca.clone())))
            .await
            .expect("the supplied CA must validate the chain");
        let client = pool.get().await.expect("checkout");
        let row = client
            .query_one(
                "SELECT ssl, version FROM pg_stat_ssl JOIN pg_stat_activity USING (pid) \
                 WHERE pid = pg_backend_pid()",
                &[],
            )
            .await
            .expect("pg_stat_ssl");
        let ssl: bool = row.get(0);
        let version: Option<String> = row.get(1);
        assert!(ssl, "the connection the CA validated must actually be TLS");
        println!("supplied CA    -> connected, ssl={ssl} version={version:?}");

        // 3. The negative control: verification is really ON, not bypassed.
        let e = create_pool(&cfg(true, true, Some(other_ca.clone())))
            .await
            .expect_err("an unrelated CA must NOT validate this chain");
        println!("wrong CA       -> {e}");

        // 4. The decision: a supplied CA outranks the escape hatch, so a WRONG
        //    CA fails even with reject_unauthorized=false. If the hatch won
        //    instead, this would connect -- and the security fix would be
        //    silently inert on every cell still carrying the old flag.
        let e = create_pool(&cfg(true, false, Some(other_ca)))
            .await
            .expect_err("PXDB_SSL_SSL_REJECT_UNAUTHORIZED=false must not rescue a wrong CA");
        println!("wrong CA + hatch -> {e}");

        // 5. And the hatch alone still works, unchanged, for anyone who has not
        //    migrated yet.
        create_pool(&cfg(true, false, None))
            .await
            .expect("reject_unauthorized=false alone is unchanged");

        println!("\nOK -- the supplied CA is the CA that is trusted, and the only one.");
    }

    /// A self-signed CA unrelated to anything, for the negative controls above.
    /// Valid to 2126.
    const OTHER_CA: &str = "-----BEGIN CERTIFICATE-----\n\
MIIBWTCCAQCgAwIBAgIJAKmzX6A030osMAoGCCqGSM49BAMCMB8xHTAbBgNVBAMM\n\
FFF1ZWVuIFRlc3QgUm9vdCBDQSAyMCAXDTI2MDgyNzExMTYxN1oYDzIxMjYwODAz\n\
MTExNjE3WjAfMR0wGwYDVQQDDBRRdWVlbiBUZXN0IFJvb3QgQ0EgMjBZMBMGByqG\n\
SM49AgEGCCqGSM49AwEHA0IABF6twyLh3QAgSqyQuFXN6YQpXqV9UW5xVjHk9QhH\n\
TLKoCVy19qOVzNumm5N3EQVVKDaAnssJkg1dL7nEtgBHV0qjIzAhMA8GA1UdEwEB\n\
/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEGMAoGCCqGSM49BAMCA0cAMEQCIGCfF1DG\n\
B/pG4xgUMEbP3/6vEIKVVA+xzh8lHN6SN1jRAiB/wSDR3RDFzV3Mw4/S8WIJFlvW\n\
cFiWxl/YjudWoYm2+g==\n\
-----END CERTIFICATE-----\n";
}
