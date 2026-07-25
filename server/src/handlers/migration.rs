//! Migration HTTP handlers — port of the C++ routes/migration.cpp admin tool that
//! copies the broker's own Postgres DB to a target Postgres via `pg_dump | pg_restore`
//! and validates the copy by per-table row counts.
//!
//! Differences vs the C++ original, all deliberate for the segments engine:
//!   * SOURCE params come from the broker env (PG_*), same as config.rs. TARGET
//!     params come from the request body.
//!   * TLS: the broker pool is NoTls; target probes/dumps here are NoTls too. An
//!     `ssl:true` target is attempted without TLS and reported honestly if it fails.
//!   * validate/dump operate on whatever tables actually exist in the `queen`
//!     schema (segments `seg_*` + shared tables) — NOT the C++ hardcoded rows-engine
//!     table list, which does not exist under this engine.
//!
//! One migration at a time, tracked in a process-global state (OnceLock), exactly
//! like the C++ g_migration_state singleton.

use super::*;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use axum::body::Bytes;
use axum::http::StatusCode;
use axum::response::Response;
use serde::Deserialize;
use tokio_postgres::NoTls;

#[derive(Clone)]
struct MigrationState {
    status: String, // idle | running | completed | error
    current_step: String,
    started: Option<Instant>,
    elapsed_frozen: Option<f64>,
    error: Option<String>,
    dump_output: Option<String>,
    restore_output: Option<String>,
}

impl Default for MigrationState {
    fn default() -> Self {
        MigrationState {
            status: "idle".into(),
            current_step: String::new(),
            started: None,
            elapsed_frozen: None,
            error: None,
            dump_output: None,
            restore_output: None,
        }
    }
}

impl MigrationState {
    fn elapsed(&self) -> f64 {
        if let Some(f) = self.elapsed_frozen {
            f
        } else if let Some(s) = self.started {
            s.elapsed().as_secs_f64()
        } else {
            0.0
        }
    }
    fn to_json(&self) -> serde_json::Value {
        let mut o = serde_json::json!({
            "status": self.status,
            "currentStep": self.current_step,
            "elapsedSeconds": self.elapsed(),
        });
        if let Some(e) = &self.error {
            o["error"] = serde_json::Value::String(e.clone());
        }
        if let Some(d) = &self.dump_output {
            o["dumpOutput"] = serde_json::Value::String(d.clone());
        }
        if let Some(r) = &self.restore_output {
            o["restoreOutput"] = serde_json::Value::String(r.clone());
        }
        o
    }
}

fn state() -> &'static Mutex<MigrationState> {
    static S: OnceLock<Mutex<MigrationState>> = OnceLock::new();
    S.get_or_init(|| Mutex::new(MigrationState::default()))
}

// ---------------------------------------------------------------- target config
#[derive(Deserialize, Default)]
#[serde(default)]
struct TargetConfig {
    host: String,
    port: serde_json::Value,
    database: String,
    user: String,
    password: String,
    ssl: bool,
    #[serde(rename = "excludeTableData")]
    exclude_table_data: Vec<String>,
}

impl TargetConfig {
    fn port_str(&self) -> String {
        match &self.port {
            serde_json::Value::String(s) if !s.is_empty() => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            _ => "5432".to_string(),
        }
    }
    fn connstr(&self) -> String {
        // RUSTFIX item 5: honor the request `ssl` flag. sslmode=require triggers
        // TLS; the actual chain check for the tokio-postgres validate/probe path
        // is done by the rustls connector (target is encrypt-only, mirroring the
        // C++ `sslmode=require`).
        format!(
            "host={} port={} user={} password={} dbname={} sslmode={} connect_timeout=10",
            self.host,
            self.port_str(),
            self.user,
            self.password,
            self.database,
            if self.ssl { "require" } else { "disable" }
        )
    }
}

// Broker's own DB params (the migration SOURCE), read from the same env as config.rs.
struct Source {
    host: String,
    port: String,
    user: String,
    password: String,
    database: String,
    // RUSTFIX item 5: PG_USE_SSL / PG_SSL_REJECT_UNAUTHORIZED for the source side,
    // same as the broker pool (config.rs). The source is usually the broker's own
    // DB, so it follows the broker's TLS policy, not the request body.
    use_ssl: bool,
    reject_unauthorized: bool,
}
fn source() -> Source {
    fn ev(k: &str, d: &str) -> String {
        std::env::var(k).ok().filter(|v| !v.is_empty()).unwrap_or_else(|| d.to_string())
    }
    Source {
        host: ev("PG_HOST", "localhost"),
        port: ev("PG_PORT", "5432"),
        user: ev("PG_USER", "postgres"),
        password: ev("PG_PASSWORD", "postgres"),
        // RUSTFIX item 4: PG_DATABASE → PG_DB → postgres, identical to the pool.
        database: crate::config::resolve_db_name(),
        use_ssl: crate::config::env_bool("PG_USE_SSL", false),
        reject_unauthorized: crate::config::env_bool("PG_SSL_REJECT_UNAUTHORIZED", true),
    }
}
impl Source {
    fn connstr(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={} sslmode={} connect_timeout=10",
            self.host,
            self.port,
            self.user,
            self.password,
            self.database,
            if self.use_ssl { "require" } else { "disable" }
        )
    }
}

// One-shot connect honoring TLS (RUSTFIX item 5). When `use_ssl` is false this is
// the original NoTls path (byte-for-byte); when true a rustls connector is used,
// with chain verification gated by `reject`. Returns the client + the spawned
// connection driver handle (abort it when done).
async fn connect_pg(
    connstr: &str,
    use_ssl: bool,
    reject: bool,
) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>), String> {
    if use_ssl {
        let connector = crate::pgtls::make_connector(reject);
        let (client, connection) = tokio_postgres::connect(connstr, connector)
            .await
            .map_err(|e| e.to_string())?;
        let handle = tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok((client, handle))
    } else {
        let (client, connection) = tokio_postgres::connect(connstr, NoTls)
            .await
            .map_err(|e| e.to_string())?;
        let handle = tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok((client, handle))
    }
}

fn bad_request(msg: &str) -> Response {
    json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"{msg}\"}}"))
}

// -------------------------------------------- POST /api/v1/migration/test-connection
pub async fn handle_migration_test(body: Bytes) -> Response {
    let cfg: TargetConfig = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return bad_request(&format!("bad body: {e}")),
    };
    if cfg.host.is_empty() || cfg.database.is_empty() || cfg.user.is_empty() {
        return bad_request("host, database and user are required");
    }
    // Target TLS follows the request `ssl` flag; encrypt-only (no chain verify),
    // mirroring the C++ target `sslmode=require`.
    match probe_version(&cfg.connstr(), cfg.ssl, false).await {
        Ok(version) => json(
            StatusCode::OK,
            serde_json::json!({ "success": true, "message": "Connection successful", "version": version })
                .to_string(),
        ),
        Err(e) => json(
            StatusCode::OK,
            serde_json::json!({ "success": false, "message": format!("Connection failed: {e}") })
                .to_string(),
        ),
    }
}

// SELECT version() over a one-shot connection (TLS-aware, RUSTFIX item 5); the
// connection future is driven to completion, then dropped.
async fn probe_version(connstr: &str, use_ssl: bool, reject: bool) -> Result<String, String> {
    let (client, handle) = connect_pg(connstr, use_ssl, reject).await?;
    let row = client
        .query_one("SELECT version()", &[])
        .await
        .map_err(|e| e.to_string());
    drop(client);
    handle.abort();
    row.map(|r| r.get::<_, String>(0))
}

// ------------------------------------------------- GET /api/v1/migration/status
pub async fn handle_migration_status() -> Response {
    let s = state().lock().unwrap().clone();
    json(StatusCode::OK, s.to_json().to_string())
}

// -------------------------------------------------- POST /api/v1/migration/reset
pub async fn handle_migration_reset() -> Response {
    let mut s = state().lock().unwrap();
    if s.status == "running" {
        return json(
            StatusCode::CONFLICT,
            "{\"error\":\"a migration is currently running\"}".to_string(),
        );
    }
    *s = MigrationState::default();
    json(
        StatusCode::OK,
        "{\"status\":\"idle\",\"message\":\"migration state reset\"}".to_string(),
    )
}

// ------------------------------------------------- POST /api/v1/migration/validate
// Compare per-table row counts between source and target for every base table in
// the `queen` schema on the source. Read-only.
pub async fn handle_migration_validate(body: Bytes) -> Response {
    let cfg: TargetConfig = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return bad_request(&format!("bad body: {e}")),
    };
    if cfg.host.is_empty() || cfg.database.is_empty() || cfg.user.is_empty() {
        return bad_request("host, database and user are required");
    }
    let src = source();
    match validate(&src, &cfg).await {
        Ok(v) => json(StatusCode::OK, v.to_string()),
        Err(e) => json(
            StatusCode::OK,
            serde_json::json!({ "success": false, "error": e }).to_string(),
        ),
    }
}

async fn validate(src: &Source, tgt: &TargetConfig) -> Result<serde_json::Value, String> {
    // TLS-aware connects (RUSTFIX item 5): source follows PG_USE_SSL/reject,
    // target follows the request `ssl` flag (encrypt-only).
    let (sc, sh) = connect_pg(&src.connstr(), src.use_ssl, src.reject_unauthorized)
        .await
        .map_err(|e| format!("source: {e}"))?;
    let (tc, th) = connect_pg(&tgt.connstr(), tgt.ssl, false)
        .await
        .map_err(|e| format!("target: {e}"))?;

    let tables_res = async {
        let rows = sc
            .query(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema='queen' AND table_type='BASE TABLE' ORDER BY table_name",
                &[],
            )
            .await
            .map_err(|e| e.to_string())?;
        let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();

        let mut out = Vec::new();
        let mut all_match = true;
        for t in names {
            let q = format!("SELECT count(*)::bigint FROM queen.\"{}\"", t.replace('"', "\"\""));
            let s_count: i64 = sc.query_one(&q, &[]).await.map(|r| r.get(0)).unwrap_or(-1);
            let t_count: i64 = tc.query_one(&q, &[]).await.map(|r| r.get(0)).unwrap_or(-1);
            let m = s_count == t_count && s_count >= 0;
            if !m {
                all_match = false;
            }
            out.push(serde_json::json!({
                "table": t, "sourceCount": s_count, "targetCount": t_count, "match": m
            }));
        }
        Ok::<serde_json::Value, String>(serde_json::json!({
            "success": true, "allMatch": all_match, "tables": out
        }))
    }
    .await;

    drop(sc);
    drop(tc);
    sh.abort();
    th.abort();
    tables_res
}

// --------------------------------------------------- POST /api/v1/migration/start
// Guards single-run, then runs `pg_dump (source) | pg_restore (target)` on a
// background thread, updating the shared state. Returns 202 immediately.
pub async fn handle_migration_start(body: Bytes) -> Response {
    let cfg: TargetConfig = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return bad_request(&format!("bad body: {e}")),
    };
    if cfg.host.is_empty() || cfg.database.is_empty() || cfg.user.is_empty() {
        return bad_request("host, database and user are required");
    }
    {
        let mut s = state().lock().unwrap();
        if s.status == "running" {
            return json(
                StatusCode::CONFLICT,
                "{\"error\":\"a migration is already running\"}".to_string(),
            );
        }
        *s = MigrationState {
            status: "running".into(),
            current_step: "starting".into(),
            started: Some(Instant::now()),
            ..Default::default()
        };
    }

    let src = source();
    // Per-table --exclude-table-data flags (schema still copied, rows skipped).
    let excludes: Vec<String> = cfg
        .exclude_table_data
        .iter()
        .filter(|t| !t.is_empty())
        .map(|t| format!("--exclude-table-data=queen.{}", shell_ident(t)))
        .collect();
    let tgt_host = cfg.host.clone();
    let tgt_port = cfg.port_str();
    let tgt_user = cfg.user.clone();
    let tgt_db = cfg.database.clone();
    let tgt_pw = cfg.password.clone();
    let tgt_ssl = cfg.ssl;

    std::thread::spawn(move || {
        run_migration(src, tgt_host, tgt_port, tgt_user, tgt_db, tgt_pw, tgt_ssl, excludes);
    });

    json(
        StatusCode::ACCEPTED,
        "{\"status\":\"started\",\"message\":\"migration started\"}".to_string(),
    )
}

// Whitelist an identifier for interpolation into the pg_dump flag (defence in
// depth — table names come from the request body).
fn shell_ident(s: &str) -> String {
    s.chars().filter(|c| c.is_alphanumeric() || *c == '_').collect()
}

fn set_state<F: FnOnce(&mut MigrationState)>(f: F) {
    let mut s = state().lock().unwrap();
    f(&mut s);
}

fn run_migration(
    src: Source,
    tgt_host: String,
    tgt_port: String,
    tgt_user: String,
    tgt_db: String,
    tgt_pw: String,
    tgt_ssl: bool,
    excludes: Vec<String>,
) {
    use std::process::Command;

    // RUSTFIX item 5: sslmode for each libpq process. Matches C++ build_connstr
    // (require when ssl, else disable). Source follows PG_USE_SSL, target the body
    // `ssl` flag.
    let src_sslmode = if src.use_ssl { "require" } else { "disable" };
    let tgt_sslmode = if tgt_ssl { "require" } else { "disable" };

    // Ensure the target schema exists (pg_dump of a schema does not emit CREATE
    // SCHEMA), then stream dump|restore. --clean --if-exists makes re-runs idempotent.
    set_state(|s| s.current_step = "preparing target schema".into());
    let precreate = Command::new("psql")
        .env("PGPASSWORD", &tgt_pw)
        .env("PGSSLMODE", tgt_sslmode)
        .args([
            "-h", &tgt_host, "-p", &tgt_port, "-U", &tgt_user, "-d", &tgt_db,
            "-v", "ON_ERROR_STOP=1", "-c", "CREATE SCHEMA IF NOT EXISTS queen",
        ])
        .output();
    if let Err(e) = &precreate {
        return set_state(|s| {
            s.status = "error".into();
            s.error = Some(format!("psql not available: {e}"));
            s.elapsed_frozen = Some(s.elapsed());
        });
    }

    set_state(|s| s.current_step = "dump | restore".into());
    let exclude_flags = excludes.join(" ");
    // Per-side inline PGSSLMODE (each applies only to its own command in the pipe).
    let pipeline = format!(
        "set -o pipefail; \
         PGPASSWORD=\"$SRC_PW\" PGSSLMODE={src_ssl} pg_dump --format=custom -Z 0 --schema=queen --no-owner --no-privileges {excludes} \
             -h {sh} -p {sp} -U {su} -d {sd} \
         | PGPASSWORD=\"$TGT_PW\" PGSSLMODE={tgt_ssl} pg_restore --no-owner --no-privileges --clean --if-exists --exit-on-error \
             -h {th} -p {tp} -U {tu} -d {td}",
        excludes = exclude_flags,
        src_ssl = src_sslmode, tgt_ssl = tgt_sslmode,
        sh = src.host, sp = src.port, su = src.user, sd = src.database,
        th = tgt_host, tp = tgt_port, tu = tgt_user, td = tgt_db,
    );

    let out = Command::new("bash")
        .arg("-c")
        .arg(&pipeline)
        .env("SRC_PW", &src.password)
        .env("TGT_PW", &tgt_pw)
        .output();

    match out {
        Ok(o) => {
            let combined = format!(
                "{}{}",
                String::from_utf8_lossy(&o.stdout),
                String::from_utf8_lossy(&o.stderr)
            );
            let ok = o.status.success();
            set_state(|s| {
                s.restore_output = Some(combined.clone());
                s.dump_output = Some(combined);
                s.elapsed_frozen = Some(s.elapsed());
                if ok {
                    s.status = "completed".into();
                    s.current_step = "done".into();
                } else {
                    s.status = "error".into();
                    s.error = Some(format!("dump/restore exited with {}", o.status));
                }
            });
        }
        Err(e) => set_state(|s| {
            s.status = "error".into();
            s.error = Some(format!("failed to run pg_dump/pg_restore: {e}"));
            s.elapsed_frozen = Some(s.elapsed());
        }),
    }
}
