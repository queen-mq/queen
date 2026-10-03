//! The rules of the connector document (PLAN §3.2, §3.3). OWNER: agent C.
//!
//! Every refusal is an [`Error::Config`] whose sentence starts with the
//! field it is about, in the document's own spelling
//! (`source.tables[2].queue: …`), because the API hands it to a person who
//! has the JSON open. Limits are refused, never clamped: a clamped value is a
//! document that says one thing and runs another.

use std::collections::HashMap;

use serde_json::Value;

use crate::config::{
    valid_connector_name, ConnectionSpec, ConnectorDoc, Kind, MetadataColumns, NodeKnobs,
    PartitionBy, SinkMode, SinkSpec, SourceSpec, SslMode, TruncatePolicy,
};
use crate::error::{Error, Result};
use crate::pg::catalog::{valid_identifier, TableName};

/// The longest queue or consumer group name a connector may use. The broker
/// itself only bounds the SUM of a message key's names (tenant + queue +
/// group-or-partition ≤ 447 bytes, `rsm::facade::check_message_key_names`,
/// answered with a 413 at the first push); 128 here, with partition names
/// capped at 128 by the source (PLAN §4.6) and a 36-byte tenant, keeps every
/// key a connector writes inside that budget, so a document that validates
/// never meets a 413 at run time.
pub(crate) const MAX_QUEUE_NAME_BYTES: usize = 128;

/// The longest sql-mode statement.
pub(crate) const MAX_STATEMENT_BYTES: usize = 64 * 1024;

fn err(field: &str, msg: impl std::fmt::Display) -> Error {
    Error::config(format!("{field}: {msg}"))
}

/// The inner sentence of a config error (to re-prefix it with a field).
fn sentence(e: Error) -> String {
    match e {
        Error::Config(m) => m,
        other => other.to_string(),
    }
}

// ---------------------------------------------------------------------------
// normalize
// ---------------------------------------------------------------------------

pub(crate) fn normalize(doc: &mut ConnectorDoc) -> Result<()> {
    let Some(url) = doc.connection.url.take() else {
        return Ok(());
    };
    let parts = parse_url(&url).map_err(|m| err("connection.url", m))?;
    let c = &mut doc.connection;
    if let Some(h) = parts.host {
        c.host = h;
    }
    if let Some(p) = parts.port {
        c.port = p;
    }
    if let Some(d) = parts.database {
        c.database = d;
    }
    if let Some(u) = parts.user {
        c.user = u;
    }
    if let Some(p) = parts.password {
        c.password = Some(p);
    }
    if let Some(m) = parts.ssl_mode {
        c.ssl_mode = m;
    }
    if let Some(t) = parts.connect_timeout_ms {
        c.connect_timeout_ms = t;
    }
    Ok(())
}

#[derive(Debug, Default, PartialEq)]
struct UrlParts {
    host: Option<String>,
    port: Option<u16>,
    database: Option<String>,
    user: Option<String>,
    password: Option<String>,
    ssl_mode: Option<SslMode>,
    connect_timeout_ms: Option<u64>,
}

/// `postgres[ql]://[user[:password]@]host[:port][/database][?param=value&…]`,
/// libpq's URI form with one host. Components are percent-decoded; an IPv6
/// host is bracketed (`[::1]`). Query parameters: `sslmode`,
/// `connect_timeout` (seconds, as libpq), and the components' own names
/// (`host`, `port`, `dbname`, `user`, `password`). Anything else is refused
/// rather than ignored: a parameter the connector would silently drop
/// (`sslrootcert=/path`, `target_session_attrs`) is a surprise later.
fn parse_url(url: &str) -> std::result::Result<UrlParts, String> {
    let rest = url
        .strip_prefix("postgresql://")
        .or_else(|| url.strip_prefix("postgres://"))
        .ok_or("must start with postgres:// or postgresql://")?;
    let (main, query) = match rest.split_once('?') {
        Some((m, q)) => (m, Some(q)),
        None => (rest, None),
    };
    let (authority, path) = match main.find('/') {
        Some(i) => (&main[..i], Some(&main[i + 1..])),
        None => (main, None),
    };
    let mut out = UrlParts::default();
    // The LAST '@': a password with an unencoded '@' still splits right.
    let (userinfo, hostport) = match authority.rfind('@') {
        Some(i) => (Some(&authority[..i]), &authority[i + 1..]),
        None => (None, authority),
    };
    if let Some(ui) = userinfo {
        let (u, p) = match ui.split_once(':') {
            Some((u, p)) => (u, Some(p)),
            None => (ui, None),
        };
        if !u.is_empty() {
            out.user = Some(decode(u, "the user")?);
        }
        if let Some(p) = p {
            out.password = Some(decode(p, "the password")?);
        }
    }
    if hostport.contains(',') {
        return Err("one host only (a multi-host URL is not supported)".into());
    }
    let (host, port) = if let Some(after) = hostport.strip_prefix('[') {
        let (h, tail) = after
            .split_once(']')
            .ok_or("an IPv6 host needs its closing ']'")?;
        let port = match tail {
            "" => None,
            t => Some(t.strip_prefix(':').ok_or("expected :port after ']'")?),
        };
        (h.to_string(), port)
    } else {
        match hostport.split_once(':') {
            Some((h, p)) => (decode(h, "the host")?, Some(p)),
            None => (decode(hostport, "the host")?, None),
        }
    };
    if !host.is_empty() {
        out.host = Some(host);
    }
    if let Some(p) = port.filter(|p| !p.is_empty()) {
        out.port = Some(parse_port(p)?);
    }
    if let Some(d) = path.filter(|d| !d.is_empty()) {
        out.database = Some(decode(d, "the database")?);
    }
    for pair in query.unwrap_or("").split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair
            .split_once('=')
            .ok_or_else(|| format!("parameter {pair:?} has no value"))?;
        let k = decode(k, "a parameter name")?;
        let v = decode(v, "a parameter value")?;
        match k.as_str() {
            "sslmode" => {
                out.ssl_mode = Some(match v.as_str() {
                    "disable" => SslMode::Disable,
                    "prefer" => SslMode::Prefer,
                    "require" => SslMode::Require,
                    "verify-full" => SslMode::VerifyFull,
                    "verify-ca" => {
                        return Err("sslmode verify-ca is not supported: use verify-full \
                                    (the certificate must also name the host)"
                            .into())
                    }
                    other => {
                        return Err(format!(
                            "sslmode {other:?}: expected disable, prefer, require or verify-full"
                        ))
                    }
                })
            }
            "connect_timeout" => {
                let secs: u64 = v
                    .parse()
                    .map_err(|_| format!("connect_timeout {v:?} is not a number of seconds"))?;
                out.connect_timeout_ms = Some(secs.saturating_mul(1000));
            }
            "host" => out.host = Some(v),
            "port" => out.port = Some(parse_port(&v)?),
            "dbname" => out.database = Some(v),
            "user" => out.user = Some(v),
            "password" => out.password = Some(v),
            "sslrootcert" => {
                return Err(
                    "sslrootcert names a file; put the PEM text in connection.sslRootCert".into(),
                )
            }
            other => return Err(format!("unsupported parameter {other:?}")),
        }
    }
    Ok(out)
}

fn parse_port(p: &str) -> std::result::Result<u16, String> {
    p.parse::<u16>()
        .ok()
        .filter(|p| *p > 0)
        .ok_or_else(|| format!("port {p:?} is not 1..=65535"))
}

/// Percent-decoding (`%XX`), UTF-8 required. `+` is a plus: libpq URIs are
/// not form-encoded.
fn decode(s: &str, what: &str) -> std::result::Result<String, String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let hex = b
                .get(i + 1..i + 3)
                .and_then(|h| std::str::from_utf8(h).ok())
                .and_then(|h| u8::from_str_radix(h, 16).ok())
                .ok_or_else(|| format!("{what} has a bad percent-escape"))?;
            out.push(hex);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| format!("{what} is not UTF-8 once decoded"))
}

// ---------------------------------------------------------------------------
// validate
// ---------------------------------------------------------------------------

pub(crate) fn validate(doc: &ConnectorDoc, name: &str) -> Result<()> {
    if !valid_connector_name(name) {
        return Err(err(
            "name",
            format!("{name:?} does not match ^[a-z0-9][a-z0-9_-]{{0,47}}$"),
        ));
    }
    match (doc.kind, &doc.source, &doc.sink) {
        (Kind::Source, Some(_), None) | (Kind::Sink, None, Some(_)) => {}
        (k, _, _) => {
            return Err(err(
                "kind",
                format!(
                    "a {k} document needs a `{k}` block and no `{other}` block",
                    k = k.as_str(),
                    other = match k {
                        Kind::Source => "sink",
                        Kind::Sink => "source",
                    }
                ),
            ))
        }
    }
    validate_connection(&doc.connection)?;
    if let Some(s) = &doc.source {
        validate_source(doc, s, name)?;
    }
    if let Some(s) = &doc.sink {
        validate_sink(doc, s, name)?;
    }
    Ok(())
}

fn validate_connection(c: &ConnectionSpec) -> Result<()> {
    if c.url.is_some() {
        return Err(err(
            "connection.url",
            "is folded into the fields on PUT and never stored; normalize the document first",
        ));
    }
    let bare = c
        .host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(&c.host);
    if bare.is_empty() {
        return Err(err("connection.host", "is required"));
    }
    if bare.len() > 253
        || bare.starts_with('/')
        || bare
            .chars()
            .any(|ch| ch.is_whitespace() || ch.is_control() || matches!(ch, '/' | ',' | '@'))
    {
        return Err(err(
            "connection.host",
            format!(
                "{:?} is not a host name or an IP address (Unix sockets are not supported)",
                c.host
            ),
        ));
    }
    if c.port == 0 {
        return Err(err("connection.port", "must be 1..=65535"));
    }
    for (field, v) in [
        ("connection.database", &c.database),
        ("connection.user", &c.user),
    ] {
        if v.is_empty() {
            return Err(err(field, "is required"));
        }
        if v.len() > 63 || v.chars().any(char::is_control) {
            return Err(err(
                field,
                "must be at most 63 bytes (PostgreSQL's identifier limit) without control characters",
            ));
        }
    }
    if !(1_000..=120_000).contains(&c.connect_timeout_ms) {
        return Err(err(
            "connection.connectTimeoutMs",
            format!("{} is outside 1000..=120000", c.connect_timeout_ms),
        ));
    }
    if let Some(pem) = &c.ssl_root_cert {
        if c.ssl_mode != SslMode::VerifyFull {
            return Err(err(
                "connection.sslRootCert",
                "is used by sslMode verify-full only (the other modes do not verify certificates)",
            ));
        }
        crate::pg::connect::root_store_from_pem(pem)
            .map_err(|m| err("connection.sslRootCert", m))?;
    }
    Ok(())
}

/// A queue (or consumer group) name the broker will take: non-empty, at
/// most [`MAX_QUEUE_NAME_BYTES`], no control characters.
fn check_queue_name(field: &str, q: &str) -> Result<()> {
    if q.is_empty() {
        return Err(err(field, "is required"));
    }
    if q.len() > MAX_QUEUE_NAME_BYTES {
        return Err(err(
            field,
            format!("is {} bytes, the limit is {MAX_QUEUE_NAME_BYTES}", q.len()),
        ));
    }
    if q.chars().any(char::is_control) {
        return Err(err(field, "must not contain control characters"));
    }
    Ok(())
}

/// `^[a-z_][a-z0-9_]{0,62}$`: a slot or publication name (lowercase, so the
/// unquoted and the quoted spelling are the same object).
fn valid_slot_name(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && b.len() <= 63
        && (b[0].is_ascii_lowercase() || b[0] == b'_')
        && b[1..]
            .iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_')
}

fn in_range<T: PartialOrd + std::fmt::Display + Copy>(
    field: &str,
    v: T,
    lo: T,
    hi: T,
) -> Result<()> {
    if v < lo || v > hi {
        return Err(err(field, format!("{v} is outside {lo}..={hi}")));
    }
    Ok(())
}

/// Column names: valid identifiers, no repeats, at least one.
fn check_columns(field: &str, cols: &[String]) -> Result<()> {
    if cols.is_empty() {
        return Err(err(field, "needs at least one column"));
    }
    for (i, c) in cols.iter().enumerate() {
        if !valid_identifier(c) {
            return Err(err(
                &format!("{field}[{i}]"),
                format!("{c:?} is not an identifier ^[A-Za-z_][A-Za-z0-9_$]{{0,62}}$"),
            ));
        }
        if cols[..i].contains(c) {
            return Err(err(
                &format!("{field}[{i}]"),
                format!("{c:?} is listed twice"),
            ));
        }
    }
    Ok(())
}

fn validate_source(doc: &ConnectorDoc, s: &SourceSpec, name: &str) -> Result<()> {
    if s.tables.is_empty() || s.tables.len() > 256 {
        return Err(err(
            "source.tables",
            format!("has {} tables, expected 1..=256", s.tables.len()),
        ));
    }
    let mut seen: HashMap<TableName, usize> = HashMap::new();
    for (i, t) in s.tables.iter().enumerate() {
        let field = format!("source.tables[{i}]");
        let tn =
            TableName::parse(&t.table).map_err(|e| err(&format!("{field}.table"), sentence(e)))?;
        if let Some(first) = seen.insert(tn.clone(), i) {
            return Err(err(
                &format!("{field}.table"),
                format!("{tn} is already source.tables[{first}]"),
            ));
        }
        check_queue_name(&format!("{field}.queue"), &t.queue)?;
        if let PartitionBy::Columns(cols) = &t.partition_by {
            check_columns(&format!("{field}.partitionBy"), cols)?;
        }
    }
    for (field, value) in [
        ("source.slot", doc.slot_name(name)),
        ("source.publication", doc.publication_name(name)),
    ] {
        if !valid_slot_name(&value) {
            return Err(err(
                field,
                format!("{value:?} does not match ^[a-z_][a-z0-9_]{{0,62}}$"),
            ));
        }
    }
    in_range(
        "source.snapshotChunkRows",
        s.snapshot_chunk_rows,
        100,
        100_000,
    )?;
    in_range("source.maxBundleMessages", s.max_bundle_messages, 1, 50_000)?;
    in_range(
        "source.maxBundleBytes",
        s.max_bundle_bytes,
        64 * 1024,
        64 * 1024 * 1024,
    )?;
    in_range("source.lingerMs", s.linger_ms, 0, 5_000)?;
    in_range("source.heartbeatSeconds", s.heartbeat_seconds, 1, 3_600)?;
    if s.on_truncate == TruncatePolicy::Emit {
        if let Some(i) = s.tables.iter().position(|t| !t.partition_by.is_single()) {
            return Err(err(
                "source.onTruncate",
                format!(
                    "emit needs partitionBy \"single\" on every table (source.tables[{i}] is \
                     not): a truncate cannot be ordered against rows spread over partitions"
                ),
            ));
        }
    }
    Ok(())
}

fn validate_sink(doc: &ConnectorDoc, s: &SinkSpec, name: &str) -> Result<()> {
    check_queue_name("sink.queue", &s.queue)?;
    if let Some(g) = &s.consumer_group {
        check_queue_name("sink.consumerGroup", g)?;
    }
    check_queue_name("sink.consumerGroup", &doc.consumer_group(name))?;
    if !matches!(s.subscription_mode.as_str(), "all" | "new") {
        return Err(err(
            "sink.subscriptionMode",
            format!("{:?}: expected all or new", s.subscription_mode),
        ));
    }
    let table = TableName::parse(&s.table).map_err(|e| err("sink.table", sentence(e)))?;

    match s.mode {
        SinkMode::Sql => {
            let st = s
                .statement
                .as_deref()
                .ok_or_else(|| err("sink.statement", "is required with mode sql"))?;
            if st.trim().is_empty() {
                return Err(err("sink.statement", "must not be empty"));
            }
            if st.len() > MAX_STATEMENT_BYTES {
                return Err(err(
                    "sink.statement",
                    format!("is {} bytes, the limit is {MAX_STATEMENT_BYTES}", st.len()),
                ));
            }
            let params = s.params.as_ref().ok_or_else(|| {
                err(
                    "sink.params",
                    "is required with mode sql (an empty list when the statement takes none)",
                )
            })?;
            for (i, p) in params.iter().enumerate() {
                crate::sink::Param::parse(p).map_err(|m| err(&format!("sink.params[{i}]"), m))?;
            }
        }
        SinkMode::Append | SinkMode::Upsert | SinkMode::Cdc => {
            if s.statement.is_some() {
                return Err(err("sink.statement", "is for mode sql only"));
            }
            if s.params.is_some() {
                return Err(err("sink.params", "is for mode sql only"));
            }
        }
    }
    if let Some(key) = &s.key {
        if !matches!(s.mode, SinkMode::Upsert | SinkMode::Cdc) {
            return Err(err("sink.key", "is for modes upsert and cdc only"));
        }
        check_columns("sink.key", key)?;
    }
    if let Some(m) = &s.metadata {
        if !matches!(s.mode, SinkMode::Append | SinkMode::Upsert) {
            return Err(err("sink.metadata", "is for modes append and upsert only"));
        }
        check_metadata(m)?;
    }
    in_range("sink.batch", s.batch, 1, 10_000)?;
    in_range("sink.workers", s.workers, 1, 32)?;
    in_range("sink.leaseSeconds", s.lease_seconds, 5, 3_600)?;
    in_range("sink.maxAttempts", s.max_attempts, 1, 100)?;
    if s.progress_table.split_once('.').is_none() {
        return Err(err(
            "sink.progressTable",
            format!("{:?}: expected schema.name", s.progress_table),
        ));
    }
    let progress =
        TableName::parse(&s.progress_table).map_err(|e| err("sink.progressTable", sentence(e)))?;
    if progress == table {
        return Err(err(
            "sink.progressTable",
            "must not be the target table itself",
        ));
    }
    Ok(())
}

fn check_metadata(m: &MetadataColumns) -> Result<()> {
    let fields = [
        ("partition", &m.partition),
        ("partitionId", &m.partition_id),
        ("offset", &m.offset),
        ("transactionId", &m.transaction_id),
        ("createdAt", &m.created_at),
        ("payload", &m.payload),
    ];
    let mut used: Vec<(&str, &str)> = Vec::new();
    for (f, col) in fields {
        let Some(col) = col else { continue };
        let field = format!("sink.metadata.{f}");
        if !valid_identifier(col) {
            return Err(err(
                &field,
                format!("{col:?} is not an identifier ^[A-Za-z_][A-Za-z0-9_$]{{0,62}}$"),
            ));
        }
        if let Some((other, _)) = used.iter().find(|(_, c)| *c == col) {
            return Err(err(
                &field,
                format!("column {col:?} is already sink.metadata.{other}"),
            ));
        }
        used.push((f, col));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// redacted
// ---------------------------------------------------------------------------

pub(crate) fn redacted(doc: &ConnectorDoc) -> Value {
    let mut v = serde_json::to_value(doc).unwrap_or(Value::Null);
    let has_password =
        doc.connection.password.is_some() || doc.connection.password_sealed.is_some();
    if let Some(c) = v.get_mut("connection").and_then(Value::as_object_mut) {
        c.remove("password");
        c.remove("passwordSealed");
        // A URL is normalized away before storing; one that was not could
        // carry the password, so it never leaves either.
        c.remove("url");
        c.insert("passwordSet".into(), Value::Bool(has_password));
    }
    v
}

// ---------------------------------------------------------------------------
// knobs
// ---------------------------------------------------------------------------

pub(crate) fn knobs_from_env(
    node: &str,
    proxy_embedded: bool,
) -> std::result::Result<NodeKnobs, String> {
    knobs_from(|k| std::env::var(k).ok(), node, proxy_embedded)
}

/// [`knobs_from_env`] over any lookup (the tests pass a map: the process
/// environment is shared by every test thread). An empty value reads as
/// unset, as deployment templates write `VAR=` for "default".
pub(crate) fn knobs_from(
    get: impl Fn(&str) -> Option<String>,
    node: &str,
    proxy_embedded: bool,
) -> std::result::Result<NodeKnobs, String> {
    let read = |var: &str| {
        get(var)
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    };
    let num = |var: &str, default: u64, lo: u64, hi: u64| -> std::result::Result<u64, String> {
        match read(var) {
            None => Ok(default),
            Some(v) => v
                .parse::<u64>()
                .ok()
                .filter(|n| (lo..=hi).contains(n))
                .ok_or_else(|| format!("{var}={v:?}: expected an integer in {lo}..={hi}")),
        }
    };
    let allow_private = match read("QUEEN_PG_ALLOW_PRIVATE_NETWORKS") {
        None => !proxy_embedded,
        Some(v) if v.eq_ignore_ascii_case("true") => true,
        Some(v) if v.eq_ignore_ascii_case("false") => false,
        Some(v) => {
            return Err(format!(
                "QUEEN_PG_ALLOW_PRIVATE_NETWORKS={v:?}: expected true or false"
            ))
        }
    };
    Ok(NodeKnobs {
        node: node.to_string(),
        threads: num("QUEEN_PG_THREADS", 2, 1, 16)? as usize,
        reload_ms: num("QUEEN_PG_RELOAD_MS", 5_000, 500, 60_000)?,
        lease_ttl_ms: num("QUEEN_PG_LEASE_TTL_MS", 10_000, 3_000, 120_000)?,
        shutdown_grace_ms: num("QUEEN_PG_SHUTDOWN_GRACE_MS", 10_000, 0, 120_000)?,
        allow_private_networks: allow_private,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn source_doc() -> Value {
        json!({
            "kind": "source",
            "connection": {"host": "db.example.com", "database": "app", "user": "cdc", "password": "pw"},
            "source": {"tables": [
                {"table": "public.orders", "queue": "orders", "partitionBy": "key"},
                {"table": "payments", "queue": "payments", "partitionBy": ["customer_id"]}
            ]}
        })
    }

    fn sink_doc() -> Value {
        json!({
            "kind": "sink",
            "connection": {"host": "10.0.0.5", "port": 6432, "database": "dw", "user": "w"},
            "sink": {"queue": "orders", "table": "public.orders_copy", "mode": "upsert", "key": ["id"]}
        })
    }

    fn doc(v: Value) -> ConnectorDoc {
        ConnectorDoc::from_json(&v).unwrap()
    }

    fn refused(v: Value, field: &str) {
        let d = doc(v);
        let e = d.validate("c1").expect_err(field);
        assert_eq!(e.code(), "config");
        let m = e.to_string();
        assert!(
            m.contains(&format!("{field}:")),
            "expected {field:?} in {m:?}"
        );
    }

    fn with(mut v: Value, path: &[&str], value: Value) -> Value {
        let mut cur = &mut v;
        for p in &path[..path.len() - 1] {
            cur = if let Ok(i) = p.parse::<usize>() {
                &mut cur[i]
            } else {
                &mut cur[*p]
            };
        }
        let last = path[path.len() - 1];
        if value.is_null() {
            cur.as_object_mut().unwrap().remove(last);
        } else {
            cur[last] = value;
        }
        v
    }

    #[test]
    fn good_documents_validate() {
        doc(source_doc()).validate("orders-src").unwrap();
        doc(sink_doc()).validate("c1").unwrap();
        let sql = json!({
            "kind": "sink",
            "connection": {"host": "db", "database": "d", "user": "u", "sslMode": "verify-full",
                           "sslRootCert": crate::pg::connect::TEST_ROOT_PEM},
            "sink": {"queue": "q", "table": "accounts", "mode": "sql",
                     "statement": "UPDATE accounts SET balance = balance + $1::numeric WHERE id = $2::bigint",
                     "params": ["$.amount", "$.account_id"]}
        });
        doc(sql).validate("c1").unwrap();
        let mut single = source_doc();
        single["source"]["tables"][0]["partitionBy"] = json!("single");
        single["source"]["tables"][1]["partitionBy"] = json!("single");
        single["source"]["onTruncate"] = json!("emit");
        doc(single).validate("c1").unwrap();
    }

    #[test]
    fn the_name_and_the_kind() {
        let d = doc(source_doc());
        for bad in ["", "Orders", "-x", "a b", &"a".repeat(49)] {
            let e = d.validate(bad).unwrap_err();
            assert!(e.to_string().contains("name:"), "{bad:?}: {e}");
        }
        let mut both = source_doc();
        both["sink"] = sink_doc()["sink"].clone();
        refused(both, "kind");
        refused(with(source_doc(), &["kind"], json!("sink")), "kind");
    }

    #[test]
    fn connection_rules() {
        refused(
            with(source_doc(), &["connection", "host"], json!("")),
            "connection.host",
        );
        refused(
            with(
                source_doc(),
                &["connection", "host"],
                json!("/var/run/postgresql"),
            ),
            "connection.host",
        );
        refused(
            with(source_doc(), &["connection", "host"], json!("a b")),
            "connection.host",
        );
        refused(
            with(source_doc(), &["connection", "port"], json!(0)),
            "connection.port",
        );
        refused(
            with(source_doc(), &["connection", "database"], json!("")),
            "connection.database",
        );
        refused(
            with(source_doc(), &["connection", "user"], json!("")),
            "connection.user",
        );
        refused(
            with(
                source_doc(),
                &["connection", "connectTimeoutMs"],
                json!(999),
            ),
            "connection.connectTimeoutMs",
        );
        refused(
            with(
                source_doc(),
                &["connection", "connectTimeoutMs"],
                json!(120_001),
            ),
            "connection.connectTimeoutMs",
        );
        let pem = json!(crate::pg::connect::TEST_ROOT_PEM);
        refused(
            with(source_doc(), &["connection", "sslRootCert"], pem.clone()),
            "connection.sslRootCert",
        );
        let vf = with(
            source_doc(),
            &["connection", "sslMode"],
            json!("verify-full"),
        );
        doc(with(vf.clone(), &["connection", "sslRootCert"], pem))
            .validate("c")
            .unwrap();
        refused(
            with(vf, &["connection", "sslRootCert"], json!("garbage")),
            "connection.sslRootCert",
        );
        refused(
            with(
                source_doc(),
                &["connection", "url"],
                json!("postgres://h/d"),
            ),
            "connection.url",
        );
        doc(with(
            source_doc(),
            &["connection", "host"],
            json!("[2001:db8::1]"),
        ))
        .validate("c")
        .unwrap();
        doc(with(
            source_doc(),
            &["connection", "connectTimeoutMs"],
            json!(1000),
        ))
        .validate("c")
        .unwrap();
    }

    #[test]
    fn source_rules() {
        refused(
            with(source_doc(), &["source", "tables"], json!([])),
            "source.tables",
        );
        let many: Vec<Value> = (0..257)
            .map(|i| json!({"table": format!("t{i}"), "queue": "q"}))
            .collect();
        refused(
            with(source_doc(), &["source", "tables"], json!(many)),
            "source.tables",
        );
        let ok: Vec<Value> = (0..256)
            .map(|i| json!({"table": format!("t{i}"), "queue": "q"}))
            .collect();
        doc(with(source_doc(), &["source", "tables"], json!(ok)))
            .validate("c")
            .unwrap();
        refused(
            with(
                source_doc(),
                &["source", "tables", "1", "table"],
                json!("a.b.c"),
            ),
            "source.tables[1].table",
        );
        refused(
            with(
                source_doc(),
                &["source", "tables", "1", "table"],
                json!("orders"),
            ),
            "source.tables[1].table",
        );
        refused(
            with(source_doc(), &["source", "tables", "0", "queue"], json!("")),
            "source.tables[0].queue",
        );
        refused(
            with(
                source_doc(),
                &["source", "tables", "0", "queue"],
                json!("x".repeat(129)),
            ),
            "source.tables[0].queue",
        );
        refused(
            with(
                source_doc(),
                &["source", "tables", "0", "queue"],
                json!("a\nb"),
            ),
            "source.tables[0].queue",
        );
        doc(with(
            source_doc(),
            &["source", "tables", "0", "queue"],
            json!("x".repeat(128)),
        ))
        .validate("c")
        .unwrap();
        refused(
            with(
                source_doc(),
                &["source", "tables", "1", "partitionBy"],
                json!([]),
            ),
            "source.tables[1].partitionBy",
        );
        refused(
            with(
                source_doc(),
                &["source", "tables", "1", "partitionBy"],
                json!(["a", "a"]),
            ),
            "source.tables[1].partitionBy[1]",
        );
        refused(
            with(
                source_doc(),
                &["source", "tables", "1", "partitionBy"],
                json!(["a-b"]),
            ),
            "source.tables[1].partitionBy[0]",
        );
        refused(
            with(source_doc(), &["source", "slot"], json!("Bad")),
            "source.slot",
        );
        refused(
            with(source_doc(), &["source", "publication"], json!("9pub")),
            "source.publication",
        );
        refused(
            with(source_doc(), &["source", "slot"], json!("x".repeat(64))),
            "source.slot",
        );
        doc(with(source_doc(), &["source", "slot"], json!("_my_slot_2")))
            .validate("c")
            .unwrap();
        for (f, lo, hi) in [
            ("snapshotChunkRows", 100u64, 100_000u64),
            ("maxBundleMessages", 1, 50_000),
            ("maxBundleBytes", 65_536, 67_108_864),
            ("lingerMs", 0, 5_000),
            ("heartbeatSeconds", 1, 3_600),
        ] {
            doc(with(source_doc(), &["source", f], json!(lo)))
                .validate("c")
                .unwrap();
            doc(with(source_doc(), &["source", f], json!(hi)))
                .validate("c")
                .unwrap();
            refused(
                with(source_doc(), &["source", f], json!(hi + 1)),
                &format!("source.{f}"),
            );
            if lo > 0 {
                refused(
                    with(source_doc(), &["source", f], json!(lo - 1)),
                    &format!("source.{f}"),
                );
            }
        }
        refused(
            with(source_doc(), &["source", "onTruncate"], json!("emit")),
            "source.onTruncate",
        );
    }

    #[test]
    fn sink_rules() {
        refused(
            with(sink_doc(), &["sink", "queue"], json!("")),
            "sink.queue",
        );
        refused(
            with(sink_doc(), &["sink", "consumerGroup"], json!("")),
            "sink.consumerGroup",
        );
        refused(
            with(sink_doc(), &["sink", "subscriptionMode"], json!("latest")),
            "sink.subscriptionMode",
        );
        doc(with(
            sink_doc(),
            &["sink", "subscriptionMode"],
            json!("new"),
        ))
        .validate("c")
        .unwrap();
        refused(
            with(sink_doc(), &["sink", "table"], json!("x.y.z")),
            "sink.table",
        );
        refused(with(sink_doc(), &["sink", "key"], json!([])), "sink.key");
        refused(
            with(sink_doc(), &["sink", "key"], json!(["id", "id"])),
            "sink.key[1]",
        );
        refused(
            with(sink_doc(), &["sink", "key"], json!(["Bad Col"])),
            "sink.key[0]",
        );
        let append = with(
            with(sink_doc(), &["sink", "mode"], json!("append")),
            &["sink", "key"],
            Value::Null,
        );
        doc(append.clone()).validate("c").unwrap();
        refused(
            with(append.clone(), &["sink", "key"], json!(["id"])),
            "sink.key",
        );
        refused(
            with(append.clone(), &["sink", "statement"], json!("SELECT 1")),
            "sink.statement",
        );
        refused(
            with(append.clone(), &["sink", "params"], json!([])),
            "sink.params",
        );
        let meta = with(
            append.clone(),
            &["sink", "metadata"],
            json!({"offset": "queen_offset", "partition": "queen_partition"}),
        );
        doc(meta).validate("c").unwrap();
        refused(
            with(
                append.clone(),
                &["sink", "metadata"],
                json!({"offset": "o", "partition": "o"}),
            ),
            "sink.metadata.offset",
        );
        refused(
            with(
                append.clone(),
                &["sink", "metadata"],
                json!({"payload": "1x"}),
            ),
            "sink.metadata.payload",
        );
        let cdc = with(sink_doc(), &["sink", "mode"], json!("cdc"));
        doc(cdc.clone()).validate("c").unwrap();
        refused(
            with(cdc, &["sink", "metadata"], json!({"offset": "o"})),
            "sink.metadata",
        );
        let sql = with(
            with(sink_doc(), &["sink", "mode"], json!("sql")),
            &["sink", "key"],
            Value::Null,
        );
        refused(sql.clone(), "sink.statement");
        let sql = with(sql, &["sink", "statement"], json!("SELECT $1"));
        refused(sql.clone(), "sink.params");
        doc(with(sql.clone(), &["sink", "params"], json!([])))
            .validate("c")
            .unwrap();
        doc(with(
            sql.clone(),
            &["sink", "params"],
            json!(["$", "@offset", "$.a[2].b"]),
        ))
        .validate("c")
        .unwrap();
        refused(
            with(sql.clone(), &["sink", "params"], json!(["$", "@nope"])),
            "sink.params[1]",
        );
        refused(
            with(sql.clone(), &["sink", "statement"], json!("   ")),
            "sink.statement",
        );
        refused(
            with(
                sql.clone(),
                &["sink", "statement"],
                json!("x".repeat(64 * 1024 + 1)),
            ),
            "sink.statement",
        );
        for (f, lo, hi) in [
            ("batch", 1u64, 10_000u64),
            ("workers", 1, 32),
            ("leaseSeconds", 5, 3_600),
            ("maxAttempts", 1, 100),
        ] {
            doc(with(sink_doc(), &["sink", f], json!(lo)))
                .validate("c")
                .unwrap();
            doc(with(sink_doc(), &["sink", f], json!(hi)))
                .validate("c")
                .unwrap();
            refused(
                with(sink_doc(), &["sink", f], json!(lo - 1)),
                &format!("sink.{f}"),
            );
            refused(
                with(sink_doc(), &["sink", f], json!(hi + 1)),
                &format!("sink.{f}"),
            );
        }
        refused(
            with(sink_doc(), &["sink", "progressTable"], json!("progress")),
            "sink.progressTable",
        );
        refused(
            with(
                sink_doc(),
                &["sink", "progressTable"],
                json!("public.orders_copy"),
            ),
            "sink.progressTable",
        );
        doc(with(
            sink_doc(),
            &["sink", "progressTable"],
            json!("ops.Progress"),
        ))
        .validate("c")
        .unwrap();
    }

    #[test]
    fn urls_normalize_into_the_fields() {
        let mut d = doc(json!({
            "kind": "sink",
            "connection": {"url": "postgresql://us%40er:p%3Aw%2Fd@db.example.com:6543/my%20db?sslmode=verify-full&connect_timeout=7"},
            "sink": {"queue": "q", "table": "t", "mode": "append"}
        }));
        d.normalize().unwrap();
        let c = &d.connection;
        assert_eq!(c.url, None);
        assert_eq!(c.user, "us@er");
        assert_eq!(c.password.as_deref(), Some("p:w/d"));
        assert_eq!(c.host, "db.example.com");
        assert_eq!(c.port, 6543);
        assert_eq!(c.database, "my db");
        assert_eq!(c.ssl_mode, SslMode::VerifyFull);
        assert_eq!(c.connect_timeout_ms, 7_000);
        d.validate("c").unwrap();

        let p = parse_url("postgres://[::1]:5433/d").unwrap();
        assert_eq!((p.host.as_deref(), p.port), (Some("::1"), Some(5433)));
        let p = parse_url("postgres://u@h").unwrap();
        assert_eq!(
            (p.user.as_deref(), p.password, p.port, p.database),
            (Some("u"), None, None, None)
        );
        let p = parse_url("postgres://u:p@ss@h/db").unwrap();
        assert_eq!(p.password.as_deref(), Some("p@ss"));
        let p = parse_url("postgres:///db?host=h&port=1&user=u&password=x&dbname=other").unwrap();
        assert_eq!(p.host.as_deref(), Some("h"));
        assert_eq!(p.database.as_deref(), Some("other"));
        for bad in [
            "mysql://h/d",
            "postgres://h1,h2/d",
            "postgres://h:0/d",
            "postgres://h:99999/d",
            "postgres://h/d?sslmode=verify-ca",
            "postgres://h/d?sslmode=nope",
            "postgres://h/d?sslrootcert=/x.pem",
            "postgres://h/d?target_session_attrs=any",
            "postgres://h/d?connect_timeout=x",
            "postgres://h/d%zz",
            "postgres://[::1/d",
        ] {
            assert!(parse_url(bad).is_err(), "{bad}");
        }
        // A URL without a password keeps the document's.
        let mut d = doc(json!({
            "kind": "sink",
            "connection": {"url": "postgres://u@h/d", "password": "kept"},
            "sink": {"queue": "q", "table": "t", "mode": "append"}
        }));
        d.normalize().unwrap();
        assert_eq!(d.connection.password.as_deref(), Some("kept"));
        let mut bad = d.clone();
        bad.connection.url = Some("http://x".into());
        let e = bad.normalize().unwrap_err();
        assert!(e.to_string().contains("connection.url:"), "{e}");
    }

    #[test]
    fn redaction_never_returns_a_password() {
        let mut d = doc(source_doc());
        let v = d.redacted();
        assert_eq!(v["connection"]["passwordSet"], true);
        assert!(v["connection"].get("passwordSealed").is_none());
        assert_eq!(v["source"]["tables"][0]["queue"], "orders");
        assert!(!v.to_string().contains("\"pw\""));
        d.connection.password = None;
        d.connection.password_sealed = Some("{\"sealed\":\"zzz\"}".into());
        d.connection.url = Some("postgres://u:secret@h/d".into());
        let v = d.redacted();
        assert_eq!(v["connection"]["passwordSet"], true);
        assert!(!v.to_string().contains("zzz"));
        assert!(!v.to_string().contains("secret"));
        d.connection.password_sealed = None;
        assert!(d.redacted()["connection"].get("password").is_none());
        assert_eq!(d.redacted()["connection"]["passwordSet"], false);
        // A read written back as it came parses (passwordSet is read-only).
        let back = ConnectorDoc::from_json(&d.redacted()).unwrap();
        assert_eq!(back.connection.password, None);
        assert_eq!(back.connection.password_sealed, None);
    }

    #[test]
    fn knobs_have_defaults_ranges_and_the_egress_default() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |k: &str| {
                pairs
                    .iter()
                    .find(|(n, _)| *n == k)
                    .map(|(_, v)| v.to_string())
            }
        };
        let k = knobs_from(env(&[]), "n1", false).unwrap();
        assert_eq!(k, NodeKnobs::defaults("n1"));
        assert!(
            !knobs_from(env(&[]), "n1", true)
                .unwrap()
                .allow_private_networks
        );
        let k = knobs_from(
            env(&[
                ("QUEEN_PG_THREADS", "16"),
                ("QUEEN_PG_RELOAD_MS", "500"),
                ("QUEEN_PG_LEASE_TTL_MS", "120000"),
                ("QUEEN_PG_SHUTDOWN_GRACE_MS", "0"),
                ("QUEEN_PG_ALLOW_PRIVATE_NETWORKS", "FALSE"),
            ]),
            "n2",
            false,
        )
        .unwrap();
        assert_eq!(
            (k.threads, k.reload_ms, k.lease_ttl_ms, k.shutdown_grace_ms),
            (16, 500, 120_000, 0)
        );
        assert!(!k.allow_private_networks);
        assert!(
            knobs_from(
                env(&[("QUEEN_PG_ALLOW_PRIVATE_NETWORKS", "true")]),
                "n",
                true
            )
            .unwrap()
            .allow_private_networks
        );
        assert_eq!(
            knobs_from(env(&[("QUEEN_PG_THREADS", " ")]), "n", false)
                .unwrap()
                .threads,
            2
        );
        for (var, v) in [
            ("QUEEN_PG_THREADS", "0"),
            ("QUEEN_PG_THREADS", "17"),
            ("QUEEN_PG_THREADS", "two"),
            ("QUEEN_PG_RELOAD_MS", "499"),
            ("QUEEN_PG_LEASE_TTL_MS", "2999"),
            ("QUEEN_PG_SHUTDOWN_GRACE_MS", "120001"),
            ("QUEEN_PG_SHUTDOWN_GRACE_MS", "-1"),
            ("QUEEN_PG_ALLOW_PRIVATE_NETWORKS", "yes"),
        ] {
            let pairs: &'static [(&'static str, &'static str)] =
                Box::leak(vec![(var, v)].into_boxed_slice());
            let e = knobs_from(env(pairs), "n", false).unwrap_err();
            assert!(e.contains(var), "{var}={v}: {e}");
        }
    }
}
