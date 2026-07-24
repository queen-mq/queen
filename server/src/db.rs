use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use deadpool_postgres::{Pool, PoolConfig, Runtime};
use tokio_postgres::{CancelToken, NoTls};

use crate::config::Config;

pub fn create_pool(cfg: &Config) -> Pool {
    let mut pg = cfg.pg.clone();
    pg.pool = Some(PoolConfig::new(cfg.pool_size));
    // RUSTFIX item 5: wire a rustls connector when PG_USE_SSL=true, else the
    // byte-for-byte-unchanged NoTls path. The two arms have divergent TlsConnect
    // types, so the pool-config mutation above is shared and only the terminal
    // create_pool differs.
    if cfg.pg_use_ssl {
        // Force TLS negotiation (C++ appended sslmode=require). Chain verification
        // is delegated to the rustls connector's ClientConfig, gated by
        // PG_SSL_REJECT_UNAUTHORIZED.
        pg.ssl_mode = Some(deadpool_postgres::SslMode::Require);
        let connector = crate::pgtls::make_connector(cfg.pg_ssl_reject_unauthorized);
        pg.create_pool(Some(Runtime::Tokio1), connector)
            .expect("failed to create TLS pool")
    } else {
        pg.create_pool(Some(Runtime::Tokio1), NoTls)
            .expect("failed to create pool")
    }
}

// ============================================================================
// Server-side statement cancellation on broker-side timeout.
//
// A `tokio::time::timeout(stmt_timeout, db_call)` that elapses DROPS the in-flight
// query future but leaves the PostgreSQL backend running the statement — still
// holding its locks. Under the first-contact `Lock:transactionid` convoy (18-log-
// engine.md) that is the terminal amplifier: abandoned-but-still-executing
// statements accumulate and poison the pool. These helpers (a) fire a best-effort
// SERVER-SIDE cancel so the backend actually stops, and (b) QUARANTINE the pooled
// connection (deadpool `Object::take`) so a connection whose statement we abandoned
// is never recycled to the next caller.
// ============================================================================

/// TLS connector for out-of-band query cancellation. `CancelToken::cancel_query`
/// opens a NEW short-lived connection to deliver the cancel request, so it needs a
/// connector matching how the pool itself was built (`create_pool` / pgtls.rs):
/// `NoTls` normally, a rustls connector when `PG_USE_SSL=true`. Built once from the
/// same env `config::load()` reads, so a mismatch with the pool is impossible.
#[derive(Clone)]
enum CancelTls {
    NoTls,
    Rustls(tokio_postgres_rustls::MakeRustlsConnect),
}

fn cancel_tls() -> CancelTls {
    static CANCEL_TLS: OnceLock<CancelTls> = OnceLock::new();
    CANCEL_TLS
        .get_or_init(|| {
            // Mirror config::load(): PG_USE_SSL / PG_SSL_REJECT_UNAUTHORIZED, with
            // the C++ get_env_bool parity (only the exact literal "true" is truthy).
            let use_ssl = std::env::var("PG_USE_SSL").map(|v| v == "true").unwrap_or(false);
            if use_ssl {
                let reject = std::env::var("PG_SSL_REJECT_UNAUTHORIZED")
                    .map(|v| v == "true")
                    .unwrap_or(true);
                CancelTls::Rustls(crate::pgtls::make_connector(reject))
            } else {
                CancelTls::NoTls
            }
        })
        .clone()
}

/// Log at most once per 30s per event class, so a cancellation storm (exactly the
/// scenario this exists for) does not flood the log. `what` is the event class
/// (e.g. "pop_wildcard").
fn log_cancel_once(what: &'static str, msg: &str) {
    static LAST: OnceLock<Mutex<std::collections::HashMap<&'static str, Instant>>> = OnceLock::new();
    let map = LAST.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
    let mut g = map.lock().unwrap();
    let now = Instant::now();
    let fire = g.get(what).map_or(true, |&t| now.duration_since(t) >= Duration::from_secs(30));
    if fire {
        g.insert(what, now);
        drop(g);
        tracing::warn!(target: "db", op = %what, detail = %msg, "statement-timeout cancel");
    }
}

/// Best-effort SERVER-SIDE cancellation of the statement whose broker-side future a
/// tokio timeout abandoned. Spawned detached (cancellation is inherently racy and
/// must never block the request path); `cancel_query` opens its OWN connection, so
/// it does not touch the poisoned pooled connection. Logging is throttled per class.
fn spawn_cancel(cancel: CancelToken, what: &'static str) {
    let tls = cancel_tls();
    log_cancel_once(what, &format!("stmt-timeout ({what}): issuing server-side query cancellation"));
    tokio::spawn(async move {
        let r = match tls {
            CancelTls::NoTls => cancel.cancel_query(NoTls).await,
            CancelTls::Rustls(c) => cancel.cancel_query(c).await,
        };
        if let Err(e) = r {
            log_cancel_once(what, &format!("stmt-timeout ({what}): cancel request could not be delivered: {e}"));
        }
    });
}

/// Resolve a `tokio::time::timeout(stmt_timeout, db_call).await` result, handling
/// the timeout arm with server-side cancellation + connection quarantine.
///
/// * `Ok(Ok(v))`  — success: the pooled connection is healthy, so it is dropped
///   back to the pool (this also satisfies the spec-§10 "release the connection
///   before a parked long-poll" rule the callers rely on), and `Some(v)` returned.
/// * `Ok(Err(_))` — the statement itself errored (not a timeout): the connection is
///   still usable, so it is dropped back to the pool; `None`.
/// * `Err(Elapsed)` — broker-side timeout: fire a best-effort server-side cancel and
///   DETACH the connection from the pool (never recycle one whose in-flight
///   statement we abandoned + cancelled); `None`.
pub fn resolve_query_timeout<T>(
    res: Result<Result<T, tokio_postgres::Error>, tokio::time::error::Elapsed>,
    client: deadpool_postgres::Client,
    cancel: CancelToken,
    what: &'static str,
) -> Option<T> {
    match res {
        Ok(Ok(v)) => {
            drop(client);
            Some(v)
        }
        Ok(Err(_)) => {
            drop(client);
            None
        }
        Err(_elapsed) => {
            spawn_cancel(cancel, what);
            // Quarantine: take() removes the object from the pool permanently; the
            // returned ClientWrapper is dropped here, closing the connection so the
            // still-running (now cancelled) statement's connection is not reused.
            let _ = deadpool_postgres::Object::take(client);
            None
        }
    }
}

// Lowercase hex of a 16-byte txn hash — the token format queen.log_ack_by_hash_v1
// returns in noopHashes/staleHashes (PG encode(..,'hex') is lowercase).
fn hex16(b: &[u8; 16]) -> String {
    let mut s = String::with_capacity(32);
    for x in b {
        use std::fmt::Write as _;
        let _ = write!(s, "{:02x}", x);
    }
    s
}

// ------------------------------------------------------------------------ ack
// NEW (log engine): positional ack — advance the (queue, partition, group)
// cursor to the absolute offset `upto` via queen.log_ack_v1 (044). `ok=false`
// releases the lease without advancing (full redelivery). Returns the SP JSON
// as text: {"ok":bool,"off":i64,"acked":i64,...} or {"ok":false,"error":..}.
#[allow(clippy::too_many_arguments)]
pub async fn ack_position(
    client: &deadpool_postgres::Client,
    queue: &str,
    partition: &str,
    group: &str,
    worker: &str,
    upto: i64,
    ok: bool,
    count: i32,
) -> Result<String, tokio_postgres::Error> {
    let stmt = client
        .prepare_cached(
            "SELECT (queen.log_ack_v1($1, $2, $3, $4, $5::bigint, $6::bool, $7::int))::text",
        )
        .await?;
    let row = client
        .query_one(&stmt, &[&queue, &partition, &group, &worker, &upto, &ok, &count])
        .await?;
    Ok(row.get(0))
}

// ACK REGISTRY fast path (server/src/ack_registry.rs): partition-id-addressed
// positional ack via queen.log_ack_at_v1 (044) — the pid-keyed twin of
// log_ack_v1. The registry HIT already proved (worker matches, whole batch
// completed), so this is a single positional cursor advance to `upto`
// (= batch_end) with NO hash resolution. log_ack_at_v1 STILL re-validates the
// lease under the consumer row lock, so a stale HIT for an expired/reassigned
// lease returns {"ok":false,...} and the caller falls back to ack_by_hash.
// Returns the SP JSON as text ({"ok":bool,"acked":i64,...}). $1::text::uuid pins
// $1 to TEXT so the partition_id &str binds.
#[allow(clippy::too_many_arguments)]
pub async fn ack_at(
    client: &deadpool_postgres::Client,
    partition_id: &str,
    group: &str,
    worker: &str,
    upto: i64,
    ok: bool,
    count: i32,
) -> Result<String, tokio_postgres::Error> {
    let stmt = client
        .prepare_cached(
            "SELECT (queen.log_ack_at_v1($1::text::uuid, $2, $3, $4::bigint, $5::bool, $6::int))::text",
        )
        .await?;
    let row = client
        .query_one(&stmt, &[&partition_id, &group, &worker, &upto, &ok, &count])
        .await?;
    Ok(row.get(0))
}

// ACK FUSION (server/src/ack_fusion.rs): N positional full-batch cursor advances
// in ONE transaction via queen.log_ack_multi_v1 (044) — the ack-side twin of
// log_push_multi_v1. Each row is the pid-keyed log_ack_at_v1 decision (p_ok=true,
// upto=batch_end); one commit / one fsync collapses the ack commit rate under
// load. All five arrays are index-aligned; the SP returns
// {"ok":true,"results":[{"ok":bool,"acked":i64,"error":?}, ...]} in INPUT order
// (the SP writes by ordinal). The pids travel as text[] ($1::text[] so the &str
// slice binds); the SP casts each u.pid::uuid internally.
pub async fn ack_multi(
    client: &deadpool_postgres::Client,
    pids: &[String],
    groups: &[String],
    ends: &[i64],
    workers: &[String],
    counts: &[i32],
) -> Result<String, tokio_postgres::Error> {
    let stmt = client
        .prepare_cached(
            "SELECT (queen.log_ack_multi_v1($1::text[], $2::text[], $3::int8[], $4::text[], $5::int4[]))::text",
        )
        .await?;
    let row = client
        .query_one(&stmt, &[&pids, &groups, &ends, &workers, &counts])
        .await?;
    Ok(row.get(0))
}

// NEW (log engine): hash-addressed ack for ONE (partition, group, worker) via
// queen.log_ack_by_hash_v1 (044). `hashes` are the 16-byte xxh3_128 txn hashes
// (crate::util::txn_hash128), index-aligned with `statuses`
// (completed|failed|retry|dlq). Returns the SP JSON as text — the 044 contract:
// {ok, off, acked, dlq?/dropped?, noopHashes:[32-hex], staleHashes:[32-hex],
// error?}; the CALLER maps hex hashes back to txn strings (it has the request's
// txn list). $1::text::uuid pins $1 to TEXT so a &str binds.
pub async fn ack_by_hash(
    client: &deadpool_postgres::Client,
    partition_id: &str,
    group: &str,
    worker: &str,
    hashes: &[Vec<u8>],
    statuses: &[String],
) -> Result<String, tokio_postgres::Error> {
    let stmt = client
        .prepare_cached(
            "SELECT (queen.log_ack_by_hash_v1($1::text::uuid, $2, $3, $4::bytea[], $5::text[]))::text",
        )
        .await?;
    let row = client
        .query_one(&stmt, &[&partition_id, &group, &worker, &hashes, &statuses])
        .await?;
    Ok(row.get(0))
}

// Ack a set of (transactionId -> status) for ONE (partition, consumer_group,
// worker). COMPATIBILITY wrapper kept with its pre-log signature: the log SQL is
// hash-addressed (SQL never sees txn strings, §3), so this parses `acks_json`
// ([{"txn":..,"status":..}] — legacy [{"txn":..,"ok":bool}] accepted), computes
// the 16B xxh3_128 hashes broker-side, calls queen.log_ack_by_hash_v1, and maps
// the returned noopHashes/staleHashes back to noopTxns/staleTxns for the caller.
// The dlq-handoff position is mirrored into the legacy "seq" key ("frameIdx":0)
// alongside the contract's "off". Prefer ack_by_hash on new code paths.
pub async fn ack_by_txn(
    client: &deadpool_postgres::Client,
    partition_id: &str,
    group: &str,
    worker: &str,
    acks_json: &str,
) -> Result<String, tokio_postgres::Error> {
    let items: Vec<serde_json::Value> =
        serde_json::from_str(acks_json).unwrap_or_default();
    let mut hashes: Vec<Vec<u8>> = Vec::with_capacity(items.len());
    let mut statuses: Vec<String> = Vec::with_capacity(items.len());
    let mut by_hex: std::collections::HashMap<String, String> =
        std::collections::HashMap::with_capacity(items.len());
    for it in &items {
        let txn = it.get("txn").and_then(|x| x.as_str()).unwrap_or("").to_string();
        let status = match it.get("status").and_then(|x| x.as_str()) {
            Some(s) => s.to_string(),
            // Legacy boolean form: ok -> completed, !ok -> failed.
            None => match it.get("ok").and_then(|x| x.as_bool()) {
                Some(true) | None => "completed".to_string(),
                Some(false) => "failed".to_string(),
            },
        };
        let h = crate::util::txn_hash128(&txn);
        by_hex.insert(hex16(&h), txn);
        hashes.push(h.to_vec());
        statuses.push(status);
    }

    let raw = ack_by_hash(client, partition_id, group, worker, &hashes, &statuses).await?;

    // Post-process: hex hash lists -> txn lists; legacy position keys. On any
    // parse hiccup return the SP text untouched (the caller degrades gracefully).
    let mut v: serde_json::Value = match serde_json::from_str(&raw) {
        Ok(v) => v,
        Err(_) => return Ok(raw),
    };
    if let Some(obj) = v.as_object_mut() {
        for (hkey, tkey) in [("noopHashes", "noopTxns"), ("staleHashes", "staleTxns")] {
            let mapped: Vec<serde_json::Value> = obj
                .get(hkey)
                .and_then(|x| x.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|h| h.as_str())
                        .filter_map(|h| by_hex.get(&h.to_ascii_lowercase()))
                        .map(|t| serde_json::Value::String(t.clone()))
                        .collect()
                })
                .unwrap_or_default();
            if !mapped.is_empty() {
                obj.insert(tkey.to_string(), serde_json::Value::Array(mapped));
            }
        }
        // Legacy (seq, frameIdx) mirror of the scalar offset for pre-log callers.
        if let Some(off) = obj.get("off").and_then(|x| x.as_i64()) {
            obj.insert("seq".to_string(), serde_json::Value::from(off));
            obj.insert("frameIdx".to_string(), serde_json::Value::from(0));
        }
    }
    Ok(v.to_string())
}

// Dead-letter the poison HEAD frame of a leased batch via queen.log_dlq_head_v1
// (044). Called by the broker after the ack SP signalled `dlq:true` on a nack
// whose delivery attempt exceeded the queue's retry_limit: the broker alone can
// decompress the frame, so it extracts (message_id, txn, payload) from the
// leased segment and passes them here as a SNAPSHOT. Under the still-held lease
// the SP files the queen.log_dlq row, advances the cursor past the frame,
// resets the attempt state and releases the lease. Returns {"ok":bool,...} as
// text. Signature kept from the seg engine: the position now travels in `seq`
// (the absolute message OFFSET); `frame_idx` is vestigial and ignored (pass 0).
#[allow(clippy::too_many_arguments)]
pub async fn seg_dlq_head(
    client: &deadpool_postgres::Client,
    partition_id: &str,
    group: &str,
    worker: &str,
    seq: i64,
    _frame_idx: i32,
    message_id: &str,
    txn: &str,
    payload_json: &str,
    error: &str,
) -> Result<String, tokio_postgres::Error> {
    let stmt = client
        .prepare_cached(
            "SELECT (queen.log_dlq_head_v1($1::text::uuid, $2, $3, $4::bigint, \
             $5::text::uuid, $6, $7::text::jsonb, $8))::text",
        )
        .await?;
    let row = client
        .query_one(
            &stmt,
            &[
                &partition_id,
                &group,
                &worker,
                &seq,
                &message_id,
                &txn,
                &payload_json,
                &error,
            ],
        )
        .await?;
    Ok(row.get(0))
}

// Specific-partition pop (+auto-ack when auto_ack). queen.log_pop_v1 (043)
// returns raw segment rows; the single-partition wire JSON the callers expect
// is assembled here in SQL, byte-compatible with the retired seg-engine
// specific-pop wire shape:
// {"segments":[{seq,startOff,take,msgCount,createdAt,blob}],"partitionId":..,"attempt":..}.
// `seq` carries base_offset and `startOff` the start frame index (§11 opaque
// tokens). The r_attempt column is gone from the log pop (043: the display
// delivery count is broker-owned) — "attempt" is emitted as 0 for shape compat.
#[allow(clippy::too_many_arguments)]
pub async fn pop_specific(
    client: &deadpool_postgres::Client,
    queue: &str,
    partition: &str,
    group: &str,
    budget: i32,
    lease_seconds: i32,
    worker: &str,
    auto_ack: bool,
    sub_mode: &str,
    sub_from: &str,
) -> Result<String, tokio_postgres::Error> {
    let stmt = client
        .prepare_cached(
            "WITH seg AS ( \
                 SELECT * FROM queen.log_pop_v1($1, $2, $3, $4::int, $5::int, $6, $7::bool, $8, $9) \
             ) \
             SELECT (jsonb_build_object( \
                 'segments', COALESCE((SELECT jsonb_agg(jsonb_build_object( \
                     'seq', s.r_base, \
                     'startOff', s.r_start_idx, \
                     'take', s.r_take, \
                     'msgCount', s.r_msg_count, \
                     'createdAt', to_char(s.r_created_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'), \
                     'blob', encode(s.r_blob, 'base64') \
                 ) ORDER BY s.r_base) FROM seg s), '[]'::jsonb), \
                 'partitionId', COALESCE((SELECT p.id::text FROM queen.log_partitions p \
                     JOIN queen.log_queues q ON q.id = p.queue_id \
                     WHERE q.name = $1 AND p.name = $2), ''), \
                 'attempt', 0))::text",
        )
        .await?;
    let row = client
        .query_one(
            &stmt,
            &[&queue, &partition, &group, &budget, &lease_seconds, &worker, &auto_ack,
              &sub_mode, &sub_from],
        )
        .await?;
    Ok(row.get(0))
}

// 19-wildcard-hotlist §7/§9: candidate-list pop. The broker hands the K
// candidate partition NAMES it pulled from the in-memory ring; the SP claims
// each via queen.log_pop_v1 (invariant) and returns the bin-shaped meta + the
// aligned blobs (IDENTICAL to pop_wildcard_bin, so render_pop_parts is reused)
// PLUS the tri-state `states` JSON the broker checks back into the ring. When
// `skip_window` is true the SQL windowBuffer debounce is bypassed (§6 — the
// broker wheel owns windowBuffer). Returns (meta_json, blobs, states_json).
#[allow(clippy::too_many_arguments)]
pub async fn pop_list(
    client: &deadpool_postgres::Client,
    queue: &str,
    group: &str,
    partitions: &[String],
    budget: i32,
    lease_seconds: i32,
    worker: &str,
    auto_ack: bool,
    max_partitions: i32,
    sub_mode: &str,
    sub_from: &str,
    skip_window: bool,
) -> Result<(String, Vec<Vec<u8>>, String), tokio_postgres::Error> {
    let stmt = client
        .prepare_cached(
            "SELECT (t.meta)::text, t.blobs, (t.states)::text \
             FROM queen.log_pop_list_v1($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) t",
        )
        .await?;
    let row = client
        .query_one(
            &stmt,
            &[&queue, &group, &partitions, &budget, &lease_seconds, &worker, &auto_ack,
              &max_partitions, &sub_mode, &sub_from, &skip_window],
        )
        .await?;
    Ok((row.get(0), row.get(1), row.get(2)))
}

// 19-wildcard-hotlist §8: reseed / cold-start keyset discovery. One bounded page
// of probably-pending partitions for (queue, group) with id > after_id, in id
// order. Returns (partition_id_text, partition_name) rows; the broker interns
// the name, remembers the id for the ack bridge, and marks each into the ring.
pub async fn hotlist_reseed(
    client: &deadpool_postgres::Client,
    queue: &str,
    group: &str,
    after_id: &str,
    limit: i32,
) -> Result<Vec<(String, String)>, tokio_postgres::Error> {
    let stmt = client
        .prepare_cached(
            "SELECT r_id::text, r_name \
             FROM queen.log_hotlist_reseed_v1($1,$2,$3::text::uuid,$4)",
        )
        .await?;
    let rows = client
        .query(&stmt, &[&queue, &group, &after_id, &limit])
        .await?;
    Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
}

// 19-wildcard-hotlist §6: a queue's deferral config (delayed_processing,
// window_buffer) in seconds, for the broker's hot-list mark routing / wheel /
// skip-window decision. Absent row = (0, 0).
pub async fn queue_defer_cfg(
    client: &deadpool_postgres::Client,
    queue: &str,
) -> Result<(i32, i32), tokio_postgres::Error> {
    let stmt = client
        .prepare_cached(
            "SELECT COALESCE(delayed_processing,0), COALESCE(window_buffer,0) \
             FROM queen.queues WHERE name = $1",
        )
        .await?;
    match client.query_opt(&stmt, &[&queue]).await? {
        Some(r) => Ok((r.get(0), r.get(1))),
        None => Ok((0, 0)),
    }
}

// Cheap pending probe (discovery-latency fix, 2026-07-24): does any partition of
// `queue` hold frames `group` has not consumed? `queen.log_has_pending_v1` is one
// indexed EXISTS (last_offset > committed) — no lease/retention refinement, so it
// is a CORRECT SUPERSET: a `false` means there is definitively nothing to deliver,
// a `true` means "maybe" (the pop resolves the truth under the row lock). The
// legacy (hotlist-off) long-poll uses it to gate the pop_vegas-limited wildcard
// scan: a quiet re-poll (has_pending=false) returns empty WITHOUT taking the
// serving permit, so the O(#queues) empty re-poll storm never saturates the pop
// limiter (the multitenant discovery-latency regression).
pub async fn has_pending(
    client: &deadpool_postgres::Client,
    queue: &str,
    group: &str,
) -> Result<bool, tokio_postgres::Error> {
    let stmt = client
        .prepare_cached("SELECT queen.log_has_pending_v1($1, $2)")
        .await?;
    let row = client.query_one(&stmt, &[&queue, &group]).await?;
    Ok(row.get(0))
}

// Phase 2 first-contact safety: does the group-first-contact bulk-seed MARKER
// exist for (queue, group)? The marker is the single partition_name='' row that
// queen.log_pop_wildcard_*_v1 / log_pop_discover_wire_v1 (043) insert on a
// (queue, group)'s FIRST wildcard contact, in the SAME transaction as the
// set-based cursor seed of every existing partition. Its presence therefore
// means that seed has committed, so a targeted single-partition pop
// (queen.log_pop_v1) can no longer trigger the per-partition first-contact
// INSERT storm the seed prevents. One indexed point lookup on the
// (consumer_group, queue_name, partition_name, ...) unique index; non-locking.
pub async fn group_seed_marker_exists(
    client: &deadpool_postgres::Client,
    queue: &str,
    group: &str,
) -> Result<bool, tokio_postgres::Error> {
    let stmt = client
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM queen.consumer_groups_metadata \
             WHERE consumer_group = $1 AND queue_name = $2 AND partition_name = '')",
        )
        .await?;
    let row = client.query_one(&stmt, &[&group, &queue]).await?;
    Ok(row.get(0))
}

// Renew every live lease held by `worker` via queen.log_renew_lease_v1
// (044). Returns the SP result JSON as text: {"renewed":n,"expiresAt":iso|null}.
pub async fn renew_lease(
    client: &deadpool_postgres::Client,
    worker: &str,
    seconds: i32,
) -> Result<String, tokio_postgres::Error> {
    let stmt = client
        .prepare_cached("SELECT (queen.log_renew_lease_v1($1, $2::int))::text")
        .await?;
    let row = client.query_one(&stmt, &[&worker, &seconds]).await?;
    Ok(row.get(0))
}

// Atomic multi-op push+ack via queen.log_transaction_wire_v1 (044). `payload`
// is the already-built {"pushes":[...],"acks":[...]} JSON (044 wire: pushes
// carry hashesHex/blobB64/verified; acks are positional or hash-form). Returns
// the SP result JSON as text: {"ok":bool,...} or raises on rollback
// (duplicate/ack failure).
pub async fn transaction(
    client: &deadpool_postgres::Client,
    payload: &str,
) -> Result<String, tokio_postgres::Error> {
    // $1::text::jsonb pins $1 to TEXT so a &str binds (a bare $1::jsonb would
    // make tokio-postgres expect a Json param).
    let stmt = client
        .prepare_cached("SELECT (queen.log_transaction_wire_v1($1::text::jsonb))::text")
        .await?;
    let row = client.query_one(&stmt, &[&payload]).await?;
    Ok(row.get(0))
}

// ---------------------------------------------------------------- configure
// Create/update a queue's options via queen.configure_queue_v1 (012). `opts_json`
// is the options object as JSON text. Returns the SP result JSON as text:
// {configured, queueId, partitionId, queue, namespace, task, options:{all 15}}.
pub async fn configure_queue(
    client: &deadpool_postgres::Client,
    queue: &str,
    opts_json: &str,
) -> Result<String, tokio_postgres::Error> {
    // $2::text::jsonb pins $2 to TEXT so a &str binds (a bare $2::jsonb would make
    // tokio-postgres expect a Json param).
    let stmt = "SELECT (queen.configure_queue_v1($1, $2::text::jsonb))::text";
    let row = client.query_one(stmt, &[&queue, &opts_json]).await?;
    Ok(row.get(0))
}

// Pin this queue to the log engine. The public /configure contract keeps the
// storage value 'segments' (§11 wire compat), which is what routes queues to
// the log push/pop paths.
pub async fn mark_queue_segments(
    client: &deadpool_postgres::Client,
    queue: &str,
) -> Result<(), tokio_postgres::Error> {
    client
        .execute("UPDATE queen.queues SET storage='segments' WHERE name=$1", &[&queue])
        .await?;
    Ok(())
}

// Ensure a queen.log_queues row exists with the queue's lease/retention/dedup so
// the log pop path reads the configured lease_time (and retention/dedup for the
// maintenance/push paths). Upsert so a reconfigure updates in place.
pub async fn upsert_seg_queue(
    client: &deadpool_postgres::Client,
    queue: &str,
    lease_time: i32,
    retention_seconds: i32,
    dedup_window_seconds: i32,
) -> Result<(), tokio_postgres::Error> {
    let stmt = "INSERT INTO queen.log_queues(name, lease_time, retention_seconds, dedup_window_seconds) \
                VALUES($1, $2, $3, $4) \
                ON CONFLICT(name) DO UPDATE SET \
                    lease_time = EXCLUDED.lease_time, \
                    retention_seconds = EXCLUDED.retention_seconds, \
                    dedup_window_seconds = EXCLUDED.dedup_window_seconds";
    client
        .execute(stmt, &[&queue, &lease_time, &retention_seconds, &dedup_window_seconds])
        .await?;
    Ok(())
}

// ------------------------------------------------------------------- delete
// Drop a queue + all rows-side coordination data via queen.delete_queue_v1 (007).
// Returns the SP result JSON as text: {deleted:true, queue, existed}.
pub async fn delete_queue(
    client: &deadpool_postgres::Client,
    queue: &str,
) -> Result<String, tokio_postgres::Error> {
    let stmt = "SELECT (queen.delete_queue_v1($1))::text";
    let row = client.query_one(stmt, &[&queue]).await?;
    Ok(row.get(0))
}

// Drop the queue's log-engine data. Deleting the log_queues row cascades to
// log_partitions -> log_segments / log_consumers (FKs, 041). queen.log_txns and
// queen.log_dlq deliberately carry NO FK (the purge path must never pay
// FK-trigger cost), so their rows are removed explicitly FIRST, while
// log_partitions still resolves the queue.
pub async fn delete_seg_queue(
    client: &deadpool_postgres::Client,
    queue: &str,
) -> Result<(), tokio_postgres::Error> {
    client
        .execute(
            "DELETE FROM queen.log_txns t \
             USING queen.log_partitions p \
             JOIN queen.log_queues q ON q.id = p.queue_id \
             WHERE t.partition_id = p.id AND q.name = $1",
            &[&queue],
        )
        .await?;
    client
        .execute(
            "DELETE FROM queen.log_dlq d \
             USING queen.log_partitions p \
             JOIN queen.log_queues q ON q.id = p.queue_id \
             WHERE d.partition_id = p.id AND q.name = $1",
            &[&queue],
        )
        .await?;
    client
        .execute("DELETE FROM queen.log_queues WHERE name=$1", &[&queue])
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------- get
// Basic queue detail via queen.get_queue_v2 (013). Returns the SP result JSON as
// text: {id,name,namespace,task,createdAt,partitions,totals} or {error:..} when
// the queue does not exist in queen.queues.
pub async fn get_queue(
    client: &deadpool_postgres::Client,
    queue: &str,
) -> Result<String, tokio_postgres::Error> {
    let stmt = "SELECT (queen.get_queue_v2($1))::text";
    let row = client.query_one(stmt, &[&queue]).await?;
    Ok(row.get(0))
}

// Segment counts for a queue: (#segments, live message count) via
// queen.log_queue_message_stats_v1 (048). Used to enrich the get_queue_v2
// detail (whose stats come from queen.stats, refreshed on a cadence).
pub async fn seg_queue_message_stats(
    client: &deadpool_postgres::Client,
    queue: &str,
) -> Result<(i64, i64), tokio_postgres::Error> {
    let stmt = "SELECT segments, messages FROM queen.log_queue_message_stats_v1($1)";
    let row = client.query_one(stmt, &[&queue]).await?;
    Ok((row.get(0), row.get(1)))
}

// ------------------------------------------------------- resources LIST API
// The get_*_v2 / get_system_overview_v3 SPs power the /api/v1/resources/* list
// endpoints. They read from queen.queues / queen.stats; queen.stats is refreshed
// on a cadence (log_refresh_all_stats_v1), so the queue list is enriched with
// the live per-queue counters separately (see seg_queue_stats_all).
// Each returns the SP result JSON as text.
pub async fn get_queues(
    client: &deadpool_postgres::Client,
) -> Result<String, tokio_postgres::Error> {
    let row = client.query_one("SELECT (queen.get_queues_v2())::text", &[]).await?;
    Ok(row.get(0))
}

pub async fn get_system_overview(
    client: &deadpool_postgres::Client,
) -> Result<String, tokio_postgres::Error> {
    let row = client
        .query_one("SELECT (queen.get_system_overview_v3())::text", &[])
        .await?;
    Ok(row.get(0))
}

pub async fn get_namespaces(
    client: &deadpool_postgres::Client,
) -> Result<String, tokio_postgres::Error> {
    let row = client.query_one("SELECT (queen.get_namespaces_v2())::text", &[]).await?;
    Ok(row.get(0))
}

pub async fn get_tasks(
    client: &deadpool_postgres::Client,
) -> Result<String, tokio_postgres::Error> {
    let row = client.query_one("SELECT (queen.get_tasks_v2())::text", &[]).await?;
    Ok(row.get(0))
}

// Per-queue live stats for the whole broker: name -> (partitions, segments,
// messages) via queen.log_queue_stats_all_v1 (048). Used to enrich the queue
// LIST endpoint between queen.stats refreshes.
pub async fn seg_queue_stats_all(
    client: &deadpool_postgres::Client,
) -> Result<Vec<(String, i64, i64, i64)>, tokio_postgres::Error> {
    let stmt = "SELECT queue_name, partitions, segments, messages \
                FROM queen.log_queue_stats_all_v1()";
    let rows = client.query(stmt, &[]).await?;
    Ok(rows
        .iter()
        .map(|r| {
            (
                r.get::<_, String>(0),
                r.get::<_, i64>(1),
                r.get::<_, i64>(2),
                r.get::<_, i64>(3),
            )
        })
        .collect())
}

// Configured lease time (seconds) for a queue, from queen.log_queues by name.
// None when the queue has no log_queues row yet (caller defaults to 300).
pub async fn queue_lease_time(
    client: &deadpool_postgres::Client,
    queue: &str,
) -> Result<Option<i32>, tokio_postgres::Error> {
    let stmt = client
        .prepare_cached("SELECT lease_time FROM queen.log_queues WHERE name = $1")
        .await?;
    let rows = client.query(&stmt, &[&queue]).await?;
    Ok(rows.first().map(|r| r.get::<_, i32>(0)))
}

// RUSTFIX item 8: per-queue at-rest encryption flag. Lives on queen.queues
// (written by 012_configure from options.encryptionEnabled), NOT log_queues.
// None when the queue has no queen.queues row yet (caller treats as false).
pub async fn queue_encryption_enabled(
    client: &deadpool_postgres::Client,
    queue: &str,
) -> Result<Option<bool>, tokio_postgres::Error> {
    let stmt = "SELECT encryption_enabled FROM queen.queues WHERE name = $1";
    let rows = client.query(stmt, &[&queue]).await?;
    Ok(rows.first().map(|r| r.get::<_, bool>(0)))
}

// ------------------------------------------------------- per-message access
// NEW (log engine): resolve (partition_id, txn hash) -> absolute message offset
// through queen.log_txns + log_unnest_hashes. `hash` is the 16B xxh3_128 of the
// txn string (crate::util::txn_hash128). Newest occurrence wins (descending
// base_offset). None once the log_txns rows have been purged (older than the
// txns window) or the hash is unknown for this partition. Admin-path resolver —
// no permanent per-message index; the scan is bounded by the partition's live
// log_txns rows. The caller resolves the message id by decoding the covering
// segment (log_segment_covering) — SQL has no per-message ids in the log engine.
pub async fn log_resolve_position(
    client: &deadpool_postgres::Client,
    partition_id: &str,
    hash: &[u8],
) -> Result<Option<i64>, tokio_postgres::Error> {
    // $1::text::uuid pins $1 to TEXT so a &str binds.
    let stmt = client
        .prepare_cached(
            "SELECT t.base_offset + th.idx \
             FROM queen.log_txns t \
             CROSS JOIN LATERAL queen.log_unnest_hashes(t.hashes) th \
             WHERE t.partition_id = $1::text::uuid AND th.h = $2 \
             ORDER BY t.base_offset DESC, th.idx DESC \
             LIMIT 1",
        )
        .await?;
    let rows = client.query(&stmt, &[&partition_id, &hash]).await?;
    Ok(rows.first().map(|r| r.get::<_, i64>(0)))
}

// NEW (log engine): fetch the segment COVERING an absolute offset via
// queen.log_segment_at_v1 (042): (base_offset, end_offset, blob). The caller
// decodes frame (off - base_offset) in Rust. None when retention already
// deleted the covering segment (duplicate-push mid resolution then reports the
// zero uuid) or the offset sits in a retention gap / beyond the tail.
pub async fn log_segment_covering(
    client: &deadpool_postgres::Client,
    partition_id: &str,
    off: i64,
) -> Result<Option<(i64, i64, Vec<u8>)>, tokio_postgres::Error> {
    let stmt = client
        .prepare_cached(
            "SELECT r_base, r_end, r_blob \
             FROM queen.log_segment_at_v1($1::text::uuid, $2::bigint)",
        )
        .await?;
    let rows = client.query(&stmt, &[&partition_id, &off]).await?;
    Ok(rows
        .first()
        .map(|r| (r.get::<_, i64>(0), r.get::<_, i64>(1), r.get::<_, Vec<u8>>(2))))
}

// RUSTFIX item 23: fallback resolver for a txn whose log_txns rows have been
// purged. Scans the partition's segment blobs newest-first (bounded) and
// returns (base_offset, blob) candidates so the handler can decode + match
// f.txn. O(segments in the partition) — acceptable for a management endpoint;
// bounded by LIMIT to avoid a pathological scan. The first tuple element now
// carries base_offset (the log address of the blob's first frame).
pub async fn seg_scan_segments(
    client: &deadpool_postgres::Client,
    partition_id: &str,
    limit: i64,
) -> Result<Vec<(i64, Vec<u8>)>, tokio_postgres::Error> {
    let stmt = "SELECT s.base_offset, s.blob FROM queen.log_segments s \
                WHERE s.partition_id = $1::text::uuid ORDER BY s.base_offset DESC LIMIT $2";
    let rows = client.query(stmt, &[&partition_id, &limit]).await?;
    Ok(rows.iter().map(|r| (r.get::<_, i64>(0), r.get::<_, Vec<u8>>(1))).collect())
}

// RUSTFIX item 23: the extra management fields for GET /messages/:pid/:txn
// (010_messages.sql:194-271 parity) — queue/namespace/task, queueConfig, the
// per-group consumerGroups + leaseExpiresAt, DLQ error, and the flags to derive
// `status`. Returns the assembled JSON as text (or None if the partition is
// gone). Tolerates a missing queen.queues row (item 26) via COALESCE +
// split_part. Signature kept from the seg engine: the message position travels
// in `seq` (the absolute OFFSET); `frame_idx` is vestigial and ignored (pass 0).
// Scalar cursor semantics: consumed = offset <= committed.
pub async fn seg_message_detail(
    client: &deadpool_postgres::Client,
    partition_id: &str,
    seq: i64,
    _frame_idx: i32,
) -> Result<Option<String>, tokio_postgres::Error> {
    let stmt = "\
        WITH q AS ( \
            SELECT lq.name AS queue, lp.name AS partition, \
                   COALESCE(NULLIF(qq.namespace, ''), split_part(lq.name, '.', 1)) AS namespace, \
                   COALESCE(NULLIF(qq.task, ''), CASE WHEN position('.' in lq.name) > 0 \
                        THEN split_part(lq.name, '.', 2) ELSE '' END) AS task, \
                   COALESCE(qq.lease_time, 60) AS lease_time, \
                   COALESCE(qq.retry_limit, 3) AS retry_limit, \
                   COALESCE(qq.retry_delay, 1000) AS retry_delay, \
                   COALESCE(qq.ttl, 3600) AS ttl, \
                   COALESCE(qq.priority, 0) AS priority \
            FROM queen.log_partitions lp \
            JOIN queen.log_queues lq ON lq.id = lp.queue_id \
            LEFT JOIN queen.queues qq ON qq.name = lq.name \
            WHERE lp.id = $1::text::uuid ), \
        d AS ( SELECT error, COALESCE(retry_count, 0) AS retry_count FROM queen.log_dlq dl \
               WHERE dl.partition_id = $1::text::uuid AND dl.\"offset\" = $2::bigint LIMIT 1 ), \
        g AS ( \
            SELECT COALESCE(jsonb_agg(jsonb_build_object( \
                       'name', c.consumer_group, \
                       'group', c.consumer_group, \
                       'consumed', c.committed >= $2::bigint, \
                       'leaseExpiresAt', c.lease_expires_at) \
                   ORDER BY c.consumer_group), '[]'::jsonb) AS arr, \
                   MAX(CASE WHEN c.consumer_group = '__QUEUE_MODE__' THEN c.lease_expires_at END) AS qmode_lease, \
                   bool_and(c.committed >= $2::bigint) \
                       FILTER (WHERE c.consumer_group <> '__QUEUE_MODE__') AS bus_all_passed, \
                   bool_or(c.committed >= $2::bigint) \
                       FILTER (WHERE c.consumer_group = '__QUEUE_MODE__') AS qmode_passed, \
                   COUNT(*) FILTER (WHERE c.consumer_group <> '__QUEUE_MODE__') AS bus_groups, \
                   COUNT(*) FILTER (WHERE c.consumer_group = '__QUEUE_MODE__') AS has_qmode, \
                   bool_or(c.lease_expires_at IS NOT NULL AND c.lease_expires_at > now()) AS any_lease \
            FROM queen.log_consumers c WHERE c.partition_id = $1::text::uuid ) \
        SELECT jsonb_build_object( \
            'queue', q.queue, 'partition', q.partition, 'namespace', q.namespace, 'task', q.task, \
            'queueConfig', jsonb_build_object('leaseTime', q.lease_time, 'retryLimit', q.retry_limit, \
                'retryDelay', q.retry_delay, 'ttl', q.ttl, 'priority', q.priority), \
            'errorMessage', (SELECT error FROM d), \
            'isDlq', EXISTS(SELECT 1 FROM d), \
            'dlqRetryCount', COALESCE((SELECT retry_count FROM d), 0), \
            'consumerGroups', (SELECT arr FROM g), \
            'leaseExpiresAt', (SELECT qmode_lease FROM g), \
            'busAllPassed', COALESCE((SELECT bus_all_passed FROM g), false), \
            'qmodePassed', COALESCE((SELECT qmode_passed FROM g), false), \
            'busGroups', COALESCE((SELECT bus_groups FROM g), 0), \
            'hasQueueMode', COALESCE((SELECT has_qmode FROM g), 0) > 0, \
            'anyLeaseLive', COALESCE((SELECT any_lease FROM g), false) \
        )::text FROM q";
    let rows = client.query(stmt, &[&partition_id, &seq]).await?;
    Ok(rows.first().map(|r| r.get::<_, String>(0)))
}

// Fetch one segment's decode inputs: (createdAt ISO text, partition name, raw
// zstd blob bytes) for (partition_id, base_offset). None when the segment no
// longer exists (retention deleted). Signature kept from the seg engine: `seq`
// now carries the segment's base_offset (the log_segments PK). For "which
// segment covers offset X" use log_segment_covering instead.
pub async fn seg_fetch_segment(
    client: &deadpool_postgres::Client,
    partition_id: &str,
    seq: i64,
) -> Result<Option<(String, String, Vec<u8>)>, tokio_postgres::Error> {
    let stmt = "SELECT to_char(s.created_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), \
                       lp.name, s.blob \
                FROM queen.log_segments s \
                JOIN queen.log_partitions lp ON lp.id = s.partition_id \
                WHERE s.partition_id = $1::text::uuid AND s.base_offset = $2::bigint";
    let rows = client.query(stmt, &[&partition_id, &seq]).await?;
    Ok(rows
        .first()
        .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1), r.get::<_, Vec<u8>>(2))))
}

// ------------------------------------------------------------- messages / dlq
// List messages (dual-engine 047). `filters_json` is the JSON filter bag
// ({queue,partition,namespace,task,status,from,to,limit,offset}). Log-queue
// entries carry payloadAvailable:false + segment:{seq,frameIdx} where seq =
// base_offset and frameIdx = offset - base_offset — the broker decodes their
// payloads. Returns the SP result JSON as text.
pub async fn list_messages(
    client: &deadpool_postgres::Client,
    filters_json: &str,
) -> Result<String, tokio_postgres::Error> {
    let stmt = "SELECT (queen.list_messages_v1($1::text::jsonb))::text";
    let row = client.query_one(stmt, &[&filters_json]).await?;
    Ok(row.get(0))
}

// DLQ messages (dual-engine 047). queen.log_dlq stores payload SNAPSHOTS, so no
// decode is needed. `filters_json` = {queue,consumerGroup,limit,offset}. Returns
// {messages:[...], pagination:{...}} as text.
pub async fn get_dlq_messages(
    client: &deadpool_postgres::Client,
    filters_json: &str,
) -> Result<String, tokio_postgres::Error> {
    let stmt = "SELECT (queen.get_dlq_messages_v1($1::text::jsonb))::text";
    let row = client.query_one(stmt, &[&filters_json]).await?;
    Ok(row.get(0))
}

// Delete a message addressed by (partition_id, transaction_id). The log engine
// stores live payloads in immutable segments, so the deletable rows here are
// the DLQ snapshots in queen.log_dlq (the DLQ manual-requeue workflow drops
// one). Also runs the rows-engine delete_message_v1 for dual-engine parity (a
// no-op on a log queue). Returns true when either path removed a row.
pub async fn delete_message(
    client: &deadpool_postgres::Client,
    partition_id: &str,
    txn: &str,
) -> Result<bool, tokio_postgres::Error> {
    let seg = client
        .execute(
            "DELETE FROM queen.log_dlq WHERE partition_id = $1::text::uuid AND transaction_id = $2",
            &[&partition_id, &txn],
        )
        .await?;
    let rows_txt: String = client
        .query_one(
            "SELECT (queen.delete_message_v1($1::text::uuid, $2))::text",
            &[&partition_id, &txn],
        )
        .await?
        .get(0);
    let rows_deleted = serde_json::from_str::<serde_json::Value>(&rows_txt)
        .ok()
        .and_then(|v| v.get("success").and_then(|x| x.as_bool()))
        .unwrap_or(false);
    Ok(seg > 0 || rows_deleted)
}

// ------------------------------------------------------------------- traces
// All four trace SPs are engine-agnostic (queen.message_traces keyed by
// transaction_id) — no blob decode. record_trace_v1 is made log-aware in 047
// (message_id stays NULL for log messages, whose mids live inside segment
// blobs).
pub async fn record_trace(
    client: &deadpool_postgres::Client,
    data_json: &str,
) -> Result<String, tokio_postgres::Error> {
    let stmt = client
        .prepare_cached("SELECT (queen.record_trace_v1($1::text::jsonb))::text")
        .await?;
    let row = client.query_one(&stmt, &[&data_json]).await?;
    Ok(row.get(0))
}

pub async fn get_message_traces(
    client: &deadpool_postgres::Client,
    partition_id: &str,
    txn: &str,
) -> Result<String, tokio_postgres::Error> {
    let stmt = "SELECT (queen.get_message_traces_v1($1::text::uuid, $2))::text";
    let row = client.query_one(stmt, &[&partition_id, &txn]).await?;
    Ok(row.get(0))
}

pub async fn get_traces_by_name(
    client: &deadpool_postgres::Client,
    name: &str,
    limit: i32,
    offset: i32,
) -> Result<String, tokio_postgres::Error> {
    let stmt = "SELECT (queen.get_traces_by_name_v1($1, $2::int, $3::int))::text";
    let row = client.query_one(stmt, &[&name, &limit, &offset]).await?;
    Ok(row.get(0))
}

pub async fn get_trace_names(
    client: &deadpool_postgres::Client,
    limit: i32,
    offset: i32,
) -> Result<String, tokio_postgres::Error> {
    let stmt = "SELECT (queen.get_available_trace_names_v1($1::int, $2::int))::text";
    let row = client.query_one(stmt, &[&limit, &offset]).await?;
    Ok(row.get(0))
}

// ------------------------------------------------------------ status / metrics
// get_status_v3 (dashboard overview) — filters JSON {from,to,queue,namespace,task}.
pub async fn get_status(
    client: &deadpool_postgres::Client,
    filters_json: &str,
) -> Result<String, tokio_postgres::Error> {
    let stmt = "SELECT (queen.get_status_v3($1::text::jsonb))::text";
    let row = client.query_one(stmt, &[&filters_json]).await?;
    Ok(row.get(0))
}

// get_status_queues_v2 (queues list) — filters JSON + limit/offset.
pub async fn get_status_queues(
    client: &deadpool_postgres::Client,
    filters_json: &str,
    limit: i32,
    offset: i32,
) -> Result<String, tokio_postgres::Error> {
    let stmt = "SELECT (queen.get_status_queues_v2($1::text::jsonb, $2::int, $3::int))::text";
    let row = client.query_one(stmt, &[&filters_json, &limit, &offset]).await?;
    Ok(row.get(0))
}

// Prometheus DB metrics blob (018). Returns the SP JSON as text; the handler
// formats a subset into Prometheus exposition lines.
pub async fn get_prometheus_metrics(
    client: &deadpool_postgres::Client,
) -> Result<String, tokio_postgres::Error> {
    let stmt = "SELECT (queen.get_prometheus_metrics_v1())::text";
    let row = client.query_one(stmt, &[]).await?;
    Ok(row.get(0))
}

// Liveness probe for /health: a trivial round-trip that fails iff the pool
// connection is unusable.
pub async fn ping(client: &deadpool_postgres::Client) -> Result<(), tokio_postgres::Error> {
    client.query_one("SELECT 1", &[]).await?;
    Ok(())
}

// ============================================================================
// Analytics / metrics read wrappers (dashboard System / Analytics / QueueOps).
// Each returns the stored-procedure JSON as raw text; the handler serves it
// verbatim (the C++ routes did the same — no envelope). All the SPs already
// ship in sql/procedures/{009,013,014,015,017,048}; these just dispatch them.
// ============================================================================

// get_queue_detail_v2 (log-aware in 048) — /api/v1/status/queues/:queue.
pub async fn get_queue_detail(
    client: &deadpool_postgres::Client,
    queue: &str,
) -> Result<String, tokio_postgres::Error> {
    let row = client
        .query_one("SELECT (queen.get_queue_detail_v2($1))::text", &[&queue])
        .await?;
    Ok(row.get(0))
}

// get_analytics_v1 (log-aware in 048) — /api/v1/status/analytics.
pub async fn get_analytics(
    client: &deadpool_postgres::Client,
    filters_json: &str,
) -> Result<String, tokio_postgres::Error> {
    let row = client
        .query_one("SELECT (queen.get_analytics_v1($1::text::jsonb))::text", &[&filters_json])
        .await?;
    Ok(row.get(0))
}

// get_system_metrics_v1 — /api/v1/analytics/system-metrics (per-replica series).
pub async fn get_system_metrics(
    client: &deadpool_postgres::Client,
    filters_json: &str,
) -> Result<String, tokio_postgres::Error> {
    let row = client
        .query_one("SELECT (queen.get_system_metrics_v1($1::text::jsonb))::text", &[&filters_json])
        .await?;
    Ok(row.get(0))
}

// get_worker_metrics_timeseries_v1 — /api/v1/analytics/worker-metrics.
pub async fn get_worker_metrics_ts(
    client: &deadpool_postgres::Client,
    filters_json: &str,
) -> Result<String, tokio_postgres::Error> {
    let row = client
        .query_one(
            "SELECT (queen.get_worker_metrics_timeseries_v1($1::text::jsonb))::text",
            &[&filters_json],
        )
        .await?;
    Ok(row.get(0))
}

// get_queue_lag_v1 — /api/v1/analytics/queue-lag. Positional (from,to,queue) args;
// NULLs fall back to the SP defaults (last hour). Returns a BARE JSON array.
pub async fn get_queue_lag(
    client: &deadpool_postgres::Client,
    from: Option<&str>,
    to: Option<&str>,
    queue: Option<&str>,
) -> Result<String, tokio_postgres::Error> {
    let stmt = "SELECT (queen.get_queue_lag_v1(\
                COALESCE($1::text::timestamptz, NOW() - INTERVAL '1 hour'), \
                COALESCE($2::text::timestamptz, NOW()), $3::text))::text";
    let row = client.query_one(stmt, &[&from, &to, &queue]).await?;
    Ok(row.get(0))
}

// get_queue_ops_v1 — /api/v1/analytics/queue-ops (per-queue push/pop/ack + parts).
pub async fn get_queue_ops(
    client: &deadpool_postgres::Client,
    filters_json: &str,
) -> Result<String, tokio_postgres::Error> {
    let row = client
        .query_one("SELECT (queen.get_queue_ops_v1($1::text::jsonb))::text", &[&filters_json])
        .await?;
    Ok(row.get(0))
}

// get_queue_parked_per_replica_v1 — /api/v1/analytics/queue-parked-replicas.
pub async fn get_queue_parked_replicas(
    client: &deadpool_postgres::Client,
    filters_json: &str,
) -> Result<String, tokio_postgres::Error> {
    let row = client
        .query_one(
            "SELECT (queen.get_queue_parked_per_replica_v1($1::text::jsonb))::text",
            &[&filters_json],
        )
        .await?;
    Ok(row.get(0))
}

// get_retention_timeseries_v1 — /api/v1/analytics/retention.
pub async fn get_retention_ts(
    client: &deadpool_postgres::Client,
    filters_json: &str,
) -> Result<String, tokio_postgres::Error> {
    let row = client
        .query_one("SELECT (queen.get_retention_timeseries_v1($1::text::jsonb))::text", &[&filters_json])
        .await?;
    Ok(row.get(0))
}

// get_postgres_stats_v1 — /api/v1/analytics/postgres-stats (pg_stat_* snapshot).
pub async fn get_postgres_stats(
    client: &deadpool_postgres::Client,
) -> Result<String, tokio_postgres::Error> {
    let row = client
        .query_one("SELECT (queen.get_postgres_stats_v1())::text", &[])
        .await?;
    Ok(row.get(0))
}

// ============================================================================
// Background collector writes (stats.rs / syscollect.rs).
// ============================================================================

// Log-native queen.stats refresh (048). Returns the SP summary JSON. Rust name
// kept from the seg engine (pinned db.rs API); the SQL home moved to
// queen.log_refresh_all_stats_v1.
pub async fn seg_refresh_all_stats(
    client: &deadpool_postgres::Client,
) -> Result<String, tokio_postgres::Error> {
    let row = client
        .query_one("SELECT (queen.log_refresh_all_stats_v1())::text", &[])
        .await?;
    Ok(row.get(0))
}

// Insert one per-minute worker_metrics row (deltas). The AFTER INSERT trigger
// (queen.update_worker_metrics_summary) rolls these into worker_metrics_summary,
// so the lifetime totals the dashboard reads stay correct. ON CONFLICT DO NOTHING
// guards a same-minute double flush (rare; drops one delta rather than firing the
// summary trigger twice).
#[allow(clippy::too_many_arguments)]
pub async fn insert_worker_metrics(
    client: &deadpool_postgres::Client,
    hostname: &str,
    worker_id: i32,
    pid: i32,
    push_requests: i64,
    push_messages: i64,
    pop_requests: i64,
    pop_messages: i64,
    ack_requests: i64,
    ack_messages: i64,
    ack_success: i64,
    ack_failed: i64,
    transactions: i64,
    dlq: i64,
    db_errors: i64,
    avg_event_loop_lag_ms: i32,
    max_event_loop_lag_ms: i32,
    avg_lag_ms: i64,
    max_lag_ms: i64,
    lag_count: i64,
) -> Result<(), tokio_postgres::Error> {
    let stmt = "INSERT INTO queen.worker_metrics (\
            hostname, worker_id, pid, bucket_time, \
            jobs_done, push_request_count, pop_request_count, ack_request_count, transaction_count, \
            push_message_count, pop_message_count, ack_message_count, ack_success_count, ack_failed_count, \
            db_error_count, dlq_count, avg_event_loop_lag_ms, max_event_loop_lag_ms, \
            avg_lag_ms, max_lag_ms, lag_count) \
        VALUES ($1,$2,$3, date_trunc('second', NOW()), \
            $4,$4,$6,$8,$12, $5,$7,$9,$10,$11, $14,$13, $15,$16, $17,$18,$19) \
        ON CONFLICT (hostname, worker_id, pid, bucket_time) DO NOTHING";
    client
        .execute(
            stmt,
            &[
                &hostname,       // $1
                &worker_id,      // $2
                &pid,            // $3
                &push_requests,  // $4  (also jobs_done + push_request_count)
                &push_messages,  // $5
                &pop_requests,   // $6
                &pop_messages,   // $7
                &ack_requests,   // $8
                &ack_messages,   // $9
                &ack_success,    // $10
                &ack_failed,     // $11
                &transactions,   // $12
                &dlq,            // $13
                &db_errors,      // $14
                &avg_event_loop_lag_ms, // $15
                &max_event_loop_lag_ms, // $16
                &avg_lag_ms,     // $17
                &max_lag_ms,     // $18
                &lag_count,      // $19
            ],
        )
        .await?;
    Ok(())
}

// NEW (log engine): multi-segment typed-array push via queen.log_push_multi_v1
// (042). Arrays are index-aligned per segment: `counts[i]` frames, `hashes[i]`
// = 16*counts[i] bytes of xxh3_128 (frame order, big-endian), `verified[i]` =
// the broker dedup watermark (-1 = no cache, probe the whole window),
// `blobs[i]` = the packed+zstd frame blob. Returns the SP JSONB (input-order
// array of {"status":"queued",...} | {"status":"duplicate","dups":[...]}) as
// text. Used by fusion.rs (hot path) and file_buffer.rs (spool replay).
pub async fn log_push_multi(
    client: &deadpool_postgres::Client,
    queues: &[String],
    partitions: &[String],
    counts: &[i32],
    hashes: &[Vec<u8>],
    verified: &[i64],
    blobs: &[Vec<u8>],
) -> Result<String, tokio_postgres::Error> {
    let stmt = client
        .prepare_cached(
            "SELECT (queen.log_push_multi_v1($1::text[], $2::text[], $3::int4[], \
             $4::bytea[], $5::int8[], $6::bytea[]))::text",
        )
        .await?;
    let row = client
        .query_one(&stmt, &[&queues, &partitions, &counts, &hashes, &verified, &blobs])
        .await?;
    Ok(row.get(0))
}

// Insert one per-window system_metrics row (host/process gauges as a JSONB blob
// shaped exactly like the C++ AggregatedMetrics.to_json(), which
// queen.get_system_metrics_v1 re-aggregates). ON CONFLICT keeps the latest.
pub async fn insert_system_metrics(
    client: &deadpool_postgres::Client,
    hostname: &str,
    port: i32,
    worker_id: &str,
    sample_count: i32,
    metrics_json: &str,
) -> Result<(), tokio_postgres::Error> {
    let stmt = "INSERT INTO queen.system_metrics (timestamp, hostname, port, worker_id, sample_count, metrics) \
        VALUES (date_trunc('second', NOW()), $1, $2, $3, $4, $5::text::jsonb) \
        ON CONFLICT (timestamp, hostname, port, worker_id) DO UPDATE SET \
            metrics = EXCLUDED.metrics, sample_count = EXCLUDED.sample_count";
    client
        .execute(stmt, &[&hostname, &port, &worker_id, &sample_count, &metrics_json])
        .await?;
    Ok(())
}

// RUSTFIX item 24: accumulate one replica's per-minute per-queue throughput into
// queen.queue_lag_metrics (UNIQUE(bucket_time, queue_name)). ON CONFLICT SUMs, so
// several replicas writing the same minute bucket aggregate cluster-wide (matching
// the "aggregated across all workers" semantics the readers expect).
// Lag merges as a weighted average (SUM(avg*count)/SUM(count), the same identity
// the read-side SPs use across buckets), max as GREATEST, parked additively (a
// gauge SUMmed across workers — 014_worker_metrics.sql:104-111).
#[allow(clippy::too_many_arguments)]
pub async fn upsert_queue_lag_metrics(
    client: &deadpool_postgres::Client,
    queue: &str,
    pop_count: i64,
    push_request_count: i64,
    push_message_count: i64,
    pop_empty_count: i64,
    transaction_count: i64,
    ack_request_count: i64,
    ack_success_count: i64,
    ack_failed_count: i64,
    avg_lag_ms: i64,
    max_lag_ms: i64,
    lag_count: i64,
    parked_count: i32,
) -> Result<(), tokio_postgres::Error> {
    let stmt = "INSERT INTO queen.queue_lag_metrics \
        (bucket_time, queue_name, pop_count, push_request_count, push_message_count, \
         pop_empty_count, transaction_count, ack_request_count, ack_success_count, \
         ack_failed_count, avg_lag_ms, max_lag_ms, lag_count, parked_count) \
        VALUES (date_trunc('minute', NOW()), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) \
        ON CONFLICT (bucket_time, queue_name) DO UPDATE SET \
            pop_count = queen.queue_lag_metrics.pop_count + EXCLUDED.pop_count, \
            push_request_count = queen.queue_lag_metrics.push_request_count + EXCLUDED.push_request_count, \
            push_message_count = queen.queue_lag_metrics.push_message_count + EXCLUDED.push_message_count, \
            pop_empty_count = queen.queue_lag_metrics.pop_empty_count + EXCLUDED.pop_empty_count, \
            transaction_count = queen.queue_lag_metrics.transaction_count + EXCLUDED.transaction_count, \
            ack_request_count = queen.queue_lag_metrics.ack_request_count + EXCLUDED.ack_request_count, \
            ack_success_count = queen.queue_lag_metrics.ack_success_count + EXCLUDED.ack_success_count, \
            ack_failed_count = queen.queue_lag_metrics.ack_failed_count + EXCLUDED.ack_failed_count, \
            avg_lag_ms = CASE \
                WHEN queen.queue_lag_metrics.lag_count + EXCLUDED.lag_count > 0 \
                THEN (queen.queue_lag_metrics.avg_lag_ms * queen.queue_lag_metrics.lag_count \
                      + EXCLUDED.avg_lag_ms * EXCLUDED.lag_count) \
                     / (queen.queue_lag_metrics.lag_count + EXCLUDED.lag_count) \
                ELSE 0 END, \
            max_lag_ms = GREATEST(queen.queue_lag_metrics.max_lag_ms, EXCLUDED.max_lag_ms), \
            lag_count = queen.queue_lag_metrics.lag_count + EXCLUDED.lag_count, \
            parked_count = COALESCE(queen.queue_lag_metrics.parked_count, 0) + EXCLUDED.parked_count";
    client
        .execute(
            stmt,
            &[
                &queue,
                &pop_count,
                &push_request_count,
                &push_message_count,
                &pop_empty_count,
                &transaction_count,
                &ack_request_count,
                &ack_success_count,
                &ack_failed_count,
                &avg_lag_ms,
                &max_lag_ms,
                &lag_count,
                &parked_count,
            ],
        )
        .await?;
    Ok(())
}

// Per-replica parked-count breakdown (queen.queue_parked_replica): each worker
// writes its own minute-averaged gauge; the System view charts the per-replica
// series while queue_lag_metrics.parked_count stays the cluster aggregate.
pub async fn upsert_queue_parked_replica(
    client: &deadpool_postgres::Client,
    queue: &str,
    hostname: &str,
    worker_id: i32,
    parked_count: i32,
) -> Result<(), tokio_postgres::Error> {
    let stmt = "INSERT INTO queen.queue_parked_replica \
        (bucket_time, queue_name, hostname, worker_id, parked_count) \
        VALUES (date_trunc('minute', NOW()), $1, $2, $3, $4) \
        ON CONFLICT (bucket_time, queue_name, hostname, worker_id) DO UPDATE SET \
            parked_count = EXCLUDED.parked_count";
    client
        .execute(stmt, &[&queue, &hostname, &worker_id, &parked_count])
        .await?;
    Ok(())
}

// Partition-id -> queue-name lookup backing the AppState ack-attribution memo.
pub async fn partition_queue_name(
    client: &deadpool_postgres::Client,
    partition_id: &str,
) -> Result<Option<String>, tokio_postgres::Error> {
    let stmt = client
        .prepare_cached(
            "SELECT q.name FROM queen.log_partitions p \
             JOIN queen.log_queues q ON q.id = p.queue_id \
             WHERE p.id = $1::text::uuid",
        )
        .await?;
    let rows = client.query(&stmt, &[&partition_id]).await?;
    Ok(rows.first().map(|r| r.get(0)))
}

// Trim worker/lag/parked metrics older than the retention window (called
// occasionally by the stats loop). Returns the SP JSON summary.
pub async fn cleanup_worker_metrics(
    client: &deadpool_postgres::Client,
    days: i32,
) -> Result<String, tokio_postgres::Error> {
    let row = client
        .query_one("SELECT (queen.cleanup_worker_metrics_v1($1::int))::text", &[&days])
        .await?;
    Ok(row.get(0))
}

// RUSTFIX item 20: purge queen.system_metrics rows older than `days`. This table
// is written every metrics window by syscollect.rs (insert_system_metrics above)
// and had NO purge anywhere, so it grew unbounded — the C++ RetentionService
// (retention_service.cpp:432-476) deleted it on a 90-day window. Batched so a
// large backlog doesn't lock the table in one statement; `batch` bounds each
// DELETE (RETENTION_BATCH_SIZE). Returns the total rows deleted.
pub async fn cleanup_system_metrics(
    client: &deadpool_postgres::Client,
    days: i32,
    batch: usize,
) -> Result<u64, tokio_postgres::Error> {
    let batch = batch.max(1) as i64;
    let stmt = "DELETE FROM queen.system_metrics \
                WHERE ctid IN ( \
                    SELECT ctid FROM queen.system_metrics \
                    WHERE timestamp < NOW() - make_interval(days => $1) \
                    LIMIT $2 )";
    let mut total: u64 = 0;
    loop {
        let n = client.execute(stmt, &[&days, &batch]).await?;
        total += n;
        if (n as i64) < batch {
            break;
        }
    }
    Ok(total)
}

// Wildcard pop (+auto-ack when auto_ack). Returns the SP result JSON as text.
// sub_mode / sub_from carry the caller's subscription intent ('all' | 'new' |
// timestamp) through to queen.log_pop_wildcard_wire_v1 (043), which seeds a NEW
// (partition, group) cursor accordingly on first contact (existing cursors are
// never re-seeded).
#[allow(clippy::too_many_arguments)]
pub async fn pop_wildcard(
    client: &deadpool_postgres::Client,
    queue: &str,
    group: &str,
    budget: i32,
    lease_seconds: i32,
    worker: &str,
    auto_ack: bool,
    max_partitions: i32,
    sub_mode: &str,
    sub_from: &str,
) -> Result<String, tokio_postgres::Error> {
    let stmt = client
        .prepare_cached(
            "SELECT (queen.log_pop_wildcard_wire_v1($1, $2, $3::int, $4::int, $5, $6::bool, $7::int, $8, $9))::text",
        )
        .await?;
    let row = client
        .query_one(
            &stmt,
            &[&queue, &group, &budget, &lease_seconds, &worker, &auto_ack,
              &max_partitions, &sub_mode, &sub_from],
        )
        .await?;
    Ok(row.get(0))
}

// Binary wildcard pop: same claim algorithm, but blobs come back as a NATIVE
// bytea[] aligned with the meta's segment traversal order — no base64 encode
// on PG, no whitespace-strip + decode on the broker (see log_pop_wildcard_bin_v1).
#[allow(clippy::too_many_arguments)]
pub async fn pop_wildcard_bin(
    client: &deadpool_postgres::Client,
    queue: &str,
    group: &str,
    budget: i32,
    lease_seconds: i32,
    worker: &str,
    auto_ack: bool,
    max_partitions: i32,
    sub_mode: &str,
    sub_from: &str,
) -> Result<(String, Vec<Vec<u8>>), tokio_postgres::Error> {
    let stmt = client
        .prepare_cached(
            "SELECT (t.meta)::text, t.blobs FROM queen.log_pop_wildcard_bin_v1($1,$2,$3,$4,$5,$6,$7,$8,$9) t",
        )
        .await?;
    let row = client
        .query_one(
            &stmt,
            &[&queue, &group, &budget, &lease_seconds, &worker, &auto_ack,
              &max_partitions, &sub_mode, &sub_from],
        )
        .await?;
    Ok((row.get(0), row.get(1)))
}

// Namespace/task discovery pop — GET /api/v1/pop (no queue in path). Wildcard-pops
// across every log queue whose queen.queues row matches the namespace/task
// filter, returning the SAME {"partitions":[...]} shape as pop_wildcard. Empty
// `namespace`/`task` disable that side of the filter (the handler guards against
// both being empty). `lease_seconds` is only the fallback lease when a matching
// queue has no log_queues.lease_time; each partition is otherwise leased with its
// own queue's configured leaseTime inside the SP.
#[allow(clippy::too_many_arguments)]
pub async fn pop_discover(
    client: &deadpool_postgres::Client,
    namespace: &str,
    task: &str,
    group: &str,
    budget: i32,
    lease_seconds: i32,
    worker: &str,
    auto_ack: bool,
    max_partitions: i32,
    sub_mode: &str,
    sub_from: &str,
) -> Result<String, tokio_postgres::Error> {
    let stmt = client
        .prepare_cached(
            "SELECT (queen.log_pop_discover_wire_v1($1, $2, $3, $4::int, $5::int, $6, $7::bool, $8::int, $9, $10))::text",
        )
        .await?;
    let row = client
        .query_one(
            &stmt,
            &[&namespace, &task, &group, &budget, &lease_seconds, &worker,
              &auto_ack, &max_partitions, &sub_mode, &sub_from],
        )
        .await?;
    Ok(row.get(0))
}

// ------------------------------------------------------ consumer groups
// GET /api/v1/consumer-groups -> queen.get_consumer_groups_v4() (047 redefined
// it dual-engine; the log branch reads queen.log_consumers). Returns the SP
// result JSON array as text.
pub async fn get_consumer_groups(
    client: &deadpool_postgres::Client,
) -> Result<String, tokio_postgres::Error> {
    let row = client
        .query_one("SELECT (queen.get_consumer_groups_v4())::text", &[])
        .await?;
    Ok(row.get(0))
}

// GET /api/v1/consumer-groups/lagging -> queen.get_lagging_partitions_v1($1)
// (047 redefined it dual-engine).
pub async fn get_lagging_partitions(
    client: &deadpool_postgres::Client,
    min_lag_seconds: i32,
) -> Result<String, tokio_postgres::Error> {
    let row = client
        .query_one(
            "SELECT (queen.get_lagging_partitions_v1($1::int))::text",
            &[&min_lag_seconds],
        )
        .await?;
    Ok(row.get(0))
}

// GET /api/v1/consumer-groups/:group -> queen.get_consumer_group_details_v1($1)
// (047 redefined it dual-engine).
pub async fn get_consumer_group_details(
    client: &deadpool_postgres::Client,
    group: &str,
) -> Result<String, tokio_postgres::Error> {
    let row = client
        .query_one(
            "SELECT (queen.get_consumer_group_details_v1($1))::text",
            &[&group],
        )
        .await?;
    Ok(row.get(0))
}

// Rows-side delete: queen.delete_consumer_group_v1 clears queen.partition_consumers,
// consumer_watermarks, and (when delete_metadata) consumer_groups_metadata.
pub async fn delete_consumer_group_rows(
    client: &deadpool_postgres::Client,
    group: &str,
    delete_metadata: bool,
) -> Result<String, tokio_postgres::Error> {
    let row = client
        .query_one(
            "SELECT (queen.delete_consumer_group_v1($1, $2::boolean))::text",
            &[&group, &delete_metadata],
        )
        .await?;
    Ok(row.get(0))
}

// Log-side delete: queen.log_delete_consumer_group_v1 (047) clears
// queen.log_consumers (all partitions) + consumer_watermarks for the group.
pub async fn delete_consumer_group_seg(
    client: &deadpool_postgres::Client,
    group: &str,
    delete_metadata: bool,
) -> Result<String, tokio_postgres::Error> {
    let row = client
        .query_one(
            "SELECT (queen.log_delete_consumer_group_v1($1, $2::boolean))::text",
            &[&group, &delete_metadata],
        )
        .await?;
    Ok(row.get(0))
}

// Log-side per-queue delete: drop this group's cursors for EVERY partition of
// the named queue (queen.log_consumers) + the empty-scan watermark row for
// (queue, group). Returns the number of log_consumers rows removed (the log
// analogue of deletedPartitions). No log SQL proc exists for the queue-scoped
// delete, so the two DELETEs are issued directly.
pub async fn delete_consumer_group_for_queue_seg(
    client: &deadpool_postgres::Client,
    group: &str,
    queue: &str,
) -> Result<u64, tokio_postgres::Error> {
    let n = client
        .execute(
            "DELETE FROM queen.log_consumers c \
             USING queen.log_partitions p \
             JOIN queen.log_queues q ON q.id = p.queue_id \
             WHERE c.partition_id = p.id \
               AND c.consumer_group = $1 \
               AND q.name = $2",
            &[&group, &queue],
        )
        .await?;
    client
        .execute(
            "DELETE FROM queen.consumer_watermarks \
             WHERE queue_name = $1 AND consumer_group = $2",
            &[&queue, &group],
        )
        .await?;
    Ok(n)
}

// Rows-side per-queue delete: queen.delete_consumer_group_for_queue_v1 clears
// queen.partition_consumers for the queue, consumer_watermarks for (queue,group),
// and (when delete_metadata) consumer_groups_metadata. Returns the SP JSON.
pub async fn delete_consumer_group_for_queue_rows(
    client: &deadpool_postgres::Client,
    group: &str,
    queue: &str,
    delete_metadata: bool,
) -> Result<String, tokio_postgres::Error> {
    let row = client
        .query_one(
            "SELECT (queen.delete_consumer_group_for_queue_v1($1, $2, $3::boolean))::text",
            &[&group, &queue, &delete_metadata],
        )
        .await?;
    Ok(row.get(0))
}

// POST /api/v1/consumer-groups/:group/subscription ->
// queen.update_consumer_group_subscription_v1 (writes consumer_groups_metadata;
// engine-agnostic). p_new_timestamp is TEXT SQL-side.
pub async fn update_consumer_group_subscription(
    client: &deadpool_postgres::Client,
    group: &str,
    timestamp: &str,
) -> Result<String, tokio_postgres::Error> {
    let row = client
        .query_one(
            "SELECT (queen.update_consumer_group_subscription_v1($1, $2))::text",
            &[&group, &timestamp],
        )
        .await?;
    Ok(row.get(0))
}

// POST /api/v1/consumer-groups/:group/queues/:queue/seek -> seek the LOG cursor
// of every partition of the queue (047). timestamp is an optional ISO string
// bound as TEXT and cast to timestamptz SQL-side (NULL when seeking to end).
pub async fn seg_seek_consumer_group(
    client: &deadpool_postgres::Client,
    group: &str,
    queue: &str,
    to_end: bool,
    timestamp: Option<&str>,
) -> Result<String, tokio_postgres::Error> {
    let stmt = "SELECT (queen.log_seek_consumer_group_v1($1, $2, $3::bool, $4::text::timestamptz))::text";
    let row = client
        .query_one(stmt, &[&group, &queue, &to_end, &timestamp])
        .await?;
    Ok(row.get(0))
}

// POST /.../partitions/:partition/seek -> seek ONE partition's log cursor.
pub async fn seg_seek_partition(
    client: &deadpool_postgres::Client,
    group: &str,
    queue: &str,
    partition: &str,
    to_end: bool,
    timestamp: Option<&str>,
) -> Result<String, tokio_postgres::Error> {
    let stmt = "SELECT (queen.log_seek_partition_v1($1, $2, $3, $4::bool, $5::text::timestamptz))::text";
    let row = client
        .query_one(stmt, &[&group, &queue, &partition, &to_end, &timestamp])
        .await?;
    Ok(row.get(0))
}

// ------------------------------------------------------------------- streams
// The three streaming SPs all take a JSONB ARRAY of requests ([{idx,..}]) and
// return a JSONB ARRAY of [{idx, result}]. The handlers wrap the single client
// body in a one-element array (idx:0) and unwrap [0].result, mirroring the C++
// streams routes. `requests_json` is that already-built one-element array.

// POST /streams/v1/queries -> queen.streams_register_query_v1 (020, unchanged).
// Returns the SP result array JSON as text.
pub async fn streams_register(
    client: &deadpool_postgres::Client,
    requests_json: &str,
) -> Result<String, tokio_postgres::Error> {
    // $1::text::jsonb pins $1 to TEXT so a &str binds.
    let stmt = "SELECT (queen.streams_register_query_v1($1::text::jsonb))::text";
    let row = client.query_one(stmt, &[&requests_json]).await?;
    Ok(row.get(0))
}

// POST /streams/v1/state/get -> queen.streams_state_get_v1 (022, unchanged).
// Read-only. Returns the SP result array JSON as text.
pub async fn streams_state_get(
    client: &deadpool_postgres::Client,
    requests_json: &str,
) -> Result<String, tokio_postgres::Error> {
    let stmt = "SELECT (queen.streams_state_get_v1($1::text::jsonb))::text";
    let row = client.query_one(stmt, &[&requests_json]).await?;
    Ok(row.get(0))
}

// POST /streams/v1/cycle -> queen.log_streams_cycle_v1 (046, log engine).
// `requests_json` is the one-element array whose sink frames are ALREADY
// broker-packed (metas + base64 zstd blob). Returns the SP result array JSON as
// text: [{idx, result:{success, query_id, partition_id, queueName,
// state_ops_applied, push_results, ack_result}}].
pub async fn streams_cycle(
    client: &deadpool_postgres::Client,
    requests_json: &str,
) -> Result<String, tokio_postgres::Error> {
    let stmt = client
        .prepare_cached("SELECT (queen.log_streams_cycle_v1($1::text::jsonb))::text")
        .await?;
    let row = client.query_one(&stmt, &[&requests_json]).await?;
    Ok(row.get(0))
}

// POST /api/v1/stats/refresh -> queen.refresh_all_stats_v1(true). Engine stats
// for the log engine are reconciled by log_refresh_all_stats_v1 on the stats
// cadence; this is wired for parity with the rows reconciler the dashboard
// expects.
pub async fn refresh_all_stats(
    client: &deadpool_postgres::Client,
) -> Result<String, tokio_postgres::Error> {
    let row = client
        .query_one("SELECT (queen.refresh_all_stats_v1(true))::text", &[])
        .await?;
    Ok(row.get(0))
}

// ------------------------------------------------------- system state (maintenance)
// Maintenance flags live in queen.system_state as {"enabled": bool} JSONB, keyed
// 'maintenance_mode' / 'pop_maintenance_mode' — the SAME rows the C++ broker's
// SharedStateManager reads/writes (server/src/services/shared_state_manager.cpp),
// so a mixed deployment stays consistent. Absent row => false.
pub async fn get_system_flag(
    client: &deadpool_postgres::Client,
    key: &str,
) -> Result<bool, tokio_postgres::Error> {
    let stmt = "SELECT (value->>'enabled') = 'true' FROM queen.system_state WHERE key = $1";
    let rows = client.query(stmt, &[&key]).await?;
    Ok(rows.first().and_then(|r| r.get::<_, Option<bool>>(0)).unwrap_or(false))
}

pub async fn set_system_flag(
    client: &deadpool_postgres::Client,
    key: &str,
    enabled: bool,
) -> Result<(), tokio_postgres::Error> {
    // $2::text::jsonb pins the value param to TEXT so a &str binds.
    let value = if enabled { "{\"enabled\": true}" } else { "{\"enabled\": false}" };
    let stmt = "INSERT INTO queen.system_state (key, value, updated_at) \
                VALUES ($1, $2::text::jsonb, NOW()) \
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()";
    client.execute(stmt, &[&key, &value]).await?;
    Ok(())
}
