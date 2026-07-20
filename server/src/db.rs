use deadpool_postgres::{Pool, PoolConfig, Runtime};
use tokio_postgres::NoTls;

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

// Ack a set of (transactionId -> ok) for ONE (partition, consumer_group, worker)
// via queen.seg_ack_by_txn_v1: validates the worker/lease, resolves each txn to a
// segment position through seg_dedup, and advances the cursor to the highest
// contiguous acked-ok prefix. `acks_json` is a JSON array [{"txn":..,"ok":bool}].
// Returns the SP result JSON as text: {"ok":bool,"acked":n,"seq":..,"off":..}.
pub async fn ack_by_txn(
    client: &deadpool_postgres::Client,
    partition_id: &str,
    group: &str,
    worker: &str,
    acks_json: &str,
) -> Result<String, tokio_postgres::Error> {
    // $1::text::uuid pins $1 to TEXT so a &str binds (a bare $1::uuid would make
    // tokio-postgres expect a Uuid); $4::text::jsonb likewise for the acks array.
    let stmt =
        "SELECT (queen.seg_ack_by_txn_v1($1::text::uuid, $2, $3, $4::text::jsonb))::text";
    let row = client
        .query_one(stmt, &[&partition_id, &group, &worker, &acks_json])
        .await?;
    Ok(row.get(0))
}

// Dead-letter the poison HEAD frame of a leased batch via queen.seg_dlq_head_v1
// (025). Called by the broker after seg_ack_by_txn_v1 signalled `dlq:true` on a
// nack whose delivery attempt exceeded the queue's retry_limit: the broker alone
// can decompress the frame, so it extracts (message_id, txn, payload) from the
// leased segment and passes them here as a SNAPSHOT. Under the still-held lease
// the SP files the seg_dlq row, advances the cursor past the frame, resets the
// attempt state and releases the lease. Returns {"ok":bool,...} as text.
#[allow(clippy::too_many_arguments)]
pub async fn seg_dlq_head(
    client: &deadpool_postgres::Client,
    partition_id: &str,
    group: &str,
    worker: &str,
    seq: i64,
    frame_idx: i32,
    message_id: &str,
    txn: &str,
    payload_json: &str,
    error: &str,
) -> Result<String, tokio_postgres::Error> {
    let stmt = "SELECT (queen.seg_dlq_head_v1($1::text::uuid, $2, $3, $4::bigint, \
                $5::int, $6::text::uuid, $7, $8::text::jsonb, $9))::text";
    let row = client
        .query_one(
            stmt,
            &[
                &partition_id,
                &group,
                &worker,
                &seq,
                &frame_idx,
                &message_id,
                &txn,
                &payload_json,
                &error,
            ],
        )
        .await?;
    Ok(row.get(0))
}

// Specific-partition pop (+auto-ack when auto_ack). Calls the 9-arg
// queen.seg_pop_segments_wire_v1 (025). Result JSON shape is single-partition:
// {"segments":[{seq,startOff,take,msgCount,createdAt,blob}],"partitionId":..,"attempt":..}.
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
    let stmt = "SELECT (queen.seg_pop_segments_wire_v1($1, $2, $3, $4::int, $5::int, $6, $7::bool, $8, $9))::text";
    let row = client
        .query_one(
            stmt,
            &[&queue, &partition, &group, &budget, &lease_seconds, &worker, &auto_ack,
              &sub_mode, &sub_from],
        )
        .await?;
    Ok(row.get(0))
}

// Renew every live lease held by `worker` via queen.seg_renew_lease_v1
// (023). Returns the SP result JSON as text: {"renewed":n,"expiresAt":iso|null}.
pub async fn renew_lease(
    client: &deadpool_postgres::Client,
    worker: &str,
    seconds: i32,
) -> Result<String, tokio_postgres::Error> {
    let stmt = "SELECT (queen.seg_renew_lease_v1($1, $2::int))::text";
    let row = client.query_one(stmt, &[&worker, &seconds]).await?;
    Ok(row.get(0))
}

// Atomic multi-op push+ack via queen.seg_transaction_wire_v1 (026). `payload`
// is the already-built {"pushes":[...],"acks":[...]} JSON. Returns the SP result
// JSON as text: {"ok":bool,...} or raises on rollback (duplicate/ack failure).
pub async fn transaction(
    client: &deadpool_postgres::Client,
    payload: &str,
) -> Result<String, tokio_postgres::Error> {
    // $1::text::jsonb pins $1 to TEXT so a &str binds (a bare $1::jsonb would
    // make tokio-postgres expect a Json param).
    let stmt = "SELECT (queen.seg_transaction_wire_v1($1::text::jsonb))::text";
    let row = client.query_one(stmt, &[&payload]).await?;
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

// Pin this queue to the segments engine (defaults to 'segments' already, but be
// explicit — the segments pop/push paths assume it).
pub async fn mark_queue_segments(
    client: &deadpool_postgres::Client,
    queue: &str,
) -> Result<(), tokio_postgres::Error> {
    client
        .execute("UPDATE queen.queues SET storage='segments' WHERE name=$1", &[&queue])
        .await?;
    Ok(())
}

// Ensure a queen.seg_queues row exists with the queue's lease/retention/dedup so
// the segments pop path reads the configured lease_time (and retention/dedup for
// later slices). Upsert so a reconfigure updates in place.
pub async fn upsert_seg_queue(
    client: &deadpool_postgres::Client,
    queue: &str,
    lease_time: i32,
    retention_seconds: i32,
    dedup_window_seconds: i32,
) -> Result<(), tokio_postgres::Error> {
    let stmt = "INSERT INTO queen.seg_queues(name, lease_time, retention_seconds, dedup_window_seconds) \
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

// Drop the queue's segment data. Deleting the seg_queues row cascades to
// seg_partitions -> seg_segments / seg_dedup. The coordination fold dropped the
// partition_consumers -> partitions FK (segment cursors key on seg_partitions.id
// with no FK), so the cursor rows are NO LONGER cascade-deleted; remove them
// explicitly first, while seg_partitions still resolves the queue.
pub async fn delete_seg_queue(
    client: &deadpool_postgres::Client,
    queue: &str,
) -> Result<(), tokio_postgres::Error> {
    client
        .execute(
            "DELETE FROM queen.partition_consumers pc \
             USING queen.seg_partitions sp \
             JOIN queen.seg_queues sq ON sq.id = sp.queue_id \
             WHERE pc.partition_id = sp.id AND sq.name = $1",
            &[&queue],
        )
        .await?;
    client
        .execute("DELETE FROM queen.seg_queues WHERE name=$1", &[&queue])
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

// Segment counts for a queue: (#segments, sum msg_count). Used to enrich the
// get_queue_v2 detail (whose stats come from queen.stats, which the segments
// engine does not populate).
pub async fn seg_queue_message_stats(
    client: &deadpool_postgres::Client,
    queue: &str,
) -> Result<(i64, i64), tokio_postgres::Error> {
    let stmt = "SELECT count(*)::bigint, coalesce(sum(s.msg_count),0)::bigint \
                FROM queen.seg_segments s \
                JOIN queen.seg_partitions p ON p.id = s.partition_id \
                JOIN queen.seg_queues q ON q.id = p.queue_id \
                WHERE q.name = $1";
    let row = client.query_one(stmt, &[&queue]).await?;
    Ok((row.get(0), row.get(1)))
}

// ------------------------------------------------------- resources LIST API
// The get_*_v2 / get_system_overview_v3 SPs power the /api/v1/resources/* list
// endpoints. They read from queen.queues / queen.stats; the segments engine
// populates queen.queues but NOT queen.stats, so the queue list comes back with
// zeroed partitions/messages and is enriched separately (see seg_queue_stats_all).
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

// Per-queue segment stats for the whole broker: name -> (partitions, segments,
// messages). Grouped analogue of seg_queue_message_stats, used to enrich the
// queue LIST endpoint since queen.stats is empty for the segments engine.
pub async fn seg_queue_stats_all(
    client: &deadpool_postgres::Client,
) -> Result<Vec<(String, i64, i64, i64)>, tokio_postgres::Error> {
    let stmt = "SELECT q.name, \
                       count(DISTINCT p.id)::bigint AS partitions, \
                       count(s.seq)::bigint AS segments, \
                       COALESCE(sum(s.msg_count), 0)::bigint AS messages \
                FROM queen.seg_queues q \
                LEFT JOIN queen.seg_partitions p ON p.queue_id = q.id \
                LEFT JOIN queen.seg_segments s ON s.partition_id = p.id \
                GROUP BY q.name";
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

// Configured lease time (seconds) for a queue, from queen.seg_queues by name.
// None when the queue has no seg_queues row yet (caller defaults to 300).
pub async fn queue_lease_time(
    client: &deadpool_postgres::Client,
    queue: &str,
) -> Result<Option<i32>, tokio_postgres::Error> {
    let stmt = "SELECT lease_time FROM queen.seg_queues WHERE name = $1";
    let rows = client.query(stmt, &[&queue]).await?;
    Ok(rows.first().map(|r| r.get::<_, i32>(0)))
}

// RUSTFIX item 8: per-queue at-rest encryption flag. Lives on queen.queues
// (written by 012_configure from options.encryptionEnabled), NOT seg_queues.
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
// Resolve (partition_id, transactionId) -> (seq, frame_idx, message_id) through
// queen.seg_dedup, whose PK is (partition_id, hashtextextended(txn, 0)) — the
// same hash the push path stores. O(1) inside the dedup window; None once the
// dedup row has been purged (older than dedupWindowSeconds) or the txn is
// unknown for this partition. This is the admin-path resolver from the plan's
// "Per-message access" decision: no permanent per-message index.
pub async fn seg_resolve_position(
    client: &deadpool_postgres::Client,
    partition_id: &str,
    txn: &str,
) -> Result<Option<(i64, i32, String)>, tokio_postgres::Error> {
    // $1::text::uuid pins $1 to TEXT so a &str binds.
    let stmt = "SELECT seq, frame_idx, message_id::text \
                FROM queen.seg_dedup \
                WHERE partition_id = $1::text::uuid \
                  AND txn_hash = hashtextextended($2, 0)";
    let rows = client.query(stmt, &[&partition_id, &txn]).await?;
    Ok(rows.first().map(|r| (r.get::<_, i64>(0), r.get::<_, i32>(1), r.get::<_, String>(2))))
}

// RUSTFIX item 23: fallback resolver for a txn whose seg_dedup entry has been
// purged (older than dedupWindowSeconds). Scans the partition's segment blobs
// newest-first (bounded) and returns (seq, blob) candidates so the handler can
// decode + match f.txn. O(segments in the partition) — acceptable for a
// management endpoint; bounded by LIMIT to avoid a pathological scan.
pub async fn seg_scan_segments(
    client: &deadpool_postgres::Client,
    partition_id: &str,
    limit: i64,
) -> Result<Vec<(i64, Vec<u8>)>, tokio_postgres::Error> {
    let stmt = "SELECT s.seq, s.blob FROM queen.seg_segments s \
                WHERE s.partition_id = $1::text::uuid ORDER BY s.seq DESC LIMIT $2";
    let rows = client.query(stmt, &[&partition_id, &limit]).await?;
    Ok(rows.iter().map(|r| (r.get::<_, i64>(0), r.get::<_, Vec<u8>>(1))).collect())
}

// RUSTFIX item 23: the extra management fields for GET /messages/:pid/:txn
// (010_messages.sql:194-271 parity) — queue/namespace/task, queueConfig, the
// per-group consumerGroups + leaseExpiresAt, DLQ error, and the flags to derive
// `status`. Returns the assembled JSON as text (or None if the partition is gone).
// Tolerates a missing queen.queues row (item 26) via COALESCE + split_part.
pub async fn seg_message_detail(
    client: &deadpool_postgres::Client,
    partition_id: &str,
    seq: i64,
    frame_idx: i32,
) -> Result<Option<String>, tokio_postgres::Error> {
    let stmt = "\
        WITH q AS ( \
            SELECT sq.name AS queue, sp.name AS partition, \
                   COALESCE(NULLIF(qq.namespace, ''), split_part(sq.name, '.', 1)) AS namespace, \
                   COALESCE(NULLIF(qq.task, ''), CASE WHEN position('.' in sq.name) > 0 \
                        THEN split_part(sq.name, '.', 2) ELSE '' END) AS task, \
                   COALESCE(qq.lease_time, 60) AS lease_time, \
                   COALESCE(qq.retry_limit, 3) AS retry_limit, \
                   COALESCE(qq.retry_delay, 1000) AS retry_delay, \
                   COALESCE(qq.ttl, 3600) AS ttl, \
                   COALESCE(qq.priority, 0) AS priority \
            FROM queen.seg_partitions sp \
            JOIN queen.seg_queues sq ON sq.id = sp.queue_id \
            LEFT JOIN queen.queues qq ON qq.name = sq.name \
            WHERE sp.id = $1::text::uuid ), \
        d AS ( SELECT error, COALESCE(retry_count, 0) AS retry_count FROM queen.seg_dlq \
               WHERE partition_id = $1::text::uuid AND seq = $2::bigint AND frame_idx = $3::int LIMIT 1 ), \
        g AS ( \
            SELECT COALESCE(jsonb_agg(jsonb_build_object( \
                       'name', pc.consumer_group, \
                       'group', pc.consumer_group, \
                       'consumed', (pc.next_seq, pc.next_off) > ($2::bigint, $3::int), \
                       'leaseExpiresAt', pc.lease_expires_at) \
                   ORDER BY pc.consumer_group), '[]'::jsonb) AS arr, \
                   MAX(CASE WHEN pc.consumer_group = '__QUEUE_MODE__' THEN pc.lease_expires_at END) AS qmode_lease, \
                   bool_and((pc.next_seq, pc.next_off) > ($2::bigint, $3::int)) \
                       FILTER (WHERE pc.consumer_group <> '__QUEUE_MODE__') AS bus_all_passed, \
                   bool_or((pc.next_seq, pc.next_off) > ($2::bigint, $3::int)) \
                       FILTER (WHERE pc.consumer_group = '__QUEUE_MODE__') AS qmode_passed, \
                   COUNT(*) FILTER (WHERE pc.consumer_group <> '__QUEUE_MODE__') AS bus_groups, \
                   COUNT(*) FILTER (WHERE pc.consumer_group = '__QUEUE_MODE__') AS has_qmode, \
                   bool_or(pc.lease_expires_at IS NOT NULL AND pc.lease_expires_at > now()) AS any_lease \
            FROM queen.partition_consumers pc WHERE pc.partition_id = $1::text::uuid ) \
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
    let rows = client
        .query(stmt, &[&partition_id, &seq, &frame_idx])
        .await?;
    Ok(rows.first().map(|r| r.get::<_, String>(0)))
}

// Fetch one segment's decode inputs: (createdAt ISO text, partition name, raw
// zstd blob bytes) for (partition_id, seq). None when the segment no longer
// exists (retention deleted). The blob comes back as BYTEA -> Vec<u8> directly
// (no base64 round-trip needed for a server-side query).
pub async fn seg_fetch_segment(
    client: &deadpool_postgres::Client,
    partition_id: &str,
    seq: i64,
) -> Result<Option<(String, String, Vec<u8>)>, tokio_postgres::Error> {
    let stmt = "SELECT to_char(s.created_at, 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'), \
                       sp.name, s.blob \
                FROM queen.seg_segments s \
                JOIN queen.seg_partitions sp ON sp.id = s.partition_id \
                WHERE s.partition_id = $1::text::uuid AND s.seq = $2::bigint";
    let rows = client.query(stmt, &[&partition_id, &seq]).await?;
    Ok(rows
        .first()
        .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1), r.get::<_, Vec<u8>>(2))))
}

// ------------------------------------------------------------- messages / dlq
// List messages (dual-engine 027). `filters_json` is the JSON filter bag
// ({queue,partition,namespace,task,status,from,to,limit,offset}). Segment-queue
// entries carry payloadAvailable:false + segment:{seq,frameIdx} — the broker
// decodes their payloads. Returns the SP result JSON as text.
pub async fn list_messages(
    client: &deadpool_postgres::Client,
    filters_json: &str,
) -> Result<String, tokio_postgres::Error> {
    let stmt = "SELECT (queen.list_messages_v1($1::text::jsonb))::text";
    let row = client.query_one(stmt, &[&filters_json]).await?;
    Ok(row.get(0))
}

// DLQ messages (dual-engine 027). queen.seg_dlq stores payload SNAPSHOTS, so no
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

// Delete a message addressed by (partition_id, transaction_id). The segments
// engine stores live payloads in immutable segments, so the deletable rows here
// are the DLQ snapshots in queen.seg_dlq (the DLQ manual-requeue workflow drops
// one). Also runs the rows-engine delete_message_v1 for dual-engine parity (a
// no-op on a pure-segments queue). Returns true when either path removed a row.
pub async fn delete_message(
    client: &deadpool_postgres::Client,
    partition_id: &str,
    txn: &str,
) -> Result<bool, tokio_postgres::Error> {
    let seg = client
        .execute(
            "DELETE FROM queen.seg_dlq WHERE partition_id = $1::text::uuid AND transaction_id = $2",
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
// transaction_id) — no blob decode. record_trace_v1 is made segment-aware in
// 030 (resolves message_id via seg_dedup, records with NULL message_id when the
// txn predates the dedup window).
pub async fn record_trace(
    client: &deadpool_postgres::Client,
    data_json: &str,
) -> Result<String, tokio_postgres::Error> {
    let stmt = "SELECT (queen.record_trace_v1($1::text::jsonb))::text";
    let row = client.query_one(stmt, &[&data_json]).await?;
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

// Prometheus DB metrics blob (dual-engine 027). Returns the SP JSON as text; the
// handler formats a subset into Prometheus exposition lines.
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
// ship in sql/procedures/{009,013,014,015,017,034}; these just dispatch them.
// ============================================================================

// get_queue_detail_v2 (segments-aware in 034) — /api/v1/status/queues/:queue.
pub async fn get_queue_detail(
    client: &deadpool_postgres::Client,
    queue: &str,
) -> Result<String, tokio_postgres::Error> {
    let row = client
        .query_one("SELECT (queen.get_queue_detail_v2($1))::text", &[&queue])
        .await?;
    Ok(row.get(0))
}

// get_analytics_v1 — /api/v1/status/analytics (message time-series).
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

// Segments-native queen.stats refresh (034). Returns the SP summary JSON.
pub async fn seg_refresh_all_stats(
    client: &deadpool_postgres::Client,
) -> Result<String, tokio_postgres::Error> {
    let row = client
        .query_one("SELECT (queen.seg_refresh_all_stats_v1())::text", &[])
        .await?;
    Ok(row.get(0))
}

// Insert one per-minute worker_metrics row (deltas). The AFTER INSERT trigger
// (queen.update_worker_metrics_summary) rolls these into worker_metrics_summary,
// so the lifetime totals the dashboard reads stay correct. ON CONFLICT DO NOTHING
// guards a same-minute double flush (rare; drops one delta rather than firing the
// summary trigger twice).
#[allow(clippy::too_many_arguments)]
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

// RUSTFIX item 1: multi-segment push for the file-buffer drain. Identical SP call
// to the fusion path (queen.seg_push_segments_multi_v1), exposed so file_buffer.rs
// can replay spooled events preserving each original transactionId (dedup makes
// the replay idempotent). `segments_json` is index-aligned with `blobs`.
pub async fn push_segments_multi(
    client: &deadpool_postgres::Client,
    segments_json: &str,
    blobs: Vec<Vec<u8>>,
) -> Result<String, tokio_postgres::Error> {
    let stmt = "SELECT (queen.seg_push_segments_multi_v1($1::text::jsonb, $2))::text";
    let row = client.query_one(stmt, &[&segments_json, &blobs]).await?;
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
    let stmt = "SELECT sq.name FROM queen.seg_partitions sp \
                JOIN queen.seg_queues sq ON sq.id = sp.queue_id \
                WHERE sp.id = $1::text::uuid";
    let rows = client.query(stmt, &[&partition_id]).await?;
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

// ---------------------------------------------------------------- retention
// Segment-queue max_wait_time_seconds eviction — the one maintenance rule the
// segments SQL has no function for. The C++ EvictionService
// (server/src/services/eviction_service.cpp) deletes pending messages older than
// queen.queues.max_wait_time_seconds; there is no queen.seg_* SP for it (026's
// seg_evict_v1 is max_queue_size only, seg_retention_sweep_v1 reads
// retention_seconds/completed_retention_seconds). We reproduce it here so a
// queue configured with only { retentionEnabled, maxWaitTimeSeconds } still gets
// swept (retention.js::retentionTestMaxTime).
//
// Semantics mirror seg_retention_sweep_v1's time rule: for each segment queue
// with max_wait_time_seconds > 0, delete the contiguous seq prefix of segments
// older than now()-max_wait_time_seconds (using the existing STABLE helper
// queen.seg_retention_boundary_v1 to find the first still-fresh seq) and advance
// the partition's retention_seq watermark. Whole-segment granularity, like the
// other maintenance rules; a cursor left below the new watermark resumes at the
// next existing seq (the pop path tolerates the gap).
//
// Partitions are processed in ascending id order — the same total lock order
// seg_transaction_wire_v1 and the 026 sweeps use — so this never deadlocks
// (40P01) against a concurrent push. Runs inside the caller's advisory-locked
// transaction (so it sees any retention_seq already advanced by the sweep/evict
// calls in the same cycle). Returns the number of segments deleted.
pub async fn seg_evict_max_wait(
    client: &deadpool_postgres::Client,
) -> Result<i64, tokio_postgres::Error> {
    let rows = client
        .query(
            "SELECT p.id::text, p.retention_seq, \
                    queen.seg_retention_boundary_v1(p.id, p.retention_seq, \
                        now() - make_interval(secs => qq.max_wait_time_seconds)) \
             FROM queen.queues qq \
             JOIN queen.seg_queues q2q ON q2q.name = qq.name \
             JOIN queen.seg_partitions p ON p.queue_id = q2q.id \
             WHERE qq.storage = 'segments' AND qq.max_wait_time_seconds > 0 \
             ORDER BY p.id",
            &[],
        )
        .await?;

    let mut deleted: i64 = 0;
    for row in &rows {
        let pid: String = row.get(0);
        let from_seq: i64 = row.get(1);
        let boundary: i64 = row.get(2);
        if boundary <= from_seq {
            continue;
        }
        let n = client
            .execute(
                "DELETE FROM queen.seg_segments \
                 WHERE partition_id = $1::text::uuid AND seq >= $2 AND seq < $3",
                &[&pid, &from_seq, &boundary],
            )
            .await?;
        deleted += n as i64;
        client
            .execute(
                "UPDATE queen.seg_partitions SET retention_seq = $2 \
                 WHERE id = $1::text::uuid AND retention_seq < $2",
                &[&pid, &boundary],
            )
            .await?;
    }
    Ok(deleted)
}

// Wildcard pop (+auto-ack when auto_ack). Returns the SP result JSON as text.
// sub_mode / sub_from carry the caller's subscription intent ('all' | 'new' |
// timestamp) through to queen.seg_pop_segments_v1, which seeds a NEW
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
    let stmt = "SELECT (queen.seg_pop_wildcard_wire_v1($1, $2, $3::int, $4::int, $5, $6::bool, $7::int, $8, $9))::text";
    let row = client
        .query_one(
            stmt,
            &[&queue, &group, &budget, &lease_seconds, &worker, &auto_ack,
              &max_partitions, &sub_mode, &sub_from],
        )
        .await?;
    Ok(row.get(0))
}

// Namespace/task discovery pop — GET /api/v1/pop (no queue in path). Wildcard-pops
// across every segment queue whose queen.queues row matches the namespace/task
// filter, returning the SAME {"partitions":[...]} shape as pop_wildcard. Empty
// `namespace`/`task` disable that side of the filter (the handler guards against
// both being empty). `lease_seconds` is only the fallback lease when a matching
// queue has no seg_queues.lease_time; each partition is otherwise leased with its
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
    let stmt = "SELECT (queen.seg_pop_discover_wire_v1($1, $2, $3, $4::int, $5::int, $6, $7::bool, $8::int, $9, $10))::text";
    let row = client
        .query_one(
            stmt,
            &[&namespace, &task, &group, &budget, &lease_seconds, &worker,
              &auto_ack, &max_partitions, &sub_mode, &sub_from],
        )
        .await?;
    Ok(row.get(0))
}

// ------------------------------------------------------ consumer groups
// GET /api/v1/consumer-groups -> queen.get_consumer_groups_v4() (027 redefined
// it dual-engine; the segments branch reads queen.partition_consumers). Returns the
// SP result JSON array as text.
pub async fn get_consumer_groups(
    client: &deadpool_postgres::Client,
) -> Result<String, tokio_postgres::Error> {
    let row = client
        .query_one("SELECT (queen.get_consumer_groups_v4())::text", &[])
        .await?;
    Ok(row.get(0))
}

// GET /api/v1/consumer-groups/lagging -> queen.get_lagging_partitions_v1($1).
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

// GET /api/v1/consumer-groups/:group -> queen.get_consumer_group_details_v1($1).
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

// Segment-side delete: queen.seg_delete_consumer_group_v1 clears queen.partition_consumers
// (all partitions) + consumer_watermarks for the group.
pub async fn delete_consumer_group_seg(
    client: &deadpool_postgres::Client,
    group: &str,
    delete_metadata: bool,
) -> Result<String, tokio_postgres::Error> {
    let row = client
        .query_one(
            "SELECT (queen.seg_delete_consumer_group_v1($1, $2::boolean))::text",
            &[&group, &delete_metadata],
        )
        .await?;
    Ok(row.get(0))
}

// Segment-side per-queue delete: drop this group's segment cursors for EVERY
// partition of the named queue (queen.partition_consumers) + the seg empty-scan
// watermark row for (queue, group). Returns the number of partition_consumers rows
// removed (the segment analogue of deletedPartitions). No seg SQL proc exists
// for the queue-scoped delete, so the two DELETEs are issued directly.
pub async fn delete_consumer_group_for_queue_seg(
    client: &deadpool_postgres::Client,
    group: &str,
    queue: &str,
) -> Result<u64, tokio_postgres::Error> {
    let n = client
        .execute(
            "DELETE FROM queen.partition_consumers c \
             USING queen.seg_partitions p \
             JOIN queen.seg_queues q ON q.id = p.queue_id \
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

// POST /api/v1/consumer-groups/:group/queues/:queue/seek -> seek the SEGMENT
// cursor of every partition of the queue (031). timestamp is an optional ISO
// string bound as TEXT and cast to timestamptz SQL-side (NULL when seeking to
// end).
pub async fn seg_seek_consumer_group(
    client: &deadpool_postgres::Client,
    group: &str,
    queue: &str,
    to_end: bool,
    timestamp: Option<&str>,
) -> Result<String, tokio_postgres::Error> {
    let stmt = "SELECT (queen.seg_seek_consumer_group_v1($1, $2, $3::bool, $4::text::timestamptz))::text";
    let row = client
        .query_one(stmt, &[&group, &queue, &to_end, &timestamp])
        .await?;
    Ok(row.get(0))
}

// POST /.../partitions/:partition/seek -> seek ONE partition's segment cursor.
pub async fn seg_seek_partition(
    client: &deadpool_postgres::Client,
    group: &str,
    queue: &str,
    partition: &str,
    to_end: bool,
    timestamp: Option<&str>,
) -> Result<String, tokio_postgres::Error> {
    let stmt = "SELECT (queen.seg_seek_partition_v1($1, $2, $3, $4::bool, $5::text::timestamptz))::text";
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

// POST /streams/v1/cycle -> queen.seg_streams_cycle_v1 (029, segments engine).
// `requests_json` is the one-element array whose sink frames are ALREADY
// broker-packed (metas + base64 zstd blob). Returns the SP result array JSON as
// text: [{idx, result:{success, query_id, partition_id, queueName,
// state_ops_applied, push_results, ack_result}}].
pub async fn streams_cycle(
    client: &deadpool_postgres::Client,
    requests_json: &str,
) -> Result<String, tokio_postgres::Error> {
    let stmt = "SELECT (queen.seg_streams_cycle_v1($1::text::jsonb))::text";
    let row = client.query_one(stmt, &[&requests_json]).await?;
    Ok(row.get(0))
}

// POST /api/v1/stats/refresh -> queen.refresh_all_stats_v1(true). Engine stats
// for segments are computed live, so this is effectively a no-op there; wired
// for parity with the rows reconciler the dashboard expects.
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
