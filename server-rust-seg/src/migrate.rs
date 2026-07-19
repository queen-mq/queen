//! Big-bang OFFLINE rows→segments migration — the `queen-seg migrate ...`
//! subcommand of the broker.
//!
//! Why a subcommand and not a standalone script: the migration reuses the EXACT
//! frame codec (`frames::pack_frames` + zstd) the live push path uses, so a
//! migrated segment is byte-for-byte popable by the running broker. A separate
//! re-implementation of packing would risk a one-byte framing drift that
//! silently corrupts every future pop. The broker stays codec-only; the dedup
//! hash, the atomic explicit-seq insert, and the cursor arithmetic (K in one
//! place) live in SQL (`procedures/028_storage_v2_migrate.sql`).
//!
//! Flow (see the plan's "Migration tool" section):
//!   1. Identity mapping — create `seg_queues`/`seg_partitions` REUSING the
//!      rows-engine UUIDs, so `partition_id` is one identity across engines.
//!   2. Per (queue,partition), in its OWN transaction (resumable):
//!        delete-partial → keyset-paginate `queen.messages` in (created_at,id)
//!        order → buffer K frames → pack+zstd+base64 → write segment at an
//!        EXPLICIT seq → seed the segment cursors from the rows cursors →
//!        finalize `last_seq` → mark done in `seg_migration_progress`.
//!   3. DLQ carry-over (once) + flip `queen.queues.storage='segments'`.
//!
//! Modes: `all` (migrate everything, cursors land mid-stream), `unconsumed`
//! (migrate only rank ≥ Cmin, seq from 1, shift each group's cursor by Cmin),
//! `window` (migrate only created_at ≥ now()-since, reprocess-attach cursors).

use std::time::Instant;

use base64::Engine as _;
use deadpool_postgres::{Client, Pool};
use tokio_postgres::types::ToSql;

use crate::config::Config;
use crate::db;
use crate::frames::{pack_frames, uuid_string_to_bytes, unpack_frames, zstd_compress, zstd_decompress, FrameIn};

const ZERO_UUID: &str = "00000000-0000-0000-0000-000000000000";
const NEG_INF: &str = "-infinity";
const PAGE: i64 = 2000;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    All,
    Unconsumed,
    Window,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum DedupScope {
    Window,
    Unconsumed,
    All,
}

struct Opts {
    mode: Mode,
    since_mins: i64,
    seg_frames: usize, // K
    dedup_scope: DedupScope,
    dedup_window_secs: i32,
    queues_glob: String,
    zstd_level: i32,
}

#[derive(Default)]
struct Summary {
    queues: usize,
    partitions: usize,
    skipped: usize,
    messages: i64,
    segments: i64,
    dlq_carried: i64,
}

// One buffered frame, owning everything pack_frames / the metas builder borrow.
struct MFrame {
    mid_bytes: [u8; 16],
    mid_str: String,
    txn: String,
    payload: Vec<u8>,
    created_at: String, // created_at::text (PG-parseable timestamptz)
    encrypted: bool,
    trace_id: Option<[u8; 16]>,
    producer_sub: Option<String>,
    dedup: bool,
}

pub async fn run(cfg: Config, args: Vec<String>) {
    let opts = parse_opts(&args);
    println!(
        "migrate: mode={:?} since={}min seg_frames(K)={} dedup_scope={:?} dedup_window={}s queues='{}'",
        opts.mode, opts.since_mins, opts.seg_frames, opts.dedup_scope, opts.dedup_window_secs, opts.queues_glob
    );

    let pool = db::create_pool(&cfg);

    // Ensure the segments schema + the 028 migration functions exist. Idempotent;
    // makes the tool self-contained (does not assume a prior server boot).
    if let Err(e) = crate::schema::apply(&pool).await {
        eprintln!("FATAL: schema apply failed: {e}");
        std::process::exit(1);
    }

    let t0 = Instant::now();
    let mut summary = Summary::default();

    // ---- 1. Identity mapping: seg_queues / seg_partitions reuse rows UUIDs. ----
    let like = glob_to_like(&opts.queues_glob);
    if let Err(e) = identity_map(&pool, &opts, like.as_deref(), &mut summary).await {
        eprintln!("FATAL: identity mapping failed: {e}");
        std::process::exit(1);
    }

    // ---- 2. Enumerate partitions to migrate, then migrate each. ----
    let partitions = match enumerate_partitions(&pool, like.as_deref()).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("FATAL: partition enumeration failed: {e}");
            std::process::exit(1);
        }
    };
    println!("migrate: {} partition(s) in scope", partitions.len());

    let mut first_all_mode_pid: Option<String> = None;
    for (pid, queue_name, partition_name) in &partitions {
        match migrate_partition(&pool, &opts, pid, queue_name, partition_name, &mut summary).await {
            Ok(migrated) => {
                if migrated && opts.mode == Mode::All && first_all_mode_pid.is_none() {
                    first_all_mode_pid = Some(pid.clone());
                }
            }
            Err(e) => {
                eprintln!("FATAL: partition {queue_name}/{partition_name} ({pid}) failed: {e}");
                std::process::exit(1);
            }
        }
    }

    // ---- 3. Carry-over: DLQ snapshots + flip the storage flag on migrated queues.
    match dlq_carryover(&pool).await {
        Ok(n) => summary.dlq_carried = n,
        Err(e) => eprintln!("WARN: DLQ carry-over failed: {e}"),
    }
    if let Err(e) = mark_queues_segments(&pool, like.as_deref()).await {
        eprintln!("WARN: storage flag flip failed: {e}");
    }

    // ---- Validation: per-partition count parity + a pop-parity spot check. ----
    if let Err(e) = validate_counts(&pool, like.as_deref()).await {
        eprintln!("WARN: count validation failed: {e}");
    }
    if let Some(pid) = first_all_mode_pid {
        if let Err(e) = validate_pop_parity(&pool, &pid).await {
            eprintln!("WARN: pop-parity spot check failed: {e}");
        }
    }

    println!(
        "\nmigrate: DONE in {:.2}s — queues={} partitions_migrated={} skipped(done)={} messages={} segments={} dlq_carried={}",
        t0.elapsed().as_secs_f64(),
        summary.queues,
        summary.partitions,
        summary.skipped,
        summary.messages,
        summary.segments,
        summary.dlq_carried,
    );
}

// ---------------------------------------------------------------- CLI parsing
fn parse_opts(args: &[String]) -> Opts {
    let mut mode = Mode::All;
    let mut since_mins: i64 = 60;
    let mut seg_frames: usize = 500;
    let mut dedup_scope = DedupScope::Window;
    let mut dedup_window_secs: i32 = 3600;
    let mut queues_glob = String::from("*");
    let zstd_level = std::env::var("QUEEN_V2_ZSTD_LEVEL")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);

    let mut i = 0;
    while i < args.len() {
        let a = args[i].as_str();
        let mut next = || {
            i += 1;
            args.get(i).cloned().unwrap_or_default()
        };
        match a {
            "--mode" => {
                mode = match next().as_str() {
                    "unconsumed" => Mode::Unconsumed,
                    "window" => Mode::Window,
                    _ => Mode::All,
                }
            }
            "--since" => since_mins = next().parse().unwrap_or(60),
            "--seg-frames" => seg_frames = next().parse::<usize>().unwrap_or(500).max(1),
            "--dedup-scope" => {
                dedup_scope = match next().as_str() {
                    "unconsumed" => DedupScope::Unconsumed,
                    "all" => DedupScope::All,
                    _ => DedupScope::Window,
                }
            }
            "--dedup-window" => dedup_window_secs = next().parse().unwrap_or(3600),
            "--queues" => queues_glob = next(),
            _ => {}
        }
        i += 1;
    }

    Opts {
        mode,
        since_mins,
        seg_frames,
        dedup_scope,
        dedup_window_secs,
        queues_glob,
        zstd_level,
    }
}

// Convert a simple queue glob to a SQL LIKE pattern. `*`→`%`, `?`→`_`. Returns
// None (no filter) for the "all" cases.
fn glob_to_like(glob: &str) -> Option<String> {
    if glob.is_empty() || glob == "*" || glob == "all" {
        return None;
    }
    let mut out = String::with_capacity(glob.len());
    for c in glob.chars() {
        match c {
            '*' => out.push('%'),
            '?' => out.push('_'),
            '%' | '_' | '\\' => {
                out.push('\\');
                out.push(c);
            }
            c => out.push(c),
        }
    }
    Some(out)
}

// --------------------------------------------------------- identity mapping
async fn identity_map(
    pool: &Pool,
    opts: &Opts,
    like: Option<&str>,
    summary: &mut Summary,
) -> Result<(), tokio_postgres::Error> {
    let client = pool.get().await.expect("pool");

    // seg_queues reuse queen.queues UUIDs; copy lease_time/retention, set dedup.
    let q_sql = format!(
        "INSERT INTO queen.seg_queues (id, name, lease_time, retention_seconds, dedup_window_seconds) \
         SELECT id, name, lease_time, retention_seconds, $1::int \
         FROM queen.queues{} \
         ON CONFLICT DO NOTHING",
        if like.is_some() { " WHERE name LIKE $2" } else { "" }
    );
    if let Some(pat) = like {
        client.execute(q_sql.as_str(), &[&opts.dedup_window_secs, &pat]).await?;
    } else {
        client.execute(q_sql.as_str(), &[&opts.dedup_window_secs]).await?;
    }

    // seg_partitions reuse queen.partitions UUIDs; queue_id already matches the
    // reused seg_queues UUID.
    let p_sql = format!(
        "INSERT INTO queen.seg_partitions (id, queue_id, name) \
         SELECT p.id, p.queue_id, p.name \
         FROM queen.partitions p JOIN queen.queues q ON q.id = p.queue_id{} \
         ON CONFLICT DO NOTHING",
        if like.is_some() { " WHERE q.name LIKE $1" } else { "" }
    );
    if let Some(pat) = like {
        client.execute(p_sql.as_str(), &[&pat]).await?;
    } else {
        client.execute(p_sql.as_str(), &[]).await?;
    }

    // Count queues in scope for the summary.
    let cnt_sql = format!(
        "SELECT count(*)::bigint FROM queen.queues{}",
        if like.is_some() { " WHERE name LIKE $1" } else { "" }
    );
    let row = if let Some(pat) = like {
        client.query_one(cnt_sql.as_str(), &[&pat]).await?
    } else {
        client.query_one(cnt_sql.as_str(), &[]).await?
    };
    summary.queues = row.get::<_, i64>(0) as usize;
    Ok(())
}

async fn enumerate_partitions(
    pool: &Pool,
    like: Option<&str>,
) -> Result<Vec<(String, String, String)>, tokio_postgres::Error> {
    let client = pool.get().await.expect("pool");
    let sql = format!(
        "SELECT p.id::text, q.name, p.name \
         FROM queen.partitions p JOIN queen.queues q ON q.id = p.queue_id{} \
         ORDER BY q.name, p.name",
        if like.is_some() { " WHERE q.name LIKE $1" } else { "" }
    );
    let rows = if let Some(pat) = like {
        client.query(sql.as_str(), &[&pat]).await?
    } else {
        client.query(sql.as_str(), &[]).await?
    };
    Ok(rows
        .iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1), r.get::<_, String>(2)))
        .collect())
}

// --------------------------------------------------------- per-partition work
// Returns Ok(true) if it migrated this partition, Ok(false) if it was skipped
// (already done). Runs in a single transaction so a crash resumes cleanly.
async fn migrate_partition(
    pool: &Pool,
    opts: &Opts,
    pid: &str,
    queue_name: &str,
    partition_name: &str,
    summary: &mut Summary,
) -> Result<bool, tokio_postgres::Error> {
    let client = pool.get().await.expect("pool");

    // Skip if already finished on a prior run.
    let done: bool = client
        .query_one(
            "SELECT COALESCE((SELECT status = 'done' FROM queen.seg_migration_progress \
             WHERE partition_id = $1::text::uuid), false)",
            &[&pid],
        )
        .await?
        .get(0);
    if done {
        summary.skipped += 1;
        return Ok(false);
    }

    client.batch_execute("BEGIN").await?;
    let res = migrate_partition_txn(&client, opts, pid, queue_name, partition_name).await;
    match res {
        Ok(pstats) => {
            client.batch_execute("COMMIT").await?;
            summary.partitions += 1;
            summary.messages += pstats.messages;
            summary.segments += pstats.segments;
            println!(
                "  migrated {queue_name}/{partition_name}: {} msg -> {} seg (last_seq={})",
                pstats.messages, pstats.segments, pstats.segments
            );
            Ok(true)
        }
        Err(e) => {
            let _ = client.batch_execute("ROLLBACK").await;
            Err(e)
        }
    }
}

struct PartStats {
    messages: i64,
    segments: i64,
}

async fn migrate_partition_txn(
    client: &Client,
    opts: &Opts,
    pid: &str,
    queue_name: &str,
    _partition_name: &str,
) -> Result<PartStats, tokio_postgres::Error> {
    // Mark in_progress.
    client
        .execute(
            "INSERT INTO queen.seg_migration_progress (partition_id, status, started_at) \
             VALUES ($1::text::uuid, 'in_progress', now()) \
             ON CONFLICT (partition_id) DO UPDATE SET status='in_progress', started_at=now()",
            &[&pid],
        )
        .await?;

    // Delete-partial: clean any partially-written segment/dedup/cursor state so a
    // re-run is a fresh, deterministic rebuild (seq is a function of survivor rank).
    client
        .execute("DELETE FROM queen.seg_segments WHERE partition_id = $1::text::uuid", &[&pid])
        .await?;
    client
        .execute("DELETE FROM queen.seg_dedup WHERE partition_id = $1::text::uuid", &[&pid])
        .await?;
    client
        .execute("DELETE FROM queen.seg_consumers WHERE partition_id = $1::text::uuid", &[&pid])
        .await?;

    // ---- Read the rows-engine cursors and compute each group's consumed count. --
    let group_rows = client
        .query(
            "SELECT consumer_group, last_consumed_created_at::text, last_consumed_id::text \
             FROM queen.partition_consumers WHERE partition_id = $1::text::uuid",
            &[&pid],
        )
        .await?;

    struct GroupInfo {
        group: String,
        last_ca: Option<String>,
        last_id: Option<String>,
        consumed: i64, // C_g
    }
    let mut groups: Vec<GroupInfo> = Vec::with_capacity(group_rows.len());
    for r in &group_rows {
        let group: String = r.get(0);
        let last_ca: Option<String> = r.get(1);
        let last_id: Option<String> = r.get(2);
        let consumed: i64 = client
            .query_one(
                "SELECT queen.seg_migrate_consumed_count_v1($1::text::uuid, $2::text::timestamptz, $3::text::uuid)",
                &[&pid, &last_ca, &last_id],
            )
            .await?
            .get(0);
        groups.push(GroupInfo { group, last_ca, last_id, consumed });
    }

    // Cmin = min consumed over the groups (a never-consumed / absent group => 0).
    let cmin: i64 = groups.iter().map(|g| g.consumed).min().unwrap_or(0);

    // ---- Determine pagination start + a window cutoff. ----
    let mut start_ca = NEG_INF.to_string();
    let mut start_id = ZERO_UUID.to_string();
    let mut window_cutoff: Option<String> = None;

    match opts.mode {
        Mode::All => {}
        Mode::Unconsumed => {
            if cmin > 0 {
                // Start strictly AFTER the least-advanced group's cursor: the
                // WHERE (created_at,id) > cursor yields exactly ranks >= Cmin, so
                // seq starts at 1 with no wasted skip-reads.
                if let Some(g) = groups.iter().find(|g| g.consumed == cmin) {
                    if let (Some(ca), Some(id)) = (&g.last_ca, &g.last_id) {
                        start_ca = ca.clone();
                        start_id = id.clone();
                    }
                }
            }
        }
        Mode::Window => {
            let cutoff: String = client
                .query_one("SELECT (now() - make_interval(mins => $1::int))::text", &[&(opts.since_mins as i32)])
                .await?
                .get(0);
            window_cutoff = Some(cutoff);
        }
    }

    // ---- Keyset-paginate + build segments. ----
    let base_sql = if window_cutoff.is_some() {
        "SELECT id::text, transaction_id, payload::text, created_at::text, is_encrypted, \
                trace_id::text, producer_sub, \
                (created_at >= (now() - make_interval(secs => $4::int)))::bool \
         FROM queen.messages \
         WHERE partition_id = $1::text::uuid \
           AND (created_at, id) > ($2::text::timestamptz, $3::text::uuid) \
           AND created_at >= $5::text::timestamptz \
         ORDER BY created_at, id \
         LIMIT $6::bigint"
            .to_string()
    } else {
        "SELECT id::text, transaction_id, payload::text, created_at::text, is_encrypted, \
                trace_id::text, producer_sub, \
                (created_at >= (now() - make_interval(secs => $4::int)))::bool \
         FROM queen.messages \
         WHERE partition_id = $1::text::uuid \
           AND (created_at, id) > ($2::text::timestamptz, $3::text::uuid) \
         ORDER BY created_at, id \
         LIMIT $5::bigint"
            .to_string()
    };

    let mut buf: Vec<MFrame> = Vec::with_capacity(opts.seg_frames);
    let mut next_seq: i64 = 1;
    let mut segments: i64 = 0;
    let mut messages: i64 = 0;
    let mut local_rank: i64 = 0; // 0-based rank within the MIGRATED set

    loop {
        let dw = opts.dedup_window_secs;
        let rows = if let Some(cut) = &window_cutoff {
            let params: [&(dyn ToSql + Sync); 6] = [&pid, &start_ca, &start_id, &dw, cut, &PAGE];
            client.query(base_sql.as_str(), &params).await?
        } else {
            let params: [&(dyn ToSql + Sync); 5] = [&pid, &start_ca, &start_id, &dw, &PAGE];
            client.query(base_sql.as_str(), &params).await?
        };
        if rows.is_empty() {
            break;
        }
        let n = rows.len();
        for r in &rows {
            let mid_str: String = r.get(0);
            let txn: String = r.get(1);
            let payload_txt: String = r.get(2);
            let created_at: String = r.get(3);
            let encrypted: bool = r.get(4);
            let trace_txt: Option<String> = r.get(5);
            let producer_sub: Option<String> = r.get(6);
            let window_ok: bool = r.get(7);

            // advance the keyset cursor
            start_ca = created_at.clone();
            start_id = mid_str.clone();

            let global_rank = cmin + local_rank; // meaningful for All (cmin=0) & Unconsumed
            let dedup = match opts.dedup_scope {
                DedupScope::All => true,
                DedupScope::Window => window_ok,
                DedupScope::Unconsumed => match opts.mode {
                    // only the not-yet-consumed suffix; migrated frames in the
                    // other modes are already all-unconsumed.
                    Mode::All => global_rank >= cmin, // == true here unless cmin>0
                    _ => true,
                },
            };

            let mid_bytes = uuid_string_to_bytes(&mid_str)
                .unwrap_or([0u8; 16]);
            let trace_id = trace_txt.as_deref().and_then(uuid_string_to_bytes);

            buf.push(MFrame {
                mid_bytes,
                mid_str,
                txn,
                payload: payload_txt.into_bytes(),
                created_at,
                encrypted,
                trace_id,
                producer_sub,
                dedup,
            });
            local_rank += 1;

            if buf.len() >= opts.seg_frames {
                flush_segment(client, opts, pid, next_seq, &buf).await?;
                messages += buf.len() as i64;
                segments += 1;
                next_seq += 1;
                buf.clear();
            }
        }
        if n < PAGE as usize {
            break;
        }
    }
    // Final partial segment.
    if !buf.is_empty() {
        flush_segment(client, opts, pid, next_seq, &buf).await?;
        messages += buf.len() as i64;
        segments += 1;
        buf.clear();
    }

    // last_seq = number of segments written (allocator resumes here for live push).
    client
        .query_one(
            "SELECT queen.seg_migrate_finalize_partition_v1($1::text::uuid, $2::bigint)",
            &[&pid, &segments],
        )
        .await?;

    // ---- Seed the segment cursors from the rows cursors. ----
    for g in &groups {
        let seeded = match opts.mode {
            Mode::All => g.consumed,           // cursor lands mid-stream
            Mode::Unconsumed => g.consumed - cmin, // shift by Cmin, seq from 1
            Mode::Window => 0,                 // reprocess the migrated window
        };
        client
            .query_one(
                "SELECT queen.seg_migrate_seed_consumer_v1($1::text::uuid, $2, $3::bigint, $4::int)",
                &[&pid, &g.group, &seeded, &(opts.seg_frames as i32)],
            )
            .await?;
    }

    // Mark done.
    client
        .execute(
            "UPDATE queen.seg_migration_progress \
             SET status='done', segments_written=$2::bigint, consumers_done=true, done_at=now() \
             WHERE partition_id=$1::text::uuid",
            &[&pid, &segments],
        )
        .await?;

    let _ = queue_name;
    Ok(PartStats { messages, segments })
}

// Pack the buffered frames into ONE segment and write it at an explicit seq.
async fn flush_segment(
    client: &Client,
    opts: &Opts,
    pid: &str,
    seq: i64,
    buf: &[MFrame],
) -> Result<(), tokio_postgres::Error> {
    // metas: [{"i":frame_idx,"mid":..,"txn":..,"createdAt":..,"dedup":bool}]
    let metas: Vec<serde_json::Value> = buf
        .iter()
        .enumerate()
        .map(|(i, f)| {
            serde_json::json!({
                "i": i,
                "mid": f.mid_str,
                "txn": f.txn,
                "createdAt": f.created_at,
                "dedup": f.dedup,
            })
        })
        .collect();
    let metas_json = serde_json::to_string(&metas).unwrap_or_else(|_| "[]".to_string());

    // Pack frames with the SAME codec as the live push path — CRITICAL: carry the
    // encrypted flag (else migrated encrypted payloads become undecryptable), plus
    // trace_id / producer_sub which the live push path drops.
    let fins: Vec<FrameIn> = buf
        .iter()
        .map(|f| FrameIn {
            message_id: f.mid_bytes,
            txn: &f.txn,
            trace_id: f.trace_id,
            producer_sub: f.producer_sub.as_deref(),
            payload: &f.payload,
            encrypted: f.encrypted,
        })
        .collect();
    let packed = pack_frames(&fins);
    let blob = zstd_compress(&packed, opts.zstd_level);
    let blob_b64 = base64::engine::general_purpose::STANDARD.encode(&blob);

    // Segment created_at = the NEWEST frame's created_at, so segment created_at is
    // non-decreasing in seq (frames are in (created_at,id) order) and time-based
    // retention never drops a segment while its newest frame is still in-window.
    let seg_created_at = buf.last().map(|f| f.created_at.clone()).unwrap_or_else(|| NEG_INF.to_string());
    let msg_count = buf.len() as i32;

    client
        .query_one(
            "SELECT queen.seg_migrate_write_segment_v1($1::text::uuid, $2::bigint, \
             $3::text::timestamptz, $4::text::jsonb, $5, $6::int)",
            &[&pid, &seq, &seg_created_at, &metas_json, &blob_b64, &msg_count],
        )
        .await?;
    Ok(())
}

// --------------------------------------------------------------- carry-over
async fn dlq_carryover(pool: &Pool) -> Result<i64, tokio_postgres::Error> {
    let client = pool.get().await.expect("pool");
    let n: i64 = client
        .query_one("SELECT queen.seg_migrate_dlq_v1()", &[])
        .await?
        .get(0);
    Ok(n)
}

async fn mark_queues_segments(pool: &Pool, like: Option<&str>) -> Result<(), tokio_postgres::Error> {
    let client = pool.get().await.expect("pool");
    if let Some(pat) = like {
        client
            .execute("UPDATE queen.queues SET storage='segments' WHERE name LIKE $1", &[&pat])
            .await?;
    } else {
        client
            .execute("UPDATE queen.queues SET storage='segments'", &[])
            .await?;
    }
    Ok(())
}

// --------------------------------------------------------------- validation
// Per-partition count parity: count(messages) vs sum(seg_segments.msg_count).
async fn validate_counts(pool: &Pool, like: Option<&str>) -> Result<(), tokio_postgres::Error> {
    let client = pool.get().await.expect("pool");
    let sql = format!(
        "SELECT q.name, p.name, \
                (SELECT count(*) FROM queen.messages m WHERE m.partition_id = p.id)::bigint AS rows_cnt, \
                COALESCE((SELECT sum(s.msg_count) FROM queen.seg_segments s WHERE s.partition_id = p.id),0)::bigint AS seg_cnt \
         FROM queen.partitions p JOIN queen.queues q ON q.id = p.queue_id{} \
         ORDER BY q.name, p.name",
        if like.is_some() { " WHERE q.name LIKE $1" } else { "" }
    );
    let rows = if let Some(pat) = like {
        client.query(sql.as_str(), &[&pat]).await?
    } else {
        client.query(sql.as_str(), &[]).await?
    };
    println!("\nmigrate: count parity (rows == sum(seg msg_count)):");
    let mut all_ok = true;
    for r in &rows {
        let q: String = r.get(0);
        let p: String = r.get(1);
        let rows_cnt: i64 = r.get(2);
        let seg_cnt: i64 = r.get(3);
        // In non-`all` modes seg_cnt is intentionally a subset (unconsumed/window),
        // so only flag a hard mismatch as FAIL; report both counts regardless.
        let ok = rows_cnt == seg_cnt;
        if !ok {
            all_ok = false;
        }
        println!(
            "  {} {q}/{p}: rows={rows_cnt} seg={seg_cnt}",
            if ok { "OK  " } else { "DIFF" }
        );
    }
    println!(
        "migrate: count parity {}",
        if all_ok { "ALL EXACT" } else { "some partitions differ (expected in unconsumed/window modes)" }
    );
    Ok(())
}

// Pop-parity spot check (mode=all): decode segment seq=1 with the SAME codec and
// compare message_id / txn / payload to queen.messages ORDER BY created_at,id at
// matching ranks — the strongest framing+ordering check.
async fn validate_pop_parity(pool: &Pool, pid: &str) -> Result<(), tokio_postgres::Error> {
    let client = pool.get().await.expect("pool");
    let seg = client
        .query_opt(
            "SELECT blob, msg_count FROM queen.seg_segments \
             WHERE partition_id = $1::text::uuid AND seq = 1",
            &[&pid],
        )
        .await?;
    let Some(seg) = seg else {
        println!("\nmigrate: pop-parity spot check skipped (no segment seq=1)");
        return Ok(());
    };
    let blob: Vec<u8> = seg.get(0);
    let msg_count: i32 = seg.get(1);

    let raw = zstd_decompress(&blob);
    let frames = match unpack_frames(&raw) {
        Some(f) => f,
        None => {
            println!("\nmigrate: pop-parity FAIL — segment seq=1 did not unpack");
            return Ok(());
        }
    };

    let rows = client
        .query(
            "SELECT id::text, transaction_id, payload::text, is_encrypted \
             FROM queen.messages WHERE partition_id = $1::text::uuid \
             ORDER BY created_at, id LIMIT $2::bigint",
            &[&pid, &(msg_count as i64)],
        )
        .await?;

    let mut mismatches = 0;
    let n = frames.len().min(rows.len());
    for i in 0..n {
        let f = &frames[i];
        let want_mid: String = rows[i].get(0);
        let want_txn: String = rows[i].get(1);
        let want_payload: String = rows[i].get(2);
        let want_enc: bool = rows[i].get(3);
        if f.message_id != want_mid
            || f.txn != want_txn
            || f.payload != want_payload.as_bytes()
            || f.encrypted != want_enc
        {
            mismatches += 1;
            if mismatches <= 3 {
                println!(
                    "  MISMATCH rank {i}: mid {}=={} txn {}=={} enc {}=={} payload_eq={}",
                    f.message_id,
                    want_mid,
                    f.txn,
                    want_txn,
                    f.encrypted,
                    want_enc,
                    f.payload == want_payload.as_bytes()
                );
            }
        }
    }
    println!(
        "\nmigrate: pop-parity spot check (partition {pid}, seg seq=1): compared {n} frame(s), {} — {}",
        if mismatches == 0 { "ALL MATCH".to_string() } else { format!("{mismatches} mismatch(es)") },
        if frames.len() as i32 == msg_count { "frame count == msg_count" } else { "FRAME COUNT DRIFT" }
    );
    Ok(())
}
