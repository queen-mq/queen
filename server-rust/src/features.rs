use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::Duration;

use deadpool_postgres::Pool;

/// The always-on "machinery" the C++ broker runs on every op and in the
/// background — replicated so the Rust/C++ comparison is apples-to-apples:
///   * per-queue metric attribution (zero-copy byte-scan for queueName)
///   * per-message pop lag computation (zero-copy byte-scan of createdAt)
///   * per-push queue-config cache lookup (+ PG fetch on miss)
///   * background metrics flush to Postgres
///   * background retention (completed-message cleanup)
/// Enabled with QUEEN_FULL_FEATURES=1.
#[derive(Default)]
struct QCounters {
    push_msgs: u64,
    pop_msgs: u64,
    lag_sum_ms: u64,
    lag_max_ms: u64,
}

pub struct Features {
    pub enabled: bool,
    pool: Pool,
    aux_pool: Pool,
    qmetrics: Mutex<HashMap<String, QCounters>>,
    qcache: Mutex<HashSet<String>>,
}

impl Features {
    /// `pool` is the hot-path pool (config lookups). `aux_pool` is a dedicated
    /// sidecar pool for background retention + metrics flush, so retention never
    /// starves behind hot-path connections (the C++ SIDECAR_POOL_SIZE analogue).
    pub fn new(enabled: bool, pool: Pool, aux_pool: Pool) -> Features {
        Features {
            enabled,
            pool,
            aux_pool,
            qmetrics: Mutex::new(HashMap::new()),
            qcache: Mutex::new(HashSet::new()),
        }
    }

    /// Per-push queue-config lookup: cache hit is a lock+contains; miss does a
    /// real PG fetch of the queue row (the C++ get_or_fetch_queue_config path).
    pub async fn ensure_queue_config(&self, queue: &str) {
        {
            let c = self.qcache.lock().unwrap();
            if c.contains(queue) {
                return;
            }
        }
        if let Ok(client) = self.pool.get().await {
            if let Ok(stmt) = client
                .prepare_cached("SELECT encryption_enabled, retention_enabled, lease_time FROM queen.queues WHERE name = $1")
                .await
            {
                let _ = client.query_opt(&stmt, &[&queue]).await;
            }
        }
        self.qcache.lock().unwrap().insert(queue.to_string());
    }

    /// Per-queue push attribution — ZERO-COPY: byte-scan each result element for
    /// the "queueName" value slice and tally, no serde parse, no per-message
    /// String allocation (only allocates a key the first time a queue is seen).
    pub fn attribute_push(&self, elems: &[Vec<u8>]) {
        let mut m = self.qmetrics.lock().unwrap();
        for e in elems {
            if let Some(qn) = find_str_slice(e, b"\"queueName\":") {
                if let Some(c) = m.get_mut(qn) {
                    c.push_msgs += 1;
                } else {
                    m.entry(qn.to_string()).or_default().push_msgs += 1;
                }
            }
        }
    }

    /// Per-message pop attribution + lag — ZERO-COPY: single forward byte-scan of
    /// the result for each "createdAt" value, parse the timestamp in place and
    /// accumulate count/lag. No serde_json::Value, no allocations, one lock.
    pub fn attribute_pop(&self, queue: &str, result: &[u8]) {
        const KEY: &[u8] = b"\"createdAt\":\"";
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let (mut cnt, mut lag_sum, mut lag_max) = (0u64, 0u64, 0u64);
        let n = result.len();
        let mut i = 0;
        while i + KEY.len() <= n {
            if &result[i..i + KEY.len()] == KEY {
                let start = i + KEY.len();
                let mut j = start;
                while j < n && result[j] != b'"' {
                    j += 1;
                }
                if let Some(ms) = parse_iso_ms(&result[start..j]) {
                    let lag = now_ms.saturating_sub(ms);
                    lag_sum += lag;
                    if lag > lag_max {
                        lag_max = lag;
                    }
                }
                cnt += 1;
                i = j + 1;
            } else {
                i += 1;
            }
        }
        if cnt > 0 {
            let mut m = self.qmetrics.lock().unwrap();
            let c = m.entry(queue.to_string()).or_default();
            c.pop_msgs += cnt;
            c.lag_sum_ms += lag_sum;
            if lag_max > c.lag_max_ms {
                c.lag_max_ms = lag_max;
            }
        }
    }

    fn snapshot(&self) -> Vec<(String, u64, u64, u64)> {
        let m = self.qmetrics.lock().unwrap();
        m.iter()
            .map(|(k, v)| (k.clone(), v.push_msgs, v.pop_msgs, v.lag_max_ms))
            .collect()
    }
}

pub fn start_background(f: std::sync::Arc<Features>, stmt_to: Duration) {
    if !f.enabled {
        return;
    }
    // Metrics flush to PG (representative of worker_metrics/queue_lag flush).
    {
        let f = f.clone();
        tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(1));
            loop {
                t.tick().await;
                let snap = f.snapshot();
                if snap.is_empty() {
                    continue;
                }
                if let Ok(client) = f.aux_pool.get().await {
                    for (q, pu, po, lag) in snap {
                        let _ = tokio::time::timeout(
                            stmt_to,
                            client.execute(
                                "INSERT INTO queen.queue_lag_metrics (queue_name, push_message_count, pop_count, max_lag_ms, minute_bucket) \
                                 VALUES ($1,$2,$3,$4, date_trunc('minute', now())) ON CONFLICT DO NOTHING",
                                &[&q, &(pu as i64), &(po as i64), &(lag as i64)],
                            ),
                        )
                        .await;
                    }
                }
            }
        });
    }
    // Parallel completed-retention — faithful port of the C++ RetentionService:
    // one cycle lock, list eligible partitions, then K workers delete completed
    // messages per-partition in LIMIT batches via a shared work-stealing cursor.
    {
        let f = f.clone();
        let parallelism = env_int("RETENTION_PARALLELISM", 1).max(1) as usize;
        let batch = env_int("RETENTION_BATCH_SIZE", 1000).max(1);
        let interval_ms = env_int("RETENTION_INTERVAL", 300000).max(200) as u64;
        tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_millis(interval_ms));
            loop {
                t.tick().await;
                retention_cycle(&f, parallelism, batch, stmt_to).await;
            }
        });
    }
}

const LIST_PARTITIONS_SQL: &str = "\
    SELECT p.id::text AS partition_id, \
           (NOW() - (q.completed_retention_seconds || ' seconds')::INTERVAL)::text AS cutoff, \
           MIN(pc.last_consumed_id::text) AS safe_consumed_id \
    FROM queen.partitions p \
    JOIN queen.queues q ON p.queue_id = q.id \
    JOIN queen.partition_consumers pc ON pc.partition_id = p.id \
    WHERE q.retention_enabled = true AND q.completed_retention_seconds > 0 \
      AND p.created_at < NOW() - (q.completed_retention_seconds || ' seconds')::INTERVAL \
      AND EXISTS (SELECT 1 FROM queen.messages m WHERE m.partition_id = p.id \
                  AND m.created_at < NOW() - (q.completed_retention_seconds || ' seconds')::INTERVAL) \
    GROUP BY p.id, q.completed_retention_seconds \
    HAVING MIN(pc.last_consumed_id::text) <> '00000000-0000-0000-0000-000000000000'";

// NB: casts are $n::text::uuid / ::text::timestamptz so tokio-postgres (binary,
// strict type inference) describes the params as TEXT — passing &String would
// otherwise fail against an inferred uuid/timestamptz type.
const DELETE_BATCH_SQL: &str = "\
    DELETE FROM queen.messages WHERE id IN ( \
        SELECT id FROM queen.messages \
        WHERE partition_id = $1::text::uuid AND id <= $2::text::uuid AND created_at < $3::text::timestamptz \
        LIMIT $4)";

async fn retention_cycle(f: &Features, parallelism: usize, batch: i64, stmt_to: Duration) {
    let started = std::time::Instant::now();
    let list_client = match f.aux_pool.get().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[retention] no list conn: {e}");
            return;
        }
    };
    let jobs: Vec<(String, String, String)> = match list_client.query(LIST_PARTITIONS_SQL, &[]).await {
        Ok(rows) => rows
            .iter()
            .map(|r| {
                (
                    r.get::<_, String>("partition_id"),
                    r.get::<_, String>("cutoff"),
                    r.get::<_, String>("safe_consumed_id"),
                )
            })
            .collect(),
        Err(e) => {
            eprintln!("[retention] list failed: {e}");
            return;
        }
    };
    drop(list_client);
    if jobs.is_empty() {
        return;
    }

    let n_parts = jobs.len();
    // Pre-acquire one dedicated connection per worker so K deletes truly run in
    // parallel (don't rely on lazy pool.get() racing inside the workers).
    let mut conns = Vec::with_capacity(parallelism);
    for _ in 0..parallelism {
        match f.aux_pool.get().await {
            Ok(c) => conns.push(c),
            Err(_) => break,
        }
    }
    let n_workers = conns.len();
    eprintln!("[retention] cycle start: {n_parts} parts eligible, {n_workers} worker conns acquired");

    let jobs = std::sync::Arc::new(jobs);
    let cursor = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let deleted = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut handles = Vec::with_capacity(n_workers);
    for client in conns {
        let jobs = jobs.clone();
        let cursor = cursor.clone();
        let deleted = deleted.clone();
        handles.push(tokio::spawn(async move {
            use std::sync::atomic::Ordering::Relaxed;
            loop {
                let i = cursor.fetch_add(1, Relaxed);
                if i >= jobs.len() {
                    break;
                }
                let (pid, cutoff, safe) = &jobs[i];
                loop {
                    match tokio::time::timeout(
                        stmt_to,
                        client.execute(DELETE_BATCH_SQL, &[pid, safe, cutoff, &batch]),
                    )
                    .await
                    {
                        Ok(Ok(n)) => {
                            deleted.fetch_add(n, Relaxed);
                            if (n as i64) < batch {
                                break;
                            }
                        }
                        Ok(Err(e)) => {
                            eprintln!("[retention] delete error: {e}");
                            break;
                        }
                        Err(_) => break,
                    }
                }
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    let d = deleted.load(std::sync::atomic::Ordering::Relaxed);
    eprintln!(
        "[retention] cycle done: {n_workers} workers deleted {d} rows in {:.1}s",
        started.elapsed().as_secs_f64()
    );
}

fn env_int(k: &str, def: i64) -> i64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(def)
}

/// Borrow the string value that follows `key` (e.g. b"\"queueName\":") as a
/// &str slice into `raw` — no allocation.
fn find_str_slice<'a>(raw: &'a [u8], key: &[u8]) -> Option<&'a str> {
    let pos = raw.windows(key.len()).position(|w| w == key)?;
    let mut i = pos + key.len();
    while i < raw.len() && (raw[i] == b' ' || raw[i] == b'"') {
        i += 1;
    }
    let start = i;
    while i < raw.len() && raw[i] != b'"' {
        i += 1;
    }
    if i > start {
        std::str::from_utf8(&raw[start..i]).ok()
    } else {
        None
    }
}

fn parse_iso_ms(b: &[u8]) -> Option<u64> {
    // Cheap parse of "YYYY-MM-DDTHH:MM:SS(.ffffff)?Z?" bytes to epoch ms.
    if b.len() < 19 {
        return None;
    }
    let num = |a: usize, n: usize| -> i64 {
        let mut v = 0i64;
        for k in a..a + n {
            if k < b.len() && b[k].is_ascii_digit() {
                v = v * 10 + (b[k] - b'0') as i64;
            }
        }
        v
    };
    let (y, mo, d) = (num(0, 4), num(5, 2), num(8, 2));
    let (h, mi, se) = (num(11, 2), num(14, 2), num(17, 2));
    // days since epoch (civil calendar, Howard Hinnant's algorithm)
    let y2 = if mo <= 2 { y - 1 } else { y };
    let era = if y2 >= 0 { y2 } else { y2 - 399 } / 400;
    let yoe = y2 - era * 400;
    let doy = (153 * (if mo > 2 { mo - 3 } else { mo + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    Some(((days * 86400 + h * 3600 + mi * 60 + se) * 1000) as u64)
}
