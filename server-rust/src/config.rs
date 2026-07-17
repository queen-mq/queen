use std::time::Duration;

#[derive(Clone)]
pub struct TypePolicy {
    pub preferred: usize,
    pub max_batch: usize,
    pub max_hold: Duration,
    pub max_concurrent: usize,
}

pub struct Config {
    pub port: String,
    pub pg: deadpool_postgres::Config,
    pub push: TypePolicy,
    pub pop: TypePolicy,
    pub ack: TypePolicy,
    pub stmt_timeout: Duration,
    pub global_concurrency: usize,
    pub push_max_parts: usize,
    pub push_weight: usize,
    pub pop_weight: usize,
    pub ack_weight: usize,
    pub partition_flush: Duration,
    pub reconcile_interval: Duration,
    pub reconcile_lookback: i32,
    pub pop_wait_initial_ms: u64,
    pub pop_wait_threshold: u32,
    pub pop_wait_multiplier: u64,
    pub pop_wait_max_ms: u64,
    pub pop_default_timeout_ms: u64,
}

fn env_str(k: &str, def: &str) -> String {
    std::env::var(k).ok().filter(|v| !v.is_empty()).unwrap_or_else(|| def.to_string())
}
fn env_int(k: &str, def: i64) -> i64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(def)
}

pub fn load() -> Config {
    let mut pg = deadpool_postgres::Config::new();
    pg.host = Some(env_str("PG_HOST", "localhost"));
    pg.port = Some(env_int("PG_PORT", 5432) as u16);
    pg.user = Some(env_str("PG_USER", "postgres"));
    pg.password = Some(env_str("PG_PASSWORD", "postgres"));
    pg.dbname = Some(env_str("PG_DATABASE", "postgres"));

    let ms = |k: &str, d: i64| Duration::from_millis(env_int(k, d) as u64);

    Config {
        port: env_str("PORT", "6632"),
        pg,
        push: TypePolicy {
            preferred: env_int("QUEEN_PUSH_PREFERRED_BATCH_SIZE", 50) as usize,
            max_hold: ms("QUEEN_PUSH_MAX_HOLD_MS", 20),
            max_batch: env_int("QUEEN_PUSH_MAX_BATCH_SIZE", 500) as usize,
            max_concurrent: env_int("QUEEN_PUSH_MAX_CONCURRENT", 24) as usize,
        },
        pop: TypePolicy {
            preferred: env_int("QUEEN_POP_PREFERRED_BATCH_SIZE", 20) as usize,
            max_hold: ms("QUEEN_POP_MAX_HOLD_MS", 5),
            max_batch: env_int("QUEEN_POP_MAX_BATCH_SIZE", 500) as usize,
            max_concurrent: env_int("QUEEN_POP_MAX_CONCURRENT", 40) as usize,
        },
        ack: TypePolicy {
            preferred: env_int("QUEEN_ACK_PREFERRED_BATCH_SIZE", 50) as usize,
            max_hold: ms("QUEEN_ACK_MAX_HOLD_MS", 20),
            max_batch: env_int("QUEEN_ACK_MAX_BATCH_SIZE", 500) as usize,
            max_concurrent: env_int("QUEEN_ACK_MAX_CONCURRENT", 16) as usize,
        },
        stmt_timeout: ms("QUEEN_STMT_TIMEOUT_MS", 30000),
        global_concurrency: env_int("QUEEN_GLOBAL_CONCURRENCY", 72) as usize,
        push_max_parts: env_int("QUEEN_PUSH_MAX_PARTITIONS_PER_BATCH", 8) as usize,
        push_weight: env_int("QUEEN_PUSH_WEIGHT", 1) as usize,
        pop_weight: env_int("QUEEN_POP_WEIGHT", 1) as usize,
        ack_weight: env_int("QUEEN_ACK_WEIGHT", 1) as usize,
        partition_flush: ms("PARTITION_LOOKUP_FLUSH_MS", 100),
        reconcile_interval: ms("RECONCILE_INTERVAL_MS", 2000),
        reconcile_lookback: env_int("RECONCILE_LOOKBACK_SECONDS", 30) as i32,
        pop_wait_initial_ms: env_int("POP_WAIT_INITIAL_INTERVAL_MS", 10) as u64,
        pop_wait_threshold: env_int("POP_WAIT_BACKOFF_THRESHOLD", 5) as u32,
        pop_wait_multiplier: env_int("POP_WAIT_BACKOFF_MULTIPLIER", 2) as u64,
        pop_wait_max_ms: env_int("POP_WAIT_MAX_INTERVAL_MS", 100) as u64,
        pop_default_timeout_ms: env_int("POP_DEFAULT_TIMEOUT_MS", 2000) as u64,
    }
}
