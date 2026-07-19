use std::time::Duration;

pub struct Config {
    pub port: String,
    pub pg: deadpool_postgres::Config,
    pub pool_size: usize,
    pub stmt_timeout: Duration,
    pub zstd_level: i32,
    // Vegas per-lane: initial / min / max / alpha / beta.
    pub push_init: u64,
    pub push_min: u64,
    pub push_max: u64,
    pub pop_init: u64,
    pub pop_min: u64,
    pub pop_max: u64,
    pub vegas_alpha: f64,
    pub vegas_beta: f64,
    // pop long-poll
    pub pop_default_timeout_ms: u64,
    pub pop_wait_poll_ms: u64,
    // cross-request fusion
    pub fusion_shards: usize,
    pub fusion_frames: usize,
    pub fusion_hold_ms: u64,
    // background retention/eviction sweep cadence (ms)
    pub retention_interval_ms: u64,
}

fn env_str(k: &str, def: &str) -> String {
    std::env::var(k).ok().filter(|v| !v.is_empty()).unwrap_or_else(|| def.to_string())
}
fn env_int(k: &str, def: i64) -> i64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(def)
}
fn env_f64(k: &str, def: f64) -> f64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(def)
}

pub fn load() -> Config {
    let mut pg = deadpool_postgres::Config::new();
    pg.host = Some(env_str("PG_HOST", "localhost"));
    pg.port = Some(env_int("PG_PORT", 5432) as u16);
    pg.user = Some(env_str("PG_USER", "postgres"));
    pg.password = Some(env_str("PG_PASSWORD", "postgres"));
    pg.dbname = Some(env_str("PG_DATABASE", "postgres"));

    Config {
        port: env_str("PORT", "6632"),
        pg,
        pool_size: env_int("DB_POOL_SIZE", 160) as usize,
        stmt_timeout: Duration::from_millis(env_int("QUEEN_STMT_TIMEOUT_MS", 30000) as u64),
        zstd_level: env_int("QUEEN_V2_ZSTD_LEVEL", 3) as i32,
        push_init: env_int("QUEEN_SEG_PUSH_INIT", 16) as u64,
        push_min: env_int("QUEEN_SEG_PUSH_MIN", 4) as u64,
        push_max: env_int("QUEEN_SEG_PUSH_MAX", 64) as u64,
        pop_init: env_int("QUEEN_SEG_POP_INIT", 16) as u64,
        pop_min: env_int("QUEEN_SEG_POP_MIN", 4) as u64,
        pop_max: env_int("QUEEN_SEG_POP_MAX", 64) as u64,
        vegas_alpha: env_f64("QUEEN_VEGAS_ALPHA", 3.0),
        vegas_beta: env_f64("QUEEN_VEGAS_BETA", 6.0),
        pop_default_timeout_ms: env_int("POP_DEFAULT_TIMEOUT_MS", 2000) as u64,
        pop_wait_poll_ms: env_int("POP_WAIT_POLL_MS", 25) as u64,
        fusion_shards: env_int("QUEEN_V2_FUSION_SHARDS", 8).max(1) as usize,
        fusion_frames: env_int("QUEEN_V2_FUSION_FRAMES", 500).max(1) as usize,
        fusion_hold_ms: env_int("QUEEN_V2_FUSION_HOLD_MS", 15).max(1) as u64,
        // RetentionService cadence (C++ parity). retention.js starts the server
        // with RETENTION_INTERVAL=2000; default matches the C++ 5-minute sweep.
        retention_interval_ms: env_int("RETENTION_INTERVAL", 300000).max(1) as u64,
    }
}
