use deadpool_postgres::{Pool, PoolConfig, Runtime};
use tokio_postgres::NoTls;

use crate::config::Config;

pub fn create_pool(cfg: &Config) -> Pool {
    let mut pg = cfg.pg.clone();
    pg.pool = Some(PoolConfig::new(cfg.pool_size));
    pg.create_pool(Some(Runtime::Tokio1), NoTls)
        .expect("failed to create pool")
}

// Push one segment. blob is the raw zstd bytes, sent as a BINARY bytea param
// (tokio-postgres maps &[u8] -> bytea). Returns the SP result JSON as text.
pub async fn push_segment(
    client: &deadpool_postgres::Client,
    queue: &str,
    partition: &str,
    metas_json: &str,
    blob: &[u8],
    msg_count: i32,
) -> Result<String, tokio_postgres::Error> {
    // $3::text::jsonb forces the metas param to be sent as TEXT (the function arg
    // is jsonb, which would otherwise make tokio-postgres reject a &str bind).
    let stmt =
        "SELECT (queen.seg_push_segment_wire_v1($1, $2, $3::text::jsonb, $4, $5::int))::text";
    let row = client
        .query_one(stmt, &[&queue, &partition, &metas_json, &blob, &msg_count])
        .await?;
    Ok(row.get(0))
}

// Wildcard pop (+auto-ack when auto_ack). Returns the SP result JSON as text.
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
) -> Result<String, tokio_postgres::Error> {
    let stmt = "SELECT (queen.seg_pop_wildcard_wire_v1($1, $2, $3::int, $4::int, $5, $6::bool, $7::int, $8, $9))::text";
    let sub_mode = "all";
    let sub_from = "";
    let row = client
        .query_one(
            stmt,
            &[&queue, &group, &budget, &lease_seconds, &worker, &auto_ack,
              &max_partitions, &sub_mode, &sub_from],
        )
        .await?;
    Ok(row.get(0))
}
