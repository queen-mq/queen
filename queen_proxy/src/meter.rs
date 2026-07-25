//! Usage metering. OWNER: Agent D.
//! Contract (M1–M6, PLAN §4): meter post-response from per-item statuses —
//! never charge `error`, never double-charge `duplicate`, `buffered` counts
//! as accepted; exempt 5xx and scope-403s. In-memory per-(cluster, op, minute)
//! aggregates, flushed to queen_proxy.usage_minutes every cfg.meter_flush_ms,
//! spooled to disk (spool.rs) when pxdb is unreachable.

use uuid::Uuid;

use crate::state::OpClass;

#[derive(Clone, Debug)]
pub struct Sample {
    pub cluster_id: Uuid,
    pub op: OpClass,
    pub reqs: u64,
    pub msgs: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

pub struct Meter {
    _flush_ms: u64,
}

impl Meter {
    pub fn new(cfg: &crate::config::Config) -> Meter {
        Meter { _flush_ms: cfg.meter_flush_ms }
    }

    pub fn record(&self, s: Sample) {
        // Agent D: sharded aggregation; skeleton logs at debug for smoke tests.
        tracing::debug!(
            cluster = %s.cluster_id, op = s.op.as_str(), reqs = s.reqs, msgs = s.msgs,
            bytes_in = s.bytes_in, bytes_out = s.bytes_out, "meter"
        );
    }

    pub fn spawn_flush(self: &std::sync::Arc<Self>, _db: Option<deadpool_postgres::Pool>) {
        // Agent D: minute rollup writer + spool drain on recovery.
    }
}
