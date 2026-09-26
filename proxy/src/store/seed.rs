//! First boot of the single binary: the layout version, the default plans
//! (with the kv/timers/ephemeral families on) and this cell, written into the
//! KV layout once. Every node runs it at boot; the unique
//! index of each row (`putIfAbsent` + `required`) makes exactly one write win
//! and the rest no-ops, so it is safe to race.

use serde_json::json;
use uuid::Uuid;

use super::kv::{self, Expect, KvBackend, KvError, Ttl};
use super::schema::{self, ns, CellDoc, MetaDoc, PlanDoc};

fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// The slug of the cell a single-binary node serves (it is its own cell).
pub const SELF_CELL: &str = "local";

/// The default plan rows and their features.
fn plans(now: i64) -> Vec<PlanDoc> {
    let fam = |extra: &[(&str, bool)]| {
        let mut f = json!({"kv": true, "timers": true, "ephemeral": true});
        for (k, v) in extra {
            f[*k] = json!(v);
        }
        f
    };
    let p = |code: &str, class: &str, rps, rb, mps, mb, q, ppq, parked, payload, batch, retained: i64, ret_days: i64, features| PlanDoc {
        id: Uuid::new_v4(),
        code: code.into(),
        cell_class: class.into(),
        max_req_per_sec: Some(rps),
        req_burst: Some(rb),
        max_msgs_per_sec: Some(mps),
        msgs_burst: Some(mb),
        max_queues: Some(q),
        max_partitions_per_queue: Some(ppq),
        max_parked_pops: Some(parked),
        max_payload_bytes: Some(payload),
        max_batch_items: Some(batch),
        max_retained_bytes: Some(retained),
        max_retention_seconds: Some(ret_days * 86_400),
        monthly_msgs_quota: None,
        features,
        created_at_us: now,
    };
    const MIB: i64 = 1024 * 1024;
    vec![
        p("free", "shared", 5, 25, 20, 100, 20, 8, 50, 256 * 1024, 1000, 1024 * MIB, 7, fam(&[])),
        p("dev", "shared", 10, 50, 40, 200, 50, 16, 150, 512 * 1024, 2000, 3 * 1024 * MIB, 14, fam(&[])),
        p("pro", "shared", 50, 200, 200, 800, 200, 32, 1000, MIB, 5000, 20 * 1024 * MIB, 30, fam(&[("streams", true), ("traces", true)])),
        p("dedicated-s", "dedicated", 200, 800, 600, 2000, 1000, 64, 5000, 4 * MIB, 10000, 200 * 1024 * MIB, 90, fam(&[("streams", true), ("traces", true)])),
    ]
}

/// Seed the layout version, the default plans and this cell. Idempotent and
/// safe to run on every node at once. `Ok(true)` when anything was written.
pub async fn seed(kv: &dyn KvBackend) -> Result<bool, KvError> {
    let now = now_us();
    let mut wrote = false;
    let meta = MetaDoc { version: schema::SCHEMA_VERSION };
    wrote |= once(kv, vec![kv::put_op(ns::META, &schema::key("schema"), &meta, Expect::Absent, Ttl::Forever, true)]).await?;
    for plan in plans(now) {
        wrote |= once(
            kv,
            vec![
                kv::put_op(ns::PLAN_CODE, &schema::key(&plan.code), &plan.id, Expect::Absent, Ttl::Forever, true),
                kv::put_op(ns::PLANS, &schema::key(plan.id), &plan, Expect::Absent, Ttl::Forever, true),
            ],
        )
        .await?;
    }
    let cell = CellDoc {
        id: Uuid::new_v4(),
        slug: SELF_CELL.into(),
        region: "local".into(),
        // Never dialled: a single-binary node relays in-process.
        base_url: "inprocess://self".into(),
        class: "shared".into(),
        capacity_slots: 0,
        used_slots: 0,
        broker_version: None,
        status: "active".into(),
        cell_secret: None,
        created_at_us: now,
    };
    wrote |= once(
        kv,
        vec![
            kv::put_op(ns::CELL_SLUG, &schema::key(SELF_CELL), &cell.id, Expect::Absent, Ttl::Forever, true),
            kv::put_op(ns::CELLS, &schema::key(cell.id), &cell, Expect::Absent, Ttl::Forever, true),
        ],
    )
    .await?;
    Ok(wrote)
}

/// Run a batch whose first op is a unique index claim: a lost claim means
/// another node (or an earlier boot) already wrote it.
async fn once(kv: &dyn KvBackend, ops: Vec<serde_json::Value>) -> Result<bool, KvError> {
    match kv::write(kv, ops).await {
        Ok(_) => Ok(true),
        Err(KvError::Precondition { .. }) => Ok(false),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memkv::MemKv;

    #[tokio::test]
    async fn seeding_twice_writes_once() {
        let m = MemKv::new();
        assert!(seed(&m).await.unwrap());
        assert!(!seed(&m).await.unwrap());
        assert_eq!(m.keys(ns::PLANS).len(), 4);
        assert_eq!(m.keys(ns::PLAN_CODE).len(), 4);
        assert_eq!(m.keys(ns::CELLS).len(), 1);
        let free: Option<kv::Doc<Uuid>> = kv::get(&m, ns::PLAN_CODE, &schema::key("free")).await.unwrap();
        let plan: Option<kv::Doc<PlanDoc>> = kv::get(&m, ns::PLANS, &schema::key(free.unwrap().value)).await.unwrap();
        assert_eq!(plan.unwrap().value.features["kv"], true);
    }
}
