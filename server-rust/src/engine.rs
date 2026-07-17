use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use deadpool_postgres::Pool;
use serde::Deserialize;
use serde_json::value::RawValue;
use tokio::sync::{mpsc, oneshot};

use crate::config::TypePolicy;
use crate::metrics::OpMetrics;
use crate::util::{build_merged, extract_leading_idx};

pub const LANE_PUSH: usize = 0;
pub const LANE_POP: usize = 1;
pub const LANE_ACK: usize = 2;

pub type JobResult = Result<Vec<Vec<u8>>, String>;

pub struct Job {
    pub items: Vec<Vec<u8>>,
    pub parts: Vec<String>,
    pub enqueued: Instant,
    pub resp: oneshot::Sender<JobResult>,
}

pub type OnPu = Arc<dyn Fn(Vec<u8>) + Send + Sync>;

pub struct LaneSpec {
    pub name: &'static str,
    pub sql: String,
    pub policy: TypePolicy,
    pub weight: usize,
    pub gate: bool,
    pub max_parts: usize,
    pub push_object: bool,
    pub metrics: Arc<OpMetrics>,
    pub on_pu: Option<OnPu>,
}

struct Lane {
    spec: LaneSpec,
    pending: VecDeque<Job>,
    inflight: HashMap<String, i32>,
    active: usize,
}

enum Msg {
    Submit(usize, Job),
    Done(usize, Vec<String>),
}

#[derive(Clone)]
pub struct Engine {
    tx: mpsc::Sender<Msg>,
}

impl Engine {
    pub fn new(specs: Vec<LaneSpec>, pool: Pool, global: usize, stmt_to: Duration) -> Engine {
        let (tx, rx) = mpsc::channel(32768);
        let tx2 = tx.clone();
        tokio::spawn(run(rx, tx2, specs, pool, global.max(1), stmt_to));
        Engine { tx }
    }

    pub async fn submit(&self, lane: usize, items: Vec<Vec<u8>>, parts: Vec<String>) -> JobResult {
        let (rtx, rrx) = oneshot::channel();
        let job = Job { items, parts, enqueued: Instant::now(), resp: rtx };
        if self.tx.send(Msg::Submit(lane, job)).await.is_err() {
            return Err("engine closed".into());
        }
        rrx.await.unwrap_or_else(|_| Err("engine dropped".into()))
    }
}

async fn run(
    mut rx: mpsc::Receiver<Msg>,
    sched_tx: mpsc::Sender<Msg>,
    specs: Vec<LaneSpec>,
    pool: Pool,
    global: usize,
    stmt_to: Duration,
) {
    let mut lanes: Vec<Lane> = specs
        .into_iter()
        .map(|s| Lane { spec: s, pending: VecDeque::new(), inflight: HashMap::new(), active: 0 })
        .collect();
    let mut global_active = 0usize;
    let mut rr = 0usize;
    let n = lanes.len();

    loop {
        // Earliest hold deadline across lanes (so we wake to fire held batches).
        let mut next: Option<Instant> = None;
        for l in &lanes {
            if let Some(front) = l.pending.front() {
                if l.pending.len() < l.spec.policy.preferred {
                    let d = front.enqueued + l.spec.policy.max_hold;
                    next = Some(match next { Some(x) => x.min(d), None => d });
                }
            }
        }

        tokio::select! {
            maybe = rx.recv() => {
                match maybe {
                    Some(Msg::Submit(l, j)) => lanes[l].pending.push_back(j),
                    Some(Msg::Done(l, reserved)) => {
                        lanes[l].active -= 1;
                        global_active -= 1;
                        for p in reserved {
                            if let Some(c) = lanes[l].inflight.get_mut(&p) {
                                *c -= 1;
                                if *c <= 0 { lanes[l].inflight.remove(&p); }
                            }
                        }
                    }
                    None => return,
                }
            }
            _ = sleep_until_opt(next) => {}
        }

        dispatch(&mut lanes, n, &mut global_active, &mut rr, global, &pool, &sched_tx, stmt_to);
    }
}

async fn sleep_until_opt(d: Option<Instant>) {
    match d {
        Some(t) => {
            let now = Instant::now();
            // Floor at 1ms: a held-but-unfireable job (e.g. gate-blocked) has a
            // deadline in the past; without a floor sleep_until returns instantly
            // and the scheduler busy-spins a core, starving every lane.
            let dur = t.checked_duration_since(now).unwrap_or(Duration::ZERO);
            tokio::time::sleep(dur.max(Duration::from_millis(1))).await;
        }
        None => std::future::pending::<()>().await,
    }
}

#[allow(clippy::too_many_arguments)]
fn dispatch(
    lanes: &mut [Lane],
    n: usize,
    global_active: &mut usize,
    rr: &mut usize,
    global: usize,
    pool: &Pool,
    sched_tx: &mpsc::Sender<Msg>,
    stmt_to: Duration,
) {
    loop {
        if *global_active >= global {
            break;
        }
        let mut progress = false;
        for k in 0..n {
            if *global_active >= global {
                break;
            }
            let li = (*rr + k) % n;
            let weight = lanes[li].spec.weight.max(1);
            for _ in 0..weight {
                if *global_active >= global || lanes[li].active >= lanes[li].spec.policy.max_concurrent {
                    break;
                }
                if lanes[li].pending.is_empty() {
                    break;
                }
                // Hold for fusion: fire only at Preferred OR after MaxHold.
                if lanes[li].pending.len() < lanes[li].spec.policy.preferred {
                    let age = lanes[li].pending.front().unwrap().enqueued.elapsed();
                    if age < lanes[li].spec.policy.max_hold {
                        break;
                    }
                }
                let (batch, reserved) = select_batch(&mut lanes[li]);
                if batch.is_empty() {
                    break;
                }
                lanes[li].active += 1;
                *global_active += 1;
                let sql = lanes[li].spec.sql.clone();
                let push_object = lanes[li].spec.push_object;
                let metrics = lanes[li].spec.metrics.clone();
                let on_pu = lanes[li].spec.on_pu.clone();
                let pool2 = pool.clone();
                let tx2 = sched_tx.clone();
                tokio::spawn(fire(pool2, sql, batch, push_object, metrics, on_pu, tx2, li, reserved, stmt_to));
                progress = true;
            }
        }
        *rr = (*rr + 1) % n;
        if !progress {
            break;
        }
    }
}

fn select_batch(lane: &mut Lane) -> (Vec<Job>, Vec<String>) {
    let limit = lane.spec.policy.preferred.max(1).min(lane.spec.policy.max_batch);
    if !lane.spec.gate {
        let take = lane.pending.len().min(limit);
        let mut batch = Vec::with_capacity(take);
        for _ in 0..take {
            batch.push(lane.pending.pop_front().unwrap());
        }
        return (batch, Vec::new());
    }
    let max_parts = if lane.spec.max_parts < 1 { 8 } else { lane.spec.max_parts };
    let mut batch: Vec<Job> = Vec::new();
    let mut remaining: VecDeque<Job> = VecDeque::new();
    let mut res_set: HashMap<String, ()> = HashMap::new();
    while let Some(j) = lane.pending.pop_front() {
        if batch.len() >= limit {
            remaining.push_back(j);
            continue;
        }
        let conflict = j.parts.iter().any(|p| lane.inflight.contains_key(p));
        if conflict {
            remaining.push_back(j);
            continue;
        }
        let new_parts = j.parts.iter().filter(|p| !res_set.contains_key(*p)).count();
        if !res_set.is_empty() && res_set.len() + new_parts > max_parts {
            remaining.push_back(j);
            continue;
        }
        for p in &j.parts {
            res_set.insert(p.clone(), ());
        }
        batch.push(j);
    }
    lane.pending = remaining;
    for p in res_set.keys() {
        *lane.inflight.entry(p.clone()).or_insert(0) += 1;
    }
    let reserved: Vec<String> = res_set.into_keys().collect();
    (batch, reserved)
}

#[allow(clippy::too_many_arguments)]
async fn fire(
    pool: Pool,
    sql: String,
    batch: Vec<Job>,
    push_object: bool,
    metrics: Arc<OpMetrics>,
    on_pu: Option<OnPu>,
    sched_tx: mpsc::Sender<Msg>,
    lane_idx: usize,
    reserved: Vec<String>,
    stmt_to: Duration,
) {
    let (merged, ranges) = build_merged(&batch);
    let items: usize = ranges.iter().map(|r| r.1).sum();
    let start = Instant::now();
    let res = run_query(&pool, &sql, &merged, stmt_to).await;
    let rtt = start.elapsed();
    match res {
        Ok(raw) => {
            metrics.record_batch(items, true, rtt);
            demux(raw, batch, &ranges, push_object, on_pu);
        }
        Err(e) => {
            metrics.record_batch(items, false, rtt);
            for j in batch {
                let _ = j.resp.send(Err(e.clone()));
            }
        }
    }
    let _ = sched_tx.send(Msg::Done(lane_idx, reserved)).await;
}

async fn run_query(pool: &Pool, sql: &str, merged: &str, stmt_to: Duration) -> Result<Vec<u8>, String> {
    let client = pool.get().await.map_err(|e| e.to_string())?;
    let stmt = client.prepare_cached(sql).await.map_err(|e| e.to_string())?;
    let row = tokio::time::timeout(stmt_to, client.query_one(&stmt, &[&merged]))
        .await
        .map_err(|_| "statement timeout".to_string())?
        .map_err(|e| e.to_string())?;
    let val: String = row.get(0);
    Ok(val.into_bytes())
}

#[derive(Deserialize)]
struct PushRes<'a> {
    #[serde(borrow)]
    items: Vec<&'a RawValue>,
    #[serde(borrow, default)]
    partition_updates: Option<&'a RawValue>,
}

fn demux(raw: Vec<u8>, batch: Vec<Job>, ranges: &[(usize, usize)], push_object: bool, on_pu: Option<OnPu>) {
    let arr: Vec<&RawValue>;
    if push_object {
        match serde_json::from_slice::<PushRes>(&raw) {
            Ok(pr) => {
                if let (Some(pu), Some(f)) = (pr.partition_updates, on_pu.as_ref()) {
                    let b = pu.get().as_bytes();
                    if b != b"[]" {
                        f(b.to_vec());
                    }
                }
                arr = pr.items;
            }
            Err(e) => {
                let msg = format!("push result parse: {e}");
                for j in batch {
                    let _ = j.resp.send(Err(msg.clone()));
                }
                return;
            }
        }
    } else {
        match serde_json::from_slice::<Vec<&RawValue>>(&raw) {
            Ok(v) => arr = v,
            Err(e) => {
                let msg = format!("result parse: {e}");
                for j in batch {
                    let _ = j.resp.send(Err(msg.clone()));
                }
                return;
            }
        }
    }

    let mut by_idx: HashMap<i64, &[u8]> = HashMap::with_capacity(arr.len());
    for el in &arr {
        let bytes = el.get().as_bytes();
        let id = extract_leading_idx(bytes);
        if id >= 0 {
            by_idx.insert(id, bytes);
        }
    }
    for (j, &(start, count)) in batch.into_iter().zip(ranges.iter()) {
        let mut elems: Vec<Vec<u8>> = Vec::with_capacity(count);
        for k in start..start + count {
            if let Some(b) = by_idx.get(&(k as i64)) {
                elems.push(b.to_vec());
            }
        }
        let _ = j.resp.send(Ok(elems));
    }
}
