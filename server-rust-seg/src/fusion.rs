use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use deadpool_postgres::Pool;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::db;
use crate::frames::{pack_frames, uuid_bytes_to_string, zstd_compress, FrameIn};
use crate::metrics::Metrics;
use crate::vegas::Vegas;

// Per push-request state. The handler parks on `notify` until every segment its
// frames landed in has committed (pending -> 0).
pub struct ItemResult {
    pub message_id: String,
    pub txn: String,
    pub queue: String,
    pub status: &'static str, // "queued" | "error"
}

pub struct PushState {
    pub results: Mutex<Vec<ItemResult>>,
    pub pending: AtomicUsize,
    pub done: Mutex<Option<oneshot::Sender<()>>>,
}

pub struct OwnedFrame {
    pub message_id: [u8; 16],
    pub txn: String,
    pub payload: Vec<u8>,
}

pub struct Contributor {
    pub state: Arc<PushState>,
    pub item_indices: Vec<usize>,
}

struct FusionGroup {
    queue: String,
    partition: String,
    frames: Vec<OwnedFrame>,
    contributors: Vec<Contributor>,
    first_at: Instant,
}

pub struct AddMsg {
    pub queue: String,
    pub partition: String,
    pub frames: Vec<OwnedFrame>,
    pub contrib: Contributor,
}

pub struct Fusion {
    senders: Vec<mpsc::UnboundedSender<AddMsg>>,
}

struct FlushCtx {
    pool: Pool,
    vegas: Arc<Vegas>,
    metrics: Arc<Metrics>,
    zstd_level: i32,
    stmt_timeout: Duration,
}

impl Fusion {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shards: usize,
        pool: Pool,
        vegas: Arc<Vegas>,
        metrics: Arc<Metrics>,
        zstd_level: i32,
        fusion_frames: usize,
        hold_ms: u64,
        stmt_timeout: Duration,
    ) -> Arc<Fusion> {
        let mut senders = Vec::with_capacity(shards);
        for _ in 0..shards {
            let (tx, rx) = mpsc::unbounded_channel::<AddMsg>();
            senders.push(tx);
            let ctx = Arc::new(FlushCtx {
                pool: pool.clone(),
                vegas: vegas.clone(),
                metrics: metrics.clone(),
                zstd_level,
                stmt_timeout,
            });
            tokio::spawn(shard_loop(rx, ctx, fusion_frames, hold_ms));
        }
        Arc::new(Fusion { senders })
    }

    pub fn submit(&self, msg: AddMsg) {
        let mut h: u64 = 1469598103934665603;
        for b in msg.queue.as_bytes().iter().chain(msg.partition.as_bytes()) {
            h ^= *b as u64;
            h = h.wrapping_mul(1099511628211);
        }
        let idx = (h as usize) % self.senders.len();
        let _ = self.senders[idx].send(msg);
    }
}

async fn shard_loop(
    mut rx: mpsc::UnboundedReceiver<AddMsg>,
    ctx: Arc<FlushCtx>,
    fusion_frames: usize,
    hold_ms: u64,
) {
    let mut groups: HashMap<String, FusionGroup> = HashMap::new();
    let mut tick = tokio::time::interval(Duration::from_millis(hold_ms.max(1)));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            maybe = rx.recv() => {
                let Some(msg) = maybe else { break; };
                let key = format!("{}\x1f{}", msg.queue, msg.partition);
                let g = groups.entry(key.clone()).or_insert_with(|| FusionGroup {
                    queue: msg.queue.clone(),
                    partition: msg.partition.clone(),
                    frames: Vec::new(),
                    contributors: Vec::new(),
                    first_at: Instant::now(),
                });
                if g.frames.is_empty() {
                    g.first_at = Instant::now();
                }
                g.frames.extend(msg.frames);
                g.contributors.push(msg.contrib);
                if g.frames.len() >= fusion_frames {
                    if let Some(grp) = groups.remove(&key) {
                        spawn_flush(ctx.clone(), grp);
                    }
                }
            }
            _ = tick.tick() => {
                let now = Instant::now();
                let hold = Duration::from_millis(hold_ms);
                let ready: Vec<String> = groups.iter()
                    .filter(|(_, g)| !g.frames.is_empty() && now.duration_since(g.first_at) >= hold)
                    .map(|(k, _)| k.clone())
                    .collect();
                for k in ready {
                    if let Some(grp) = groups.remove(&k) {
                        spawn_flush(ctx.clone(), grp);
                    }
                }
            }
        }
    }
}

fn spawn_flush(ctx: Arc<FlushCtx>, group: FusionGroup) {
    tokio::spawn(async move {
        let count = group.frames.len();
        // Build metas [{"i":k,"mid":"..","txn":".."}] directly (no JSON lib).
        let mut metas = String::with_capacity(count * 80 + 2);
        metas.push('[');
        for (k, f) in group.frames.iter().enumerate() {
            if k > 0 {
                metas.push(',');
            }
            metas.push_str("{\"i\":");
            metas.push_str(&k.to_string());
            metas.push_str(",\"mid\":\"");
            metas.push_str(&uuid_bytes_to_string(&f.message_id));
            metas.push_str("\",\"txn\":\"");
            json_escape_into(&mut metas, &f.txn);
            metas.push_str("\"}");
        }
        metas.push(']');

        let fins: Vec<FrameIn> = group
            .frames
            .iter()
            .map(|f| FrameIn {
                message_id: f.message_id,
                txn: &f.txn,
                trace_id: None,
                producer_sub: None,
                payload: &f.payload,
                encrypted: false,
            })
            .collect();
        let packed = pack_frames(&fins);
        let blob = zstd_compress(&packed, ctx.zstd_level);

        let permit = ctx.vegas.acquire().await;
        let t0 = Instant::now();
        let ok = match ctx.pool.get().await {
            Ok(client) => match tokio::time::timeout(
                ctx.stmt_timeout,
                db::push_segment(&client, &group.queue, &group.partition, &metas, &blob, count as i32),
            )
            .await
            {
                Ok(Ok(_)) => true,
                Ok(Err(e)) => {
                    eprintln!("[flush] push_segment error q={} p={}: {}", group.queue, group.partition, e);
                    false
                }
                Err(_) => {
                    eprintln!("[flush] push_segment timeout q={} p={}", group.queue, group.partition);
                    false
                }
            },
            Err(e) => {
                eprintln!("[flush] pool.get error: {}", e);
                false
            }
        };
        let rtt = t0.elapsed();
        drop(permit);
        ctx.vegas.record(rtt);
        ctx.metrics.push.record_batch(count, ok, rtt);

        for c in group.contributors {
            if !ok {
                if let Ok(mut r) = c.state.results.lock() {
                    for &i in &c.item_indices {
                        if i < r.len() {
                            r[i].status = "error";
                        }
                    }
                }
            }
            if c.state.pending.fetch_sub(1, Ordering::AcqRel) == 1 {
                if let Some(tx) = c.state.done.lock().unwrap().take() {
                    let _ = tx.send(());
                }
            }
        }
    });
}

pub fn json_escape_into(out: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
}
