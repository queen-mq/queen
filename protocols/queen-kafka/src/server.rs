//! The listener, and what every connection shares.

use std::future::Future;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio::sync::Semaphore;

use crate::config::Config;
use crate::core::{Broker, Queen};
use crate::groups::Coordinator;
use crate::topics::Topics;
use crate::txn::Txns;

/// What every connection shares. Nothing here is durable: the topics are a
/// cache of the broker's queues, the groups live only while this node leads.
pub struct Ctx {
    pub cfg: Config,
    pub broker: Arc<dyn Broker>,
    pub queen: Queen,
    pub topics: Topics,
    pub groups: Coordinator,
    pub txns: Txns,
    pub producer_ids: AtomicU64,
    pub indexed: crate::presence::Indexed,
    /// Request frames in flight, in bytes (`QUEEN_KAFKA_INFLIGHT_BYTES`):
    /// taken before a frame's buffer is allocated ([`crate::conn`]).
    pub frame_bytes: Semaphore,
    /// SASL logins checked recently ([`crate::handlers::sasl`]).
    pub logins: crate::handlers::sasl::Logins,
}

/// Serve Kafka clients until `stop` resolves.
pub async fn serve(
    cfg: Config,
    broker: Arc<dyn Broker>,
    stop: impl Future<Output = ()> + Send,
) -> Result<(), String> {
    let listener = TcpListener::bind(&cfg.listen)
        .await
        .map_err(|e| format!("cannot listen on {}: {e}", cfg.listen))?;
    let queen = Queen::new(Arc::clone(&broker));
    let ctx = Arc::new(Ctx {
        topics: Topics::new(
            queen.clone(),
            cfg.default_partitions,
            cfg.dedup_window_seconds,
        ),
        groups: Coordinator::new(cfg.join_delay),
        txns: Txns::new(queen.clone()),
        producer_ids: AtomicU64::new(0),
        indexed: crate::presence::Indexed::default(),
        frame_bytes: Semaphore::new(crate::config::inflight_bytes()),
        logins: crate::handlers::sasl::Logins::default(),
        queen,
        broker,
        cfg,
    });
    tracing::info!(
        target: "kafka",
        listen = %ctx.cfg.listen,
        advertised = %ctx.cfg.advertised(),
        "Kafka listener up"
    );
    ctx.txns.set_node(ctx.broker.cluster().node_id);
    let ticker = tokio::spawn(tick(Arc::clone(&ctx)));
    let slots = Arc::new(Semaphore::new(ctx.cfg.max_connections));
    tokio::pin!(stop);
    loop {
        tokio::select! {
            _ = &mut stop => break,
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(a) => a,
                    Err(e) => {
                        tracing::warn!(target: "kafka", error = %e, "accept failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                let Ok(slot) = Arc::clone(&slots).try_acquire_owned() else {
                    tracing::warn!(target: "kafka", %peer, "connection refused: QUEEN_KAFKA_MAX_CONNECTIONS reached");
                    continue;
                };
                let _ = stream.set_nodelay(true);
                let ctx = Arc::clone(&ctx);
                tokio::spawn(async move {
                    crate::conn::serve(ctx, stream, peer).await;
                    drop(slot);
                });
            }
        }
    }
    ticker.abort();
    ctx.groups.clear();
    ctx.txns.clear().await;
    Ok(())
}

/// Session expiry and rebalance deadlines, and the one rule about leadership:
/// a node that stops leading drops every group.
async fn tick(ctx: Arc<Ctx>) {
    let mut led = false;
    let mut n: u64 = 0;
    let mut writing: Option<tokio::task::JoinHandle<()>> = None;
    let mut every = tokio::time::interval(Duration::from_millis(100));
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        every.tick().await;
        let leads = ctx.broker.cluster().leads();
        if led && !leads {
            if !ctx.groups.is_empty() {
                tracing::info!(target: "kafka", "raft leadership moved away: consumer groups handed to the new leader");
            }
            ctx.groups.clear();
            ctx.indexed.clear();
            ctx.txns.clear().await;
        }
        if leads != led {
            ctx.topics.invalidate();
        }
        led = leads;
        ctx.groups.tick(Instant::now());
        n += 1;
        if leads && n.is_multiple_of(10) {
            ctx.txns.expire().await;
            // One presence write at a time: a row deleted for a group that
            // left is never put back by an earlier write that landed late.
            if writing.as_ref().is_none_or(|w| w.is_finished()) {
                let rows = ctx
                    .groups
                    .presence_due(Instant::now(), crate::presence::EVERY);
                let gone = ctx.groups.gone();
                if !rows.is_empty() || !gone.is_empty() {
                    let ctx = Arc::clone(&ctx);
                    let node = ctx.broker.cluster().node_id;
                    writing = Some(tokio::spawn(async move {
                        crate::presence::write(&ctx.queen, &ctx.indexed, node, rows, gone).await;
                    }));
                }
            }
        }
    }
}
