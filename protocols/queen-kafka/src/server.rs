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

/// What every connection shares. Nothing here is durable: the topics are a
/// cache of the broker's queues, the groups live only while this node leads.
pub struct Ctx {
    pub cfg: Config,
    pub broker: Arc<dyn Broker>,
    pub queen: Queen,
    pub topics: Topics,
    pub groups: Coordinator,
    pub producer_ids: AtomicU64,
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
        topics: Topics::new(queen.clone(), cfg.default_partitions),
        groups: Coordinator::new(cfg.join_delay),
        producer_ids: AtomicU64::new(0),
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
    Ok(())
}

/// Session expiry and rebalance deadlines, and the one rule about leadership:
/// a node that stops leading drops every group.
async fn tick(ctx: Arc<Ctx>) {
    let mut led = false;
    let mut every = tokio::time::interval(Duration::from_millis(100));
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        every.tick().await;
        let leads = ctx.broker.cluster().leads();
        if led && !leads && !ctx.groups.is_empty() {
            tracing::info!(target: "kafka", "raft leadership moved away: consumer groups handed to the new leader");
            ctx.groups.clear();
        }
        if leads != led {
            ctx.topics.invalidate();
        }
        led = leads;
        ctx.groups.tick(Instant::now());
    }
}
