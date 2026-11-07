//! `run(FeedRunOptions::new(id).shards(n))`: the N shards of one feed stream
//! in this process, one runner each (runtime/FeedShards.js). Each shard has
//! its own lock: the same code started in several processes runs each shard
//! in the first process that takes its lock, and keeps it as a standby in the
//! others.

use std::sync::Arc;

use serde_json::{json, Map, Value};

use crate::error::{Error, Result};
use crate::js;

use super::runner::{FeedRunOptions, FeedRunner, FeedStateView};
use super::CompiledFeed;

const SUMMED: [&str; 22] = [
    "cycles",
    "entriesRead",
    "emitted",
    "outputs",
    "effects",
    "commits",
    "conflicts",
    "errors",
    "retries",
    "poisoned",
    "skipped",
    "deadLettered",
    "late",
    "ticks",
    "takeovers",
    "snapshots",
    "replays",
    "replayEntries",
    "tableRows",
    "pendingOutputs",
    "open",
    "stateKeys",
];

/// The shards of one stream in this process: answers like one runner.
#[derive(Debug)]
pub struct FeedShardGroup {
    runners: Vec<FeedRunner>,
    count: u32,
}

impl FeedShardGroup {
    pub(crate) async fn start(compiled: CompiledFeed, opts: FeedRunOptions) -> Result<Self> {
        let n = opts.shards.unwrap_or(0);
        if !(2..=4096).contains(&n) {
            return Err(Error::Invalid(format!(
                "run({{ shards }}): shards is a whole number from 2 to 4096, got {n}"
            )));
        }
        if opts.shard.is_some() {
            return Err(Error::Invalid(
                "run(): give shards (all of them here) or shard (one), not both".into(),
            ));
        }
        let mut runners: Vec<FeedRunner> = Vec::new();
        // One at a time: the first shard writes the layout the others then find.
        for i in 0..n {
            let mut o = opts.clone();
            o.shards = None;
            o.shard = Some((i, n));
            match FeedRunner::start(compiled.clone(), o).await {
                Ok(r) => runners.push(r),
                Err(e) => {
                    for r in &runners {
                        r.stop().await;
                    }
                    return Err(e);
                }
            }
        }
        Ok(Self { runners, count: n })
    }

    pub fn shards(&self) -> u32 {
        self.count
    }

    pub fn runners(&self) -> &[FeedRunner] {
        &self.runners
    }

    pub async fn stop(&self) {
        let stops: Vec<_> = self
            .runners
            .iter()
            .map(|r| {
                let r = r.clone();
                tokio::spawn(async move { r.stop().await })
            })
            .collect();
        for s in stops {
            let _ = s.await;
        }
    }

    pub async fn done(&self) {
        for r in &self.runners {
            r.done().await;
        }
    }

    /// The sums, and every shard's metrics (`perShard`).
    pub fn metrics(&self) -> Value {
        let shards: Vec<Value> = self.runners.iter().map(|r| r.metrics()).collect();
        let mut out = Map::new();
        out.insert("shards".into(), json!(self.count));
        out.insert(
            "running".into(),
            json!(shards
                .iter()
                .filter(|m| m.get("role").and_then(|r| r.as_str()) == Some("active"))
                .count()),
        );
        for k in SUMMED {
            let sum: f64 = shards.iter().filter_map(|m| js::finite(m.get(k))).sum();
            out.insert(k.into(), js::num(sum));
        }
        out.insert("perShard".into(), Value::Array(shards));
        Value::Object(out)
    }

    /// The live state of one step over the shards this process runs.
    pub fn state(&self, step: &str) -> FeedStateView {
        FeedStateView::over(
            self.runners
                .iter()
                .map(|r| r.shared())
                .collect::<Vec<Arc<_>>>(),
            step,
        )
    }
}
