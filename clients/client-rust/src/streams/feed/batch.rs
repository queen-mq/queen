//! `.foreach_batch(f, BatchOptions)`: the end of a feed chain that hands the
//! results to `f` in batches (operators/ForeachBatchOperator.js). The runner
//! keeps the results and calls `f(items)` with up to `max_items` of them: at
//! the end of each read (by default), or, with `max_wait_millis`, once the
//! batch is full or its first item has waited that long. The position moves
//! only after `f` returns, in the commit right after it: a crash before that
//! delivers the batch again, with the same ids.

use std::future::Future;
use std::sync::Arc;

use serde::Serialize;
use serde_json::Value;

use super::ops::{BoxFuture, CatchUnwind, FnResult, IntoFeedDone};

/// One result in a batch: the value and context `.foreach()` would get, and
/// an id that is the same on every delivery of that result (a key for an
/// upsert): the id a `.to()` output of it would carry.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct BatchItem {
    /// The result (`None`: the chain made no value, JS `undefined`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    /// A message's result: `{ position, queue, partition, partitionId,
    /// offset, transactionId }`; a step's result: what the step says of it.
    pub ctx: Value,
    pub id: String,
}

/// The options of [`FeedStream::foreach_batch`](super::FeedStream::foreach_batch):
/// at most `max_items` (1000) a call, and how long the first item may wait
/// for more (`max_wait_millis`, 0: the batch goes at the end of each read).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchOptions {
    pub max_items: usize,
    pub max_wait_millis: u64,
}

impl Default for BatchOptions {
    fn default() -> Self {
        Self {
            max_items: DEFAULT_MAX_ITEMS,
            max_wait_millis: 0,
        }
    }
}

impl BatchOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// At most this many items a call (a whole number from 1).
    pub fn max_items(mut self, n: usize) -> Self {
        self.max_items = n;
        self
    }

    /// A batch waits for more items this long after its first, at most.
    pub fn max_wait_millis(mut self, ms: u64) -> Self {
        self.max_wait_millis = ms;
        self
    }
}

const DEFAULT_MAX_ITEMS: usize = 1000;

pub(crate) type BatchFn = Arc<dyn Fn(Vec<BatchItem>) -> BoxFuture<FnResult<()>> + Send + Sync>;

/// The sink as the runner holds it.
#[derive(Clone)]
pub(crate) struct ForeachBatch {
    pub f: BatchFn,
    pub max_items: usize,
    pub max_wait_millis: u64,
}

impl ForeachBatch {
    pub(crate) fn new<F, Fut, R>(f: F, opts: BatchOptions) -> Result<Self, String>
    where
        F: Fn(Vec<BatchItem>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoFeedDone + 'static,
    {
        if opts.max_items < 1 {
            return Err(format!(
                ".foreachBatch(fn, options): maxItems is a whole number from 1, got {}",
                opts.max_items
            ));
        }
        let f = Arc::new(f);
        let fb: BatchFn = Arc::new(move |items| {
            let fut = f(items);
            Box::pin(async move {
                match CatchUnwind(Box::pin(fut)).await {
                    Ok(r) => r.into_feed_done(),
                    Err(e) => Err(e),
                }
            })
        });
        Ok(Self {
            f: fb,
            max_items: opts.max_items,
            max_wait_millis: opts.max_wait_millis,
        })
    }
}
