//! Where the time of the consume path goes, process-wide: offset commits and
//! fetches, the two requests a consumer group sends most. Cheap enough to be
//! always on ([`crate::obs::Timing`]: four relaxed atomics a sample), and
//! reported beside the rest of the facade's own state in the broker's
//! `GET /status` ([`crate::introspect`]).

use std::sync::atomic::{AtomicU64, Ordering};

use crate::obs::Timing;

/// A plain counter.
pub struct Counter(AtomicU64);

impl Counter {
    pub const fn new() -> Counter {
        Counter(AtomicU64::new(0))
    }

    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

impl Default for Counter {
    fn default() -> Counter {
        Counter::new()
    }
}

/// One OffsetCommit, from the handler's entry to the answer it renders.
pub static COMMIT: Timing = Timing::new();
/// The group coordinator's membership check of a commit (one actor round trip).
pub static COMMIT_CHECK: Timing = Timing::new();
/// From a commit joining its group's queue to the moment the write that carried
/// it was answered — the queue plus the write.
pub static COMMIT_WRITTEN: Timing = Timing::new();
/// One KV call of a commit write (a chunk of up to 255 offsets and the fence).
pub static COMMIT_KV: Timing = Timing::new();
/// Commit writes, and the commits and offsets they carried.
pub static COMMIT_BATCHES: Counter = Counter::new();
/// OffsetCommit requests answered from a write shared with the requests
/// queued behind them on their connection (`offset_commit::handle_many`).
pub static COMMIT_COALESCED: Counter = Counter::new();
pub static COMMIT_BATCH_JOBS: Counter = Counter::new();
pub static COMMIT_BATCH_PAIRS: Counter = Counter::new();

/// One Fetch, from the handler's entry to the answer it renders.
pub static FETCH: Timing = Timing::new();
/// The Queen call(s) behind one Fetch (`POST /api/v1/fetch`, long poll included).
pub static FETCH_UPSTREAM: Timing = Timing::new();
/// Fetches, the partitions they named, and the records they returned.
pub static FETCH_ENTRIES: Counter = Counter::new();
pub static FETCH_RECORDS: Counter = Counter::new();
pub static FETCH_EMPTY: Counter = Counter::new();

/// One coalesced Produce: resolving its topics and staging its records (the
/// envelope of every record), before the push.
pub static PRODUCE_STAGE: Timing = Timing::new();
/// One request's topics resolved ([`crate::handlers::produce`]'s
/// `topic_plans`: the catalog read and any auto-create).
pub static PRODUCE_PLANS: Timing = Timing::new();
/// One partition's batch headers, refusals and idempotent sequence checked.
pub static PRODUCE_CHECK: Timing = Timing::new();
/// One partition's records decoded (and decompressed).
pub static PRODUCE_DECODE: Timing = Timing::new();
/// One partition's records written as envelopes.
pub static PRODUCE_ENVELOPE: Timing = Timing::new();
/// The push into Queen of one coalesced Produce, to its answer.
pub static PRODUCE_PUSH: Timing = Timing::new();
/// Rendering the answers of one coalesced Produce.
pub static PRODUCE_RENDER: Timing = Timing::new();
/// Coalesced Produces handled, the requests they carried, and their records.
pub static PRODUCE_PUSHES: Counter = Counter::new();
pub static PRODUCE_REQUESTS: Counter = Counter::new();
pub static PRODUCE_RECORDS: Counter = Counter::new();
/// Connections ended because a produce was not written: an idempotent batch
/// refused and answered (`conn::RESEND_IN_ORDER`, on the node that refused it
/// or a follower that relayed it), or a pipelined push that failed and is not
/// answered (`conn::PIPELINE_FAILED`). Each is a client that reconnects and
/// resends in order.
pub static PRODUCE_RESEND_CLOSES: Counter = Counter::new();

/// The whole report.
pub fn snapshot() -> serde_json::Value {
    serde_json::json!({
        "produceStage": PRODUCE_STAGE.snapshot(),
        "producePlans": PRODUCE_PLANS.snapshot(),
        "produceCheck": PRODUCE_CHECK.snapshot(),
        "produceDecode": PRODUCE_DECODE.snapshot(),
        "produceEnvelope": PRODUCE_ENVELOPE.snapshot(),
        "producePush": PRODUCE_PUSH.snapshot(),
        "produceRender": PRODUCE_RENDER.snapshot(),
        "producePushes": PRODUCE_PUSHES.get(),
        "produceRequests": PRODUCE_REQUESTS.get(),
        "produceRecords": PRODUCE_RECORDS.get(),
        "produceResendCloses": PRODUCE_RESEND_CLOSES.get(),
        "commit": COMMIT.snapshot(),
        "commitCheck": COMMIT_CHECK.snapshot(),
        "commitWritten": COMMIT_WRITTEN.snapshot(),
        "commitKv": COMMIT_KV.snapshot(),
        "commitBatches": COMMIT_BATCHES.get(),
        "commitCoalesced": COMMIT_COALESCED.get(),
        "commitBatchJobs": COMMIT_BATCH_JOBS.get(),
        "commitBatchPairs": COMMIT_BATCH_PAIRS.get(),
        "fetch": FETCH.snapshot(),
        "fetchUpstream": FETCH_UPSTREAM.snapshot(),
        "fetchEntries": FETCH_ENTRIES.get(),
        "fetchRecords": FETCH_RECORDS.get(),
        "fetchEmpty": FETCH_EMPTY.get(),
    })
}
