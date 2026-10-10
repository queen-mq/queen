//! Leader-only Phase-2 maintenance planning.
//!
//! The clock is supplied by the batcher and every result is an ordinary
//! replicated effect. Nothing in this module mutates the store: followers run
//! the same deterministic apply path as for client commands.

use crate::rsm::dedup::TxnsRow;
use crate::rsm::effect::{Effect, Pid, RowsMark};
use crate::rsm::qlog::set::QLogReader;
use crate::rsm::store::keys;
use crate::rsm::store::rows;
use crate::rsm::store::{Keyspace, Reads, Result, StoreError, TypedReads};

#[derive(Clone, Debug)]
pub struct Config {
    /// Maximum append/garbage rows considered in one entry.
    pub row_limit: usize,
    pub trace_retention_s: i64,
    pub partition_cleanup_enabled: bool,
    pub partition_cleanup_days: i64,
    /// `QUEEN_RAFT_RETENTION_VISIT` (default 8192): partitions of ONE queue a
    /// pass visits at most. The walk resumes where the last pass stopped
    /// ([`Config::walk`]) and wraps, so a queue with more partitions is swept
    /// over several passes. `row_limit` bounds the work a pass FINDS; this
    /// bounds the partitions it LOOKS AT — without it every pass read every
    /// partition's rows on the planning thread (1M partitions: a pause of
    /// hundreds of ms every 5 s, push p99 416 ms against 55 ms without).
    pub visit_cap: usize,
    /// Where each queue's walk resumes, `(tenant, queue)` -> first pid of the
    /// next pass. Leader-local and advisory: it only orders which partitions a
    /// pass looks at first; every effect is still judged from committed state.
    pub walk: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<(String, String), Pid>>>,
    /// `QUEEN_RAFT_RETENTION_VISIT_TOTAL` (default 4096): partitions a pass
    /// looks at over ALL queues. `visit_cap` bounds one queue; with it alone a
    /// pass over 1,000 queues of 10 partitions judged all 10,000 in one cycle
    /// (13.7% of the planning thread at 1M msg/s, 2026-09-29). A pass that
    /// runs out resumes at the next queue ([`Config::walk_queue`]).
    pub visit_total: usize,
    /// The queue the next pass starts at, `None` = the first.
    pub walk_queue: std::sync::Arc<std::sync::Mutex<Option<(String, String)>>>,
    /// `QUEEN_RAFT_TXN_WINDOW_MIN_S` (default 900): the least time a message's
    /// hash list — and so its queue-log record — is kept, however short the
    /// queue's dedup window. The physical reclaim of a queue log follows this
    /// watermark, so nothing is freed before it.
    pub txn_window_min_s: i64,
    /// Walk the partitions here, on the planning thread (the old way). Off
    /// when the background scanner ([`crate::rsm::retention_scan`]) walks them:
    /// then [`plan`] keeps only the garbage and trace steps.
    pub partition_walk: bool,
    /// The node's trace environment (`<data_dir>/traces`, `rsm/traces.rs`),
    /// looked up in the process's registry when a step is planned. `None`
    /// (a batcher without a facade): only the legacy RAM traces are judged.
    pub traces_dir: Option<std::path::PathBuf>,
    /// `QUEEN_RAFT_TRACE_TRIM_LIMIT` (default 512): the most traces one
    /// `TraceTrim` deletes from each of the legacy RAM keyspaces and the trace
    /// environment. It travels in the effect, so every node deletes the same
    /// rows; more due than this sets [`Planned::more`] and the next step runs
    /// at once.
    pub trace_trim_limit: usize,
    /// `QUEEN_RAFT_ROWS_WINDOW` (default on): where the cluster reads
    /// catalogue version 6, a partition's `txns` rows leave the store when
    /// they leave the queue's txns window, whether or not their messages are
    /// still retained ([`Cutoffs::rows_past_log`]). Off: rows keep following
    /// `log_start`, as before version 6, so a queue holds one row in RAM per
    /// retained push.
    pub rows_window: bool,
    /// This node's queue-log reader: where retention finds the appends of
    /// messages that have no row any more (`[log_start, rows_start)`). `None`
    /// (a batcher without a facade, or the queue log off): rows are never let
    /// past `log_start` by this leader, and such messages are not judged.
    pub qlog: Option<QLogReader>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            row_limit: 1_000,
            trace_retention_s: 7 * 24 * 60 * 60,
            partition_cleanup_enabled: true,
            partition_cleanup_days: 30,
            visit_cap: 8_192,
            walk: Default::default(),
            visit_total: 4_096,
            walk_queue: Default::default(),
            txn_window_min_s: 900,
            partition_walk: true,
            traces_dir: None,
            trace_trim_limit: 512,
            rows_window: true,
            qlog: None,
        }
    }
}

#[derive(Default)]
pub struct Planned {
    pub effects: Vec<Effect>,
    /// More bounded garbage work is known to remain and should run without
    /// waiting for the next periodic tick.
    pub more: bool,
}

/// Reclaim sealed queue-log files made dead by already-committed txns
/// watermarks. This is intentionally node-local: the positions and file
/// boundaries differ per replica. It runs from the blocking maintenance
/// cycle, never on a Tokio worker, and is safe to repeat after a crash.
///
/// Per file: a pass judges sealed files one at a time and looks up only the
/// partitions that have records in them ([`QLogReader::reclaim_log`]). It used
/// to read EVERY partition row on every pass to build each log's watermark
/// map: about 0.5 µs a partition, so ~0.5 s a pass at 1M partitions and 4-5 s
/// at 10M, repeated every 5 s on every node whatever the files held.
pub fn reclaim_qlogs<R: Reads + ?Sized>(r: &R, qlogs: &QLogReader) -> Result<usize> {
    use std::collections::HashMap;

    // pid -> (the log its records go to, its txns_start); None: the partition
    // is gone. One store read per pid per pass, shared by every file and log.
    let mut known: HashMap<Pid, Option<(u64, u64)>> = HashMap::new();
    let mut failed: Option<StoreError> = None;

    let mut logs = qlogs.log_ids();
    logs.sort_unstable();
    let cursor = qlogs.reclaim_queue_cursor();
    let split = logs.partition_point(|id| *id < cursor);
    logs.rotate_left(split);

    // A bounded pass: up to `RECLAIM_EXAMINE_PER_PASS` sealed files judged and
    // `RECLAIM_LOOKUPS_PER_PASS` partitions looked up, every dead file
    // unlinked (cheap), but at most ONE copy-forward rewrite (it can touch tens
    // of MiB), all within `RECLAIM_PASS_BUDGET`. A file's judging resumes where
    // the last pass stopped, so a big file is judged over several passes.
    let mut budget = crate::rsm::qlog::set::ReclaimBudget {
        files: RECLAIM_EXAMINE_PER_PASS,
        lookups: RECLAIM_LOOKUPS_PER_PASS,
        deadline: std::time::Instant::now() + RECLAIM_PASS_BUDGET,
        rewrite: true,
    };
    let mut removed = 0usize;
    for log_id in logs {
        if budget.spent() {
            break;
        }
        // The log that holds a partition's records is its lane's log of its
        // queue. A record in any other log is dead: the key must be exactly
        // the log the writer routed the partition to.
        let mut live_start = |pid: u64| -> std::io::Result<Option<u64>> {
            let at = match known.get(&pid) {
                Some(at) => *at,
                None => {
                    let at = match r.partition(pid) {
                        Ok(row) => row.map(|row| {
                            let queue_id = QLogReader::queue_id_of(&row.tenant, &row.queue);
                            (qlogs.log_id_for(queue_id, pid), row.txns_start)
                        }),
                        Err(e) => {
                            failed = Some(e);
                            return Err(std::io::Error::other("partition read failed"));
                        }
                    };
                    known.insert(pid, at);
                    at
                }
            };
            Ok(at.and_then(|(log, start)| (log == log_id).then_some(start)))
        };
        let progress = qlogs.reclaim_log(log_id, &mut live_start, &mut budget);
        if let Some(e) = failed.take() {
            return Err(e);
        }
        let progress = progress.map_err(|e| StoreError::Io(format!("qlog retention: {e}")))?;
        qlogs.advance_reclaim_queue_cursor(log_id);
        removed += progress.changed;
    }
    Ok(removed)
}

/// The most sealed queue-log files one retention pass judges.
const RECLAIM_EXAMINE_PER_PASS: usize = 256;

/// The most partitions one retention pass looks up (one per partition run in a
/// judged file; a file bigger than this is judged over several passes).
const RECLAIM_LOOKUPS_PER_PASS: usize = 262_144;

/// The wall time one retention pass may take before it stops looking.
const RECLAIM_PASS_BUDGET: std::time::Duration = std::time::Duration::from_millis(200);

pub fn plan<R: Reads + ?Sized>(r: &R, now_us: i64, cfg: &Config) -> Result<Planned> {
    let mut out = Planned::default();
    let mut budget = cfg.row_limit.max(1);

    // Resume deletes that survived a crash/restart or an endpoint deadline.
    // One effect per marker preserves its exact scope (especially group
    // deletes, which must never widen to a queue delete).
    let mut garbage = Vec::new();
    r.scan_raw(
        Keyspace::Garbage,
        &[],
        &[],
        budget + 1,
        &mut |key, value| {
            if let (Some(pid), Ok(row)) = (keys::pid_of(key), rows::garbage_decode(value)) {
                garbage.push((pid, row.scope));
            }
            true
        },
    )?;
    out.more = garbage.len() > budget;
    garbage.truncate(budget);
    for (pid, scope) in garbage {
        out.effects.push(Effect::DeleteChunk {
            pids: vec![pid],
            scope,
            resume: Vec::new(),
            limit: cfg.row_limit.max(1).min(u32::MAX as usize) as u32,
        });
        budget = budget.saturating_sub(1);
    }

    let mut queues = Vec::new();
    // The queue keyspace has no all-tenants typed iterator. Decode its
    // self-delimiting key and retain the config row.
    let mut bad_queue = false;
    r.scan_raw(Keyspace::Queues, &[], &[], usize::MAX, &mut |key, value| {
        let parsed = (|| {
            let (tenant, at) = keys::read_name(key, 0)?;
            let (queue, end) = keys::read_name(key, at)?;
            (end == key.len()).then_some((tenant, queue))
        })();
        match (parsed, rows::queue_decode(value)) {
            (Some((tenant, queue)), Ok(config)) => {
                queues.push((tenant, queue, config));
                true
            }
            _ => {
                bad_queue = true;
                false
            }
        }
    })?;
    if bad_queue {
        return Err(crate::rsm::store::StoreError::corrupt(
            Keyspace::Queues,
            "queue row",
        ));
    }

    let queues = if cfg.partition_walk {
        queues
    } else {
        Vec::new()
    };
    let resume = cfg.walk_queue.lock().expect("retention walk").take();
    let mut visits_left = cfg.visit_total.max(1);
    let mut stopped_at: Option<(String, String)> = None;
    for (tenant, queue, qcfg) in queues {
        if resume
            .as_ref()
            .is_some_and(|(t, q)| (tenant.as_str(), queue.as_str()) < (t.as_str(), q.as_str()))
        {
            continue;
        }
        if budget == 0 {
            out.more = true;
            break;
        }
        if visits_left == 0 {
            // The rest of the queues wait for the next pass.
            stopped_at = Some((tenant.clone(), queue.clone()));
            break;
        }
        let cut = queue_cutoffs(r, now_us, cfg, &tenant, &queue, &qcfg);

        // A bounded window of the queue's partitions, resuming where the last
        // pass stopped and wrapping to the first ones (see `visit_cap`).
        let cap = cfg.visit_cap.max(1).min(visits_left);
        let walk_key = (tenant.clone(), queue.clone());
        let start = cfg
            .walk
            .lock()
            .expect("retention walk")
            .get(&walk_key)
            .copied();
        let mut pids = Vec::new();
        r.scan_queue_partitions(&tenant, &queue, start, cap, &mut |pid| {
            pids.push(pid);
            true
        })?;
        // `None`: the next pass starts from the first partition again.
        let mut next: Option<Pid> = None;
        if pids.len() >= cap {
            next = pids.last().map(|p| p.saturating_add(1));
        } else if let Some(s) = start {
            // The tail ran out: wrap, up to the partition this pass began at.
            let room = cap - pids.len();
            let mut head = Vec::new();
            r.scan_queue_partitions(&tenant, &queue, None, room, &mut |pid| {
                if pid >= s {
                    return false;
                }
                head.push(pid);
                true
            })?;
            if head.len() >= room {
                next = head.last().map(|p| p.saturating_add(1));
            }
            pids.extend(head);
        }
        for pid in pids {
            if budget == 0 {
                out.more = true;
                // The next pass resumes at the first partition not looked at.
                next = Some(pid);
                break;
            }
            visits_left = visits_left.saturating_sub(1);
            if r.garbage(pid)?.is_some() {
                continue;
            }
            let Some(part) = r.partition(pid)? else {
                continue;
            };
            match judge_partition(r, now_us, cfg, pid, &part, &cut)? {
                Some(Verdict::Watermark {
                    log_start,
                    txns_start,
                    rows,
                    more,
                }) => {
                    out.effects.push(Effect::Watermark {
                        pid,
                        log_start,
                        txns_start,
                        rows,
                    });
                    out.more |= more;
                    budget = budget.saturating_sub(1);
                }
                Some(Verdict::Delete) => {
                    out.effects.push(Effect::PartitionDelete { pid });
                    budget = budget.saturating_sub(1);
                }
                None => {}
            }
        }
        {
            let mut walk = cfg.walk.lock().expect("retention walk");
            match next {
                Some(p) => {
                    walk.insert(walk_key, p);
                }
                None => {
                    walk.remove(&walk_key);
                }
            }
        }
        if visits_left == 0 && next.is_some() {
            // This queue is not done: the next pass starts here.
            stopped_at = Some((tenant.clone(), queue.clone()));
            break;
        }
    }
    *cfg.walk_queue.lock().expect("retention walk") = stopped_at;

    // D18: only log an expiry command when the oldest index row is due.
    let trace_cutoff = now_us.saturating_sub(cfg.trace_retention_s.max(1) * 1_000_000);
    if crate::rsm::effect::cluster_allows(r.cluster_version()?, crate::rsm::effect::VERSION_4) {
        plan_trace_trim(r, cfg, trace_cutoff, &mut out)?;
        return Ok(out);
    }
    let mut trace_due = false;
    r.scan_raw(Keyspace::TraceExpiry, &[], &[], 1, &mut |key, _| {
        trace_due = keys::trace_expiry_created_of(key).is_some_and(|at| at < trace_cutoff);
        false
    })?;
    if trace_due {
        out.effects.push(Effect::TraceExpire {
            cutoff_us: trace_cutoff,
        });
    }
    Ok(out)
}

/// Catalogue version 4 (traces on disk): one BOUNDED trim step when a legacy
/// RAM trace or a trace in the trace environment is past retention, instead
/// of a `TraceExpire` that deletes every one of them in one apply step. More
/// due than one step deletes asks for the next step at once (`more`).
fn plan_trace_trim<R: Reads + ?Sized>(
    r: &R,
    cfg: &Config,
    cutoff_us: i64,
    out: &mut Planned,
) -> Result<()> {
    let limit = cfg.trace_trim_limit.clamp(1, u32::MAX as usize);
    let mut ram_due = 0usize;
    r.scan_raw(Keyspace::TraceExpiry, &[], &[], limit + 1, &mut |key, _| {
        if keys::trace_expiry_created_of(key).is_some_and(|at| at < cutoff_us) {
            ram_due += 1;
            true
        } else {
            false
        }
    })?;
    let disk_due = match cfg
        .traces_dir
        .as_deref()
        .and_then(crate::rsm::traces::lookup)
    {
        Some(t) => t.read(|t| t.due(cutoff_us, limit + 1))?,
        None => 0,
    };
    if ram_due > 0 || disk_due > 0 {
        out.effects.push(Effect::TraceTrim {
            cutoff_us,
            limit: limit as u32,
        });
        if ram_due > limit || disk_due > limit {
            out.more = true;
        }
    }
    Ok(())
}

/// A queue's retention cutoffs at `now_us` ([`judge_partition`]).
#[derive(Clone, Copy, Debug)]
pub(crate) struct Cutoffs {
    all: Option<i64>,
    completed: Option<i64>,
    max_wait: Option<i64>,
    txns: i64,
    /// The cluster reads catalogue version 6: a watermark carries its rows
    /// tail ([`RowsMark`]).
    v6: bool,
    /// Rows may leave the store past `log_start` (version 6, the
    /// `rows_window` switch on, and this node has a queue-log reader to judge
    /// the messages they leave behind).
    rows_past_log: bool,
}

impl Cutoffs {
    /// The newest cutoff that can move a partition's LOG watermark (`None`: no
    /// rule can — retention off and no max wait). The txns watermark never
    /// passes the log one ([`judge_partition`]'s `.min(log_target)`), so with
    /// none of these it can only catch up to where the log already starts.
    fn log_cut(&self) -> Option<i64> {
        [self.all, self.max_wait, self.completed]
            .into_iter()
            .flatten()
            .max()
    }
}

/// Retention cutoffs straight from their parts, for the tests of
/// [`judge_partition`] (a queue's are [`queue_cutoffs`]).
#[cfg(test)]
pub(crate) fn cutoffs_for_test(
    all: Option<i64>,
    completed: Option<i64>,
    max_wait: Option<i64>,
    txns: i64,
) -> Cutoffs {
    Cutoffs {
        all,
        completed,
        max_wait,
        txns,
        v6: false,
        rows_past_log: false,
    }
}

/// [`cutoffs_for_test`] on a cluster at catalogue version 6, rows free to
/// pass `log_start` when `rows_past_log`.
#[cfg(test)]
pub(crate) fn cutoffs_v6_for_test(
    all: Option<i64>,
    completed: Option<i64>,
    max_wait: Option<i64>,
    txns: i64,
    rows_past_log: bool,
) -> Cutoffs {
    Cutoffs {
        all,
        completed,
        max_wait,
        txns,
        v6: true,
        rows_past_log,
    }
}

/// The cutoffs `qcfg` sets for queue `(tenant, queue)` at `now_us`.
pub(crate) fn queue_cutoffs<R: Reads + ?Sized>(
    r: &R,
    now_us: i64,
    cfg: &Config,
    tenant: &str,
    queue: &str,
    qcfg: &crate::rsm::effect::QueueConfig,
) -> Cutoffs {
    let sink_floor = sink_floor(r, now_us, tenant, queue, qcfg);
    let all = (qcfg.retention_enabled && qcfg.retention_seconds > 0)
        .then(|| now_us.saturating_sub(qcfg.retention_seconds as i64 * 1_000_000))
        .map(|v| v.min(sink_floor));
    let completed = (qcfg.retention_enabled && qcfg.completed_retention_seconds > 0)
        .then(|| now_us.saturating_sub(qcfg.completed_retention_seconds as i64 * 1_000_000))
        .map(|v| v.min(sink_floor));
    let max_wait = (qcfg.max_wait_time_seconds > 0)
        .then(|| now_us.saturating_sub(qcfg.max_wait_time_seconds as i64 * 1_000_000));
    // The txns window, how long a push keeps its row: the dedup window, and
    // never under the node's floor. Completed retention was part of it until
    // 2.2.0 and is not: it says when a consumed message leaves the LOG
    // (`completed` above), and nothing reads a row older than the dedup window
    // but an ack repeated after it (a probe ignores an older row, dedup.rs).
    // With it, a queue that keeps its consumed messages for a month kept a
    // month of rows in every node's memory.
    let txn_window_s = i64::from(qcfg.dedup_window_seconds).max(cfg.txn_window_min_s.max(0));
    // Catalogue version 6, read from committed state as every gate is (D20).
    // A store that cannot say reads as below it: the old shape is always safe.
    let v6 = r
        .cluster_version()
        .is_ok_and(|v| crate::rsm::effect::cluster_allows(v, crate::rsm::effect::VERSION_6));
    Cutoffs {
        all,
        completed,
        max_wait,
        txns: now_us.saturating_sub(txn_window_s * 1_000_000),
        v6,
        rows_past_log: v6
            && cfg.rows_window
            && cfg.qlog.is_some()
            && crate::rsm::dedup::record_index_mode() != crate::rsm::dedup::IndexMode::Segment,
    }
}

/// What retention does to one partition now ([`judge_partition`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Verdict {
    Watermark {
        log_start: u64,
        txns_start: u64,
        /// The version-6 tail ([`Cutoffs::v6`]); `None` below it.
        rows: Option<RowsMark>,
        /// A step stopped at its row limit: more is due for this partition
        /// now, without waiting for the next round.
        more: bool,
    },
    Delete,
}

/// Retention's judgment of partition `pid` (row `part`) under its queue's
/// cutoffs: move its watermarks, delete it (idle past the cleanup age), or
/// nothing. Shared by the walk on the planning thread ([`plan`]) and the
/// background scanner ([`scan_slice`]).
pub(crate) fn judge_partition<R: Reads + ?Sized>(
    r: &R,
    now_us: i64,
    cfg: &Config,
    pid: Pid,
    part: &rows::PartitionRow,
    cut: &Cutoffs,
) -> Result<Option<Verdict>> {
    judge_with(r, now_us, cfg, pid, part, cut, true)
}

/// [`judge_partition`] without its two shortcuts for a partition nothing can
/// move: the walk as it ran until 2026-10-01, the reference the tests hold
/// the shortcuts to.
#[cfg(test)]
pub(crate) fn judge_partition_unskipped<R: Reads + ?Sized>(
    r: &R,
    now_us: i64,
    cfg: &Config,
    pid: Pid,
    part: &rows::PartitionRow,
    cut: &Cutoffs,
) -> Result<Option<Verdict>> {
    judge_with(r, now_us, cfg, pid, part, cut, false)
}

/// See [`judge_partition`]; `skip` takes the shortcuts.
///
/// # When nothing can move (2026-10-01)
///
/// The judgment ends in a watermark only when `log_target > log_start` or
/// `txn_target > txns_start`, and `txn_target` is capped at `log_target`.
/// `log_target` rises above `log_start` only through a LOG cutoff (`all`,
/// `max_wait`, `completed`) that a row at or past `log_start` is older than.
/// So when no log cutoff can move the log, `log_target == log_start <=
/// txns_start` and neither watermark moves: the verdict is [`idle_verdict`]'s,
/// whatever the rows say. Two cases make that knowable early:
///
/// - **no log cutoff at all** (retention off, no max wait — every queue on
///   prod, 2026-10-01) and `txns_start >= log_start`: decided before any txns
///   row is read. The dedup window alone (`cut.txns`) used to send every such
///   partition whose oldest row was older than an hour through the
///   `row_limit + 1` scan, ~70 µs a visit for nothing.
/// - **the oldest row is no older than any log cutoff**: rows are in created
///   order, so every row at or past `log_start` (they all come after the
///   oldest, `txns_start <= log_start`) is too, and no log cutoff moves the
///   log; with `txns_start >= log_start` the full scan is skipped as well.
///
/// Same verdict in every case, then. The one thing the first shortcut gives
/// up is noticing an oldest txns row that does not decode (the second reads
/// that row, and one that does not decode takes the full scan, as before).
/// The walk was never the corruption check — it reads at most
/// `row_limit + 1` rows, and none past an oldest one that is fresh — and
/// every reader of the row still refuses it.
fn judge_with<R: Reads + ?Sized>(
    r: &R,
    now_us: i64,
    cfg: &Config,
    pid: Pid,
    part: &rows::PartitionRow,
    cut: &Cutoffs,
    skip: bool,
) -> Result<Option<Verdict>> {
    let log_cut = cut.log_cut();
    // Where the partition's rows begin (catalogue version 6: at or above
    // `txns_start`, and past `log_start` when retained messages already lost
    // theirs). Below version 6 it IS `txns_start`.
    let rows_at = part.rows_start.max(part.txns_start);
    // Retained messages without a row: `[log_start, rows_at)`.
    let unrowed = part.log_start < rows_at;
    // The partition holds no row at all (everything it has is unrowed, or it
    // is empty): nothing to scan, nothing to expire.
    let no_rows = rows_at as i64 > part.last_offset;
    // Rows that can never pass the log (below version 6, or the switch off)
    // and already start at or past it cannot move without the log.
    let rows_capped = !cut.rows_past_log && rows_at >= part.log_start;
    if skip && log_cut.is_none() && (no_rows || rows_capped) {
        return idle_verdict(r, now_us, cfg, pid, part);
    }

    let prefix = keys::txns_prefix(pid);
    let from = keys::txns(pid, rows_at);
    // The oldest row's stamp (`None`: no row). One that does not decode is
    // fresh for no cutoff, so the scan below meets it and reports it.
    let mut oldest_at: Option<i64> = None;
    let mut oldest_bad = false;
    if !no_rows {
        r.scan_raw(Keyspace::Txns, &from, &prefix, 1, &mut |_key, value| {
            match TxnsRow::decode(value) {
                Ok(row) => oldest_at = Some(row.created_at_us),
                Err(_) => oldest_bad = true,
            }
            false
        })?;
    }
    let fresh = |cutoff: i64| !oldest_bad && oldest_at.is_none_or(|at| at >= cutoff);

    // Can the LOG move? Only through a log cutoff the oldest retained message
    // is older than. With a row at `log_start` the oldest row bounds it (rows
    // are in created order and none starts above the first retained frame);
    // without one the partition row's own stamp does, and an unknown stamp
    // lets the queue-log walk below decide.
    let log_may_move = match log_cut {
        None => false,
        Some(c) if unrowed => part.oldest_live_at_us.is_none_or(|at| at < c),
        Some(c) => !fresh(c),
    };
    // Can the ROWS move? Only with a row older than the txns cutoff, and, where
    // they cannot pass the log, only up to where the log goes.
    let rows_may_move = !no_rows && !fresh(cut.txns) && (!rows_capped || log_may_move);
    if skip && !log_may_move && !rows_may_move {
        return idle_verdict(r, now_us, cfg, pid, part);
    }
    // Rows are in created order: when the oldest is newer than every cutoff,
    // nothing here is stale, and the full scan below would move no watermark.
    let newest_cut = log_cut.map_or(cut.txns, |c| c.max(cut.txns));
    if !unrowed && fresh(newest_cut) {
        return idle_verdict(r, now_us, cfg, pid, part);
    }

    // The rows themselves, when a judgment below reads them: the rows part,
    // and the log part of a partition whose retained messages all have one.
    let need_rows =
        !no_rows && (!skip || oldest_bad || rows_may_move || (!unrowed && log_may_move));
    let mut segments = Vec::new();
    let mut corrupt = false;
    if need_rows {
        r.scan_raw(
            Keyspace::Txns,
            &from,
            &prefix,
            cfg.row_limit + 1,
            &mut |key, value| match (keys::txns_base_of(key), TxnsRow::decode(value)) {
                (Some(base), Ok(row)) => {
                    segments.push((base, row));
                    true
                }
                _ => {
                    corrupt = true;
                    false
                }
            },
        )?;
    }
    if corrupt {
        return Err(crate::rsm::store::StoreError::corrupt(
            Keyspace::Txns,
            "txns row",
        ));
    }

    // The cap of the completed-retention cutoff: one past the lowest
    // committed cursor (`None`: no group reads the partition, nothing is
    // "completed").
    let mut completed_cap: Option<u64> = None;
    if cut.completed.is_some() {
        let mut min_committed: Option<i64> = None;
        r.scan_cursors(pid, usize::MAX, &mut |_group, cursor| {
            min_committed =
                Some(min_committed.map_or(cursor.committed, |old| old.min(cursor.committed)));
            true
        })?;
        completed_cap = min_committed.map(|v| v.saturating_add(1).max(0) as u64);
    }
    // The log target over one list of appends (rows, or the queue log's
    // records for the unrowed range), and whether a cutoff stopped at its
    // row limit.
    let log_over = |appends: &[(u64, TxnsRow)]| -> (u64, bool) {
        let mut target = part.log_start;
        let mut full = false;
        let mut take = |cutoff: i64, cap: Option<u64>| {
            let (t, n) = stale_boundary_n(appends, part.log_start, cutoff, cap, cfg.row_limit);
            full |= n >= cfg.row_limit.max(1);
            target = target.max(t);
        };
        if let Some(cutoff) = cut.all {
            take(cutoff, None);
        }
        if let Some(cutoff) = cut.max_wait {
            take(cutoff, None);
        }
        if let (Some(cutoff), Some(cap)) = (cut.completed, completed_cap) {
            take(cutoff, Some(cap));
        }
        (target, full)
    };

    let mut log_target = part.log_start;
    let mut more = false;
    // The stamp of the oldest message left, when the new `log_start` stays
    // inside the unrowed range: apply has no row to read it from.
    let mut oldest_carry: Option<i64> = None;
    if unrowed {
        // The messages in `[log_start, rows_at)` have no row: their appends
        // come from this node's queue-log index. A node without one (or
        // without those files) judges nothing here; the rows part still runs.
        if let (true, Some(q)) = (log_may_move || !skip, cfg.qlog.as_ref()) {
            let found = unrowed_appends(q, part, pid, rows_at, cfg.row_limit + 1)?;
            let (target, full) = log_over(&found);
            // Never past the rows: the frames from `rows_at` on are judged
            // from their rows, at the next step.
            log_target = target.min(rows_at);
            more |= full || (log_target == rows_at && log_target > part.log_start);
            if log_target > part.log_start && log_target < rows_at {
                oldest_carry = found
                    .iter()
                    .find(|(base, _)| *base >= log_target)
                    .map(|(_, row)| row.created_at_us);
            }
        }
    } else if log_cut.is_some() {
        let (target, full) = log_over(&segments);
        log_target = target;
        more |= full;
    }

    // The rows: every row older than the txns cutoff — capped at the log
    // where rows cannot pass it (the version-1 rule).
    let rows_target = if no_rows {
        rows_at
    } else if cut.rows_past_log {
        let (t, n) = stale_boundary_n(&segments, rows_at, cut.txns, None, cfg.row_limit);
        more |= n >= cfg.row_limit.max(1);
        t
    } else {
        let (t, n) = stale_boundary_n(
            &segments,
            rows_at,
            cut.txns,
            Some(log_target),
            cfg.row_limit,
        );
        more |= n >= cfg.row_limit.max(1);
        t.min(log_target).max(rows_at)
    };
    // The hash-list watermark, which gates the queue log's reclaim: below it
    // a record has neither its payload nor its row.
    let txn_target = rows_target.min(log_target).max(part.txns_start);
    if log_target > part.log_start || txn_target > part.txns_start || rows_target > rows_at {
        return Ok(Some(Verdict::Watermark {
            log_start: log_target,
            txns_start: txn_target,
            rows: cut.v6.then_some(RowsMark {
                rows_start: rows_target,
                oldest_live_at_us: oldest_carry,
            }),
            more,
        }));
    }
    idle_verdict(r, now_us, cfg, pid, part)
}

/// The appends of the messages `part` retains without a row,
/// `[log_start, rows_at)`, oldest first and at most `limit`, read from this
/// node's queue-log index: `(base, end, created_at)` in a [`TxnsRow`] with no
/// hashes, the shape [`stale_boundary`] judges.
fn unrowed_appends(
    q: &QLogReader,
    part: &rows::PartitionRow,
    pid: Pid,
    rows_at: u64,
    limit: usize,
) -> Result<Vec<(u64, TxnsRow)>> {
    let queue_id = QLogReader::queue_id_of(&part.tenant, &part.queue);
    let mut out: Vec<(u64, TxnsRow)> = Vec::new();
    q.claim_frames(
        queue_id,
        pid,
        part.log_start,
        rows_at,
        false,
        &mut |base, end, created_at_us, _hashes| {
            out.push((
                base,
                TxnsRow {
                    end,
                    created_at_us,
                    hashes: Vec::new(),
                },
            ));
            out.len() < limit.max(1)
        },
    )
    .map_err(|e| StoreError::Io(format!("qlog retention walk: {e}")))?;
    Ok(out)
}

/// The partition-cleanup half of [`judge_partition`]: delete a partition idle
/// past the cleanup age.
fn idle_verdict<R: Reads + ?Sized>(
    r: &R,
    now_us: i64,
    cfg: &Config,
    pid: Pid,
    part: &rows::PartitionRow,
) -> Result<Option<Verdict>> {
    if cfg.partition_cleanup_enabled
        && partition_dead(
            r,
            pid,
            part,
            now_us.saturating_sub(cfg.partition_cleanup_days.max(1) * 86_400 * 1_000_000),
            now_us,
        )?
    {
        return Ok(Some(Verdict::Delete));
    }
    Ok(None)
}

/// One slice of the background retention walk
/// ([`crate::rsm::retention_scan`]).
pub(crate) struct Slice {
    pub proposals: Vec<crate::rsm::retention_scan::Proposal>,
    /// Partitions looked at.
    pub visited: usize,
    /// The slice reached the last partition: the cursor is back at the first.
    pub wrapped: bool,
    /// Partitions whose step stopped at its row limit ([`Verdict::Watermark`]'s
    /// `more`): they are due again as soon as the step has landed.
    pub more: Vec<Pid>,
}

/// Up to `limit` partitions in pid order from `*cursor` — every partition of
/// the node in turn, whatever queue it is in — judged as [`plan`]'s walk judges
/// them. Advances `*cursor`, wrapping to the first partition after the last.
pub(crate) fn scan_slice<R: Reads + ?Sized>(
    r: &R,
    now_us: i64,
    cfg: &Config,
    cursor: &mut Pid,
    limit: usize,
) -> Result<Slice> {
    use crate::rsm::retention_scan::Proposal;
    use std::collections::hash_map::Entry;
    use std::collections::HashMap;

    let limit = limit.max(1);
    let mut parts = Vec::with_capacity(limit);
    let mut corrupt = false;
    r.scan_raw(
        Keyspace::Partitions,
        &keys::pid(*cursor),
        &[],
        limit,
        &mut |key, value| match (keys::pid_of(key), rows::partition_decode(value)) {
            (Some(pid), Ok(row)) => {
                parts.push((pid, row));
                true
            }
            _ => {
                corrupt = true;
                false
            }
        },
    )?;
    if corrupt {
        return Err(StoreError::corrupt(Keyspace::Partitions, "partition row"));
    }
    let wrapped = parts.len() < limit;
    *cursor = match parts.last() {
        Some((pid, _)) if !wrapped => pid.saturating_add(1),
        _ => 0,
    };
    let mut out = Slice {
        proposals: Vec::new(),
        visited: parts.len(),
        wrapped,
        more: Vec::new(),
    };
    let mut queues: HashMap<(String, String), Option<Cutoffs>> = HashMap::new();
    for (pid, part) in parts {
        if r.garbage(pid)?.is_some() {
            continue;
        }
        let cut = match queues.entry((part.tenant.clone(), part.queue.clone())) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(v) => {
                let cut = r
                    .queue(&part.tenant, &part.queue)?
                    .map(|qcfg| queue_cutoffs(r, now_us, cfg, &part.tenant, &part.queue, &qcfg));
                v.insert(cut)
            }
        };
        // A partition whose queue row is gone is the garbage path's, as in
        // the walk over queue rows.
        let Some(cut) = cut.as_ref() else {
            continue;
        };
        match judge_partition(r, now_us, cfg, pid, &part, cut)? {
            Some(Verdict::Watermark {
                log_start,
                txns_start,
                rows,
                more,
            }) => {
                out.proposals.push(Proposal::Watermark {
                    pid,
                    log_start,
                    txns_start,
                    rows,
                });
                if more {
                    out.more.push(pid);
                }
            }
            Some(Verdict::Delete) => out.proposals.push(Proposal::Delete { pid }),
            None => {}
        }
    }
    Ok(out)
}

/// [`scan_slice`] for the partitions named in `pids` (the hot ones of
/// [`crate::rsm::retention_scan`], whose last step was full): each judged as
/// the walk judges it.
pub(crate) fn scan_pids<R: Reads + ?Sized>(
    r: &R,
    now_us: i64,
    cfg: &Config,
    pids: &[Pid],
) -> Result<Slice> {
    use crate::rsm::retention_scan::Proposal;
    use std::collections::hash_map::Entry;
    use std::collections::HashMap;

    let mut out = Slice {
        proposals: Vec::new(),
        visited: pids.len(),
        wrapped: false,
        more: Vec::new(),
    };
    let mut queues: HashMap<(String, String), Option<Cutoffs>> = HashMap::new();
    for &pid in pids {
        if r.garbage(pid)?.is_some() {
            continue;
        }
        let Some(part) = r.partition(pid)? else {
            continue;
        };
        let cut = match queues.entry((part.tenant.clone(), part.queue.clone())) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(v) => {
                let cut = r
                    .queue(&part.tenant, &part.queue)?
                    .map(|qcfg| queue_cutoffs(r, now_us, cfg, &part.tenant, &part.queue, &qcfg));
                v.insert(cut)
            }
        };
        let Some(cut) = cut.as_ref() else {
            continue;
        };
        if let Some(Verdict::Watermark {
            log_start,
            txns_start,
            rows,
            more,
        }) = judge_partition(r, now_us, cfg, pid, &part, cut)?
        {
            out.proposals.push(Proposal::Watermark {
                pid,
                log_start,
                txns_start,
                rows,
            });
            if more {
                out.more.push(pid);
            }
        }
    }
    Ok(out)
}

/// [`stale_boundary_n`]'s target alone.
#[cfg(test)]
fn stale_boundary(
    rows: &[(u64, TxnsRow)],
    from: u64,
    cutoff_us: i64,
    cap: Option<u64>,
    limit: usize,
) -> u64 {
    stale_boundary_n(rows, from, cutoff_us, cap, limit).0
}

/// How far the appends of `rows` at or past `from` are stale — older than
/// `cutoff_us`, and wholly below `cap` — taking at most `limit` of them: one
/// past the last stale append's end, and how many were taken.
fn stale_boundary_n(
    rows: &[(u64, TxnsRow)],
    from: u64,
    cutoff_us: i64,
    cap: Option<u64>,
    limit: usize,
) -> (u64, usize) {
    let mut target = from;
    let mut taken = 0usize;
    for (base, row) in rows {
        if *base < from {
            continue;
        }
        if row.created_at_us >= cutoff_us || cap.is_some_and(|cap| row.end >= cap) {
            break;
        }
        target = row.end.saturating_add(1);
        taken += 1;
        if taken >= limit.max(1) {
            break;
        }
    }
    (target, taken)
}

fn sink_floor<R: Reads + ?Sized>(
    r: &R,
    now_us: i64,
    tenant: &str,
    queue: &str,
    cfg: &crate::rsm::effect::QueueConfig,
) -> i64 {
    if cfg.retention_sink_hold.is_empty() {
        return i64::MAX;
    }
    let cap =
        now_us.saturating_sub(i64::from(cfg.retention_sink_hold_max_seconds.max(60)) * 1_000_000);
    let escaped = percent_escape(queue);
    let key = format!("s3:{}:{}:committed", cfg.retention_sink_hold, escaped);
    let committed = r
        .kv(tenant, "queen-s3", &key)
        .ok()
        .flatten()
        .filter(|row| row.live(now_us))
        .and_then(|row| serde_json::from_slice::<serde_json::Value>(&row.value).ok())
        .and_then(|v| v.get("tEnd").and_then(|x| x.as_str()).map(str::to_string))
        .and_then(|s| crate::util::parse_iso_ms(&s))
        .map(|ms| ms.saturating_mul(1_000).saturating_sub(60_000_000));
    committed.map_or(cap, |at| at.max(cap))
}

fn percent_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'.' | b'_') {
            out.push(*byte as char);
        } else {
            use std::fmt::Write;
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

pub(crate) fn partition_dead<R: Reads + ?Sized>(
    r: &R,
    pid: Pid,
    part: &rows::PartitionRow,
    cutoff_us: i64,
    now_us: i64,
) -> Result<bool> {
    if part.created_at_us >= cutoff_us
        || part.last_write_at_us >= cutoff_us
        || part.log_start < part.last_offset.saturating_add(1).max(0) as u64
    {
        return Ok(false);
    }
    let mut veto = false;
    r.scan_raw(
        Keyspace::DlqByPos,
        &keys::dlq_by_pos_pid_prefix(pid),
        &keys::dlq_by_pos_pid_prefix(pid),
        1,
        &mut |_, _| {
            veto = true;
            false
        },
    )?;
    if veto {
        return Ok(false);
    }
    r.scan_cursors(pid, usize::MAX, &mut |_group, cursor| {
        let live_lease = cursor.batch_end.is_some()
            && cursor
                .lease_expires_at_us
                .is_none_or(|expires| expires > now_us);
        let recent = cursor.created_at_us >= cutoff_us
            || cursor
                .lease_acquired_at_us
                .is_some_and(|at| at >= cutoff_us)
            || cursor.lease_expires_at_us.is_some_and(|at| at >= cutoff_us);
        if live_lease || recent {
            veto = true;
            false
        } else {
            true
        }
    })?;
    if veto {
        return Ok(false);
    }
    r.scan_raw(
        Keyspace::StreamsState,
        &[],
        &[],
        usize::MAX,
        &mut |key, _| {
            if keys::streams_state_parts(key).is_some_and(|(_, state_pid, _)| state_pid == pid) {
                veto = true;
                false
            } else {
                true
            }
        },
    )?;
    Ok(!veto)
}
