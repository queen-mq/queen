//! Leader-only Phase-2 maintenance planning.
//!
//! The clock is supplied by the batcher and every result is an ordinary
//! replicated effect. Nothing in this module mutates the store: followers run
//! the same deterministic apply path as for client commands.

use crate::rsm::dedup::TxnsRow;
use crate::rsm::effect::{Effect, Pid};
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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            row_limit: 1_000,
            trace_retention_s: 7 * 24 * 60 * 60,
            partition_cleanup_enabled: true,
            partition_cleanup_days: 30,
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

/// Reclaim whole sealed queue-log files made dead by already-committed txns
/// watermarks.  This is intentionally node-local: the positions and file
/// boundaries differ per replica.  It runs from the blocking maintenance
/// cycle, never on a Tokio worker, and is safe to repeat after a crash.
pub fn reclaim_qlogs<R: Reads + ?Sized>(r: &R, qlogs: &QLogReader) -> Result<usize> {
    use std::collections::HashMap;

    let mut queues: HashMap<u64, HashMap<Pid, u64>> = HashMap::new();
    let mut corrupt = false;
    r.scan_raw(
        Keyspace::Partitions,
        &[],
        &[],
        usize::MAX,
        &mut |key, value| match (keys::pid_of(key), rows::partition_decode(value)) {
            (Some(pid), Ok(row)) => {
                let qid = QLogReader::queue_id_of(&row.tenant, &row.queue);
                queues.entry(qid).or_default().insert(pid, row.txns_start);
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

    let mut removed = 0usize;
    for (qid, starts) in queues {
        removed += qlogs
            .reclaim_below_txns(qid, &starts)
            .map_err(|e| StoreError::Io(format!("qlog retention: {e}")))?;
    }
    Ok(removed)
}

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

    for (tenant, queue, qcfg) in queues {
        if budget == 0 {
            out.more = true;
            break;
        }
        let sink_floor = sink_floor(r, now_us, &tenant, &queue, &qcfg);
        let all_cutoff = (qcfg.retention_enabled && qcfg.retention_seconds > 0)
            .then(|| now_us.saturating_sub(qcfg.retention_seconds as i64 * 1_000_000))
            .map(|v| v.min(sink_floor));
        let completed_cutoff = (qcfg.retention_enabled && qcfg.completed_retention_seconds > 0)
            .then(|| now_us.saturating_sub(qcfg.completed_retention_seconds as i64 * 1_000_000))
            .map(|v| v.min(sink_floor));
        let max_wait_cutoff = (qcfg.max_wait_time_seconds > 0)
            .then(|| now_us.saturating_sub(qcfg.max_wait_time_seconds as i64 * 1_000_000));
        let txn_window_s = i64::from(qcfg.dedup_window_seconds)
            .max(i64::from(qcfg.completed_retention_seconds))
            .max(900);
        let txns_cutoff = now_us.saturating_sub(txn_window_s * 1_000_000);

        let mut pids = Vec::new();
        r.scan_queue_partitions(&tenant, &queue, None, usize::MAX, &mut |pid| {
            pids.push(pid);
            true
        })?;
        for pid in pids {
            if budget == 0 {
                out.more = true;
                break;
            }
            if r.garbage(pid)?.is_some() {
                continue;
            }
            let Some(part) = r.partition(pid)? else {
                continue;
            };
            let mut segments = Vec::new();
            let prefix = keys::txns_prefix(pid);
            let from = keys::txns(pid, part.txns_start);
            let mut corrupt = false;
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
            if corrupt {
                return Err(crate::rsm::store::StoreError::corrupt(
                    Keyspace::Txns,
                    "txns row",
                ));
            }

            let mut log_target = part.log_start;
            if let Some(cutoff) = all_cutoff {
                log_target = log_target.max(stale_boundary(
                    &segments,
                    part.log_start,
                    cutoff,
                    None,
                    cfg.row_limit,
                ));
            }
            if let Some(cutoff) = max_wait_cutoff {
                log_target = log_target.max(stale_boundary(
                    &segments,
                    part.log_start,
                    cutoff,
                    None,
                    cfg.row_limit,
                ));
            }
            if let Some(cutoff) = completed_cutoff {
                let mut min_committed: Option<i64> = None;
                r.scan_cursors(pid, usize::MAX, &mut |_group, cursor| {
                    min_committed = Some(
                        min_committed.map_or(cursor.committed, |old| old.min(cursor.committed)),
                    );
                    true
                })?;
                if let Some(cap) = min_committed.map(|v| v.saturating_add(1).max(0) as u64) {
                    log_target = log_target.max(stale_boundary(
                        &segments,
                        part.log_start,
                        cutoff,
                        Some(cap),
                        cfg.row_limit,
                    ));
                }
            }

            let txn_target = stale_boundary(
                &segments,
                part.txns_start,
                txns_cutoff,
                Some(log_target),
                cfg.row_limit,
            )
            .min(log_target);
            if log_target > part.log_start || txn_target > part.txns_start {
                out.effects.push(Effect::Watermark {
                    pid,
                    log_start: log_target,
                    txns_start: txn_target,
                });
                budget = budget.saturating_sub(1);
            } else if cfg.partition_cleanup_enabled
                && partition_dead(
                    r,
                    pid,
                    &part,
                    now_us.saturating_sub(cfg.partition_cleanup_days.max(1) * 86_400 * 1_000_000),
                    now_us,
                )?
            {
                out.effects.push(Effect::PartitionDelete { pid });
                budget = budget.saturating_sub(1);
            }
        }
    }

    // D18: only log an expiry command when the oldest index row is due.
    let trace_cutoff = now_us.saturating_sub(cfg.trace_retention_s.max(1) * 1_000_000);
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

fn stale_boundary(
    rows: &[(u64, TxnsRow)],
    from: u64,
    cutoff_us: i64,
    cap: Option<u64>,
    limit: usize,
) -> u64 {
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
    target
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

fn partition_dead<R: Reads + ?Sized>(
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
