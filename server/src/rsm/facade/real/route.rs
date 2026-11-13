//! The receiver's side of routing (PLAN_ROUTING.md): a reader's catch-up
//! from history, and a filtered group's hidden queue.
//!
//! # Catch-up (§8)
//!
//! A reader that starts with history (`start: "all"`, a filtered group with
//! `subscriptionMode('all')` or `subscriptionFrom`) is created
//! `catching_up`: the planner links no live push into it. The catch-up walks
//! each source partition's retained messages in order, from its mark (or the
//! partition's log start), takes those the reader's `where` takes, and pushes
//! their links into the reader as an ordinary push of link frames. Then it
//! asks for the flip with its marks; the planner checks every source
//! partition's head against them (`planner::route::check_flip`) and refuses a
//! flip that would leave a message behind, and the catch-up reads the rest
//! and asks again. So every source message from the start on is in the reader
//! once, in its partition's order, and the first live link comes after the
//! last read one.
//!
//! It runs inside the request that needs it, within a time budget; past the
//! budget it records its marks on the reader (`RouteCfg::marks`), answers
//! that the reader is still catching up, and the next `configure` (or a
//! filtered group's next pop) goes on from there.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::{Prebuilt, PushInput, RaftFacade};
use crate::frames::{pack_frames, unpack_frames_ref, FrameIn, LinkRef};
use crate::rsm::effect::{Effect, QueueConfig};
use crate::rsm::facade::{ReqCtx, RsmError};
use crate::rsm::qlog::set::QLogReader;
use crate::rsm::route::{self, cond};
use crate::rsm::store::{Store, TypedReads};
use crate::util::uuidv7_bytes;

/// How long one request spends catching a reader up before it answers.
pub(super) const CATCH_UP_BUDGET: Duration = Duration::from_secs(10);

/// Links pushed per command of a catch-up.
const CATCH_UP_BATCH: usize = 1_000;

/// The flips a catch-up asks for in one request before it answers: each
/// refused one reads only what came since the last.
const CATCH_UP_FLIPS: usize = 64;

/// A link waiting to be pushed: its reader partition, transaction id, id and
/// frame.
struct Pending {
    partition: String,
    txn: String,
    id: [u8; 16],
    frame: bytes::Bytes,
}

impl RaftFacade {
    /// Catch reader `queue` up within `budget`. `Ok(true)`: it takes live
    /// links now (or never was catching up); `Ok(false)`: the budget ran out,
    /// its marks are recorded, and a later call goes on.
    pub(super) async fn catch_up(
        &self,
        ctx: &ReqCtx,
        queue: &str,
        budget: Duration,
    ) -> Result<bool, RsmError> {
        let started = Instant::now();
        let Some(ql) = self.qlog_reader.clone() else {
            return Err(RsmError::Rejected {
                code: "bad_request".into(),
                message: "a catch-up reads the queue log (QUEEN_RAFT_QLOG)".into(),
            });
        };
        let mut flips = 0usize;
        loop {
            let Some(cfg) = self.queue_config_of(&ctx.tenant, queue).await? else {
                return Ok(true);
            };
            let Some(r) = cfg.route() else {
                return Ok(true);
            };
            if !r.catching_up {
                return Ok(true);
            }
            let mut marks: HashMap<u64, u64> = r.marks.iter().copied().collect();
            let finished = self
                .catch_up_pass(ctx, queue, &cfg, &ql, &mut marks, started, budget)
                .await?;
            let mut next = cfg.clone();
            let nr = next.route_mut();
            let mut m: Vec<(u64, u64)> = marks.into_iter().collect();
            m.sort_unstable();
            nr.marks = m;
            if !finished {
                // Out of time: keep the marks, go on next time.
                self.submit_reader(ctx, queue, next).await?;
                return Ok(false);
            }
            nr.catching_up = false;
            match self.submit_reader(ctx, queue, next.clone()).await {
                Ok(()) => return Ok(true),
                // Behind: messages came since the pass read the heads.
                Err(RsmError::Retry { .. })
                    if flips + 1 < CATCH_UP_FLIPS && started.elapsed() < budget =>
                {
                    flips += 1;
                    // Record where this pass got to, still catching up, and read on.
                    let mut keep = next;
                    keep.route_mut().catching_up = true;
                    self.submit_reader(ctx, queue, keep).await?;
                }
                Err(RsmError::Retry { .. }) => {
                    let mut keep = next;
                    keep.route_mut().catching_up = true;
                    self.submit_reader(ctx, queue, keep).await?;
                    return Ok(false);
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// One reader config upsert, with its own request id.
    async fn submit_reader(
        &self,
        ctx: &ReqCtx,
        queue: &str,
        cfg: QueueConfig,
    ) -> Result<(), RsmError> {
        let mut c = ctx.clone();
        c.request_id = uuidv7_bytes();
        c.scope = None;
        self.submit_effects_internal(
            &c,
            vec![Effect::QueueUpsert {
                tenant: ctx.tenant.clone(),
                queue: queue.to_string(),
                cfg,
            }],
        )
        .await
    }

    /// Read every source partition from its mark to its committed head and
    /// push the links the reader takes. `Ok(false)` when the budget ran out
    /// first (the marks say how far it got).
    #[allow(clippy::too_many_arguments)]
    async fn catch_up_pass(
        &self,
        ctx: &ReqCtx,
        queue: &str,
        cfg: &QueueConfig,
        ql: &QLogReader,
        marks: &mut HashMap<u64, u64>,
        started: Instant,
        budget: Duration,
    ) -> Result<bool, RsmError> {
        let Some(r) = cfg.route() else {
            return Ok(true);
        };
        for source in r.sources.clone() {
            let conds = route::compiled(&source.where_json);
            let partition_by = r.partition_by.clone();
            // The source's partitions as committed now: (pid, name, start, head).
            let store = self.store.clone();
            let tenant = ctx.tenant.clone();
            let sq = source.queue.clone();
            let parts: Vec<(u64, String, u64, u64)> = tokio::task::spawn_blocking(move || {
                store.read(|rd| {
                    let mut pids = Vec::new();
                    rd.scan_queue_partitions(&tenant, &sq, None, usize::MAX, &mut |pid| {
                        pids.push(pid);
                        true
                    })?;
                    let mut out = Vec::with_capacity(pids.len());
                    for pid in pids {
                        if let Some(p) = rd.partition(pid)? {
                            out.push((
                                pid,
                                p.partition,
                                p.log_start,
                                (p.last_offset + 1).max(0) as u64,
                            ));
                        }
                    }
                    Ok(out)
                })
            })
            .await
            .map_err(|e| RsmError::Internal(format!("catch-up read task: {e}")))?
            .map_err(|e| RsmError::Internal(format!("catch-up read: {e}")))?;
            let qid = QLogReader::queue_id_of(&ctx.tenant, &source.queue);
            for (pid, name, log_start, head) in parts {
                let mut off = marks.get(&pid).copied().unwrap_or(log_start).max(log_start);
                let mut pending: Vec<Pending> = Vec::new();
                while off < head {
                    let ql2 = ql.clone();
                    let rec = tokio::task::spawn_blocking(move || ql2.read_owned(qid, pid, off))
                        .await
                        .map_err(|e| RsmError::Internal(format!("catch-up read task: {e}")))?
                        .map_err(|e| RsmError::Internal(format!("catch-up qlog read: {e}")))?;
                    let Some(rec) = rec else {
                        off += 1;
                        continue;
                    };
                    let Some(frames) = unpack_frames_ref(&rec.payload) else {
                        off = rec.base_offset + u64::from(rec.count.max(1));
                        continue;
                    };
                    for (i, f) in frames.iter().enumerate() {
                        let o = rec.base_offset + i as u64;
                        if o < off || o >= head {
                            continue;
                        }
                        // What the condition reads: a link's message is read
                        // through (a source that is a reader itself).
                        let resolved;
                        let (payload, headers, link) = match f.link {
                            Some(l) => {
                                resolved = route::link::resolve(
                                    ql,
                                    pack_frames(&[FrameIn {
                                        message_id: f.message_id,
                                        txn: f.txn,
                                        trace_id: None,
                                        producer_sub: None,
                                        payload: &[],
                                        encrypted: false,
                                        headers: f.headers,
                                        link: Some(l),
                                    }]),
                                    &mut |_| Some(0),
                                );
                                match unpack_frames_ref(&resolved)
                                    .and_then(|v| v.into_iter().next())
                                {
                                    Some(m) if !m.is_link() => {
                                        let p = if m.encrypted {
                                            self.encryption.decrypt_payload_bytes(m.payload)
                                        } else {
                                            Some(m.payload.to_vec())
                                        };
                                        (p, m.headers.map(<[u8]>::to_vec), l)
                                    }
                                    _ => continue,
                                }
                            }
                            None => {
                                let p = if f.encrypted {
                                    self.encryption.decrypt_payload_bytes(f.payload)
                                } else {
                                    Some(f.payload.to_vec())
                                };
                                (
                                    p,
                                    f.headers.map(<[u8]>::to_vec),
                                    LinkRef {
                                        qid,
                                        pid,
                                        offset: o,
                                    },
                                )
                            }
                        };
                        let doc: Option<serde_json::Value> =
                            if cond::wants_payload(&conds) || partition_by.starts_with("data.") {
                                payload
                                    .as_deref()
                                    .and_then(|p| serde_json::from_slice(p).ok())
                            } else {
                                None
                            };
                        let subject = cond::Subject {
                            queue: &source.queue,
                            partition: &name,
                            txn: f.txn,
                            offset: None,
                            position: None,
                            doc: doc.as_ref(),
                            headers: headers.as_deref(),
                        };
                        if !cond::all_hold(&conds, &subject) {
                            continue;
                        }
                        let Some(tp) = route::target_partition(
                            &partition_by,
                            &name,
                            doc.as_ref(),
                            headers.as_deref(),
                        ) else {
                            continue;
                        };
                        let frame = pack_frames(&[FrameIn {
                            message_id: f.message_id,
                            txn: f.txn,
                            trace_id: None,
                            producer_sub: None,
                            payload: &[],
                            encrypted: false,
                            headers: None,
                            link: Some(link),
                        }]);
                        pending.push(Pending {
                            partition: tp,
                            txn: f.txn.to_string(),
                            id: f.message_id,
                            frame: frame.into(),
                        });
                    }
                    off = (rec.base_offset + u64::from(rec.count)).max(off + 1);
                    if pending.len() >= CATCH_UP_BATCH {
                        self.push_links(ctx, queue, &mut pending).await?;
                        marks.insert(pid, off.min(head));
                        if started.elapsed() >= budget {
                            return Ok(false);
                        }
                    }
                }
                self.push_links(ctx, queue, &mut pending).await?;
                marks.insert(pid, head);
                if started.elapsed() >= budget {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    /// Push `pending` links into reader `queue` (one push), and empty it.
    async fn push_links(
        &self,
        ctx: &ReqCtx,
        queue: &str,
        pending: &mut Vec<Pending>,
    ) -> Result<(), RsmError> {
        if pending.is_empty() {
            return Ok(());
        }
        let mut c = ctx.clone();
        c.request_id = uuidv7_bytes();
        c.scope = None;
        c.producer_sub = None;
        let inputs: Vec<PushInput<'_>> = pending
            .iter()
            .map(|p| PushInput {
                queue,
                partition: Some(p.partition.as_str()),
                payload: &[],
                txn: Some(p.txn.as_str()),
                prebuilt: Some(Prebuilt {
                    id: p.id,
                    txn: p.txn.as_str(),
                    frame: p.frame.clone(),
                    unique: false,
                }),
                headers: None,
            })
            .collect();
        self.push_inputs(&c, &inputs, None, false, Some(Default::default()))
            .await?;
        pending.clear();
        Ok(())
    }

    /// The queue a pop of `group` on `queue` reads: the group's hidden queue
    /// when it filters (`where` given, or registered with one before), else
    /// `queue` itself. The first pop with a `where` creates the hidden queue;
    /// a later one with another `where` is refused (`where_mismatch`).
    pub(crate) async fn filtered_queue(
        &self,
        ctx: &ReqCtx,
        queue: &str,
        group: &str,
        where_raw: Option<&str>,
        start_all: bool,
    ) -> Result<String, RsmError> {
        let hidden = route::hidden_queue(queue, group);
        let existing = self.queue_config_of(&ctx.tenant, &hidden).await?;
        let Some(where_raw) = where_raw.filter(|w| !w.trim().is_empty()) else {
            // No `where` on this pop: a group that filters keeps filtering.
            return Ok(match existing {
                Some(c) if c.is_reader() => {
                    if c.route().is_some_and(|r| r.catching_up) {
                        self.catch_up(ctx, &hidden, CATCH_UP_BUDGET).await?;
                    }
                    hidden
                }
                _ => queue.to_string(),
            });
        };
        if group.is_empty() || group == super::QUEUE_MODE_GROUP {
            return Err(RsmError::Rejected {
                code: "bad_request".into(),
                message: "where filters a consumer group: name one (consumerGroup)".into(),
            });
        }
        if !self.cluster_allows(crate::rsm::effect::VERSION_8) {
            return Err(super::headers_need_version_8());
        }
        let rules = cond::Rules::route();
        let as_value: serde_json::Value = match serde_json::from_str::<serde_json::Value>(where_raw)
        {
            Ok(v @ serde_json::Value::Array(_)) => v,
            _ => serde_json::Value::String(where_raw.to_string()),
        };
        let conds = cond::parse_value(&as_value, &rules).map_err(|e| RsmError::Rejected {
            code: "bad_where".into(),
            message: e,
        })?;
        let where_json = cond::to_json(&conds).to_string();
        if let Some(c) = existing {
            let same = c
                .route()
                .and_then(|r| r.sources.first())
                .is_some_and(|s| s.where_json == where_json);
            if !same {
                let current = c
                    .route()
                    .and_then(|r| r.sources.first())
                    .map(|s| cond::to_text(&route::compiled(&s.where_json)))
                    .unwrap_or_default();
                return Err(RsmError::Rejected {
                    code: "where_mismatch".into(),
                    message: format!(
                        "group {group} on {queue} filters with where {current:?}; change it with POST /api/v1/consumer-groups/{group}/subscription, or use another group"
                    ),
                });
            }
            if c.route().is_some_and(|r| r.catching_up) {
                self.catch_up(ctx, &hidden, CATCH_UP_BUDGET).await?;
            }
            return Ok(hidden);
        }
        // A group that already reads the queue unfiltered keeps its cursor
        // there: a filter starts as a new group (PLAN_ROUTING.md §4.2).
        let store = self.store.clone();
        let (t, q, g) = (ctx.tenant.clone(), queue.to_string(), group.to_string());
        let registered = tokio::task::spawn_blocking(move || store.read(|r| r.group(&t, &q, &g)))
            .await
            .map_err(|e| RsmError::Internal(format!("group read task: {e}")))?
            .map_err(|e| RsmError::Internal(format!("group read: {e}")))?;
        if registered.is_some() {
            return Err(RsmError::Rejected {
                code: "where_mismatch".into(),
                message: format!(
                    "group {group} reads {queue} without a where; a filter starts as a new group (another name, subscriptionFrom now)"
                ),
            });
        }
        let Some(source) = self.queue_config_of(&ctx.tenant, queue).await? else {
            return Err(RsmError::Rejected {
                code: "queue_not_found".into(),
                message: format!(
                    "{queue} does not exist: a filtered group reads an existing queue"
                ),
            });
        };
        let mut cfg = source.clone();
        cfg.id = uuidv7_bytes();
        cfg.created_at_us = super::wall_micros();
        cfg.route = None;
        route::follow_source(&mut cfg, &source);
        {
            let r = cfg.route_mut();
            r.sources = vec![crate::rsm::effect::SourceRule {
                queue: queue.to_string(),
                where_json,
            }];
            r.hidden_group = group.to_string();
            r.catching_up = start_all;
        }
        let mut c = ctx.clone();
        c.request_id = uuidv7_bytes();
        c.scope = None;
        self.submit_effects_internal(
            &c,
            vec![Effect::QueueUpsert {
                tenant: ctx.tenant.clone(),
                queue: hidden.clone(),
                cfg,
            }],
        )
        .await?;
        if start_all {
            self.catch_up(ctx, &hidden, CATCH_UP_BUDGET).await?;
        }
        Ok(hidden)
    }

    /// Change a filtered group's `where` (`POST /consumer-groups/:g/subscription`
    /// with `where`): applies from the next push.
    pub(crate) async fn set_group_where(
        &self,
        ctx: &ReqCtx,
        queue: &str,
        group: &str,
        where_raw: &serde_json::Value,
    ) -> Result<bool, RsmError> {
        let hidden = route::hidden_queue(queue, group);
        let Some(mut cfg) = self.queue_config_of(&ctx.tenant, &hidden).await? else {
            return Ok(false);
        };
        let conds = cond::parse_value(where_raw, &cond::Rules::route()).map_err(|e| {
            RsmError::Rejected {
                code: "bad_where".into(),
                message: e,
            }
        })?;
        if let Some(s) = cfg.route_mut().sources.first_mut() {
            s.where_json = cond::to_json(&conds).to_string();
        }
        let mut c = ctx.clone();
        c.request_id = uuidv7_bytes();
        self.submit_effects_internal(
            &c,
            vec![Effect::QueueUpsert {
                tenant: ctx.tenant.clone(),
                queue: hidden,
                cfg,
            }],
        )
        .await?;
        Ok(true)
    }
}

impl RaftFacade {
    /// The queue a pop reads: a filtered group's hidden queue
    /// ([`Self::filtered_queue`]) for a named group with a `where`, or one
    /// registered with one; else the queue named. A client never names a
    /// hidden queue itself.
    pub(super) async fn pop_target(
        &self,
        ctx: &ReqCtx,
        queue: String,
        group: Option<&str>,
        options: &crate::rsm::facade::PopOptions,
    ) -> Result<String, RsmError> {
        if route::is_hidden(&queue) {
            return Err(RsmError::Rejected {
                code: "bad_request".into(),
                message: "a queue name may not contain U+0001".into(),
            });
        }
        let Some(group) = group.filter(|g| !g.is_empty()) else {
            if options
                .where_filter
                .as_deref()
                .is_some_and(|w| !w.trim().is_empty())
            {
                return Err(RsmError::Rejected {
                    code: "bad_request".into(),
                    message: "where filters a consumer group: name one (consumerGroup)".into(),
                });
            }
            return Ok(queue);
        };
        // A pop with no `where`: one read of the queue's row in the state's
        // memory, and the queue itself unless this group filters it.
        if options.where_filter.is_none() {
            if !self.cluster_allows(crate::rsm::effect::VERSION_8) {
                return Ok(queue);
            }
            let hidden = route::hidden_queue(&queue, group);
            let filters = self
                .store
                .read(|r| r.queue(&ctx.tenant, &queue))
                .ok()
                .flatten()
                .is_some_and(|c| c.route().is_some_and(|r| r.readers.contains(&hidden)));
            if !filters {
                return Ok(queue);
            }
        }
        let start_all =
            options.subscription_mode == "all" || options.subscription_from_us.is_some();
        self.filtered_queue(
            ctx,
            &queue,
            group,
            options.where_filter.as_deref(),
            start_all,
        )
        .await
    }

    /// Submit effects as one command of this node's own (a catch-up's
    /// marks, a filtered group's hidden queue), answered as `configure` is.
    pub(super) async fn submit_effects_internal(
        &self,
        ctx: &ReqCtx,
        effects: Vec<Effect>,
    ) -> Result<(), RsmError> {
        use crate::rsm::batcher::{Command, Reply};
        use crate::rsm::planner::EffectsCommand;
        let cmd = Command::Effects(EffectsCommand {
            request_id: ctx.request_id,
            tenant: ctx.tenant.clone(),
            effects,
        });
        match self.submit(ctx, cmd).await? {
            Reply::Done { .. } => Ok(()),
            Reply::Refused(r) if r.code == crate::rsm::link::STANDBY_CODE => Err(RsmError::Standby),
            Reply::Refused(r) if r.retryable => Err(RsmError::Retry { leader_hint: None }),
            Reply::Refused(r) => Err(RsmError::Rejected {
                code: r.code,
                message: r.message,
            }),
            Reply::Retry { hint } => Err(RsmError::Retry {
                leader_hint: hint.map(|h| h.to_string()),
            }),
        }
    }
}
