//! Routing in the planner (PLAN_ROUTING.md §3): a push to a queue that other
//! queues read appends, in the same entry, a LINK frame to every reader whose
//! `where` takes the message.
//!
//! The planner decides once and writes the answer: the entry carries the
//! links, so followers and replays never evaluate a condition, and a rule
//! committed at entry N applies to every push planned after N (the overlay
//! holds the catalog of every entry in flight).
//!
//! A reader's link always points at the ORIGINAL message: a reader that is
//! itself read (a chain) passes the same addresses on, evaluated against the
//! original payload and headers, under its own queue and partition names.
//! Only what a reader's own append CREATED goes on (a link it already holds,
//! within its dedup window, is a duplicate and stops there).
//!
//! Pushes into a reader are refused unless every item is a link: a reading
//! queue takes only what its sources route to it. A link pushed into an
//! ordinary queue (a transaction's `link`) makes it a HOLDER of the source,
//! recorded on the source's route, so retention waits for it.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use serde_json::Value;

use crate::frames::{pack_frames, unpack_frames_ref, FrameIn, LinkRef};
use crate::rsm::effect::{Effect, Pid, QueueConfig};
use crate::rsm::entry::{Outcome, PushVerdict};
use crate::rsm::qlog::set::QLogReader;
use crate::rsm::route::{self, cond};
use crate::rsm::store::Reads;

use super::{Overlay, Plan, Planner, PushCommand, PushItem, Refusal};

/// The most partitions [`Planner::in_use`] looks at before it takes a queue
/// to be in use.
const UNUSED_PARTITIONS: usize = 1_024;

/// Messages that met no reader's partition field (`partitionBy` named a field
/// the message does not have): the `queen_route_unrouted_total` counter.
pub static UNROUTED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Links appended by routes (`queen_route_links_total`).
pub static ROUTED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The node's payload key, for a condition on the payload of an encrypted
/// queue (the leader decrypts to decide, and writes only the decision).
fn encryption() -> &'static Arc<crate::encryption::Encryption> {
    static KEY: OnceLock<Arc<crate::encryption::Encryption>> = OnceLock::new();
    KEY.get_or_init(crate::encryption::Encryption::from_env)
}

/// One message on its way through routes: its dedup hash, id and transaction
/// id, the address of the original, and what a condition may read.
#[derive(Clone)]
pub(super) struct Routed {
    pub hash: [u8; 16],
    pub message_id: [u8; 16],
    pub txn: String,
    pub link: LinkRef,
    /// The payload in clear (JSON), when the original holds one.
    pub payload: Option<Arc<Vec<u8>>>,
    pub headers: Option<Arc<Vec<u8>>>,
    /// The payload parsed, once, when a condition reads it.
    doc: std::cell::OnceCell<Option<Value>>,
}

impl Routed {
    fn doc(&self) -> Option<&Value> {
        self.doc
            .get_or_init(|| {
                self.payload
                    .as_ref()
                    .and_then(|p| serde_json::from_slice(p).ok())
            })
            .as_ref()
    }
}

/// Whether every frame of the items is a link (a frame's flags byte is the
/// fifth, after its length).
pub(super) fn all_links(items: &[PushItem]) -> bool {
    items.iter().all(|it| is_link_frame(&it.frame))
}

pub(crate) fn is_link_frame(frame: &[u8]) -> bool {
    frame.len() > 4 && frame[4] & crate::frames::FLAG_LINK != 0
}

impl<'a, R: Reads + ?Sized> Planner<'a, R> {
    /// The messages of a planned append, as routes read them: `survivors`
    /// (indexes into `cmd.items`) got offsets `base..` in partition `pid` of
    /// `cmd.queue`. A survivor that is a link keeps its address and is read
    /// through, from the committed queue log, for its payload and headers.
    pub(super) fn routed_of(
        &self,
        cmd: &PushCommand,
        pid: Pid,
        base: u64,
        survivors: &[usize],
        wants_payload: bool,
    ) -> Vec<Routed> {
        let qid = QLogReader::queue_id_of(&cmd.tenant, &cmd.queue);
        let mut out = Vec::with_capacity(survivors.len());
        for (k, &i) in survivors.iter().enumerate() {
            let item = &cmd.items[i];
            let Some(frames) = unpack_frames_ref(&item.frame) else {
                continue;
            };
            let Some(f) = frames.first() else {
                continue;
            };
            let (link, payload, headers) = match f.link {
                None => {
                    let payload = if !wants_payload {
                        None
                    } else if f.encrypted {
                        encryption().decrypt_payload_bytes(f.payload).map(Arc::new)
                    } else {
                        Some(Arc::new(f.payload.to_vec()))
                    };
                    (
                        LinkRef {
                            qid,
                            pid,
                            offset: base + k as u64,
                        },
                        payload,
                        f.headers.map(|h| Arc::new(h.to_vec())),
                    )
                }
                Some(l) => {
                    // A link (a transaction's `link`): what it stands for is in
                    // the committed log; read it for what a condition needs.
                    let (payload, src_headers) = match &self.qlog_reader {
                        Some(ql) => {
                            let resolved =
                                route::link::resolve(ql, item.frame.to_vec(), &mut |_| Some(0));
                            match unpack_frames_ref(&resolved).and_then(|v| v.into_iter().next()) {
                                Some(r) if !r.is_link() => {
                                    let p = if !wants_payload {
                                        None
                                    } else if r.encrypted {
                                        encryption().decrypt_payload_bytes(r.payload).map(Arc::new)
                                    } else {
                                        Some(Arc::new(r.payload.to_vec()))
                                    };
                                    (p, r.headers.map(|h| Arc::new(h.to_vec())))
                                }
                                _ => (None, f.headers.map(|h| Arc::new(h.to_vec()))),
                            }
                        }
                        None => (None, f.headers.map(|h| Arc::new(h.to_vec()))),
                    };
                    (l, payload, src_headers)
                }
            };
            out.push(Routed {
                hash: item.hash,
                message_id: f.message_id,
                txn: f.txn.to_string(),
                link,
                payload,
                headers,
                doc: std::cell::OnceCell::new(),
            });
        }
        out
    }

    /// The appends routes add for messages of `(queue, partition)`: one per
    /// reader partition that takes any, then the same for each reader's own
    /// readers (a chain), to [`route::MAX_CHAIN_DEPTH`].
    pub(super) fn plan_routes(
        &self,
        ov: &mut Overlay,
        tenant: &str,
        queue: &str,
        partition: &str,
        cfg: &QueueConfig,
        messages: Vec<Routed>,
    ) -> Result<Vec<Effect>, Refusal> {
        let mut effects = Vec::new();
        // The source's own push is folded before its routes are planned
        // (push.rs), so nothing a client sent may refuse it from here: a
        // message a reader cannot take is not routed to that reader, as one
        // without its `partitionBy` field is not. Only a store error, which
        // refuses the whole cycle, still returns.
        let max_key = self.reads().max_key_len();
        // (queue, partition, its config, its messages), level by level.
        let mut work: Vec<(String, String, QueueConfig, Vec<Routed>, usize)> = vec![(
            queue.to_string(),
            partition.to_string(),
            cfg.clone(),
            messages,
            0,
        )];
        while let Some((q, p, qcfg, msgs, depth)) = work.pop() {
            if msgs.is_empty() || depth >= route::MAX_CHAIN_DEPTH {
                continue;
            }
            let Some(qroute) = qcfg.route() else {
                continue;
            };
            for reader in &qroute.readers {
                let Some(rcfg) = self.queue_cfg(ov, tenant, reader)? else {
                    continue;
                };
                let Some(rroute) = rcfg.route() else {
                    continue;
                };
                // A reader still filled from history takes no live link: its
                // catch-up reads them, and the flip turns this on.
                if rroute.catching_up {
                    continue;
                }
                let rules: Vec<Arc<Vec<cond::Cond>>> = rroute
                    .sources
                    .iter()
                    .filter(|s| s.queue == q)
                    .map(|s| route::compiled(&s.where_json))
                    .collect();
                if rules.is_empty() {
                    continue;
                }
                // The reader's partitions, in the order the messages came.
                let mut by_part: BTreeMap<String, Vec<usize>> = BTreeMap::new();
                let mut order: Vec<String> = Vec::new();
                for (i, m) in msgs.iter().enumerate() {
                    let subject = cond::Subject {
                        queue: route::shown_name(&q),
                        partition: &p,
                        txn: &m.txn,
                        offset: None,
                        position: None,
                        doc: None,
                        headers: m.headers.as_deref().map(Vec::as_slice),
                    };
                    let takes = rules.iter().any(|conds| {
                        let s = if cond::wants_payload(conds) {
                            cond::Subject {
                                doc: m.doc(),
                                ..subject
                            }
                        } else {
                            subject
                        };
                        cond::all_hold(conds, &s)
                    });
                    if !takes {
                        continue;
                    }
                    let doc = if rroute.partition_by.starts_with("data.") {
                        m.doc()
                    } else {
                        None
                    };
                    // A partition name read from the payload may not fit a
                    // store key: not routed.
                    let Some(tp) = route::target_partition(
                        &rroute.partition_by,
                        &p,
                        doc,
                        m.headers.as_deref().map(Vec::as_slice),
                    )
                    .filter(|tp| {
                        super::keycheck::partition_keys_len(tenant, reader, tp) <= max_key
                    }) else {
                        UNROUTED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        continue;
                    };
                    if !by_part.contains_key(&tp) {
                        order.push(tp.clone());
                    }
                    by_part.entry(tp).or_default().push(i);
                }
                for tp in order {
                    let idx = by_part.remove(&tp).unwrap_or_default();
                    let items: Vec<PushItem> = idx
                        .iter()
                        .map(|&i| {
                            let m = &msgs[i];
                            let frame = pack_frames(&[FrameIn {
                                message_id: m.message_id,
                                txn: &m.txn,
                                trace_id: None,
                                producer_sub: None,
                                payload: &[],
                                encrypted: false,
                                headers: None,
                                link: Some(m.link),
                            }]);
                            PushItem {
                                hash: m.hash,
                                frame: frame.into(),
                            }
                        })
                        .collect();
                    let sub = PushCommand {
                        request_id: Default::default(),
                        tenant: tenant.to_string(),
                        queue: reader.clone(),
                        partition: tp.clone(),
                        items,
                        create_cfg: rcfg.clone(),
                        links: Default::default(),
                    };
                    let planned = match self.plan_push_inner(ov, &sub, None, false) {
                        Ok(p) => p,
                        // Its checks run before it folds anything.
                        Err(r) if !r.retryable => {
                            UNROUTED
                                .fetch_add(idx.len() as u64, std::sync::atomic::Ordering::Relaxed);
                            continue;
                        }
                        Err(r) => return Err(r),
                    };
                    match planned {
                        Plan::Logged {
                            effects: e,
                            outcome,
                        } => {
                            effects.extend(e);
                            // What the append created goes on to the reader's
                            // own readers; a duplicate stops here.
                            let created: Vec<Routed> = match outcome {
                                Outcome::Push(o) => o
                                    .items
                                    .iter()
                                    .zip(&idx)
                                    .filter(|(v, _)| matches!(v, PushVerdict::Created { .. }))
                                    .map(|(_, &i)| msgs[i].clone())
                                    .collect(),
                                _ => Vec::new(),
                            };
                            ROUTED.fetch_add(
                                created.len() as u64,
                                std::sync::atomic::Ordering::Relaxed,
                            );
                            if rcfg.has_readers() && !created.is_empty() {
                                work.push((reader.clone(), tp, rcfg.clone(), created, depth + 1));
                            }
                        }
                        Plan::Empty(_) => {}
                        Plan::Refused(r) if !r.retryable => {
                            UNROUTED
                                .fetch_add(idx.len() as u64, std::sync::atomic::Ordering::Relaxed);
                        }
                        Plan::Refused(r) => return Err(r),
                    }
                }
            }
        }
        Ok(effects)
    }

    /// For links pushed into an ordinary queue (a transaction's `link`): the
    /// sources that do not list `holder` yet, with it added to their holders.
    pub(super) fn holder_upserts(
        &self,
        ov: &Overlay,
        tenant: &str,
        holder: &str,
        cmd: &PushCommand,
        survivors: &[usize],
    ) -> Result<Vec<Effect>, Refusal> {
        let mut pids: Vec<Pid> = Vec::new();
        for &i in survivors {
            if let Some(f) =
                unpack_frames_ref(&cmd.items[i].frame).and_then(|v| v.into_iter().next())
            {
                if let Some(l) = f.link {
                    if !pids.contains(&l.pid) {
                        pids.push(l.pid);
                    }
                }
            }
        }
        let mut out = Vec::new();
        let mut done: Vec<String> = Vec::new();
        for pid in pids {
            // The source of a client's link is committed (the receiver read
            // it there): its row names its queue.
            use crate::rsm::store::TypedReads;
            let Some(part) = self.reads().partition(pid).map_err(super::store_err)? else {
                continue;
            };
            if part.tenant != tenant || part.queue == holder || done.contains(&part.queue) {
                continue;
            }
            done.push(part.queue.clone());
            let Some(mut src) = self.queue_cfg(ov, tenant, &part.queue)? else {
                continue;
            };
            let listed = src
                .route()
                .is_some_and(|r| r.readers.iter().chain(&r.holders).any(|q| q == holder));
            if listed {
                continue;
            }
            src.route_mut().holders.push(holder.to_string());
            out.push(Effect::QueueUpsert {
                tenant: tenant.to_string(),
                queue: part.queue.clone(),
                cfg: src,
            });
        }
        Ok(out)
    }
}

impl<'a, R: Reads + ?Sized> Planner<'a, R> {
    /// Complete the catalog changes of an effects command (a `configure`, a
    /// delete) with what routing keeps on the OTHER side of each change, in
    /// the same entry and against every entry in flight:
    ///
    /// - a reader's sources list it in their `readers` (added and removed as
    ///   its `from` changes, removed when it is deleted), after the checks a
    ///   reader needs: every source exists in the tenant, is not a hidden
    ///   queue, is not the reader, does not read the reader back at any depth,
    ///   and has room for one more reader;
    /// - a reader's keep settings follow its sources (a hidden queue's consume
    ///   settings too, `route::follow_source`), and so do every reader's when
    ///   a source's settings change;
    /// - `readers` and `holders` are the planner's: an upsert never sets them,
    ///   it keeps what the overlay holds.
    pub(super) fn complete_routes(
        &self,
        ov: &Overlay,
        effects: &mut Vec<Effect>,
    ) -> Result<(), Refusal> {
        let mut extra: Vec<Effect> = Vec::new();
        // Configs as this command leaves them, for the checks and for a
        // second change of one queue in the same command.
        let mut pending: BTreeMap<(String, String), Option<QueueConfig>> = BTreeMap::new();
        for e in effects.iter_mut() {
            match e {
                Effect::QueueUpsert { tenant, queue, cfg } => {
                    let old = self.cfg_pending(ov, &pending, tenant, queue)?;
                    // A catch-up's flip: every source partition read to its
                    // head, or the reader keeps catching up.
                    let was_catching = old
                        .as_ref()
                        .and_then(|c| c.route())
                        .is_some_and(|r| r.catching_up);
                    let flips = was_catching
                        && cfg.route().is_some_and(|r| !r.catching_up && r.is_reader());
                    if flips {
                        self.check_flip(ov, tenant, cfg)?;
                        if let Some(r) = cfg.route.as_deref_mut() {
                            r.marks.clear();
                        }
                    }
                    // The planner's own lists survive any upsert.
                    let (readers, holders) = old
                        .as_ref()
                        .and_then(|c| c.route())
                        .map(|r| (r.readers.clone(), r.holders.clone()))
                        .unwrap_or_default();
                    if !readers.is_empty() || !holders.is_empty() || cfg.route.is_some() {
                        let r = cfg.route_mut();
                        r.readers = readers;
                        r.holders = holders;
                    }
                    if cfg.route.as_ref().is_some_and(|r| r.is_empty()) {
                        cfg.route = None;
                    }
                    let old_sources: Vec<String> = old
                        .as_ref()
                        .and_then(|c| c.route())
                        .map(|r| r.sources.iter().map(|s| s.queue.clone()).collect())
                        .unwrap_or_default();
                    let new_sources: Vec<String> = cfg
                        .route()
                        .map(|r| r.sources.iter().map(|s| s.queue.clone()).collect())
                        .unwrap_or_default();
                    // An ordinary queue of this name becomes a reader: only
                    // one that holds nothing of its own ([`Planner::in_use`]).
                    if old.is_some()
                        && old_sources.is_empty()
                        && !new_sources.is_empty()
                        && self.in_use(ov, tenant, queue)?
                    {
                        return Err(Refusal::client(
                            "bad_request",
                            format!(
                                "{} is an ordinary queue that holds messages; a reading queue starts empty: pick another name",
                                route::shown_name(queue)
                            ),
                        ));
                    }
                    // A reader: check its sources, and keep what they keep.
                    if !new_sources.is_empty() {
                        if new_sources.len() > route::max_sources_per_queue() {
                            return Err(Refusal::client(
                                "too_many_sources",
                                format!(
                                    "a reading queue reads at most {} queues (QUEEN_MAX_SOURCES_PER_QUEUE)",
                                    route::max_sources_per_queue()
                                ),
                            ));
                        }
                        let mut srcs: Vec<QueueConfig> = Vec::new();
                        for s in &new_sources {
                            if s == queue {
                                return Err(Refusal::client(
                                    "route_loop",
                                    format!("{} cannot read itself", route::shown_name(queue)),
                                ));
                            }
                            if route::is_hidden(s) {
                                return Err(Refusal::client(
                                    "bad_source",
                                    format!("{s:?} is a filtered group's queue, not one to read"),
                                ));
                            }
                            let Some(sc) = self.cfg_pending(ov, &pending, tenant, s)? else {
                                return Err(Refusal::client(
                                    "source_not_found",
                                    format!(
                                        "{} reads {s}, which does not exist",
                                        route::shown_name(queue)
                                    ),
                                ));
                            };
                            if self.reads_back(ov, &pending, tenant, s, queue, 0)? {
                                return Err(Refusal::client(
                                    "route_loop",
                                    format!(
                                        "{} reads {s}, which reads {} back",
                                        route::shown_name(queue),
                                        route::shown_name(queue)
                                    ),
                                ));
                            }
                            srcs.push(sc);
                        }
                        let hidden = cfg.route().is_some_and(|r| !r.hidden_group.is_empty());
                        if hidden {
                            route::follow_source(cfg, &srcs[0]);
                        } else {
                            let refs: Vec<&QueueConfig> = srcs.iter().collect();
                            route::keep_like(cfg, &refs);
                        }
                    }
                    // The sources' lists of readers.
                    for s in old_sources.iter().filter(|s| !new_sources.contains(s)) {
                        if let Some(mut sc) = self.cfg_pending(ov, &pending, tenant, s)? {
                            if let Some(r) = sc.route.as_deref_mut() {
                                r.readers.retain(|q| q != queue);
                            }
                            if sc.route.as_ref().is_some_and(|r| r.is_empty()) {
                                sc.route = None;
                            }
                            pending.insert((tenant.clone(), s.clone()), Some(sc.clone()));
                            extra.push(Effect::QueueUpsert {
                                tenant: tenant.clone(),
                                queue: s.clone(),
                                cfg: sc,
                            });
                        }
                    }
                    for s in new_sources.iter().filter(|s| !old_sources.contains(s)) {
                        let Some(mut sc) = self.cfg_pending(ov, &pending, tenant, s)? else {
                            continue;
                        };
                        let r = sc.route_mut();
                        if r.readers.len() >= route::max_readers_per_queue() {
                            return Err(Refusal::client(
                                "too_many_readers",
                                format!(
                                    "{s} has {} readers already (QUEEN_MAX_READERS_PER_QUEUE)",
                                    r.readers.len()
                                ),
                            ));
                        }
                        if !r.readers.contains(queue) {
                            r.readers.push(queue.clone());
                        }
                        pending.insert((tenant.clone(), s.clone()), Some(sc.clone()));
                        extra.push(Effect::QueueUpsert {
                            tenant: tenant.clone(),
                            queue: s.clone(),
                            cfg: sc,
                        });
                    }
                    // A source whose settings changed: its readers follow.
                    if let Some(route_now) = cfg.route() {
                        for rd in route_now.readers.clone() {
                            if new_sources.contains(&rd) {
                                continue;
                            }
                            let Some(mut rc) = self.cfg_pending(ov, &pending, tenant, &rd)? else {
                                continue;
                            };
                            let before = rc.clone();
                            let Some(rr) = rc.route().cloned() else {
                                continue;
                            };
                            if !rr.hidden_group.is_empty() {
                                route::follow_source(&mut rc, cfg);
                            } else {
                                let mut srcs: Vec<QueueConfig> = Vec::new();
                                for s in &rr.sources {
                                    if s.queue == *queue {
                                        srcs.push(cfg.clone());
                                    } else if let Some(c) =
                                        self.cfg_pending(ov, &pending, tenant, &s.queue)?
                                    {
                                        srcs.push(c);
                                    }
                                }
                                let refs: Vec<&QueueConfig> = srcs.iter().collect();
                                route::keep_like(&mut rc, &refs);
                            }
                            if rc != before {
                                pending.insert((tenant.clone(), rd.clone()), Some(rc.clone()));
                                extra.push(Effect::QueueUpsert {
                                    tenant: tenant.clone(),
                                    queue: rd.clone(),
                                    cfg: rc,
                                });
                            }
                        }
                    }
                    pending.insert((tenant.clone(), queue.clone()), Some(cfg.clone()));
                }
                Effect::QueueDelete { tenant, queue } => {
                    let old = self.cfg_pending(ov, &pending, tenant, queue)?;
                    let sources: Vec<String> = old
                        .as_ref()
                        .and_then(|c| c.route())
                        .map(|r| r.sources.iter().map(|s| s.queue.clone()).collect())
                        .unwrap_or_default();
                    for s in sources {
                        if let Some(mut sc) = self.cfg_pending(ov, &pending, tenant, &s)? {
                            if let Some(r) = sc.route.as_deref_mut() {
                                r.readers.retain(|q| q != queue);
                                r.holders.retain(|q| q != queue);
                            }
                            if sc.route.as_ref().is_some_and(|r| r.is_empty()) {
                                sc.route = None;
                            }
                            pending.insert((tenant.clone(), s.clone()), Some(sc.clone()));
                            extra.push(Effect::QueueUpsert {
                                tenant: tenant.clone(),
                                queue: s,
                                cfg: sc,
                            });
                        }
                    }
                    pending.insert((tenant.clone(), queue.clone()), None);
                }
                _ => {}
            }
        }
        // A queue changed twice keeps its last config only: each later upsert
        // in `extra` holds every change made to it before.
        effects.extend(extra);
        Ok(())
    }

    /// Whether `queue` holds, or ever held, a message of its own: one of its
    /// partitions, committed or created by an entry in flight, has an offset.
    ///
    /// A queue that does not is a name and its settings, nothing more, and may
    /// become a reading queue. It is what a consumer leaves behind that
    /// starts before its reading queue is configured: a pop of a queue that
    /// does not exist creates it, so that its group is registered from then
    /// on (`consume/pop.rs`), and until 2026-11-14 that queue, being
    /// "ordinary", held the name for good (Jepsen W16: the reader made in the
    /// middle of the run was refused, three runs out of three).
    ///
    /// A queue with more than [`UNUSED_PARTITIONS`] partitions is taken to be
    /// in use without looking at each: this runs on the planning thread.
    fn in_use(&self, ov: &Overlay, tenant: &str, queue: &str) -> Result<bool, Refusal> {
        use crate::rsm::store::TypedReads;
        let mut pids: Vec<Pid> = Vec::new();
        self.reads()
            .scan_queue_partitions(tenant, queue, None, UNUSED_PARTITIONS + 1, &mut |pid| {
                pids.push(pid);
                true
            })
            .map_err(super::store_err)?;
        if pids.len() > UNUSED_PARTITIONS {
            return Ok(true);
        }
        for ((t, q, _), pid) in ov.names_in_flight() {
            if t == tenant && q == queue && !pids.contains(&pid) {
                pids.push(pid);
            }
        }
        for pid in pids {
            if self
                .partition(ov, pid)?
                .is_some_and(|part| part.last_offset >= 0)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Whether a catch-up may stop: every partition of every source, committed
    /// or created in flight, read to its head (`marks`). Refused retryable
    /// otherwise, with what is left; the catch-up reads it and asks again.
    fn check_flip(&self, ov: &Overlay, tenant: &str, cfg: &QueueConfig) -> Result<(), Refusal> {
        use crate::rsm::store::TypedReads;
        let Some(r) = cfg.route() else {
            return Ok(());
        };
        let marks: std::collections::HashMap<u64, u64> = r.marks.iter().copied().collect();
        for s in &r.sources {
            let mut pids: Vec<Pid> = Vec::new();
            self.reads()
                .scan_queue_partitions(tenant, &s.queue, None, usize::MAX, &mut |pid| {
                    pids.push(pid);
                    true
                })
                .map_err(super::store_err)?;
            for ((t, q, _), pid) in ov.names_in_flight() {
                if t == tenant && q == s.queue && !pids.contains(&pid) {
                    pids.push(pid);
                }
            }
            for pid in pids {
                let Some(part) = self.partition(ov, pid)? else {
                    continue;
                };
                let head = (part.last_offset + 1).max(0) as u64;
                let read_to = marks.get(&pid).copied().unwrap_or(0);
                if head > read_to {
                    return Err(Refusal::retry(
                        "catch_up_behind",
                        format!(
                            "{} has messages past the catch-up in {} (partition {pid}: {} of {head})",
                            route::shown_name(&cfg_name_hint(cfg)),
                            s.queue,
                            read_to
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    /// A queue's config as this command leaves it so far, else the overlay's.
    fn cfg_pending(
        &self,
        ov: &Overlay,
        pending: &BTreeMap<(String, String), Option<QueueConfig>>,
        tenant: &str,
        queue: &str,
    ) -> Result<Option<QueueConfig>, Refusal> {
        if let Some(c) = pending.get(&(tenant.to_string(), queue.to_string())) {
            return Ok(c.clone());
        }
        self.queue_cfg(ov, tenant, queue)
    }

    /// Whether `from`, through its sources at any depth, reads `target`.
    fn reads_back(
        &self,
        ov: &Overlay,
        pending: &BTreeMap<(String, String), Option<QueueConfig>>,
        tenant: &str,
        from: &str,
        target: &str,
        depth: usize,
    ) -> Result<bool, Refusal> {
        if depth > route::MAX_CHAIN_DEPTH * 4 {
            return Ok(true);
        }
        let Some(cfg) = self.cfg_pending(ov, pending, tenant, from)? else {
            return Ok(false);
        };
        let Some(r) = cfg.route() else {
            return Ok(false);
        };
        for s in &r.sources {
            if s.queue == target
                || self.reads_back(ov, pending, tenant, &s.queue, target, depth + 1)?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// A reader's display name for a refusal: its hidden group's source, or the
/// first source it reads (the effect, not the config, carries the name).
fn cfg_name_hint(cfg: &QueueConfig) -> String {
    match cfg.route() {
        Some(r) if !r.hidden_group.is_empty() => r
            .sources
            .first()
            .map(|s| format!("{}/{}", s.queue, r.hidden_group))
            .unwrap_or_default(),
        _ => "the reading queue".to_string(),
    }
}
