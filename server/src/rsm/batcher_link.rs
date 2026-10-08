//! The driver while its cluster is part of a link ([`crate::rsm::link`]).
//!
//! A STANDBY's driver plans nothing of its own. Its client queue stays empty
//! ([`RunState::enqueue`] answers every command with the standby refusal, and
//! [`RunState::refuse_queued`] the ones that waited out the election), its
//! leader steps do not run — no request-id expiry, no KV sweep, no timer fire,
//! no retention, no cluster-version raise: the source's leader ran them, and
//! what they did arrives as entries — and the consumption engine is never told
//! it leads. The only entries it builds are the link's, one per cycle, through
//! the same pipeline as every other entry: the predicted index, the propose in
//! plan order, the hold on a timeout and the reset on a lost leadership are
//! the driver's as they always were.
//!
//! # Exactly once, by construction
//!
//! A source entry must become one entry of the standby's log, never two. The
//! driver holds the [`Cursor`]: read from committed state when this node
//! begins to lead — a leader plans only after it has applied an entry of its
//! own term, so everything an earlier leader logged is applied and the read is
//! exact — and advanced by every entry it hands to the replicator. A mirrored
//! entry is planned only when it follows the cursor's position, and it carries
//! its own position in the same entry, so whichever of its entries commit, the
//! next leader reads how far the standby got.
//!
//! # Becoming a standby, and leaving
//!
//! A cluster is a standby when its role row says so. The first one is written
//! by the driver itself: a node that begins to lead a cluster holding exactly
//! the state a [`LinkBoot`] names (an empty cluster, or a seed) plans the
//! standby entry before anything else. A PROMOTION is one more entry; until it
//! has applied here the driver plans nothing at all, and then it plans as an
//! ordinary leader from the log's end, with the reset a leadership regain does.
//!
//! Whoever asked for the promotion is answered by that last step
//! ([`RunState::check_promotion`]), not by the entry: the answer says the
//! cluster takes its clients' commands, and until the driver plans as an
//! ordinary leader and has told the engine so, it does not — a command sent
//! between the entry's apply and that step is still refused as a standby's.

use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use super::{Launched, PlanOutput, Reply, RunState, Slot, Waiter};
use crate::rsm::entry::{Entry, Outcome};
use crate::rsm::link::{self, mirror, Cursor, Position, Refused};
use crate::rsm::planner::Refusal;
use crate::rsm::replicator::{NodeId, Replicator};
use crate::rsm::store::{Store, TypedReads};

/// A link command sent to a cluster that is not a standby (never one, or
/// promoted since). Not retryable: the link has nothing more to do here.
pub const NOT_STANDBY_CODE: &str = "not_standby";
/// A mirrored entry that does not follow the standby's position. Retryable:
/// the sender reads the position again.
pub const OUT_OF_SEQUENCE_CODE: &str = "link_out_of_sequence";
/// A source entry that does not continue the standby's state. Not retryable:
/// the standby needs a new seed.
pub const DIVERGED_CODE: &str = "link_diverged";
/// A source entry that raises the cluster version above what a member of the
/// standby reads. Retryable: it goes through once every member is upgraded.
pub const MEMBER_BEHIND_CODE: &str = "link_member_behind";

/// When a driver of this process last said why the link's entries wait
/// ([`RunState::note_link_wait`]).
static LINK_NOTED: std::sync::Mutex<Option<std::time::Instant>> = std::sync::Mutex::new(None);

/// What a link asks of the driver.
#[derive(Clone, Debug)]
pub enum LinkOp {
    /// Replay the source entry `entry`, which sat at `index` / `term` in the
    /// source's log and follows source index `prev`.
    Mirror {
        prev: u64,
        index: u64,
        term: u64,
        entry: Arc<Entry>,
    },
    /// Promote this standby: from the entry this writes, an ordinary cluster.
    Promote,
}

/// One submission on the driver's link channel. A mirrored entry is answered
/// [`Reply::Done`] once it has applied on this node, so it is in this
/// cluster's log for good, whichever node leads next. A promotion is answered
/// `Done` a step later: once this driver plans as an ordinary leader and its
/// engine serves ([`RunState::check_promotion`]), so that whoever asked can
/// send the cluster a command next.
pub struct LinkSubmission {
    pub op: LinkOp,
    pub reply: oneshot::Sender<Reply>,
}

impl LinkSubmission {
    pub fn new(op: LinkOp) -> (LinkSubmission, oneshot::Receiver<Reply>) {
        let (tx, rx) = oneshot::channel();
        (LinkSubmission { op, reply: tx }, rx)
    }
}

/// The sender a link feeds. Bounded: a link that reads faster than the
/// standby commits waits here.
pub type LinkTx = mpsc::Sender<LinkSubmission>;

/// What makes a cluster a standby the first time one of its nodes leads it in
/// the state named here: nothing applied since, and its role row untouched.
/// Once the cluster has written a role row of its own — the standby entry, a
/// promotion — the directive no longer matches and is ignored, so a promoted
/// cluster restarted with it still set stays promoted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkBoot {
    /// Where the source is reached: a label for the role row.
    pub source: String,
    /// Where this cluster's state is in the source's log:
    /// [`Position::START`] for an empty cluster, a seed's position otherwise.
    pub position: Position,
    /// `meta.last_now_us` of that state: 0 for an empty cluster.
    pub last_now_us: i64,
    /// The role row of that state: `None` for an empty cluster, what the
    /// source's snapshot held for a seed.
    pub role_row: Option<Vec<u8>>,
    /// The standby's id ([`link::RoleDoc::id`]), when it was drawn before the
    /// standby entry: a seed's, which its source already holds its log for
    /// under the name made of it ([`link::seed`]). `None`: drawn with the
    /// standby entry, from its request id.
    pub id: Option<String>,
}

impl LinkBoot {
    /// The directive for an empty cluster: a standby of `source` from the
    /// start of its log.
    pub fn empty(source: &str) -> LinkBoot {
        LinkBoot {
            source: source.to_string(),
            position: Position::START,
            last_now_us: 0,
            role_row: None,
            id: None,
        }
    }
}

/// What the driver knows of its cluster's place in a link.
pub(super) enum LinkMode {
    /// Not read: this node does not lead, or the store did not answer. The
    /// driver plans nothing.
    Unknown,
    /// An ordinary cluster. `promoted`: it was a standby once.
    Primary { promoted: bool },
    Standby(Standby),
}

pub(super) struct Standby {
    /// The state the next entry lands on, after everything planned so far.
    cursor: Cursor,
    /// The standby's id ([`link::RoleDoc::id`]), for the role rows. Empty
    /// until the standby entry is planned, unless a seed drew it before
    /// ([`LinkBoot::id`]).
    id: String,
    /// The source's label, for the role rows.
    source: String,
    /// `Some`: committed state does not say standby yet. The standby entry is
    /// planned first, recording this position.
    attach: Option<Position>,
    /// The index of the promotion's entry, once planned.
    promoting: Option<u64>,
    /// Whoever asked for the promotion in flight. Answered `Done` once this
    /// driver plans as an ordinary leader ([`RunState::check_promotion`]),
    /// `Retry` if it stops leading first ([`RunState::fail_link`]).
    asked: Vec<oneshot::Sender<Reply>>,
}

/// What a link cycle planned, for the driver to note once the entry is handed
/// to the replicator ([`RunState::link_proposed`]).
pub(crate) enum LinkPlanned {
    /// The standby entry: the cluster is the standby `id`, at `cursor`.
    Attached { cursor: Cursor, id: String },
    /// A source entry: the standby is at `cursor`.
    Mirrored { cursor: Cursor },
    /// The promotion.
    Promoted,
}

/// One link cycle's work.
enum Job {
    Attach(Position),
    Mirror {
        prev: u64,
        index: u64,
        term: u64,
        entry: Arc<Entry>,
    },
    Promote,
}

/// The cluster's place in a link, from committed state and the boot
/// directive. Called when this node begins to lead.
pub(super) fn read_mode<S: Store>(store: &S, boot: Option<&LinkBoot>) -> LinkMode {
    let read = store.read(|r| Ok((r.flag(link::FLAG_ROLE)?, Cursor::read(r)?)));
    let (row, cursor) = match read {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(
                target: "rsm",
                error = %e,
                "rsm link: this cluster's role could not be read; the driver plans nothing until it can",
            );
            return LinkMode::Unknown;
        }
    };
    if let Some(b) = boot {
        if row == b.role_row && cursor.last_now_us == b.last_now_us {
            tracing::info!(
                target: "rsm",
                source = %b.source,
                index = b.position.index,
                "rsm link: this cluster becomes a standby",
            );
            return LinkMode::Standby(Standby {
                cursor,
                id: b.id.clone().unwrap_or_default(),
                source: b.source.clone(),
                attach: Some(b.position),
                promoting: None,
                asked: Vec::new(),
            });
        }
    }
    match link::decode_role(row.as_deref()) {
        Ok(link::Role::Standby(doc)) => LinkMode::Standby(Standby {
            cursor,
            id: doc.id,
            source: doc.source,
            attach: None,
            promoting: None,
            asked: Vec::new(),
        }),
        Ok(link::Role::Promoted(_)) => LinkMode::Primary { promoted: true },
        Ok(link::Role::Primary) => {
            if let Some(b) = boot {
                tracing::error!(
                    target: "rsm",
                    source = %b.source,
                    "rsm link: this cluster is configured as a standby but holds entries of its own, \
                     so it stays an ordinary cluster: a standby starts empty or from a seed of its source",
                );
            }
            LinkMode::Primary { promoted: false }
        }
        Err(why) => {
            tracing::error!(
                target: "rsm",
                why,
                "rsm link: the driver plans nothing while this cluster's role row is unreadable",
            );
            LinkMode::Unknown
        }
    }
}

/// Whether the source entry `e` writes the role row of the standby `id`.
fn writes_role_of(e: &Entry, id: &str) -> bool {
    use crate::rsm::effect::Effect;
    !id.is_empty()
        && e.effects.iter().any(|eff| match eff {
            Effect::FlagSet { key, value } if key == link::FLAG_ROLE => {
                serde_json::from_slice::<link::RoleDoc>(value).is_ok_and(|doc| doc.id == id)
            }
            _ => false,
        })
}

/// This node's wall clock, µs: written into the role rows for people
/// ([`link::RoleDoc::wall_us`]), never a stamp.
fn wall_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as i64)
}

/// Plan one link cycle (on the planner thread). `answered`: a caller waits
/// for the answer; a refusal with nobody to tell is logged instead.
fn plan_link<S: Store>(
    store: &S,
    job: Job,
    cursor: Cursor,
    id: &str,
    source: &str,
    kinds_floor: Option<u32>,
    answered: bool,
) -> crate::rsm::store::Result<PlanOutput> {
    let store_applied = store.read(|r| r.applied_index())?;
    let refuse = |r: Refusal| {
        if !answered {
            tracing::error!(target: "rsm", code = %r.code, why = %r.message, "rsm link: a link entry was not planned");
            return (None, Vec::new(), None);
        }
        (None, vec![Slot::Immediate(Reply::Refused(r))], None)
    };
    let waiting = |answered: bool| {
        if answered {
            vec![Slot::Empty(Outcome::Empty)]
        } else {
            Vec::new()
        }
    };
    let (entry, slots, planned) = match job {
        Job::Attach(position) => {
            let request_id = crate::util::uuidv7_bytes();
            // A seed's id is the one its source already knows the standby
            // by; an empty cluster's is drawn here.
            let id = match id {
                "" => link::link_id(&request_id),
                seeded => seeded.to_string(),
            };
            match mirror::standby_entry(&cursor, request_id, &id, source, position, wall_us()) {
                Ok(e) => {
                    let mut after = cursor;
                    after.position = position;
                    (
                        Some(Arc::new(e)),
                        waiting(answered),
                        Some(LinkPlanned::Attached { cursor: after, id }),
                    )
                }
                Err(e) => refuse(Refusal::client("internal", format!("standby entry: {e:?}"))),
            }
        }
        Job::Promote => {
            let request_id = crate::util::uuidv7_bytes();
            match mirror::promote_entry(&cursor, request_id, id, source, wall_us()) {
                Ok(e) => (
                    Some(Arc::new(e)),
                    waiting(answered),
                    Some(LinkPlanned::Promoted),
                ),
                Err(e) => refuse(Refusal::client("internal", format!("promote entry: {e:?}"))),
            }
        }
        // The entry that made THIS cluster a standby can only come out of
        // this cluster's own log: its source is itself (or a cluster that
        // replays it). Every entry replayed would be one more to replay.
        Job::Mirror {
            index, entry: src, ..
        } if writes_role_of(&src, id) => refuse(Refusal::client(
            DIVERGED_CODE,
            format!(
                "source entry {index} is this cluster's own standby entry: QUEEN_LINK_SOURCE \
                 names this cluster itself, or a cluster that follows it"
            ),
        )),
        Job::Mirror {
            prev,
            index,
            term,
            entry: src,
        } => match cursor.admit(prev, index, &src) {
            Err(why @ Refused::OutOfSequence { .. }) => {
                refuse(Refusal::retry(OUT_OF_SEQUENCE_CODE, why.to_string()))
            }
            Err(why @ Refused::Diverged(_)) => {
                refuse(Refusal::client(DIVERGED_CODE, why.to_string()))
            }
            Ok(()) => match cursor
                .raises(&src)
                .filter(|v| kinds_floor.is_none_or(|floor| floor < *v))
            {
                Some(v) => refuse(Refusal::retry(
                    MEMBER_BEHIND_CODE,
                    format!(
                        "source entry {index} raises the cluster version to {v}, and not every \
                         member of this standby is known to read it: upgrade the standby first"
                    ),
                )),
                None => match mirror::mirror_entry(&src, index, term) {
                    Ok(e) => {
                        let mut after = cursor;
                        after.advance(index, term, &src);
                        (
                            Some(Arc::new(e)),
                            waiting(answered),
                            Some(LinkPlanned::Mirrored { cursor: after }),
                        )
                    }
                    Err(e) => refuse(Refusal::client(
                        DIVERGED_CODE,
                        format!("source entry {index} does not mirror: {e:?}"),
                    )),
                },
            },
        },
    };
    Ok(PlanOutput {
        store_applied,
        entry,
        slots,
        expired: false,
        expire_more: false,
        kv_swept: false,
        fired: false,
        fire_more: false,
        maintained: false,
        maintenance_more: false,
        // What every entry planned so far leaves the cluster at: the driver
        // checks the entry against it, and `Cursor::admit` already has.
        cluster_version: cursor.cluster_version,
        link: planned,
    })
}

impl<S: Store + 'static, R: Replicator> RunState<S, R> {
    /// Read the cluster's place in a link: this node has just begun to lead
    /// (everything logged before its term is applied).
    pub(super) fn load_link(&mut self) {
        let mode = read_mode(&*self.store, self.link_boot.as_ref());
        self.adopt_link(mode);
    }

    /// Take `mode` as the cluster's place in a link, tell the engine whether
    /// the cluster takes client commands at all, and answer the commands that
    /// waited for this node to lead when it does not.
    pub(super) fn adopt_link(&mut self, mode: LinkMode) {
        self.link = mode;
        if let Some(e) = &self.engine {
            e.set_standby(matches!(self.link, LinkMode::Standby(_)));
        }
        self.refuse_queued();
    }

    /// Answer every command in the queue what a command is answered on a
    /// cluster that is not an ordinary one ([`Self::link_refusal`]); nothing
    /// on an ordinary cluster. A command that arrives while this node is
    /// paused with no leader known (an election) is queued, because the node
    /// may win and plan it. One that wins a standby's election plans nothing
    /// of its own: the command is refused as it would have been a moment
    /// later, not kept until a promotion that may be hours away, and never
    /// planned ([`RunState::can_prefetch`]).
    pub(super) fn refuse_queued(&mut self) {
        let Some(refusal) = self.link_refusal() else {
            return;
        };
        while let Some(sub) = self.lane.pop_front().or_else(|| self.queue.pop_front()) {
            let _ = sub.reply.send(Reply::Refused(refusal.clone()));
        }
    }

    /// Take what the link sent while a cycle ran.
    pub(super) fn take_link(&mut self) {
        loop {
            let Some(rx) = self.link_rx.as_mut() else {
                return;
            };
            match rx.try_recv() {
                Ok(sub) => self.on_link(sub),
                Err(_) => return,
            }
        }
    }

    /// Whether the driver plans as an ordinary leader.
    pub(super) fn link_primary(&self) -> bool {
        matches!(self.link, LinkMode::Primary { .. })
    }

    /// A leading driver that could not read its link state tries again; one
    /// that then finds an ordinary cluster starts the engine it held back.
    pub(super) fn sync_link(&mut self) {
        if self.paused || self.stopped || !matches!(self.link, LinkMode::Unknown) {
            return;
        }
        self.load_link();
        if self.link_primary() {
            if let (Some(term), Some(e)) = (self.planning_term, &self.engine) {
                e.on_leader(term, self.next_index - 1);
            }
        }
    }

    /// What a client command is answered instead of a place in the queue;
    /// `None` on an ordinary cluster.
    pub(super) fn link_refusal(&self) -> Option<Refusal> {
        match &self.link {
            LinkMode::Primary { .. } => None,
            LinkMode::Standby(_) => Some(link::standby_refusal()),
            LinkMode::Unknown => Some(Refusal::retry(
                "unavailable",
                "this cluster's link role is not read yet",
            )),
        }
    }

    /// Say why the link's work is not planned, at most once every five
    /// seconds: a standby that does not replay, or that cannot be promoted, is
    /// otherwise silent about what its leader's driver waits for, and whoever
    /// asked only reads "the request deadline elapsed".
    pub(super) fn note_link_wait(&self, what: &'static str) {
        {
            let mut last = LINK_NOTED.lock().unwrap_or_else(|p| p.into_inner());
            let now = std::time::Instant::now();
            if last.is_some_and(|at| now.duration_since(at) < std::time::Duration::from_secs(5)) {
                return;
            }
            *last = Some(now);
        }
        let link = match &self.link {
            LinkMode::Unknown => "unknown".to_string(),
            LinkMode::Primary { promoted } => format!("primary promoted={promoted}"),
            LinkMode::Standby(s) => format!(
                "standby attach={} promoting={:?} position={}",
                s.attach.is_some(),
                s.promoting,
                s.cursor.position.index
            ),
        };
        tracing::warn!(
            target: "rsm",
            what,
            role = ?*self.role_rx.borrow(),
            paused = self.paused,
            hold = ?self.holding_until,
            quiescing = self.quiescing.is_some(),
            realign = ?self.realign_at,
            term = ?self.planning_term,
            link = %link,
            link_queue = self.link_queue.len(),
            queued = self.queued(),
            in_flight = self.inflight.len(),
            uncommitted = self.uncommitted(),
            unresolved = self.unresolved(),
            next_index = self.next_index,
            applied = self.repl.applied_index(),
            "rsm link: the driver does not plan the link's entries",
        );
    }

    /// `Some(due)` while this is not an ordinary cluster: the driver plans
    /// link entries only, and `due` says whether one waits. `None`: plan as
    /// always.
    pub(super) fn link_due(&self) -> Option<bool> {
        match &self.link {
            LinkMode::Primary { .. } => None,
            LinkMode::Unknown => Some(false),
            LinkMode::Standby(s) => Some(
                s.promoting.is_none() && (s.attach.is_some() || !self.link_queue.is_empty()),
            ),
        }
    }

    /// Whether a promotion's entry is in flight ([`Self::check_promotion`]
    /// polls for it).
    pub(super) fn link_promoting(&self) -> bool {
        matches!(&self.link, LinkMode::Standby(s) if s.promoting.is_some())
    }

    /// A link submission arrived.
    pub(super) fn on_link(&mut self, sub: LinkSubmission) {
        if self.paused {
            // Another node leads, or none is known: only a leader builds the
            // link's entries, and the sender follows the hint.
            let hint = self.bounce_hint().flatten();
            if matches!(sub.op, LinkOp::Promote) && hint.is_none() {
                self.note_link_wait("a promotion was asked of a driver that is paused, with no leader to send it to");
            }
            let _ = sub.reply.send(Reply::Retry { hint });
            return;
        }
        if matches!(sub.op, LinkOp::Promote)
            && match &self.link {
                LinkMode::Unknown => true,
                LinkMode::Standby(s) => s.promoting.is_some(),
                LinkMode::Primary { .. } => false,
            }
        {
            self.note_link_wait("a promotion was asked while the link role is unread or a promotion is in flight");
        }
        let answer = match (&self.link, &sub.op) {
            (LinkMode::Unknown, _) => self.link_refusal().map(Reply::Refused),
            // Promoting a promoted cluster again changes nothing.
            (LinkMode::Primary { promoted: true }, LinkOp::Promote) => Some(Reply::Done {
                outcome: Outcome::Empty,
                at: None,
            }),
            (LinkMode::Primary { .. }, _) => Some(Reply::Refused(Refusal::client(
                NOT_STANDBY_CODE,
                "this cluster is not a standby",
            ))),
            (LinkMode::Standby(s), LinkOp::Promote) if s.promoting.is_some() => {
                Some(Reply::Refused(Refusal::retry(
                    "unavailable",
                    "this standby's promotion is in flight",
                )))
            }
            (LinkMode::Standby(s), LinkOp::Mirror { .. }) if s.promoting.is_some() => {
                Some(Reply::Refused(Refusal::client(
                    NOT_STANDBY_CODE,
                    "this standby is being promoted",
                )))
            }
            (LinkMode::Standby(_), _) => None,
        };
        match answer {
            Some(reply) => {
                let _ = sub.reply.send(reply);
            }
            None => self.link_queue.push_back(sub),
        }
    }

    /// Answer every queued link submission `Retry`, and whoever waits for a
    /// promotion in flight: its entry may still commit under whichever node
    /// leads next, and asking that node says whether it did.
    pub(super) fn fail_link(&mut self, hint: Option<NodeId>) {
        for sub in self.link_queue.drain(..) {
            let _ = sub.reply.send(Reply::Retry { hint });
        }
        if let LinkMode::Standby(s) = &mut self.link {
            for reply in s.asked.drain(..) {
                let _ = reply.send(Reply::Retry { hint });
            }
        }
    }

    /// This node stopped leading: what it knew of the link is the next
    /// leader's to read.
    pub(super) fn lose_link(&mut self, hint: Option<NodeId>) {
        self.fail_link(hint);
        self.link = LinkMode::Unknown;
    }

    /// The first half of a link cycle: the standby entry if it is still owed,
    /// else the next queued submission, handed to the planner thread.
    pub(super) fn launch_link(&mut self) -> Option<Launched> {
        let LinkMode::Standby(s) = &self.link else {
            return None;
        };
        if s.promoting.is_some() {
            return None;
        }
        let (job, reply) = match s.attach {
            Some(position) => (Job::Attach(position), None),
            None => {
                let sub = self.link_queue.pop_front()?;
                let job = match sub.op {
                    LinkOp::Mirror {
                        prev,
                        index,
                        term,
                        entry,
                    } => Job::Mirror {
                        prev,
                        index,
                        term,
                        entry,
                    },
                    LinkOp::Promote => Job::Promote,
                };
                (job, Some(sub.reply))
            }
        };
        let (cursor, id, source) = (s.cursor, s.id.clone(), s.source.clone());
        let kinds_floor = self.repl.kinds_floor();
        let store = self.store.clone();
        let answered = reply.is_some();
        let rx = self.planner_thread.run(move |_kept| {
            plan_link(&*store, job, cursor, &id, &source, kinds_floor, answered)
        });
        let n = usize::from(answered);
        Some(Launched {
            rx,
            replies: reply.into_iter().collect(),
            received: vec![None; n],
            arrivals: Vec::new(),
            txn_ids: vec![None; n],
            epoch0: self.plan_epoch,
            hold0: self.holding_until.is_some(),
            expire: false,
            kv_sweep: false,
            fire: false,
            maintenance: false,
            scanned: false,
            trace: false,
            tr: (0, 0, 0, 0, 0),
            timing_on: false,
            drain_started: None,
        })
    }

    /// A link cycle's entry was handed to the replicator at `index`.
    pub(super) fn link_proposed(&mut self, planned: LinkPlanned, index: u64) {
        let LinkMode::Standby(s) = &mut self.link else {
            return;
        };
        match planned {
            LinkPlanned::Attached { cursor, id } => {
                s.cursor = cursor;
                s.id = id;
                s.attach = None;
            }
            LinkPlanned::Mirrored { cursor } => s.cursor = cursor,
            LinkPlanned::Promoted => {
                s.promoting = Some(index);
                // An entry answers its waiters when it applies here, and a
                // promotion is done a step after that. Its answer is taken
                // off the entry: the driver gives it
                // ([`Self::check_promotion`]). A propose that times out
                // therefore leaves whoever asked waiting, to the deadline of
                // their own, for the promotion or for the end of this
                // leadership.
                if let Some(e) = self.inflight.iter_mut().rev().find(|e| e.index == index) {
                    s.asked.extend(e.waiters.drain(..).map(|w| match w {
                        Waiter::Command { reply, .. } | Waiter::Fixed { reply, .. } => reply,
                    }));
                }
            }
        }
    }

    /// After a link cycle. A standby entry that was not proposed (the store
    /// did not answer, the pipeline changed under it) is planned again from a
    /// fresh read, at the driver's next wake rather than in a loop.
    pub(super) fn link_settle(&mut self) {
        if matches!(&self.link, LinkMode::Standby(s) if s.attach.is_some()) {
            self.link = LinkMode::Unknown;
        }
    }

    /// The promotion's entry has applied here: plan as an ordinary leader,
    /// from the log's end, with the reset a leadership regain does. The clock
    /// starts from this node's wall clock (never behind the last stamp), and
    /// the consumption engine serves from the cursor rows as they are — their
    /// leases live on, as across any leader change.
    ///
    /// Whoever asked for the promotion is answered here, last: the command
    /// they send next is planned by this driver or served by the engine.
    pub(super) fn check_promotion(&mut self) {
        let LinkMode::Standby(s) = &mut self.link else {
            return;
        };
        let Some(at) = s.promoting else {
            return;
        };
        // In flight until it is resolved, which it is once applied here in
        // this driver's term; a lost leadership reset the link instead.
        let landed = match self.inflight.iter().find(|e| e.index == at) {
            Some(e) if e.resolved.is_none() => return,
            Some(e) => e.resolved,
            None => None,
        };
        let asked = std::mem::take(&mut s.asked);
        tracing::info!(
            target: "rsm",
            index = at,
            source = %s.source,
            position = s.cursor.position.index,
            "rsm link: this standby is promoted; it serves as an ordinary cluster from here",
        );
        self.link = LinkMode::Primary { promoted: true };
        self.front.reset();
        self.invalidate_kept();
        self.begin_term_clock();
        if let Some(e) = &self.engine {
            e.set_standby(false);
            if let Some(term) = self.planning_term {
                e.on_leader(term, self.next_index - 1);
            }
        }
        for reply in asked {
            let _ = reply.send(Reply::Done {
                outcome: Outcome::Empty,
                at: landed,
            });
        }
    }
}
