//! One group's state machine, and the task that owns it.
//!
//! ## The four states
//!
//! ```text
//!                 first join
//!      Empty ───────────────────► PreparingRebalance ◄──────────┐
//!        ▲                                │                     │
//!        │                     the join window closes           │ a member
//!        │ the last member                │                     │ joins, leaves
//!        │ leaves or dies                 ▼                     │ or dies
//!        └────────────────── CompletingRebalance ───────────────┤
//!                                         │                     │
//!                          the leader posts assignments         │
//!                                         ▼                     │
//!                                      Stable ──────────────────┘
//! ```
//!
//! PreparingRebalance is the JOIN phase: members announce themselves and their
//! JoinGroup responses are HELD — nothing is answered until the window closes,
//! because a member's response has to carry the generation the whole group
//! agreed on and that number does not exist until the last joiner is in.
//! CompletingRebalance is the SYNC phase: everyone has its generation, the
//! leader is computing the assignment, and every follower's SyncGroup is held
//! until it lands. Stable is a group doing its work.
//!
//! ## Holding a response is the protocol, not a stall
//!
//! Two of the six commands park: JoinGroup for the length of the join window,
//! and a follower's SyncGroup for as long as the leader takes. Each parks with
//! its connection muted (conn.rs answers one request at a time), which is
//! exactly what a long-poll Fetch already does and exactly what Apache Kafka
//! does with the same two requests — the client is not polling for an answer,
//! it is waiting for one, and its own rebalance timeout is the bound. Nothing
//! else in this file waits: a heartbeat, a leave and a commit check are pure
//! state reads answered on the spot.
//!
//! ## Every deadline is one select arm
//!
//! Session timers, the join window and the reaper are not tasks and not
//! `JoinHandle`s to cancel — the loop computes the EARLIEST deadline it cares
//! about and sleeps until it, then re-derives everything. There is one timer in
//! flight per group, cancellation is "recompute", and a timer cannot fire
//! against state that moved under it. It is also what makes the whole FSM
//! testable under `tokio::time::pause`: the only clock is tokio's.
//!
//! ## The two things this file must never do
//!
//! PARSE AN ASSIGNMENT, or CHOOSE ONE. Protocol metadata and assignments are
//! `Bytes` from the moment they arrive to the moment they leave. The encoding
//! belongs to the client's assignor — range, round-robin, sticky, cooperative
//! sticky, or something written last week — and a coordinator that decoded one
//! would work until it met the next.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::error::ResponseError;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use super::{
    Command, GroupConfig, GroupDescription, JoinAnswer, JoinRequest, MemberDescription, MemberId,
    Protocol, Snapshot, SyncAnswer, SyncRequest,
};
use crate::obs::Sampler;

/// How many minted-but-unused member ids one group remembers at a time.
///
/// The set is the KIP-394 round trip's memory ([`Group::pending`]) and its
/// natural size is "consumers of this group starting at this moment", which is
/// a handful. A thousand is far past any real group and small enough that the
/// per-command scan over it cannot be felt.
const MAX_PENDING_MEMBERS: usize = 1_024;

/// One line per window when a group's minted ids are being forgotten — the
/// situation is by definition a flood.
static PENDING_CAP: Sampler = Sampler::new(60_000);

/// Ceiling on a member's REBALANCE timeout, which is how long the join window
/// may be held open for it.
///
/// The session timeout is bounded by configuration on the way in
/// (`GroupConfig::min_session_timeout`/`max_session_timeout`, and a request
/// outside them is refused INVALID_SESSION_TIMEOUT). The rebalance timeout has
/// no such gate — the protocol gives it no bounds and Apache Kafka does not
/// bound it either — so as sent it is any `i32`, and `i32::MAX` milliseconds is
/// twenty-five DAYS. One member asking for that is one member holding every
/// other member of its group in a join window for twenty-five days: the join
/// responses are parked, the consumers are not consuming, and nothing in the
/// FSM ever wakes to end it, because the deadline it computed is real.
///
/// Thirty minutes is far past any honest value — it is the time a client gives
/// its own `poll()` loop to finish processing and rejoin, and Kafka's own
/// `max.poll.interval.ms` default is five minutes — and it bounds the damage one
/// bad number can do to the group around it at one join window.
///
/// CLAMPED, not refused. A too-long rebalance timeout is not a client that
/// cannot work, it is one whose window is shorter than it asked for, and Kafka
/// has no error code for it that a consumer treats as configuration (the
/// INVALID_SESSION_TIMEOUT next door is about the other field). So the member
/// joins, with a window it will not notice, and the clamp is loud
/// ([`REBALANCE_CLAMP`]) because it is a client that ought to be fixed.
const MAX_REBALANCE_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// One line per window when rebalance timeouts are being clamped: a misbuilt
/// client rejoins on its own timer, so the line would otherwise repeat for as
/// long as it is deployed.
static REBALANCE_CLAMP: Sampler = Sampler::new(60_000);

/// Where a group is in the membership protocol. See the diagram above.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// No members. The group still exists — its generation counter and its id
    /// do — and its committed offsets are in Queen, untouched by any of this.
    Empty,
    /// Collecting joins. The state a rebalance is IN, and the one that answers
    /// a heartbeat REBALANCE_IN_PROGRESS.
    PreparingRebalance,
    /// Everyone has joined and knows the generation; the leader owes us an
    /// assignment.
    CompletingRebalance,
    /// Assigned and working.
    Stable,
    /// The actor is exiting. Terminal, and reached only by the reaper: a group
    /// that has had no members for [`GroupConfig::empty_reap`] is a task and a
    /// map entry with nothing behind them.
    Dead,
}

/// One member, and everything the coordinator knows about it — which is its
/// identity, its timings, and two opaque byte strings.
struct Member {
    id: MemberId,
    /// The two identity strings DescribeGroups answers and nothing else reads:
    /// the `client.id` from the request header and the peer address
    /// `conn::serve` accepted. Kept per member rather than derived, because a
    /// group's members arrive on different connections and only the member
    /// knows which one was its own.
    client_id: String,
    client_host: String,
    /// Every protocol it says it speaks, in ITS order of preference.
    protocols: Vec<Protocol>,
    session_timeout: Duration,
    rebalance_timeout: Duration,
    /// When this member is evicted if nothing is heard from it. Armed at join
    /// and pushed forward by every heartbeat, sync and offset commit.
    deadline: Instant,
    /// Whether it has rejoined the rebalance in progress. Meaningless outside
    /// PreparingRebalance.
    joined: bool,
    /// The slice the leader last gave it, replayed to a SyncGroup that arrives
    /// after the group already stabilised (a retry, or a client that was slow
    /// off the mark).
    assignment: Bytes,
    /// A JoinGroup response held for the join window.
    join_reply: Option<oneshot::Sender<JoinAnswer>>,
    /// A SyncGroup response held for the leader.
    sync_reply: Option<oneshot::Sender<SyncAnswer>>,
}

impl Member {
    fn metadata_for(&self, protocol: &str) -> Bytes {
        self.protocols
            .iter()
            .find(|p| p.name == protocol)
            .map(|p| p.metadata.clone())
            .unwrap_or_default()
    }

    /// Is this member waiting for an answer THIS coordinator owes it?
    ///
    /// A parked member is not a silent one. It sends nothing while its
    /// JoinGroup or SyncGroup is held — clients stop heartbeating for the
    /// length of a rebalance — so the session timer, which only ever moves
    /// forward on something the member SENDS, is not evidence about it. Apache
    /// Kafka says the same thing in `MemberMetadata.hasSatisfiedHeartbeat`,
    /// which returns true for exactly `isAwaitingJoin || isAwaitingSync`.
    ///
    /// Read in two places, and both of them matter: the expiry filter in
    /// [`Group::on_timer`], and [`Group::next_deadline`] — a deadline that
    /// cannot expire must not be slept on either, or the actor wakes on an
    /// instant already past and spins.
    fn parked(&self) -> bool {
        self.join_reply.is_some() || self.sync_reply.is_some()
    }

    /// Answer a held JoinGroup, if there is one. The sender may be gone — the
    /// client disconnected, or its request timed out — and that is not an error
    /// anywhere: the member stays until its session says otherwise, exactly as
    /// it would if the response had been written to a socket nobody read.
    ///
    /// Answering is also the moment the session timer starts meaning something
    /// again: the member was exempt from it while parked, and from here it owes
    /// us a heartbeat like anyone else. Kafka does the same thing in the same
    /// place ("reset the session timeout for members after propagating the
    /// member's assignment", `GroupCoordinator.propagateAssignment`).
    fn answer_join(&mut self, answer: JoinAnswer) {
        if let Some(reply) = self.join_reply.take() {
            let _ = reply.send(answer);
            self.deadline = Instant::now() + self.session_timeout;
        }
    }

    fn answer_sync(&mut self, answer: SyncAnswer) {
        if let Some(reply) = self.sync_reply.take() {
            let _ = reply.send(answer);
            self.deadline = Instant::now() + self.session_timeout;
        }
    }
}

/// The state machine. Owned by exactly one task, so `&mut self` everywhere is
/// the whole concurrency story.
struct Group {
    id: String,
    cfg: GroupConfig,
    state: State,
    /// Bumped once per completed join phase, and once more whenever the group
    /// empties. It is the fence: every request carrying a stale one is refused
    /// ILLEGAL_GENERATION, which is how a member of a dead agreement is stopped
    /// from committing over a live one.
    generation: i32,
    protocol_type: Option<String>,
    protocol_name: Option<String>,
    leader: Option<MemberId>,
    /// In join order. A `Vec` and not a map because the order IS data — it
    /// elects the leader — and because a group with enough members for the
    /// linear scans to matter has other problems.
    members: Vec<Member>,
    /// Member ids minted for the MEMBER_ID_REQUIRED round trip and not yet used
    /// to join, each with the instant it stops being honoured.
    ///
    /// It is what makes the round trip possible at all: the client comes back
    /// with an id that is NOT a member yet, and without a record of having just
    /// issued it, that request is indistinguishable from a member of a
    /// coordinator that no longer exists — which is answered UNKNOWN_MEMBER_ID.
    /// Kafka keeps the same set for the same reason.
    ///
    /// The expiry is the client's own session timeout: a client that asked for
    /// an id and never came back has, by its own declaration, that long to do
    /// so, and this must not become a map that grows with every consumer that
    /// ever failed to start.
    ///
    /// Capped at [`MAX_PENDING_MEMBERS`], because the expiry alone is not a
    /// bound: a JoinGroup with an empty member id is answered immediately and
    /// costs the client one frame, so a peer can mint ids as fast as it can
    /// write for as long as the slowest client's session timeout — five
    /// minutes by default — while every command in this group pays a scan of
    /// the result ([`Group::next_deadline`]).
    pending: Vec<(MemberId, Instant)>,
    /// When the join window closes. Set only in PreparingRebalance.
    join_deadline: Option<Instant>,
    /// Whether the rebalance in flight started from an EMPTY group. It decides
    /// what closes the window: with no known members there is nobody to wait
    /// for, so only the clock can close it; with known members, the window
    /// closes as soon as the last of them has rejoined.
    forming: bool,
    /// When an empty group's actor exits. Set only in Empty.
    reap_deadline: Option<Instant>,
}

/// Whether the actor should keep running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lifecycle {
    Alive,
    Dead,
}

/// Run one group until it is reaped. Spawned by [`super::Coordinator::actor`].
// `key` is deliberately separate from `id`: the key carries the CREDENTIAL that
// owns the group ([`super::GroupKey`]) and is used for the registry lookup and
// for nothing else, while `id` — the group name, and only the group name — is
// what the state machine and its log lines see.
pub(super) async fn run(
    id: String,
    key: super::GroupKey,
    cfg: GroupConfig,
    mut rx: mpsc::Receiver<Command>,
    self_tx: mpsc::Sender<Command>,
    registry: Arc<Mutex<HashMap<super::GroupKey, mpsc::Sender<Command>>>>,
) {
    let mut group = Group::new(id.clone(), cfg);
    loop {
        let deadline = group.next_deadline();
        tokio::select! {
            // Commands first: a group that is being talked to is not idle, and
            // a deterministic order is one fewer thing for a test to be flaky
            // about.
            biased;
            cmd = rx.recv() => match cmd {
                Some(cmd) => {
                    group.on_command(cmd);
                    // `biased` means the timer arm is only reached when the
                    // channel is EMPTY, and a client that keeps a command in
                    // flight — a tight heartbeat loop, or a flood — keeps it
                    // empty never. Every deadline in this file would then be
                    // deferred for as long as the traffic lasts: sessions that
                    // do not expire, a join window that does not close, a
                    // rebalance that does not finish. So an expiry that is
                    // already due is run HERE, between commands, which is the
                    // same work the timer arm does and at most one scan of the
                    // members per command.
                    if group.overdue() && group.on_timer() == Lifecycle::Dead {
                        break;
                    }
                }
                // Unreachable while the registry holds a sender, which it does
                // for as long as this actor is the registered one.
                None => break,
            },
            _ = sleep_until(deadline) => {
                if group.on_timer() == Lifecycle::Dead {
                    break;
                }
            }
        }
    }
    // Deregister, but only if the registry still points at THIS actor: a client
    // may already have raced in and spawned a replacement, and removing that
    // one would strand it.
    let mut groups = registry
        .lock()
        .expect("the group registry lock is never held across a panic");
    if groups.get(&key).is_some_and(|tx| tx.same_channel(&self_tx)) {
        groups.remove(&key);
    }
    tracing::debug!(target: "kafka", group = %id, "group coordinator exited");
}

/// Sleep until `deadline`, or forever when there is nothing to wait for.
async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

impl Group {
    fn new(id: String, cfg: GroupConfig) -> Group {
        Group {
            id,
            cfg,
            state: State::Empty,
            // Kafka's first completed generation is 1, so an empty group is 0.
            generation: 0,
            protocol_type: None,
            protocol_name: None,
            leader: None,
            members: Vec::new(),
            pending: Vec::new(),
            join_deadline: None,
            forming: false,
            // A group nobody ever joins still exits: the actor is spawned by the
            // first request that NAMES the group, which may be a heartbeat from
            // a member of a coordinator that no longer exists.
            reap_deadline: Some(Instant::now() + cfg.empty_reap),
        }
    }

    /// The earliest thing the loop has to wake up for.
    ///
    /// A PARKED member's deadline is not one of them — see [`Member::parked`].
    /// It is exempt from expiry, so sleeping on it would wake the actor on an
    /// instant that is already past, do nothing, and sleep on it again: a
    /// tight loop on the runtime every other tenant's connections share. The
    /// group is never left without a wake-up by the omission: a parked JOIN
    /// only exists in PreparingRebalance, which has a `join_deadline`, and a
    /// parked SYNC only exists in CompletingRebalance, where the leader is by
    /// construction NOT parked (its own SyncGroup is answered the instant it
    /// arrives, by `distribute`) and its session is what bounds the wait.
    fn next_deadline(&self) -> Option<Instant> {
        self.members
            .iter()
            .filter(|m| !m.parked())
            .map(|m| m.deadline)
            .chain(self.pending.iter().map(|(_, at)| *at))
            .chain(self.join_deadline)
            .chain(self.reap_deadline)
            .min()
    }

    /// Whether [`Group::next_deadline`] is already in the past — the question
    /// the select's timer arm answers by not being reachable. See [`run`].
    fn overdue(&self) -> bool {
        self.next_deadline().is_some_and(|at| at <= Instant::now())
    }

    fn on_command(&mut self, cmd: Command) {
        match cmd {
            Command::Join(req, reply) => self.on_join(req, reply),
            Command::Sync(req, reply) => self.on_sync(req, reply),
            Command::Heartbeat(member, generation, reply) => {
                let _ = reply.send(self.on_heartbeat(&member, generation));
            }
            Command::Leave(member, reply) => {
                let _ = reply.send(self.on_leave(&member));
            }
            Command::CheckCommit(member, generation, reply) => {
                let _ = reply.send(self.on_check_commit(&member, generation));
            }
            Command::Describe(reply) => {
                let _ = reply.send(self.snapshot());
            }
            Command::DescribeGroup(reply) => {
                let _ = reply.send(self.description());
            }
            Command::Discard(reply) => {
                let empty = self.members.is_empty();
                // Reaped NOW rather than in `empty_reap`, and through the
                // reaper's own path rather than a second one: a deleted group
                // has to read back `Dead` on the very next DescribeGroups, and
                // an actor left standing would answer `Empty` — which is what
                // an operator would read as "the delete did not happen".
                //
                // The run loop takes it from here: `overdue()` is true the
                // instant this returns, `on_timer` finds an Empty group whose
                // reap deadline has passed, and the actor exits and
                // deregisters itself.
                if empty && self.state == State::Empty {
                    self.reap_deadline = Some(Instant::now());
                }
                let _ = reply.send(empty);
            }
        }
    }

    fn snapshot(&self) -> Snapshot {
        Snapshot {
            state: self.state,
            generation: self.generation,
            leader: self.leader.clone(),
            protocol_type: self.protocol_type.clone(),
            protocol_name: self.protocol_name.clone(),
            members: self.members.iter().map(|m| m.id.clone()).collect(),
            pending: self.pending.len(),
        }
    }

    /// The same instant, in DescribeGroups' shape. See [`GroupDescription`] for
    /// why it is a second read of the same fields and not a widened
    /// [`Snapshot`].
    ///
    /// The two byte strings are the whole value of the API and neither is
    /// touched: the metadata is what this member sent for the ELECTED protocol
    /// (a member that does not speak it has none, which is the empty string
    /// Kafka answers too), and the assignment is exactly the bytes the leader
    /// posted at SyncGroup.
    fn description(&self) -> GroupDescription {
        let elected = self.protocol_name.clone().unwrap_or_default();
        GroupDescription {
            state: self.state,
            generation: self.generation,
            protocol_type: self.protocol_type.clone(),
            protocol_name: self.protocol_name.clone(),
            members: self
                .members
                .iter()
                .map(|m| MemberDescription {
                    id: m.id.clone(),
                    client_id: m.client_id.clone(),
                    client_host: m.client_host.clone(),
                    metadata: m.metadata_for(&elected),
                    assignment: m.assignment.clone(),
                })
                .collect(),
        }
    }

    // ------------------------------------------------------------------ join

    fn on_join(&mut self, req: JoinRequest, reply: oneshot::Sender<JoinAnswer>) {
        let session = Duration::from_millis(req.session_timeout_ms.max(0) as u64);
        if req.session_timeout_ms < 0
            || session < self.cfg.min_session_timeout
            || session > self.cfg.max_session_timeout
        {
            let _ = reply.send(JoinAnswer::refused(
                ResponseError::InvalidSessionTimeout,
                req.member_id,
            ));
            return;
        }
        // A member that names no protocol has nothing to agree with anyone
        // about. INCONSISTENT_GROUP_PROTOCOL is the code whose own description
        // in the protocol spells this case out ("tried to join with empty
        // protocol type or empty protocol list").
        if req.protocol_type.is_empty() || req.protocols.is_empty() {
            let _ = reply.send(JoinAnswer::refused(
                ResponseError::InconsistentGroupProtocol,
                req.member_id,
            ));
            return;
        }
        // A group agrees on ONE protocol type. A `connect` worker joining a
        // `consumer` group is a configuration mistake, not a negotiation.
        if let Some(existing) = &self.protocol_type {
            if !self.members.is_empty() && existing != &req.protocol_type {
                let _ = reply.send(JoinAnswer::refused(
                    ResponseError::InconsistentGroupProtocol,
                    req.member_id,
                ));
                return;
            }
        }
        // ...and on one assignment protocol NAME, which is checked here, on the
        // way in, and not at the end of the join window. The difference is who
        // pays for one mis-set `partition.assignment.strategy`: refused here it
        // is the joining client alone, and a running fleet never notices; found
        // at the window it is every member of the group answered
        // INCONSISTENT_GROUP_PROTOCOL — a code the Java consumer raises out of
        // poll() — on every retry of the bad client. Apache Kafka checks it in
        // the same place, `GroupMetadata.supportsProtocols` inside doJoinGroup.
        if !self.speaks_a_common_protocol(&req.member_id, &req.protocols) {
            tracing::debug!(
                target: "kafka",
                group = %self.id,
                members = self.members.len(),
                "a joiner offered no assignment protocol the group's members all speak"
            );
            let _ = reply.send(JoinAnswer::refused(
                ResponseError::InconsistentGroupProtocol,
                req.member_id,
            ));
            return;
        }

        let now = Instant::now();
        let session_deadline = now + session;
        let member_id = if req.member_id.is_empty() {
            let minted = super::new_member_id(&req.client_id);
            if req.member_id_required {
                // KIP-394: hand the id back and let the client come again with
                // it. NO member is created — that is the whole point, and it is
                // why the id is remembered instead: the request that carries it
                // back is what enters the group.
                self.remember_pending(minted.clone(), session_deadline, now);
                let _ = reply.send(JoinAnswer::refused(ResponseError::MemberIdRequired, minted));
                return;
            }
            minted
        } else if self.members.iter().any(|m| m.id == req.member_id) {
            req.member_id.clone()
        } else if let Some(i) = self.pending.iter().position(|(id, _)| id == &req.member_id) {
            // The other half of the round trip: an id we issued, coming back.
            self.pending.remove(i);
            req.member_id.clone()
        } else {
            // A member id this coordinator never issued, or issued before it
            // restarted, or evicted. The client's answer to UNKNOWN_MEMBER_ID
            // is to forget its id and join again with an empty one, which is
            // the branch above.
            let _ = reply.send(JoinAnswer::refused(
                ResponseError::UnknownMemberId,
                req.member_id,
            ));
            return;
        };

        let asked = Duration::from_millis(if req.rebalance_timeout_ms > 0 {
            req.rebalance_timeout_ms as u64
        } else {
            // JoinGroup v0 has no rebalance timeout: the session timeout is
            // what Kafka substitutes, and it is the only bound v0 offers.
            req.session_timeout_ms.max(0) as u64
        });
        // See [`MAX_REBALANCE_TIMEOUT`]: this number is how long ONE member may
        // hold the whole group's join window open, and nothing else bounds it.
        let rebalance = asked.min(MAX_REBALANCE_TIMEOUT);
        if asked > rebalance {
            if let Some(suppressed) = REBALANCE_CLAMP.tick_now() {
                tracing::warn!(
                    target: "kafka",
                    group = %self.id,
                    asked_ms = asked.as_millis(),
                    clamped_ms = rebalance.as_millis(),
                    suppressed,
                    "a member asked for a rebalance timeout past the ceiling; it holds the \
                     group's join window open, so it was clamped. Lower max.poll.interval.ms \
                     on that client"
                );
            }
        }
        let known = self.members.iter().any(|m| m.id == member_id);
        if !known {
            self.members.push(Member {
                id: member_id.clone(),
                client_id: req.client_id.clone(),
                client_host: req.client_host.clone(),
                protocols: req.protocols.clone(),
                session_timeout: session,
                rebalance_timeout: rebalance,
                deadline: session_deadline,
                joined: false,
                assignment: Bytes::new(),
                join_reply: None,
                sync_reply: None,
            });
        }
        if self.protocol_type.is_none() {
            self.protocol_type = Some(req.protocol_type.clone());
        }

        // A join always starts or extends a rebalance: the group's membership
        // or a member's subscription just changed, and both are exactly what an
        // assignment is a function of.
        match self.state {
            State::Empty => self.begin_rebalance(true),
            State::Stable | State::CompletingRebalance => self.begin_rebalance(false),
            State::PreparingRebalance | State::Dead => {}
        }

        let Some(member) = self.members.iter_mut().find(|m| m.id == member_id) else {
            return;
        };
        member.protocols = req.protocols;
        // Refreshed on every rejoin, not only at the first join: a member that
        // rejoins does so on whatever connection it holds NOW, and a host an
        // operator reads has to be the one the member is attached to rather
        // than the one it first arrived on.
        member.client_id = req.client_id;
        member.client_host = req.client_host;
        member.session_timeout = session;
        member.rebalance_timeout = rebalance;
        member.deadline = session_deadline;
        member.joined = true;
        // A second join from a member already holding one: answer the old
        // request so its connection is not left waiting for a response that
        // will now go to the new one.
        member.answer_join(JoinAnswer::refused(
            ResponseError::RebalanceInProgress,
            member_id.clone(),
        ));
        member.join_reply = Some(reply);

        self.maybe_complete_join();
    }

    /// Record a minted member id, keeping [`Group::pending`] bounded.
    ///
    /// Expired ids go first, because they are the ones nobody is waiting on.
    /// If the cap still binds after that, the OLDEST live id is dropped: a
    /// client whose id was forgotten comes back with an id this coordinator no
    /// longer knows, is answered UNKNOWN_MEMBER_ID, and joins again with an
    /// empty one — the recovery every client already implements, and a far
    /// better answer than a refusal it would have to treat as fatal.
    fn remember_pending(&mut self, id: MemberId, expires_at: Instant, now: Instant) {
        if self.pending.len() >= MAX_PENDING_MEMBERS {
            self.pending.retain(|(_, at)| *at > now);
        }
        if self.pending.len() >= MAX_PENDING_MEMBERS {
            self.pending.remove(0);
            if let Some(suppressed) = PENDING_CAP.tick_now() {
                tracing::warn!(
                    target: "kafka",
                    group = %self.id,
                    pending = MAX_PENDING_MEMBERS,
                    suppressed,
                    "more member ids have been minted for this group than are being used; \
                     the oldest are being forgotten and those clients will join again"
                );
            }
        }
        self.pending.push((id, expires_at));
    }

    /// Open a join window. `forming` says whether it started from an empty
    /// group — see [`Group::forming`].
    fn begin_rebalance(&mut self, forming: bool) {
        // Anyone parked in a sync belongs to the generation being replaced.
        // REBALANCE_IN_PROGRESS is what makes a client rejoin, which is the
        // only thing it can usefully do now.
        for m in &mut self.members {
            m.joined = false;
            m.answer_sync(SyncAnswer::refused(ResponseError::RebalanceInProgress));
        }
        // How long to wait for the members we know about. Their own rebalance
        // timeout is the number they chose for this: past it, a client has
        // already given up on the rebalance itself.
        let window = if forming {
            self.cfg.join_delay
        } else {
            self.members
                .iter()
                .map(|m| m.rebalance_timeout)
                .max()
                .unwrap_or(self.cfg.join_delay)
        };
        self.state = State::PreparingRebalance;
        self.forming = forming;
        self.join_deadline = Some(Instant::now() + window);
        self.reap_deadline = None;
        tracing::debug!(
            target: "kafka",
            group = %self.id,
            generation = self.generation,
            members = self.members.len(),
            window_ms = window.as_millis() as u64,
            forming,
            "rebalance started"
        );
    }

    /// Close the join window early when there is nobody left to wait for.
    fn maybe_complete_join(&mut self) {
        if self.state != State::PreparingRebalance {
            return;
        }
        // A group being FORMED waits out its window whatever happens: the whole
        // point of the delay is to collect members nobody has heard from yet.
        if !self.forming && self.members.iter().all(|m| m.joined) {
            self.complete_join();
        }
    }

    /// The join phase is over: drop the stragglers, elect a leader, pick the
    /// protocol, bump the generation and answer everyone.
    fn complete_join(&mut self) {
        // A member that did not rejoin inside the window is gone. Its own
        // rebalance timeout is what it asked to be given, and the group cannot
        // hold an assignment open for a client that has stopped asking.
        let dropped: Vec<MemberId> = self
            .members
            .iter()
            .filter(|m| !m.joined)
            .map(|m| m.id.clone())
            .collect();
        if !dropped.is_empty() {
            tracing::info!(
                target: "kafka",
                group = %self.id,
                members = dropped.len(),
                member = %dropped[0],
                "members did not rejoin the rebalance in time"
            );
        }
        self.members.retain(|m| m.joined);
        if self.members.is_empty() {
            self.become_empty();
            return;
        }

        let Some(protocol) = self.common_protocol() else {
            // Unreachable, and kept: `on_join` refuses a member that would
            // leave the group without a common name, so by induction one
            // exists here. A belt for a state machine that must not answer a
            // client a protocol nobody offered — if it ever fires, everyone is
            // answered and the group is reset rather than left holding an
            // agreement that does not exist.
            tracing::warn!(
                target: "kafka",
                group = %self.id,
                members = self.members.len(),
                "no assignment protocol is common to every member"
            );
            for m in &mut self.members {
                m.answer_join(JoinAnswer::refused(
                    ResponseError::InconsistentGroupProtocol,
                    m.id.clone(),
                ));
            }
            self.become_empty();
            return;
        };

        // The leader keeps its job across a rebalance when it is still here:
        // clients are built for it (a leader that stays can compute a sticky
        // assignment against what it gave out last time), and a new leader is
        // one more thing to change when nothing needs changing.
        let leader = match &self.leader {
            Some(id) if self.members.iter().any(|m| &m.id == id) => id.clone(),
            _ => self.members[0].id.clone(),
        };
        self.generation += 1;
        self.protocol_name = Some(protocol.clone());
        self.leader = Some(leader.clone());
        self.state = State::CompletingRebalance;
        self.join_deadline = None;
        self.forming = false;

        // Built once and cloned for the leader alone: at 1024 members this is
        // the one allocation in the file that is a function of group size.
        let roster: Vec<(MemberId, Bytes)> = self
            .members
            .iter()
            .map(|m| (m.id.clone(), m.metadata_for(&protocol)))
            .collect();
        let now = Instant::now();
        let (generation, protocol_type) = (self.generation, self.protocol_type.clone());
        for m in &mut self.members {
            // The generation is new, so is the assignment that will come with
            // it; the old one must not be replayed to a late sync.
            m.assignment = Bytes::new();
            m.deadline = now + m.session_timeout;
            let is_leader = m.id == leader;
            m.answer_join(JoinAnswer {
                error: None,
                generation,
                protocol_type: protocol_type.clone(),
                protocol_name: Some(protocol.clone()),
                leader: leader.clone(),
                member_id: m.id.clone(),
                members: if is_leader {
                    roster.clone()
                } else {
                    Vec::new()
                },
            });
        }
        tracing::info!(
            target: "kafka",
            group = %self.id,
            generation = self.generation,
            members = self.members.len(),
            protocol = %protocol,
            leader = %leader,
            "rebalance joined"
        );
    }

    /// Would a member offering `protocols` leave the group with a name every
    /// one of its members speaks?
    ///
    /// `joiner` is excluded from the comparison because a member REJOINING with
    /// a changed assignor list is asking about its new list, not its old one:
    /// what has to hold is that the group still agrees afterwards. Kafka
    /// compares against the old list too (`supportedProtocols` is updated after
    /// the check), which refuses a member that adds an assignor everyone else
    /// already speaks; there is nothing to gain from copying that.
    ///
    /// An empty group is agreement with nobody, so anything the joiner offers
    /// is common to every member of it — which is what the `all` over an empty
    /// iterator says.
    fn speaks_a_common_protocol(&self, joiner: &str, protocols: &[Protocol]) -> bool {
        protocols.iter().any(|candidate| {
            self.members
                .iter()
                .filter(|m| m.id != joiner)
                .all(|m| m.protocols.iter().any(|p| p.name == candidate.name))
        })
    }

    /// The assignment protocol every member supports, in the FIRST member's
    /// order of preference.
    ///
    /// Kafka votes: each member's list is a ranked ballot and the winner is the
    /// most preferred candidate that is in everyone's. Taking the first
    /// member's order is the same answer whenever the clients agree on their
    /// assignor list, which is the case a group is in unless it is mid-upgrade
    /// — and in that case it is the same rule applied to a different first
    /// ballot, never a protocol somebody does not speak.
    fn common_protocol(&self) -> Option<String> {
        let first = self.members.first()?;
        first
            .protocols
            .iter()
            .find(|candidate| {
                self.members
                    .iter()
                    .all(|m| m.protocols.iter().any(|p| p.name == candidate.name))
            })
            .map(|p| p.name.clone())
    }

    // ------------------------------------------------------------------ sync

    fn on_sync(&mut self, req: SyncRequest, reply: oneshot::Sender<SyncAnswer>) {
        let Some(index) = self.members.iter().position(|m| m.id == req.member_id) else {
            let _ = reply.send(SyncAnswer::refused(ResponseError::UnknownMemberId));
            return;
        };
        if req.generation != self.generation {
            let _ = reply.send(SyncAnswer::refused(ResponseError::IllegalGeneration));
            return;
        }
        let now = Instant::now();
        self.members[index].deadline = now + self.members[index].session_timeout;

        match self.state {
            // The join phase reopened under this member. It has to rejoin, and
            // the assignment it may be carrying is for a generation that ended.
            State::PreparingRebalance => {
                let _ = reply.send(SyncAnswer::refused(ResponseError::RebalanceInProgress));
            }
            // Already assigned: this is a retry, or a client that got here late.
            // Its slice is replayed rather than made to wait for a rebalance
            // that is not happening.
            State::Stable => {
                let _ = reply.send(SyncAnswer {
                    error: None,
                    assignment: self.members[index].assignment.clone(),
                });
            }
            State::CompletingRebalance => {
                self.members[index]
                    .answer_sync(SyncAnswer::refused(ResponseError::RebalanceInProgress));
                self.members[index].sync_reply = Some(reply);
                if self.leader.as_deref() == Some(req.member_id.as_str()) {
                    self.distribute(req.assignments);
                }
            }
            // No members exist in either state, so the lookup above already
            // failed. Kept explicit rather than as a catch-all: a new state
            // must be decided here, not defaulted.
            State::Empty | State::Dead => {
                let _ = reply.send(SyncAnswer::refused(ResponseError::UnknownMemberId));
            }
        }
    }

    /// The leader has spoken: hand every member its slice and go Stable.
    ///
    /// A member the leader did not name gets an EMPTY assignment and no error.
    /// That is a legitimate outcome — more consumers than partitions is the
    /// normal shape of a group sized for failover — and it is the assignor's
    /// decision, not this file's.
    fn distribute(&mut self, assignments: Vec<(MemberId, Bytes)>) {
        let by_member: HashMap<MemberId, Bytes> = assignments.into_iter().collect();
        self.state = State::Stable;
        let now = Instant::now();
        for m in &mut self.members {
            m.assignment = by_member.get(&m.id).cloned().unwrap_or_default();
            m.deadline = now + m.session_timeout;
            let assignment = m.assignment.clone();
            m.answer_sync(SyncAnswer {
                error: None,
                assignment,
            });
        }
        tracing::info!(
            target: "kafka",
            group = %self.id,
            generation = self.generation,
            members = self.members.len(),
            "group stable"
        );
    }

    // ------------------------------------------------------- heartbeat, leave

    fn on_heartbeat(&mut self, member_id: &str, generation: i32) -> Option<ResponseError> {
        let Some(member) = self.members.iter_mut().find(|m| m.id == member_id) else {
            return Some(ResponseError::UnknownMemberId);
        };
        if generation != self.generation {
            return Some(ResponseError::IllegalGeneration);
        }
        member.deadline = Instant::now() + member.session_timeout;
        match self.state {
            // THE signal. A client's heartbeat is the only thing it sends
            // between polls, so this error code is how every rebalance in the
            // world starts on the members that did not cause it.
            State::PreparingRebalance => Some(ResponseError::RebalanceInProgress),
            // Deliberately NOT the same answer. A member in CompletingRebalance
            // has already had its JoinGroup response and owes us a SyncGroup;
            // telling it to rebalance would invite a rejoin, and a rejoin
            // reopens the join window it is currently on the far side of — the
            // group would chase its own tail for as long as the client keeps
            // heartbeating. It is inside the rebalance already, so it is told
            // to carry on.
            State::CompletingRebalance | State::Stable => None,
            State::Empty | State::Dead => Some(ResponseError::UnknownMemberId),
        }
    }

    fn on_leave(&mut self, member_id: &str) -> Option<ResponseError> {
        if !self.members.iter().any(|m| m.id == member_id) {
            return Some(ResponseError::UnknownMemberId);
        }
        tracing::debug!(target: "kafka", group = %self.id, member = %member_id, "member left");
        self.evict(&[member_id.to_string()]);
        None
    }

    // ------------------------------------------------------------ commit gate

    fn on_check_commit(&mut self, member_id: &str, generation: i32) -> Option<ResponseError> {
        if generation < 0 && member_id.is_empty() {
            // A simple consumer, using the group only as a place to keep
            // offsets. Allowed while nobody is managing the group, refused the
            // moment somebody is — see `Coordinator::check_commit`.
            return (!self.members.is_empty()).then_some(ResponseError::UnknownMemberId);
        }
        let Some(member) = self.members.iter_mut().find(|m| m.id == member_id) else {
            return Some(ResponseError::UnknownMemberId);
        };
        if generation != self.generation {
            return Some(ResponseError::IllegalGeneration);
        }
        // A commit is proof of life as much as a heartbeat is: a consumer deep
        // in a slow batch commits without heartbeating in some clients, and
        // evicting it for that would be evicting it for working.
        member.deadline = Instant::now() + member.session_timeout;
        None
    }

    // ---------------------------------------------------------------- timers

    fn on_timer(&mut self) -> Lifecycle {
        let now = Instant::now();
        // An id minted for a client that never came back with it.
        self.pending.retain(|(_, at)| *at > now);
        // A member whose response THIS coordinator is holding is not evicted for
        // saying nothing: it is waiting for us. See [`Member::parked`] — without
        // the exemption the ordinary "one slow member" rebalance tears the group
        // down, because a non-forming join window is the members' rebalance
        // timeout (a minute for franz-go, five for a Java consumer) against a
        // session timeout of 45 seconds.
        let expired: Vec<MemberId> = self
            .members
            .iter()
            .filter(|m| !m.parked() && m.deadline <= now)
            .map(|m| m.id.clone())
            .collect();
        if !expired.is_empty() {
            tracing::info!(
                target: "kafka",
                group = %self.id,
                generation = self.generation,
                members = expired.len(),
                member = %expired[0],
                state = ?self.state,
                "session timeout"
            );
            self.evict(&expired);
        }
        if self.state == State::PreparingRebalance && self.join_deadline.is_some_and(|at| at <= now)
        {
            self.complete_join();
        }
        if self.state == State::Empty && self.reap_deadline.is_some_and(|at| at <= now) {
            self.state = State::Dead;
            return Lifecycle::Dead;
        }
        Lifecycle::Alive
    }

    /// Remove members and put the group back into a state that is true.
    ///
    /// One function for every way a member goes — LeaveGroup, a session that
    /// expired, a straggler dropped at the end of a join window — because the
    /// consequences are the same in every case and the interesting one is the
    /// LEADER dying mid-sync: the group is in CompletingRebalance waiting for
    /// an assignment from a client that is gone, and nothing but a new
    /// rebalance can produce one. That is the `CompletingRebalance` arm, and it
    /// is not special-cased for the leader: a follower that dies mid-sync
    /// changes the assignment too, so the leader's answer would be wrong by the
    /// time it arrived.
    fn evict(&mut self, ids: &[MemberId]) {
        for id in ids {
            if let Some(index) = self.members.iter().position(|m| &m.id == id) {
                let mut gone = self.members.remove(index);
                gone.answer_join(JoinAnswer::refused(
                    ResponseError::UnknownMemberId,
                    id.clone(),
                ));
                gone.answer_sync(SyncAnswer::refused(ResponseError::UnknownMemberId));
            }
        }
        if self.members.is_empty() {
            self.become_empty();
            return;
        }
        match self.state {
            State::Stable | State::CompletingRebalance => self.begin_rebalance(false),
            // Already collecting: the members that are left may now be all of
            // them, in which case the window can close early.
            State::PreparingRebalance => self.maybe_complete_join(),
            State::Empty | State::Dead => {}
        }
    }

    /// No members left. The generation is bumped on the way in, so that
    /// anything still holding the last one is refused rather than served.
    fn become_empty(&mut self) {
        self.members.clear();
        self.state = State::Empty;
        self.generation += 1;
        self.leader = None;
        self.protocol_name = None;
        // Cleared, not kept: the next group under this id is free to be a
        // `connect` group where the last one was a `consumer` group.
        self.protocol_type = None;
        self.join_deadline = None;
        self.forming = false;
        self.reap_deadline = Some(Instant::now() + self.cfg.empty_reap);
        tracing::debug!(
            target: "kafka",
            group = %self.id,
            generation = self.generation,
            "group empty"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::{Coordinator, NO_GENERATION};
    use crate::identity::TenantKey;
    use tokio::task::JoinHandle;

    // ------------------------------------------------------------- fixtures

    const JOIN_DELAY_MS: u64 = 3_000;
    const SESSION_MS: i32 = 10_000;
    const REBALANCE_MS: i32 = 60_000;

    fn coordinator() -> Arc<Coordinator> {
        Arc::new(Coordinator::new(GroupConfig {
            join_delay: Duration::from_millis(JOIN_DELAY_MS),
            ..GroupConfig::default()
        }))
    }

    /// A JoinGroup as a modern client sends its FIRST one: v4 or above, with no
    /// member id yet, so it gets the MEMBER_ID_REQUIRED round trip.
    ///
    /// `label` never reaches the protocol — it is stamped into the opaque
    /// metadata, because distribution is only proved by the leader receiving
    /// the RIGHT bytes for each member rather than merely the right number of
    /// them.
    fn join_request(label: &str, protocols: &[&str]) -> JoinRequest {
        JoinRequest {
            member_id: String::new(),
            client_host: "/127.0.0.1".to_string(),
            client_id: "consumer".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: protocols
                .iter()
                .map(|name| Protocol {
                    name: name.to_string(),
                    metadata: Bytes::from(format!("{label}/{name}")),
                })
                .collect(),
            session_timeout_ms: SESSION_MS,
            rebalance_timeout_ms: REBALANCE_MS,
            member_id_required: true,
        }
    }

    /// A client's whole first join: the MEMBER_ID_REQUIRED round trip, then the
    /// real one, which parks until the window closes.
    fn spawn_join(c: &Arc<Coordinator>, group: &str, label: &str) -> JoinHandle<JoinAnswer> {
        spawn_join_with(c, group, label, &["range"])
    }

    /// The same, for a client with its own list of assignors.
    fn spawn_join_with(
        c: &Arc<Coordinator>,
        group: &str,
        label: &str,
        protocols: &[&str],
    ) -> JoinHandle<JoinAnswer> {
        let c = Arc::clone(c);
        let (group, label) = (group.to_string(), label.to_string());
        let protocols: Vec<String> = protocols.iter().map(|p| p.to_string()).collect();
        tokio::spawn(async move {
            let names: Vec<&str> = protocols.iter().map(String::as_str).collect();
            let mut req = join_request(&label, &names);
            let minted = c.join(&group, req.clone()).await;
            assert_eq!(minted.error, Some(ResponseError::MemberIdRequired));
            req.member_id = minted.member_id;
            c.join(&group, req).await
        })
    }

    /// Wait until the group holds `n` members, so a test can close a join
    /// window knowing what is inside it. Bounded, so a coordinator that never
    /// gets there fails the test instead of hanging it.
    async fn wait_for_members(c: &Coordinator, group: &str, n: usize) {
        for _ in 0..1_000 {
            if c.describe(group)
                .await
                .is_some_and(|s| s.members.len() == n)
            {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("{group} never reached {n} members");
    }

    async fn wait_for_state(c: &Coordinator, group: &str, state: State) {
        for _ in 0..1_000 {
            if c.describe(group).await.is_some_and(|s| s.state == state) {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("{group} never reached {state:?}");
    }

    /// Close the join window that is open.
    async fn close_join_window() {
        tokio::time::advance(Duration::from_millis(JOIN_DELAY_MS + 1)).await;
    }

    // ---------------------------------------------------------------- joining

    /// THE join window: three consumers starting together produce ONE
    /// rebalance, not three, and all of them land in the same generation with
    /// the same leader.
    #[tokio::test(start_paused = true)]
    async fn three_joiners_inside_the_window_are_one_rebalance() {
        let c = coordinator();
        let joins: Vec<JoinHandle<JoinAnswer>> = ["a", "b", "c"]
            .iter()
            .map(|m| spawn_join(&c, "orders", m))
            .collect();
        wait_for_members(&c, "orders", 3).await;
        // Nothing is answered yet: the generation they will all be told does
        // not exist until the window closes.
        let mid = c.describe("orders").await.unwrap();
        assert_eq!(mid.state, State::PreparingRebalance);
        assert_eq!(mid.generation, 0);

        close_join_window().await;
        let answers: Vec<JoinAnswer> = futures_join(joins).await;

        for a in &answers {
            assert_eq!(a.error, None);
            assert_eq!(a.generation, 1, "one rebalance, one generation");
            assert_eq!(a.leader, answers[0].leader);
            assert_eq!(a.protocol_name.as_deref(), Some("range"));
            assert_eq!(a.protocol_type.as_deref(), Some("consumer"));
        }
        let leaders = answers.iter().filter(|a| a.members.len() == 3).count();
        assert_eq!(leaders, 1, "exactly one member is handed the roster");
    }

    /// The v4 dance: an empty member id is refused WITH the id to use, and the
    /// rejoin with that id is the one that enters the group.
    #[tokio::test(start_paused = true)]
    async fn an_empty_member_id_at_v4_is_told_which_id_to_use() {
        let c = coordinator();
        let req = join_request("a", &["range"]);

        let first = c.join("orders", req.clone()).await;
        assert_eq!(first.error, Some(ResponseError::MemberIdRequired));
        assert!(!first.member_id.is_empty(), "the answer IS the id");
        assert!(first.member_id.starts_with("consumer-"));
        assert_eq!(first.generation, NO_GENERATION);
        // Refused, so nothing joined: the group is still empty.
        assert_eq!(
            c.describe("orders").await.unwrap().state,
            State::Empty,
            "the refused join created a member"
        );

        let minted = first.member_id.clone();
        let rejoin = {
            let c = Arc::clone(&c);
            let mut req = req.clone();
            req.member_id = minted.clone();
            tokio::spawn(async move { c.join("orders", req).await })
        };
        wait_for_members(&c, "orders", 1).await;
        close_join_window().await;

        let answer = rejoin.await.unwrap();
        assert_eq!(answer.error, None);
        assert_eq!(answer.member_id, minted);
        assert_eq!(answer.leader, minted);
        assert_eq!(answer.generation, 1);
    }

    /// Below v4 the same empty id is accepted outright, with the minted id in
    /// the successful answer. Old clients (and kcat's group mode) send this.
    #[tokio::test(start_paused = true)]
    async fn an_empty_member_id_below_v4_joins_immediately() {
        let c = coordinator();
        let join = {
            let c = Arc::clone(&c);
            let mut req = join_request("", &["range"]);
            req.member_id_required = false;
            tokio::spawn(async move { c.join("orders", req).await })
        };
        wait_for_members(&c, "orders", 1).await;
        close_join_window().await;

        let answer = join.await.unwrap();
        assert_eq!(answer.error, None);
        assert!(answer.member_id.starts_with("consumer-"));
        assert_eq!(answer.leader, answer.member_id);
    }

    /// The leader is the first joiner, and it is the only member handed
    /// everyone's metadata — verbatim, per member, for the elected protocol.
    #[tokio::test(start_paused = true)]
    async fn the_leader_is_the_first_joiner_and_gets_every_members_metadata() {
        let c = coordinator();
        let first = spawn_join(&c, "orders", "first");
        wait_for_members(&c, "orders", 1).await;
        let second = spawn_join(&c, "orders", "second");
        wait_for_members(&c, "orders", 2).await;
        close_join_window().await;

        let (a, b) = (first.await.unwrap(), second.await.unwrap());
        assert_eq!(a.leader, a.member_id, "the first joiner leads");
        assert_eq!(b.leader, a.member_id);
        assert!(b.members.is_empty(), "a follower gets no roster");

        let roster: HashMap<MemberId, Bytes> = a.members.iter().cloned().collect();
        assert_eq!(roster.len(), 2);
        assert_eq!(
            roster.get(&a.member_id).map(|m| &m[..]),
            Some(&b"first/range"[..])
        );
        assert_eq!(
            roster.get(&b.member_id).map(|m| &m[..]),
            Some(&b"second/range"[..])
        );
    }

    /// The elected protocol is one EVERY member speaks, in the first member's
    /// order of preference — not simply the first name anyone offered.
    #[tokio::test(start_paused = true)]
    async fn the_elected_protocol_is_the_one_everybody_speaks() {
        let c = coordinator();
        let a = spawn_join_with(&c, "orders", "a", &["cooperative-sticky", "range"]);
        wait_for_members(&c, "orders", 1).await;
        let b = spawn_join_with(&c, "orders", "b", &["range"]);
        wait_for_members(&c, "orders", 2).await;
        close_join_window().await;

        let (a, b) = (a.await.unwrap(), b.await.unwrap());
        assert_eq!(a.protocol_name.as_deref(), Some("range"));
        assert_eq!(b.protocol_name.as_deref(), Some("range"));
        // ...and the roster carries the metadata of THAT protocol, not of the
        // leader's favourite.
        let roster: HashMap<MemberId, Bytes> = a.members.iter().cloned().collect();
        assert_eq!(
            roster.get(&a.member_id).map(|m| &m[..]),
            Some(&b"a/range"[..])
        );
    }

    /// A joiner whose assignors are disjoint from the group's is refused ON THE
    /// WAY IN, and it is refused ALONE: the members that were there are neither
    /// answered nor disturbed, and the group they are in survives.
    ///
    /// One mis-set `partition.assignment.strategy` used to take the whole fleet
    /// down with it — every member answered INCONSISTENT_GROUP_PROTOCOL, which
    /// the Java consumer raises out of `poll()`, on every retry of the bad
    /// client.
    #[tokio::test(start_paused = true)]
    async fn a_joiner_with_nothing_in_common_is_refused_alone() {
        let c = coordinator();
        let (leader, follower) = two_member_group(&c, "orders").await;
        stabilise(&c, "orders", &leader, &follower).await;

        let mut bad = join_request("bad", &["sticky"]);
        bad.member_id_required = false;
        let refused = c.join("orders", bad).await;
        assert_eq!(
            refused.error,
            Some(ResponseError::InconsistentGroupProtocol)
        );

        // The group did not move: no rebalance, no generation, same members.
        let after = c.describe("orders").await.unwrap();
        assert_eq!(after.state, State::Stable);
        assert_eq!(after.generation, leader.generation);
        assert_eq!(
            after.members,
            vec![leader.member_id.clone(), follower.member_id.clone()]
        );
        // ...and the members are still working: a heartbeat is clean, which it
        // would not be if the group had been reset underneath them.
        assert_eq!(
            c.heartbeat("orders", &follower.member_id, follower.generation)
                .await,
            None
        );
        // The refusal is not a one-off either — the bad client retries, and
        // every retry is still its own problem.
        let mut again = join_request("bad", &["sticky"]);
        again.member_id_required = false;
        assert_eq!(
            c.join("orders", again).await.error,
            Some(ResponseError::InconsistentGroupProtocol)
        );
        assert_eq!(c.describe("orders").await.unwrap().state, State::Stable);
    }

    /// The same rule while a group is still FORMING: the first joiner sets what
    /// the group speaks, and a second one that speaks nothing in common is
    /// refused without ending the window the first is parked in.
    #[tokio::test(start_paused = true)]
    async fn a_disjoint_joiner_does_not_end_the_window_it_arrives_in() {
        let c = coordinator();
        let first = spawn_join_with(&c, "orders", "a", &["range"]);
        wait_for_members(&c, "orders", 1).await;

        let mut bad = join_request("b", &["sticky"]);
        bad.member_id_required = false;
        assert_eq!(
            c.join("orders", bad).await.error,
            Some(ResponseError::InconsistentGroupProtocol)
        );
        assert_eq!(c.describe("orders").await.unwrap().members.len(), 1);

        close_join_window().await;
        let answer = first.await.unwrap();
        assert_eq!(answer.error, None);
        assert_eq!(answer.protocol_name.as_deref(), Some("range"));
    }

    /// The election is unchanged for the case it was written for: a member that
    /// speaks a SUPERSET is admitted, and the group settles on the name they
    /// share.
    #[tokio::test(start_paused = true)]
    async fn a_joiner_that_shares_one_name_is_admitted() {
        let c = coordinator();
        let a = spawn_join_with(&c, "orders", "a", &["range"]);
        wait_for_members(&c, "orders", 1).await;
        let b = spawn_join_with(&c, "orders", "b", &["sticky", "range"]);
        wait_for_members(&c, "orders", 2).await;
        close_join_window().await;

        for answer in [a.await.unwrap(), b.await.unwrap()] {
            assert_eq!(answer.error, None);
            assert_eq!(answer.protocol_name.as_deref(), Some("range"));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_session_timeout_outside_the_bounds_is_refused() {
        let c = coordinator();
        for bad in [0, 1, 5_999, 300_001, -1] {
            let mut req = join_request("", &["range"]);
            req.session_timeout_ms = bad;
            let answer = c.join("orders", req).await;
            assert_eq!(
                answer.error,
                Some(ResponseError::InvalidSessionTimeout),
                "{bad} was accepted"
            );
        }
        assert_eq!(c.describe("orders").await.unwrap().state, State::Empty);
    }

    #[tokio::test(start_paused = true)]
    async fn a_member_id_the_coordinator_never_issued_is_unknown() {
        let c = coordinator();
        let mut req = join_request("a", &["range"]);
        req.member_id = "invented".to_string();
        let answer = c.join("orders", req).await;
        assert_eq!(answer.error, Some(ResponseError::UnknownMemberId));
        assert_eq!(answer.member_id, "invented", "the id is echoed back");
    }

    #[tokio::test(start_paused = true)]
    async fn a_join_with_no_protocol_is_refused() {
        let c = coordinator();
        let mut empty_list = join_request("", &[]);
        empty_list.member_id_required = false;
        assert_eq!(
            c.join("orders", empty_list).await.error,
            Some(ResponseError::InconsistentGroupProtocol)
        );
        let mut no_type = join_request("", &["range"]);
        no_type.protocol_type.clear();
        no_type.member_id_required = false;
        assert_eq!(
            c.join("orders", no_type).await.error,
            Some(ResponseError::InconsistentGroupProtocol)
        );
    }

    // ----------------------------------------------------------------- sync

    /// The follower's SyncGroup is HELD until the leader posts the
    /// assignments, and what comes back is the leader's own bytes for that
    /// member, untouched.
    #[tokio::test(start_paused = true)]
    async fn a_follower_parks_in_sync_until_the_leader_assigns() {
        let c = coordinator();
        let (leader, follower) = two_member_group(&c, "orders").await;

        let parked = {
            let c = Arc::clone(&c);
            let (id, generation) = (follower.member_id.clone(), follower.generation);
            tokio::spawn(async move {
                c.sync(
                    "orders",
                    SyncRequest {
                        member_id: id,
                        generation,
                        assignments: Vec::new(),
                    },
                )
                .await
            })
        };
        // Still in the sync phase: the follower is waiting and the group has not
        // moved.
        wait_for_state(&c, "orders", State::CompletingRebalance).await;

        let leader_answer = c
            .sync(
                "orders",
                SyncRequest {
                    member_id: leader.member_id.clone(),
                    generation: leader.generation,
                    assignments: vec![
                        (
                            leader.member_id.clone(),
                            Bytes::from_static(b"leader-slice"),
                        ),
                        (
                            follower.member_id.clone(),
                            Bytes::from_static(b"follower-slice"),
                        ),
                    ],
                },
            )
            .await;

        assert_eq!(leader_answer.error, None);
        assert_eq!(&leader_answer.assignment[..], b"leader-slice");
        let follower_answer = parked.await.unwrap();
        assert_eq!(follower_answer.error, None);
        assert_eq!(&follower_answer.assignment[..], b"follower-slice");
        assert_eq!(c.describe("orders").await.unwrap().state, State::Stable);
    }

    /// A member the leader did not name gets an empty assignment and no error:
    /// more consumers than partitions is a normal group, not a fault.
    #[tokio::test(start_paused = true)]
    async fn a_member_the_leader_left_out_gets_an_empty_assignment() {
        let c = coordinator();
        let (leader, follower) = two_member_group(&c, "orders").await;
        let parked = spawn_sync(&c, "orders", &follower.member_id, follower.generation);
        c.sync(
            "orders",
            SyncRequest {
                member_id: leader.member_id.clone(),
                generation: leader.generation,
                assignments: vec![(leader.member_id.clone(), Bytes::from_static(b"all"))],
            },
        )
        .await;
        let answer = parked.await.unwrap();
        assert_eq!(answer.error, None);
        assert!(answer.assignment.is_empty());
    }

    /// A sync at the wrong generation is refused, and a sync from a member the
    /// group does not have is refused differently — the two errors send a
    /// client down two different recovery paths.
    #[tokio::test(start_paused = true)]
    async fn a_sync_is_fenced_by_member_and_generation() {
        let c = coordinator();
        let (leader, _) = two_member_group(&c, "orders").await;

        let stale = c
            .sync(
                "orders",
                SyncRequest {
                    member_id: leader.member_id.clone(),
                    generation: leader.generation - 1,
                    assignments: Vec::new(),
                },
            )
            .await;
        assert_eq!(stale.error, Some(ResponseError::IllegalGeneration));

        let stranger = c
            .sync(
                "orders",
                SyncRequest {
                    member_id: "nobody".to_string(),
                    generation: leader.generation,
                    assignments: Vec::new(),
                },
            )
            .await;
        assert_eq!(stranger.error, Some(ResponseError::UnknownMemberId));
    }

    /// A sync that arrives while the group is collecting joins again is told to
    /// rebalance — the assignment it is asking about belongs to a generation
    /// that is over.
    #[tokio::test(start_paused = true)]
    async fn a_sync_during_a_rebalance_is_told_to_rebalance() {
        let c = coordinator();
        let (leader, follower) = two_member_group(&c, "orders").await;
        stabilise(&c, "orders", &leader, &follower).await;

        // A third consumer arrives: the group reopens the join window.
        let _third = spawn_join(&c, "orders", "third");
        wait_for_state(&c, "orders", State::PreparingRebalance).await;

        let answer = c
            .sync(
                "orders",
                SyncRequest {
                    member_id: follower.member_id.clone(),
                    generation: follower.generation,
                    assignments: Vec::new(),
                },
            )
            .await;
        assert_eq!(answer.error, Some(ResponseError::RebalanceInProgress));
    }

    /// A member that syncs after the group is already Stable — a retry, a slow
    /// client — is answered from what it was given, not made to wait.
    #[tokio::test(start_paused = true)]
    async fn a_late_sync_replays_the_assignment() {
        let c = coordinator();
        let (leader, follower) = two_member_group(&c, "orders").await;
        stabilise(&c, "orders", &leader, &follower).await;

        let again = c
            .sync(
                "orders",
                SyncRequest {
                    member_id: follower.member_id.clone(),
                    generation: follower.generation,
                    assignments: Vec::new(),
                },
            )
            .await;
        assert_eq!(again.error, None);
        assert_eq!(&again.assignment[..], b"follower-slice");
    }

    // ------------------------------------------------------------ heartbeats

    #[tokio::test(start_paused = true)]
    async fn a_heartbeat_is_fenced_and_signals_the_rebalance() {
        let c = coordinator();
        let (leader, follower) = two_member_group(&c, "orders").await;
        stabilise(&c, "orders", &leader, &follower).await;

        assert_eq!(
            c.heartbeat("orders", &follower.member_id, follower.generation)
                .await,
            None,
            "a stable group heartbeats clean"
        );
        assert_eq!(
            c.heartbeat("orders", "nobody", follower.generation).await,
            Some(ResponseError::UnknownMemberId)
        );
        assert_eq!(
            c.heartbeat("orders", &follower.member_id, follower.generation - 1)
                .await,
            Some(ResponseError::IllegalGeneration)
        );

        // A third member arrives, and THIS is how the other two find out.
        let _third = spawn_join(&c, "orders", "third");
        wait_for_state(&c, "orders", State::PreparingRebalance).await;
        assert_eq!(
            c.heartbeat("orders", &follower.member_id, follower.generation)
                .await,
            Some(ResponseError::RebalanceInProgress)
        );
    }

    /// A heartbeat between JoinGroup and SyncGroup is told to carry on. It is
    /// already inside the rebalance; sending it back to the join window would
    /// reopen the one it just came through.
    #[tokio::test(start_paused = true)]
    async fn a_heartbeat_in_the_sync_phase_does_not_restart_the_rebalance() {
        let c = coordinator();
        let (leader, _) = two_member_group(&c, "orders").await;
        assert_eq!(
            c.describe("orders").await.unwrap().state,
            State::CompletingRebalance
        );
        assert_eq!(
            c.heartbeat("orders", &leader.member_id, leader.generation)
                .await,
            None
        );
        assert_eq!(
            c.describe("orders").await.unwrap().state,
            State::CompletingRebalance,
            "the heartbeat moved the group"
        );
    }

    // --------------------------------------------------------------- liveness

    /// A member that stops heartbeating is evicted, and its eviction is a
    /// membership change like any other: the group rebalances.
    #[tokio::test(start_paused = true)]
    async fn a_session_timeout_evicts_and_rebalances() {
        let c = coordinator();
        let (leader, follower) = two_member_group(&c, "orders").await;
        stabilise(&c, "orders", &leader, &follower).await;

        // Half a session in, with a heartbeat: both are alive.
        tokio::time::advance(Duration::from_millis(SESSION_MS as u64 / 2)).await;
        assert_eq!(
            c.heartbeat("orders", &leader.member_id, leader.generation)
                .await,
            None
        );

        // Just past the follower's whole session, and half of the leader's
        // — which its heartbeat pushed forward. Exactly one of them is left,
        // and that is the point: the timer is per member, not per group.
        tokio::time::advance(Duration::from_millis(SESSION_MS as u64 / 2 + 1)).await;
        wait_for_members(&c, "orders", 1).await;
        let after = c.describe("orders").await.unwrap();
        assert_eq!(after.members, vec![leader.member_id.clone()]);
        assert_eq!(after.state, State::PreparingRebalance);
        // ...and the survivor is told, the only way it can be.
        assert_eq!(
            c.heartbeat("orders", &leader.member_id, leader.generation)
                .await,
            Some(ResponseError::RebalanceInProgress)
        );
    }

    /// The leader dying mid-sync is the case the group cannot wait out: the
    /// assignment is coming from a client that is gone. The rebalance restarts,
    /// the parked follower is told so, and the follower becomes the leader.
    #[tokio::test(start_paused = true)]
    async fn a_leader_that_dies_mid_sync_restarts_the_rebalance() {
        let c = coordinator();
        let (_leader, follower) = two_member_group(&c, "orders").await;
        let parked = spawn_sync(&c, "orders", &follower.member_id, follower.generation);

        // The leader never syncs and never heartbeats again.
        tokio::time::advance(Duration::from_millis(SESSION_MS as u64 + 1)).await;

        let answer = parked.await.unwrap();
        assert_eq!(answer.error, Some(ResponseError::RebalanceInProgress));
        let after = c.describe("orders").await.unwrap();
        assert_eq!(after.state, State::PreparingRebalance);
        assert_eq!(after.members, vec![follower.member_id.clone()]);

        // The survivor rejoins and leads the next generation.
        let rejoin = {
            let c = Arc::clone(&c);
            let mut req = join_request("follower", &["range"]);
            req.member_id = follower.member_id.clone();
            tokio::spawn(async move { c.join("orders", req).await })
        };
        let next = rejoin.await.unwrap();
        assert_eq!(next.error, None);
        assert_eq!(next.leader, follower.member_id);
        assert_eq!(
            next.generation,
            follower.generation + 1,
            "the generation fences the assignment that never arrived"
        );
    }

    /// A member whose JoinGroup this coordinator is HOLDING is not evicted for
    /// being quiet: it is quiet because it is waiting for us.
    ///
    /// The shape is the ordinary rebalance: a group is Stable, one more
    /// consumer arrives, and the window that opens is the members' own
    /// rebalance timeout (60s here, and 60s for franz-go's default) — six times
    /// the session timeout. The joiner sends nothing at all while it is parked,
    /// because a client stops heartbeating for the length of a rebalance. Evict
    /// it at its session deadline and the normal "one slow member" rebalance
    /// tears the whole group down instead of completing.
    ///
    /// This test also pins the second half of the exemption: a deadline that
    /// cannot expire is not slept on either ([`Group::next_deadline`]). Without
    /// that, the actor wakes on an instant already past, does nothing, and
    /// wakes again — and this test does not fail, it HANGS, because the spin
    /// never yields the runtime back.
    #[tokio::test(start_paused = true)]
    async fn a_parked_joiner_outlives_its_session_timeout() {
        let c = coordinator();
        let (leader, follower) = two_member_group(&c, "orders").await;
        stabilise(&c, "orders", &leader, &follower).await;

        // The third consumer: the window opens for the members' rebalance
        // timeout, and this one parks in it.
        let third = spawn_join(&c, "orders", "third");
        wait_for_members(&c, "orders", 3).await;
        assert_eq!(
            c.describe("orders").await.unwrap().state,
            State::PreparingRebalance
        );

        // Half a session in, the follower heartbeats and the LEADER goes
        // silent. Past the leader's session the timer fires for it — which is
        // what puts the parked joiner in front of the expiry filter at an
        // instant when its own deadline is long past.
        tokio::time::advance(Duration::from_millis(SESSION_MS as u64 / 2)).await;
        assert_eq!(
            c.heartbeat("orders", &follower.member_id, follower.generation)
                .await,
            Some(ResponseError::RebalanceInProgress)
        );
        tokio::time::advance(Duration::from_millis(SESSION_MS as u64 / 2 + 1)).await;

        wait_for_members(&c, "orders", 2).await;
        let waiting = c.describe("orders").await.unwrap();
        assert_eq!(
            waiting.members,
            vec![follower.member_id.clone(), third_id(&waiting, &follower)],
            "the dead leader took the parked joiner with it"
        );
        assert_eq!(waiting.state, State::PreparingRebalance);
        assert!(
            !third.is_finished(),
            "the parked joiner was answered before the window closed"
        );

        // ...and when the survivor rejoins, the rebalance completes with both
        // of them in it.
        let b = rejoin(&c, "orders", &follower.member_id);
        for answer in futures_join(vec![third, b]).await {
            assert_eq!(answer.error, None);
            assert_eq!(answer.generation, leader.generation + 1);
        }
    }

    /// The member of `snapshot` that is not the one named. For the test above,
    /// whose third joiner is only ever known by the id the coordinator minted.
    fn third_id(snapshot: &Snapshot, known: &JoinAnswer) -> MemberId {
        snapshot
            .members
            .iter()
            .find(|id| *id != &known.member_id)
            .cloned()
            .expect("the group holds a member other than the known one")
    }

    /// The same exemption on the other parked request: a follower waiting for
    /// the leader's assignment, while the leader takes longer than one session
    /// to compute it and heartbeats normally throughout.
    ///
    /// Evicting the follower here answers it UNKNOWN_MEMBER_ID and drops the
    /// group back to PreparingRebalance — a rebalance that restarts because a
    /// member did exactly what the protocol told it to.
    #[tokio::test(start_paused = true)]
    async fn a_follower_parked_in_sync_outlives_its_session_timeout() {
        let c = coordinator();
        let (leader, follower) = two_member_group(&c, "orders").await;
        let parked = spawn_sync(&c, "orders", &follower.member_id, follower.generation);
        wait_for_state(&c, "orders", State::CompletingRebalance).await;

        // Somebody else asks for a member id and never comes back with it. It
        // is what arms a timer while the leader is still thinking: the minted
        // id expires at ITS session, and that wake-up is when every member's
        // deadline is looked at.
        let mut passer_by = join_request("passer-by", &["range"]);
        passer_by.member_id_required = true;
        assert_eq!(
            c.join("orders", passer_by).await.error,
            Some(ResponseError::MemberIdRequired)
        );

        for _ in 0..3 {
            tokio::time::advance(Duration::from_millis(SESSION_MS as u64 / 2)).await;
            assert_eq!(
                c.heartbeat("orders", &leader.member_id, leader.generation)
                    .await,
                None,
                "the leader is inside the rebalance and is told to carry on"
            );
        }
        assert!(!parked.is_finished(), "the parked follower was answered");
        let waiting = c.describe("orders").await.unwrap();
        assert_eq!(waiting.members.len(), 2, "the parked follower was evicted");
        assert_eq!(waiting.state, State::CompletingRebalance);

        // The leader finally posts the assignment, and the follower gets its
        // slice — the rebalance completes rather than starting over.
        let assigned = c
            .sync(
                "orders",
                SyncRequest {
                    member_id: leader.member_id.clone(),
                    generation: leader.generation,
                    assignments: vec![(
                        follower.member_id.clone(),
                        Bytes::from_static(b"follower-slice"),
                    )],
                },
            )
            .await;
        assert_eq!(assigned.error, None);
        let answer = parked.await.unwrap();
        assert_eq!(answer.error, None);
        assert_eq!(&answer.assignment[..], b"follower-slice");
        assert_eq!(c.describe("orders").await.unwrap().state, State::Stable);
    }

    /// Answering a parked member re-arms its session, so a member that comes
    /// out of a park with a deadline already behind it is not evicted for the
    /// silence it was told to keep.
    ///
    /// The shape is a follower parked in sync for longer than its session while
    /// the leader dies: the eviction of the leader answers the follower
    /// REBALANCE_IN_PROGRESS, and that answer is the first moment the follower
    /// can act. Without the re-arm it is evicted on the very next turn of the
    /// loop — before it could possibly have rejoined — and the group empties.
    /// Kafka re-arms in the same place, with the same reason in a comment:
    /// "reset the session timeout for members after propagating the member's
    /// assignment" (`GroupCoordinator.propagateAssignment`).
    #[tokio::test(start_paused = true)]
    async fn answering_a_parked_member_re_arms_its_session() {
        let c = coordinator();
        let (_leader, follower) = two_member_group(&c, "orders").await;
        let parked = spawn_sync(&c, "orders", &follower.member_id, follower.generation);
        // A tick, so the parked sync is registered at a known instant rather
        // than after the jump below — the whole point is that its deadline is
        // in the PAST when the leader's expiry fires.
        tokio::time::advance(Duration::from_millis(1)).await;
        wait_for_state(&c, "orders", State::CompletingRebalance).await;

        // The leader never posts the assignment and never heartbeats again.
        tokio::time::advance(Duration::from_millis(SESSION_MS as u64 + 1)).await;
        assert_eq!(
            parked.await.unwrap().error,
            Some(ResponseError::RebalanceInProgress)
        );
        wait_for_members(&c, "orders", 1).await;

        // A turn later — the turn on which a stale deadline would have been
        // acted on — the survivor is still here, with a session that starts
        // from the answer it was just given.
        tokio::time::advance(Duration::from_millis(1)).await;
        let after = c.describe("orders").await.unwrap();
        assert_eq!(
            after.members,
            vec![follower.member_id.clone()],
            "the member was evicted for the silence it was told to keep"
        );
        assert_eq!(after.state, State::PreparingRebalance);

        // ...and it uses that session: the rejoin lands inside it.
        let again = rejoin(&c, "orders", &follower.member_id);
        assert_eq!(again.await.unwrap().error, None);
    }

    /// The last member leaving empties the group — and the generation moves, so
    /// anything still holding the old one is refused rather than served.
    #[tokio::test(start_paused = true)]
    async fn the_last_member_to_leave_empties_the_group() {
        let c = coordinator();
        let (leader, follower) = two_member_group(&c, "orders").await;
        stabilise(&c, "orders", &leader, &follower).await;

        assert_eq!(c.leave("orders", &follower.member_id).await, None);
        let one_left = c.describe("orders").await.unwrap();
        assert_eq!(one_left.members, vec![leader.member_id.clone()]);
        assert_eq!(one_left.state, State::PreparingRebalance);

        assert_eq!(c.leave("orders", &leader.member_id).await, None);
        let empty = c.describe("orders").await.unwrap();
        assert_eq!(empty.state, State::Empty);
        assert!(empty.members.is_empty());
        assert!(empty.leader.is_none());
        assert!(empty.generation > leader.generation);

        assert_eq!(
            c.leave("orders", &leader.member_id).await,
            Some(ResponseError::UnknownMemberId),
            "leaving twice is not leaving"
        );
    }

    /// An empty group's actor exits, taking its registry entry with it. A
    /// facade that has served a million short-lived group ids holds none of
    /// them.
    #[tokio::test(start_paused = true)]
    async fn an_empty_group_is_eventually_reaped() {
        let c = coordinator();
        let (leader, follower) = two_member_group(&c, "orders").await;
        stabilise(&c, "orders", &leader, &follower).await;
        c.leave("orders", &follower.member_id).await;
        c.leave("orders", &leader.member_id).await;
        assert_eq!(c.live_groups(), 1);

        tokio::time::advance(GroupConfig::default().empty_reap + Duration::from_secs(1)).await;
        for _ in 0..1_000 {
            if c.live_groups() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(c.live_groups(), 0, "the reaped group is still registered");
        assert!(c.describe("orders").await.is_none());

        // ...and the group can be formed again from nothing, which is what a
        // client that comes back finds.
        let join = spawn_join(&c, "orders", "again");
        wait_for_members(&c, "orders", 1).await;
        close_join_window().await;
        assert_eq!(join.await.unwrap().error, None);
    }

    // ------------------------------------------------------------ commit gate

    #[tokio::test(start_paused = true)]
    async fn a_commit_is_fenced_by_member_and_generation() {
        let c = coordinator();
        let (leader, follower) = two_member_group(&c, "orders").await;
        stabilise(&c, "orders", &leader, &follower).await;

        assert_eq!(
            c.check_commit("orders", &follower.member_id, follower.generation)
                .await,
            None
        );
        assert_eq!(
            c.check_commit("orders", &follower.member_id, follower.generation - 1)
                .await,
            Some(ResponseError::IllegalGeneration)
        );
        assert_eq!(
            c.check_commit("orders", "nobody", follower.generation)
                .await,
            Some(ResponseError::UnknownMemberId)
        );
        // A simple consumer committing underneath a live group is refused: two
        // writers on one group's offsets is how progress silently disappears.
        assert_eq!(
            c.check_commit("orders", "", NO_GENERATION).await,
            Some(ResponseError::UnknownMemberId)
        );
    }

    /// The simple consumer: no membership, no generation, and its commits work.
    /// It is what every `assign()`-based client and every offset tool sends.
    #[tokio::test(start_paused = true)]
    async fn a_simple_consumer_commits_without_a_group() {
        let c = coordinator();
        assert_eq!(c.check_commit("standalone", "", NO_GENERATION).await, None);
        // ...and it did not conjure a coordinator to do it.
        assert_eq!(c.live_groups(), 0);

        // A commit that names a member into a group nobody manages is a member
        // of a generation that ended: rejoining is the answer, and
        // ILLEGAL_GENERATION is what asks for it.
        assert_eq!(
            c.check_commit("standalone", "someone", 7).await,
            Some(ResponseError::IllegalGeneration)
        );
        assert_eq!(c.live_groups(), 0);
    }

    /// A commit is proof of life: a consumer deep in a slow batch that commits
    /// without heartbeating must not be evicted for working.
    #[tokio::test(start_paused = true)]
    async fn a_commit_keeps_a_member_alive() {
        let c = coordinator();
        let (leader, follower) = two_member_group(&c, "orders").await;
        stabilise(&c, "orders", &leader, &follower).await;

        for _ in 0..4 {
            tokio::time::advance(Duration::from_millis(SESSION_MS as u64 / 2)).await;
            for m in [&leader, &follower] {
                assert_eq!(
                    c.check_commit("orders", &m.member_id, m.generation).await,
                    None
                );
            }
        }
        assert_eq!(c.describe("orders").await.unwrap().state, State::Stable);
    }

    // ------------------------------------------------------------- generations

    /// Every completed join phase is a new generation, and an empty group moves
    /// one too — so no number is ever reused by two different agreements.
    #[tokio::test(start_paused = true)]
    async fn generations_never_go_backwards() {
        let c = coordinator();
        let (leader, follower) = two_member_group(&c, "orders").await;
        assert_eq!(leader.generation, 1);
        stabilise(&c, "orders", &leader, &follower).await;

        // A third joiner: one more generation, and it is the members that were
        // already there that carry the group into it.
        let third = spawn_join(&c, "orders", "third");
        wait_for_state(&c, "orders", State::PreparingRebalance).await;
        let rejoin_leader = rejoin(&c, "orders", &leader.member_id);
        let rejoin_follower = rejoin(&c, "orders", &follower.member_id);
        let answers = futures_join(vec![third, rejoin_leader, rejoin_follower]).await;
        for a in &answers {
            assert_eq!(a.error, None);
            assert_eq!(a.generation, 2);
        }

        // Everyone leaves: the generation moves again, past the one anybody
        // holds.
        for a in &answers {
            c.leave("orders", &a.member_id).await;
        }
        assert!(c.describe("orders").await.unwrap().generation > 2);
    }

    // ------------------------------------------------------------------ bounds

    /// The rebalance timeout is how long ONE member may hold the whole group's
    /// join window open ([`Group::begin_rebalance`] takes the MAX of them), and
    /// the protocol puts no bound on it: `i32::MAX` milliseconds is
    /// twenty-five DAYS of parked consumers, and nothing in the FSM would wake
    /// to end it. It is clamped rather than refused — the member works, with a
    /// window it will not notice — and an honest value is untouched.
    #[tokio::test(start_paused = true)]
    async fn a_rebalance_timeout_is_bounded_before_it_becomes_a_join_window() {
        for (asked, expected) in [
            (i32::MAX, MAX_REBALANCE_TIMEOUT),
            (60_000, Duration::from_millis(60_000)),
            // v0 has no such field and substitutes the session timeout, which
            // is bounded on its own way in.
            (0, Duration::from_millis(SESSION_MS as u64)),
        ] {
            let mut group = Group::new("orders".to_string(), GroupConfig::default());
            let mut req = join_request("a", &["range"]);
            req.member_id_required = false;
            req.rebalance_timeout_ms = asked;
            let (tx, _rx) = oneshot::channel();
            group.on_join(req, tx);
            assert_eq!(group.members.len(), 1, "asked {asked}: the member joined");
            assert_eq!(
                group.members[0].rebalance_timeout, expected,
                "asked {asked}"
            );
        }

        // ...and the window a later rebalance opens is that clamped number, not
        // the one the client sent.
        let mut group = Group::new("orders".to_string(), GroupConfig::default());
        let mut req = join_request("a", &["range"]);
        req.member_id_required = false;
        req.rebalance_timeout_ms = i32::MAX;
        let (tx, _rx) = oneshot::channel();
        group.on_join(req, tx);
        let opened_at = Instant::now();
        group.begin_rebalance(false);
        assert_eq!(group.join_deadline, Some(opened_at + MAX_REBALANCE_TIMEOUT));
    }

    /// The actor's select is `biased`, so the timer arm is reached only when
    /// the command channel is EMPTY — which a client sending back-to-back
    /// requests never leaves it. Every deadline in this file would then be
    /// deferred for as long as the traffic lasts: sessions that do not expire,
    /// a join window that does not close.
    ///
    /// Eight commands are queued BEFORE the clock moves, so the actor has one
    /// waiting every time it loops, and the clock then passes a member's
    /// session. The eviction happens BETWEEN the queued commands — the
    /// heartbeats after it are told there is a rebalance — rather than after
    /// the last of them.
    #[tokio::test(start_paused = true)]
    async fn a_command_flood_does_not_defer_the_deadlines() {
        let c = coordinator();
        let (alive, doomed) = two_member_group(&c, "orders").await;
        assert_eq!(c.describe("orders").await.unwrap().members.len(), 2);
        let tx = c.existing("orders").expect("the group is running");

        // Filled without yielding: the channel has room, so nothing here parks
        // and the actor is not scheduled until the advance below.
        let mut answers = Vec::new();
        for _ in 0..8 {
            let (reply, answer) = oneshot::channel();
            tx.send(Command::Heartbeat(
                alive.member_id.clone(),
                alive.generation,
                reply,
            ))
            .await
            .expect("the actor is alive");
            answers.push(answer);
        }
        assert!(
            !answers.iter().any(|a| a.is_terminated()),
            "the actor drained the queue before the clock moved"
        );

        tokio::time::advance(Duration::from_millis(SESSION_MS as u64 + 1)).await;
        let mut answered = Vec::new();
        for a in answers {
            answered.push(a.await.expect("every command is answered"));
        }

        // The first heartbeat re-armed the live member and found the group
        // Stable. Then the overdue session was noticed — between commands — so
        // the rest are told to rebalance.
        assert_eq!(answered[0], None);
        assert!(
            answered[1..].contains(&Some(ResponseError::RebalanceInProgress)),
            "the whole burst was answered before the expired session was noticed: {answered:?}"
        );
        assert!(
            !c.describe("orders")
                .await
                .unwrap()
                .members
                .contains(&doomed.member_id),
            "the dead member was never evicted"
        );
    }

    /// The MEMBER_ID_REQUIRED round trip is the one part of a group's state a
    /// peer can grow without ever becoming a member: the answer is immediate,
    /// so ids can be minted as fast as frames can be written, and every one of
    /// them was kept until its session expired — up to five minutes — while
    /// every command in the group paid a scan of the result.
    #[tokio::test(start_paused = true)]
    async fn minted_member_ids_are_bounded() {
        let c = coordinator();
        let mut last = String::new();
        for _ in 0..MAX_PENDING_MEMBERS + 64 {
            let answer = c.join("orders", join_request("junk", &["range"])).await;
            assert_eq!(answer.error, Some(ResponseError::MemberIdRequired));
            last = answer.member_id;
        }
        assert_eq!(
            c.describe("orders").await.unwrap().pending,
            MAX_PENDING_MEMBERS,
            "the minted-id set grew past its bound"
        );

        // The ids that survive are the NEWEST, so the client that just asked
        // for one can still use it — the round trip stays a round trip.
        let joined = {
            let c = Arc::clone(&c);
            let mut req = join_request("junk", &["range"]);
            req.member_id = last.clone();
            tokio::spawn(async move { c.join("orders", req).await })
        };
        wait_for_members(&c, "orders", 1).await;
        close_join_window().await;
        let answer = joined.await.unwrap();
        assert_eq!(answer.error, None);
        assert_eq!(answer.member_id, last);
    }

    /// A heartbeat, a leave and a sync name a membership that is supposed to
    /// exist already, so none of them may CREATE a group: the group id is a
    /// string a peer chooses, and each spawn is a task plus a registry entry
    /// held for `empty_reap` — the hazard `check_commit` was already written to
    /// avoid, reached by three other doors.
    #[tokio::test(start_paused = true)]
    async fn a_heartbeat_a_leave_and_a_sync_never_create_a_group() {
        let c = coordinator();
        for i in 0..100 {
            let group = format!("never-seen-{i}");
            assert_eq!(
                c.heartbeat(&group, "someone", 1).await,
                Some(ResponseError::UnknownMemberId)
            );
            assert_eq!(
                c.leave(&group, "someone").await,
                Some(ResponseError::UnknownMemberId)
            );
            assert_eq!(
                c.sync(
                    &group,
                    SyncRequest {
                        member_id: "someone".to_string(),
                        generation: 1,
                        assignments: Vec::new(),
                    },
                )
                .await
                .error,
                Some(ResponseError::UnknownMemberId)
            );
        }
        assert_eq!(c.live_groups(), 0, "a group id conjured a coordinator");

        // ...and the same three requests against a group that DOES exist are
        // answered by it.
        let (leader, follower) = two_member_group(&c, "orders").await;
        stabilise(&c, "orders", &leader, &follower).await;
        assert_eq!(
            c.heartbeat("orders", &leader.member_id, leader.generation)
                .await,
            None
        );
        assert_eq!(c.leave("orders", &follower.member_id).await, None);
        assert_eq!(c.live_groups(), 1);
    }

    /// ...and the one door that does create a group — JoinGroup, because a join
    /// is what a group is made of — is bounded, retriably.
    #[tokio::test(start_paused = true)]
    async fn the_number_of_group_actors_is_bounded() {
        let c = coordinator();
        for i in 0..crate::coordinator::MAX_GROUPS {
            let answer = c
                .join(&format!("junk-{i}"), join_request("j", &["range"]))
                .await;
            assert_eq!(answer.error, Some(ResponseError::MemberIdRequired), "{i}");
        }
        assert_eq!(c.live_groups(), crate::coordinator::MAX_GROUPS);

        // One more name is refused with the code every client backs off and
        // retries, and it leaves nothing behind.
        let refused = c.join("one-too-many", join_request("j", &["range"])).await;
        assert_eq!(
            refused.error,
            Some(ResponseError::CoordinatorNotAvailable),
            "the group cap let another actor through"
        );
        assert_eq!(c.live_groups(), crate::coordinator::MAX_GROUPS);
        assert!(c.describe("one-too-many").await.is_none());

        // A group that is already there is unaffected by the cap: the members
        // of a live group keep working while new ones are refused.
        assert_eq!(
            c.join("junk-0", join_request("j", &["range"])).await.error,
            Some(ResponseError::MemberIdRequired)
        );

        // ...and the cap is the FACADE's, not a tenant's: it counts actors,
        // and the scope a group is filed under (a resolved tenant, here) does
        // not buy another ten thousand of them.
        let tenant = Arc::new(c.scoped(TenantKey::Tenant(Arc::from("cluster-1"))));
        assert_eq!(
            tenant
                .join("one-too-many", join_request("j", &["range"]))
                .await
                .error,
            Some(ResponseError::CoordinatorNotAvailable),
            "a second scope let another ten thousand actors through"
        );
        assert_eq!(c.live_groups(), crate::coordinator::MAX_GROUPS);
    }

    /// Eviction is keyed by the same identity the registry is: a tenant's
    /// group takes ITS entry with it when it is reaped, and leaves another
    /// tenant's group of the same name registered.
    ///
    /// The two are reaped at different instants (they were created at
    /// different instants), which is what makes the second half of this an
    /// assertion about the KEY rather than about the clock.
    #[tokio::test(start_paused = true)]
    async fn a_reaped_group_takes_only_its_own_tenants_entry() {
        let reap = GroupConfig::default().empty_reap;
        let root = coordinator();
        let one = Arc::new(root.scoped(TenantKey::Tenant(Arc::from("cluster-1"))));
        let two = Arc::new(root.scoped(TenantKey::Tenant(Arc::from("cluster-2"))));

        // A group id named by a client is enough to spawn an actor, which is
        // why an empty one is reaped at all.
        one.join("orders", join_request("j", &["range"])).await;
        tokio::time::advance(reap / 2).await;
        two.join("orders", join_request("j", &["range"])).await;
        assert_eq!(root.live_groups(), 2, "one group id served two tenants");

        // Past the FIRST tenant's reap deadline and short of the second's.
        tokio::time::advance(reap / 2 + Duration::from_secs(1)).await;
        for _ in 0..1_000 {
            if root.live_groups() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            one.describe("orders").await.is_none(),
            "the reaped group is still registered"
        );
        assert!(
            two.describe("orders").await.is_some(),
            "reaping one tenant's group took another tenant's entry with it"
        );

        // ...and the survivor goes on its own deadline, leaving nothing.
        tokio::time::advance(reap).await;
        for _ in 0..1_000 {
            if root.live_groups() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            root.live_groups(),
            0,
            "an actor exited without removing its own registry entry"
        );
    }

    // ------------------------------------------------------ the two readings

    /// [`Snapshot`] and [`GroupDescription`] are two reads of the SAME instant
    /// in the same actor, kept apart so that a wire shape carrying opaque
    /// member bytes does not churn the FSM assertions in this file. The one
    /// thing they must never do is disagree, and this is the test the doc
    /// comment on `GroupDescription` promises.
    ///
    /// Asserted at every state a live actor can be observed in and not at one,
    /// because a divergence would come from a TRANSITION rather than from a
    /// resting state, and because the two paths are two independent actor
    /// `Command`s: `ListGroups` renders a group from `describe`, `DescribeGroups`
    /// from `describe_group`, and nothing but this test stops the two from
    /// drifting into answering an operator's two tools differently about the
    /// same group in the same second.
    ///
    /// The walk is one group through its whole life: PreparingRebalance while
    /// forming (members known, nothing elected), CompletingRebalance (the
    /// generation has moved, the leader owes an assignment), Stable (every
    /// field populated and the opaque bytes carried), PreparingRebalance again
    /// after a member leaves (which is NOT the same shape as the first one),
    /// Empty (the FSM clears the protocol type and the member list at once),
    /// and the absence left by the reaper.
    ///
    /// `Dead` itself is deliberately absent from that list: it is not
    /// observable. The actor sets it and returns from its loop on the same turn
    /// ([`Group::on_timer`]), so no `Command` is ever served in that state and
    /// neither reading can be taken there. What replaces it is a group with no
    /// actor, which both readings answer `None` for, and that is what the last
    /// step pins.
    #[tokio::test(start_paused = true)]
    async fn the_two_readings_of_a_group_never_disagree() {
        let c = coordinator();

        async fn agree(c: &Coordinator, group: &str, at: &str) -> Snapshot {
            use crate::handlers::list_groups::state_name;

            let snapshot = c.describe(group).await.expect("no actor");
            let described = c.describe_group(group).await.expect("no actor");
            assert_eq!(described.state, snapshot.state, "state at {at}");
            // The same states again in the WIRE vocabulary, because the string
            // is what a client compares: `states_filter` on ListGroups is
            // matched literally, and DescribeGroups puts the answer in
            // `group_state`. Both render through the one `state_name`, so the
            // only way the two APIs can disagree about a group is the two
            // Commands' `state` fields disagreeing here.
            assert_eq!(
                state_name(described.state),
                state_name(snapshot.state),
                "state string at {at}"
            );
            assert_eq!(
                described.generation, snapshot.generation,
                "generation at {at}"
            );
            assert_eq!(
                described
                    .members
                    .iter()
                    .map(|m| m.id.clone())
                    .collect::<Vec<_>>(),
                snapshot.members,
                "member ids (and their order) at {at}"
            );
            // Not named by the design, but read off the same fields and just as
            // cheap to pin: a handler branches on the protocol type being None.
            assert_eq!(
                described.protocol_type, snapshot.protocol_type,
                "protocol type at {at}"
            );
            assert_eq!(
                described.protocol_name, snapshot.protocol_name,
                "protocol name at {at}"
            );
            snapshot
        }

        // PreparingRebalance while FORMING: both joins are parked inside the
        // window, so the members are known and the generation they will all be
        // told does not exist yet. Driven by hand rather than through
        // `two_member_group`, which closes the window on its way out and would
        // step over this point.
        let first = spawn_join(&c, "orders", "leader");
        wait_for_members(&c, "orders", 1).await;
        let second = spawn_join(&c, "orders", "follower");
        wait_for_members(&c, "orders", 2).await;
        let forming = agree(&c, "orders", "PreparingRebalance (forming)").await;
        assert_eq!(forming.state, State::PreparingRebalance);
        assert_eq!(forming.generation, 0, "the fixture is wrong");
        assert_eq!(
            forming.protocol_name, None,
            "nothing is elected until the window closes"
        );
        assert_eq!(
            forming.protocol_type.as_deref(),
            Some("consumer"),
            "the type is known from the first join, the assignor is not"
        );

        // Mid-rebalance: the window closed, nobody has synced.
        close_join_window().await;
        let (leader, follower) = (first.await.unwrap(), second.await.unwrap());
        assert_eq!(leader.error, None);
        assert_eq!(follower.error, None);
        let joining = agree(&c, "orders", "CompletingRebalance").await;
        assert_eq!(joining.state, State::CompletingRebalance);
        assert_eq!(joining.members.len(), 2, "the fixture is wrong");

        // Stable, where every field is populated and the members carry the
        // leader's own bytes.
        stabilise(&c, "orders", &leader, &follower).await;
        let stable = agree(&c, "orders", "Stable").await;
        assert_eq!(stable.state, State::Stable);
        let described = c.describe_group("orders").await.unwrap();
        assert_eq!(
            described
                .members
                .iter()
                .map(|m| (m.metadata.clone(), m.assignment.clone()))
                .collect::<Vec<_>>(),
            vec![
                (
                    Bytes::from_static(b"leader/range"),
                    Bytes::from_static(b"leader-slice"),
                ),
                (
                    Bytes::from_static(b"follower/range"),
                    Bytes::from_static(b"follower-slice"),
                ),
            ],
            "the description carried bytes the snapshot's member ids did not agree with"
        );

        // PreparingRebalance again, and NOT the shape the first one had: one
        // member is left, the generation has already moved and the elected
        // protocol is still set. A reading that took "rebalancing" to mean
        // "nothing is elected" would agree while forming and disagree here.
        assert_eq!(c.leave("orders", &follower.member_id).await, None);
        wait_for_state(&c, "orders", State::PreparingRebalance).await;
        let reforming = agree(&c, "orders", "PreparingRebalance (re-forming)").await;
        assert_eq!(reforming.members, vec![leader.member_id.clone()]);
        assert_eq!(reforming.protocol_name.as_deref(), Some("range"));

        // Empty: the last member leaves, and the two readings must clear
        // together.
        assert_eq!(c.leave("orders", &leader.member_id).await, None);
        wait_for_state(&c, "orders", State::Empty).await;
        let empty = agree(&c, "orders", "Empty").await;
        assert!(empty.members.is_empty());
        assert_eq!(empty.protocol_type, None);

        // ...and then the reaper, which is as close to `Dead` as either reading
        // gets. Both must answer `None`, because each handler renders that
        // absence its own way and both renderings are only correct if the
        // absence is common: DescribeGroups asks the durable index whether the
        // group is `Empty` or `Dead`, ListGroups drops it from the live half
        // and lets the index answer for it.
        tokio::time::advance(GroupConfig::default().empty_reap + Duration::from_secs(1)).await;
        for _ in 0..1_000 {
            if c.live_groups() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(c.live_groups(), 0, "the empty group was never reaped");
        assert!(
            c.describe("orders").await.is_none(),
            "a reaped group still has a snapshot"
        );
        assert!(
            c.describe_group("orders").await.is_none(),
            "a reaped group still has a description, so DescribeGroups would \
             answer a live group where ListGroups shows none"
        );
    }

    // ---------------------------------------------------------------- helpers

    fn spawn_sync(
        c: &Arc<Coordinator>,
        group: &str,
        member: &str,
        generation: i32,
    ) -> JoinHandle<SyncAnswer> {
        let c = Arc::clone(c);
        let (group, member) = (group.to_string(), member.to_string());
        tokio::spawn(async move {
            c.sync(
                &group,
                SyncRequest {
                    member_id: member,
                    generation,
                    assignments: Vec::new(),
                },
            )
            .await
        })
    }

    fn rejoin(c: &Arc<Coordinator>, group: &str, member: &str) -> JoinHandle<JoinAnswer> {
        let c = Arc::clone(c);
        let (group, member) = (group.to_string(), member.to_string());
        tokio::spawn(async move {
            let mut req = join_request(&member, &["range"]);
            req.member_id = member;
            c.join(&group, req).await
        })
    }

    async fn futures_join(handles: Vec<JoinHandle<JoinAnswer>>) -> Vec<JoinAnswer> {
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            out.push(h.await.unwrap());
        }
        out
    }

    /// A group of two, joined and waiting in the sync phase. Returns (leader,
    /// follower) answers.
    async fn two_member_group(c: &Arc<Coordinator>, group: &str) -> (JoinAnswer, JoinAnswer) {
        let first = spawn_join(c, group, "leader");
        wait_for_members(c, group, 1).await;
        let second = spawn_join(c, group, "follower");
        wait_for_members(c, group, 2).await;
        close_join_window().await;
        let (a, b) = (first.await.unwrap(), second.await.unwrap());
        assert_eq!(a.error, None);
        assert_eq!(b.error, None);
        assert_eq!(a.leader, a.member_id, "the first joiner leads");
        (a, b)
    }

    /// Take a joined group through its sync phase, with a slice each.
    async fn stabilise(
        c: &Arc<Coordinator>,
        group: &str,
        leader: &JoinAnswer,
        follower: &JoinAnswer,
    ) {
        let parked = spawn_sync(c, group, &follower.member_id, follower.generation);
        let answer = c
            .sync(
                group,
                SyncRequest {
                    member_id: leader.member_id.clone(),
                    generation: leader.generation,
                    assignments: vec![
                        (
                            leader.member_id.clone(),
                            Bytes::from_static(b"leader-slice"),
                        ),
                        (
                            follower.member_id.clone(),
                            Bytes::from_static(b"follower-slice"),
                        ),
                    ],
                },
            )
            .await;
        assert_eq!(answer.error, None);
        assert_eq!(parked.await.unwrap().error, None);
        assert_eq!(c.describe(group).await.unwrap().state, State::Stable);
    }
}
