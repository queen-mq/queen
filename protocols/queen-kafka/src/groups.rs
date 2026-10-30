//! Consumer groups: Kafka's classic group protocol, held in memory on the raft
//! leader and nowhere else.
//!
//! Membership is the one Kafka state with no Queen twin, and it is rebuilt by
//! the clients themselves: when leadership moves, the new leader starts with no
//! groups, every member's next heartbeat or commit is answered
//! `UNKNOWN_MEMBER_ID` / `ILLEGAL_GENERATION`, and the members rejoin. The
//! committed offsets are the Queen group's positions and do not move.
//!
//! A group with no members leaves memory [`EMPTY_GROUP_TTL`] after its last
//! member did, so a leader that runs for weeks does not keep every group id a
//! client ever used. Its positions stay; a member that joins after that starts
//! the group again, as after a leader change.

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::errors::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
}

impl State {
    pub fn name(self) -> &'static str {
        match self {
            State::Empty => "Empty",
            State::PreparingRebalance => "PreparingRebalance",
            State::CompletingRebalance => "CompletingRebalance",
            State::Stable => "Stable",
        }
    }
}

#[derive(Debug, Clone)]
pub struct JoinArgs {
    pub group: String,
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub protocol_type: String,
    /// Each protocol's metadata, copied out of the request frame: a slice of
    /// it would keep the whole frame alive as long as the member.
    pub protocols: Vec<(String, Bytes)>,
    /// JoinGroup v4+ (KIP-394): a member with no id is handed one with
    /// MEMBER_ID_REQUIRED and joins with it, instead of joining at once.
    pub require_known_member: bool,
}

#[derive(Debug, Clone, Default)]
pub struct JoinOut {
    pub error: i16,
    pub generation: i32,
    pub protocol: Option<String>,
    pub leader: String,
    pub member_id: String,
    pub members: Vec<(String, Bytes)>,
}

impl JoinOut {
    pub fn error(error: i16, member_id: &str) -> JoinOut {
        JoinOut {
            error,
            generation: -1,
            member_id: member_id.to_string(),
            ..JoinOut::default()
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SyncOut {
    pub error: i16,
    pub assignment: Bytes,
}

impl SyncOut {
    pub fn error(error: i16) -> SyncOut {
        SyncOut {
            error,
            ..SyncOut::default()
        }
    }
}

#[derive(Debug, Clone)]
pub struct MemberView {
    pub id: String,
    pub client_id: String,
    pub client_host: String,
    pub metadata: Bytes,
    pub assignment: Bytes,
}

#[derive(Debug, Clone)]
pub struct GroupView {
    pub state: State,
    pub protocol_type: String,
    pub protocol: String,
    pub members: Vec<MemberView>,
}

struct Member {
    client_id: String,
    client_host: String,
    session_timeout: Duration,
    rebalance_timeout: Duration,
    protocols: Vec<(String, Bytes)>,
    assignment: Bytes,
    deadline: Instant,
    order: u64,
    join: Option<oneshot::Sender<JoinOut>>,
    sync: Option<oneshot::Sender<SyncOut>>,
}

impl Member {
    fn metadata_for(&self, protocol: &str) -> Bytes {
        self.protocols
            .iter()
            .find(|(n, _)| n == protocol)
            .map(|(_, m)| m.clone())
            .unwrap_or_default()
    }
}

struct Group {
    state: State,
    generation: i32,
    protocol_type: Option<String>,
    protocol: Option<String>,
    leader: Option<String>,
    members: HashMap<String, Member>,
    /// When a PreparingRebalance completes whoever rejoined.
    deadline: Option<Instant>,
    /// A new group's first round: it waits the join delay even when every
    /// member already joined, so the others of a deploy can join the same
    /// generation.
    initial: Option<Instant>,
    /// The token its clients authenticate with: the console's presence row is
    /// written with it, in their tenant.
    bearer: Option<String>,
    /// Membership changed since the last presence row.
    dirty: bool,
    last_presence: Option<Instant>,
    /// Since when it has had no members ([`EMPTY_GROUP_TTL`]).
    empty_since: Option<Instant>,
    /// Member ids handed out with MEMBER_ID_REQUIRED and not joined yet, with
    /// the time they are forgotten.
    pending: HashMap<String, Instant>,
    /// No member's session ends before this (a lower bound: heartbeats only
    /// move deadlines later), so the sweep skips the group until then.
    next_expiry: Option<Instant>,
}

impl Group {
    fn new() -> Group {
        Group {
            state: State::Empty,
            generation: 0,
            protocol_type: None,
            protocol: None,
            leader: None,
            members: HashMap::new(),
            deadline: None,
            initial: None,
            bearer: None,
            dirty: true,
            last_presence: None,
            empty_since: None,
            pending: HashMap::new(),
            next_expiry: None,
        }
    }
}

pub struct Coordinator {
    inner: Mutex<Inner>,
    join_delay: Duration,
    /// `QUEEN_KAFKA_GROUP_MAX_MEMBERS`: members (and ids handed out to join)
    /// one group may have.
    max_members: usize,
}

struct Inner {
    groups: HashMap<String, Group>,
    seq: u64,
    /// Groups that commit without members (`assign()` consumers): their token
    /// and the time of their last commit.
    committers: HashMap<String, (Option<String>, Instant, Option<Instant>)>,
    /// Groups that left memory after their console row was written: the row
    /// to delete, with the token it was written with ([`Coordinator::gone`]).
    gone: HashMap<String, Option<String>>,
}

/// One group's row for the console ([`Coordinator::presence_due`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresenceRow {
    pub group: String,
    pub bearer: Option<String>,
    pub members: u64,
    /// Members per subscribed topic; `None` when one member's subscription
    /// could not be read.
    pub topics: Option<std::collections::BTreeMap<String, u64>>,
    pub committing: bool,
    pub state: &'static str,
    pub generation: i32,
    pub protocol_type: String,
}

pub enum Joined {
    Now(JoinOut),
    Wait(oneshot::Receiver<JoinOut>),
}

pub enum Synced {
    Now(SyncOut),
    Wait(oneshot::Receiver<SyncOut>),
}

const MIN_SESSION_MS: i32 = 1_000;
const MAX_SESSION_MS: i32 = 3_600_000;

/// How long a group with no members stays in memory after its last member
/// left. Until then ListGroups lists it as Empty and the console shows it;
/// after, DescribeGroups answers Dead and its console row is deleted.
pub const EMPTY_GROUP_TTL: Duration = Duration::from_secs(600);

impl Coordinator {
    pub fn new(join_delay: Duration) -> Coordinator {
        Coordinator {
            inner: Mutex::new(Inner {
                groups: HashMap::new(),
                seq: 0,
                committers: HashMap::new(),
                gone: HashMap::new(),
            }),
            join_delay,
            max_members: crate::config::group_max_members(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn join(&self, a: JoinArgs, now: Instant) -> Joined {
        if a.protocol_type.is_empty() || a.protocols.is_empty() {
            return Joined::Now(JoinOut::error(INCONSISTENT_GROUP_PROTOCOL, &a.member_id));
        }
        if !(MIN_SESSION_MS..=MAX_SESSION_MS).contains(&a.session_timeout_ms) {
            return Joined::Now(JoinOut::error(INVALID_SESSION_TIMEOUT, &a.member_id));
        }
        let mut inner = self.lock();
        inner.seq += 1;
        let seq = inner.seq;
        let g = inner
            .groups
            .entry(a.group.clone())
            .or_insert_with(Group::new);
        if !g.members.is_empty() {
            let same_type = g.protocol_type.as_deref() == Some(a.protocol_type.as_str());
            let common = a.protocols.iter().any(|(name, _)| {
                g.members
                    .values()
                    .all(|m| m.protocols.iter().any(|(n, _)| n == name))
            });
            if !same_type || !common {
                return Joined::Now(JoinOut::error(INCONSISTENT_GROUP_PROTOCOL, &a.member_id));
            }
        }
        let session = Duration::from_millis(a.session_timeout_ms as u64);
        let full = g.members.len() + g.pending.len() >= self.max_members;
        let member_id = if a.member_id.is_empty() {
            if full {
                return Joined::Now(JoinOut::error(GROUP_MAX_SIZE_REACHED, &a.member_id));
            }
            let id = new_member_id(&a.client_id);
            if a.require_known_member {
                // KIP-394: the id first, the membership when the client comes
                // back with it. A client that never does is forgotten after
                // its session timeout.
                g.pending.insert(id.clone(), now + session);
                return Joined::Now(JoinOut::error(MEMBER_ID_REQUIRED, &id));
            }
            id
        } else if g.members.contains_key(&a.member_id) {
            a.member_id.clone()
        } else if g.pending.remove(&a.member_id).is_some() {
            a.member_id.clone()
        } else {
            return Joined::Now(JoinOut::error(UNKNOWN_MEMBER_ID, &a.member_id));
        };
        if !g.members.contains_key(&member_id) && g.members.len() >= self.max_members {
            return Joined::Now(JoinOut::error(GROUP_MAX_SIZE_REACHED, &member_id));
        }
        let (tx, rx) = oneshot::channel();
        let rebalance =
            Duration::from_millis(a.rebalance_timeout_ms.max(a.session_timeout_ms) as u64);
        let is_new = !g.members.contains_key(&member_id);
        let order = g.members.get(&member_id).map_or(seq, |m| m.order);
        let previous = g.members.insert(
            member_id.clone(),
            Member {
                client_id: a.client_id,
                client_host: a.client_host,
                session_timeout: session,
                rebalance_timeout: rebalance,
                protocols: a.protocols,
                assignment: Bytes::new(),
                deadline: now + session,
                order,
                join: Some(tx),
                sync: None,
            },
        );
        g.next_expiry = Some(g.next_expiry.map_or(now + session, |e| e.min(now + session)));
        if let Some(mut p) = previous {
            // A repeat join of a member still waiting: the newer request wins.
            if let Some(old) = p.join.take() {
                let _ = old.send(JoinOut::error(REBALANCE_IN_PROGRESS, &member_id));
            }
            if let Some(old) = p.sync.take() {
                let _ = old.send(SyncOut::error(REBALANCE_IN_PROGRESS));
            }
        }
        g.protocol_type = Some(a.protocol_type);
        match g.state {
            State::Empty | State::Stable | State::CompletingRebalance => {
                start_rebalance(g, now, self.join_delay)
            }
            State::PreparingRebalance => {
                if let (Some(started), true) = (g.initial, is_new) {
                    // Each member arriving in the first round buys the others
                    // another delay, up to the longest rebalance timeout.
                    let cap = started + max_rebalance(g);
                    g.deadline = Some(
                        (now + self.join_delay)
                            .min(cap)
                            .max(g.deadline.unwrap_or(now)),
                    );
                }
            }
        }
        try_complete(g, now);
        Joined::Wait(rx)
    }

    pub fn sync(
        &self,
        group: &str,
        member_id: &str,
        generation: i32,
        assignments: Vec<(String, Bytes)>,
        now: Instant,
    ) -> Synced {
        let mut inner = self.lock();
        let Some(g) = inner.groups.get_mut(group) else {
            return Synced::Now(SyncOut::error(UNKNOWN_MEMBER_ID));
        };
        if !g.members.contains_key(member_id) {
            return Synced::Now(SyncOut::error(UNKNOWN_MEMBER_ID));
        }
        if generation != g.generation {
            return Synced::Now(SyncOut::error(ILLEGAL_GENERATION));
        }
        match g.state {
            State::Empty => Synced::Now(SyncOut::error(UNKNOWN_MEMBER_ID)),
            State::PreparingRebalance => Synced::Now(SyncOut::error(REBALANCE_IN_PROGRESS)),
            State::Stable => {
                let m = g.members.get_mut(member_id).expect("checked above");
                m.deadline = now + m.session_timeout;
                Synced::Now(SyncOut {
                    error: NONE,
                    assignment: m.assignment.clone(),
                })
            }
            State::CompletingRebalance => {
                let (tx, rx) = oneshot::channel();
                {
                    let m = g.members.get_mut(member_id).expect("checked above");
                    m.deadline = now + m.session_timeout;
                    m.sync = Some(tx);
                }
                if g.leader.as_deref() == Some(member_id) {
                    let mut given: HashMap<String, Bytes> = assignments.into_iter().collect();
                    for (id, m) in g.members.iter_mut() {
                        m.assignment = given.remove(id).unwrap_or_default();
                    }
                    g.state = State::Stable;
                    g.dirty = true;
                    for m in g.members.values_mut() {
                        if let Some(s) = m.sync.take() {
                            let _ = s.send(SyncOut {
                                error: NONE,
                                assignment: m.assignment.clone(),
                            });
                        }
                    }
                }
                Synced::Wait(rx)
            }
        }
    }

    pub fn heartbeat(&self, group: &str, member_id: &str, generation: i32, now: Instant) -> i16 {
        let mut inner = self.lock();
        let Some(g) = inner.groups.get_mut(group) else {
            return UNKNOWN_MEMBER_ID;
        };
        let state = g.state;
        let current = g.generation;
        let Some(m) = g.members.get_mut(member_id) else {
            return UNKNOWN_MEMBER_ID;
        };
        if generation != current {
            return ILLEGAL_GENERATION;
        }
        m.deadline = now + m.session_timeout;
        match state {
            State::Empty => UNKNOWN_MEMBER_ID,
            State::PreparingRebalance => REBALANCE_IN_PROGRESS,
            State::CompletingRebalance | State::Stable => NONE,
        }
    }

    pub fn leave(&self, group: &str, member_id: &str, now: Instant) -> i16 {
        let mut inner = self.lock();
        let Some(g) = inner.groups.get_mut(group) else {
            return UNKNOWN_MEMBER_ID;
        };
        let Some(mut m) = g.members.remove(member_id) else {
            return UNKNOWN_MEMBER_ID;
        };
        if let Some(j) = m.join.take() {
            let _ = j.send(JoinOut::error(UNKNOWN_MEMBER_ID, member_id));
        }
        if let Some(s) = m.sync.take() {
            let _ = s.send(SyncOut::error(UNKNOWN_MEMBER_ID));
        }
        members_changed(g, now, self.join_delay);
        NONE
    }

    /// Whether `member_id` at `generation` may commit offsets of `group`.
    pub fn may_commit(&self, group: &str, member_id: &str, generation: i32) -> i16 {
        let inner = self.lock();
        let Some(g) = inner.groups.get(group) else {
            return if generation < 0 {
                NONE
            } else {
                ILLEGAL_GENERATION
            };
        };
        if generation < 0 && g.state == State::Empty {
            return NONE;
        }
        if g.state == State::CompletingRebalance {
            return REBALANCE_IN_PROGRESS;
        }
        if !g.members.contains_key(member_id) {
            return UNKNOWN_MEMBER_ID;
        }
        if generation != g.generation {
            return ILLEGAL_GENERATION;
        }
        NONE
    }

    pub fn describe(&self, group: &str) -> Option<GroupView> {
        let inner = self.lock();
        let g = inner.groups.get(group)?;
        let protocol = g.protocol.clone().unwrap_or_default();
        let mut members: Vec<(&String, &Member)> = g.members.iter().collect();
        members.sort_by_key(|(_, m)| m.order);
        Some(GroupView {
            state: g.state,
            protocol_type: g.protocol_type.clone().unwrap_or_default(),
            members: members
                .into_iter()
                .map(|(id, m)| MemberView {
                    id: id.clone(),
                    client_id: m.client_id.clone(),
                    client_host: m.client_host.clone(),
                    metadata: m.metadata_for(&protocol),
                    assignment: m.assignment.clone(),
                })
                .collect(),
            protocol,
        })
    }

    /// Every group: name, protocol type, state.
    pub fn list(&self) -> Vec<(String, String, State)> {
        let inner = self.lock();
        let mut out: Vec<(String, String, State)> = inner
            .groups
            .iter()
            .map(|(n, g)| {
                (
                    n.clone(),
                    g.protocol_type.clone().unwrap_or_default(),
                    g.state,
                )
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Drop a group with no members. `Err(NON_EMPTY_GROUP)` when it has some.
    pub fn delete(&self, group: &str) -> Result<bool, i16> {
        let mut inner = self.lock();
        match inner.groups.get(group) {
            Some(g) if !g.members.is_empty() => Err(NON_EMPTY_GROUP),
            Some(_) => {
                inner.groups.remove(group);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// The token a group's client authenticated with, for its presence row.
    pub fn note_client(&self, group: &str, bearer: Option<&str>) {
        let mut inner = self.lock();
        if let Some(g) = inner.groups.get_mut(group) {
            if g.bearer.as_deref() != bearer {
                g.bearer = bearer.map(str::to_string);
            }
        }
    }

    /// A commit landed for `group`: a group with no members is an `assign()`
    /// consumer, live while it commits.
    pub fn note_commit(&self, group: &str, bearer: Option<&str>, now: Instant) {
        let mut inner = self.lock();
        if inner
            .groups
            .get(group)
            .is_some_and(|g| !g.members.is_empty())
        {
            return;
        }
        let e = inner
            .committers
            .entry(group.to_string())
            .or_insert((None, now, None));
        e.0 = bearer.map(str::to_string);
        e.1 = now;
    }

    /// The groups whose console row is due: changed since the last one (at
    /// most once a second), or `every` old.
    pub fn presence_due(&self, now: Instant, every: Duration) -> Vec<PresenceRow> {
        // The subscriptions are decoded after the lock is let go: under it,
        // only the metadata handles are cloned (a reference count each).
        let mut subscriptions: Vec<(usize, Vec<Bytes>)> = Vec::new();
        let mut out = self.presence_rows(now, every, &mut subscriptions);
        for (i, metadata) in subscriptions {
            out[i].topics = subscribed_topics(metadata.into_iter());
        }
        out
    }

    fn presence_rows(
        &self,
        now: Instant,
        every: Duration,
        subscriptions: &mut Vec<(usize, Vec<Bytes>)>,
    ) -> Vec<PresenceRow> {
        let mut inner = self.lock();
        let mut out = Vec::new();
        for (name, g) in inner.groups.iter_mut() {
            let since = g.last_presence.map(|t| now.duration_since(t));
            let due = match since {
                None => true,
                Some(d) => d >= every || (g.dirty && d >= Duration::from_secs(1)),
            };
            if !due {
                continue;
            }
            g.dirty = false;
            g.last_presence = Some(now);
            let protocol = g.protocol.clone().unwrap_or_default();
            if g.protocol_type.as_deref() == Some("consumer") {
                subscriptions.push((
                    out.len(),
                    g.members.values().map(|m| m.metadata_for(&protocol)).collect(),
                ));
            }
            out.push(PresenceRow {
                group: name.clone(),
                bearer: g.bearer.clone(),
                members: g.members.len() as u64,
                topics: None,
                committing: false,
                state: g.state.name(),
                generation: g.generation,
                protocol_type: g.protocol_type.clone().unwrap_or_default(),
            });
        }
        // `assign()` consumers: live while they commit, for one window.
        let joined: std::collections::HashSet<String> = inner
            .groups
            .iter()
            .filter(|(_, g)| !g.members.is_empty())
            .map(|(n, _)| n.clone())
            .collect();
        inner.committers.retain(|name, (_, last, _)| {
            !joined.contains(name) && now.duration_since(*last) < every * 3
        });
        for (name, (bearer, _, written)) in inner.committers.iter_mut() {
            if written.is_some_and(|w| now.duration_since(w) < every) {
                continue;
            }
            *written = Some(now);
            out.push(PresenceRow {
                group: name.clone(),
                bearer: bearer.clone(),
                members: 0,
                topics: None,
                committing: true,
                state: "Empty",
                generation: -1,
                protocol_type: String::new(),
            });
        }
        out
    }

    /// This node stopped leading: every group is the next leader's to rebuild.
    pub fn clear(&self) {
        let mut inner = self.lock();
        // The next leader writes the rows now: a delete from here could land
        // after its write.
        inner.gone.clear();
        for (_, mut g) in inner.groups.drain() {
            for (id, m) in g.members.iter_mut() {
                if let Some(j) = m.join.take() {
                    let _ = j.send(JoinOut::error(NOT_COORDINATOR, id));
                }
                if let Some(s) = m.sync.take() {
                    let _ = s.send(SyncOut::error(NOT_COORDINATOR));
                }
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.lock().groups.is_empty()
    }

    /// Expire sessions, finish rebalances whose time is up, and let go of the
    /// groups with no members: at once when they never formed a generation,
    /// [`EMPTY_GROUP_TTL`] after their last member left when they did.
    pub fn tick(&self, now: Instant) {
        let mut inner = self.lock();
        for g in inner.groups.values_mut() {
            // Every member's session, only once the earliest may have ended:
            // this runs ten times a second under the coordinator's lock.
            if g.next_expiry.is_some_and(|e| now >= e) {
                let expired: Vec<String> = g
                    .members
                    .iter()
                    .filter(|(_, m)| m.join.is_none() && m.sync.is_none() && now >= m.deadline)
                    .map(|(id, _)| id.clone())
                    .collect();
                if !expired.is_empty() {
                    for id in &expired {
                        g.members.remove(id);
                    }
                    tracing::info!(target: "kafka", members = ?expired, "group members expired");
                    members_changed(g, now, self.join_delay);
                }
                g.next_expiry = g.members.values().map(|m| m.deadline).min();
            }
            if !g.pending.is_empty() {
                g.pending.retain(|_, until| now < *until);
            }
            try_complete(g, now);
            if g.state == State::Empty && g.members.is_empty() && g.pending.is_empty() {
                g.empty_since.get_or_insert(now);
            } else {
                g.empty_since = None;
            }
        }
        let Inner { groups, gone, .. } = &mut *inner;
        groups.retain(|name, g| {
            let keep = match g.empty_since {
                None => true,
                Some(_) if g.generation == 0 => false,
                Some(since) => now.duration_since(since) < EMPTY_GROUP_TTL,
            };
            if !keep && g.last_presence.is_some() {
                gone.insert(name.clone(), g.bearer.clone());
            }
            keep
        });
    }

    /// The console rows of the groups that left memory, to delete: name and
    /// the token the row was written with. A group that came back since (it
    /// rejoined, or commits without members) is not among them: its row is
    /// written again.
    pub fn gone(&self) -> Vec<(String, Option<String>)> {
        let mut inner = self.lock();
        let Inner {
            groups,
            committers,
            gone,
            ..
        } = &mut *inner;
        gone.drain()
            .filter(|(name, _)| !groups.contains_key(name) && !committers.contains_key(name))
            .collect()
    }
}

fn max_rebalance(g: &Group) -> Duration {
    g.members
        .values()
        .map(|m| m.rebalance_timeout)
        .max()
        .unwrap_or(Duration::from_secs(60))
}

fn start_rebalance(g: &mut Group, now: Instant, join_delay: Duration) {
    if g.state == State::CompletingRebalance {
        for m in g.members.values_mut() {
            if let Some(s) = m.sync.take() {
                let _ = s.send(SyncOut::error(REBALANCE_IN_PROGRESS));
            }
        }
    }
    if g.state == State::Empty {
        g.initial = Some(now);
        g.deadline = Some(now + join_delay.min(max_rebalance(g)));
    } else {
        g.initial = None;
        g.deadline = Some(now + max_rebalance(g));
    }
    g.state = State::PreparingRebalance;
}

/// A member left or expired.
fn members_changed(g: &mut Group, now: Instant, join_delay: Duration) {
    g.dirty = true;
    if g.members.is_empty() {
        g.state = State::Empty;
        g.generation += 1;
        g.protocol = None;
        g.leader = None;
        g.deadline = None;
        g.initial = None;
        return;
    }
    match g.state {
        State::Stable | State::CompletingRebalance => start_rebalance(g, now, join_delay),
        State::PreparingRebalance => try_complete(g, now),
        State::Empty => {}
    }
}

fn try_complete(g: &mut Group, now: Instant) {
    if g.state != State::PreparingRebalance {
        return;
    }
    let all_joined = g.members.values().all(|m| m.join.is_some());
    let due = g.deadline.is_none_or(|d| now >= d);
    if !(due || (all_joined && g.initial.is_none())) {
        return;
    }
    // Whoever did not rejoin in time is out.
    g.members.retain(|_, m| m.join.is_some());
    if g.members.is_empty() {
        g.state = State::Empty;
        g.generation += 1;
        g.protocol = None;
        g.leader = None;
        g.deadline = None;
        g.initial = None;
        return;
    }
    g.generation += 1;
    g.protocol = select_protocol(g);
    let protocol = g.protocol.clone().unwrap_or_default();
    let leader = match g.leader.as_ref().filter(|l| g.members.contains_key(*l)) {
        Some(l) => l.clone(),
        None => g
            .members
            .iter()
            .min_by_key(|(_, m)| m.order)
            .map(|(id, _)| id.clone())
            .expect("members is not empty"),
    };
    g.leader = Some(leader.clone());
    g.state = State::CompletingRebalance;
    g.dirty = true;
    g.deadline = None;
    g.initial = None;
    let mut everyone: Vec<(u64, String, Bytes)> = g
        .members
        .iter()
        .map(|(id, m)| (m.order, id.clone(), m.metadata_for(&protocol)))
        .collect();
    everyone.sort_by_key(|(o, _, _)| *o);
    let everyone: Vec<(String, Bytes)> = everyone.into_iter().map(|(_, id, md)| (id, md)).collect();
    let generation = g.generation;
    for (id, m) in g.members.iter_mut() {
        m.deadline = now + m.session_timeout;
        if let Some(j) = m.join.take() {
            let _ = j.send(JoinOut {
                error: NONE,
                generation,
                protocol: Some(protocol.clone()),
                leader: leader.clone(),
                member_id: id.clone(),
                members: if *id == leader {
                    everyone.clone()
                } else {
                    Vec::new()
                },
            });
        }
    }
}

/// Members per subscribed topic, read from each member's consumer-protocol
/// subscription; `None` when one of them cannot be read.
fn subscribed_topics(
    metadata: impl Iterator<Item = Bytes>,
) -> Option<std::collections::BTreeMap<String, u64>> {
    use kafka_protocol::messages::ConsumerProtocolSubscription;
    use kafka_protocol::protocol::Decodable;
    let mut out = std::collections::BTreeMap::new();
    for md in metadata {
        if md.len() < 2 {
            return None;
        }
        let version = i16::from_be_bytes([md[0], md[1]]).clamp(0, 3);
        let mut body = md.slice(2..);
        let sub = ConsumerProtocolSubscription::decode(&mut body, version).ok()?;
        for t in sub.topics {
            *out.entry(t.to_string()).or_insert(0) += 1;
        }
    }
    Some(out)
}

/// Kafka's choice: among the protocols every member supports, the one most
/// members list first.
fn select_protocol(g: &Group) -> Option<String> {
    let mut members: Vec<&Member> = g.members.values().collect();
    members.sort_by_key(|m| m.order);
    let first = members.first()?;
    let candidates: Vec<&String> = first
        .protocols
        .iter()
        .map(|(n, _)| n)
        .filter(|n| {
            members
                .iter()
                .all(|m| m.protocols.iter().any(|(x, _)| x == *n))
        })
        .collect();
    let mut best: Option<(&String, usize)> = None;
    for c in &candidates {
        let votes = members
            .iter()
            .filter(|m| {
                m.protocols
                    .iter()
                    .map(|(n, _)| n)
                    .find(|n| candidates.contains(n))
                    == Some(*c)
            })
            .count();
        if best.is_none_or(|(_, v)| votes > v) {
            best = Some((c, votes));
        }
    }
    best.map(|(c, _)| c.clone())
}

fn new_member_id(client_id: &str) -> String {
    let mut a = RandomState::new().build_hasher();
    a.write_u64(1);
    let mut b = RandomState::new().build_hasher();
    b.write_u64(2);
    let prefix = if client_id.is_empty() {
        "member"
    } else {
        client_id
    };
    format!("{prefix}-{:016x}{:016x}", a.finish(), b.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn join_args(member: &str) -> JoinArgs {
        JoinArgs {
            group: "g".into(),
            member_id: member.into(),
            client_id: "c".into(),
            client_host: "/127.0.0.1".into(),
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 30_000,
            protocol_type: "consumer".into(),
            protocols: vec![("range".into(), Bytes::from_static(b"md"))],
            require_known_member: false,
        }
    }

    fn joined(j: Joined) -> oneshot::Receiver<JoinOut> {
        match j {
            Joined::Wait(rx) => rx,
            Joined::Now(out) => panic!("answered at once: {out:?}"),
        }
    }

    #[test]
    fn a_new_group_waits_the_delay_then_forms_one_generation() {
        let c = Coordinator::new(Duration::from_millis(3000));
        let t0 = Instant::now();
        let mut a = joined(c.join(join_args(""), t0));
        let mut b = joined(c.join(join_args(""), t0 + Duration::from_millis(100)));
        c.tick(t0 + Duration::from_millis(1000));
        assert!(a.try_recv().is_err(), "still inside the delay");
        c.tick(t0 + Duration::from_millis(3200));
        let (a, b) = (a.try_recv().unwrap(), b.try_recv().unwrap());
        assert_eq!((a.error, b.error), (NONE, NONE));
        assert_eq!((a.generation, b.generation), (1, 1));
        assert_eq!(a.leader, a.member_id, "the first member leads");
        assert_eq!(a.members.len(), 2);
        assert!(b.members.is_empty(), "only the leader gets the member list");
        assert_eq!(a.protocol.as_deref(), Some("range"));

        // The leader's sync answers both.
        let follower = match c.sync("g", &b.member_id, 1, vec![], t0) {
            Synced::Wait(rx) => rx,
            Synced::Now(o) => panic!("{o:?}"),
        };
        let assignments = vec![
            (a.member_id.clone(), Bytes::from_static(b"A")),
            (b.member_id.clone(), Bytes::from_static(b"B")),
        ];
        let mut leader = match c.sync("g", &a.member_id, 1, assignments, t0) {
            Synced::Wait(rx) => rx,
            Synced::Now(o) => panic!("{o:?}"),
        };
        assert_eq!(
            leader.try_recv().unwrap().assignment,
            Bytes::from_static(b"A")
        );
        let mut follower = follower;
        assert_eq!(
            follower.try_recv().unwrap().assignment,
            Bytes::from_static(b"B")
        );
        assert_eq!(c.heartbeat("g", &a.member_id, 1, t0), NONE);
        assert_eq!(c.heartbeat("g", &a.member_id, 0, t0), ILLEGAL_GENERATION);
        assert_eq!(c.may_commit("g", &b.member_id, 1), NONE);
        assert_eq!(c.may_commit("g", "stranger", 1), UNKNOWN_MEMBER_ID);
    }

    #[test]
    fn a_member_that_stops_heartbeating_triggers_a_rebalance_without_it() {
        let c = Coordinator::new(Duration::from_millis(0));
        let t0 = Instant::now();
        let mut a = joined(c.join(join_args(""), t0));
        c.tick(t0);
        let a = a.try_recv().unwrap();
        let mut b = joined(c.join(join_args(""), t0));
        // A known member joining moves the group to PreparingRebalance: the
        // first one learns it from its heartbeat and rejoins.
        assert_eq!(
            c.heartbeat("g", &a.member_id, a.generation, t0),
            REBALANCE_IN_PROGRESS
        );
        let mut a2 = joined(c.join(join_args(&a.member_id), t0));
        let (a2, b) = (a2.try_recv().unwrap(), b.try_recv().unwrap());
        assert_eq!((a2.generation, b.generation), (2, 2));
        // b goes silent past its session.
        c.sync("g", &a2.member_id, 2, vec![], t0);
        c.tick(t0 + Duration::from_millis(9_000));
        assert_eq!(
            c.heartbeat("g", &a2.member_id, 2, t0 + Duration::from_millis(9_000)),
            NONE
        );
        c.tick(t0 + Duration::from_millis(15_000));
        assert_eq!(
            c.heartbeat("g", &a2.member_id, 2, t0 + Duration::from_millis(15_000)),
            REBALANCE_IN_PROGRESS
        );
        assert_eq!(c.heartbeat("g", &b.member_id, 2, t0), UNKNOWN_MEMBER_ID);
    }

    #[test]
    fn a_new_leader_knows_no_group_and_members_rejoin() {
        let c = Coordinator::new(Duration::from_millis(0));
        assert_eq!(
            c.heartbeat("g", "m-1", 4, Instant::now()),
            UNKNOWN_MEMBER_ID
        );
        assert_eq!(c.may_commit("g", "m-1", 4), ILLEGAL_GENERATION);
        assert_eq!(
            c.may_commit("g", "", -1),
            NONE,
            "an assign() consumer commits freely"
        );
        match c.join(join_args("m-1"), Instant::now()) {
            Joined::Now(o) => assert_eq!(o.error, UNKNOWN_MEMBER_ID),
            Joined::Wait(_) => panic!("an unknown member id must rejoin without one"),
        }
    }

    /// One member joins, its row is written, it leaves at `t0`.
    fn left_at(t0: Instant) -> Coordinator {
        let c = Coordinator::new(Duration::from_millis(0));
        let mut a = joined(c.join(join_args(""), t0));
        c.tick(t0);
        let a = a.try_recv().unwrap();
        assert_eq!(c.presence_due(t0, Duration::from_secs(15)).len(), 1);
        assert_eq!(c.leave("g", &a.member_id, t0), NONE);
        c.tick(t0);
        c
    }

    #[test]
    fn a_group_with_no_members_leaves_memory_after_the_ttl_and_its_row_is_deleted() {
        let t0 = Instant::now();
        let c = left_at(t0);
        let empty = vec![("g".to_string(), "consumer".to_string(), State::Empty)];
        assert_eq!(c.list(), empty);
        c.tick(t0 + EMPTY_GROUP_TTL - Duration::from_secs(1));
        assert_eq!(c.list(), empty, "kept until the TTL");
        assert!(c.gone().is_empty());

        c.tick(t0 + EMPTY_GROUP_TTL);
        assert!(c.list().is_empty(), "dropped at the TTL");
        assert!(c.describe("g").is_none());
        assert_eq!(c.gone(), vec![("g".to_string(), None)]);
        assert!(c.gone().is_empty(), "each row is deleted once");

        // A member that joins after that starts the group again.
        let later = t0 + EMPTY_GROUP_TTL;
        let mut b = joined(c.join(join_args(""), later));
        c.tick(later);
        assert_eq!(b.try_recv().unwrap().generation, 1);
    }

    #[test]
    fn a_group_that_rejoins_before_its_row_is_deleted_keeps_it() {
        let t0 = Instant::now();
        let c = left_at(t0);
        let later = t0 + EMPTY_GROUP_TTL;
        c.tick(later);
        assert!(c.list().is_empty());
        let _b = c.join(join_args(""), later);
        assert!(c.gone().is_empty(), "its row is written again, not deleted");
        assert_eq!(c.presence_due(later, Duration::from_secs(15)).len(), 1);
    }

    #[test]
    fn leaving_and_clearing_answer_the_waiters() {
        let c = Coordinator::new(Duration::from_millis(5000));
        let t0 = Instant::now();
        let mut a = joined(c.join(join_args(""), t0));
        c.clear();
        assert_eq!(a.try_recv().unwrap().error, NOT_COORDINATOR);
        assert!(c.is_empty());
    }

    #[test]
    fn the_protocol_is_the_one_most_members_prefer() {
        let c = Coordinator::new(Duration::from_millis(10));
        let t0 = Instant::now();
        let mut x = join_args("");
        x.protocols = vec![
            ("range".into(), Bytes::new()),
            ("roundrobin".into(), Bytes::new()),
        ];
        let mut y = join_args("");
        y.protocols = vec![
            ("roundrobin".into(), Bytes::new()),
            ("range".into(), Bytes::new()),
        ];
        let mut z = join_args("");
        z.protocols = vec![("roundrobin".into(), Bytes::new())];
        let mut rx = [
            joined(c.join(x, t0)),
            joined(c.join(y, t0)),
            joined(c.join(z, t0)),
        ];
        c.tick(t0 + Duration::from_millis(20));
        for r in rx.iter_mut() {
            assert_eq!(
                r.try_recv().unwrap().protocol.as_deref(),
                Some("roundrobin")
            );
        }
    }
}
