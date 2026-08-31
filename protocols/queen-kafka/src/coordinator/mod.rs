//! The group coordinator: one actor per consumer group, and the registry that
//! finds it.
//!
//! ## What a coordinator is, in one paragraph
//!
//! A Kafka consumer group is a distributed agreement between clients, arbitrated
//! by one broker: members announce themselves (JoinGroup) with the assignment
//! protocols they speak, the broker elects one of them LEADER and hands it every
//! member's opaque protocol metadata, the leader computes who reads what and
//! posts the answer back (SyncGroup), and the broker hands each member its own
//! slice. Everything after that is liveness: a heartbeat every few seconds, a
//! session timer that evicts a member that stops, and a GENERATION number that
//! makes every message from a member of a previous agreement refusable. The
//! broker never parses an assignment and never decides one — that is the
//! client's own partition.assignment.strategy, and a broker that peeked would
//! break every client whose strategy it does not know.
//!
//! ## Why an actor
//!
//! Every rule in that paragraph is a rule about ORDER: a join that lands during
//! a sync, a session that expires while its member is being handed an
//! assignment, a leader that dies between the two. A lock around a shared group
//! struct would make each of those a separate interleaving to reason about; a
//! single task owning the state, reading commands off an mpsc and answering
//! oneshots, makes the FSM literally single-threaded and the interleavings
//! literally impossible. It is also what makes the tests in [`group`]
//! deterministic under `tokio::time::pause`: the only clock the state machine
//! reads is tokio's.
//!
//! ## Nothing here is durable, and that is the design
//!
//! PLAN_QUEEN_KAFKA.md: "Group membership is in-memory; a facade restart behaves
//! like a Kafka broker restart (clients rejoin, resume from committed offsets)."
//! So this module holds members, generations and assignments, and NOT offsets —
//! those go to Queen through [`crate::offsets`] and outlive everything here. A
//! restarted facade knows no members, answers UNKNOWN_MEMBER_ID to the first
//! heartbeat of every survivor, and they rejoin: the same sequence a real broker
//! failover produces, which every client already implements.
//!
//! ## The M5 seam: what the registry is keyed by, and why it is the TENANT
//!
//! The registry is keyed by (tenant, group id) — [`GroupKey`] — because a
//! connection carries its own credential from M5 on and `orders-consumer` is a
//! name two strangers pick. Without the first half, two tenants would share one
//! coordinator, one generation and one another's members.
//!
//! The first half used to be the CREDENTIAL, and that closed the collision in
//! one direction while opening another. Queen scopes committed offsets by
//! TENANT (the broker takes it from the header the proxy stamps for the cluster
//! the credential named, and the key this facade composes carries no credential
//! at all — [`crate::offsets::key`]), so a registry keyed by the credential
//! agreed with the offsets only while a tenant had ONE credential. With two — a
//! key rotation, a per-service key — two consumers of one group id became the
//! sole member of two groups, each assigned everything by its own leader, both
//! writing the same offset keys with a generation the other cannot see.
//!
//! So the scope is now the tenant Queen names for the credential
//! ([`crate::identity`]): one call to `GET /auth/me` per credential, at
//! authentication, and the answer keys this registry and the queue-list cache
//! alike. Two credentials of one tenant are ONE coordinator; two tenants that
//! pick the same group id are still two.
//!
//! What remains, and it is named here rather than left to be found: **that call
//! resolves nothing in most deployments today**. The broker answers `/auth/me`
//! with a dashboard session or a 401, and the proxy's reads a cookie, so a
//! bearer is identified only on an auth-off broker (where every credential
//! genuinely IS one tenant). Everywhere else the key falls back to the hashed
//! credential — exactly the behaviour above, including its seam — and the
//! facade does the one honest thing available to it: it NOTICES.
//! [`Coordinator::actor`] logs when a group id goes live under a second scope
//! that nobody could resolve, which is the diagnostic for the duplicate
//! consumption that configuration produces. Two RESOLVED tenants sharing a
//! group id are no longer worth a line: that is two tenants, which is what the
//! key is for.
//!
//! ## What is deliberately not here
//!
//! STATIC MEMBERSHIP (KIP-345, `group.instance.id`). A static member survives a
//! restart without triggering a rebalance, which means the coordinator must hold
//! its identity across the session timeout and fence a second instance of it.
//! That is a second, subtler liveness model on top of this one, so it is out of
//! scope — and it is kept out by the VERSION CAPS in `crate::versions` rather
//! than by ignoring the field: JoinGroup stops at v4, SyncGroup, Heartbeat and
//! LeaveGroup at v2, OffsetCommit at v6, each one version below where
//! `group_instance_id` appears. A client cannot send what it cannot negotiate,
//! so there is no path here that silently drops a static member's identity.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::error::ResponseError;
use tokio::sync::{mpsc, oneshot};

use crate::identity::TenantKey;

pub mod group;

/// A member id. Minted by the coordinator, opaque to everyone else.
pub type MemberId = String;

/// The commands a group actor takes. Every one carries the oneshot it must be
/// answered on — including the two that are allowed to hold it: a JoinGroup
/// parks until the join window closes, and a follower's SyncGroup parks until
/// the leader posts the assignments.
enum Command {
    Join(JoinRequest, oneshot::Sender<JoinAnswer>),
    Sync(SyncRequest, oneshot::Sender<SyncAnswer>),
    Heartbeat(MemberId, i32, oneshot::Sender<Option<ResponseError>>),
    Leave(MemberId, oneshot::Sender<Option<ResponseError>>),
    /// OffsetCommit's membership check. Not a write: the write is
    /// [`crate::offsets`]'s, and it happens only if this says yes.
    CheckCommit(MemberId, i32, oneshot::Sender<Option<ResponseError>>),
    Describe(oneshot::Sender<Snapshot>),
    /// DescribeGroups' read. A SECOND observation of the same fields rather
    /// than a widened [`Snapshot`]: see [`GroupDescription`].
    DescribeGroup(oneshot::Sender<GroupDescription>),
    /// DeleteGroups' last step. Answers whether the group was empty, and reaps
    /// it when it was — see [`Coordinator::discard_if_empty`].
    Discard(oneshot::Sender<bool>),
}

/// One assignment protocol a member supports: a name the group votes on, and
/// metadata this facade never looks inside.
///
/// The metadata is the client's subscription — its topic list, its user data,
/// its rack — encoded by an assignor this facade has never heard of. It is
/// carried from every member's JoinGroup to the leader's JoinGroup response and
/// nowhere else, byte for byte.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Protocol {
    pub name: String,
    pub metadata: Bytes,
}

/// What a JoinGroup asks for.
#[derive(Debug, Clone)]
pub struct JoinRequest {
    /// Empty on a client's very first join. See [`JoinRequest::member_id_required`].
    pub member_id: MemberId,
    /// The connection's `client.id`. It makes a minted member id readable in a
    /// log — Kafka's own coordinator names members `<client.id>-<uuid>` for the
    /// same reason — and from M7 F2 it is also a column of
    /// `kafka-consumer-groups.sh --describe` ([`MemberDescription`]).
    pub client_id: String,
    /// Where this member connected from, in Apache Kafka's own spelling
    /// (`/127.0.0.1`). Carried because it is the first column an operator reads
    /// when a partition is stuck on a member, and it exists nowhere else in the
    /// facade: the peer address is known only to `conn::serve`, which is why
    /// [`crate::conn::Conn`] carries it from the accept loop to this field.
    ///
    /// Empty is a legal value and means "nobody told us" — never a
    /// plausible-looking placeholder, which is what an operator would act on.
    pub client_host: String,
    /// `consumer` for a consumer group, `connect` for Kafka Connect, and
    /// whatever else a client invents. Compared across members, never
    /// interpreted.
    pub protocol_type: String,
    pub protocols: Vec<Protocol>,
    pub session_timeout_ms: i32,
    /// How long the group may wait for this member to rejoin a rebalance. v0
    /// has no such field; the handler passes the session timeout there.
    pub rebalance_timeout_ms: i32,
    /// Whether this request came at JoinGroup v4 or above, where an empty
    /// member id is answered MEMBER_ID_REQUIRED with a freshly minted id and
    /// the client immediately rejoins with it (KIP-394). Below v4 the same
    /// empty id is accepted and the minted id comes back with the join.
    ///
    /// The dance exists because a client that gives up between joining and
    /// syncing — a `poll()` that took too long, a process that was killed —
    /// leaves the coordinator holding a member that will never sync, and every
    /// retry of that client used to create ANOTHER one. Making the id a
    /// round trip means a member only ever enters the group when the client is
    /// still there to hear its own name.
    pub member_id_required: bool,
}

/// What a JoinGroup is answered with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinAnswer {
    pub error: Option<ResponseError>,
    pub generation: i32,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub leader: MemberId,
    /// Always set, even on MEMBER_ID_REQUIRED — that answer IS the id.
    pub member_id: MemberId,
    /// Every member's metadata for the elected protocol, and only ever for the
    /// LEADER. A follower gets an empty list, which is how it knows it is one.
    pub members: Vec<(MemberId, Bytes)>,
}

impl JoinAnswer {
    /// A refusal, in the shape Kafka gives one: no generation, no leader, no
    /// protocol. `member_id` is echoed because a client that has one must not
    /// lose it to an error.
    pub(crate) fn refused(error: ResponseError, member_id: MemberId) -> JoinAnswer {
        JoinAnswer {
            error: Some(error),
            generation: NO_GENERATION,
            protocol_type: None,
            protocol_name: None,
            leader: String::new(),
            member_id,
            members: Vec::new(),
        }
    }
}

/// What a SyncGroup asks for: the leader posts every member's assignment, a
/// follower posts nothing and waits.
#[derive(Debug, Clone)]
pub struct SyncRequest {
    pub member_id: MemberId,
    pub generation: i32,
    pub assignments: Vec<(MemberId, Bytes)>,
}

/// What a SyncGroup is answered with: this member's own slice, verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncAnswer {
    pub error: Option<ResponseError>,
    pub assignment: Bytes,
}

impl SyncAnswer {
    pub(crate) fn refused(error: ResponseError) -> SyncAnswer {
        SyncAnswer {
            error: Some(error),
            assignment: Bytes::new(),
        }
    }
}

/// Kafka's "no generation": what a member outside a generation sends and what
/// every refusal answers with.
pub const NO_GENERATION: i32 = -1;

/// A group as of one instant, for the tests and for the log line a stuck
/// rebalance needs. The FSM is not otherwise observable from outside, by
/// design: everything else about it is something a client asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub state: group::State,
    pub generation: i32,
    pub leader: Option<MemberId>,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    /// In join order, which is also leader-election order.
    pub members: Vec<MemberId>,
    /// Member ids minted for the KIP-394 round trip and not yet used to join.
    /// Observable because it is the one part of a group's state a client can
    /// grow without becoming a member, so its bound is worth a test.
    pub pending: usize,
}

/// A group as DescribeGroups needs it: the same instant [`Snapshot`] observes,
/// in the shape the protocol asks for.
///
/// A SECOND read of the same fields rather than a widened `Snapshot`, and the
/// reason is that the two have different owners. `Snapshot` is the FSM's own
/// observation point — the paused-clock tests in [`group`] and the tenancy
/// tests in `crate::lib` read `members: Vec<MemberId>` and assert on it — while
/// this one is a wire shape that carries two opaque byte strings per member.
/// Widening the first into the second would churn every one of those assertions
/// for a handler that does not need them. What the two must never do is
/// disagree — ListGroups renders a group from [`Coordinator::describe`] and
/// DescribeGroups from [`Coordinator::describe_group`], so a drift between them
/// answers an operator's two tools differently about the same group in the same
/// second. `group`'s `the_two_readings_of_a_group_never_disagree` pins that at
/// every state a live actor can be observed in, and pins the ABSENCE both
/// answer once the actor is reaped; `Dead` is not among them because the actor
/// sets it and exits on the same turn, so no command is ever served in it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupDescription {
    pub state: group::State,
    pub generation: i32,
    /// `consumer`, `connect`, whatever the members agreed on. `None` for a
    /// group with no members: the FSM clears it so the next group under this id
    /// is free to be a different kind ([`group`]'s `become_empty`), and the
    /// durable index is what remembers it across that
    /// ([`crate::offsets::index_key`]).
    pub protocol_type: Option<String>,
    /// The elected assignment protocol — `range`, `cooperative-sticky`. `None`
    /// outside a completed generation.
    pub protocol_name: Option<String>,
    /// In join order, which is also leader-election order.
    pub members: Vec<MemberDescription>,
}

/// One member, as `kafka-consumer-groups.sh --describe` prints it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDescription {
    pub id: MemberId,
    pub client_id: String,
    /// See [`JoinRequest::client_host`].
    pub client_host: String,
    /// The subscription bytes this member sent at JoinGroup for the ELECTED
    /// protocol, verbatim. Never parsed here — the same rule the whole
    /// coordinator is built on — and passing them through byte for byte is the
    /// whole value of the API: it is what lets a client render the members ×
    /// partitions table.
    pub metadata: Bytes,
    /// The slice the leader posted for this member at SyncGroup, verbatim.
    pub assignment: Bytes,
}

/// The timings of the membership protocol. Every one of them is a duration a
/// client can feel, so every one is an operator knob.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupConfig {
    /// `QUEEN_KAFKA_GROUP_JOIN_DELAY_MS` — how long the FIRST join of an empty
    /// group waits for company before closing the join window.
    ///
    /// Kafka's `group.initial.rebalance.delay.ms`, same default and same
    /// purpose: a fleet of N consumers starting together would otherwise
    /// produce N rebalances, each one assigning everything to the members that
    /// happened to have arrived, and every one of them revoked microseconds
    /// later. Waiting once costs the first consumer three seconds of startup
    /// and saves the group N-1 assignment storms.
    pub join_delay: Duration,
    /// `QUEEN_KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS` — Kafka's
    /// `group.min.session.timeout.ms`. Below it a client's heartbeat interval
    /// would have to be so short that an ordinary GC pause evicts it.
    pub min_session_timeout: Duration,
    /// `QUEEN_KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS` — Kafka's
    /// `group.max.session.timeout.ms`. Above it a dead consumer's partitions
    /// stay unread for longer than anyone would call a failover.
    pub max_session_timeout: Duration,
    /// How long a group with no members is kept before its actor exits.
    ///
    /// Not a Kafka concept and not a client-visible one: committed offsets live
    /// in Queen, so an empty group holds nothing a client can lose — only a
    /// generation counter and a task. Keeping it briefly means a group that
    /// bounces (a rolling restart, a scale to zero and back) resumes its
    /// generation numbering instead of restarting it, and letting it go means a
    /// facade that has served a million short-lived group ids is not holding a
    /// million tasks.
    pub empty_reap: Duration,
}

impl Default for GroupConfig {
    fn default() -> GroupConfig {
        GroupConfig {
            join_delay: Duration::from_millis(3_000),
            min_session_timeout: Duration::from_millis(6_000),
            max_session_timeout: Duration::from_millis(300_000),
            empty_reap: Duration::from_secs(300),
        }
    }
}

/// Ceiling on the two session-timeout bounds and on the join delay. An hour is
/// already far past any of them being useful and is where a typo (`300000` typed
/// as `3000000`) stops being a slow group and starts being one that never
/// notices a dead member.
const MAX_CONFIGURABLE_MS: u64 = 3_600_000;

impl GroupConfig {
    /// Resolve the group knobs from the environment, loudly.
    ///
    /// Same rule as every other knob in this binary (main.rs): a value that
    /// does not parse is a boot failure and not a silent fall back to the
    /// default, because the default is not there to paper over a typo.
    pub fn resolve(get: &dyn Fn(&str) -> Option<String>) -> Result<GroupConfig, String> {
        let def = GroupConfig::default();
        let ms = |key: &str, default: Duration, floor: u64| -> Result<Duration, String> {
            match get(key)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
            {
                None => Ok(default),
                Some(v) => match v.parse::<u64>() {
                    Ok(n) if (floor..=MAX_CONFIGURABLE_MS).contains(&n) => {
                        Ok(Duration::from_millis(n))
                    }
                    _ => Err(format!(
                        "{key}={v} is not a duration in milliseconds — give it an integer in \
                         {floor}..={MAX_CONFIGURABLE_MS}, or unset it for {}",
                        default.as_millis()
                    )),
                },
            }
        };
        // The join delay may legitimately be zero: a single-consumer deployment
        // that wants its group to form immediately, and the compat rig.
        let join_delay = ms("QUEEN_KAFKA_GROUP_JOIN_DELAY_MS", def.join_delay, 0)?;
        let min_session_timeout = ms(
            "QUEEN_KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS",
            def.min_session_timeout,
            1,
        )?;
        let max_session_timeout = ms(
            "QUEEN_KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS",
            def.max_session_timeout,
            1,
        )?;
        if min_session_timeout > max_session_timeout {
            return Err(format!(
                "QUEEN_KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS={} is above \
                 QUEEN_KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS={} — every consumer would be refused \
                 INVALID_SESSION_TIMEOUT whatever it asked for",
                min_session_timeout.as_millis(),
                max_session_timeout.as_millis()
            ));
        }
        Ok(GroupConfig {
            join_delay,
            min_session_timeout,
            max_session_timeout,
            empty_reap: def.empty_reap,
        })
    }
}

/// How many commands a group actor buffers before its senders wait.
///
/// The actor answers every command without doing I/O — the only awaits in the
/// FSM are the ones it holds ON PURPOSE, and those are oneshots it keeps rather
/// than work it is doing — so this queue drains at memory speed and its depth
/// only matters for a thundering herd of joins. A few hundred is more members
/// than any real group has.
const COMMAND_QUEUE: usize = 512;

/// What a group's actor is filed under: the tenant that owns it, and the group
/// id.
///
/// The tenant half is M5's. With SASL on, two tenants may use the same group
/// name — and `orders-consumer` is a name two strangers pick — so a registry
/// keyed by the group id alone would put them in one group: one generation, one
/// leader, one assignment, each rebalancing the other.
///
/// It is the tenant and not the credential, because the tenant is what Queen
/// scopes the group's COMMITTED OFFSETS by: keyed by the credential, one
/// tenant's two keys were two groups sharing one offset namespace. See the
/// module header, and [`crate::identity::TenantKey`] for how a credential is
/// turned into one — including the fallback, which is the hashed credential
/// and never a shared bucket. Either way it is not the token: this map is
/// process-wide and lives as long as the facade, and a map that outlives every
/// connection must not be where a fleet's bearer tokens are kept. The actor is
/// handed the GROUP ID for its logs and its state, and only the registry ever
/// sees the pair.
type GroupKey = (TenantKey, String);

/// How many group actors this facade runs at once.
///
/// A group is spawned by the first JoinGroup that names it and lives at least
/// [`GroupConfig::empty_reap`] past its last member, so without a cap the
/// number of tasks and registry entries is whatever a peer cares to name — on
/// a listener with no SASL, without authenticating at all. Ten thousand is more
/// consumer groups than any Queen tenant has and is two orders of magnitude
/// below where the tasks would be felt; past it a new group is answered
/// COORDINATOR_NOT_AVAILABLE, which every client retries.
const MAX_GROUPS: usize = 10_000;

/// Ceiling on a group id, in characters.
///
/// The protocol has none: `GroupId` is a Kafka string, so at the non-flexible
/// versions this facade advertises a client may send ~32 KB of it, and every
/// copy — the registry key, the actor's own `id`, every log line it writes — is
/// that long. No real group id is: Kafka's own tooling, every client default
/// and every example is a name a person typed. 255 is the same bound the broker
/// puts on a queue name, and INVALID_GROUP_ID is the code a client already
/// treats as "fix your configuration".
pub const MAX_GROUP_ID_CHARS: usize = 255;

/// The rule every group-addressed handler applies before the coordinator is
/// asked anything: is this a group id at all?
///
/// One function so the six APIs that name a group cannot disagree about it —
/// a name JoinGroup refuses but OffsetCommit accepts is a group that can commit
/// and never join.
pub fn invalid_group_id(group: &str) -> Option<ResponseError> {
    (group.is_empty() || group.chars().count() > MAX_GROUP_ID_CHARS)
        .then_some(ResponseError::InvalidGroupId)
}

/// One line per window when the group cap is refusing joins. The cap is by
/// definition reached under a flood, so the log line must not be part of it.
static GROUP_CAP: crate::obs::Sampler = crate::obs::Sampler::new(60_000);

/// ...and for the diagnostic in the module header: one group id under two
/// scopes, at least one of which nobody could resolve to a tenant. Sampled
/// because a fleet that is configured this way produces it on every reconnect
/// of every consumer.
static SHARED_GROUP: crate::obs::Sampler = crate::obs::Sampler::new(60_000);

/// The registry: (tenant, group id) → the actor that owns that group.
///
/// One per process, behind the [`crate::Facade`], and shared by every
/// per-connection view of it. A group's actor is spawned by the first request
/// that names it and exits when the group has been empty for
/// [`GroupConfig::empty_reap`], removing its own entry on the way out.
#[derive(Clone)]
pub struct Coordinator {
    cfg: GroupConfig,
    /// The tenant every group looked up through THIS view belongs to.
    /// Anonymous on the process-wide view and on a listener with no SASL.
    scope: TenantKey,
    /// A `std::sync::Mutex` and not tokio's, because nothing is ever awaited
    /// while it is held: the sender is CLONED out and the lock released before
    /// the command goes anywhere.
    groups: Arc<Mutex<HashMap<GroupKey, mpsc::Sender<Command>>>>,
}

impl Coordinator {
    pub fn new(cfg: GroupConfig) -> Coordinator {
        Coordinator {
            cfg,
            scope: TenantKey::anonymous(),
            groups: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// The same registry, seen by one tenant: every group it names is filed
    /// under that tenant and cannot collide with another one's group of the
    /// same name — while every credential of that same tenant reaches the very
    /// same group. See [`GroupKey`] and [`crate::identity::TenantKey`].
    pub fn scoped(&self, tenant: TenantKey) -> Coordinator {
        Coordinator {
            cfg: self.cfg,
            scope: tenant,
            groups: Arc::clone(&self.groups),
        }
    }

    pub fn config(&self) -> &GroupConfig {
        &self.cfg
    }

    fn key(&self, group: &str) -> GroupKey {
        (self.scope.clone(), group.to_string())
    }

    /// The actor for `group`, spawning it if this is the first anyone has heard
    /// of it, or `None` if the facade is already running [`MAX_GROUPS`].
    ///
    /// The whole lookup happens under one lock, so a burst of first-joins
    /// spawns exactly one actor and the cap cannot be raced past.
    fn actor(&self, group: &str) -> Option<mpsc::Sender<Command>> {
        let key = self.key(group);
        let mut groups = self
            .groups
            .lock()
            .expect("the group registry lock is never held across a panic");
        if let Some(tx) = groups.get(&key) {
            return Some(tx.clone());
        }
        if groups.len() >= MAX_GROUPS {
            if let Some(suppressed) = GROUP_CAP.tick_now() {
                tracing::warn!(
                    target: "kafka",
                    groups = groups.len(),
                    suppressed,
                    "this facade is already coordinating as many consumer groups as it will; \
                     further group ids are answered COORDINATOR_NOT_AVAILABLE until some are \
                     reaped"
                );
            }
            return None;
        }
        // The seam the module header names, now down to its last case. Two
        // RESOLVED tenants running one group id is exactly what this key is
        // for and is not worth a line; an UNRESOLVED scope sharing a group id
        // with anything is the case nobody can judge — those two scopes are
        // either two tenants (fine) or one tenant's two credentials (not fine,
        // and invisible from here, because Queen would not say). Said once per
        // window, because it is a configuration a fleet repeats on every
        // reconnect.
        if shares_with_an_unresolved_scope(&groups, &key) {
            if let Some(suppressed) = SHARED_GROUP.tick_now() {
                tracing::warn!(
                    target: "kafka",
                    group,
                    suppressed,
                    "two scopes are running this group id and at least one of them could not be \
                     resolved to a tenant, so it is filed under its credential. If the two are \
                     two tenants that is expected; if they are ONE tenant's two credentials (a \
                     key rotation, a per-service key) the two groups share one set of committed \
                     offsets and cannot fence each other — consumers will read the same \
                     partitions twice and overwrite each other's progress. Queen names a \
                     credential's tenant at GET /auth/me; a deployment where that answers a \
                     bearer resolves this by itself"
                );
            }
        }
        let (tx, rx) = mpsc::channel(COMMAND_QUEUE);
        tokio::spawn(group::run(
            group.to_string(),
            key.clone(),
            self.cfg,
            rx,
            tx.clone(),
            Arc::clone(&self.groups),
        ));
        groups.insert(key, tx.clone());
        Some(tx)
    }

    /// The actor for `group`, only if there already is one.
    fn existing(&self, group: &str) -> Option<mpsc::Sender<Command>> {
        self.groups
            .lock()
            .expect("the group registry lock is never held across a panic")
            .get(&self.key(group))
            .cloned()
    }

    /// Forget `group`'s actor if the registry still points at this exact
    /// channel. The comparison is what makes it safe: between finding a dead
    /// sender and removing it, another caller may already have spawned a live
    /// replacement, and removing THAT would strand a running actor.
    fn forget(&self, group: &str, dead: &mpsc::Sender<Command>) {
        let key = self.key(group);
        let mut groups = self
            .groups
            .lock()
            .expect("the group registry lock is never held across a panic");
        if groups.get(&key).is_some_and(|tx| tx.same_channel(dead)) {
            groups.remove(&key);
        }
    }

    /// Send one command and wait for its answer, SPAWNING the group's actor if
    /// there is none and restarting it once if the one in the registry has
    /// exited.
    ///
    /// The race it closes: a group goes empty, its actor reaps itself, and a
    /// client that was mid-reconnect sends into the closed channel. Retrying
    /// turns that into "the group is new again", which is exactly what it is.
    ///
    /// THE RETRY IS ON THE SEND AND NOWHERE ELSE, and the distinction is
    /// load-bearing rather than tidy. A failed `send` means the command reached
    /// nobody, so re-sending it is safe. A failed `answer` means the command WAS
    /// delivered and the actor dropped the reply — re-sending a JoinGroup there
    /// would put a SECOND member into the group for one client, and forgetting
    /// the actor would strand a live group behind a fresh one. So a lost answer
    /// is reported as a lost answer: `None`, which the caller turns into a
    /// retriable Kafka error and the client asks again.
    async fn ask<T>(&self, group: &str, make: impl Fn(oneshot::Sender<T>) -> Command) -> Option<T> {
        for _ in 0..2 {
            let tx = self.actor(group)?;
            let (reply, answer) = oneshot::channel();
            if tx.send(make(reply)).await.is_err() {
                // The actor exited between the lookup and the send. Nothing was
                // delivered, so a fresh one can have the same command.
                self.forget(group, &tx);
                continue;
            }
            return match answer.await {
                Ok(v) => Some(v),
                Err(_) => {
                    tracing::warn!(
                        target: "kafka",
                        group,
                        "the group coordinator took a command and answered nothing"
                    );
                    None
                }
            };
        }
        tracing::warn!(target: "kafka", group, "the group coordinator could not be reached");
        None
    }

    /// Send one command to a group that ALREADY EXISTS, and answer `absent`
    /// when none does.
    ///
    /// The distinction from [`Coordinator::ask`] is the whole of what keeps a
    /// group id from being a way to spend this facade's memory. A JoinGroup
    /// creates a group, because that is what a join is; a heartbeat, a leave
    /// and a sync are all about a membership that is supposed to exist already,
    /// so naming a group nobody has joined must not conjure a task and a
    /// registry entry that then sit there for [`GroupConfig::empty_reap`] — the
    /// hazard [`Coordinator::check_commit`] was already written to avoid.
    ///
    /// `absent` is not a new answer either: it is exactly what a freshly
    /// spawned, empty group used to reply, minus the group.
    async fn ask_existing<T>(
        &self,
        group: &str,
        make: impl Fn(oneshot::Sender<T>) -> Command,
        absent: T,
    ) -> Option<T> {
        let Some(tx) = self.existing(group) else {
            return Some(absent);
        };
        let (reply, answer) = oneshot::channel();
        if tx.send(make(reply)).await.is_err() {
            // The actor reaped itself between the lookup and the send, so the
            // group is gone: the unknown-group answer is the right one after
            // all. Nothing was delivered, so nothing is being retried.
            self.forget(group, &tx);
            return Some(absent);
        }
        match answer.await {
            Ok(v) => Some(v),
            Err(_) => {
                tracing::warn!(
                    target: "kafka",
                    group,
                    "the group coordinator took a command and answered nothing"
                );
                None
            }
        }
    }

    /// JoinGroup. Parks until the join window closes — see the module header of
    /// [`group`] for why that is correct and not a stall.
    pub async fn join(&self, group: &str, req: JoinRequest) -> JoinAnswer {
        let member_id = req.member_id.clone();
        self.ask(group, |reply| Command::Join(req.clone(), reply))
            .await
            .unwrap_or_else(|| {
                JoinAnswer::refused(ResponseError::CoordinatorNotAvailable, member_id.clone())
            })
    }

    /// SyncGroup. A follower parks until the leader posts the assignments.
    ///
    /// Never creates a group: a sync belongs to a generation this coordinator
    /// handed out, so a group it does not hold is a member it does not know.
    pub async fn sync(&self, group: &str, req: SyncRequest) -> SyncAnswer {
        self.ask_existing(
            group,
            |reply| Command::Sync(req.clone(), reply),
            SyncAnswer::refused(ResponseError::UnknownMemberId),
        )
        .await
        .unwrap_or_else(|| SyncAnswer::refused(ResponseError::CoordinatorNotAvailable))
    }

    /// Heartbeat. Never creates a group, for the same reason as
    /// [`Coordinator::sync`] — and with more force, since a heartbeat is the
    /// cheapest frame a client sends.
    pub async fn heartbeat(
        &self,
        group: &str,
        member: &str,
        generation: i32,
    ) -> Option<ResponseError> {
        self.ask_existing(
            group,
            |reply| Command::Heartbeat(member.to_string(), generation, reply),
            Some(ResponseError::UnknownMemberId),
        )
        .await
        .unwrap_or(Some(ResponseError::CoordinatorNotAvailable))
    }

    /// LeaveGroup. Never creates a group: leaving one that is not there is
    /// already the answer.
    pub async fn leave(&self, group: &str, member: &str) -> Option<ResponseError> {
        self.ask_existing(
            group,
            |reply| Command::Leave(member.to_string(), reply),
            Some(ResponseError::UnknownMemberId),
        )
        .await
        .unwrap_or(Some(ResponseError::CoordinatorNotAvailable))
    }

    /// May this (member, generation) commit offsets for `group`?
    ///
    /// The one place the coordinator is asked about a group it may never have
    /// heard of, and the reason is Kafka's SIMPLE CONSUMER: a client that
    /// assigns its own partitions and manages no membership at all still commits
    /// its offsets, and it does so with generation -1 and an empty member id.
    /// That commit must work — it is what every `assign()`-based consumer, every
    /// `kafka-console-consumer --group` and every offset-management tool sends —
    /// and it must not conjure a group actor, because a tool that commits once
    /// for ten thousand group ids would otherwise leave ten thousand tasks
    /// behind.
    ///
    /// So an unknown group takes the answer straight from the rule, and the
    /// rules are Apache Kafka's own:
    ///
    ///   * generation -1 with an empty member id, group unknown or EMPTY: yes.
    ///     The group is using Queen as an offset store and nothing else.
    ///   * generation -1 with an empty member id into a group that HAS members:
    ///     UNKNOWN_MEMBER_ID. Someone is committing underneath a live group,
    ///     which is how two consumers silently overwrite each other's progress.
    ///   * anything else against an unknown group: ILLEGAL_GENERATION — either
    ///     the coordinator restarted or this is a member of a generation that
    ///     ended, and both are answered by rejoining.
    pub async fn check_commit(
        &self,
        group: &str,
        member: &str,
        generation: i32,
    ) -> Option<ResponseError> {
        let simple = generation < 0 && member.is_empty();
        let unknown_group = (!simple).then_some(ResponseError::IllegalGeneration);
        self.ask_existing(
            group,
            |reply| Command::CheckCommit(member.to_string(), generation, reply),
            unknown_group,
        )
        .await
        .unwrap_or(unknown_group)
    }

    /// The group as of now, or `None` if no actor holds one. The observation
    /// point the FSM tests read, and the shape DescribeGroups would need.
    pub async fn describe(&self, group: &str) -> Option<Snapshot> {
        let tx = self.existing(group)?;
        let (reply, answer) = oneshot::channel();
        tx.send(Command::Describe(reply)).await.ok()?;
        answer.await.ok()
    }

    /// The same group, in the shape DescribeGroups answers. `None` when no
    /// actor holds one, which the handler turns into Kafka's own answer for a
    /// group it has never heard of: `Dead`, with no error (measured against
    /// `apache/kafka:3.9.1`, not assumed — see `handlers::describe_groups`).
    ///
    /// Never creates a group, for the same reason
    /// [`Coordinator::ask_existing`] exists: an admin tool that describes ten
    /// thousand group ids must not leave ten thousand actors behind.
    pub async fn describe_group(&self, group: &str) -> Option<GroupDescription> {
        let tx = self.existing(group)?;
        let (reply, answer) = oneshot::channel();
        tx.send(Command::DescribeGroup(reply)).await.ok()?;
        answer.await.ok()
    }

    /// The group ids THIS tenant has a live actor for.
    ///
    /// It FILTERS by the scope rather than enumerating the registry, and that
    /// is the whole of ListGroups' tenant safety: the map is process-wide, so
    /// an enumeration would hand one tenant's admin tool another tenant's group
    /// ids. See [`GroupKey`].
    pub fn live(&self) -> Vec<String> {
        self.groups
            .lock()
            .expect("the group registry lock is never held across a panic")
            .keys()
            .filter(|(scope, _)| scope == &self.scope)
            .map(|(_, id)| id.clone())
            .collect()
    }

    /// DeleteGroups' membership rule and its last step in one round trip: is
    /// this group empty, and if it is, reap it now.
    ///
    /// `Some(false)` is Kafka's NON_EMPTY_GROUP — a group with members is not
    /// deletable, which is what stops a delete from silently resetting a
    /// running fleet to `auto.offset.reset`. `Some(true)` is "it was empty and
    /// it is gone", so the DescribeGroups after a delete says `Dead` rather
    /// than `Empty`. `None` is "there is no actor", which is not a failure:
    /// the group's offsets may still be in Queen and are deleted anyway.
    pub async fn discard_if_empty(&self, group: &str) -> Option<bool> {
        let tx = self.existing(group)?;
        let (reply, answer) = oneshot::channel();
        tx.send(Command::Discard(reply)).await.ok()?;
        answer.await.ok()
    }

    /// How many group actors are alive. For the tests that prove one exits.
    pub fn live_groups(&self) -> usize {
        self.groups
            .lock()
            .expect("the group registry lock is never held across a panic")
            .len()
    }
}

/// Is `key`'s group id already live under a different scope that NOBODY could
/// resolve to a tenant?
///
/// A scan of the registry, run once per group creation and bounded by
/// [`MAX_GROUPS`]. It answers the one question this facade can still ask about
/// the seam in the module header. Two scopes that Queen NAMED are two tenants,
/// which is what the key is for and is silent. A scope that fell back to its
/// credential — on either side of the pair — could be one tenant's second key,
/// and nothing here can tell which, so it produces a warning and not a refusal.
fn shares_with_an_unresolved_scope(
    groups: &HashMap<GroupKey, mpsc::Sender<Command>>,
    key: &GroupKey,
) -> bool {
    // With no SASL there is one credential in the whole process, so the
    // question cannot arise and the scan is skipped.
    if key.0.is_anonymous() {
        return false;
    }
    groups.keys().any(|(scope, id)| {
        id == &key.1 && scope != &key.0 && !(scope.is_named() && key.0.is_named())
    })
}

// ------------------------------------------------------------------ member ids

/// Mint a member id for `client_id`.
///
/// The shape is Apache Kafka's — `<client.id>-<uuid>` — and the reason to keep
/// it is operational rather than protocol: every client logs its member id, and
/// one that names the process it belongs to is the difference between reading a
/// rebalance log and guessing at it. Clients never parse it.
///
/// The uuid half is uuid-SHAPED and not a UUIDv4: 64 bits drawn once per process
/// from the OS (through `RandomState`, the same source `HashMap` seeds from) and
/// 64 bits of a process-local counter. That makes a collision within a process
/// impossible rather than improbable, which is the property that matters here —
/// two members sharing an id would be two consumers sharing one seat — and it
/// costs no dependency for a string nothing verifies.
/// How much of a `client.id` a minted member id carries. Long enough for
/// `orders-consumer-7f4c9b-prod-eu-west-1`, far short of the 32 KB the wire
/// allows.
const MAX_CLIENT_ID_CHARS: usize = 64;

pub fn new_member_id(client_id: &str) -> MemberId {
    static SEED: OnceLock<u64> = OnceLock::new();
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let hi = *SEED.get_or_init(|| {
        use std::hash::{BuildHasher, Hasher};
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u64(
            std::time::UNIX_EPOCH
                .elapsed()
                .map_or(0, |d| d.as_nanos() as u64),
        );
        h.finish()
    });
    let lo = COUNTER.fetch_add(1, Ordering::Relaxed);
    let name = match client_id.trim() {
        "" => "queen-kafka",
        // Truncated, because `client.id` is a string the client chooses and the
        // request header it arrives in is not flexible at these versions —
        // ~32 KB of it, copied into a member id that is then kept in the
        // group's pending set, in its member list, and in every log line about
        // it. The id is for reading, and no name worth reading is longer.
        c => {
            let end = c
                .char_indices()
                .nth(MAX_CLIENT_ID_CHARS)
                .map_or(c.len(), |(at, _)| at);
            &c[..end]
        }
    };
    format!(
        "{name}-{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (hi >> 32) as u32,
        (hi >> 16) as u16,
        hi as u16,
        (lo >> 48) as u16,
        lo & 0x0000_ffff_ffff_ffff,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_minted_member_id_names_its_client_and_never_repeats() {
        let a = new_member_id("orders-consumer-1");
        let b = new_member_id("orders-consumer-1");
        assert!(a.starts_with("orders-consumer-1-"), "{a}");
        assert_ne!(a, b, "two members would share one seat");
        // The uuid half is fixed width, whatever the counter is at.
        assert_eq!(a.len(), b.len());
        // A client that sent no client id still gets a usable name.
        assert!(new_member_id("").starts_with("queen-kafka-"));
        assert!(new_member_id("   ").starts_with("queen-kafka-"));
    }

    /// A minted id names its client, and a client that names itself in 32 KB
    /// does not get 32 KB of member id: the string is the client's, and every
    /// copy of it is this facade's.
    #[test]
    fn a_minted_member_id_does_not_carry_an_unbounded_client_id() {
        let id = new_member_id(&"c".repeat(50_000));
        assert!(
            id.len() < MAX_CLIENT_ID_CHARS + 64,
            "a member id of {} bytes",
            id.len()
        );
        assert!(id.starts_with(&"c".repeat(MAX_CLIENT_ID_CHARS)));
        // ...and a multi-byte client id is cut on a character boundary rather
        // than panicking on one.
        assert!(new_member_id(&"é".repeat(50_000)).starts_with("é"));
    }

    /// The rule every group-addressed API applies, in one place so they cannot
    /// disagree — a name JoinGroup refuses and OffsetCommit accepts is a group
    /// that can commit and never join.
    #[test]
    fn a_group_id_is_a_name_and_not_a_payload() {
        assert_eq!(invalid_group_id("orders-consumer"), None);
        assert_eq!(invalid_group_id(&"g".repeat(MAX_GROUP_ID_CHARS)), None);
        assert_eq!(
            invalid_group_id(""),
            Some(ResponseError::InvalidGroupId),
            "an empty group id is Kafka's own configuration mistake"
        );
        assert_eq!(
            invalid_group_id(&"g".repeat(MAX_GROUP_ID_CHARS + 1)),
            Some(ResponseError::InvalidGroupId)
        );
        // Counted in CHARACTERS, so a name that is legal is not refused for
        // being written in a script with wide code points.
        assert_eq!(invalid_group_id(&"é".repeat(MAX_GROUP_ID_CHARS)), None);
    }

    /// The diagnostic for what is LEFT of the seam in the module header: one
    /// group id under two scopes, at least one of them unresolved. It cannot
    /// tell two tenants (fine) from one tenant with two keys (silent duplicate
    /// consumption), which is why it warns rather than refuses — but it must
    /// not fire on the cases that are plainly neither, and it must go quiet on
    /// the case the tenant key has now settled.
    #[test]
    fn one_group_id_under_two_unresolved_scopes_is_noticed() {
        let (tx, _rx) = mpsc::channel(1);
        let mut groups: HashMap<GroupKey, mpsc::Sender<Command>> = HashMap::new();
        let cred =
            |token: Option<&str>, group: &str| (TenantKey::of_credential(token), group.to_string());
        let tenant = |id: &str, group: &str| {
            (
                TenantKey::Tenant(std::sync::Arc::from(id)),
                group.to_string(),
            )
        };
        groups.insert(cred(Some("key-a"), "orders"), tx.clone());

        // The same credential naming the same group is the ordinary case: one
        // tenant, two connections.
        assert!(!shares_with_an_unresolved_scope(
            &groups,
            &cred(Some("key-a"), "orders")
        ));
        // A different group under the same credential is nothing at all.
        assert!(!shares_with_an_unresolved_scope(
            &groups,
            &cred(Some("key-a"), "clicks")
        ));
        // A different, unresolved credential naming the same group is the one
        // worth a line: it may be this tenant's other key.
        assert!(shares_with_an_unresolved_scope(
            &groups,
            &cred(Some("key-b"), "orders")
        ));
        // ...and so is a RESOLVED tenant next to an unresolved credential —
        // the unresolved one could be that same tenant's second key.
        assert!(shares_with_an_unresolved_scope(
            &groups,
            &tenant("cluster-1", "orders")
        ));

        // Two tenants Queen NAMED are two tenants. That is what the key is
        // for, and it is silent.
        let mut named: HashMap<GroupKey, mpsc::Sender<Command>> = HashMap::new();
        named.insert(tenant("cluster-1", "orders"), tx.clone());
        assert!(!shares_with_an_unresolved_scope(
            &named,
            &tenant("cluster-2", "orders")
        ));

        // With no SASL there is one credential in the process, so the question
        // cannot arise and is not asked.
        let mut anonymous: HashMap<GroupKey, mpsc::Sender<Command>> = HashMap::new();
        anonymous.insert(cred(None, "orders"), tx);
        assert!(!shares_with_an_unresolved_scope(
            &anonymous,
            &cred(None, "orders")
        ));
    }

    fn resolve(pairs: &[(&str, &str)]) -> Result<GroupConfig, String> {
        let owned: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        GroupConfig::resolve(&move |k: &str| {
            owned
                .iter()
                .find(|(key, _)| key == k)
                .map(|(_, v)| v.clone())
        })
    }

    #[test]
    fn the_group_knobs_default_to_kafkas_own() {
        let cfg = resolve(&[]).unwrap();
        assert_eq!(cfg.join_delay, Duration::from_millis(3_000));
        assert_eq!(cfg.min_session_timeout, Duration::from_millis(6_000));
        assert_eq!(cfg.max_session_timeout, Duration::from_millis(300_000));
    }

    #[test]
    fn the_group_knobs_are_overridable_and_loud_when_wrong() {
        let cfg = resolve(&[
            ("QUEEN_KAFKA_GROUP_JOIN_DELAY_MS", "0"),
            ("QUEEN_KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", "1000"),
            ("QUEEN_KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS", "60000"),
        ])
        .unwrap();
        assert_eq!(cfg.join_delay, Duration::ZERO);
        assert_eq!(cfg.min_session_timeout, Duration::from_millis(1_000));

        for bad in ["-1", "3 seconds", "1e3", "4000000", ""] {
            // An empty value is unset, not zero — same rule as main.rs.
            let got = resolve(&[("QUEEN_KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", bad)]);
            if bad.is_empty() {
                assert_eq!(
                    got.unwrap().min_session_timeout,
                    Duration::from_millis(6_000)
                );
            } else {
                assert!(
                    got.unwrap_err().contains("MIN_SESSION_TIMEOUT"),
                    "{bad} was accepted"
                );
            }
        }
        // A window with nothing in it refuses every consumer, so it is refused
        // at boot instead.
        let err = resolve(&[
            ("QUEEN_KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", "60000"),
            ("QUEEN_KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS", "6000"),
        ])
        .unwrap_err();
        assert!(err.contains("INVALID_SESSION_TIMEOUT"), "{err}");
    }
}
