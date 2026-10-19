//! The idempotent producer's sequence window (M7 F3).
//!
//! Java has defaulted `enable.idempotence=true` since 3.0 and Spring Boot
//! inherits it, so until this module existed the single largest onboarding
//! papercut of the whole facade was a stock producer with NO configuration
//! dying on its first send: `handlers::produce::refuse` answered
//! UNSUPPORTED_FOR_MESSAGE_FORMAT to any batch carrying a producer id, and the
//! only cure was finding and setting one property. This is what replaces that
//! refusal — not with an acceptance, which would have been the worse outcome,
//! but with the check the producer believes is happening.
//!
//! ## What a Kafka producer actually asks for
//!
//! Under idempotence a producer holds a `(producer_id, epoch)` granted by
//! [`crate::handlers::init_producer_id`] and numbers every batch it sends to a
//! topic-partition with a monotonically increasing `base_sequence`. The broker's
//! side of that contract, which this module implements, is three answers:
//!
//!   * the NEXT sequence is appended, and its offsets are remembered;
//!   * a sequence ALREADY appended is answered `error_code = 0` carrying the
//!     offsets the original append got, and nothing is written — this is what
//!     makes a producer's retry after a lost response invisible, and it is
//!     Kafka's own behaviour rather than an invention here;
//!   * a sequence that would leave a GAP is refused OUT_OF_ORDER_SEQUENCE_NUMBER
//!     and nothing is written — without which "idempotent" would be a claim
//!     about duplicates that said nothing about order.
//!
//! ## Why an in-process window is sound per producer session
//!
//! Three properties compose, and they are properties of THIS facade rather than
//! hopes about clients:
//!
//! 1. The facade advertises itself as the sole node and the leader of every
//!    partition, so a producer's `NetworkClient` holds exactly one connection to
//!    it and every Produce of one producer session arrives on one socket.
//! 2. `conn::connection` checks a connection's produce in arrival order. On
//!    the node that leads it pipelines the pushes (2026-10-17): a batch is
//!    checked while the batches before it may still be in flight, and
//!    [`Producers::check_on`] accepts one that follows the newest batch the
//!    SAME connection has in flight, as it always did within one coalesced
//!    push; the runs are committed to the window as the connection answers
//!    them (or settles them, when the client went away first), in request
//!    order too. Everywhere else the connection is serial: one push,
//!    answered, then the next.
//! 3. The producer itself never advances past a gap: under idempotence the Java
//!    client and librdkafka both re-sequence on failure, which is why
//!    `base_sequence == last_seq + 1` below is a check and not a race.
//!
//! Nothing here is awaited while the lock is held, because there is nothing to
//! await: the window is consulted before the push and committed after it, and
//! the push happens between two separate lock acquisitions.
//!
//! ## THE CAVEAT, stated at the top because it is the honest one
//!
//! A real Kafka broker persists producer state in the log (producer snapshots),
//! so a broker restart or a leader change does not lose a sequence window. This
//! facade holds no durable state by design, so after a **facade restart**, a
//! **leader change**, a connection that **moves to another facade** behind one
//! advertised address, or an LRU **eviction** of an idle producer's entry, the
//! next batch of a producer meets an absent entry.
//!
//! That batch is ACCEPTED, whatever its sequence, and the window starts at it.
//! It is Kafka's own rule for a producer whose state the broker no longer holds:
//! with no producer epoch on record, `ProducerAppendInfo.checkSequence` accepts
//! writes with any sequence number. The cost is bounded and it is real: a batch
//! the producer had in flight when the state was lost, and resends because its
//! answer never came, is **written a second time** — at-least-once for at most
//! [`WINDOW`] batches per partition, once per lost window.
//!
//! ## A window can be OUT OF DATE, too
//!
//! In a cluster a producer's batches for one partition reach whichever facade
//! serves them at that moment: the raft leader's, through the followers'
//! relays (`conn::Relay`); a follower's own, for the second after a restart in
//! which it does not know the leader yet; the next leader's after a change. So
//! a facade can hold a window another facade has since moved past, and the
//! producer's next batch then looks like a GAP in it. Stage, 2026-10-19 21:31:
//! node 0 came back, served one producer's batches 18556..18600 itself, learned
//! the leader a second later and relayed 18601 to node 2 — whose window, from
//! before node 0 returned, still expected 18556. Refusing that gap killed four
//! librdkafka producers, exactly as a lost window did before 2026-10-19.
//!
//! So a gap in a held window is read as what it almost always is here: a window
//! this facade fell behind on. It restarts at the batch, like a lost window,
//! with the same bounded cost, and is counted ([`Producers::resynced`]).
//!
//! Below the window the same holds in reverse: a batch this facade cannot match
//! exactly may be one that was still travelling through another facade when the
//! window restarted (a client that sent its next batch to a new leader before an
//! older one to the old leader failed — the Java client does). Answering it
//! DUPLICATE_SEQUENCE_NUMBER would be a SUCCESS for records nobody wrote, so a
//! clustered facade writes it: at-least-once, possibly behind a later batch,
//! never lost. A facade on its own keeps Apache Kafka's answers to both: nothing
//! else writes its partitions, so its window is never behind.
//!
//! What keeps a gap that IS one — a batch accepted here that was not written,
//! and the batches the client had already sent behind it — from being read as a
//! window that fell behind is the CONNECTION: an idempotent batch that is not
//! written, for any reason but being a duplicate, is answered and the connection
//! is closed after the answer (`handlers::produce`, `conn`; a push that failed
//! on the leader's pipelined path closes it without an answer). A facade on its
//! own that held the producer's window when the batch was accepted keeps the
//! connection for a failed push: the batch behind it is a gap in that window,
//! refused as Kafka refuses it ([`Pending::closes_on_failure`]). Nothing the client
//! sent behind it on that connection is read, relayed or served anywhere, and the
//! client sends again, in order, from its oldest unanswered batch — what a failed
//! pipelined push already did. Without that, a facade with no window whose first
//! push for a producer failed would start a window at the batch behind it, and
//! the failed batch's resend would be answered as a duplicate: a record reported
//! delivered that never was. (librdkafka also counts a batch it is retrying as
//! delivered once a LATER batch of the partition succeeds.) Three places make
//! "nothing behind it" true:
//!
//!   * requests read together — a coalesced group, a pipelined one — are staged
//!     up to the one that refused the batch and no further: the rest are never
//!     written or answered (`handlers::produce::handle_coalesced`,
//!     `stage_coalesced`), and the pipelined connection stages nothing more;
//!   * a follower that RELAYED the batch reads the leader's answer the way the
//!     leader did and ends its client's connection with the relay
//!     (`conn::relayed_answer_ends`), so the next batch cannot reach a new
//!     leader, or the follower itself, ahead of the resend;
//!   * a TRANSACTION's batch is exempt: it is staged, not written, and its
//!     connection binds the transaction, which a close would orphan. What
//!     stops the batch behind a refused one there is the window: a
//!     transaction's window never starts past sequence 0 on a facade on its
//!     own (`transaction_starts_past_zero`).
//!
//! And a batch follows a batch in flight only on the connection that sent it
//! (`Producers::check_on`): after a reconnect, the old connection's push may
//! still be running, and nothing would order a push of the new one behind it.
//!
//! For a lost or out-of-date window both refusals are worse, and the first one
//! was what this facade answered until it stopped a fleet (stage, 2026-10-19: a rolling restart under
//! librdkafka 2.15.1 producers, the library under the Node, Python, Go, .NET,
//! Ruby and PHP clients of the matrix):
//!
//!   * OUT_OF_ORDER_SEQUENCE_NUMBER (45) on the batch at the head of the
//!     producer's queue is FATAL in librdkafka ("sequence desynchronization",
//!     `rd_kafka_handle_idempotent_Produce_error` in `rdkafka_request.c`), and
//!     after a lost window that batch is always the first one the facade sees.
//!     Every producer stopped for good within a second of the leader moving.
//!     Java answers the same code with a KIP-360 epoch bump, which is what the
//!     2.5.0 leader-kill runs exercised.
//!   * UNKNOWN_PRODUCER_ID (59) is recovered by librdkafka only for a batch it
//!     has not sent before, and a connection that drops with a request on the
//!     wire counts as a send (the retry count rises on a transport error) —
//!     which is what a stopping node does to every request in flight.
//!
//! The durable cure exists and is recorded rather than taken: `PushItem`'s
//! `transactionId` is a broker-side dedup key, and
//! `"qk:<pid>:<epoch>:<partition>:<seq>"` would move this whole window into
//! Queen, making a resend across a lost window the duplicate it is. It is not
//! done here because the guarantee would silently depend on a per-queue
//! `dedupWindowSeconds` a Kafka client cannot see, and because it puts a dedup
//! key on every record of every idempotent producer — a cost on the hot write
//! path that needs a measurement before it is a design.

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use kafka_protocol::error::ResponseError;
use kafka_protocol::records::BatchDecodeInfo;
/// Kafka's "this batch carries no producer id" (-1), and the value every
/// non-idempotent producer writes into the batch header. Re-exported from the
/// protocol crate rather than restated, so the two can never disagree.
pub use kafka_protocol::records::NO_PRODUCER_ID;

use crate::identity::TenantKey;
use crate::obs;

/// How many batches of one `(producer, topic-partition)` are remembered.
///
/// Five is not arbitrary: it is Kafka's own `ProducerStateEntry` depth, and it
/// is the ceiling `max.in.flight.requests.per.connection` is clamped to under
/// idempotence — so it is exactly deep enough to answer a retransmission of
/// anything that could still be in flight, and no deeper.
pub const WINDOW: usize = 5;

/// How many `(tenant, producer, topic-partition)` sequence windows this process
/// keeps when `QUEEN_KAFKA_MAX_PRODUCER_STATES` is not set.
///
/// Sized for the shapes this facade is run at, not for a hope: every Kafka
/// producer client keeps one sequence per partition it writes, so a node sees
/// about as many pairs as it leads partitions times the producers writing each
/// one. A 3-node cluster at 1M partitions with three producer fleets is ~1M
/// pairs per node steady and ~3M on a node that briefly leads everything; 4Mi
/// covers that with room. Past the cap the least recently WRITTEN entry is
/// evicted, and an evicted entry behaves exactly like one that was never there:
/// the producer's next batch starts a new window (the module header), and a
/// resend of a batch from before the eviction is written again rather than
/// answered as the duplicate it is. So the cap is set where a healthy cluster
/// never reaches it (the old 65,536 was reached by a 100k-partition benchmark
/// ~30,000 times a minute).
///
/// Memory is paid per entry that EXISTS, not per unit of cap: [`ENTRY_BYTES`].
pub const DEFAULT_MAX_TRACKED: usize = 4_194_304;

/// The smallest `QUEEN_KAFKA_MAX_PRODUCER_STATES` accepted: below it a handful
/// of producers writing a few hundred partitions evict each other.
pub const MIN_MAX_TRACKED: usize = 1_024;

/// The largest accepted: 64Mi entries is ~9 GB, past which the number is a typo.
pub const MAX_MAX_TRACKED: usize = 67_108_864;

/// What one tracked entry costs in resident memory, at most: its slab slot (112
/// bytes: the five-batch window, 16 bytes per batch, plus its key and its four
/// list links), its index entry (~26 bytes at the hash map's average load), and
/// the slack of both growing by doubling. Measured on a release build filling
/// a tracker the benchmark's way (150 producers, 5 batches each): 500k entries
/// = 95 MB, 1M = 200 MB, 2M = 352 MB, 4M = 580 MB — 150 to 210 bytes an entry,
/// so ~0.6 GB at the default cap if every entry is ever used.
pub const ENTRY_BYTES: usize = 210;

/// One line per window when the tracker is evicting, not one per produce: the
/// cap is reached by a fleet, so a line per eviction is a line per producer per
/// batch. See [`obs::Sampler`]. The total is [`Producers::evictions`].
static TRACK_CAP: obs::Sampler = obs::Sampler::new(60_000);

/// One line a minute for windows started mid-sequence ([`Producers::resumed`]):
/// after a restart every producer-partition of the fleet starts one at once.
static RESUMED: obs::Sampler = obs::Sampler::new(60_000);

/// One line a minute for held windows restarted on a gap
/// ([`Producers::resynced`]): a leader change moves a fleet at once.
static RESYNCED: obs::Sampler = obs::Sampler::new(60_000);

/// A `transactional.id` as the FACADE reads it, with Erlang's encoding defect
/// filtered out in one place so the two sites that care cannot drift.
///
/// `kafka_protocol 4.3.6` (brod, and with it `broadway_kafka` and `kaffe` —
/// most of the Elixir in production) hand-rolls its encoders instead of
/// deriving them from its own schema and types this field `string` where the
/// protocol says `nullable_string` (`kpro_req_lib.erl:308`, `kpro_lib.erl:140`
/// encoding null as `""`). So a plainly NON-transactional client puts a
/// zero-length string on the wire where the protocol says null. Apache Kafka
/// takes those same bytes without complaint. An EMPTY id is therefore not a
/// transactional id, and reading it as one costs brod every produce it makes
/// (measured: `compat/brod/README.md`).
///
/// Used by both [`crate::handlers::produce`] and
/// [`crate::handlers::init_producer_id`]: a producer that meets the transaction
/// refusal must meet ONE message about transactions, not two different ones.
pub fn transactional_id(id: Option<&str>) -> Option<&str> {
    id.filter(|id| !id.is_empty())
}

/// A fresh producer id: 62 bits of entropy, never 0 and never
/// [`NO_PRODUCER_ID`].
///
/// The same seed-plus-counter construction `coordinator::new_member_id` uses —
/// a `RandomState` seeded once from the OS, mixed with an atomic counter — and
/// RANDOM rather than a plain counter for one reason that matters the moment
/// there is more than one facade behind one advertised address: two facades
/// minting from counters would hand two different producers the same id, and a
/// producer whose connection later landed on the other facade would meet a
/// sequence window that is not its own. That is the one failure mode in this
/// design that could silently DROP records, and 62 bits of entropy per grant is
/// what makes it impossible rather than unlikely.
///
/// Kafka's own ids come from the controller in small contiguous blocks; clients
/// treat them as opaque and no client anywhere derives anything from the value.
pub fn new_producer_id() -> i64 {
    static SEED: OnceLock<RandomState> = OnceLock::new();
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut h = SEED.get_or_init(RandomState::new).build_hasher();
    h.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
    // 62 bits: non-negative with room to spare, so the value can never be read
    // as -1 by a client that widened it, and never collides with the sentinel.
    let id = (h.finish() & ((1u64 << 62) - 1)) as i64;
    // 0 is a legal producer id in Kafka and is minted here as 1 anyway: a zero
    // reads as "unset" to enough hand-rolled code that handing it out buys
    // nothing and costs a support thread.
    if id == 0 {
        1
    } else {
        id
    }
}

/// Kafka's own sequence arithmetic, wrap included.
///
/// Sequence numbers are `int32` and Kafka wraps them back through zero rather
/// than overflowing (`DefaultRecordBatch.incrementSequence`). A producer that
/// has written two billion records to one partition without restarting is rare
/// and is not a reason to answer it wrongly.
fn increment_sequence(sequence: i32, increment: i32) -> i32 {
    if sequence > i32::MAX - increment {
        increment - (i32::MAX - sequence) - 1
    } else {
        sequence + increment
    }
}

/// The last sequence a batch covers, from the header alone.
///
/// An EMPTY batch (`record_count == 0`) covers nothing, and its "last" is one
/// below its base — which is what makes the next batch's `base_sequence` still
/// contiguous with it.
fn last_sequence(info: &BatchDecodeInfo) -> i32 {
    match info.record_count {
        n if n > 0 => increment_sequence(info.base_sequence, n - 1),
        _ => info.base_sequence.wrapping_sub(1),
    }
}

/// One remembered append: the sequence range a batch covered and the absolute
/// offset its first record got.
///
/// The offset is the whole point. C1 (PLAN_QUEEN_KAFKA.md) has the push response
/// carry each message's absolute offset, so a duplicate can be answered as the
/// SUCCESS it was — with the offsets the original got — rather than as an error
/// a producer would have to interpret.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Batch {
    base_seq: i32,
    last_seq: i32,
    base_offset: i64,
}

/// The scope a producer's state is filed under.
///
/// The topic is part of the key and that is not an over-specification: a Kafka
/// producer keeps one sequence per `TopicPartition` and every one of them starts
/// at 0, so a producer writing to two topics has two sequence 0s in flight at
/// once. A key without the topic would read the second one as a duplicate of the
/// first and silently drop records — the precise failure this module exists to
/// prevent. Kafka's own `ProducerStateManager` is per partition LOG for the same
/// reason.
///
/// The tenant is the connection's [`TenantKey`], the same scope the coordinator
/// and the catalog use, so two tenants cannot reach each other's producer state
/// even in the (arithmetically impossible) event of an id collision.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Key {
    tenant: TenantKey,
    producer_id: i64,
    topic: String,
    partition: i32,
}

/// One batch of an accepted run, before the push has said where it landed.
///
/// `offset_delta` is how many records of the same run precede it, which is what
/// turns the ONE base offset a push answers per partition into a base offset per
/// batch. It is exact rather than an estimate: the produce path only commits a
/// run whose offsets came back contiguous (`handlers::produce::base_offset`), so
/// record `n` of the run is at `base_offset + n`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PendingBatch {
    base_seq: i32,
    last_seq: i32,
    records: i64,
    offset_delta: i64,
}

/// An accepted run of batches, and everything needed to remember it once the
/// push has assigned it a base offset.
///
/// Carried on the produce path from the check to the commit, which is why it is
/// public: the check happens before the records are staged and the commit after
/// the broker has answered, and nothing may be remembered in between.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pending {
    key: Key,
    epoch: i16,
    batches: Vec<PendingBatch>,
    /// The connection the run was accepted on ([`Producers::check_on`]).
    conn: u64,
    /// Whether the connection must close if this run is not written
    /// ([`Pending::closes_on_failure`]).
    close_on_failure: bool,
}

impl Pending {
    /// How many records this run declared. The produce path checks it against
    /// what the push actually appended before anything is remembered.
    pub fn records(&self) -> i64 {
        self.batches.iter().map(|b| b.records).sum()
    }

    /// The first sequence of the run. For log lines and tests.
    pub fn base_sequence(&self) -> i32 {
        self.batches.first().map_or(-1, |b| b.base_seq)
    }

    /// The last sequence of the run. For log lines and tests.
    pub fn last_sequence(&self) -> i32 {
        self.batches.last().map_or(-1, |b| b.last_seq)
    }

    /// Whether a failure to write this run ends the connection (the module
    /// header, "What keeps a gap that IS one"). Not on a facade on its own that
    /// held this producer's window when the run was accepted: there the batch
    /// the client sent behind it is a gap in that window, refused
    /// OUT_OF_ORDER_SEQUENCE_NUMBER — which every client retries behind the
    /// failed one — exactly as Kafka answers it, and the connection is spared a
    /// reconnect (and its SASL handshake) per throttled produce. In a cluster,
    /// or with no window to refuse the gap, the connection closes.
    pub fn closes_on_failure(&self) -> bool {
        self.close_on_failure
    }
}

/// What the window says about one `(topic, partition)` entry of a Produce.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// No batch here carries a producer id: this is an ordinary produce and
    /// nothing in this module applies to it.
    NotIdempotent,
    /// Stage and push. Commit the [`Pending`] when the push assigns a base
    /// offset, and only then ([`Producers::commit`]).
    Accept(Pending),
    /// Already appended. Answer `error_code = 0` with this base offset and write
    /// NOTHING — Kafka's own duplicate semantics.
    Duplicate(i64),
    /// Refuse this partition with this code and write nothing.
    Reject(ResponseError, String),
}

/// Every idempotent producer this process is currently tracking.
///
/// One per process, shared by every connection through [`crate::Facade`] — the
/// tenant is in the key, not in the container, because a producer id is granted
/// per connection and used on that one connection, while the state has to
/// survive the `Facade` clones that `for_connection` and `authenticated_as`
/// make.
///
/// ## Shaped for millions of entries
///
/// The map is SHARDED by the hash of the full key, each shard behind its own
/// mutex, so two produces to different partitions almost never meet on a lock
/// — the old single mutex was taken by every produce entry of every connection
/// of the node. Each shard keeps its entries in a slab with two intrusive
/// doubly-linked lists through it, and every operation is O(1) in the size of
/// the table:
///
///   * the LRU, most recently written first: a write moves its entry to the
///     front, and EVICTION unlinks the tail — no scan. (The old eviction was a
///     `min_by_key` over the whole table under the one lock, once per new entry
///     past the cap: at the cap every new producer-partition paid a full scan
///     of 65,536 entries while every produce of the node waited.)
///   * each producer's entries, so [`Producers::forget`] visits the entries it
///     drops and nothing else. (The old `forget` was a `retain` over the whole
///     table — and it runs on every KIP-360 epoch bump, i.e. once per eviction
///     victim that comes back, which made the two costs compound.)
///
/// The cap is GLOBAL and exact: nothing is evicted while fewer than the cap are
/// tracked, whichever shard is full. At the cap, the shard receiving the new
/// entry gives up its own least recently written one. Keys are spread by hash,
/// so every shard holds the same mix of cold and hot entries and the victim is
/// the coldest of a uniform 1/N sample — the global LRU order, approximately.
/// Small caps use fewer shards (one per [`MIN_PER_SHARD`] entries), so a small
/// tracker is an exact LRU.
///
/// A key's producer and topic are interned per shard, so a slot stores two
/// `u32`s and a partition rather than a tenant, an id and a `String`.
pub struct Producers {
    shards: Box<[Mutex<Shard>]>,
    hasher: RandomState,
    cap: usize,
    /// Entries tracked across every shard.
    len: AtomicUsize,
    /// Entries evicted for room since the process started.
    evictions: AtomicU64,
    /// Windows started at a sequence other than 0 ([`Producers::resumed`]).
    resumed: AtomicU64,
    /// Held windows restarted on a gap ([`Producers::resynced`]).
    resynced: AtomicU64,
    /// Whether a gap in a held window restarts it rather than being refused
    /// ([`Producers::restart_windows_on_gaps`]): only a CLUSTER of facades can
    /// hold a window another one has moved past.
    gaps_restart: std::sync::atomic::AtomicBool,
    /// Batches accepted and not yet answered, per key, in the order they were
    /// accepted ([`Producers::begin`]), each with its connection: what a
    /// producer's next batch on that connection follows while the ones before
    /// it are still being written — coalesced into one push
    /// (`handlers::produce::handle_coalesced`) or pipelined behind it
    /// (`conn::connection`). Only batches in flight are here, so it is as
    /// small as a connection's pipeline is deep.
    inflight: Mutex<HashMap<Key, Vec<InFlight>>>,
}

/// How a batch relates to its key's batches in flight ([`Producers::begin`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InFlightFit {
    /// The next batch after the newest in flight: accepted, coalesced or
    /// pipelined behind it.
    Follows,
    /// At or past the next batch after the newest in flight, which is in
    /// flight on ANOTHER connection: nothing orders this push behind that one,
    /// and that connection's evidence is no gap's ([`fit`]).
    Elsewhere,
    /// A batch still in flight, sent again.
    Repeats,
    /// Past the newest in flight, which ends before this sequence.
    Gap(i32),
}

/// One batch accepted and not yet answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct InFlight {
    epoch: i16,
    base_seq: i32,
    last_seq: i32,
    conn: u64,
    /// When it was begun: an entry older than [`INFLIGHT_STALE`] is one that
    /// leaked ([`Producers::inflight_view`]).
    since: std::time::Instant,
}

/// How long a batch can be in flight. Far past any push: one has the facade's
/// 10-second budget for a call to Queen, and a pipelined one waits for at most
/// [`crate::conn::pipeline_depth`] before it. An entry this old was never ended
/// (a bug: a panic between `begin` and `end`), and left there it would refuse
/// its producer's batches for ever (Repeats, Elsewhere); it is dropped instead.
const INFLIGHT_STALE: std::time::Duration = std::time::Duration::from_secs(180);

/// One line a minute for dropped stale in-flight entries.
static STALE_INFLIGHT: obs::Sampler = obs::Sampler::new(60_000);

/// At most this many shards: past it the contention is already gone and the
/// per-shard LRU sample only gets smaller.
const MAX_SHARDS: usize = 64;

/// A shard is only added per this many entries of cap, so a shard's LRU is a
/// sample worth evicting from.
const MIN_PER_SHARD: usize = 4_096;

/// The "no slot" link.
const NIL: u32 = u32::MAX;

/// One tracked window, in its shard's slab.
struct Slot {
    /// The last [`WINDOW`] batches, oldest first; `len` of them are real.
    recent: [Batch; WINDOW],
    key: SlotKey,
    /// The shard's LRU, most recently written at the head. `next` is also the
    /// free list's link while the slot is free.
    prev: u32,
    next: u32,
    /// The other slots of this slot's producer in the shard.
    oprev: u32,
    onext: u32,
    /// The highest epoch seen. A batch below it is fenced; a batch above it is
    /// a reset ([`Producers::check`]).
    epoch: i16,
    len: u8,
}

impl Slot {
    fn new(key: SlotKey, epoch: i16) -> Slot {
        Slot {
            recent: [Batch {
                base_seq: 0,
                last_seq: 0,
                base_offset: 0,
            }; WINDOW],
            key,
            prev: NIL,
            next: NIL,
            oprev: NIL,
            onext: NIL,
            epoch,
            len: 0,
        }
    }

    /// Remember one more batch, dropping the oldest once [`WINDOW`] are kept.
    fn push(&mut self, batch: Batch) {
        let len = usize::from(self.len);
        if len == WINDOW {
            self.recent.copy_within(1.., 0);
            self.recent[WINDOW - 1] = batch;
        } else {
            self.recent[len] = batch;
            self.len += 1;
        }
    }

    /// Remember one more batch IN SEQUENCE ORDER, so the newest stays last.
    ///
    /// Nearly always that is [`Slot::push`]: a producer's batches are written
    /// in order. A clustered facade also writes a batch from below its window
    /// (`decide`); appended last, it would become the "newest", the producer's
    /// next sequence would be read from it, and a resend of a batch already
    /// written after it would be taken as the next one and written again. So it
    /// goes where its sequence puts it, and when the window is full and it is
    /// older than every batch kept, it is not kept.
    fn insert(&mut self, batch: Batch) {
        let len = usize::from(self.len);
        let at = self.recent[..len]
            .iter()
            .rposition(|b| !is_before(batch.base_seq, b.base_seq))
            .map_or(0, |i| i + 1);
        if at == len {
            self.push(batch);
            return;
        }
        if len == WINDOW {
            if at == 0 {
                return;
            }
            // Drop the oldest, then slide the ones before the new place down.
            self.recent.copy_within(1..at, 0);
            self.recent[at - 1] = batch;
            return;
        }
        self.recent.copy_within(at..len, at + 1);
        self.recent[at] = batch;
        self.len += 1;
    }
}

/// A slot's key inside its shard: the interned producer and topic, and the
/// partition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct SlotKey {
    owner: u32,
    topic: u32,
    partition: i32,
}

/// One `(tenant, producer id)` of a shard, and the head of its slot list.
struct Owner {
    tenant: TenantKey,
    producer_id: i64,
    head: u32,
    count: u32,
}

/// One topic name of a shard, and how many slots name it.
struct Topic {
    name: Arc<str>,
    count: u32,
}

/// One shard: a slab of slots, the index into it, and the interned producers
/// and topics its keys are made of.
struct Shard {
    index: HashMap<SlotKey, u32>,
    slots: Vec<Slot>,
    /// The first free slot, or [`NIL`].
    free: u32,
    /// The LRU's ends: most recently written, and the eviction victim.
    head: u32,
    tail: u32,
    /// Producer id → its owner records (one per tenant using that id, which is
    /// one: an id is 62 random bits).
    owners: HashMap<i64, Vec<u32>>,
    owner_recs: Vec<Option<Owner>>,
    owner_free: Vec<u32>,
    topics: HashMap<Arc<str>, u32>,
    topic_recs: Vec<Option<Topic>>,
    topic_free: Vec<u32>,
}

impl Shard {
    fn new() -> Shard {
        Shard {
            index: HashMap::new(),
            slots: Vec::new(),
            free: NIL,
            head: NIL,
            tail: NIL,
            owners: HashMap::new(),
            owner_recs: Vec::new(),
            owner_free: Vec::new(),
            topics: HashMap::new(),
            topic_recs: Vec::new(),
            topic_free: Vec::new(),
        }
    }

    fn owner_of(&self, tenant: &TenantKey, producer_id: i64) -> Option<u32> {
        self.owners.get(&producer_id)?.iter().copied().find(|o| {
            self.owner_recs[*o as usize]
                .as_ref()
                .is_some_and(|r| &r.tenant == tenant)
        })
    }

    fn find(
        &self,
        tenant: &TenantKey,
        producer_id: i64,
        topic: &str,
        partition: i32,
    ) -> Option<u32> {
        let owner = self.owner_of(tenant, producer_id)?;
        let topic = *self.topics.get(topic)?;
        self.index
            .get(&SlotKey {
                owner,
                topic,
                partition,
            })
            .copied()
    }

    fn owner_or_insert(&mut self, tenant: &TenantKey, producer_id: i64) -> u32 {
        if let Some(o) = self.owner_of(tenant, producer_id) {
            return o;
        }
        let rec = Owner {
            tenant: tenant.clone(),
            producer_id,
            head: NIL,
            count: 0,
        };
        let o = match self.owner_free.pop() {
            Some(o) => {
                self.owner_recs[o as usize] = Some(rec);
                o
            }
            None => {
                self.owner_recs.push(Some(rec));
                (self.owner_recs.len() - 1) as u32
            }
        };
        self.owners.entry(producer_id).or_default().push(o);
        o
    }

    fn topic_or_insert(&mut self, name: &str) -> u32 {
        if let Some(t) = self.topics.get(name) {
            return *t;
        }
        let name: Arc<str> = Arc::from(name);
        let rec = Topic {
            name: Arc::clone(&name),
            count: 0,
        };
        let t = match self.topic_free.pop() {
            Some(t) => {
                self.topic_recs[t as usize] = Some(rec);
                t
            }
            None => {
                self.topic_recs.push(Some(rec));
                (self.topic_recs.len() - 1) as u32
            }
        };
        self.topics.insert(name, t);
        t
    }

    /// Add a slot for `key`, linked at the head of the LRU and of its
    /// producer's list.
    fn insert(&mut self, slot: Slot) -> u32 {
        let key = slot.key;
        let i = if self.free != NIL {
            let i = self.free;
            self.free = self.slots[i as usize].next;
            self.slots[i as usize] = slot;
            i
        } else {
            self.slots.push(slot);
            (self.slots.len() - 1) as u32
        };
        self.index.insert(key, i);
        self.lru_push_front(i);
        let owner = self.owner_recs[key.owner as usize]
            .as_mut()
            .expect("a slot's owner record is live while the slot is");
        let head = owner.head;
        owner.head = i;
        owner.count += 1;
        {
            let s = &mut self.slots[i as usize];
            s.oprev = NIL;
            s.onext = head;
        }
        if head != NIL {
            self.slots[head as usize].oprev = i;
        }
        self.topic_recs[key.topic as usize]
            .as_mut()
            .expect("a slot's topic record is live while the slot is")
            .count += 1;
        i
    }

    fn lru_unlink(&mut self, i: u32) {
        let (prev, next) = {
            let s = &self.slots[i as usize];
            (s.prev, s.next)
        };
        if prev != NIL {
            self.slots[prev as usize].next = next;
        } else {
            self.head = next;
        }
        if next != NIL {
            self.slots[next as usize].prev = prev;
        } else {
            self.tail = prev;
        }
    }

    fn lru_push_front(&mut self, i: u32) {
        let head = self.head;
        {
            let s = &mut self.slots[i as usize];
            s.prev = NIL;
            s.next = head;
        }
        if head != NIL {
            self.slots[head as usize].prev = i;
        } else {
            self.tail = i;
        }
        self.head = i;
    }

    /// Move a slot that was just written to the front of the LRU.
    fn touch(&mut self, i: u32) {
        if self.head != i {
            self.lru_unlink(i);
            self.lru_push_front(i);
        }
    }

    /// Remove one slot: from the LRU, the index, its producer's list and its
    /// topic's count, releasing the producer and topic records it was the last
    /// user of. O(1).
    fn remove(&mut self, i: u32) {
        self.lru_unlink(i);
        let (key, oprev, onext) = {
            let s = &self.slots[i as usize];
            (s.key, s.oprev, s.onext)
        };
        self.index.remove(&key);
        if oprev != NIL {
            self.slots[oprev as usize].onext = onext;
        }
        if onext != NIL {
            self.slots[onext as usize].oprev = oprev;
        }
        let owner = self.owner_recs[key.owner as usize]
            .as_mut()
            .expect("a slot's owner record is live while the slot is");
        if oprev == NIL {
            owner.head = onext;
        }
        owner.count -= 1;
        if owner.count == 0 {
            self.release_owner(key.owner);
        }
        self.release_topic_ref(key.topic);
        self.slots[i as usize].next = self.free;
        self.free = i;
    }

    fn release_owner(&mut self, o: u32) {
        let Some(rec) = self.owner_recs[o as usize].take() else {
            return;
        };
        if let Some(ids) = self.owners.get_mut(&rec.producer_id) {
            ids.retain(|x| *x != o);
            if ids.is_empty() {
                self.owners.remove(&rec.producer_id);
            }
        }
        self.owner_free.push(o);
    }

    fn release_topic_ref(&mut self, t: u32) {
        let rec = self.topic_recs[t as usize]
            .as_mut()
            .expect("a slot's topic record is live while the slot is");
        rec.count -= 1;
        if rec.count == 0 {
            let name = Arc::clone(&rec.name);
            self.topic_recs[t as usize] = None;
            self.topics.remove(&name);
            self.topic_free.push(t);
        }
    }

    /// Drop every slot of one producer, walking its own list. How many went.
    fn forget(&mut self, tenant: &TenantKey, producer_id: i64) -> usize {
        let Some(o) = self.owner_of(tenant, producer_id) else {
            return 0;
        };
        let mut removed = 0;
        loop {
            let head = match self.owner_recs[o as usize].as_ref() {
                Some(rec) if rec.head != NIL => rec.head,
                _ => break,
            };
            self.remove(head);
            removed += 1;
        }
        removed
    }
}

impl Default for Producers {
    fn default() -> Producers {
        Producers::new()
    }
}

impl Producers {
    /// A tracker at [`DEFAULT_MAX_TRACKED`].
    pub fn new() -> Producers {
        Producers::with_capacity(DEFAULT_MAX_TRACKED)
    }

    /// A tracker that keeps at most `cap` entries
    /// (`QUEEN_KAFKA_MAX_PRODUCER_STATES`). Nothing is allocated up front:
    /// memory grows with the entries that exist, [`ENTRY_BYTES`] each.
    pub fn with_capacity(cap: usize) -> Producers {
        let cap = cap.max(1);
        let shards = (cap / MIN_PER_SHARD)
            .next_power_of_two()
            .clamp(1, MAX_SHARDS);
        Producers {
            shards: (0..shards).map(|_| Mutex::new(Shard::new())).collect(),
            hasher: RandomState::new(),
            cap,
            len: AtomicUsize::new(0),
            evictions: AtomicU64::new(0),
            resumed: AtomicU64::new(0),
            resynced: AtomicU64::new(0),
            gaps_restart: std::sync::atomic::AtomicBool::new(false),
            inflight: Mutex::new(HashMap::new()),
        }
    }

    /// The batches of an accepted run are IN FLIGHT: staged for a push that has
    /// not answered yet. The same producer's next batch — in the same coalesced
    /// push — is judged against them ([`Producers::check`]) until
    /// [`Producers::end`].
    pub fn begin(&self, pending: &Pending) {
        let mut inflight = self
            .inflight
            .lock()
            .expect("the in-flight map lock is never held across a panic");
        let entry = inflight.entry(pending.key.clone()).or_default();
        let since = std::time::Instant::now();
        entry.extend(pending.batches.iter().map(|b| InFlight {
            epoch: pending.epoch,
            base_seq: b.base_seq,
            last_seq: b.last_seq,
            conn: pending.conn,
            since,
        }));
    }

    /// The push of an accepted run answered, whatever it answered: its batches
    /// are no longer in flight. Called AFTER [`Producers::commit`] when the
    /// run landed, so the window never misses them in between.
    pub fn end(&self, pending: &Pending) {
        let mut inflight = self
            .inflight
            .lock()
            .expect("the in-flight map lock is never held across a panic");
        if let Some(entry) = inflight.get_mut(&pending.key) {
            entry.retain(|f| {
                !(f.epoch == pending.epoch
                    && pending.batches.iter().any(|b| b.base_seq == f.base_seq))
            });
            if entry.is_empty() {
                inflight.remove(&pending.key);
            }
        }
    }

    /// What the in-flight list holds for a key at one epoch, read under its
    /// lock. Read BEFORE the committed window ([`Producers::check_on`]): a run
    /// is committed before it leaves the list ([`Producers::end`]), so a batch
    /// missing from this view was already in the window when that is read.
    fn inflight_view(&self, key: &Key, epoch: i16, base_seq: i32) -> Option<InFlightView> {
        let mut inflight = self
            .inflight
            .lock()
            .expect("the in-flight map lock is never held across a panic");
        let entry = inflight.get_mut(key)?;
        let before = entry.len();
        entry.retain(|f| f.since.elapsed() < INFLIGHT_STALE);
        if entry.len() < before {
            if let Some(suppressed) = STALE_INFLIGHT.tick_now() {
                tracing::error!(
                    target: "kafka",
                    producer_id = key.producer_id,
                    topic = %key.topic,
                    partition = key.partition,
                    dropped = before - entry.len(),
                    suppressed,
                    "batches in flight for longer than any push can take were never ended; \
                     they are dropped so their producer is not refused for ever"
                );
            }
            if entry.is_empty() {
                inflight.remove(key);
                return None;
            }
        }
        let entry = inflight.get(key)?;
        let mut newest: Option<InFlight> = None;
        let mut repeats = false;
        for f in entry.iter().filter(|f| f.epoch == epoch) {
            repeats |= f.base_seq == base_seq;
            // The batch furthest AHEAD, not the one begun last.
            newest = match newest {
                Some(n) if !is_before(n.last_seq, f.last_seq) => Some(n),
                _ => Some(*f),
            };
        }
        Some(InFlightView {
            newest: newest?,
            repeats,
        })
    }

    /// The shard a key lives in. The same tuple TYPE is hashed on every path,
    /// or a key would be looked for in a shard it is not in.
    fn shard(
        &self,
        tenant: &TenantKey,
        producer_id: i64,
        topic: &str,
        partition: i32,
    ) -> MutexGuard<'_, Shard> {
        let h = self
            .hasher
            .hash_one((tenant, producer_id, topic, partition));
        self.shards[(h as usize) & (self.shards.len() - 1)]
            .lock()
            .expect("the producer map lock is never held across a panic")
    }

    /// How many entries are tracked right now.
    pub fn tracked(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// The most entries this tracker keeps.
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// How many entries have been evicted to make room since the process
    /// started. Each one is a producer whose next batch starts a new window, so
    /// a resend from before the eviction would be written twice; on a healthy
    /// node this stays at 0.
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    /// How many windows have been started at a sequence other than 0 since the
    /// process started: producers this facade held no state for that were
    /// already mid-sequence — after a restart, a leader change or an eviction.
    /// Each one may have had a batch in flight that is written twice (the
    /// module header). It rises once per producer-partition on a restart and
    /// stays flat otherwise.
    pub fn resumed(&self) -> u64 {
        self.resumed.load(Ordering::Relaxed)
    }

    /// Restart a held window on a gap instead of refusing the batch: what a
    /// CLUSTERED facade does, because there a producer can write through
    /// another facade and leave this one's window behind (the module header).
    /// A facade on its own keeps Apache Kafka's answer, OUT_OF_ORDER_SEQUENCE_NUMBER:
    /// nothing else writes its partitions, so a gap in a window it holds is
    /// a real gap. Set once, at boot.
    pub fn restart_windows_on_gaps(&self, on: bool) {
        self.gaps_restart.store(on, Ordering::Relaxed);
    }

    /// [`Producers::restart_windows_on_gaps`].
    pub fn restarts_windows_on_gaps(&self) -> bool {
        self.gaps_restart.load(Ordering::Relaxed)
    }

    /// How many held windows have been restarted on a gap since the process
    /// started: windows this facade fell behind on while the producer wrote
    /// through another facade (the module header). Like a resumed window, each
    /// may have had a batch in flight that is written twice. It rises when
    /// partitions move between facades and stays flat otherwise.
    pub fn resynced(&self) -> u64 {
        self.resynced.load(Ordering::Relaxed)
    }

    /// Forget everything about one producer id under one tenant.
    ///
    /// Called by [`crate::handlers::init_producer_id`] on KIP-360's epoch bump:
    /// a producer that asked for a new epoch has reset its own sequences to 0,
    /// so keeping the old ranges could only produce a wrong answer. The produce
    /// path resets on a higher epoch too ([`Producers::check`]) — this is the
    /// cheaper half of the same rule, and it returns the memory rather than
    /// waiting for the LRU to.
    ///
    /// One visit per shard plus one per entry of THIS producer: the entries of
    /// every other producer are never looked at.
    pub fn forget(&self, tenant: &TenantKey, producer_id: i64) {
        for shard in self.shards.iter() {
            let removed = shard
                .lock()
                .expect("the producer map lock is never held across a panic")
                .forget(tenant, producer_id);
            if removed > 0 {
                self.len.fetch_sub(removed, Ordering::Relaxed);
            }
        }
    }

    /// [`Producers::check_on`] outside any connection (connection 0).
    pub fn check(
        &self,
        tenant: &TenantKey,
        topic: &str,
        partition: i32,
        infos: &[BatchDecodeInfo],
    ) -> Verdict {
        self.check_on(tenant, topic, partition, infos, 0)
    }

    /// What to do with one `(topic, partition)` entry's batches, decided on the
    /// batch HEADERS — the place [`crate::handlers::produce::refuse`] already
    /// stands, so a batch this refuses is never decompressed.
    ///
    /// `conn` is the connection it arrived on ([`ResendInOrder::connection`]):
    /// a batch follows a batch in flight only on the same connection, which is
    /// what orders its push behind that one. The next batch after a batch in
    /// flight on ANOTHER connection — a client that reconnected while its old
    /// connection's push is still running — is refused retriably: its resend
    /// comes after that push has answered.
    pub fn check_on(
        &self,
        tenant: &TenantKey,
        topic: &str,
        partition: i32,
        infos: &[BatchDecodeInfo],
        conn: u64,
    ) -> Verdict {
        let idempotent = infos
            .iter()
            .filter(|i| i.producer_id != NO_PRODUCER_ID)
            .count();
        if idempotent == 0 {
            return Verdict::NotIdempotent;
        }
        if idempotent != infos.len() {
            // No client does this, and the reason to refuse rather than to
            // handle half of it is that the answer is ONE error code and ONE
            // base offset per partition: a run that is half remembered and half
            // new cannot be described by either.
            return Verdict::Reject(
                ResponseError::InvalidRecord,
                "one produce entry mixes idempotent and non-idempotent record batches, which \
                 cannot be answered with a single base offset"
                    .to_string(),
            );
        }
        let first = &infos[0];
        if infos
            .iter()
            .any(|i| i.producer_id != first.producer_id || i.producer_epoch != first.producer_epoch)
        {
            return Verdict::Reject(
                ResponseError::InvalidRecord,
                "one produce entry carries record batches from more than one producer session"
                    .to_string(),
            );
        }
        // Contiguous WITHIN the request, before anything is compared to what is
        // remembered: a producer that sent 0-4 and 6-9 in one frame is already
        // out of order and the window it is checked against does not matter.
        for pair in infos.windows(2) {
            let expected = increment_sequence(last_sequence(&pair[0]), 1);
            if pair[1].base_sequence != expected {
                return Verdict::Reject(
                    ResponseError::OutOfOrderSequenceNumber,
                    format!(
                        "the batches of this request are not contiguous: sequence {} follows a \
                         batch ending at {}",
                        pair[1].base_sequence,
                        last_sequence(&pair[0])
                    ),
                );
            }
        }

        let key = Key {
            tenant: tenant.clone(),
            producer_id: first.producer_id,
            topic: topic.to_string(),
            partition,
        };
        let epoch = first.producer_epoch;
        let mut offset_delta = 0i64;
        let batches: Vec<PendingBatch> = infos
            .iter()
            .map(|i| {
                let records = i64::from(i.record_count.max(0));
                let batch = PendingBatch {
                    base_seq: i.base_sequence,
                    last_seq: last_sequence(i),
                    records,
                    offset_delta,
                };
                offset_delta += records;
                batch
            })
            .collect();
        // The in-flight list FIRST, then the committed window, copied out so
        // its lock is not held with the other ([`Producers::inflight_view`]).
        let view = self.inflight_view(&key, epoch, first.base_sequence);
        let window: Option<(i16, [Batch; WINDOW], u8)> = {
            let shard = self.shard(tenant, first.producer_id, topic, partition);
            shard
                .find(tenant, first.producer_id, topic, partition)
                .map(|i| {
                    let s = &shard.slots[i as usize];
                    (s.epoch, s.recent, s.len)
                })
        };
        let entry = window
            .as_ref()
            .map(|(epoch, recent, len)| (*epoch, &recent[..usize::from(*len)]));
        let window_next = entry
            .filter(|(entry_epoch, _)| *entry_epoch == epoch)
            .and_then(|(_, recent)| recent.last())
            .map(|b| increment_sequence(b.last_seq, 1));
        // The producer's previous batches are accepted and their push not yet
        // answered (a coalesced run), so the committed window does not hold
        // them yet and would read this one as a gap.
        let follows = view.and_then(|v| fit(&v, first.base_sequence, window_next, conn));
        let gaps_restart = self.restarts_windows_on_gaps();
        // A window that will refuse the gap a failure of this run leaves:
        // held at this epoch, with batches, on a facade that refuses gaps.
        let window_refuses_gaps = !gaps_restart
            && entry
                .is_some_and(|(entry_epoch, recent)| entry_epoch == epoch && !recent.is_empty());
        let accept = Verdict::Accept(Pending {
            key,
            epoch,
            batches,
            conn,
            close_on_failure: !window_refuses_gaps,
        });
        match follows {
            Some(InFlightFit::Follows) => return accept,
            Some(InFlightFit::Elsewhere) => {
                return Verdict::Reject(
                    ResponseError::RequestTimedOut,
                    "a batch of this producer is still in flight on another connection; this \
                     one is answered once that push is"
                        .to_string(),
                )
            }
            Some(InFlightFit::Repeats) => {
                return Verdict::Reject(
                    ResponseError::RequestTimedOut,
                    "a retransmission of a batch whose push has not answered yet; it is \
                     answered once that push is"
                        .to_string(),
                )
            }
            Some(InFlightFit::Gap(next)) => {
                return Verdict::Reject(
                    ResponseError::OutOfOrderSequenceNumber,
                    format!(
                        "sequence {} for producer {} on {topic}-{partition} would leave a gap \
                         behind the batches in flight here, which end before {next}",
                        first.base_sequence, first.producer_id
                    ),
                )
            }
            None => {}
        }

        if entry.is_none() && first.base_sequence != 0 {
            // A producer already mid-sequence that this facade holds nothing
            // for: its window starts here (`decide`). Counted, and said once a
            // minute rather than per partition — a restart says it for every
            // producer-partition of the fleet at once.
            let total = self.resumed.fetch_add(1, Ordering::Relaxed) + 1;
            if let Some(suppressed) = RESUMED.tick_now() {
                tracing::info!(
                    target: "kafka",
                    producer_id = first.producer_id,
                    topic,
                    partition,
                    base_sequence = first.base_sequence,
                    resumed_total = total,
                    suppressed,
                    "an idempotent producer this facade holds no sequence state for is already \
                     mid-sequence (a restart, a leader change, an eviction); its window starts \
                     at this batch, and a batch it had in flight may be written twice"
                );
            }
        }
        if let Some((entry_epoch, recent)) = entry {
            if gaps_restart && is_gap(entry_epoch, recent, infos) {
                // A window another facade moved past (the module header):
                // restarted at this batch by `decide`, counted and said once
                // a minute, as a resumed one is.
                let total = self.resynced.fetch_add(1, Ordering::Relaxed) + 1;
                if let Some(suppressed) = RESYNCED.tick_now() {
                    tracing::info!(
                        target: "kafka",
                        producer_id = first.producer_id,
                        topic,
                        partition,
                        base_sequence = first.base_sequence,
                        expected = recent.last().map(|b| increment_sequence(b.last_seq, 1)),
                        resynced_total = total,
                        suppressed,
                        "an idempotent producer is past the sequence window this facade holds \
                         (it wrote through another facade since: a leader change, a relay \
                         that came up); the window restarts at this batch, and a batch it had \
                         in flight may be written twice"
                    );
                }
            }
        }
        decide(entry, infos, topic, partition, accept, gaps_restart)
    }

    /// Remember an accepted run, now that the push has told us where it landed.
    ///
    /// Called ONLY when the whole run came back with contiguous offsets — the
    /// check `handlers::produce::base_offset` already performs. A partially
    /// failed push leaves the entry untouched, so the client's retry of the same
    /// sequence is accepted as new work rather than skipped as a duplicate.
    ///
    /// `appended` is how many records the push actually took. When it disagrees
    /// with what the batch headers declared, NOTHING is remembered: an offset
    /// per batch can only be derived from a run whose length is the one the
    /// headers described, and a guessed offset is a producer told its records
    /// are at a place they are not. The cost of remembering nothing is a retry
    /// treated as new work — at-least-once, the safe direction — and it is loud.
    pub fn commit(&self, pending: &Pending, base_offset: i64, appended: usize) {
        let declared = pending.records();
        if declared != appended as i64 {
            tracing::error!(
                target: "kafka",
                producer_id = pending.key.producer_id,
                topic = %pending.key.topic,
                partition = pending.key.partition,
                declared,
                appended,
                "the batch headers and the push disagree about how many records this run had; \
                 the sequence window is not being advanced for it"
            );
            return;
        }
        let key = &pending.key;
        // One remembered range per BATCH and not one per run, because a producer
        // that retries sends the batches it sent — Kafka's own
        // `ProducerStateEntry` is per batch for the same reason.
        let batches = pending.batches.iter().map(|b| Batch {
            base_seq: b.base_seq,
            last_seq: b.last_seq,
            base_offset: base_offset + b.offset_delta,
        });
        let mut shard = self.shard(&key.tenant, key.producer_id, &key.topic, key.partition);
        if let Some(i) = shard.find(&key.tenant, key.producer_id, &key.topic, key.partition) {
            let slot = &mut shard.slots[i as usize];
            if pending.epoch < slot.epoch {
                // A run of a session the producer has since bumped past, whose
                // push answered late. Remembered in the new epoch's window it
                // would move that window's next sequence, and the new session's
                // own batches would be answered as duplicates of it.
                return;
            }
            if pending.epoch > slot.epoch {
                slot.epoch = pending.epoch;
                slot.len = 0;
            }
            for batch in batches {
                slot.insert(batch);
            }
            shard.touch(i);
            return;
        }
        // A new entry. Room is made FIRST, and only when the whole tracker is
        // at its cap: nothing is evicted below it, whichever shard this is. The
        // victim is this shard's least recently written entry — O(1), and the
        // coldest of a uniform sample (the type header). Made before the new
        // key's producer and topic are interned, because the victim may be the
        // last user of either.
        while self.len.load(Ordering::Relaxed) >= self.cap && shard.tail != NIL {
            let victim = shard.tail;
            shard.remove(victim);
            self.len.fetch_sub(1, Ordering::Relaxed);
            let total = self.evictions.fetch_add(1, Ordering::Relaxed) + 1;
            if let Some(suppressed) = TRACK_CAP.tick_now() {
                tracing::warn!(
                    target: "kafka",
                    tracked = self.cap,
                    evicted_total = total,
                    suppressed,
                    "more idempotent producers than this facade keeps sequence state for \
                     (QUEEN_KAFKA_MAX_PRODUCER_STATES); the least recently written are being \
                     dropped, their next batch starts a new sequence window, and a resend of a \
                     batch from before the drop would be written twice"
                );
            }
        }
        let owner = shard.owner_or_insert(&key.tenant, key.producer_id);
        let topic = shard.topic_or_insert(&key.topic);
        let mut slot = Slot::new(
            SlotKey {
                owner,
                topic,
                partition: key.partition,
            },
            pending.epoch,
        );
        for batch in batches {
            slot.push(batch);
        }
        shard.insert(slot);
        self.len.fetch_add(1, Ordering::Relaxed);
    }
}

/// One connection's "close after this answer" (the module header): set when an
/// idempotent batch of the request being answered was not written for any
/// reason but being a duplicate, so that nothing the client sent behind it on
/// this connection is read, and the client sends again, in order.
///
/// Per connection, a fresh one in `Facade::for_connection`; taken by the
/// connection once the answers are written.
///
/// It also names the connection ([`ResendInOrder::connection`]): the batches a
/// connection has in flight are its own ([`Producers::check_on`]).
#[derive(Debug)]
pub struct ResendInOrder {
    required: std::sync::atomic::AtomicBool,
    connection: u64,
}

impl Default for ResendInOrder {
    fn default() -> ResendInOrder {
        ResendInOrder::new()
    }
}

impl ResendInOrder {
    pub fn new() -> ResendInOrder {
        // 0 is the connection of a check made outside one ([`Producers::check`]).
        static NEXT: AtomicU64 = AtomicU64::new(1);
        ResendInOrder {
            required: std::sync::atomic::AtomicBool::new(false),
            connection: NEXT.fetch_add(1, Ordering::Relaxed),
        }
    }

    /// This connection's id, unique in the process.
    pub fn connection(&self) -> u64 {
        self.connection
    }

    /// The answer being built leaves an idempotent batch unwritten.
    pub fn require(&self) {
        self.required.store(true, Ordering::Relaxed);
    }

    /// Whether the connection must close after the answers it just wrote; it
    /// is cleared by asking.
    pub fn take(&self) -> bool {
        self.required.swap(false, Ordering::Relaxed)
    }

    /// Whether it is required, without clearing it.
    pub fn is_required(&self) -> bool {
        self.required.load(Ordering::Relaxed)
    }
}

/// What the in-flight list holds for one key at one epoch
/// ([`Producers::inflight_view`]).
#[derive(Clone, Copy, Debug)]
struct InFlightView {
    /// The batch furthest ahead.
    newest: InFlight,
    /// Whether the batch being checked starts where one in flight starts.
    repeats: bool,
}

/// How a batch starting at `base_seq`, arriving on connection `conn`, relates
/// to the batches in flight, or `None` when the committed window decides.
///
/// `window_next` is the next sequence of the committed window at this epoch.
/// A newest batch in flight that ends BEHIND it is one a clustered facade
/// accepted from below its window (`decide`): it is not where the producer
/// is, so nothing follows it or is a gap behind it. A newest batch in flight
/// on ANOTHER connection orders nothing on this one: a batch at or past its
/// next sequence waits for it ([`InFlightFit::Elsewhere`], retriable) and is
/// never refused as a gap on its evidence — that connection may be one the
/// client already left (a relay drained at a restart, a push still running),
/// and the producer may have moved on through another facade since.
fn fit(
    view: &InFlightView,
    base_seq: i32,
    window_next: Option<i32>,
    conn: u64,
) -> Option<InFlightFit> {
    if view.repeats {
        return Some(InFlightFit::Repeats);
    }
    let next = increment_sequence(view.newest.last_seq, 1);
    if window_next.is_some_and(|w| is_before(next, w)) || is_before(base_seq, next) {
        return None;
    }
    if view.newest.conn != conn {
        return Some(InFlightFit::Elsewhere);
    }
    if base_seq == next {
        return Some(InFlightFit::Follows);
    }
    // Past the newest batch this connection has in flight: a real gap, on
    // this facade's own evidence, unlike a stale window's.
    Some(InFlightFit::Gap(next))
}

/// The verdict for one produce entry, given what is remembered for its key:
/// `entry` is `(epoch, the remembered batches)`, or `None` when nothing is.
/// `accept` is the answer for the batches being the next ones.
fn decide(
    entry: Option<(i16, &[Batch])>,
    infos: &[BatchDecodeInfo],
    topic: &str,
    partition: i32,
    accept: Verdict,
    gaps_restart: bool,
) -> Verdict {
    let first = &infos[0];
    let epoch = first.producer_epoch;
    let base_seq = first.base_sequence;
    let Some((entry_epoch, recent)) = entry else {
        // No entry: a fresh producer, or one whose window this facade lost —
        // a restart, a leader change, a facade switch, an eviction. Either way
        // the window starts at this batch, at whatever sequence it carries:
        // Kafka's rule for a producer it holds no state for, and the module
        // header says why both refusals are worse.
        return transaction_starts_past_zero(first, gaps_restart, topic, partition)
            .unwrap_or(accept);
    };
    if epoch < entry_epoch {
        // Fenced. Not the transactional fencing this facade refuses: a
        // producer that bumped its own epoch and then retried a batch it had
        // already queued at the old one.
        return Verdict::Reject(
            ResponseError::InvalidProducerEpoch,
            format!(
                "producer {} sent epoch {epoch} for {topic}-{partition}, which is below the \
                 epoch {entry_epoch} this facade has already seen",
                first.producer_id
            ),
        );
    }
    if epoch > entry_epoch {
        // A bump IS a reset: the producer has restarted its own sequences
        // (`TransactionManager.resetSequenceNumbers`, called on exactly this
        // transition), so every remembered range belongs to a session that
        // no longer exists.
        return transaction_starts_past_zero(first, gaps_restart, topic, partition)
            .unwrap_or(accept);
    }
    let Some(newest) = recent.last() else {
        // An entry with an epoch and no batches: nothing this facade appended
        // at this epoch. At sequence 0 a new run; past it, in a cluster, a
        // producer that wrote through another facade.
        return if base_seq == 0 || gaps_restart {
            accept
        } else {
            Verdict::Reject(
                ResponseError::OutOfOrderSequenceNumber,
                format!(
                    "nothing has been appended for producer {} on {topic}-{partition} at epoch \
                     {epoch}, and the batch starts at sequence {base_seq} rather than 0",
                    first.producer_id
                ),
            )
        };
    };
    // A RESEND of exactly what was appended is a success carrying the
    // offsets the original got. This row is what makes idempotence real, and
    // it is Kafka's own behaviour rather than a convenience: a producer
    // whose response was lost retries, and the retry must be invisible.
    // Asked FIRST: within one epoch a sequence is never sent twice as new
    // work, so a batch the window holds is a resend wherever it sits.
    if let Some(offset) = exact_match(recent, infos) {
        return Verdict::Duplicate(offset);
    }
    let expected = increment_sequence(newest.last_seq, 1);
    if base_seq == expected {
        return accept;
    }
    if is_before(base_seq, expected) {
        if gaps_restart {
            // In a CLUSTER this facade cannot vouch for what is below its
            // window: the window may have restarted past batches that were
            // still on their way through another facade (a client that sends
            // the next batch to a new leader before an older one to the old
            // leader has failed — Java does). Answering such a batch
            // DUPLICATE_SEQUENCE_NUMBER is a SUCCESS to every client, for
            // records that may never have been written. So it is written:
            // at-least-once, and possibly behind a later batch, never lost.
            return accept;
        }
        // Inside or below the remembered range, but not a batch that was
        // appended as sent — a producer that re-batched its retry, or one
        // reaching past the five batches that are kept.
        return Verdict::Reject(
            ResponseError::DuplicateSequenceNumber,
            format!(
                "sequence {base_seq} for producer {} on {topic}-{partition} is at or below the \
                 last appended sequence {} and is not a batch this facade appended",
                first.producer_id, newest.last_seq
            ),
        );
    }
    // A gap in the held window. The gap a failed batch leaves — the batch the
    // client had already sent behind it — never reaches this point: the
    // connection that answered the failure is closed after it, so nothing
    // behind it is read (the module header). What is left, in a CLUSTER, is a
    // window another facade moved past while the producer wrote through it (a
    // leader change, a relay that came up), and refusing THAT is fatal to
    // librdkafka for the head of its queue — the module header. So there the
    // window restarts at this batch, like a lost one; the batches remembered
    // before the gap are kept, so an exact resend of one is still its
    // duplicate.
    if gaps_restart {
        return accept;
    }
    // A facade on its own: nothing else writes these partitions, so the gap is
    // real. Refusing it and writing NOTHING is the other half of idempotence,
    // and Apache Kafka's answer.
    Verdict::Reject(
        ResponseError::OutOfOrderSequenceNumber,
        format!(
            "sequence {base_seq} for producer {} on {topic}-{partition} would leave a gap: the \
             next sequence this facade will append is {expected}",
            first.producer_id
        ),
    )
}

/// A TRANSACTION's window that would start past sequence 0, on a facade on
/// its own: refused OUT_OF_ORDER_SEQUENCE_NUMBER.
///
/// A transactional producer's sequences start at 0 with every epoch this
/// facade grants it, and its transaction lives in this process (a restart
/// loses the transaction, not just the window), so its first batch of an
/// epoch past 0 is one whose earlier batches were refused — a full stage, a
/// partition not added — and are being retried behind it. A transaction's
/// refusal does not end the connection (`handlers::produce`), so this is
/// where that batch is stopped: started at it, the window would answer the
/// retry of the batches before it DUPLICATE_SEQUENCE_NUMBER, a success for
/// records the transaction never staged. The Java client retries the refusal
/// after the batches before it; a window lost to an eviction mid-transaction
/// is an abortable error for it, which is the truth.
fn transaction_starts_past_zero(
    first: &BatchDecodeInfo,
    gaps_restart: bool,
    topic: &str,
    partition: i32,
) -> Option<Verdict> {
    (first.transactional && first.base_sequence != 0 && !gaps_restart).then(|| {
        Verdict::Reject(
            ResponseError::OutOfOrderSequenceNumber,
            format!(
                "transactional producer {} starts epoch {} on {topic}-{partition} at sequence {} \
                 rather than 0: the batches before it were not staged here",
                first.producer_id, first.producer_epoch, first.base_sequence
            ),
        )
    })
}

/// Does `infos` leave a gap in a held window of the same epoch: past its next
/// sequence (or past 0 in a window with no batches) rather than at or below it?
fn is_gap(entry_epoch: i16, recent: &[Batch], infos: &[BatchDecodeInfo]) -> bool {
    let Some(first) = infos.first() else {
        return false;
    };
    if first.producer_epoch != entry_epoch {
        return false;
    }
    match recent.last() {
        None => first.base_sequence != 0,
        Some(newest) => {
            let expected = increment_sequence(newest.last_seq, 1);
            first.base_sequence != expected && !is_before(first.base_sequence, expected)
        }
    }
}

/// The base offset of a remembered run that matches `infos` batch for batch, or
/// `None`.
///
/// Batch for batch and not merely "the first sequence looks familiar": a
/// producer that re-batched its retry differently is NOT sending the same
/// records, and answering it with an offset from a batch of another shape would
/// hand it offsets for records it did not send.
fn exact_match(recent: &[Batch], infos: &[BatchDecodeInfo]) -> Option<i64> {
    let first = infos.first()?;
    let at = recent
        .iter()
        .position(|b| b.base_seq == first.base_sequence && b.last_seq == last_sequence(first))?;
    let run = recent.get(at..at + infos.len())?;
    run.iter()
        .zip(infos)
        .all(|(b, i)| b.base_seq == i.base_sequence && b.last_seq == last_sequence(i))
        .then_some(run[0].base_offset)
}

/// Is `seq` behind `expected` in Kafka's wrapping sequence space?
///
/// Kafka sequences are non-negative and wrap from `i32::MAX` to 0
/// ([`increment_sequence`]), so the comparison is made on the DISTANCE modulo
/// 2^31 rather than on the values or on `i32` subtraction: a producer whose
/// sequence has wrapped is still read as moving forwards. Half the space is
/// the boundary, which is the same convention TCP uses for the same problem
/// and the only one available without remembering how many times the counter
/// went round.
fn is_before(seq: i32, expected: i32) -> bool {
    const SPACE: i64 = 1 << 31;
    let behind = (i64::from(expected) - i64::from(seq)).rem_euclid(SPACE);
    behind != 0 && behind < SPACE / 2
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::records::{Compression, TimestampType};

    fn tenant() -> TenantKey {
        TenantKey::Tenant("acme".into())
    }

    /// One batch header, as a producer writes it.
    fn batch(producer_id: i64, epoch: i16, base_sequence: i32, records: i32) -> BatchDecodeInfo {
        BatchDecodeInfo {
            record_count: records,
            timestamp_type: TimestampType::Creation,
            min_offset: 0,
            min_timestamp: 0,
            base_sequence,
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id,
            producer_epoch: epoch,
            compression: Compression::None,
            version: 2,
        }
    }

    fn plain() -> BatchDecodeInfo {
        batch(NO_PRODUCER_ID, -1, -1, 3)
    }

    fn accepted(v: Verdict) -> Pending {
        match v {
            Verdict::Accept(p) => p,
            other => panic!("expected an accept, got {other:?}"),
        }
    }

    fn rejection(v: Verdict) -> ResponseError {
        match v {
            Verdict::Reject(e, _) => e,
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    /// The commit as the produce path makes it: the push took exactly the
    /// records the headers declared. The disagreement is a test of its own.
    fn landed(p: &Producers, pending: &Pending, base_offset: i64) {
        p.commit(pending, base_offset, pending.records() as usize);
    }

    // ------------------------------------------------------------ the happy path

    #[test]
    fn a_produce_with_no_producer_id_is_not_this_modules_business() {
        let p = Producers::new();
        assert_eq!(
            p.check(&tenant(), "orders", 0, &[plain(), plain()]),
            Verdict::NotIdempotent
        );
        assert_eq!(p.tracked(), 0);
    }

    #[test]
    fn a_fresh_producer_starts_at_sequence_zero() {
        let p = Producers::new();
        let pending = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 3)]));
        assert_eq!(pending.base_sequence(), 0);
        assert_eq!(pending.last_sequence(), 2);
        // Nothing is remembered until the push has landed.
        assert_eq!(p.tracked(), 0);
        landed(&p, &pending, 100);
        assert_eq!(p.tracked(), 1);
    }

    /// Two requests of one producer coalesced into one push: the second batch
    /// is checked while the first is still in flight. It follows the in-flight
    /// batch, a retransmission of the in-flight batch is a retriable refusal,
    /// and once both land the window is the one a serial producer leaves.
    #[test]
    fn a_coalesced_batch_follows_the_batch_in_flight() {
        let p = Producers::new();
        let first = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 3)]));
        // Without `begin` the window holds nothing, so the next batch reads as
        // a producer this facade lost and would start a window of its own:
        // `begin` is what places it AFTER the batch in flight.
        assert_eq!(p.resumed(), 0);
        assert!(matches!(
            p.check(&tenant(), "orders", 0, &[batch(7, 0, 3, 2)]),
            Verdict::Accept(_)
        ));
        assert_eq!(p.resumed(), 1);
        p.begin(&first);
        let second = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 3, 2)]));
        p.begin(&second);
        assert_eq!(
            rejection(p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 3)])),
            ResponseError::RequestTimedOut,
            "a resend of a batch still in flight"
        );
        // Another partition and another producer are not affected.
        accepted(p.check(&tenant(), "orders", 1, &[batch(7, 0, 0, 1)]));
        accepted(p.check(&tenant(), "orders", 0, &[batch(8, 0, 0, 1)]));
        landed(&p, &first, 100);
        p.end(&first);
        landed(&p, &second, 103);
        p.end(&second);
        assert_eq!(
            p.check(&tenant(), "orders", 0, &[batch(7, 0, 3, 2)]),
            Verdict::Duplicate(103)
        );
        let third = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 5, 1)]));
        assert_eq!(third.base_sequence(), 5);
        assert!(p.inflight.lock().unwrap().is_empty());
    }

    #[test]
    fn the_next_contiguous_batch_is_accepted() {
        let p = Producers::new();
        let first = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 3)]));
        landed(&p, &first, 100);
        let next = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 3, 2)]));
        assert_eq!((next.base_sequence(), next.last_sequence()), (3, 4));
        landed(&p, &next, 103);
    }

    /// The row that makes idempotence real: a resend of an appended batch is a
    /// SUCCESS carrying the offsets the original got.
    #[test]
    fn an_exact_resend_is_answered_with_the_offsets_the_original_got() {
        let p = Producers::new();
        let first = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 3)]));
        landed(&p, &first, 100);
        assert_eq!(
            p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 3)]),
            Verdict::Duplicate(100)
        );
        // ...and it is still a duplicate four batches later, which is what
        // WINDOW is for.
        for (seq, offset) in [(3, 103), (5, 105), (7, 107)] {
            let more = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, seq, 2)]));
            landed(&p, &more, offset);
        }
        assert_eq!(
            p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 3)]),
            Verdict::Duplicate(100)
        );
    }

    /// ...and past the ring's depth it is no longer a duplicate we can prove, so
    /// it is answered as the duplicate we cannot place rather than as a success.
    #[test]
    fn a_resend_older_than_the_window_is_duplicate_sequence_number() {
        let p = Producers::new();
        let mut seq = 0;
        for i in 0..(WINDOW as i64 + 2) {
            let v = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, seq, 2)]));
            landed(&p, &v, 100 + i * 2);
            seq += 2;
        }
        assert_eq!(
            rejection(p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 2)])),
            ResponseError::DuplicateSequenceNumber
        );
    }

    // ------------------------------------------------------------- the refusals

    /// A gap in a held window is a window another facade moved past while the
    /// producer wrote through it (stage, 2026-10-19 21:31, the module header):
    /// it restarts at the batch instead of killing the producer, is counted,
    /// and the ranges it fell behind are dropped.
    #[test]
    fn a_gap_in_a_held_window_restarts_it() {
        let p = Producers::new();
        p.restart_windows_on_gaps(true);
        let first = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 3)]));
        landed(&p, &first, 100);
        // 3 is the next sequence here; 5 is a producer that wrote 3 and 4
        // through another facade.
        let resynced = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 5, 2)]));
        assert_eq!(resynced.base_sequence(), 5);
        assert_eq!(p.resynced(), 1);
        assert_eq!(p.resumed(), 0, "a held window is not a lost one");
        landed(&p, &resynced, 200);
        // The window is the new one: a resend of 5 is its duplicate...
        assert_eq!(
            p.check(&tenant(), "orders", 0, &[batch(7, 0, 5, 2)]),
            Verdict::Duplicate(200)
        );
        // ...the ranges before it are KEPT, so an exact resend of a batch this
        // facade appended before the gap is still its duplicate...
        assert_eq!(
            p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 3)]),
            Verdict::Duplicate(100)
        );
        // ...a batch below the window it cannot vouch for (3, written through
        // the other facade or still on its way) is written, never answered as
        // a duplicate of nothing...
        let late = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 3, 2)]));
        assert_eq!(late.base_sequence(), 3);
        // ...and the next batch follows the new window.
        accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 7, 1)]));
        assert_eq!(p.resynced(), 1);
    }

    /// A late batch a clustered facade writes from below its window is not
    /// where the producer is. In flight, the producer's real next batch is not
    /// a gap behind it; committed, it does not become the window's newest, so
    /// an exact resend of a batch after it is still that batch's duplicate.
    #[test]
    fn a_late_batch_below_the_window_does_not_move_it() {
        let p = Producers::new();
        p.restart_windows_on_gaps(true);
        for (seq, offset) in [(0, 100), (6, 106), (10, 110)] {
            let run = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, seq, 1)]));
            landed(&p, &run, offset);
        }
        // 5 arrives late and is written; while it is in flight...
        let late = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 5, 1)]));
        p.begin(&late);
        // ...the producer's next batch follows the WINDOW, not the late one...
        let next = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 11, 1)]));
        p.begin(&next);
        // ...a resend of the late one is still answered from in flight...
        assert_eq!(
            rejection(p.check(&tenant(), "orders", 0, &[batch(7, 0, 5, 1)])),
            ResponseError::RequestTimedOut
        );
        // ...and a batch the window holds is its duplicate.
        assert_eq!(
            p.check(&tenant(), "orders", 0, &[batch(7, 0, 6, 1)]),
            Verdict::Duplicate(106)
        );
        landed(&p, &late, 111);
        p.end(&late);
        landed(&p, &next, 112);
        p.end(&next);
        // Committed in sequence order: 6 is a duplicate (it was answered
        // `expected` and written twice before), 12 is next, and the late 5 is
        // remembered too.
        assert_eq!(
            p.check(&tenant(), "orders", 0, &[batch(7, 0, 6, 1)]),
            Verdict::Duplicate(106)
        );
        assert_eq!(
            p.check(&tenant(), "orders", 0, &[batch(7, 0, 5, 1)]),
            Verdict::Duplicate(111)
        );
        accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 12, 1)]));
    }

    /// A batch follows a batch in flight only on the connection that sent that
    /// one: on another — a client that reconnected while its old connection's
    /// push still runs — nothing orders the two pushes, so it waits, refused
    /// retriably, for that push to answer.
    #[test]
    fn the_batch_after_one_in_flight_on_another_connection_waits_for_it() {
        let p = Producers::new();
        p.restart_windows_on_gaps(true);
        let old = accepted(p.check_on(&tenant(), "orders", 0, &[batch(7, 0, 0, 5)], 1));
        p.begin(&old);
        assert_eq!(
            rejection(p.check_on(&tenant(), "orders", 0, &[batch(7, 0, 5, 5)], 2)),
            ResponseError::RequestTimedOut
        );
        // The resend of the batch in flight is refused on any connection...
        assert_eq!(
            rejection(p.check_on(&tenant(), "orders", 0, &[batch(7, 0, 0, 5)], 2)),
            ResponseError::RequestTimedOut
        );
        // ...the old connection pipelines behind its own batch...
        accepted(p.check_on(&tenant(), "orders", 0, &[batch(7, 0, 5, 5)], 1));
        // ...and once the push answered, the new connection goes on from it.
        landed(&p, &old, 0);
        p.end(&old);
        accepted(p.check_on(&tenant(), "orders", 0, &[batch(7, 0, 5, 5)], 2));
    }

    /// A batch in flight for longer than any push can take was never ended — a
    /// bug, not a push — and must not refuse its producer for ever: it is
    /// dropped, and the producer's batch is judged by the window.
    #[test]
    fn a_batch_in_flight_for_too_long_is_dropped() {
        let p = Producers::new();
        let leaked = accepted(p.check_on(&tenant(), "orders", 0, &[batch(7, 0, 0, 5)], 1));
        p.begin(&leaked);
        assert_eq!(
            rejection(p.check_on(&tenant(), "orders", 0, &[batch(7, 0, 0, 5)], 2)),
            ResponseError::RequestTimedOut
        );
        for f in p.inflight.lock().unwrap().values_mut().flatten() {
            f.since = std::time::Instant::now()
                .checked_sub(INFLIGHT_STALE + std::time::Duration::from_secs(1))
                .unwrap();
        }
        accepted(p.check_on(&tenant(), "orders", 0, &[batch(7, 0, 0, 5)], 2));
        assert!(
            p.inflight.lock().unwrap().is_empty(),
            "the stale entry was kept"
        );
    }

    /// A batch in flight on another connection is no evidence of a gap: that
    /// connection may be one the client left (a relay drained at a restart)
    /// while the producer moved on through another facade. A batch past it
    /// waits, refused retriably — never OUT_OF_ORDER_SEQUENCE_NUMBER, which is
    /// fatal to librdkafka for its head batch. On its own connection the same
    /// batch IS a gap.
    #[test]
    fn another_connections_batch_in_flight_is_no_gap() {
        let p = Producers::new();
        p.restart_windows_on_gaps(true);
        let first = accepted(p.check_on(&tenant(), "orders", 0, &[batch(7, 0, 0, 1)], 1));
        landed(&p, &first, 0);
        let stale = accepted(p.check_on(&tenant(), "orders", 0, &[batch(7, 0, 1, 1)], 1));
        p.begin(&stale);
        assert_eq!(
            rejection(p.check_on(&tenant(), "orders", 0, &[batch(7, 0, 4, 1)], 2)),
            ResponseError::RequestTimedOut
        );
        assert_eq!(
            rejection(p.check_on(&tenant(), "orders", 0, &[batch(7, 0, 4, 1)], 1)),
            ResponseError::OutOfOrderSequenceNumber
        );
        p.end(&stale);
        // Once it answered, the window decides: a gap restarts it here.
        accepted(p.check_on(&tenant(), "orders", 0, &[batch(7, 0, 4, 1)], 2));
    }

    /// A run is committed before it leaves the in-flight list, so a check that
    /// reads the list first and the window second sees every begun batch in
    /// one or the other: its resend is never taken for new work, however the
    /// two interleave. Two threads, one writing a producer's batches through
    /// begin → commit → end, the other resending the newest one begun.
    #[test]
    fn a_resend_racing_its_commit_is_never_new_work() {
        use std::sync::atomic::AtomicI32;
        let p = Arc::new(Producers::new());
        let begun = Arc::new(AtomicI32::new(-1));
        const N: i32 = 20_000;
        let writer = {
            let (p, begun) = (Arc::clone(&p), Arc::clone(&begun));
            std::thread::spawn(move || {
                for seq in 0..N {
                    let run =
                        accepted(p.check_on(&tenant(), "orders", 0, &[batch(7, 0, seq, 1)], 1));
                    p.begin(&run);
                    begun.store(seq, Ordering::SeqCst);
                    std::thread::yield_now();
                    landed(&p, &run, i64::from(seq));
                    p.end(&run);
                }
            })
        };
        let mut checked = 0u64;
        while !writer.is_finished() {
            let seq = begun.load(Ordering::SeqCst);
            if seq < 0 {
                continue;
            }
            let v = p.check_on(&tenant(), "orders", 0, &[batch(7, 0, seq, 1)], 2);
            assert!(
                !matches!(v, Verdict::Accept(_)),
                "the resend of {seq} was taken for new work"
            );
            checked += 1;
        }
        writer.join().unwrap();
        assert!(checked > 0);
    }

    /// A transaction's window never starts past 0 on a facade on its own: its
    /// first batch of an epoch there is one whose earlier batches were refused
    /// and are being retried behind it, and starting at it would answer their
    /// retry as duplicates of records never staged.
    #[test]
    fn a_transactions_window_never_starts_past_zero() {
        let txn = |epoch: i16, seq: i32| BatchDecodeInfo {
            transactional: true,
            ..batch(7, epoch, seq, 5)
        };
        let p = Producers::new();
        assert_eq!(
            rejection(p.check(&tenant(), "orders", 0, &[txn(0, 5)])),
            ResponseError::OutOfOrderSequenceNumber
        );
        let first = accepted(p.check(&tenant(), "orders", 0, &[txn(0, 0)]));
        landed(&p, &first, -1);
        accepted(p.check(&tenant(), "orders", 0, &[txn(0, 5)]));
        // A new epoch starts at 0 too.
        assert_eq!(
            rejection(p.check(&tenant(), "orders", 0, &[txn(1, 5)])),
            ResponseError::OutOfOrderSequenceNumber
        );
        accepted(p.check(&tenant(), "orders", 0, &[txn(1, 0)]));
        // An idempotent producer outside a transaction resumes mid-sequence,
        // as after any lost window.
        accepted(p.check(&tenant(), "orders", 1, &[batch(7, 0, 5, 5)]));
    }

    /// The window keeps its batches in sequence order whatever order they are
    /// committed in, and a full window does not take one older than all of it.
    #[test]
    fn the_window_is_kept_in_sequence_order() {
        let mut slot = Slot::new(
            SlotKey {
                owner: 0,
                topic: 0,
                partition: 0,
            },
            0,
        );
        let b = |seq: i32| Batch {
            base_seq: seq,
            last_seq: seq,
            base_offset: i64::from(seq),
        };
        for seq in [10, 30, 20, 50, 40] {
            slot.insert(b(seq));
        }
        let seqs = |s: &Slot| -> Vec<i32> {
            s.recent[..usize::from(s.len)]
                .iter()
                .map(|x| x.base_seq)
                .collect()
        };
        assert_eq!(seqs(&slot), vec![10, 20, 30, 40, 50]);
        slot.insert(b(5));
        assert_eq!(seqs(&slot), vec![10, 20, 30, 40, 50], "older than all kept");
        slot.insert(b(25));
        assert_eq!(seqs(&slot), vec![20, 25, 30, 40, 50]);
        slot.insert(b(60));
        assert_eq!(seqs(&slot), vec![25, 30, 40, 50, 60]);
    }

    /// A run of an epoch the producer has since bumped past, committed late,
    /// is not remembered in the new epoch's window: it would move its next
    /// sequence, and the new session's own batches would be taken for
    /// duplicates of it.
    #[test]
    fn a_late_run_of_an_old_epoch_does_not_enter_the_new_window() {
        let p = Producers::new();
        let old = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 500, 5)]));
        p.begin(&old);
        // The producer bumps its epoch and starts again at 0.
        let fresh = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 1, 0, 5)]));
        landed(&p, &fresh, 0);
        // The old push answers now.
        landed(&p, &old, 5);
        p.end(&old);
        accepted(p.check(&tenant(), "orders", 0, &[batch(7, 1, 5, 5)]));
        assert_eq!(
            rejection(p.check(&tenant(), "orders", 0, &[batch(7, 0, 505, 5)])),
            ResponseError::InvalidProducerEpoch
        );
    }

    /// Kafka sequences wrap from i32::MAX to 0, and "behind" is decided in that
    /// space: a producer that wrapped is moving forwards, not resending.
    #[test]
    fn sequences_compare_across_the_wrap() {
        assert!(is_before(i32::MAX - 2, i32::MAX));
        assert!(
            is_before(i32::MAX, 3),
            "MAX is behind 3 once the counter wrapped"
        );
        assert!(!is_before(3, i32::MAX), "3 after the wrap is AHEAD of MAX");
        assert!(!is_before(5, 5));
        assert!(is_before(0, 1));
        assert!(!is_before(1, 0));
        // A window at the top of the space is extended by the wrapped batch.
        let p = Producers::new();
        let top = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, i32::MAX - 1, 2)]));
        landed(&p, &top, 100);
        let wrapped = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 1)]));
        assert_eq!(wrapped.base_sequence(), 0);
    }

    /// A facade on its OWN keeps Apache Kafka's answer: nothing else writes
    /// its partitions, so a gap in a window it holds is a real one, refused,
    /// and the window does not move.
    #[test]
    fn a_single_facade_refuses_a_gap_and_remembers_nothing() {
        let p = Producers::new();
        let first = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 3)]));
        landed(&p, &first, 100);
        assert_eq!(
            rejection(p.check(&tenant(), "orders", 0, &[batch(7, 0, 5, 2)])),
            ResponseError::OutOfOrderSequenceNumber
        );
        assert_eq!(p.resynced(), 0);
        let next = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 3, 2)]));
        assert_eq!(next.base_sequence(), 3);
    }

    /// The gap this facade has its own evidence for is still refused: a batch
    /// past the newest one IN FLIGHT here. The client sent them in order on
    /// one connection, so the one between them was refused or failed here.
    #[test]
    fn a_gap_behind_batches_in_flight_here_is_refused() {
        let p = Producers::new();
        let run = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 3)]));
        p.begin(&run);
        assert_eq!(
            rejection(p.check(&tenant(), "orders", 0, &[batch(7, 0, 5, 2)])),
            ResponseError::OutOfOrderSequenceNumber
        );
        assert_eq!(p.resynced(), 0);
        // The batch that follows the one in flight is accepted, pipelined.
        let next = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 3, 2)]));
        assert_eq!(next.base_sequence(), 3);
        p.end(&run);
    }

    /// The module header's caveat, as a test: a restart, a leader change, an
    /// eviction or a facade switch is an absent entry, and an absent entry
    /// starts the window at the batch that finds it, whatever its sequence —
    /// Kafka's rule for a producer it holds no state for. Refusing it is fatal
    /// to librdkafka. From there the window is an ordinary one.
    #[test]
    fn a_lost_window_starts_again_at_the_batch_that_finds_it() {
        let p = Producers::new();
        p.restart_windows_on_gaps(true);
        let resumed = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 42, 3)]));
        assert_eq!(resumed.base_sequence(), 42);
        assert_eq!(p.resumed(), 1);
        landed(&p, &resumed, 100);
        // A resend of it is a duplicate again, with the offset it got...
        assert_eq!(
            p.check(&tenant(), "orders", 0, &[batch(7, 0, 42, 3)]),
            Verdict::Duplicate(100)
        );
        // ...the next batch follows it...
        let next = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 45, 2)]));
        assert_eq!(next.base_sequence(), 45);
        // ...and a gap after it restarts it again: the producer wrote 47-49
        // elsewhere.
        accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 50, 1)]));
        assert_eq!(p.resynced(), 1);
        // A producer starting at 0 is a new one, not a resumed one.
        accepted(p.check(&tenant(), "orders", 0, &[batch(8, 0, 0, 1)]));
        assert_eq!(p.resumed(), 1);
    }

    #[test]
    fn an_old_epoch_is_fenced() {
        let p = Producers::new();
        let v = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 4, 0, 3)]));
        landed(&p, &v, 100);
        assert_eq!(
            rejection(p.check(&tenant(), "orders", 0, &[batch(7, 3, 3, 3)])),
            ResponseError::InvalidProducerEpoch
        );
    }

    /// KIP-360: a bump IS a reset. The client has restarted its own sequences,
    /// so the remembered ranges belong to a session that no longer exists.
    #[test]
    fn a_higher_epoch_resets_the_window() {
        let p = Producers::new();
        let v = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 3)]));
        landed(&p, &v, 100);
        let bumped = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 1, 0, 3)]));
        landed(&p, &bumped, 200);
        // The old epoch's range is gone: sequence 0 at the NEW epoch is the
        // duplicate now, and it carries the new offset.
        assert_eq!(
            p.check(&tenant(), "orders", 0, &[batch(7, 1, 0, 3)]),
            Verdict::Duplicate(200)
        );
    }

    #[test]
    fn batches_of_one_request_must_be_contiguous_with_each_other() {
        let p = Producers::new();
        assert_eq!(
            rejection(p.check(
                &tenant(),
                "orders",
                0,
                &[batch(7, 0, 0, 3), batch(7, 0, 5, 2)]
            )),
            ResponseError::OutOfOrderSequenceNumber
        );
    }

    /// A run is remembered batch by batch, and the offset of each is derived
    /// from the ONE base offset the push answered. That is what lets a resend of
    /// the whole run, or of any batch of it, be answered with the offsets those
    /// records actually got.
    #[test]
    fn a_run_of_contiguous_batches_is_remembered_batch_by_batch() {
        let p = Producers::new();
        let v = accepted(p.check(
            &tenant(),
            "orders",
            0,
            &[batch(7, 0, 0, 3), batch(7, 0, 3, 2)],
        ));
        assert_eq!((v.base_sequence(), v.last_sequence()), (0, 4));
        assert_eq!(v.records(), 5);
        landed(&p, &v, 100);
        assert_eq!(
            p.check(
                &tenant(),
                "orders",
                0,
                &[batch(7, 0, 0, 3), batch(7, 0, 3, 2)]
            ),
            Verdict::Duplicate(100)
        );
        // The SECOND batch alone: its three predecessors put it at 103.
        assert_eq!(
            p.check(&tenant(), "orders", 0, &[batch(7, 0, 3, 2)]),
            Verdict::Duplicate(103)
        );
    }

    /// A resend that re-batched the same records differently is NOT the batch
    /// this facade appended, and answering it with an offset from a batch of
    /// another shape would hand it offsets for records it did not send.
    #[test]
    fn a_rebatched_resend_is_not_answered_as_an_exact_duplicate() {
        let p = Producers::new();
        let v = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 3)]));
        landed(&p, &v, 100);
        // Same sequence start, two records instead of three.
        assert_eq!(
            rejection(p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 2)])),
            ResponseError::DuplicateSequenceNumber
        );
    }

    /// The window is advanced only by a push whose length is the one the batch
    /// headers described. Anything else and an offset per batch would have to be
    /// guessed, so nothing is remembered — at-least-once, the safe direction.
    #[test]
    fn a_push_that_does_not_match_the_headers_advances_nothing() {
        let p = Producers::new();
        let v = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 3)]));
        p.commit(&v, 100, 2);
        assert_eq!(p.tracked(), 0);
        // ...so the same batch is new work rather than a duplicate.
        assert!(matches!(
            p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 3)]),
            Verdict::Accept(_)
        ));
    }

    #[test]
    fn a_mixed_entry_is_refused_rather_than_half_answered() {
        let p = Producers::new();
        assert_eq!(
            rejection(p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 3), plain()])),
            ResponseError::InvalidRecord
        );
        assert_eq!(
            rejection(p.check(
                &tenant(),
                "orders",
                0,
                &[batch(7, 0, 0, 3), batch(8, 0, 0, 3)]
            )),
            ResponseError::InvalidRecord
        );
    }

    // ------------------------------------------------------------------ scoping

    /// A producer id is 62 bits of entropy, so this cannot happen — and if it
    /// did, one tenant must not be able to reach another's sequence state.
    #[test]
    fn two_tenants_with_one_producer_id_do_not_share_a_window() {
        let p = Producers::new();
        let other = TenantKey::Tenant("globex".into());
        let a = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 3)]));
        landed(&p, &a, 100);
        // The same id, the same topic, the same sequence, a different tenant:
        // an accept, not a duplicate.
        let b = accepted(p.check(&other, "orders", 0, &[batch(7, 0, 0, 3)]));
        landed(&p, &b, 500);
        assert_eq!(
            p.check(&other, "orders", 0, &[batch(7, 0, 0, 3)]),
            Verdict::Duplicate(500)
        );
        assert_eq!(
            p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 3)]),
            Verdict::Duplicate(100)
        );
    }

    /// The key includes the TOPIC, and this is the test that says why: a
    /// producer writing to two topics has two sequence 0s in flight, and a key
    /// without the topic would drop the second one as a duplicate.
    #[test]
    fn one_producer_writing_two_topics_has_two_windows() {
        let p = Producers::new();
        let a = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 3)]));
        landed(&p, &a, 100);
        let b = accepted(p.check(&tenant(), "invoices", 0, &[batch(7, 0, 0, 3)]));
        landed(&p, &b, 0);
        assert_eq!(p.tracked(), 2);
    }

    #[test]
    fn one_producer_writing_two_partitions_has_two_windows() {
        let p = Producers::new();
        let a = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 3)]));
        landed(&p, &a, 100);
        let b = accepted(p.check(&tenant(), "orders", 1, &[batch(7, 0, 0, 3)]));
        landed(&p, &b, 0);
        assert_eq!(p.tracked(), 2);
    }

    #[test]
    fn forget_drops_one_producer_of_one_tenant_and_nothing_else() {
        let p = Producers::new();
        let other = TenantKey::Tenant("globex".into());
        for (t, id) in [(tenant(), 7), (tenant(), 8), (other.clone(), 7)] {
            let v = accepted(p.check(&t, "orders", 0, &[batch(id, 0, 0, 3)]));
            landed(&p, &v, 100);
        }
        assert_eq!(p.tracked(), 3);
        p.forget(&tenant(), 7);
        assert_eq!(p.tracked(), 2);
        // The other tenant's identical id survived...
        assert_eq!(
            p.check(&other, "orders", 0, &[batch(7, 0, 0, 3)]),
            Verdict::Duplicate(100)
        );
        // ...and this tenant's is a fresh producer again.
        assert!(matches!(
            p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 3)]),
            Verdict::Accept(_)
        ));
    }

    // ------------------------------------------------------------- the plumbing

    #[test]
    fn an_empty_transactional_id_is_not_a_transactional_id() {
        assert_eq!(transactional_id(None), None);
        assert_eq!(transactional_id(Some("")), None);
        assert_eq!(transactional_id(Some("tx-1")), Some("tx-1"));
    }

    #[test]
    fn a_minted_producer_id_is_never_a_sentinel_and_never_repeats() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..10_000 {
            let id = new_producer_id();
            assert!(id > 0, "producer id {id} is not positive");
            assert_ne!(id, NO_PRODUCER_ID);
            assert!(id < (1i64 << 62), "producer id {id} is wider than 62 bits");
            assert!(seen.insert(id), "producer id {id} was minted twice");
        }
    }

    #[test]
    fn sequences_wrap_the_way_kafka_wraps_them() {
        assert_eq!(increment_sequence(0, 1), 1);
        assert_eq!(increment_sequence(i32::MAX, 1), 0);
        assert_eq!(increment_sequence(i32::MAX - 1, 3), 1);
        // ...and an empty batch's "last" is one below its base, so the next
        // batch is still contiguous with it.
        assert_eq!(last_sequence(&batch(7, 0, 10, 0)), 9);
        assert_eq!(last_sequence(&batch(7, 0, 10, 1)), 10);
    }

    // ------------------------------------------------------------- the cap

    /// A commit of `(producer, topic, partition)` at sequence 0, as a fresh
    /// producer's first batch lands.
    fn first_batch(p: &Producers, t: &TenantKey, id: i64, topic: &str, partition: i32) {
        let v = accepted(p.check(t, topic, partition, &[batch(id, 0, 0, 1)]));
        landed(p, &v, 0);
    }

    /// The slot is the bulk of an entry's memory, and ENTRY_BYTES is stated
    /// from it: a field added to it must be paid for knowingly.
    #[test]
    fn a_slot_is_the_size_the_memory_figure_is_stated_from() {
        assert!(
            std::mem::size_of::<Slot>() <= 112,
            "{} bytes",
            std::mem::size_of::<Slot>()
        );
        assert!(ENTRY_BYTES >= std::mem::size_of::<Slot>());
    }

    /// The tracker never holds more than its cap, and every entry it drops for
    /// room is counted — the number a healthy node keeps at 0.
    #[test]
    fn the_tracker_never_holds_more_than_its_cap_and_counts_what_it_evicts() {
        let p = Producers::with_capacity(MIN_MAX_TRACKED);
        let total = MIN_MAX_TRACKED as i64 * 3;
        for id in 0..total {
            first_batch(&p, &tenant(), id + 1, "orders", 0);
            assert!(p.tracked() <= MIN_MAX_TRACKED);
        }
        assert_eq!(p.tracked(), MIN_MAX_TRACKED);
        assert_eq!(p.evictions(), (total as u64) - MIN_MAX_TRACKED as u64);
        assert_eq!(p.capacity(), MIN_MAX_TRACKED);
    }

    /// Eviction takes the least recently WRITTEN entry and nothing else: an
    /// entry that keeps being written survives any number of newcomers, and the
    /// one that went quiet first goes first. (A small tracker is one shard, so
    /// this is the exact LRU.)
    #[test]
    fn eviction_takes_the_least_recently_written_entry() {
        let p = Producers::with_capacity(MIN_MAX_TRACKED);
        assert_eq!(p.shards.len(), 1);
        for id in 1..=MIN_MAX_TRACKED as i64 {
            first_batch(&p, &tenant(), id, "orders", 0);
        }
        // Producer 1 keeps writing; the others are silent.
        let mut seq = 1;
        for newcomer in 0..(MIN_MAX_TRACKED as i64 * 2) {
            let v = accepted(p.check(&tenant(), "orders", 0, &[batch(1, 0, seq, 1)]));
            landed(&p, &v, i64::from(seq));
            seq += 1;
            first_batch(&p, &tenant(), 100_000 + newcomer, "orders", 0);
        }
        // Producer 1 was never evicted: a resend of its last batch is still
        // the duplicate it is.
        assert_eq!(
            p.check(&tenant(), "orders", 0, &[batch(1, 0, seq - 1, 1)]),
            Verdict::Duplicate(i64::from(seq - 1))
        );
        // Producer 2, the least recently written, went first — its resend is
        // no longer a duplicate this facade can place, and starts a window.
        assert!(matches!(
            p.check(&tenant(), "orders", 0, &[batch(2, 0, 0, 1)]),
            Verdict::Accept(_)
        ));
    }

    /// THE property the cap exists to keep: below it, nothing is evicted — not
    /// in any shard, however the keys hash — so an active producer never loses
    /// its window (and its duplicate detection) for want of room. A sharded
    /// tracker filled to exactly its cap and written again, entry by entry,
    /// evicts nothing.
    #[test]
    fn an_active_producer_below_the_cap_is_never_evicted() {
        let cap = 65_536;
        let p = Producers::with_capacity(cap);
        assert!(
            p.shards.len() > 1,
            "the property is about a SHARDED tracker"
        );
        // 64 producers x 1024 partitions: every entry of the cap, in use.
        for id in 1..=64i64 {
            for partition in 0..1024 {
                first_batch(&p, &tenant(), id, "orders", partition);
            }
        }
        assert_eq!(p.tracked(), cap);
        for id in 1..=64i64 {
            for partition in 0..1024 {
                let v = accepted(p.check(&tenant(), "orders", partition, &[batch(id, 0, 1, 1)]));
                landed(&p, &v, 1);
            }
        }
        assert_eq!(p.evictions(), 0);
        assert_eq!(p.tracked(), cap);
    }

    /// `forget` drops one producer's entries in every shard and leaves every
    /// other producer's alone — without looking at them.
    #[test]
    fn forget_drops_one_producer_across_every_shard() {
        let p = Producers::with_capacity(DEFAULT_MAX_TRACKED);
        assert_eq!(p.shards.len(), MAX_SHARDS);
        for partition in 0..2_000 {
            first_batch(&p, &tenant(), 7, "orders", partition);
            first_batch(&p, &tenant(), 8, "orders", partition);
        }
        assert_eq!(p.tracked(), 4_000);
        p.forget(&tenant(), 7);
        assert_eq!(p.tracked(), 2_000);
        for partition in [0, 999, 1_999] {
            // 7's windows are gone: its resend of sequence 0 starts a new one
            // rather than being answered as a duplicate...
            assert!(matches!(
                p.check(&tenant(), "orders", partition, &[batch(7, 0, 0, 1)]),
                Verdict::Accept(_)
            ));
            // ...while 8's are untouched.
            assert_eq!(
                p.check(&tenant(), "orders", partition, &[batch(8, 0, 0, 1)]),
                Verdict::Duplicate(0)
            );
        }
        for partition in 0..2_000 {
            first_batch(&p, &tenant(), 9, "orders", partition);
        }
        assert_eq!(p.tracked(), 4_000);
    }

    /// A freed slot is reused before the slab grows: a tracker whose producers
    /// come and go stays the size of what it holds.
    #[test]
    fn freed_slots_are_reused_before_the_slab_grows() {
        let p = Producers::with_capacity(MIN_MAX_TRACKED);
        for partition in 0..1_000 {
            first_batch(&p, &tenant(), 7, "orders", partition);
        }
        let slots = p.shards[0].lock().unwrap().slots.len();
        for round in 0..20 {
            p.forget(&tenant(), 7 + round);
            for partition in 0..1_000 {
                first_batch(&p, &tenant(), 8 + round, "orders", partition);
            }
        }
        assert_eq!(p.shards[0].lock().unwrap().slots.len(), slots);
        assert_eq!(p.tracked(), 1_000);
    }

    /// The interned producers and topics go with their last slot: a tracker
    /// that has seen a million keys and dropped them holds nothing for them.
    #[test]
    fn eviction_and_forget_leave_no_interned_records_behind() {
        let p = Producers::with_capacity(MIN_MAX_TRACKED);
        for id in 1..=5_000i64 {
            first_batch(
                &p,
                &tenant(),
                id,
                &format!("topic-{}", id % 97),
                (id % 13) as i32,
            );
        }
        {
            let shard = p.shards[0].lock().unwrap();
            let owners: usize = shard.owners.values().map(Vec::len).sum();
            let live_owners = shard.owner_recs.iter().filter(|o| o.is_some()).count();
            assert_eq!(owners, live_owners);
            assert!(live_owners <= MIN_MAX_TRACKED);
            assert_eq!(
                shard.topics.len(),
                shard.topic_recs.iter().filter(|t| t.is_some()).count()
            );
        }
        for id in 1..=5_000i64 {
            p.forget(&tenant(), id);
        }
        assert_eq!(p.tracked(), 0);
        let shard = p.shards[0].lock().unwrap();
        assert!(shard.owners.is_empty());
        assert!(shard.topics.is_empty());
        assert!(shard.index.is_empty());
        assert_eq!((shard.head, shard.tail), (NIL, NIL));
    }

    /// The verdicts are a function of the remembered windows and nothing else,
    /// so the sharded slab must answer EXACTLY what the table it replaced
    /// answered, for any interleaving of produces, resends, gaps, bumps,
    /// forgets and evictions. A tiny model of the old table — a map and a scan
    /// for the coldest entry — is driven with the same random operations.
    #[test]
    fn the_tracker_answers_exactly_what_a_plain_lru_table_answers() {
        tracker_against_model(false);
    }

    /// The same, for a CLUSTERED facade, which restarts a window on a gap.
    #[test]
    fn a_clustered_tracker_answers_exactly_what_a_plain_lru_table_answers() {
        tracker_against_model(true);
    }

    fn tracker_against_model(gaps_restart: bool) {
        use std::collections::HashMap as Map;

        #[derive(Clone)]
        struct ModelEntry {
            epoch: i16,
            recent: Vec<Batch>,
            used: u64,
        }
        struct Model {
            cap: usize,
            clock: u64,
            entries: Map<(i64, String, i32), ModelEntry>,
            gaps_restart: bool,
        }
        impl Model {
            fn check(&self, topic: &str, partition: i32, infos: &[BatchDecodeInfo]) -> Verdict {
                let key = (infos[0].producer_id, topic.to_string(), partition);
                let held = self
                    .entries
                    .get(&key)
                    .is_some_and(|e| e.epoch == infos[0].producer_epoch && !e.recent.is_empty());
                let pending = Pending {
                    key: Key {
                        tenant: tenant(),
                        producer_id: infos[0].producer_id,
                        topic: topic.to_string(),
                        partition,
                    },
                    epoch: infos[0].producer_epoch,
                    conn: 0,
                    close_on_failure: self.gaps_restart || !held,
                    batches: {
                        let mut delta = 0;
                        infos
                            .iter()
                            .map(|i| {
                                let records = i64::from(i.record_count.max(0));
                                let b = PendingBatch {
                                    base_seq: i.base_sequence,
                                    last_seq: last_sequence(i),
                                    records,
                                    offset_delta: delta,
                                };
                                delta += records;
                                b
                            })
                            .collect()
                    },
                };
                let entry = self
                    .entries
                    .get(&key)
                    .map(|e| (e.epoch, e.recent.as_slice()));
                decide(
                    entry,
                    infos,
                    topic,
                    partition,
                    Verdict::Accept(pending),
                    self.gaps_restart,
                )
            }
            fn commit(&mut self, pending: &Pending, base: i64) {
                self.clock += 1;
                let key = (
                    pending.key.producer_id,
                    pending.key.topic.clone(),
                    pending.key.partition,
                );
                let batches: Vec<Batch> = pending
                    .batches
                    .iter()
                    .map(|b| Batch {
                        base_seq: b.base_seq,
                        last_seq: b.last_seq,
                        base_offset: base + b.offset_delta,
                    })
                    .collect();
                if let Some(e) = self.entries.get_mut(&key) {
                    if pending.epoch > e.epoch {
                        e.epoch = pending.epoch;
                        e.recent.clear();
                    }
                    e.recent.extend(batches);
                    while e.recent.len() > WINDOW {
                        e.recent.remove(0);
                    }
                    e.used = self.clock;
                    return;
                }
                while self.entries.len() >= self.cap {
                    let coldest = self
                        .entries
                        .iter()
                        .min_by_key(|(_, e)| e.used)
                        .map(|(k, _)| k.clone())
                        .unwrap();
                    self.entries.remove(&coldest);
                }
                let mut recent = batches;
                while recent.len() > WINDOW {
                    recent.remove(0);
                }
                self.entries.insert(
                    key,
                    ModelEntry {
                        epoch: pending.epoch,
                        recent,
                        used: self.clock,
                    },
                );
            }
            fn forget(&mut self, id: i64) {
                self.entries.retain(|k, _| k.0 != id);
            }
        }

        // A deterministic xorshift, so a failure reproduces.
        let mut rng = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move |n: u64| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng % n
        };
        let cap = MIN_MAX_TRACKED;
        let p = Producers::with_capacity(cap);
        p.restart_windows_on_gaps(gaps_restart);
        let mut model = Model {
            cap,
            clock: 0,
            entries: Map::new(),
            gaps_restart,
        };
        // Per (producer, topic, partition): the sequence the producer is at.
        let mut at: Map<(i64, &str, i32), (i16, i32)> = Map::new();
        let mut offset = 0i64;
        for step in 0..60_000 {
            let id = 1 + next(40) as i64;
            let topic = ["orders", "clicks", "a"][next(3) as usize];
            let partition = next(40) as i32;
            let (epoch, seq) = *at.entry((id, topic, partition)).or_insert((0, 0));
            let op = next(100);
            if op < 2 {
                p.forget(&tenant(), id);
                model.forget(id);
                continue;
            }
            // Mostly the next batch; sometimes a resend, a gap, a lower or
            // higher epoch, a multi-batch run.
            let (e, base, runs) = match op {
                2..=9 => (epoch, (seq - 1 - next(8) as i32).max(0), 1),
                10..=13 => (epoch, seq + 1 + next(3) as i32, 1),
                14..=15 => (epoch.saturating_sub(1), seq, 1),
                16..=18 => (epoch + 1, 0, 1),
                19..=24 => (epoch, seq, 2 + next(3) as usize),
                _ => (epoch, seq, 1),
            };
            let mut infos = Vec::new();
            let mut s = base;
            for _ in 0..runs {
                let n = 1 + next(4) as i32;
                infos.push(batch(id, e, s, n));
                s += n;
            }
            let got = p.check(&tenant(), topic, partition, &infos);
            let want = model.check(topic, partition, &infos);
            assert_eq!(got, want, "step {step}: {id} {topic}-{partition} {infos:?}");
            if let Verdict::Accept(pending) = got {
                let records = pending.records();
                landed(&p, &pending, offset);
                model.commit(&pending, offset);
                offset += records;
                at.insert((id, topic, partition), (e, s));
            }
            assert_eq!(p.tracked(), model.entries.len(), "step {step}");
        }
        assert!(p.evictions() > 0, "the run never reached the cap");
    }

    /// Many connections produce at once; the shards take them in parallel and
    /// the count stays exact.
    #[test]
    fn concurrent_producers_keep_an_exact_count() {
        let p = std::sync::Arc::new(Producers::with_capacity(DEFAULT_MAX_TRACKED));
        let threads: Vec<_> = (0..8i64)
            .map(|t| {
                let p = std::sync::Arc::clone(&p);
                std::thread::spawn(move || {
                    for partition in 0..5_000 {
                        first_batch(&p, &tenant(), t + 1, "orders", partition);
                        let v = accepted(p.check(
                            &tenant(),
                            "orders",
                            partition,
                            &[batch(t + 1, 0, 1, 1)],
                        ));
                        landed(&p, &v, 1);
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(p.tracked(), 40_000);
        assert_eq!(p.evictions(), 0);
    }
}
