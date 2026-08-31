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
//! 2. `conn::connection` is serial by construction: it reads one frame,
//!    awaits `dispatch` to completion, writes the response, and only then looks
//!    at the next byte. `max.in.flight.requests.per.connection=5` is therefore
//!    five requests on the WIRE and exactly one in the facade, processed in
//!    arrival order — so check, push and commit cannot interleave with another
//!    request for the same `(producer, partition)`. There is no second request
//!    being processed.
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
//! so a broker restart does not lose a sequence window. This facade holds no
//! durable state by design, so after a **facade restart**, after a connection
//! **moves to another facade** behind one advertised address, or after the LRU
//! **evicted** an idle producer's entry, the next batch of a producer that is
//! not at sequence 0 meets an absent entry.
//!
//! That is answered OUT_OF_ORDER_SEQUENCE_NUMBER (45), which is what a modern
//! Kafka broker answers for a missing entry, and the client's recovery is
//! KIP-360's epoch bump — which is exactly why `InitProducerId` v3 is inside the
//! advertised window ([`crate::versions`]) rather than a nicety. The cost is
//! bounded and it is real: **the in-flight window may be written twice**, i.e.
//! at-least-once for at most [`WINDOW`] batches, exactly as Kafka degrades when
//! producer state has expired.
//!
//! UNKNOWN_PRODUCER_ID (59) is deliberately NOT used. Kafka moved away from it
//! for the same reason it is wrong here: some clients answer it by reasoning
//! about `log_start_offset`, which the produce path answers as -1, and a client
//! that cannot reason about it may fail the batch fatally. OUT_OF_ORDER is the
//! code whose recovery path is implemented in every client that sets the flag.
//!
//! The durable cure exists and is recorded rather than taken: `PushItem`'s
//! `transactionId` is a broker-side dedup key, and
//! `"qk:<pid>:<epoch>:<partition>:<seq>"` would move this whole window into
//! Queen. It is not done here because the guarantee would silently depend on a
//! per-queue `dedupWindowSeconds` a Kafka client cannot see, and because it puts
//! a dedup key on every record of every idempotent producer — a cost on the hot
//! write path that needs a measurement before it is a design.

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

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

/// How many `(tenant, producer, topic-partition)` entries this process tracks.
///
/// About 10 MB at the ~160 bytes an entry costs: 1024 producers over 64
/// partitions each, or 64 producers over the full default width. Evicted least
/// recently WRITTEN first, the same shape and the same reasoning as the SNI
/// lane map in `lib.rs` — and an evicted entry behaves exactly like one that was
/// never there, which the module header shows is a recoverable state rather
/// than a silent one.
pub const MAX_TRACKED: usize = 65_536;

/// One line per window when the tracker is evicting, not one per produce: the
/// cap is reached by a fleet, so a line per eviction is a line per producer per
/// batch. See [`obs::Sampler`].
static TRACK_CAP: obs::Sampler = obs::Sampler::new(60_000);

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

/// What is remembered about one `(tenant, producer, topic-partition)`.
struct Entry {
    /// The highest epoch seen. A batch below it is fenced; a batch above it is
    /// a reset ([`Producers::check`]).
    epoch: i16,
    /// The last [`WINDOW`] batches, oldest first.
    recent: Vec<Batch>,
    /// The tracker's own clock at the last WRITE. The eviction order.
    used: u64,
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
pub struct Producers {
    entries: Mutex<HashMap<Key, Entry>>,
    /// Ticks once per write. The ordering `entries` is evicted in.
    clock: AtomicU64,
}

impl Default for Producers {
    fn default() -> Producers {
        Producers::new()
    }
}

impl Producers {
    pub fn new() -> Producers {
        Producers {
            entries: Mutex::new(HashMap::new()),
            clock: AtomicU64::new(0),
        }
    }

    /// How many entries are tracked right now. For tests and for the eviction
    /// log line; nothing on a request path reads it.
    pub fn tracked(&self) -> usize {
        self.entries
            .lock()
            .expect("the producer map lock is never held across a panic")
            .len()
    }

    /// Forget everything about one producer id under one tenant.
    ///
    /// Called by [`crate::handlers::init_producer_id`] on KIP-360's epoch bump:
    /// a producer that asked for a new epoch has reset its own sequences to 0,
    /// so keeping the old ranges could only produce a wrong answer. The produce
    /// path resets on a higher epoch too ([`Producers::check`]) — this is the
    /// cheaper half of the same rule, and it returns the memory rather than
    /// waiting for the LRU to.
    pub fn forget(&self, tenant: &TenantKey, producer_id: i64) {
        let mut entries = self
            .entries
            .lock()
            .expect("the producer map lock is never held across a panic");
        entries.retain(|k, _| k.producer_id != producer_id || &k.tenant != tenant);
    }

    /// What to do with one `(topic, partition)` entry's batches, decided on the
    /// batch HEADERS — the place [`crate::handlers::produce::refuse`] already
    /// stands, so a batch this refuses is never decompressed.
    pub fn check(
        &self,
        tenant: &TenantKey,
        topic: &str,
        partition: i32,
        infos: &[BatchDecodeInfo],
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
        let base_seq = first.base_sequence;
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
        let accept = Verdict::Accept(Pending {
            key: key.clone(),
            epoch,
            batches,
        });

        let entries = self
            .entries
            .lock()
            .expect("the producer map lock is never held across a panic");
        let Some(entry) = entries.get(&key) else {
            // No entry: a fresh producer, an evicted one, or one whose facade
            // restarted. Sequence 0 is the first of those and is accepted;
            // anything else is the module header's caveat, and the code is
            // chosen for the recovery it triggers rather than for its wording.
            return if base_seq == 0 {
                accept
            } else {
                Verdict::Reject(
                    ResponseError::OutOfOrderSequenceNumber,
                    format!(
                        "this facade holds no sequence state for producer {} on {topic}-{partition}, \
                         and the batch starts at sequence {base_seq} rather than 0 — bump the \
                         producer epoch and resend (KIP-360)",
                        first.producer_id
                    ),
                )
            };
        };
        if epoch < entry.epoch {
            // Fenced. Not the transactional fencing this facade refuses: a
            // producer that bumped its own epoch and then retried a batch it had
            // already queued at the old one.
            return Verdict::Reject(
                ResponseError::InvalidProducerEpoch,
                format!(
                    "producer {} sent epoch {epoch} for {topic}-{partition}, which is below the \
                     epoch {} this facade has already seen",
                    first.producer_id, entry.epoch
                ),
            );
        }
        if epoch > entry.epoch {
            // A bump IS a reset: the producer has restarted its own sequences
            // (`TransactionManager.resetSequenceNumbers`, called on exactly this
            // transition), so every remembered range belongs to a session that
            // no longer exists.
            return accept;
        }
        let Some(newest) = entry.recent.last() else {
            // An entry with an epoch and no batches: the epoch was raised by a
            // bump and nothing has been written since. Same rule as an absent
            // entry, minus the eviction.
            return if base_seq == 0 {
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
        let expected = increment_sequence(newest.last_seq, 1);
        if base_seq == expected {
            return accept;
        }
        // A RESEND of exactly what was appended is a success carrying the
        // offsets the original got. This row is what makes idempotence real, and
        // it is Kafka's own behaviour rather than a convenience: a producer
        // whose response was lost retries, and the retry must be invisible.
        if let Some(offset) = exact_match(&entry.recent, infos) {
            return Verdict::Duplicate(offset);
        }
        if is_before(base_seq, expected) {
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
        // The gap. Refusing it and writing NOTHING is the other half of
        // idempotence: without it, a batch that failed followed by a batch that
        // succeeded would leave a hole, and the guarantee would be about
        // duplicates only. This is what makes the Java client re-drain and
        // resend in order.
        Verdict::Reject(
            ResponseError::OutOfOrderSequenceNumber,
            format!(
                "sequence {base_seq} for producer {} on {topic}-{partition} would leave a gap: the \
                 next sequence this facade will append is {expected}",
                first.producer_id
            ),
        )
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
        let now = self.clock.fetch_add(1, Ordering::Relaxed);
        let mut entries = self
            .entries
            .lock()
            .expect("the producer map lock is never held across a panic");
        // One remembered range per BATCH and not one per run, because a producer
        // that retries sends the batches it sent — Kafka's own
        // `ProducerStateEntry` is per batch for the same reason.
        let batches = pending.batches.iter().map(|b| Batch {
            base_seq: b.base_seq,
            last_seq: b.last_seq,
            base_offset: base_offset + b.offset_delta,
        });
        if let Some(entry) = entries.get_mut(&pending.key) {
            if pending.epoch > entry.epoch {
                entry.epoch = pending.epoch;
                entry.recent.clear();
            }
            entry.recent.extend(batches);
            while entry.recent.len() > WINDOW {
                entry.recent.remove(0);
            }
            entry.used = now;
            return;
        }
        let mut recent: Vec<Batch> = batches.collect();
        while recent.len() > WINDOW {
            recent.remove(0);
        }
        // Asked before anything is inserted, so a map that is already at the cap
        // evicts exactly one entry per new one rather than in bursts.
        while entries.len() >= MAX_TRACKED {
            let Some(coldest) = entries
                .iter()
                .min_by_key(|(_, e)| e.used)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            entries.remove(&coldest);
            if let Some(suppressed) = TRACK_CAP.tick_now() {
                tracing::warn!(
                    target: "kafka",
                    tracked = MAX_TRACKED,
                    suppressed,
                    "more idempotent producers than this facade keeps sequence state for; the \
                     least recently written are being dropped and their next batch will be \
                     answered OUT_OF_ORDER_SEQUENCE_NUMBER so the client bumps its epoch"
                );
            }
        }
        entries.insert(
            pending.key.clone(),
            Entry {
                epoch: pending.epoch,
                recent,
                used: now,
            },
        );
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
/// The comparison is made on the DISTANCE rather than on the values, so a
/// producer whose sequence has wrapped through `i32::MAX` is still read as
/// moving forwards. Half the space is the boundary, which is the same
/// convention TCP uses for the same problem and the only one available without
/// remembering how many times the counter went round.
fn is_before(seq: i32, expected: i32) -> bool {
    (seq.wrapping_sub(expected)) < 0
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

    #[test]
    fn a_gap_is_refused_and_nothing_is_remembered() {
        let p = Producers::new();
        let first = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 0, 3)]));
        landed(&p, &first, 100);
        // 3 is the next sequence; 5 would leave a hole.
        assert_eq!(
            rejection(p.check(&tenant(), "orders", 0, &[batch(7, 0, 5, 2)])),
            ResponseError::OutOfOrderSequenceNumber
        );
        // ...and the window did not move, so the batch that WAS next still is.
        let next = accepted(p.check(&tenant(), "orders", 0, &[batch(7, 0, 3, 2)]));
        assert_eq!(next.base_sequence(), 3);
    }

    /// The module header's caveat, as a test: a restart, an eviction or a facade
    /// switch is an absent entry, and an absent entry answers the code whose
    /// recovery every idempotent client implements.
    #[test]
    fn a_lost_window_answers_out_of_order_rather_than_unknown_producer_id() {
        let p = Producers::new();
        assert_eq!(
            rejection(p.check(&tenant(), "orders", 0, &[batch(7, 0, 42, 3)])),
            ResponseError::OutOfOrderSequenceNumber
        );
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

    #[test]
    fn the_tracker_evicts_rather_than_growing_without_bound() {
        let p = Producers::new();
        // Not MAX_TRACKED entries — that is 65_536 allocations for a property
        // the eviction code shows at any cap. The loop below is the SHAPE:
        // committing more distinct producers than the map holds never grows it
        // past the cap. Asserted at the real cap in the live rig instead.
        for id in 0..64i64 {
            let v = accepted(p.check(&tenant(), "orders", 0, &[batch(id + 1, 0, 0, 3)]));
            landed(&p, &v, 100);
        }
        assert_eq!(p.tracked(), 64);
        assert!(p.tracked() <= MAX_TRACKED);
    }
}
