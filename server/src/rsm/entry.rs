//! The entry: header, commands, outcomes, request ids (PLAN_RAFT.md §5.1,
//! §5.3, §5.4).
//!
//! One Raft log entry is one PLANNING CYCLE: the effects of a batch of
//! commands, in apply order, plus one [`CommandRecord`] per command with the
//! compact answer data (the [`Outcome`]) the receiver renders its reply from.
//! Nothing else goes in. Apply executes the effects in order and records
//! `request_id → (now, outcome)` for every command (D6, §5.4).
//!
//! # Wire form
//!
//! ```text
//! body_len:u32 | xxh3_64(body):u64 | body[body_len]
//!
//! body = format:u16
//!      | kinds_version:u32
//!      | now_us:i64
//!      | pid_base:u64
//!      | kv_version_base:u64
//!      | command_count:u32 | commands…
//!      | effect_count:u32  | effects…
//!
//! command = request_id[16] | first_effect:u32 | effect_count:u32 | outcome
//! outcome = tag:u16 | version:u16 | body_len:u32 | body[body_len]
//! ```
//!
//! An outcome's `version` is drawn from the SAME one catalogue sequence as the
//! effect kinds ([`super::effect`]), and the entry header's `kinds_version`
//! covers both ([`catalogue_version_of`]). It has to: an outcome is the answer
//! a retried command is served from (D6, I6), so a node that would read a new
//! outcome body as the old shape must stop (I16) and a new shape must wait for
//! the cluster version that admits it (D20) — exactly like a new effect kind.
//!
//! `format` is INSIDE the checksummed body on purpose: a bit flip in it is
//! then a checksum failure (a torn tail) rather than a format this build would
//! try to interpret. Effects carry their own `kind | version | len` header
//! ([`super::effect::write_effect`]); one xxh3 covers the whole body.
//!
//! # The header fields
//!
//! - `now_us` — the planner's stamp (D5). Apply has no clock; every absolute
//!   time in the effects was computed from this value, and apply sets
//!   `meta.last_now_us` from it so time never runs backwards across leaders
//!   (I5).
//! - `pid_base` — `meta.next_pid` when the cycle was planned. A partition
//!   created by the n-th `PartitionCreate` of the entry has
//!   `pid = pid_base + n`. Apply ASSERTS the base equals `meta` before
//!   advancing it; a mismatch is fatal (I18). That assertion is only half of
//!   I18 — it compares two numbers that a duplicate assignment leaves equal —
//!   so [`Entry::validate`] checks the other half here, where the effects are:
//!   the n-th creation must really carry `pid_base + n`.
//! - `kv_version_base` — the same contract for KV versions, by ordinal of the
//!   versioned write, checked the same way. The log index cannot stand in for
//!   either: outcomes carrying versions are fixed at planning, before the
//!   library assigns an index, and blank and membership entries interleave.
//! - `kinds_version` — the highest effect catalogue version used inside
//!   (§5.3). A node whose [`super::effect::SUPPORTED_KINDS_VERSION`] is lower
//!   stops rather than skipping an effect it cannot read (I16).

use super::effect::{
    checksum, decode_effect, kinds_version_of, write_effect, Assigns, CodecError, Effect, Pid,
    Reader, Writer, MAX_BODY_LEN,
};

/// The one entry layout this build writes.
pub const ENTRY_FORMAT: u16 = 1;

/// `body_len:u32 | checksum:u64`.
pub const ENTRY_HEADER_LEN: usize = 4 + 8;

/// The default of `QUEEN_RAFT_ENTRY_MAX_BYTES` (§5.1, Appendix H): the largest
/// PLANNED command the planner will accept before answering 413. It is not a
/// codec limit — [`MAX_BODY_LEN`] is — and it is quoted here so the batcher
/// and the planner cannot drift apart on the number.
pub const ENTRY_MAX_BYTES_DEFAULT: usize = 96 * 1024 * 1024;

/// The default of `QUEEN_RAFT_BATCH_MAX_BYTES` (§5.1): where a cycle is cut.
pub const BATCH_MAX_BYTES_DEFAULT: usize = 4 * 1024 * 1024;

/// The default of `QUEEN_RAFT_BATCH_MAX_CMDS` (§5.1).
pub const BATCH_MAX_CMDS_DEFAULT: usize = 4096;

/// The 16-byte id the RECEIVER mints once per command and reuses for every
/// forwarding retry (D6). A hit in the committed `request_ids` keyspace
/// returns the recorded outcome and plans nothing (I6).
pub type RequestId = [u8; 16];

// ---------------------------------------------------------------------------
// Outcomes (§5.4)
// ---------------------------------------------------------------------------

/// Outcome tags. Permanent, like effect kind ids.
mod tag {
    pub const EMPTY: u16 = 0;
    pub const PUSH: u16 = 1;
    pub const POP: u16 = 2;
    pub const ACK: u16 = 3;
    pub const RENEW: u16 = 4;
    pub const DLQ_HEAD: u16 = 5;
    // 6..=0xEFFF: reserved for the typed outcomes of phase 2 (transaction, KV,
    // timers, streams, admin).
    /// The first tag a [`super::Placeholder`] may carry.
    pub const PLACEHOLDER_MIN: u16 = 0xF000;
}

/// Which outcome tags this build knows at all (§5.4): the typed ones it has a
/// decoder for, and the whole reserved range, whose bodies are a planner's
/// private encoding.
///
/// A tag outside both is [`CodecError::UnknownOutcome`] — fatal like an
/// unknown effect kind (I16), and named sharply so the log line says which
/// number arrived.
fn outcome_tag_is_known(tag: u16) -> bool {
    match tag {
        tag::EMPTY | tag::PUSH | tag::POP | tag::ACK | tag::RENEW | tag::DLQ_HEAD => true,
        // 6..=0xEFFF are reserved for the typed outcomes of phase 2 and do not
        // exist yet; the placeholder range does.
        other => other >= tag::PLACEHOLDER_MIN,
    }
}

/// Which `(tag, version)` pairs this build's outcome decoder knows (§5.3).
///
/// Not "≤ [`SUPPORTED_KINDS_VERSION`]": versions come from the one catalogue
/// sequence the effects use, so a build that supports catalogue version 3 may
/// still know only version 1 of a given outcome. An outcome at any other
/// version is [`CodecError::UnknownVersion`] — fatal, never guessed at (I16),
/// and that holds for the RESERVED range too: a placeholder body is a private
/// encoding, and reading a later shape as this one would answer a retried
/// command from bytes this node never understood.
///
/// Every outcome this build knows is at catalogue version 1. The match is on
/// `tag`, not a single constant, so a tag that gains a second shape lists the
/// versions whose decoder is compiled in beside the new one, one tag at a time.
fn outcome_version_is_known(tag: u16, version: u16) -> bool {
    match tag {
        tag::EMPTY | tag::PUSH | tag::POP | tag::ACK | tag::RENEW | tag::DLQ_HEAD => {
            version == super::effect::VERSION_1
        }
        other if other >= tag::PLACEHOLDER_MIN => version == super::effect::VERSION_1,
        // A tag this build does not know has no version it knows either; the
        // caller reports the tag first, so this arm is the belt to that brace.
        _ => false,
    }
}

/// The compact answer data of one command (§5.4). Payload bytes are NEVER
/// stored in an outcome: a pop's outcome carries the claim, and the receiver
/// reads the bytes from its own files once it has applied the entry (D7).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The command changed state and needs no data in its answer (an admin
    /// delete, a flag set, a leader-loop step).
    Empty,
    /// Per-item push verdicts, in input order (003).
    Push(PushOutcome),
    /// The claims a pop took (004).
    Pop(PopOutcome),
    /// Per-target ack results, in input order (005).
    Ack(AckOutcome),
    /// `log_renew_lease_v1` (005).
    Renew(RenewOutcome),
    /// `log_dlq_head_v1` (005).
    DlqHead(DlqHeadOutcome),
    /// The generic placeholder for the commands phase 1 does not plan
    /// (transactions, KV, timers, streams, admin). The planner that owns a
    /// surface replaces its uses with a typed variant at a tag below
    /// [`tag::PLACEHOLDER_MIN`] and bumps the outcome's version; until then the
    /// body is that planner's private encoding and nothing else reads it.
    Placeholder(Placeholder),
}

/// The contents of an [`Outcome::Placeholder`]: a tag in the reserved range,
/// the catalogue version the body was minted at, and bytes only its own
/// planner reads.
///
/// The fields are private and [`Placeholder::new`] is the only way in. A
/// placeholder carrying a TYPED tag is a value this build can write into the
/// log and can never read back — the decoder would read its private body as a
/// pop, an ack or a push — and a placeholder at an unknown version is a body
/// nothing here can honour. Both used to be a `debug_assert!`, which is
/// nothing at all in a release build.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Placeholder {
    tag: u16,
    version: u16,
    body: Vec<u8>,
}

impl Placeholder {
    /// Refuses a tag below [`tag::PLACEHOLDER_MIN`] and a version this build
    /// does not know.
    pub fn new(tag: u16, version: u16, body: Vec<u8>) -> Result<Placeholder, CodecError> {
        if tag < tag::PLACEHOLDER_MIN {
            return Err(CodecError::Layout(
                "a placeholder outcome must carry a tag in the reserved range",
            ));
        }
        if !outcome_version_is_known(tag, version) {
            return Err(CodecError::UnknownVersion { kind: tag, version });
        }
        Ok(Placeholder { tag, version, body })
    }

    /// A placeholder at a version this build does NOT know, for the tests that
    /// need one: the gate that keeps a later shape off an older node cannot be
    /// exercised with values that build can mint. Never compiled into the
    /// product.
    #[cfg(test)]
    pub(crate) fn at_version_for_tests(tag: u16, version: u16, body: Vec<u8>) -> Placeholder {
        Placeholder { tag, version, body }
    }

    pub fn tag(&self) -> u16 {
        self.tag
    }
    pub fn version(&self) -> u16 {
        self.version
    }
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

/// One item of a push (003). A duplicate returns the ORIGINAL offset and
/// writes nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PushVerdict {
    Created {
        pid: Pid,
        offset: u64,
        created_at_us: i64,
    },
    Duplicate {
        pid: Pid,
        offset: u64,
    },
    /// A per-item refusal (the push answers 201 with a per-item status, so an
    /// item can fail inside a successful request).
    Refused {
        code: String,
        message: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct PushOutcome {
    /// In INPUT order, one per item the client sent.
    pub items: Vec<PushVerdict>,
}

/// One claimed batch (004). The receiver reads the payloads itself once its
/// applied index has reached the claim (§7.5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PopClaim {
    pub pid: Pid,
    /// Inclusive range `(committed, batch_end]` of the claim.
    pub start_offset: u64,
    pub end_offset: u64,
    /// The worker holding the lease. On today's wire this IS `leaseId`
    /// (handlers/data.rs ≈1050), so the claim mints no second identifier; if
    /// one is ever minted it arrives as a new outcome version.
    pub worker: String,
    /// `None` = an autoAck pop, which advances `committed` and holds no lease.
    pub lease_expires_at_us: Option<i64>,
    /// 1 on first delivery; redelivery keeps the first offset and counts up.
    pub delivery_attempt: u32,
    /// The claim was a CONFLATING one (newest visible frame per key).
    pub conflated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct PopOutcome {
    pub claims: Vec<PopClaim>,
}

/// One (partition, group) target of an ack (005).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AckResult {
    pub pid: Pid,
    /// The cursor after the ack.
    pub committed: i64,
    pub acked: u32,
    pub conflated: u32,
    /// How many of the acked messages were filed as dead letters.
    pub dlq: u32,
    pub lease_released: bool,
    pub batch_retry_count: u32,
    /// Hashes that resolved to nothing to do, and hashes that resolved BELOW
    /// the cursor: the `noopHashes` / `staleHashes` vocabulary of 005. They
    /// are the xxh3_128 hashes, not the transaction ids — the receiver holds
    /// the request and maps them back, so the entry never carries client
    /// strings it already has.
    pub noop_hashes: Vec<[u8; 16]>,
    pub stale_hashes: Vec<[u8; 16]>,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct AckOutcome {
    /// In INPUT order.
    pub results: Vec<AckResult>,
}

/// `log_renew_lease_v1`: every live lease of a worker is renewed and the
/// answer carries the MIN expiry (005).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct RenewOutcome {
    pub renewed: u32,
    pub min_expires_at_us: Option<i64>,
}

/// `log_dlq_head_v1`: the head frame is filed, the cursor advances past it and
/// the lease is released (005).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DlqHeadOutcome {
    pub pid: Pid,
    pub dlq_id: [u8; 16],
    /// The offset filed; `-1` never appears here (that is the timers' DLQ).
    pub offset: i64,
    pub committed: i64,
    pub lease_released: bool,
}

impl Outcome {
    fn tag(&self) -> u16 {
        match self {
            Outcome::Empty => tag::EMPTY,
            Outcome::Push(_) => tag::PUSH,
            Outcome::Pop(_) => tag::POP,
            Outcome::Ack(_) => tag::ACK,
            Outcome::Renew(_) => tag::RENEW,
            Outcome::DlqHead(_) => tag::DLQ_HEAD,
            Outcome::Placeholder(p) => p.tag(),
        }
    }

    /// The outcome catalogue version, on the same one-sequence rule as effect
    /// kinds (see [`super::effect`]).
    ///
    /// It is part of what the entry header's `kinds_version` must cover
    /// ([`catalogue_version_of`]), so a new outcome shape is gated by the
    /// cluster version exactly like a new effect kind (D20).
    ///
    /// Exhaustive, like [`super::effect::Effect::version`]: a wildcard arm
    /// would mint a phase-2 outcome at version 1 by default and walk it past
    /// that gate.
    pub fn version(&self) -> u16 {
        match self {
            Outcome::Empty
            | Outcome::Push(_)
            | Outcome::Pop(_)
            | Outcome::Ack(_)
            | Outcome::Renew(_)
            | Outcome::DlqHead(_) => super::effect::VERSION_1,
            Outcome::Placeholder(p) => p.version(),
        }
    }

    /// The outcome ON ITS OWN, framed exactly as it is framed inside an entry
    /// (`tag:u16 | version:u16 | body_len:u32 | body`).
    ///
    /// Added by WP-1.2: apply records `request_id → (now, outcome)` in the
    /// `request_ids` keyspace (D6, §5.4), and that row holds one outcome
    /// outside any entry. The bytes are the entry's bytes, so a row written by
    /// one build and read by another is governed by the same catalogue gate
    /// (I16): [`Outcome::decode`] refuses an unknown tag or version.
    pub fn encode(&self) -> Vec<u8> {
        let body = self.encode_body();
        let mut out = Vec::with_capacity(8 + body.len());
        out.extend_from_slice(&self.tag().to_le_bytes());
        out.extend_from_slice(&self.version().to_le_bytes());
        out.extend_from_slice(&(body.len() as u32).to_le_bytes());
        out.extend_from_slice(&body);
        out
    }

    /// The inverse of [`Outcome::encode`]. Trailing bytes are a layout error,
    /// not something to ignore.
    pub fn decode(b: &[u8]) -> Result<Outcome, CodecError> {
        let mut r = Reader::new(b);
        let tag_id = r.u16("outcome tag")?;
        let version = r.u16("outcome version")?;
        let len = r.u32("outcome len")? as usize;
        if len != r.remaining() {
            return Err(CodecError::Layout("outcome len does not fill the row"));
        }
        let out = Outcome::decode_body(tag_id, version, &r.rest()[..len])?;
        Ok(out)
    }

    fn encode_body(&self) -> Vec<u8> {
        let mut w = Writer::with_capacity(64);
        match self {
            Outcome::Empty => {}
            Outcome::Push(p) => {
                w.u32(p.items.len() as u32);
                for it in &p.items {
                    match it {
                        PushVerdict::Created {
                            pid,
                            offset,
                            created_at_us,
                        } => {
                            w.u8(0);
                            w.u64(*pid);
                            w.u64(*offset);
                            w.i64(*created_at_us);
                        }
                        PushVerdict::Duplicate { pid, offset } => {
                            w.u8(1);
                            w.u64(*pid);
                            w.u64(*offset);
                        }
                        PushVerdict::Refused { code, message } => {
                            w.u8(2);
                            w.str(code);
                            w.str(message);
                        }
                    }
                }
            }
            Outcome::Pop(p) => {
                w.u32(p.claims.len() as u32);
                for c in &p.claims {
                    w.u64(c.pid);
                    w.u64(c.start_offset);
                    w.u64(c.end_offset);
                    w.str(&c.worker);
                    w.opt_i64(c.lease_expires_at_us);
                    w.u32(c.delivery_attempt);
                    w.bool(c.conflated);
                }
            }
            Outcome::Ack(a) => {
                w.u32(a.results.len() as u32);
                for r in &a.results {
                    w.u64(r.pid);
                    w.i64(r.committed);
                    w.u32(r.acked);
                    w.u32(r.conflated);
                    w.u32(r.dlq);
                    w.bool(r.lease_released);
                    w.u32(r.batch_retry_count);
                    w.vec_bytes16(&r.noop_hashes);
                    w.vec_bytes16(&r.stale_hashes);
                }
            }
            Outcome::Renew(r) => {
                w.u32(r.renewed);
                w.opt_i64(r.min_expires_at_us);
            }
            Outcome::DlqHead(d) => {
                w.u64(d.pid);
                w.bytes16(&d.dlq_id);
                w.i64(d.offset);
                w.i64(d.committed);
                w.bool(d.lease_released);
            }
            Outcome::Placeholder(p) => {
                return p.body().to_vec();
            }
        }
        w.into_inner()
    }

    fn decode_body(tag_id: u16, version: u16, body: &[u8]) -> Result<Outcome, CodecError> {
        // A tag from a catalogue this build does not have, named as itself.
        if !outcome_tag_is_known(tag_id) {
            return Err(CodecError::UnknownOutcome(tag_id));
        }
        // Then the version gate, and it comes BEFORE the reserved-range
        // shortcut below. A placeholder decoded at the wrong version is not a
        // harmless opaque blob: apply records it under the command's request
        // id, and the retry of that command (D6, I6) is then answered from a
        // body this node never understood. I16 says stop, not interpret.
        if !outcome_version_is_known(tag_id, version) {
            return Err(CodecError::UnknownVersion {
                kind: tag_id,
                version,
            });
        }
        if tag_id >= tag::PLACEHOLDER_MIN {
            return Ok(Outcome::Placeholder(Placeholder {
                tag: tag_id,
                version,
                body: body.to_vec(),
            }));
        }
        let mut r = Reader::new(body);
        let out = match tag_id {
            tag::EMPTY => Outcome::Empty,
            tag::PUSH => {
                let n = r.u32("push items")?;
                let mut items = Vec::with_capacity(r.cap_hint(n, 1));
                for _ in 0..n {
                    items.push(match r.u8("push verdict")? {
                        0 => PushVerdict::Created {
                            pid: r.u64("pid")?,
                            offset: r.u64("offset")?,
                            created_at_us: r.i64("created_at")?,
                        },
                        1 => PushVerdict::Duplicate {
                            pid: r.u64("pid")?,
                            offset: r.u64("offset")?,
                        },
                        2 => PushVerdict::Refused {
                            code: r.str("code")?,
                            message: r.str("message")?,
                        },
                        _ => return Err(CodecError::Field("push verdict")),
                    });
                }
                Outcome::Push(PushOutcome { items })
            }
            tag::POP => {
                let n = r.u32("pop claims")?;
                let mut claims = Vec::with_capacity(r.cap_hint(n, 33));
                for _ in 0..n {
                    claims.push(PopClaim {
                        pid: r.u64("pid")?,
                        start_offset: r.u64("start_offset")?,
                        end_offset: r.u64("end_offset")?,
                        worker: r.str("worker")?,
                        lease_expires_at_us: r.opt_i64("lease_expires_at")?,
                        delivery_attempt: r.u32("delivery_attempt")?,
                        conflated: r.bool("conflated")?,
                    });
                }
                Outcome::Pop(PopOutcome { claims })
            }
            tag::ACK => {
                let n = r.u32("ack results")?;
                let mut results = Vec::with_capacity(r.cap_hint(n, 38));
                for _ in 0..n {
                    results.push(AckResult {
                        pid: r.u64("pid")?,
                        committed: r.i64("committed")?,
                        acked: r.u32("acked")?,
                        conflated: r.u32("conflated")?,
                        dlq: r.u32("dlq")?,
                        lease_released: r.bool("lease_released")?,
                        batch_retry_count: r.u32("batch_retry_count")?,
                        noop_hashes: r.vec_bytes16("noop_hashes")?,
                        stale_hashes: r.vec_bytes16("stale_hashes")?,
                    });
                }
                Outcome::Ack(AckOutcome { results })
            }
            tag::RENEW => Outcome::Renew(RenewOutcome {
                renewed: r.u32("renewed")?,
                min_expires_at_us: r.opt_i64("min_expires_at")?,
            }),
            tag::DLQ_HEAD => Outcome::DlqHead(DlqHeadOutcome {
                pid: r.u64("pid")?,
                dlq_id: r.bytes16("dlq_id")?,
                offset: r.i64("offset")?,
                committed: r.i64("committed")?,
                lease_released: r.bool("lease_released")?,
            }),
            other => return Err(CodecError::UnknownOutcome(other)),
        };
        if !r.done() {
            return Err(CodecError::Field("outcome trailing bytes"));
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Entry
// ---------------------------------------------------------------------------

/// One planned command inside an entry (§5.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandRecord {
    pub request_id: RequestId,
    /// Index into [`Entry::effects`] of this command's first effect.
    pub first_effect: u32,
    /// How many effects it planned. ALWAYS ≥ 1: a command that plans nothing
    /// is never logged and its id is never recorded (§5.4) — logging empty
    /// pops and refusals would make cost follow the poll rate, against G-3.
    pub effect_count: u32,
    pub outcome: Outcome,
}

/// One Raft log entry: a batch of commands' effects plus the header (§5.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub format: u16,
    pub kinds_version: u32,
    pub now_us: i64,
    pub pid_base: u64,
    pub kv_version_base: u64,
    /// In PLAN order, one per logged command.
    pub commands: Vec<CommandRecord>,
    /// In APPLY order.
    pub effects: Vec<Effect>,
}

impl Entry {
    /// An empty entry stamped for one planning cycle.
    pub fn new(now_us: i64, pid_base: u64, kv_version_base: u64) -> Entry {
        Entry {
            format: ENTRY_FORMAT,
            kinds_version: 0,
            now_us,
            pid_base,
            kv_version_base,
            commands: Vec::new(),
            effects: Vec::new(),
        }
    }

    /// Append a planned command with its effects, keeping `kinds_version` and
    /// the command's span right. The ONLY way the planner should build an
    /// entry: spans stay contiguous and cover every effect by construction.
    ///
    /// Refuses a command with no effects (§5.4).
    pub fn add_command(
        &mut self,
        request_id: RequestId,
        outcome: Outcome,
        effects: Vec<Effect>,
    ) -> Result<(), CodecError> {
        if effects.is_empty() {
            return Err(CodecError::Layout("command with no effects"));
        }
        let first_effect = self.effects.len() as u32;
        let effect_count = effects.len() as u32;
        // Effects AND the outcome: the header gates both (§5.3, D20).
        self.kinds_version = self
            .kinds_version
            .max(kinds_version_of(&effects))
            .max(outcome.version() as u32);
        self.effects.extend(effects);
        self.commands.push(CommandRecord {
            request_id,
            first_effect,
            effect_count,
            outcome,
        });
        Ok(())
    }

    /// The effects of one command.
    pub fn effects_of(&self, cmd: &CommandRecord) -> &[Effect] {
        let a = cmd.first_effect as usize;
        let b = a + cmd.effect_count as usize;
        &self.effects[a..b]
    }

    /// Layout checks that must hold for any entry this build will apply: a
    /// known format, a `kinds_version` this build supports and that covers
    /// what the entry carries (I16, D20), every span in bounds, non-empty and
    /// disjoint, every effect belonging to exactly one command, DISTINCT
    /// request ids, every planner-assigned counter equal to the header's base
    /// plus its ordinal (I18), and every effect internally consistent
    /// ([`Effect::check`]).
    ///
    /// COVERAGE is required, not merely produced by [`Entry::add_command`].
    /// §5.1 models every logged effect as one command's, and apply has two
    /// natural shapes — walk the effects, or walk the commands and their
    /// spans. An orphan effect makes those two disagree: the first applies it,
    /// the second silently skips it, and two nodes running different code
    /// paths diverge without a word. A leader loop that proposes effects of
    /// its own (retention, expiry, the chunked deletes of §5.2) mints a
    /// request id for its step like any other command; D6 then makes the step
    /// answerable and idempotent instead of anonymous.
    ///
    /// DISTINCT request ids, because §5.4 has apply insert
    /// `request_id → (now, outcome)` for every logged command: two commands
    /// sharing an id in one entry collapse to the last one's outcome, and the
    /// retry of the FIRST (D6 reuses the id across forwarding retries, so a
    /// same-cycle duplicate is what a `propose` timeout plus a retry looks
    /// like) is then answered with the second's — a pop answered with another
    /// pop's claim. The scan is over a sorted copy, so it is deterministic
    /// (I2). A surface that plans SEVERAL records for one client request —
    /// §8's streams cycle is one record per array element, because each
    /// element is its own atomic unit — mints one id per record for the same
    /// reason: sharing one would record only the last element's outcome and
    /// answer the retry of the whole cycle from it.
    ///
    /// I18, because the header exists to make it checkable and nothing else
    /// checks it: apply asserts the BASES equal `meta`, which they do, and
    /// then advances them by the count. Two `PartitionCreate`s carrying one
    /// pid make both partitions one partition on every node at once — the
    /// second queue's appends land in the first's segments and cursors, across
    /// tenants under a `Tenant` scope — and two `KvPut`s at one version let a
    /// stale `expect` win at the Kafka `qk:fence` and the S3 sink lease (§8,
    /// 024). It costs one pass over the effects.
    pub fn validate(&self) -> Result<(), CodecError> {
        if self.format != ENTRY_FORMAT {
            return Err(CodecError::UnknownFormat(self.format));
        }
        if self.kinds_version > super::effect::SUPPORTED_KINDS_VERSION {
            return Err(CodecError::UnknownCatalogue(self.kinds_version));
        }
        // A header that UNDERSTATES what it carries would walk an effect — or
        // an OUTCOME — past the cluster-version gate (I16, D20).
        if catalogue_version_of(&self.commands, &self.effects) > self.kinds_version {
            return Err(CodecError::Layout(
                "kinds_version below what the entry carries",
            ));
        }
        let n = self.effects.len();
        let mut covered = vec![false; n];
        for c in &self.commands {
            if c.effect_count == 0 {
                return Err(CodecError::Layout("command with no effects"));
            }
            let a = c.first_effect as usize;
            let b = a
                .checked_add(c.effect_count as usize)
                .ok_or(CodecError::Layout("command span overflows"))?;
            if b > n {
                return Err(CodecError::Layout("command span past the effects"));
            }
            for slot in covered.iter_mut().take(b).skip(a) {
                if *slot {
                    return Err(CodecError::Layout("command spans overlap"));
                }
                *slot = true;
            }
        }
        if covered.iter().any(|slot| !slot) {
            return Err(CodecError::Layout("an effect belongs to no command"));
        }
        if self.commands.len() > 1 {
            let mut ids: Vec<&RequestId> = self.commands.iter().map(|c| &c.request_id).collect();
            ids.sort_unstable();
            if ids.windows(2).any(|w| w[0] == w[1]) {
                return Err(CodecError::Layout("two commands share a request id"));
            }
        }
        // I18: the counters the planner handed out, against the bases this
        // entry's own header carries.
        let mut next_pid = self.pid_base;
        let mut next_kv_version = self.kv_version_base;
        for eff in &self.effects {
            eff.check()?;
            match eff.assigns() {
                Assigns::Nothing => {}
                Assigns::Pid(pid) => {
                    if pid != next_pid {
                        return Err(CodecError::Layout(
                            "a created pid is not pid_base + its ordinal",
                        ));
                    }
                    next_pid = next_pid
                        .checked_add(1)
                        .ok_or(CodecError::Layout("pid_base + ordinal overflows"))?;
                }
                Assigns::KvVersion(version) => {
                    if version != next_kv_version {
                        return Err(CodecError::Layout(
                            "a KV version is not kv_version_base + its ordinal",
                        ));
                    }
                    next_kv_version = next_kv_version
                        .checked_add(1)
                        .ok_or(CodecError::Layout("kv_version_base + ordinal overflows"))?;
                }
            }
        }
        Ok(())
    }

    /// The exact encoded size: what the batcher compares against
    /// `QUEEN_RAFT_BATCH_MAX_BYTES` and the planner against
    /// `QUEEN_RAFT_ENTRY_MAX_BYTES` (§5.1).
    ///
    /// It MEASURES by encoding, so it is exact and it is not free. The cycle
    /// of §7.1 encodes once and proposes those bytes; this is for tests and
    /// for the places that need the number without the buffer. It encodes
    /// through [`encode_entry`], so an entry that could not be proposed has no
    /// size either.
    pub fn encoded_len(&self) -> Result<usize, CodecError> {
        Ok(encode_entry(self)?.len())
    }
}

/// The highest catalogue version an entry's contents use: its EFFECTS and its
/// OUTCOMES (§5.1, §5.3). This is the entry header's `kinds_version`.
///
/// Outcomes count. Without them an entry whose only novelty is a new outcome
/// shape would report the old version, D20's gate would let an old voter apply
/// it, and that voter would read the new body as the old shape, record it
/// under the command's request id and answer the retry (D6, I6) from bytes it
/// never understood — a wrong answer to a client instead of a node that stops.
pub fn catalogue_version_of(commands: &[CommandRecord], effects: &[Effect]) -> u32 {
    commands
        .iter()
        .map(|c| c.outcome.version() as u32)
        .max()
        .unwrap_or(0)
        .max(kinds_version_of(effects))
}

/// Encode an entry, framed and checksummed — after [`Entry::validate`].
///
/// The check is here, not only in the planner, because these bytes are what
/// gets PROPOSED. An entry with overlapping spans, an orphan effect, two
/// commands sharing a request id, a counter assigned twice (I18), a
/// `kinds_version` below what it carries or a body above [`MAX_BODY_LEN`]
/// would otherwise be committed by the whole cluster and then refused by every
/// node that read it back — or, worse for the two middle ones, APPLIED
/// identically and wrongly by all of them, which no later check can undo. The
/// size bound is the same door on
/// length — `QUEEN_RAFT_ENTRY_MAX_BYTES` is a tunable (its default is
/// [`ENTRY_MAX_BYTES_DEFAULT`], 96 MiB), and raising it past the codec's limit
/// must fail here rather than produce a value the decoder calls damage.
pub fn encode_entry(e: &Entry) -> Result<Vec<u8>, CodecError> {
    e.validate()?;
    let body = encode_entry_body(e);
    if body.len() > MAX_BODY_LEN as usize {
        return Err(CodecError::Layout("entry body above the codec's limit"));
    }
    Ok(frame_entry_body(&body))
}

/// The raw serializer, with no checks at all: for the tests that must FORGE
/// bytes a valid encoder cannot produce, so the decoder's refusals can be
/// exercised. Never compiled into the product — the product path is
/// [`encode_entry`].
#[cfg(test)]
pub(crate) fn encode_entry_unchecked(e: &Entry) -> Vec<u8> {
    frame_entry_body(&encode_entry_body(e))
}

/// `body_len:u32 | xxh3_64(body):u64 | body`. The caller has checked that the
/// body fits [`MAX_BODY_LEN`]; the length prefix is a `u32`.
fn frame_entry_body(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(ENTRY_HEADER_LEN + body.len());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&checksum(body).to_le_bytes());
    out.extend_from_slice(body);
    out
}

fn encode_entry_body(e: &Entry) -> Vec<u8> {
    let mut body = Vec::with_capacity(64 + e.effects.len() * 64);
    body.extend_from_slice(&e.format.to_le_bytes());
    body.extend_from_slice(&e.kinds_version.to_le_bytes());
    body.extend_from_slice(&e.now_us.to_le_bytes());
    body.extend_from_slice(&e.pid_base.to_le_bytes());
    body.extend_from_slice(&e.kv_version_base.to_le_bytes());

    body.extend_from_slice(&(e.commands.len() as u32).to_le_bytes());
    for c in &e.commands {
        body.extend_from_slice(&c.request_id);
        body.extend_from_slice(&c.first_effect.to_le_bytes());
        body.extend_from_slice(&c.effect_count.to_le_bytes());
        let ob = c.outcome.encode_body();
        body.extend_from_slice(&c.outcome.tag().to_le_bytes());
        body.extend_from_slice(&c.outcome.version().to_le_bytes());
        body.extend_from_slice(&(ob.len() as u32).to_le_bytes());
        body.extend_from_slice(&ob);
    }

    body.extend_from_slice(&(e.effects.len() as u32).to_le_bytes());
    for eff in &e.effects {
        write_effect(&mut body, eff);
    }

    body
}

/// The framed header of an entry. `None` = not a header (an absurd length):
/// the caller treats it as the torn tail of a file.
#[derive(Clone, Copy, Debug)]
pub struct EntryHeader {
    pub body_len: u32,
    pub checksum: u64,
}

pub fn parse_entry_header(b: &[u8]) -> Option<EntryHeader> {
    if b.len() < ENTRY_HEADER_LEN {
        return None;
    }
    let body_len = u32::from_le_bytes(b[0..4].try_into().unwrap());
    if body_len > MAX_BODY_LEN {
        return None;
    }
    let checksum = u64::from_le_bytes(b[4..12].try_into().unwrap());
    Some(EntryHeader { body_len, checksum })
}

/// Decode one framed entry from the head of `b`; returns it and the bytes
/// consumed. Errors, never panics, on anything the bytes can be.
///
/// Where the error is raised decides what the caller may do with the record,
/// so the frame is the boundary:
///
/// - BEFORE the checksum — an absurd length prefix ([`CodecError::Header`]), a
///   buffer that ends early ([`CodecError::Truncated`]), a mismatch
///   ([`CodecError::Checksum`]) — the bytes are damage or a torn tail, and
///   §11.5 may truncate the file there.
/// - AFTER it, the bytes are exactly what the leader wrote and what a quorum
///   committed. Nothing about them is this node's to repair, so every error
///   from the body goes through [`CodecError::after_checksum`] and comes back
///   fatal: an unknown kind, version, outcome, format or catalogue (I16), or
///   [`CodecError::Malformed`] for everything else.
pub fn decode_entry_at(b: &[u8]) -> Result<(Entry, usize), CodecError> {
    let h = parse_entry_header(b).ok_or(CodecError::Header)?;
    let total = ENTRY_HEADER_LEN + h.body_len as usize;
    if b.len() < total {
        return Err(CodecError::Truncated);
    }
    let body = &b[ENTRY_HEADER_LEN..total];
    if checksum(body) != h.checksum {
        return Err(CodecError::Checksum);
    }
    let e = decode_verified_body(body).map_err(CodecError::after_checksum)?;
    Ok((e, total))
}

/// The body of an entry whose frame has already been verified. Every error it
/// returns is re-classified by its caller; nothing else may call it.
fn decode_verified_body(body: &[u8]) -> Result<Entry, CodecError> {
    let mut r = Reader::new(body);
    let format = r.u16("format")?;
    if format != ENTRY_FORMAT {
        return Err(CodecError::UnknownFormat(format));
    }
    let kinds_version = r.u32("kinds_version")?;
    // The catalogue gate, BEFORE any body is read (§12.8, I16): an entry from
    // a catalogue this build does not have is refused whole. Reading its
    // fields first would only decide, field by field, which of them this
    // binary happens to still understand.
    if kinds_version > super::effect::SUPPORTED_KINDS_VERSION {
        return Err(CodecError::UnknownCatalogue(kinds_version));
    }
    let now_us = r.i64("now_us")?;
    let pid_base = r.u64("pid_base")?;
    let kv_version_base = r.u64("kv_version_base")?;

    let cmd_count = r.u32("command_count")?;
    // 16 + 4 + 4 + 8 = the smallest a command can encode to.
    let mut commands = Vec::with_capacity(r.cap_hint(cmd_count, 32));
    for _ in 0..cmd_count {
        let request_id = r.bytes16("request_id")?;
        let first_effect = r.u32("first_effect")?;
        let effect_count = r.u32("effect_count")?;
        let tag_id = r.u16("outcome tag")?;
        let version = r.u16("outcome version")?;
        let len = r.u32("outcome len")? as usize;
        if len > r.remaining() {
            return Err(CodecError::Field("outcome len"));
        }
        let outcome = Outcome::decode_body(tag_id, version, &r.rest()[..len])?;
        r.skip(len, "outcome body")?;
        commands.push(CommandRecord {
            request_id,
            first_effect,
            effect_count,
            outcome,
        });
    }

    let eff_count = r.u32("effect_count")?;
    // 2 + 2 + 4 = the smallest an effect can encode to (an empty body).
    let mut effects = Vec::with_capacity(r.cap_hint(eff_count, 8));
    for _ in 0..eff_count {
        let (eff, used) = decode_effect(r.rest())?;
        r.skip(used, "effect")?;
        effects.push(eff);
    }

    if !r.done() {
        return Err(CodecError::Field("entry trailing bytes"));
    }

    let e = Entry {
        format,
        kinds_version,
        now_us,
        pid_base,
        kv_version_base,
        commands,
        effects,
    };
    e.validate()?;
    Ok(e)
}

/// Decode exactly one framed entry from `b`, which must hold nothing else.
pub fn decode_entry(b: &[u8]) -> Result<Entry, CodecError> {
    let (e, used) = decode_entry_at(b)?;
    if used != b.len() {
        return Err(CodecError::Field("trailing bytes after the entry"));
    }
    Ok(e)
}
