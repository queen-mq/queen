//! Transactions (Phase B, `ALICE_PGLESS_NEWARCH.md` §4): push and ack bundled
//! ALL-OR-NOTHING into ONE command, so ONE entry carries every effect.
//!
//! Atomicity comes from Phase C for free: the writer writes that entry into
//! EVERY queue log it touches with its copy count, and recovery replays an
//! entry only when all its copies are present and identical — the
//! present-in-all commit rule (§4.1–4.3). A torn bundle is cut from every tail
//! and leaves no offset gap (its offsets were never applied).
//!
//! The consumption half of a bundle (`acks`, `positional_acks`, `positions`,
//! `requiredLeases`) is the consumption engine's ([`crate::rsm::consume`]): it
//! validates and reserves them on the leader BEFORE planning, and hands the
//! planner the cursor and DLQ rows they write as `extra_effects` and their
//! per-target results as `engine_acks`, with the acks stripped. The planner
//! plans what it owns — pushes, KV, timers, and the positions on partitions
//! that do not exist yet (allocating a partition is the planner's,
//! [`Planner::plan_positions`]) — and refuses a bundle that still carries acks
//! (they must never reach the planner).
//!
//! The planner side mirrors the SQL wire transaction: a pushed message that is
//! a DUPLICATE rolls the WHOLE bundle back (`QDUP`, HTTP 200 `success:false`),
//! and so does a lost KV `required` precondition. The overlay is restored on
//! refusal so a rolled-back bundle leaves no phantom state for later commands
//! of the same cycle.

use crate::rsm::effect::Effect;
use crate::rsm::entry::{
    AckOutcome, KvOpOutcome, KvOutcome, Outcome, Placeholder, PushOutcome, PushVerdict, RequestId,
};

use super::{
    AckPositionalCommand, AckTarget, KvOp, Overlay, Plan, Planned, Planner, PushCommand, Refusal,
};
use crate::rsm::store::Reads;

/// The placeholder tag of a transaction outcome (reserved range, §5.4).
pub const TXN_OUTCOME_TAG: u16 = 0xF001;

/// One transaction: every push group, ack target and KV op of the bundle.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct TxnCommand {
    pub request_id: RequestId,
    pub tenant: String,
    /// One per (queue, partition), items in bundle order (the receiver packs
    /// them exactly like a push).
    pub pushes: Vec<PushCommand>,
    /// One per (pid, group, worker), like a batch ack. The engine's
    /// (`txn_prepare`): stripped before planning.
    pub acks: Vec<AckTarget>,
    /// Positional source acknowledgements used by Streams cycles, so source
    /// ack, state and sink pushes remain one atomic entry. The engine's:
    /// stripped before planning.
    pub positional_acks: Vec<AckPositionalCommand>,
    /// The `kv` rider, validated with the wire's limits (`parse_ops(.., true, ..)`).
    pub kv: Vec<KvOp>,
    /// The `timers` rider (schedules and cancels, `parse_timer_ops`).
    pub timers: Vec<super::timers::TimerOp>,
    /// Deterministic riders that must commit with the transaction. Streams
    /// uses this for state cells; the consumption engine puts the cursor and
    /// DLQ rows of the bundle's acks and positions here. Keeping them on the
    /// transaction command is what makes source ack + sink append + state
    /// update one log entry.
    pub extra_effects: Vec<Effect>,
    /// DLQ replay is the one transaction-shaped operation where a duplicate
    /// destination is a successful no-op: the source row must remain in DLQ.
    /// Public transactions keep their all-or-nothing QDUP refusal.
    pub allow_duplicate: bool,
    /// The `positions` rider ([`super::positions`]): consumer-group positions
    /// set or forgotten, all-or-nothing with everything else in the bundle.
    /// The engine's, but for those on a partition that does not exist yet,
    /// which it leaves here for the planner to create.
    #[serde(default)]
    pub positions: Vec<super::positions::PositionOp>,
    /// The engine's per-target results for the bundle's `acks` and
    /// `positional_acks` (`TxnPart::acks` of `txn_prepare`), in their input
    /// order. The planner records them as the outcome's `acks`, so a retry of
    /// the request id reads what the engine answered.
    #[serde(default)]
    pub engine_acks: AckOutcome,
}

/// The decoded transaction outcome: per push group, per ack target.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TxnOutcome {
    pub pushes: Vec<PushOutcome>,
    pub acks: AckOutcome,
    /// The KV rider's verdicts, or the ONE lost `required` precondition that
    /// rolled the whole bundle back (then nothing was logged).
    pub kv: KvOutcome,
    /// The timers rider's results, as the timers wire renders them
    /// (`TimerOpResult::to_json`).
    pub timers: Vec<serde_json::Value>,
}

impl TxnOutcome {
    /// `u32 n | n × (u32 len | Outcome::Push bytes) | u32 len | Outcome::Ack bytes`
    /// — the typed codecs reused, inside the placeholder's private body.
    pub fn into_outcome(self) -> Result<Outcome, Refusal> {
        // A push written as runs is a version-7 outcome, so the placeholder
        // that carries it is minted at 7: the entry's catalogue version then
        // covers it (D20, I16), and a build that cannot read it stops.
        let version = if self.pushes.iter().any(|p| p.compact) {
            crate::rsm::effect::VERSION_7
        } else {
            crate::rsm::effect::VERSION_1
        };
        let mut body: Vec<u8> = Vec::new();
        body.extend_from_slice(&(self.pushes.len() as u32).to_le_bytes());
        for p in self.pushes {
            let b = Outcome::Push(p).encode();
            body.extend_from_slice(&(b.len() as u32).to_le_bytes());
            body.extend_from_slice(&b);
        }
        let a = Outcome::Ack(self.acks).encode();
        body.extend_from_slice(&(a.len() as u32).to_le_bytes());
        body.extend_from_slice(&a);
        let k = Outcome::Kv(self.kv).encode();
        body.extend_from_slice(&(k.len() as u32).to_le_bytes());
        body.extend_from_slice(&k);
        let t = serde_json::to_vec(&self.timers).unwrap_or_else(|_| b"[]".to_vec());
        body.extend_from_slice(&(t.len() as u32).to_le_bytes());
        body.extend_from_slice(&t);
        Placeholder::new(TXN_OUTCOME_TAG, version, body)
            .map(Outcome::Placeholder)
            .map_err(|e| Refusal::retry("internal", format!("txn outcome: {e:?}")))
    }

    pub fn from_outcome(o: &Outcome) -> Option<TxnOutcome> {
        let Outcome::Placeholder(p) = o else {
            return None;
        };
        if p.tag() != TXN_OUTCOME_TAG {
            return None;
        }
        let b = p.body();
        let mut at = 0usize;
        let u32_at = |at: &mut usize| -> Option<usize> {
            let v = u32::from_le_bytes(b.get(*at..*at + 4)?.try_into().ok()?) as usize;
            *at += 4;
            Some(v)
        };
        let n = u32_at(&mut at)?;
        let mut pushes = Vec::with_capacity(n);
        for _ in 0..n {
            let len = u32_at(&mut at)?;
            match Outcome::decode(b.get(at..at + len)?).ok()? {
                Outcome::Push(p) => pushes.push(p),
                _ => return None,
            }
            at += len;
        }
        let len = u32_at(&mut at)?;
        let acks = match Outcome::decode(b.get(at..at + len)?).ok()? {
            Outcome::Ack(a) => a,
            _ => return None,
        };
        at += len;
        let len = u32_at(&mut at)?;
        let kv = match Outcome::decode(b.get(at..at + len)?).ok()? {
            Outcome::Kv(k) => k,
            _ => return None,
        };
        at += len;
        let len = u32_at(&mut at)?;
        let timers: Vec<serde_json::Value> = serde_json::from_slice(b.get(at..at + len)?).ok()?;
        Some(TxnOutcome {
            pushes,
            acks,
            kv,
            timers,
        })
    }
}

/// One planning thread's share of a transaction whose keys have several homes
/// (the scheduler's second round, `batcher_scheduler.rs` S8): the pushes, the
/// KV ops and the timers whose keys live in its shards.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct TxnPart {
    /// Indexes into [`TxnCommand::pushes`], ascending.
    pub(crate) pushes: Vec<usize>,
    /// Indexes into [`TxnCommand::kv`], ascending.
    pub(crate) kv: Vec<usize>,
    /// The timers rider, whole: its keys share this one home.
    pub(crate) timers: bool,
    /// The share that also checks the command and folds its riders
    /// ([`TxnCommand::extra_effects`]): one of a transaction's shares.
    pub(crate) lead: bool,
}

/// What a planning thread planned of its share ([`Planner::plan_transaction_part`]).
#[derive(Debug, Default)]
pub(crate) struct TxnPartPlan {
    /// By index into [`TxnCommand::pushes`]: a push's effects (none when it
    /// wrote nothing) and its verdicts.
    pushes: Vec<(usize, Vec<Effect>, PushOutcome)>,
    /// By index into [`TxnCommand::kv`], in apply order: what
    /// [`super::kv::KvEach`] holds.
    kv: Vec<(usize, Option<Effect>, KvOpOutcome)>,
    /// The timers rider's effects and results.
    timers: Option<(Vec<Effect>, Vec<serde_json::Value>)>,
}

impl TxnPartPlan {
    /// The effects this share logs (the lead's riders are the command's own).
    pub(crate) fn effects(&self) -> impl Iterator<Item = &Effect> {
        self.pushes
            .iter()
            .flat_map(|p| p.1.iter())
            .chain(self.kv.iter().filter_map(|k| k.1.as_ref()))
            .chain(self.timers.iter().flat_map(|t| t.0.iter()))
    }

    /// How many effects planning this share folded into its overlay.
    pub(crate) fn folded(&self, cmd: &TxnCommand, part: &TxnPart) -> u64 {
        let riders = if part.lead {
            cmd.extra_effects.len()
        } else {
            0
        };
        (self.effects().count() + riders) as u64
    }
}

impl TxnCommand {
    /// The shares of a transaction put back together, when every one of them
    /// held: the effects in the order [`Planner::plan_transaction`] logs them
    /// (the pushes in order, the KV rider in apply order, the timers, the
    /// riders) and the outcome it answers. Empty effects are a transaction
    /// that logs nothing (`Plan::Empty`).
    pub(crate) fn assemble_parts(
        &self,
        parts: Vec<TxnPartPlan>,
    ) -> Result<(Vec<Effect>, Outcome), Refusal> {
        let missing = || Refusal::retry("internal", "a transaction's share was not planned");
        let mut pushes: Vec<Option<(Vec<Effect>, PushOutcome)>> =
            (0..self.pushes.len()).map(|_| None).collect();
        let mut kv: Vec<(usize, Option<Effect>, KvOpOutcome)> = Vec::new();
        let mut timers = None;
        for part in parts {
            for (k, effects, outcome) in part.pushes {
                *pushes.get_mut(k).ok_or_else(missing)? = Some((effects, outcome));
            }
            kv.extend(part.kv);
            if part.timers.is_some() {
                timers = part.timers;
            }
        }
        let mut effects = Vec::new();
        let mut out = TxnOutcome::default();
        for p in pushes {
            let (e, o) = p.ok_or_else(missing)?;
            effects.extend(e);
            out.pushes.push(o);
        }
        if !self.kv.is_empty() {
            // The shares' ops, back in the one order the call applies in.
            let mut rank = vec![0usize; self.kv.len()];
            for (r, i) in super::kv::apply_order(&self.kv).into_iter().enumerate() {
                rank[i] = r;
            }
            if kv.iter().any(|k| k.0 >= rank.len()) {
                return Err(missing());
            }
            kv.sort_by_key(|k| rank[k.0]);
            let mut results = vec![KvOpOutcome::Deferred; self.kv.len()];
            for (i, eff, verdict) in kv {
                effects.extend(eff);
                results[i] = verdict;
            }
            out.kv = KvOutcome {
                results,
                failed: None,
            };
        }
        if !self.timers.is_empty() {
            let (e, results) = timers.ok_or_else(missing)?;
            effects.extend(e);
            out.timers = results;
        }
        effects.extend(self.extra_effects.iter().cloned());
        out.acks = self.engine_acks.clone();
        Ok((effects, out.into_outcome()?))
    }
}

impl<'a, R: Reads + ?Sized> Planner<'a, R> {
    /// Plan `part` of `cmd`, a transaction whose keys have several homes, on
    /// the planning thread that owns the part's keys: what
    /// [`Planner::plan_transaction`] plans for those keys, folded into `ov`.
    /// `None` when the transaction does not commit as this share sees it (a
    /// refusal, a pushed message that is a duplicate, a lost `required`
    /// precondition): it is then planned whole by the single planner's loop,
    /// which answers what it answers. The caller holds the savepoint: a share
    /// takes none, and a share that did not hold leaves its folds for the
    /// caller to take back.
    ///
    /// A transaction's legs read only their own keys (a push its partition and
    /// its routes' targets, a KV op its key, a timer its key), so the shares
    /// planned apart are what the whole transaction plans:
    /// [`TxnCommand::assemble_parts`] puts them back in its order.
    pub(crate) fn plan_transaction_part(
        &self,
        ov: &mut Overlay,
        cmd: &TxnCommand,
        part: &TxnPart,
    ) -> Option<TxnPartPlan> {
        let mut plan = TxnPartPlan::default();
        if part.lead {
            if !cmd.acks.is_empty() || !cmd.positional_acks.is_empty() || !cmd.positions.is_empty()
            {
                return None;
            }
            self.check_partition_rows(ov, &cmd.extra_effects).ok()?;
            self.check_effect_keys(ov, &cmd.extra_effects).ok()?;
            check_streams_state_ops(&cmd.extra_effects).ok()?;
        }
        for &k in &part.pushes {
            let p = cmd.pushes.get(k)?;
            let (effects, o) = match self.plan_push(ov, p) {
                Ok(Plan::Logged {
                    effects,
                    outcome: Outcome::Push(o),
                }) => (effects, o),
                Ok(Plan::Empty(Outcome::Push(o))) => (Vec::new(), o),
                _ => return None,
            };
            // The step's gate: a pushed message that is a duplicate rolls the
            // whole transaction back (a link the partition already holds does
            // not).
            let duplicate = o.items.iter().zip(&p.items).any(|(v, item)| {
                matches!(v, PushVerdict::Duplicate { .. }) && !crate::frames::all_links(&item.frame)
            });
            if duplicate {
                return None;
            }
            plan.pushes.push((k, effects, o));
        }
        if !part.kv.is_empty() {
            let each = self
                .plan_kv_each(ov, &cmd.tenant, &cmd.kv, Some(&part.kv))
                .ok()?;
            if each.failed.is_some() {
                return None;
            }
            for (_, effect, _) in &each.each {
                if let Some(e) = effect {
                    ov.apply_effects(std::slice::from_ref(e));
                }
            }
            plan.kv = each.each;
        }
        if part.timers {
            let (effects, results) = self.plan_timer_ops(ov, &cmd.tenant, &cmd.timers).ok()?;
            plan.timers = Some((effects, results.iter().map(|r| r.to_json()).collect()));
        }
        if part.lead && !cmd.extra_effects.is_empty() {
            ov.apply_effects(&cmd.extra_effects);
        }
        Some(plan)
    }

    /// Plan a transaction all-or-nothing (see the module header).
    pub fn plan_transaction(&self, ov: &mut Overlay, cmd: &TxnCommand) -> Planned {
        if !cmd.acks.is_empty() || !cmd.positional_acks.is_empty() {
            return Err(Refusal::retry(
                "internal",
                "a transaction's acks are served by the consumption engine",
            ));
        }
        // The engine's cursor and dead-letter rows name partitions it read
        // before this was planned: checked first, before anything folds.
        self.check_partition_rows(ov, &cmd.extra_effects)?;
        // Their keys (a Streams state key is the client's) must fit the store,
        // and the Streams ceilings hold here too: a command a follower
        // prepared is planned here, and a follower on an older build may not
        // have checked them.
        self.check_effect_keys(ov, &cmd.extra_effects)?;
        check_streams_state_ops(&cmd.extra_effects)?;
        if cmd.pushes.is_empty() && cmd.timers.is_empty() {
            return self.plan_riders_bundle(ov, cmd);
        }
        // The bundle plans against the overlay as it goes (a push sees the
        // pushes before it), and keeps what it folded only if it is logged:
        // anything else (a refusal, a duplicate, a lost precondition) takes
        // its folds back out. A savepoint, not a copy of the overlay: that
        // copy was proportional to everything in flight, and with the
        // footprint scheduler to every partition its planning thread owns.
        let saved = ov.save();
        let planned = self.plan_bundle(ov, cmd);
        match &planned {
            Ok(Plan::Logged { .. }) => ov.commit(saved),
            _ => ov.restore(saved),
        }
        planned
    }

    /// [`Planner::plan_transaction`] for a bundle with pushes or timers, under
    /// its savepoint: an answer that is not `Logged` has its folds taken back.
    fn plan_bundle(&self, ov: &mut Overlay, cmd: &TxnCommand) -> Planned {
        let mut effects = Vec::new();
        let mut out = TxnOutcome::default();

        for p in &cmd.pushes {
            let (effs, o) = match self.plan_push(ov, p) {
                Ok(Plan::Logged {
                    effects,
                    outcome: Outcome::Push(o),
                }) => (effects, o),
                Ok(Plan::Empty(Outcome::Push(o))) => (Vec::new(), o),
                Ok(Plan::Refused(r)) | Err(r) => return Err(r),
                Ok(other) => {
                    return Err(Refusal::retry(
                        "internal",
                        format!("push planned {other:?}"),
                    ))
                }
            };
            // A link the partition already holds (the same transaction id) is
            // a no-op: what it asks for is there. Only a pushed message's
            // duplicate rolls the bundle back, as the step's gate.
            if let Some(PushVerdict::Duplicate { offset, .. }) = o
                .items
                .iter()
                .zip(&p.items)
                .find(|(v, item)| {
                    matches!(v, PushVerdict::Duplicate { .. })
                        && !crate::frames::all_links(&item.frame)
                })
                .map(|(v, _)| v)
            {
                if cmd.allow_duplicate {
                    out.pushes.push(o);
                    let outcome = out.into_outcome()?;
                    // Empty logs nothing, so it folds nothing either: the
                    // savepoint takes the pushes before this one back out, or
                    // a later command of the cycle would plan on a partition
                    // and an append no entry carries.
                    return Ok(Plan::Empty(outcome));
                }
                return Err(Refusal::client(
                    "duplicate",
                    format!(
                        "QDUP a pushed message to {}/{} is a duplicate (original offset \
                             {offset}); the transaction rolled back",
                        p.queue, p.partition
                    ),
                ));
            }
            effects.extend(effs);
            out.pushes.push(o);
        }

        // Positions on new partitions, after the bundle's messages and before
        // its keys: planned against everything above, folded like them.
        if !cmd.positions.is_empty() {
            let effs = self.plan_positions(ov, &cmd.tenant, &cmd.positions)?;
            ov.apply_effects(&effs);
            effects.extend(effs);
        }

        // The KV rider after the bundle's messages (024's order: the bundle's
        // messages, then its keys). A lost `required` precondition aborts the
        // WHOLE bundle: nothing is logged, the overlay is restored, and the
        // answer carries the one failed precondition so the receiver renders
        // 024's detail.
        if !cmd.kv.is_empty() {
            let kvp = self.plan_kv_writes(ov, &cmd.tenant, &cmd.kv)?;
            if let Some(f) = kvp.failed {
                return failed_kv(f);
            }
            ov.apply_effects(&kvp.effects);
            effects.extend(kvp.effects);
            out.kv = KvOutcome {
                results: kvp.results,
                failed: None,
            };
        }

        // The timers rider (the SQL wire's order: messages, keys, timers). The
        // helper folds its effects into the overlay on success and leaves it
        // untouched on refusal; the whole bundle is refused either way.
        if !cmd.timers.is_empty() {
            let (effs, results) = self.plan_timer_ops(ov, &cmd.tenant, &cmd.timers)?;
            effects.extend(effs);
            out.timers = results.iter().map(|r| r.to_json()).collect();
        }

        if !cmd.extra_effects.is_empty() {
            ov.apply_effects(&cmd.extra_effects);
            effects.extend(cmd.extra_effects.clone());
        }

        out.acks = cmd.engine_acks.clone();
        let outcome = out.into_outcome()?;
        if effects.is_empty() {
            Ok(Plan::Empty(outcome))
        } else {
            Ok(Plan::logged(effects, outcome))
        }
    }

    /// A bundle with no pushes and no timers: KV operations, positions on new
    /// partitions and the riders the engine (or Streams) hands over — what a
    /// consumer group's commit is: its cursor rows, and a `required` fence
    /// beside them.
    ///
    /// The KV and positions legs plan WITHOUT touching the overlay
    /// ([`Planner::plan_kv_writes`], [`Planner::plan_positions`]) and
    /// everything folds at the end, so a refusal or a lost precondition has
    /// nothing to restore — which spares this bundle the overlay CLONE the
    /// general path takes to be able to roll back. A group committing after
    /// every poll sends one of these per commit batch, and the clone is
    /// proportional to everything in flight.
    fn plan_riders_bundle(&self, ov: &mut Overlay, cmd: &TxnCommand) -> Planned {
        let mut effects: Vec<Effect> = Vec::new();
        let mut out = TxnOutcome::default();
        if !cmd.kv.is_empty() {
            let kvp = self.plan_kv_writes(ov, &cmd.tenant, &cmd.kv)?;
            if let Some(f) = kvp.failed {
                return failed_kv(f);
            }
            effects.extend(kvp.effects);
            out.kv = KvOutcome {
                results: kvp.results,
                failed: None,
            };
        }
        if !cmd.positions.is_empty() {
            effects.extend(self.plan_positions(ov, &cmd.tenant, &cmd.positions)?);
        }
        effects.extend(cmd.extra_effects.iter().cloned());
        out.acks = cmd.engine_acks.clone();
        let outcome = out.into_outcome()?;
        ov.apply_effects(&effects);
        if effects.is_empty() {
            Ok(Plan::Empty(outcome))
        } else {
            Ok(Plan::logged(effects, outcome))
        }
    }
}

/// The answer of a bundle whose KV `required` precondition was lost: nothing
/// logged, the one failed precondition, and no ack results (none applied).
fn failed_kv(f: crate::rsm::entry::KvPrecondition) -> Planned {
    let failed = TxnOutcome {
        kv: KvOutcome {
            results: Vec::new(),
            failed: Some(f),
        },
        ..TxnOutcome::default()
    };
    failed.into_outcome().map(Plan::Empty)
}

/// The Streams ceilings on a cycle's state ops, applied where the command is
/// planned: the value ceiling (`QUEEN_STREAMS_MAX_STATE_BYTES`) and the op
/// count (`QUEEN_STREAMS_MAX_STATE_OPS`). The receiver checks both first with a
/// clearer answer; this is the planner's copy for commands it did not receive.
fn check_streams_state_ops(effects: &[Effect]) -> Result<(), Refusal> {
    let max_value = crate::rsm::facade::streams_max_state_bytes();
    let max_ops = crate::rsm::facade::streams_max_state_ops();
    let mut ops = 0usize;
    for e in effects {
        match e {
            Effect::StreamsStatePut { value, .. } => {
                ops += 1;
                if value.len() > max_value {
                    return Err(Refusal::client(
                        "state_too_large",
                        format!(
                            "a state value is {} bytes, the ceiling is {max_value} \
                             (QUEEN_STREAMS_MAX_STATE_BYTES)",
                            value.len()
                        ),
                    ));
                }
            }
            Effect::StreamsStateDelete { .. } => ops += 1,
            _ => {}
        }
    }
    if ops > max_ops {
        return Err(Refusal::client(
            "too_many_state_ops",
            format!("{ops} state ops in one cycle, the ceiling is {max_ops} (QUEEN_STREAMS_MAX_STATE_OPS)"),
        ));
    }
    Ok(())
}
