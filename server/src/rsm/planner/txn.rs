//! Transactions (Phase B, `ALICE_PGLESS_NEWARCH.md` §4): push and ack bundled
//! ALL-OR-NOTHING into ONE command, so ONE entry carries every effect.
//!
//! Atomicity comes from Phase C for free: the writer writes that entry into
//! EVERY queue log it touches with its copy count, and recovery replays an
//! entry only when all its copies are present and identical — the
//! present-in-all commit rule (§4.1–4.3). A torn bundle is cut from every tail
//! and leaves no offset gap (its offsets were never applied).
//!
//! The planner side mirrors the SQL wire transaction: a pushed message that is
//! a DUPLICATE, or an ack the cursor does not honour (wrong/expired lease,
//! already acked), rolls the WHOLE bundle back (`QDUP` / `QTXN`, HTTP 200
//! `success:false`). The overlay is restored on refusal so a rolled-back bundle
//! leaves no phantom state for later commands of the same cycle.

use crate::rsm::entry::{AckOutcome, Outcome, Placeholder, PushOutcome, PushVerdict, RequestId};

use super::{AckTarget, Overlay, Plan, Planned, Planner, PushCommand, Refusal};
use crate::rsm::store::Reads;

/// The placeholder tag of a transaction outcome (reserved range, §5.4).
pub const TXN_OUTCOME_TAG: u16 = 0xF001;

/// One transaction: every push group and every ack target of the bundle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxnCommand {
    pub request_id: RequestId,
    pub tenant: String,
    /// One per (queue, partition), items in bundle order (the receiver packs
    /// them exactly like a push).
    pub pushes: Vec<PushCommand>,
    /// One per (pid, group, worker), like a batch ack.
    pub acks: Vec<AckTarget>,
}

/// The decoded transaction outcome: per push group, per ack target.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TxnOutcome {
    pub pushes: Vec<PushOutcome>,
    pub acks: AckOutcome,
}

impl TxnOutcome {
    /// `u32 n | n × (u32 len | Outcome::Push bytes) | u32 len | Outcome::Ack bytes`
    /// — the typed codecs reused, inside the placeholder's private body.
    pub fn into_outcome(self) -> Result<Outcome, Refusal> {
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
        Placeholder::new(TXN_OUTCOME_TAG, crate::rsm::effect::VERSION_1, body)
            .map(Outcome::Placeholder)
            .map_err(|e| Refusal::retry("internal", format!("txn outcome: {e:?}")))
    }

    pub fn from_outcome(o: &Outcome) -> Option<TxnOutcome> {
        let Outcome::Placeholder(p) = o else { return None };
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
        Some(TxnOutcome { pushes, acks })
    }
}

impl<'a, R: Reads + ?Sized> Planner<'a, R> {
    /// Plan a transaction all-or-nothing (see the module header).
    pub fn plan_transaction(&self, ov: &mut Overlay, cmd: &TxnCommand) -> Planned {
        let saved = ov.clone();
        let refuse = |ov: &mut Overlay, r: Refusal| -> Planned {
            *ov = saved.clone();
            Err(r)
        };
        let mut effects = Vec::new();
        let mut out = TxnOutcome::default();

        for p in &cmd.pushes {
            let (effs, o) = match self.plan_push(ov, p) {
                Ok(Plan::Logged {
                    effects,
                    outcome: Outcome::Push(o),
                }) => (effects, o),
                Ok(Plan::Empty(Outcome::Push(o))) => (Vec::new(), o),
                Ok(Plan::Refused(r)) | Err(r) => return refuse(ov, r),
                Ok(other) => {
                    return refuse(
                        ov,
                        Refusal::retry("internal", format!("push planned {other:?}")),
                    )
                }
            };
            if let Some(PushVerdict::Duplicate { offset, .. }) = o
                .items
                .iter()
                .find(|v| matches!(v, PushVerdict::Duplicate { .. }))
            {
                return refuse(
                    ov,
                    Refusal::client(
                        "duplicate",
                        format!(
                            "QDUP a pushed message to {}/{} is a duplicate (original offset \
                             {offset}); the transaction rolled back",
                            p.queue, p.partition
                        ),
                    ),
                );
            }
            effects.extend(effs);
            out.pushes.push(o);
        }

        for t in &cmd.acks {
            let (res, effs) = match self.ack_target(ov, t) {
                Ok(v) => v,
                Err(r) => return refuse(ov, r),
            };
            if !res.stale_hashes.is_empty() {
                return refuse(
                    ov,
                    Refusal::client(
                        "rejected_ack",
                        format!(
                            "QTXN {} acked message(s) on partition {} are not leased by \
                             this worker or were already acked; the transaction rolled back",
                            res.stale_hashes.len(),
                            t.pid
                        ),
                    ),
                );
            }
            ov.apply_effects(&effs);
            effects.extend(effs);
            out.acks.results.push(res);
        }

        let outcome = match out.into_outcome() {
            Ok(o) => o,
            Err(r) => return refuse(ov, r),
        };
        if effects.is_empty() {
            Ok(Plan::Empty(outcome))
        } else {
            Ok(Plan::logged(effects, outcome))
        }
    }
}
