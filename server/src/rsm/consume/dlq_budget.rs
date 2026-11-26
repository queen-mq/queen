//! The dead-letter budget (`QUEEN_DLQ_MAX_BYTES_TOTAL`, default 512 MiB; `0`
//! turns it off).
//!
//! A dead letter keeps a full copy of its message's body, in the RAM tables of
//! every node, reloaded at every boot. The per-queue caps
//! (`QUEEN_DLQ_MAX_ROWS_PER_QUEUE`, `QUEEN_DLQ_MAX_BYTES_PER_QUEUE`,
//! `QUEEN_DLQ_MAX_AGE_S`) bound one queue each, and the maintenance passes
//! apply them after the fact: a few queues that fail large messages fast can
//! still fill every node's memory at once, and a node killed for it reloads
//! the same dead letters when it restarts. This budget bounds all of them, as
//! they are filed: once the dead letters hold the budget, a new one is filed
//! WITHOUT its body. Its id, transaction id, partition, offset, retry count and
//! error stay, the error saying why ([`BODY_NOT_KEPT`]), and a replay of it is
//! refused ([`body_dropped`]).
//!
//! The leader decides, as it plans the ack, and the dead letter goes into the
//! replicated entry as decided: every node stores the same row. What the dead
//! letters hold is the store's own count of their bytes
//! ([`HeedStore::ram_value_bytes`]), read again at most once a second, plus
//! the bodies the leader kept since. Timer dead letters (a timer that could
//! not fire) are not budgeted: they are rare, and as small as a timer's
//! message.
//!
//! [`HeedStore::ram_value_bytes`]: crate::rsm::store::HeedStore::ram_value_bytes

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use super::{mono_us, Engine, SEC_US};
use crate::rsm::effect::Effect;
use crate::rsm::store::Keyspace;

/// The end of the error of a dead letter filed without its body.
pub(crate) const BODY_NOT_KEPT: &str =
    " (body not kept: the dead letters held QUEEN_DLQ_MAX_BYTES_TOTAL)";

/// Whether a stored dead letter was filed without its body by the budget.
pub(crate) fn body_dropped(payload: &[u8], error: &str) -> bool {
    payload.is_empty() && error.ends_with(BODY_NOT_KEPT)
}

/// The budget on the dead letters `effects` file, when the dead letters
/// already hold `held` bytes: each body is kept while `held` plus the bodies
/// kept before it stay within `max`, and dropped past it. A dead letter with
/// no body, or with no message id (the unread placeholder, which
/// `ack::files_unread` refuses), is left as it is. Returns the bytes of the
/// bodies kept and how many were dropped. `max` 0: nothing is dropped.
pub(crate) fn bodies(held: u64, max: u64, effects: &mut [Effect]) -> (u64, u64) {
    let (mut kept, mut dropped) = (0u64, 0u64);
    for e in effects.iter_mut() {
        let Effect::DlqInsert {
            payload,
            error,
            message_id,
            ..
        } = e
        else {
            continue;
        };
        if payload.is_empty() || message_id.is_none() {
            continue;
        }
        let n = payload.len() as u64;
        if max == 0 || held.saturating_add(kept).saturating_add(n) <= max {
            kept += n;
        } else {
            *payload = Vec::new();
            error.push_str(BODY_NOT_KEPT);
            dropped += 1;
        }
    }
    (kept, dropped)
}

/// The leader's view of what the dead letters hold, and what it dropped.
pub(crate) struct Budget {
    /// The budget in bytes (`Knobs::dlq_max_bytes`); 0: off.
    max: AtomicU64,
    /// Bytes the dead letters held at the last count, plus the bodies kept
    /// since.
    held: AtomicU64,
    /// When the store last counted them ([`mono_us`]); `i64::MIN`: never.
    counted_us: AtomicI64,
    /// Dead letters filed without their body.
    pub(crate) not_kept: AtomicU64,
}

impl Budget {
    pub(crate) fn new(max: u64) -> Budget {
        Budget {
            max: AtomicU64::new(max),
            held: AtomicU64::new(0),
            counted_us: AtomicI64::new(i64::MIN),
            not_kept: AtomicU64::new(0),
        }
    }

    /// A facade test's budget: the facade builds its engine from the
    /// environment, which every test in the process shares.
    #[cfg(test)]
    pub(crate) fn set_max_for_test(&self, max: u64) {
        self.max.store(max, Ordering::Relaxed);
    }
}

impl Engine {
    /// Apply the dead-letter budget to the dead letters `effects` file
    /// ([`bodies`]), counting what the dead letters hold from the store at
    /// most once a second.
    pub(crate) fn budget_dlq(&self, effects: &mut [Effect]) {
        let b = &self.dlq_budget;
        let max = b.max.load(Ordering::Relaxed);
        if max == 0
            || !effects
                .iter()
                .any(|e| matches!(e, Effect::DlqInsert { .. }))
        {
            return;
        }
        let now = mono_us();
        if now.saturating_sub(b.counted_us.load(Ordering::Relaxed)) >= SEC_US {
            b.held
                .store(self.store.ram_value_bytes(Keyspace::Dlq), Ordering::Relaxed);
            b.counted_us.store(now, Ordering::Relaxed);
        }
        let (kept, dropped) = bodies(b.held.load(Ordering::Relaxed), max, effects);
        b.held.fetch_add(kept, Ordering::Relaxed);
        if dropped > 0 {
            b.not_kept.fetch_add(dropped, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dead_letter(payload: &[u8], message_id: Option<[u8; 16]>) -> Effect {
        Effect::DlqInsert {
            dlq_id: [7; 16],
            tenant: "t".into(),
            queue: "q".into(),
            pid: 1,
            group: "g".into(),
            offset: 3,
            message_id,
            txn: "tx".into(),
            payload: payload.to_vec(),
            error: "boom".into(),
            retry_count: 2,
            failed_at_us: 9,
            headers: Default::default(),
        }
    }

    fn body(e: &Effect) -> (&[u8], &str) {
        match e {
            Effect::DlqInsert { payload, error, .. } => (payload, error),
            _ => unreachable!(),
        }
    }

    #[test]
    fn bodies_are_kept_until_the_budget_then_filed_without() {
        let id = Some([1; 16]);
        let mut effs = vec![
            dead_letter(&[b'a'; 40], id),
            dead_letter(&[b'b'; 40], id),
            Effect::GroupDelete {
                tenant: "t".into(),
                queue: "q".into(),
                group: "g".into(),
            },
            dead_letter(&[b'c'; 40], id),
            dead_letter(&[b'd'; 10], id),
        ];
        // 30 held, a budget of 120: a and b fit (110), c does not (150), d
        // does (120 exactly).
        let (kept, dropped) = bodies(30, 120, &mut effs);
        assert_eq!((kept, dropped), (90, 1));
        assert_eq!(body(&effs[0]), (&[b'a'; 40][..], "boom"));
        assert_eq!(body(&effs[1]).0.len(), 40);
        let (p, e) = body(&effs[3]);
        assert!(p.is_empty());
        assert_eq!(e, format!("boom{BODY_NOT_KEPT}"));
        assert!(body_dropped(p, e));
        assert_eq!(body(&effs[4]).0.len(), 10);
        assert!(!body_dropped(body(&effs[0]).0, body(&effs[0]).1));
    }

    #[test]
    fn the_budget_leaves_the_unread_placeholder_and_empty_bodies_alone() {
        // No message id and no body: the placeholder `files_unread` refuses.
        let mut effs = vec![dead_letter(b"", None), dead_letter(b"", Some([1; 16]))];
        assert_eq!(bodies(u64::MAX, 1, &mut effs), (0, 0));
        assert_eq!(body(&effs[0]), (&b""[..], "boom"));
        assert_eq!(body(&effs[1]), (&b""[..], "boom"));
        // A body with no message id is not the budget's to drop either.
        let mut effs = vec![dead_letter(b"xyz", None)];
        assert_eq!(bodies(u64::MAX, 1, &mut effs), (0, 0));
        assert_eq!(body(&effs[0]).0, b"xyz");
    }

    #[test]
    fn a_budget_of_zero_keeps_every_body() {
        let mut effs = vec![dead_letter(&[b'a'; 1 << 20], Some([1; 16]))];
        assert_eq!(bodies(u64::MAX, 0, &mut effs), (1 << 20, 0));
        assert_eq!(body(&effs[0]).0.len(), 1 << 20);
    }
}
