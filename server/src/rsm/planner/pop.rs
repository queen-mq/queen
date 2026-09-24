//! Pop — the port of `004_log_pop.sql` (`log_pop_v1` and its pinned, wildcard
//! and discovery entry points, plus `log_pop_list_v1`'s budget walk).
//!
//! Every pop funnels through [`Planner::claim_one`], exactly as every variant of
//! 004 funnels through `log_pop_v1`. The pinned route claims one named
//! partition; the wildcard and discovery routes walk candidates sharing one
//! budget.
//!
//! What the RSM changes from pgless (and why it is still the SQL's behaviour):
//!
//! * **No `hw`**: `last_offset` is the visible tail, so the `visible` guard is
//!   gone and a segment planned in this same cycle (overlay) is claimable — its
//!   append and this claim commit together.
//! * **Segments come from `txns`**, not RAM SegRefs (positions are node-local).
//! * **The ring is apply-owned (I1)**: the planner READS it (`candidates`) and
//!   never parks or pops it. A busy/quiet/leased candidate is skipped; apply's
//!   `pending` row (with its `ready_at`) is what re-arms it, so the planner
//!   needs no `defer`.
//! * **Seeding is lazy and by the subscription instant** (see the module
//!   header): the first pop of a group registers it (a one-time `GroupUpsert`,
//!   §8) and each partition is seeded on its own first contact — no
//!   bulk O(partitions) seed on the plain path. A first contact that delivers
//!   nothing writes NO cursor, because the seed is stable (a re-poll recomputes
//!   the same value), which is what keeps cost off the poll rate (G-3). The one
//!   exception is the empty-partition seal. The SQL's queue-wide bulk seed
//!   survives in exactly one place, the CONFLATING pinned registrar
//!   (`register_pinned_conflating`, 004:243-296): a pinned pop registers the
//!   group only when it is conflating AND its pinned partition has no cursor of
//!   its own yet — 004 nests the registrar inside the outer first-contact guard
//!   `NOT EXISTS(cursor for this partition+group)` (004:189-190). The group then
//!   being present, no later wildcard pop enumerates the queue for it, so its
//!   whole partition set is seeded there, once. A plain pinned pop registers
//!   nothing (004:226), and so does a conflating pop whose pinned partition a
//!   plain pop already seeded (the outer guard is then false); the wildcard
//!   enumeration still reaches the group's backlog on the untouched partitions.
//! * **The delivered set is recorded on the claim** (O16): the distinct hashes
//!   in the claimed run, so the ack fast path is deterministic.

use crate::rsm::dedup::IndexMode;
use crate::rsm::dedup::TxnsRow;
use crate::rsm::effect::{Effect, GroupMeta, Pid, QueueConfig, SubscriptionMode};
use crate::rsm::entry::{Outcome, PopClaim, PopOutcome};
use crate::rsm::store::rows::{cursor_fresh, lease_live, GroupRow};
use crate::rsm::store::{keys, Keyspace, Reads, StoreError, TypedReads};

use super::{
    group_meta_for_registration, store_err, Overlay, PartView, Plan, Planned, Planner, PopCommand,
    Refusal, Seg, SEC_US,
};

/// The most wildcard/discovery candidates the walk gathers before claiming.
/// The claim loop breaks early once the budget or `max_parts` is met; this only
/// bounds the up-front gather so a queue of millions of ready partitions cannot
/// make one pop allocate without limit. A tighter bound (gather near the budget)
/// is a follow-up, and the same O(ready) the pgless walk had (R-105).
const CANDIDATE_GATHER_CAP: usize = 65_536;

/// One segment as the claim walk sees it, carrying the per-frame hashes when the
/// bounded claim path (PERF-I) collected them in the same pass it read the
/// segment shape. `hashes` is `None` on the baseline (`segs_from`) path and on
/// an auto-ack claim (whose delivered set is discarded), `Some` when the claim
/// will record a delivered set on the cursor (O16).
#[derive(Clone, Debug)]
struct SegH {
    base: u64,
    end: u64,
    created_at_us: i64,
    hashes: Option<Vec<[u8; 16]>>,
}

impl SegH {
    fn plain(s: Seg) -> SegH {
        SegH {
            base: s.base,
            end: s.end,
            created_at_us: s.created_at_us,
            hashes: None,
        }
    }
}

/// The default cap on the `pending` prefix scan [`wildcard_pop_provably_empty`]
/// makes. A caught-up group has zero pending rows, so the common case stops at
/// once; this only bounds a pathological group with very many deferred (leased /
/// delayed / window-buffered) partitions, where the fastpath conservatively
/// falls back to submitting rather than scan without limit.
pub const POP_FASTPATH_SCAN_CAP: usize = 512;

/// PERF-J pop empty fastpath (`QUEEN_RAFT_POP_FASTPATH_EMPTY`). Whether a
/// wildcard pop of `(tenant, queue, group)` is PROVABLY empty from committed
/// state alone, so the facade can answer it WITHOUT submitting a `PopWildcard`
/// command onto the single serial batcher pipeline (where its ~0.5 ms plan
/// competes with the pushes — the round-4 finding).
///
/// It reduces to the same `pending.ready_at` truth the parked long-poll's
/// `has_pending` gate reads (§9.5), so a `true` here is consistent with what the
/// wildcard walk would then offer: it means [`Planner::plan_pop_wildcard`] would
/// return [`Plan::Empty`] (no log entry). Conservative by construction — it
/// returns `true` (fastpath, skip the submit) ONLY when
///  * the queue exists (a missing queue that a `create_cfg` would materialise is
///    an effect; the caller passes no create when it uses this),
///  * the group is already registered (a first-contact pop emits a `GroupUpsert`
///    effect, so it is NOT empty and MUST be planned),
///  * the group is not conflating (conflation delivers the tail, not the ring),
///    and
///  * no pending partition of the group is ready (`ready_at <= now_us`).
///
/// Everything else returns `false` = "submit and let the planner decide" (it may
/// still be empty). This can only ever cost one extra command; it can never
/// strand work, because a push that lands after this read re-arms the ring AND
/// wakes the parked consumer (apply's per-group append wake, §9.5), which
/// re-polls — so no message is lost, exactly as a post-empty re-poll behaves
/// today.
pub fn wildcard_pop_provably_empty<R: TypedReads + ?Sized>(
    r: &R,
    tenant: &str,
    queue: &str,
    group: &str,
    now_us: i64,
    cap: usize,
) -> crate::rsm::store::Result<bool> {
    // A missing queue is empty only when no create config rides the pop; the
    // caller guarantees that, but the read is cheap and keeps this self-contained.
    if r.queue(tenant, queue)?.is_none() {
        return Ok(false);
    }
    // First contact registers the group (an effect) => not empty; must submit.
    let Some(g) = r.group(tenant, queue, group)? else {
        return Ok(false);
    };
    // Conflation reads the tail, which the pending ring does not fully capture.
    if g.meta.conflation {
        return Ok(false);
    }
    // Any pending partition ready now => the planner would try to claim it.
    let from = keys::pending_prefix(tenant, queue, group);
    let mut ready = false; // a claimable (ready_at <= now) partition exists
    let mut boundary = false; // the scan walked off this group's prefix
    let mut scanned = 0usize; // rows seen that belong to THIS group
    r.scan_pending(&from, cap, &mut |t, q, gg, _pid, ready_at| {
        if t != tenant || q != queue || gg != group {
            boundary = true;
            return false; // past this group's rows: nothing more can be ready
        }
        scanned += 1;
        if ready_at <= now_us {
            ready = true;
            return false;
        }
        true
    })?;
    if ready {
        return Ok(false);
    }
    // Proven empty only if the scan reached this group's boundary or exhausted
    // the keyspace (fewer than `cap` of this group's rows). A full `cap` of
    // deferred rows with no boundary might hide a ready row past the cap, so be
    // conservative and submit.
    Ok(boundary || scanned < cap)
}

/// The pop autopilot's width input: how many of the group's partitions are
/// claimable NOW (`pending` rows due), counted up to `cap`. `None` when the
/// count means nothing for a claim (no such queue or group yet, or a
/// conflating group, whose claim reads the tail rather than the ring).
pub fn wildcard_ready_count<R: TypedReads + ?Sized>(
    r: &R,
    tenant: &str,
    queue: &str,
    group: &str,
    now_us: i64,
    cap: usize,
) -> crate::rsm::store::Result<Option<usize>> {
    if r.queue(tenant, queue)?.is_none() {
        return Ok(None);
    }
    let Some(g) = r.group(tenant, queue, group)? else {
        return Ok(None);
    };
    if g.meta.conflation {
        return Ok(None);
    }
    let from = keys::pending_prefix(tenant, queue, group);
    let mut ready = 0usize;
    let mut seen = 0usize;
    r.scan_pending(&from, cap.max(1), &mut |t, q, gg, _pid, ready_at| {
        if t != tenant || q != queue || gg != group {
            return false;
        }
        seen += 1;
        if ready_at <= now_us {
            ready += 1;
        }
        seen < cap
    })?;
    Ok(Some(ready))
}

/// PLAN_RAFT_DRAIN_FIX P1.2: the time an answer needs after the plan (propose,
/// commit, apply, reply). A pop whose waiter's deadline falls inside it is
/// answered EMPTY instead of claiming: a claim nobody receives is a lease
/// nobody acks, and it freezes the partition for the whole lease
/// (`QUEEN_RAFT_POP_REPLY_MARGIN_MS`, default 50 ms).
static POP_REPLY_MARGIN_US: std::sync::LazyLock<i64> = std::sync::LazyLock::new(|| {
    std::env::var("QUEEN_RAFT_POP_REPLY_MARGIN_MS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(50)
        .saturating_mul(1000)
});

/// Whether nobody can receive this pop's answer any more (P1.2).
fn pop_expired(cmd: &PopCommand, now_us: i64) -> bool {
    cmd.deadline_us > 0 && now_us.saturating_add(*POP_REPLY_MARGIN_US) > cmd.deadline_us
}

impl<'a, R: Reads + ?Sized> Planner<'a, R> {
    /// A pinned pop of one named partition (`log_pop_specific_v1`). An unknown
    /// partition answers EMPTY and is never provisioned — only a push
    /// materialises one.
    pub fn plan_pop_pinned(&self, ov: &mut Overlay, cmd: &PopCommand) -> Planned {
        if pop_expired(cmd, self.now_us) {
            return Ok(Plan::Empty(Outcome::Pop(PopOutcome::default())));
        }
        let partition = cmd
            .partition
            .as_deref()
            .ok_or_else(|| Refusal::client("bad_request", "a pinned pop needs a partition"))?;
        let cfg = match self.queue_cfg(ov, &cmd.tenant, &cmd.queue)? {
            Some(c) => c,
            None => return Ok(Plan::Empty(Outcome::Pop(PopOutcome::default()))),
        };
        let Some(pid) = self.pid_of(ov, &cmd.tenant, &cmd.queue, partition)? else {
            return Ok(Plan::Empty(Outcome::Pop(PopOutcome::default())));
        };

        let mut effects: Vec<Effect> = Vec::new();

        // 004 the-registrar (`log_pop_v1` ≈189-346). The SQL nests its registrar
        // (`IF v_from_ts IS NULL AND v_conflate`, 004:226) INSIDE the outer
        // first-contact guard `p_group <> '__QUEUE_MODE__' AND NOT EXISTS(cursor
        // for THIS partition + p_group)` (004:189-190). So a pinned pop registers
        // the group (and runs the queue-wide bulk seed) only when BOTH the group
        // is unregistered AND the PINNED partition has no cursor of its own yet.
        // The two conditions are not the same: a plain pinned pop of this same
        // (partition, group) can seed this partition's cursor while leaving the
        // group unregistered (it takes `claim_one`'s `group_row = None` branch,
        // 004 ≈320) — and then a conflating pop finds the outer NOT EXISTS FALSE,
        // so 004 registers NOTHING and seeds NOTHING; the pop just serves the
        // existing cursor. Gating the registrar on the group alone (as a refuted
        // revision did) would fire it here, manufacturing a durable conflating
        // group and a queue-wide seed the oracle never writes — a dual-backend
        // I12 divergence and a §1.1 stored policy that is itself wrong (a later
        // wildcard would then read conflation=true and drop every partition's
        // backlog instead of registering plain on first contact). So require both.
        //
        // Leaving `group()` absent whenever we are NOT the registrar is
        // load-bearing for I12/§8: a later wildcard/discover pop is then still
        // detected as first contact and enumerates the queue, reaching the
        // group's pre-existing backlog on the partitions this pinned pop never
        // touched.
        let group_row = match self.group(ov, &cmd.tenant, &cmd.queue, &cmd.group)? {
            // Already registered — by a wildcard/discover pop, or by an earlier
            // conflating pinned pop: the STORED policy wins (§1.1, 004 ≈298).
            Some(row) => Some(row),
            None => {
                if cmd.conflate && self.cursor(ov, pid, &cmd.group)?.is_none() {
                    // First contact of a CONFLATING pinned pop whose pinned
                    // partition is itself first contact (the outer guard holds):
                    // this call is the registrar (004:226). A conflating group has
                    // nowhere else to publish its delivery policy for a second
                    // consumer to find, and — the group being registered here — no
                    // later wildcard pop will enumerate the queue for it, so the
                    // whole partition set must be made reachable NOW by the
                    // queue-wide bulk seed (004:243-296).
                    Some(self.register_pinned_conflating(
                        ov,
                        &mut effects,
                        &cmd.tenant,
                        &cmd.queue,
                        cmd,
                    )?)
                } else {
                    // Not the registrar: a PLAIN pinned pop, or a conflating pop
                    // whose pinned partition already carries a cursor (the outer
                    // NOT EXISTS is false). `claim_one` serves this partition from
                    // its existing/seeded cursor with the pop-carried conflation
                    // (`group_row = None`), and the group stays unregistered.
                    None
                }
            }
        };

        let claim = self.claim_one(
            ov,
            &mut effects,
            &cfg,
            group_row.as_ref(),
            cmd,
            pid,
            cmd.budget,
        )?;
        self.finish_pop(ov, effects, claim.into_iter().collect())
    }

    /// A wildcard pop of a whole queue (`log_pop_wildcard_*_v1`): claim up to
    /// `max_parts` partitions from the candidate ring in FIFO order, sharing one
    /// budget.
    pub fn plan_pop_wildcard(&self, ov: &mut Overlay, cmd: &PopCommand) -> Planned {
        if pop_expired(cmd, self.now_us) {
            return Ok(Plan::Empty(Outcome::Pop(PopOutcome::default())));
        }
        let cfg = match self.queue_cfg(ov, &cmd.tenant, &cmd.queue)? {
            Some(c) => c,
            None => {
                // A wildcard pop MAY create a missing queue (004 ≈1046) so the
                // group can register against it; without a create config it
                // answers empty.
                let Some(create) = &cmd.create_cfg else {
                    return Ok(Plan::Empty(Outcome::Pop(PopOutcome::default())));
                };
                let mut c = create.clone();
                c.created_at_us = self.now_us;
                let mut effects = vec![Effect::QueueUpsert {
                    tenant: cmd.tenant.clone(),
                    queue: cmd.queue.clone(),
                    cfg: c.clone(),
                }];
                // Register the group over the just-created queue; there are no
                // partitions yet, so nothing is claimed.
                let _ = self.register_on_first_contact(
                    ov,
                    &mut effects,
                    &cmd.tenant,
                    &cmd.queue,
                    &cmd.group,
                    cmd,
                )?;
                return self.finish_pop(ov, effects, Vec::new());
            }
        };
        self.plan_pop_over_queue(ov, cmd, &cmd.queue, &cfg)
    }

    /// A discovery pop across every queue of a namespace/task
    /// (`log_pop_discover_*_v1`): the wildcard walk over each matching queue,
    /// sharing the pop's budget and `max_parts` across all of them.
    pub fn plan_pop_discover(&self, ov: &mut Overlay, cmd: &PopCommand) -> Planned {
        if pop_expired(cmd, self.now_us) {
            return Ok(Plan::Empty(Outcome::Pop(PopOutcome::default())));
        }
        // Resolve the queues of the tenant whose namespace/task match. The
        // group registers per (queue, group), like the SQL's discovery group.
        let mut queues: Vec<(String, QueueConfig)> = Vec::new();
        let ns = &cmd.namespace;
        let task = &cmd.task;
        let mut bad: Option<Refusal> = None;
        self.committed
            .reads()
            .scan_queues(&cmd.tenant, usize::MAX, &mut |name, cfg| {
                let ns_ok = ns.is_empty() || cfg.namespace.as_deref() == Some(ns.as_str());
                let task_ok = task.is_empty() || cfg.task.as_deref() == Some(task.as_str());
                if ns_ok && task_ok {
                    queues.push((name.to_string(), cfg));
                }
                true
            })
            .map_err(|e| {
                bad = Some(Refusal::from_store(e.clone()));
                Refusal::from_store(e)
            })
            .ok();
        if let Some(e) = bad {
            return Err(e);
        }
        // Each per-queue call folds its own effects into the overlay (through
        // `finish_pop`), so discovery only AGGREGATES the returned effects and
        // claims for the one entry — it must not re-fold.
        let mut effects: Vec<Effect> = Vec::new();
        let mut all_claims: Vec<PopClaim> = Vec::new();
        let mut budget = cmd.budget.max(1);
        let mut max_parts = if cmd.max_parts <= 0 {
            i32::MAX
        } else {
            cmd.max_parts
        };
        for (queue, _) in queues {
            if budget <= 0 || max_parts <= 0 {
                break;
            }
            // The committed set, seen through the overlay (as the pinned and
            // wildcard pops see their queue): a queue a delete in flight drops
            // is not one to register a group on.
            let Some(cfg) = self.queue_cfg(ov, &cmd.tenant, &queue)? else {
                continue;
            };
            if (!ns.is_empty() && cfg.namespace.as_deref() != Some(ns.as_str()))
                || (!task.is_empty() && cfg.task.as_deref() != Some(task.as_str()))
            {
                continue;
            }
            let mut sub = cmd.clone();
            sub.queue = queue.clone();
            sub.budget = budget;
            sub.max_parts = max_parts;
            let (mut e, claims) = match self.plan_pop_over_queue(ov, &sub, &queue, &cfg)? {
                Plan::Logged { effects, outcome } => (effects, claims_of(outcome)),
                Plan::Empty(outcome) => (Vec::new(), claims_of(outcome)),
                Plan::Refused(r) => return Err(r),
            };
            effects.append(&mut e);
            for c in &claims {
                let took = (c.end_offset as i64 - c.start_offset as i64 + 1).max(0) as i32;
                budget -= took;
                max_parts -= 1;
            }
            all_claims.extend(claims);
        }
        let outcome = Outcome::Pop(PopOutcome { claims: all_claims });
        if effects.is_empty() {
            Ok(Plan::Empty(outcome))
        } else {
            Ok(Plan::logged(effects, outcome))
        }
    }

    /// The wildcard walk over one resolved queue: register the group on first
    /// contact, gather candidates, claim them sharing the budget.
    fn plan_pop_over_queue(
        &self,
        ov: &mut Overlay,
        cmd: &PopCommand,
        queue: &str,
        cfg: &QueueConfig,
    ) -> Planned {
        let mut effects: Vec<Effect> = Vec::new();
        let existed = self.group(ov, &cmd.tenant, queue, &cmd.group)?.is_some();
        let group_row =
            self.register_on_first_contact(ov, &mut effects, &cmd.tenant, queue, &cmd.group, cmd)?;
        let first_contact = !existed;

        let candidates = self.wildcard_candidates(
            ov,
            &cmd.tenant,
            queue,
            &cmd.group,
            first_contact,
            group_row.as_ref(),
        )?;

        {
            use crate::rsm::dbgctr::{inc, C};
            inc(&C.pop_over_queue, 1);
            inc(&C.pop_cands, candidates.len() as u64);
        }
        let n_cands = candidates.len();
        let mut remaining = cmd.budget.max(1);
        let max_parts = if cmd.max_parts <= 0 {
            i32::MAX
        } else {
            cmd.max_parts
        };
        let mut claimed = 0i32;
        let mut claims: Vec<PopClaim> = Vec::new();
        for pid in candidates {
            if remaining <= 0 || claimed >= max_parts {
                break;
            }
            if let Some(claim) = self.claim_one(
                ov,
                &mut effects,
                cfg,
                group_row.as_ref(),
                cmd,
                pid,
                remaining,
            )? {
                let took = (claim.end_offset as i64 - claim.start_offset as i64 + 1).max(0) as i32;
                remaining -= took;
                claimed += 1;
                claims.push(claim);
            }
        }
        {
            use crate::rsm::dbgctr::{inc, C};
            if claims.is_empty() {
                if n_cands == 0 {
                    inc(&C.pop_empty_nocand, 1);
                } else {
                    inc(&C.pop_empty_allfail, 1);
                }
            } else {
                inc(&C.pop_claims, claims.len() as u64);
            }
        }
        self.finish_pop(ov, effects, claims)
    }

    /// Fold the pop's effects into the overlay and shape the result: a pop with
    /// no effect at all (nothing claimed, nothing registered, nothing sealed) is
    /// [`Plan::Empty`] and not logged (§5.4); anything with an effect is logged.
    fn finish_pop(&self, ov: &mut Overlay, effects: Vec<Effect>, claims: Vec<PopClaim>) -> Planned {
        let outcome = Outcome::Pop(PopOutcome { claims });
        if effects.is_empty() {
            return Ok(Plan::Empty(outcome));
        }
        ov.apply_effects(&effects);
        Ok(Plan::logged(effects, outcome))
    }

    /// Register the group on first contact (§8, 004 the-registrar): emit one
    /// `GroupUpsert` built from the pop-carried intent, and return the group row
    /// the seed will use. A group already registered returns its stored row and
    /// emits nothing.
    fn register_on_first_contact(
        &self,
        ov: &Overlay,
        effects: &mut Vec<Effect>,
        tenant: &str,
        queue: &str,
        group: &str,
        cmd: &PopCommand,
    ) -> Result<Option<GroupRow>, Refusal> {
        if let Some(row) = self.group(ov, tenant, queue, group)? {
            return Ok(Some(row));
        }
        let meta: GroupMeta = group_meta_for_registration(
            crate::util::uuidv7_bytes(),
            "",
            &cmd.namespace,
            &cmd.task,
            &cmd.sub,
            cmd.conflate,
            self.now_us,
        );
        effects.push(Effect::GroupUpsert {
            tenant: tenant.to_string(),
            queue: queue.to_string(),
            group: group.to_string(),
            meta: meta.clone(),
        });
        // The row the seed uses this cycle. apply assigns the real
        // (reg_index, reg_effect); the planner does not read them (it seeds by
        // the instant), so 0/0 here is only a placeholder.
        Ok(Some(GroupRow {
            meta,
            reg_index: 0,
            reg_effect: 0,
        }))
    }

    /// The registrar half of a CONFLATING first-contact pinned pop (004
    /// ≈226-296). Register the group from the pop-carried intent, then seed the
    /// cursor of EVERY partition of the queue — the SQL's queue-wide bulk seed,
    /// which the pinned route runs only here because, unlike the wildcard route,
    /// a later pop of a now-registered group will not enumerate the queue for it.
    /// Each seed is exactly the value `claim_one` computes from the same group
    /// row (`seed_committed`), so the pinned partition's own claim, run next,
    /// agrees with the row seeded here by construction; `apply`'s `CursorSet`
    /// derives `pending` from it, so a partition with backlog becomes a wildcard
    /// candidate and one seeded at its tail (`new`) does not. A partition that
    /// already carries a cursor for the group this cycle is left untouched (the
    /// SQL's `ON CONFLICT DO NOTHING`).
    fn register_pinned_conflating(
        &self,
        ov: &Overlay,
        effects: &mut Vec<Effect>,
        tenant: &str,
        queue: &str,
        cmd: &PopCommand,
    ) -> Result<GroupRow, Refusal> {
        let meta: GroupMeta = group_meta_for_registration(
            crate::util::uuidv7_bytes(),
            "",
            &cmd.namespace,
            &cmd.task,
            &cmd.sub,
            cmd.conflate,
            self.now_us,
        );
        effects.push(Effect::GroupUpsert {
            tenant: tenant.to_string(),
            queue: queue.to_string(),
            group: cmd.group.clone(),
            meta: meta.clone(),
        });
        // The row the seed uses this cycle (apply assigns the real
        // (reg_index, reg_effect); the planner seeds by the instant, not the
        // position, so 0/0 is only a placeholder — as in `register_on_first_contact`).
        let group_row = GroupRow {
            meta,
            reg_index: 0,
            reg_effect: 0,
        };
        for pid in self.queue_partitions(ov, tenant, queue)? {
            if self.cursor(ov, pid, &cmd.group)?.is_some() {
                continue;
            }
            let Some(part) = self.partition(ov, pid)? else {
                continue;
            };
            let seed = self.seed_committed(ov, &part, &group_row)?;
            effects.push(Effect::CursorSet {
                pid,
                group: cmd.group.clone(),
                row: cursor_fresh(seed, self.now_us),
            });
        }
        Ok(group_row)
    }

    /// Every partition of a queue, as the wildcard first-contact enumeration
    /// sees it: the committed partition set plus any partition this cycle's
    /// overlay created for the same queue, in a deterministic order. It is the
    /// set the conflating bulk seed writes cursors for, so the seed reaches
    /// EXACTLY the partitions a wildcard first contact would — the equivalence
    /// the registrar owes. Bounded by [`CANDIDATE_GATHER_CAP`], the same
    /// O(ready) gather the wildcard walk has (R-105).
    fn queue_partitions(
        &self,
        ov: &Overlay,
        tenant: &str,
        queue: &str,
    ) -> Result<Vec<Pid>, Refusal> {
        let mut out: Vec<Pid> = Vec::new();
        let mut seen: std::collections::HashSet<Pid> = std::collections::HashSet::new();
        let mut bad: Option<Refusal> = None;
        self.committed
            .reads()
            .scan_queue_partitions(tenant, queue, None, CANDIDATE_GATHER_CAP, &mut |pid| {
                if seen.insert(pid) {
                    out.push(pid);
                }
                out.len() < CANDIDATE_GATHER_CAP
            })
            .map_err(|e| {
                bad = Some(Refusal::from_store(e.clone()));
                Refusal::from_store(e)
            })
            .ok();
        if let Some(e) = bad {
            return Err(e);
        }
        let mut overlay_pids: Vec<Pid> = ov.appended_pids();
        overlay_pids.sort_unstable();
        for pid in overlay_pids {
            if seen.contains(&pid) {
                continue;
            }
            if let Some(part) = self.partition(ov, pid)? {
                if part.tenant == tenant && part.queue == queue {
                    seen.insert(pid);
                    out.push(pid);
                }
            }
        }
        Ok(out)
    }

    /// The candidate pids of a wildcard walk (in a deterministic order):
    ///
    /// * FIRST CONTACT of a group with backlog to find (`all`/`timestamp`):
    ///   every partition of the queue, in pid order — the RSM's stand-in for
    ///   the SQL's bulk seed, because a just-registered group has no `pending`
    ///   rows for the partitions that predate it. A `new` group has no backlog,
    ///   so it skips the enumeration and rides the ring alone.
    /// * STEADY STATE: the committed ready ring (FIFO), then any partition this
    ///   cycle's overlay appended to that the ring does not already hold.
    fn wildcard_candidates(
        &self,
        ov: &Overlay,
        tenant: &str,
        queue: &str,
        group: &str,
        first_contact: bool,
        group_row: Option<&GroupRow>,
    ) -> Result<Vec<Pid>, Refusal> {
        let mut out: Vec<Pid> = Vec::new();
        let mut seen: std::collections::HashSet<Pid> = std::collections::HashSet::new();

        let enumerate =
            first_contact && group_row.is_some_and(|g| g.meta.mode != SubscriptionMode::New);
        if enumerate {
            let mut bad: Option<Refusal> = None;
            self.committed
                .reads()
                .scan_queue_partitions(tenant, queue, None, CANDIDATE_GATHER_CAP, &mut |pid| {
                    if seen.insert(pid) {
                        out.push(pid);
                    }
                    out.len() < CANDIDATE_GATHER_CAP
                })
                .map_err(|e| {
                    bad = Some(Refusal::from_store(e.clone()));
                    Refusal::from_store(e)
                })
                .ok();
            if let Some(e) = bad {
                return Err(e);
            }
        } else {
            self.committed
                .candidates(tenant, queue, group, CANDIDATE_GATHER_CAP, &mut |pid| {
                    if seen.insert(pid) {
                        out.push(pid);
                    }
                    out.len() < CANDIDATE_GATHER_CAP
                });
        }

        // Overlay-appended partitions of this queue that the ring cannot yet
        // know about (a push planned earlier this cycle): a bounded set (one
        // cycle's appends).
        let mut overlay_pids: Vec<Pid> = ov.appended_pids();
        overlay_pids.sort_unstable();
        for pid in overlay_pids {
            if seen.contains(&pid) {
                continue;
            }
            if let Some(part) = self.partition(ov, pid)? {
                if part.tenant == tenant && part.queue == queue {
                    seen.insert(pid);
                    out.push(pid);
                }
            }
        }
        Ok(out)
    }

    /// Claim frames of ONE partition for one group — the claim core (004
    /// `log_pop_v1`). `None` when nothing was delivered (leased, quiet,
    /// delayed, or caught up); the effects and the outcome are pushed on
    /// delivery, and a seal is written for the empty-retained case.
    #[allow(clippy::too_many_arguments)]
    fn claim_one(
        &self,
        ov: &mut Overlay,
        effects: &mut Vec<Effect>,
        cfg: &QueueConfig,
        group_row: Option<&GroupRow>,
        cmd: &PopCommand,
        pid: Pid,
        budget: i32,
    ) -> Result<Option<PopClaim>, Refusal> {
        use crate::rsm::dbgctr::{inc, C};
        let budget = budget.max(1);
        let Some(part) = self.partition(ov, pid)? else {
            inc(&C.claim_none_nopart, 1);
            return Ok(None);
        };
        let now = self.now_us;

        // Conflation: the STORED policy wins over the request (a stale broker
        // cache must not conflate a plain group).
        let conflate = match group_row {
            Some(g) => g.meta.conflation,
            None => cmd.conflate,
        };

        // window_buffer quiet debounce: a partition written within the last
        // window_buffer seconds delivers nothing (a burst is one batch). The
        // hot-list wheel owns the hold on the pinned path (skip_window_debounce).
        if cfg.window_buffer > 0 && !cmd.skip_window_debounce {
            let win = cfg.window_buffer as i64 * SEC_US;
            if let Some(newest) = self.newest_seg(ov, &part)? {
                if newest.created_at_us > now - win {
                    inc(&C.claim_none_window, 1);
                    return Ok(None);
                }
            }
        }

        // Seeding on the FIRST contact of this (partition, group).
        let existing = self.cursor(ov, pid, &cmd.group)?;
        let first_contact = existing.is_none();
        let mut cur = match existing {
            Some(c) => {
                if lease_live(&c, now) {
                    // Held by a live lease (this worker or another): skip.
                    inc(&C.claim_none_leased, 1);
                    return Ok(None);
                }
                c
            }
            None => {
                let seed = match group_row {
                    Some(gr) => self.seed_committed(ov, &part, gr)?,
                    None => self.seed_from_intent(ov, &part, &cmd.sub)?.unwrap_or(-1),
                };
                cursor_fresh(seed, now)
            }
        };

        let committed = cur.committed;
        let wanted = committed + 1;
        let deadline = if cfg.delayed_processing > 0 {
            Some(now - cfg.delayed_processing as i64 * SEC_US)
        } else {
            None
        };
        let last_offset = part.last_offset;

        // The segments the claim needs (from `txns` + overlay), and whether the
        // partition holds any live segment at all (the empty-partition seal).
        //
        // PERF-I: on the steady-state path the O(claimed) BOUNDED gather reads
        // one txns row per segment the claim actually touches — starting at the
        // segment covering `wanted`, stopping once the budget is met or a
        // deferred segment is reached — and folds the delivered hashes into that
        // same pass. The baseline path scans `segs_from(log_start)`, i.e. the
        // whole retained history of the partition (unbounded with retention off),
        // and then reads the delivered set with a SECOND scan (`hashes_in_range`).
        // The gather is taken only when the shape makes it provably equal to the
        // baseline: not conflating (which needs the tail), no front retention gap
        // (`wanted ≥ log_start`), and the partition has live segments — every
        // other shape falls back and is byte-identical to today.
        let has_live = last_offset >= part.log_start as i64;
        let bounded =
            self.claim_from_ring() && !conflate && has_live && wanted >= part.log_start as i64;
        let segs: Vec<SegH> = if bounded {
            self.claim_gather_bounded(ov, &part, wanted, budget, deadline, !cmd.auto_ack)?
        } else {
            self.segs_from(ov, &part, part.log_start)?
                .into_iter()
                .map(SegH::plain)
                .collect()
        };
        let has_segments = if bounded { has_live } else { !segs.is_empty() };
        let fresh_enough = |s: &SegH| deadline.is_none_or(|d| s.created_at_us <= d);

        let mut taken: i64 = 0;
        let mut start: Option<i64> = None;
        let mut last: i64 = -1;

        if conflate {
            // ONE backward step to the newest fresh segment, serving its last
            // frame and leasing (committed, tail].
            if let Some(head) = segs.iter().rev().find(|s| fresh_enough(s)) {
                if head.end as i64 >= wanted {
                    taken = 1;
                    start = Some(wanted);
                    last = head.end as i64;
                }
            }
        } else {
            // Head probe: the greatest base <= wanted.
            if let Some(h) = segs.iter().rev().find(|s| (s.base as i64) <= wanted) {
                if h.end as i64 >= wanted && fresh_enough(h) {
                    let avail = h.end as i64 - wanted + 1;
                    let take = avail.min(budget as i64);
                    taken = take;
                    start = Some(wanted);
                    last = wanted + take - 1;
                }
            }
            if taken < budget as i64 {
                for s in segs.iter().filter(|s| s.base as i64 > wanted) {
                    if !fresh_enough(s) {
                        break; // created_at monotone: everything after is deferred too
                    }
                    let avail = s.end as i64 - s.base as i64 + 1;
                    let take = avail.min(budget as i64 - taken);
                    if take <= 0 {
                        break;
                    }
                    if start.is_none() {
                        start = Some(s.base as i64); // retention gap: the batch starts here
                    }
                    taken += take;
                    last = s.base as i64 + take - 1;
                    if taken >= budget as i64 {
                        break;
                    }
                }
            }
        }

        if taken == 0 {
            // Empty-partition seal (starvation fix): a partition whose segments
            // were ALL removed by retention keeps last_offset > committed for
            // ever. Seal the cursor to the tail, but ONLY with no segments at
            // all (a deferred segment must not be skipped). A first contact that
            // delivered nothing writes NO cursor otherwise — the seed is stable,
            // so a re-poll recomputes it (unlike pgless, whose `new` seed was
            // the moving tail).
            let sealed = last_offset > cur.committed && !has_segments;
            if sealed {
                inc(&C.claim_none_sealed, 1);
            } else if segs.is_empty() {
                inc(&C.claim_none_taken0_nosegs, 1);
            } else {
                inc(&C.claim_none_taken0_segs, 1);
            }
            if sealed {
                cur.committed = last_offset;
                let e = Effect::CursorSet {
                    pid,
                    group: cmd.group.clone(),
                    row: cur,
                };
                effects.push(e);
            } else if first_contact {
                // First contact, segments present but not claimable now
                // (deferred/quiet/leased handled above): leave no cursor; the
                // pending row the seeding append wrote keeps the ring arming
                // this partition, and a re-poll seeds the same value.
            }
            return Ok(None);
        }

        // Commit the delivery. `start_off` anchors the attempt marker (the
        // episode start, which for a conflating claim is `wanted`); the CLAIM's
        // read range is the single frame at the tail when conflating, the whole
        // run otherwise.
        let start_off = start.expect("taken > 0 implies a start") as u64;
        let (read_start, read_end) = if conflate {
            (last as u64, last as u64)
        } else {
            (start_off, last as u64)
        };
        // The delivered set (O16), recorded on the cursor for the ack fast path.
        // On the bounded path the hashes were read in the gather pass, so there
        // is NO second scan; the baseline reads them with `hashes_in_range`. An
        // auto-ack claim discards the delivered set (`cur.delivered = Vec::new()`
        // below), so the bounded gather never collected it — the baseline still
        // reads it here, exactly as today, and discards it the same way.
        let delivered = if bounded {
            if cmd.auto_ack {
                Vec::new()
            } else {
                delivered_from_gathered(&segs, read_start, read_end)
            }
        } else {
            self.hashes_in_range(ov, pid, read_start, read_end)?
        };
        let delivery_attempt: u32;
        let lease_expires: Option<i64>;
        if cmd.auto_ack {
            cur.committed = last;
            cur.worker = None;
            cur.lease_expires_at_us = None;
            cur.lease_acquired_at_us = None;
            cur.batch_end = None;
            cur.lease_conflated = false;
            cur.total_consumed += taken as u64;
            cur.delivered = Vec::new();
            delivery_attempt = 1;
            lease_expires = None;
        } else {
            // Attempt tracking: the SAME first offset as the previous non-auto
            // delivery is a redelivery (nack or lease expiry left the cursor);
            // anywhere else resets to 1. The retry BUDGET is
            // `batch_retry_count`, charged only by an explicit `failed` ack —
            // never by a lease expiry.
            cur.attempt_count = if cur.attempt_offset == Some(start_off) {
                cur.attempt_count + 1
            } else {
                1
            };
            cur.attempt_offset = Some(start_off);
            let exp = now + cmd.lease_seconds.max(1) as i64 * SEC_US;
            cur.worker = Some(cmd.worker.clone());
            cur.lease_expires_at_us = Some(exp);
            cur.lease_acquired_at_us = Some(now);
            cur.batch_end = Some(last as u64);
            cur.lease_conflated = conflate;
            cur.delivered = delivered;
            // total_consumed is untouched until the ack retires the batch.
            delivery_attempt = cur.attempt_count.max(1);
            lease_expires = Some(exp);
        }

        effects.push(Effect::CursorSet {
            pid,
            group: cmd.group.clone(),
            row: cur,
        });
        inc(&C.claim_ok, 1);
        inc(&C.claim_ok_msgs, taken.max(0) as u64);

        Ok(Some(PopClaim {
            pid,
            start_offset: read_start,
            end_offset: read_end,
            worker: cmd.worker.clone(),
            lease_expires_at_us: lease_expires,
            delivery_attempt,
            conflated: conflate,
        }))
    }

    /// The segments a non-conflating claim could touch, gathered FORWARD from
    /// the segment covering `wanted`, each carrying its per-frame hashes when
    /// `need_hashes` — the O(claimed) replacement (PERF-I) for
    /// `segs_from(log_start)` + `hashes_in_range` on the steady-state claim path.
    ///
    /// The gather is a strict SUPERSET of the segments the claim arithmetic will
    /// consume, so feeding it into that arithmetic (unchanged) yields the same
    /// `(start, last, taken)` as the full `segs_from`, and the same delivered set
    /// over the claimed range — while reading only the rows the budget reaches.
    /// It stops at the first segment deferred past the freshness deadline (the
    /// forward loop breaks there, `created_at` monotone) or once the frames
    /// available from `wanted` meet the budget. The caller has already gated on
    /// `wanted ≥ log_start` and the partition having live segments, so the
    /// covering segment exists and no front retention gap is in play.
    fn claim_gather_bounded(
        &self,
        ov: &Overlay,
        part: &PartView,
        wanted: i64,
        budget: i32,
        deadline: Option<i64>,
        need_hashes: bool,
    ) -> Result<Vec<SegH>, Refusal> {
        let budget = budget.max(1) as i64;
        let fresh = |c: i64| deadline.is_none_or(|d| c <= d);
        let mut out: Vec<SegH> = Vec::with_capacity(8);
        let mut avail: i64 = 0;

        let mut stop = false;
        if self.cfg.index_mode == IndexMode::Segment {
            // O(claimed) forward walk of the committed index from `wanted`: it
            // reads — and, for a delivered set (`need_hashes`), `pread`s — ONLY
            // the ≤ budget frames the claim consumes, never the whole cursor→tail
            // span. This is the PERF-I-equivalent for segment-authority dedup:
            // the delivered-set cost is independent of how far the cursor lags.
            // (`wanted >= log_start` is the caller's gate, so the range is
            // contiguous; the walk stops at a deferred frame or budget.) The
            // source is the QUEUE LOG when `QUEEN_RAFT_QLOG` is on (Phase A2), the
            // `.seg` files otherwise — SAME committed bound (`ctx.committed_end`),
            // SAME O(claimed) walk, so the delivered set is identical.
            if let Some(ctx) = self.seg_ctx(part.pid)? {
                self.claim_frames_of(
                    part.pid,
                    &ctx,
                    wanted as u64,
                    need_hashes,
                    &mut |base, end_incl, created, hashes| {
                        let deferred = !fresh(created);
                        let seg_from = (base as i64).max(wanted);
                        if end_incl as i64 >= seg_from {
                            avail += end_incl as i64 - seg_from + 1;
                        }
                        out.push(SegH {
                            base,
                            end: end_incl,
                            created_at_us: created,
                            hashes: hashes.map(|h| {
                                h.chunks_exact(16)
                                    .map(|c| <[u8; 16]>::try_from(c).unwrap())
                                    .collect()
                            }),
                        });
                        if deferred || avail >= budget {
                            stop = true;
                            return false;
                        }
                        true
                    },
                )?;
            }
        } else {
            // The committed segment covering `wanted` (greatest base ≤ wanted),
            // or `wanted` when none does — the txns scan starts there.
            let start_base = self
                .seg_base_covering(part.pid, wanted as u64)?
                .unwrap_or(wanted as u64);
            let prefix = keys::txns_prefix(part.pid);
            let from = keys::txns(part.pid, start_base);
            let mut bad: Option<StoreError> = None;
            self.reads()
                .scan_raw(Keyspace::Txns, &from, &prefix, usize::MAX, &mut |k, v| {
                    if stop {
                        return false;
                    }
                    match (keys::txns_base_of(k), TxnsRow::decode(v)) {
                        (Some(base), Ok(row)) => {
                            let deferred = !fresh(row.created_at_us);
                            // frames this segment offers from `wanted` on
                            let seg_from = (base as i64).max(wanted);
                            if row.end as i64 >= seg_from {
                                avail += row.end as i64 - seg_from + 1;
                            }
                            out.push(SegH {
                                base,
                                end: row.end,
                                created_at_us: row.created_at_us,
                                hashes: need_hashes.then(|| row.iter_hashes().collect()),
                            });
                            if deferred || avail >= budget {
                                stop = true;
                                return false;
                            }
                            true
                        }
                        _ => {
                            bad = Some(StoreError::corrupt(Keyspace::Txns, "txns row"));
                            false
                        }
                    }
                })
                .map_err(store_err)?;
            if let Some(e) = bad {
                return Err(store_err(e));
            }
        }

        // Overlay appends are the tail (base > every committed offset, so always
        // >= `wanted`): fold the ones the claim can reach, honouring the same
        // stop rule. One cycle's appends, so a bounded set.
        if !stop {
            if let Some(o) = ov.parts.get(&part.pid) {
                for a in &o.appends {
                    if (a.base as i64) < wanted {
                        continue;
                    }
                    let deferred = !fresh(a.created_at_us);
                    let seg_from = (a.base as i64).max(wanted);
                    if a.end as i64 >= seg_from {
                        avail += a.end as i64 - seg_from + 1;
                    }
                    out.push(SegH {
                        base: a.base,
                        end: a.end,
                        created_at_us: a.created_at_us,
                        hashes: need_hashes.then(|| a.hashes.clone()),
                    });
                    if deferred || avail >= budget {
                        break;
                    }
                }
            }
        }
        out.sort_by_key(|s| s.base);
        Ok(out)
    }

    /// The newest segment of a partition (committed `txns` tail or the last
    /// overlay append), for the window-buffer debounce.
    fn newest_seg(&self, ov: &Overlay, part: &PartView) -> Result<Option<Seg>, Refusal> {
        // The overlay's own appends are always the tail if present.
        if let Some(a) = ov.newest_append(part.pid) {
            return Ok(Some(a));
        }
        if part.last_offset < part.log_start as i64 {
            return Ok(None);
        }
        let base = self.seg_base_covering(part.pid, part.last_offset as u64)?;
        match base {
            Some(b) => {
                // Read that one row for its created_at.
                let segs = self.segs_from(ov, part, b)?;
                Ok(segs.into_iter().last())
            }
            None => Ok(None),
        }
    }
}

/// The distinct transaction hashes in the inclusive offset range `[lo, hi]`,
/// read from the segments the bounded claim gather already scanned (PERF-I) —
/// the same result `Planner::hashes_in_range` produces with a fresh scan, in the
/// same first-seen order (the gather is base-sorted, committed segments before
/// overlay appends, frames in offset order within each), so a claim's recorded
/// delivered set (O16) is byte-identical whichever path produced it.
fn delivered_from_gathered(segs: &[SegH], lo: u64, hi: u64) -> Vec<[u8; 16]> {
    if hi < lo {
        return Vec::new();
    }
    // Pre-sized, and a hash set instead of a B-tree (only membership is used;
    // `out` keeps the first-seen order): one allocation each instead of a node
    // or a regrow every few hashes on a 1000-message claim.
    let span = ((hi - lo) as usize).saturating_add(1).min(1 << 16);
    let mut seen: std::collections::HashSet<[u8; 16], crate::rsm::fasthash::FxBuild> =
        std::collections::HashSet::with_capacity_and_hasher(span, Default::default());
    let mut out: Vec<[u8; 16]> = Vec::with_capacity(span);
    for s in segs {
        let Some(hashes) = &s.hashes else { continue };
        for (i, h) in hashes.iter().enumerate() {
            let off = s.base + i as u64;
            if off >= lo && off <= hi && seen.insert(*h) {
                out.push(*h);
            }
        }
    }
    out
}

/// The claims an outcome carries (or empty for a non-pop outcome).
fn claims_of(outcome: Outcome) -> Vec<PopClaim> {
    match outcome {
        Outcome::Pop(p) => p.claims,
        _ => Vec::new(),
    }
}

impl Overlay {
    /// The pids the overlay has appended to this cycle/pipeline.
    fn appended_pids(&self) -> Vec<Pid> {
        self.parts
            .iter()
            .filter(|(_, p)| !p.appends.is_empty())
            .map(|(pid, _)| *pid)
            .collect()
    }

    /// The newest overlay append of a partition, as a [`Seg`].
    fn newest_append(&self, pid: Pid) -> Option<Seg> {
        self.parts.get(&pid).and_then(|p| {
            p.appends.last().map(|a| Seg {
                base: a.base,
                end: a.end,
                created_at_us: a.created_at_us,
            })
        })
    }
}
